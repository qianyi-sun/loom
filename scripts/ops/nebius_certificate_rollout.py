#!/usr/bin/env python3
"""Send pinned certificate tooling, but no Secrets, to the protected gateway."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from scripts.ops.nebius_certificate_gateway import LIMITS, MAX_BUNDLE, safe_report  # noqa: E402
from scripts.ops.nebius_certificates import load_installation  # noqa: E402


class RolloutError(RuntimeError):
    """Fixed protected-operation failure; no remote diagnostics are forwarded."""


def build_bundle(config: dict[str, Any], *, uv: Path, requirements: Path) -> bytes:
    files = {"uv": uv.read_bytes(), "requirements.txt": requirements.read_bytes(),
             "installation.json": json.dumps(config, sort_keys=True).encode()}
    for name in ("scripts/ops/nebius_certificates.py", "scripts/ops/nebius_dns_challenge.py",
                 "scripts/ops/nebius_certificate_gateway.py"):
        files[name] = (ROOT / name).read_bytes()
    files["manifest.json"] = json.dumps({name: hashlib.sha256(value).hexdigest() for name, value in files.items()},
                                       sort_keys=True).encode()
    if any(len(value) > LIMITS[name] for name, value in files.items()):
        raise RolloutError("certificate tooling member exceeds bound")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, value in sorted(files.items()):
            # Stable member metadata makes daily reuse content-addressable.
            entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            entry.compress_type = zipfile.ZIP_DEFLATED
            entry.external_attr = 0o100600 << 16
            archive.writestr(entry, value)
    result = buffer.getvalue()
    if len(result) > MAX_BUNDLE:
        raise RolloutError("certificate tooling bundle exceeds bound")
    return result


def transfer(content: bytes, *, target: str, key: Path, known_hosts: Path) -> dict[str, Any]:
    if (not re.fullmatch(r"[a-zA-Z0-9_.-]+@[a-zA-Z0-9_.-]+", target)
            or not key.is_absolute() or not known_hosts.is_absolute() or len(content) > MAX_BUNDLE):
        raise RolloutError("invalid protected certificate transport configuration")
    bootstrap = (ROOT / "scripts" / "ops" / "nebius_certificate_gateway.py").read_text()
    command = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "IdentitiesOnly=yes",
               "-o", "ConnectTimeout=15", "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
               "-o", "UserKnownHostsFile=" + str(known_hosts), "-i", str(key), target,
               shlex.join(["python3", "-c", bootstrap])]
    try:
        result = subprocess.run(command, input=content, capture_output=True, timeout=2400, check=False)
        if result.returncode:
            raise RolloutError("certificate gateway operation failed; preserve private state before retry")
        return safe_report(result.stdout)
    except (OSError, subprocess.TimeoutExpired):
        raise RolloutError("certificate gateway outcome unknown; reconcile before retry") from None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requirements", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    args = parser.parse_args()
    result: dict[str, Any] = {"status": "blocked"}
    try:
        raw = os.environ.get("NEBIUS_CERTIFICATE_INSTALLATION_JSON", "")
        if not 0 < len(raw.encode()) <= 16_384:
            raise RolloutError("protected certificate installation input missing or oversized")
        with tempfile.TemporaryDirectory(prefix="loom-certificate-config-") as directory:
            path = Path(directory) / "installation.json"
            path.touch(mode=0o600)
            path.write_text(raw)
            config = load_installation(path)
        executable = shutil.which("uv")
        if executable is None:
            raise RolloutError("qualified tooling installer unavailable")
        version = subprocess.run([executable, "--version"], capture_output=True, timeout=15, check=False)
        if version.returncode or version.stdout.strip() != b"uv 0.11.26 (x86_64-unknown-linux-gnu)":
            raise RolloutError("tooling installer differs from qualified pin or architecture")
        content = build_bundle(config, uv=Path(executable), requirements=args.requirements)
        result = transfer(content, target=os.environ["LOOM_DEPLOY_SSH_TARGET"],
                          key=Path(os.environ["LOOM_DEPLOY_SSH_KEY_FILE"]),
                          known_hosts=Path(os.environ["LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE"]))
    except Exception:
        # Neither exception messages nor SSH/client output are public evidence.
        result = {"status": "blocked", "reason": "certificate operation failed; reconcile private gateway state"}
    args.evidence_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    (args.evidence_dir / "certificate-result.json").write_text(json.dumps(result, sort_keys=True) + "\n")
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "qualified" else 1


if __name__ == "__main__":
    raise SystemExit(main())
