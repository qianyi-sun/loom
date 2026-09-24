"""Exact-source private ingress tooling, independent of certificate authority."""
from __future__ import annotations

import fcntl
import hashlib
import io
import json
import os
import re
import stat
import sys
import zipfile
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.ops.nebius_certificate_gateway import _directory, _read, _write, run_private

SCRIPTS = (
    "scripts/ops/nebius_certificate_gateway.py", "scripts/ops/nebius_certificates.py",
    "scripts/ops/nebius_ingress_bootstrap.py", "scripts/ops/nebius_ingress_entry.py",
    "scripts/ops/nebius_ingress_gateway.py", "scripts/ops/nebius_ingress_stage.py",
    "scripts/ops/nebius_ingress_cutover.py", "scripts/ops/nebius_ingress_image.py",
    "scripts/ops/nebius_ingress_operation.py", "scripts/ops/nebius_ingress_probe.py",
)
LIMITS = {**dict.fromkeys(SCRIPTS, 262_144), "uv": 80 * 1024 * 1024,
          "requirements.txt": 262_144, "installation.json": 16_384, "manifest.json": 16_384}
MAX_BUNDLE = 100 * 1024 * 1024
MAX_WHEEL = 16 * 1024 * 1024
COMMANDS = {"loom-nebius-ingress-v1": "install", "loom-nebius-ingress-rollback-v1": "rollback",
            "loom-nebius-ingress-image-intent-v1": "image-intent"}
_ENTRY = "import sys; sys.path.insert(0, sys.argv[1]); from scripts.ops.nebius_ingress_entry import main; raise SystemExit(main(sys.argv[2], sys.argv[3]))"


class BootstrapError(RuntimeError):
    """Private ingress tooling unavailable; preserve installed state."""


def validate_config(config: dict[str, Any]) -> None:
    """Stdlib preflight before extraction; typed runtime validation follows."""
    fields = {"schema", "source_sha", "candidate", "state_dir", "certificate_config", "kubeconfig", "kubectl",
              "cluster_id", "api_server", "ingress_class", "image", "binding"}
    binding_fields = {"installation_id", "certificate_installation_id", "namespace", "namespace_uid",
                      "kube_system_uid", "child_domain", "management_host"}
    try:
        if (set(config) != fields or config["schema"] != "loom.nebius-ingress-installation.v1"
                or set(config["binding"]) != binding_fields
                or any(not isinstance(config[key], str) or not 0 < len(config[key]) <= 1024 for key in fields - {"binding"})
                or any(not isinstance(value, str) or not 0 < len(value) <= 253 for value in config["binding"].values())):
            raise ValueError()
        for key in ("source_sha", "candidate"):
            if not re.fullmatch(r"[0-9a-f]{40}", config[key]):
                raise ValueError()
        if not re.fullmatch(r"mk8scluster-[a-z0-9]+", config["cluster_id"]):
            raise ValueError()
        for key in ("installation_id", "certificate_installation_id", "namespace_uid", "kube_system_uid"):
            value = config["binding"][key]
            if str(UUID(value)) != value or UUID(value).int == 0:
                raise ValueError()
        for key in ("state_dir", "certificate_config", "kubeconfig", "kubectl"):
            path = Path(config[key])
            if not path.is_absolute() or path != path.resolve() or not re.fullmatch(r"/[A-Za-z0-9_./-]+", str(path)):
                raise ValueError()
        state = Path(config["state_dir"])
        if state.name != "state" or state.parent.name != "nebius-ingress":
            raise ValueError()
    except (ValueError, KeyError, TypeError, AttributeError):
        raise BootstrapError("invalid ingress installation metadata") from None


def unpack_bundle(content: bytes) -> tuple[dict[str, bytes], dict[str, Any]]:
    if not 0 < len(content) <= MAX_BUNDLE:
        raise BootstrapError("ingress bundle exceeds bound")
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            entries = archive.infolist()
            names = [entry.filename for entry in entries]
            wheels = {name for name in names if name.startswith("wheels/")}
            if (len(names) != len(set(names)) or set(names) != set(LIMITS) | wheels or len(wheels) != 2
                    or not all(sum(bool(re.fullmatch(r"wheels/" + package + r"-[0-9][0-9.]*-py3-none-any\.whl", name))
                                   for name in wheels) == 1 for package in ("loom", "loom_bundle_checksum"))):
                raise BootstrapError("unexpected ingress bundle members")
            if any(entry.file_size > LIMITS.get(entry.filename, MAX_WHEEL) or entry.is_dir()
                   or stat.S_ISLNK(entry.external_attr >> 16) for entry in entries):
                raise BootstrapError("ingress bundle member exceeds boundary")
            files = {entry.filename: archive.read(entry) for entry in entries}
        manifest = json.loads(files["manifest.json"])
        if manifest != {name: hashlib.sha256(value).hexdigest() for name, value in files.items() if name != "manifest.json"}:
            raise BootstrapError("ingress bundle digest mismatch")
        config = json.loads(files["installation.json"])
        validate_config(config)
        return files, config
    except BootstrapError:
        raise
    except Exception:
        raise BootstrapError("invalid ingress bundle") from None


def command(release: Path, action: str) -> list[str]:
    if action not in {"install", "rollback", "qualify", "image-intent"}:
        raise BootstrapError("ingress action outside installed authority")
    return [str(release / "venv/bin/python"), "-I", "-c", _ENTRY,
            str(release), str(release / "installation.json"), action]


def prepare_release(content: bytes) -> tuple[Path, Path]:
    files, config = unpack_bundle(content)
    root = Path(config["state_dir"]).parent
    try:
        _directory(root)
        lock_path = root / "tooling.lock"
        descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        with os.fdopen(descriptor, "rb+") as lock:
            _read(lock_path, 1)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            releases = root / "releases"
            _directory(releases)
            release = releases / hashlib.sha256(content).hexdigest()
            if release.exists() or release.is_symlink():
                _directory(release)
                if _read(release / "complete", 64) != b"complete":
                    raise BootstrapError("incomplete ingress tooling requires reconciliation")
                for name, expected in files.items():
                    if _read(release / name, LIMITS.get(name, MAX_WHEEL)) != expected:
                        raise BootstrapError("installed ingress tooling changed")
                return release, release / "installation.json"
            if sum(1 for _ in releases.iterdir()) >= 8:
                raise BootstrapError("retained ingress tooling limit reached")
            for path in (release, release / "scripts", release / "scripts/ops", release / "wheels"):
                _directory(path)
            for name, value in files.items():
                _write(release / name, value, executable=name == "uv")
            uv, python = str(release / "uv"), str(release / "venv/bin/python")
            run_private([uv, "venv", "--no-config", "--no-cache", "--no-python-downloads",
                         "--python", "/usr/bin/python3", str(release / "venv")], timeout=90)
            run_private([uv, "pip", "sync", "--no-config", "--no-cache", "--python", python,
                         "--require-hashes", "--only-binary", ":all:", "--index-url", "https://pypi.org/simple",
                         str(release / "requirements.txt")], timeout=600)
            run_private([uv, "pip", "install", "--no-config", "--no-cache", "--python", python,
                         "--offline", "--no-deps", *sorted(str(release / name) for name in files if name.startswith("wheels/"))],
                        timeout=90)
            run_private(command(release, "qualify"), timeout=60)
            _write(release / "complete", b"complete")
            return release, release / "installation.json"
    except BootstrapError:
        raise
    except Exception:
        raise BootstrapError("ingress tooling preparation incomplete; retain private state") from None


def safe_report(raw: bytes) -> dict[str, Any]:
    try:
        if len(raw) > 65_536:
            raise ValueError()
        value = json.loads(raw)
        if value["status"] not in {"complete", "rolled_back", "skipped_busy", "skipped_locked", "image_copy_once", "image_readback_only"}:
            raise ValueError()
        result = {key: value[key] for key in ("status", "installation_id", "candidate", "namespace")}
        if (str(UUID(result["installation_id"])) != result["installation_id"] or UUID(result["installation_id"]).int == 0
                or not re.fullmatch(r"[0-9a-f]{40}", result["candidate"])
                or not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", result["namespace"])):
            raise ValueError()
        for key in ("service_uid", "controller_uid", "secret_uid"):
            if key in value:
                if str(UUID(value[key])) != value[key] or UUID(value[key]).int == 0:
                    raise ValueError()
                result[key] = value[key]
        if "fingerprint_sha256" in value:
            if not re.fullmatch(r"[0-9a-f]{64}", value["fingerprint_sha256"]):
                raise ValueError()
            result["fingerprint_sha256"] = value["fingerprint_sha256"]
        if value["status"] == "complete" and not {"controller_uid", "secret_uid", "fingerprint_sha256"} <= result.keys():
            raise ValueError()
        if value["status"] in {"image_copy_once", "image_readback_only"}:
            if not re.fullmatch(r"cr\.[a-z0-9-]+\.nebius\.cloud/[a-z0-9]+/loom-shared-ingress@sha256:[0-9a-f]{64}", value["image"]):
                raise ValueError()
            result["image"] = value["image"]
        return result
    except Exception:
        raise BootstrapError("invalid ingress operation report") from None


def authorized_main(expected_sha256: str) -> int:
    action = COMMANDS.get(os.environ.get("SSH_ORIGINAL_COMMAND", ""))
    if action is None or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        return 126
    content = sys.stdin.buffer.read(MAX_BUNDLE + 1)
    if not 0 < len(content) <= MAX_BUNDLE or hashlib.sha256(content).hexdigest() != expected_sha256:
        return 126
    try:
        release, _ = prepare_release(content)
        report = safe_report(run_private(command(release, action), timeout=1800))
        print(json.dumps(report, sort_keys=True))
        return 0 if report["status"] in {"complete", "rolled_back", "image_copy_once", "image_readback_only"} else 1
    except Exception:
        print("protected ingress operation incomplete; preserve private recovery state", file=sys.stderr)
        return 1
