#!/usr/bin/env python3
"""Stdlib-only private gateway bootstrap, invoked by protected nebius-rollout.

Input is a bounded bundle of trusted tooling, never personal source or Secrets.
"""
from __future__ import annotations

import fcntl
import hashlib
import io
import json
import os
import re
import stat
import subprocess
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

LIMITS = {"uv": 80 * 1024 * 1024, "requirements.txt": 262_144,
          "scripts/ops/nebius_certificates.py": 262_144,
          "scripts/ops/nebius_dns_challenge.py": 262_144,
          "installation.json": 16_384, "manifest.json": 16_384}
MAX_BUNDLE = 90 * 1024 * 1024


class GatewayError(RuntimeError):
    """Fixed gateway failure, without private paths or process output."""


def run_private(args: list[str], *, timeout: int) -> bytes:
    result = subprocess.run(args, stdin=subprocess.DEVNULL, capture_output=True, timeout=timeout,
                            env={"PATH": os.defpath, "LANG": "C.UTF-8"}, umask=0o077, check=False)
    if result.returncode or len(result.stdout) > 65_536:
        raise GatewayError("private tooling command failed")
    return result.stdout


def _directory(path: Path) -> None:
    if path.absolute() != path.resolve():
        raise GatewayError("tooling directory must not traverse symlinks")
    path.mkdir(mode=0o700, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise GatewayError("private owned tooling directory required")


def _read(path: Path, limit: int) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_mode & 0o077 or info.st_nlink != 1):
            raise GatewayError("private regular tooling file required")
        result = stream.read(limit + 1)
    if len(result) > limit:
        raise GatewayError("tooling file exceeds bound")
    return result


def _write(path: Path, value: bytes, *, executable: bool = False) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o700 if executable else 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())


def prepare_release(content: bytes) -> tuple[Path, Path]:
    if not 0 < len(content) <= MAX_BUNDLE:
        raise GatewayError("tooling bundle exceeds bound")
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            entries = archive.infolist()
            if len(entries) != len(LIMITS) or {entry.filename for entry in entries} != set(LIMITS):
                raise GatewayError("tooling bundle has unexpected files")
            if any(entry.file_size > LIMITS[entry.filename] or entry.is_dir()
                   or stat.S_ISLNK(entry.external_attr >> 16) for entry in entries):
                raise GatewayError("tooling bundle member exceeds boundary")
            files = {entry.filename: archive.read(entry) for entry in entries}
        manifest = json.loads(files["manifest.json"])
        if manifest != {name: hashlib.sha256(value).hexdigest() for name, value in files.items() if name != "manifest.json"}:
            raise GatewayError("tooling bundle digest mismatch")
        config = json.loads(files["installation.json"])
        state = Path(config["state_dir"])
        root = state.parent
        if (not state.is_absolute() or state != state.resolve() or state.name != "state"
                or root.name != "nebius-certificates"):
            raise GatewayError("tooling requires its dedicated certificate root")
    except GatewayError:
        raise
    except (OSError, ValueError, TypeError, KeyError, zipfile.BadZipFile, RecursionError):
        raise GatewayError("invalid tooling bundle") from None
    try:
        _directory(root)
        descriptor = os.open(root / "tooling.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        with os.fdopen(descriptor, "rb+") as lock:
            info = os.fstat(lock.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or info.st_mode & 0o077 or info.st_nlink != 1):
                raise GatewayError("private tooling lock required")
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            releases = root / "releases"
            _directory(releases)
            release = releases / hashlib.sha256(content).hexdigest()
            if release.exists() or release.is_symlink():
                _directory(release)
                if _read(release / "complete", 64) != b"complete":
                    raise GatewayError("incomplete tooling release requires reconciliation")
                for name, expected in files.items():
                    if _read(release / name, LIMITS[name]) != expected:
                        raise GatewayError("tooling release content changed")
                return release, release / "installation.json"
            # Never grow gateway storage without a bound or delete another
            # operation's retained tools. The operator can retire exact releases.
            if sum(1 for _ in releases.iterdir()) >= 8:
                raise GatewayError("retained tooling release limit reached")
            _directory(release)
            _directory(release / "scripts")
            _directory(release / "scripts" / "ops")
            for name, value in files.items():
                _write(release / name, value, executable=name == "uv")
            uv = str(release / "uv")
            python = str(release / "venv" / "bin" / "python")
            run_private([uv, "venv", "--no-config", "--no-cache", "--no-python-downloads",
                         "--python", "/usr/bin/python3", str(release / "venv")], timeout=90)
            run_private([uv, "pip", "sync", "--no-config", "--no-cache", "--python", python,
                         "--require-hashes", "--only-binary", ":all:", "--index-url", "https://pypi.org/simple",
                         str(release / "requirements.txt")], timeout=300)
            _write(release / "complete", b"complete")
            return release, release / "installation.json"
    except OSError:
        raise GatewayError("private tooling storage unavailable or in use") from None


def safe_report(raw: bytes) -> dict[str, Any]:
    if len(raw) > 65_536:
        raise GatewayError("certificate report exceeds bound")
    try:
        value = json.loads(raw)
        if (value["status"] != "qualified" or UUID(value["installation_id"]).int == 0
                or not all(re.fullmatch(r"[0-9a-f]{64}", value[key]) for key in ("generation", "fingerprint_sha256"))
                or not isinstance(value["sans"], list) or len(value["sans"]) != 2
                or any(not isinstance(name, str) or len(name) > 253
                       or not re.fullmatch(r"(?:\*\.)?[a-z0-9.-]+", name) for name in value["sans"])
                or datetime.fromisoformat(value["expires_at"]).tzinfo is None):
            raise GatewayError("invalid certificate report")
        return {key: value[key] for key in ("status", "installation_id", "generation", "fingerprint_sha256", "sans", "expires_at")}
    except GatewayError:
        raise
    except (ValueError, TypeError, KeyError, RecursionError):
        raise GatewayError("invalid certificate report") from None


def main() -> int:
    try:
        release, config = prepare_release(sys.stdin.buffer.read(MAX_BUNDLE + 1))
        result = run_private([str(release / "venv" / "bin" / "python"),
                              str(release / "scripts" / "ops" / "nebius_certificates.py"),
                              "issue", "--config", str(config)], timeout=1900)
        print(json.dumps(safe_report(result), sort_keys=True))
        return 0
    except Exception:
        print("protected certificate operation failed; preserve gateway state for reconciliation", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
