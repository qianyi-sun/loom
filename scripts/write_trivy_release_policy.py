#!/usr/bin/env python3
"""Write and verify the fixed Trivy policy used by image release scans."""

from __future__ import annotations

import argparse
import hashlib
import os
import stat
import sys
import tempfile
from pathlib import Path

TRIVY_SCANNER_NAME = "Trivy"
TRIVY_VERSION = "v0.70.0"
TRIVY_CONFIG_BYTES = (
    b"exit-code: 1\n"
    b"scan:\n"
    b"  scanners:\n"
    b"    - vuln\n"
    b"severity:\n"
    b"  - CRITICAL\n"
    b"timeout: 10m0s\n"
    b"vulnerability:\n"
    b"  ignore-unfixed: false\n"
    b"  type:\n"
    b"    - os\n"
    b"    - library\n"
)
TRIVY_IGNORE_BYTES = b""
TRIVY_CONFIG_SHA256 = "11c249a9a4b4c3b45c521d424a83a619ff25e4e02c6b205ea38a946d376052bf"
TRIVY_IGNORE_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


class TrivyPolicyError(RuntimeError):
    """The controlled release scan policy could not be materialized exactly."""


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _verify_constant_hashes() -> None:
    if _sha256(TRIVY_CONFIG_BYTES) != TRIVY_CONFIG_SHA256:
        raise TrivyPolicyError("controlled Trivy configuration constant is invalid")
    if _sha256(TRIVY_IGNORE_BYTES) != TRIVY_IGNORE_SHA256:
        raise TrivyPolicyError("controlled Trivy ignore constant is invalid")


def _write_exact_regular_file(path: Path, payload: bytes, expected_sha256: str) -> None:
    if not path.is_absolute():
        raise TrivyPolicyError("controlled Trivy policy paths must be absolute")
    parent = path.parent
    parent_mode = parent.lstat().st_mode
    if not stat.S_ISDIR(parent_mode) or parent.is_symlink():
        raise TrivyPolicyError("controlled Trivy policy parent is invalid")

    temporary_path: Path | None = None
    try:
        descriptor, raw_temporary_path = tempfile.mkstemp(
            prefix=f".{path.name}.",
            dir=parent,
        )
        temporary_path = Path(raw_temporary_path)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        temporary_path.chmod(0o644)
        os.replace(temporary_path, path)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)

    metadata = path.lstat()
    observed = path.read_bytes()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or path.is_symlink()
        or observed != payload
        or _sha256(observed) != expected_sha256
    ):
        raise TrivyPolicyError("controlled Trivy policy verification failed")


def write_release_policy(config_file: Path, ignore_file: Path) -> None:
    """Atomically write and verify the reviewed config and empty ignore file."""

    _verify_constant_hashes()
    if config_file == ignore_file:
        raise TrivyPolicyError("controlled Trivy policy paths must be distinct")
    _write_exact_regular_file(config_file, TRIVY_CONFIG_BYTES, TRIVY_CONFIG_SHA256)
    _write_exact_regular_file(ignore_file, TRIVY_IGNORE_BYTES, TRIVY_IGNORE_SHA256)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-file", type=Path, required=True)
    parser.add_argument("--ignore-file", type=Path, required=True)
    arguments = parser.parse_args()
    try:
        write_release_policy(arguments.config_file, arguments.ignore_file)
    except (OSError, TrivyPolicyError):
        sys.stderr.write("error: controlled Trivy policy generation failed\n")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()


__all__ = [
    "TRIVY_CONFIG_BYTES",
    "TRIVY_CONFIG_SHA256",
    "TRIVY_IGNORE_BYTES",
    "TRIVY_IGNORE_SHA256",
    "TRIVY_SCANNER_NAME",
    "TRIVY_VERSION",
    "TrivyPolicyError",
    "write_release_policy",
]
