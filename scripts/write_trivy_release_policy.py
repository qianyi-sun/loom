#!/usr/bin/env python3
"""Write and verify the fixed Trivy policy used by image release scans."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import stat
import sys
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import NamedTuple


class TrivyException(NamedTuple):
    """One reviewed, temporary vulnerability-policy exception."""

    vulnerability_id: str
    purl: str
    expires_at: date
    statement: str

TRIVY_SCANNER_NAME = "Trivy"
TRIVY_VERSION = "v0.70.0"
_TRIVY_EXCEPTION_PURL = "pkg:deb/debian/perl-base"
_TRIVY_EXCEPTION_STATEMENT = "No fixed Debian package was available on 2026-08-12."
TRIVY_CONFIG_BYTES = (
    b"exit-code: 1\n"
    b"pkg:\n"
    b"  types:\n"
    b"    - os\n"
    b"    - library\n"
    b"scan:\n"
    b"  scanners:\n"
    b"    - vuln\n"
    b"severity:\n"
    b"  - CRITICAL\n"
    b"timeout: 10m0s\n"
    b"vulnerability:\n"
    b"  ignore-unfixed: false\n"
)
TRIVY_EXCEPTIONS = (
    TrivyException(
        "CVE-2026-13221",
        _TRIVY_EXCEPTION_PURL,
        date(2026, 9, 12),
        _TRIVY_EXCEPTION_STATEMENT,
    ),
    TrivyException(
        "CVE-2026-42496",
        _TRIVY_EXCEPTION_PURL,
        date(2026, 9, 12),
        _TRIVY_EXCEPTION_STATEMENT,
    ),
    TrivyException(
        "CVE-2026-57433",
        _TRIVY_EXCEPTION_PURL,
        date(2026, 9, 12),
        _TRIVY_EXCEPTION_STATEMENT,
    ),
    TrivyException(
        "CVE-2026-8376",
        _TRIVY_EXCEPTION_PURL,
        date(2026, 9, 12),
        _TRIVY_EXCEPTION_STATEMENT,
    ),
)
TRIVY_IGNORE_BYTES = (
    b"vulnerabilities:\n"
    b"  - id: CVE-2026-13221\n"
    b"    purls:\n"
    b'      - "pkg:deb/debian/perl-base"\n'
    b"    expired_at: 2026-09-12\n"
    b"    statement: No fixed Debian package was available on 2026-08-12.\n"
    b"  - id: CVE-2026-42496\n"
    b"    purls:\n"
    b'      - "pkg:deb/debian/perl-base"\n'
    b"    expired_at: 2026-09-12\n"
    b"    statement: No fixed Debian package was available on 2026-08-12.\n"
    b"  - id: CVE-2026-57433\n"
    b"    purls:\n"
    b'      - "pkg:deb/debian/perl-base"\n'
    b"    expired_at: 2026-09-12\n"
    b"    statement: No fixed Debian package was available on 2026-08-12.\n"
    b"  - id: CVE-2026-8376\n"
    b"    purls:\n"
    b'      - "pkg:deb/debian/perl-base"\n'
    b"    expired_at: 2026-09-12\n"
    b"    statement: No fixed Debian package was available on 2026-08-12.\n"
)
TRIVY_CONFIG_SHA256 = "35492da1d08b142bd1489ac54ecdedab62634b7b3095a37cebbe10b61df1adac"
TRIVY_IGNORE_SHA256 = "83156c673c73bc58e7848876fe2144f36e7ab2dc147b7a6a55a41bfa2a88ee29"


class TrivyPolicyError(RuntimeError):
    """The controlled release scan policy could not be materialized exactly."""


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _verify_constant_hashes() -> None:
    if _sha256(TRIVY_CONFIG_BYTES) != TRIVY_CONFIG_SHA256:
        raise TrivyPolicyError("controlled Trivy configuration constant is invalid")
    if _sha256(TRIVY_IGNORE_BYTES) != TRIVY_IGNORE_SHA256:
        raise TrivyPolicyError("controlled Trivy ignore constant is invalid")


def _verify_exception_policy(today: date) -> None:
    declared_ids = tuple(exception.vulnerability_id for exception in TRIVY_EXCEPTIONS)
    rendered_lines = ["vulnerabilities:"]
    for exception in TRIVY_EXCEPTIONS:
        rendered_lines.extend(
            (
                f"  - id: {exception.vulnerability_id}",
                "    purls:",
                f'      - "{exception.purl}"',
                f"    expired_at: {exception.expires_at.isoformat()}",
                f"    statement: {exception.statement}",
            )
        )
    rendered_ignore = ("\n".join(rendered_lines) + "\n").encode("ascii")
    if (
        declared_ids != tuple(sorted(set(declared_ids)))
        or rendered_ignore != TRIVY_IGNORE_BYTES
    ):
        raise TrivyPolicyError("controlled Trivy exceptions are inconsistent")
    for exception in TRIVY_EXCEPTIONS:
        if re.fullmatch(r"CVE-[0-9]{4}-[0-9]{4,}", exception.vulnerability_id) is None:
            raise TrivyPolicyError("controlled Trivy exception identifier is invalid")
        if exception.purl != _TRIVY_EXCEPTION_PURL:
            raise TrivyPolicyError("controlled Trivy exception package is invalid")
        if not exception.statement or "\n" in exception.statement:
            raise TrivyPolicyError("controlled Trivy exception statement is invalid")
        if today >= exception.expires_at:
            raise TrivyPolicyError(
                f"controlled Trivy exception expired: {exception.vulnerability_id}"
            )


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


def write_release_policy(
    config_file: Path,
    ignore_file: Path,
    *,
    today: date | None = None,
) -> None:
    """Atomically write and verify the reviewed config and temporary exceptions."""

    _verify_constant_hashes()
    _verify_exception_policy(today or datetime.now(UTC).date())
    if config_file == ignore_file:
        raise TrivyPolicyError("controlled Trivy policy paths must be distinct")
    if ignore_file.suffix not in {".yaml", ".yml"}:
        raise TrivyPolicyError("controlled Trivy ignore path must select YAML parsing")
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
    "TRIVY_EXCEPTIONS",
    "TRIVY_IGNORE_BYTES",
    "TRIVY_IGNORE_SHA256",
    "TRIVY_SCANNER_NAME",
    "TRIVY_VERSION",
    "TrivyException",
    "TrivyPolicyError",
    "write_release_policy",
]
