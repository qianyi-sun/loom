"""Stable digest-pinned loading for the owner execution preparation policy."""

from __future__ import annotations

import hashlib
import os
import re
import stat
from pathlib import Path

from pydantic import ValidationError

from loom_capacity_manager.contracts import MAX_CONTRACT_BYTES
from loom_capacity_manager.executable_contracts import ExecutionPreparationPolicyV2

MAX_EXECUTION_POLICY_BYTES = MAX_CONTRACT_BYTES
_DIGEST = re.compile(r"[0-9a-f]{64}")
_GENERIC_ERROR = "execution preparation policy is invalid"


class ExecutionPolicyError(ValueError):
    """The pinned execution policy cannot be trusted or parsed exactly."""


def _invalid() -> ExecutionPolicyError:
    return ExecutionPolicyError(_GENERIC_ERROR)


def load_execution_preparation_policy(
    path: Path,
    expected_sha256: str,
) -> ExecutionPreparationPolicyV2:
    """Load one bounded stable regular file and verify its exact digest."""

    if (
        not isinstance(expected_sha256, str)
        or _DIGEST.fullmatch(expected_sha256) is None
        or expected_sha256 == "0" * 64
    ):
        raise _invalid()
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(path, flags)
        try:
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_uid != os.geteuid()
                or stat.S_IMODE(before.st_mode) & 0o022
                or before.st_nlink != 1
                or not 0 < before.st_size <= MAX_EXECUTION_POLICY_BYTES
            ):
                raise _invalid()
            chunks: list[bytes] = []
            remaining = MAX_EXECUTION_POLICY_BYTES + 1
            while remaining > 0:
                chunk = os.read(descriptor, min(64 * 1024, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            payload = b"".join(chunks)
            after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
    except ExecutionPolicyError:
        raise
    except OSError:
        raise _invalid() from None
    stable_fields = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_uid",
        "st_gid",
        "st_nlink",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    if (
        len(payload) != before.st_size
        or len(payload) > MAX_EXECUTION_POLICY_BYTES
        or any(getattr(before, field) != getattr(after, field) for field in stable_fields)
        or hashlib.sha256(payload).hexdigest() != expected_sha256
    ):
        raise _invalid()
    try:
        return ExecutionPreparationPolicyV2.model_validate_json(payload)
    except (ValidationError, ValueError):
        raise _invalid() from None


__all__ = [
    "MAX_EXECUTION_POLICY_BYTES",
    "ExecutionPolicyError",
    "load_execution_preparation_policy",
]
