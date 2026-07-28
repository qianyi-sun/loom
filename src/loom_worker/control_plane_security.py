"""Secret-safe worker authentication and Control Plane TLS helpers."""

from __future__ import annotations

import stat
from pathlib import Path

from loom_worker.config import WorkerSettings


class WorkerSecurityError(RuntimeError):
    """Worker authentication material is missing, ambiguous, or unsafe."""


def _secure_regular_file(path: Path, *, private: bool) -> Path:
    if not path.is_absolute() or ".." in path.parts:
        raise WorkerSecurityError("worker security paths must be absolute")
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise WorkerSecurityError("worker security material is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or (metadata.st_mode & 0o022)
    ):
        raise WorkerSecurityError(
            "worker security material must be a non-writable single-link file",
        )
    if private and (metadata.st_mode & 0o077):
        raise WorkerSecurityError("worker private material must be owner-only")
    return path.resolve(strict=True)


def resolve_worker_token(settings: WorkerSettings) -> str:
    secret = getattr(settings, "token", None)
    inline = secret.get_secret_value() if secret is not None else ""
    token_file = getattr(settings, "token_file", None)
    if inline and token_file is not None:
        raise WorkerSecurityError("worker token sources are ambiguous")
    if inline:
        if inline != inline.strip() or "\n" in inline or "\r" in inline:
            raise WorkerSecurityError("inline worker token is malformed")
        return inline
    if token_file is None:
        raise WorkerSecurityError("worker token is required")
    path = _secure_regular_file(token_file, private=True)
    try:
        raw = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise WorkerSecurityError("worker token file is unreadable") from exc
    token = raw[:-1] if raw.endswith("\n") else raw
    if (
        not token.startswith("loom_w_")
        or raw not in {token, token + "\n"}
        or token != token.strip()
        or "\n" in token
        or "\r" in token
        or len(token) > 4096
    ):
        raise WorkerSecurityError("worker token file is malformed")
    return token


def _read_opaque_secret(path: Path, *, label: str) -> str:
    secure_path = _secure_regular_file(path, private=True)
    try:
        raw = secure_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise WorkerSecurityError(f"{label} file is unreadable") from exc
    value = raw[:-1] if raw.endswith("\n") else raw
    if (
        not value
        or raw not in {value, value + "\n"}
        or value != value.strip()
        or "\n" in value
        or "\r" in value
        or len(value) > 4096
    ):
        raise WorkerSecurityError(f"{label} file is malformed")
    return value


def resolve_worker_minio_credentials(
    settings: WorkerSettings,
) -> tuple[str, str]:
    access_secret = getattr(settings, "minio_access_key", None)
    secret_secret = getattr(settings, "minio_secret_key", None)
    inline_access = access_secret.get_secret_value() if access_secret is not None else ""
    inline_secret = secret_secret.get_secret_value() if secret_secret is not None else ""
    access_file = getattr(settings, "minio_access_key_file", None)
    secret_file = getattr(settings, "minio_secret_key_file", None)
    inline = (inline_access, inline_secret)
    files = (access_file, secret_file)
    if any(inline) and any(path is not None for path in files):
        raise WorkerSecurityError("MinIO credential sources are ambiguous")
    if all(inline):
        if any(value != value.strip() or "\n" in value or "\r" in value for value in inline):
            raise WorkerSecurityError("inline MinIO credentials are malformed")
        return inline
    if any(inline):
        raise WorkerSecurityError("inline MinIO credentials must be configured together")
    if any(path is None for path in files):
        raise WorkerSecurityError("MinIO credential files must be configured together")
    assert access_file is not None
    assert secret_file is not None
    return (
        _read_opaque_secret(access_file, label="MinIO access key"),
        _read_opaque_secret(secret_file, label="MinIO secret key"),
    )
