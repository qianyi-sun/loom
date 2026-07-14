"""Backup manifest helpers for protected Loom cluster environments.

The manifest is intentionally metadata-only: it records paths, sizes, and
digests for operator-created backups without copying secret material into CLI
stdout or durable issue/PR evidence.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

REQUIRED_BACKUP_COMPONENTS: tuple[str, ...] = (
    "postgres",
    "minio",
    "k8s_secrets",
)
PROTECTED_ENVIRONMENTS: frozenset[str] = frozenset(
    {
        "staging",
        "production",
    }
)
DEFAULT_BACKUP_MAX_AGE_HOURS = 24
_PRIVATE_FILE_MODE = 0o600
_PRIVATE_DIRECTORY_MODE = 0o700


class _ComponentInspectionError(ValueError):
    """Safe internal error for strict no-follow component inspection."""


def infer_environment(
    *,
    environment: str | None,
    namespace: str,
) -> str:
    lowered = namespace.lower()
    if "staging" in lowered:
        namespace_environment = "staging"
    elif "stage" in lowered:
        namespace_environment = "staging"
    elif "prod" in lowered:
        namespace_environment = "production"
    else:
        namespace_environment = "development"

    if (
        namespace_environment in PROTECTED_ENVIRONMENTS
        and environment is not None
        and environment != namespace_environment
    ):
        raise ValueError(
            "explicit environment conflicts with authoritative protected namespace",
        )
    return environment or namespace_environment


def is_protected_environment(
    *,
    environment: str | None,
    namespace: str,
) -> bool:
    return (
        infer_environment(
            environment=environment,
            namespace=namespace,
        )
        in PROTECTED_ENVIRONMENTS
    )


def _parse_time(raw: object) -> datetime | None:
    if not isinstance(raw, str):
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _file_digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _directory_open_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )


def _entry_open_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )


def _open_absolute_no_follow(path: Path) -> int:
    """Open an absolute path without following any path-component symlink."""
    if not path.is_absolute() or ".." in path.parts or len(path.parts) < 2:
        raise _ComponentInspectionError("path is not an absolute protected path")
    try:
        parent_fd = os.open("/", _directory_open_flags())
    except OSError:
        raise _ComponentInspectionError("path could not be opened safely") from None
    try:
        for part in path.parts[1:-1]:
            try:
                child_fd = os.open(part, _directory_open_flags(), dir_fd=parent_fd)
            except OSError:
                raise _ComponentInspectionError(
                    "path contains a symlink or unavailable directory"
                ) from None
            os.close(parent_fd)
            parent_fd = child_fd
        try:
            return os.open(path.parts[-1], _entry_open_flags(), dir_fd=parent_fd)
        except OSError:
            raise _ComponentInspectionError("path is a symlink or unavailable") from None
    finally:
        os.close(parent_fd)


def _open_child_no_follow(directory_fd: int, name: str) -> int:
    try:
        return os.open(name, _entry_open_flags(), dir_fd=directory_fd)
    except OSError:
        raise _ComponentInspectionError("contains a symlink or unavailable entry") from None


def _validate_strict_metadata(
    metadata: os.stat_result,
    *,
    expected_owner_uid: int | None,
    require_private_files: bool,
) -> str:
    if expected_owner_uid is not None and metadata.st_uid != expected_owner_uid:
        raise _ComponentInspectionError("owner UID does not match")
    mode = stat.S_IMODE(metadata.st_mode)
    if stat.S_ISREG(metadata.st_mode):
        if require_private_files and mode != _PRIVATE_FILE_MODE:
            raise _ComponentInspectionError("file mode must be 0600")
        return "file"
    if stat.S_ISDIR(metadata.st_mode):
        if require_private_files and mode != _PRIVATE_DIRECTORY_MODE:
            raise _ComponentInspectionError("directory mode must be 0700")
        return "directory"
    raise _ComponentInspectionError("must be a regular file or directory")


def _strict_file_metadata(fd: int, *, require_nonempty: bool) -> tuple[int, str]:
    before = os.fstat(fd)
    digest = hashlib.sha256()
    total_size = 0
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        while True:
            chunk = os.read(fd, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            total_size += len(chunk)
    except OSError:
        raise _ComponentInspectionError("file could not be read safely") from None
    after = os.fstat(fd)
    stable_fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
    if any(getattr(before, field) != getattr(after, field) for field in stable_fields):
        raise _ComponentInspectionError("file changed during inspection")
    if total_size != after.st_size:
        raise _ComponentInspectionError("file size changed during inspection")
    if require_nonempty and total_size <= 0:
        raise _ComponentInspectionError("file is empty")
    return total_size, digest.hexdigest()


def _strict_directory_files(
    directory_fd: int,
    *,
    expected_owner_uid: int | None,
    require_private_files: bool,
    prefix: tuple[str, ...] = (),
) -> list[tuple[str, int, str]]:
    try:
        names = os.listdir(directory_fd)
    except OSError:
        raise _ComponentInspectionError("directory could not be listed safely") from None
    files: list[tuple[str, int, str]] = []
    for name in names:
        child_fd = _open_child_no_follow(directory_fd, name)
        try:
            metadata = os.fstat(child_fd)
            kind = _validate_strict_metadata(
                metadata,
                expected_owner_uid=expected_owner_uid,
                require_private_files=require_private_files,
            )
            relative_parts = (*prefix, name)
            if kind == "directory":
                files.extend(
                    _strict_directory_files(
                        child_fd,
                        expected_owner_uid=expected_owner_uid,
                        require_private_files=require_private_files,
                        prefix=relative_parts,
                    )
                )
            else:
                size, digest = _strict_file_metadata(child_fd, require_nonempty=False)
                files.append((Path(*relative_parts).as_posix(), size, digest))
        finally:
            os.close(child_fd)
    return files


def _strict_component_metadata(
    path: Path,
    *,
    expected_owner_uid: int | None,
    require_private_files: bool,
) -> dict[str, Any]:
    fd = _open_absolute_no_follow(path)
    try:
        metadata = os.fstat(fd)
        kind = _validate_strict_metadata(
            metadata,
            expected_owner_uid=expected_owner_uid,
            require_private_files=require_private_files,
        )
        if kind == "file":
            size, digest = _strict_file_metadata(fd, require_nonempty=True)
            return {
                "path": str(path),
                "kind": "file",
                "size_bytes": size,
                "sha256": digest,
            }
        files = _strict_directory_files(
            fd,
            expected_owner_uid=expected_owner_uid,
            require_private_files=require_private_files,
        )
        if not files:
            raise _ComponentInspectionError("directory is empty")
        directory_digest = hashlib.sha256()
        total_size = 0
        for relative, size, file_digest in sorted(files):
            directory_digest.update(relative.encode("utf-8"))
            directory_digest.update(b"\0")
            directory_digest.update(bytes.fromhex(file_digest))
            total_size += size
        return {
            "path": str(path),
            "kind": "directory",
            "file_count": len(files),
            "size_bytes": total_size,
            "sha256": directory_digest.hexdigest(),
        }
    finally:
        os.close(fd)


def _read_strict_manifest(
    path: Path,
    *,
    expected_owner_uid: int | None,
    require_private_files: bool,
) -> str:
    fd = _open_absolute_no_follow(path)
    try:
        metadata = os.fstat(fd)
        kind = _validate_strict_metadata(
            metadata,
            expected_owner_uid=expected_owner_uid,
            require_private_files=require_private_files,
        )
        if kind != "file":
            raise _ComponentInspectionError("must be a regular file")
        before = metadata
        chunks: list[bytes] = []
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            while True:
                chunk = os.read(fd, 1024 * 1024)
                if not chunk:
                    break
                chunks.append(chunk)
        except OSError:
            raise _ComponentInspectionError("could not be read safely") from None
        after = os.fstat(fd)
        stable_fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
        if any(getattr(before, field) != getattr(after, field) for field in stable_fields):
            raise _ComponentInspectionError("changed during inspection")
        try:
            return b"".join(chunks).decode("utf-8")
        except UnicodeDecodeError:
            raise _ComponentInspectionError("must be valid UTF-8") from None
    finally:
        os.close(fd)


def _validate_strict_manifest_root(
    path: Path,
    *,
    expected_owner_uid: int | None,
    require_private_files: bool,
) -> None:
    fd = _open_absolute_no_follow(path)
    try:
        kind = _validate_strict_metadata(
            os.fstat(fd),
            expected_owner_uid=expected_owner_uid,
            require_private_files=require_private_files,
        )
        if kind != "directory":
            raise _ComponentInspectionError("must be a directory")
    finally:
        os.close(fd)


def _directory_stats(path: Path) -> tuple[int, int, str]:
    h = hashlib.sha256()
    file_count = 0
    total_size = 0
    for child in sorted(p for p in path.rglob("*") if p.is_file()):
        rel = child.relative_to(path).as_posix().encode("utf-8")
        h.update(rel)
        h.update(b"\0")
        h.update(bytes.fromhex(_file_digest(child)))
        file_count += 1
        total_size += child.stat().st_size
    return file_count, total_size, h.hexdigest()


def _component_metadata(
    path: Path,
    *,
    expected_owner_uid: int | None = None,
    require_private_files: bool = False,
) -> dict[str, Any]:
    if expected_owner_uid is not None or require_private_files:
        return _strict_component_metadata(
            path,
            expected_owner_uid=expected_owner_uid,
            require_private_files=require_private_files,
        )
    resolved = path.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"backup component does not exist: {resolved}")
    if resolved.is_dir():
        file_count, total_size, digest = _directory_stats(resolved)
        if file_count == 0:
            raise ValueError(f"backup component directory is empty: {resolved}")
        return {
            "path": str(resolved),
            "kind": "directory",
            "file_count": file_count,
            "size_bytes": total_size,
            "sha256": digest,
        }
    if resolved.is_file():
        size = resolved.stat().st_size
        if size <= 0:
            raise ValueError(f"backup component file is empty: {resolved}")
        return {
            "path": str(resolved),
            "kind": "file",
            "size_bytes": size,
            "sha256": _file_digest(resolved),
        }
    raise ValueError(f"backup component must be file or directory: {resolved}")


def backup_manifest_sha256(
    manifest_path: Path,
    *,
    expected_owner_uid: int,
    require_private_file: bool = True,
) -> str:
    """Hash one private manifest through the same no-follow file boundary."""
    try:
        fd = _open_absolute_no_follow(Path(manifest_path))
        try:
            kind = _validate_strict_metadata(
                os.fstat(fd),
                expected_owner_uid=expected_owner_uid,
                require_private_files=require_private_file,
            )
            if kind != "file":
                raise _ComponentInspectionError("must be a regular file")
            _size, digest = _strict_file_metadata(fd, require_nonempty=True)
            return digest
        finally:
            os.close(fd)
    except _ComponentInspectionError:
        raise ValueError("backup manifest could not be hashed safely") from None


def backup_manifest_created_at(
    manifest_path: Path,
    *,
    expected_owner_uid: int,
    require_private_file: bool = True,
) -> datetime:
    """Read only the creation time through the strict no-follow boundary."""
    try:
        manifest_text = _read_strict_manifest(
            Path(manifest_path),
            expected_owner_uid=expected_owner_uid,
            require_private_files=require_private_file,
        )
        manifest = json.loads(manifest_text)
        if not isinstance(manifest, dict):
            raise _ComponentInspectionError("must be a JSON object")
        created_at = _parse_time(manifest.get("created_at"))
        if created_at is None:
            raise _ComponentInspectionError("created_at is missing or invalid")
        return created_at
    except (json.JSONDecodeError, _ComponentInspectionError):
        raise ValueError("backup manifest creation time could not be read safely") from None


def write_backup_manifest(
    *,
    environment: str,
    namespace: str,
    output_path: Path,
    components: dict[str, Path],
    now: datetime | None = None,
) -> dict[str, Any]:
    missing = sorted(set(REQUIRED_BACKUP_COMPONENTS) - set(components))
    if missing:
        raise ValueError(
            "missing required backup component(s): " + ", ".join(missing),
        )
    created_at = (now or datetime.now(UTC)).astimezone(UTC)
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "environment": environment,
        "namespace": namespace,
        "created_at": created_at.isoformat(),
        "components": {
            name: _component_metadata(path) for name, path in sorted(components.items())
        },
        "verification": {
            "status": "verified",
            "checked_at": created_at.isoformat(),
            "required_components": list(REQUIRED_BACKUP_COMPONENTS),
        },
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.chmod(output_path, 0o600)
    return manifest


def validate_backup_manifest(
    manifest_path: Path | None,
    *,
    environment: str,
    namespace: str,
    max_age_hours: int = DEFAULT_BACKUP_MAX_AGE_HOURS,
    min_remaining_hours: int = 0,
    now: datetime | None = None,
    expected_owner_uid: int | None = None,
    require_private_files: bool = False,
    enforce_freshness: bool = True,
) -> list[str]:
    if manifest_path is None:
        return ["backup manifest is required"]
    path = Path(manifest_path)
    strict = expected_owner_uid is not None or require_private_files
    if strict:
        try:
            _validate_strict_manifest_root(
                path.parent,
                expected_owner_uid=expected_owner_uid,
                require_private_files=require_private_files,
            )
        except _ComponentInspectionError as exc:
            return [f"backup manifest root {exc}"]
        try:
            manifest_text = _read_strict_manifest(
                path,
                expected_owner_uid=expected_owner_uid,
                require_private_files=require_private_files,
            )
        except _ComponentInspectionError as exc:
            return [f"backup manifest {exc}"]
    else:
        if not path.exists():
            return [f"backup manifest not found: {path}"]
        try:
            manifest_text = path.read_text(encoding="utf-8")
        except OSError as exc:
            return [f"backup manifest is not readable JSON: {type(exc).__name__}: {exc}"]
    try:
        manifest = json.loads(manifest_text)
    except json.JSONDecodeError as exc:
        return [f"backup manifest is not readable JSON: {type(exc).__name__}: {exc}"]
    problems: list[str] = []
    if type(manifest.get("schema_version")) is not int or manifest.get("schema_version") != 1:
        problems.append("backup manifest schema_version must be 1")
    if manifest.get("environment") != environment:
        problems.append(
            f"backup manifest environment {manifest.get('environment')!r} "
            f"does not match {environment!r}",
        )
    if manifest.get("namespace") != namespace:
        problems.append(
            f"backup manifest namespace {manifest.get('namespace')!r} does not match {namespace!r}",
        )
    created_at = _parse_time(manifest.get("created_at"))
    if created_at is None:
        problems.append("backup manifest created_at is missing or invalid")
    else:
        age = (now or datetime.now(UTC)).astimezone(UTC) - created_at
        if age.total_seconds() < 0:
            problems.append("backup manifest created_at is in the future")
        elif enforce_freshness and age > timedelta(hours=max_age_hours):
            problems.append(
                f"backup manifest is stale: age exceeds {max_age_hours}h",
            )
        elif enforce_freshness and min_remaining_hours > 0:
            remaining = timedelta(hours=max_age_hours) - age
            if remaining < timedelta(hours=min_remaining_hours):
                problems.append(
                    "backup manifest expires too soon: "
                    f"requires at least {min_remaining_hours}h remaining",
                )
    verification = manifest.get("verification")
    if not isinstance(verification, dict) or verification.get("status") != "verified":
        problems.append("backup manifest verification.status must be 'verified'")
    components = manifest.get("components")
    if not isinstance(components, dict):
        problems.append("backup manifest components must be an object")
        return problems
    for name in REQUIRED_BACKUP_COMPONENTS:
        component = components.get(name)
        if not isinstance(component, dict):
            problems.append(f"backup manifest missing component {name!r}")
            continue
        size = component.get("size_bytes")
        if type(size) is not int or size <= 0:
            problems.append(f"backup component {name!r} has no recorded bytes")
        component_path = component.get("path")
        if not isinstance(component_path, str) or not component_path:
            problems.append(f"backup component {name!r} has no path")
            continue
        component_fs_path = Path(component_path)
        if strict and (
            not component_fs_path.is_absolute()
            or ".." in component_fs_path.parts
            or component_fs_path == path.parent
            or not component_fs_path.is_relative_to(path.parent)
        ):
            problems.append(f"backup component {name!r} is outside manifest root")
            continue
        try:
            actual = _component_metadata(
                component_fs_path,
                expected_owner_uid=expected_owner_uid,
                require_private_files=require_private_files,
            )
        except _ComponentInspectionError as exc:
            problems.append(f"backup component {name!r} {exc}")
            continue
        except (OSError, ValueError):
            problems.append(f"backup component {name!r} could not be inspected")
            continue
        for field in ("kind", "size_bytes", "sha256"):
            recorded_value = component.get(field)
            actual_value = actual.get(field)
            if type(recorded_value) is not type(actual_value) or recorded_value != actual_value:
                problems.append(f"backup component {name!r} {field} does not match")
        if actual["kind"] == "directory":
            recorded_count = component.get("file_count")
            actual_count = actual.get("file_count")
            if type(recorded_count) is not int or recorded_count != actual_count:
                problems.append(f"backup component {name!r} file_count does not match")
    return problems
