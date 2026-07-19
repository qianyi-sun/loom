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
from collections.abc import Callable, Generator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from math import isfinite
from pathlib import Path
from time import monotonic as _monotonic
from typing import Any

REQUIRED_BACKUP_COMPONENTS: tuple[str, ...] = (
    "postgres",
    "minio",
    "k8s_secrets",
)
ROLLOUT_CHECKPOINT_COMPONENTS: tuple[str, ...] = (
    "postgres",
    "object_inventory",
    "k8s_secrets",
)
_BACKUP_COMPONENTS_BY_SCHEMA: dict[int, tuple[str, ...]] = {
    1: REQUIRED_BACKUP_COMPONENTS,
    2: ROLLOUT_CHECKPOINT_COMPONENTS,
}
PROTECTED_ENVIRONMENTS: frozenset[str] = frozenset(
    {
        "staging",
        "production",
    }
)
DEFAULT_BACKUP_MAX_AGE_HOURS = 24
DEFAULT_BACKUP_MAX_FILES = 100_000
DEFAULT_BACKUP_MAX_ENTRIES = 1_000_000
DEFAULT_BACKUP_MAX_TOTAL_BYTES = 16 * 1024**4
DEFAULT_BACKUP_MAX_DEPTH = 64
DEFAULT_BACKUP_MAX_DIRECTORY_ENTRIES = 100_000
DEFAULT_BACKUP_MAX_ELAPSED_SECONDS = 22 * 60 * 60
DEFAULT_BACKUP_MAX_MANIFEST_BYTES = 1024 * 1024
_PRIVATE_FILE_MODE = 0o600
_PRIVATE_DIRECTORY_MODE = 0o700


@dataclass(frozen=True, slots=True)
class BackupTraversalLimits:
    """Explicit resource ceilings for one manifest write or validation."""

    max_files: int = DEFAULT_BACKUP_MAX_FILES
    max_entries: int = DEFAULT_BACKUP_MAX_ENTRIES
    max_total_bytes: int = DEFAULT_BACKUP_MAX_TOTAL_BYTES
    max_depth: int = DEFAULT_BACKUP_MAX_DEPTH
    max_directory_entries: int = DEFAULT_BACKUP_MAX_DIRECTORY_ENTRIES
    max_elapsed_seconds: float = DEFAULT_BACKUP_MAX_ELAPSED_SECONDS
    max_manifest_bytes: int = DEFAULT_BACKUP_MAX_MANIFEST_BYTES
    monotonic: Callable[[], float] = field(
        default=_monotonic,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        for name in (
            "max_files",
            "max_entries",
            "max_total_bytes",
            "max_directory_entries",
            "max_manifest_bytes",
        ):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if type(self.max_depth) is not int or self.max_depth < 0:
            raise ValueError("max_depth must be a non-negative integer")
        if (
            isinstance(self.max_elapsed_seconds, bool)
            or not isinstance(self.max_elapsed_seconds, (int, float))
            or not isfinite(self.max_elapsed_seconds)
            or self.max_elapsed_seconds <= 0
        ):
            raise ValueError("max_elapsed_seconds must be positive")
        if not callable(self.monotonic):
            raise ValueError("monotonic must be callable")


class _ComponentInspectionError(ValueError):
    """Safe internal error for strict no-follow component inspection."""


class _TraversalLimitError(_ComponentInspectionError):
    """One configured traversal resource ceiling was crossed."""


class _TraversalBudget:
    """Mutable per-operation counters shared by every backup component."""

    def __init__(
        self,
        limits: BackupTraversalLimits,
        *,
        started_at: float | None = None,
    ) -> None:
        self.limits = limits
        self.files = 0
        self.entries = 0
        self.total_bytes = 0
        self._started_at = limits.monotonic() if started_at is None else started_at
        if not isfinite(self._started_at):
            raise _TraversalLimitError("traversal clock is invalid")

    def fresh_counters(self) -> _TraversalBudget:
        """Reset inventory counters without extending the absolute deadline."""
        return _TraversalBudget(self.limits, started_at=self._started_at)

    @property
    def remaining_entries(self) -> int:
        return self.limits.max_entries - self.entries

    @property
    def remaining_bytes(self) -> int:
        return self.limits.max_total_bytes - self.total_bytes

    def check_deadline(self) -> None:
        elapsed = self.limits.monotonic() - self._started_at
        if not isfinite(elapsed) or elapsed < 0 or elapsed > self.limits.max_elapsed_seconds:
            raise _TraversalLimitError("traversal deadline exceeded")

    def consume_entry(self, *, depth: int) -> None:
        self.check_deadline()
        if depth > self.limits.max_depth:
            raise _TraversalLimitError("traversal depth limit exceeded")
        if self.entries >= self.limits.max_entries:
            raise _TraversalLimitError("traversal entry count limit exceeded")
        self.entries += 1

    def consume_file(self) -> None:
        self.check_deadline()
        if self.files >= self.limits.max_files:
            raise _TraversalLimitError("traversal file count limit exceeded")
        self.files += 1

    def ensure_bytes_fit(self, size: int) -> None:
        self.check_deadline()
        if size < 0 or size > self.remaining_bytes:
            raise _TraversalLimitError("traversal byte limit exceeded")

    def consume_bytes(self, size: int) -> None:
        self.ensure_bytes_fit(size)
        self.total_bytes += size


_STABLE_METADATA_FIELDS: tuple[str, ...] = (
    "st_dev",
    "st_ino",
    "st_mode",
    "st_nlink",
    "st_uid",
    "st_gid",
    "st_size",
    "st_mtime_ns",
    "st_ctime_ns",
)


def _metadata_changed(before: os.stat_result, after: os.stat_result) -> bool:
    return any(getattr(before, field) != getattr(after, field) for field in _STABLE_METADATA_FIELDS)


@dataclass(frozen=True, slots=True)
class _ComponentState:
    kind: str
    file_count: int
    size_bytes: int
    metadata_sha256: str


@dataclass(frozen=True, slots=True)
class _InspectedComponent:
    manifest_metadata: dict[str, Any]
    state: _ComponentState


@dataclass(frozen=True, slots=True)
class _ManifestSnapshot:
    text: str
    size_bytes: int
    sha256: str
    metadata: os.stat_result


def _update_metadata_digest(
    digest: Any,
    relative: str,
    metadata: os.stat_result,
) -> None:
    try:
        encoded = json.dumps(
            [relative, *(getattr(metadata, field) for field in _STABLE_METADATA_FIELDS)],
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("ascii")
    except (TypeError, UnicodeEncodeError):
        raise _ComponentInspectionError("metadata could not be encoded safely") from None
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)


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


def _checked_open(
    path: str,
    flags: int,
    *,
    budget: _TraversalBudget,
    dir_fd: int | None = None,
    mode: int = 0o777,
) -> int:
    budget.check_deadline()
    fd = os.open(path, flags, mode, dir_fd=dir_fd)
    try:
        budget.check_deadline()
    except BaseException:
        os.close(fd)
        raise
    return fd


def _bounded_fstat(fd: int, *, budget: _TraversalBudget) -> os.stat_result:
    budget.check_deadline()
    try:
        metadata = os.fstat(fd)
    except OSError:
        raise _ComponentInspectionError("metadata could not be read safely") from None
    budget.check_deadline()
    return metadata


def _open_parent_no_follow(
    path: Path,
    *,
    budget: _TraversalBudget,
) -> tuple[int, str]:
    if not path.is_absolute() or ".." in path.parts or len(path.parts) < 2:
        raise _ComponentInspectionError("path is not an absolute protected path")
    try:
        parent_fd = _checked_open("/", _directory_open_flags(), budget=budget)
    except OSError:
        raise _ComponentInspectionError("path could not be opened safely") from None
    try:
        for part in path.parts[1:-1]:
            try:
                child_fd = _checked_open(
                    part,
                    _directory_open_flags(),
                    budget=budget,
                    dir_fd=parent_fd,
                )
            except OSError:
                raise _ComponentInspectionError(
                    "path contains a symlink or unavailable directory"
                ) from None
            os.close(parent_fd)
            parent_fd = child_fd
        return parent_fd, path.parts[-1]
    except BaseException:
        os.close(parent_fd)
        raise


def _open_absolute_no_follow(path: Path, *, budget: _TraversalBudget) -> int:
    """Open an absolute path without following any path-component symlink."""
    parent_fd, name = _open_parent_no_follow(path, budget=budget)
    try:
        try:
            return _checked_open(
                name,
                _entry_open_flags(),
                budget=budget,
                dir_fd=parent_fd,
            )
        except OSError:
            raise _ComponentInspectionError("path is a symlink or unavailable") from None
    finally:
        os.close(parent_fd)


def _open_child_no_follow(
    directory_fd: int,
    name: str,
    *,
    budget: _TraversalBudget,
) -> int:
    try:
        return _checked_open(
            name,
            _entry_open_flags(),
            budget=budget,
            dir_fd=directory_fd,
        )
    except OSError:
        raise _ComponentInspectionError("contains a symlink or unavailable entry") from None


def _ensure_path_identity(
    path: Path,
    expected: os.stat_result,
    *,
    budget: _TraversalBudget,
) -> None:
    try:
        fd = _open_absolute_no_follow(path, budget=budget)
    except _TraversalLimitError:
        raise
    except _ComponentInspectionError:
        raise _ComponentInspectionError("path changed during inspection") from None
    try:
        actual = _bounded_fstat(fd, budget=budget)
    finally:
        os.close(fd)
    if _metadata_changed(expected, actual):
        raise _ComponentInspectionError("path changed during inspection")


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


def _strict_file_metadata(
    fd: int,
    *,
    require_nonempty: bool,
    budget: _TraversalBudget,
    account_component_bytes: bool,
    max_bytes: int | None = None,
) -> tuple[int, str, os.stat_result]:
    before = _bounded_fstat(fd, budget=budget)
    if max_bytes is not None and before.st_size > max_bytes:
        raise _TraversalLimitError("manifest size limit exceeded")
    if account_component_bytes:
        budget.ensure_bytes_fit(before.st_size)
    digest = hashlib.sha256()
    total_size = 0
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        while True:
            budget.check_deadline()
            read_size = 1024 * 1024
            if max_bytes is not None:
                read_size = min(read_size, max_bytes - total_size + 1)
            elif account_component_bytes:
                read_size = min(read_size, budget.remaining_bytes + 1)
            chunk = os.read(fd, max(read_size, 1))
            budget.check_deadline()
            if not chunk:
                break
            if max_bytes is not None and total_size + len(chunk) > max_bytes:
                raise _TraversalLimitError("manifest size limit exceeded")
            if account_component_bytes:
                budget.consume_bytes(len(chunk))
            digest.update(chunk)
            total_size += len(chunk)
    except OSError:
        raise _ComponentInspectionError("file could not be read safely") from None
    after = _bounded_fstat(fd, budget=budget)
    if _metadata_changed(before, after):
        raise _ComponentInspectionError("file changed during inspection")
    if total_size != after.st_size:
        raise _ComponentInspectionError("file size changed during inspection")
    if require_nonempty and total_size <= 0:
        raise _ComponentInspectionError("file is empty")
    return total_size, digest.hexdigest(), after


def _directory_names(
    directory_fd: int,
    *,
    budget: _TraversalBudget,
    child_depth: int,
) -> list[str]:
    names: list[str] = []
    entries: Any | None = None
    try:
        budget.check_deadline()
        entries = os.scandir(directory_fd)
        budget.check_deadline()
        for entry in entries:
            budget.check_deadline()
            if len(names) >= budget.limits.max_directory_entries:
                raise _TraversalLimitError("directory entry limit exceeded")
            budget.consume_entry(depth=child_depth)
            names.append(entry.name)
    except OSError:
        raise _ComponentInspectionError("directory could not be listed safely") from None
    finally:
        if entries is not None:
            entries.close()
    return sorted(names)


def _strict_directory_files(
    directory_fd: int,
    *,
    metadata: os.stat_result,
    expected_owner_uid: int | None,
    require_private_files: bool,
    budget: _TraversalBudget,
    state_digest: Any,
    depth: int,
    prefix: tuple[str, ...] = (),
) -> Generator[tuple[str, int, str], None, None]:
    child_depth = depth + 1
    names = _directory_names(
        directory_fd,
        budget=budget,
        child_depth=child_depth,
    )
    for name in names:
        budget.check_deadline()
        child_fd = _open_child_no_follow(directory_fd, name, budget=budget)
        record: tuple[str, int, str] | None = None
        try:
            child_metadata = _bounded_fstat(child_fd, budget=budget)
            kind = _validate_strict_metadata(
                child_metadata,
                expected_owner_uid=expected_owner_uid,
                require_private_files=require_private_files,
            )
            relative_parts = (*prefix, name)
            if kind == "directory":
                yield from _strict_directory_files(
                    child_fd,
                    metadata=child_metadata,
                    expected_owner_uid=expected_owner_uid,
                    require_private_files=require_private_files,
                    budget=budget,
                    state_digest=state_digest,
                    depth=child_depth,
                    prefix=relative_parts,
                )
            else:
                budget.consume_file()
                size, digest, stable_metadata = _strict_file_metadata(
                    child_fd,
                    require_nonempty=False,
                    budget=budget,
                    account_component_bytes=True,
                )
                _update_metadata_digest(
                    state_digest,
                    Path(*relative_parts).as_posix(),
                    stable_metadata,
                )
                record = Path(*relative_parts).as_posix(), size, digest
        finally:
            os.close(child_fd)
        if record is not None:
            yield record
    budget.check_deadline()
    after = _bounded_fstat(directory_fd, budget=budget)
    if _metadata_changed(metadata, after):
        raise _ComponentInspectionError("directory changed during inspection")
    relative = Path(*prefix).as_posix() if prefix else "."
    _update_metadata_digest(state_digest, relative, after)


def _strict_component_metadata(
    path: Path,
    *,
    expected_owner_uid: int | None,
    require_private_files: bool,
    budget: _TraversalBudget,
) -> _InspectedComponent:
    fd = _open_absolute_no_follow(path, budget=budget)
    try:
        metadata = _bounded_fstat(fd, budget=budget)
        kind = _validate_strict_metadata(
            metadata,
            expected_owner_uid=expected_owner_uid,
            require_private_files=require_private_files,
        )
        budget.consume_entry(depth=0)
        state_digest = hashlib.sha256()
        if kind == "file":
            budget.consume_file()
            size, digest, stable_metadata = _strict_file_metadata(
                fd,
                require_nonempty=True,
                budget=budget,
                account_component_bytes=True,
            )
            _ensure_path_identity(path, stable_metadata, budget=budget)
            _update_metadata_digest(state_digest, ".", stable_metadata)
            return _InspectedComponent(
                manifest_metadata={
                    "path": str(path),
                    "kind": "file",
                    "size_bytes": size,
                    "sha256": digest,
                },
                state=_ComponentState(
                    kind="file",
                    file_count=1,
                    size_bytes=size,
                    metadata_sha256=state_digest.hexdigest(),
                ),
            )
        directory_digest = hashlib.sha256()
        total_size = 0
        file_count = 0
        records = _strict_directory_files(
            fd,
            metadata=metadata,
            expected_owner_uid=expected_owner_uid,
            require_private_files=require_private_files,
            budget=budget,
            state_digest=state_digest,
            depth=0,
        )
        try:
            for relative, size, file_digest in records:
                try:
                    encoded_relative = relative.encode("utf-8")
                except UnicodeEncodeError:
                    raise _ComponentInspectionError("file path is not valid UTF-8") from None
                directory_digest.update(encoded_relative)
                directory_digest.update(b"\0")
                directory_digest.update(bytes.fromhex(file_digest))
                total_size += size
                file_count += 1
        finally:
            records.close()
        if file_count == 0:
            raise _ComponentInspectionError("directory is empty")
        _ensure_path_identity(path, metadata, budget=budget)
        return _InspectedComponent(
            manifest_metadata={
                "path": str(path),
                "kind": "directory",
                "file_count": file_count,
                "size_bytes": total_size,
                "sha256": directory_digest.hexdigest(),
            },
            state=_ComponentState(
                kind="directory",
                file_count=file_count,
                size_bytes=total_size,
                metadata_sha256=state_digest.hexdigest(),
            ),
        )
    finally:
        os.close(fd)


def _metadata_only_directory_state(
    directory_fd: int,
    *,
    metadata: os.stat_result,
    expected_owner_uid: int | None,
    require_private_files: bool,
    budget: _TraversalBudget,
    state_digest: Any,
    depth: int,
    prefix: tuple[str, ...] = (),
) -> tuple[int, int]:
    child_depth = depth + 1
    names = _directory_names(
        directory_fd,
        budget=budget,
        child_depth=child_depth,
    )
    file_count = 0
    total_size = 0
    for name in names:
        child_fd = _open_child_no_follow(directory_fd, name, budget=budget)
        try:
            before = _bounded_fstat(child_fd, budget=budget)
            kind = _validate_strict_metadata(
                before,
                expected_owner_uid=expected_owner_uid,
                require_private_files=require_private_files,
            )
            relative_parts = (*prefix, name)
            relative = Path(*relative_parts).as_posix()
            if kind == "directory":
                nested_count, nested_size = _metadata_only_directory_state(
                    child_fd,
                    metadata=before,
                    expected_owner_uid=expected_owner_uid,
                    require_private_files=require_private_files,
                    budget=budget,
                    state_digest=state_digest,
                    depth=child_depth,
                    prefix=relative_parts,
                )
                file_count += nested_count
                total_size += nested_size
            else:
                budget.consume_file()
                budget.consume_bytes(before.st_size)
                after = _bounded_fstat(child_fd, budget=budget)
                if _metadata_changed(before, after):
                    raise _ComponentInspectionError("file changed during inspection")
                _update_metadata_digest(state_digest, relative, after)
                file_count += 1
                total_size += after.st_size
        finally:
            os.close(child_fd)
    after = _bounded_fstat(directory_fd, budget=budget)
    if _metadata_changed(metadata, after):
        raise _ComponentInspectionError("directory changed during inspection")
    relative = Path(*prefix).as_posix() if prefix else "."
    _update_metadata_digest(state_digest, relative, after)
    return file_count, total_size


def _component_state_metadata_only(
    path: Path,
    *,
    expected_owner_uid: int | None,
    require_private_files: bool,
    budget: _TraversalBudget,
) -> _ComponentState:
    fd = _open_absolute_no_follow(path, budget=budget)
    try:
        metadata = _bounded_fstat(fd, budget=budget)
        kind = _validate_strict_metadata(
            metadata,
            expected_owner_uid=expected_owner_uid,
            require_private_files=require_private_files,
        )
        budget.consume_entry(depth=0)
        state_digest = hashlib.sha256()
        if kind == "file":
            budget.consume_file()
            budget.consume_bytes(metadata.st_size)
            after = _bounded_fstat(fd, budget=budget)
            if _metadata_changed(metadata, after):
                raise _ComponentInspectionError("file changed during inspection")
            if after.st_size <= 0:
                raise _ComponentInspectionError("file is empty")
            _update_metadata_digest(state_digest, ".", after)
            _ensure_path_identity(path, after, budget=budget)
            return _ComponentState(
                kind="file",
                file_count=1,
                size_bytes=after.st_size,
                metadata_sha256=state_digest.hexdigest(),
            )
        file_count, total_size = _metadata_only_directory_state(
            fd,
            metadata=metadata,
            expected_owner_uid=expected_owner_uid,
            require_private_files=require_private_files,
            budget=budget,
            state_digest=state_digest,
            depth=0,
        )
        if file_count == 0:
            raise _ComponentInspectionError("directory is empty")
        _ensure_path_identity(path, metadata, budget=budget)
        return _ComponentState(
            kind="directory",
            file_count=file_count,
            size_bytes=total_size,
            metadata_sha256=state_digest.hexdigest(),
        )
    finally:
        os.close(fd)


def _read_strict_manifest(
    path: Path,
    *,
    expected_owner_uid: int | None,
    require_private_files: bool,
    budget: _TraversalBudget,
) -> _ManifestSnapshot:
    fd = _open_absolute_no_follow(path, budget=budget)
    try:
        metadata = _bounded_fstat(fd, budget=budget)
        kind = _validate_strict_metadata(
            metadata,
            expected_owner_uid=expected_owner_uid,
            require_private_files=require_private_files,
        )
        if kind != "file":
            raise _ComponentInspectionError("must be a regular file")
        before = metadata
        if before.st_size > budget.limits.max_manifest_bytes:
            raise _TraversalLimitError("manifest size limit exceeded")
        payload = bytearray()
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            while True:
                budget.check_deadline()
                remaining = budget.limits.max_manifest_bytes - len(payload)
                chunk = os.read(fd, min(1024 * 1024, remaining + 1))
                budget.check_deadline()
                if not chunk:
                    break
                payload.extend(chunk)
                if len(payload) > budget.limits.max_manifest_bytes:
                    raise _TraversalLimitError("manifest size limit exceeded")
        except OSError:
            raise _ComponentInspectionError("could not be read safely") from None
        after = _bounded_fstat(fd, budget=budget)
        if _metadata_changed(before, after):
            raise _ComponentInspectionError("changed during inspection")
        if len(payload) != after.st_size:
            raise _ComponentInspectionError("size changed during inspection")
        _ensure_path_identity(path, after, budget=budget)
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError:
            raise _ComponentInspectionError("must be valid UTF-8") from None
        return _ManifestSnapshot(
            text=text,
            size_bytes=len(payload),
            sha256=hashlib.sha256(payload).hexdigest(),
            metadata=after,
        )
    finally:
        os.close(fd)


def _revalidate_manifest_snapshot(
    path: Path,
    snapshot: _ManifestSnapshot,
    *,
    expected_owner_uid: int | None,
    require_private_files: bool,
    budget: _TraversalBudget,
) -> None:
    """Fail closed when the bounded manifest changed after its initial read."""
    fd = _open_absolute_no_follow(path, budget=budget)
    try:
        metadata = _bounded_fstat(fd, budget=budget)
        kind = _validate_strict_metadata(
            metadata,
            expected_owner_uid=expected_owner_uid,
            require_private_files=require_private_files,
        )
        if kind != "file":
            raise _ComponentInspectionError("changed during inspection")
        size, digest, stable_metadata = _strict_file_metadata(
            fd,
            require_nonempty=False,
            budget=budget,
            account_component_bytes=False,
            max_bytes=budget.limits.max_manifest_bytes,
        )
        if (
            size != snapshot.size_bytes
            or digest != snapshot.sha256
            or _metadata_changed(snapshot.metadata, stable_metadata)
        ):
            raise _ComponentInspectionError("changed during inspection")
        _ensure_path_identity(path, stable_metadata, budget=budget)
    finally:
        os.close(fd)


def _validate_strict_manifest_root(
    path: Path,
    *,
    expected_owner_uid: int | None,
    require_private_files: bool,
    budget: _TraversalBudget,
) -> os.stat_result:
    budget.check_deadline()
    fd = _open_absolute_no_follow(path, budget=budget)
    try:
        metadata = _bounded_fstat(fd, budget=budget)
        kind = _validate_strict_metadata(
            metadata,
            expected_owner_uid=expected_owner_uid,
            require_private_files=require_private_files,
        )
        if kind != "directory":
            raise _ComponentInspectionError("must be a directory")
        budget.check_deadline()
        return metadata
    finally:
        os.close(fd)


def _component_metadata(
    path: Path,
    *,
    budget: _TraversalBudget,
    expected_owner_uid: int | None = None,
    require_private_files: bool = False,
) -> _InspectedComponent:
    if expected_owner_uid is not None or require_private_files:
        return _strict_component_metadata(
            path,
            expected_owner_uid=expected_owner_uid,
            require_private_files=require_private_files,
            budget=budget,
        )
    absolute = Path(os.path.abspath(os.fspath(path)))
    return _strict_component_metadata(
        absolute,
        expected_owner_uid=None,
        require_private_files=False,
        budget=budget,
    )


def _inspect_component_set(
    components: dict[str, Path],
    *,
    budget: _TraversalBudget,
    expected_owner_uid: int | None = None,
    require_private_files: bool = False,
) -> dict[str, _InspectedComponent]:
    return {
        name: _component_metadata(
            path,
            budget=budget,
            expected_owner_uid=expected_owner_uid,
            require_private_files=require_private_files,
        )
        for name, path in sorted(components.items())
    }


def _component_states_metadata_only(
    inspections: dict[str, _InspectedComponent],
    *,
    budget: _TraversalBudget,
    expected_owner_uid: int | None = None,
    require_private_files: bool = False,
) -> dict[str, _ComponentState]:
    """Rewalk all entries without doubling backup-content I/O.

    The caller must supply completed backup artifacts whose service writer is
    quiesced. The second pass detects mutation after an entry's content pass and
    before its metadata pass because a non-root writer changes at least one
    hashed stable stat field, including ctime_ns. This bounded guard deliberately
    retains no global per-file inventory or descriptors and is not a filesystem
    snapshot: mutation after an entry's final metadata observation is outside
    this function's consistency boundary.
    """
    states: dict[str, _ComponentState] = {}
    for name, inspection in sorted(inspections.items()):
        states[name] = _component_state_metadata_only(
            Path(inspection.manifest_metadata["path"]),
            expected_owner_uid=expected_owner_uid,
            require_private_files=require_private_files,
            budget=budget,
        )
    return states


def backup_manifest_sha256(
    manifest_path: Path,
    *,
    expected_owner_uid: int,
    require_private_file: bool = True,
    limits: BackupTraversalLimits | None = None,
) -> str:
    """Hash one private manifest through the same no-follow file boundary."""
    budget = _TraversalBudget(limits or BackupTraversalLimits())
    try:
        fd = _open_absolute_no_follow(Path(manifest_path), budget=budget)
        try:
            kind = _validate_strict_metadata(
                _bounded_fstat(fd, budget=budget),
                expected_owner_uid=expected_owner_uid,
                require_private_files=require_private_file,
            )
            if kind != "file":
                raise _ComponentInspectionError("must be a regular file")
            _size, digest, stable_metadata = _strict_file_metadata(
                fd,
                require_nonempty=True,
                budget=budget,
                account_component_bytes=False,
                max_bytes=budget.limits.max_manifest_bytes,
            )
            _ensure_path_identity(
                Path(manifest_path),
                stable_metadata,
                budget=budget,
            )
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
    limits: BackupTraversalLimits | None = None,
) -> datetime:
    """Read only the creation time through the strict no-follow boundary."""
    budget = _TraversalBudget(limits or BackupTraversalLimits())
    try:
        manifest_snapshot = _read_strict_manifest(
            Path(manifest_path),
            expected_owner_uid=expected_owner_uid,
            require_private_files=require_private_file,
            budget=budget,
        )
        manifest = json.loads(manifest_snapshot.text)
        if not isinstance(manifest, dict):
            raise _ComponentInspectionError("must be a JSON object")
        created_at = _parse_time(manifest.get("created_at"))
        if created_at is None:
            raise _ComponentInspectionError("created_at is missing or invalid")
        return created_at
    except (json.JSONDecodeError, _ComponentInspectionError):
        raise ValueError("backup manifest creation time could not be read safely") from None


def _prepare_manifest_output_parent(
    output_path: Path,
    *,
    budget: _TraversalBudget,
) -> Path:
    absolute = Path(os.path.abspath(os.fspath(output_path)))
    parent = absolute.parent
    if not parent.is_absolute() or ".." in parent.parts:
        raise _ComponentInspectionError("path is not an absolute protected path")
    try:
        directory_fd = _checked_open("/", _directory_open_flags(), budget=budget)
    except OSError:
        raise _ComponentInspectionError("path could not be opened safely") from None
    try:
        for part in parent.parts[1:]:
            try:
                child_fd = _checked_open(
                    part,
                    _directory_open_flags(),
                    budget=budget,
                    dir_fd=directory_fd,
                )
            except FileNotFoundError:
                try:
                    budget.check_deadline()
                    os.mkdir(part, mode=0o777, dir_fd=directory_fd)
                    budget.check_deadline()
                    child_fd = _checked_open(
                        part,
                        _directory_open_flags(),
                        budget=budget,
                        dir_fd=directory_fd,
                    )
                except OSError:
                    raise _ComponentInspectionError(
                        "path contains a symlink or unavailable directory"
                    ) from None
            except OSError:
                raise _ComponentInspectionError(
                    "path contains a symlink or unavailable directory"
                ) from None
            os.close(directory_fd)
            directory_fd = child_fd
    finally:
        os.close(directory_fd)
    parent_fd, _name = _open_parent_no_follow(absolute, budget=budget)
    os.close(parent_fd)
    return absolute


def _write_all(fd: int, payload: bytes, *, budget: _TraversalBudget) -> None:
    written = 0
    while written < len(payload):
        budget.check_deadline()
        try:
            count = os.write(fd, payload[written:])
        except OSError:
            raise _ComponentInspectionError(
                "backup manifest could not be persisted safely"
            ) from None
        budget.check_deadline()
        if count <= 0:
            raise _ComponentInspectionError("backup manifest could not be persisted safely")
        written += count


def _default_write_manifest_output(
    output_path: Path,
    payload: bytes,
    *,
    budget: _TraversalBudget,
) -> None:
    parent_fd, name = _open_parent_no_follow(output_path, budget=budget)
    try:
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_TRUNC
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        try:
            fd = _checked_open(
                name,
                flags,
                budget=budget,
                dir_fd=parent_fd,
                mode=_PRIVATE_FILE_MODE,
            )
        except OSError:
            raise _ComponentInspectionError(
                "backup manifest could not be persisted safely"
            ) from None
    finally:
        os.close(parent_fd)
    try:
        metadata = _bounded_fstat(fd, budget=budget)
        if (
            _validate_strict_metadata(
                metadata,
                expected_owner_uid=None,
                require_private_files=False,
            )
            != "file"
        ):
            raise _ComponentInspectionError("backup manifest could not be persisted safely")
        _write_all(fd, payload, budget=budget)
    finally:
        os.close(fd)


def _verify_manifest_output(
    output_path: Path,
    payload: bytes,
    *,
    budget: _TraversalBudget,
) -> None:
    try:
        fd = _open_absolute_no_follow(output_path, budget=budget)
    except _TraversalLimitError:
        raise
    except _ComponentInspectionError:
        raise _ComponentInspectionError("backup manifest was not persisted safely") from None
    try:
        metadata = _bounded_fstat(fd, budget=budget)
        if (
            _validate_strict_metadata(
                metadata,
                expected_owner_uid=None,
                require_private_files=False,
            )
            != "file"
        ):
            raise _ComponentInspectionError("backup manifest was not persisted safely")
        try:
            budget.check_deadline()
            os.fchmod(fd, _PRIVATE_FILE_MODE)
            budget.check_deadline()
        except OSError:
            raise _ComponentInspectionError("backup manifest was not persisted safely") from None
        size, digest, stable_metadata = _strict_file_metadata(
            fd,
            require_nonempty=True,
            budget=budget,
            account_component_bytes=False,
            max_bytes=budget.limits.max_manifest_bytes,
        )
        expected_digest = hashlib.sha256(payload).hexdigest()
        if size != len(payload) or digest != expected_digest:
            raise _ComponentInspectionError("backup manifest persisted payload does not match")
        try:
            budget.check_deadline()
            os.fsync(fd)
            budget.check_deadline()
        except OSError:
            raise _ComponentInspectionError("backup manifest was not persisted safely") from None
        _ensure_path_identity(output_path, stable_metadata, budget=budget)
    finally:
        os.close(fd)
    parent_fd, _name = _open_parent_no_follow(output_path, budget=budget)
    try:
        try:
            budget.check_deadline()
            os.fsync(parent_fd)
            budget.check_deadline()
        except OSError:
            raise _ComponentInspectionError("backup manifest was not persisted safely") from None
    finally:
        os.close(parent_fd)


def write_backup_manifest(
    *,
    environment: str,
    namespace: str,
    output_path: Path,
    components: dict[str, Path],
    now: datetime | None = None,
    limits: BackupTraversalLimits | None = None,
    write_output: Callable[[Path, bytes], None] | None = None,
    schema_version: int = 1,
) -> dict[str, Any]:
    """Describe completed, externally quiesced backup artifacts.

    The bounded two-pass guard detects changes between an entry's content and
    metadata observations. It is not an atomic filesystem snapshot; callers
    must keep service writers quiesced through manifest persistence.
    """
    required_components = (
        _BACKUP_COMPONENTS_BY_SCHEMA.get(schema_version) if type(schema_version) is int else None
    )
    if required_components is None:
        raise ValueError("backup manifest schema_version is unsupported")
    missing = sorted(set(required_components) - set(components))
    if missing:
        raise ValueError(
            "missing required backup component(s): " + ", ".join(missing),
        )
    budget = _TraversalBudget(limits or BackupTraversalLimits())
    created_at = (now or datetime.now(UTC)).astimezone(UTC)
    inspections = _inspect_component_set(components, budget=budget)
    first_states = {name: inspection.state for name, inspection in inspections.items()}
    second_states = _component_states_metadata_only(
        inspections,
        budget=budget.fresh_counters(),
    )
    if first_states != second_states:
        raise _ComponentInspectionError("backup components changed during inspection")
    manifest: dict[str, Any] = {
        "schema_version": schema_version,
        "environment": environment,
        "namespace": namespace,
        "created_at": created_at.isoformat(),
        "components": {
            name: inspection.manifest_metadata for name, inspection in sorted(inspections.items())
        },
        "verification": {
            "status": "verified",
            "checked_at": created_at.isoformat(),
            "required_components": list(required_components),
        },
    }
    budget.check_deadline()
    payload = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")
    if len(payload) > budget.limits.max_manifest_bytes:
        raise _TraversalLimitError("manifest size limit exceeded")
    budget.check_deadline()
    absolute_output = _prepare_manifest_output_parent(output_path, budget=budget)
    if write_output is None:
        _default_write_manifest_output(absolute_output, payload, budget=budget)
    else:
        budget.check_deadline()
        write_output(output_path, payload)
        budget.check_deadline()
    _verify_manifest_output(absolute_output, payload, budget=budget)
    budget.check_deadline()
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
    limits: BackupTraversalLimits | None = None,
) -> list[str]:
    """Validate completed backup artifacts under the bounded two-pass contract.

    Callers must keep service writers quiesced. The traversal detects changes
    between an entry's content and metadata observations but deliberately does
    not retain the global descriptors needed for an atomic filesystem snapshot.
    """
    if manifest_path is None:
        return ["backup manifest is required"]
    path = Path(manifest_path)
    manifest_read_path = path
    budget = _TraversalBudget(limits or BackupTraversalLimits())
    strict = expected_owner_uid is not None or require_private_files
    manifest_root_metadata: os.stat_result | None = None
    if strict:
        try:
            manifest_root_metadata = _validate_strict_manifest_root(
                path.parent,
                expected_owner_uid=expected_owner_uid,
                require_private_files=require_private_files,
                budget=budget,
            )
        except _ComponentInspectionError as exc:
            return [f"backup manifest root {exc}"]
        try:
            manifest_snapshot = _read_strict_manifest(
                path,
                expected_owner_uid=expected_owner_uid,
                require_private_files=require_private_files,
                budget=budget,
            )
        except _ComponentInspectionError as exc:
            return [f"backup manifest {exc}"]
    else:
        if not path.exists():
            return [f"backup manifest not found: {path}"]
        manifest_read_path = Path(os.path.abspath(os.fspath(path)))
        try:
            manifest_snapshot = _read_strict_manifest(
                manifest_read_path,
                expected_owner_uid=None,
                require_private_files=False,
                budget=budget,
            )
        except _ComponentInspectionError as exc:
            return [f"backup manifest {exc}"]
    try:
        manifest = json.loads(manifest_snapshot.text)
    except json.JSONDecodeError as exc:
        return [f"backup manifest is not readable JSON: {type(exc).__name__}: {exc}"]
    problems: list[str] = []
    schema_version = manifest.get("schema_version")
    if type(schema_version) is not int or schema_version not in _BACKUP_COMPONENTS_BY_SCHEMA:
        problems.append("backup manifest schema_version must be 1 or 2")
        required_components: tuple[str, ...] = ()
    else:
        required_components = _BACKUP_COMPONENTS_BY_SCHEMA[schema_version]
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
    elif verification.get("required_components") != list(required_components):
        problems.append("backup manifest verification.required_components does not match schema")
    components = manifest.get("components")
    if not isinstance(components, dict):
        problems.append("backup manifest components must be an object")
        return problems
    inspections: dict[str, _InspectedComponent] = {}
    for name in required_components:
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
            inspection = _component_metadata(
                component_fs_path,
                budget=budget,
                expected_owner_uid=expected_owner_uid,
                require_private_files=require_private_files,
            )
        except _ComponentInspectionError as exc:
            problems.append(f"backup component {name!r} {exc}")
            continue
        except (OSError, ValueError):
            problems.append(f"backup component {name!r} could not be inspected")
            continue
        inspections[name] = inspection
        actual = inspection.manifest_metadata
        for field_name in ("kind", "size_bytes", "sha256"):
            recorded_value = component.get(field_name)
            actual_value = actual.get(field_name)
            if type(recorded_value) is not type(actual_value) or recorded_value != actual_value:
                problems.append(f"backup component {name!r} {field_name} does not match")
        if actual["kind"] == "directory":
            recorded_count = component.get("file_count")
            actual_count = actual.get("file_count")
            if type(recorded_count) is not int or recorded_count != actual_count:
                problems.append(f"backup component {name!r} file_count does not match")
    if required_components and len(inspections) == len(required_components):
        first_states = {name: inspection.state for name, inspection in inspections.items()}
        try:
            second_states = _component_states_metadata_only(
                inspections,
                budget=budget.fresh_counters(),
                expected_owner_uid=expected_owner_uid,
                require_private_files=require_private_files,
            )
        except _ComponentInspectionError as exc:
            problems.append(f"backup components {exc}")
        except (OSError, ValueError):
            problems.append("backup components could not be reinspected")
        else:
            if first_states != second_states:
                problems.append("backup components changed during inspection")
    if manifest_root_metadata is not None:
        try:
            _ensure_path_identity(
                path.parent,
                manifest_root_metadata,
                budget=budget,
            )
        except _ComponentInspectionError as exc:
            problems.append(f"backup manifest root {exc}")
    try:
        _revalidate_manifest_snapshot(
            manifest_read_path,
            manifest_snapshot,
            expected_owner_uid=expected_owner_uid,
            require_private_files=require_private_files,
            budget=budget,
        )
    except _ComponentInspectionError as exc:
        problems.append(f"backup manifest {exc}")
    return problems
