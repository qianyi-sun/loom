"""Private immutable persistence for protected execution prerequisites."""

from __future__ import annotations

import contextlib
import fcntl
import os
import re
import stat
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from loom_cli.rollout.credential_authority import read_trusted_file

from .protected_execution_prerequisites import (
    ProtectedExecutionPrerequisiteArtifact,
    canonical_execution_prerequisite_bytes,
    parse_execution_prerequisite_bytes,
)

_PRIVATE_FILE_MODE = 0o600
_PRIVATE_DIRECTORY_MODE = 0o700
_MAX_ARTIFACT_BYTES = 2 * 1024 * 1024
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class ProtectedExecutionPrerequisiteStoreError(RuntimeError):
    """Raised when prerequisite authority cannot be persisted or verified."""


@dataclass(frozen=True, slots=True)
class ProtectedExecutionPrerequisitePublication:
    path: Path
    artifact_sha256: str

    def __post_init__(self) -> None:
        if (
            not self.path.is_absolute()
            or ".." in self.path.parts
            or _SHA256_RE.fullmatch(self.artifact_sha256) is None
            or self.path.name != f"{self.artifact_sha256}.json"
        ):
            raise ValueError("execution prerequisite publication identity is invalid")


class ProtectedExecutionPrerequisiteStore:
    """Publish exact digest-addressed prerequisite artifacts without replacement."""

    def __init__(self, state_root: Path | str, *, service_uid: int) -> None:
        self.state_root = Path(state_root)
        self.root = self.state_root / "execution-prerequisites"
        self.lifecycle_lock_path = self.state_root / "execution-prerequisites.lock"
        self.service_uid = service_uid
        self._held_lifecycle_lock: ContextVar[str | None] = ContextVar(
            f"execution_prerequisite_lifecycle_lock_{id(self)}",
            default=None,
        )
        if (
            not self.state_root.is_absolute()
            or ".." in self.state_root.parts
            or type(service_uid) is not int
            or service_uid < 0
        ):
            raise ProtectedExecutionPrerequisiteStoreError(
                "execution prerequisite store authority is invalid"
            )

    def publish(
        self,
        artifact: ProtectedExecutionPrerequisiteArtifact,
    ) -> ProtectedExecutionPrerequisitePublication:
        _ensure_directory(
            self.state_root,
            "execution prerequisite state root",
            service_uid=self.service_uid,
            parents=True,
        )
        with self.exclusive_lifecycle_lock():
            return self._publish_locked(artifact)

    def _publish_locked(
        self,
        artifact: ProtectedExecutionPrerequisiteArtifact,
    ) -> ProtectedExecutionPrerequisitePublication:
        if not isinstance(artifact, ProtectedExecutionPrerequisiteArtifact):
            raise ProtectedExecutionPrerequisiteStoreError(
                "execution prerequisite artifact is invalid"
            )
        payload = canonical_execution_prerequisite_bytes(artifact)
        if len(payload) > _MAX_ARTIFACT_BYTES:
            raise ProtectedExecutionPrerequisiteStoreError(
                "execution prerequisite artifact is too large"
            )
        try:
            if parse_execution_prerequisite_bytes(payload) != artifact:
                raise ValueError("artifact round-trip drifted")
        except ValueError as exc:
            raise ProtectedExecutionPrerequisiteStoreError(
                "execution prerequisite artifact is invalid"
            ) from exc
        self._ensure()
        publication = ProtectedExecutionPrerequisitePublication(
            path=self.root / f"{artifact.artifact_sha256}.json",
            artifact_sha256=artifact.artifact_sha256,
        )
        try:
            publication.path.lstat()
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise ProtectedExecutionPrerequisiteStoreError(
                "could not inspect execution prerequisite artifact"
            ) from exc
        else:
            if self.read(publication) != artifact:
                raise ProtectedExecutionPrerequisiteStoreError(
                    "execution prerequisite artifact digest collision"
                )
            return publication

        directory_fd = os.open(
            self.root,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        temporary = f".{publication.path.name}.{uuid4().hex}.tmp"
        temporary_exists = False
        raced = False
        try:
            descriptor = os.open(
                temporary,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                _PRIVATE_FILE_MODE,
                dir_fd=directory_fd,
            )
            temporary_exists = True
            try:
                os.fchmod(descriptor, _PRIVATE_FILE_MODE)
                offset = 0
                while offset < len(payload):
                    written = os.write(descriptor, payload[offset:])
                    if written < 1:
                        raise ProtectedExecutionPrerequisiteStoreError(
                            "execution prerequisite artifact write was incomplete"
                        )
                    offset += written
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            os.link(
                temporary,
                publication.path.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
            os.unlink(temporary, dir_fd=directory_fd)
            temporary_exists = False
            os.fsync(directory_fd)
        except FileExistsError:
            raced = True
        except OSError as exc:
            raise ProtectedExecutionPrerequisiteStoreError(
                "could not publish execution prerequisite artifact"
            ) from exc
        finally:
            if temporary_exists:
                try:
                    os.unlink(temporary, dir_fd=directory_fd)
                except OSError:
                    pass
            os.close(directory_fd)
        if raced and self.read(publication) != artifact:
            raise ProtectedExecutionPrerequisiteStoreError(
                "execution prerequisite artifact digest collision"
            )
        return publication

    def read(
        self,
        publication: ProtectedExecutionPrerequisitePublication,
    ) -> ProtectedExecutionPrerequisiteArtifact:
        with self.shared_lifecycle_lock():
            return self._read_locked(publication)

    def attestation_evidence(
        self,
        publication: ProtectedExecutionPrerequisitePublication,
    ) -> dict[str, object]:
        """Project one locked publication into secret-free preflight evidence."""
        with self.shared_lifecycle_lock():
            artifact = self._read_locked(publication)
            return {
                **artifact.attestation_evidence(),
                "mode": "activation",
                "bootstrap-authority-sha256": "0" * 64,
                "artifact-path": str(publication.path),
            }

    def _read_locked(
        self,
        publication: ProtectedExecutionPrerequisitePublication,
    ) -> ProtectedExecutionPrerequisiteArtifact:
        if (
            not isinstance(publication, ProtectedExecutionPrerequisitePublication)
            or publication.path.parent != self.root
        ):
            raise ProtectedExecutionPrerequisiteStoreError(
                "execution prerequisite publication is invalid"
            )
        _validate_directory(
            self.state_root,
            "execution prerequisite state root",
            service_uid=self.service_uid,
        )
        _validate_directory(
            self.root,
            "execution prerequisites directory",
            service_uid=self.service_uid,
        )
        try:
            trusted = read_trusted_file(
                publication.path,
                service_uid=self.service_uid,
                private=True,
                max_bytes=_MAX_ARTIFACT_BYTES,
                require_nonempty=True,
            )
            artifact = parse_execution_prerequisite_bytes(trusted.payload)
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            raise ProtectedExecutionPrerequisiteStoreError(
                "execution prerequisite artifact is invalid"
            ) from exc
        if artifact.artifact_sha256 != publication.artifact_sha256:
            raise ProtectedExecutionPrerequisiteStoreError(
                "execution prerequisite digest does not match its path"
            )
        return artifact

    def _ensure(self) -> None:
        _ensure_directory(
            self.state_root,
            "execution prerequisite state root",
            service_uid=self.service_uid,
            parents=True,
        )
        _ensure_directory(
            self.root,
            "execution prerequisites directory",
            service_uid=self.service_uid,
        )

    def shared_lifecycle_lock(self) -> AbstractContextManager[None]:
        """Hold one shared lock across an immutable prerequisite read."""
        return self._lifecycle_lock("shared")

    def exclusive_lifecycle_lock(self) -> AbstractContextManager[None]:
        """Exclude readers while publishing or retiring prerequisites."""
        return self._lifecycle_lock("exclusive")

    @contextmanager
    def _lifecycle_lock(self, requested: str) -> Iterator[None]:
        held = self._held_lifecycle_lock.get()
        if held == "exclusive" or held == requested == "shared":
            yield
            return
        if held == "shared":
            raise ProtectedExecutionPrerequisiteStoreError(
                "execution prerequisite lifecycle lock cannot be promoted"
            )
        _validate_directory(
            self.state_root,
            "execution prerequisite state root",
            service_uid=self.service_uid,
        )
        created = False
        create_flags = (
            os.O_RDWR
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        existing_flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            try:
                descriptor = os.open(
                    self.lifecycle_lock_path,
                    create_flags,
                    _PRIVATE_FILE_MODE,
                )
                created = True
            except FileExistsError:
                descriptor = os.open(self.lifecycle_lock_path, existing_flags)
        except OSError as exc:
            raise ProtectedExecutionPrerequisiteStoreError(
                "execution prerequisite lifecycle lock is unsafe"
            ) from exc
        locked = False
        token = None
        try:
            if created:
                os.fchmod(descriptor, _PRIVATE_FILE_MODE)
                os.fsync(descriptor)
                _fsync_directory(self.state_root)
            before = os.fstat(descriptor)
            _require_safe_lifecycle_lock(before, service_uid=self.service_uid)
            fcntl.flock(
                descriptor,
                fcntl.LOCK_SH if requested == "shared" else fcntl.LOCK_EX,
            )
            locked = True
            after = os.fstat(descriptor)
            _require_safe_lifecycle_lock(after, service_uid=self.service_uid)
            if _metadata_identity(after) != _metadata_identity(before):
                raise ProtectedExecutionPrerequisiteStoreError(
                    "execution prerequisite lifecycle lock changed during acquisition"
                )
            token = self._held_lifecycle_lock.set(requested)
            yield
        except ProtectedExecutionPrerequisiteStoreError:
            raise
        except OSError as exc:
            raise ProtectedExecutionPrerequisiteStoreError(
                "execution prerequisite lifecycle lock failed"
            ) from exc
        finally:
            if token is not None:
                self._held_lifecycle_lock.reset(token)
            if locked:
                with contextlib.suppress(OSError):
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)


def _validate_directory(path: Path, label: str, *, service_uid: int) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ProtectedExecutionPrerequisiteStoreError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != service_uid
        or stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE
    ):
        raise ProtectedExecutionPrerequisiteStoreError(f"{label} authority is unsafe")


def _ensure_directory(
    path: Path,
    label: str,
    *,
    service_uid: int,
    parents: bool = False,
) -> None:
    created = False
    try:
        path.mkdir(mode=_PRIVATE_DIRECTORY_MODE, parents=parents)
        created = True
    except FileExistsError:
        pass
    except OSError as exc:
        raise ProtectedExecutionPrerequisiteStoreError(f"could not create {label}") from exc
    if created:
        try:
            path.chmod(_PRIVATE_DIRECTORY_MODE)
            _fsync_directory(path.parent)
        except OSError as exc:
            raise ProtectedExecutionPrerequisiteStoreError(f"could not finalize {label}") from exc
    _validate_directory(path, label, service_uid=service_uid)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _require_safe_lifecycle_lock(metadata: os.stat_result, *, service_uid: int) -> None:
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != service_uid
        or stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE
        or metadata.st_nlink != 1
    ):
        raise ProtectedExecutionPrerequisiteStoreError(
            "execution prerequisite lifecycle lock is unsafe"
        )


def _metadata_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_size,
    )


__all__ = [
    "ProtectedExecutionPrerequisitePublication",
    "ProtectedExecutionPrerequisiteStore",
    "ProtectedExecutionPrerequisiteStoreError",
]
