"""Attempt-private runtime secret file management for Pipeline containers.

The container receives this directory through a recursively read-only mount.
Only the worker mutates the host-side tmpfs.  Rotation deliberately uses a
same-directory exclusive temporary file followed by ``os.replace`` so an
open reader either observes the old complete token or the new complete token.
"""

from __future__ import annotations

import asyncio
import os
import stat
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4

STEP_JWT_NAME = "step-jwt"
VISIBLE_DIRECTORY_MODE = 0o500
PRIVATE_DIRECTORY_MODE = 0o700
STEP_JWT_MODE = 0o400
MAX_STEP_JWT_BYTES = 1_048_576
PIPELINE_PROVIDER_STEP_IDS = frozenset(
    {"offline_judge", "recovery_primitive"} | {f"generate_card_{index:02d}" for index in range(18)}
)


class RuntimeSecretError(RuntimeError):
    """The runtime secret boundary is malformed or cannot converge safely."""


def require_runtime_secret_tmpfs(
    root: Path,
    *,
    mountinfo_path: Path = Path("/proc/self/mountinfo"),
) -> Path:
    """Return an exact writable root only when its backing mount is tmpfs."""

    try:
        if root.is_symlink():
            raise RuntimeSecretError("runtime secret root must not be a symlink")
        resolved = root.resolve(strict=True)
        info = resolved.lstat()
        if not stat.S_ISDIR(info.st_mode):
            raise RuntimeSecretError("runtime secret root must be a directory")
        if not os.access(resolved, os.W_OK | os.X_OK):
            raise RuntimeSecretError("runtime secret root is not writable")
        mountinfo = mountinfo_path.read_text(encoding="utf-8")
    except RuntimeSecretError:
        raise
    except OSError as exc:
        raise RuntimeSecretError("runtime secret tmpfs cannot be observed") from exc

    candidates: list[tuple[int, str]] = []
    for line in mountinfo.splitlines():
        fields = line.split(" ")
        try:
            separator = fields.index("-")
        except ValueError:
            continue
        if separator + 1 >= len(fields) or len(fields) < 5:
            continue
        target = Path(_unescape_mountinfo(fields[4]))
        try:
            resolved.relative_to(target)
        except ValueError:
            continue
        candidates.append((len(target.parts), fields[separator + 1]))
    if not candidates or max(candidates, key=lambda item: item[0])[1] != "tmpfs":
        raise RuntimeSecretError("runtime secret root is not backed by tmpfs")
    return resolved


def _unescape_mountinfo(value: str) -> str:
    return (
        value.replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
    )


@dataclass(frozen=True)
class RuntimeSecretFileIdentity:
    """Stable identity returned after a successful atomic rotation."""

    device: int
    inode: int
    size_bytes: int
    uid: int
    gid: int
    mode: int


PipelineTokenMint = Callable[[UUID, str, int], Awaitable[str]]


@dataclass
class PipelineStepJwtRotator:
    """Keep one Attempt-private secret mount current for a Provider node.

    ``mint`` receives only the Attempt UUID, fixed node identity, and exact TTL;
    claim headers and worker authentication stay closed inside the callback.
    Every successful mint is installed with :meth:`RuntimeSecretMount.rotate`,
    preserving the 0400 owner and atomic-inode contract.
    """

    attempt_id: UUID
    step_id: str
    ttl_seconds: int
    secret_mount: RuntimeSecretMount
    mint: PipelineTokenMint
    _task: asyncio.Task[None] | None = None
    _failure: BaseException | None = None

    def __post_init__(self) -> None:
        if self.step_id not in PIPELINE_PROVIDER_STEP_IDS:
            raise RuntimeSecretError(
                "Pipeline JWT rotation is limited to registered Provider nodes"
            )
        if (
            isinstance(self.ttl_seconds, bool)
            or not isinstance(self.ttl_seconds, int)
            or not 1 <= self.ttl_seconds <= 30_000
        ):
            raise RuntimeSecretError("Pipeline JWT TTL must be in 1..30000 seconds")

    async def start(self) -> None:
        if self._task is not None:
            raise RuntimeSecretError("Pipeline JWT rotator is already started")
        token = await self.mint(self.attempt_id, self.step_id, self.ttl_seconds)
        self.secret_mount.rotate(token)
        self._failure = None
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        failure = self._failure
        self._failure = None
        if failure is not None:
            raise RuntimeSecretError("Pipeline JWT rotation failed") from failure

    async def _run(self) -> None:
        interval = max(1.0, self.ttl_seconds / 2.0)
        try:
            while True:
                await asyncio.sleep(interval)
                token = await self.mint(self.attempt_id, self.step_id, self.ttl_seconds)
                self.secret_mount.rotate(token)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            # Stop rotating after the first failure.  The worker observes the
            # failure on ``stop`` and must fail the Attempt; it must not log or
            # retry with a potentially expired bearer indefinitely.
            self._failure = exc

    async def __aenter__(self) -> PipelineStepJwtRotator:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()


@dataclass
class AttemptRuntimeSecretLifecycle:
    """Initialize, rotate, revoke, and remove one Attempt secret atomically."""

    secret_mount: RuntimeSecretMount
    rotator: PipelineStepJwtRotator
    started: bool = False

    async def start(self) -> None:
        if self.started:
            raise RuntimeSecretError("Attempt runtime secret is already started")
        self.secret_mount.initialize()
        try:
            await self.rotator.start()
        except BaseException:
            self.secret_mount.teardown()
            raise
        self.started = True

    async def teardown(self) -> None:
        if not self.started:
            if _path_lexists(self.secret_mount.root):
                self.secret_mount.teardown()
            return
        rotation_error: BaseException | None = None
        try:
            await self.rotator.stop()
        except BaseException as exc:
            rotation_error = exc
        finally:
            self.secret_mount.teardown()
            self.started = False
        if rotation_error is not None:
            raise RuntimeSecretError("Attempt JWT lifecycle failed") from rotation_error

    @property
    def absent(self) -> bool:
        return not self.started and not _path_lexists(self.secret_mount.root)


class RuntimeSecretMount:
    """Own exactly one ``step-jwt`` file below an Attempt-private directory.

    ``root`` must be a dedicated tmpfs directory selected by the worker.  The
    class never follows a symlink and refuses to tear down a directory that
    contains an unknown entry.  The container runtime must mount ``root``
    recursively read-only with ``nosuid,nodev,noexec``; that mount policy is
    represented by :class:`loom_worker.pipeline_container_runner.MountSpec`.
    """

    def __init__(
        self,
        root: Path,
        *,
        container_uid: int,
        container_gid: int,
    ) -> None:
        self.root = root
        self.container_uid = _positive_id(container_uid, "container_uid")
        self.container_gid = _positive_id(container_gid, "container_gid")
        self._lock = threading.Lock()
        self._initialized = False

    @property
    def token_path(self) -> Path:
        return self.root / STEP_JWT_NAME

    def initialize(self) -> None:
        """Create or verify the private empty directory.

        Existing secret contents are never adopted after a worker restart;
        callers must tear down a prior journal-owned directory first.
        """

        with self._lock:
            if self._initialized:
                self._validate_root()
                return
            created = False
            try:
                self.root.mkdir(mode=PRIVATE_DIRECTORY_MODE, parents=False, exist_ok=False)
                created = True
            except FileExistsError:
                self._validate_root()
                if any(self.root.iterdir()):
                    raise RuntimeSecretError("runtime secret directory is not empty") from None
            except OSError as exc:
                raise RuntimeSecretError("cannot create runtime secret directory") from exc
            try:
                os.chown(
                    self.root,
                    self.container_uid,
                    self.container_gid,
                    follow_symlinks=False,
                )
                os.chmod(self.root, VISIBLE_DIRECTORY_MODE, follow_symlinks=False)
            except OSError as exc:
                if created:
                    try:
                        self.root.rmdir()
                    except OSError:
                        pass
                raise RuntimeSecretError(
                    "worker cannot prepare the runtime secret directory for the container identity"
                ) from exc
            self._initialized = True

    def rotate(self, token: str | bytes) -> RuntimeSecretFileIdentity:
        """Atomically install ``token`` and return the new file identity."""

        payload = token.encode("utf-8") if isinstance(token, str) else bytes(token)
        if not payload or len(payload) > MAX_STEP_JWT_BYTES or b"\x00" in payload:
            raise RuntimeSecretError("step JWT must be non-empty bounded bytes without NUL")

        with self._lock:
            if not self._initialized:
                raise RuntimeSecretError("runtime secret directory is not initialized")
            self._validate_root()
            temporary = self.root / f".{STEP_JWT_NAME}.{uuid4().hex}.tmp"
            descriptor = -1
            # The source directory is mounted read-only into the container.
            # Its host-side owner needs a narrow write window for same-directory
            # replacement; restore the visible 0500 mode before returning.
            os.chmod(self.root, PRIVATE_DIRECTORY_MODE, follow_symlinks=False)
            try:
                flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
                flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
                descriptor = os.open(temporary, flags, 0o600)
                _write_all(descriptor, payload)
                os.fsync(descriptor)
                try:
                    os.fchown(descriptor, self.container_uid, self.container_gid)
                except PermissionError as exc:
                    raise RuntimeSecretError(
                        "worker cannot assign the step JWT to the remapped container identity"
                    ) from exc
                os.fchmod(descriptor, STEP_JWT_MODE)
                os.fsync(descriptor)
                os.close(descriptor)
                descriptor = -1
                os.replace(temporary, self.token_path)
                _fsync_directory(self.root)
            except BaseException:
                if descriptor >= 0:
                    os.close(descriptor)
                try:
                    temporary.unlink(missing_ok=True)
                except OSError:
                    pass
                raise
            finally:
                os.chmod(self.root, VISIBLE_DIRECTORY_MODE, follow_symlinks=False)

            return self.verify()

    def verify(self) -> RuntimeSecretFileIdentity:
        """Fail closed unless the sole secret is a regular 0400 owned file."""

        self._validate_root()
        entries = list(self.root.iterdir())
        if entries != [self.token_path]:
            raise RuntimeSecretError("runtime secret directory must contain only step-jwt")
        try:
            info = self.token_path.lstat()
        except OSError as exc:
            raise RuntimeSecretError("step JWT is missing") from exc
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise RuntimeSecretError("step JWT must be one regular non-linked file")
        mode = stat.S_IMODE(info.st_mode)
        if mode != STEP_JWT_MODE:
            raise RuntimeSecretError("step JWT mode must be 0400")
        if info.st_uid != self.container_uid or info.st_gid != self.container_gid:
            raise RuntimeSecretError("step JWT owner does not match the container identity")
        return RuntimeSecretFileIdentity(
            device=info.st_dev,
            inode=info.st_ino,
            size_bytes=info.st_size,
            uid=info.st_uid,
            gid=info.st_gid,
            mode=mode,
        )

    def read_verified(self) -> bytes:
        """Read the current token without following a substituted symlink."""

        before = self.verify()
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(self.token_path, flags)
        try:
            current = os.fstat(descriptor)
            if (current.st_dev, current.st_ino) != (before.device, before.inode):
                raise RuntimeSecretError("step JWT identity changed while opening")
            chunks: list[bytes] = []
            remaining = MAX_STEP_JWT_BYTES + 1
            while remaining:
                chunk = os.read(descriptor, min(remaining, 65_536))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            payload = b"".join(chunks)
            if not payload or len(payload) > MAX_STEP_JWT_BYTES:
                raise RuntimeSecretError("step JWT content is empty or oversized")
            return payload
        finally:
            os.close(descriptor)

    def teardown(self) -> None:
        """Remove the exact secret tree, refusing unrelated entries."""

        with self._lock:
            if not _path_lexists(self.root):
                self._initialized = False
                return
            self._validate_root()
            entries = list(self.root.iterdir())
            unexpected = [entry for entry in entries if entry.name != STEP_JWT_NAME]
            if unexpected:
                raise RuntimeSecretError("refusing teardown with unexpected runtime secret entries")
            os.chmod(self.root, PRIVATE_DIRECTORY_MODE, follow_symlinks=False)
            try:
                self.token_path.unlink(missing_ok=True)
                _fsync_directory(self.root)
                self.root.rmdir()
            except OSError as exc:
                raise RuntimeSecretError("runtime secret teardown did not converge") from exc
            self._initialized = False

    def _validate_root(self) -> None:
        try:
            info = self.root.lstat()
        except OSError as exc:
            raise RuntimeSecretError("runtime secret directory is missing") from exc
        if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
            raise RuntimeSecretError("runtime secret root must be a real directory")
        if info.st_uid != self.container_uid or info.st_gid != self.container_gid:
            raise RuntimeSecretError(
                "runtime secret directory owner does not match the container identity"
            )
        if stat.S_IMODE(info.st_mode) not in {VISIBLE_DIRECTORY_MODE, PRIVATE_DIRECTORY_MODE}:
            raise RuntimeSecretError("runtime secret directory mode is not private")


def _positive_id(value: int, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{label} must be a positive non-root integer")
    return value


def _path_lexists(path: Path) -> bool:
    return os.path.lexists(path)


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise RuntimeSecretError("short write while rotating step JWT")
        view = view[written:]


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
