"""Fail-closed local producer and Worker publisher for Stage 1 live preview.

Preview is deliberately outside the Artifact path.  The Stage container leaves
an atomic JPEG/record pair in an Attempt-private spool; the Worker admits that
pair as untrusted local input and sends it over the existing claim-bound
Control Plane channel.  Every failure is contained to this optional preview
channel so callers never have to change the Stage result.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import math
import os
import stat
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol
from uuid import UUID

import httpx

from loom.pipeline.keys import MAX_SAFE_INTEGER, canonical_document
from loom.pipeline.live_preview import (
    PREVIEW_HEIGHT,
    PREVIEW_MAX_BYTES,
    PREVIEW_MAX_FRAME_BYTES,
    PREVIEW_MAX_FRAMES,
    PREVIEW_MIN_INTERVAL,
    PREVIEW_SCHEMA_VERSION,
    PREVIEW_WIDTH,
    LivePreviewContractError,
    LivePreviewRecordV1,
    validate_preview_jpeg,
)
from loom_worker.control_plane_client import ExecutionAttemptClaimHeaders
from loom_worker.metrics import PIPELINE_LIVE_PREVIEW_EVENTS_TOTAL

PREVIEW_MAX_JPEG_BYTES = PREVIEW_MAX_FRAME_BYTES
PREVIEW_MAX_LOCAL_FRAMES = PREVIEW_MAX_FRAMES
PREVIEW_MAX_LOCAL_BYTES = PREVIEW_MAX_BYTES
PREVIEW_MIN_INTERVAL_SECONDS = PREVIEW_MIN_INTERVAL.total_seconds()
PREVIEW_SEQUENCE_WIDTH = 20
PREVIEW_MAX_RECORD_BYTES = 512
_UINT64_MAX = (1 << 64) - 1
_DIRECTORY_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_CLOEXEC", 0)
)
_FILE_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
_NON_BASELINE_SOF_MARKERS = tuple(
    bytes((0xFF, marker))
    for marker in (0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF)
)


class PipelineLivePreviewError(ValueError):
    """A stable, secret-free reason for closing only the preview channel."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class _InodeIdentity:
    device: int
    inode: int
    size: int
    modified_ns: int
    changed_ns: int


@dataclass(frozen=True, slots=True)
class PreparedLivePreviewFrame:
    record: LivePreviewRecordV1
    record_bytes: bytes
    jpeg: bytes
    record_inode: _InodeIdentity
    jpeg_inode: _InodeIdentity

    @property
    def signature(self) -> tuple[int, str, int]:
        return self.record.step_idx, self.record.jpeg_sha256, self.record.jpeg_size_bytes


@dataclass(frozen=True, slots=True)
class PreviewProducerResult:
    accepted: bool
    reason: Literal["accepted", "cadence_drop"]
    evicted_sequences: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class PreviewPublishResult:
    state: Literal["idle", "published", "retrying", "closed"]
    sequence: int | None = None
    reason: str | None = None


class LivePreviewControlPlaneV1(Protocol):
    async def publish_live_preview_frame(self, **kwargs: Any) -> Mapping[str, Any]: ...


class PipelineLivePreviewProducer:
    """Atomic, non-blocking writer for one Attempt-private preview spool."""

    def __init__(
        self,
        *,
        preview_root: Path,
        episode_bound: int,
        owner_uid: int | None = None,
    ) -> None:
        if not preview_root.is_absolute():
            raise PipelineLivePreviewError("preview_root_not_absolute")
        self.preview_root = preview_root
        self.episode_bound = _validate_episode_bound(episode_bound)
        self.owner_uid = os.geteuid() if owner_uid is None else owner_uid
        self._next_sequence = 0
        self._last_step_idx: int | None = None
        self._last_emitted_monotonic: float | None = None
        _create_or_validate_private_directory(preview_root, owner_uid=self.owner_uid)
        _remove_partial_files(preview_root, owner_uid=self.owner_uid)
        existing = scan_live_preview_frames(
            preview_root,
            owner_uid=self.owner_uid,
            episode_bound=self.episode_bound,
        )
        if existing:
            self._next_sequence = existing[-1].record.sequence + 1
            self._last_step_idx = existing[-1].record.step_idx

    def emit(
        self,
        *,
        sequence: int,
        step_idx: int,
        jpeg: bytes,
        monotonic_now: float,
    ) -> PreviewProducerResult:
        if (
            not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence < 0
            or sequence > MAX_SAFE_INTEGER
            or sequence != self._next_sequence
        ):
            raise PipelineLivePreviewError("preview_sequence_not_contiguous")
        if not isinstance(step_idx, int) or isinstance(step_idx, bool):
            raise PipelineLivePreviewError("preview_step_invalid")
        if step_idx < 0 or step_idx >= self.episode_bound:
            raise PipelineLivePreviewError("preview_step_invalid")
        if self._last_step_idx is not None and step_idx < self._last_step_idx:
            raise PipelineLivePreviewError("preview_step_regressed")
        if not isinstance(monotonic_now, (int, float)) or not math.isfinite(monotonic_now):
            raise PipelineLivePreviewError("preview_clock_invalid")
        if self._last_emitted_monotonic is not None:
            elapsed = monotonic_now - self._last_emitted_monotonic
            if elapsed < 0:
                raise PipelineLivePreviewError("preview_clock_regressed")
            if elapsed < PREVIEW_MIN_INTERVAL_SECONDS:
                PIPELINE_LIVE_PREVIEW_EVENTS_TOTAL.labels(result="dropped", reason="cadence").inc()
                return PreviewProducerResult(accepted=False, reason="cadence_drop")

        if not isinstance(jpeg, bytes):
            raise PipelineLivePreviewError("preview_jpeg_invalid")
        value = jpeg
        _validate_jpeg(value)
        digest = f"sha256:{hashlib.sha256(value).hexdigest()}"
        record = LivePreviewRecordV1(
            schema_version="loom.behavior-stage1-live-preview.v1",
            sequence=sequence,
            step_idx=step_idx,
            jpeg_sha256=digest,
            jpeg_size_bytes=len(value),
        )
        record_bytes = canonical_document(record.model_dump(mode="json"))
        evicted = _make_local_space(
            self.preview_root,
            incoming_bytes=len(value) + len(record_bytes),
            owner_uid=self.owner_uid,
            episode_bound=self.episode_bound,
        )
        _atomic_write_pair(
            self.preview_root,
            record=record,
            jpeg=value,
            owner_uid=self.owner_uid,
        )
        self._next_sequence += 1
        self._last_step_idx = step_idx
        self._last_emitted_monotonic = monotonic_now
        PIPELINE_LIVE_PREVIEW_EVENTS_TOTAL.labels(result="produced", reason="ok").inc()
        return PreviewProducerResult(
            accepted=True,
            reason="accepted",
            evicted_sequences=tuple(evicted),
        )


class PipelineLivePreviewPublisher:
    """Bounded one-frame-at-a-time publisher; errors never escape to Stage logic."""

    def __init__(
        self,
        *,
        preview_root: Path,
        attempt_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        control_plane: LivePreviewControlPlaneV1,
        episode_bound: int,
        owner_uid: int | None = None,
    ) -> None:
        if not preview_root.is_absolute():
            raise PipelineLivePreviewError("preview_root_not_absolute")
        self.preview_root = preview_root
        self.attempt_id = attempt_id
        self.claim = claim
        self.control_plane = control_plane
        self.episode_bound = _validate_episode_bound(episode_bound)
        self.owner_uid = os.geteuid() if owner_uid is None else owner_uid
        self.expected_sequence = 0
        self.closed_reason: str | None = None
        self._last_publish_attempt_monotonic: float | None = None
        self._last_published_step_idx: int | None = None
        self._observed: dict[int, tuple[int, str, int]] = {}
        self._published: dict[int, tuple[int, str, int]] = {}
        self._publish_lock = asyncio.Lock()

    async def publish_if_due(self, *, monotonic_now: float) -> PreviewPublishResult:
        if self._publish_lock.locked():
            return PreviewPublishResult(state="idle")
        async with self._publish_lock:
            return await self._publish_if_due(monotonic_now=monotonic_now)

    async def run_until(self, stop: asyncio.Event) -> None:
        """Poll at the fixed low-rate cadence until the runner ends this Attempt."""

        while not stop.is_set() and self.closed_reason is None:
            await self.publish_if_due(monotonic_now=time.monotonic())
            try:
                await asyncio.wait_for(stop.wait(), timeout=PREVIEW_MIN_INTERVAL_SECONDS)
            except TimeoutError:
                pass

    async def _publish_if_due(self, *, monotonic_now: float) -> PreviewPublishResult:
        if self.closed_reason is not None:
            return PreviewPublishResult(state="closed", reason=self.closed_reason)
        if not isinstance(monotonic_now, (int, float)) or not math.isfinite(monotonic_now):
            return self._close("preview_clock_invalid")
        if self._last_publish_attempt_monotonic is not None:
            elapsed = monotonic_now - self._last_publish_attempt_monotonic
            if elapsed < 0:
                return self._close("preview_clock_regressed")
            if elapsed < PREVIEW_MIN_INTERVAL_SECONDS:
                return PreviewPublishResult(state="idle")
        self._last_publish_attempt_monotonic = monotonic_now
        try:
            frames = scan_live_preview_frames(
                self.preview_root,
                owner_uid=self.owner_uid,
                episode_bound=self.episode_bound,
            )
            if not frames:
                return PreviewPublishResult(state="idle")
            frame = frames[0]
            sequence = frame.record.sequence
            if sequence > self.expected_sequence:
                return self._close("preview_sequence_gap")
            if sequence < self.expected_sequence:
                if self._published.get(sequence) != frame.signature:
                    return self._close("preview_sequence_equivocation")
            elif (
                self._last_published_step_idx is not None
                and frame.record.step_idx < self._last_published_step_idx
            ):
                return self._close("preview_step_regressed")
            prior = self._observed.setdefault(sequence, frame.signature)
            if prior != frame.signature:
                return self._close("preview_sequence_equivocation")
            try:
                await self.control_plane.publish_live_preview_frame(
                    attempt_id=self.attempt_id,
                    sequence=sequence,
                    step_idx=frame.record.step_idx,
                    jpeg_sha256=frame.record.jpeg_sha256,
                    jpeg=frame.jpeg,
                    claim=self.claim,
                )
            except httpx.HTTPStatusError as exc:
                if 400 <= exc.response.status_code < 500:
                    return self._close("control_plane_preview_rejected")
                PIPELINE_LIVE_PREVIEW_EVENTS_TOTAL.labels(
                    result="retrying", reason="control_plane"
                ).inc()
                return PreviewPublishResult(
                    state="retrying",
                    sequence=sequence,
                    reason="control_plane_publish_failed",
                )
            except Exception:
                PIPELINE_LIVE_PREVIEW_EVENTS_TOTAL.labels(
                    result="retrying", reason="control_plane"
                ).inc()
                return PreviewPublishResult(
                    state="retrying",
                    sequence=sequence,
                    reason="control_plane_publish_failed",
                )
            if self.closed_reason is not None:
                return PreviewPublishResult(state="closed", reason=self.closed_reason)
            _release_frame(
                self.preview_root,
                frame=frame,
                owner_uid=self.owner_uid,
            )
            self._published[sequence] = frame.signature
            if sequence == self.expected_sequence:
                self.expected_sequence += 1
                self._last_published_step_idx = frame.record.step_idx
            self._trim_signatures()
            PIPELINE_LIVE_PREVIEW_EVENTS_TOTAL.labels(result="published", reason="ok").inc()
            return PreviewPublishResult(state="published", sequence=sequence)
        except PipelineLivePreviewError as exc:
            if exc.reason == "preview_spool_changed":
                return PreviewPublishResult(state="retrying", reason=exc.reason)
            return self._close(exc.reason)
        except OSError:
            return self._close("preview_local_io_invalid")

    def stop(self, *, reason: str = "preview_lifecycle_ended") -> PreviewPublishResult:
        """Fence publication and idempotently purge local bytes."""

        self.closed_reason = reason
        with contextlib.suppress(PipelineLivePreviewError, OSError):
            purge_live_preview(self.preview_root, owner_uid=self.owner_uid)
        return PreviewPublishResult(state="closed", reason=reason)

    def _close(self, reason: str) -> PreviewPublishResult:
        PIPELINE_LIVE_PREVIEW_EVENTS_TOTAL.labels(result="closed", reason=reason).inc()
        return self.stop(reason=reason)

    def _trim_signatures(self) -> None:
        floor = max(0, self.expected_sequence - PREVIEW_MAX_LOCAL_FRAMES)
        self._published = {
            sequence: signature
            for sequence, signature in self._published.items()
            if sequence >= floor
        }
        self._observed = {
            sequence: signature
            for sequence, signature in self._observed.items()
            if sequence >= floor
        }


def scan_live_preview_frames(
    preview_root: Path,
    *,
    owner_uid: int | None = None,
    episode_bound: int,
) -> list[PreparedLivePreviewFrame]:
    """Read only exact committed pairs through a no-follow directory fd."""

    if not preview_root.is_absolute():
        raise PipelineLivePreviewError("preview_root_not_absolute")
    episode_bound = _validate_episode_bound(episode_bound)
    expected_owner = os.geteuid() if owner_uid is None else owner_uid
    try:
        root_fd = os.open(preview_root, _DIRECTORY_FLAGS)
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise PipelineLivePreviewError("preview_root_invalid") from exc
    try:
        root_before = _validate_directory_fd(root_fd, owner_uid=expected_owner)
        _validate_root_path(preview_root, root_fd, owner_uid=expected_owner)
        names = sorted(os.listdir(root_fd), key=lambda value: value.encode("utf-8"))
        partial = [name for name in names if name.startswith(".")]
        if any(not _valid_partial_name(name) for name in partial):
            raise PipelineLivePreviewError("preview_spool_entry_invalid")
        committed = [name for name in names if not name.startswith(".")]
        if any(not _valid_committed_name(name) for name in committed):
            raise PipelineLivePreviewError("preview_spool_entry_invalid")
        if (
            sum(".jpg." in name for name in partial)
            + sum(name.endswith(".jpg") for name in committed)
            > PREVIEW_MAX_LOCAL_FRAMES
        ):
            raise PipelineLivePreviewError("preview_local_frame_limit")
        local_bytes = 0
        local_jpegs = 0
        for name in partial:
            facts = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
            _validate_private_file(facts, owner_uid=expected_owner)
            limit = PREVIEW_MAX_JPEG_BYTES if ".jpg." in name else PREVIEW_MAX_RECORD_BYTES
            if facts.st_size > limit:
                raise PipelineLivePreviewError("preview_file_size_invalid")
            local_bytes += facts.st_size
            local_jpegs += int(".jpg." in name)
        stems: dict[str, set[str]] = {}
        for name in committed:
            facts = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
            _validate_private_file(facts, owner_uid=expected_owner)
            local_bytes += facts.st_size
            local_jpegs += int(name.endswith(".jpg"))
            stem, extension = name.rsplit(".", 1)
            stems.setdefault(stem, set()).add(extension)
        if any("json" in extensions and "jpg" not in extensions for extensions in stems.values()):
            raise PipelineLivePreviewError("preview_atomic_pair_incomplete")
        frames = [
            _read_frame_at(
                root_fd,
                stem=stem,
                owner_uid=expected_owner,
                episode_bound=episode_bound,
            )
            for stem, extensions in sorted(stems.items())
            if extensions == {"json", "jpg"}
        ]
        sequences = [item.record.sequence for item in frames]
        if sequences and sequences != list(range(sequences[0], sequences[0] + len(sequences))):
            raise PipelineLivePreviewError("preview_sequence_gap")
        steps = [item.record.step_idx for item in frames]
        if steps != sorted(steps):
            raise PipelineLivePreviewError("preview_step_regressed")
        if local_jpegs > PREVIEW_MAX_LOCAL_FRAMES:
            raise PipelineLivePreviewError("preview_local_frame_limit")
        if local_bytes > PREVIEW_MAX_LOCAL_BYTES:
            raise PipelineLivePreviewError("preview_local_byte_limit")
        if _directory_identity(root_fd) != root_before:
            raise PipelineLivePreviewError("preview_spool_changed")
        _validate_root_path(preview_root, root_fd, owner_uid=expected_owner)
        return frames
    finally:
        os.close(root_fd)


def purge_live_preview(preview_root: Path, *, owner_uid: int | None = None) -> int:
    """Idempotently remove private regular files without following links."""

    if not preview_root.is_absolute():
        raise PipelineLivePreviewError("preview_root_not_absolute")
    expected_owner = os.geteuid() if owner_uid is None else owner_uid
    try:
        root_fd = os.open(preview_root, _DIRECTORY_FLAGS)
    except FileNotFoundError:
        return 0
    except OSError as exc:
        raise PipelineLivePreviewError("preview_root_invalid") from exc
    removed = 0
    try:
        _validate_directory_fd(root_fd, owner_uid=expected_owner)
        _validate_root_path(preview_root, root_fd, owner_uid=expected_owner)
        for name in os.listdir(root_fd):
            facts = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
            _validate_private_file(facts, owner_uid=expected_owner)
            os.unlink(name, dir_fd=root_fd)
            removed += 1
        os.fsync(root_fd)
        _validate_root_path(preview_root, root_fd, owner_uid=expected_owner)
        return removed
    except OSError as exc:
        raise PipelineLivePreviewError("preview_purge_failed") from exc
    finally:
        os.close(root_fd)


def _read_frame_at(
    root_fd: int,
    *,
    stem: str,
    owner_uid: int,
    episode_bound: int,
) -> PreparedLivePreviewFrame:
    sequence = int(stem)
    record_bytes, record_inode = _read_private_regular_at(
        root_fd,
        f"{stem}.json",
        max_bytes=PREVIEW_MAX_RECORD_BYTES,
        owner_uid=owner_uid,
    )
    jpeg, jpeg_inode = _read_private_regular_at(
        root_fd,
        f"{stem}.jpg",
        max_bytes=PREVIEW_MAX_JPEG_BYTES,
        owner_uid=owner_uid,
    )
    try:
        record = LivePreviewRecordV1.model_validate_json(record_bytes)
    except ValueError as exc:
        raise PipelineLivePreviewError("preview_record_invalid") from exc
    if canonical_document(record.model_dump(mode="json")) != record_bytes:
        raise PipelineLivePreviewError("preview_record_not_canonical")
    if record.sequence != sequence:
        raise PipelineLivePreviewError("preview_sequence_mismatch")
    if record.step_idx >= episode_bound:
        raise PipelineLivePreviewError("preview_step_invalid")
    if record.jpeg_size_bytes != len(jpeg):
        raise PipelineLivePreviewError("preview_size_mismatch")
    digest = f"sha256:{hashlib.sha256(jpeg).hexdigest()}"
    if record.jpeg_sha256 != digest:
        raise PipelineLivePreviewError("preview_digest_mismatch")
    _validate_jpeg(jpeg)
    return PreparedLivePreviewFrame(
        record=record,
        record_bytes=record_bytes,
        jpeg=jpeg,
        record_inode=record_inode,
        jpeg_inode=jpeg_inode,
    )


def _validate_jpeg(value: bytes) -> None:
    if b"\xff\xc0" not in value or any(marker in value for marker in _NON_BASELINE_SOF_MARKERS):
        raise PipelineLivePreviewError("preview_jpeg_not_baseline")
    try:
        validate_preview_jpeg(value)
    except LivePreviewContractError as exc:
        raise PipelineLivePreviewError(exc.reason) from exc


def _atomic_write_pair(
    preview_root: Path,
    *,
    record: LivePreviewRecordV1,
    jpeg: bytes,
    owner_uid: int,
) -> None:
    root_fd = os.open(preview_root, _DIRECTORY_FLAGS)
    stem = f"{record.sequence:0{PREVIEW_SEQUENCE_WIDTH}d}"
    jpeg_name = f"{stem}.jpg"
    record_name = f"{stem}.json"
    jpeg_temp = f".{jpeg_name}.partial"
    record_temp = f".{record_name}.partial"
    record_value = canonical_document(record.model_dump(mode="json"))
    try:
        _validate_directory_fd(root_fd, owner_uid=owner_uid)
        _validate_root_path(preview_root, root_fd, owner_uid=owner_uid)
        for name in (jpeg_name, record_name):
            with contextlib.suppress(FileNotFoundError):
                os.stat(name, dir_fd=root_fd, follow_symlinks=False)
                raise PipelineLivePreviewError("preview_sequence_already_exists")
        _write_private_at(root_fd, jpeg_temp, jpeg)
        os.rename(jpeg_temp, jpeg_name, src_dir_fd=root_fd, dst_dir_fd=root_fd)
        os.fsync(root_fd)
        try:
            _write_private_at(root_fd, record_temp, record_value)
            os.rename(record_temp, record_name, src_dir_fd=root_fd, dst_dir_fd=root_fd)
            os.fsync(root_fd)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(jpeg_name, dir_fd=root_fd)
            os.fsync(root_fd)
            raise
        _validate_directory_fd(root_fd, owner_uid=owner_uid)
    except OSError as exc:
        raise PipelineLivePreviewError("preview_atomic_publish_failed") from exc
    finally:
        for name in (jpeg_temp, record_temp):
            with contextlib.suppress(FileNotFoundError):
                os.unlink(name, dir_fd=root_fd)
        os.close(root_fd)


def _write_private_at(root_fd: int, name: str, value: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(name, flags, 0o600, dir_fd=root_fd)
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(name, dir_fd=root_fd)
        raise


def _make_local_space(
    preview_root: Path,
    *,
    incoming_bytes: int,
    owner_uid: int,
    episode_bound: int,
) -> list[int]:
    frames = scan_live_preview_frames(
        preview_root,
        owner_uid=owner_uid,
        episode_bound=episode_bound,
    )
    total = sum(len(item.jpeg) + len(item.record_bytes) for item in frames)
    evicted: list[int] = []
    while frames and (
        len(frames) + 1 > PREVIEW_MAX_LOCAL_FRAMES
        or total + incoming_bytes > PREVIEW_MAX_LOCAL_BYTES
    ):
        frame = frames.pop(0)
        _release_frame(preview_root, frame=frame, owner_uid=owner_uid)
        total -= len(frame.jpeg) + len(frame.record_bytes)
        evicted.append(frame.record.sequence)
    if incoming_bytes > PREVIEW_MAX_LOCAL_BYTES:
        raise PipelineLivePreviewError("preview_local_byte_limit")
    return evicted


def _release_frame(
    preview_root: Path,
    *,
    frame: PreparedLivePreviewFrame,
    owner_uid: int,
) -> None:
    root_fd = os.open(preview_root, _DIRECTORY_FLAGS)
    stem = f"{frame.record.sequence:0{PREVIEW_SEQUENCE_WIDTH}d}"
    pairs = ((f"{stem}.json", frame.record_inode), (f"{stem}.jpg", frame.jpeg_inode))
    try:
        _validate_directory_fd(root_fd, owner_uid=owner_uid)
        _validate_root_path(preview_root, root_fd, owner_uid=owner_uid)
        for name, identity in pairs:
            current = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
            _validate_private_file(current, owner_uid=owner_uid)
            if _inode_identity(current) != identity:
                raise PipelineLivePreviewError("preview_path_drift")
        for name, _identity in pairs:
            os.unlink(name, dir_fd=root_fd)
        os.fsync(root_fd)
        _validate_root_path(preview_root, root_fd, owner_uid=owner_uid)
    except OSError as exc:
        raise PipelineLivePreviewError("preview_release_failed") from exc
    finally:
        os.close(root_fd)


def _read_private_regular_at(
    parent_fd: int,
    name: str,
    *,
    max_bytes: int,
    owner_uid: int,
) -> tuple[bytes, _InodeIdentity]:
    try:
        before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        _validate_private_file(before, owner_uid=owner_uid)
        if before.st_size < 1 or before.st_size > max_bytes:
            raise PipelineLivePreviewError("preview_file_size_invalid")
        fd = os.open(name, _FILE_FLAGS, dir_fd=parent_fd)
        try:
            opened = os.fstat(fd)
            _validate_private_file(opened, owner_uid=owner_uid)
            if _inode_identity(opened) != _inode_identity(before):
                raise PipelineLivePreviewError("preview_path_drift")
            chunks: list[bytes] = []
            total = 0
            while True:
                chunk = os.read(fd, min(64 * 1024, max_bytes + 1 - total))
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
                if total > max_bytes:
                    raise PipelineLivePreviewError("preview_file_size_invalid")
            after = os.fstat(fd)
            if _inode_identity(after) != _inode_identity(opened) or total != after.st_size:
                raise PipelineLivePreviewError("preview_path_drift")
            return b"".join(chunks), _inode_identity(after)
        finally:
            os.close(fd)
    except OSError as exc:
        raise PipelineLivePreviewError("preview_file_invalid") from exc


def _create_or_validate_private_directory(path: Path, *, owner_uid: int) -> None:
    try:
        path.mkdir(mode=0o700, parents=True, exist_ok=False)
    except FileExistsError:
        pass
    try:
        descriptor = os.open(path, _DIRECTORY_FLAGS)
    except OSError as exc:
        raise PipelineLivePreviewError("preview_root_invalid") from exc
    try:
        _validate_directory_fd(descriptor, owner_uid=owner_uid)
    finally:
        os.close(descriptor)


def _remove_partial_files(path: Path, *, owner_uid: int) -> None:
    descriptor = os.open(path, _DIRECTORY_FLAGS)
    try:
        _validate_directory_fd(descriptor, owner_uid=owner_uid)
        _validate_root_path(path, descriptor, owner_uid=owner_uid)
        names = set(os.listdir(descriptor))
        changed = False
        for name in names:
            partial = _valid_partial_name(name)
            orphan = _valid_committed_name(name) and (
                (name.endswith(".jpg") and name.removesuffix(".jpg") + ".json" not in names)
                or (name.endswith(".json") and name.removesuffix(".json") + ".jpg" not in names)
            )
            if not partial and not orphan:
                continue
            facts = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            _validate_private_file(facts, owner_uid=owner_uid)
            os.unlink(name, dir_fd=descriptor)
            changed = True
        if changed:
            os.fsync(descriptor)
    except OSError as exc:
        raise PipelineLivePreviewError("preview_partial_cleanup_failed") from exc
    finally:
        os.close(descriptor)


def _validate_directory_fd(fd: int, *, owner_uid: int) -> tuple[int, int, int, int, int, int]:
    facts = os.fstat(fd)
    if (
        not stat.S_ISDIR(facts.st_mode)
        or stat.S_IMODE(facts.st_mode) != 0o700
        or facts.st_uid != owner_uid
    ):
        raise PipelineLivePreviewError("preview_root_not_private")
    return _directory_identity(fd)


def _validate_private_file(facts: os.stat_result, *, owner_uid: int) -> None:
    if (
        not stat.S_ISREG(facts.st_mode)
        or facts.st_nlink != 1
        or stat.S_IMODE(facts.st_mode) != 0o600
        or facts.st_uid != owner_uid
    ):
        raise PipelineLivePreviewError("preview_file_not_private")


def _directory_identity(fd: int) -> tuple[int, int, int, int, int, int]:
    facts = os.fstat(fd)
    return (
        facts.st_dev,
        facts.st_ino,
        facts.st_uid,
        stat.S_IMODE(facts.st_mode),
        facts.st_mtime_ns,
        facts.st_ctime_ns,
    )


def _inode_identity(facts: os.stat_result) -> _InodeIdentity:
    return _InodeIdentity(
        device=facts.st_dev,
        inode=facts.st_ino,
        size=facts.st_size,
        modified_ns=facts.st_mtime_ns,
        changed_ns=facts.st_ctime_ns,
    )


def _valid_committed_name(name: str) -> bool:
    if name.count(".") != 1:
        return False
    stem, extension = name.rsplit(".", 1)
    return (
        extension in {"jpg", "json"}
        and len(stem) == PREVIEW_SEQUENCE_WIDTH
        and stem.isascii()
        and stem.isdigit()
    )


def _valid_partial_name(name: str) -> bool:
    if not name.startswith(".") or not name.endswith(".partial"):
        return False
    return _valid_committed_name(name[1:].removesuffix(".partial"))


def _validate_episode_bound(value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0 or value > _UINT64_MAX:
        raise PipelineLivePreviewError("preview_episode_bound_invalid")
    return value


def _validate_root_path(path: Path, fd: int, *, owner_uid: int) -> None:
    try:
        facts = os.stat(path, follow_symlinks=False)
    except OSError as exc:
        raise PipelineLivePreviewError("preview_path_drift") from exc
    _validate_private_directory(facts, owner_uid=owner_uid)
    opened = os.fstat(fd)
    if (facts.st_dev, facts.st_ino) != (opened.st_dev, opened.st_ino):
        raise PipelineLivePreviewError("preview_path_drift")


def _validate_private_directory(facts: os.stat_result, *, owner_uid: int) -> None:
    if (
        not stat.S_ISDIR(facts.st_mode)
        or stat.S_IMODE(facts.st_mode) != 0o700
        or facts.st_uid != owner_uid
    ):
        raise PipelineLivePreviewError("preview_root_not_private")


__all__ = [
    "PREVIEW_HEIGHT",
    "PREVIEW_MAX_JPEG_BYTES",
    "PREVIEW_MAX_LOCAL_BYTES",
    "PREVIEW_MAX_LOCAL_FRAMES",
    "PREVIEW_MIN_INTERVAL_SECONDS",
    "PREVIEW_SCHEMA_VERSION",
    "PREVIEW_WIDTH",
    "LivePreviewControlPlaneV1",
    "LivePreviewRecordV1",
    "PipelineLivePreviewError",
    "PipelineLivePreviewProducer",
    "PipelineLivePreviewPublisher",
    "PreparedLivePreviewFrame",
    "PreviewProducerResult",
    "PreviewPublishResult",
    "purge_live_preview",
    "scan_live_preview_frames",
]
