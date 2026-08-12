"""Safe local discovery and admission for committed Pipeline checkpoints.

This module performs no object-store or database mutation.  It converts only
fully committed ``AttemptWorkspace`` directories into worker-owned outer
envelopes and persists the small local journal needed for restart and
cancellation discovery fencing.
"""

from __future__ import annotations

import contextlib
import os
import stat
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID, uuid4, uuid5

from pydantic import Field, field_validator, model_validator

from loom.pipeline.checkpoint import (
    CheckpointPayloadFileV1,
    ExecutionCheckpointV1,
    resume_compatibility_key,
)
from loom.pipeline.keys import MAX_SAFE_INTEGER, canonical_digest, canonical_document, digest_bytes
from loom.pipeline.spec import CheckpointPolicyV1, Digest, PipelineModel
from loom_worker.pipeline_attempt_workspace import (
    CHECKPOINT_SEQUENCE_WIDTH,
    MAX_CHECKPOINT_BYTES,
    MAX_COMPLETE_BYTES,
    BehaviorCheckpointCompleteV1,
    BehaviorRecoveryLedgerV1,
)

CHECKPOINT_SCAN_INTERVAL_SECONDS = 5.0
_READ_CHUNK_BYTES = 1024 * 1024
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_FILE_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)


class PipelineCheckpointWatcherError(ValueError):
    """A local checkpoint cannot safely become a Pipeline Artifact."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class CheckpointClaimIdentityV1(PipelineModel):
    """Server-frozen identity copied into every outer envelope."""

    pipeline_run_id: UUID
    stage_run_id: UUID
    attempt_id: UUID
    recipe_digest: Digest
    resolved_input_bindings_digest: Digest
    execution_spec_digest: Digest
    image_digest: Digest

    @property
    def resume_compatibility_key(self) -> str:
        return resume_compatibility_key(
            recipe_digest=self.recipe_digest,
            resolved_input_bindings_digest=self.resolved_input_bindings_digest,
            execution_spec_digest=self.execution_spec_digest,
            image_digest=self.image_digest,
        )


@dataclass(frozen=True, slots=True)
class LocalCheckpointFile:
    descriptor: CheckpointPayloadFileV1
    value: bytes


@dataclass(frozen=True, slots=True)
class CompletedCheckpointDirectory:
    root: Path
    sequence: int
    ledger: BehaviorRecoveryLedgerV1
    complete: BehaviorCheckpointCompleteV1
    files: tuple[LocalCheckpointFile, ...]

    @property
    def ledger_sha256(self) -> str:
        return next(
            item.descriptor.sha256
            for item in self.files
            if item.descriptor.relative_path == "ledger.json"
        )

    @property
    def complete_sha256(self) -> str:
        return next(
            item.descriptor.sha256
            for item in self.files
            if item.descriptor.relative_path == "COMPLETE.json"
        )

    def outer_envelope(self, identity: CheckpointClaimIdentityV1) -> ExecutionCheckpointV1:
        ledger = self.ledger
        if (
            ledger.input_digest != identity.resolved_input_bindings_digest
            or ledger.execution_spec_digest != identity.execution_spec_digest
            or ledger.resume_compatibility_key != identity.resume_compatibility_key
        ):
            raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
        return ExecutionCheckpointV1(
            schema_version="loom.execution-checkpoint.v1",
            pipeline_run_id=identity.pipeline_run_id,
            stage_run_id=identity.stage_run_id,
            attempt_id=identity.attempt_id,
            sequence=self.sequence,
            recipe_digest=identity.recipe_digest,
            resolved_input_bindings_digest=identity.resolved_input_bindings_digest,
            execution_spec_digest=identity.execution_spec_digest,
            image_digest=identity.image_digest,
            resume_compatibility_key=identity.resume_compatibility_key,
            inner_ledger_sha256=self.ledger_sha256,
            inner_complete_sha256=self.complete_sha256,
            files=[item.descriptor for item in self.files],
        )

    def prepare(
        self,
        *,
        identity: CheckpointClaimIdentityV1,
        policy: CheckpointPolicyV1,
    ) -> PreparedCheckpoint:
        envelope = self.outer_envelope(identity)
        try:
            envelope.require_within(policy.max_bytes)
        except ValueError as exc:
            if str(exc) == "checkpoint_too_large":
                raise PipelineCheckpointWatcherError("checkpoint_too_large") from exc
            raise
        return PreparedCheckpoint(
            local=self,
            envelope=envelope,
            checkpoint_json=envelope.persisted_bytes(),
        )


@dataclass(frozen=True, slots=True)
class PreparedCheckpoint:
    local: CompletedCheckpointDirectory
    envelope: ExecutionCheckpointV1
    checkpoint_json: bytes

    @property
    def exact_artifact_data_bytes(self) -> int:
        return len(self.checkpoint_json) + sum(
            item.descriptor.size_bytes for item in self.local.files
        )


class CheckpointJournalSignatureV1(PipelineModel):
    sequence: Annotated[int, Field(strict=True, ge=0, le=MAX_SAFE_INTEGER)]
    ledger_sha256: Digest
    complete_sha256: Digest


class CheckpointJournalEntryV1(CheckpointJournalSignatureV1):
    state: Literal["discovered", "session_started", "committed"]
    upload_session_id: UUID | None = None
    committed_artifact_id: UUID | None = None
    committed_at: datetime | None

    _committed_at_is_aware = field_validator("committed_at")(
        lambda value: _aware_or_none(value)
    )

    @model_validator(mode="after")
    def committed_timestamp_matches_state(self) -> CheckpointJournalEntryV1:
        if (self.state == "committed") != (self.committed_at is not None):
            raise ValueError("checkpoint journal committed timestamp group is invalid")
        if self.state == "discovered" and self.upload_session_id is not None:
            raise ValueError("discovered checkpoint cannot carry an upload session")
        if self.state == "session_started" and self.upload_session_id is None:
            raise ValueError("started checkpoint must carry its durable upload session")
        if (self.state == "committed") != (self.committed_artifact_id is not None):
            raise ValueError("committed checkpoint must carry its Artifact authority")
        return self


class CheckpointWatcherJournalV1(PipelineModel):
    schema_version: Literal["loom.pipeline-checkpoint-watcher-journal.v1"]
    entries: list[CheckpointJournalEntryV1]
    cancellation_observed_at: datetime | None
    cancellation_discovery_closed: bool
    cancellation_frozen: list[CheckpointJournalSignatureV1]
    cancel_drain_sequence: Annotated[
        int | None, Field(strict=True, ge=0, le=MAX_SAFE_INTEGER)
    ]

    _observed_at_is_aware = field_validator("cancellation_observed_at")(
        lambda value: _aware_or_none(value)
    )

    @model_validator(mode="after")
    def ordered_and_frozen_groups_are_exact(self) -> CheckpointWatcherJournalV1:
        entry_sequences = [item.sequence for item in self.entries]
        frozen_sequences = [item.sequence for item in self.cancellation_frozen]
        if entry_sequences != sorted(entry_sequences) or len(entry_sequences) != len(
            set(entry_sequences)
        ):
            raise ValueError("checkpoint journal entries must be sequence ordered and unique")
        if frozen_sequences != sorted(frozen_sequences) or len(frozen_sequences) != len(
            set(frozen_sequences)
        ):
            raise ValueError("frozen checkpoint discovery must be ordered and unique")
        if (self.cancellation_observed_at is not None) != self.cancellation_discovery_closed:
            raise ValueError("cancellation freeze timestamp and closed flag must pair")
        if not self.cancellation_discovery_closed and self.cancellation_frozen:
            raise ValueError("open checkpoint discovery cannot carry a frozen inventory")
        if self.cancel_drain_sequence is not None and self.cancel_drain_sequence not in set(
            frozen_sequences
        ):
            raise ValueError("cancel drain sequence is outside frozen discovery")
        return self


def _aware_or_none(value: datetime | None) -> datetime | None:
    if value is not None and (value.tzinfo is None or value.utcoffset() is None):
        raise ValueError("checkpoint journal timestamps must be timezone-aware")
    return value


class CheckpointWatcherJournal:
    """Durable, atomically replaced local checkpoint watcher journal."""

    def __init__(self, path: Path) -> None:
        if not path.is_absolute():
            raise PipelineCheckpointWatcherError("checkpoint_journal_path_not_absolute")
        self.path = path
        self.state = self._load()

    def _load(self) -> CheckpointWatcherJournalV1:
        if not self.path.exists():
            return CheckpointWatcherJournalV1(
                schema_version="loom.pipeline-checkpoint-watcher-journal.v1",
                entries=[],
                cancellation_observed_at=None,
                cancellation_discovery_closed=False,
                cancellation_frozen=[],
                cancel_drain_sequence=None,
            )
        if self.path.is_symlink() or not self.path.is_file():
            raise PipelineCheckpointWatcherError("checkpoint_journal_invalid")
        try:
            value = self.path.read_bytes()
            state = CheckpointWatcherJournalV1.model_validate_json(value)
        except (OSError, ValueError) as exc:
            raise PipelineCheckpointWatcherError("checkpoint_journal_invalid") from exc
        if value != _journal_bytes(state):
            raise PipelineCheckpointWatcherError("checkpoint_journal_not_canonical")
        return state

    def observe(self, checkpoints: list[CompletedCheckpointDirectory]) -> None:
        entries = {item.sequence: item for item in self.state.entries}
        frozen = {item.sequence: item for item in self.state.cancellation_frozen}
        for checkpoint in checkpoints:
            signature = CheckpointJournalSignatureV1(
                sequence=checkpoint.sequence,
                ledger_sha256=checkpoint.ledger_sha256,
                complete_sha256=checkpoint.complete_sha256,
            )
            if self.state.cancellation_observed_at is not None:
                prior_frozen = frozen.get(checkpoint.sequence)
                if prior_frozen is None:
                    continue
                if prior_frozen != signature:
                    raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
            prior = entries.get(checkpoint.sequence)
            if prior is not None:
                if (
                    prior.ledger_sha256 != signature.ledger_sha256
                    or prior.complete_sha256 != signature.complete_sha256
                ):
                    raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
                continue
            entries[checkpoint.sequence] = CheckpointJournalEntryV1(
                **signature.model_dump(mode="python"),
                state="discovered",
                upload_session_id=None,
                committed_artifact_id=None,
                committed_at=None,
            )
        self._replace(entries=list(entries.values()))

    def freeze_cancellation(
        self,
        *,
        observed_at: datetime,
        checkpoints: list[CompletedCheckpointDirectory],
    ) -> None:
        _aware_or_none(observed_at)
        signatures = [
            CheckpointJournalSignatureV1(
                sequence=item.sequence,
                ledger_sha256=item.ledger_sha256,
                complete_sha256=item.complete_sha256,
            )
            for item in checkpoints
        ]
        if self.state.cancellation_observed_at is not None:
            if (
                self.state.cancellation_observed_at != observed_at
                or self.state.cancellation_frozen != signatures
            ):
                raise PipelineCheckpointWatcherError("cancellation_freeze_drift")
            return
        self.observe(checkpoints)
        self._replace(
            cancellation_observed_at=observed_at,
            cancellation_discovery_closed=True,
            cancellation_frozen=signatures,
        )

    def cancel_drain_candidate(
        self,
        *,
        terminal_cause: str,
        reservation_fits: bool,
    ) -> int | None:
        if (
            terminal_cause != "user_cancel"
            or not reservation_fits
            or self.state.cancellation_observed_at is None
        ):
            return None
        discovered = {
            item.sequence for item in self.state.entries if item.state == "discovered"
        }
        candidates = [
            item.sequence
            for item in self.state.cancellation_frozen
            if item.sequence in discovered
        ]
        return max(candidates) if candidates else None

    def mark_session_started(
        self,
        sequence: int,
        *,
        upload_session_id: UUID | None = None,
        cancel_drain: bool = False,
    ) -> None:
        entries = self._entry_map()
        entry = entries.get(sequence)
        if entry is None:
            raise PipelineCheckpointWatcherError("checkpoint_not_discovered")
        if self.state.cancellation_observed_at is not None:
            if not cancel_drain or self.cancel_drain_candidate(
                terminal_cause="user_cancel", reservation_fits=True
            ) != sequence:
                raise PipelineCheckpointWatcherError("checkpoint_cancel_drain_forbidden")
            if self.state.cancel_drain_sequence not in {None, sequence}:
                raise PipelineCheckpointWatcherError("checkpoint_cancel_drain_forbidden")
        if entry.state == "committed":
            return
        if entry.state == "session_started":
            if upload_session_id is not None and entry.upload_session_id != upload_session_id:
                raise PipelineCheckpointWatcherError("checkpoint_upload_session_drift")
            return
        if upload_session_id is None:
            # Compatibility for existing direct journal callers; production
            # uploader always supplies the durable session identity.
            upload_session_id = uuid5(UUID(int=0), f"legacy-checkpoint:{sequence}")
        entries[sequence] = entry.model_copy(
            update={"state": "session_started", "upload_session_id": upload_session_id}
        )
        self._replace(
            entries=list(entries.values()),
            cancel_drain_sequence=(sequence if cancel_drain else self.state.cancel_drain_sequence),
        )

    def mark_committed(
        self,
        sequence: int,
        *,
        committed_at: datetime,
        artifact_id: UUID | None = None,
    ) -> None:
        _aware_or_none(committed_at)
        entries = self._entry_map()
        entry = entries.get(sequence)
        if entry is None:
            raise PipelineCheckpointWatcherError("checkpoint_not_discovered")
        if entry.state == "committed":
            if artifact_id is not None and entry.committed_artifact_id != artifact_id:
                raise PipelineCheckpointWatcherError("checkpoint_commit_replay_drift")
            return
        if artifact_id is None:
            artifact_id = uuid5(UUID(int=0), f"legacy-checkpoint-artifact:{sequence}")
        entries[sequence] = entry.model_copy(
            update={
                "state": "committed",
                "committed_artifact_id": artifact_id,
                "committed_at": committed_at,
            }
        )
        self._replace(entries=list(entries.values()))

    def session_admitted(
        self,
        *,
        sequence: int,
        now: datetime,
        policy: CheckpointPolicyV1,
    ) -> bool:
        _aware_or_none(now)
        entries = self._entry_map()
        entry = entries.get(sequence)
        if entry is None or entry.state != "discovered":
            return False
        committed = [item for item in entries.values() if item.state == "committed"]
        active = [item for item in entries.values() if item.state == "session_started"]
        if len(committed) + len(active) >= policy.max_committed_per_attempt:
            return False
        if active:
            return False
        if committed:
            last = max(item.committed_at for item in committed if item.committed_at is not None)
            if now < last + timedelta(seconds=policy.min_interval_seconds):
                return False
        return self.state.cancellation_observed_at is None

    def _entry_map(self) -> dict[int, CheckpointJournalEntryV1]:
        return {item.sequence: item for item in self.state.entries}

    def _replace(self, **updates: object) -> None:
        state = self.state.model_copy(update=updates)
        state = CheckpointWatcherJournalV1.model_validate(state.model_dump(mode="python"))
        self._persist(state)
        self.state = state

    def _persist(self, state: CheckpointWatcherJournalV1) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.parent.is_symlink():
            raise PipelineCheckpointWatcherError("checkpoint_journal_invalid")
        temporary = self.path.with_name(f".{self.path.name}.{uuid4().hex}.partial")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(temporary, flags, 0o600)
        try:
            value = _journal_bytes(state)
            with os.fdopen(fd, "wb", closefd=True) as stream:
                stream.write(value)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            directory_fd = os.open(self.path.parent, _DIRECTORY_FLAGS)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                temporary.unlink()
            raise


class PipelineCheckpointWatcher:
    """Five-second, directory-fd/no-follow scanner for completed directories."""

    def __init__(
        self,
        *,
        checkpoint_root: Path,
        identity: CheckpointClaimIdentityV1,
        policy: CheckpointPolicyV1,
        journal: CheckpointWatcherJournal,
    ) -> None:
        if not checkpoint_root.is_absolute():
            raise PipelineCheckpointWatcherError("checkpoint_root_not_absolute")
        self.checkpoint_root = checkpoint_root
        self.identity = identity
        self.policy = policy
        self.journal = journal
        self._last_scan_monotonic: float | None = None

    def scan_if_due(
        self,
        *,
        monotonic_now: float,
        force: bool = False,
    ) -> list[CompletedCheckpointDirectory]:
        if (
            not force
            and self._last_scan_monotonic is not None
            and monotonic_now - self._last_scan_monotonic < CHECKPOINT_SCAN_INTERVAL_SECONDS
        ):
            return []
        self._last_scan_monotonic = monotonic_now
        checkpoints = scan_completed_checkpoints(self.checkpoint_root)
        self.journal.observe(checkpoints)
        return checkpoints

    def prepare(self, checkpoint: CompletedCheckpointDirectory) -> PreparedCheckpoint:
        return checkpoint.prepare(identity=self.identity, policy=self.policy)


def scan_completed_checkpoints(checkpoint_root: Path) -> list[CompletedCheckpointDirectory]:
    """Return only locally committed numeric directories, ignoring ``.partial``."""

    try:
        root_fd = os.open(checkpoint_root, _DIRECTORY_FLAGS)
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise PipelineCheckpointWatcherError("checkpoint_root_invalid") from exc
    try:
        root_before = _directory_identity(root_fd)
        names = sorted(os.listdir(root_fd), key=lambda value: value.encode("utf-8"))
        checkpoints: list[CompletedCheckpointDirectory] = []
        for name in names:
            if name == ".partial":
                continue
            if len(name) != CHECKPOINT_SEQUENCE_WIDTH or not name.isascii() or not name.isdigit():
                raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
            checkpoints.append(
                _read_completed_directory(
                    parent_fd=root_fd,
                    name=name,
                    root=checkpoint_root / name,
                )
            )
        sequences = [item.sequence for item in checkpoints]
        if sequences != list(range(len(sequences))):
            raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
        if _directory_identity(root_fd) != root_before:
            raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
        return checkpoints
    finally:
        os.close(root_fd)


def _read_completed_directory(
    *, parent_fd: int, name: str, root: Path
) -> CompletedCheckpointDirectory:
    try:
        directory_fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    except OSError as exc:
        raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch") from exc
    try:
        directory_before = _directory_identity(directory_fd)
        entries = sorted(os.listdir(directory_fd), key=lambda value: value.encode("utf-8"))
        if entries != ["COMPLETE.json", "ledger.json", "payload"]:
            raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
        complete_bytes = _read_regular_at(
            directory_fd, "COMPLETE.json", max_bytes=MAX_COMPLETE_BYTES
        )
        ledger_bytes = _read_regular_at(
            directory_fd, "ledger.json", max_bytes=MAX_CHECKPOINT_BYTES
        )
        payload_fd = os.open("payload", _DIRECTORY_FLAGS, dir_fd=directory_fd)
        try:
            payload_files = _read_payload_tree(payload_fd, prefix="payload")
        finally:
            os.close(payload_fd)
        if _directory_identity(directory_fd) != directory_before:
            raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
    except OSError as exc:
        raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch") from exc
    finally:
        os.close(directory_fd)

    try:
        complete = BehaviorCheckpointCompleteV1.model_validate_json(complete_bytes)
        ledger = BehaviorRecoveryLedgerV1.model_validate_json(ledger_bytes)
    except ValueError as exc:
        raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch") from exc
    if complete_bytes != canonical_document(complete) or ledger_bytes != canonical_document(ledger):
        raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
    sequence = int(name)
    if complete.sequence != sequence or complete.ledger_sha256 != digest_bytes(ledger_bytes):
        raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
    payload_descriptors = [item.descriptor for item in payload_files]
    payload_values = [item.model_dump(mode="python") for item in payload_descriptors]
    if (
        [item.model_dump(mode="python") for item in complete.files] != payload_values
        or [item.model_dump(mode="python") for item in ledger.files] != payload_values
    ):
        raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
    if complete.payload_sha256 != canonical_digest(payload_descriptors):
        raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
    files = [
        LocalCheckpointFile(
            descriptor=CheckpointPayloadFileV1(
                relative_path="COMPLETE.json",
                size_bytes=len(complete_bytes),
                sha256=digest_bytes(complete_bytes),
            ),
            value=complete_bytes,
        ),
        LocalCheckpointFile(
            descriptor=CheckpointPayloadFileV1(
                relative_path="ledger.json",
                size_bytes=len(ledger_bytes),
                sha256=digest_bytes(ledger_bytes),
            ),
            value=ledger_bytes,
        ),
        *payload_files,
    ]
    files.sort(key=lambda item: item.descriptor.relative_path.encode("utf-8"))
    return CompletedCheckpointDirectory(root, sequence, ledger, complete, tuple(files))


def _read_payload_tree(directory_fd: int, *, prefix: str) -> list[LocalCheckpointFile]:
    directory_before = _directory_identity(directory_fd)
    result: list[LocalCheckpointFile] = []
    for name in sorted(os.listdir(directory_fd), key=lambda value: value.encode("utf-8")):
        if not name or name in {".", ".."} or "/" in name or "\\" in name:
            raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
        try:
            name.encode("utf-8", errors="strict")
        except UnicodeError as exc:
            raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch") from exc
        if unicodedata.normalize("NFC", name) != name:
            raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
        facts = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        relative_path = f"{prefix}/{name}"
        if stat.S_ISDIR(facts.st_mode):
            child_fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=directory_fd)
            try:
                children = _read_payload_tree(child_fd, prefix=relative_path)
            finally:
                os.close(child_fd)
            if not children:
                raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
            result.extend(children)
        elif stat.S_ISREG(facts.st_mode):
            value = _read_regular_at(directory_fd, name, max_bytes=MAX_CHECKPOINT_BYTES)
            result.append(
                LocalCheckpointFile(
                    descriptor=CheckpointPayloadFileV1(
                        relative_path=relative_path,
                        size_bytes=len(value),
                        sha256=digest_bytes(value),
                    ),
                    value=value,
                )
            )
        else:
            raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
    if _directory_identity(directory_fd) != directory_before:
        raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
    return result


def _read_regular_at(parent_fd: int, name: str, *, max_bytes: int) -> bytes:
    before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > max_bytes:
        raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
    fd = os.open(name, _FILE_FLAGS, dir_fd=parent_fd)
    try:
        opened = os.fstat(fd)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
        ):
            raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(fd, min(_READ_CHUNK_BYTES, max_bytes + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
            chunks.append(chunk)
        after = os.fstat(fd)
        if (
            (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
            or total != after.st_size
        ):
            raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
        return b"".join(chunks)
    finally:
        os.close(fd)


def _journal_bytes(state: CheckpointWatcherJournalV1) -> bytes:
    return canonical_document(state.model_dump(mode="json", exclude_none=False))


def _directory_identity(fd: int) -> tuple[int, int, int, int]:
    facts = os.fstat(fd)
    if not stat.S_ISDIR(facts.st_mode):
        raise PipelineCheckpointWatcherError("checkpoint_contract_mismatch")
    return facts.st_dev, facts.st_ino, facts.st_mtime_ns, facts.st_ctime_ns


__all__ = [
    "CHECKPOINT_SCAN_INTERVAL_SECONDS",
    "CheckpointClaimIdentityV1",
    "CheckpointJournalEntryV1",
    "CheckpointJournalSignatureV1",
    "CheckpointWatcherJournal",
    "CheckpointWatcherJournalV1",
    "CompletedCheckpointDirectory",
    "LocalCheckpointFile",
    "PipelineCheckpointWatcher",
    "PipelineCheckpointWatcherError",
    "PreparedCheckpoint",
    "scan_completed_checkpoints",
]
