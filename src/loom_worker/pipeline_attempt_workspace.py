"""Crash-safe, attempt-local output authority for Pipeline containers.

The workspace deliberately owns only the local commit protocol.  Object-store
upload, multipart state, and artifact lifecycle are worker/platform concerns
implemented outside this module.
"""

from __future__ import annotations

import contextlib
import json
import math
import os
import stat
import time
import unicodedata
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path, PurePosixPath
from typing import Annotated, Any, BinaryIO, Literal
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from loom.integrations.behavior.canonical_json import load_canonical_document
from loom.pipeline.artifact_validators import validate_official_artifact_document
from loom.pipeline.keys import MAX_SAFE_INTEGER, canonical_digest, canonical_document, digest_bytes
from loom.pipeline.spec import (
    ArtifactType,
    BindingName,
    ContainerNodeV1,
    Digest,
    PipelineModel,
    PlatformFanoutIndexV1,
)
from loom.pipeline.state import StageResultV1

MAX_COMPLETE_BYTES = 16_777_216
MAX_CHECKPOINT_BYTES = 16_777_216
CHECKPOINT_SEQUENCE_WIDTH = 12


class AttemptWorkspaceError(ValueError):
    """The local output tree cannot be committed without weakening the contract."""


class AttemptWorkspaceCrashError(RuntimeError):
    """Default exception useful to deterministic crash-injection tests."""


CrashInjector = Callable[[str], None]
CancellationCheck = Callable[[], bool]
Sleep = Callable[[float], None]
Monotonic = Callable[[], float]


class AttemptCompleteFileV1(PipelineModel):
    relative_path: str
    sha256: Digest
    size_bytes: Annotated[int, Field(strict=True, ge=0, le=MAX_SAFE_INTEGER)]

    @field_validator("relative_path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return _safe_relative_path(value, required_prefix="payload")


class AttemptCompleteOutputV1(PipelineModel):
    name: BindingName
    artifact_json_sha256: Digest
    files: list[AttemptCompleteFileV1]

    @model_validator(mode="after")
    def validate_files(self) -> AttemptCompleteOutputV1:
        _require_bytewise_unique(
            [item.relative_path for item in self.files], "COMPLETE payload files"
        )
        return self


class AttemptCompleteV1(PipelineModel):
    schema_version: Literal["loom.attempt-complete.v1"]
    idempotency_key: str
    stage_result_sha256: Digest
    outputs: list[AttemptCompleteOutputV1]

    @field_validator("idempotency_key")
    @classmethod
    def validate_key(cls, value: str) -> str:
        return _exact_nfc(value, "idempotency_key", max_bytes=512)

    @model_validator(mode="after")
    def validate_outputs(self) -> AttemptCompleteV1:
        _require_bytewise_unique([item.name for item in self.outputs], "COMPLETE outputs")
        return self


class LegacyBehaviorAttemptCompleteV1(PipelineModel):
    """Read-only compatibility for attempts committed before the generic marker."""

    schema_version: Literal["behavior.attempt-complete.v1"]
    idempotency_key: str
    stage_result_sha256: Digest
    outputs: list[AttemptCompleteOutputV1]

    @field_validator("idempotency_key")
    @classmethod
    def validate_key(cls, value: str) -> str:
        return _exact_nfc(value, "idempotency_key", max_bytes=512)

    @model_validator(mode="after")
    def validate_outputs(self) -> LegacyBehaviorAttemptCompleteV1:
        _require_bytewise_unique([item.name for item in self.outputs], "COMPLETE outputs")
        return self


def parse_attempt_complete(
    value: object,
) -> AttemptCompleteV1 | LegacyBehaviorAttemptCompleteV1:
    if not isinstance(value, dict):
        raise ValueError("attempt COMPLETE marker must be an object")
    if value.get("schema_version") == "loom.attempt-complete.v1":
        return AttemptCompleteV1.model_validate(value)
    return LegacyBehaviorAttemptCompleteV1.model_validate(value)


class RecoveryTerminalCandidateV1(PipelineModel):
    candidate_id: str
    terminal_state: Literal["rejected", "inconclusive", "failed"]
    output_sha256: None
    q_score_delta: float | None

    @field_validator("candidate_id")
    @classmethod
    def validate_candidate_id(cls, value: str) -> str:
        return _exact_nfc(value, "candidate_id", max_bytes=512)

    @field_validator("q_score_delta")
    @classmethod
    def finite_delta(cls, value: float | None) -> float | None:
        if value is not None and not math.isfinite(value):
            raise ValueError("q_score_delta must be finite or null")
        return value


class CheckpointFileV1(PipelineModel):
    """A future-compatible input shape; v1 MOP checkpoints require an empty iterable."""

    relative_path: str
    sha256: Digest
    size_bytes: Annotated[int, Field(strict=True, ge=0, le=MAX_SAFE_INTEGER)]

    @field_validator("relative_path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return _safe_relative_path(value, required_prefix="payload")


class BehaviorRecoveryLedgerV1(PipelineModel):
    schema_version: Literal["behavior.recovery-ledger.v1"]
    stream: Literal["mop"]
    input_digest: Digest
    execution_spec_digest: Digest
    resume_compatibility_key: Digest
    sample_id: UUID
    terminal_candidates: Annotated[list[RecoveryTerminalCandidateV1], Field(max_length=20)]
    success_episode_ids: list[UUID]
    files: list[CheckpointFileV1]

    @model_validator(mode="after")
    def validate_checkpoint_ledger(self) -> BehaviorRecoveryLedgerV1:
        _require_bytewise_unique(
            [item.candidate_id for item in self.terminal_candidates],
            "terminal checkpoint candidates",
        )
        if self.success_episode_ids or self.files:
            raise ValueError("MOP checkpoint ledger cannot contain successes or files")
        return self


class BehaviorCheckpointCompleteV1(PipelineModel):
    schema_version: Literal["behavior.checkpoint-complete.v1"]
    sequence: Annotated[int, Field(strict=True, ge=0, le=MAX_SAFE_INTEGER)]
    ledger_sha256: Digest
    payload_sha256: Digest
    files: list[CheckpointFileV1]

    @model_validator(mode="after")
    def empty_payload(self) -> BehaviorCheckpointCompleteV1:
        if self.files:
            raise ValueError("v1 MOP checkpoint payload inventory must be empty")
        return self


@dataclass(frozen=True)
class CommittedAttempt:
    root: Path
    stage_result: StageResultV1
    complete: AttemptCompleteV1 | LegacyBehaviorAttemptCompleteV1


@dataclass(frozen=True)
class CommittedCheckpoint:
    root: Path
    sequence: int
    ledger: BehaviorRecoveryLedgerV1
    complete: BehaviorCheckpointCompleteV1


class AttemptWorkspace:
    """The sole production writer below one Pipeline attempt's ``/outputs`` root."""

    def __init__(
        self,
        output_dir: Path,
        attempt_id: UUID | str,
        idempotency_key: str,
        *,
        node: ContainerNodeV1 | None = None,
        output_declarations: Mapping[str, str] | None = None,
        final_output_bytes_limit: int,
        checkpoint_bytes_limit: int = 0,
        checkpoint_min_interval_seconds: float = 5.0,
        checkpoint_max_committed: int = 20,
        resolved_input_bindings_digest: str | None = None,
        execution_spec_digest: str | None = None,
        recipe_digest: str | None = None,
        image_digest: str | None = None,
        crash_injector: CrashInjector | None = None,
        monotonic: Monotonic = time.monotonic,
        sleep: Sleep = time.sleep,
        cancelled: CancellationCheck | None = None,
    ) -> None:
        self._output_dir = Path(output_dir)
        self.attempt_id = _attempt_component(attempt_id)
        self.idempotency_key = _exact_nfc(idempotency_key, "idempotency_key", max_bytes=512)
        self.node = node
        self.final_output_bytes_limit = _budget(
            final_output_bytes_limit, "final output", allow_zero=True
        )
        self.checkpoint_bytes_limit = _budget(checkpoint_bytes_limit, "checkpoint", allow_zero=True)
        if checkpoint_min_interval_seconds != 5 or not math.isfinite(
            checkpoint_min_interval_seconds
        ):
            raise AttemptWorkspaceError("BEHAVIOR checkpoint interval must be exactly 5 seconds")
        if checkpoint_max_committed != 20:
            raise AttemptWorkspaceError("BEHAVIOR checkpoint maximum must be exactly 20")
        self.checkpoint_min_interval_seconds = checkpoint_min_interval_seconds
        self.checkpoint_max_committed = checkpoint_max_committed
        self.resolved_input_bindings_digest = resolved_input_bindings_digest
        self.execution_spec_digest = execution_spec_digest
        self.recipe_digest = recipe_digest
        self.image_digest = image_digest
        self._crash_injector = crash_injector
        self._monotonic = monotonic
        self._sleep = sleep
        self._cancelled = cancelled or (lambda: False)
        self._last_checkpoint_monotonic: float | None = None

        _require_existing_directory(self._output_dir, "output-dir")
        self._output_dir = self._output_dir.resolve(strict=True)
        declarations = {
            _binding_name(name): _artifact_type(artifact_type)
            for name, artifact_type in (output_declarations or {}).items()
        }
        if node is not None:
            declared_from_node = {
                output.name: output.artifact_type
                for output in node.outputs
                if output.producer == "container"
            }
            if declarations and declarations != declared_from_node:
                raise AttemptWorkspaceError("output declarations disagree with the Pipeline node")
            declarations = declared_from_node
        self._declarations = declarations
        self._partial_root = self._output_dir / ".partial" / self.attempt_id
        self._partial_artifacts = self._partial_root / "artifacts"
        self._partial_preexisting = self._partial_root.exists()
        self._committed_artifacts = self._output_dir / "artifacts"
        self._checkpoint_root = self._output_dir / ".loom" / "checkpoints"

    @property
    def partial_root(self) -> Path:
        return self._partial_root

    def artifact_root(self, output_name: str) -> Path:
        """Create and return one admitted partial output root."""

        name = self._admit_literal_name(output_name)
        root = self._partial_artifacts / name
        _mkdir_chain_no_symlink(root / "payload", stop=self._output_dir)
        return root

    @contextlib.contextmanager
    def open_payload(self, output_name: str, relative_path: str) -> Iterator[BinaryIO]:
        """Open a new regular payload file without following links or overwriting bytes."""

        root = self.artifact_root(output_name)
        relative = _safe_relative_path(relative_path, required_prefix="payload")
        target = root.joinpath(*PurePosixPath(relative).parts)
        _mkdir_chain_no_symlink(target.parent, stop=root)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(target, flags, 0o600)
        try:
            with os.fdopen(fd, "wb", closefd=True) as stream:
                yield stream
                stream.flush()
                os.fsync(stream.fileno())
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                target.unlink()
            raise
        if self._partial_size() > self.final_output_bytes_limit:
            raise AttemptWorkspaceError("final output byte budget exceeded")

    def write_payload_bytes(self, output_name: str, relative_path: str, value: bytes) -> Path:
        with self.open_payload(output_name, relative_path) as stream:
            stream.write(value)
        return self.artifact_root(output_name).joinpath(*PurePosixPath(relative_path).parts)

    def write_artifact_json(self, output_name: str, value: object) -> Path:
        name = self._admit_literal_name(output_name)
        encoded = canonical_document(value)
        root = self.artifact_root(name)
        target = root / "artifact.json"
        _write_new_file(target, encoded)
        if self._partial_size() > self.final_output_bytes_limit:
            raise AttemptWorkspaceError("final output byte budget exceeded")
        return target

    def commit(
        self,
        stage_result: StageResultV1 | Mapping[str, Any],
        *,
        fanout_index: PlatformFanoutIndexV1 | Mapping[str, Any] | None = None,
    ) -> CommittedAttempt:
        """Validate, fsync, and atomically publish the terminal attempt tree."""

        result = (
            stage_result
            if isinstance(stage_result, StageResultV1)
            else StageResultV1.model_validate_json(canonical_document(stage_result))
        )
        self._validate_output_root()
        prior = self._read_prior_attempt()
        if prior is not None:
            if prior.complete.idempotency_key != self.idempotency_key:
                raise AttemptWorkspaceError("committed attempt uses a different idempotency key")
            if prior.stage_result != result:
                raise AttemptWorkspaceError("conflicting StageResult replay")
            return prior
        if self._partial_preexisting:
            raise AttemptWorkspaceError("incomplete partial attempt residue exists")
        if self._output_dir.joinpath("COMPLETE.json").exists():
            raise AttemptWorkspaceError("invalid or partial COMPLETE exists")
        if (
            self._committed_artifacts.exists()
            or self._output_dir.joinpath("stage_result.json").exists()
        ):
            raise AttemptWorkspaceError("incomplete committed output residue exists")
        if not self._partial_artifacts.exists():
            _mkdir_chain_no_symlink(self._partial_artifacts, stop=self._output_dir)
        partial_entries = sorted(item.name for item in self._partial_root.iterdir())
        if partial_entries != ["artifacts"]:
            raise AttemptWorkspaceError("partial Attempt contains a write outside artifacts")

        parsed_index = self._parse_fanout_index(fanout_index)
        inventory = self._terminal_inventory(result, parsed_index, self._partial_artifacts)
        if self._tree_size(self._partial_artifacts) > self.final_output_bytes_limit:
            raise AttemptWorkspaceError("final output byte budget exceeded")
        _fsync_tree(self._partial_artifacts)
        self._crash("terminal_before_artifacts_rename")
        os.rename(self._partial_artifacts, self._committed_artifacts)
        _fsync_directory(self._output_dir)
        self._crash("terminal_after_artifacts_rename")

        stage_bytes = canonical_document(result)
        _atomic_write_new(self._output_dir / "stage_result.json", stage_bytes)
        self._crash("terminal_after_stage_result")
        self._crash("terminal_during_complete_inventory")
        complete = AttemptCompleteV1(
            schema_version="loom.attempt-complete.v1",
            idempotency_key=self.idempotency_key,
            stage_result_sha256=digest_bytes(stage_bytes),
            outputs=inventory,
        )
        complete_bytes = canonical_document(complete)
        if len(complete_bytes) > MAX_COMPLETE_BYTES:
            raise AttemptWorkspaceError("COMPLETE document exceeds 16 MiB")
        _atomic_write_new(self._output_dir / "COMPLETE.json", complete_bytes)
        self._crash("terminal_after_complete")
        return self._validate_committed_attempt(expected_key=self.idempotency_key)

    commit_terminal = commit
    commit_outputs = commit

    def validate_committed(self) -> CommittedAttempt:
        return self._validate_committed_attempt(expected_key=self.idempotency_key)

    def commit_checkpoint(
        self,
        sequence: int,
        ledger: BehaviorRecoveryLedgerV1,
        files: Iterable[CheckpointFileV1],
    ) -> CommittedCheckpoint:
        """Commit the exact empty-payload MOP recovery ledger for one candidate."""

        if self.checkpoint_bytes_limit == 0:
            raise AttemptWorkspaceError("checkpoints are disabled for this Attempt")
        if isinstance(sequence, bool) or not 0 <= sequence <= MAX_SAFE_INTEGER:
            raise AttemptWorkspaceError("checkpoint sequence is outside uint64/JCS range")
        if list(files):
            raise AttemptWorkspaceError("v1 MOP checkpoints cannot contain payload files")
        ledger = BehaviorRecoveryLedgerV1.model_validate(ledger)
        self._validate_ledger_binding(ledger)
        committed = self._valid_checkpoints(fail_on_corrupt=True)
        prior = next((item for item in committed if item.sequence == sequence), None)
        ledger_bytes = canonical_document(ledger)
        if prior is not None:
            if canonical_document(prior.ledger) != ledger_bytes:
                raise AttemptWorkspaceError("conflicting checkpoint replay")
            return prior
        expected_sequence = 0 if not committed else committed[-1].sequence + 1
        if sequence != expected_sequence:
            raise AttemptWorkspaceError("checkpoint sequence has a gap or regression")
        if len(ledger.terminal_candidates) != sequence + 1:
            raise AttemptWorkspaceError("checkpoint must add exactly one terminal candidate")
        if committed and (
            ledger.terminal_candidates[:-1] != committed[-1].ledger.terminal_candidates
            or ledger.sample_id != committed[-1].ledger.sample_id
        ):
            raise AttemptWorkspaceError("checkpoint candidate history is not append-only")
        if len(committed) >= self.checkpoint_max_committed:
            raise AttemptWorkspaceError("checkpoint count limit exceeded")
        if len(ledger_bytes) > min(self.checkpoint_bytes_limit, MAX_CHECKPOINT_BYTES):
            raise AttemptWorkspaceError("checkpoint byte budget exceeded")
        self._wait_for_checkpoint_interval()

        sequence_name = f"{sequence:0{CHECKPOINT_SEQUENCE_WIDTH}d}"
        partial = self._checkpoint_root / ".partial" / sequence_name
        final = self._checkpoint_root / sequence_name
        if partial.exists() or final.exists():
            raise AttemptWorkspaceError("checkpoint residue exists")
        payload = partial / "payload"
        _mkdir_chain_no_symlink(payload, stop=self._output_dir)
        _fsync_directory(payload)
        self._crash("checkpoint_before_payload_rename")
        _mkdir_chain_no_symlink(final.parent, stop=self._output_dir)
        os.rename(partial, final)
        _fsync_directory(final.parent)
        self._crash("checkpoint_after_payload_rename")
        _atomic_write_new(final / "ledger.json", ledger_bytes)
        self._crash("checkpoint_after_ledger")
        complete = BehaviorCheckpointCompleteV1(
            schema_version="behavior.checkpoint-complete.v1",
            sequence=sequence,
            ledger_sha256=digest_bytes(ledger_bytes),
            payload_sha256=canonical_digest([]),
            files=[],
        )
        _atomic_write_new(final / "COMPLETE.json", canonical_document(complete))
        self._crash("checkpoint_after_complete")
        result = self._validate_checkpoint(final)
        self._last_checkpoint_monotonic = self._monotonic()
        return result

    def latest_committed_checkpoint(self) -> CommittedCheckpoint | None:
        valid = self._valid_checkpoints(fail_on_corrupt=False)
        return valid[-1] if valid else None

    def _admit_literal_name(self, output_name: str) -> str:
        name = _binding_name(output_name)
        if name in self._declarations:
            if self.node is not None and self.node.fanout_commit is not None:
                if name == self.node.fanout_commit.item_binding_name:
                    raise AttemptWorkspaceError("fanout template name is not an actual output")
            return name
        # Dynamic fanout names cannot be admitted until commit sees the signed index.
        if self.node is not None and self.node.fanout_commit is not None:
            return name
        raise AttemptWorkspaceError("output name is not a declared container output")

    def _parse_fanout_index(
        self, value: PlatformFanoutIndexV1 | Mapping[str, Any] | None
    ) -> PlatformFanoutIndexV1 | None:
        if value is None:
            return None
        return (
            value
            if isinstance(value, PlatformFanoutIndexV1)
            else PlatformFanoutIndexV1.model_validate(value)
        )

    def _terminal_inventory(
        self,
        result: StageResultV1,
        fanout_index: PlatformFanoutIndexV1 | None,
        artifacts_root: Path,
    ) -> list[AttemptCompleteOutputV1]:
        roots = _regular_directory_names(artifacts_root)
        result_types = {item.name: item.artifact_type for item in result.outputs}
        if set(roots) != set(result_types):
            raise AttemptWorkspaceError(
                "StageResult outputs do not equal the artifact directory set"
            )
        if self.node is not None:
            template_name = (
                self.node.fanout_commit.item_binding_name
                if self.node.fanout_commit is not None
                else None
            )
            required = {
                item.name
                for item in self.node.outputs
                if item.producer == "container" and item.required and item.name != template_name
            }
            if not required.issubset(roots):
                raise AttemptWorkspaceError("required container output is missing")
        allowed = dict(self._declarations)
        if self.node is not None and self.node.fanout_commit is not None:
            commit = self.node.fanout_commit
            if fanout_index is None:
                index_root = artifacts_root / commit.index_output_name / "artifact.json"
                raw = load_canonical_document(index_root, max_bytes=MAX_COMPLETE_BYTES)
                fanout_index = PlatformFanoutIndexV1.model_validate(raw)
            indexed = {item.output_name for item in fanout_index.items}
            template = next(
                item for item in self.node.outputs if item.name == commit.item_binding_name
            )
            if commit.item_binding_name in roots:
                raise AttemptWorkspaceError("fanout template name cannot be committed")
            if indexed & (set(self._declarations) - {commit.item_binding_name}):
                raise AttemptWorkspaceError("fanout dynamic output collides with a literal output")
            for name in indexed:
                allowed[name] = template.artifact_type
            actual_dynamic = set(roots) - (set(self._declarations) - {commit.item_binding_name})
            if actual_dynamic != indexed:
                raise AttemptWorkspaceError(
                    "fanout index does not equal dynamic output directories"
                )
        elif fanout_index is not None:
            raise AttemptWorkspaceError("fanout index is invalid without PlatformFanoutCommit")
        if any(allowed.get(name) != artifact_type for name, artifact_type in result_types.items()):
            raise AttemptWorkspaceError("StageResult output type is undeclared or mismatched")

        records: list[AttemptCompleteOutputV1] = []
        for name in roots:
            root = artifacts_root / name
            artifact_path = root / "artifact.json"
            artifact_bytes = _read_regular_file(artifact_path, max_bytes=67_108_864)
            raw = _load_exact_canonical_bytes(artifact_bytes)
            artifact: Any | None = None
            if result_types[name] == "loom.platform-fanout-index.v1":
                PlatformFanoutIndexV1.model_validate(raw)
            else:
                artifact = validate_official_artifact_document(result_types[name], raw)
                if getattr(artifact, "schema_version", None) != result_types[name]:
                    raise AttemptWorkspaceError("artifact.json schema does not match StageResult")
            files = _payload_inventory(root)
            if artifact is not None:
                raw_files = getattr(artifact, "files", None)
                if not isinstance(raw_files, list):
                    raise AttemptWorkspaceError("artifact.json has no closed file inventory")
                declared_payload = [
                    (item.relative_path, item.sha256, item.size_bytes) for item in raw_files
                ]
                actual_payload = [
                    (item.relative_path, item.sha256, item.size_bytes) for item in files
                ]
                if declared_payload != actual_payload:
                    raise AttemptWorkspaceError(
                        "artifact.json file descriptors do not match payload bytes"
                    )
            output_limit = self._output_limit(name)
            if output_limit is not None:
                output_size = len(artifact_bytes) + sum(item.size_bytes for item in files)
                if output_size > output_limit:
                    raise AttemptWorkspaceError("declared output byte budget exceeded")
            records.append(
                AttemptCompleteOutputV1(
                    name=name,
                    artifact_json_sha256=digest_bytes(artifact_bytes),
                    files=files,
                )
            )
        return records

    def _output_limit(self, name: str) -> int | None:
        if self.node is None:
            return None
        direct = next((item for item in self.node.outputs if item.name == name), None)
        if direct is not None:
            return direct.max_bytes
        if self.node.fanout_commit is None:
            return None
        template = next(
            item
            for item in self.node.outputs
            if item.name == self.node.fanout_commit.item_binding_name
        )
        return template.max_bytes

    def _read_prior_attempt(self) -> CommittedAttempt | None:
        complete_path = self._output_dir / "COMPLETE.json"
        if not complete_path.exists():
            return None
        return self._validate_committed_attempt(expected_key=None)

    def _validate_output_root(self) -> None:
        allowed = {".loom", ".partial", "artifacts", "stage_result.json", "COMPLETE.json"}
        for entry in self._output_dir.iterdir():
            if entry.name not in allowed:
                raise AttemptWorkspaceError("output-dir contains a write outside the workspace")
            if entry.name in {".loom", ".partial", "artifacts"}:
                details = entry.lstat()
                if not stat.S_ISDIR(details.st_mode) or stat.S_ISLNK(details.st_mode):
                    raise AttemptWorkspaceError("workspace directory is replaced by a link or file")
        loom_root = self._output_dir / ".loom"
        if loom_root.exists():
            entries = {entry.name for entry in loom_root.iterdir()}
            if entries - {"checkpoints"}:
                raise AttemptWorkspaceError("reserved .loom workspace contains an adapter write")

    def _validate_committed_attempt(self, *, expected_key: str | None) -> CommittedAttempt:
        complete_path = self._output_dir / "COMPLETE.json"
        raw = load_canonical_document(complete_path, max_bytes=MAX_COMPLETE_BYTES)
        complete = parse_attempt_complete(raw)
        complete_bytes = _read_regular_file(complete_path, max_bytes=MAX_COMPLETE_BYTES)
        if complete_bytes != canonical_document(complete):
            raise AttemptWorkspaceError("COMPLETE bytes are not canonical")
        if expected_key is not None and complete.idempotency_key != expected_key:
            raise AttemptWorkspaceError("committed attempt uses a different idempotency key")
        result_path = self._output_dir / "stage_result.json"
        result_bytes = _read_regular_file(result_path, max_bytes=MAX_COMPLETE_BYTES)
        _load_exact_canonical_bytes(result_bytes)
        result = StageResultV1.model_validate_json(result_bytes)
        if digest_bytes(result_bytes) != complete.stage_result_sha256:
            raise AttemptWorkspaceError("StageResult digest mismatch")
        current = self._terminal_inventory(result, None, self._committed_artifacts)
        if current != complete.outputs:
            raise AttemptWorkspaceError("committed artifact inventory mismatch")
        return CommittedAttempt(self._output_dir, result, complete)

    def _validate_ledger_binding(self, ledger: BehaviorRecoveryLedgerV1) -> None:
        expected = (
            ("input_digest", self.resolved_input_bindings_digest),
            ("execution_spec_digest", self.execution_spec_digest),
        )
        for field, value in expected:
            if value is not None and getattr(ledger, field) != value:
                raise AttemptWorkspaceError(f"checkpoint {field} does not match the Attempt claim")
        if all(
            value is not None
            for value in (
                self.execution_spec_digest,
                self.image_digest,
                self.resolved_input_bindings_digest,
                self.recipe_digest,
            )
        ):
            resume = canonical_digest(
                {
                    "checkpoint_schema": "loom.execution-checkpoint.v1",
                    "execution_spec_digest": self.execution_spec_digest,
                    "image_digest": self.image_digest,
                    "resolved_input_bindings_digest": self.resolved_input_bindings_digest,
                    "recipe_digest": self.recipe_digest,
                },
                persisted=False,
            )
            if ledger.resume_compatibility_key != resume:
                raise AttemptWorkspaceError("checkpoint resume compatibility key drift")

    def _valid_checkpoints(self, *, fail_on_corrupt: bool) -> list[CommittedCheckpoint]:
        if not self._checkpoint_root.exists():
            return []
        _mkdir_chain_no_symlink(self._checkpoint_root, stop=self._output_dir)
        _require_existing_directory(self._checkpoint_root, "checkpoint root")
        valid: list[CommittedCheckpoint] = []
        for path in sorted(self._checkpoint_root.iterdir(), key=lambda item: item.name):
            if path.name == ".partial":
                continue
            if not path.name.isdigit() or len(path.name) != CHECKPOINT_SEQUENCE_WIDTH:
                if fail_on_corrupt:
                    raise AttemptWorkspaceError("unexpected checkpoint directory")
                continue
            try:
                valid.append(self._validate_checkpoint(path))
            except (OSError, ValueError) as exc:
                if fail_on_corrupt:
                    raise AttemptWorkspaceError("corrupt committed checkpoint") from exc
        sequences = [item.sequence for item in valid]
        if sequences != list(range(len(sequences))):
            if fail_on_corrupt:
                raise AttemptWorkspaceError("committed checkpoint sequence is not contiguous")
            return []
        for previous, current in pairwise(valid):
            if (
                current.ledger.terminal_candidates[:-1] != previous.ledger.terminal_candidates
                or current.ledger.sample_id != previous.ledger.sample_id
            ):
                if fail_on_corrupt:
                    raise AttemptWorkspaceError("checkpoint candidate history is corrupt")
                return []
        return valid

    def _validate_checkpoint(self, root: Path) -> CommittedCheckpoint:
        _require_existing_directory(root, "checkpoint")
        entries = sorted(item.name for item in root.iterdir())
        if entries != ["COMPLETE.json", "ledger.json", "payload"]:
            raise AttemptWorkspaceError("checkpoint directory inventory mismatch")
        _require_existing_directory(root / "payload", "checkpoint payload")
        if any((root / "payload").iterdir()):
            raise AttemptWorkspaceError("v1 checkpoint payload must be empty")
        ledger_bytes = _read_regular_file(root / "ledger.json", max_bytes=MAX_CHECKPOINT_BYTES)
        _load_exact_canonical_bytes(ledger_bytes)
        ledger = BehaviorRecoveryLedgerV1.model_validate_json(ledger_bytes)
        complete_bytes = _read_regular_file(root / "COMPLETE.json", max_bytes=MAX_COMPLETE_BYTES)
        _load_exact_canonical_bytes(complete_bytes)
        complete = BehaviorCheckpointCompleteV1.model_validate_json(complete_bytes)
        if root.name != f"{complete.sequence:0{CHECKPOINT_SEQUENCE_WIDTH}d}":
            raise AttemptWorkspaceError("checkpoint directory and sequence disagree")
        if len(ledger.terminal_candidates) != complete.sequence + 1:
            raise AttemptWorkspaceError("checkpoint candidate inventory is coalesced or incomplete")
        if complete.ledger_sha256 != digest_bytes(ledger_bytes):
            raise AttemptWorkspaceError("checkpoint ledger digest mismatch")
        if complete.payload_sha256 != canonical_digest([]):
            raise AttemptWorkspaceError("checkpoint empty payload digest mismatch")
        self._validate_ledger_binding(ledger)
        return CommittedCheckpoint(root, complete.sequence, ledger, complete)

    def _wait_for_checkpoint_interval(self) -> None:
        if self._last_checkpoint_monotonic is None:
            return
        deadline = self._last_checkpoint_monotonic + self.checkpoint_min_interval_seconds
        while True:
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                return
            if self._cancelled():
                raise AttemptWorkspaceError("checkpoint interval wait was cancelled")
            self._sleep(min(remaining, 0.1))

    def _partial_size(self) -> int:
        return self._tree_size(self._partial_artifacts)

    @staticmethod
    def _tree_size(root: Path) -> int:
        if not root.exists():
            return 0
        return sum(item.size_bytes for item in _regular_tree_inventory(root))

    def _crash(self, boundary: str) -> None:
        if self._crash_injector is not None:
            self._crash_injector(boundary)


@dataclass(frozen=True)
class _TreeFile:
    relative_path: str
    sha256: str
    size_bytes: int


def _attempt_component(value: UUID | str) -> str:
    text = str(value)
    if not text or text in {".", ".."} or any(char in text for char in "/\\\x00"):
        raise AttemptWorkspaceError("attempt-id is not a safe path component")
    return _exact_nfc(text, "attempt-id", max_bytes=512)


def _budget(value: int, label: str, *, allow_zero: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise AttemptWorkspaceError(f"{label} byte budget must be an integer")
    minimum = 0 if allow_zero else 1
    if not minimum <= value <= MAX_SAFE_INTEGER:
        raise AttemptWorkspaceError(f"{label} byte budget is out of range")
    return value


def _exact_nfc(value: str, label: str, *, max_bytes: int) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be text")
    value.encode("utf-8", errors="strict")
    if value != unicodedata.normalize("NFC", value):
        raise ValueError(f"{label} must already be NFC")
    if not value or len(value.encode("utf-8")) > max_bytes:
        raise ValueError(f"{label} must be 1..{max_bytes} UTF-8 bytes")
    return value


def _binding_name(value: str) -> str:
    from pydantic import TypeAdapter

    return TypeAdapter(BindingName).validate_python(value, strict=True)


def _artifact_type(value: str) -> str:
    from pydantic import TypeAdapter

    return TypeAdapter(ArtifactType).validate_python(value, strict=True)


def _safe_relative_path(value: str, *, required_prefix: str) -> str:
    value = _exact_nfc(value, "relative_path", max_bytes=4096)
    if value.startswith("/") or "\\" in value or "\x00" in value:
        raise ValueError("path must be relative POSIX")
    path = PurePosixPath(value)
    if any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("path contains an invalid component")
    if len(path.parts) < 2 or path.parts[0] != required_prefix:
        raise ValueError(f"path must be below {required_prefix}/")
    return value


def _require_bytewise_unique(values: list[str], label: str) -> None:
    if values != sorted(values, key=lambda value: value.encode("utf-8")):
        raise ValueError(f"{label} must be bytewise sorted")
    if len(values) != len(set(values)):
        raise ValueError(f"{label} must be unique")


def _require_existing_directory(path: Path, label: str) -> None:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError as exc:
        raise AttemptWorkspaceError(f"{label} must exist") from exc
    if not stat.S_ISDIR(mode) or stat.S_ISLNK(mode):
        raise AttemptWorkspaceError(f"{label} must be a real directory")


def _mkdir_chain_no_symlink(path: Path, *, stop: Path) -> None:
    stop = stop.resolve(strict=True)
    try:
        relative = path.relative_to(stop)
    except ValueError as exc:
        raise AttemptWorkspaceError("workspace path escapes output-dir") from exc
    current = stop
    for part in relative.parts:
        current = current / part
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError:
            current.mkdir(mode=0o700)
            mode = current.lstat().st_mode
        if not stat.S_ISDIR(mode) or stat.S_ISLNK(mode):
            raise AttemptWorkspaceError("workspace path crosses a non-directory or symlink")


def _write_new_file(path: Path, value: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags, 0o600)
    with os.fdopen(fd, "wb", closefd=True) as stream:
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())
    _fsync_directory(path.parent)


def _atomic_write_new(path: Path, value: bytes) -> None:
    temporary = path.with_name(f".{path.name}.partial")
    if path.exists() or temporary.exists():
        raise AttemptWorkspaceError(f"refusing to overwrite {path.name}")
    _write_new_file(temporary, value)
    os.rename(temporary, path)
    _fsync_directory(path.parent)


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_tree(root: Path) -> None:
    for path in sorted(root.rglob("*"), reverse=True):
        mode = path.lstat().st_mode
        if stat.S_ISLNK(mode) or not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
            raise AttemptWorkspaceError("output tree contains a forbidden filesystem object")
        if stat.S_ISREG(mode):
            with path.open("rb") as stream:
                os.fsync(stream.fileno())
        else:
            _fsync_directory(path)
    _fsync_directory(root)


def _read_regular_file(path: Path, *, max_bytes: int) -> bytes:
    details = path.lstat()
    if not stat.S_ISREG(details.st_mode) or stat.S_ISLNK(details.st_mode) or details.st_nlink != 1:
        raise AttemptWorkspaceError("inventory contains a non-regular or linked file")
    if details.st_size > max_bytes:
        raise AttemptWorkspaceError("file exceeds its read limit")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags)
    try:
        opened = os.fstat(fd)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or (opened.st_dev, opened.st_ino) != (details.st_dev, details.st_ino)
        ):
            raise AttemptWorkspaceError("file identity changed during safe open")
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining:
            chunk = os.read(fd, min(1_048_576, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        value = b"".join(chunks)
    finally:
        os.close(fd)
    if len(value) > max_bytes:
        raise AttemptWorkspaceError("file exceeds its read limit")
    return value


def _regular_tree_inventory(root: Path) -> list[_TreeFile]:
    _require_existing_directory(root, "inventory root")
    files: list[_TreeFile] = []
    seen_inodes: set[tuple[int, int]] = set()
    for path in sorted(
        root.rglob("*"), key=lambda item: item.relative_to(root).as_posix().encode("utf-8")
    ):
        details = path.lstat()
        if stat.S_ISLNK(details.st_mode) or not (
            stat.S_ISREG(details.st_mode) or stat.S_ISDIR(details.st_mode)
        ):
            raise AttemptWorkspaceError("inventory contains a forbidden filesystem object")
        if stat.S_ISDIR(details.st_mode):
            continue
        inode = (details.st_dev, details.st_ino)
        if details.st_nlink != 1 or inode in seen_inodes:
            raise AttemptWorkspaceError("hard-linked output files are forbidden")
        seen_inodes.add(inode)
        value = _read_regular_file(path, max_bytes=MAX_SAFE_INTEGER)
        relative = path.relative_to(root).as_posix()
        if relative != unicodedata.normalize("NFC", relative):
            raise AttemptWorkspaceError("filesystem path is not NFC")
        files.append(_TreeFile(relative, digest_bytes(value), len(value)))
    return files


def _payload_inventory(root: Path) -> list[AttemptCompleteFileV1]:
    payload = root / "payload"
    _require_existing_directory(payload, "artifact payload")
    all_files = _regular_tree_inventory(root)
    if not any(item.relative_path == "artifact.json" for item in all_files):
        raise AttemptWorkspaceError("artifact root is missing artifact.json")
    if any(
        item.relative_path != "artifact.json" and not item.relative_path.startswith("payload/")
        for item in all_files
    ):
        raise AttemptWorkspaceError("artifact root contains an undeclared file")
    return [
        AttemptCompleteFileV1(
            relative_path=item.relative_path,
            sha256=item.sha256,
            size_bytes=item.size_bytes,
        )
        for item in all_files
        if item.relative_path.startswith("payload/")
    ]


def _regular_directory_names(root: Path) -> list[str]:
    _require_existing_directory(root, "artifacts root")
    names: list[str] = []
    for path in root.iterdir():
        details = path.lstat()
        if not stat.S_ISDIR(details.st_mode) or stat.S_ISLNK(details.st_mode):
            raise AttemptWorkspaceError("artifacts root may contain only output directories")
        names.append(_binding_name(path.name))
    _require_bytewise_unique(sorted(names, key=lambda item: item.encode("utf-8")), "output roots")
    return sorted(names, key=lambda item: item.encode("utf-8"))


def _load_exact_canonical_bytes(value: bytes) -> Any:
    if (
        len(value) > 67_108_864
        or not value.endswith(b"\n")
        or value.endswith(b"\n\n")
        or b"\r" in value
    ):
        raise AttemptWorkspaceError("JSON document is not bounded canonical JCS+LF")
    try:
        raw = json.loads(value)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AttemptWorkspaceError("invalid JSON document") from exc
    if canonical_document(raw) != value:
        raise AttemptWorkspaceError("JSON document is not canonical JCS+LF")
    return raw
