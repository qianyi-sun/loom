"""Typed artifacts for the TerminalGen authoring and publication boundary."""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, StringConstraints, field_validator, model_validator

from loom.integrations.terminalgen.contracts import MAX_PLAN_SLOTS
from loom.pipeline.keys import canonical_document
from loom.pipeline.spec import (
    ARTIFACT_TYPE_PATTERN,
    IMAGE_PATTERN,
    Digest,
    NonNegativeSafeInt,
    PipelineModel,
    PositiveSafeInt,
    reject_secret_literals,
)

_ArtifactType = Annotated[str, StringConstraints(pattern=ARTIFACT_TYPE_PATTERN)]
_ImageDigest = Annotated[str, StringConstraints(pattern=IMAGE_PATTERN)]
_Reason = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]{0,127}$")]
_TaskId = Annotated[
    str,
    StringConstraints(pattern=r"^terminalgen-[a-z0-9][a-z0-9_-]{0,190}$"),
]
_SlotId = Annotated[
    str,
    StringConstraints(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*__[a-z0-9-]+__[0-9]{4}$"),
]

MAX_ARTIFACT_DOCUMENT_BYTES = 67_108_864


def _safe_payload_path(value: str) -> str:
    if not value or len(value.encode()) > 4096 or "\\" in value or "\x00" in value:
        raise ValueError("artifact path is empty or unsafe")
    path = PurePosixPath(value)
    if value.startswith("/") or not path.parts or path.parts[0] != "payload":
        raise ValueError("artifact path must be below payload/")
    if any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("artifact path contains an invalid component")
    return value


class ArtifactRefV1(PipelineModel):
    artifact_id: UUID
    artifact_type: _ArtifactType
    manifest_sha256: Digest


class PipelineArtifactProvenanceV1(PipelineModel):
    producer_kind: Literal["pipeline"]
    loom_commit_sha: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
    pipeline_run_id: UUID
    stage_run_id: UUID
    execution_attempt_id: UUID
    recipe_digest: Digest
    execution_spec_digest: Digest
    image_digest: _ImageDigest
    compatibility_manifest_sha256: Digest
    source_artifacts: list[ArtifactRefV1]

    @field_validator("source_artifacts")
    @classmethod
    def source_refs_are_canonical(cls, values: list[ArtifactRefV1]) -> list[ArtifactRefV1]:
        if values != sorted(values, key=lambda item: item.artifact_id.bytes):
            raise ValueError("source_artifacts must be sorted by artifact UUID")
        if len({item.artifact_id for item in values}) != len(values):
            raise ValueError("source_artifacts must be unique")
        return values


class TaskBundleFileV1(PipelineModel):
    role: Literal[
        "task_config",
        "instruction",
        "environment",
        "dependency_lock",
        "verifier",
        "reference_solution",
        "support",
    ]
    relative_path: str
    sha256: Digest
    size_bytes: NonNegativeSafeInt
    media_type: Annotated[
        str,
        StringConstraints(pattern=r"^[a-z0-9][a-z0-9!#$&^_.+-]*/[a-z0-9][a-z0-9!#$&^_.+-]*$"),
    ]

    @field_validator("relative_path")
    @classmethod
    def relative_path_is_safe(cls, value: str) -> str:
        return _safe_payload_path(value)


class TerminalTaskIdentityV1(PipelineModel):
    slot_id: _SlotId
    template_family_id: _SlotId
    task_id: _TaskId
    task_name: Annotated[str, StringConstraints(min_length=1, max_length=256)]
    source_task: Annotated[str, StringConstraints(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")]
    catalog_sha256: Digest
    parameters_sha256: Digest
    task_tree_sha256: Digest
    task_config_sha256: Digest
    verifier_bridge_sha256: Digest

    @model_validator(mode="after")
    def identities_match(self) -> TerminalTaskIdentityV1:
        if self.slot_id != self.template_family_id:
            raise ValueError("slot and template-family identity drift")
        if self.task_id != f"terminalgen-{self.slot_id}":
            raise ValueError("task_id must be the canonical slot projection")
        return self


class TerminalTaskBundleArtifactV1(PipelineModel):
    schema_version: Literal["terminalgen_task_bundle.v1"]
    access_class: Literal["authoring_restricted"]
    contains_reference_solution: Literal[True]
    task: TerminalTaskIdentityV1
    files: Annotated[list[TaskBundleFileV1], Field(min_length=6, max_length=10_000)]
    provenance: PipelineArtifactProvenanceV1

    @model_validator(mode="after")
    def file_inventory_is_complete(self) -> TerminalTaskBundleArtifactV1:
        paths = [item.relative_path for item in self.files]
        if paths != sorted(paths, key=str.encode) or len(paths) != len(set(paths)):
            raise ValueError("task bundle files must be bytewise sorted and unique")
        counts: dict[str, int] = {}
        for item in self.files:
            counts[item.role] = counts.get(item.role, 0) + 1
        for role in {
            "task_config",
            "instruction",
            "environment",
            "dependency_lock",
            "verifier",
            "reference_solution",
        }:
            if counts.get(role) != 1:
                raise ValueError(f"task bundle requires exactly one {role} file")
        task_config = next(item for item in self.files if item.role == "task_config")
        if task_config.sha256 != self.task.task_config_sha256:
            raise ValueError("task config digest drift")
        verifier = next(item for item in self.files if item.role == "verifier")
        if verifier.sha256 != self.task.verifier_bridge_sha256:
            raise ValueError("verifier bridge digest drift")
        return self


class ValidationCheckV1(PipelineModel):
    check: Literal[
        "path_safety",
        "task_config",
        "dependency_lock",
        "image_policy",
        "verifier_contract",
        "secret_scan",
        "solution_boundary",
    ]
    passed: Literal[True]
    evidence_sha256: Digest


class DynamicValidationRunV1(PipelineModel):
    mode: Literal["baseline_unsolved", "reference_solution"]
    repetition: Literal[1, 2]
    expected_reward: Literal[0, 1]
    observed_reward: Literal[0, 1]
    runner_result_sha256: Digest
    stdout_sha256: Digest
    stderr_sha256: Digest
    duration_milliseconds: PositiveSafeInt
    validator_image: _ImageDigest
    task_base_image: _ImageDigest
    validation_policy_sha256: Digest

    @model_validator(mode="after")
    def reward_matches_mode(self) -> DynamicValidationRunV1:
        expected = 0 if self.mode == "baseline_unsolved" else 1
        if self.expected_reward != expected or self.observed_reward != expected:
            raise ValueError("dynamic validation reward does not prove the requested mode")
        return self


class TerminalTaskValidationArtifactV1(PipelineModel):
    schema_version: Literal["terminalgen_task_validation.v1"]
    access_class: Literal["authoring_restricted"]
    slot_id: _SlotId
    task_artifact: ArtifactRefV1
    task_tree_sha256: Digest
    static_checks: Annotated[list[ValidationCheckV1], Field(min_length=7, max_length=7)]
    dynamic_runs: Annotated[list[DynamicValidationRunV1], Field(min_length=4, max_length=4)]
    provenance: PipelineArtifactProvenanceV1

    @model_validator(mode="after")
    def validation_is_complete(self) -> TerminalTaskValidationArtifactV1:
        checks = [item.check for item in self.static_checks]
        if checks != sorted(checks, key=str.encode) or len(checks) != len(set(checks)):
            raise ValueError("static checks must be complete, bytewise sorted, and unique")
        expected = [
            ("baseline_unsolved", 1),
            ("baseline_unsolved", 2),
            ("reference_solution", 1),
            ("reference_solution", 2),
        ]
        actual = [(item.mode, item.repetition) for item in self.dynamic_runs]
        if actual != expected:
            raise ValueError("dynamic validation must contain two ordered repetitions per mode")
        return self


class FinalAuditCountsV1(PipelineModel):
    requested: Annotated[int, Field(strict=True, ge=1, le=MAX_PLAN_SLOTS)]
    accepted: NonNegativeSafeInt
    rejected: NonNegativeSafeInt
    exhausted: NonNegativeSafeInt
    cancelled: NonNegativeSafeInt
    cleanup_failed: NonNegativeSafeInt
    dynamically_validated: NonNegativeSafeInt

    @model_validator(mode="after")
    def terminal_counts_close(self) -> FinalAuditCountsV1:
        terminal = (
            self.accepted + self.rejected + self.exhausted + self.cancelled + self.cleanup_failed
        )
        if terminal != self.requested:
            raise ValueError("terminal slot counts must equal requested slots")
        if self.dynamically_validated > self.accepted:
            raise ValueError("validated count cannot exceed accepted count")
        return self


class TerminalGenFinalAuditArtifactV1(PipelineModel):
    schema_version: Literal["terminalgen_final_audit.v1"]
    access_class: Literal["sanitized_audit"]
    terminal_outcome: Literal["complete", "partial_failed", "failed", "cancelled"]
    reason_code: _Reason
    plan_identity_sha256: Digest
    slot_terminal_set_sha256: Digest
    task_artifact_set_sha256: Digest
    validation_artifact_set_sha256: Digest
    counts: FinalAuditCountsV1
    all_slot_ids_unique: Literal[True]
    all_template_family_ids_unique: Literal[True]
    quota_complete: bool
    validation_complete: bool
    provenance: PipelineArtifactProvenanceV1

    @model_validator(mode="after")
    def complete_outcome_is_exact(self) -> TerminalGenFinalAuditArtifactV1:
        exact = (
            self.counts.accepted == self.counts.requested
            and self.counts.rejected == 0
            and self.counts.exhausted == 0
            and self.counts.cancelled == 0
            and self.counts.cleanup_failed == 0
            and self.counts.dynamically_validated == self.counts.requested
            and self.quota_complete
            and self.validation_complete
        )
        if (self.terminal_outcome == "complete") != exact:
            raise ValueError("complete authoring outcome requires exact accepted validation quota")
        return self


class CorpusTaskEntryV1(PipelineModel):
    slot_id: _SlotId
    task_id: _TaskId
    task_name: Annotated[str, StringConstraints(min_length=1, max_length=256)]
    source_task_tree_sha256: Digest
    projected_task_tree_sha256: Digest
    source_task_artifact: ArtifactRefV1
    validation_artifact: ArtifactRefV1
    bundle_relative_path: str
    bundle_sha256: Digest
    bundle_size_bytes: PositiveSafeInt
    verifier_bridge_sha256: Digest
    files: Annotated[list[TaskBundleFileV1], Field(min_length=5, max_length=10_000)]

    @model_validator(mode="after")
    def task_projection_is_canonical(self) -> CorpusTaskEntryV1:
        if self.task_id != f"terminalgen-{self.slot_id}":
            raise ValueError("corpus task_id must be the canonical slot projection")
        expected_path = f"payload/tasks/{self.task_id}.tar"
        if _safe_payload_path(self.bundle_relative_path) != expected_path:
            raise ValueError("corpus bundle path must be the canonical task projection")
        paths = [item.relative_path for item in self.files]
        if paths != sorted(paths, key=str.encode) or len(paths) != len(set(paths)):
            raise ValueError("corpus task files must be bytewise sorted and unique")
        counts: dict[str, int] = {}
        for item in self.files:
            counts[item.role] = counts.get(item.role, 0) + 1
        for role in {
            "task_config",
            "instruction",
            "environment",
            "dependency_lock",
            "verifier",
        }:
            if counts.get(role) != 1:
                raise ValueError(f"runtime corpus task requires exactly one {role} file")
        verifier = next(item for item in self.files if item.role == "verifier")
        if verifier.sha256 != self.verifier_bridge_sha256:
            raise ValueError("runtime corpus verifier bridge digest drift")
        return self


class TerminalGenCorpusArtifactV1(PipelineModel):
    schema_version: Literal["terminalgen_corpus.v1"]
    corpus_kind: Literal["authoring", "runtime"]
    access_class: Literal["authoring_restricted", "team_runtime"]
    contains_reference_solutions: bool
    corpus_id: Annotated[str, StringConstraints(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")]
    corpus_version: PositiveSafeInt
    final_audit_artifact: ArtifactRefV1
    plan_identity_sha256: Digest
    task_count: Annotated[int, Field(strict=True, ge=1, le=MAX_PLAN_SLOTS)]
    tasks: Annotated[list[CorpusTaskEntryV1], Field(min_length=1, max_length=MAX_PLAN_SLOTS)]
    corpus_tree_sha256: Digest
    task_archive_format: Literal["tar"]
    provenance: PipelineArtifactProvenanceV1

    @model_validator(mode="after")
    def corpus_boundary_is_closed(self) -> TerminalGenCorpusArtifactV1:
        expected_access = (
            "authoring_restricted" if self.corpus_kind == "authoring" else "team_runtime"
        )
        expected_solution = self.corpus_kind == "authoring"
        if self.access_class != expected_access:
            raise ValueError("corpus access class does not match corpus kind")
        if self.contains_reference_solutions != expected_solution:
            raise ValueError("runtime corpus must be solution-free")
        solution_counts = [
            sum(item.role == "reference_solution" for item in task.files)
            for task in self.tasks
        ]
        if self.corpus_kind == "runtime" and any(solution_counts):
            raise ValueError("runtime corpus task inventory must be solution-free")
        if self.corpus_kind == "authoring" and any(count != 1 for count in solution_counts):
            raise ValueError("authoring corpus tasks require one reference solution")
        if len(self.tasks) != self.task_count:
            raise ValueError("corpus task count drift")
        ids = [item.slot_id for item in self.tasks]
        if ids != sorted(ids, key=str.encode) or len(ids) != len(set(ids)):
            raise ValueError("corpus tasks must be bytewise sorted and unique")
        return self


class TerminalGenPublicationRequestV1(PipelineModel):
    schema_version: Literal["terminalgen.publication-request.v1"]
    pipeline_run_id: UUID
    recipe_digest: Digest
    corpus_id: Annotated[str, StringConstraints(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")]
    corpus_version: PositiveSafeInt
    alias: Annotated[str, StringConstraints(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")]
    expected_previous_version_sha256: Digest | None
    final_audit_artifact: ArtifactRefV1
    authoring_corpus_artifact: ArtifactRefV1
    runtime_corpus_artifact: ArtifactRefV1
    taskset_smoke_count: Annotated[int, Field(strict=True, ge=1, le=500)]

    @model_validator(mode="after")
    def references_are_exact(self) -> TerminalGenPublicationRequestV1:
        expected = {
            "final_audit_artifact": "terminalgen_final_audit.v1",
            "authoring_corpus_artifact": "terminalgen_corpus.v1",
            "runtime_corpus_artifact": "terminalgen_corpus.v1",
        }
        for field, artifact_type in expected.items():
            if getattr(self, field).artifact_type != artifact_type:
                raise ValueError(f"{field} has the wrong Artifact type")
        ids = {
            self.final_audit_artifact.artifact_id,
            self.authoring_corpus_artifact.artifact_id,
            self.runtime_corpus_artifact.artifact_id,
        }
        if len(ids) != 3:
            raise ValueError("publication inputs must be distinct Artifacts")
        return self


class TerminalGenTaskSetSmokeV1(PipelineModel):
    schema_version: Literal["terminalgen.taskset-smoke.v1"]
    corpus_version_sha256: Digest
    task_count: Annotated[int, Field(strict=True, ge=1, le=500)]
    task_ids: Annotated[list[_TaskId], Field(min_length=1, max_length=500)]
    manifest_sha256: Digest
    archive_sha256: Digest
    archive_size_bytes: PositiveSafeInt

    @model_validator(mode="after")
    def tasks_are_canonical(self) -> TerminalGenTaskSetSmokeV1:
        if len(self.task_ids) != self.task_count:
            raise ValueError("TaskSet smoke task count drift")
        if self.task_ids != sorted(self.task_ids, key=str.encode):
            raise ValueError("TaskSet smoke task IDs must be bytewise sorted")
        if len(set(self.task_ids)) != len(self.task_ids):
            raise ValueError("TaskSet smoke task IDs must be unique")
        return self


class TerminalGenPublicationReceiptV1(PipelineModel):
    schema_version: Literal["terminalgen.publication-receipt.v1"]
    publication_id: UUID
    pipeline_run_id: UUID
    corpus_version_id: UUID
    corpus_version_sha256: Digest
    corpus_id: Annotated[str, StringConstraints(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")]
    corpus_version: PositiveSafeInt
    alias: Annotated[str, StringConstraints(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")]
    alias_generation: PositiveSafeInt
    previous_corpus_version_id: UUID | None
    authoring_corpus_artifact: ArtifactRefV1
    runtime_corpus_artifact: ArtifactRefV1
    taskset_smoke: TerminalGenTaskSetSmokeV1


ARTIFACT_MODELS: dict[str, type[PipelineModel]] = {
    "terminalgen_task_bundle.v1": TerminalTaskBundleArtifactV1,
    "terminalgen_task_validation.v1": TerminalTaskValidationArtifactV1,
    "terminalgen_final_audit.v1": TerminalGenFinalAuditArtifactV1,
    "terminalgen_corpus.v1": TerminalGenCorpusArtifactV1,
    "terminalgen.publication-request.v1": TerminalGenPublicationRequestV1,
    "terminalgen.publication-receipt.v1": TerminalGenPublicationReceiptV1,
    "terminalgen.taskset-smoke.v1": TerminalGenTaskSetSmokeV1,
}


def validate_artifact_document(value: object) -> PipelineModel:
    if not isinstance(value, dict):
        raise ValueError("TerminalGen artifact document must be a JSON object")
    model = ARTIFACT_MODELS.get(str(value.get("schema_version")))
    if model is None:
        raise ValueError("unknown TerminalGen artifact type")
    encoded = canonical_document(value)
    if len(encoded) > MAX_ARTIFACT_DOCUMENT_BYTES:
        raise ValueError("TerminalGen artifact document exceeds the byte limit")
    result = model.model_validate_json(encoded)
    reject_secret_literals(result)
    return result


__all__ = [
    "ARTIFACT_MODELS",
    "ArtifactRefV1",
    "CorpusTaskEntryV1",
    "DynamicValidationRunV1",
    "FinalAuditCountsV1",
    "PipelineArtifactProvenanceV1",
    "TaskBundleFileV1",
    "TerminalGenCorpusArtifactV1",
    "TerminalGenFinalAuditArtifactV1",
    "TerminalGenPublicationReceiptV1",
    "TerminalGenPublicationRequestV1",
    "TerminalGenTaskSetSmokeV1",
    "TerminalTaskBundleArtifactV1",
    "TerminalTaskIdentityV1",
    "TerminalTaskValidationArtifactV1",
    "ValidationCheckV1",
    "validate_artifact_document",
]
