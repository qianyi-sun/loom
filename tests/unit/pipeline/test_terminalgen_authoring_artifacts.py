from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import ValidationError

from loom.integrations.terminalgen.artifacts import (
    ArtifactRefV1,
    CorpusTaskEntryV1,
    DynamicValidationRunV1,
    FinalAuditCountsV1,
    PipelineArtifactProvenanceV1,
    TaskBundleFileV1,
    TerminalGenCorpusArtifactV1,
    TerminalGenFinalAuditArtifactV1,
    TerminalTaskBundleArtifactV1,
    TerminalTaskIdentityV1,
    TerminalTaskValidationArtifactV1,
    ValidationCheckV1,
    validate_artifact_document,
)
from loom.pipeline.keys import canonical_document

DIGEST = "sha256:" + "a" * 64
IMAGE = "registry.example.invalid/loom/terminalgen@sha256:" + "b" * 64
SLOT = "capability-00__same-domain-parametric__0001"


def _ref(index: int, artifact_type: str) -> ArtifactRefV1:
    return ArtifactRefV1(
        artifact_id=UUID(int=index), artifact_type=artifact_type, manifest_sha256=DIGEST
    )


def _provenance(*refs: ArtifactRefV1) -> PipelineArtifactProvenanceV1:
    return PipelineArtifactProvenanceV1(
        producer_kind="pipeline",
        loom_commit_sha="c" * 40,
        pipeline_run_id=UUID(int=20),
        stage_run_id=UUID(int=21),
        execution_attempt_id=UUID(int=22),
        recipe_digest=DIGEST,
        execution_spec_digest=DIGEST,
        image_digest=IMAGE,
        compatibility_manifest_sha256=DIGEST,
        source_artifacts=sorted(refs, key=lambda item: item.artifact_id.bytes),
    )


def _file(role: str, name: str) -> TaskBundleFileV1:
    return TaskBundleFileV1(
        role=role,
        relative_path=f"payload/{name}",
        sha256=DIGEST,
        size_bytes=10,
        media_type="text/plain",
    )


def _task_bundle() -> TerminalTaskBundleArtifactV1:
    files = [
        _file("environment", "Dockerfile"),
        _file("dependency_lock", "dependencies.lock"),
        _file("instruction", "instruction.md"),
        _file("reference_solution", "solution.sh"),
        _file("task_config", "task.toml"),
        _file("verifier", "tests/test_task.py"),
    ]
    return TerminalTaskBundleArtifactV1(
        schema_version="terminalgen_task_bundle.v1",
        access_class="authoring_restricted",
        contains_reference_solution=True,
        task=TerminalTaskIdentityV1(
            slot_id=SLOT,
            template_family_id=SLOT,
            task_id=f"terminalgen-{SLOT}",
            task_name="Durable synthetic terminal task",
            source_task="source-task-00",
            catalog_sha256=DIGEST,
            parameters_sha256=DIGEST,
            task_tree_sha256=DIGEST,
            task_config_sha256=DIGEST,
            verifier_bridge_sha256=DIGEST,
        ),
        files=sorted(files, key=lambda item: item.relative_path.encode()),
        provenance=_provenance(),
    )


def _validation() -> TerminalTaskValidationArtifactV1:
    checks = [
        ValidationCheckV1(check=check, passed=True, evidence_sha256=DIGEST)
        for check in sorted(
            [
                "path_safety",
                "task_config",
                "dependency_lock",
                "image_policy",
                "verifier_contract",
                "secret_scan",
                "solution_boundary",
            ],
            key=str.encode,
        )
    ]
    dynamic = [
        DynamicValidationRunV1(
            mode=mode,
            repetition=repetition,
            expected_reward=0 if mode == "baseline_unsolved" else 1,
            observed_reward=0 if mode == "baseline_unsolved" else 1,
            runner_result_sha256=DIGEST,
            stdout_sha256=DIGEST,
            stderr_sha256=DIGEST,
            duration_milliseconds=1,
            validator_image=IMAGE,
            task_base_image=IMAGE,
            validation_policy_sha256=DIGEST,
        )
        for mode in ("baseline_unsolved", "reference_solution")
        for repetition in (1, 2)
    ]
    task_ref = _ref(1, "terminalgen_task_bundle.v1")
    return TerminalTaskValidationArtifactV1(
        schema_version="terminalgen_task_validation.v1",
        access_class="authoring_restricted",
        slot_id=SLOT,
        task_artifact=task_ref,
        task_tree_sha256=DIGEST,
        static_checks=checks,
        dynamic_runs=dynamic,
        provenance=_provenance(task_ref),
    )


def _audit(*, outcome: str = "complete") -> TerminalGenFinalAuditArtifactV1:
    complete = outcome == "complete"
    return TerminalGenFinalAuditArtifactV1(
        schema_version="terminalgen_final_audit.v1",
        access_class="sanitized_audit",
        terminal_outcome=outcome,
        reason_code="quota_complete" if complete else "quota_exhausted",
        plan_identity_sha256=DIGEST,
        slot_terminal_set_sha256=DIGEST,
        task_artifact_set_sha256=DIGEST,
        validation_artifact_set_sha256=DIGEST,
        counts=FinalAuditCountsV1(
            requested=1,
            accepted=1 if complete else 0,
            rejected=0,
            exhausted=0 if complete else 1,
            cancelled=0,
            cleanup_failed=0,
            dynamically_validated=1 if complete else 0,
        ),
        all_slot_ids_unique=True,
        all_template_family_ids_unique=True,
        quota_complete=complete,
        validation_complete=complete,
        provenance=_provenance(),
    )


def test_task_bundle_requires_canonical_identity_restricted_solution_and_inventory() -> None:
    bundle = _task_bundle()
    assert validate_artifact_document(bundle.model_dump(mode="json")) == bundle

    payload = bundle.model_dump(mode="json")
    payload["access_class"] = "team_runtime"
    with pytest.raises(ValidationError):
        TerminalTaskBundleArtifactV1.model_validate_json(canonical_document(payload))

    payload = bundle.model_dump(mode="json")
    next(item for item in payload["files"] if item["role"] == "reference_solution")["role"] = (
        "support"
    )
    with pytest.raises(ValidationError, match="reference_solution"):
        TerminalTaskBundleArtifactV1.model_validate_json(canonical_document(payload))


def test_validation_requires_static_contract_and_two_runs_per_mode() -> None:
    validation = _validation()
    assert validate_artifact_document(validation.model_dump(mode="json")) == validation

    payload = validation.model_dump(mode="json")
    payload["dynamic_runs"][0]["observed_reward"] = 1
    with pytest.raises(ValidationError, match="does not prove"):
        TerminalTaskValidationArtifactV1.model_validate_json(canonical_document(payload))

    payload = validation.model_dump(mode="json")
    payload["dynamic_runs"] = payload["dynamic_runs"][:-1]
    with pytest.raises(ValidationError):
        TerminalTaskValidationArtifactV1.model_validate_json(canonical_document(payload))


def test_final_audit_cannot_call_partial_quota_complete() -> None:
    assert _audit().terminal_outcome == "complete"
    assert _audit(outcome="partial_failed").terminal_outcome == "partial_failed"

    payload = _audit(outcome="partial_failed").model_dump(mode="json")
    payload["terminal_outcome"] = "complete"
    with pytest.raises(ValidationError, match="exact accepted validation quota"):
        TerminalGenFinalAuditArtifactV1.model_validate_json(canonical_document(payload))


def test_runtime_corpus_is_solution_free_and_authoring_corpus_is_restricted() -> None:
    task = _task_bundle().task
    entry = CorpusTaskEntryV1(
        slot_id=task.slot_id,
        task_id=task.task_id,
        task_name=task.task_name,
        task_tree_sha256=task.task_tree_sha256,
        task_artifact=_ref(2, "terminalgen_runtime_task.v1"),
        validation_artifact=_ref(3, "terminalgen_task_validation.v1"),
    )
    runtime = TerminalGenCorpusArtifactV1(
        schema_version="terminalgen_corpus.v1",
        corpus_kind="runtime",
        access_class="team_runtime",
        contains_reference_solutions=False,
        corpus_id="terminalgen-authorized",
        corpus_version=1,
        final_audit_artifact=_ref(4, "terminalgen_final_audit.v1"),
        plan_identity_sha256=DIGEST,
        task_count=1,
        tasks=[entry],
        corpus_tree_sha256=DIGEST,
        provenance=_provenance(),
    )
    assert validate_artifact_document(runtime.model_dump(mode="json")) == runtime

    payload = runtime.model_dump(mode="json")
    payload["contains_reference_solutions"] = True
    with pytest.raises(ValidationError, match="solution-free"):
        TerminalGenCorpusArtifactV1.model_validate_json(canonical_document(payload))

    payload = runtime.model_dump(mode="json")
    payload["corpus_kind"] = "authoring"
    with pytest.raises(ValidationError, match="access class"):
        TerminalGenCorpusArtifactV1.model_validate_json(canonical_document(payload))


def test_artifact_validator_rejects_unknown_types_fields_and_secret_literals() -> None:
    with pytest.raises(ValueError, match="unknown TerminalGen"):
        validate_artifact_document({"schema_version": "terminalgen_unknown.v1"})

    payload = _task_bundle().model_dump(mode="json")
    payload["workers"] = 150
    with pytest.raises(ValidationError, match="Extra inputs"):
        validate_artifact_document(payload)

    payload = _task_bundle().model_dump(mode="json")
    payload["task"]["task_name"] = "Bearer abcdefghijklmnopqrstuvwxyz"
    with pytest.raises(ValueError, match="secret-looking literal"):
        validate_artifact_document(payload)
