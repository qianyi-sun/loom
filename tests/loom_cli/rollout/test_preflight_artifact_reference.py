from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from loom_cli.rollout.preflight_artifact_reference import (
    PreflightArtifactReference,
    PreflightArtifactReferenceError,
)
from loom_cli.rollout.preflight_artifact_store import PreflightArtifactPublication
from loom_cli.rollout.preflight_contract import (
    CheckExecution,
    CheckOperation,
    CheckOutcome,
    StageCapability,
)
from loom_cli.rollout.preflight_pipeline import PreflightAssessment

NOW = datetime(2026, 8, 25, 12, tzinfo=UTC)
EVIDENCE = {
    "bundle-digest": "1" * 64,
    "image-artifact-digest": "2" * 64,
    "manifest-artifact-digest": "3" * 64,
    "rendered-manifest-digest": "4" * 64,
    "migration-manifest-digest": "5" * 64,
    "migration-artifact-digest": "6" * 64,
    "production-defaults-digest": "7" * 64,
}


def _execution() -> CheckExecution:
    return CheckExecution(
        check_id="artifacts.publish",
        failure_code="artifacts.publish.failed",
        tier=1,
        stage=StageCapability.STATIC,
        operation=CheckOperation.PROBE,
        outcome=CheckOutcome.PASS,
        input_fingerprint="8" * 64,
        implementation_digest="9" * 64,
        evidence=EVIDENCE,
        evidence_hash=hashlib.sha256(
            json.dumps(EVIDENCE, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        started_at=NOW,
        finished_at=NOW,
        expires_at=NOW + timedelta(hours=1),
        remediation="restore the exact preflight artifact publication",
    )


def _assessment(*executions: CheckExecution) -> PreflightAssessment:
    return PreflightAssessment(
        through_tier=2,
        registry_digest="a" * 64,
        coverage_digest="b" * 64,
        executions=executions,
        blockers=(),
        assessment_digest="c" * 64,
    )


def test_reference_projects_the_one_passing_publication_execution() -> None:
    reference = PreflightArtifactReference.from_assessment(_assessment(_execution()))

    assert reference == PreflightArtifactReference(
        bundle_digest="1" * 64,
        image_artifact_sha256="2" * 64,
        manifest_artifact_sha256="3" * 64,
        rendered_manifest_sha256="4" * 64,
        migration_manifest_sha256="5" * 64,
        migration_artifact_sha256="6" * 64,
        production_defaults_sha256="7" * 64,
    )


@pytest.mark.parametrize(
    "executions",
    (
        (),
        (_execution(), _execution()),
        (replace(_execution(), outcome=CheckOutcome.FAIL),),
    ),
    ids=("missing", "duplicate", "failed"),
)
def test_reference_rejects_non_authoritative_publication_execution(
    executions: tuple[CheckExecution, ...],
) -> None:
    with pytest.raises(
        PreflightArtifactReferenceError,
        match=r"missing or ambiguous|did not pass",
    ):
        PreflightArtifactReference.from_assessment(_assessment(*executions))


def test_reference_rejects_malformed_publication_evidence() -> None:
    execution = _execution()
    malformed = replace(
        execution,
        evidence={**EVIDENCE, "bundle-digest": "not-a-digest"},
    )

    with pytest.raises(PreflightArtifactReferenceError, match="evidence is invalid"):
        PreflightArtifactReference.from_assessment(_assessment(malformed))


def _publication(tmp_path: Path) -> PreflightArtifactPublication:
    root = (tmp_path / ("1" * 64)).resolve()
    return PreflightArtifactPublication(
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        mutation_epoch=7,
        bundle_digest="1" * 64,
        descriptor_path=root / "artifact.json",
        rendered_manifest_path=root / "rendered.yaml",
        migration_manifest_path=root / "migration.yaml",
        production_defaults_path=root / "production-defaults.json",
        image_artifact_sha256="2" * 64,
        manifest_artifact_sha256="3" * 64,
        rendered_manifest_sha256="4" * 64,
        migration_manifest_artifact_sha256="6" * 64,
        migration_manifest_sha256="5" * 64,
        migration_job_name="loom-migration-exact",
        migration_image_id="sha256:" + "8" * 64,
        migration_plan_sha256="9" * 64,
        migration_target_revision="0109",
        browser_report_schema_sha256="a" * 64,
        production_defaults_sha256="7" * 64,
    )


def test_reference_requires_every_publication_component_digest(tmp_path: Path) -> None:
    reference = PreflightArtifactReference.from_assessment(_assessment(_execution()))
    publication = _publication(tmp_path)

    reference.require_publication(publication)

    with pytest.raises(PreflightArtifactReferenceError, match="drifted from assessment"):
        reference.require_publication(
            replace(publication, migration_manifest_artifact_sha256="f" * 64)
        )
