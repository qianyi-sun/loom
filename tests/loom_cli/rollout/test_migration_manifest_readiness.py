from __future__ import annotations

from dataclasses import dataclass

import pytest

from loom_cli.rollout.migration_manifest_readiness import (
    build_migration_manifest_artifact,
    inspect_migration_manifest_artifact,
)
from loom_cli.rollout.preflight_contract import CheckContext, CheckOperation
from loom_cli.rollout.preflight_registered_checks import (
    build_migration_manifest_check,
)
from tests.loom_cli.rollout.test_preflight_artifact_store import _images


@dataclass(frozen=True)
class Result:
    returncode: int


def _artifact(*, returncode: int = 0):
    return build_migration_manifest_artifact(
        lambda _manifest: Result(returncode),
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        image_tag="staging-aaaaaaa",
        image_id="sha256:" + "1" * 64,
        namespace="loom-staging",
        migration_plan_sha256="2" * 64,
        migration_target_revision="0067",
    )


def test_builds_one_deterministic_server_validated_migration_artifact() -> None:
    artifact = _artifact()
    reconstructed = inspect_migration_manifest_artifact(
        artifact.rendered_yaml,
        candidate_sha=artifact.candidate_sha,
        candidate_tree=artifact.candidate_tree,
        image_tag=artifact.image_tag,
        image_id=artifact.image_id,
        namespace=artifact.namespace,
        migration_plan_sha256=artifact.migration_plan_sha256,
        migration_target_revision=artifact.migration_target_revision,
    )

    assert reconstructed == artifact
    assert artifact.job_name.endswith("-pf-b0dba3447a61")
    assert artifact.image_id == "sha256:" + "1" * 64


def test_rejects_server_dry_run_failure_or_render_drift() -> None:
    with pytest.raises(ValueError, match="server-side"):
        _artifact(returncode=1)

    artifact = _artifact()
    with pytest.raises(ValueError, match="contract drifted"):
        inspect_migration_manifest_artifact(
            artifact.rendered_yaml.replace("backoffLimit: 1", "backoffLimit: 2"),
            candidate_sha=artifact.candidate_sha,
            candidate_tree=artifact.candidate_tree,
            image_tag=artifact.image_tag,
            image_id=artifact.image_id,
            namespace=artifact.namespace,
            migration_plan_sha256=artifact.migration_plan_sha256,
            migration_target_revision=artifact.migration_target_revision,
        )


def test_registered_check_exposes_single_tier_one_manifest_predicate() -> None:
    captured = []
    check = build_migration_manifest_check(
        lambda _manifest: Result(0),
        image_artifact=_images,
        migration_plan=lambda: ("2" * 64, "0067"),
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        image_tag="staging-aaaaaaa",
        namespace="loom-staging",
        artifact_sink=captured.append,
    )

    outcome = check.operations[CheckOperation.PROBE](
        CheckContext({"candidate.sha": "a" * 40, "candidate.tree": "b" * 40})
    )

    assert outcome.passed
    assert check.spec.tier == 1
    assert check.spec.dependencies == (
        "images.contract",
        "migration.plan",
        "kubernetes.client",
    )
    assert outcome.evidence["artifact-digest"] == captured[0].artifact_digest
