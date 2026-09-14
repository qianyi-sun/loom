from __future__ import annotations

import warnings
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

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


@pytest.mark.parametrize("drift", [None, "runtime-secret", "owner", "token", "extra-env", "expiry"])
def test_separated_owner_migration_artifact_binds_its_dedicated_credential(drift):
    options = dict(candidate_sha="a" * 40, candidate_tree="b" * 40,
        image_tag="staging-aaaaaaa", image_id="sha256:" + "1" * 64,
        namespace="loom-staging", migration_plan_sha256="2" * 64,
        migration_target_revision="0142", application_owner_role="loom_app_staging_owner")
    artifact = build_migration_manifest_artifact(lambda _: Result(0), **options)
    job = yaml.safe_load(artifact.rendered_yaml)
    spec = job["spec"]["template"]["spec"]
    container = spec["containers"][0]
    env = {e["name"]: e for e in container["env"]}
    assert env["LOOM_DB_OWNER_ROLE"]["value"] == "loom_app_staging_owner"
    assert env["LOOM_DB_URL"]["valueFrom"]["secretKeyRef"]["name"].startswith("loom-app-migration-")
    assert spec["automountServiceAccountToken"] is False
    assert "ttlSecondsAfterFinished" not in job["spec"]
    if drift == "runtime-secret":
        env["LOOM_DB_URL"]["valueFrom"]["secretKeyRef"] = {"name": "loom-secrets", "key": "cp-db-url"}
    elif drift == "owner":
        env["LOOM_DB_OWNER_ROLE"]["value"] = "loom"
    elif drift == "token":
        spec["automountServiceAccountToken"] = True
    elif drift == "extra-env":
        container["env"].append({"name": "PGOPTIONS", "value": "-c role=postgres"})
    elif drift == "expiry":
        job["spec"]["ttlSecondsAfterFinished"] = 1
    if drift:
        with pytest.raises(ValueError, match="migration"):
            inspect_migration_manifest_artifact(yaml.safe_dump(job), **options)
    else:
        assert inspect_migration_manifest_artifact(artifact.rendered_yaml, **options) == artifact
        with pytest.raises(ValueError, match="migration"):
            inspect_migration_manifest_artifact(artifact.rendered_yaml,
                **{k: v for k, v in options.items() if k != "application_owner_role"})


def test_all_migration_sources_compile_without_syntax_warnings() -> None:
    migration_root = Path(__file__).resolve().parents[3] / "migrations/versions"
    for path in sorted(migration_root.glob("*.py")):
        with warnings.catch_warnings():
            warnings.simplefilter("error", SyntaxWarning)
            compile(path.read_bytes(), str(path), "exec")


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


def test_registry_migration_artifact_uses_exact_pull_prefix() -> None:
    manifest_digest = "sha256:" + "3" * 64
    artifact = build_migration_manifest_artifact(
        lambda _manifest: Result(0),
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        image_tag="staging-aaaaaaa",
        image_id="sha256:" + "1" * 64,
        namespace="loom-staging",
        migration_plan_sha256="2" * 64,
        migration_target_revision="0067",
        container_registry="192.168.50.13:5000",
        registry_digest=manifest_digest,
    )

    assert f"image: 192.168.50.13:5000/loom-control-plane@{manifest_digest}" in (
        artifact.rendered_yaml
    )
    assert inspect_migration_manifest_artifact(
        artifact.rendered_yaml,
        candidate_sha=artifact.candidate_sha,
        candidate_tree=artifact.candidate_tree,
        image_tag=artifact.image_tag,
        image_id=artifact.image_id,
        namespace=artifact.namespace,
        migration_plan_sha256=artifact.migration_plan_sha256,
        migration_target_revision=artifact.migration_target_revision,
        container_registry=artifact.container_registry,
        registry_digest=artifact.registry_digest,
    ) == artifact


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
