from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from uuid import UUID

import pytest

from loom_cli.rollout.operator.checkpoint_database_authority import DatabaseAuthorityEvidence
from loom_cli.rollout.operator.protected_apply_journal import ComponentState
from loom_cli.rollout.operator.protected_migration_component import (
    KubernetesProtectedMigrationComponent,
)
from loom_cli.rollout.preflight_artifact_store import PreflightArtifactStore
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan as _base_plan
from tests.loom_cli.rollout.test_preflight_artifact_store import (
    _images,
    _manifests,
    _migration,
    _production_defaults,
)


def _rebind_schema3_authority(plan, *, schema_revision: str):
    authority = DatabaseAuthorityEvidence(
        public_schema_revision=schema_revision,
        capacity_guard_schema_revision=plan.capacity_guard_schema_revision,
        configuration_epoch=plan.manager_configuration_epoch,  # type: ignore[arg-type]
        configuration_digest=plan.manager_configuration_digest,  # type: ignore[arg-type]
        authority_incarnation=UUID(str(plan.manager_authority_incarnation)),
        writer_epoch=plan.manager_writer_epoch,  # type: ignore[arg-type]
        execution_state=plan.manager_execution_state,  # type: ignore[arg-type]
        execution_epoch=plan.manager_execution_epoch,  # type: ignore[arg-type]
        execution_manifest_sha256=plan.manager_execution_manifest_sha256,
        executable_new_capacity_ceiling=(
            plan.manager_executable_new_capacity_ceiling  # type: ignore[arg-type]
        ),
        increase_freeze=plan.manager_increase_freeze,  # type: ignore[arg-type]
    )
    checkpoint_components = dict(plan.checkpoint_component_sha256 or {})
    checkpoint_components["database_authority"] = authority.digest
    return replace(
        plan,
        schema_revision=schema_revision,
        public_schema_revision=authority.public_schema_revision,
        database_authority_digest=authority.digest,
        checkpoint_component_sha256=checkpoint_components,
    )


def _plan(tmp_path: Path):  # type: ignore[no-untyped-def]
    return replace(
        _rebind_schema3_authority(_base_plan(tmp_path), schema_revision="0069"),
        migration_target_revision="0072",
    )


class Runner:
    def __init__(self, revision: str) -> None:
        self.revision = revision
        self.calls: list[tuple[tuple[str, ...], bytes | None]] = []

    def capture_stdout(self, argv, *, env, timeout_seconds):
        assert env == {"KUBECONFIG": "/exact"}
        assert timeout_seconds == 30.0
        assert "loom-staging" in argv
        return (self.revision + "\n").encode()

    def run_checked(self, argv, *, env, input_payload, timeout_seconds):
        assert env == {"KUBECONFIG": "/exact"}
        self.calls.append((tuple(argv), input_payload))
        if "apply" in argv:
            assert timeout_seconds == 60.0
        else:
            assert timeout_seconds == 660.0
            self.revision = "0072"


def _published_plan(tmp_path: Path):
    base = _plan(tmp_path)
    state = tmp_path / "state"
    images = _images()
    migration = _migration(
        images,
        candidate_tree="b" * 40,
        migration_plan_sha256="4" * 64,
        migration_target_revision="0072",
    )
    publication = PreflightArtifactStore(state).publish(
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        mutation_epoch=7,
        images=images,
        manifests=_manifests(images),
        migration=migration,
        production_defaults=_production_defaults(candidate_tree="b" * 40),
        migration_plan_sha256="4" * 64,
        migration_target_revision="0072",
        browser_report_schema_sha256="8" * 64,
    )
    return replace(
        base,
        artifact_bundle_digest=publication.bundle_digest,
        artifact_descriptor_path=str(publication.descriptor_path),
        rendered_manifest_path=str(publication.rendered_manifest_path),
        rendered_manifest_sha256=publication.rendered_manifest_sha256,
        migration_manifest_path=str(publication.migration_manifest_path),
        migration_manifest_sha256=publication.migration_manifest_sha256,
        migration_manifest_artifact_sha256=publication.migration_manifest_artifact_sha256,
        production_defaults_path=str(publication.production_defaults_path),
        production_defaults_sha256=publication.production_defaults_sha256,
        migration_job_name=publication.migration_job_name,
        migration_image_id=publication.migration_image_id,
    )


def test_migration_component_converges_exact_published_job(tmp_path: Path) -> None:
    plan = _published_plan(tmp_path)
    runner = Runner("0069")
    authority = KubernetesProtectedMigrationComponent(
        runner=runner,
        environment={"KUBECONFIG": "/exact"},
        service_uid=__import__("os").geteuid(),
    )

    assert authority.classify(plan).state is ComponentState.READY
    authority.apply(plan)
    exact = authority.classify(plan)

    assert exact.state is ComponentState.EXACT
    assert exact.observed_epoch == plan.starting_mutation_epoch + 1
    assert runner.calls[0][1] == Path(plan.migration_manifest_path).read_bytes()
    assert runner.calls[1][0][-1] == f"job/{plan.migration_job_name}"


def test_migration_component_rejects_schema_or_manifest_drift(tmp_path: Path) -> None:
    plan = _published_plan(tmp_path)
    authority = KubernetesProtectedMigrationComponent(
        runner=Runner("0065"),
        environment={"KUBECONFIG": "/exact"},
        service_uid=__import__("os").geteuid(),
    )
    assert authority.classify(plan).state is ComponentState.DRIFTED

    Path(plan.migration_manifest_path).write_text("changed\n")
    with pytest.raises(ValueError, match="content drifted"):
        authority.apply(plan)


def test_legacy_migration_observation_remains_epoch_zero(tmp_path: Path) -> None:
    plan = replace(
        _rebind_schema3_authority(_published_plan(tmp_path), schema_revision="0065"),
        starting_mutation_epoch=0,
    )
    authority = KubernetesProtectedMigrationComponent(
        runner=Runner("0065"),
        environment={"KUBECONFIG": "/exact"},
        service_uid=__import__("os").geteuid(),
    )

    assert authority.classify(plan).observed_epoch == 0


def test_migration_component_rechecks_schema_immediately_before_apply(tmp_path: Path) -> None:
    plan = _published_plan(tmp_path)

    class RacingRunner(Runner):
        def capture_stdout(self, argv, *, env, timeout_seconds):
            self.revision = "0065"
            return super().capture_stdout(argv, env=env, timeout_seconds=timeout_seconds)

    runner = RacingRunner("0069")
    authority = KubernetesProtectedMigrationComponent(
        runner=runner,
        environment={"KUBECONFIG": "/exact"},
        service_uid=__import__("os").geteuid(),
    )

    with pytest.raises(RuntimeError, match="changed before apply"):
        authority.apply(plan)
    assert runner.calls == []
