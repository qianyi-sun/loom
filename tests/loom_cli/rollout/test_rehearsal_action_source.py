from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from loom_cli.rollout.external_supervisor_readiness import build_external_supervisor_artifact
from loom_cli.rollout.image_readiness import (
    ALL_BUILD_IMAGES,
    ROLLOUT_IMAGES,
    ImageArtifactSet,
    ImageDescriptor,
)
from loom_cli.rollout.manifest_readiness import ManifestArtifact
from loom_cli.rollout.migration_manifest_readiness import (
    build_migration_manifest_artifact,
)
from loom_cli.rollout.operator.checkpoint_lease import CriticalCheckpointEvidence
from loom_cli.rollout.operator.model import APPROVED_REMOTE_URL, CandidateBinding
from loom_cli.rollout.preflight_artifact_store import PreflightArtifactStore
from loom_cli.rollout.rehearsal_action_source import (
    RehearsalActionSource,
    RehearsalObservation,
    RehearsalPlan,
    RehearsalResources,
    RehearsalSmokeAuthority,
)
from loom_cli.rollout.rehearsal_readiness import REHEARSAL_CHECK_IDS
from tests.loom_cli.rollout.rehearsal_fixtures import gb10_rehearsal_authority
from tests.loom_cli.rollout.test_preflight_artifact_store import _production_defaults


def _candidate() -> CandidateBinding:
    return CandidateBinding(
        remote_url=APPROVED_REMOTE_URL,
        target_ref="origin/dev",
        resolved_sha="a" * 40,
        image_tag="staging-aaaaaaa",
        fetched_at="2026-07-19T12:00:00Z",
        source_mode="sealed-cumulative",
        resolved_tree="b" * 40,
        approved_base_sha="c" * 40,
    )


def _checkpoint(tmp_path: Path) -> CriticalCheckpointEvidence:
    return CriticalCheckpointEvidence(
        request_id="req-abcdefgh",
        manifest_path=tmp_path / "backup-manifest.json",
        manifest_sha256="1" * 64,
        component_sha256={
            "k8s_secrets": "2" * 64,
            "object_inventory": "3" * 64,
            "postgres": "4" * 64,
        },
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=9,
        db_snapshot_identity="pgdump-sha256:" + "5" * 64,
        schema_revision="0066",
        object_inventory_root="6" * 64,
        created_at=datetime(2026, 7, 19, 12, tzinfo=UTC),
    )


def _artifacts() -> ImageArtifactSet:
    return ImageArtifactSet(
        descriptors={
            name: ImageDescriptor(
                image_id="sha256:" + f"{index + 1:064x}",
                revision="a" * 40,
                os="linux",
                architecture="amd64",
                entrypoint=(
                    ("node", "/opt/loom/web/scripts/staging-admin-browser-smoke.mjs")
                    if name == "loom-staging-admin-browser-smoke"
                    else ()
                ),
            )
            for index, (name, _dockerfile) in enumerate(ALL_BUILD_IMAGES)
        },
        plan_digest="7" * 64,
        artifact_digest="8" * 64,
    )


def _manifests() -> ManifestArtifact:
    artifacts = _artifacts()
    rendered = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: exact\n"
    return ManifestArtifact(
        rendered_yaml=rendered,
        rendered_sha256=__import__("hashlib").sha256(rendered.encode()).hexdigest(),
        resource_count=1,
        resource_set_digest="d" * 64,
        image_identities={
            name: artifacts.image_digests[name] for name, _dockerfile in ROLLOUT_IMAGES
        },
        artifact_digest="e" * 64,
    )


@dataclass(frozen=True)
class _DryRunResult:
    returncode: int = 0


def _migration():
    artifacts = _artifacts()
    return build_migration_manifest_artifact(
        lambda _manifest: _DryRunResult(),
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        image_tag="staging-aaaaaaa",
        image_id=artifacts.image_digests["loom-control-plane"],
        namespace="loom-staging",
        migration_plan_sha256="b" * 64,
        migration_target_revision="0067",
    )


class Backend:
    def __init__(self) -> None:
        self.calls: list[tuple[str, RehearsalPlan]] = []

    def execute(self, check_id: str, plan: RehearsalPlan) -> RehearsalObservation:
        self.calls.append((check_id, plan))
        return RehearsalObservation(
            check_id=check_id,
            evidence_digest="9" * 64,
            journal_digest="a" * 64,
            protected_mutation=False,
            cleanup_verified=check_id == "rehearsal.cleanup",
            blockers={},
        )


def _source(backend: Backend, tmp_path: Path) -> RehearsalActionSource:
    return RehearsalActionSource(
        image_artifacts=_artifacts,
        manifest_artifacts=_manifests,
        migration_artifacts=_migration,
        production_defaults_artifacts=lambda: _production_defaults(candidate_tree="b" * 40),
        external_supervisor_artifacts=_external_supervisor,
        artifact_store=PreflightArtifactStore(tmp_path / "state"),
        migration_plan_sha256="b" * 64,
        migration_target_revision="0067",
        browser_report_schema_sha256="c" * 64,
        cluster_name="loom-staging",
        route_origin="https://staging.example.test/dev",
        smoke_authority=_smoke_authority(),
        gb10_authority=gb10_rehearsal_authority(),
        backend=backend,
    )


def _external_supervisor():
    return build_external_supervisor_artifact(
        Path(__file__).resolve().parents[3],
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        image_tag="staging-aaaaaaa",
    )


def _smoke_authority() -> RehearsalSmokeAuthority:
    return RehearsalSmokeAuthority(
        represented_username="devansh",
        team_id="11111111-1111-4111-8111-111111111111",
        admin_actor="loom-staging-rollout",
        task_id="loom-smoke/gb10-oracle-hello-world",
        required_worker_pool="gb10",
        agent="oracle",
    )


def test_source_binds_identity_actions_and_isolated_resources(tmp_path: Path) -> None:
    backend = Backend()
    source = _source(backend, tmp_path)
    candidate = _candidate()
    checkpoint = _checkpoint(tmp_path)

    isolation_id, plan_digest = source.identity(candidate, checkpoint)
    actions = source.actions(candidate, checkpoint, isolation_id)

    assert isolation_id.startswith("rehearsal-")
    assert len(plan_digest) == 64
    assert set(actions) == set(REHEARSAL_CHECK_IDS)
    for check_id in REHEARSAL_CHECK_IDS:
        result = actions[check_id]()
        assert result.ready
    plans = {call[1].plan_digest for call in backend.calls}
    assert plans == {plan_digest}
    resources = backend.calls[0][1].resources
    assert resources.namespace.startswith("loom-rehearsal-")
    assert resources.namespace != "loom-staging"
    assert resources.database.startswith("loom_rehearsal_")
    assert resources.object_prefix.startswith("rehearsal/")
    suffix = resources.namespace.removeprefix("loom-rehearsal-")
    assert resources.route == f"https://staging.example.test/dev/rehearsal/{suffix}"
    assert resources.systemd_unit.startswith("loom-preflight-")


def test_resource_authority_rejects_route_without_staging_prefix() -> None:
    with pytest.raises(ValueError, match="resource identity is invalid"):
        RehearsalResources.derive(
            "rehearsal-" + "a" * 24,
            route_origin="https://staging.example.test",
        )


def test_source_rejects_isolation_identity_drift(tmp_path: Path) -> None:
    source = _source(Backend(), tmp_path)
    with pytest.raises(ValueError, match="isolation identity drifted"):
        source.actions(_candidate(), _checkpoint(tmp_path), "rehearsal-" + "f" * 24)


def test_isolation_identity_changes_with_browser_contract(tmp_path: Path) -> None:
    original = _source(Backend(), tmp_path)
    changed = RehearsalActionSource(
        image_artifacts=_artifacts,
        manifest_artifacts=_manifests,
        migration_artifacts=_migration,
        production_defaults_artifacts=lambda: _production_defaults(candidate_tree="b" * 40),
        external_supervisor_artifacts=_external_supervisor,
        artifact_store=PreflightArtifactStore(tmp_path / "changed-state"),
        migration_plan_sha256="b" * 64,
        migration_target_revision="0067",
        browser_report_schema_sha256="d" * 64,
        cluster_name="loom-staging",
        route_origin="https://staging.example.test/dev",
        smoke_authority=_smoke_authority(),
        gb10_authority=gb10_rehearsal_authority(),
        backend=Backend(),
    )

    assert original.identity(_candidate(), _checkpoint(tmp_path)) != changed.identity(
        _candidate(), _checkpoint(tmp_path)
    )


def test_isolation_identity_changes_with_smoke_authority(tmp_path: Path) -> None:
    original = _source(Backend(), tmp_path)
    changed = RehearsalActionSource(
        image_artifacts=_artifacts,
        manifest_artifacts=_manifests,
        migration_artifacts=_migration,
        production_defaults_artifacts=lambda: _production_defaults(candidate_tree="b" * 40),
        external_supervisor_artifacts=_external_supervisor,
        artifact_store=PreflightArtifactStore(tmp_path / "changed-state"),
        migration_plan_sha256="b" * 64,
        migration_target_revision="0067",
        browser_report_schema_sha256="c" * 64,
        cluster_name="loom-staging",
        route_origin="https://staging.example.test/dev",
        smoke_authority=replace(_smoke_authority(), agent="codex"),
        gb10_authority=gb10_rehearsal_authority(),
        backend=Backend(),
    )

    assert original.identity(_candidate(), _checkpoint(tmp_path)) != changed.identity(
        _candidate(), _checkpoint(tmp_path)
    )


def test_isolation_identity_changes_with_gb10_transport_authority(tmp_path: Path) -> None:
    original = _source(Backend(), tmp_path)
    changed = replace(
        original,
        artifact_store=PreflightArtifactStore(tmp_path / "changed-state"),
        gb10_authority=replace(
            gb10_rehearsal_authority(),
            identity_metadata_fingerprint="d" * 64,
        ),
    )

    assert original.identity(_candidate(), _checkpoint(tmp_path)) != changed.identity(
        _candidate(), _checkpoint(tmp_path)
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("team_id", "11111111-1111-1111-8111-111111111111"),
        ("team_id", "11111111-1111-4111-1111-111111111111"),
        ("task_id", "../loom-smoke/gb10-oracle-hello-world"),
        ("task_id", "loom-smoke//gb10-oracle-hello-world"),
        ("admin_actor", "rollout actor"),
    ],
)
def test_smoke_authority_rejects_unsafe_identity(field: str, value: str) -> None:
    with pytest.raises(ValueError, match=r"admin smoke (team identity|authority) is invalid"):
        replace(_smoke_authority(), **{field: value})


def test_plan_schema_round_trips_smoke_authority(tmp_path: Path) -> None:
    backend = Backend()
    source = _source(backend, tmp_path)
    candidate = _candidate()
    checkpoint = _checkpoint(tmp_path)
    isolation_id, _digest = source.identity(candidate, checkpoint)
    source.actions(candidate, checkpoint, isolation_id)["rehearsal.namespace"]()
    plan = backend.calls[-1][1]

    record = plan.to_record()
    assert RehearsalPlan.from_record(record) == plan

    record["schema_version"] = 1
    with pytest.raises(ValueError, match="plan schema is invalid"):
        RehearsalPlan.from_record(record)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("namespace", "loom-staging"),
        ("database", "loom"),
        ("object_prefix", "staging/objects/"),
        ("route", "https://staging.example.test/dev"),
        ("systemd_unit", "loom-staging-rollout.service"),
    ],
)
def test_resource_authority_rejects_protected_names(field: str, value: str) -> None:
    values = {
        "namespace": "loom-rehearsal-" + "a" * 24,
        "database": "loom_rehearsal_" + "a" * 24,
        "object_prefix": "rehearsal/" + "a" * 24 + "/",
        "route": "https://staging.example.test/dev/rehearsal/" + "a" * 24,
        "systemd_unit": "loom-preflight-" + "a" * 24 + ".service",
    }
    values[field] = value
    with pytest.raises(ValueError, match="escaped isolated authority"):
        RehearsalResources(**values).require_isolated()


def test_backend_cannot_claim_protected_mutation(tmp_path: Path) -> None:
    class UnsafeBackend(Backend):
        def execute(self, check_id: str, plan: RehearsalPlan) -> RehearsalObservation:
            return RehearsalObservation(
                check_id=check_id,
                evidence_digest="9" * 64,
                journal_digest="a" * 64,
                protected_mutation=True,
                cleanup_verified=False,
                blockers={},
            )

    source = _source(UnsafeBackend(), tmp_path)
    candidate = _candidate()
    checkpoint = _checkpoint(tmp_path)
    isolation_id, _digest = source.identity(candidate, checkpoint)

    with pytest.raises(ValueError, match="evidence is invalid"):
        source.actions(candidate, checkpoint, isolation_id)["rehearsal.namespace"]()
