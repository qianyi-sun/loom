from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

from loom_cli.rollout.gb10_readiness import GB10ProbeTarget
from loom_cli.rollout.migration_readiness import DEFAULT_MIGRATION_POLICY
from loom_cli.rollout.operator.model import APPROVED_REMOTE_URL, CandidateBinding
from loom_cli.rollout.preflight_artifact_store import PreflightArtifactStore
from loom_cli.rollout.preflight_registered_checks import CredentialProbeSource
from loom_cli.rollout.preflight_runtime_sources import (
    BackupAdmissionAuthority,
    PreflightRuntimeSources,
)
from loom_cli.rollout.readonly_authority import ReadonlyAuthorityEvidence
from loom_cli.rollout.rehearsal_readiness import REHEARSAL_CHECK_IDS
from tests.loom_cli.rollout.operator.test_checkpoint_inventory_provider import _config


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


def _result():
    return type("Result", (), {"returncode": 0, "stdout": ""})()


def test_fresh_authority_is_explicit_and_does_not_claim_a_lease() -> None:
    authority = BackupAdmissionAuthority.fresh(
        schema_revision="0066",
        object_inventory_root="d" * 64,
    )

    assert authority.lease_source() is None
    assert authority.expected_lease_digest == "0" * 64
    assert authority.source_request_id == "fresh-checkpoint"
    assert set(authority.component_sha256) == {
        "k8s_secrets",
        "object_inventory",
        "postgres",
    }


def test_sources_build_complete_exact_registry_without_running_probes(tmp_path: Path) -> None:
    config = _config(tmp_path)
    candidate = _candidate()
    token = tmp_path / "state" / "admin"
    credential = CredentialProbeSource(label="admin", path=token)
    policy_digest = hashlib.sha256(DEFAULT_MIGRATION_POLICY.read_bytes()).hexdigest()

    def command(*_args, **_kwargs):
        return _result()

    sources = PreflightRuntimeSources(
        config=config,
        candidate=candidate,
        candidate_root=tmp_path,
        artifact_store=PreflightArtifactStore(tmp_path / "state", service_uid=501),
        service_uid=501,
        service_gid=20,
        runner_install_digest="1" * 64,
        git_run=command,
        credential_sources=(credential,),
        executable_lookup=lambda _name: "/fixed/tool",
        docker_runtime_run=command,
        kubernetes_run=command,
        kubeconfig_metadata_digest="2" * 64,
        readonly_authority_source=lambda: ReadonlyAuthorityEvidence(
            principal="loom-rollout-readonly",
            environment="staging",
            namespace="loom-staging",
            kubernetes_verbs=("get", "list", "watch"),
            kubernetes_resources=("deployments", "pods", "services"),
            http_methods=("GET", "HEAD"),
            capability_source_digest="8" * 64,
        ),
        capacity_source=lambda: None,  # type: ignore[arg-type,return-value]
        backup_authority=BackupAdmissionAuthority.fresh(
            schema_revision="0066",
            object_inventory_root="3" * 64,
        ),
        systemd_run=command,
        gb10_run=command,
        gb10_targets=(
            GB10ProbeTarget(
                ssh_target="trt-gb10-1",
                node_agent_service="loom-gb10-node-agent.service",
            ),
        ),
        gb10_ssh_config=tmp_path / "ssh-config",
        gb10_identity=tmp_path / "identity",
        gb10_ssh_config_sha256="4" * 64,
        gb10_identity_metadata_fingerprint="5" * 64,
        gb10_mount_source=lambda: None,  # type: ignore[arg-type,return-value]
        gb10_mount_binding_digest="6" * 64,
        alembic_ini=tmp_path / "alembic.ini",
        migration_policy_digest=policy_digest,
        systemd_analyze_run=command,
        image_run=command,
        render_manifest=lambda: "",
        server_dry_run=lambda _rendered: _result(),
        browser_run=command,
        browser_token_path=token,
        baseline_probe_factory=lambda _epoch: {
            check_id: (lambda: None)  # type: ignore[dict-item,return-value]
            for check_id in (
                "staging.health",
                "staging.auth",
                "staging.catalog-task",
                "staging.storage-db",
                "staging.network",
            )
        },
        route="https://staging.example.invalid",
        rehearsal_actions=lambda _candidate, _checkpoint, _isolation: {
            check_id: (lambda: None)  # type: ignore[dict-item,return-value]
            for check_id in REHEARSAL_CHECK_IDS
        },
        rehearsal_identity=lambda _candidate, _checkpoint: (
            "rehearsal-exact-checkpoint",
            "7" * 64,
        ),
        now=lambda: datetime(2026, 7, 19, 12, tzinfo=UTC),
    )

    runtime = sources.build(mutation_epoch=9)
    plan = runtime.prebackup_plan(candidate)

    assert plan.registry.through_tier == 3
    assert len(plan.registry.checks) == len(
        [check for check in plan.registry.checks if check.spec.tier in {0, 1, 2, 3}]
    )
    assert {check.spec.check_id for check in plan.registry.checks} >= {
        "candidate.identity",
        "readonly.authority",
        "backup.lease-eligibility",
        "images.build",
        "staging.release-baseline",
        "rehearsal.cleanup",
    }
    assert plan.context.bindings["staging.mutation-epoch"] == 9
    assert plan.context.bindings["backup.source-request"] == "fresh-checkpoint"
