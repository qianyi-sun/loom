from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom_cli.rollout.browser_runtime_readiness import browser_report_schema_digest
from loom_cli.rollout.credential_authority import read_trusted_file, safe_content_fingerprint
from loom_cli.rollout.external_supervisor_readiness import (
    SCRIPT_PATH,
    TASK_IMAGE_BUILDER_SCRIPT_PATH,
)
from loom_cli.rollout.final_gate_readiness import (
    FINAL_CHECK_IDS,
    PROTECTED_MUTATION_CHECK_IDS,
    FinalGateResult,
)
from loom_cli.rollout.gb10_readiness import (
    GB10CandidateSourceReadiness,
    GB10ProbeTarget,
    GB10SharedMountReadiness,
)
from loom_cli.rollout.image_readiness import (
    ALL_BUILD_IMAGES,
    BROWSER_ENTRYPOINT,
    BROWSER_IMAGE,
    REHEARSAL_POSTGRES_ENTRYPOINT,
    REHEARSAL_POSTGRES_IMAGE,
    REVISION_LABEL,
    ImageDescriptor,
    image_plan_digest,
)
from loom_cli.rollout.lifecycle_protocol import lifecycle_protocol_digest
from loom_cli.rollout.operator.backup_lease import BackupLease, component_set_digest
from loom_cli.rollout.operator.backup_rotation import (
    BackupPayloadPhase,
    BackupRetirementRecord,
    BackupRotationState,
    begin_candidate,
    promote_candidate,
    record_manifest_verified,
    record_restore_verified,
)
from loom_cli.rollout.operator.candidate import (
    AdmittedCandidateGitRunner,
    CandidateIdentityEvidence,
)
from loom_cli.rollout.operator.config import OperatorConfig
from loom_cli.rollout.operator.model import CandidateBinding
from loom_cli.rollout.preflight_contract import (
    EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
    EXTERNAL_SUPERVISOR_UNIT_DIRECTORY,
    CheckContext,
    CheckExecution,
    CheckOperation,
    CheckProbe,
    CheckSpec,
    EvidenceField,
    MutationClass,
    PreflightDag,
    RegisteredCheck,
    SecretRedactionPolicy,
    StageCapability,
)
from loom_cli.rollout.preflight_registered_checks import (
    CredentialProbeSource,
    ExternalSupervisorPredecessorSnapshot,
    build_backup_lease_eligibility_check,
    build_backup_rotation_capacity_check,
    build_browser_runtime_check,
    build_candidate_identity_check,
    build_capacity_high_water_check,
    build_credentials_metadata_check,
    build_docker_runtime_check,
    build_external_supervisor_predecessor_check,
    build_final_gate_checks,
    build_gb10_candidate_source_check,
    build_gb10_host_readiness_check,
    build_gb10_shared_mount_check,
    build_gb10_ssh_topology_check,
    build_image_preflight_checks,
    build_kubernetes_client_check,
    build_lifecycle_launch_cancel_check,
    build_manifest_preflight_checks,
    build_migration_plan_check,
    build_readonly_authority_check,
    build_rehearsal_checks,
    build_staging_baseline_checks,
    build_systemd_render_check,
    build_systemd_user_manager_check,
    build_tools_runtime_check,
    credential_source_set_digest,
    gb10_mount_binding_digest,
    gb10_target_inventory_digest,
)
from loom_cli.rollout.readonly_authority import (
    ReadonlyAuthorityEvidence,
    readonly_authority_policy_digest,
)
from loom_cli.rollout.rehearsal_readiness import REHEARSAL_CHECK_IDS, RehearsalResult
from loom_cli.rollout.runtime_readiness import REQUIRED_EXECUTABLES, REQUIRED_IMPORTS
from loom_cli.rollout.staging_baseline_readiness import BaselineProbeResult

BOOT_ID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"


def _secure_static_candidate(tmp_path: Path) -> Path:
    repo_root = Path(__file__).resolve().parents[3]
    sources = {
        "deploy/environment-state/staging.toml": 0o644,
        "scripts/ops/worker_pool_autoscaler_external_once.py": 0o755,
        "scripts/ops/task_image_builder_autoscaler_external_once.py": 0o755,
        "deploy/worker-pools/gb10/loom-gb10-node-agent.service": 0o644,
        "deploy/worker-pools/gb10/loom-gb10-node-agent.timer": 0o644,
        "deploy/worker-pools/gb10/loom-gb10-worker.service": 0o644,
    }
    for relative, mode in sources.items():
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(repo_root / relative, destination)
        destination.chmod(mode)
    return tmp_path


def _candidate_config(tmp_path: Path) -> OperatorConfig:
    return OperatorConfig(
        schema_version=1,
        service_user="loom-rollout",
        operator_group="loom-staging-operators",
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="refs/heads/dev",
        runner_repo=tmp_path / "runner",
        state_root=tmp_path / "state",
        runtime_root=tmp_path / "runtime",
        rollout_root=tmp_path / "rollout",
        kubeconfig_path=tmp_path / "kubeconfig",
        cluster_config_path=tmp_path / "staging.cluster.toml",
        admin_token_source=f"file:{tmp_path / 'admin'}",
        worker_token_source=f"file:{tmp_path / 'worker'}",
        service_token_source=f"file:{tmp_path / 'service'}",
        expect_admin_token_fingerprint="sha256:abc123def456 len=64",
        cluster_name="loom-staging",
        namespace="loom-staging",
        environment="staging",
        cp_url="http://127.0.0.1:18081",
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="11111111-1111-4111-8111-111111111111",
        scope="current-gb10",
        gb10_prep_concurrency=8,
        config_path=tmp_path / "staging-rollout.toml",
        config_sha256="a" * 64,
        source_mode="sealed-cumulative",
        source_commit_sha="1" * 40,
        source_tree_sha="2" * 40,
        source_base_sha="3" * 40,
    )


def _candidate_binding() -> CandidateBinding:
    return CandidateBinding(
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="origin/dev",
        resolved_sha="1" * 40,
        image_tag="staging-1111111",
        fetched_at="2026-07-19T16:00:00Z",
        source_mode="sealed-cumulative",
        resolved_tree="2" * 40,
        approved_base_sha="3" * 40,
    )


def _candidate_context(*, sha: str = "1" * 40) -> CheckContext:
    return CheckContext(
        {
            "candidate.base.sha": "3" * 40,
            "candidate.sha": sha,
            "candidate.source-mode": "sealed-cumulative",
            "runner.config.sha256": "a" * 64,
        }
    )


def test_registered_candidate_identity_uses_shared_verifier(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: list[CandidateBinding] = []

    def verify(_config, binding, *, run):
        del run
        calls.append(binding)
        return CandidateIdentityEvidence(
            resolved_sha=binding.resolved_sha,
            resolved_tree="2" * 40,
            source_mode="sealed-cumulative",
            approved_base_sha="3" * 40,
            linear_history_count=46,
            evidence_digest="4" * 64,
        )

    monkeypatch.setattr(
        "loom_cli.rollout.preflight_registered_checks.verify_bound_candidate",
        verify,
    )
    candidate = _candidate_binding()
    check = build_candidate_identity_check(
        config=_candidate_config(tmp_path),
        candidate=candidate,
        run=lambda _argv: subprocess.CompletedProcess([], 0, "", ""),
    )

    result = PreflightDag((check,)).run(_candidate_context())[0]

    assert result.passed
    assert result.evidence["resolved-tree"] == "2" * 40
    assert result.evidence["linear-history-count"] == 46
    assert calls == [candidate]


def test_registered_candidate_identity_uses_admitted_resume_git_runner(tmp_path: Path) -> None:
    candidate = _candidate_binding()
    calls: list[CandidateBinding] = []

    def resume_identity(_config: object, found: CandidateBinding) -> CandidateIdentityEvidence:
        calls.append(found)
        return CandidateIdentityEvidence(
            resolved_sha=candidate.resolved_sha,
            resolved_tree="2" * 40,
            source_mode="sealed-cumulative",
            approved_base_sha="3" * 40,
            linear_history_count=46,
            evidence_digest="4" * 64,
        )

    check = build_candidate_identity_check(
        config=_candidate_config(tmp_path),
        candidate=candidate,
        run=AdmittedCandidateGitRunner(
            run=lambda _argv: subprocess.CompletedProcess([], 0, "", ""),
            verify_candidate=resume_identity,
        ),
    )

    result = PreflightDag((check,)).run(_candidate_context())[0]

    assert result.passed
    assert result.evidence["identity-digest"] == "4" * 64
    assert calls == [candidate]


def test_registered_candidate_identity_rejects_drifted_resume_git_runner(tmp_path: Path) -> None:
    candidate = _candidate_binding()
    check = build_candidate_identity_check(
        config=_candidate_config(tmp_path),
        candidate=candidate,
        run=AdmittedCandidateGitRunner(
            run=lambda _argv: subprocess.CompletedProcess([], 0, "", ""),
            verify_candidate=lambda _config, _found: CandidateIdentityEvidence(
                resolved_sha="f" * 40,
                resolved_tree="2" * 40,
                source_mode="sealed-cumulative",
                approved_base_sha="3" * 40,
                linear_history_count=46,
                evidence_digest="4" * 64,
            ),
        ),
    )

    result = PreflightDag((check,)).run(_candidate_context())[0]

    assert not result.passed
    assert result.evidence["identity-digest"] == "0" * 64


def test_registered_candidate_identity_rejects_binding_drift_before_git(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: list[object] = []
    monkeypatch.setattr(
        "loom_cli.rollout.preflight_registered_checks.verify_bound_candidate",
        lambda *_args, **_kwargs: calls.append(object()),
    )
    check = build_candidate_identity_check(
        config=_candidate_config(tmp_path),
        candidate=_candidate_binding(),
        run=lambda _argv: subprocess.CompletedProcess([], 0, "", ""),
    )

    result = PreflightDag((check,)).run(_candidate_context(sha="f" * 40))[0]

    assert not result.passed
    assert result.evidence["identity-digest"] == "0" * 64
    assert calls == []


def _tools_runtime_check() -> RegisteredCheck:
    return RegisteredCheck(
        spec=CheckSpec(
            check_id="tools.runtime",
            failure_code="tools.runtime.unavailable",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=(),
            mutation_class=MutationClass.NONE,
            input_keys=("runner.config.sha256",),
            evidence_schema=(EvidenceField("ready", "boolean"),),
            timeout_seconds=5,
            freshness_ttl_seconds=120,
            remediation="restore the fixed rollout runtime tools",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="test-v1",
        operations={
            CheckOperation.PROBE: lambda context: CheckProbe(
                passed=True,
                evidence={"ready": bool(context.bindings["runner.config.sha256"])},
            )
        },
    )


def _passing_dependency(check_id: str) -> RegisteredCheck:
    return RegisteredCheck(
        spec=CheckSpec(
            check_id=check_id,
            failure_code=f"{check_id}.failed",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=(),
            mutation_class=MutationClass.NONE,
            input_keys=("runner.config.sha256",),
            evidence_schema=(EvidenceField("ready", "boolean"),),
            timeout_seconds=5,
            freshness_ttl_seconds=120,
            remediation=f"restore {check_id}",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="test-v1",
        operations={
            CheckOperation.PROBE: lambda _context: CheckProbe(
                passed=True,
                evidence={"ready": True},
            )
        },
    )


def test_registered_tools_runtime_reports_every_blocker_without_diagnostics() -> None:
    executable_calls: list[str] = []
    import_calls: list[str] = []

    def executable_lookup(name: str) -> str | None:
        executable_calls.append(name)
        if name in {"kind", "systemd-run"}:
            return None
        return f"/fixed/bin/{name}"

    def importer(name: str) -> object:
        import_calls.append(name)
        if name == "loom_benchmark_terminal_bench_2.adapter":
            raise ModuleNotFoundError("raw diagnostic must not enter evidence")
        return object()

    install_hash = "9" * 64
    check = build_tools_runtime_check(
        runner_install_hash=install_hash,
        executable_lookup=executable_lookup,
        importer=importer,
    )
    assert check.spec.timeout_seconds == 30
    context = CheckContext(
        {
            "runner.config.sha256": "a" * 64,
            "runner.install.sha256": install_hash,
        }
    )

    executions = PreflightDag((_passing_dependency("runner.install"), check)).run(
        context,
        through_tier=0,
    )

    runtime = next(item for item in executions if item.check_id == "tools.runtime")
    assert not runtime.passed
    assert executable_calls == list(REQUIRED_EXECUTABLES)
    assert import_calls == list(REQUIRED_IMPORTS)
    assert runtime.evidence["executables"] == {
        name: "missing" if name in {"kind", "systemd-run"} else "available"
        for name in REQUIRED_EXECUTABLES
    }
    assert runtime.evidence["imports"] == {
        name: ("missing" if name == "loom_benchmark_terminal_bench_2.adapter" else "available")
        for name in REQUIRED_IMPORTS
    }
    assert "diagnostic" not in str(dict(runtime.evidence))


def test_registered_tools_runtime_rejects_install_binding_before_probing() -> None:
    calls: list[str] = []
    install_hash = "9" * 64
    check = build_tools_runtime_check(
        runner_install_hash=install_hash,
        executable_lookup=lambda name: calls.append(name) or f"/fixed/bin/{name}",
        importer=lambda name: calls.append(name) or object(),
    )
    context = CheckContext(
        {
            "runner.config.sha256": "a" * 64,
            "runner.install.sha256": "8" * 64,
        }
    )

    executions = PreflightDag((_passing_dependency("runner.install"), check)).run(
        context,
        through_tier=0,
    )

    runtime = next(item for item in executions if item.check_id == "tools.runtime")
    assert not runtime.passed
    assert set(runtime.evidence["executables"].values()) == {"missing"}  # type: ignore[union-attr]
    assert calls == []


def test_registered_docker_runtime_reports_both_blockers_without_diagnostics() -> None:
    calls: list[tuple[str, ...]] = []

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 1, "token=do-not-echo", "private")

    check = build_docker_runtime_check(run)
    context = CheckContext(
        {
            "runner.config.sha256": "a" * 64,
            "runner.install.sha256": "9" * 64,
        }
    )

    executions = PreflightDag((_tools_runtime_check(), check)).run(context, through_tier=0)

    docker = next(item for item in executions if item.check_id == "docker.runtime")
    assert docker.passed is False
    assert docker.evidence["daemon-ready"] is False
    assert docker.evidence["buildx-ready"] is False
    assert docker.evidence["inotify-capacity-ready"] is False
    assert len(str(docker.evidence["runtime-digest"])) == 64
    assert calls == [
        ("docker", "info"),
        ("docker", "buildx", "version"),
        ("/usr/sbin/sysctl", "-n", "fs.inotify.max_user_instances"),
    ]
    assert "token" not in str(dict(docker.evidence))
    assert "private" not in str(dict(docker.evidence))


def test_registered_kubernetes_client_binds_kubeconfig_and_safe_evidence(
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, ...]] = []

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        stdout = "loom-staging\n" if "current-context" in argv else ""
        return subprocess.CompletedProcess(argv, 0, stdout, "")

    metadata_digest = "7" * 64
    check = build_kubernetes_client_check(
        run,
        config=_candidate_config(tmp_path),
        expected_kubeconfig_metadata_digest=metadata_digest,
    )
    context = CheckContext(
        {
            "kubeconfig.metadata.sha256": metadata_digest,
            "runner.config.sha256": "a" * 64,
            "runner.install.sha256": "9" * 64,
        }
    )

    executions = PreflightDag((_tools_runtime_check(), check)).run(context, through_tier=0)

    kubernetes = next(item for item in executions if item.check_id == "kubernetes.client")
    assert kubernetes.passed
    assert kubernetes.evidence["current-context"] == "loom-staging"
    assert kubernetes.evidence["namespace"] == "loom-staging"
    assert kubernetes.evidence["kubeconfig-metadata-digest"] == metadata_digest
    assert len(calls) == 2


def test_registered_kubernetes_client_rejects_metadata_drift_before_commands(
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, ...]] = []
    check = build_kubernetes_client_check(
        lambda argv: calls.append(tuple(argv)) or subprocess.CompletedProcess(argv, 0, "", ""),
        config=_candidate_config(tmp_path),
        expected_kubeconfig_metadata_digest="7" * 64,
    )
    context = CheckContext(
        {
            "kubeconfig.metadata.sha256": "8" * 64,
            "runner.config.sha256": "a" * 64,
            "runner.install.sha256": "9" * 64,
        }
    )

    executions = PreflightDag((_tools_runtime_check(), check)).run(context, through_tier=0)

    kubernetes = next(item for item in executions if item.check_id == "kubernetes.client")
    assert kubernetes.passed is False
    assert kubernetes.evidence["client-digest"] == "0" * 64
    assert calls == []


def test_registered_readonly_authority_rejects_mutation_capability() -> None:
    check = build_readonly_authority_check(
        lambda: ReadonlyAuthorityEvidence(
            principal="loom-rollout-readonly",
            environment="staging",
            namespace="loom-staging",
            kubernetes_verbs=("get", "create"),
            kubernetes_resources=("pods",),
            http_methods=("GET",),
            capability_source_digest="f" * 64,
        )
    )
    context = CheckContext(
        {
            "readonly.principal.sha256": readonly_authority_policy_digest(),
            "runner.config.sha256": "a" * 64,
        }
    )

    executions = PreflightDag((_passing_dependency("runner.install"), check)).run(
        context,
        through_tier=0,
    )

    result = next(item for item in executions if item.check_id == "readonly.authority")
    assert not result.passed
    assert result.evidence["mutation-denied"] is False


def test_registered_capacity_high_water_reports_all_bound_metrics() -> None:
    capacity = StagingCapacity(
        object_count=249_999,
        bytes_used=15 * 1024**3,
        disk_free_percent=21,
        inode_free_percent=22,
    )
    check = build_capacity_high_water_check(lambda: capacity)
    assert check.spec.timeout_seconds == 60
    context = CheckContext(
        {
            "capacity.policy.sha256": staging_capacity_policy_digest(),
            "runner.config.sha256": "a" * 64,
        }
    )

    executions = PreflightDag((_passing_dependency("runner.install"), check)).run(context)

    result = next(item for item in executions if item.check_id == "capacity.high-water")
    assert result.passed
    assert result.evidence == {
        "object-count": 249_999,
        "bytes-used": 15 * 1024**3,
        "disk-free-percent": 21,
        "inode-free-percent": 22,
        "gc-required": True,
        "admission-allowed": True,
        "policy-digest": staging_capacity_policy_digest(),
        "capacity-digest": capacity.evidence_digest,
    }


@pytest.mark.parametrize(
    "capacity",
    [
        StagingCapacity(250_000, 1, 100, 100),
        StagingCapacity(1, 16 * 1024**3, 100, 100),
        StagingCapacity(1, 1, 19, 100),
        StagingCapacity(1, 1, 100, 19),
    ],
)
def test_registered_capacity_high_water_fails_each_admission_boundary(
    capacity: StagingCapacity,
) -> None:
    check = build_capacity_high_water_check(lambda: capacity)
    context = CheckContext(
        {
            "capacity.policy.sha256": staging_capacity_policy_digest(),
            "runner.config.sha256": "a" * 64,
        }
    )

    executions = PreflightDag((_passing_dependency("runner.install"), check)).run(context)

    result = next(item for item in executions if item.check_id == "capacity.high-water")
    assert result.passed is False
    assert result.evidence["admission-allowed"] is False


def test_registered_capacity_high_water_rejects_policy_drift_before_inventory() -> None:
    calls: list[object] = []
    check = build_capacity_high_water_check(
        lambda: calls.append(object()) or StagingCapacity(0, 0, 100, 100)
    )
    context = CheckContext(
        {
            "capacity.policy.sha256": "0" * 64,
            "runner.config.sha256": "a" * 64,
        }
    )

    executions = PreflightDag((_passing_dependency("runner.install"), check)).run(context)

    result = next(item for item in executions if item.check_id == "capacity.high-water")
    assert result.passed is False
    assert result.evidence["capacity-digest"] == "0" * 64
    assert calls == []


def test_registered_user_manager_check_runs_through_shared_dag() -> None:
    outputs = iter(("255.4-1ubuntu8.14\n", "yes\n", f"{BOOT_ID}\n"))
    clock = iter((5.0, 5.125))

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(argv, 0, next(outputs), "")

    check = build_systemd_user_manager_check(
        run,
        service_uid=1001,
        monotonic=lambda: next(clock),
    )
    context = CheckContext({"runner.config.sha256": "a" * 64, "service.uid": 1001})

    executions = PreflightDag((_tools_runtime_check(), check)).run(
        context,
        through_tier=0,
        now=lambda: datetime(2026, 7, 19, tzinfo=UTC),
    )

    by_id = {execution.check_id: execution for execution in executions}
    assert by_id["tools.runtime"].passed
    manager = by_id["systemd.user-manager"]
    assert manager.passed
    assert manager.evidence == {
        "version": "255.4-1ubuntu8.14",
        "linger": True,
        "boot-id": BOOT_ID,
        "rpc-latency-ms": 125,
        "rpc-budget-ms": 5000,
        "readiness-digest": manager.evidence["readiness-digest"],
    }
    assert len(str(manager.evidence["readiness-digest"])) == 64


def test_registered_user_manager_check_fails_closed_without_raw_output() -> None:
    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(argv, 1, "token=do-not-echo", "secret")

    check = build_systemd_user_manager_check(run, service_uid=1001)
    context = CheckContext({"runner.config.sha256": "a" * 64, "service.uid": 1001})

    executions = PreflightDag((_tools_runtime_check(), check)).run(context, through_tier=0)
    manager = next(item for item in executions if item.check_id == "systemd.user-manager")

    assert not manager.passed
    assert "token" not in str(dict(manager.evidence))
    assert "secret" not in str(dict(manager.evidence))


def _external_supervisor_context() -> CheckContext:
    return CheckContext(
        {
            "candidate.sha": "1" * 40,
            "candidate.tree": "2" * 40,
            "environment": "staging",
            "external-supervisor.unit-directory": EXTERNAL_SUPERVISOR_UNIT_DIRECTORY,
            "runner.config.sha256": "a" * 64,
            "schema.revision": "0066",
            "database.schema.revision": "0066",
            "service.uid": 1001,
        }
    )


def _external_supervisor_snapshot() -> ExternalSupervisorPredecessorSnapshot:
    return ExternalSupervisorPredecessorSnapshot(
        kind="legacy-manifest",
        authority_digest="b" * 64,
        pointer_digest=EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
        unit_sha256={
            "loom-autoscaler-gb10-staging.service": "c" * 64,
            "loom-autoscaler-gb10-staging.timer": "d" * 64,
        },
        live_evidence_digest="e" * 64,
        pending_transition_digest=hashlib.sha256(b"{}").hexdigest(),
        transition_clear=True,
        runtime_ready=True,
        pool_identity_digest="f" * 64,
    )


def _external_supervisor_snapshots(
    gb10: ExternalSupervisorPredecessorSnapshot,
) -> dict[str, ExternalSupervisorPredecessorSnapshot]:
    if gb10.kind == "absent":
        oldlab = replace(gb10, live_evidence_digest="4" * 64)
    else:
        oldlab = replace(
            gb10,
            authority_digest="1" * 64,
            unit_sha256={
                "loom-autoscaler-oldlab-staging.service": "2" * 64,
                "loom-autoscaler-oldlab-staging.timer": "3" * 64,
            },
            live_evidence_digest="4" * 64,
        )
    return {
        "gx10-01c7": gb10,
        "TRT-EAI-OLDLAB-1": oldlab,
    }


def test_registered_external_supervisor_predecessor_binds_legacy_authority() -> None:
    snapshot = _external_supervisor_snapshot()
    check = build_external_supervisor_predecessor_check(
        lambda _context: _external_supervisor_snapshots(snapshot)
    )

    assert check.spec.timeout_seconds == 3600
    probe = check.operations[CheckOperation.PROBE](_external_supervisor_context())

    assert probe.passed
    assert probe.evidence["authority-kind"] == "legacy-manifest"
    assert probe.evidence["unit-digests"] == dict(snapshot.unit_sha256)
    assert probe.evidence["transition-clear"] is True
    assert probe.evidence["pool-identity-digest"] == snapshot.pool_identity_digest
    assert "database.schema.revision" in check.spec.input_keys


def test_registered_external_supervisor_predecessor_binds_every_controller() -> None:
    gb10 = _external_supervisor_snapshot()
    snapshots = _external_supervisor_snapshots(gb10)
    oldlab = snapshots["TRT-EAI-OLDLAB-1"]
    check = build_external_supervisor_predecessor_check(lambda _context: snapshots)

    probe = check.operations[CheckOperation.PROBE](_external_supervisor_context())

    assert probe.passed
    controller_identities = probe.evidence["controller-identity-bindings"]
    controller_runtime = probe.evidence["controller-runtime-observations"]
    assert isinstance(controller_identities, dict)
    assert isinstance(controller_runtime, dict)
    assert controller_identities["gx10-01c7/authority-digest"] == gb10.authority_digest
    assert controller_runtime["gx10-01c7/runtime-state"] == "ready"
    assert controller_identities["TRT-EAI-OLDLAB-1/authority-digest"] == (oldlab.authority_digest)
    assert (
        controller_identities["TRT-EAI-OLDLAB-1/unit/loom-autoscaler-oldlab-staging.timer"]
        == oldlab.unit_sha256["loom-autoscaler-oldlab-staging.timer"]
    )


def test_registered_external_supervisor_predecessor_rejects_missing_controller() -> None:
    check = build_external_supervisor_predecessor_check(
        lambda _context: {"gx10-01c7": _external_supervisor_snapshot()}
    )

    probe = check.operations[CheckOperation.PROBE](_external_supervisor_context())

    assert not probe.passed
    assert probe.evidence["controller-identity-bindings"] == {}
    assert probe.evidence["controller-runtime-observations"] == {}


def test_registered_external_supervisor_predecessor_accepts_absent_but_rejects_malformed_or_pending() -> (
    None
):
    # First introduction of the supervisor: an absent predecessor (no units, the
    # absent authority/pointer digests, clear transition) is a legitimate
    # bootstrap and the check passes.
    absent = ExternalSupervisorPredecessorSnapshot(
        kind="absent",
        authority_digest=EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
        pointer_digest=EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
        unit_sha256={},
        live_evidence_digest="e" * 64,
        pending_transition_digest=hashlib.sha256(b"{}").hexdigest(),
        transition_clear=True,
        runtime_ready=True,
        pool_identity_digest="f" * 64,
    )
    check = build_external_supervisor_predecessor_check(
        lambda _context: _external_supervisor_snapshots(absent)
    )
    assert check.operations[CheckOperation.PROBE](_external_supervisor_context()).passed

    # But absent stays tightly gated: it may not carry units, and it must carry
    # the absent authority digest (a present predecessor can never masquerade).
    with pytest.raises(ValueError, match="snapshot is invalid"):
        replace(absent, unit_sha256={"loom-autoscaler-gb10-staging.service": "a" * 64})
    with pytest.raises(ValueError, match="snapshot is invalid"):
        replace(absent, authority_digest="a" * 64)

    pending = replace(
        _external_supervisor_snapshot(),
        pending_transition_digest="f" * 64,
        transition_clear=False,
    )
    check = build_external_supervisor_predecessor_check(
        lambda _context: _external_supervisor_snapshots(pending)
    )
    probe = check.operations[CheckOperation.PROBE](_external_supervisor_context())
    assert not probe.passed
    assert probe.evidence["pending-transition-digest"] == "f" * 64


def _credential_sources(tmp_path: Path) -> tuple[CredentialProbeSource, ...]:
    payloads = {
        "admin": b"admin-private-value\n",
        "worker": b"worker-private-value\n",
        "service": b"service-private-value\n",
        "catalog": b"CATALOG_PASSWORD=private-value\n",
    }
    sources: list[CredentialProbeSource] = []
    for label, payload in payloads.items():
        path = tmp_path / label
        path.write_bytes(payload)
        path.chmod(0o600)
        sources.append(
            CredentialProbeSource(
                label=label,
                path=path,
                expected_content_fingerprint=(
                    safe_content_fingerprint(payload.strip()) if label == "admin" else None
                ),
            )
        )
    return tuple(sources)


def _credential_context(sources: tuple[CredentialProbeSource, ...]) -> CheckContext:
    return CheckContext(
        {
            "protected-inputs.sha256": credential_source_set_digest(sources),
            "runner.config.sha256": "a" * 64,
            "secret-fingerprints": {
                source.label: source.expected_content_fingerprint
                for source in sources
                if source.expected_content_fingerprint is not None
            },
            "service.uid": os.getuid(),
        }
    )


def test_registered_credentials_check_attests_metadata_without_secret_values(
    tmp_path: Path,
) -> None:
    sources = _credential_sources(tmp_path)
    check = build_credentials_metadata_check(
        sources=sources,
        service_uid=os.getuid(),
    )
    dag = PreflightDag((_passing_dependency("runner.install"), check))

    result = next(
        item
        for item in dag.run(_credential_context(sources))
        if item.check_id == check.spec.check_id
    )

    assert result.passed
    assert result.evidence["failed-sources"] == {}
    assert set(result.evidence["stable-metadata-fingerprints"]) == {
        "admin",
        "worker",
        "service",
        "catalog",
    }
    assert result.evidence["rotating-metadata-fingerprints"] == {}
    rendered = json.dumps(dict(result.evidence), sort_keys=True)
    assert "private-value" not in rendered
    assert "CATALOG_PASSWORD" not in rendered
    assert str(tmp_path) not in rendered


def test_registered_credentials_check_normalizes_expected_token_line_ending(
    tmp_path: Path,
) -> None:
    sources = _credential_sources(tmp_path)
    check = build_credentials_metadata_check(
        sources=sources,
        service_uid=os.getuid(),
    )

    result = next(
        item
        for item in PreflightDag((_passing_dependency("runner.install"), check)).run(
            _credential_context(sources)
        )
        if item.check_id == check.spec.check_id
    )

    assert result.passed
    assert result.evidence["content-fingerprints"]["admin"] == safe_content_fingerprint(
        b"admin-private-value"
    )


def test_registered_credentials_check_reports_all_unsafe_sources(
    tmp_path: Path,
) -> None:
    original = _credential_sources(tmp_path)
    sources = tuple(
        CredentialProbeSource(
            label=source.label,
            path=source.path,
            expected_content_fingerprint=(
                "sha256:000000000000 len=1" if source.label == "admin" else None
            ),
        )
        for source in original
    )
    (tmp_path / "worker").chmod(0o660)
    check = build_credentials_metadata_check(
        sources=sources,
        service_uid=os.getuid(),
    )
    dag = PreflightDag((_passing_dependency("runner.install"), check))

    result = next(
        item
        for item in dag.run(_credential_context(sources))
        if item.check_id == check.spec.check_id
    )

    assert not result.passed
    assert result.evidence["failed-sources"] == {
        "admin": "content-fingerprint-mismatch",
        "worker": "authority-or-stability-failed",
    }
    assert set(result.evidence["stable-metadata-fingerprints"]) == {"service", "catalog"}


def test_registered_credentials_check_rejects_source_binding_drift_before_read(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sources = _credential_sources(tmp_path)
    calls: list[Path] = []

    def unexpected_read(path: Path, **_kwargs):
        calls.append(path)
        raise AssertionError("credential files must not be read after binding drift")

    monkeypatch.setattr(
        "loom_cli.rollout.preflight_registered_checks.read_trusted_file",
        unexpected_read,
    )
    check = build_credentials_metadata_check(
        sources=sources,
        service_uid=os.getuid(),
    )
    context = _credential_context(sources)
    drifted = CheckContext({**dict(context.bindings), "protected-inputs.sha256": "b" * 64})
    dag = PreflightDag((_passing_dependency("runner.install"), check))

    result = next(item for item in dag.run(drifted) if item.check_id == check.spec.check_id)

    assert not result.passed
    assert set(result.evidence["failed-sources"]) == {
        "admin",
        "worker",
        "service",
        "catalog",
    }
    assert calls == []


def test_registered_gb10_readiness_check_returns_bound_fleet_evidence() -> None:
    targets = (
        GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service"),
        GB10ProbeTarget("trt-gb10-2", "loom-gb10-node-agent.service"),
    )
    boot_ids = {
        "trt-gb10-1": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        "trt-gb10-2": "11111111-2222-4333-8444-555555555555",
    }

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        host = argv[-2]
        payload = {
            "schema_version": 1,
            "boot_id": boot_ids[host],
            "manager_version": "255",
            "linger_enabled": True,
            "timer_enabled": True,
            "service": {
                "LoadState": "loaded",
                "Type": "oneshot",
                "Result": "success",
                "ExecMainStatus": "0",
                "ActiveState": "inactive",
                "SubState": "dead",
                "NeedDaemonReload": "no",
            },
            "timer": {
                "LoadState": "loaded",
                "ActiveState": "active",
                "SubState": "waiting",
                "Unit": "loom-gb10-node-agent.service",
                "NeedDaemonReload": "no",
            },
        }
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")

    check = build_gb10_host_readiness_check(
        run,
        targets=targets,
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
        max_concurrency=2,
    )
    context = CheckContext(
        {
            "runner.config.sha256": "a" * 64,
            "gb10.inventory-digest": gb10_target_inventory_digest(targets),
        }
    )
    dag = PreflightDag(
        (
            _passing_dependency("gb10.ssh-topology"),
            _passing_dependency("systemd.user-manager"),
            check,
        )
    )

    result = next(item for item in dag.run(context) if item.check_id == check.spec.check_id)

    assert result.passed
    assert result.evidence["boot-ids"] == boot_ids
    assert result.evidence["failed-hosts"] == {}
    assert result.evidence["host-count"] == 2


def test_registered_gb10_readiness_check_rejects_inventory_drift_without_ssh() -> None:
    target = GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service")
    calls: list[tuple[str, ...]] = []

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, "{}", "")

    check = build_gb10_host_readiness_check(
        run,
        targets=(target,),
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
    )
    context = CheckContext({"runner.config.sha256": "a" * 64, "gb10.inventory-digest": "b" * 64})
    dag = PreflightDag(
        (
            _passing_dependency("gb10.ssh-topology"),
            _passing_dependency("systemd.user-manager"),
            check,
        )
    )

    result = next(item for item in dag.run(context) if item.check_id == check.spec.check_id)

    assert not result.passed
    assert result.evidence["failed-hosts"] == {"trt-gb10-1": "gb10.host-readiness.failed"}
    assert calls == []


def test_registered_gb10_ssh_topology_binds_files_and_reports_all_hosts(
    tmp_path: Path,
) -> None:
    targets = (
        GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service"),
        GB10ProbeTarget("trt-gb10-2", "loom-gb10-node-agent.service"),
    )
    ssh_config = tmp_path / "ssh-config"
    identity = tmp_path / "identity"
    ssh_config.write_bytes(b"Host trt-gb10-*\n  User qianyi\n")
    identity.write_bytes(b"private-test-material")
    ssh_config.chmod(0o600)
    identity.chmod(0o600)
    service_uid = os.getuid()
    config_digest = hashlib.sha256(ssh_config.read_bytes()).hexdigest()
    identity_metadata = read_trusted_file(
        identity,
        service_uid=service_uid,
        private=True,
        require_nonempty=True,
    ).metadata_fingerprint

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        host = argv[-2]
        return subprocess.CompletedProcess(argv, 0 if host == "trt-gb10-1" else 255, "", "")

    check = build_gb10_ssh_topology_check(
        run,
        targets=targets,
        ssh_config=ssh_config,
        identity=identity,
        service_uid=service_uid,
        expected_ssh_config_sha256=config_digest,
        expected_identity_metadata_fingerprint=identity_metadata,
        max_concurrency=2,
    )
    context = CheckContext(
        {
            "runner.config.sha256": "a" * 64,
            "service.uid": service_uid,
            "gb10.inventory-digest": gb10_target_inventory_digest(targets),
            "gb10.ssh-config.sha256": config_digest,
            "gb10.identity.metadata-fingerprint": identity_metadata,
        }
    )
    dag = PreflightDag(
        (
            _passing_dependency("credentials.metadata"),
            _passing_dependency("tools.runtime"),
            check,
        )
    )

    result = next(item for item in dag.run(context) if item.check_id == check.spec.check_id)

    assert not result.passed
    assert result.evidence["reachable-hosts"] == {"trt-gb10-1": "reachable"}
    assert result.evidence["failed-hosts"] == {"trt-gb10-2": "unreachable"}


def test_registered_gb10_ssh_topology_rejects_binding_drift_without_ssh(
    tmp_path: Path,
) -> None:
    target = GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service")
    calls: list[tuple[str, ...]] = []
    check = build_gb10_ssh_topology_check(
        lambda argv: calls.append(argv) or subprocess.CompletedProcess(argv, 0, "", ""),
        targets=(target,),
        ssh_config=tmp_path / "missing-config",
        identity=tmp_path / "missing-identity",
        service_uid=os.getuid(),
        expected_ssh_config_sha256="a" * 64,
        expected_identity_metadata_fingerprint="b" * 64,
    )
    context = CheckContext(
        {
            "runner.config.sha256": "a" * 64,
            "service.uid": os.getuid(),
            "gb10.inventory-digest": "f" * 64,
            "gb10.ssh-config.sha256": "a" * 64,
            "gb10.identity.metadata-fingerprint": "b" * 64,
        }
    )
    dag = PreflightDag(
        (
            _passing_dependency("credentials.metadata"),
            _passing_dependency("tools.runtime"),
            check,
        )
    )

    result = next(item for item in dag.run(context) if item.check_id == check.spec.check_id)

    assert not result.passed
    assert result.evidence["failed-hosts"] == {"trt-gb10-1": "unreachable"}
    assert calls == []


def test_registered_gb10_shared_mount_reports_all_drifted_hosts() -> None:
    targets = (
        GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service"),
        GB10ProbeTarget("trt-gb10-2", "loom-gb10-node-agent.service"),
    )
    binding = {
        "service_uid": 1001,
        "service_primary_gid": 1001,
        "consumer_uid": 2005,
        "consumer_primary_gid": 2005,
        "shared_gid": 2007,
        "parent_device": 67,
        "parent_inode": 101,
        "authority_device": 67,
        "authority_inode": 102,
        "repository_device": 67,
        "repository_inode": 103,
    }
    binding_digest = gb10_mount_binding_digest(binding)
    check = build_gb10_shared_mount_check(
        lambda: GB10SharedMountReadiness(
            host_digests={"trt-gb10-1": "a" * 64},
            failed_hosts=("trt-gb10-2",),
        ),
        targets=targets,
        expected_binding_digest=binding_digest,
    )
    context = CheckContext(
        {
            "runner.config.sha256": "a" * 64,
            "gb10.inventory-digest": gb10_target_inventory_digest(targets),
            "gb10.mount-binding.sha256": binding_digest,
        }
    )
    dag = PreflightDag((_passing_dependency("gb10.ssh-topology"), check))

    result = next(item for item in dag.run(context) if item.check_id == check.spec.check_id)

    assert not result.passed
    assert result.evidence["host-digests"] == {"trt-gb10-1": "a" * 64}
    assert result.evidence["failed-hosts"] == {"trt-gb10-2": "mount-drift"}
    assert result.evidence["binding-digest"] == binding_digest


def test_registered_gb10_shared_mount_rejects_binding_drift_without_probe() -> None:
    target = GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service")
    calls: list[object] = []
    check = build_gb10_shared_mount_check(
        lambda: (
            calls.append(object())  # type: ignore[arg-type,return-value]
            or GB10SharedMountReadiness(host_digests={"trt-gb10-1": "a" * 64}, failed_hosts=())
        ),
        targets=(target,),
        expected_binding_digest="b" * 64,
    )
    context = CheckContext(
        {
            "runner.config.sha256": "a" * 64,
            "gb10.inventory-digest": gb10_target_inventory_digest((target,)),
            "gb10.mount-binding.sha256": "c" * 64,
        }
    )
    dag = PreflightDag((_passing_dependency("gb10.ssh-topology"), check))

    result = next(item for item in dag.run(context) if item.check_id == check.spec.check_id)

    assert not result.passed
    assert result.evidence["failed-hosts"] == {"trt-gb10-1": "mount-drift"}
    assert calls == []


def _backup_lease(now: datetime) -> BackupLease:
    return BackupLease(
        lease_id="lease-12345678",
        source_request_id="req-12345678",
        manifest_sha256="a" * 64,
        component_sha256={"postgres": "b" * 64, "authority": "c" * 64},
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=7,
        db_snapshot_identity="lsn:0/16B6C50",
        schema_revision="0066",
        object_inventory_root="d" * 64,
        created_at=now - timedelta(minutes=20),
        restore_verified_at=now - timedelta(minutes=10),
        expires_at=now + timedelta(hours=2),
    )


def test_registered_backup_rotation_capacity_reports_retirement_limit() -> None:
    state = BackupRotationState(
        generation=7,
        retirements=(
            BackupRetirementRecord(
                payload_id="payload-failed01",
                request_id="req-failed0001",
                bundle_name="20260719T180000Z-req-failed0001",
                reason="failed",
                manifest_sha256=None,
            ),
            BackupRetirementRecord(
                payload_id="payload-failed02",
                request_id="req-failed0002",
                bundle_name="20260719T190000Z-req-failed0002",
                reason="failed",
                manifest_sha256=None,
            ),
        ),
    )
    check = build_backup_rotation_capacity_check(
        lambda: state,
        expected_rotation_digest=state.evidence_digest,
    )
    dag = PreflightDag(
        (
            _passing_dependency("capacity.high-water"),
            _passing_dependency("lifecycle.launch-cancel"),
            check,
        )
    )

    result = next(
        item
        for item in dag.run(
            CheckContext(
                {
                    "backup.rotation.sha256": state.evidence_digest,
                    "runner.config.sha256": "a" * 64,
                }
            )
        )
        if item.check_id == check.spec.check_id
    )

    assert not result.passed
    assert result.failure_code == "backup.rotation-capacity.blocked"
    assert result.evidence["payload-count"] == 2
    assert result.evidence["retirement-count"] == 2
    assert result.evidence["blockers"] == {"transient-limit": "reached"}


def _run_rotation_capacity_check(check: RegisteredCheck, rotation_digest: str) -> CheckExecution:
    dag = PreflightDag(
        (
            _passing_dependency("capacity.high-water"),
            _passing_dependency("lifecycle.launch-cancel"),
            check,
        )
    )
    return next(
        item
        for item in dag.run(
            CheckContext(
                {
                    "backup.rotation.sha256": rotation_digest,
                    "runner.config.sha256": "a" * 64,
                }
            )
        )
        if item.check_id == "backup.rotation-capacity"
    )


def _reserved_manifest_verified_state() -> BackupRotationState:
    # Mirror the coordinator: reserve the candidate (CREATING) then record the
    # manifest (MANIFEST_VERIFIED) -- the exact phase the restore rehearsal
    # observes, since it runs after record_manifest_verified.
    reserved = begin_candidate(
        BackupRotationState(),
        payload_id="payload-own000001",
        request_id="req-own0000000001",
        bundle_name="20260724T210000Z-req-own0000000001",
        created_at=datetime(2026, 7, 24, 21, tzinfo=UTC),
    ).state
    return record_manifest_verified(
        reserved,
        payload_id="payload-own000001",
        manifest_sha256="d" * 64,
    ).state


def test_registered_backup_rotation_capacity_permits_own_reserved_candidate() -> None:
    # The checkpoint coordinator reserves this backup's own candidate and records
    # its manifest (phase MANIFEST_VERIFIED) before the restore rehearsal. The
    # gating admission check (permit=False) must still block on it, but the
    # restore rehearsal (permit=True) must tolerate the backup's own reservation
    # so it can attest.
    state = _reserved_manifest_verified_state()
    assert state.candidate is not None
    assert state.candidate.phase is BackupPayloadPhase.MANIFEST_VERIFIED

    gating = _run_rotation_capacity_check(
        build_backup_rotation_capacity_check(
            lambda: state,
            expected_rotation_digest=state.evidence_digest,
        ),
        state.evidence_digest,
    )
    assert not gating.passed
    assert gating.evidence["blockers"] == {"candidate": "present"}

    rehearsal = _run_rotation_capacity_check(
        build_backup_rotation_capacity_check(
            lambda: state,
            expected_rotation_digest=state.evidence_digest,
            permit_reserved_candidate=True,
        ),
        state.evidence_digest,
    )
    assert rehearsal.passed
    assert rehearsal.evidence["candidate-present"] is True
    assert rehearsal.evidence["blockers"] == {}


def _rolling_backup_state() -> BackupRotationState:
    # A first backup promoted to active, then this backup's own candidate
    # reserved and manifest-recorded: the rolling state the restore rehearsal
    # observes (prior active + own candidate => payload_count 2).
    now = datetime(2026, 7, 24, 21, tzinfo=UTC)
    state = begin_candidate(
        BackupRotationState(),
        payload_id="payload-active0001",
        request_id="req-active00000001",
        bundle_name="20260724T200000Z-req-active00000001",
        created_at=now,
    ).state
    state = record_manifest_verified(
        state, payload_id="payload-active0001", manifest_sha256="e" * 64
    ).state
    lease = BackupLease(
        lease_id="lease-active000000",
        source_request_id="req-active00000001",
        manifest_sha256="e" * 64,
        component_sha256={"postgres": "b" * 64, "authority": "c" * 64},
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=7,
        db_snapshot_identity="lsn:0/16B6C50",
        schema_revision="0066",
        object_inventory_root="d" * 64,
        created_at=now - timedelta(minutes=20),
        restore_verified_at=now - timedelta(minutes=10),
        expires_at=now + timedelta(hours=2),
    )
    state = record_restore_verified(state, payload_id="payload-active0001", lease=lease).state
    state = promote_candidate(state, payload_id="payload-active0001").state
    state = begin_candidate(
        state,
        payload_id="payload-own000001",
        request_id="req-own0000000001",
        bundle_name="20260724T210000Z-req-own0000000001",
        created_at=now,
    ).state
    return record_manifest_verified(
        state, payload_id="payload-own000001", manifest_sha256="f" * 64
    ).state


def test_registered_backup_rotation_capacity_permits_rolling_own_candidate() -> None:
    # On a rolling backup the prior active plus this backup's own candidate reach
    # the transient limit of two in the rehearsal. Gating (permit=False) still
    # blocks; the rehearsal (permit=True) tolerates it because promote retires
    # the prior active and restores capacity.
    state = _rolling_backup_state()
    assert state.active is not None and state.candidate is not None
    assert state.payload_count == 2

    gating = _run_rotation_capacity_check(
        build_backup_rotation_capacity_check(
            lambda: state, expected_rotation_digest=state.evidence_digest
        ),
        state.evidence_digest,
    )
    assert not gating.passed
    assert gating.evidence["blockers"].get("transient-limit") == "reached"

    rehearsal = _run_rotation_capacity_check(
        build_backup_rotation_capacity_check(
            lambda: state,
            expected_rotation_digest=state.evidence_digest,
            permit_reserved_candidate=True,
        ),
        state.evidence_digest,
    )
    assert rehearsal.passed
    assert rehearsal.evidence["blockers"] == {}
    assert rehearsal.evidence["active-present"] is True
    assert rehearsal.evidence["payload-count"] == 2


def test_registered_backup_rotation_capacity_permit_still_blocks_digest_drift() -> None:
    # permit=True must NOT tolerate a candidate whose state drifts from the
    # pinned expectation (an unexpected/foreign candidate is not the backup's own).
    reserved = begin_candidate(
        BackupRotationState(),
        payload_id="payload-own000001",
        request_id="req-own0000000001",
        bundle_name="20260724T210000Z-req-own0000000001",
        created_at=datetime(2026, 7, 24, 21, tzinfo=UTC),
    ).state
    drifted = begin_candidate(
        BackupRotationState(generation=99),
        payload_id="payload-own000001",
        request_id="req-own0000000001",
        bundle_name="20260724T210000Z-req-own0000000001",
        created_at=datetime(2026, 7, 24, 21, tzinfo=UTC),
    ).state

    result = _run_rotation_capacity_check(
        build_backup_rotation_capacity_check(
            lambda: drifted,
            expected_rotation_digest=reserved.evidence_digest,
            permit_reserved_candidate=True,
        ),
        reserved.evidence_digest,
    )
    assert not result.passed
    assert result.evidence["blockers"]["rotation-digest"] == "drifted"
    assert result.evidence["blockers"].get("candidate") == "present"


def test_registered_backup_rotation_capacity_rejects_digest_drift() -> None:
    expected = BackupRotationState()
    observed = BackupRotationState(generation=1)
    check = build_backup_rotation_capacity_check(
        lambda: observed,
        expected_rotation_digest=expected.evidence_digest,
    )
    dag = PreflightDag(
        (
            _passing_dependency("capacity.high-water"),
            _passing_dependency("lifecycle.launch-cancel"),
            check,
        )
    )

    result = next(
        item
        for item in dag.run(
            CheckContext(
                {
                    "backup.rotation.sha256": expected.evidence_digest,
                    "runner.config.sha256": "a" * 64,
                }
            )
        )
        if item.check_id == check.spec.check_id
    )

    assert not result.passed
    assert result.evidence["blockers"] == {"rotation-digest": "drifted"}


def _backup_lease_context(lease: BackupLease) -> CheckContext:
    return CheckContext(
        {
            "runner.config.sha256": "a" * 64,
            "backup.component-set.sha256": component_set_digest(lease.component_sha256),
            "backup.lease.sha256": lease.evidence_digest,
            "backup.manifest.sha256": lease.manifest_sha256,
            "backup.source-request": lease.source_request_id,
            "db.snapshot-identity": lease.db_snapshot_identity,
            "environment": lease.environment,
            "namespace": lease.namespace,
            "object.inventory-root": lease.object_inventory_root,
            "schema.revision": lease.schema_revision,
            "staging.mutation-epoch": lease.mutation_epoch,
        }
    )


def _backup_lease_check(lease: BackupLease, now: datetime):
    return build_backup_lease_eligibility_check(
        lambda: lease,
        now=lambda: now,
        expected_lease_digest=lease.evidence_digest,
        source_request_id=lease.source_request_id,
        environment=lease.environment,
        namespace=lease.namespace,
        mutation_epoch=lease.mutation_epoch,
        db_snapshot_identity=lease.db_snapshot_identity,
        schema_revision=lease.schema_revision,
        object_inventory_root=lease.object_inventory_root,
        manifest_sha256=lease.manifest_sha256,
        component_sha256=lease.component_sha256,
    )


def test_registered_backup_lease_accepts_exact_unchanged_restored_authority() -> None:
    now = datetime(2026, 7, 19, 18, tzinfo=UTC)
    lease = _backup_lease(now)
    check = _backup_lease_check(lease, now)
    dag = PreflightDag(
        (
            _passing_dependency("backup.rotation-capacity"),
            _passing_dependency("kubernetes.client"),
            check,
        )
    )

    result = next(
        item
        for item in dag.run(_backup_lease_context(lease))
        if item.check_id == check.spec.check_id
    )

    assert result.passed
    assert result.evidence["admission-allowed"] is True
    assert result.evidence["eligible"] is True
    assert result.evidence["strategy"] == "reuse"
    assert result.evidence["blockers"] == {}
    assert result.evidence["lease-digest"] == lease.evidence_digest


def test_registered_backup_lease_selects_fresh_when_active_lease_is_absent() -> None:
    now = datetime(2026, 7, 19, 18, tzinfo=UTC)
    expected = _backup_lease(now)
    check = build_backup_lease_eligibility_check(
        lambda: None,
        now=lambda: now,
        expected_lease_digest=expected.evidence_digest,
        source_request_id=expected.source_request_id,
        environment=expected.environment,
        namespace=expected.namespace,
        mutation_epoch=expected.mutation_epoch,
        db_snapshot_identity=expected.db_snapshot_identity,
        schema_revision=expected.schema_revision,
        object_inventory_root=expected.object_inventory_root,
        manifest_sha256=expected.manifest_sha256,
        component_sha256=expected.component_sha256,
    )
    dag = PreflightDag(
        (
            _passing_dependency("backup.rotation-capacity"),
            _passing_dependency("kubernetes.client"),
            check,
        )
    )

    result = next(
        item
        for item in dag.run(_backup_lease_context(expected))
        if item.check_id == check.spec.check_id
    )

    assert result.passed
    assert result.evidence["admission-allowed"] is True
    assert result.evidence["eligible"] is False
    assert result.evidence["strategy"] == "fresh"
    assert result.evidence["blockers"] == {"lease-absent": "fresh-required"}


def test_registered_backup_lease_selects_fresh_when_lease_expired() -> None:
    now = datetime(2026, 7, 19, 18, tzinfo=UTC)
    expected = _backup_lease(now)
    expired = replace(expected, expires_at=now)
    check = _backup_lease_check(expired, now)
    dag = PreflightDag(
        (
            _passing_dependency("backup.rotation-capacity"),
            _passing_dependency("kubernetes.client"),
            check,
        )
    )

    result = next(
        item
        for item in dag.run(_backup_lease_context(expired))
        if item.check_id == check.spec.check_id
    )

    assert result.passed
    assert result.evidence["eligible"] is False
    assert result.evidence["strategy"] == "fresh"
    assert result.evidence["blockers"] == {"freshness": "fresh-required"}


def test_registered_backup_lease_rejects_unreadable_authority() -> None:
    now = datetime(2026, 7, 19, 18, tzinfo=UTC)
    expected = _backup_lease(now)

    def unreadable() -> BackupLease | None:
        raise OSError("authority unavailable")

    check = build_backup_lease_eligibility_check(
        unreadable,
        now=lambda: now,
        expected_lease_digest=expected.evidence_digest,
        source_request_id=expected.source_request_id,
        environment=expected.environment,
        namespace=expected.namespace,
        mutation_epoch=expected.mutation_epoch,
        db_snapshot_identity=expected.db_snapshot_identity,
        schema_revision=expected.schema_revision,
        object_inventory_root=expected.object_inventory_root,
        manifest_sha256=expected.manifest_sha256,
        component_sha256=expected.component_sha256,
    )
    dag = PreflightDag(
        (
            _passing_dependency("backup.rotation-capacity"),
            _passing_dependency("kubernetes.client"),
            check,
        )
    )

    result = next(
        item
        for item in dag.run(_backup_lease_context(expected))
        if item.check_id == check.spec.check_id
    )

    assert not result.passed
    assert result.evidence["admission-allowed"] is False
    assert result.evidence["strategy"] == "blocked"
    assert result.evidence["blockers"] == {"lease-unavailable": "blocked"}


def test_registered_backup_lease_rejects_runtime_authority_failure() -> None:
    now = datetime(2026, 7, 19, 18, tzinfo=UTC)
    expected = _backup_lease(now)

    def unreadable() -> BackupLease | None:
        raise RuntimeError("authority unavailable")

    check = build_backup_lease_eligibility_check(
        unreadable,
        now=lambda: now,
        expected_lease_digest=expected.evidence_digest,
        source_request_id=expected.source_request_id,
        environment=expected.environment,
        namespace=expected.namespace,
        mutation_epoch=expected.mutation_epoch,
        db_snapshot_identity=expected.db_snapshot_identity,
        schema_revision=expected.schema_revision,
        object_inventory_root=expected.object_inventory_root,
        manifest_sha256=expected.manifest_sha256,
        component_sha256=expected.component_sha256,
    )
    dag = PreflightDag(
        (
            _passing_dependency("backup.rotation-capacity"),
            _passing_dependency("kubernetes.client"),
            check,
        )
    )

    result = next(
        item
        for item in dag.run(_backup_lease_context(expected))
        if item.check_id == check.spec.check_id
    )

    assert not result.passed
    assert result.evidence["blockers"] == {"lease-unavailable": "blocked"}


def test_registered_backup_lease_rejects_context_drift_without_reading_lease() -> None:
    now = datetime(2026, 7, 19, 18, tzinfo=UTC)
    lease = _backup_lease(now)
    calls: list[object] = []
    check = build_backup_lease_eligibility_check(
        lambda: calls.append(object()) or lease,
        now=lambda: now,
        expected_lease_digest=lease.evidence_digest,
        source_request_id=lease.source_request_id,
        environment=lease.environment,
        namespace=lease.namespace,
        mutation_epoch=lease.mutation_epoch,
        db_snapshot_identity=lease.db_snapshot_identity,
        schema_revision=lease.schema_revision,
        object_inventory_root=lease.object_inventory_root,
        manifest_sha256=lease.manifest_sha256,
        component_sha256=lease.component_sha256,
    )
    bindings = dict(_backup_lease_context(lease).bindings)
    bindings["staging.mutation-epoch"] = 8
    dag = PreflightDag(
        (
            _passing_dependency("backup.rotation-capacity"),
            _passing_dependency("kubernetes.client"),
            check,
        )
    )

    result = next(
        item for item in dag.run(CheckContext(bindings)) if item.check_id == check.spec.check_id
    )

    assert not result.passed
    assert result.evidence["blockers"] == {"input-binding": "blocked"}
    assert calls == []


def test_registered_lifecycle_check_runs_shared_protocol_self_test() -> None:
    from loom_cli.rollout.operator.systemd import SystemdLaunchCancelEvidence

    check = build_lifecycle_launch_cancel_check(
        runtime_test=lambda: SystemdLaunchCancelEvidence(
            ready=True,
            launched=True,
            cancelled=True,
            unit_absent=True,
            launch_latency_ms=11,
            cancel_latency_ms=9,
            latency_budget_ms=15_000,
            evidence_digest="e" * 64,
        )
    )
    context = CheckContext(
        {
            "runner.config.sha256": "a" * 64,
            "candidate.sha": "b" * 40,
            "lifecycle.protocol.sha256": lifecycle_protocol_digest(),
        }
    )
    dag = PreflightDag((_passing_dependency("systemd.user-manager"), check))

    result = next(item for item in dag.run(context) if item.check_id == check.spec.check_id)

    assert result.passed
    assert result.evidence["ready"] is True
    assert result.evidence["scenario-count"] == 4
    assert result.evidence["rejection-count"] == 6
    assert result.evidence["runtime-ready"] is True
    assert result.evidence["unit-absent"] is True


def test_registered_lifecycle_check_rejects_protocol_binding_drift() -> None:
    calls: list[object] = []
    check = build_lifecycle_launch_cancel_check(
        lambda: calls.append(object()),  # type: ignore[arg-type,return-value]
        lambda: calls.append(object()),  # type: ignore[arg-type,return-value]
    )
    context = CheckContext(
        {
            "runner.config.sha256": "a" * 64,
            "candidate.sha": "b" * 40,
            "lifecycle.protocol.sha256": "f" * 64,
        }
    )
    dag = PreflightDag((_passing_dependency("systemd.user-manager"), check))

    result = next(item for item in dag.run(context) if item.check_id == check.spec.check_id)

    assert not result.passed
    assert result.evidence["scenario-count"] == 0
    assert calls == []


def test_registered_lifecycle_check_fails_closed_without_runtime_probe() -> None:
    check = build_lifecycle_launch_cancel_check()
    context = CheckContext(
        {
            "candidate.sha": "b" * 40,
            "runner.config.sha256": "a" * 64,
            "lifecycle.protocol.sha256": lifecycle_protocol_digest(),
        }
    )
    dag = PreflightDag((_passing_dependency("systemd.user-manager"), check))

    result = next(item for item in dag.run(context) if item.check_id == check.spec.check_id)

    assert not result.passed
    assert result.evidence["runtime-ready"] is False
    assert result.evidence["runtime-digest"] == "0" * 64


def test_registered_migration_plan_binds_exact_candidate_graph_and_policy() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    policy = repo_root / "config/staging-migration-policy.json"
    policy_digest = hashlib.sha256(policy.read_bytes()).hexdigest()
    check = build_migration_plan_check(
        alembic_ini=repo_root / "migrations/alembic.ini",
        expected_candidate_sha="1" * 40,
        expected_policy_digest=policy_digest,
        policy_path=policy,
    )
    context = CheckContext(
        {
            "runner.config.sha256": "a" * 64,
            "candidate.sha": "1" * 40,
            "migration.policy.sha256": policy_digest,
        }
    )
    dag = PreflightDag((_passing_dependency("candidate.identity"), check))

    result = next(
        item for item in dag.run(context, through_tier=1) if item.check_id == check.spec.check_id
    )

    assert result.passed
    assert result.evidence["head"] == "0125"
    assert result.evidence["revision-count"] == 126
    assert result.evidence["linear"] is True
    assert result.evidence["policy-digest"] == policy_digest


def test_registered_migration_plan_rejects_candidate_drift_before_graph_read(
    monkeypatch,
) -> None:
    calls: list[object] = []
    monkeypatch.setattr(
        "loom_cli.rollout.preflight_registered_checks.inspect_migration_plan",
        lambda *_args, **_kwargs: calls.append(object()),
    )
    check = build_migration_plan_check(
        alembic_ini=Path("/missing/alembic.ini"),
        expected_candidate_sha="1" * 40,
        expected_policy_digest="a" * 64,
    )
    context = CheckContext(
        {
            "runner.config.sha256": "a" * 64,
            "candidate.sha": "2" * 40,
            "migration.policy.sha256": "a" * 64,
        }
    )
    dag = PreflightDag((_passing_dependency("candidate.identity"), check))

    result = next(
        item for item in dag.run(context, through_tier=1) if item.check_id == check.spec.check_id
    )

    assert not result.passed
    assert result.evidence["plan-digest"] == "0" * 64
    assert calls == []


def test_registered_systemd_render_uses_exact_static_unit_verifier(tmp_path: Path) -> None:
    candidate_root = _secure_static_candidate(tmp_path)
    candidate_sha = "1" * 40
    candidate_tree = "2" * 40
    image_tag = "staging-1111111"
    check = build_systemd_render_check(
        lambda argv: subprocess.CompletedProcess(argv, 0, "", ""),
        candidate_root=candidate_root,
        expected_candidate_sha=candidate_sha,
        expected_candidate_tree=candidate_tree,
        expected_image_tag=image_tag,
        expected_environment="staging",
    )
    context = CheckContext(
        {
            "runner.config.sha256": "a" * 64,
            "candidate.sha": candidate_sha,
            "candidate.tree": candidate_tree,
            "candidate.image-tag": image_tag,
            "environment": "staging",
        }
    )
    dag = PreflightDag(
        (
            _passing_dependency("candidate.identity"),
            _passing_dependency("systemd.user-manager"),
            check,
        )
    )

    result = next(
        item for item in dag.run(context, through_tier=1) if item.check_id == check.spec.check_id
    )

    assert result.passed
    assert result.evidence["failed-units"] == {}
    assert result.evidence["supervisor-artifact-digest"] != "0" * 64
    assert result.evidence["supervisor-profile-sha256"] != "0" * 64
    assert set(result.evidence["supervisor-unit-digests"]) == {
        "loom-autoscaler-gb10-staging.service",
        "loom-autoscaler-gb10-staging.timer",
        "loom-autoscaler-oldlab-staging.service",
        "loom-autoscaler-oldlab-staging.timer",
        "loom-task-image-builder-gb10-staging.service",
        "loom-task-image-builder-gb10-staging.timer",
        "loom-task-image-builder-oldlab-staging.service",
        "loom-task-image-builder-oldlab-staging.timer",
    }
    assert set(result.evidence["supervisor-controller-artifact-digests"]) == {
        "gx10-01c7",
        "TRT-EAI-OLDLAB-1",
    }
    assert set(result.evidence["supervisor-controller-unit-set-digests"]) == {
        "gx10-01c7",
        "TRT-EAI-OLDLAB-1",
    }
    assert set(result.evidence["supervisor-controller-unit-digests"]) == {
        "gx10-01c7/loom-autoscaler-gb10-staging.service",
        "gx10-01c7/loom-autoscaler-gb10-staging.timer",
        "gx10-01c7/loom-task-image-builder-gb10-staging.service",
        "gx10-01c7/loom-task-image-builder-gb10-staging.timer",
        "TRT-EAI-OLDLAB-1/loom-autoscaler-oldlab-staging.service",
        "TRT-EAI-OLDLAB-1/loom-autoscaler-oldlab-staging.timer",
        "TRT-EAI-OLDLAB-1/loom-task-image-builder-oldlab-staging.service",
        "TRT-EAI-OLDLAB-1/loom-task-image-builder-oldlab-staging.timer",
    }
    assert result.evidence["unit-count"] == 11
    assert set(result.evidence["supervisor-script-digests"]) == {
        SCRIPT_PATH,
        TASK_IMAGE_BUILDER_SCRIPT_PATH,
    }


@pytest.mark.parametrize(
    ("binding", "observed"),
    [
        ("candidate.sha", "9" * 40),
        ("candidate.tree", "8" * 40),
        ("candidate.image-tag", "staging-9999999"),
        ("environment", "production"),
    ],
)
def test_registered_systemd_render_rejects_candidate_drift_without_verifier(
    binding: str,
    observed: str,
) -> None:
    calls: list[tuple[str, ...]] = []
    check = build_systemd_render_check(
        lambda argv: calls.append(tuple(argv)) or subprocess.CompletedProcess(argv, 0, "", ""),
        candidate_root=Path("/missing/candidate"),
        expected_candidate_sha="1" * 40,
        expected_candidate_tree="2" * 40,
        expected_image_tag="staging-1111111",
        expected_environment="staging",
    )
    bindings = {
        "runner.config.sha256": "a" * 64,
        "candidate.sha": "1" * 40,
        "candidate.tree": "2" * 40,
        "candidate.image-tag": "staging-1111111",
        "environment": "staging",
    }
    bindings[binding] = observed
    context = CheckContext(
        bindings,
    )
    dag = PreflightDag(
        (
            _passing_dependency("candidate.identity"),
            _passing_dependency("systemd.user-manager"),
            check,
        )
    )

    result = next(
        item for item in dag.run(context, through_tier=1) if item.check_id == check.spec.check_id
    )

    assert not result.passed
    assert result.evidence["unit-count"] == 0
    assert result.evidence["supervisor-artifact-digest"] == "0" * 64
    assert result.evidence["supervisor-profile-sha256"] == "0" * 64
    assert result.evidence["supervisor-script-digests"] == {}
    assert calls == []


def test_registered_gb10_candidate_source_binds_exact_shared_checkout(
    tmp_path: Path,
    monkeypatch,
) -> None:
    candidate_root = _secure_static_candidate(tmp_path)
    target = GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service")
    observed: list[dict[str, object]] = []

    def source_probe(*_args, **kwargs):
        observed.append(kwargs)
        return GB10CandidateSourceReadiness(
            host_digests={"trt-gb10-1": "4" * 64},
            failed_hosts=(),
            candidate_sha="1" * 40,
            candidate_tree="2" * 40,
            unit_set_digest=str(kwargs["unit_set_digest"]),
        )

    monkeypatch.setattr(
        "loom_cli.rollout.preflight_registered_checks.probe_gb10_candidate_source_readonly",
        source_probe,
    )
    check = build_gb10_candidate_source_check(
        lambda argv: subprocess.CompletedProcess(argv, 0, "", ""),
        targets=(target,),
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
        candidate_root=candidate_root,
        expected_candidate_sha="1" * 40,
        expected_candidate_tree="2" * 40,
        image_tag="staging-1111111",
    )
    context = CheckContext(
        {
            "candidate.sha": "1" * 40,
            "candidate.tree": "2" * 40,
            "gb10.inventory-digest": gb10_target_inventory_digest((target,)),
            "runner.config.sha256": "3" * 64,
        }
    )
    dag = PreflightDag(
        (
            _passing_dependency("candidate.identity"),
            _passing_dependency("gb10.ssh-topology"),
            _passing_dependency("gb10.shared-mount"),
            _passing_dependency("gb10.host-readiness"),
            check,
        )
    )

    result = next(
        item for item in dag.run(context, through_tier=0) if item.check_id == check.spec.check_id
    )

    assert result.passed
    assert result.evidence["candidate-sha"] == "1" * 40
    assert result.evidence["candidate-tree"] == "2" * 40
    assert result.evidence["host-count"] == 1
    assert check.spec.timeout_seconds == 510
    assert check.spec.dependencies == (
        "candidate.identity",
        "gb10.shared-mount",
        "gb10.host-readiness",
    )
    assert len(observed) == 1
    assert set(observed[0]["unit_sha256"]) == {
        "deploy/worker-pools/gb10/loom-gb10-node-agent.service",
        "deploy/worker-pools/gb10/loom-gb10-node-agent.timer",
        "deploy/worker-pools/gb10/loom-gb10-worker.service",
    }


def _image_inspect_payload(name: str, revision: str) -> str:
    image_id = hashlib.sha256(name.encode()).hexdigest()
    return json.dumps(
        [
            {
                "Id": f"sha256:{image_id}",
                "Os": "linux",
                "Architecture": "amd64",
                "Config": {
                    "Labels": {REVISION_LABEL: revision},
                    "Entrypoint": (
                        list(BROWSER_ENTRYPOINT)
                        if name == BROWSER_IMAGE
                        else (
                            list(REHEARSAL_POSTGRES_ENTRYPOINT)
                            if name == REHEARSAL_POSTGRES_IMAGE
                            else []
                        )
                    ),
                },
            }
        ]
    )


def test_registered_image_checks_build_once_then_reinspect_exact_ids(tmp_path: Path) -> None:
    revision = "1" * 40
    inspect_calls: list[str] = []

    def run(argv, cwd):
        command = tuple(argv)
        if command[:2] == ("docker", "run"):
            assert cwd is None
            return subprocess.CompletedProcess(argv, 0, "", "")
        assert command[:3] == ("docker", "image", "inspect")
        assert cwd is None
        name = command[-1].split(":", 1)[0]
        inspect_calls.append(name)
        return subprocess.CompletedProcess(argv, 0, _image_inspect_payload(name, revision), "")

    build, contract = build_image_preflight_checks(
        run,
        candidate_root=tmp_path,
        image_tag="staging-1111111",
        expected_candidate_sha=revision,
    )
    context = CheckContext(
        {
            "candidate.sha": revision,
            "image.plan.sha256": image_plan_digest(),
            "runner.config.sha256": "a" * 64,
        }
    )
    dag = PreflightDag(
        (
            _passing_dependency("docker.runtime"),
            _passing_dependency("candidate.identity"),
            build,
            contract,
        )
    )

    results = dag.run(context, through_tier=1)
    by_id = {result.check_id: result for result in results}
    assert by_id["images.build"].passed
    assert by_id["images.contract"].passed
    assert (
        by_id["images.build"].evidence["image-digests"]
        == by_id["images.contract"].evidence["image-digests"]
    )
    assert len(inspect_calls) == len(ALL_BUILD_IMAGES) * 2


def test_registered_image_build_rejects_candidate_drift_without_docker(tmp_path: Path) -> None:
    calls: list[object] = []
    build, _contract = build_image_preflight_checks(
        lambda *_args: calls.append(object()),  # type: ignore[arg-type,return-value]
        candidate_root=tmp_path,
        image_tag="staging-1111111",
        expected_candidate_sha="1" * 40,
    )
    context = CheckContext(
        {
            "candidate.sha": "2" * 40,
            "image.plan.sha256": image_plan_digest(),
            "runner.config.sha256": "a" * 64,
        }
    )
    dag = PreflightDag(
        (
            _passing_dependency("docker.runtime"),
            _passing_dependency("candidate.identity"),
            build,
        )
    )

    result = next(
        item for item in dag.run(context, through_tier=1) if item.check_id == "images.build"
    )
    assert not result.passed
    assert calls == []


def _rendered_image_manifest(image_tag: str = "staging-1111111") -> str:
    containers = "\n".join(
        f"        - name: {name}\n          image: {name}:{image_tag}"
        for name, _path in ALL_BUILD_IMAGES
        if name != BROWSER_IMAGE
    )
    return f"""apiVersion: apps/v1
kind: Deployment
metadata:
  name: exact-candidate
  namespace: loom-staging
spec:
  template:
    spec:
      containers:
{containers}
"""


def test_registered_manifest_checks_render_once_then_server_validate() -> None:
    revision = "1" * 40
    digests = {
        name: f"sha256:{hashlib.sha256(name.encode()).hexdigest()}"
        for name, _path in ALL_BUILD_IMAGES
    }
    image_artifact = SimpleNamespace(image_digests=digests, registry_digests={})
    render_calls: list[object] = []
    server_payloads: list[str] = []
    rendered, server_schema, field_ownership = build_manifest_preflight_checks(
        lambda: render_calls.append(object()) or _rendered_image_manifest(),
        lambda payload: (
            server_payloads.append(payload) or subprocess.CompletedProcess([], 0, "", "")
        ),
        lambda: image_artifact,  # type: ignore[arg-type,return-value]
        image_tag="staging-1111111",
        namespace="loom-staging",
        expected_candidate_sha=revision,
        expected_config_digest="a" * 64,
    )
    context = CheckContext(
        {
            "candidate.sha": revision,
            "runner.config.sha256": "a" * 64,
        }
    )
    dag = PreflightDag(
        (
            _passing_dependency("candidate.identity"),
            _passing_dependency("images.contract"),
            _passing_dependency("kubernetes.client"),
            rendered,
            server_schema,
            field_ownership,
        )
    )

    results = dag.run(context, through_tier=1)
    by_id = {result.check_id: result for result in results}
    assert by_id["manifests.render"].passed
    assert by_id["manifests.render"].evidence["server-valid"] is False
    assert by_id["manifests.server-schema"].passed
    assert by_id["manifests.server-schema"].evidence["server-valid"] is True
    assert by_id["manifests.field-ownership"].passed
    assert by_id["manifests.field-ownership"].evidence["ownership-ready"] is True
    assert server_schema.spec.freshness_ttl_seconds == 3600
    assert field_ownership.spec.freshness_ttl_seconds == 3600
    assert len(render_calls) == 1
    assert server_payloads == [_rendered_image_manifest(), _rendered_image_manifest()]


def test_registered_browser_runtime_binds_exact_image_token_and_schema(tmp_path: Path) -> None:
    token = tmp_path / "admin-token"
    token.write_text("not-evidence", encoding="utf-8")
    token.chmod(0o600)
    browser = ImageDescriptor(
        image_id="sha256:" + "b" * 64,
        revision="1" * 40,
        os="linux",
        architecture="amd64",
        entrypoint=BROWSER_ENTRYPOINT,
    )
    artifact = SimpleNamespace(descriptors={BROWSER_IMAGE: browser})
    calls: list[tuple[str, ...]] = []

    def run(argv):
        calls.append(tuple(argv))
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps({"runtime": "ready", "schema_version": 4}, separators=(",", ":")),
            "",
        )

    source_digest = "c" * 64
    check = build_browser_runtime_check(
        run,
        lambda: artifact,  # type: ignore[arg-type,return-value]
        token_path=token,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        expected_candidate_sha="1" * 40,
        expected_source_set_digest=source_digest,
    )
    context = CheckContext(
        {
            "browser.report-schema.sha256": browser_report_schema_digest(),
            "candidate.sha": "1" * 40,
            "protected-inputs.sha256": source_digest,
            "runner.config.sha256": "a" * 64,
        }
    )
    dag = PreflightDag(
        (
            _passing_dependency("images.contract"),
            _passing_dependency("credentials.metadata"),
            check,
        )
    )

    result = next(
        item for item in dag.run(context, through_tier=1) if item.check_id == "browser.runtime"
    )
    assert result.passed
    assert result.evidence["image-id"] == browser.image_id
    assert result.evidence["report-schema-digest"] == browser_report_schema_digest()
    assert check.spec.freshness_ttl_seconds == 3600
    assert "--network=none" in calls[0]


def test_registered_staging_baseline_runs_independent_readonly_blockers() -> None:
    check_ids = (
        "staging.health",
        "staging.auth",
        "staging.catalog-task",
        "staging.storage-db",
        "staging.network",
    )
    calls: list[str] = []

    def result(check_id: str) -> BaselineProbeResult:
        calls.append(check_id)
        return BaselineProbeResult(
            check_id=check_id,
            environment="staging",
            namespace="loom-staging",
            route="https://yylx.world/dev",
            readonly_principal="loom-rollout-readonly",
            observed_mutation_epoch=8,
            resource_digest=hashlib.sha256(check_id.encode()).hexdigest(),
            blockers={"dns": "canonical-route-unresolved"} if check_id == "staging.network" else {},
        )

    checks = build_staging_baseline_checks(
        {check_id: lambda check_id=check_id: result(check_id) for check_id in check_ids},
        environment="staging",
        namespace="loom-staging",
        route="https://yylx.world/dev",
        mutation_epoch=8,
    )
    context = CheckContext(
        {
            "environment": "staging",
            "namespace": "loom-staging",
            "readonly.principal.sha256": readonly_authority_policy_digest(),
            "route": "https://yylx.world/dev",
            "staging.mutation-epoch": 8,
            "runner.config.sha256": "a" * 64,
        }
    )
    dag = PreflightDag(
        (
            _passing_dependency("kubernetes.client"),
            _passing_dependency("readonly.authority"),
            _passing_dependency("credentials.metadata"),
            *checks,
        )
    )

    executions = dag.run(context, through_tier=2)
    by_id = {execution.check_id: execution for execution in executions}
    assert set(calls) == set(check_ids)
    assert not by_id["staging.network"].passed
    assert by_id["staging.network"].evidence["blockers"] == {"dns": "canonical-route-unresolved"}
    assert by_id["staging.storage-db"].passed
    assert by_id["staging.release-baseline"].blocked_by == ("staging.network",)


def test_strengthened_staging_auth_has_distinct_implementation_identity() -> None:
    check_ids = (
        "staging.health",
        "staging.auth",
        "staging.catalog-task",
        "staging.storage-db",
        "staging.network",
    )
    checks = build_staging_baseline_checks(
        {
            check_id: lambda check_id=check_id: BaselineProbeResult(
                check_id=check_id,
                environment="staging",
                namespace="loom-staging",
                route="https://yylx.world/dev",
                readonly_principal="loom-rollout-readonly",
                observed_mutation_epoch=8,
                resource_digest=hashlib.sha256(check_id.encode()).hexdigest(),
                blockers={},
            )
            for check_id in check_ids
        },
        environment="staging",
        namespace="loom-staging",
        route="https://yylx.world/dev",
        mutation_epoch=8,
    )
    by_id = {check.spec.check_id: check for check in checks}
    auth = by_id["staging.auth"]

    assert auth.implementation_version == "v3"
    assert (
        auth.implementation_digest
        != replace(auth, implementation_version="v2").implementation_digest
    )
    assert all(
        by_id[check_id].implementation_version == "v2"
        for check_id in check_ids
        if check_id != "staging.auth"
    )


def test_baseline_route_transition_binds_target_context_but_probes_predecessor() -> None:
    # #936 route transition: the plan/context carry the TARGET route (/staging)
    # while the live readonly probes still hit the PREDECESSOR (/dev, not migrated
    # yet). The context binding must stay on the target so bindings_match agrees;
    # only the probe session targets the predecessor. Regression guard: binding the
    # predecessor into the context (the original #937 defect) made every baseline
    # check fall through to the empty "staging-baseline-unavailable" probe.
    target = "https://yylx.world/staging"
    predecessor = "https://yylx.world/dev"
    check_ids = (
        "staging.health",
        "staging.auth",
        "staging.catalog-task",
        "staging.storage-db",
        "staging.network",
    )
    calls: list[str] = []

    def result(check_id: str) -> BaselineProbeResult:
        calls.append(check_id)
        return BaselineProbeResult(
            check_id=check_id,
            environment="staging",
            namespace="loom-staging",
            route=predecessor,  # the live probes hit the predecessor route
            readonly_principal="loom-rollout-readonly",
            observed_mutation_epoch=8,
            resource_digest=hashlib.sha256(check_id.encode()).hexdigest(),
            blockers={},
        )

    checks = build_staging_baseline_checks(
        {check_id: lambda check_id=check_id: result(check_id) for check_id in check_ids},
        environment="staging",
        namespace="loom-staging",
        route=target,
        baseline_probe_route=predecessor,
        mutation_epoch=8,
    )
    context = CheckContext(
        {
            "environment": "staging",
            "namespace": "loom-staging",
            "readonly.principal.sha256": readonly_authority_policy_digest(),
            "route": target,  # the plan/context carry the TARGET route
            "staging.mutation-epoch": 8,
            "runner.config.sha256": "a" * 64,
        }
    )
    dag = PreflightDag(
        (
            _passing_dependency("kubernetes.client"),
            _passing_dependency("readonly.authority"),
            _passing_dependency("credentials.metadata"),
            *checks,
        )
    )

    by_id = {e.check_id: e for e in dag.run(context, through_tier=2)}
    assert set(calls) == set(check_ids)
    # Every baseline check must BIND and pass — not fall through to the empty
    # "staging-baseline-unavailable" probe that the context/probe route mismatch
    # produced.
    for check_id in check_ids:
        assert by_id[check_id].passed, check_id
        assert by_id[check_id].evidence["readonly-principal"] == "loom-rollout-readonly"
    assert by_id["staging.release-baseline"].passed


def test_registered_rehearsal_runs_exact_isolated_journaled_actions() -> None:
    calls: list[str] = []

    def result(check_id: str) -> RehearsalResult:
        calls.append(check_id)
        return RehearsalResult(
            check_id=check_id,
            isolation_id="rehearsal-abc123",
            candidate_sha="1" * 40,
            mutation_epoch=8,
            evidence_digest=hashlib.sha256(check_id.encode()).hexdigest(),
            journal_digest=hashlib.sha256((check_id + "-journal").encode()).hexdigest(),
            protected_mutation=False,
            cleanup_verified=check_id == "rehearsal.cleanup",
            blockers={},
        )

    checks = build_rehearsal_checks(
        {check_id: lambda check_id=check_id: result(check_id) for check_id in REHEARSAL_CHECK_IDS},
        isolation_id="rehearsal-abc123",
        candidate_sha="1" * 40,
        mutation_epoch=8,
        checkpoint_evidence_digest="b" * 64,
        rehearsal_plan_digest="c" * 64,
    )
    dependencies = sorted(
        {
            dependency
            for check in checks
            for dependency in check.spec.dependencies
            if not dependency.startswith("rehearsal.")
        }
    )
    context = CheckContext(
        {
            "candidate.sha": "1" * 40,
            "checkpoint.evidence.sha256": "b" * 64,
            "rehearsal.plan.sha256": "c" * 64,
            "staging.mutation-epoch": 8,
            "runner.config.sha256": "a" * 64,
        }
    )
    dag = PreflightDag((*(_passing_dependency(item) for item in dependencies), *checks))

    executions = dag.run(context, through_tier=3)
    by_id = {execution.check_id: execution for execution in executions}
    specs = {check.spec.check_id: check.spec for check in checks}
    assert set(calls) == set(REHEARSAL_CHECK_IDS)
    assert all(by_id[check_id].passed for check_id in REHEARSAL_CHECK_IDS)
    assert "rehearsal.systemd-launch" in specs["rehearsal.release"].dependencies
    assert by_id["rehearsal.cleanup"].evidence["cleanup-verified"] is True
    assert by_id["rehearsal.cleanup"].evidence["protected-mutation"] is False
    assert all(
        {CheckOperation.PROBE, CheckOperation.APPLY, CheckOperation.VERIFY} <= set(check.operations)
        for check in checks
    )


def test_registered_final_gates_expose_only_declared_protected_mutations() -> None:
    def result(check_id: str, operation: CheckOperation) -> FinalGateResult:
        return FinalGateResult(
            check_id=check_id,
            operation=operation,
            candidate_sha="1" * 40,
            attestation_digest="2" * 64,
            observed_epoch=9 if operation is CheckOperation.APPLY else 8,
            evidence_digest=hashlib.sha256(f"{check_id}:{operation.value}".encode()).hexdigest(),
            protected_mutation=bool(
                check_id in PROTECTED_MUTATION_CHECK_IDS and operation is CheckOperation.APPLY
            ),
            blockers={},
        )

    checks = build_final_gate_checks(
        {
            check_id: lambda operation, check_id=check_id: result(check_id, operation)
            for check_id in FINAL_CHECK_IDS
        },
        candidate_sha="1" * 40,
        attestation_digest="2" * 64,
        mutation_epoch=8,
    )
    context = CheckContext(
        {
            "candidate.sha": "1" * 40,
            "preflight.attestation.sha256": "2" * 64,
            "staging.mutation-epoch": 8,
        }
    )
    by_id = {check.spec.check_id: check for check in checks}
    assert set(by_id) == set(FINAL_CHECK_IDS)
    assert all(check.spec.stage is StageCapability.FINAL_ONLY for check in checks)
    assert {
        check_id
        for check_id, check in by_id.items()
        if check.spec.mutation_class is MutationClass.PROTECTED_STAGING
    } == PROTECTED_MUTATION_CHECK_IDS
    assert all(
        check.spec.mutation_class is MutationClass.NONE
        for check_id, check in by_id.items()
        if check_id not in PROTECTED_MUTATION_CHECK_IDS
    )
    probe = by_id["final.protected-apply"].operations[CheckOperation.PROBE](context)
    applied = by_id["final.protected-apply"].operations[CheckOperation.APPLY](context)
    assert probe.passed and probe.evidence["protected-mutation"] is False
    assert applied.passed and applied.evidence["protected-mutation"] is True
    smoke = by_id["final.smoke"].operations[CheckOperation.APPLY](context)
    assert smoke.passed and smoke.evidence["protected-mutation"] is True
