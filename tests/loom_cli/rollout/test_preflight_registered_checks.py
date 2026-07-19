from __future__ import annotations

import json
import os
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom_cli.rollout.credential_authority import safe_content_fingerprint
from loom_cli.rollout.gb10_readiness import GB10ProbeTarget
from loom_cli.rollout.operator.backup_lease import BackupLease, component_set_digest
from loom_cli.rollout.operator.candidate import CandidateIdentityEvidence
from loom_cli.rollout.operator.config import OperatorConfig
from loom_cli.rollout.operator.model import CandidateBinding
from loom_cli.rollout.preflight_contract import (
    CheckContext,
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
    build_backup_lease_eligibility_check,
    build_candidate_identity_check,
    build_capacity_high_water_check,
    build_credentials_metadata_check,
    build_docker_runtime_check,
    build_gb10_host_readiness_check,
    build_kubernetes_client_check,
    build_systemd_user_manager_check,
    build_tools_runtime_check,
    credential_source_set_digest,
    gb10_target_inventory_digest,
)
from loom_cli.rollout.runtime_readiness import REQUIRED_EXECUTABLES, REQUIRED_IMPORTS

BOOT_ID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"


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
    assert len(str(docker.evidence["runtime-digest"])) == 64
    assert calls == [("docker", "info"), ("docker", "buildx", "version")]
    assert "token" not in str(dict(docker.evidence))
    assert "private" not in str(dict(docker.evidence))


def test_registered_kubernetes_client_binds_kubeconfig_and_safe_evidence(
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, ...]] = []

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        stdout = "kind-loom-staging\n" if "current-context" in argv else ""
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
    assert kubernetes.evidence["current-context"] == "kind-loom-staging"
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


def test_registered_capacity_high_water_reports_all_bound_metrics() -> None:
    capacity = StagingCapacity(
        object_count=249_999,
        bytes_used=15 * 1024**3,
        disk_free_percent=21,
        inode_free_percent=22,
    )
    check = build_capacity_high_water_check(lambda: capacity)
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
                    safe_content_fingerprint(payload) if label == "admin" else None
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
    assert set(result.evidence["metadata-fingerprints"]) == {
        "admin",
        "worker",
        "service",
        "catalog",
    }
    rendered = json.dumps(dict(result.evidence), sort_keys=True)
    assert "private-value" not in rendered
    assert "CATALOG_PASSWORD" not in rendered
    assert str(tmp_path) not in rendered


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
    assert set(result.evidence["metadata-fingerprints"]) == {"service", "catalog"}


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
            _passing_dependency("capacity.high-water"),
            _passing_dependency("lifecycle.launch-cancel"),
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
    assert result.evidence["eligible"] is True
    assert result.evidence["blockers"] == {}
    assert result.evidence["lease-digest"] == lease.evidence_digest


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
            _passing_dependency("capacity.high-water"),
            _passing_dependency("lifecycle.launch-cancel"),
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
