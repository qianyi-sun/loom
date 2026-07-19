from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from loom_cli.rollout.gb10_readiness import GB10ProbeTarget
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
    build_gb10_host_readiness_check,
    build_systemd_user_manager_check,
    gb10_target_inventory_digest,
)

BOOT_ID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"


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
