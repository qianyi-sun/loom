from __future__ import annotations

import subprocess
from datetime import UTC, datetime

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
from loom_cli.rollout.preflight_registered_checks import build_systemd_user_manager_check

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
