from __future__ import annotations

import json
import subprocess

import pytest

from loom_cli.rollout.systemd_readiness import (
    NodeAgentTimerState,
    RehearsalSystemdActivation,
    UserManagerReadiness,
    classify_node_agent_timer,
    node_agent_service_is_prepared,
    parse_gb10_host_readiness,
    parse_systemctl_properties,
    probe_user_manager_readonly,
)

BOOT_ID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"


def test_node_agent_predicates_replay_waiting_and_transient_history() -> None:
    service = {
        "LoadState": "loaded",
        "Type": "oneshot",
        "Result": "success",
        "ExecMainStatus": "0",
        "ActiveState": "inactive",
        "SubState": "dead",
        "NeedDaemonReload": "no",
    }
    timer = {
        "LoadState": "loaded",
        "ActiveState": "active",
        "SubState": "running",
        "Unit": "loom-gb10-node-agent.service",
        "NeedDaemonReload": "no",
    }

    assert node_agent_service_is_prepared(service)
    assert (
        classify_node_agent_timer(timer, service="loom-gb10-node-agent.service")
        is NodeAgentTimerState.TRANSIENT_RUNNING
    )
    timer["SubState"] = "waiting"
    assert (
        classify_node_agent_timer(timer, service="loom-gb10-node-agent.service")
        is NodeAgentTimerState.PREPARED
    )
    timer["SubState"] = "elapsed"
    assert (
        classify_node_agent_timer(timer, service="loom-gb10-node-agent.service")
        is NodeAgentTimerState.REPAIRABLE_ELAPSED
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("LoadState", "not-found"),
        ("ActiveState", "failed"),
        ("SubState", "dead"),
        ("Unit", "other.service"),
        ("NeedDaemonReload", "yes"),
    ],
)
def test_node_agent_timer_classifier_fails_closed(field: str, value: str) -> None:
    properties = {
        "LoadState": "loaded",
        "ActiveState": "active",
        "SubState": "waiting",
        "Unit": "loom-gb10-node-agent.service",
        "NeedDaemonReload": "no",
    }
    properties[field] = value

    assert (
        classify_node_agent_timer(properties, service="loom-gb10-node-agent.service")
        is NodeAgentTimerState.INVALID
    )


def test_parse_elapsed_timer_is_ready_only_for_protected_repair() -> None:
    payload = {
        "schema_version": 1,
        "boot_id": BOOT_ID,
        "manager_version": "255.4-1ubuntu8.16",
        "linger_enabled": True,
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
            "SubState": "elapsed",
            "Unit": "loom-gb10-node-agent.service",
            "NeedDaemonReload": "no",
        },
        "timer_enabled": True,
    }

    evidence = parse_gb10_host_readiness(
        json.dumps(payload),
        service="loom-gb10-node-agent.service",
    )

    assert evidence is not None
    assert evidence.ready
    assert evidence.repairable_timer
    assert not evidence.transient_timer


def test_parse_systemctl_properties_ignores_unstructured_lines() -> None:
    assert parse_systemctl_properties("LoadState=loaded\nnoise\nSubState=waiting\n") == {
        "LoadState": "loaded",
        "SubState": "waiting",
    }


def test_rehearsal_activation_contract_binds_fixed_sandbox_and_exact_readback() -> None:
    contract = RehearsalSystemdActivation(
        unit="loom-preflight-" + "a" * 24 + ".service",
        plan_digest="b" * 64,
    )

    assert contract.start_argv[-2:] == ("--", "/usr/bin/true")
    assert "--property=NoNewPrivileges=yes" in contract.start_argv
    assert "--property=ProtectSystem=strict" in contract.start_argv
    assert "--property=IPAddressDeny=any" in contract.start_argv
    assert contract.load_state_argv[-2:] == ("--property=LoadState", "--value")
    properties = {
        "LoadState": "loaded",
        "ActiveState": "active",
        "SubState": "exited",
        "Type": "oneshot",
        "Result": "success",
        "ExecMainStatus": "0",
        "NeedDaemonReload": "no",
        "Transient": "yes",
        "Description": contract.description,
    }
    assert contract.ready(properties, latency_ms=4999)
    assert not contract.ready({**properties, "Description": "other"}, latency_ms=1)
    assert not contract.ready(properties, latency_ms=5001)
    assert contract.absent({"LoadState": "not-found"})


@pytest.mark.parametrize(
    ("unit", "digest"),
    [
        ("loom-staging-rollout.service", "b" * 64),
        ("loom-preflight-short.service", "b" * 64),
        ("loom-preflight-" + "a" * 24 + ".service", "wrong"),
    ],
)
def test_rehearsal_activation_rejects_nonisolated_identity(unit: str, digest: str) -> None:
    with pytest.raises(ValueError, match="authority"):
        RehearsalSystemdActivation(unit=unit, plan_digest=digest)


def test_user_manager_readonly_probe_binds_version_linger_boot_and_latency() -> None:
    calls: list[tuple[str, ...]] = []
    outputs = iter(("255.4-1ubuntu8.14\n", "yes\n", f"{BOOT_ID}\n"))
    clock = iter((4.0, 4.125))

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, next(outputs), "")

    evidence = probe_user_manager_readonly(
        run,
        uid=1001,
        rpc_budget_ms=500,
        monotonic=lambda: next(clock),
    )

    assert evidence == UserManagerReadiness(
        version="255.4-1ubuntu8.14",
        linger_enabled=True,
        boot_id=BOOT_ID,
        rpc_latency_ms=125,
        rpc_budget_ms=500,
    )
    assert len(evidence.evidence_digest) == 64
    assert calls == [
        ("systemctl", "--user", "show", "--property=Version", "--value"),
        ("loginctl", "show-user", "1001", "--property=Linger", "--value"),
        ("cat", "/proc/sys/kernel/random/boot_id"),
    ]


@pytest.mark.parametrize(
    "outputs",
    [
        ("degraded\n", "yes\n", f"{BOOT_ID}\n"),
        ("255\n", "no\n", f"{BOOT_ID}\n"),
        ("255\n", "yes\n", "not-a-boot-id\n"),
    ],
)
def test_user_manager_readonly_probe_rejects_invalid_authority(
    outputs: tuple[str, str, str],
) -> None:
    values = iter(outputs)

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(argv, 0, next(values), "")

    assert probe_user_manager_readonly(run, uid=1001, monotonic=lambda: 1.0) is None


def test_user_manager_readonly_probe_enforces_latency_budget() -> None:
    outputs = iter(("255\n", "yes\n", f"{BOOT_ID}\n"))
    clock = iter((10.0, 15.001))

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(argv, 0, next(outputs), "")

    assert (
        probe_user_manager_readonly(
            run,
            uid=1001,
            rpc_budget_ms=5000,
            monotonic=lambda: next(clock),
        )
        is None
    )


def _gb10_payload(*, timer_substate: str = "waiting") -> str:
    return json.dumps(
        {
            "schema_version": 1,
            "boot_id": BOOT_ID,
            "manager_version": "255.4-1ubuntu8.14",
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
                "SubState": timer_substate,
                "Unit": "loom-gb10-node-agent.service",
                "NeedDaemonReload": "no",
            },
        }
    )


def test_parse_gb10_host_readiness_accepts_prepared_and_classifies_transient() -> None:
    prepared = parse_gb10_host_readiness(_gb10_payload(), service="loom-gb10-node-agent.service")
    transient = parse_gb10_host_readiness(
        _gb10_payload(timer_substate="running"),
        service="loom-gb10-node-agent.service",
    )

    assert prepared is not None and prepared.ready
    assert len(prepared.evidence_digest) == 64
    assert transient is not None and not transient.ready and transient.transient_timer


@pytest.mark.parametrize(
    "mutation",
    [
        {"boot_id": "wrong"},
        {"manager_version": "degraded"},
        {"linger_enabled": False},
        {"timer_enabled": False},
        {"extra": "unknown"},
    ],
)
def test_parse_gb10_host_readiness_fails_closed_on_authority_drift(
    mutation: dict[str, object],
) -> None:
    payload = json.loads(_gb10_payload())
    payload.update(mutation)

    evidence = parse_gb10_host_readiness(
        json.dumps(payload), service="loom-gb10-node-agent.service"
    )

    if set(mutation) <= {"linger_enabled", "timer_enabled"}:
        assert evidence is not None and not evidence.ready
    else:
        assert evidence is None
