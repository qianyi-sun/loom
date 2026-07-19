from __future__ import annotations

import subprocess

import pytest

from loom_cli.rollout.systemd_readiness import (
    NodeAgentTimerState,
    UserManagerReadiness,
    classify_node_agent_timer,
    node_agent_service_is_prepared,
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


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("LoadState", "not-found"),
        ("ActiveState", "failed"),
        ("SubState", "elapsed"),
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


def test_parse_systemctl_properties_ignores_unstructured_lines() -> None:
    assert parse_systemctl_properties("LoadState=loaded\nnoise\nSubState=waiting\n") == {
        "LoadState": "loaded",
        "SubState": "waiting",
    }


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
