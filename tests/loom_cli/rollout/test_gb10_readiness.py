from __future__ import annotations

import json
import subprocess
from collections import defaultdict
from pathlib import Path

from loom_cli.rollout.gb10_readiness import (
    GB10ProbeTarget,
    probe_gb10_fleet_readonly,
    remote_probe_command,
)

BOOT_IDS = {
    "trt-gb10-1": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
    "trt-gb10-2": "11111111-2222-4333-8444-555555555555",
}


def _payload(host: str, *, timer_state: str = "waiting") -> str:
    return json.dumps(
        {
            "schema_version": 1,
            "boot_id": BOOT_IDS[host],
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
                "SubState": timer_state,
                "Unit": "loom-gb10-node-agent.service",
                "NeedDaemonReload": "no",
            },
        }
    )


def test_remote_probe_command_is_fixed_and_readonly() -> None:
    command = remote_probe_command("loom-gb10-node-agent.service")

    assert command.startswith("python3 -c ")
    assert "systemd-run" not in command
    assert " start " not in command
    assert " enable " not in command
    assert " disable " not in command
    assert "--user" in command


def test_fleet_probe_collects_all_hosts_and_settles_transient_timer() -> None:
    targets = tuple(GB10ProbeTarget(host, "loom-gb10-node-agent.service") for host in BOOT_IDS)
    calls: defaultdict[str, int] = defaultdict(int)
    sleeps: list[float] = []

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        host = argv[-2]
        calls[host] += 1
        state = "running" if host == "trt-gb10-1" and calls[host] == 1 else "waiting"
        return subprocess.CompletedProcess(argv, 0, _payload(host, timer_state=state), "")

    result = probe_gb10_fleet_readonly(
        run,
        targets,
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
        max_concurrency=2,
        settle_attempts=3,
        settle_interval_seconds=0.25,
        sleep=sleeps.append,
    )

    assert result.ready
    assert result.failed_hosts == ()
    assert result.transient_hosts == ("trt-gb10-1",)
    assert result.host_boot_ids == BOOT_IDS
    assert set(result.host_evidence_digests) == set(BOOT_IDS)
    assert len(result.inventory_digest) == 64
    assert calls == {"trt-gb10-1": 2, "trt-gb10-2": 1}
    assert sleeps == [0.25]


def test_fleet_probe_returns_every_independent_blocker() -> None:
    targets = tuple(GB10ProbeTarget(host, "loom-gb10-node-agent.service") for host in BOOT_IDS)

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        host = argv[-2]
        if host == "trt-gb10-1":
            return subprocess.CompletedProcess(argv, 255, "", "unavailable")
        payload = json.loads(_payload(host))
        payload["linger_enabled"] = False
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")

    result = probe_gb10_fleet_readonly(
        run,
        targets,
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
        max_concurrency=2,
    )

    assert not result.ready
    assert result.failed_hosts == ("trt-gb10-1", "trt-gb10-2")
    assert "unavailable" not in str(result)


def test_fleet_probe_fails_closed_when_transient_never_settles() -> None:
    target = GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service")
    calls = 0

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(
            argv,
            0,
            _payload("trt-gb10-1", timer_state="running"),
            "",
        )

    result = probe_gb10_fleet_readonly(
        run,
        (target,),
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
        settle_attempts=3,
        settle_interval_seconds=0,
    )

    assert not result.ready
    assert result.failed_hosts == ("trt-gb10-1",)
    assert result.transient_hosts == ("trt-gb10-1",)
    assert calls == 3
