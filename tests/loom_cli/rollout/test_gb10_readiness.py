from __future__ import annotations

import json
import subprocess
from collections import defaultdict
from pathlib import Path

from loom_cli.rollout.gb10_readiness import (
    GB10ProbeTarget,
    candidate_source_remote_command,
    probe_gb10_candidate_source_readonly,
    probe_gb10_fleet_readonly,
    probe_gb10_ssh_topology,
    remote_probe_command,
)

BOOT_IDS = {
    "trt-gb10-1": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
    "trt-gb10-2": "11111111-2222-4333-8444-555555555555",
}
UNIT_DIGESTS = {
    "deploy/worker-pools/gb10/loom-gb10-node-agent.service": "1" * 64,
    "deploy/worker-pools/gb10/loom-gb10-node-agent.timer": "2" * 64,
    "deploy/worker-pools/gb10/loom-gb10-worker.service": "3" * 64,
}
UNIT_SET_DIGEST = "4" * 64
CANDIDATE_SHA = "a" * 40
CANDIDATE_TREE = "b" * 40


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


def test_candidate_source_remote_command_is_fixed_readonly_and_exact() -> None:
    command = candidate_source_remote_command(
        candidate_sha=CANDIDATE_SHA,
        candidate_tree=CANDIDATE_TREE,
        image_tag="staging-aaaaaaa",
        unit_sha256=UNIT_DIGESTS,
    )

    assert command.startswith("python3 -c ")
    assert "/shared_work2/loom-staging-rollout/worker-repos/" in command
    assert CANDIDATE_SHA in command
    assert CANDIDATE_TREE in command
    assert "checkout" not in command
    assert "fetch" not in command
    assert "systemctl" not in command
    assert 'getattr(os, "O_NOFOLLOW", 0)' in command
    assert '"GIT_CONFIG_GLOBAL": "/dev/null"' in command
    assert '"GIT_CONFIG_NOSYSTEM": "1"' in command
    assert '"GIT_NO_REPLACE_OBJECTS": "1"' in command
    assert '"GIT_OPTIONAL_LOCKS": "0"' in command
    assert '"GIT_TERMINAL_PROMPT": "0"' in command
    assert "timeout=30" in command
    assert "\x00" not in command
    assert '"\\x00" in result.stdout' in command


def test_candidate_source_probe_collects_all_exact_hosts() -> None:
    targets = tuple(GB10ProbeTarget(host, "loom-gb10-node-agent.service") for host in BOOT_IDS)
    payload = json.dumps(
        {
            "candidate_sha": CANDIDATE_SHA,
            "candidate_tree": CANDIDATE_TREE,
            "unit_sha256": UNIT_DIGESTS,
        },
        sort_keys=True,
        separators=(",", ":"),
    )

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(argv, 0, payload, "")

    result = probe_gb10_candidate_source_readonly(
        run,
        targets,
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
        candidate_sha=CANDIDATE_SHA,
        candidate_tree=CANDIDATE_TREE,
        image_tag="staging-aaaaaaa",
        unit_sha256=UNIT_DIGESTS,
        unit_set_digest=UNIT_SET_DIGEST,
        max_concurrency=2,
    )

    assert result.ready
    assert result.failed_hosts == ()
    assert set(result.host_digests) == set(BOOT_IDS)
    assert result.candidate_sha == CANDIDATE_SHA
    assert result.candidate_tree == CANDIDATE_TREE
    assert result.unit_set_digest == UNIT_SET_DIGEST
    assert len(result.evidence_digest) == 64


def test_candidate_source_probe_reports_complete_host_failures() -> None:
    targets = tuple(GB10ProbeTarget(host, "loom-gb10-node-agent.service") for host in BOOT_IDS)

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        host = argv[-2]
        if host == "trt-gb10-1":
            return subprocess.CompletedProcess(argv, 255, "", "unavailable")
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(
                {
                    "candidate_sha": CANDIDATE_SHA,
                    "candidate_tree": "c" * 40,
                    "unit_sha256": UNIT_DIGESTS,
                }
            ),
            "",
        )

    result = probe_gb10_candidate_source_readonly(
        run,
        targets,
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
        candidate_sha=CANDIDATE_SHA,
        candidate_tree=CANDIDATE_TREE,
        image_tag="staging-aaaaaaa",
        unit_sha256=UNIT_DIGESTS,
        unit_set_digest=UNIT_SET_DIGEST,
        max_concurrency=2,
        settle_attempts=1,
    )

    assert not result.ready
    assert result.failed_hosts == tuple(sorted(BOOT_IDS))
    assert result.host_digests == {}


def test_serial_candidate_source_probe_stops_after_first_failed_host() -> None:
    targets = tuple(
        GB10ProbeTarget(host, "loom-gb10-node-agent.service")
        for host in ("trt-gb10-1", "trt-gb10-2", "trt-gb10-3")
    )
    calls: list[str] = []
    payload = json.dumps(
        {
            "candidate_sha": CANDIDATE_SHA,
            "candidate_tree": CANDIDATE_TREE,
            "unit_sha256": UNIT_DIGESTS,
        }
    )

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        host = argv[-2]
        calls.append(host)
        if host == "trt-gb10-1":
            return subprocess.CompletedProcess(argv, 0, payload, "")
        return subprocess.CompletedProcess(argv, 255, "", "unavailable")

    result = probe_gb10_candidate_source_readonly(
        run,
        targets,
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
        candidate_sha=CANDIDATE_SHA,
        candidate_tree=CANDIDATE_TREE,
        image_tag="staging-aaaaaaa",
        unit_sha256=UNIT_DIGESTS,
        unit_set_digest=UNIT_SET_DIGEST,
        max_concurrency=1,
        settle_attempts=2,
        settle_interval_seconds=0,
    )

    assert calls == ["trt-gb10-1", "trt-gb10-2", "trt-gb10-2"]
    assert result.host_digests.keys() == {"trt-gb10-1"}
    assert result.failed_hosts == ("trt-gb10-2", "trt-gb10-3")


def test_candidate_source_probe_settles_one_transient_shared_source_read() -> None:
    target = GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service")
    calls = 0
    sleeps: list[float] = []
    payload = json.dumps(
        {
            "candidate_sha": CANDIDATE_SHA,
            "candidate_tree": CANDIDATE_TREE,
            "unit_sha256": UNIT_DIGESTS,
        },
        sort_keys=True,
        separators=(",", ":"),
    )

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        if calls == 1:
            return subprocess.CompletedProcess(argv, 1, "", "stale shared source")
        return subprocess.CompletedProcess(argv, 0, payload, "")

    result = probe_gb10_candidate_source_readonly(
        run,
        (target,),
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
        candidate_sha=CANDIDATE_SHA,
        candidate_tree=CANDIDATE_TREE,
        image_tag="staging-aaaaaaa",
        unit_sha256=UNIT_DIGESTS,
        unit_set_digest=UNIT_SET_DIGEST,
        settle_interval_seconds=0.25,
        sleep=sleeps.append,
    )

    assert result.ready
    assert calls == 2
    assert sleeps == [0.25]


def test_candidate_source_probe_bounds_nfs_publication_backoff() -> None:
    target = GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service")
    calls = 0
    sleeps: list[float] = []
    payload = json.dumps(
        {
            "candidate_sha": CANDIDATE_SHA,
            "candidate_tree": CANDIDATE_TREE,
            "unit_sha256": UNIT_DIGESTS,
        },
        sort_keys=True,
        separators=(",", ":"),
    )

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        if calls < 6:
            return subprocess.CompletedProcess(argv, 1, "", "shared publication pending")
        return subprocess.CompletedProcess(argv, 0, payload, "")

    result = probe_gb10_candidate_source_readonly(
        run,
        (target,),
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
        candidate_sha=CANDIDATE_SHA,
        candidate_tree=CANDIDATE_TREE,
        image_tag="staging-aaaaaaa",
        unit_sha256=UNIT_DIGESTS,
        unit_set_digest=UNIT_SET_DIGEST,
        settle_attempts=6,
        settle_interval_seconds=2.0,
        sleep=sleeps.append,
    )

    assert result.ready
    assert calls == 6
    assert sleeps == [2.0, 4.0, 8.0, 16.0, 30.0]
    assert sum(sleeps) == 60.0


def test_candidate_source_probe_never_retries_divergent_content() -> None:
    target = GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service")
    calls = 0
    sleeps: list[float] = []

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(
                {
                    "candidate_sha": CANDIDATE_SHA,
                    "candidate_tree": "c" * 40,
                    "unit_sha256": UNIT_DIGESTS,
                }
            ),
            "",
        )

    result = probe_gb10_candidate_source_readonly(
        run,
        (target,),
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
        candidate_sha=CANDIDATE_SHA,
        candidate_tree=CANDIDATE_TREE,
        image_tag="staging-aaaaaaa",
        unit_sha256=UNIT_DIGESTS,
        unit_set_digest=UNIT_SET_DIGEST,
        sleep=sleeps.append,
    )

    assert not result.ready
    assert calls == 1
    assert sleeps == []


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


def test_fleet_probe_accepts_exact_elapsed_timer_for_protected_repair() -> None:
    target = GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service")
    calls = 0

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(
            argv,
            0,
            _payload("trt-gb10-1", timer_state="elapsed"),
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

    assert result.ready
    assert result.failed_hosts == ()
    assert result.transient_hosts == ("trt-gb10-1",)
    assert calls == 1


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
        settle_attempts=1,
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


def test_fleet_probe_retries_transient_transport_failure() -> None:
    target = GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service")
    calls = 0
    sleeps: list[float] = []

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        # The single bastion drops the first two connections (ssh exit 255) before
        # the host answers with a fully ready observation.
        if calls < 3:
            return subprocess.CompletedProcess(argv, 255, "", "kex_exchange_identification")
        return subprocess.CompletedProcess(argv, 0, _payload("trt-gb10-1"), "")

    result = probe_gb10_fleet_readonly(
        run,
        (target,),
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
        settle_attempts=5,
        settle_interval_seconds=0.25,
        sleep=sleeps.append,
    )

    assert result.ready
    assert result.failed_hosts == ()
    # A transport failure is retried but never reported as a transient-timer host.
    assert result.transient_hosts == ()
    assert result.host_boot_ids == {"trt-gb10-1": BOOT_IDS["trt-gb10-1"]}
    assert calls == 3
    assert sleeps == [0.25, 0.25]


def test_fleet_probe_fails_closed_when_transport_never_recovers() -> None:
    target = GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service")
    calls = 0

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(argv, 255, "", "connection reset by peer")

    result = probe_gb10_fleet_readonly(
        run,
        (target,),
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
        settle_attempts=4,
        settle_interval_seconds=0,
    )

    assert not result.ready
    assert result.failed_hosts == ("trt-gb10-1",)
    assert result.transient_hosts == ()
    assert result.host_boot_ids == {}
    assert calls == 4


def test_ssh_topology_reports_every_host_without_remote_diagnostics() -> None:
    targets = tuple(GB10ProbeTarget(host, "loom-gb10-node-agent.service") for host in BOOT_IDS)

    def run(argv: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        host = argv[-2]
        assert argv[-1] == "true"
        assert "StrictHostKeyChecking=yes" in argv
        assert "BatchMode=yes" in argv
        return subprocess.CompletedProcess(argv, 0 if host == "trt-gb10-1" else 255, "", "")

    result = probe_gb10_ssh_topology(
        run,
        targets,
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
        max_concurrency=2,
    )

    assert not result.ready
    assert result.reachable_hosts == ("trt-gb10-1",)
    assert result.failed_hosts == ("trt-gb10-2",)
    assert len(result.evidence_digest) == 64


def test_ssh_topology_rejects_nonempty_remote_output() -> None:
    target = GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service")

    result = probe_gb10_ssh_topology(
        lambda argv: subprocess.CompletedProcess(argv, 0, "unexpected", ""),
        (target,),
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
    )

    assert not result.ready
    assert result.failed_hosts == ("trt-gb10-1",)
