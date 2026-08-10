from __future__ import annotations

import json
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

from loom_cli.rollout.gb10_convergence import (
    GB10ConvergencePlan,
    GB10ConvergenceState,
    GB10HostMutation,
    GB10MutationKind,
)
from loom_cli.rollout.operator.protected_gb10_transport import (
    _REMOTE_SHARED_GIT_TIMEOUT_SECONDS,
    FixedGB10SSHTransport,
    GB10FleetApplyError,
    GB10TransportTarget,
    _remote_apply_source,
    _remote_observation_source,
    build_fixed_gb10_ssh_transport,
)
from tests.loom_cli.rollout.operator.test_protected_migration_component import _published_plan


def _target(host: str = "trt-gb10-1") -> GB10TransportTarget:
    return GB10TransportTarget(
        ssh_target=host,
        node_agent_service="loom-gb10-node-agent.service",
    )


def _plan(tmp_path: Path, *hosts: str):
    return replace(
        _published_plan(tmp_path),
        gb10_boot_ids={host: f"boot-{index}" for index, host in enumerate(hosts, 1)},
    )


def _transport(run, *targets: GB10TransportTarget) -> FixedGB10SSHTransport:
    return FixedGB10SSHTransport(
        targets=targets,
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
        run=run,
        max_concurrency=2,
    )


def test_fixed_transport_observes_exact_candidate_without_remote_mutation(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1")
    calls: list[tuple[str, ...]] = []
    payload = {
        "baseline_ready": True,
        "boot_id": "boot-1",
        "candidate_source_exact": True,
        "checkout_exact": False,
        "environment_exact": False,
        "legacy_absent": True,
        "service_timer_exact": False,
        "service_timer_transient": False,
        "units_exact": False,
    }

    def run(argv):
        calls.append(tuple(argv))
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")

    observed = _transport(run, _target()).observe(plan)

    assert set(observed.hosts) == {"trt-gb10-1"}
    assert observed.hosts["trt-gb10-1"].applicable
    assert not observed.hosts["trt-gb10-1"].exact
    assert observed.candidate_source_digest == plan.gb10_unit_digest
    assert len(calls) == 1
    argv = calls[0]
    assert argv[-2] == "trt-gb10-1"
    assert plan.candidate_sha in argv[-1]
    assert plan.candidate_tree in argv[-1]
    assert "/shared_work2/loom-staging-rollout/worker-repos" in argv[-1]
    assert "git fetch" not in argv[-1]
    assert "systemctl --user start" not in argv[-1]


def test_fixed_transport_applies_only_typed_operations_to_planned_host(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1", "trt-gb10-2")
    calls: list[tuple[str, ...]] = []

    def run(argv):
        calls.append(tuple(argv))
        return subprocess.CompletedProcess(argv, 0, "", "")

    convergence = GB10ConvergencePlan(
        state=GB10ConvergenceState.READY,
        mutations=(
            GB10HostMutation(
                "trt-gb10-2",
                (GB10MutationKind.LEGACY_RETIRE, GB10MutationKind.SERVICE_TIMER),
            ),
        ),
        blockers={},
        evidence_digest="1" * 64,
    )
    _transport(run, _target(), _target("trt-gb10-2")).apply(plan, convergence)

    assert len(calls) == 1
    assert calls[0][-2] == "trt-gb10-2"
    assert "legacy-retire" in calls[0][-1]
    assert "service-timer" in calls[0][-1]
    assert plan.candidate_sha in calls[0][-1]
    assert "loom-gb10-worker.service" in calls[0][-1]
    assert "disable" in calls[0][-1]
    assert "enable" not in calls[0][-1]
    assert "git fetch" not in calls[0][-1]
    assert "curl" not in calls[0][-1]


def _retrying_transport(
    run, *targets: GB10TransportTarget, sleeps: list[float], attempts: int = 5
) -> FixedGB10SSHTransport:
    return FixedGB10SSHTransport(
        targets=targets,
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
        run=run,
        max_concurrency=2,
        settle_attempts=attempts,
        settle_interval_seconds=0.5,
        sleep=sleeps.append,
    )


def test_fixed_transport_retries_transient_observe_failure(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1")
    calls = 0
    sleeps: list[float] = []
    payload = {
        "baseline_ready": True,
        "boot_id": "boot-1",
        "candidate_source_exact": True,
        "checkout_exact": True,
        "environment_exact": True,
        "legacy_absent": True,
        "service_timer_exact": True,
        "service_timer_transient": False,
        "units_exact": True,
    }

    def run(argv):
        nonlocal calls
        calls += 1
        # The single bastion drops the first two connections (ssh exit 255).
        if calls < 3:
            return subprocess.CompletedProcess(argv, 255, "", "kex_exchange_identification")
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")

    observed = _retrying_transport(run, _target(), sleeps=sleeps).observe(plan)

    host = observed.hosts["trt-gb10-1"]
    assert host.exact
    assert host.boot_id == "boot-1"
    assert calls == 3
    assert sleeps == [0.5, 0.5]


def test_fixed_transport_observe_fails_closed_after_exhausting_retries(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1")
    calls = 0
    sleeps: list[float] = []

    def run(argv):
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(argv, 255, "", "connection reset by peer")

    observed = _retrying_transport(run, _target(), sleeps=sleeps, attempts=4).observe(plan)

    host = observed.hosts["trt-gb10-1"]
    assert host.boot_id == "unavailable"
    assert not host.applicable
    assert calls == 4
    assert sleeps == [0.5, 0.5, 0.5]


def test_fixed_transport_does_not_retry_a_valid_drifted_observation(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1")
    calls = 0
    sleeps: list[float] = []
    payload = {
        "baseline_ready": False,
        "boot_id": "boot-1",
        "candidate_source_exact": False,
        "checkout_exact": False,
        "environment_exact": False,
        "legacy_absent": True,
        "service_timer_exact": False,
        "service_timer_transient": False,
        "units_exact": False,
    }

    def run(argv):
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")

    observed = _retrying_transport(run, _target(), sleeps=sleeps).observe(plan)

    # A well-formed observation is authoritative -- genuine drift is never retried.
    assert not observed.hosts["trt-gb10-1"].applicable
    assert observed.hosts["trt-gb10-1"].boot_id == "boot-1"
    assert calls == 1
    assert sleeps == []


def test_fixed_transport_settles_a_firing_node_agent_to_exact(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1")
    calls = 0
    sleeps: list[float] = []
    firing = {
        "baseline_ready": True,
        "boot_id": "boot-1",
        "candidate_source_exact": True,
        "checkout_exact": True,
        "environment_exact": True,
        "legacy_absent": True,
        "service_timer_exact": False,
        "service_timer_transient": True,
        "units_exact": True,
    }
    settled = {**firing, "service_timer_exact": True, "service_timer_transient": False}

    def run(argv):
        nonlocal calls
        calls += 1
        # The node-agent oneshot fires on the first two observes; then the timer
        # returns to "waiting" and the host reads exact.
        return subprocess.CompletedProcess(
            argv, 0, json.dumps(firing if calls < 3 else settled), ""
        )

    observed = _retrying_transport(run, _target(), sleeps=sleeps).observe(plan)

    assert observed.hosts["trt-gb10-1"].exact
    assert calls == 3
    assert sleeps == [0.5, 0.5]


def test_fixed_transport_does_not_settle_a_durably_non_exact_host(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1")
    calls = 0
    sleeps: list[float] = []
    # checkout_exact is False -- a real convergence mutation is required -- so even
    # with the timer firing the observation is authoritative and returned at once.
    payload = {
        "baseline_ready": True,
        "boot_id": "boot-1",
        "candidate_source_exact": True,
        "checkout_exact": False,
        "environment_exact": True,
        "legacy_absent": True,
        "service_timer_exact": False,
        "service_timer_transient": True,
        "units_exact": True,
    }

    def run(argv):
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")

    observed = _retrying_transport(run, _target(), sleeps=sleeps).observe(plan)

    host = observed.hosts["trt-gb10-1"]
    assert not host.exact
    assert host.applicable
    assert calls == 1
    assert sleeps == []


def test_fixed_transport_apply_retries_transient_failure(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1", "trt-gb10-2")
    calls = 0
    sleeps: list[float] = []

    def run(argv):
        nonlocal calls
        calls += 1
        if calls < 2:
            return subprocess.CompletedProcess(argv, 255, "", "reset")
        return subprocess.CompletedProcess(argv, 0, "", "")

    convergence = GB10ConvergencePlan(
        state=GB10ConvergenceState.READY,
        mutations=(GB10HostMutation("trt-gb10-2", (GB10MutationKind.SERVICE_TIMER,)),),
        blockers={},
        evidence_digest="1" * 64,
    )
    # A retried transient apply failure must converge without raising.
    _retrying_transport(run, _target(), _target("trt-gb10-2"), sleeps=sleeps, attempts=3).apply(
        plan, convergence
    )

    assert calls == 2
    assert sleeps == [0.5]


def test_fixed_transport_reports_only_validated_failed_hosts(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1", "trt-gb10-2")

    def run(argv):
        host = argv[-2]
        return subprocess.CompletedProcess(
            argv,
            0 if host == "trt-gb10-1" else 255,
            "",
            "" if host == "trt-gb10-1" else "private transport detail",
        )

    convergence = GB10ConvergencePlan(
        state=GB10ConvergenceState.READY,
        mutations=(
            GB10HostMutation("trt-gb10-1", (GB10MutationKind.SERVICE_TIMER,)),
            GB10HostMutation("trt-gb10-2", (GB10MutationKind.SERVICE_TIMER,)),
        ),
        blockers={},
        evidence_digest="1" * 64,
    )

    with pytest.raises(GB10FleetApplyError) as caught:
        _transport(run, _target(), _target("trt-gb10-2")).apply(plan, convergence)

    assert caught.value.failed_hosts == ("trt-gb10-2",)
    assert "private transport detail" not in str(caught.value)


def test_fixed_remote_programs_compile_and_apply_rejects_noncanonical_order(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1")
    target = _target()
    compile(_remote_observation_source(target, plan), "<gb10-observe>", "exec")
    compile(
        _remote_apply_source(
            target,
            plan,
            (GB10MutationKind.LEGACY_RETIRE, GB10MutationKind.SERVICE_TIMER),
        ),
        "<gb10-apply>",
        "exec",
    )
    transport = _transport(
        lambda argv: subprocess.CompletedProcess(argv, 0, "", ""),
        target,
    )
    convergence = GB10ConvergencePlan(
        state=GB10ConvergenceState.READY,
        mutations=(
            GB10HostMutation(
                "trt-gb10-1",
                (GB10MutationKind.SERVICE_TIMER, GB10MutationKind.LEGACY_RETIRE),
            ),
        ),
        blockers={},
        evidence_digest="2" * 64,
    )
    with pytest.raises(RuntimeError, match="failed safely"):
        transport.apply(plan, convergence)


def test_remote_shared_candidate_git_uses_live_nfs_timeout_budget(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1")
    observation = _remote_observation_source(_target(), plan)
    apply = _remote_apply_source(
        _target(),
        plan,
        (GB10MutationKind.LEGACY_RETIRE, GB10MutationKind.SERVICE_TIMER),
    )

    assert _REMOTE_SHARED_GIT_TIMEOUT_SECONDS == 30
    assert "def run(argv, *, cwd=None, timeout_seconds=10):" in observation
    assert "timeout=timeout_seconds" in observation
    assert f"timeout_seconds={_REMOTE_SHARED_GIT_TIMEOUT_SECONDS}" in observation
    assert "def output(argv, *, timeout_seconds=20):" in apply
    assert "timeout=timeout_seconds" in apply
    assert f"timeout_seconds={_REMOTE_SHARED_GIT_TIMEOUT_SECONDS}" in apply


def test_remote_retirement_uses_only_service_owned_candidate_and_disables_agents(
    tmp_path,
) -> None:
    plan = _plan(tmp_path, "trt-gb10-1")
    apply = _remote_apply_source(
        _target(),
        plan,
        (GB10MutationKind.LEGACY_RETIRE, GB10MutationKind.SERVICE_TIMER),
    )
    shared = (
        "/shared_work2/loom-staging-rollout/worker-repos/"
        f"loom-remote-worker-staging-{plan.candidate_sha[:7]}"
    )
    assert f"shared = pathlib.Path({shared!r})" in apply
    assert "/home/qianyi" not in apply
    assert "git fetch" not in apply
    assert (
        'run(["systemctl", "--user", "disable", "--now", unit], accept_missing=True)'
        in apply
    )
    assert 'run(["systemctl", "--user", "reset-failed", unit], accept_missing=True)' in apply
    assert "safe.directory=*" not in apply
    assert "config --global" not in apply
    assert "config --system" not in apply


def test_fixed_transport_aggregates_unavailable_hosts_and_rejects_inventory_drift(
    tmp_path,
) -> None:
    targets = (_target(), _target("trt-gb10-2"))
    plan = _plan(tmp_path, "trt-gb10-1", "trt-gb10-2")
    transport = _transport(
        lambda argv: subprocess.CompletedProcess(argv, 255, "", "unavailable"),
        *targets,
    )

    observed = transport.observe(plan)
    assert not any(host.applicable for host in observed.hosts.values())
    assert {host.boot_id for host in observed.hosts.values()} == {"unavailable"}

    with pytest.raises(ValueError, match="inventory drifted"):
        transport.observe(replace(plan, gb10_boot_ids={"trt-gb10-1": "boot-1"}))


@pytest.mark.parametrize("service", ("relative", "other.service", "../escape.service"))
def test_fixed_transport_target_rejects_service_authority_drift(service) -> None:
    with pytest.raises(ValueError, match="outside fixed authority"):
        GB10TransportTarget(
            ssh_target="trt-gb10-1",
            node_agent_service=service,
        )


@pytest.mark.parametrize(
    "profile",
    ("staging.cluster.toml", "staging.multinode.cluster.toml"),
)
def test_fixed_transport_factory_binds_checked_in_staging_inventory(profile: str) -> None:
    repo_root = Path(__file__).resolve().parents[4]
    hosts = tuple(f"trt-gb10-{index}" for index in range(1, 16))
    transport = build_fixed_gb10_ssh_transport(
        repo_root / "deploy/environments" / profile,
        expected_hosts=hosts,
        run=lambda argv: subprocess.CompletedProcess(argv, 0, "", ""),
        max_concurrency=4,
    )

    assert tuple(target.ssh_target for target in transport.targets) == hosts
    assert {target.node_agent_service for target in transport.targets} == {
        "loom-gb10-node-agent.service"
    }
    assert transport.identity == Path("/var/lib/loom-staging-rollout/gb10-deploy-ed25519")
    assert transport.ssh_config == (repo_root / "deploy/worker-pools/gb10/ssh_config").resolve()
