from __future__ import annotations

import json
import subprocess
from dataclasses import replace
from pathlib import Path, PurePosixPath

import pytest

from loom_cli.rollout.gb10_convergence import (
    GB10ConvergencePlan,
    GB10ConvergenceState,
    GB10HostMutation,
    GB10MutationKind,
)
from loom_cli.rollout.operator.protected_gb10_transport import (
    FixedGB10SSHTransport,
    GB10TransportTarget,
    _remote_apply_source,
    _remote_observation_source,
)
from tests.loom_cli.rollout.operator.test_protected_migration_component import _published_plan


def _target(host: str = "trt-gb10-1") -> GB10TransportTarget:
    return GB10TransportTarget(
        ssh_target=host,
        repo_path=PurePosixPath("/home/qianyi/loom-worker-build-staging"),
        env_file_path=PurePosixPath("/home/qianyi/loom-worker-build-staging/.env"),
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
    assert "/shared_work2/qianyi/.loom-staging-rollout/worker-repos" in argv[-1]
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
                (GB10MutationKind.ENVIRONMENT, GB10MutationKind.UNITS),
            ),
        ),
        blockers={},
        evidence_digest="1" * 64,
    )
    _transport(run, _target(), _target("trt-gb10-2")).apply(plan, convergence)

    assert len(calls) == 1
    assert calls[0][-2] == "trt-gb10-2"
    assert "environment" in calls[0][-1]
    assert "units" in calls[0][-1]
    assert plan.candidate_sha in calls[0][-1]
    assert "loom-gb10-worker.service" in calls[0][-1]
    assert "curl" not in calls[0][-1]


def test_fixed_remote_programs_compile_and_apply_rejects_noncanonical_order(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1")
    target = _target()
    compile(_remote_observation_source(target, plan), "<gb10-observe>", "exec")
    compile(
        _remote_apply_source(
            target,
            plan,
            (GB10MutationKind.CHECKOUT, GB10MutationKind.SERVICE_TIMER),
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
                (GB10MutationKind.UNITS, GB10MutationKind.ENVIRONMENT),
            ),
        ),
        blockers={},
        evidence_digest="2" * 64,
    )
    with pytest.raises(RuntimeError, match="failed safely"):
        transport.apply(plan, convergence)


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


@pytest.mark.parametrize(
    ("repo", "env_file"),
    (
        ("relative", "/home/qianyi/loom-worker-build-staging/.env"),
        ("/home/qianyi/repo/../escape", "/home/qianyi/escape/.env"),
        ("/home/qianyi/repo", "/tmp/.env"),
    ),
)
def test_fixed_transport_target_rejects_path_authority_drift(repo, env_file) -> None:
    with pytest.raises(ValueError, match="outside fixed authority"):
        GB10TransportTarget(
            ssh_target="trt-gb10-1",
            repo_path=PurePosixPath(repo),
            env_file_path=PurePosixPath(env_file),
            node_agent_service="loom-gb10-node-agent.service",
        )
