"""Exercise final policy binding through the real controller/actuator paths."""

import time
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from loom_control_plane import elastic_slurm_worker_controller as controller
from loom_control_plane import worker_pool_autoscaler as autoscaler
from loom_control_plane.slurm_memory_probe import MemoryObservation
from tests.unit.test_elastic_slurm_worker_controller import _config
from tests.unit.test_worker_pool_autoscaler import _FakeResult, _FakeSession, _policy_row


def _wire(monkeypatch: pytest.MonkeyPatch, *, normal_probe_failure: bool = False):  # type: ignore[no-untyped-def]
    observed: list[tuple[str, str]] = []
    submitted: list[tuple[str, str]] = []

    async def snapshot(self, nodes):  # type: ignore[no-untyped-def]
        return {
            node: controller.SlurmNodeResource(
                node, "mixed", 24, 100000, 1.0, 24, schedulable_memory_mib=100000
            )
            for node in nodes
        }

    async def memory(self, node):  # type: ignore[no-untyped-def]
        observed.append((node, self._config.slurm_qos))
        # A warm-floor job can have permission/availability only in boost QoS.
        if self._config.slurm_qos == "normal" and not submitted:
            raise RuntimeError("normal unavailable below the warm floor")
        if normal_probe_failure and self._config.slurm_qos == "normal" and node == "oldlab-2":
            raise RuntimeError("fresh final-QoS evidence unavailable")
        return MemoryObservation(100000, time.monotonic())

    async def run(args, **_kwargs):  # type: ignore[no-untyped-def]
        assert args[0] == "sbatch"
        node = next(arg.split("=", 1)[1] for arg in args if arg.startswith("--nodelist="))
        qos = next((arg.split("=", 1)[1] for arg in args if arg.startswith("--qos=")), "")
        submitted.append((node, qos))
        return controller._CommandResult(str(100 + len(submitted)), "")

    async def record(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        return None

    monkeypatch.setattr(
        controller.SubprocessSlurmCommandRunner, "_query_node_resource_snapshot", snapshot
    )
    monkeypatch.setattr(
        controller.SubprocessSlurmCommandRunner, "_query_node_available_memory", memory
    )
    monkeypatch.setattr(controller, "_run_command", run)
    monkeypatch.setattr(controller, "record_slurm_worker_job", record)
    monkeypatch.setattr(autoscaler, "record_slurm_worker_job", record)
    return observed, submitted


@pytest.mark.parametrize("normal_probe_failure", [False, True])
async def test_autoscaler_probes_initial_boost_then_rebinds_at_floor(
    monkeypatch: pytest.MonkeyPatch,
    normal_probe_failure: bool,
) -> None:
    observed, submitted = _wire(monkeypatch, normal_probe_failure=normal_probe_failure)
    policy = _policy_row(
        min_slots=1,
        max_slots=3,
        actuator_config={
            "allowed_nodes": ["oldlab-1", "oldlab-2", "oldlab-3"],
            "env_file": "/secure/worker.env",
            "repo_dir": "/opt/loom",
            "partition": "loom-staging",
            "slurm_cluster_name": "trt-oldlab",
            "qos_normal": "normal",
            "qos_boost": "boost",
            "requested_cpus": 2,
            "requested_memory_mib": 8192,
            "requested_concurrency": 1,
            "resource_aware": True,
            "probe_mem_available": True,
            "max_concurrency_per_node": 1,
            "max_jobs": 3,
            "pending_job_cap": 3,
            "job_pids_max": 4096,
            "candidate_sha": "a" * 40,
            "container_cpus": 2,
            "container_memory_mib": 8192,
            "container_pids": 512,
        },
    )
    result = await autoscaler._apply_slurm_scale_up(
        cast(Any, _FakeSession([_FakeResult(scalars=[])])),
        policy,
        autoscaler.AutoscalerDecision(
            action="scale_up",
            reason="queued_deficit",
            desired_slots=3,
            actual_slots=0,
            pending_slots=0,
            draining_slots=0,
            occupied_slots=0,
            queued_slots=3,
        ),
        runner=None,
        now=datetime(2026, 9, 4, tzinfo=UTC),
    )
    if normal_probe_failure:
        assert result.error is not None and "fresh candidate-bound" in result.error
    else:
        assert result.error is None
    assert result.blocked_reason is None
    assert submitted == (
        [("oldlab-1", "boost"), ("oldlab-3", "normal")]
        if normal_probe_failure
        else [("oldlab-1", "boost"), ("oldlab-2", "normal"), ("oldlab-3", "normal")]
    )
    assert observed == [
        ("oldlab-1", "boost"),
        ("oldlab-2", "boost"),
        ("oldlab-3", "boost"),
        ("oldlab-2", "normal"),
        ("oldlab-3", "normal"),
    ]


@pytest.mark.parametrize("canonical", ["OLDLAB-1", "oldlab-1"])
async def test_standalone_rebinds_canonical_and_reduced_allowlist_before_probe(
    monkeypatch: pytest.MonkeyPatch,
    canonical: str,
) -> None:
    observed, submitted = _wire(monkeypatch)
    config = _config(
        allowed_nodes=("oldlab-1", "unknown"),
        resource_aware=True,
        probe_mem_available=True,
        requested_cpus=2,
        requested_memory_mib=8192,
        requested_concurrency=1,
        max_concurrency_per_node=1,
        job_pids_max=4096,
        candidate_sha="a" * 40,
        container_cpus=2,
        container_memory_mib=8192,
        container_pids=512,
    )
    runner = controller.SubprocessSlurmCommandRunner().bind_config(config)

    async def resolve(_nodes):  # type: ignore[no-untyped-def]
        return {"oldlab-1": canonical}

    async def load(*_args):  # type: ignore[no-untyped-def]
        return controller.SlurmWorkerCapacitySnapshot(
            queued_trials=1,
            running_trials=0,
            pending_jobs=0,
            running_jobs=0,
            active_slots=0,
            pending_slots=0,
            active_nodes=set(),
            cancellable_pending_job_ids=(),
            active_job_ids=(),
        )

    monkeypatch.setattr(runner, "resolve_node_names", resolve)
    monkeypatch.setattr(controller, "load_capacity_snapshot", load)
    result = await controller.run_elastic_slurm_worker_controller_once(
        cast(Any, _FakeSession()),
        config=config,
        runner=runner,
    )
    assert result.submitted_job_ids == ("101",)
    assert submitted == [(canonical, "")]
    assert observed == [(canonical, "")]
    assert runner._config.allowed_nodes == (canonical,)
