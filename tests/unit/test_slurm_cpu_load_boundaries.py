"""#1803: only finite, nonnegative measured load can authorize capacity."""

from __future__ import annotations

from dataclasses import asdict

import pytest

from loom_control_plane import elastic_slurm_worker_controller as controller
from tests.unit.test_elastic_slurm_worker_controller import _config


@pytest.mark.parametrize("load", [float("nan"), float("inf"), float("-inf"), -1.0])
@pytest.mark.parametrize("parsed", [False, True])
def test_invalid_load_never_admits(load: float, parsed: bool) -> None:
    resource = (
        controller.parse_sinfo_node_resources(f"n1|mixed|20|110000|100000|{load}|0/20/0/20")["n1"]
        if parsed
        else controller.SlurmNodeResource("n1", "mixed", 20, 100000, load, idle_cpus=20)
    )
    plan = controller.compute_node_capacity_plan(
        _config(), node="n1", resource=resource, active_nodes=set()
    )
    assert plan.safe_slots == 0


@pytest.mark.parametrize("ratio", [float("nan"), float("inf"), float("-inf"), 0.0, -1.0])
def test_invalid_load_ratio_rejected(ratio: float) -> None:
    resource = controller.SlurmNodeResource("n1", "mixed", 20, 100000, 1.0, idle_cpus=20)
    assert (
        controller.compute_node_capacity_plan(
            _config(max_cpu_load_ratio=ratio), node="n1", resource=resource, active_nodes=set()
        ).safe_slots
        == 0
    )
    fields = asdict(
        _config(
            container_cpus=2,
            container_memory_mib=8192,
            container_pids=512,
            max_concurrency_per_node=6,
            candidate_sha="a" * 40,
            job_output_dir="/tmp/loom-output",
        )
    )
    fields.pop("requested_gpus")
    fields["allowed_nodes_csv"] = ",".join(fields.pop("allowed_nodes"))
    fields["max_cpu_load_ratio"] = ratio
    with pytest.raises(ValueError, match="max_cpu_load_ratio"):
        controller.build_controller_config(enabled=True, **fields)


@pytest.mark.parametrize("load,slots", [(0.0, 8), (21.0, 0)])
def test_zero_and_high_load_keep_existing_meaning(load: float, slots: int) -> None:
    resource = controller.SlurmNodeResource("n1", "mixed", 20, 100000, load, idle_cpus=20)
    plan = controller.compute_node_capacity_plan(
        _config(), node="n1", resource=resource, active_nodes=set()
    )
    assert plan.safe_slots == slots
