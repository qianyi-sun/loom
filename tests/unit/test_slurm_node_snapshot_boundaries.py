"""Admission boundaries for partition-scoped Slurm node observations."""

from __future__ import annotations

import json
from dataclasses import asdict

import pytest

from loom_control_plane import elastic_slurm_worker_controller as controller
from tests.unit.test_elastic_slurm_worker_controller import _config


@pytest.mark.parametrize("reader", ["resources", "allocated-memory"])
@pytest.mark.parametrize("duplicate", ["identical", "conflicting", "malformed"])
async def test_node_snapshot_duplicate_rows_cannot_erase_restrictions(
    monkeypatch: pytest.MonkeyPatch, reader: str, duplicate: str
) -> None:
    commands: list[tuple[str, ...]] = []

    async def run(args, **_kwargs):  # type: ignore[no-untyped-def]
        commands.append(args)
        if args[0] == "scontrol":
            payload = {
                "nodes": [{"name": "oldlab-4", "state": ["MIXED"]}],
                "errors": [],
                "warnings": [],
            }
            return controller._CommandResult(json.dumps(payload), "")
        if "-O" in args:
            first = (
                {"identical": "0", "conflicting": "8192", "malformed": "N/A"}[duplicate]
                if reader == "allocated-memory"
                else "0"
            )
            return controller._CommandResult(f"oldlab-4 {first}\noldlab-4 0\n", "")
        first = "MIXED+RESERVED" if duplicate != "identical" and reader == "resources" else "mixed"
        free_memory = "N/A" if duplicate == "malformed" and reader == "resources" else "100000"
        return controller._CommandResult(
            f"oldlab-4|{first}|24|120000|{free_memory}|1.0|0/24/0/24\n"
            "oldlab-4|mixed|24|120000|100000|1.0|0/24/0/24\n",
            "",
        )

    monkeypatch.setattr(controller, "_run_command", run)
    runner = controller.SubprocessSlurmCommandRunner().bind_config(
        _config(allowed_nodes=("oldlab-4",), resource_aware=True)
    )
    if duplicate != "identical":
        with pytest.raises(RuntimeError, match=r"ambiguous|invalid"):
            await runner.query_node_resources(("oldlab-4",))
    else:
        resources = await runner.query_node_resources(("oldlab-4",))
        assert resources["oldlab-4"].state == "mixed"
    assert all(command[0] != "srun" for command in commands)


@pytest.mark.parametrize("partition", ["loom-staging", ""])
async def test_node_snapshot_filters_both_sinfo_reads_to_configured_partition(
    monkeypatch: pytest.MonkeyPatch, partition: str
) -> None:
    commands: list[tuple[str, ...]] = []

    async def run(args, **_kwargs):  # type: ignore[no-untyped-def]
        commands.append(args)
        if args[0] == "scontrol":
            return controller._CommandResult(
                '{"errors": [], "warnings": [], "nodes": [{"name": "oldlab-4", "state": ["MIXED"]}]}',
                "",
            )
        return controller._CommandResult(
            "oldlab-4 0\n" if "-O" in args else "oldlab-4|mixed|24|120000|100000|1.0|0/24/0/24\n",
            "",
        )

    monkeypatch.setattr(controller, "_run_command", run)
    runner = controller.SubprocessSlurmCommandRunner().bind_config(
        _config(allowed_nodes=("oldlab-4",), resource_aware=True, partition=partition)
    )
    await runner.query_node_resources(("oldlab-4",))
    sinfo = [command for command in commands if command[0] == "sinfo"]
    assert len(sinfo) == 2
    for command in sinfo:
        if partition:
            assert command[command.index("-p") + 1] == partition
        else:
            assert "-p" not in command


@pytest.mark.parametrize("field", ["errors", "warnings"])
@pytest.mark.parametrize("value", [None, False, {}, "", ["unavailable"], "missing"])
def test_node_state_snapshot_requires_present_empty_diagnostic_arrays(
    field: str, value: object
) -> None:
    payload: dict[str, object] = {
        "errors": [],
        "warnings": [],
        "nodes": [{"name": "oldlab-4", "state": ["MIXED"]}],
    }
    if value == "missing":
        payload.pop(field)
    else:
        payload[field] = value
    with pytest.raises(RuntimeError, match="snapshot failed"):
        controller._parse_scontrol_node_states(json.dumps(payload), nodes=("oldlab-4",))


def test_resource_aware_reservation_requires_unavailable_ownership_authority() -> None:
    values = asdict(
        _config(
            container_cpus=2.0,
            container_memory_mib=4096,
            container_pids=512,
            candidate_sha="a" * 40,
            job_pids_max=4096,
            slurm_reservation="dedicated-owner",
        )
    )
    values["allowed_nodes_csv"] = ",".join(values.pop("allowed_nodes"))
    values.pop("requested_gpus")
    with pytest.raises(ValueError, match="reservation ownership"):
        controller.build_controller_config(enabled=True, **{**values, "resource_aware": True})
    config = controller.build_controller_config(enabled=True, **values)
    assert config is not None
    assert config.slurm_reservation == "dedicated-owner"


@pytest.mark.parametrize(
    "cpu_column",
    ["", "|N/A", "|0/24", "|0/24/0/24/0", "|-1/25/0/24", "|0/23/0/24", "|0/25/0/25"],
)
def test_node_snapshot_requires_complete_consistent_cpu_counts(cpu_column: str) -> None:
    with pytest.raises(RuntimeError, match="snapshot is invalid"):
        controller.parse_sinfo_node_resources("oldlab-4|mixed|24|120000|100000|1.0" + cpu_column)
