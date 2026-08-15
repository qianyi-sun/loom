from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from loom_capacity_manager.contracts import NodeEnvelopeV1, ResourceVectorV1
from loom_capacity_manager.executable_contracts import ExecutionContextV2
from loom_capacity_pool_executor.slurm_inventory import (
    SlurmInventoryPolicy,
    SlurmReportBinding,
    SubprocessReadOnlySlurmCommandRunner,
    build_slurm_capacity_reports,
    capture_slurm_capacity_reports,
)


def _quantity(number: int) -> dict[str, object]:
    return {"set": True, "infinite": False, "number": number}


def _node(name: str, *, state: str = "IDLE") -> dict[str, object]:
    return {
        "name": name,
        "partitions": ["gb10"],
        "state": [state],
        "cpus": 20,
        "effective_cpus": 20,
        "real_memory": 115_000,
        "gres": "gpu:1",
    }


def _documents() -> tuple[dict[str, object], dict[str, object]]:
    nodes = {
        "errors": [],
        "warnings": [],
        "last_update": _quantity(77),
        "nodes": [
            _node("trt-gb10-1"),
            _node("trt-gb10-10", state="MIXED"),
            _node("trt-gb10-16", state="ALLOCATED"),
        ],
    }
    jobs = {
        "errors": [],
        "warnings": [],
        "last_update": _quantity(77),
        "jobs": [
            {
                "job_id": 42,
                "job_state": ["RUNNING"],
                "partition": "gb10",
                "required_nodes": "",
                "nodes": "trt-gb10-10",
                "cpus": _quantity(4),
                "memory_per_node": _quantity(23_000),
                "memory_per_cpu": _quantity(0) | {"set": False},
                "node_count": _quantity(1),
                "tres_alloc_str": "cpu=4,mem=23000M,node=1,billing=4",
                "tres_req_str": "cpu=4,mem=23000M,node=1,billing=4",
                "job_resources": {
                    "allocated_cores": 4,
                    "allocated_nodes": [
                        {
                            "nodename": "trt-gb10-10",
                            "memory_allocated": 23_000,
                            "sockets": {
                                "0": {
                                    "cores": {
                                        "0": "allocated",
                                        "1": "allocated",
                                        "2": "allocated",
                                        "3": "allocated",
                                    }
                                }
                            },
                        }
                    ],
                },
            },
            {
                "job_id": 43,
                "job_state": ["RUNNING"],
                "partition": "gb10",
                "required_nodes": "",
                "nodes": "trt-gb10-16",
                "cpus": _quantity(20),
                "memory_per_node": _quantity(115_000),
                "memory_per_cpu": _quantity(0) | {"set": False},
                "node_count": _quantity(1),
                "tres_alloc_str": "cpu=20,mem=115000M,node=1,billing=20",
                "tres_req_str": "cpu=20,mem=115000M,node=1,billing=20",
                "job_resources": {
                    "allocated_cores": 20,
                    "allocated_nodes": [
                        {
                            "nodename": "trt-gb10-16",
                            "memory_allocated": 115_000,
                            "sockets": {},
                        }
                    ],
                },
            },
        ],
    }
    return nodes, jobs


def _policy() -> SlurmInventoryPolicy:
    return SlurmInventoryPolicy(
        pool_id="gb10",
        pool_generation=3,
        reporter_incarnation=UUID("10000000-0000-4000-8000-000000000001"),
        nodes=(
            NodeEnvelopeV1(
                node_id="trt-gb10-1",
                allocatable=ResourceVectorV1(
                    slots=10,
                    cpu_millicores=20_000,
                    memory_bytes=115_000 * 1024**2,
                    gpu_count=1,
                ),
            ),
            NodeEnvelopeV1(
                node_id="trt-gb10-10",
                allocatable=ResourceVectorV1(
                    slots=10,
                    cpu_millicores=20_000,
                    memory_bytes=115_000 * 1024**2,
                    gpu_count=1,
                ),
            ),
        ),
        relevant_partitions=("gb10",),
        slot_resources=ResourceVectorV1(
            slots=1,
            cpu_millicores=2_000,
            memory_bytes=11_500 * 1024**2,
        ),
    )


def _binding() -> SlurmReportBinding:
    return SlurmReportBinding(
        pool_sequence=5,
        inventory_sequence=8,
        execution=ExecutionContextV2(
            authority_incarnation=UUID("20000000-0000-4000-8000-000000000001"),
            writer_epoch=4,
            configuration_epoch=6,
            execution_epoch=2,
            execution_manifest_sha256="a" * 64,
            execution_state="prepared",
            executable_new_capacity_ceiling=0,
            executable_new_capacity_rate_per_minute=0,
            trusted_fleet_release_sha256="b" * 64,
        ),
        executor_id="gb10-global-executor",
        executor_incarnation=UUID("30000000-0000-4000-8000-000000000001"),
        journal_sequence=0,
        journal_digest="0" * 64,
    )


def test_busy_canonical_node_is_charged_and_out_of_authority_node_is_ignored() -> None:
    node_document, job_document = _documents()

    reports = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    assert reports.controller_last_update == 77
    assert reports.pool_observation.health == "eligible"
    assert len(reports.pool_observation.commitments) == 1
    commitment = reports.pool_observation.commitments[0]
    assert commitment.physical_identity == "slurm-job-42"
    assert commitment.state == "quarantined"
    assert commitment.node_ids == ("trt-gb10-10",)
    assert commitment.resources == ResourceVectorV1(
        slots=2,
        cpu_millicores=4_000,
        memory_bytes=23_000 * 1024**2,
    )

    assert len(reports.executable_inventory.records) == 1
    inventory_record = reports.executable_inventory.records[0]
    assert inventory_record.physical_identity == commitment.physical_identity
    assert inventory_record.authority_scope == "foreign"
    assert inventory_record.state == "active"
    assert inventory_record.node_ids == commitment.node_ids
    assert inventory_record.resources == commitment.resources
    assert inventory_record.controller_evidence_sha256 == reports.controller_sha256


def test_snapshot_with_controller_warning_is_rejected() -> None:
    node_document, job_document = _documents()
    node_document["warnings"] = [{"description": "partial controller response"}]

    with pytest.raises(ValueError, match="warnings"):
        build_slurm_capacity_reports(
            node_document,
            job_document,
            policy=_policy(),
            binding=_binding(),
            source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
        )


def test_controller_digest_binds_every_canonical_node_state() -> None:
    node_document, job_document = _documents()
    first = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )
    nodes = node_document["nodes"]
    assert isinstance(nodes, list)
    canonical = nodes[0]
    assert isinstance(canonical, dict)
    canonical["state"] = ["MIXED"]

    changed = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 1, tzinfo=UTC),
    )

    assert changed.controller_sha256 != first.controller_sha256


def test_duplicate_canonical_node_policy_is_rejected() -> None:
    policy = _policy()

    with pytest.raises(ValueError, match="duplicate canonical node"):
        SlurmInventoryPolicy(
            pool_id=policy.pool_id,
            pool_generation=policy.pool_generation,
            reporter_incarnation=policy.reporter_incarnation,
            nodes=(policy.nodes[0], policy.nodes[0]),
            relevant_partitions=policy.relevant_partitions,
            slot_resources=policy.slot_resources,
        )


def test_policy_normalizes_numerically_listed_hosts_to_contract_order() -> None:
    policy = _policy()
    node_two = NodeEnvelopeV1(
        node_id="trt-gb10-2",
        allocatable=policy.nodes[0].allocatable,
    )

    normalized = SlurmInventoryPolicy(
        pool_id=policy.pool_id,
        pool_generation=policy.pool_generation,
        reporter_incarnation=policy.reporter_incarnation,
        nodes=(policy.nodes[0], node_two, policy.nodes[1]),
        relevant_partitions=policy.relevant_partitions,
        slot_resources=policy.slot_resources,
    )

    assert tuple(node.node_id for node in normalized.nodes) == (
        "trt-gb10-1",
        "trt-gb10-10",
        "trt-gb10-2",
    )


def test_policy_rejects_a_canonical_node_without_physical_slots() -> None:
    policy = _policy()
    zero = policy.nodes[0].model_copy(
        update={"allocatable": policy.nodes[0].allocatable.model_copy(update={"slots": 0})}
    )

    with pytest.raises(ValueError, match="positive allocatable slots"):
        SlurmInventoryPolicy(
            pool_id=policy.pool_id,
            pool_generation=policy.pool_generation,
            reporter_incarnation=policy.reporter_incarnation,
            nodes=(zero, policy.nodes[1]),
            relevant_partitions=policy.relevant_partitions,
            slot_resources=policy.slot_resources,
        )


def test_controller_node_names_map_case_insensitively_to_canonical_ids() -> None:
    node_document, job_document = _documents()
    nodes = node_document["nodes"]
    assert isinstance(nodes, list)
    for raw in nodes[:2]:
        assert isinstance(raw, dict)
        name = raw["name"]
        assert isinstance(name, str)
        raw["name"] = name.upper()
    jobs = job_document["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    resources = job["job_resources"]
    assert isinstance(resources, dict)
    allocated_nodes = resources["allocated_nodes"]
    assert isinstance(allocated_nodes, list)
    allocated = allocated_nodes[0]
    assert isinstance(allocated, dict)
    allocated["nodename"] = "TRT-GB10-10"
    job["nodes"] = "TRT-GB10-10"

    reports = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    assert reports.pool_observation.commitments[0].node_ids == ("trt-gb10-10",)
    assert reports.executable_inventory.records[0].node_ids == ("trt-gb10-10",)


def test_unhealthy_canonical_node_remains_visible_as_full_quarantine() -> None:
    node_document, job_document = _documents()
    nodes = node_document["nodes"]
    assert isinstance(nodes, list)
    canonical = nodes[0]
    assert isinstance(canonical, dict)
    canonical["state"] = ["IDLE", "DRAIN"]
    job_document["jobs"] = []

    reports = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    assert reports.pool_observation.health == "eligible"
    assert len(reports.pool_observation.commitments) == 1
    commitment = reports.pool_observation.commitments[0]
    assert commitment.commitment_id == "slurm-node-trt-gb10-1-unavailable"
    assert commitment.node_ids == ("trt-gb10-1",)
    assert commitment.resources == _policy().nodes[0].allocatable
    assert commitment.state == "quarantined"

    assert len(reports.executable_inventory.records) == 1
    record = reports.executable_inventory.records[0]
    assert record.physical_identity == commitment.physical_identity
    assert record.physical_kind == "worker"
    assert record.authority_scope == "foreign"
    assert record.state == "unknown"
    assert record.resources == commitment.resources


def test_pending_foreign_job_for_shared_partition_is_charged_without_a_node() -> None:
    node_document, job_document = _documents()
    job_document["jobs"] = [
        {
            "job_id": 44,
            "job_state": ["PENDING"],
            "partition": "gb10",
            "required_nodes": "",
            "nodes": "",
            "cpus": _quantity(20),
            "memory_per_node": _quantity(115_000),
            "memory_per_cpu": _quantity(0) | {"set": False},
            "node_count": _quantity(1),
            "tres_alloc_str": "",
            "tres_req_str": "cpu=20,mem=115000M,node=1,billing=20",
            "job_resources": {
                "allocated_cores": 0,
                "allocated_nodes": None,
            },
        }
    ]

    reports = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    assert len(reports.pool_observation.commitments) == 1
    commitment = reports.pool_observation.commitments[0]
    assert commitment.physical_identity == "slurm-job-44"
    assert commitment.node_ids == ()
    assert commitment.resources == ResourceVectorV1(
        slots=10,
        cpu_millicores=20_000,
        memory_bytes=115_000 * 1024**2,
    )
    record = reports.executable_inventory.records[0]
    assert record.physical_identity == commitment.physical_identity
    assert record.state == "pending"
    assert record.node_ids == ()


def test_pending_job_in_any_partition_of_a_canonical_node_is_charged() -> None:
    node_document, job_document = _documents()
    nodes = node_document["nodes"]
    assert isinstance(nodes, list)
    canonical = nodes[0]
    assert isinstance(canonical, dict)
    canonical["partitions"] = ["gb10", "shared"]
    job_document["jobs"] = [
        {
            "job_id": 45,
            "job_state": ["PENDING"],
            "partition": "shared",
            "required_nodes": "",
            "nodes": "",
            "cpus": _quantity(2),
            "memory_per_node": _quantity(11_500),
            "memory_per_cpu": _quantity(0) | {"set": False},
            "node_count": _quantity(1),
            "tres_alloc_str": "",
            "tres_req_str": "cpu=2,mem=11500M,node=1,billing=2",
            "job_resources": {"allocated_cores": 0, "allocated_nodes": []},
        }
    ]

    reports = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    assert [item.physical_identity for item in reports.pool_observation.commitments] == [
        "slurm-job-45"
    ]


def test_gpu_tres_is_included_in_the_physical_resource_charge() -> None:
    node_document, job_document = _documents()
    jobs = job_document["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    job["tres_alloc_str"] = "cpu=4,mem=23000M,node=1,gres/gpu:gb10=1,billing=4"
    job_document["jobs"] = [job]

    reports = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    assert reports.pool_observation.commitments[0].resources.gpu_count == 1
    assert reports.executable_inventory.records[0].resources.gpu_count == 1


def test_single_node_allocation_uses_total_cpu_when_core_bitmap_is_absent() -> None:
    node_document, job_document = _documents()
    jobs = job_document["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    job["cpus"] = _quantity(20)
    resources = job["job_resources"]
    assert isinstance(resources, dict)
    allocated_nodes = resources["allocated_nodes"]
    assert isinstance(allocated_nodes, list)
    allocated = allocated_nodes[0]
    assert isinstance(allocated, dict)
    allocated["sockets"] = {}
    job_document["jobs"] = [job]

    reports = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    assert reports.pool_observation.commitments[0].resources.cpu_millicores == 20_000


@pytest.mark.asyncio
async def test_capture_retries_a_mixed_update_without_adding_mutation_commands() -> None:
    node_document, job_document = _documents()
    raced_jobs = dict(job_document)
    raced_jobs["last_update"] = _quantity(78)
    outputs = iter(
        (
            json.dumps(node_document).encode("utf-8"),
            json.dumps(raced_jobs).encode("utf-8"),
            json.dumps(node_document).encode("utf-8"),
            json.dumps(job_document).encode("utf-8"),
        )
    )
    commands: list[tuple[str, ...]] = []

    class Runner:
        async def run(self, command: tuple[str, ...]) -> bytes:
            commands.append(command)
            return next(outputs)

    reports = await capture_slurm_capacity_reports(
        Runner(),
        scontrol_path="/usr/bin/scontrol",
        squeue_path="/usr/bin/squeue",
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
        max_attempts=2,
    )

    assert reports.controller_last_update == 77
    assert commands == [
        ("/usr/bin/scontrol", "show", "nodes", "--json"),
        ("/usr/bin/squeue", "--json"),
        ("/usr/bin/scontrol", "show", "nodes", "--json"),
        ("/usr/bin/squeue", "--json"),
    ]


def _json_program(path: Path, document: dict[str, object]) -> None:
    path.write_text(
        f"#!{sys.executable}\nimport sys\nsys.stdout.write({json.dumps(document)!r})\n",
        encoding="utf-8",
    )
    path.chmod(0o700)


@pytest.mark.asyncio
async def test_subprocess_runner_captures_the_exact_read_only_json_surface(
    tmp_path: Path,
) -> None:
    node_document, job_document = _documents()
    scontrol = tmp_path / "scontrol"
    squeue = tmp_path / "squeue"
    _json_program(scontrol, node_document)
    _json_program(squeue, job_document)
    runner = SubprocessReadOnlySlurmCommandRunner(
        scontrol_path=str(scontrol),
        squeue_path=str(squeue),
        timeout_seconds=2,
    )

    reports = await capture_slurm_capacity_reports(
        runner,
        scontrol_path=str(scontrol),
        squeue_path=str(squeue),
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    assert reports.controller_last_update == 77
    assert len(reports.executable_inventory.records) == 1


@pytest.mark.asyncio
async def test_subprocess_runner_rejects_every_non_inventory_command(tmp_path: Path) -> None:
    scontrol = tmp_path / "scontrol"
    squeue = tmp_path / "squeue"
    _json_program(scontrol, {})
    _json_program(squeue, {})
    runner = SubprocessReadOnlySlurmCommandRunner(
        scontrol_path=str(scontrol),
        squeue_path=str(squeue),
        timeout_seconds=2,
    )

    with pytest.raises(ValueError, match="read-only"):
        await runner.run((str(scontrol), "update", "job", "42"))
