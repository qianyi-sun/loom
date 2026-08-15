from __future__ import annotations

import asyncio
import hashlib
import json
import os
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

import loom_capacity_pool_executor.slurm_inventory as slurm_inventory
from loom_capacity_manager.contracts import NodeEnvelopeV1, ResourceVectorV1
from loom_capacity_manager.executable_contracts import ExecutionContextV2
from loom_capacity_pool_executor.slurm_inventory import (
    SlurmInventoryPolicy,
    SlurmReportBinding,
    SlurmSnapshotRaceError,
    SubprocessReadOnlySlurmCommandRunner,
    capture_slurm_capacity_reports,
)
from loom_capacity_pool_executor.slurm_inventory import (
    build_slurm_capacity_reports as _build_slurm_capacity_reports,
)


def build_slurm_capacity_reports(
    node_document: object,
    job_document: object,
    **kwargs: object,
):
    return _build_slurm_capacity_reports(
        node_document,
        job_document,
        job_document,
        **kwargs,
    )


def _quantity(number: int) -> dict[str, object]:
    return {"set": True, "infinite": False, "number": number}


def _meta(cluster: str = "trt-gb10") -> dict[str, object]:
    return {
        "slurm": {
            "cluster": cluster,
            "version": {"major": "23", "minor": "11", "micro": "4"},
        },
        "plugin": {"data_parser": "data_parser/v0.0.40"},
    }


def _node(name: str, *, state: str = "IDLE") -> dict[str, object]:
    return {
        "name": name,
        "partitions": ["gb10"],
        "state": [state],
        "cpus": 20,
        "effective_cpus": 20,
        "real_memory": 115_000,
        "alloc_cpus": 0,
        "alloc_memory": 0,
        "gres": "gpu:1",
        "gres_used": "gpu:0",
    }


def _documents() -> tuple[dict[str, object], dict[str, object]]:
    nodes = {
        "errors": [],
        "warnings": [],
        "meta": _meta(),
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
        "meta": _meta(),
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
    canonical_busy = nodes["nodes"][1]
    assert isinstance(canonical_busy, dict)
    canonical_busy["alloc_cpus"] = 4
    canonical_busy["alloc_memory"] = 23_000
    return nodes, jobs


def _clear_canonical_allocation(node_document: dict[str, object]) -> None:
    nodes = node_document["nodes"]
    assert isinstance(nodes, list)
    busy = nodes[1]
    assert isinstance(busy, dict)
    busy["state"] = ["IDLE"]
    busy["alloc_cpus"] = 0
    busy["alloc_memory"] = 0
    busy["gres_used"] = "gpu:0"


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
        controller_cluster="trt-gb10",
        slurm_version=(23, 11, 4),
        data_parser="data_parser/v0.0.40",
        query_principal="loom-capacity-slurm-reader",
        query_uid=os.geteuid(),
        job_visibility_evidence_sha256="a" * 64,
        scontrol_sha256="c" * 64,
        squeue_sha256="d" * 64,
        slurm_conf_sha256="e" * 64,
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


def _controller_bound_policy() -> SlurmInventoryPolicy:
    policy = _policy()
    return SlurmInventoryPolicy(
        pool_id=policy.pool_id,
        pool_generation=policy.pool_generation,
        reporter_incarnation=policy.reporter_incarnation,
        nodes=policy.nodes,
        relevant_partitions=policy.relevant_partitions,
        slot_resources=policy.slot_resources,
        controller_cluster="trt-gb10",
        slurm_version=(23, 11, 4),
        data_parser="data_parser/v0.0.40",
        query_principal=policy.query_principal,
        query_uid=policy.query_uid,
        job_visibility_evidence_sha256=policy.job_visibility_evidence_sha256,
        scontrol_sha256="c" * 64,
        squeue_sha256="d" * 64,
        slurm_conf_sha256="e" * 64,
    )


def test_controller_identity_is_bound_to_protected_policy() -> None:
    node_document, job_document = _documents()
    job_document["meta"] = _meta("another-cluster")

    with pytest.raises(ValueError, match="controller metadata"):
        build_slurm_capacity_reports(
            node_document,
            job_document,
            policy=_controller_bound_policy(),
            binding=_binding(),
            source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
        )


def test_policy_rejects_an_unsupported_slurm_release() -> None:
    policy = _controller_bound_policy()

    with pytest.raises(ValueError, match=r"Slurm 23\.11"):
        SlurmInventoryPolicy(
            pool_id=policy.pool_id,
            pool_generation=policy.pool_generation,
            reporter_incarnation=policy.reporter_incarnation,
            nodes=policy.nodes,
            relevant_partitions=policy.relevant_partitions,
            slot_resources=policy.slot_resources,
            controller_cluster=policy.controller_cluster,
            slurm_version=(24, 5, 0),
            data_parser=policy.data_parser,
            query_principal=policy.query_principal,
            query_uid=policy.query_uid,
            job_visibility_evidence_sha256=policy.job_visibility_evidence_sha256,
            scontrol_sha256=policy.scontrol_sha256,
            squeue_sha256=policy.squeue_sha256,
            slurm_conf_sha256=policy.slurm_conf_sha256,
        )


def test_policy_requires_protected_full_visibility_query_evidence() -> None:
    policy = _policy()

    with pytest.raises(TypeError, match="query_principal"):
        SlurmInventoryPolicy(
            pool_id=policy.pool_id,
            pool_generation=policy.pool_generation,
            reporter_incarnation=policy.reporter_incarnation,
            nodes=policy.nodes,
            relevant_partitions=policy.relevant_partitions,
            slot_resources=policy.slot_resources,
            controller_cluster=policy.controller_cluster,
            slurm_version=policy.slurm_version,
            data_parser=policy.data_parser,
            scontrol_sha256=policy.scontrol_sha256,
            squeue_sha256=policy.squeue_sha256,
            slurm_conf_sha256=policy.slurm_conf_sha256,
        )


@pytest.mark.parametrize(
    "changes",
    (
        {"query_principal": "root/operator"},
        {"query_uid": 0},
        {"query_uid": True},
        {"job_visibility_evidence_sha256": "0" * 64},
        {"job_visibility_evidence_sha256": "g" * 64},
    ),
)
def test_policy_rejects_invalid_full_visibility_query_evidence(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match=r"query|visibility"):
        replace(_policy(), **changes)


def test_policy_rejects_a_noncanonical_partition_name() -> None:
    with pytest.raises(ValueError, match="partitions"):
        replace(_policy(), relevant_partitions=("gb10,shared",))


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
            controller_cluster=policy.controller_cluster,
            slurm_version=policy.slurm_version,
            data_parser=policy.data_parser,
            query_principal=policy.query_principal,
            query_uid=policy.query_uid,
            job_visibility_evidence_sha256=policy.job_visibility_evidence_sha256,
            scontrol_sha256=policy.scontrol_sha256,
            squeue_sha256=policy.squeue_sha256,
            slurm_conf_sha256=policy.slurm_conf_sha256,
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
        controller_cluster=policy.controller_cluster,
        slurm_version=policy.slurm_version,
        data_parser=policy.data_parser,
        query_principal=policy.query_principal,
        query_uid=policy.query_uid,
        job_visibility_evidence_sha256=policy.job_visibility_evidence_sha256,
        scontrol_sha256=policy.scontrol_sha256,
        squeue_sha256=policy.squeue_sha256,
        slurm_conf_sha256=policy.slurm_conf_sha256,
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
            controller_cluster=policy.controller_cluster,
            slurm_version=policy.slurm_version,
            data_parser=policy.data_parser,
            query_principal=policy.query_principal,
            query_uid=policy.query_uid,
            job_visibility_evidence_sha256=policy.job_visibility_evidence_sha256,
            scontrol_sha256=policy.scontrol_sha256,
            squeue_sha256=policy.squeue_sha256,
            slurm_conf_sha256=policy.slurm_conf_sha256,
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
    _clear_canonical_allocation(node_document)

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
    _clear_canonical_allocation(node_document)
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
    _clear_canonical_allocation(node_document)
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


def test_pending_job_in_multiple_partitions_is_charged_when_any_is_relevant() -> None:
    node_document, job_document = _documents()
    _clear_canonical_allocation(node_document)
    jobs = job_document["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    job["job_state"] = ["PENDING"]
    job["partition"] = "gb10,shared"
    job["job_resources"] = {"allocated_nodes": None}
    job_document["jobs"] = [job]

    reports = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    assert [item.physical_identity for item in reports.pool_observation.commitments] == [
        "slurm-job-42"
    ]


@pytest.mark.parametrize(
    "partition",
    ("", "gb10,", ",gb10", "gb10,,shared", "gb10,gb10", "gb10, shared", "gb10,*"),
)
def test_node_less_job_with_malformed_partition_list_is_rejected(partition: str) -> None:
    node_document, job_document = _documents()
    _clear_canonical_allocation(node_document)
    jobs = job_document["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    job["job_state"] = ["PENDING"]
    job["partition"] = partition
    job["job_resources"] = {"allocated_nodes": None}
    job_document["jobs"] = [job]

    with pytest.raises(ValueError, match="partition is invalid"):
        build_slurm_capacity_reports(
            node_document,
            job_document,
            policy=_policy(),
            binding=_binding(),
            source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
        )


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
            json.dumps(raced_jobs).encode("utf-8"),
            json.dumps(node_document).encode("utf-8"),
            json.dumps(job_document).encode("utf-8"),
            json.dumps(job_document).encode("utf-8"),
            json.dumps(node_document).encode("utf-8"),
            json.dumps(job_document).encode("utf-8"),
        )
    )
    commands: list[str] = []

    class Runner:
        async def run(self, command: str) -> bytes:
            commands.append(command)
            return next(outputs)

    reports = await capture_slurm_capacity_reports(
        Runner(),
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
        max_attempts=2,
    )

    assert reports.controller_last_update == 77
    assert commands == [
        "jobs",
        "nodes",
        "jobs",
        "jobs",
        "nodes",
        "jobs",
    ]


@pytest.mark.asyncio
async def test_subprocess_runner_owns_fixed_commands_and_minimal_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node_document, job_document = _documents()
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    verified: list[tuple[str, str]] = []

    class Stream:
        def __init__(self, payload: bytes) -> None:
            self.payload = payload

        async def read(self, _maximum: int) -> bytes:
            payload, self.payload = self.payload, b""
            return payload

    class Process:
        def __init__(self, payload: bytes) -> None:
            self.stdout = Stream(payload)
            self.stderr = Stream(b"")
            self.returncode = 0

        async def wait(self) -> int:
            return 0

        def kill(self) -> None:  # pragma: no cover - success must not kill
            raise AssertionError("successful inventory process was killed")

    async def create_subprocess_exec(*argv: object, **kwargs: object) -> Process:
        calls.append((argv, kwargs))
        document = node_document if argv[0] == "/usr/bin/scontrol" else job_document
        return Process(json.dumps(document).encode("utf-8"))

    monkeypatch.setattr(slurm_inventory, "_verify_trusted_file", lambda path, digest: verified.append((path, digest)), raising=False)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_subprocess_exec)
    runner = SubprocessReadOnlySlurmCommandRunner(
        policy=_policy(),
        timeout_seconds=2,
    )

    reports = await capture_slurm_capacity_reports(
        runner,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    assert reports.controller_last_update == 77
    assert len(reports.executable_inventory.records) == 1
    assert [call[0] for call in calls] == [
        ("/usr/bin/squeue", "--json"),
        ("/usr/bin/scontrol", "show", "nodes", "--json"),
        ("/usr/bin/squeue", "--json"),
    ]
    assert all(
        call[1]["env"]
        == {
            "HOME": "/",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/bin:/bin",
            "SLURM_CONF": "/etc/loom/capacity/slurm.conf",
            "SQUEUE_ALL": "1",
        }
        and call[1]["cwd"] == "/"
        for call in calls
    )
    assert verified


@pytest.mark.asyncio
async def test_subprocess_runner_rejects_every_non_inventory_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(slurm_inventory, "_verify_trusted_file", lambda _path, _digest: None, raising=False)
    runner = SubprocessReadOnlySlurmCommandRunner(
        policy=_policy(),
        timeout_seconds=2,
    )

    with pytest.raises(ValueError, match="read-only"):
        await runner.run("mutate")  # type: ignore[arg-type]


def test_subprocess_runner_rejects_a_different_effective_query_uid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        slurm_inventory,
        "_verify_trusted_file",
        lambda _path, _digest: None,
    )

    with pytest.raises(RuntimeError, match="query identity"):
        SubprocessReadOnlySlurmCommandRunner(
            policy=replace(_policy(), query_uid=os.geteuid() + 1),
            timeout_seconds=2,
        )


@pytest.mark.asyncio
async def test_subprocess_runner_reaps_child_when_caller_is_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()
    reader_cancelled = asyncio.Event()

    class BlockingStream:
        async def read(self, _maximum: int) -> bytes:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                reader_cancelled.set()
            return b""  # pragma: no cover - cancellation is required

    class Process:
        stdout = BlockingStream()
        stderr = BlockingStream()
        returncode: int | None = None
        killed = False
        reaped = False

        def kill(self) -> None:
            self.killed = True

        async def wait(self) -> int:
            self.reaped = True
            self.returncode = -9
            return -9

    process = Process()

    async def create_subprocess_exec(*_argv: object, **_kwargs: object) -> Process:
        return process

    monkeypatch.setattr(
        slurm_inventory,
        "_verify_trusted_file",
        lambda _path, _digest: None,
        raising=False,
    )
    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_subprocess_exec)
    runner = SubprocessReadOnlySlurmCommandRunner(policy=_policy(), timeout_seconds=2)
    task = asyncio.create_task(runner.run("jobs"))
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert process.killed
    assert process.reaped
    assert reader_cancelled.is_set()


@pytest.mark.asyncio
async def test_subprocess_runner_cannot_be_cross_wired_to_another_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node_document, job_document = _documents()
    queue_reads = iter((job_document, job_document))
    monkeypatch.setattr(
        slurm_inventory,
        "_verify_trusted_file",
        lambda _path, _digest: None,
    )
    runner = SubprocessReadOnlySlurmCommandRunner(policy=_policy(), timeout_seconds=2)

    async def run(command: str) -> bytes:
        document = node_document if command == "nodes" else next(queue_reads)
        return json.dumps(document).encode("utf-8")

    monkeypatch.setattr(runner, "run", run)

    with pytest.raises(ValueError, match="runner policy binding"):
        await capture_slurm_capacity_reports(
            runner,
            policy=replace(_policy(), scontrol_sha256="f" * 64),
            binding=_binding(),
            source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
        )


def test_live_slurm_23_11_gpu_surfaces_are_charged() -> None:
    node_document, job_document = _documents()
    jobs = job_document["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    job["tres_alloc_str"] = "cpu=4,mem=23000M,node=1,billing=4"
    job["tres_req_str"] = "cpu=4,mem=23000M,node=1,billing=4"
    job["tres_per_job"] = ""
    job["tres_per_node"] = "gres/gpu:1"
    job["tres_per_socket"] = ""
    job["tres_per_task"] = "cpu:4"
    job["gres_detail"] = ["gpu:1(IDX:0)"]
    job_document["jobs"] = [job]

    reports = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    assert reports.pool_observation.commitments[0].resources.gpu_count == 1


def test_hidden_busy_node_allocation_is_quarantined() -> None:
    node_document, job_document = _documents()
    nodes = node_document["nodes"]
    assert isinstance(nodes, list)
    busy = nodes[1]
    assert isinstance(busy, dict)
    busy.update(
        {
            "alloc_cpus": 16,
            "alloc_memory": 115_000,
            "gres_used": "gpu:1",
        }
    )
    job_document["jobs"] = []

    reports = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    assert len(reports.pool_observation.commitments) == 1
    hidden = reports.pool_observation.commitments[0]
    assert hidden.physical_identity == "slurm-node-trt-gb10-10-hidden-allocation"
    assert hidden.node_ids == ("trt-gb10-10",)
    assert hidden.resources == ResourceVectorV1(
        slots=10,
        cpu_millicores=16_000,
        memory_bytes=115_000 * 1024**2,
        gpu_count=1,
    )


def test_visible_job_is_reconciled_against_node_allocation_counters() -> None:
    node_document, job_document = _documents()
    nodes = node_document["nodes"]
    assert isinstance(nodes, list)
    busy = nodes[1]
    assert isinstance(busy, dict)
    busy["alloc_cpus"] = 8
    busy["alloc_memory"] = 46_000
    busy["gres_used"] = "gpu:1"
    jobs = job_document["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    job["tres_per_node"] = "gres/gpu:1"
    job["gres_detail"] = ["gpu:1(IDX:0)"]
    job_document["jobs"] = [job]

    reports = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    charges = {
        item.physical_identity: item.resources for item in reports.pool_observation.commitments
    }
    assert charges["slurm-job-42"].gpu_count == 1
    assert charges["slurm-node-trt-gb10-10-hidden-allocation"] == ResourceVectorV1(
        slots=2,
        cpu_millicores=4_000,
        memory_bytes=23_000 * 1024**2,
    )


def test_live_gb10_pending_request_uses_tres_memory_and_gpu() -> None:
    node_document, job_document = _documents()
    _clear_canonical_allocation(node_document)
    jobs = job_document["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    job["job_state"] = ["PENDING"]
    job["cpus"] = _quantity(16)
    job["memory_per_node"] = _quantity(0)
    job["node_count"] = _quantity(1)
    job["tres_req_str"] = "cpu=16,mem=115000M,node=1,billing=16"
    job["tres_per_node"] = "gres/gpu:1"
    job["job_resources"] = {"allocated_nodes": None}
    job_document["jobs"] = [job]

    reports = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    assert reports.pool_observation.commitments[0].resources == ResourceVectorV1(
        slots=10,
        cpu_millicores=16_000,
        memory_bytes=115_000 * 1024**2,
        gpu_count=1,
    )


def test_duplicate_gpu_request_representations_are_not_double_counted() -> None:
    node_document, job_document = _documents()
    _clear_canonical_allocation(node_document)
    jobs = job_document["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    job["job_state"] = ["PENDING"]
    job["job_resources"] = {"allocated_nodes": None}
    job["tres_per_node"] = "gres/gpu:1"
    job["tres_req_str"] = "cpu=4,mem=23000M,node=1,gres/gpu=1,billing=4"
    job_document["jobs"] = [job]

    reports = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    assert reports.pool_observation.commitments[0].resources.gpu_count == 1


def test_controller_evidence_binds_the_trusted_runtime_digests() -> None:
    node_document, job_document = _documents()
    first = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )
    changed_policy = replace(_policy(), scontrol_sha256="f" * 64)

    changed = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=changed_policy,
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    assert changed.controller_sha256 != first.controller_sha256


@pytest.mark.parametrize(
    "changes",
    (
        {"query_principal": "loom-capacity-slurm-reader-v2"},
        {"query_uid": os.geteuid() + 1},
        {"job_visibility_evidence_sha256": "b" * 64},
    ),
)
def test_controller_evidence_binds_full_visibility_query_identity(
    changes: dict[str, object],
) -> None:
    node_document, job_document = _documents()
    first = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    changed = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=replace(_policy(), **changes),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    assert changed.controller_sha256 != first.controller_sha256


def test_terminal_job_carries_controller_terminal_evidence() -> None:
    node_document, job_document = _documents()
    jobs = job_document["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    job["job_state"] = ["COMPLETED"]
    job_document["jobs"] = [job]

    reports = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    record = next(
        item for item in reports.executable_inventory.records if item.physical_identity == "slurm-job-42"
    )
    assert record.state == "terminal"
    assert record.terminal_evidence_sha256 == reports.controller_sha256


def test_trusted_runtime_rejects_an_unsafe_file(tmp_path: Path) -> None:
    candidate = tmp_path / "scontrol"
    candidate.write_bytes(b"read-only-test")
    candidate.chmod(0o666)

    with pytest.raises(RuntimeError, match="metadata is unsafe"):
        slurm_inventory._verify_trusted_file(
            str(candidate),
            hashlib.sha256(candidate.read_bytes()).hexdigest(),
        )


def test_node_resource_drift_is_rejected() -> None:
    node_document, job_document = _documents()
    nodes = node_document["nodes"]
    assert isinstance(nodes, list)
    canonical = nodes[0]
    assert isinstance(canonical, dict)
    canonical["effective_cpus"] = 19

    with pytest.raises(ValueError, match="resource envelope"):
        build_slurm_capacity_reports(
            node_document,
            job_document,
            policy=_policy(),
            binding=_binding(),
            source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
        )


def test_every_canonical_node_requires_the_policy_partition() -> None:
    node_document, job_document = _documents()
    nodes = node_document["nodes"]
    assert isinstance(nodes, list)
    canonical = nodes[0]
    assert isinstance(canonical, dict)
    canonical["partitions"] = ["debug"]

    with pytest.raises(ValueError, match="required partition"):
        build_slurm_capacity_reports(
            node_document,
            job_document,
            policy=_policy(),
            binding=_binding(),
            source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
        )


def test_pending_compact_array_quarantines_the_pool_capacity() -> None:
    node_document, job_document = _documents()
    jobs = job_document["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    job["job_state"] = ["PENDING"]
    job["job_resources"] = {"allocated_nodes": None}
    job["array_task_id"] = {"set": False, "infinite": False, "number": 0}
    job["array_task_string"] = "0-99%5"
    job["array_max_tasks"] = _quantity(5)
    job["tres_per_node"] = "gres/gpu:1"
    job_document["jobs"] = [job]

    reports = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    assert reports.pool_observation.commitments[0].resources == ResourceVectorV1(
        slots=20,
        cpu_millicores=40_000,
        memory_bytes=230_000 * 1024**2,
        gpu_count=2,
    )


def test_configuring_node_less_job_in_relevant_partition_is_charged() -> None:
    node_document, job_document = _documents()
    jobs = job_document["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    job["job_state"] = ["CONFIGURING"]
    job["job_resources"] = {"allocated_nodes": []}
    job["tres_per_node"] = "gres/gpu:1"
    job_document["jobs"] = [job]

    reports = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    assert "slurm-job-42" in {
        item.physical_identity for item in reports.pool_observation.commitments
    }


def test_node_less_job_with_malformed_partition_is_rejected() -> None:
    node_document, job_document = _documents()
    _clear_canonical_allocation(node_document)
    jobs = job_document["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    job["job_state"] = ["PENDING"]
    job["partition"] = None
    job["job_resources"] = {"allocated_nodes": None}
    job_document["jobs"] = [job]

    with pytest.raises(ValueError, match="partition is invalid"):
        build_slurm_capacity_reports(
            node_document,
            job_document,
            policy=_policy(),
            binding=_binding(),
            source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
        )


def test_queue_order_does_not_change_controller_evidence() -> None:
    node_document, job_document = _documents()
    jobs = job_document["jobs"]
    assert isinstance(jobs, list)
    first_job = jobs[0]
    assert isinstance(first_job, dict)
    second_job = json.loads(json.dumps(first_job))
    assert isinstance(second_job, dict)
    second_job["job_id"] = 44
    job_document["jobs"] = [first_job, second_job]
    first = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )
    jobs = job_document["jobs"]
    assert isinstance(jobs, list)
    jobs.reverse()

    reordered = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=_binding(),
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    assert reordered.controller_sha256 == first.controller_sha256


@pytest.mark.asyncio
async def test_same_second_queue_change_is_rejected() -> None:
    node_document, job_document = _documents()
    changed_jobs = json.loads(json.dumps(job_document))
    assert isinstance(changed_jobs, dict)
    jobs = changed_jobs["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    job["job_state"] = ["COMPLETING"]
    queue_reads = iter((job_document, changed_jobs))

    class Runner:
        async def run(self, command: str) -> bytes:
            document = node_document if command == "nodes" else next(queue_reads)
            return json.dumps(document).encode("utf-8")

    with pytest.raises(SlurmSnapshotRaceError):
        await capture_slurm_capacity_reports(
            Runner(),
            policy=_policy(),
            binding=_binding(),
            source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
            max_attempts=1,
        )


def test_builder_requires_a_stable_bracketing_queue_snapshot() -> None:
    node_document, job_before = _documents()
    job_after = json.loads(json.dumps(job_before))
    assert isinstance(job_after, dict)
    jobs = job_after["jobs"]
    assert isinstance(jobs, list)
    job = jobs[0]
    assert isinstance(job, dict)
    job["job_state"] = ["COMPLETING"]

    with pytest.raises(SlurmSnapshotRaceError):
        _build_slurm_capacity_reports(
            node_document,
            job_before,
            job_after,
            policy=_policy(),
            binding=_binding(),
            source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
        )


def test_inventory_carries_nonzero_journal_checkpoint() -> None:
    node_document, job_document = _documents()
    binding = _binding()
    checkpointed = SlurmReportBinding(
        pool_sequence=binding.pool_sequence,
        inventory_sequence=binding.inventory_sequence,
        execution=binding.execution,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        journal_sequence=2,
        journal_digest="2" * 64,
        journal_checkpoint_sequence=1,
        journal_checkpoint_digest="1" * 64,
    )

    reports = build_slurm_capacity_reports(
        node_document,
        job_document,
        policy=_policy(),
        binding=checkpointed,
        source_observed_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )

    assert reports.executable_inventory.journal_checkpoint_sequence == 1
    assert reports.executable_inventory.journal_checkpoint_digest == "1" * 64
