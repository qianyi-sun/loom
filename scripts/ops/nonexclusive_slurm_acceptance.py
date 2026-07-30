#!/usr/bin/env python3
"""Plan, collect, and verify non-exclusive Slurm containment evidence.

This utility is deliberately repo-only and non-mutating:

* ``plan`` prints the complete acceptance contract and stop rules.
* ``collect`` validates and canonically copies a pre-collected JSON snapshot.
* ``verify`` validates an evidence artifact without contacting any host.

There is no SSH, Slurm, Docker, subprocess, or mutation path in this module.
Live observation capture remains a separately authorized operator action.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

SCHEMA_VERSION = 1
MAX_ARTIFACT_BYTES = 2 * 1024 * 1024
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCHEMA = REPO_ROOT / "docs/evidence/nonexclusive-slurm-acceptance-v1.schema.json"


def _live_acceptance_contract() -> Any:
    try:
        from scripts.ops import developer_sandbox_live_acceptance

        return developer_sandbox_live_acceptance
    except ModuleNotFoundError:
        path = Path(__file__).with_name("developer_sandbox_live_acceptance.py")
        spec = importlib.util.spec_from_file_location(
            "_loom_developer_sandbox_live_acceptance_for_gate6",
            path,
        )
        if spec is None or spec.loader is None:
            raise RuntimeError("live acceptance verifier is unavailable") from None
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module


live_acceptance = _live_acceptance_contract()
REQUIRED_ROLES = frozenset({"worker", "trial", "verifier", "sidecar"})
REQUIRED_CONTROLLERS = frozenset({"cpu", "memory", "pids"})
REQUIRED_WORKLOADS = frozenset(
    {"loom", "non_loom_slurm", "kubernetes", "minio", "longhorn"},
)
REQUIRED_RESOURCES = frozenset(
    {"worker_identity", "object_store", "result_path"},
)
REQUIRED_CLEANUP_EVENTS = frozenset(
    {"cancellation", "ttl_expiry", "worker_crash", "submit_host_restart"},
)
GATE6_POOLS = ("oldlab", "gb10")
GATE6_POOL_CONCURRENCY = {"oldlab": 4, "gb10": 8}
GATE6_POOL_NODES = {
    "oldlab": tuple(f"trt-eai-oldlab-{index}" for index in range(1, 6)),
    "gb10": tuple(f"trt-gb10-{index}" for index in range(1, 16)),
}
GATE6_CLUSTERS = {"oldlab": "trt-oldlab", "gb10": "trt-gb10"}

_SECRET_KEY_RE = re.compile(
    r"(?:authorization|credential|password|private[_-]?key|access[_-]?key|"
    r"api[_-]?key|secret|token)",
    re.IGNORECASE,
)
_SECRET_VALUE_PATTERNS = (
    re.compile(r"\b[Bb]earer\s+[A-Za-z0-9._~+/=-]{8,}"),
    re.compile(r"\bsk-[A-Za-z0-9][A-Za-z0-9_-]{10,}"),
    re.compile(r"\bghp_[A-Za-z0-9_]{10,}"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{10,}"),
    re.compile(r"\bloom_(?:api|w)_[A-Za-z0-9._-]{8,}", re.IGNORECASE),
    re.compile(
        r"(?i)(X-Amz-Signature|AWSAccessKeyId|Signature|token|api_key|"
        r"access_key)=[^&\s]+",
    ),
    re.compile(r"://([^:/@\s]+):([^@\s]+)@"),
)


class AcceptanceError(ValueError):
    """Raised when evidence cannot be processed safely."""


def _json_load(path: Path) -> Any:
    try:
        if path.stat().st_size > MAX_ARTIFACT_BYTES:
            raise AcceptanceError("JSON artifact exceeds the size limit")
        with path.open(encoding="utf-8") as handle:
            return json.load(handle)
    except AcceptanceError:
        raise
    except (OSError, json.JSONDecodeError) as exc:
        raise AcceptanceError("cannot read JSON artifact") from exc


def _scan_for_secrets(value: Any, path: tuple[str, ...] = ()) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise AcceptanceError("JSON object keys must be strings")
            if _SECRET_KEY_RE.search(key):
                location = ".".join((*path, key))
                raise AcceptanceError(f"secret-like field is forbidden at {location}")
            _scan_for_secrets(item, (*path, key))
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, item in enumerate(value):
            _scan_for_secrets(item, (*path, str(index)))
        return
    if isinstance(value, str) and any(pattern.search(value) for pattern in _SECRET_VALUE_PATTERNS):
        location = ".".join(path) or "<root>"
        raise AcceptanceError(f"secret-like value is forbidden at {location}")


def _load_schema(path: Path) -> dict[str, Any]:
    schema = _json_load(path)
    if not isinstance(schema, dict):
        raise AcceptanceError("schema root must be an object")
    Draft202012Validator.check_schema(schema)
    return schema


def _schema_failures(evidence: Any, schema: Mapping[str, Any]) -> list[str]:
    failures: list[str] = []
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    for error in sorted(validator.iter_errors(evidence), key=lambda item: list(item.path)):
        location = ".".join(str(part) for part in error.absolute_path) or "<root>"
        failures.append(f"schema violation at {location} ({error.validator})")
    return failures


def _strict_descendant(child: str, parent: str) -> bool:
    if any(part in {".", ".."} for part in child.split("/")):
        return False
    if any(part in {".", ".."} for part in parent.split("/")):
        return False
    child_path = PurePosixPath(child)
    parent_path = PurePosixPath(parent)
    if not child_path.is_absolute() or not parent_path.is_absolute():
        return False
    if child_path == parent_path or parent_path == PurePosixPath("/"):
        return False
    return parent_path in child_path.parents


def _semantic_failures(evidence: Mapping[str, Any]) -> list[str]:
    failures: list[str] = []
    try:
        collected_at = datetime.fromisoformat(evidence["collected_at"].replace("Z", "+00:00"))
        if collected_at.tzinfo is None:
            raise ValueError
    except ValueError:
        failures.append("collected_at is not a timezone-qualified RFC3339 timestamp")
    candidate_sha = evidence["candidate_sha"]
    sandbox = evidence["sandbox"]
    node = evidence["node"]
    job = evidence["job"]
    allocation = job["allocation"]
    containers = evidence["containers"]
    cgroup = evidence["cgroup"]
    aggregate = evidence["aggregate_caps"]
    devices = evidence["devices"]
    headroom = evidence["headroom"]
    isolation = evidence["negative_isolation"]
    soak = evidence["soak"]
    cleanup = evidence["cleanup"]

    if job["candidate_sha"] != candidate_sha:
        failures.append("job candidate SHA does not match evidence candidate SHA")
    if job["sandbox"] != sandbox:
        failures.append("job sandbox does not match evidence sandbox")
    if job["node"] != node["slurm_node_name"]:
        failures.append("job node does not match evidence Slurm node")
    tres = job["allocation"]["tres"]
    if "cpu=" not in tres or "mem=" not in tres:
        failures.append("Slurm TRES is missing CPU or memory allocation")
    allocated_gpu_count = len(allocation["gpu_ids"])
    tres_gpu_count = live_acceptance._positive_gpu_tres_count(tres)
    if allocated_gpu_count and tres_gpu_count != allocated_gpu_count:
        failures.append("Slurm TRES GPU count does not match allocated GPU identities")
    if not allocated_gpu_count and tres_gpu_count is not None:
        failures.append("Slurm TRES declares GPU allocation without allocated GPU identities")

    roles = [container["role"] for container in containers]
    if set(roles) != REQUIRED_ROLES or len(roles) != len(REQUIRED_ROLES):
        failures.append("container roles must be exactly worker, trial, verifier, and sidecar")
    for identity_field in ("container_id", "name", "pid", "cgroup_path"):
        identities = [container[identity_field] for container in containers]
        if len(set(identities)) != len(identities):
            failures.append(f"container {identity_field} values must be unique")

    layout_version = cgroup.get("layout_version")
    if layout_version not in {
        "legacy-cgroupfs-v0",
        "cgroupfs-job-v1",
        "systemd-mirror-v1",
    }:
        failures.append("cgroup layout_version is not an accepted explicit discriminator")
    if layout_version == "legacy-cgroupfs-v0":
        if set(cgroup) != {
            "layout_version",
            "job_path",
            "controllers",
            "delegated",
            "cpu_cores_max",
            "memory_bytes_max",
            "pids_max",
        }:
            failures.append("legacy cgroup evidence is not the exact grandfathered shape")
        if set(job) != {
            "job_id",
            "candidate_sha",
            "sandbox",
            "node",
            "compose_project",
            "allocation",
        }:
            failures.append("legacy job evidence is not the exact grandfathered shape")
    versioned_cgroup = layout_version in {"cgroupfs-job-v1", "systemd-mirror-v1"}
    if versioned_cgroup:
        try:
            live_acceptance._validate_cgroup_evidence(
                cgroup,
                [
                    {
                        **container,
                        "observed_cgroup_path": container["cgroup_path"],
                    }
                    for container in containers
                ],
                job_id=job["job_id"],
                job_start_time=job.get("job_start_time"),
                node=job["node"],
                pool=job.get("pool"),
                account=job.get("account"),
                sandbox=sandbox,
                environment={
                    "env_id": job.get("env_id"),
                    "resource_generation": job.get("resource_generation"),
                    "candidate_id": job.get("candidate_id"),
                },
                candidate_sha=candidate_sha,
                candidate_tree=job.get("candidate_tree"),
                allocation={
                    **allocation,
                    "gpu_count": len(allocation["gpu_ids"]),
                },
            )
        except live_acceptance.AcceptanceError as exc:
            failures.append(f"versioned cgroup evidence is invalid: {exc}")
    job_path = cgroup["job_path"]
    for container in containers:
        role = container["role"]
        labels = container["labels"]
        expected_labels = {
            "loom.sandbox": sandbox,
            "loom.candidate_sha": candidate_sha,
            "loom.slurm_job_id": job["job_id"],
            "loom.compose_project": job["compose_project"],
        }
        if labels != expected_labels:
            failures.append(f"{role} container labels do not match job identity")
        if not versioned_cgroup:
            if container["cgroup_parent"] != job_path:
                failures.append(f"{role} container cgroup parent is not the Slurm job cgroup")
            if not _strict_descendant(container["cgroup_path"], job_path):
                failures.append(f"{role} container is not inside the Slurm job cgroup")

    if not versioned_cgroup:
        controllers = set(cgroup["controllers"])
        if controllers != REQUIRED_CONTROLLERS:
            failures.append("job cgroup must enforce cpu, memory, and pids")
        if not cgroup["delegated"]:
            failures.append("job cgroup delegation was not proven")

    cap_fields = ("cpu_cores", "memory_bytes", "pids")
    for field in cap_fields:
        observed_sum = sum(container["limits"][field] for container in containers)
        if aggregate[field] != observed_sum:
            failures.append(f"aggregate {field} does not equal container-limit sum")
        if aggregate[field] > allocation[field]:
            failures.append(f"aggregate {field} exceeds Slurm allocation")
        if aggregate[field] > cgroup[f"{field}_max"]:
            failures.append(f"aggregate {field} exceeds job cgroup maximum")
        if cgroup[f"{field}_max"] > allocation[field]:
            failures.append(f"job cgroup {field} maximum exceeds Slurm allocation")

    allocated_ids = set(allocation["gpu_ids"])
    if set(devices["allocated_ids"]) != allocated_ids:
        failures.append("device allocation does not match Slurm allocation")
    allocated_container_ids = [
        set(container["device_ids"]) for container in containers if container["device_ids"]
    ]
    if allocated_ids:
        if len(allocated_container_ids) != 1 or allocated_container_ids[0] != allocated_ids:
            failures.append(
                "exactly one container must receive the complete Slurm GPU allocation",
            )
    elif allocated_container_ids:
        failures.append("zero-GPU allocation exposed a container GPU device")
    for container in containers:
        if not set(container["device_ids"]).issubset(allocated_ids):
            failures.append(
                f"{container['role']} container has a device outside the Slurm allocation",
            )
    if not devices["all_allocated_usable"]:
        failures.append("allocated device usability was not proven")
    if not devices["unallocated_denied"]:
        failures.append("unallocated device denial was not proven")

    if headroom["duration_seconds"] < headroom["required_duration_seconds"]:
        failures.append("headroom observation duration is below policy")
    if headroom["sample_count"] < headroom["required_sample_count"]:
        failures.append("headroom sample count is below policy")
    if headroom["min_free_cpu_cores"] < headroom["required_free_cpu_cores"]:
        failures.append("CPU headroom is below policy")
    if headroom["min_free_memory_bytes"] < headroom["required_free_memory_bytes"]:
        failures.append("memory headroom is below policy")
    if headroom["max_pid_usage_ratio"] > headroom["max_allowed_pid_usage_ratio"]:
        failures.append("PID headroom is below policy")
    if headroom["observed_peak_concurrency"] > headroom["reviewed_max_concurrency"]:
        failures.append("observed concurrency exceeds the reviewed envelope")
    if not headroom["within_reviewed_envelope"]:
        failures.append("headroom was outside the reviewed envelope")
    for health_field in (
        "kube_api_healthy",
        "minio_quorum_healthy",
        "longhorn_healthy",
    ):
        if not headroom[health_field]:
            failures.append(f"headroom checkpoint failed: {health_field}")

    sandboxes = isolation["sandboxes"]
    if len(sandboxes) < 3 or len(set(sandboxes)) != len(sandboxes):
        failures.append("negative isolation requires at least three unique sandboxes")
    if sandbox not in sandboxes:
        failures.append("evidence sandbox is absent from the isolation matrix")
    expected_checks = {
        (source, target, resource)
        for source in sandboxes
        for target in sandboxes
        if source != target
        for resource in REQUIRED_RESOURCES
    }
    actual_checks = {
        (check["source"], check["target"], check["resource"]) for check in isolation["checks"]
    }
    if actual_checks != expected_checks or len(isolation["checks"]) != len(expected_checks):
        failures.append("negative isolation matrix is incomplete or duplicated")
    if any(not check["denied"] for check in isolation["checks"]):
        failures.append("negative isolation contains a cross-sandbox access success")

    if soak["duration_seconds"] < soak["required_duration_seconds"]:
        failures.append("soak duration is below policy")
    if soak["sample_count"] < soak["required_sample_count"]:
        failures.append("soak sample count is below policy")
    if set(soak["workloads"]) != REQUIRED_WORKLOADS:
        failures.append("soak workload set is incomplete")
    if soak["trial_success_ratio"] < soak["minimum_trial_success_ratio"]:
        failures.append("soak trial success ratio is below policy")
    if soak["resource_envelope_breaches"] != 0:
        failures.append("soak recorded a resource-envelope breach")
    for health_field in (
        "kube_api_healthy",
        "minio_quorum_healthy",
        "longhorn_healthy",
        "non_loom_slurm_healthy",
    ):
        if not soak[health_field]:
            failures.append(f"soak checkpoint failed: {health_field}")

    checkpoints = cleanup["checkpoints"]
    actual_events = {checkpoint["event"] for checkpoint in checkpoints}
    if actual_events != REQUIRED_CLEANUP_EVENTS or len(checkpoints) != len(
        REQUIRED_CLEANUP_EVENTS,
    ):
        failures.append("cleanup checkpoints are incomplete or duplicated")
    for checkpoint in checkpoints:
        event = checkpoint["event"]
        if checkpoint["observed_within_seconds"] > cleanup["max_cleanup_seconds"]:
            failures.append(f"{event} cleanup exceeded the policy deadline")
        if checkpoint["live_containers"] != 0:
            failures.append(f"{event} cleanup left live containers")
        if checkpoint["live_jobs"] != 0:
            failures.append(f"{event} cleanup left live jobs")
        if not checkpoint["durable_trial_state"]:
            failures.append(f"{event} cleanup lost durable trial state")
        if not checkpoint["retryable_interrupted_trials"]:
            failures.append(f"{event} cleanup left interrupted trials non-retryable")

    return failures


def verify_evidence(evidence: Any, schema: Mapping[str, Any]) -> list[str]:
    """Return controlled, secret-safe failure reasons for an evidence artifact."""

    _scan_for_secrets(evidence)
    failures = _schema_failures(evidence, schema)
    if failures or not isinstance(evidence, Mapping):
        return failures
    return _semantic_failures(evidence)


def _canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        + "\n"
    ).encode()


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _gate6_registry_environments(
    live: Mapping[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    try:
        snapshot = live_acceptance._validated_registry_snapshot(
            live.get("registry_snapshot"),
        )
    except live_acceptance.AcceptanceError as exc:
        raise AcceptanceError("gate-6 registry snapshot is invalid") from exc
    environments = {
        str(environment["runtime_id"]): dict(environment)
        for environment in snapshot["environments"]
    }
    if len(environments) < 2 or len(environments) != len(snapshot["environments"]):
        raise AcceptanceError("gate-6 requires at least two distinct registry environments")
    return environments, snapshot


def _gate6_matrix_failures(
    matrix: Any,
    *,
    sandbox: str,
    pool: str,
    candidate: Mapping[str, str],
    environment: Mapping[str, Any],
    registry_snapshot: Mapping[str, Any],
) -> list[str]:
    """Validate the immutable allocation-probe result used by gate 6.

    The Slurm policy collector owns the detailed receipt schema.  This bridge
    independently rechecks the candidate, pool, concurrency, and complete node
    set so a valid receipt from another candidate or a partial fleet cannot be
    replayed into acceptance.
    """

    failures: list[str] = []
    if not isinstance(matrix, Mapping):
        return [f"{sandbox}/{pool} allocation matrix is not an object"]
    nodes = matrix.get("nodes")
    allowed_nodes = matrix.get("allowed_nodes")
    runtime = matrix.get("runtime_attestation")
    binding = matrix.get("candidate_binding")
    repository = binding.get("repository") if isinstance(binding, Mapping) else None
    expected_nodes = GATE6_POOL_NODES[pool]
    observed_allowed = (
        tuple(str(node).lower() for node in allowed_nodes)
        if isinstance(allowed_nodes, list)
        else ()
    )
    observed_rows = (
        tuple(str(row.get("node", "")).lower() for row in nodes if isinstance(row, Mapping))
        if isinstance(nodes, list)
        else ()
    )
    if (
        matrix.get("schema_version") != 1
        or matrix.get("artifact_type") != "developer-sandbox-slurm-allocation-matrix"
        or matrix.get("candidate_sha") != candidate["sha"]
        or matrix.get("candidate_tree") != candidate["tree"]
        or matrix.get("sandbox") != sandbox
        or matrix.get("cluster") != GATE6_CLUSTERS[pool]
        or matrix.get("expected_pool") != pool
        or matrix.get("expected_concurrency") != GATE6_POOL_CONCURRENCY[pool]
        or matrix.get("account") != environment["slurm_account"]
        or matrix.get("qos") != environment["slurm_qos"]
        or matrix.get("closed_world_verified") is not True
        or observed_allowed != expected_nodes
        or observed_rows != expected_nodes
        or len(set(observed_rows)) != len(expected_nodes)
        or not isinstance(repository, Mapping)
        or repository.get("candidate_sha") != candidate["sha"]
        or repository.get("candidate_tree") != candidate["tree"]
        or not isinstance(runtime, Mapping)
        or runtime.get("sandbox") != sandbox
        or runtime.get("candidate_sha") != candidate["sha"]
        or runtime.get("candidate_tree") != candidate["tree"]
        or runtime.get("domain") != pool
        or runtime.get("env_id") != environment["env_id"]
        or runtime.get("resource_generation")
        != next(
            item["resource_generation"]
            for item in registry_snapshot["source_registry"]["environments"]
            if item["env_id"] == environment["env_id"]
        )
        or runtime.get("registry_generation") != registry_snapshot["generation"]
        or runtime.get("registry_payload_sha256")
        != registry_snapshot["source_registry"]["payload_sha256"]
    ):
        failures.append(f"{sandbox}/{pool} allocation matrix binding is invalid")
        return failures
    assert isinstance(nodes, list)
    for row in nodes:
        if not isinstance(row, Mapping):
            failures.append(f"{sandbox}/{pool} allocation matrix row is invalid")
            continue
        compute = row.get("compute_check")
        if (
            row.get("sandbox") != sandbox
            or row.get("state") != "COMPLETED"
            or row.get("account") != environment["slurm_account"]
            or row.get("qos") != environment["slurm_qos"]
            or row.get("sbatch_verified") is not True
            or row.get("srun_verified") is not True
            or row.get("nonexclusive") is not True
            or str(row.get("explicit_nodelist", "")).lower() != str(row.get("node", "")).lower()
            or not isinstance(compute, Mapping)
            or compute.get("sandbox") != sandbox
            or compute.get("account") != environment["slurm_account"]
            or compute.get("candidate_sha") != candidate["sha"]
            or compute.get("candidate_tree") != candidate["tree"]
            or compute.get("pool") != pool
            or compute.get("concurrency") != GATE6_POOL_CONCURRENCY[pool]
            or compute.get("cgroup_guard_verified") is not True
            or compute.get("compose_verified") is not True
            or (pool == "gb10" and row.get("gpu_verified") is not True)
        ):
            failures.append(f"{sandbox}/{pool} allocation matrix row proof is invalid")
    return failures


def _gate6_pair_artifact(
    live: Mapping[str, Any],
    platform: Mapping[str, Any],
    *,
    sandbox: str,
    pool: str,
    sandboxes: Sequence[str],
) -> dict[str, Any]:
    """Losslessly materialize one v1-shaped pair from native authority fields."""

    gate = platform["gate6_observations"]
    soak = gate["soak"]
    jobs = [
        row
        for row in platform["mixed_jobs"]
        if row["sandbox"] == sandbox
        and (
            ("oldlab" if str(row["node"]).lower().startswith("trt-eai-oldlab-") else "gb10") == pool
        )
    ]
    devices = [
        row for row in gate["device_isolation"] if row["sandbox"] == sandbox and row["pool"] == pool
    ]
    headroom = [
        row for row in soak["pair_headroom"] if row["sandbox"] == sandbox and row["pool"] == pool
    ]
    if len(jobs) != 1 or len(devices) != 1 or len(headroom) != 1:
        raise AcceptanceError(f"{sandbox}/{pool} native gate-6 coverage is not exact")
    job = jobs[0]
    device = devices[0]
    observed_headroom = headroom[0]
    candidate = live["candidates"][sandbox]
    if (
        job["candidate_sha"] != candidate["sha"]
        or device["job_id"] != job["job_id"]
        or device["node"] != job["node"]
    ):
        raise AcceptanceError(f"{sandbox}/{pool} native gate-6 identity is inconsistent")
    containers = [
        {
            "role": item["role"],
            "container_id": item["container_id"],
            "name": item["name"],
            "pid": item["pid"],
            "labels": item["identity_labels"],
            "cgroup_parent": item["cgroup_parent"],
            "cgroup_path": item["observed_cgroup_path"],
            "limits": {key: item["limits"][key] for key in ("cpu_cores", "memory_bytes", "pids")},
            "device_ids": item["limits"]["gpu_ids"],
        }
        for item in job["containers"]
    ]
    cleanup = [
        {
            "event": item["event"],
            "observed_within_seconds": item["observed_within_seconds"],
            "live_containers": item["live_containers"],
            "live_jobs": item["live_jobs"],
            "durable_trial_state": item["durable_trial_state"],
            "retryable_interrupted_trials": item["retryable_interrupted_trials"],
        }
        for item in gate["cleanup"]
    ]
    mixed_checkpoint = next(
        item for item in platform["checkpoints"] if item["checkpoint"] == "mixed_non_loom"
    )
    allocation = job["allocation"]
    cgroup = job["cgroup"]
    job_evidence = {
        "job_id": job["job_id"],
        "candidate_sha": candidate["sha"],
        "sandbox": sandbox,
        "node": job["node"],
        "compose_project": job["compose_project"],
        "allocation": {
            "cpu_cores": allocation["cpu_cores"],
            "memory_bytes": allocation["memory_bytes"],
            "pids": allocation["pids"],
            "gpu_ids": device["allocated_ids"],
            "tres": allocation["tres"],
        },
    }
    if cgroup.get("layout_version") in {"cgroupfs-job-v1", "systemd-mirror-v1"}:
        job_evidence.update(
            {
                "job_start_time": job["job_start_time"],
                "pool": pool,
                "account": job["account"],
                "env_id": job["env_id"],
                "resource_generation": job["resource_generation"],
                "candidate_id": job["candidate_id"],
                "candidate_tree": candidate["tree"],
            },
        )
    cgroup_evidence = (
        dict(cgroup)
        if "layout_version" in cgroup
        else {
            "layout_version": "legacy-cgroupfs-v0",
            "job_path": cgroup["job_path"],
            "controllers": cgroup["controllers"],
            "delegated": cgroup["delegated"],
            "cpu_cores_max": cgroup["cpu_cores_max"],
            "memory_bytes_max": cgroup["memory_bytes_max"],
            "pids_max": cgroup["pids_max"],
        }
    )
    return {
        "schema_version": 1,
        "collected_at": mixed_checkpoint["observed_at"],
        "candidate_sha": candidate["sha"],
        "sandbox": sandbox,
        "node": {
            "hostname": device["host"],
            "slurm_node_name": job["node"],
        },
        "job": job_evidence,
        "containers": containers,
        "cgroup": cgroup_evidence,
        "aggregate_caps": {
            key: job["aggregate_limits"][key] for key in ("cpu_cores", "memory_bytes", "pids")
        },
        "devices": {
            "allocated_ids": device["allocated_ids"],
            "all_allocated_usable": device["all_allocated_usable"],
            "unallocated_denied": device["unallocated_denied"],
        },
        "headroom": {
            "duration_seconds": soak["duration_seconds"],
            "required_duration_seconds": 1800,
            "sample_count": soak["sample_count"],
            "required_sample_count": 30,
            "min_free_cpu_cores": observed_headroom["min_free_cpu_cores"],
            "required_free_cpu_cores": 4,
            "min_free_memory_bytes": observed_headroom["min_free_memory_bytes"],
            "required_free_memory_bytes": 16_000_000_000,
            "max_pid_usage_ratio": observed_headroom["max_pid_usage_ratio"],
            "max_allowed_pid_usage_ratio": 0.7,
            "observed_peak_concurrency": observed_headroom["observed_peak_concurrency"],
            "reviewed_max_concurrency": GATE6_POOL_CONCURRENCY[pool],
            "kube_api_healthy": soak["kube_api_healthy"],
            "minio_quorum_healthy": soak["minio_quorum_healthy"],
            "longhorn_healthy": soak["longhorn_healthy"],
            "within_reviewed_envelope": observed_headroom["within_reviewed_envelope"],
        },
        "negative_isolation": {
            "sandboxes": list(sandboxes),
            "checks": [
                {
                    "source": item["source"],
                    "target": item["target"],
                    "resource": item["resource"],
                    "denied": item["denied"],
                }
                for item in live["cross_sandbox_negative"]
            ],
        },
        "soak": {
            key: soak[key]
            for key in (
                "duration_seconds",
                "required_duration_seconds",
                "sample_count",
                "required_sample_count",
                "workloads",
                "trial_success_ratio",
                "minimum_trial_success_ratio",
                "resource_envelope_breaches",
                "kube_api_healthy",
                "minio_quorum_healthy",
                "longhorn_healthy",
                "non_loom_slurm_healthy",
            )
        },
        "cleanup": {"max_cleanup_seconds": 300, "checkpoints": cleanup},
    }


def _build_gate6_bundle(
    live: Any,
    platform: Any,
    matrices: Mapping[tuple[str, str], Any],
    schema: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[tuple[str, str], dict[str, Any]]]:
    """Verify finalized native sources and return their canonical gate-6 bundle.

    GB10 pairs are passed through the unchanged v1 schema and semantic
    verifier.  OLDLAB has no GPU by design, while v1 requires at least one GPU
    ID, so it follows the same strict semantic checks with an explicit
    zero-device contract instead of inventing a device.
    """

    _scan_for_secrets(live)
    _scan_for_secrets(platform)
    if not isinstance(live, Mapping) or not isinstance(platform, Mapping):
        raise AcceptanceError("gate-6 sources must be objects")
    environments, registry_snapshot = _gate6_registry_environments(live)
    sandboxes = tuple(environments)
    if (
        live.get("schema_version") != 2
        or len(live.get("state_machine", [])) != 33
        or live.get("topology", {}).get("excluded_nodes") != []
        or tuple(live.get("topology", {}).get("eligible_nodes", ()))
        != (*GATE6_POOL_NODES["oldlab"], *GATE6_POOL_NODES["gb10"])
        or "trt-gb10-7" not in live.get("topology", {}).get("eligible_nodes", ())
        or platform.get("session_id") != live.get("session", {}).get("id")
        or platform.get("candidates")
        != {
            sandbox: {
                "sha": live["candidates"][sandbox]["sha"],
                "tree": live["candidates"][sandbox]["tree"],
            }
            for sandbox in sandboxes
        }
        or set(live.get("candidates", {})) != set(sandboxes)
    ):
        raise AcceptanceError("finalized live/platform gate-6 binding is invalid")
    expected_pairs = {(sandbox, pool) for sandbox in sandboxes for pool in GATE6_POOLS}
    if set(matrices) != expected_pairs:
        raise AcceptanceError("gate-6 allocation matrix set is incomplete")
    failures = [
        failure
        for sandbox, pool in sorted(expected_pairs)
        for failure in _gate6_matrix_failures(
            matrices[(sandbox, pool)],
            sandbox=sandbox,
            pool=pool,
            candidate=live["candidates"][sandbox],
            environment=environments[sandbox],
            registry_snapshot=registry_snapshot,
        )
    ]
    if failures:
        raise AcceptanceError(failures[0])

    pair_artifacts: dict[tuple[str, str], dict[str, Any]] = {}
    for sandbox, pool in sorted(expected_pairs):
        artifact = _gate6_pair_artifact(
            live,
            platform,
            sandbox=sandbox,
            pool=pool,
            sandboxes=sandboxes,
        )
        pair_artifacts[(sandbox, pool)] = artifact
        if pool == "gb10":
            pair_failures = verify_evidence(artifact, schema)
            if pair_failures:
                raise AcceptanceError(f"{sandbox}/gb10 v1 verification failed")
        else:
            # v1's non-empty GPU array is intentionally inapplicable to a
            # CPU-only pool.  Remove only that schema mismatch, then preserve
            # every semantic check and require native zero-device proof.
            device = artifact["devices"]
            if (
                artifact["job"]["allocation"]["gpu_ids"] != []
                or any(container["device_ids"] for container in artifact["containers"])
                or device
                != {
                    "allocated_ids": [],
                    "all_allocated_usable": True,
                    "unallocated_denied": True,
                }
            ):
                raise AcceptanceError(f"{sandbox}/oldlab zero-device proof failed")
            semantic_failures = _semantic_failures(artifact)
            # _semantic_failures itself supports an empty allocation and still
            # enforces roles, cgroups, headroom, isolation, soak, and cleanup.
            if semantic_failures:
                raise AcceptanceError(f"{sandbox}/oldlab semantic verification failed")

    matrix_refs = [
        {
            "sandbox": sandbox,
            "pool": pool,
            "candidate_sha": live["candidates"][sandbox]["sha"],
            "candidate_tree": live["candidates"][sandbox]["tree"],
            "matrix_sha256": _digest(matrices[(sandbox, pool)]),
            "node_count": len(GATE6_POOL_NODES[pool]),
            "closed_world_verified": True,
        }
        for sandbox, pool in sorted(expected_pairs)
    ]
    artifact_refs = [
        {
            "sandbox": sandbox,
            "pool": pool,
            "verification": "nonexclusive-v1" if pool == "gb10" else "oldlab-zero-device",
            "artifact_sha256": _digest(pair_artifacts[(sandbox, pool)]),
        }
        for sandbox, pool in sorted(expected_pairs)
    ]
    unsigned = {
        "schema_version": 1,
        "kind": "loom.developer-sandbox.gate6-acceptance",
        "session_id": live["session"]["id"],
        "candidates": {
            sandbox: {
                "sha": live["candidates"][sandbox]["sha"],
                "tree": live["candidates"][sandbox]["tree"],
            }
            for sandbox in sandboxes
        },
        "registry_generation": registry_snapshot["generation"],
        "registry_payload_sha256": registry_snapshot["source_registry"]["payload_sha256"],
        "registry_projection_sha256": registry_snapshot["payload_sha256"],
        "live_evidence_sha256": _digest(live),
        "platform_health_sha256": platform["payload_sha256"],
        "state_machine_phase_count": 33,
        "excluded_nodes": [],
        "allocation_matrices": matrix_refs,
        "pair_artifacts": artifact_refs,
        "status": "pass",
    }
    return {**unsigned, "payload_sha256": _digest(unsigned)}, pair_artifacts


def build_gate6_bundle(
    live: Any,
    platform: Any,
    matrices: Mapping[tuple[str, str], Any],
    schema: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[tuple[str, str], dict[str, Any]]]:
    """Fail closed around the strict gate-6 source verifier."""

    try:
        return _build_gate6_bundle(live, platform, matrices, schema)
    except AcceptanceError:
        raise
    except (KeyError, TypeError, ValueError, StopIteration) as exc:
        raise AcceptanceError("gate-6 native evidence is incomplete") from exc


def acceptance_plan() -> dict[str, Any]:
    """Return the complete, non-mutating acceptance plan."""

    return {
        "schema_version": SCHEMA_VERSION,
        "mode": "repo_only_read_only",
        "commands": {
            "plan": "Print this contract; no external calls.",
            "collect": "Validate and canonically copy a pre-collected JSON snapshot.",
            "verify": "Offline schema and semantic verification.",
        },
        "required_evidence": [
            "schema",
            "candidate_sha",
            "sandbox",
            "node",
            "slurm_job",
            "container_identity_and_cgroup",
            "aggregate_cpu_memory_pid_caps",
            "device_allocation_and_denial",
            "node_headroom",
            "negative_cross_sandbox_isolation",
            "mixed_workload_soak",
            "cleanup_checkpoints",
        ],
        "required_container_roles": sorted(REQUIRED_ROLES),
        "required_cleanup_events": sorted(REQUIRED_CLEANUP_EVENTS),
        "stop_rules": [
            "Stop before collection unless live observation is separately authorized.",
            "Stop if the candidate SHA, sandbox, node, job, or compose identity is ambiguous.",
            "Stop if any container is outside the Slurm job cgroup.",
            "Stop if aggregate CPU, memory, PID, or device use exceeds the allocation.",
            "Stop if headroom, cross-sandbox denial, soak, or cleanup evidence is incomplete.",
            "Stop if any secret-like field or value is present.",
            "Never activate non-exclusive workers from this tool.",
        ],
        "mutations_supported": False,
    }


def _emit(payload: Mapping[str, Any], output: Path | None = None) -> None:
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if output is None:
        sys.stdout.write(rendered)
        return
    try:
        with output.open("x", encoding="utf-8") as handle:
            handle.write(rendered)
    except OSError as exc:
        raise AcceptanceError("cannot create output artifact") from exc


def _verification_report(evidence: Any, schema: Mapping[str, Any]) -> dict[str, Any]:
    failures = verify_evidence(evidence, schema)
    if failures:
        return {"status": "fail", "failures": failures}
    return {
        "status": "pass",
        "schema_version": SCHEMA_VERSION,
        "candidate_sha": evidence["candidate_sha"],
        "sandbox": evidence["sandbox"],
        "node": evidence["node"]["slurm_node_name"],
        "job_id": evidence["job"]["job_id"],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("plan")

    collect = subparsers.add_parser("collect")
    collect.add_argument("--input", type=Path, required=True)
    collect.add_argument("--output", type=Path, required=True)

    verify = subparsers.add_parser("verify")
    verify.add_argument("--evidence", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "plan":
            _emit(acceptance_plan())
            return 0

        schema = _load_schema(DEFAULT_SCHEMA)
        source = args.input if args.command == "collect" else args.evidence
        evidence = _json_load(source)
        report = _verification_report(evidence, schema)
        if report["status"] != "pass":
            _emit(report)
            return 1
        if args.command == "collect":
            _emit(evidence, args.output)
        _emit(report)
        return 0
    except AcceptanceError as exc:
        _emit({"status": "fail", "failures": [str(exc)]})
        return 1
    except (OSError, ValueError):
        _emit({"status": "fail", "failures": ["acceptance processing failed"]})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
