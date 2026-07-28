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
    if allocation["gpu_ids"] and not ("gres/gpu=" in tres or "gres/gpu:" in tres):
        failures.append("Slurm TRES is missing GPU allocation")

    roles = [container["role"] for container in containers]
    if set(roles) != REQUIRED_ROLES or len(roles) != len(REQUIRED_ROLES):
        failures.append("container roles must be exactly worker, trial, verifier, and sidecar")
    for identity_field in ("container_id", "name", "pid", "cgroup_path"):
        identities = [container[identity_field] for container in containers]
        if len(set(identities)) != len(identities):
            failures.append(f"container {identity_field} values must be unique")

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
        if container["cgroup_parent"] != job_path:
            failures.append(f"{role} container cgroup parent is not the Slurm job cgroup")
        if not _strict_descendant(container["cgroup_path"], job_path):
            failures.append(f"{role} container is not inside the Slurm job cgroup")

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
