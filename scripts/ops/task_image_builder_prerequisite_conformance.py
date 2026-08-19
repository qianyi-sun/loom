#!/usr/bin/env python3
"""Validate Phase 1 task-image builder prerequisite evidence without mutation."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tomllib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

MAX_INPUT_BYTES = 2 * 1024 * 1024
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_POLICY = REPO_ROOT / "deploy/task-image-builder/prerequisites-v1.toml"
DEFAULT_SCHEMA = (
    REPO_ROOT / "docs/evidence/task-image-builder-prerequisite-conformance-v1.schema.json"
)
EVIDENCE_SCHEMA = "loom.task-image-builder-prerequisite-conformance/v1"

_SECRET_KEY_RE = re.compile(
    r"(?:^|_)(?:api_?key|authorization|credential|password|private_?key|secret|token)$",
    re.IGNORECASE,
)
_SECRET_VALUE_RE = (
    re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]{8,}", re.IGNORECASE),
    re.compile(r"\b(?:sk|ghp)_[A-Za-z0-9_-]{10,}"),
    re.compile(r"://[^:/@\s]+:[^@\s]+@"),
    re.compile(r"(?i)(?:token|api_key|secret|signature)=[^&\s]+"),
)


class ConformanceError(ValueError):
    """Raised when policy or evidence cannot be processed safely."""


@dataclass(frozen=True)
class PrerequisitePolicy:
    raw: dict[str, Any]
    runtime: dict[str, Any]
    digest: str


def _read_regular(path: Path) -> bytes:
    try:
        if path.is_symlink() or not path.is_file():
            raise ConformanceError("input must be a regular non-symlink file")
        size = path.stat().st_size
        if size > MAX_INPUT_BYTES:
            raise ConformanceError("input exceeds the size limit")
        return path.read_bytes()
    except ConformanceError:
        raise
    except OSError as exc:
        raise ConformanceError("input is unavailable") from exc


def _json_load(path: Path) -> Any:
    try:
        return json.loads(_read_regular(path))
    except json.JSONDecodeError as exc:
        raise ConformanceError("input is not valid JSON") from exc


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _scan_for_secrets(value: Any, path: tuple[str, ...] = ()) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ConformanceError("JSON object keys must be strings")
            location = ".".join((*path, key))
            if _SECRET_KEY_RE.search(key):
                raise ConformanceError(f"secret-like field is forbidden at {location}")
            _scan_for_secrets(item, (*path, key))
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, item in enumerate(value):
            _scan_for_secrets(item, (*path, str(index)))
        return
    if isinstance(value, str) and any(pattern.search(value) for pattern in _SECRET_VALUE_RE):
        location = ".".join(path) or "<root>"
        raise ConformanceError(f"secret-like value is forbidden at {location}")


def load_policy(path: Path) -> PrerequisitePolicy:
    try:
        raw_value = tomllib.loads(_read_regular(path).decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ConformanceError("prerequisite policy is not valid TOML") from exc
    if raw_value.get("schema") != "loom.task-image-builder-prerequisites/v1":
        raise ConformanceError("prerequisite policy schema is invalid")
    if raw_value.get("production_certification_allowed") is not False:
        raise ConformanceError("Phase 1 policy cannot allow production certification")
    certified_nodes = raw_value.get("certified_nodes")
    if certified_nodes != []:
        raise ConformanceError("Phase 1 policy must certify zero nodes")
    runtime_section = raw_value.get("runtime")
    if not isinstance(runtime_section, dict):
        raise ConformanceError("prerequisite runtime policy is invalid")
    manifest_name = runtime_section.get("manifest")
    if not isinstance(manifest_name, str) or Path(manifest_name).name != manifest_name:
        raise ConformanceError("runtime manifest path is invalid")
    runtime_path = path.parent / manifest_name
    runtime_value = _json_load(runtime_path)
    if not isinstance(runtime_value, dict) or runtime_value.get("schema") != (
        "loom.task-image-builder-rootless-runtime/v1"
    ):
        raise ConformanceError("rootless runtime manifest is invalid")
    digest = hashlib.sha256(_canonical_bytes(raw_value)).hexdigest()
    return PrerequisitePolicy(raw=raw_value, runtime=runtime_value, digest=digest)


def _schema_failures(evidence: Any) -> list[str]:
    schema = _json_load(DEFAULT_SCHEMA)
    if not isinstance(schema, dict):
        raise ConformanceError("evidence schema is invalid")
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    failures: list[str] = []
    for error in sorted(validator.iter_errors(evidence), key=lambda item: list(item.path)):
        location = ".".join(str(part) for part in error.absolute_path) or "<root>"
        label = ""
        if location == "clusters":
            label = "evidence cluster set: "
        elif location.endswith("runtime.rootlesskit_flags"):
            label = "rootless runtime policy: "
        elif location.startswith("control_plane_services"):
            label = "control-plane services: "
        elif location == "production_certification_allowed":
            label = "production certification: "
        failures.append(f"{label}schema violation at {location} ({error.validator})")
    return failures


def _parse_collected_at(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ConformanceError("collected_at is not a timezone-qualified timestamp") from exc
    if parsed.tzinfo is None:
        raise ConformanceError("collected_at is not a timezone-qualified timestamp")
    return parsed.astimezone(UTC)


def _exact_mapping(actual: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    return dict(actual) == dict(expected)


def _node_failures(
    node: Mapping[str, Any],
    *,
    cluster: Mapping[str, Any],
    policy: PrerequisitePolicy,
) -> list[str]:
    failures: list[str] = []
    node_name = node["name"]
    architecture = cluster["architecture"]
    if node["architecture"] != architecture:
        failures.append(f"{node_name}: architecture does not match cluster policy")

    identity_policy = policy.raw["identity"]
    identity = node["identity"]
    expected_identity = {
        "user": identity_policy["user"],
        "uid": identity_policy["uid"],
        "group": identity_policy["group"],
        "gid": identity_policy["gid"],
        "supplementary_groups": [],
        "subuid_start": identity_policy["subid_start"],
        "subuid_count": identity_policy["subid_count"],
        "subgid_start": identity_policy["subid_start"],
        "subgid_count": identity_policy["subid_count"],
        "newuidmap_setuid_root": True,
        "newgidmap_setuid_root": True,
    }
    if not _exact_mapping(identity, expected_identity):
        failures.append(f"{node_name}: dedicated builder identity is invalid")

    kernel = node["kernel"]
    required_controllers = {"cpu", "cpuset", "io", "memory", "pids"}
    if (
        kernel["cgroup_version"] != policy.raw["cgroup"]["version"]
        or set(kernel["controllers"]) != required_controllers
        or not kernel["unprivileged_user_namespaces"]
        or not kernel["pidfd_open"]
        or not kernel["sealed_memfd"]
        or not kernel["clone3_into_cgroup"]
        or not kernel["bpffs_mounted_root_only"]
    ):
        failures.append(f"{node_name}: kernel containment prerequisites are invalid")

    runtime = node["runtime"]
    release = policy.runtime["architectures"][architecture]
    if (
        runtime["release"] != policy.runtime["release"]
        or runtime["binary_sha256"] != (release["binaries"])
    ):
        failures.append(f"{node_name}: rootless runtime binaries do not match policy")
    runtime_policy = policy.raw["runtime"]
    if (
        runtime["snapshotter"] != runtime_policy["snapshotter"]
        or runtime["network_driver"] != runtime_policy["network_driver"]
        or runtime["rootlesskit_flags"] != runtime_policy["rootlesskit_flags"]
        or runtime["insecure_entitlements"] != []
    ):
        failures.append(f"{node_name}: rootless runtime policy is invalid")

    storage = node["storage"]
    if (
        storage["filesystem"] not in policy.raw["storage"]["allowed_filesystems"]
        or not storage["project_quota"]
        or not storage["empty_job_root"]
        or not storage["cleanup_supported"]
    ):
        failures.append(f"{node_name}: storage quota or cleanup prerequisite is invalid")

    network = node["network"]
    expected_network = {
        key: policy.raw["network"][key]
        for key in (
            "ipv4_default_deny",
            "ipv6_default_deny",
            "ingress_bytes_per_second",
            "egress_bytes_per_second",
            "ingress_packets_per_second",
            "egress_packets_per_second",
            "concurrent_flows",
            "new_flows_per_second",
            "dns_queries_per_second",
        )
    }
    if not _exact_mapping(network, expected_network):
        failures.append(f"{node_name}: network policy limits do not match policy")
    if node["forbidden_paths_present"]:
        failures.append(f"{node_name}: forbidden host path is visible")
    guard = node["node_guard"]
    if guard["installed"] or guard["active"]:
        failures.append(f"{node_name}: Phase 2 node guard cannot be claimed by Phase 1")
    return failures


def verify_evidence(
    evidence: Mapping[str, object],
    policy: PrerequisitePolicy,
) -> list[str]:
    _scan_for_secrets(evidence)
    failures = _schema_failures(evidence)
    if failures:
        return failures

    item = dict(evidence)
    if item["policy_version"] != policy.raw["policy_version"]:
        failures.append("policy version does not match evidence")
    if item["policy_sha256"] != policy.digest:
        failures.append("policy digest does not match evidence")
    if item["production_certification_allowed"] is not False:
        failures.append("production certification must remain disabled in Phase 1")
    if item["certified_nodes"] != []:
        failures.append("certified_nodes must remain empty in Phase 1")
    if item["control_plane_services"] != policy.raw["control_plane_services"]:
        failures.append("control-plane services do not match prerequisite policy")

    collected_at = _parse_collected_at(str(item["collected_at"]))
    age_seconds = (datetime.now(UTC) - collected_at).total_seconds()
    if age_seconds > 3600 or age_seconds < -300:
        failures.append("evidence timestamp is outside the one-hour freshness window")

    policy_clusters = {cluster["id"]: cluster for cluster in policy.raw["clusters"]}
    evidence_clusters = item["clusters"]
    evidence_ids = [cluster["id"] for cluster in evidence_clusters]
    if set(evidence_ids) != set(policy_clusters) or len(evidence_ids) != len(policy_clusters):
        failures.append("evidence cluster set does not match policy")
    for observed in evidence_clusters:
        cluster_id = observed["id"]
        expected = policy_clusters.get(cluster_id)
        if expected is None:
            continue
        if (
            observed["slurm_cluster"] != expected["slurm_cluster"]
            or observed["controller"] != expected["controller"]
        ):
            failures.append(f"{cluster_id}: controller or Slurm cluster identity is invalid")
        if observed["architecture"] != expected["architecture"]:
            failures.append(f"{cluster_id}: architecture does not match policy")

        slurm = observed["slurm"]
        cgroup_policy = policy.raw["cgroup"]
        if (
            slurm["task_plugin"] != cgroup_policy["task_plugin"]
            or slurm["proctrack_type"] != cgroup_policy["proctrack_type"]
            or slurm["cgroup_version"] != cgroup_policy["version"]
            or not slurm["constrain_cores"]
            or not slurm["constrain_ram_space"]
            or not slurm["constrain_swap_space"]
            or not slurm["constrain_devices"]
        ):
            failures.append(f"{cluster_id}: Slurm cgroup constraints are incomplete")

        trial = slurm["trial_partition"]
        builder = slurm["builder_partition"]
        expected_nodes = expected["builder_nodes"]
        if (
            trial["name"] != expected["trial_partition"]
            or trial["priority_tier"] != expected["trial_priority_tier"]
            or trial["nodes"] != expected_nodes
        ):
            failures.append(f"{cluster_id}: trial partition does not match policy")
        if builder["name"] != expected["builder_partition"] or builder["nodes"] != (expected_nodes):
            failures.append(f"{cluster_id}: builder partition nodes do not match policy")
        if (
            builder["priority_tier"] != expected["builder_priority_tier"]
            or builder["priority_tier"] <= trial["priority_tier"]
        ):
            failures.append(f"{cluster_id}: builder partition lacks a higher priority tier")

        resources = policy.raw["resource_profile"]
        qos = slurm["qos"]
        expected_qos = {
            "name": expected["slurm_qos"],
            "flags": ["DenyOnLimit"],
            "max_jobs_per_user": resources["max_jobs_per_user"],
            "max_submit_jobs_per_user": resources["max_submit_jobs_per_user"],
            "max_wall": resources["wall_time"],
            "group_tres": {
                "cpu": resources["cpus"],
                "memory_mib": resources["memory_mib"],
                "nodes": 1,
            },
        }
        if not _exact_mapping(qos, expected_qos):
            failures.append(f"{cluster_id}: builder QoS is not exactly bounded")
        expected_association = {
            "user": policy.raw["identity"]["user"],
            "account": expected["slurm_account"],
            "partition": expected["builder_partition"],
            "qos": [expected["slurm_qos"]],
            "default_qos": expected["slurm_qos"],
        }
        if not _exact_mapping(slurm["association"], expected_association):
            failures.append(f"{cluster_id}: dedicated Slurm association is invalid")

        nodes = observed["nodes"]
        names = [node["name"] for node in nodes]
        if len(names) != len(set(names)):
            failures.append(f"{cluster_id}: duplicate node evidence is forbidden")
        if set(names) != set(expected_nodes) or len(names) != len(expected_nodes):
            failures.append(f"{cluster_id}: node evidence set does not match policy")
        for node in nodes:
            failures.extend(_node_failures(node, cluster=expected, policy=policy))
    return failures


def certification_blockers(policy: PrerequisitePolicy) -> tuple[str, ...]:
    blockers = policy.raw.get("unconditional_blockers")
    if not isinstance(blockers, list) or not all(isinstance(item, str) for item in blockers):
        raise ConformanceError("policy certification blockers are invalid")
    return tuple(blockers)


def _load_evidence(path: Path) -> dict[str, Any]:
    value = _json_load(path)
    if not isinstance(value, dict):
        raise ConformanceError("evidence root must be an object")
    return value


def _report(evidence: dict[str, Any], policy: PrerequisitePolicy) -> dict[str, Any]:
    failures = verify_evidence(evidence, policy)
    return {
        "schema": EVIDENCE_SCHEMA,
        "prerequisites_valid": not failures,
        "failures": failures,
        "policy_version": policy.raw["policy_version"],
        "policy_sha256": policy.digest,
        "production_certification_allowed": False,
        "certification_blockers": list(certification_blockers(policy)),
        "certified_nodes": [],
    }


def _write_canonical(path: Path, value: Mapping[str, Any]) -> None:
    if path.is_symlink() or path.exists():
        raise ConformanceError("canonical output already exists")
    payload = _canonical_bytes(value) + b"\n"
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as exc:
        raise ConformanceError("cannot write canonical evidence") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("plan", "verify", "canonicalize"):
        command = commands.add_parser(name)
        command.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
        if name != "plan":
            command.add_argument("--evidence", type=Path, required=True)
        if name == "canonicalize":
            command.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        policy = load_policy(arguments.policy)
        if arguments.command == "plan":
            print(
                json.dumps(
                    {
                        "schema": EVIDENCE_SCHEMA,
                        "mode": "read_only",
                        "mutations_supported": False,
                        "policy_version": policy.raw["policy_version"],
                        "policy_sha256": policy.digest,
                        "clusters": [item["id"] for item in policy.raw["clusters"]],
                        "production_certification_allowed": False,
                        "certification_blockers": list(certification_blockers(policy)),
                        "certified_nodes": [],
                    },
                    sort_keys=True,
                )
            )
            return 0
        evidence = _load_evidence(arguments.evidence)
        report = _report(evidence, policy)
        if arguments.command == "canonicalize":
            if not report["prerequisites_valid"]:
                raise ConformanceError("evidence does not satisfy prerequisites")
            _write_canonical(arguments.output, evidence)
        print(json.dumps(report, sort_keys=True))
        return 0 if report["prerequisites_valid"] else 1
    except ConformanceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
