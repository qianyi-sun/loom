#!/usr/bin/env python3
"""Collect and assemble canonical Phase 1 task-image builder evidence."""

from __future__ import annotations

import argparse
import base64
import csv
import ctypes
import errno
import fcntl
import hashlib
import ipaddress
import json
import os
import re
import socket
import stat
import subprocess
import sys
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ops import task_image_builder_authority as authority  # noqa: E402
from scripts.ops import task_image_builder_prerequisite_conformance as conformance  # noqa: E402

MAX_INPUT_BYTES = 4 * 1024 * 1024
MAX_OUTPUT_BYTES = 16 * 1024 * 1024
ZERO_HASH = "0" * 64
INERT_BLOCKER = "phase2_guard_provider_release_missing"
CONTROLLER_SCHEMA = "loom.task-image-builder-controller-evidence/v1"
NODE_SCHEMA = "loom.task-image-builder-node-evidence/v1"
ASSEMBLED_SCHEMA = "loom.task-image-builder-prerequisite-conformance/v1"
BUILDER_FEATURE = "loom_rootless_buildkit"
PHASE2_NAMES = (
    "loom-task-builder-allocation-supervisor",
    "loom-task-builder-node-guard",
    "loom-task-builder-provider",
)
PHASE2_ARTIFACT_PATHS = tuple(Path("/usr/libexec") / name for name in PHASE2_NAMES)
CLEANUP_ABSENCE_FACTS = {
    "processes_absent": True,
    "mounts_absent": True,
    "job_directory_absent": True,
}
CLEANUP_ABSENCE_STDOUT = (
    '{"job_directory_absent":true,"mounts_absent":true,"processes_absent":true,"state":"absent"}\n'
)
DESIRED_CGROUP = (
    b"CgroupPlugin=autodetect\n"
    b"ConstrainCores=yes\n"
    b"ConstrainRAMSpace=yes\n"
    b"ConstrainSwapSpace=yes\n"
    b"ConstrainDevices=yes\n"
)
SUCCESSFUL_MAINTENANCE_EVENTS = (
    "pre_state_recorded",
    "drained",
    "idle",
    "host_preflighted",
    "host_applied",
    "daemon_restarted",
    "readback_verified",
    "admission_verified",
    "reservation_created",
    "smoke_queued",
    "smoke_pending",
    "smoke_running",
    "smoke_observed",
    "smoke_released",
    "smoke_completed",
    "smoke_cleaned",
    "reservation_deleted",
    "prepared",
)


class EvidenceError(ValueError):
    """Evidence input is unsafe, inconsistent, or incomplete."""


@dataclass(frozen=True)
class EvidenceContext:
    candidate_root: Path
    policy_path: Path
    release_path: Path
    runtime_path: Path
    policy: conformance.PrerequisitePolicy
    policy_payload: bytes
    release: dict[str, Any]
    release_payload: bytes
    runtime: dict[str, Any]
    runtime_payload: bytes
    candidate_sha256: str
    authority_binding: authority.AuthorityBinding

    @property
    def policy_file_sha256(self) -> str:
        return _sha(self.policy_payload)

    @property
    def release_sha256(self) -> str:
        return _sha(self.release_payload)

    @property
    def runtime_sha256(self) -> str:
        return _sha(self.runtime_payload)


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _fingerprint(value: object) -> str:
    return _sha(_canonical(value))


def _valid_operation_id(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = uuid.UUID(value)
    except ValueError:
        return False
    return parsed.version == 4 and str(parsed) == value


def _read_regular(
    path: Path,
    label: str,
    *,
    maximum: int = MAX_INPUT_BYTES,
    required_owner: int | None = None,
    required_mode: int | None = None,
) -> bytes:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise EvidenceError(f"{label} is unavailable") from exc
    try:
        initial = os.fstat(descriptor)
        if (
            not stat.S_ISREG(initial.st_mode)
            or initial.st_size > maximum
            or initial.st_nlink != 1
            or (required_owner is not None and initial.st_uid != required_owner)
            or (required_mode is not None and stat.S_IMODE(initial.st_mode) != required_mode)
        ):
            raise EvidenceError(f"{label} metadata is unsafe")
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining:
            chunk = os.read(descriptor, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        final = os.fstat(descriptor)
        if len(payload) > maximum or (
            initial.st_dev,
            initial.st_ino,
            initial.st_size,
            initial.st_mtime_ns,
            initial.st_ctime_ns,
        ) != (
            final.st_dev,
            final.st_ino,
            final.st_size,
            final.st_mtime_ns,
            final.st_ctime_ns,
        ):
            raise EvidenceError(f"{label} changed while being read")
        return payload
    finally:
        os.close(descriptor)


def _json_object(payload: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceError(f"{label} is not valid JSON") from exc
    if not isinstance(value, dict):
        raise EvidenceError(f"{label} must be a JSON object")
    return value


def _write_owner_output(path: Path, value: Mapping[str, Any]) -> None:
    if path.exists() or path.is_symlink():
        raise EvidenceError("selected output already exists")
    payload = _canonical(value) + b"\n"
    if len(payload) > MAX_OUTPUT_BYTES:
        raise EvidenceError("selected output exceeds its size limit")
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
        )
    except OSError as exc:
        raise EvidenceError("selected output cannot be created") from exc
    try:
        os.fchmod(descriptor, 0o600)
    except OSError as exc:
        os.close(descriptor)
        raise EvidenceError("selected output mode cannot be secured") from exc
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise EvidenceError("selected output write failed")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise EvidenceError("observation timestamp must be timezone-qualified")
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_time(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise EvidenceError(f"{label} is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EvidenceError(f"{label} is invalid") from exc
    if parsed.tzinfo is None:
        raise EvidenceError(f"{label} is invalid")
    return parsed.astimezone(UTC)


def _inert(value: Mapping[str, Any], label: str) -> None:
    if (
        value.get("production_certification_allowed") is not False
        or value.get("certified_nodes") != []
        or value.get("blockers") != [INERT_BLOCKER]
    ):
        raise EvidenceError(f"{label} breached the Phase 1 inert boundary")


def _candidate_file(candidate_root: Path, relative: str) -> Path:
    path = candidate_root / relative
    if not path.is_file() or path.is_symlink():
        raise EvidenceError("candidate release input is unavailable")
    return path


def _same_input(explicit: Path, candidate: Path, label: str) -> bytes:
    payload = _read_regular(explicit, label)
    if payload != _read_regular(candidate, f"candidate {label}"):
        raise EvidenceError(f"explicit {label} does not match the candidate")
    return payload


def _load_context(candidate_root: Path, policy_path: Path, release_path: Path) -> EvidenceContext:
    if not candidate_root.is_absolute() or candidate_root.is_symlink():
        raise EvidenceError("candidate root is unsafe")
    try:
        metadata = candidate_root.lstat()
    except OSError as exc:
        raise EvidenceError("candidate root is unavailable") from exc
    if not stat.S_ISDIR(metadata.st_mode):
        raise EvidenceError("candidate root is unsafe")
    candidate_policy = _candidate_file(
        candidate_root, "deploy/task-image-builder/prerequisites-v1.toml"
    )
    candidate_release = _candidate_file(
        candidate_root, "deploy/task-image-builder/host-release-v1.json"
    )
    policy_payload = _same_input(policy_path, candidate_policy, "prerequisite policy")
    release_payload = _same_input(release_path, candidate_release, "host release")
    try:
        policy = conformance.load_policy(policy_path)
    except conformance.ConformanceError as exc:
        raise EvidenceError(str(exc)) from exc
    release = _json_object(release_payload, "host release")
    if (
        release.get("schema") != "loom.task-image-builder-host-release/v1"
        or release.get("release") != "host-release-v1"
        or not isinstance(release.get("runtime_manifest"), str)
        or Path(str(release["runtime_manifest"])).name != release["runtime_manifest"]
    ):
        raise EvidenceError("host release contract is invalid")
    runtime_path = release_path.parent / str(release["runtime_manifest"])
    candidate_runtime = _candidate_file(
        candidate_root,
        "deploy/task-image-builder/" + str(release["runtime_manifest"]),
    )
    runtime_payload = _same_input(runtime_path, candidate_runtime, "runtime manifest")
    runtime = _json_object(runtime_payload, "runtime manifest")
    if runtime.get("schema") != "loom.task-image-builder-rootless-runtime/v1":
        raise EvidenceError("runtime manifest contract is invalid")
    try:
        authority_binding = authority.load_authority_binding(candidate_root)
    except authority.AuthorityError as exc:
        raise EvidenceError("candidate authority component binding is invalid") from exc
    candidate_components = {
        "policy": _sha(policy_payload),
        "release": _sha(release_payload),
        "runtime": _sha(runtime_payload),
        **authority_binding.as_dict(),
    }
    return EvidenceContext(
        candidate_root=candidate_root,
        policy_path=policy_path,
        release_path=release_path,
        runtime_path=runtime_path,
        policy=policy,
        policy_payload=policy_payload,
        release=release,
        release_payload=release_payload,
        runtime=runtime,
        runtime_payload=runtime_payload,
        candidate_sha256=_fingerprint(candidate_components),
        authority_binding=authority_binding,
    )


def _cluster(context: EvidenceContext, cluster_id: str) -> dict[str, Any]:
    matches = [
        item
        for item in context.policy.raw["clusters"]
        if isinstance(item, dict) and item.get("id") == cluster_id
    ]
    if len(matches) != 1:
        raise EvidenceError("cluster policy is not unique")
    return matches[0]


def _slurm_candidate_digest(context: EvidenceContext) -> str:
    components: dict[str, object] = {
        "policy": context.policy_file_sha256,
        **context.authority_binding.as_dict(),
    }
    return _fingerprint(components)


def _host_candidate_digest(context: EvidenceContext) -> str:
    components: dict[str, object] = {
        "policy": context.policy_file_sha256,
        "release": context.release_sha256,
        "runtime": context.runtime_sha256,
        **context.authority_binding.as_dict(),
    }
    return _fingerprint(components)


def _maintenance_candidate_digest(context: EvidenceContext) -> str:
    return _fingerprint(
        {
            "policy": context.policy_file_sha256,
            **context.authority_binding.as_dict(),
        }
    )


def _validate_event_chain(document: Mapping[str, Any], label: str) -> list[dict[str, Any]]:
    raw_events = document.get("events")
    if not isinstance(raw_events, list) or not raw_events:
        raise EvidenceError(f"{label} event chain is invalid")
    events: list[dict[str, Any]] = []
    previous = ZERO_HASH
    for sequence, raw_event in enumerate(raw_events):
        if not isinstance(raw_event, dict):
            raise EvidenceError(f"{label} event chain is invalid")
        event = dict(raw_event)
        event_hash = event.pop("event_hash", None)
        if (
            event.get("sequence") != sequence
            or event.get("previous_hash") != previous
            or not isinstance(event.get("type"), str)
            or not isinstance(event.get("data"), dict)
            or event_hash != _fingerprint(event)
        ):
            raise EvidenceError(f"{label} event chain is invalid")
        previous = str(event_hash)
        events.append(dict(raw_event))
    return events


def _receipt_wrapper_from_path(
    path: Path,
    label: str,
    owner: int,
) -> dict[str, Any]:
    payload = _read_regular(
        path,
        label,
        required_owner=owner,
        required_mode=0o600,
    )
    document = _json_object(payload, label)
    try:
        conformance._scan_for_secrets(document)
    except conformance.ConformanceError as exc:
        raise EvidenceError(str(exc)) from exc
    if payload != _canonical(document) + b"\n":
        raise EvidenceError(f"{label} is not canonical")
    return {"sha256": _sha(payload), "document": document}


def _validate_embedded_receipt(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"sha256", "document"}:
        raise EvidenceError(f"{label} wrapper is invalid")
    digest = value.get("sha256")
    document = value.get("document")
    if (
        not isinstance(digest, str)
        or re.fullmatch(r"[a-f0-9]{64}", digest) is None
        or not isinstance(document, dict)
        or digest != _sha(_canonical(document) + b"\n")
    ):
        raise EvidenceError(f"{label} digest is invalid")
    try:
        conformance._scan_for_secrets(document)
    except conformance.ConformanceError as exc:
        raise EvidenceError(str(exc)) from exc
    return {"sha256": digest, "document": document}


def _validate_authority_binding(
    document: Mapping[str, object],
    context: EvidenceContext,
    label: str,
) -> None:
    try:
        authority.validate_authority_binding(document, context.authority_binding)
    except authority.AuthorityError as exc:
        raise EvidenceError(f"{label} authority component binding is invalid") from exc


def _validate_slurm_receipt(
    wrapper: object,
    context: EvidenceContext,
    cluster: Mapping[str, Any],
) -> dict[str, Any]:
    receipt = _validate_embedded_receipt(wrapper, "Slurm receipt")
    document = receipt["document"]
    _validate_authority_binding(document, context, "Slurm receipt")
    cluster_id = str(cluster["id"])
    if (
        document.get("schema") != "loom.task-image-builder-slurm-receipt/v1"
        or not _valid_operation_id(document.get("operation_id"))
        or document.get("cluster_id") != cluster_id
        or document.get("candidate_digest") != _slurm_candidate_digest(context)
        or document.get("policy_digest") != context.policy_file_sha256
        or document.get("cluster_digest") != _fingerprint(cluster)
    ):
        raise EvidenceError(f"{cluster_id}: Slurm receipt operation identity or binding is invalid")
    _inert(document, "Slurm receipt")
    if document.get("terminal_state") != "converged":
        raise EvidenceError(f"{cluster_id}: Slurm receipt terminal state is invalid")
    pre_state = document.get("pre_state")
    post_state = document.get("post_state")
    if not isinstance(pre_state, dict) or not isinstance(post_state, dict):
        raise EvidenceError(f"{cluster_id}: Slurm receipt state is incomplete")
    required_post = {"partition", "account", "qos", "association", "legacy"}
    if set(post_state) != required_post or any(
        not isinstance(post_state[key], dict) for key in required_post
    ):
        raise EvidenceError(f"{cluster_id}: Slurm receipt post-state is incomplete")
    pre_legacy = pre_state.get("legacy")
    post_legacy = post_state.get("legacy")
    guard = context.policy.raw["legacy_guard"]
    expected_legacy = {
        "qos": {
            "name": guard["qos"],
            "flags": ["DenyOnLimit"],
            "priority": 0,
            "max_jobs_per_user": guard["max_jobs_per_user"],
            "max_submit_jobs_per_user": guard["max_submit_jobs_per_user"],
            "max_wall": guard["max_wall"],
            "group_tres": {},
        },
        "association": {
            "cluster": cluster["slurm_cluster"],
            "account": guard["account"],
            "user": guard["user"],
            "qos": sorted([cluster["legacy_base_qos"], guard["qos"]]),
            "default_qos": cluster["legacy_base_qos"],
        },
        "reservation": {
            "name": guard["reservation"],
            "node": cluster["legacy_reservation_node"],
            "node_count": 1,
            "partition": cluster["legacy_reservation_partition"],
            "users": [guard["user"]],
            "accounts": [guard["account"]],
            "state": "ACTIVE",
            "flags": ["IGNORE_JOBS", "SPEC_NODES"],
        },
    }
    if (
        set(pre_state) != required_post
        or not isinstance(pre_legacy, dict)
        or not isinstance(post_legacy, dict)
        or pre_legacy != expected_legacy
        or post_legacy != expected_legacy
        or document.get("legacy_pre_fingerprint") != _fingerprint(pre_legacy)
        or document.get("legacy_post_fingerprint") != _fingerprint(post_legacy)
        or document.get("legacy_pre_fingerprint") != document.get("legacy_post_fingerprint")
    ):
        raise EvidenceError(f"{cluster_id}: legacy Slurm fingerprints changed")
    command = document.get("command_outcome")
    created = document.get("created_objects")
    if (
        not isinstance(command, dict)
        or command.get("returncode") != 0
        or document.get("post_readback_error") is not None
        or not isinstance(created, list)
    ):
        raise EvidenceError(f"{cluster_id}: Slurm convergence receipt is unsuccessful")
    events = _validate_event_chain(document, "Slurm receipt")
    if [event["type"] for event in events] != [
        "pre_state",
        "intent",
        "post_state",
        "converged",
    ]:
        raise EvidenceError(f"{cluster_id}: Slurm receipt event sequence is invalid")
    if (
        events[0]["data"] != {"state": pre_state}
        or events[-2]["data"]
        != {
            "state": post_state,
            "readback_error": None,
            "created_objects": created,
        }
        or events[-1]["data"] != {"returncode": 0, "legacy_unchanged": True}
    ):
        raise EvidenceError(f"{cluster_id}: Slurm receipt event binding is invalid")
    return receipt


def _validate_quota(
    value: object,
    context: EvidenceContext,
    *,
    label: str,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise EvidenceError(f"{label} quota readback is invalid")
    resources = context.policy.raw["resource_profile"]
    expected_limits = {
        "storage_root_exists": True,
        "storage_root_uid": 993,
        "storage_root_gid": 980,
        "storage_root_mode": 0o700,
        "storage_root_entries": [],
        "project_id": context.policy.raw["storage"]["project_id"],
        "project_inherit": True,
        "block_soft_limit": 0,
        "block_hard_limit": resources["scratch_bytes"] // 1024,
        "inode_soft_limit": 0,
        "inode_hard_limit": resources["scratch_inodes"],
    }
    expected_keys = {*expected_limits, "block_used", "inode_used"}
    if (
        set(value) != expected_keys
        or any(value.get(key) != expected for key, expected in expected_limits.items())
        or not isinstance(value.get("block_used"), int)
        or isinstance(value.get("block_used"), bool)
        or int(value["block_used"]) < 0
        or not isinstance(value.get("inode_used"), int)
        or isinstance(value.get("inode_used"), bool)
        or int(value["inode_used"]) < 0
    ):
        raise EvidenceError(f"{label} quota readback is invalid")
    return dict(value)


def _validate_cgroup_poststate(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise EvidenceError(f"{label} cgroup readback is invalid")
    try:
        payload = base64.b64decode(str(value.get("payload_b64")), validate=True)
    except (ValueError, TypeError) as exc:
        raise EvidenceError(f"{label} cgroup readback is invalid") from exc
    if (
        set(value) != {"kind", "payload_b64", "sha256", "mode", "uid", "gid"}
        or value.get("kind") != "regular"
        or payload != DESIRED_CGROUP
        or value.get("sha256") != _sha(payload)
        or value.get("mode") != 0o644
        or value.get("uid") != 0
        or value.get("gid") != 0
    ):
        raise EvidenceError(f"{label} cgroup readback is invalid")
    return dict(value)


def _validate_host_receipt(
    wrapper: object,
    context: EvidenceContext,
    cluster: Mapping[str, Any],
    node_name: str,
) -> dict[str, Any]:
    receipt = _validate_embedded_receipt(wrapper, "host receipt")
    document = receipt["document"]
    _validate_authority_binding(document, context, "host receipt")
    cluster_id = str(cluster["id"])
    if (
        document.get("schema") != "loom.task-image-builder-host-receipt/v1"
        or not _valid_operation_id(document.get("operation_id"))
        or document.get("cluster_id") != cluster_id
        or document.get("slurm_node") != node_name
        or document.get("candidate_digest") != _host_candidate_digest(context)
        or document.get("policy_digest") != context.policy_file_sha256
        or document.get("release_digest") != context.release_sha256
        or document.get("cluster_digest") != _fingerprint(cluster)
    ):
        raise EvidenceError(f"{node_name}: host receipt operation identity or binding is invalid")
    _inert(document, "host receipt")
    if (
        document.get("terminal_state") != "host_prepared"
        or document.get("activation_required") is not True
        or document.get("failure") is not None
        or document.get("rollback_verified") is not None
        or document.get("rollback_source_state") is not None
    ):
        raise EvidenceError(f"{node_name}: host receipt terminal state is invalid")
    pre_state = document.get("pre_state")
    post_state = document.get("post_state")
    bundle_digest = document.get("bundle_digest")
    if (
        not isinstance(pre_state, dict)
        or not isinstance(post_state, dict)
        or not isinstance(bundle_digest, str)
        or re.fullmatch(r"[a-f0-9]{64}", bundle_digest) is None
    ):
        raise EvidenceError(f"{node_name}: host receipt state is incomplete")
    expected_packages = {
        name: item["version"]
        for name, item in context.release["packages"][
            context.release["architecture_map"][cluster["architecture"]]
        ].items()
    }
    required_true = (
        "helpers_exact",
        "identity_exact",
        "runtime_exact",
        "quota_exact",
        "storage_exact",
        "kernel_exact",
        "forbidden_sockets_absent",
    )
    if (
        post_state.get("architecture") != cluster["architecture"]
        or post_state.get("slurm_node") != node_name
        or post_state.get("bundle_digest") != bundle_digest
        or post_state.get("packages") != expected_packages
        or any(post_state.get(key) is not True for key in required_true)
    ):
        raise EvidenceError(f"{node_name}: host receipt raw facts are incomplete")
    _validate_quota(post_state.get("quota_state"), context, label=node_name)
    cgroup = _validate_cgroup_poststate(document.get("cgroup_poststate"), node_name)
    events = _validate_event_chain(document, "host receipt")
    if [event["type"] for event in events] != [
        "pre_state",
        "intent",
        "post_state",
        "host_prepared",
    ]:
        raise EvidenceError(f"{node_name}: host receipt event sequence is invalid")
    binding = {
        key: document.get(key)
        for key in (
            "operation_id",
            "cluster_id",
            "slurm_node",
            "candidate_digest",
            "policy_digest",
            "release_digest",
            "cluster_digest",
            "authority_manifest_sha256",
            "authority_component_digests",
            "bundle_digest",
        )
    }
    if (
        events[0]["data"]
        != {
            "binding": binding,
            "facts": pre_state,
            "cgroup": document.get("cgroup_prestate"),
        }
        or events[-2]["data"] != {"facts": post_state, "cgroup": cgroup}
        or events[-1]["data"]
        != {
            "activation_required": True,
            "created_inert_artifacts": document.get("created_inert_artifacts"),
        }
    ):
        raise EvidenceError(f"{node_name}: host receipt event binding is invalid")
    return receipt


def _command_result(value: object, label: str) -> dict[str, Any]:
    if (
        not isinstance(value, dict)
        or set(value) != {"command", "returncode", "stdout", "stderr"}
        or not isinstance(value.get("command"), list)
        or not all(isinstance(item, str) and item for item in value["command"])
        or not isinstance(value.get("returncode"), int)
        or isinstance(value.get("returncode"), bool)
        or not isinstance(value.get("stdout"), str)
        or not isinstance(value.get("stderr"), str)
    ):
        raise EvidenceError(f"{label} command receipt is invalid")
    return dict(value)


def _reservation_rows(value: Mapping[str, Any], label: str) -> dict[str, dict[str, str]]:
    if value.get("returncode") != 0 or value.get("stderr") != "":
        raise EvidenceError(f"{label} is invalid")
    lines = str(value.get("stdout", "")).splitlines()
    rows: dict[str, dict[str, str]] = {}
    for line in lines:
        if not line:
            continue
        if line == "No reservations in the system":
            if len(lines) != 1:
                raise EvidenceError(f"{label} is invalid")
            return {}
        pairs = [token.split("=", 1) for token in line.split()]
        if any(len(pair) != 2 for pair in pairs):
            raise EvidenceError(f"{label} is invalid")
        fields = {pair[0]: pair[1] for pair in pairs}
        name = fields.get("ReservationName")
        if not name or len(fields) != len(pairs) or name in rows:
            raise EvidenceError(f"{label} is invalid")
        rows[name] = fields
    return rows


def _maintenance_command_matches(
    command: object,
    action: str,
    job_id: str,
    operation_id: str,
) -> bool:
    return bool(
        isinstance(command, list)
        and len(command) == 5
        and isinstance(command[0], str)
        and Path(command[0]).is_absolute()
        and command[0].endswith("/scripts/ops/task_image_builder_node_maintenance.py")
        and command[1:] == ["--internal-smoke", action, job_id, operation_id]
    )


def _cpuset_cpu_count(value: object) -> int:
    if not isinstance(value, str) or not value or len(value) > 4096:
        raise EvidenceError("maintenance smoke cpuset evidence is invalid")
    ranges: list[tuple[int, int]] = []
    for raw_range in value.split(","):
        fields = raw_range.split("-")
        if len(fields) not in {1, 2} or any(
            re.fullmatch(r"[0-9]+", field) is None for field in fields
        ):
            raise EvidenceError("maintenance smoke cpuset evidence is invalid")
        start = int(fields[0])
        end = start if len(fields) == 1 else int(fields[1])
        if start > end or end > 2**31 - 1:
            raise EvidenceError("maintenance smoke cpuset evidence is invalid")
        ranges.append((start, end))
    ranges.sort()
    if any(current[0] <= previous[1] for previous, current in pairwise(ranges)):
        raise EvidenceError("maintenance smoke cpuset evidence is invalid")
    return sum(end - start + 1 for start, end in ranges)


def _event_by_type(events: Sequence[Mapping[str, Any]], event_type: str) -> Mapping[str, Any]:
    matches = [event for event in events if event.get("type") == event_type]
    if len(matches) != 1:
        raise EvidenceError(f"maintenance {event_type} receipt is invalid")
    return matches[0]


def _validate_daemon_observation(value: object, node_name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"state", "cgroup_config"}:
        raise EvidenceError(f"{node_name}: Slurm cgroup daemon readback is invalid")
    config = value.get("cgroup_config")
    if (
        value.get("state") != "active"
        or not isinstance(config, dict)
        or set(config) != {"path", "sha256", "contents"}
        or config.get("path") != "/etc/slurm/cgroup.conf"
        or config.get("contents") != DESIRED_CGROUP.decode("utf-8")
        or config.get("sha256") != _sha(DESIRED_CGROUP)
    ):
        raise EvidenceError(f"{node_name}: Slurm cgroup daemon readback is invalid")
    return dict(value)


def _validate_admission(
    value: object,
    context: EvidenceContext,
    cluster: Mapping[str, Any],
    node_name: str,
) -> None:
    if not isinstance(value, dict) or set(value) != {"builder", "rollout_rejected"}:
        raise EvidenceError(f"{node_name}: Slurm admission receipt is invalid")
    builder = _command_result(value.get("builder"), "builder admission")
    rollout = _command_result(value.get("rollout_rejected"), "rollout admission")
    resources = context.policy.raw["resource_profile"]
    admission_args = [
        "--test-only",
        f"--account={cluster['slurm_account']}",
        f"--qos={cluster['slurm_qos']}",
        f"--partition={cluster['builder_partition']}",
        f"--cpus-per-task={resources['cpus']}",
        f"--mem={resources['memory_mib']}M",
        f"--time={resources['wall_time']}",
        "--wrap=/usr/bin/true",
    ]
    builder_command = builder["command"]
    rollout_command = rollout["command"]
    assert isinstance(builder_command, list) and isinstance(rollout_command, list)
    if (
        builder.get("returncode") != 0
        or int(rollout["returncode"]) == 0
        or builder_command
        != [
            "/usr/sbin/runuser",
            "--user",
            "loom-builder",
            "--",
            "/usr/bin/sbatch",
            *admission_args,
        ]
        or rollout_command
        != [
            "/usr/sbin/runuser",
            "--user",
            "loom-rollout",
            "--",
            "/usr/bin/sbatch",
            *admission_args,
        ]
    ):
        raise EvidenceError(f"{node_name}: Slurm admission receipt is invalid")


def _validate_maintenance_receipt(
    wrapper: object,
    context: EvidenceContext,
    cluster: Mapping[str, Any],
    node_name: str,
    host_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    receipt = _validate_embedded_receipt(wrapper, "maintenance receipt")
    document = receipt["document"]
    _validate_authority_binding(document, context, "maintenance receipt")
    cluster_id = str(cluster["id"])
    if (
        document.get("schema") != "loom.task-image-builder-node-maintenance/v1"
        or not _valid_operation_id(document.get("operation_id"))
        or document.get("cluster_id") != cluster_id
        or document.get("slurm_node") != node_name
        or document.get("candidate_digest") != _maintenance_candidate_digest(context)
        or document.get("policy_digest") != context.policy_file_sha256
        or document.get("operation_id") != host_receipt["document"].get("operation_id")
    ):
        raise EvidenceError(
            f"{node_name}: maintenance receipt operation identity or binding is invalid"
        )
    _inert(document, "maintenance receipt")
    if document.get("terminal_state") != "prepared" or document.get("failure") is not None:
        raise EvidenceError(f"{node_name}: maintenance receipt terminal state is invalid")
    events = _validate_event_chain(document, "maintenance receipt")
    if tuple(str(event["type"]) for event in events) != SUCCESSFUL_MAINTENANCE_EVENTS:
        raise EvidenceError(f"{node_name}: maintenance receipt terminal chain is invalid")
    pre_state = document.get("pre_state")
    if not isinstance(pre_state, dict) or set(pre_state) != {
        "state",
        "reason",
        "allocated_tres",
    }:
        raise EvidenceError(f"{node_name}: maintenance pre-state is invalid")
    operation_id = str(document["operation_id"])
    owned_reason = f"loom-task-builder-phase1/host-release-v1/{operation_id}"
    state = str(pre_state.get("state"))
    if "DRAIN" in state and pre_state.get("reason") != owned_reason:
        raise EvidenceError(f"{node_name}: foreign drain ownership is forbidden")
    if events[0]["data"] != {"pre_state": pre_state} or _event_by_type(events, "drained")[
        "data"
    ] != {"reason": owned_reason}:
        raise EvidenceError(f"{node_name}: owned drain receipt is invalid")
    observations = document.get("observations")
    if not isinstance(observations, dict) or set(observations) != {
        "daemon",
        "admission",
        "reservation",
        "smoke",
        "emergency_containment",
    }:
        raise EvidenceError(f"{node_name}: maintenance observations are incomplete")
    if observations.get("emergency_containment") is not None:
        raise EvidenceError(
            f"{node_name}: successful maintenance cannot claim emergency containment"
        )
    daemon = observations.get("daemon")
    if not isinstance(daemon, dict) or set(daemon) != {"restart", "check"}:
        raise EvidenceError(f"{node_name}: Slurm cgroup daemon readback is invalid")
    restart = _validate_daemon_observation(daemon.get("restart"), node_name)
    check = _validate_daemon_observation(daemon.get("check"), node_name)
    if restart != check:
        raise EvidenceError(f"{node_name}: Slurm cgroup daemon readback changed")
    _validate_admission(observations.get("admission"), context, cluster, node_name)

    reservation = observations.get("reservation")
    if not isinstance(reservation, dict) or set(reservation) != {
        "name",
        "prior_readback",
        "prior_absence",
        "create",
        "create_readback",
        "binding",
        "delete",
        "delete_readback",
        "absence",
    }:
        raise EvidenceError(f"{node_name}: maintenance reservation receipt is invalid")
    reservation_name = "loom_task_builder_maintenance_" + operation_id.replace("-", "")
    binding = reservation.get("binding")
    prior_absence = reservation.get("prior_absence")
    absence = reservation.get("absence")
    if (
        reservation.get("name") != reservation_name
        or binding
        != {
            "name": reservation_name,
            "node": node_name,
            "state": "ACTIVE",
            "user": "loom-builder",
        }
        or prior_absence != {"name": reservation_name, "absent": True}
        or absence != {"name": reservation_name, "absent": True}
    ):
        raise EvidenceError(f"{node_name}: maintenance reservation lifecycle is invalid")
    for key in (
        "prior_readback",
        "create",
        "create_readback",
        "delete",
        "delete_readback",
    ):
        command = _command_result(reservation.get(key), "maintenance reservation")
        expected_command = {
            "prior_readback": [
                "/usr/bin/scontrol",
                "show",
                "reservation",
                "--oneliner",
            ],
            "create": [
                "/usr/bin/scontrol",
                "create",
                "reservation",
                f"Name={reservation_name}",
                f"Nodes={node_name}",
                "Users=loom-builder",
                "StartTime=now",
                "Duration=00:15:00",
            ],
            "create_readback": [
                "/usr/bin/scontrol",
                "show",
                "reservation",
                "--oneliner",
            ],
            "delete": [
                "/usr/bin/scontrol",
                "delete",
                "reservation",
                f"Name={reservation_name}",
            ],
            "delete_readback": [
                "/usr/bin/scontrol",
                "show",
                "reservation",
                "--oneliner",
            ],
        }[key]
        if command.get("returncode") != 0 or command.get("command") != expected_command:
            raise EvidenceError(f"{node_name}: maintenance reservation lifecycle is invalid")
    prior_rows = _reservation_rows(
        reservation["prior_readback"],
        f"{node_name}: maintenance reservation prior readback",
    )
    created_rows = _reservation_rows(
        reservation["create_readback"],
        f"{node_name}: maintenance reservation create readback",
    )
    deleted_rows = _reservation_rows(
        reservation["delete_readback"],
        f"{node_name}: maintenance reservation delete readback",
    )
    created_fields = created_rows.get(reservation_name)
    if (
        reservation_name in prior_rows
        or reservation_name in deleted_rows
        or created_fields is None
        or created_fields.get("Nodes") != node_name
        or created_fields.get("Users") != "loom-builder"
        or created_fields.get("State") != "ACTIVE"
    ):
        raise EvidenceError(f"{node_name}: maintenance reservation lifecycle is invalid")
    created_event = _event_by_type(events, "reservation_created")
    deleted_event = _event_by_type(events, "reservation_deleted")
    if created_event["data"] != {
        "name": reservation_name,
        "create": reservation["create"],
        "create_readback": reservation["create_readback"],
        "binding": binding,
    } or deleted_event["data"] != {
        "name": reservation_name,
        "delete": reservation["delete"],
        "delete_readback": reservation["delete_readback"],
        "absence": absence,
    }:
        raise EvidenceError(f"{node_name}: maintenance reservation event chain is invalid")

    smoke = observations.get("smoke")
    if not isinstance(smoke, dict) or set(smoke) != {
        "job_id",
        "allocation",
        "cgroup",
        "cgroup_path",
        "cleanup",
    }:
        raise EvidenceError(f"{node_name}: maintenance smoke receipt is invalid")
    job_id = smoke.get("job_id")
    cgroup_path = smoke.get("cgroup_path")
    allocation = smoke.get("allocation")
    if (
        not isinstance(job_id, str)
        or re.fullmatch(r"[1-9][0-9]*", job_id) is None
        or not isinstance(cgroup_path, str)
        or re.fullmatch(
            rf"/[A-Za-z0-9_./:-]*/job_{re.escape(job_id)}/step_[A-Za-z0-9_.:-]+",
            cgroup_path,
        )
        is None
        or allocation != {"node": node_name, "sole_first_allocation": True}
    ):
        raise EvidenceError(f"{node_name}: maintenance smoke allocation is invalid")
    resources = context.policy.raw["resource_profile"]
    controls = smoke.get("cgroup")
    if not isinstance(controls, dict) or set(controls) != {
        "cpuset_cpus_effective",
        "cpuset_cpu_count",
        "memory_max",
        "memory_swap_max",
        "devices",
    }:
        raise EvidenceError(f"{node_name}: maintenance smoke cgroup receipt is invalid")
    devices = controls.get("devices")
    if not isinstance(devices, dict) or set(devices) != {"cgroup_path", "programs"}:
        raise EvidenceError(f"{node_name}: maintenance smoke device containment is invalid")
    programs = devices.get("programs")
    observed_cpu_count = _cpuset_cpu_count(controls.get("cpuset_cpus_effective"))
    if (
        controls.get("cpuset_cpu_count") != observed_cpu_count
        or observed_cpu_count != resources["cpus"]
        or controls.get("memory_max") != resources["memory_mib"] * 1024 * 1024
        or controls.get("memory_swap_max") != resources["swap_bytes"]
        or devices.get("cgroup_path") != cgroup_path
        or not isinstance(programs, list)
        or not programs
        or any(
            not isinstance(program, dict)
            or program.get("attach_type") != "cgroup_device"
            or not isinstance(program.get("id"), int)
            or isinstance(program.get("id"), bool)
            or int(program["id"]) <= 0
            for program in programs
        )
    ):
        raise EvidenceError(f"{node_name}: maintenance smoke device containment is invalid")
    observed = _event_by_type(events, "smoke_observed")
    expected_smoke_evidence = {
        "schema": "loom.task-image-builder-maintenance-smoke/v1",
        "operation_id": operation_id,
        "job_id": job_id,
        "cgroup_path": cgroup_path,
        "controls": controls,
    }
    if observed["data"] != {"job_id": job_id, "evidence": expected_smoke_evidence}:
        raise EvidenceError(f"{node_name}: maintenance smoke raw evidence is invalid")
    released = _event_by_type(events, "smoke_released")["data"]
    if not isinstance(released, dict) or released.get("job_id") != job_id:
        raise EvidenceError(f"{node_name}: maintenance smoke release receipt is invalid")
    release_value = released.get("release")
    if not isinstance(release_value, dict) or set(release_value) != {
        "command",
        "returncode",
        "stdout",
        "stderr",
        "outcome",
    }:
        raise EvidenceError(f"{node_name}: maintenance smoke release receipt is invalid")
    release_command = _command_result(
        {key: value for key, value in release_value.items() if key != "outcome"},
        "maintenance smoke release",
    )
    if (
        release_command.get("returncode") != 0
        or not _maintenance_command_matches(
            release_command.get("command"),
            "release",
            job_id,
            operation_id,
        )
        or release_command.get("stdout") != '{"state":"released"}\n'
        or release_value.get("outcome") != "released"
    ):
        raise EvidenceError(f"{node_name}: maintenance smoke release receipt is invalid")
    completed = _event_by_type(events, "smoke_completed")["data"]
    accounting = completed.get("accounting") if isinstance(completed, dict) else None
    if not isinstance(accounting, dict) or set(accounting) != {"readback", "top_level"}:
        raise EvidenceError(f"{node_name}: maintenance smoke accounting is invalid")
    accounting_readback = _command_result(
        accounting.get("readback"), "maintenance smoke accounting"
    )
    accounting_rows = [
        row
        for row in str(accounting_readback.get("stdout", "")).splitlines()
        if row.split("|", 1)[0] == job_id
    ]
    if (
        completed.get("job_id") != job_id
        or accounting_readback.get("returncode") != 0
        or accounting_readback.get("command")
        != [
            "/usr/bin/sacct",
            "--noheader",
            "--parsable2",
            "--jobs",
            job_id,
            "--format=JobIDRaw,State,ExitCode",
        ]
        or accounting_rows != [f"{job_id}|COMPLETED|0:0"]
        or accounting.get("top_level")
        != {"job_id": job_id, "state": "COMPLETED", "exit_code": "0:0"}
    ):
        raise EvidenceError(f"{node_name}: maintenance smoke accounting is invalid")
    cleanup_observation = smoke.get("cleanup")
    cleaned = _event_by_type(events, "smoke_cleaned")["data"]
    cleanup = cleaned.get("cleanup") if isinstance(cleaned, dict) else None
    if (
        cleanup_observation != CLEANUP_ABSENCE_FACTS
        or not isinstance(cleanup, dict)
        or cleaned.get("job_id") != job_id
        or any(cleanup.get(key) is not True for key in CLEANUP_ABSENCE_FACTS)
    ):
        raise EvidenceError(f"{node_name}: maintenance smoke cleanup is invalid")
    cleanup_command = _command_result(
        {key: value for key, value in cleanup.items() if key not in CLEANUP_ABSENCE_FACTS},
        "maintenance smoke cleanup",
    )
    if (
        cleanup_command.get("returncode") != 0
        or not _maintenance_command_matches(
            cleanup_command.get("command"),
            "cleanup",
            job_id,
            operation_id,
        )
        or cleanup_command.get("stdout") != CLEANUP_ABSENCE_STDOUT
    ):
        raise EvidenceError(f"{node_name}: maintenance smoke cleanup is invalid")
    if events[-1]["data"] != {"job_id": job_id}:
        raise EvidenceError(f"{node_name}: prepared maintenance claim is not terminal-bound")
    return receipt


def _validate_controller_observation(
    value: Mapping[str, Any],
    context: EvidenceContext,
    cluster_id: str,
) -> dict[str, Any]:
    identity = context.policy.raw["identity"]
    expected = {
        "user": identity["user"],
        "uid": identity["uid"],
        "group": identity["group"],
        "gid": identity["gid"],
        "home": identity["home"],
        "shell": identity["shell"],
        "supplementary_groups": [],
    }
    if dict(value) != expected:
        raise EvidenceError(f"{cluster_id}: controller identity readback is invalid")
    return expected


def _validate_slurm_identity(value: object, node_name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {
        "node_name",
        "node_hostname",
        "node_addr",
        "resolved_addresses",
        "local_hostnames",
        "local_addresses",
        "resolution",
        "readback",
    }:
        raise EvidenceError(f"{node_name}: physical/Slurm alias binding is invalid")
    try:
        resolved = {ipaddress.ip_address(item) for item in value["resolved_addresses"]}
        local = {ipaddress.ip_address(item) for item in value["local_addresses"]}
    except (TypeError, ValueError) as exc:
        raise EvidenceError(f"{node_name}: physical/Slurm alias binding is invalid") from exc
    hostnames = value.get("local_hostnames")
    node_addr = value.get("node_addr")
    resolution = value.get("resolution")
    readback = _command_result(value.get("readback"), "Slurm node identity")
    fields = {
        token.split("=", 1)[0]: token.split("=", 1)[1]
        for token in str(readback.get("stdout", "")).split()
        if "=" in token
    }
    if not isinstance(resolution, dict) or set(resolution) != {"query", "addresses"}:
        raise EvidenceError(f"{node_name}: physical/Slurm alias binding is invalid")
    try:
        literal_address = ipaddress.ip_address(str(node_addr))
    except ValueError:
        literal_address = None
    if (
        value.get("node_name") != node_name
        or not isinstance(value.get("node_hostname"), str)
        or not isinstance(node_addr, str)
        or not isinstance(hostnames, list)
        or not all(isinstance(item, str) for item in hostnames)
        or not resolved
        or any(address.is_loopback for address in resolved)
        or not resolved.issubset(local)
        or resolution.get("query") != node_addr
        or resolution.get("addresses") != value.get("resolved_addresses")
        or (literal_address is not None and literal_address not in resolved)
        or readback.get("command") != ["/usr/bin/scontrol", "show", "node", node_name, "-o"]
        or readback.get("returncode") != 0
        or readback.get("stderr") != ""
        or fields.get("NodeName") != node_name
        or fields.get("NodeHostName") != value.get("node_hostname")
        or fields.get("NodeAddr") != node_addr
        or BUILDER_FEATURE in _slurm_feature_names(fields.get("AvailableFeatures"))
        or BUILDER_FEATURE in _slurm_feature_names(fields.get("ActiveFeatures"))
        or str(value["node_hostname"]).casefold() not in {item.casefold() for item in hostnames}
    ):
        raise EvidenceError(f"{node_name}: physical/Slurm alias binding is invalid")
    return dict(value)


def _slurm_feature_names(value: object) -> set[str]:
    if value in {None, "", "(null)"}:
        return set()
    if not isinstance(value, str):
        return {"<invalid>"}
    return set(value.split(","))


def _validate_phase2_absence(value: object, node_name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {
        "installed",
        "active",
        "artifacts",
        "unit_readback",
        "process_readback",
    }:
        raise EvidenceError(f"{node_name}: Phase 2 node guard readback is invalid")
    expected_artifacts = [{"path": str(path), "present": False} for path in PHASE2_ARTIFACT_PATHS]
    if value.get("artifacts") != expected_artifacts:
        raise EvidenceError(f"{node_name}: Phase 2 node guard readback is invalid")
    units = value.get("unit_readback")
    processes = value.get("process_readback")
    if (
        value.get("installed") is not False
        or value.get("active") is not False
        or not isinstance(units, list)
        or not isinstance(processes, list)
        or len(units) != len(PHASE2_NAMES)
        or len(processes) != len(PHASE2_NAMES)
    ):
        raise EvidenceError(f"{node_name}: Phase 2 node guard readback is invalid")
    for name, raw in zip(PHASE2_NAMES, units, strict=True):
        if not isinstance(raw, dict) or set(raw) != {
            "name",
            "command",
            "returncode",
            "stdout",
            "stderr",
        }:
            raise EvidenceError(f"{node_name}: Phase 2 unit readback is invalid")
        command = _command_result(
            {key: item for key, item in raw.items() if key != "name"},
            "Phase 2 unit",
        )
        unit = f"{name}.service"
        fields = dict(
            line.split("=", 1) for line in str(command["stdout"]).splitlines() if "=" in line
        )
        if (
            raw.get("name") != unit
            or command.get("command")
            != [
                "/usr/bin/systemctl",
                "show",
                "--no-pager",
                "--property=LoadState",
                "--property=ActiveState",
                "--property=FragmentPath",
                unit,
            ]
            or command.get("returncode") != 0
            or command.get("stderr") != ""
            or fields != {"LoadState": "not-found", "ActiveState": "inactive", "FragmentPath": ""}
        ):
            raise EvidenceError(f"{node_name}: Phase 2 unit readback is invalid")
    for name, raw in zip(PHASE2_NAMES, processes, strict=True):
        if not isinstance(raw, dict) or set(raw) != {
            "name",
            "command",
            "returncode",
            "stdout",
            "stderr",
        }:
            raise EvidenceError(f"{node_name}: Phase 2 process readback is invalid")
        command = _command_result(
            {key: item for key, item in raw.items() if key != "name"},
            "Phase 2 process",
        )
        if (
            raw.get("name") != name
            or command.get("command") != ["/usr/bin/pgrep", "-f", f"(^|/){name}( |$)"]
            or command.get("returncode") != 1
            or command.get("stdout") != ""
            or command.get("stderr") != ""
        ):
            raise EvidenceError(f"{node_name}: Phase 2 process readback is invalid")
    return dict(value)


def _validate_identity(value: object, context: EvidenceContext, node_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise EvidenceError(f"{node_name}: dedicated identity readback is invalid")
    policy = context.policy.raw["identity"]
    expected = {
        "user": policy["user"],
        "uid": policy["uid"],
        "group": policy["group"],
        "gid": policy["gid"],
        "home": policy["home"],
        "shell": policy["shell"],
        "supplementary_groups": [],
        "subuid_start": policy["subid_start"],
        "subuid_count": policy["subid_count"],
        "subgid_start": policy["subid_start"],
        "subgid_count": policy["subid_count"],
        "newuidmap_setuid_root": True,
        "newgidmap_setuid_root": True,
    }
    if value != expected:
        raise EvidenceError(f"{node_name}: dedicated identity readback is invalid")
    return expected


def _validate_packages(
    value: object,
    context: EvidenceContext,
    cluster: Mapping[str, Any],
    node_name: str,
) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"source", "installed", "helpers"}:
        raise EvidenceError(f"{node_name}: package/source signature evidence is invalid")
    ubuntu = context.release.get("ubuntu")
    if not isinstance(ubuntu, dict):
        raise EvidenceError("host release Ubuntu source is invalid")
    expected_source = {
        "os_id": ubuntu["os_id"],
        "version_id": ubuntu["version_id"],
        "suite": ubuntu["suite"],
        "component": ubuntu["component"],
        "signer_fingerprint": ubuntu["signer_fingerprint"],
        "keyring_sha256": ubuntu["keyring_sha256"],
    }
    if value.get("source") != expected_source:
        raise EvidenceError(f"{node_name}: package/source signature evidence is invalid")
    debian_architecture = context.release["architecture_map"][cluster["architecture"]]
    release_packages = context.release["packages"][debian_architecture]
    expected_installed = [
        {
            "name": name,
            "version": package["version"],
            "architecture": package["architecture"],
            "filename": package["filename"],
            "size": package["size"],
            "artifact_sha256": package["sha256"],
        }
        for name, package in sorted(release_packages.items())
    ]
    installed = value.get("installed")
    if (
        not isinstance(installed, list)
        or not all(isinstance(item, dict) for item in installed)
        or sorted(installed, key=lambda item: str(item.get("name"))) != expected_installed
    ):
        raise EvidenceError(f"{node_name}: installed packages do not match signed release")
    helpers = value.get("helpers")
    expected_paths = ["/usr/bin/newgidmap", "/usr/bin/newuidmap"]
    if (
        not isinstance(helpers, list)
        or len(helpers) != 2
        or not all(isinstance(item, dict) for item in helpers)
    ):
        raise EvidenceError(f"{node_name}: UID-map helper evidence is invalid")
    sorted_helpers = sorted(helpers, key=lambda item: str(item.get("path")))
    for helper, expected_path in zip(sorted_helpers, expected_paths, strict=True):
        if (
            not isinstance(helper, dict)
            or set(helper) != {"path", "uid", "gid", "mode", "sha256", "file_capabilities"}
            or helper.get("path") != expected_path
            or helper.get("uid") != 0
            or helper.get("gid") != 0
            or helper.get("mode") != "4755"
            or not isinstance(helper.get("sha256"), str)
            or re.fullmatch(r"[a-f0-9]{64}", str(helper["sha256"])) is None
            or helper.get("file_capabilities") != []
        ):
            raise EvidenceError(f"{node_name}: UID-map helper evidence is invalid")
    return {
        "source": expected_source,
        "installed": expected_installed,
        "helpers": sorted_helpers,
    }


def _validate_kernel(
    value: object,
    context: EvidenceContext,
    node_name: str,
    maintenance_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {
        "cgroup_version",
        "controllers",
        "unprivileged_user_namespaces",
        "pidfd_open",
        "sealed_memfd",
        "clone3_into_cgroup",
        "bpffs_mounted_root_only",
        "cgroup_filesystem",
        "delegated_controllers",
        "slurm_cgroup_readback",
        "raw",
    }:
        raise EvidenceError(f"{node_name}: cgroup/kernel readback is invalid")
    raw = value.get("raw")
    if not isinstance(raw, dict) or set(raw) != {
        "unprivileged_user_namespaces",
        "pidfd_open",
        "sealed_memfd",
        "clone3_into_cgroup",
        "cgroup_mount",
        "bpffs_mount",
        "bpffs_metadata",
        "controllers",
        "delegation",
    }:
        raise EvidenceError(f"{node_name}: cgroup/kernel raw readback is invalid")
    sysctl = _command_result(
        raw.get("unprivileged_user_namespaces"), "unprivileged user namespace sysctl"
    )
    cgroup_mount_readback = _command_result(raw.get("cgroup_mount"), "cgroup v2 mount")
    bpffs_mount_readback = _command_result(raw.get("bpffs_mount"), "bpffs mount")
    if (
        sysctl.get("command")
        != ["/usr/sbin/sysctl", "--values", "kernel.unprivileged_userns_clone"]
        or sysctl.get("returncode") != 0
        or sysctl.get("stdout") != "1\n"
        or sysctl.get("stderr") != ""
        or cgroup_mount_readback.get("command")
        != [
            "/usr/bin/findmnt",
            "--json",
            "--target",
            "/sys/fs/cgroup",
            "--output",
            "TARGET,SOURCE,FSTYPE,OPTIONS",
        ]
        or cgroup_mount_readback.get("returncode") != 0
        or cgroup_mount_readback.get("stderr") != ""
        or bpffs_mount_readback.get("command")
        != [
            "/usr/bin/findmnt",
            "--json",
            "--target",
            "/sys/fs/bpf",
            "--output",
            "TARGET,SOURCE,FSTYPE,OPTIONS",
        ]
        or bpffs_mount_readback.get("returncode") != 0
        or bpffs_mount_readback.get("stderr") != ""
    ):
        raise EvidenceError(f"{node_name}: cgroup/kernel raw command readback is invalid")
    cgroup_mount = _single_mount(cgroup_mount_readback, "cgroup v2 mount")
    bpffs_mount = _single_mount(bpffs_mount_readback, "bpffs mount")
    pidfd = raw.get("pidfd_open")
    memfd = raw.get("sealed_memfd")
    clone3 = raw.get("clone3_into_cgroup")
    bpffs_metadata = raw.get("bpffs_metadata")
    controllers_readback = raw.get("controllers")
    delegation_readback = raw.get("delegation")
    if (
        not isinstance(pidfd, dict)
        or set(pidfd) != {"pid", "flags", "outcome"}
        or not isinstance(pidfd.get("pid"), int)
        or isinstance(pidfd.get("pid"), bool)
        or int(pidfd["pid"]) <= 0
        or pidfd.get("flags") != 0
        or pidfd.get("outcome") != "opened"
        or not isinstance(memfd, dict)
        or memfd
        != {"required_seals": 15, "observed_seals": 15, "outcome": "sealed"}
        or not isinstance(clone3, dict)
        or clone3
        != {
            "flags": "CLONE_INTO_CGROUP",
            "cgroup_fd": -1,
            "returncode": -1,
            "errno": errno.EBADF,
            "errno_name": "EBADF",
        }
        or bpffs_metadata
        != {"path": "/sys/fs/bpf", "uid": 0, "gid": 0, "mode": "0700"}
        or not isinstance(controllers_readback, dict)
        or set(controllers_readback) != {"path", "contents", "sha256"}
        or controllers_readback.get("path") != "/sys/fs/cgroup/cgroup.controllers"
        or not isinstance(controllers_readback.get("contents"), str)
        or controllers_readback.get("sha256")
        != _sha(str(controllers_readback.get("contents")).encode("utf-8"))
        or not isinstance(delegation_readback, dict)
        or set(delegation_readback) != {"path", "contents", "sha256"}
        or delegation_readback.get("path") != "/sys/fs/cgroup/cgroup.subtree_control"
        or not isinstance(delegation_readback.get("contents"), str)
        or delegation_readback.get("sha256")
        != _sha(str(delegation_readback.get("contents")).encode("utf-8"))
    ):
        raise EvidenceError(f"{node_name}: cgroup/kernel raw probe readback is invalid")
    required_controllers = {"cpu", "cpuset", "io", "memory", "pids"}
    booleans = (
        "unprivileged_user_namespaces",
        "pidfd_open",
        "sealed_memfd",
        "clone3_into_cgroup",
        "bpffs_mounted_root_only",
    )
    maintenance_daemon = maintenance_receipt["document"]["observations"]["daemon"]["check"]
    observed_controllers = sorted(str(controllers_readback["contents"]).split())
    observed_delegation = sorted(str(delegation_readback["contents"]).split())
    bpffs_options = str(bpffs_mount["options"]).split(",")
    if (
        value.get("cgroup_version") != 2
        or value.get("cgroup_filesystem") != "cgroup2"
        or cgroup_mount.get("target") != "/sys/fs/cgroup"
        or cgroup_mount.get("fstype") != "cgroup2"
        or bpffs_mount.get("target") != "/sys/fs/bpf"
        or bpffs_mount.get("fstype") != "bpf"
        or "mode=700" not in bpffs_options
        or not isinstance(value.get("controllers"), list)
        or not all(isinstance(item, str) for item in value["controllers"])
        or value.get("controllers") != observed_controllers
        or not required_controllers.issubset(value["controllers"])
        or value.get("delegated_controllers") != observed_delegation
        or observed_delegation
        != sorted(context.policy.raw["cgroup"]["required_delegated_controllers"])
        or any(value.get(key) is not True for key in booleans)
        or value.get("slurm_cgroup_readback") != maintenance_daemon["cgroup_config"]
    ):
        raise EvidenceError(f"{node_name}: cgroup/kernel readback is invalid")
    return dict(value)


def _validate_runtime(
    value: object,
    context: EvidenceContext,
    cluster: Mapping[str, Any],
    node_name: str,
) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {
        "release",
        "manifest_sha256",
        "binary_sha256",
        "dependency_sha256",
        "elf_dynamic_readback",
        "snapshotter",
        "network_driver",
        "rootlesskit_flags",
        "insecure_entitlements",
    }:
        raise EvidenceError(f"{node_name}: runtime/dependency digest evidence is invalid")
    runtime_policy = context.policy.raw["runtime"]
    expected_binaries = context.runtime["architectures"][cluster["architecture"]]["binaries"]
    dynamic = value.get("elf_dynamic_readback")
    release_root = Path("/opt/loom-task-builder/releases") / str(context.runtime["release"])
    dynamic_valid = isinstance(dynamic, dict) and set(dynamic) == set(expected_binaries)
    if isinstance(dynamic, dict):
        for name in sorted(expected_binaries):
            readback = _command_result(dynamic.get(name), "installed runtime ELF")
            if (
                readback.get("command")
                != ["/usr/bin/readelf", "-d", str(release_root / "bin" / name)]
                or readback.get("returncode") != 0
                or readback.get("stderr") != ""
                or "(NEEDED)" in str(readback.get("stdout"))
            ):
                dynamic_valid = False
    if (
        value.get("release") != context.runtime["release"]
        or value.get("manifest_sha256") != context.runtime_sha256
        or value.get("binary_sha256") != expected_binaries
        or value.get("dependency_sha256") != {}
        or not dynamic_valid
        or value.get("snapshotter") != runtime_policy["snapshotter"]
        or value.get("network_driver") != runtime_policy["network_driver"]
        or value.get("rootlesskit_flags") != runtime_policy["rootlesskit_flags"]
        or value.get("insecure_entitlements") != []
    ):
        raise EvidenceError(f"{node_name}: runtime/dependency digest evidence is invalid")
    return dict(value)


def _validate_storage(
    value: object,
    context: EvidenceContext,
    node_name: str,
    host_receipt: Mapping[str, Any],
    maintenance_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {
        "filesystem",
        "project_quota",
        "empty_job_root",
        "cleanup_supported",
        "mountpoint",
        "source",
        "mount_options",
        "dedicated",
        "quota",
        "raw",
    }:
        raise EvidenceError(f"{node_name}: dedicated mount/quota readback is invalid")
    raw = value.get("raw")
    if not isinstance(raw, dict) or set(raw) != {
        "findmnt",
        "lsblk",
        "jobs_root",
        "lsattr",
        "repquota",
        "cleanup",
    }:
        raise EvidenceError(f"{node_name}: dedicated mount/quota raw readback is invalid")
    findmnt = _command_result(raw.get("findmnt"), "dedicated builder mount")
    lsblk = _command_result(raw.get("lsblk"), "dedicated builder block device")
    lsattr = _command_result(raw.get("lsattr"), "project attribute")
    repquota = _command_result(raw.get("repquota"), "project quota")
    mount = _single_mount(findmnt, "dedicated builder mount")
    jobs_root = raw.get("jobs_root")
    cleanup = raw.get("cleanup")
    storage_policy = context.policy.raw["storage"]
    expected_cleanup = _event_by_type(
        maintenance_receipt["document"]["events"], "smoke_cleaned"
    )["data"]["cleanup"]
    if (
        findmnt.get("command")
        != [
            "/usr/bin/findmnt",
            "--json",
            "--target",
            str(storage_policy["mountpoint"]),
            "--output",
            "TARGET,SOURCE,FSTYPE,OPTIONS",
        ]
        or findmnt.get("returncode") != 0
        or findmnt.get("stderr") != ""
        or lsblk.get("command")
        != [
            "/usr/bin/lsblk",
            "--noheadings",
            "--output",
            "TYPE",
            str(mount.get("source")),
        ]
        or lsblk.get("returncode") != 0
        or lsblk.get("stderr") != ""
        or lsblk.get("stdout", "").strip() not in {"disk", "part", "lvm"}
        or lsattr.get("command")
        != ["/usr/bin/lsattr", "-pd", str(storage_policy["root"])]
        or lsattr.get("returncode") != 0
        or lsattr.get("stderr") != ""
        or repquota.get("command")
        != [
            "/usr/sbin/repquota",
            "-v",
            "-n",
            "-p",
            "-P",
            "-O",
            "csv",
            str(storage_policy["mountpoint"]),
        ]
        or repquota.get("returncode") != 0
        or repquota.get("stderr") != ""
        or jobs_root
        != {
            "path": str(storage_policy["root"]),
            "uid": 993,
            "gid": 980,
            "mode": "0700",
            "entries": [],
        }
        or cleanup != expected_cleanup
    ):
        raise EvidenceError(f"{node_name}: dedicated mount/quota raw readback is invalid")
    source = value.get("source")
    mount_options = value.get("mount_options")
    quota = _validate_quota(value.get("quota"), context, label=node_name)
    observed_quota = _quota_from_readbacks(context, jobs_root, lsattr, repquota)
    receipted_quota = host_receipt["document"]["post_state"]["quota_state"]
    if (
        value.get("filesystem") != storage_policy["site_filesystem"]
        or value.get("project_quota") is not True
        or value.get("empty_job_root") is not True
        or value.get("cleanup_supported") is not True
        or value.get("mountpoint") != storage_policy["mountpoint"]
        or mount.get("target") != value.get("mountpoint")
        or mount.get("fstype") != value.get("filesystem")
        or sorted(str(mount.get("options", "")).split(",")) != mount_options
        or not isinstance(source, str)
        or not source.startswith("/dev/")
        or source.startswith("/dev/loop")
        or not isinstance(mount_options, list)
        or not all(isinstance(item, str) for item in mount_options)
        or not set(storage_policy["required_mount_options"]).issubset(mount_options)
        or value.get("dedicated") is not True
        or quota != observed_quota
        or quota != receipted_quota
    ):
        raise EvidenceError(f"{node_name}: dedicated mount/quota readback is invalid")
    return dict(value)


def _metadata(context: EvidenceContext, observed_at: datetime) -> dict[str, Any]:
    return {
        "observed_at": _utc_text(observed_at),
        "candidate_sha256": context.candidate_sha256,
        "policy_version": context.policy.raw["policy_version"],
        "policy_sha256": context.policy.digest,
        "policy_file_sha256": context.policy_file_sha256,
        "release_name": context.release["release"],
        "release_sha256": context.release_sha256,
        "runtime_manifest_sha256": context.runtime_sha256,
        **context.authority_binding.as_dict(),
    }


def _controller_cluster(
    context: EvidenceContext,
    cluster: Mapping[str, Any],
    identity: Mapping[str, Any],
    receipt: Mapping[str, Any],
    observed_at: datetime,
) -> dict[str, Any]:
    state = receipt["document"]["post_state"]
    qos = dict(state["qos"])
    qos.pop("priority", None)
    association = dict(state["association"])
    association.pop("cluster", None)
    legacy = json.loads(json.dumps(state["legacy"]))
    legacy["reservation"].pop("node_count", None)
    cgroup = context.policy.raw["cgroup"]
    return {
        "id": cluster["id"],
        "slurm_cluster": cluster["slurm_cluster"],
        "controller": cluster["controller"],
        "architecture": cluster["architecture"],
        **_metadata(context, observed_at),
        "controller_identity": dict(identity),
        "slurm": {
            "task_plugin": cgroup["task_plugin"],
            "proctrack_type": cgroup["proctrack_type"],
            "cgroup_version": cgroup["version"],
            "constrain_cores": cgroup["constrain_cores"],
            "constrain_ram_space": cgroup["constrain_ram_space"],
            "constrain_swap_space": cgroup["constrain_swap_space"],
            "constrain_devices": cgroup["constrain_devices"],
            "trial_partition": {
                "name": cluster["trial_partition"],
                "priority_tier": cluster["trial_priority_tier"],
                "nodes": cluster["builder_nodes"],
            },
            "builder_partition": {
                "name": cluster["builder_partition"],
                "priority_tier": cluster["builder_priority_tier"],
                "nodes": cluster["builder_nodes"],
            },
            "qos": qos,
            "association": association,
            "legacy_builder": legacy,
        },
        "slurm_receipt": dict(receipt),
    }


def collect_controller(
    candidate_root: Path,
    policy_path: Path,
    release_path: Path,
    cluster_id: str,
    slurm_receipt_path: Path,
    output: Path,
    *,
    observation: Mapping[str, Any] | None = None,
    observed_at: datetime | None = None,
    required_owner: int | None = None,
) -> dict[str, Any]:
    """Collect one controller fragment from local state and a canonical Slurm receipt."""
    context = _load_context(candidate_root, policy_path, release_path)
    cluster = _cluster(context, cluster_id)
    owner = os.geteuid() if required_owner is None else required_owner
    wrapper = _receipt_wrapper_from_path(slurm_receipt_path, "Slurm receipt", owner)
    receipt = _validate_slurm_receipt(wrapper, context, cluster)
    local = (
        _system_controller_observation(context, cluster)
        if observation is None
        else dict(observation)
    )
    try:
        conformance._scan_for_secrets(local)
    except conformance.ConformanceError as exc:
        raise EvidenceError(str(exc)) from exc
    identity = _validate_controller_observation(local, context, cluster_id)
    timestamp = observed_at or datetime.now(UTC)
    fragment = {
        "schema": CONTROLLER_SCHEMA,
        **_metadata(context, timestamp),
        "cluster": _controller_cluster(context, cluster, identity, receipt, timestamp),
    }
    _write_owner_output(output, fragment)
    return fragment


def collect_node(
    candidate_root: Path,
    policy_path: Path,
    release_path: Path,
    cluster_id: str,
    node_name: str,
    host_receipt_path: Path,
    maintenance_receipt_path: Path,
    output: Path,
    *,
    observation: Mapping[str, Any] | None = None,
    observed_at: datetime | None = None,
    required_owner: int | None = None,
) -> dict[str, Any]:
    """Collect one node fragment from local state and durable producer receipts."""
    context = _load_context(candidate_root, policy_path, release_path)
    cluster = _cluster(context, cluster_id)
    if node_name not in cluster["builder_nodes"]:
        raise EvidenceError("node is outside the exact policy inventory")
    owner = os.geteuid() if required_owner is None else required_owner
    host_wrapper = _receipt_wrapper_from_path(host_receipt_path, "host receipt", owner)
    host_receipt = _validate_host_receipt(
        host_wrapper,
        context,
        cluster,
        node_name,
    )
    maintenance_wrapper = _receipt_wrapper_from_path(
        maintenance_receipt_path,
        "maintenance receipt",
        owner,
    )
    maintenance_receipt = _validate_maintenance_receipt(
        maintenance_wrapper,
        context,
        cluster,
        node_name,
        host_receipt,
    )
    local = (
        _system_node_observation(context, cluster, node_name, host_receipt, maintenance_receipt)
        if observation is None
        else dict(observation)
    )
    try:
        conformance._scan_for_secrets(local)
    except conformance.ConformanceError as exc:
        raise EvidenceError(str(exc)) from exc
    if set(local) != {
        "slurm_identity",
        "identity",
        "packages",
        "kernel",
        "runtime",
        "storage",
        "forbidden_paths_present",
        "node_guard",
    }:
        raise EvidenceError(f"{node_name}: node readback fields are invalid")
    slurm_identity = _validate_slurm_identity(local["slurm_identity"], node_name)
    identity = _validate_identity(local["identity"], context, node_name)
    packages = _validate_packages(local["packages"], context, cluster, node_name)
    kernel = _validate_kernel(local["kernel"], context, node_name, maintenance_receipt)
    runtime = _validate_runtime(local["runtime"], context, cluster, node_name)
    storage = _validate_storage(
        local["storage"], context, node_name, host_receipt, maintenance_receipt
    )
    node_guard = _validate_phase2_absence(local["node_guard"], node_name)
    forbidden = local["forbidden_paths_present"]
    if forbidden != []:
        raise EvidenceError(f"{node_name}: forbidden host path is visible")
    network = {
        key: context.policy.raw["network"][key]
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
    timestamp = observed_at or datetime.now(UTC)
    node = {
        "name": node_name,
        "architecture": cluster["architecture"],
        **_metadata(context, timestamp),
        "slurm_identity": slurm_identity,
        "identity": identity,
        "packages": packages,
        "kernel": kernel,
        "runtime": runtime,
        "storage": storage,
        "network": network,
        "forbidden_paths_present": [],
        "node_guard": node_guard,
        "host_receipt": host_receipt,
        "maintenance_receipt": maintenance_receipt,
    }
    fragment = {
        "schema": NODE_SCHEMA,
        **_metadata(context, timestamp),
        "cluster_id": cluster_id,
        "node": node,
    }
    _write_owner_output(output, fragment)
    return fragment


def _load_fragment(path: Path, label: str, owner: int) -> dict[str, Any]:
    payload = _read_regular(
        path,
        label,
        maximum=MAX_OUTPUT_BYTES,
        required_owner=owner,
        required_mode=0o600,
    )
    value = _json_object(payload, label)
    if payload != _canonical(value) + b"\n":
        raise EvidenceError(f"{label} is not canonical")
    try:
        conformance._scan_for_secrets(value)
    except conformance.ConformanceError as exc:
        raise EvidenceError(str(exc)) from exc
    return value


def _validate_metadata(
    value: Mapping[str, Any],
    context: EvidenceContext,
    collected_at: datetime,
) -> datetime:
    expected = {
        "candidate_sha256": context.candidate_sha256,
        "policy_version": context.policy.raw["policy_version"],
        "policy_sha256": context.policy.digest,
        "policy_file_sha256": context.policy_file_sha256,
        "release_name": context.release["release"],
        "release_sha256": context.release_sha256,
        "runtime_manifest_sha256": context.runtime_sha256,
        **context.authority_binding.as_dict(),
    }
    labels = {
        "candidate_sha256": "candidate digest",
        "policy_version": "policy version",
        "policy_sha256": "policy digest",
        "policy_file_sha256": "policy digest",
        "release_name": "release name",
        "release_sha256": "release digest",
        "runtime_manifest_sha256": "release digest",
        "authority_manifest_sha256": "authority manifest digest",
        "authority_component_digests": "authority component digests",
    }
    for key, expected_value in expected.items():
        if value.get(key) != expected_value:
            raise EvidenceError(f"fragment {labels[key]} does not match the assembly input")
    observed = _parse_time(value.get("observed_at"), "observation timestamp")
    age = (collected_at.astimezone(UTC) - observed).total_seconds()
    if age > 3600 or age < -300:
        raise EvidenceError("fragment observation freshness window is invalid")
    return observed


def _validate_controller_fragment(
    value: Mapping[str, Any],
    context: EvidenceContext,
    collected_at: datetime,
) -> dict[str, Any]:
    if value.get("schema") != CONTROLLER_SCHEMA:
        raise EvidenceError("controller fragment schema is invalid")
    observed = _validate_metadata(value, context, collected_at)
    raw_cluster = value.get("cluster")
    if not isinstance(raw_cluster, dict):
        raise EvidenceError("controller fragment cluster is invalid")
    cluster_id = raw_cluster.get("id")
    if not isinstance(cluster_id, str):
        raise EvidenceError("controller fragment cluster identity is invalid")
    cluster = _cluster(context, cluster_id)
    nested_observed = _validate_metadata(raw_cluster, context, collected_at)
    if nested_observed != observed:
        raise EvidenceError("controller observation timestamps do not match")
    identity_raw = raw_cluster.get("controller_identity")
    if not isinstance(identity_raw, dict):
        raise EvidenceError(f"{cluster_id}: controller identity readback is invalid")
    identity = _validate_controller_observation(identity_raw, context, cluster_id)
    receipt = _validate_slurm_receipt(raw_cluster.get("slurm_receipt"), context, cluster)
    expected = _controller_cluster(context, cluster, identity, receipt, observed)
    if raw_cluster != expected:
        raise EvidenceError(f"{cluster_id}: controller fragment facts are inconsistent")
    return expected


def _validate_node_fragment(
    value: Mapping[str, Any],
    context: EvidenceContext,
    collected_at: datetime,
) -> tuple[str, dict[str, Any]]:
    if value.get("schema") != NODE_SCHEMA:
        raise EvidenceError("node fragment schema is invalid")
    observed = _validate_metadata(value, context, collected_at)
    cluster_id = value.get("cluster_id")
    raw_node = value.get("node")
    if not isinstance(cluster_id, str) or not isinstance(raw_node, dict):
        raise EvidenceError("node fragment binding is invalid")
    cluster = _cluster(context, cluster_id)
    node_name = raw_node.get("name")
    if not isinstance(node_name, str) or node_name not in cluster["builder_nodes"]:
        raise EvidenceError("node fragment is outside the exact policy inventory")
    nested_observed = _validate_metadata(raw_node, context, collected_at)
    if nested_observed != observed:
        raise EvidenceError(f"{node_name}: observation timestamps do not match")
    host_receipt = _validate_host_receipt(raw_node.get("host_receipt"), context, cluster, node_name)
    maintenance_receipt = _validate_maintenance_receipt(
        raw_node.get("maintenance_receipt"),
        context,
        cluster,
        node_name,
        host_receipt,
    )
    slurm_identity = _validate_slurm_identity(raw_node.get("slurm_identity"), node_name)
    identity = _validate_identity(raw_node.get("identity"), context, node_name)
    packages = _validate_packages(raw_node.get("packages"), context, cluster, node_name)
    kernel = _validate_kernel(raw_node.get("kernel"), context, node_name, maintenance_receipt)
    runtime = _validate_runtime(raw_node.get("runtime"), context, cluster, node_name)
    storage = _validate_storage(
        raw_node.get("storage"),
        context,
        node_name,
        host_receipt,
        maintenance_receipt,
    )
    node_guard = _validate_phase2_absence(raw_node.get("node_guard"), node_name)
    network = {
        key: context.policy.raw["network"][key]
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
    expected = {
        "name": node_name,
        "architecture": cluster["architecture"],
        **_metadata(context, observed),
        "slurm_identity": slurm_identity,
        "identity": identity,
        "packages": packages,
        "kernel": kernel,
        "runtime": runtime,
        "storage": storage,
        "network": network,
        "forbidden_paths_present": [],
        "node_guard": node_guard,
        "host_receipt": host_receipt,
        "maintenance_receipt": maintenance_receipt,
    }
    if raw_node != expected:
        raise EvidenceError(f"{node_name}: node fragment facts are inconsistent")
    return cluster_id, expected


def assemble(
    candidate_root: Path,
    policy_path: Path,
    release_path: Path,
    controller_paths: Sequence[Path],
    node_paths: Sequence[Path],
    output: Path,
    *,
    collected_at: datetime | None = None,
    required_owner: int | None = None,
) -> dict[str, Any]:
    """Assemble validated local fragments without SSH, mutation, or live collection."""
    context = _load_context(candidate_root, policy_path, release_path)
    owner = os.geteuid() if required_owner is None else required_owner
    timestamp = collected_at or datetime.now(UTC)
    if timestamp.tzinfo is None:
        raise EvidenceError("assembly timestamp must be timezone-qualified")
    controllers: dict[str, dict[str, Any]] = {}
    for path in controller_paths:
        cluster = _validate_controller_fragment(
            _load_fragment(path, "controller fragment", owner),
            context,
            timestamp,
        )
        cluster_id = str(cluster["id"])
        if cluster_id in controllers:
            raise EvidenceError("duplicate controller evidence is forbidden")
        controllers[cluster_id] = cluster
    policy_clusters = {str(cluster["id"]): cluster for cluster in context.policy.raw["clusters"]}
    if set(controllers) != set(policy_clusters):
        raise EvidenceError("controller evidence set does not match policy")
    nodes: dict[str, dict[str, dict[str, Any]]] = {cluster_id: {} for cluster_id in policy_clusters}
    for path in node_paths:
        cluster_id, node = _validate_node_fragment(
            _load_fragment(path, "node fragment", owner),
            context,
            timestamp,
        )
        node_name = str(node["name"])
        if node_name in nodes[cluster_id]:
            raise EvidenceError(f"{cluster_id}: duplicate node evidence is forbidden")
        nodes[cluster_id][node_name] = node
    for cluster_id, policy_cluster in policy_clusters.items():
        if set(nodes[cluster_id]) != set(policy_cluster["builder_nodes"]):
            raise EvidenceError(f"{cluster_id}: node evidence set does not match policy")
    clusters: list[dict[str, Any]] = []
    for cluster_id in sorted(controllers):
        cluster = dict(controllers[cluster_id])
        cluster["nodes"] = [nodes[cluster_id][name] for name in sorted(nodes[cluster_id])]
        clusters.append(cluster)
    result = {
        "schema": ASSEMBLED_SCHEMA,
        "schema_version": 1,
        "collected_at": _utc_text(timestamp),
        "candidate_sha256": context.candidate_sha256,
        "policy_version": context.policy.raw["policy_version"],
        "policy_sha256": context.policy.digest,
        "policy_file_sha256": context.policy_file_sha256,
        "release_name": context.release["release"],
        "release_sha256": context.release_sha256,
        "runtime_manifest_sha256": context.runtime_sha256,
        **context.authority_binding.as_dict(),
        "production_certification_allowed": False,
        "certified_nodes": [],
        "blockers": [INERT_BLOCKER],
        "control_plane_services": context.policy.raw["control_plane_services"],
        "clusters": clusters,
    }
    try:
        conformance._scan_for_secrets(result)
    except conformance.ConformanceError as exc:
        raise EvidenceError(str(exc)) from exc
    _write_owner_output(output, result)
    return result


def _run(command: Sequence[str], label: str) -> CommandResult:
    try:
        completed = subprocess.run(
            list(command),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            check=False,
            timeout=30,
            env={"LC_ALL": "C", "PATH": "/usr/bin:/bin:/usr/sbin:/sbin"},
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise EvidenceError(f"{label} command is unavailable") from exc
    if len(completed.stdout) > 1024 * 1024 or len(completed.stderr) > 1024 * 1024:
        raise EvidenceError(f"{label} command output exceeds its limit")
    try:
        stdout = completed.stdout.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise EvidenceError(f"{label} command output is not UTF-8") from exc
    return CommandResult(
        completed.returncode,
        stdout,
        completed.stderr.decode("utf-8", errors="replace"),
    )


def _required_command(command: Sequence[str], label: str) -> str:
    result = _run(command, label)
    if result.returncode != 0:
        raise EvidenceError(f"{label} readback failed")
    return result.stdout


def _command_readback(command: Sequence[str], label: str) -> dict[str, Any]:
    result = _run(command, label)
    return {
        "command": list(command),
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def _required_readback(command: Sequence[str], label: str) -> dict[str, Any]:
    readback = _command_readback(command, label)
    if readback["returncode"] != 0 or readback["stderr"] != "":
        raise EvidenceError(f"{label} readback failed")
    return readback


def _file_readback(path: Path, label: str) -> dict[str, Any]:
    payload = _read_regular(path, label, maximum=4096)
    try:
        contents = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise EvidenceError(f"{label} is not UTF-8") from exc
    return {"path": str(path), "contents": contents, "sha256": _sha(payload)}


def _single_mount(readback: Mapping[str, Any], label: str) -> dict[str, Any]:
    try:
        value = json.loads(str(readback["stdout"]))
        filesystems = value["filesystems"]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise EvidenceError(f"{label} readback is invalid") from exc
    if not isinstance(filesystems, list) or len(filesystems) != 1:
        raise EvidenceError(f"{label} readback is ambiguous")
    mount = filesystems[0]
    if not isinstance(mount, dict) or set(mount) != {"target", "source", "fstype", "options"}:
        raise EvidenceError(f"{label} readback is invalid")
    return dict(mount)


def _directory_entries(path: Path) -> list[str]:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError as exc:
        raise EvidenceError("builder jobs root is unavailable") from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISDIR(metadata.st_mode):
            raise EvidenceError("builder jobs root type is unsafe")
        entries = os.listdir(descriptor)
    except OSError as exc:
        raise EvidenceError("builder jobs root is unavailable") from exc
    finally:
        os.close(descriptor)
    if len(entries) > 1024 or any(
        not isinstance(entry, str) or not entry or len(entry.encode("utf-8")) > 255
        for entry in entries
    ):
        raise EvidenceError("builder jobs root listing is unsafe")
    return sorted(entries)


def _probe_pidfd_open() -> dict[str, Any]:
    pid = os.getpid()
    try:
        descriptor = os.pidfd_open(pid, 0)
    except (AttributeError, OSError) as exc:
        raise EvidenceError("pidfd_open probe failed") from exc
    os.close(descriptor)
    return {"pid": pid, "flags": 0, "outcome": "opened"}


def _probe_sealed_memfd() -> dict[str, Any]:
    required_seals = (
        fcntl.F_SEAL_SEAL | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_GROW | fcntl.F_SEAL_WRITE
    )
    try:
        descriptor = os.memfd_create(
            "loom-task-builder-probe",
            os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING,
        )
        try:
            fcntl.fcntl(descriptor, fcntl.F_ADD_SEALS, required_seals)
            observed_seals = int(fcntl.fcntl(descriptor, fcntl.F_GET_SEALS))
        finally:
            os.close(descriptor)
    except (AttributeError, OSError) as exc:
        raise EvidenceError("sealed memfd probe failed") from exc
    if observed_seals != required_seals:
        raise EvidenceError("sealed memfd probe did not retain the required seals")
    return {
        "required_seals": required_seals,
        "observed_seals": observed_seals,
        "outcome": "sealed",
    }


def _probe_clone3_into_cgroup() -> dict[str, Any]:
    class CloneArgs(ctypes.Structure):
        _fields_ = [(name, ctypes.c_uint64) for name in (
            "flags",
            "pidfd",
            "child_tid",
            "parent_tid",
            "exit_signal",
            "stack",
            "stack_size",
            "tls",
            "set_tid",
            "set_tid_size",
            "cgroup",
        )]

    clone_into_cgroup = 1 << 33
    arguments = CloneArgs()
    arguments.flags = clone_into_cgroup
    arguments.cgroup = (1 << 64) - 1
    libc = ctypes.CDLL(None, use_errno=True)
    ctypes.set_errno(0)
    result = int(libc.syscall(435, ctypes.byref(arguments), ctypes.sizeof(arguments)))
    observed_errno = ctypes.get_errno()
    if result != -1 or observed_errno != errno.EBADF:
        raise EvidenceError("clone3 CLONE_INTO_CGROUP probe did not return EBADF")
    return {
        "flags": "CLONE_INTO_CGROUP",
        "cgroup_fd": -1,
        "returncode": result,
        "errno": observed_errno,
        "errno_name": errno.errorcode[observed_errno],
    }


def _identity_from_system(context: EvidenceContext, *, node: bool) -> dict[str, Any]:
    identity = context.policy.raw["identity"]
    user = str(identity["user"])
    group = str(identity["group"])
    passwd_rows = _required_command(
        ("/usr/bin/getent", "passwd", user), "builder identity"
    ).splitlines()
    group_rows = _required_command(
        ("/usr/bin/getent", "group", group), "builder group"
    ).splitlines()
    if len(passwd_rows) != 1 or len(group_rows) != 1:
        raise EvidenceError("builder identity readback is ambiguous")
    passwd = passwd_rows[0].split(":")
    group_row = group_rows[0].split(":")
    if len(passwd) != 7 or len(group_row) != 4:
        raise EvidenceError("builder identity readback is invalid")
    try:
        uid = int(passwd[2])
        primary_gid = int(passwd[3])
        gid = int(group_row[2])
    except ValueError as exc:
        raise EvidenceError("builder identity readback is invalid") from exc
    groups = _required_command(
        ("/usr/bin/id", "-G", "-n", user), "builder group membership"
    ).split()
    supplementary = sorted(item for item in groups if item != group)
    base = {
        "user": passwd[0],
        "uid": uid,
        "group": group_row[0],
        "gid": gid,
        "supplementary_groups": supplementary,
    }
    if primary_gid != gid:
        raise EvidenceError("builder primary group readback is invalid")
    if not node:
        return {**base, "home": passwd[5], "shell": passwd[6]}
    subids: dict[str, tuple[int, int]] = {}
    for name in ("subuid", "subgid"):
        payload = _read_regular(Path("/etc") / name, name + " database")
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise EvidenceError(f"{name} database is invalid") from exc
        matches = [line.split(":") for line in text.splitlines() if line.startswith(user + ":")]
        if len(matches) != 1 or len(matches[0]) != 3:
            raise EvidenceError(f"{name} mapping is not exact")
        try:
            subids[name] = (int(matches[0][1]), int(matches[0][2]))
        except ValueError as exc:
            raise EvidenceError(f"{name} mapping is invalid") from exc
    return {
        **base,
        "home": passwd[5],
        "shell": passwd[6],
        "subuid_start": subids["subuid"][0],
        "subuid_count": subids["subuid"][1],
        "subgid_start": subids["subgid"][0],
        "subgid_count": subids["subgid"][1],
        "newuidmap_setuid_root": True,
        "newgidmap_setuid_root": True,
    }


def _system_controller_observation(
    context: EvidenceContext,
    cluster: Mapping[str, Any],
) -> dict[str, Any]:
    hostname = _required_command(("/bin/hostname", "--short"), "controller hostname").strip()
    architecture = _required_command(("/usr/bin/uname", "-m"), "controller architecture").strip()
    if (
        hostname.casefold() != str(cluster["controller"]).casefold()
        or architecture != cluster["architecture"]
    ):
        raise EvidenceError("local controller identity does not match policy")
    return _identity_from_system(context, node=False)


def _helper_observations() -> list[dict[str, Any]]:
    helpers: list[dict[str, Any]] = []
    for path in (Path("/usr/bin/newgidmap"), Path("/usr/bin/newuidmap")):
        payload = _read_regular(path, "UID-map helper", maximum=16 * 1024 * 1024)
        metadata = path.stat(follow_symlinks=False)
        capabilities = _required_command(
            ("/usr/sbin/getcap", "-n", str(path)), "UID-map helper capabilities"
        ).strip()
        helpers.append(
            {
                "path": str(path),
                "uid": metadata.st_uid,
                "gid": metadata.st_gid,
                "mode": f"{stat.S_IMODE(metadata.st_mode):04o}",
                "sha256": _sha(payload),
                "file_capabilities": [] if not capabilities else [capabilities],
            }
        )
    return helpers


def _runtime_observation(
    context: EvidenceContext,
    cluster: Mapping[str, Any],
) -> dict[str, Any]:
    release_name = str(context.runtime["release"])
    release_root = Path("/opt/loom-task-builder/releases") / release_name
    receipt_path = release_root / "receipt.json"
    receipt = _json_object(
        _read_regular(receipt_path, "installed runtime receipt"),
        "installed runtime receipt",
    )
    binaries = context.runtime["architectures"][cluster["architecture"]]["binaries"]
    expected_receipt = {
        "schema": "loom.task-image-builder-installed-runtime/v1",
        "release": release_name,
        "architecture": cluster["architecture"],
        "manifest_sha256": context.runtime_sha256,
        "binary_sha256": binaries,
    }
    if receipt != expected_receipt:
        raise EvidenceError("installed runtime receipt is invalid")
    elf_dynamic_readback: dict[str, dict[str, Any]] = {}
    for name, digest in binaries.items():
        binary_path = release_root / "bin" / name
        if (
            _sha(
                _read_regular(
                    binary_path,
                    "installed runtime binary",
                    maximum=1024 * 1024 * 1024,
                )
            )
            != digest
        ):
            raise EvidenceError("installed runtime binary digest is invalid")
        result = _run(("/usr/bin/readelf", "-d", str(binary_path)), "installed runtime ELF")
        readback = {
            "command": ["/usr/bin/readelf", "-d", str(binary_path)],
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
        if result.returncode != 0 or result.stderr or "(NEEDED)" in result.stdout:
            raise EvidenceError("installed runtime ELF dependency closure is invalid")
        elf_dynamic_readback[name] = readback
    runtime_policy = context.policy.raw["runtime"]
    return {
        "release": release_name,
        "manifest_sha256": context.runtime_sha256,
        "binary_sha256": binaries,
        "dependency_sha256": {},
        "elf_dynamic_readback": elf_dynamic_readback,
        "snapshotter": runtime_policy["snapshotter"],
        "network_driver": runtime_policy["network_driver"],
        "rootlesskit_flags": runtime_policy["rootlesskit_flags"],
        "insecure_entitlements": [],
    }


def _slurm_identity_observation(node_name: str) -> dict[str, Any]:
    command = ("/usr/bin/scontrol", "show", "node", node_name, "-o")
    result = _run(command, "Slurm node identity")
    if result.returncode != 0 or result.stderr:
        raise EvidenceError("Slurm node identity readback failed")
    payload = result.stdout
    fields = dict(token.split("=", 1) for token in payload.split() if "=" in token)
    if fields.get("NodeName") != node_name or not fields.get("NodeAddr"):
        raise EvidenceError("Slurm node identity readback is invalid")
    short = _required_command(("/bin/hostname", "--short"), "local hostname").strip()
    fqdn = _required_command(("/bin/hostname", "--fqdn"), "local hostname").strip()
    local_addresses = _required_command(
        ("/bin/hostname", "--all-ip-addresses"), "local addresses"
    ).split()
    node_addr = str(fields["NodeAddr"])
    try:
        literal = ipaddress.ip_address(node_addr)
    except ValueError:
        literal = None
    try:
        resolved = (
            [str(literal)]
            if literal is not None
            else sorted(
                {
                    item[4][0]
                    for item in socket.getaddrinfo(node_addr, None)
                    if not ipaddress.ip_address(item[4][0]).is_loopback
                }
            )
        )
    except (OSError, ValueError) as exc:
        raise EvidenceError("Slurm NodeAddr cannot be resolved safely") from exc
    return {
        "node_name": node_name,
        "node_hostname": fields.get("NodeHostName") or short,
        "node_addr": node_addr,
        "resolved_addresses": resolved,
        "resolution": {"query": node_addr, "addresses": resolved},
        "local_hostnames": sorted({short, fqdn}),
        "local_addresses": sorted(set(local_addresses)),
        "readback": {
            "command": list(command),
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        },
    }


def _phase2_absence_observation() -> dict[str, Any]:
    artifacts: list[dict[str, Any]] = []
    for path in PHASE2_ARTIFACT_PATHS:
        try:
            os.lstat(path)
        except FileNotFoundError:
            present = False
        except OSError as exc:
            raise EvidenceError("Phase 2 artifact readback is unavailable") from exc
        else:
            present = True
        artifacts.append({"path": str(path), "present": present})
    units: list[dict[str, Any]] = []
    processes: list[dict[str, Any]] = []
    for name in PHASE2_NAMES:
        unit = f"{name}.service"
        unit_command = (
            "/usr/bin/systemctl",
            "show",
            "--no-pager",
            "--property=LoadState",
            "--property=ActiveState",
            "--property=FragmentPath",
            unit,
        )
        unit_result = _run(unit_command, "Phase 2 unit")
        units.append(
            {
                "name": unit,
                "command": list(unit_command),
                "returncode": unit_result.returncode,
                "stdout": unit_result.stdout,
                "stderr": unit_result.stderr,
            }
        )
        process_command = ("/usr/bin/pgrep", "-f", f"(^|/){name}( |$)")
        process_result = _run(process_command, "Phase 2 process")
        processes.append(
            {
                "name": name,
                "command": list(process_command),
                "returncode": process_result.returncode,
                "stdout": process_result.stdout,
                "stderr": process_result.stderr,
            }
        )
    observed = {
        "installed": any(item["present"] for item in artifacts),
        "active": any(item["returncode"] == 0 for item in processes),
        "artifacts": artifacts,
        "unit_readback": units,
        "process_readback": processes,
    }
    return observed


def _package_observation(
    context: EvidenceContext,
    cluster: Mapping[str, Any],
    host_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    ubuntu = context.release["ubuntu"]
    debian_arch = context.release["architecture_map"][cluster["architecture"]]
    release_packages = context.release["packages"][debian_arch]
    installed_versions = host_receipt["document"]["post_state"]["packages"]
    installed: list[dict[str, Any]] = []
    for name, package in sorted(release_packages.items()):
        if installed_versions.get(name) != package["version"]:
            raise EvidenceError("installed package receipt does not match release")
        installed.append(
            {
                "name": name,
                "version": package["version"],
                "architecture": package["architecture"],
                "filename": package["filename"],
                "size": package["size"],
                "artifact_sha256": package["sha256"],
            }
        )
    return {
        "source": {
            "os_id": ubuntu["os_id"],
            "version_id": ubuntu["version_id"],
            "suite": ubuntu["suite"],
            "component": ubuntu["component"],
            "signer_fingerprint": ubuntu["signer_fingerprint"],
            "keyring_sha256": ubuntu["keyring_sha256"],
        },
        "installed": installed,
        "helpers": _helper_observations(),
    }


def _quota_from_readbacks(
    context: EvidenceContext,
    jobs_root: Mapping[str, Any],
    lsattr: Mapping[str, Any],
    repquota: Mapping[str, Any],
) -> dict[str, Any]:
    storage = context.policy.raw["storage"]
    fields = str(lsattr["stdout"]).strip().split(maxsplit=2)
    if len(fields) != 3 or fields[2] != str(storage["root"]):
        raise EvidenceError("project attribute readback is invalid")
    try:
        project_id = int(fields[0])
    except ValueError as exc:
        raise EvidenceError("project attribute readback is invalid") from exc
    rows = list(csv.reader(str(repquota["stdout"]).splitlines()))
    expected_header = [
        "Project",
        "BlockStatus",
        "FileStatus",
        "BlockUsed",
        "BlockSoftLimit",
        "BlockHardLimit",
        "BlockGrace",
        "FileUsed",
        "FileSoftLimit",
        "FileHardLimit",
        "FileGrace",
    ]
    if not rows or rows[0] != expected_header:
        raise EvidenceError("project quota CSV header is invalid")
    matches: list[list[str]] = []
    for row in rows[1:]:
        if not row:
            continue
        if len(row) != len(expected_header):
            raise EvidenceError("project quota CSV row is invalid")
        try:
            row_project = int(row[0])
        except ValueError as exc:
            raise EvidenceError("project quota CSV row is invalid") from exc
        if row_project == storage["project_id"]:
            matches.append(row)
    if len(matches) != 1:
        raise EvidenceError("project quota readback is incomplete or ambiguous")
    try:
        quota_values = [int(matches[0][index]) for index in (3, 4, 5, 7, 8, 9)]
    except ValueError as exc:
        raise EvidenceError("project quota CSV row is invalid") from exc
    if any(value < 0 for value in quota_values):
        raise EvidenceError("project quota CSV row is invalid")
    return {
        "storage_root_exists": True,
        "storage_root_uid": jobs_root["uid"],
        "storage_root_gid": jobs_root["gid"],
        "storage_root_mode": int(str(jobs_root["mode"]), 8),
        "storage_root_entries": jobs_root["entries"],
        "project_id": project_id,
        "project_inherit": "P" in fields[1],
        "block_used": quota_values[0],
        "block_soft_limit": quota_values[1],
        "block_hard_limit": quota_values[2],
        "inode_used": quota_values[3],
        "inode_soft_limit": quota_values[4],
        "inode_hard_limit": quota_values[5],
    }


def _storage_observation(
    context: EvidenceContext,
    host_receipt: Mapping[str, Any],
    maintenance_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    storage = context.policy.raw["storage"]
    mountpoint = str(storage["mountpoint"])
    root = Path(str(storage["root"]))
    findmnt_command = (
        "/usr/bin/findmnt",
        "--json",
        "--target",
        mountpoint,
        "--output",
        "TARGET,SOURCE,FSTYPE,OPTIONS",
    )
    findmnt = _required_readback(findmnt_command, "dedicated builder mount")
    mount = _single_mount(findmnt, "dedicated builder mount")
    source = str(mount["source"])
    lsblk_command = ("/usr/bin/lsblk", "--noheadings", "--output", "TYPE", source)
    lsblk = _required_readback(lsblk_command, "dedicated builder block device")
    try:
        metadata = os.lstat(root)
    except OSError as exc:
        raise EvidenceError("builder jobs root is unavailable") from exc
    if not stat.S_ISDIR(metadata.st_mode):
        raise EvidenceError("builder jobs root type is unsafe")
    jobs_root = {
        "path": str(root),
        "uid": metadata.st_uid,
        "gid": metadata.st_gid,
        "mode": f"{stat.S_IMODE(metadata.st_mode):04o}",
        "entries": _directory_entries(root),
    }
    lsattr_command = ("/usr/bin/lsattr", "-pd", str(root))
    lsattr = _required_readback(lsattr_command, "project attribute")
    repquota_command = (
        "/usr/sbin/repquota",
        "-v",
        "-n",
        "-p",
        "-P",
        "-O",
        "csv",
        mountpoint,
    )
    repquota = _required_readback(repquota_command, "project quota")
    cleanup_event = _event_by_type(
        maintenance_receipt["document"]["events"], "smoke_cleaned"
    )["data"]
    cleanup = cleanup_event.get("cleanup") if isinstance(cleanup_event, Mapping) else None
    if not isinstance(cleanup, dict):
        raise EvidenceError("maintenance smoke cleanup readback is invalid")
    quota = _quota_from_readbacks(context, jobs_root, lsattr, repquota)
    options = sorted(str(mount["options"]).split(","))
    return {
        "filesystem": mount["fstype"],
        "project_quota": "prjquota" in options and quota["project_inherit"] is True,
        "empty_job_root": jobs_root["entries"] == [],
        "cleanup_supported": (
            cleanup.get("returncode") == 0
            and all(cleanup.get(key) is True for key in CLEANUP_ABSENCE_FACTS)
        ),
        "mountpoint": mount["target"],
        "source": source,
        "mount_options": options,
        "dedicated": lsblk["stdout"].strip() in {"disk", "part", "lvm"},
        "quota": quota,
        "raw": {
            "findmnt": findmnt,
            "lsblk": lsblk,
            "jobs_root": jobs_root,
            "lsattr": lsattr,
            "repquota": repquota,
            "cleanup": dict(cleanup),
        },
    }


def _kernel_observation(context: EvidenceContext) -> dict[str, Any]:
    sysctl_command = (
        "/usr/sbin/sysctl",
        "--values",
        "kernel.unprivileged_userns_clone",
    )
    sysctl = _required_readback(sysctl_command, "unprivileged user namespace sysctl")
    cgroup_command = (
        "/usr/bin/findmnt",
        "--json",
        "--target",
        "/sys/fs/cgroup",
        "--output",
        "TARGET,SOURCE,FSTYPE,OPTIONS",
    )
    cgroup_mount_readback = _required_readback(cgroup_command, "cgroup v2 mount")
    cgroup_mount = _single_mount(cgroup_mount_readback, "cgroup v2 mount")
    bpffs_command = (
        "/usr/bin/findmnt",
        "--json",
        "--target",
        "/sys/fs/bpf",
        "--output",
        "TARGET,SOURCE,FSTYPE,OPTIONS",
    )
    bpffs_mount_readback = _required_readback(bpffs_command, "bpffs mount")
    bpffs_mount = _single_mount(bpffs_mount_readback, "bpffs mount")
    try:
        bpffs_metadata = os.lstat("/sys/fs/bpf")
    except OSError as exc:
        raise EvidenceError("bpffs metadata is unavailable") from exc
    if not stat.S_ISDIR(bpffs_metadata.st_mode):
        raise EvidenceError("bpffs mountpoint type is unsafe")
    controllers = _file_readback(
        Path("/sys/fs/cgroup/cgroup.controllers"), "cgroup v2 controllers"
    )
    delegation = _file_readback(
        Path("/sys/fs/cgroup/cgroup.subtree_control"), "cgroup v2 delegation"
    )
    controller_names = sorted(str(controllers["contents"]).split())
    delegated_names = sorted(str(delegation["contents"]).split())
    pidfd = _probe_pidfd_open()
    memfd = _probe_sealed_memfd()
    clone3 = _probe_clone3_into_cgroup()
    bpffs_options = str(bpffs_mount["options"]).split(",")
    root_only_bpffs = (
        bpffs_mount["target"] == "/sys/fs/bpf"
        and bpffs_mount["fstype"] == "bpf"
        and "mode=700" in bpffs_options
        and bpffs_metadata.st_uid == 0
        and bpffs_metadata.st_gid == 0
        and stat.S_IMODE(bpffs_metadata.st_mode) == 0o700
    )
    return {
        "cgroup_version": 2 if cgroup_mount["fstype"] == "cgroup2" else 0,
        "controllers": controller_names,
        "unprivileged_user_namespaces": sysctl["stdout"] == "1\n",
        "pidfd_open": pidfd.get("outcome") == "opened",
        "sealed_memfd": memfd.get("outcome") == "sealed",
        "clone3_into_cgroup": clone3.get("errno_name") == "EBADF",
        "bpffs_mounted_root_only": root_only_bpffs,
        "cgroup_filesystem": cgroup_mount["fstype"],
        "delegated_controllers": delegated_names,
        "slurm_cgroup_readback": {},
        "raw": {
            "unprivileged_user_namespaces": sysctl,
            "pidfd_open": pidfd,
            "sealed_memfd": memfd,
            "clone3_into_cgroup": clone3,
            "cgroup_mount": cgroup_mount_readback,
            "bpffs_mount": bpffs_mount_readback,
            "bpffs_metadata": {
                "path": "/sys/fs/bpf",
                "uid": bpffs_metadata.st_uid,
                "gid": bpffs_metadata.st_gid,
                "mode": f"{stat.S_IMODE(bpffs_metadata.st_mode):04o}",
            },
            "controllers": controllers,
            "delegation": delegation,
        },
    }


def _system_node_observation(
    context: EvidenceContext,
    cluster: Mapping[str, Any],
    node_name: str,
    host_receipt: Mapping[str, Any],
    maintenance_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    architecture = _required_command(("/usr/bin/uname", "-m"), "node architecture").strip()
    if architecture != cluster["architecture"]:
        raise EvidenceError("node architecture does not match policy")
    host_facts = host_receipt["document"]["post_state"]
    daemon_config = maintenance_receipt["document"]["observations"]["daemon"]["check"][
        "cgroup_config"
    ]
    forbidden: list[str] = []
    if host_facts.get("forbidden_sockets_absent") is not True:
        forbidden = list(context.policy.raw["runtime"]["forbidden_paths"])
    kernel = _kernel_observation(context)
    kernel["slurm_cgroup_readback"] = daemon_config
    return {
        "slurm_identity": _slurm_identity_observation(node_name),
        "identity": _identity_from_system(context, node=True),
        "packages": _package_observation(context, cluster, host_receipt),
        "kernel": kernel,
        "runtime": _runtime_observation(context, cluster),
        "storage": _storage_observation(context, host_receipt, maintenance_receipt),
        "forbidden_paths_present": forbidden,
        "node_guard": _phase2_absence_observation(),
    }


def _common_arguments(command: argparse.ArgumentParser) -> None:
    command.add_argument("--candidate-root", type=Path, required=True)
    command.add_argument("--policy", type=Path, required=True)
    command.add_argument("--release", type=Path, required=True)
    command.add_argument("--output", type=Path, required=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    controller = commands.add_parser("collect-controller")
    _common_arguments(controller)
    controller.add_argument("--cluster-id", required=True)
    controller.add_argument("--slurm-receipt", type=Path, required=True)
    node = commands.add_parser("collect-node")
    _common_arguments(node)
    node.add_argument("--cluster-id", required=True)
    node.add_argument("--slurm-node", required=True)
    node.add_argument("--host-receipt", type=Path, required=True)
    node.add_argument("--maintenance-receipt", type=Path, required=True)
    assembled = commands.add_parser("assemble")
    _common_arguments(assembled)
    assembled.add_argument("--controller-evidence", type=Path, action="append", required=True)
    assembled.add_argument("--node-evidence", type=Path, action="append", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.command == "collect-controller":
            result = collect_controller(
                arguments.candidate_root,
                arguments.policy,
                arguments.release,
                arguments.cluster_id,
                arguments.slurm_receipt,
                arguments.output,
            )
        elif arguments.command == "collect-node":
            result = collect_node(
                arguments.candidate_root,
                arguments.policy,
                arguments.release,
                arguments.cluster_id,
                arguments.slurm_node,
                arguments.host_receipt,
                arguments.maintenance_receipt,
                arguments.output,
            )
        else:
            result = assemble(
                arguments.candidate_root,
                arguments.policy,
                arguments.release,
                arguments.controller_evidence,
                arguments.node_evidence,
                arguments.output,
            )
    except (EvidenceError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "schema": result["schema"],
                "output": str(arguments.output),
                "production_certification_allowed": False,
                "certified_nodes": [],
                "blockers": [INERT_BLOCKER],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
