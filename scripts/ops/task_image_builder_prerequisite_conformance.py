#!/usr/bin/env python3
"""Validate Phase 1 task-image builder prerequisite evidence without mutation."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import ipaddress
import json
import os
import re
import stat
import sys
import tomllib
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import Any, cast

from jsonschema import Draft202012Validator, FormatChecker

MAX_INPUT_BYTES = 16 * 1024 * 1024
MAX_JSON_DEPTH = 64
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.ops import task_image_builder_authority as authority  # noqa: E402
from scripts.ops import task_image_builder_host_release as host_release  # noqa: E402

DEFAULT_POLICY = REPO_ROOT / "deploy/task-image-builder/prerequisites-v1.toml"
DEFAULT_SCHEMA = (
    REPO_ROOT / "docs/evidence/task-image-builder-prerequisite-conformance-v2.schema.json"
)
EVIDENCE_SCHEMA = "loom.task-image-builder-prerequisite-conformance/v2"
INERT_BLOCKER = "phase2_guard_provider_release_missing"
ZERO_HASH = "0" * 64
EXPECTED_IDENTITY = {
    "user": "loom-builder",
    "group": "loom-task-builder",
    "uid": 993,
    "gid": 980,
    "subid_start": 3_000_000,
    "subid_count": 65_536,
    "home": "/nonexistent",
    "shell": "/usr/sbin/nologin",
    "forbidden_supplementary_groups": ["docker", "root", "sudo"],
}
EXPECTED_CLUSTER_AUTHORITY = {
    "oldlab": {
        "controller": "TRT-EAI-OLDLAB-1",
        "architecture": "x86_64",
        "builder_nodes": [
            "trt-eai-oldlab-3",
            "trt-eai-oldlab-4",
            "trt-eai-oldlab-5",
        ],
        "legacy_base_qos": "normal",
        "legacy_reservation_node": "trt-eai-oldlab-6",
        "legacy_reservation_partition": "all",
    },
    "gb10": {
        "controller": "gx10-01c7",
        "architecture": "aarch64",
        "builder_nodes": [f"trt-gb10-{index}" for index in range(1, 16)],
        "legacy_base_qos": "loom-staging",
        "legacy_reservation_node": "trt-gb10-2",
        "legacy_reservation_partition": "gb10",
    },
}
BUILDER_FEATURE = "loom_rootless_buildkit"
PHASE2_NAMES = (
    "loom-task-builder-allocation-supervisor",
    "loom-task-builder-node-guard",
    "loom-task-builder-provider",
)
PHASE2_ARTIFACT_PATHS = tuple(f"/usr/libexec/{name}" for name in PHASE2_NAMES)
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

_SECRET_KEY_RE = re.compile(
    r"(?:^|[_-])(?:authorization|credential|password|private[_-]?key|"
    r"access[_-]?key|api[_-]?key|secret|token)$",
    re.IGNORECASE,
)
_SECRET_VALUE_RE = (
    re.compile(r"\b[Bb]earer\s+[A-Za-z0-9._~+/=-]{8,}"),
    re.compile(r"\b(?:proxy-)?authorization\s*:\s*(?:basic|digest)\s+\S+", re.IGNORECASE),
    re.compile(r"\bBasic\s+[A-Za-z0-9+/]{8,}={0,2}(?![A-Za-z0-9+/=])", re.IGNORECASE),
    re.compile(
        r"\bDigest\s+(?:username|realm|nonce|uri|response)\s*=",
        re.IGNORECASE,
    ),
    re.compile(r"-----BEGIN [^-\r\n]*PRIVATE KEY(?: BLOCK)?-----", re.IGNORECASE),
    re.compile(r"\b(?:AKIA|ASIA|AIDA|AROA|AIPA|ANPA|ANVA|ASCA)[A-Z0-9]{16}\b"),
    re.compile(r"\bAIza[0-9A-Za-z_-]{30,}\b"),
    re.compile(r"\bya29\.[0-9A-Za-z_-]{20,}\b"),
    re.compile(r"\b(?:xox[aboprs]-|xapp-)[0-9A-Za-z-]{10,}\b"),
    re.compile(r"\b(?:sk|rk)_(?:live|test)_[0-9A-Za-z]{16,}\b"),
    re.compile(r"\bwhsec_[0-9A-Za-z]{16,}\b"),
    re.compile(r"\bsk-[A-Za-z0-9][A-Za-z0-9_-]{10,}"),
    re.compile(r"\bghp_[A-Za-z0-9_]{10,}"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{10,}"),
    re.compile(r"\bloom_(?:api|w)_[A-Za-z0-9._-]{8,}", re.IGNORECASE),
    re.compile(
        r"(?i)(?:X-Amz-Signature|AWSAccessKeyId|Signature|token|api_key|"
        r"access_key)=[^&\s]+"
    ),
    re.compile(r"://[^:/@\s]+:[^@\s]+@"),
    re.compile(
        r"(?i)(?:^|\s)--(?:api[-_]?key|authorization|credential|password|"
        r"private[-_]?key|access[-_]?key|secret|token)(?:=|\s|$)"
    ),
)


class ConformanceError(ValueError):
    """Raised when policy or evidence cannot be processed safely."""


@dataclass(frozen=True)
class PrerequisitePolicy:
    raw: dict[str, Any]
    runtime: dict[str, Any]
    digest: str
    policy_file_digest: str
    runtime_digest: str
    release: dict[str, Any]
    release_digest: str
    candidate_digest: str
    authority_binding: authority.AuthorityBinding


def _read_regular(path: Path) -> bytes:
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY
            | os.O_NOFOLLOW
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        initial = os.fstat(descriptor)
        if not stat.S_ISREG(initial.st_mode):
            raise ConformanceError("input must be a regular non-symlink file")
        if initial.st_size > MAX_INPUT_BYTES:
            raise ConformanceError("input exceeds the size limit")
        chunks: list[bytes] = []
        remaining = MAX_INPUT_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(65_536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        final = os.fstat(descriptor)
        if len(payload) > MAX_INPUT_BYTES:
            raise ConformanceError("input exceeds the size limit")
        if len(payload) != initial.st_size or (
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
            raise ConformanceError("input changed while being read")
        return payload
    except ConformanceError:
        raise
    except OSError as exc:
        raise ConformanceError("input is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _validate_json_depth(value: Any) -> None:
    pending: list[tuple[Any, int]] = [(value, 1)]
    while pending:
        item, depth = pending.pop()
        if isinstance(item, Mapping):
            if depth > MAX_JSON_DEPTH:
                raise ConformanceError("input exceeds the JSON nesting limit")
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, Sequence) and not isinstance(item, (str, bytes, bytearray)):
            if depth > MAX_JSON_DEPTH:
                raise ConformanceError("input exceeds the JSON nesting limit")
            pending.extend((child, depth + 1) for child in item)


def _json_load(path: Path) -> Any:
    try:
        value = json.loads(_read_regular(path))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ConformanceError("input is not valid JSON") from exc
    _validate_json_depth(value)
    return value


def _policy_json(payload: bytes, error_message: str) -> Any:
    try:
        value = json.loads(payload)
        _validate_json_depth(value)
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        RecursionError,
        ConformanceError,
    ) as exc:
        raise ConformanceError(error_message) from exc
    return value


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
    policy_payload = _read_regular(path)
    try:
        raw_value = tomllib.loads(policy_payload.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ConformanceError("prerequisite policy is not valid TOML") from exc
    if raw_value.get("schema") != "loom.task-image-builder-prerequisites/v1":
        raise ConformanceError("prerequisite policy schema is invalid")
    if raw_value.get("production_certification_allowed") is not False:
        raise ConformanceError("Phase 1 policy cannot allow production certification")
    certified_nodes = raw_value.get("certified_nodes")
    if certified_nodes != []:
        raise ConformanceError("Phase 1 policy must certify zero nodes")
    if raw_value.get("unconditional_blockers") != [INERT_BLOCKER]:
        raise ConformanceError("Phase 1 blocker policy is invalid")
    if raw_value.get("identity") != EXPECTED_IDENTITY:
        raise ConformanceError("Phase 1 identity policy is invalid")
    expected_legacy = {
        "qos": "loom-task-image-builder",
        "reservation": "loom-task-image-builder",
        "account": "loom-staging",
        "user": "loom-rollout",
        "max_jobs_per_user": 1,
        "max_submit_jobs_per_user": 1,
        "max_wall": "04:00:00",
    }
    if raw_value.get("legacy_guard") != expected_legacy:
        raise ConformanceError("legacy builder guard policy is invalid")
    expected_rootless_qos = {
        "oldlab": "loom-task-image-builder-rootless-oldlab",
        "gb10": "loom-task-image-builder-rootless-gb10",
    }
    clusters = raw_value.get("clusters")
    if not isinstance(clusters, list) or len(clusters) != 2:
        raise ConformanceError("prerequisite cluster policy is invalid")
    seen_qos: set[str] = set()
    for cluster in clusters:
        if not isinstance(cluster, dict):
            raise ConformanceError("prerequisite cluster policy is invalid")
        cluster_id = cluster.get("id")
        if cluster_id not in expected_rootless_qos:
            raise ConformanceError("prerequisite cluster policy is invalid")
        cluster_authority = EXPECTED_CLUSTER_AUTHORITY[cluster_id]
        if any(cluster.get(key) != value for key, value in cluster_authority.items()):
            raise ConformanceError("immutable cluster authority policy is invalid")
        qos = cluster.get("slurm_qos")
        if qos != expected_rootless_qos[cluster_id] or qos == expected_legacy["qos"]:
            raise ConformanceError("rootless builder QoS policy is invalid")
        if qos in seen_qos:
            raise ConformanceError("rootless builder QoS policy is not unique")
        seen_qos.add(qos)
        for key in (
            "legacy_base_qos",
            "legacy_reservation_node",
            "legacy_reservation_partition",
        ):
            if not isinstance(cluster.get(key), str) or not cluster[key]:
                raise ConformanceError("legacy cluster guard policy is invalid")
    runtime_section = raw_value.get("runtime")
    if not isinstance(runtime_section, dict):
        raise ConformanceError("prerequisite runtime policy is invalid")
    manifest_name = runtime_section.get("manifest")
    if not isinstance(manifest_name, str) or Path(manifest_name).name != manifest_name:
        raise ConformanceError("runtime manifest path is invalid")
    runtime_path = path.parent / manifest_name
    runtime_payload = _read_regular(runtime_path)
    runtime_value = _policy_json(runtime_payload, "rootless runtime manifest is invalid")
    if not isinstance(runtime_value, dict) or runtime_value.get("schema") != (
        "loom.task-image-builder-rootless-runtime/v1"
    ):
        raise ConformanceError("rootless runtime manifest is invalid")
    release_manifest = raw_value.get("host_release_manifest")
    if (
        not isinstance(release_manifest, str)
        or not release_manifest
        or "/" in release_manifest
        or "\\" in release_manifest
        or Path(release_manifest).name != release_manifest
    ):
        raise ConformanceError("host release manifest path is invalid")
    release_path = path.parent / release_manifest
    release_payload = _read_regular(release_path)
    release_value = _policy_json(release_payload, "host release is invalid")
    try:
        parsed_release = host_release.load_host_release(release_path)
    except host_release.HostReleaseError as exc:
        raise ConformanceError("host release is invalid") from exc
    if not isinstance(release_value, dict) or parsed_release.runtime_manifest != manifest_name:
        raise ConformanceError("host release is invalid")
    digest = hashlib.sha256(_canonical_bytes(raw_value)).hexdigest()
    try:
        authority_binding = authority.load_authority_binding(REPO_ROOT)
    except authority.AuthorityError as exc:
        raise ConformanceError("authority component binding is invalid") from exc
    candidate_components = {
        "policy": hashlib.sha256(policy_payload).hexdigest(),
        "release": hashlib.sha256(release_payload).hexdigest(),
        "runtime": hashlib.sha256(runtime_payload).hexdigest(),
        **authority_binding.as_dict(),
    }
    return PrerequisitePolicy(
        raw=raw_value,
        runtime=runtime_value,
        digest=digest,
        policy_file_digest=hashlib.sha256(policy_payload).hexdigest(),
        runtime_digest=hashlib.sha256(runtime_payload).hexdigest(),
        release=release_value,
        release_digest=hashlib.sha256(release_payload).hexdigest(),
        candidate_digest=hashlib.sha256(_canonical_bytes(candidate_components)).hexdigest(),
        authority_binding=authority_binding,
    )


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
        elif location.endswith(".node_guard.installed") or location.endswith(".node_guard.active"):
            label = "Phase 2 node guard: "
        elif ".packages.helpers" in location:
            label = "UID-map helper evidence: "
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


def _fingerprint(value: object) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _valid_operation_id(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = uuid.UUID(value)
    except ValueError:
        return False
    return parsed.version == 4 and str(parsed) == value


def _owned_reason(release_name: str, operation_id: object) -> str:
    return f"loom-task-builder-phase1/{release_name}/{operation_id}"


def _file_digest(relative: str) -> str:
    return hashlib.sha256(_read_regular(REPO_ROOT / relative)).hexdigest()


def _slurm_candidate_digest(policy: PrerequisitePolicy) -> str:
    return _fingerprint(
        {
            "policy": policy.policy_file_digest,
            **policy.authority_binding.as_dict(),
        }
    )


def _host_candidate_digest(policy: PrerequisitePolicy) -> str:
    return _fingerprint(
        {
            "policy": policy.policy_file_digest,
            "release": policy.release_digest,
            "runtime": policy.runtime_digest,
            **policy.authority_binding.as_dict(),
        }
    )


def _maintenance_candidate_digest(policy: PrerequisitePolicy) -> str:
    return _fingerprint(
        {
            "policy": policy.policy_file_digest,
            **policy.authority_binding.as_dict(),
        }
    )


def _receipt_document(
    value: object,
    *,
    label: str,
) -> tuple[dict[str, Any] | None, list[str]]:
    if not isinstance(value, dict) or set(value) != {"sha256", "document"}:
        return None, [f"{label} wrapper is invalid"]
    digest = value.get("sha256")
    document = value.get("document")
    if (
        not isinstance(digest, str)
        or not isinstance(document, dict)
        or digest != hashlib.sha256(_canonical_bytes(document) + b"\n").hexdigest()
    ):
        return None, [f"{label} digest is invalid"]
    return document, []


def _receipt_events(
    document: Mapping[str, Any],
    *,
    label: str,
) -> tuple[list[dict[str, Any]] | None, list[str]]:
    raw_events = document.get("events")
    if not isinstance(raw_events, list) or not raw_events:
        return None, [f"{label} event chain is invalid"]
    events: list[dict[str, Any]] = []
    previous = ZERO_HASH
    for sequence, raw_event in enumerate(raw_events):
        if not isinstance(raw_event, dict):
            return None, [f"{label} event chain is invalid"]
        event = dict(raw_event)
        event_hash = event.pop("event_hash", None)
        if (
            event.get("sequence") != sequence
            or event.get("previous_hash") != previous
            or not isinstance(event.get("type"), str)
            or not isinstance(event.get("data"), dict)
            or event_hash != _fingerprint(event)
        ):
            return None, [f"{label} event chain is invalid"]
        previous = str(event_hash)
        events.append(dict(raw_event))
    return events, []


def _event(
    events: Sequence[Mapping[str, Any]],
    event_type: str,
) -> Mapping[str, Any] | None:
    matches = [item for item in events if item.get("type") == event_type]
    return matches[0] if len(matches) == 1 else None


def _command_receipt_is_valid(value: object) -> bool:
    return bool(
        isinstance(value, dict)
        and set(value) == {"command", "returncode", "stdout", "stderr"}
        and isinstance(value.get("command"), list)
        and all(isinstance(item, str) and item for item in value["command"])
        and isinstance(value.get("returncode"), int)
        and not isinstance(value.get("returncode"), bool)
        and isinstance(value.get("stdout"), str)
        and isinstance(value.get("stderr"), str)
    )


def _reservation_rows(value: object) -> dict[str, dict[str, str]] | None:
    if (
        not _command_receipt_is_valid(value)
        or not isinstance(value, dict)
        or value.get("returncode") != 0
        or value.get("stderr") != ""
    ):
        return None
    lines = str(value.get("stdout", "")).splitlines()
    rows: dict[str, dict[str, str]] = {}
    for line in lines:
        if not line:
            continue
        if line == "No reservations in the system":
            return {} if len(lines) == 1 else None
        pairs = [token.split("=", 1) for token in line.split()]
        if any(len(pair) != 2 for pair in pairs):
            return None
        fields = {pair[0]: pair[1] for pair in pairs}
        name = fields.get("ReservationName")
        if not name or len(fields) != len(pairs) or name in rows:
            return None
        rows[name] = fields
    return rows


def _maintenance_command_matches(
    command: object,
    action: str,
    job_id: str,
    operation_id: object,
) -> bool:
    return bool(
        isinstance(operation_id, str)
        and isinstance(command, list)
        and len(command) == 5
        and isinstance(command[0], str)
        and Path(command[0]).is_absolute()
        and command[0].endswith("/scripts/ops/task_image_builder_node_maintenance.py")
        and command[1:] == ["--internal-smoke", action, job_id, operation_id]
    )


def _cpuset_cpu_count(value: object) -> int | None:
    if not isinstance(value, str) or not value or len(value) > 4096:
        return None
    ranges: list[tuple[int, int]] = []
    for raw_range in value.split(","):
        fields = raw_range.split("-")
        if len(fields) not in {1, 2} or any(
            re.fullmatch(r"[0-9]+", field) is None for field in fields
        ):
            return None
        start = int(fields[0])
        end = start if len(fields) == 1 else int(fields[1])
        if start > end or end > 2**31 - 1:
            return None
        ranges.append((start, end))
    ranges.sort()
    if any(current[0] <= previous[1] for previous, current in pairwise(ranges)):
        return None
    return sum(end - start + 1 for start, end in ranges)


def _observation_freshness_failure(
    value: object,
    *,
    collected_at: datetime,
    label: str,
) -> list[str]:
    if not isinstance(value, str):
        return [f"{label}: observation freshness timestamp is invalid"]
    try:
        observed = _parse_collected_at(value)
    except ConformanceError:
        return [f"{label}: observation freshness timestamp is invalid"]
    age_seconds = (collected_at - observed).total_seconds()
    if age_seconds > 3600 or age_seconds < -300:
        return [f"{label}: observation freshness window is invalid"]
    return []


def _binding_failures(
    value: Mapping[str, Any],
    policy: PrerequisitePolicy,
    *,
    label: str,
) -> list[str]:
    expected = {
        "candidate_sha256": policy.candidate_digest,
        "policy_version": policy.raw["policy_version"],
        "policy_sha256": policy.digest,
        "policy_file_sha256": policy.policy_file_digest,
        "release_name": policy.release["release"],
        "release_sha256": policy.release_digest,
        "runtime_manifest_sha256": policy.runtime_digest,
        **policy.authority_binding.as_dict(),
    }
    failures: list[str] = []
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
            failures.append(f"{label}: {labels[key]} does not match authority")
    return failures


def _authority_binding_failures(
    document: Mapping[str, Any],
    policy: PrerequisitePolicy,
    *,
    label: str,
) -> list[str]:
    try:
        authority.validate_authority_binding(document, policy.authority_binding)
    except authority.AuthorityError:
        return [f"{label}: authority component binding is invalid"]
    return []


def _controller_identity_failures(
    observed: Mapping[str, Any],
    *,
    cluster_id: str,
    policy: PrerequisitePolicy,
) -> list[str]:
    identity = policy.raw["identity"]
    expected = {
        "user": identity["user"],
        "uid": identity["uid"],
        "group": identity["group"],
        "gid": identity["gid"],
        "home": identity["home"],
        "shell": identity["shell"],
        "supplementary_groups": [],
    }
    if _exact_mapping(observed, expected):
        return []
    return [f"{cluster_id}: controller identity is invalid"]


def _legacy_builder_failures(
    observed: Mapping[str, Any],
    *,
    cluster: Mapping[str, Any],
    policy: PrerequisitePolicy,
) -> list[str]:
    legacy = policy.raw["legacy_guard"]
    expected = {
        "qos": {
            "name": legacy["qos"],
            "flags": ["DenyOnLimit"],
            "priority": 0,
            "max_jobs_per_user": legacy["max_jobs_per_user"],
            "max_submit_jobs_per_user": legacy["max_submit_jobs_per_user"],
            "max_wall": legacy["max_wall"],
            "group_tres": {},
        },
        "association": {
            "cluster": cluster["slurm_cluster"],
            "account": legacy["account"],
            "user": legacy["user"],
            "qos": sorted([cluster["legacy_base_qos"], legacy["qos"]]),
            "default_qos": cluster["legacy_base_qos"],
        },
        "reservation": {
            "name": legacy["reservation"],
            "node": cluster["legacy_reservation_node"],
            "partition": cluster["legacy_reservation_partition"],
            "users": [legacy["user"]],
            "accounts": [legacy["account"]],
            "state": "ACTIVE",
            "flags": ["IGNORE_JOBS", "SPEC_NODES"],
        },
    }
    if _exact_mapping(observed, expected):
        return []
    return [f"{cluster['id']}: legacy builder guard is invalid"]


def _expected_receipt_legacy(
    cluster: Mapping[str, Any],
    policy: PrerequisitePolicy,
) -> dict[str, Any]:
    legacy = policy.raw["legacy_guard"]
    return {
        "qos": {
            "name": legacy["qos"],
            "flags": ["DenyOnLimit"],
            "priority": 0,
            "max_jobs_per_user": legacy["max_jobs_per_user"],
            "max_submit_jobs_per_user": legacy["max_submit_jobs_per_user"],
            "max_wall": legacy["max_wall"],
            "group_tres": {},
        },
        "association": {
            "cluster": cluster["slurm_cluster"],
            "account": legacy["account"],
            "user": legacy["user"],
            "qos": sorted([cluster["legacy_base_qos"], legacy["qos"]]),
            "default_qos": cluster["legacy_base_qos"],
        },
        "reservation": {
            "name": legacy["reservation"],
            "node": cluster["legacy_reservation_node"],
            "node_count": 1,
            "partition": cluster["legacy_reservation_partition"],
            "users": [legacy["user"]],
            "accounts": [legacy["account"]],
            "state": "ACTIVE",
            "flags": ["IGNORE_JOBS", "SPEC_NODES"],
        },
    }


def _slurm_identity_failures(node: Mapping[str, Any]) -> list[str]:
    node_name = node["name"]
    binding = node["slurm_identity"]
    try:
        resolved = {ipaddress.ip_address(item) for item in binding["resolved_addresses"]}
        local_addresses = {ipaddress.ip_address(item) for item in binding["local_addresses"]}
        literal_address = ipaddress.ip_address(binding["node_addr"])
    except ValueError:
        literal_address = None
    except TypeError:
        return [f"{node_name}: Slurm host binding is invalid"]
    resolution = binding.get("resolution")
    readback = binding.get("readback")
    raw_fields: list[tuple[str, str]] = []
    if isinstance(readback, dict):
        for token in str(readback.get("stdout", "")).split():
            if "=" in token:
                key, item = token.split("=", 1)
                raw_fields.append((key, item))
    fields = dict(raw_fields)
    local_hostnames = {item.casefold() for item in binding["local_hostnames"]}
    if (
        binding["node_name"] != node_name
        or not resolved
        or any(address.is_loopback for address in resolved)
        or not resolved.issubset(local_addresses)
        or not isinstance(resolution, dict)
        or set(resolution) != {"query", "addresses"}
        or resolution.get("query") != binding["node_addr"]
        or resolution.get("addresses") != binding["resolved_addresses"]
        or (literal_address is not None and literal_address not in resolved)
        or not _command_receipt_is_valid(readback)
        or readback.get("command") != ["/usr/bin/scontrol", "show", "node", node_name, "-o"]
        or readback.get("returncode") != 0
        or readback.get("stderr") != ""
        or len(fields) != len(raw_fields)
        or fields.get("NodeName") != node_name
        or fields.get("NodeHostName") != binding["node_hostname"]
        or fields.get("NodeAddr") != binding["node_addr"]
        or "AvailableFeatures" not in fields
        or "ActiveFeatures" not in fields
        or BUILDER_FEATURE in _slurm_feature_names(fields.get("AvailableFeatures"))
        or BUILDER_FEATURE in _slurm_feature_names(fields.get("ActiveFeatures"))
        or binding["node_hostname"].casefold() not in local_hostnames
    ):
        return [f"{node_name}: Slurm host binding is invalid"]
    return []


def _slurm_feature_names(value: object) -> set[str]:
    if value in {None, "", "(null)"}:
        return set()
    if not isinstance(value, str):
        return {"<invalid>"}
    return set(value.split(","))


def _package_failures(
    value: Mapping[str, Any],
    *,
    cluster: Mapping[str, Any],
    policy: PrerequisitePolicy,
    node_name: str,
) -> list[str]:
    ubuntu = policy.release["ubuntu"]
    debian_architecture = policy.release["architecture_map"][cluster["architecture"]]
    expected_source = {
        "os_id": ubuntu["os_id"],
        "version_id": ubuntu["version_id"],
        "snapshot": ubuntu["snapshot"],
        "suites": list(policy.release["repositories"][debian_architecture]["indexes"]),
        "component": ubuntu["component"],
        "signer_fingerprint": ubuntu["signer_fingerprint"],
        "keyring_sha256": ubuntu["keyring_sha256"],
    }
    if value.get("source") != expected_source:
        return [f"{node_name}: package/source signature evidence is invalid"]
    expected_installed = [
        {
            "name": name,
            "source_suite": package["source_suite"],
            "version": package["version"],
            "architecture": package["architecture"],
            "filename": package["filename"],
            "size": package["size"],
            "artifact_sha256": package["sha256"],
        }
        for name, package in sorted(policy.release["packages"][debian_architecture].items())
    ]
    installed = value.get("installed")
    if not isinstance(installed, list) or installed != expected_installed:
        return [f"{node_name}: signed package artifacts are invalid"]
    helpers = value.get("helpers")
    if not isinstance(helpers, list) or len(helpers) != 2:
        return [f"{node_name}: UID-map helper evidence is invalid"]
    expected_paths = ("/usr/bin/newgidmap", "/usr/bin/newuidmap")
    sorted_helpers = sorted(helpers, key=lambda helper: str(helper.get("path")))
    for helper, path in zip(sorted_helpers, expected_paths, strict=True):
        if (
            helper.get("path") != path
            or helper.get("uid") != 0
            or helper.get("gid") != 0
            or helper.get("mode") != "4755"
            or helper.get("file_capabilities") != []
        ):
            return [f"{node_name}: UID-map helper evidence is invalid"]
    return []


def _runtime_raw_failures(
    value: Mapping[str, Any],
    *,
    cluster: Mapping[str, Any],
    policy: PrerequisitePolicy,
    node_name: str,
) -> list[str]:
    runtime_policy = policy.raw["runtime"]
    expected = {
        "release": policy.runtime["release"],
        "manifest_sha256": policy.runtime_digest,
        "binary_sha256": policy.runtime["architectures"][cluster["architecture"]]["binaries"],
        "dependency_sha256": {},
        "snapshotter": runtime_policy["snapshotter"],
        "network_driver": runtime_policy["network_driver"],
        "rootlesskit_flags": runtime_policy["rootlesskit_flags"],
        "insecure_entitlements": [],
    }
    if set(value) != {*expected, "elf_dynamic_readback"} or any(
        value.get(key) != item for key, item in expected.items()
    ):
        return [f"{node_name}: runtime/dependency digest evidence is invalid"]
    binaries = expected["binary_sha256"]
    dynamic = value.get("elf_dynamic_readback")
    if (
        not isinstance(binaries, dict)
        or not isinstance(dynamic, dict)
        or set(dynamic) != set(binaries)
    ):
        return [f"{node_name}: runtime/dependency digest evidence is invalid"]
    release_root = Path("/opt/loom-task-builder/releases") / str(policy.runtime["release"])
    for name in sorted(binaries):
        readback = dynamic.get(name)
        if (
            not isinstance(readback, dict)
            or not _command_receipt_is_valid(readback)
            or readback.get("command")
            != ["/usr/bin/readelf", "-d", str(release_root / "bin" / name)]
            or readback.get("returncode") != 0
            or readback.get("stderr") != ""
            or "(NEEDED)" in str(readback.get("stdout"))
        ):
            return [f"{node_name}: runtime/dependency digest evidence is invalid"]
    return []


def _phase2_guard_failures(value: object, *, node_name: str) -> list[str]:
    label = f"{node_name}: Phase 2 node guard readback is invalid"
    if not isinstance(value, dict) or set(value) != {
        "installed",
        "active",
        "artifacts",
        "unit_readback",
        "process_readback",
    }:
        return [label]
    expected_artifacts = [{"path": path, "present": False} for path in PHASE2_ARTIFACT_PATHS]
    units = value.get("unit_readback")
    processes = value.get("process_readback")
    if (
        value.get("installed") is not False
        or value.get("active") is not False
        or value.get("artifacts") != expected_artifacts
        or not isinstance(units, list)
        or not isinstance(processes, list)
        or len(units) != len(PHASE2_NAMES)
        or len(processes) != len(PHASE2_NAMES)
    ):
        return [label]
    for name, raw in zip(PHASE2_NAMES, units, strict=True):
        unit = f"{name}.service"
        if not isinstance(raw, dict):
            return [label]
        command = {key: item for key, item in raw.items() if key != "name"}
        fields = dict(
            line.split("=", 1) for line in str(raw.get("stdout", "")).splitlines() if "=" in line
        )
        if (
            raw.get("name") != unit
            or not _command_receipt_is_valid(command)
            or raw.get("command")
            != [
                "/usr/bin/systemctl",
                "show",
                "--no-pager",
                "--property=LoadState",
                "--property=ActiveState",
                "--property=FragmentPath",
                unit,
            ]
            or raw.get("returncode") != 0
            or raw.get("stderr") != ""
            or fields != {"LoadState": "not-found", "ActiveState": "inactive", "FragmentPath": ""}
        ):
            return [label]
    for name, raw in zip(PHASE2_NAMES, processes, strict=True):
        if not isinstance(raw, dict):
            return [label]
        command = {key: item for key, item in raw.items() if key != "name"}
        if (
            raw.get("name") != name
            or not _command_receipt_is_valid(command)
            or raw.get("command") != ["/usr/bin/pgrep", "-f", f"(^|/){name}( |$)"]
            or raw.get("returncode") != 1
            or raw.get("stdout") != ""
            or raw.get("stderr") != ""
        ):
            return [label]
    return []


def _quota_raw_failures(
    value: object,
    *,
    policy: PrerequisitePolicy,
    node_name: str,
) -> list[str]:
    if not isinstance(value, dict):
        return [f"{node_name}: quota readback is invalid"]
    resources = policy.raw["resource_profile"]
    expected = {
        "storage_root_exists": True,
        "storage_root_uid": 993,
        "storage_root_gid": 980,
        "storage_root_mode": 0o700,
        "storage_root_entries": [],
        "project_id": policy.raw["storage"]["project_id"],
        "project_inherit": True,
        "block_soft_limit": 0,
        "block_hard_limit": resources["scratch_bytes"] // 1024,
        "inode_soft_limit": 0,
        "inode_hard_limit": resources["scratch_inodes"],
    }
    if (
        any(value.get(key) != expected_value for key, expected_value in expected.items())
        or not isinstance(value.get("block_used"), int)
        or isinstance(value.get("block_used"), bool)
        or int(value["block_used"]) < 0
        or not isinstance(value.get("inode_used"), int)
        or isinstance(value.get("inode_used"), bool)
        or int(value["inode_used"]) < 0
    ):
        return [f"{node_name}: quota readback is invalid"]
    return []


def _storage_raw_failures(
    value: Mapping[str, Any],
    *,
    policy: PrerequisitePolicy,
    node_name: str,
    maintenance_document: Mapping[str, Any] | None,
) -> list[str]:
    storage = policy.raw["storage"]
    source = value.get("source")
    options = value.get("mount_options")
    failures = _quota_raw_failures(value.get("quota"), policy=policy, node_name=node_name)
    raw = value.get("raw")
    if not isinstance(raw, dict) or set(raw) != {
        "findmnt",
        "lsblk",
        "jobs_root",
        "lsattr",
        "repquota",
        "cleanup",
    }:
        failures.append(f"{node_name}: dedicated mount/quota raw readback is invalid")
        return failures
    findmnt = raw.get("findmnt")
    lsblk = raw.get("lsblk")
    jobs_root = raw.get("jobs_root")
    lsattr = raw.get("lsattr")
    repquota = raw.get("repquota")
    cleanup = raw.get("cleanup")
    try:
        findmnt_stdout = findmnt.get("stdout") if isinstance(findmnt, dict) else None
        filesystems = json.loads(str(findmnt_stdout))["filesystems"]
    except (AttributeError, KeyError, TypeError, json.JSONDecodeError):
        filesystems = []
    mount = filesystems[0] if isinstance(filesystems, list) and len(filesystems) == 1 else None
    mount_valid = isinstance(mount, dict) and set(mount) == {
        "target",
        "source",
        "fstype",
        "options",
    }
    attributes = str(lsattr.get("stdout", "")).strip().split(maxsplit=2) if isinstance(
        lsattr, dict
    ) else []
    quota_rows = (
        list(csv.reader(str(repquota.get("stdout", "")).splitlines()))
        if isinstance(repquota, dict)
        else []
    )
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
    matching_rows = [
        row
        for row in quota_rows[1:]
        if len(row) == len(expected_header) and row[0] == str(storage["project_id"])
    ] if quota_rows else []
    quota = value.get("quota")
    try:
        observed_limits = [int(matching_rows[0][index]) for index in (3, 4, 5, 7, 8, 9)]
        observed_project = int(attributes[0])
    except (IndexError, ValueError):
        observed_limits = []
        observed_project = -1
    maintenance_events = (
        maintenance_document.get("events")
        if isinstance(maintenance_document, Mapping)
        else None
    )
    cleanup_event = (
        _event(
            cast(Sequence[Mapping[str, Any]], maintenance_events),
            "smoke_cleaned",
        )
        if isinstance(maintenance_events, list)
        and all(isinstance(event, Mapping) for event in maintenance_events)
        else None
    )
    cleanup_data = cleanup_event.get("data") if isinstance(cleanup_event, dict) else None
    expected_cleanup = cleanup_data.get("cleanup") if isinstance(cleanup_data, dict) else None
    quota_mapping = quota if isinstance(quota, dict) else {}
    raw_valid = bool(
        _command_receipt_is_valid(findmnt)
        and isinstance(findmnt, dict)
        and findmnt.get("command")
        == [
            "/usr/bin/findmnt",
            "--json",
            "--target",
            storage["mountpoint"],
            "--output",
            "TARGET,SOURCE,FSTYPE,OPTIONS",
        ]
        and findmnt.get("returncode") == 0
        and findmnt.get("stderr") == ""
        and mount_valid
        and isinstance(mount, dict)
        and mount.get("target") == storage["mountpoint"]
        and mount.get("source") == source
        and mount.get("fstype") == storage["site_filesystem"]
        and sorted(str(mount.get("options", "")).split(",")) == options
        and _command_receipt_is_valid(lsblk)
        and isinstance(lsblk, dict)
        and lsblk.get("command")
        == ["/usr/bin/lsblk", "--noheadings", "--output", "TYPE", source]
        and lsblk.get("returncode") == 0
        and lsblk.get("stderr") == ""
        and str(lsblk.get("stdout", "")).strip() in {"disk", "part", "lvm"}
        and jobs_root
        == {
            "path": storage["root"],
            "uid": 993,
            "gid": 980,
            "mode": "0700",
            "entries": [],
        }
        and _command_receipt_is_valid(lsattr)
        and isinstance(lsattr, dict)
        and lsattr.get("command") == ["/usr/bin/lsattr", "-pd", storage["root"]]
        and lsattr.get("returncode") == 0
        and lsattr.get("stderr") == ""
        and len(attributes) == 3
        and attributes[2] == storage["root"]
        and observed_project == storage["project_id"]
        and "P" in attributes[1]
        and _command_receipt_is_valid(repquota)
        and isinstance(repquota, dict)
        and repquota.get("command")
        == [
            "/usr/sbin/repquota",
            "-v",
            "-n",
            "-p",
            "-P",
            "-O",
            "csv",
            storage["mountpoint"],
        ]
        and repquota.get("returncode") == 0
        and repquota.get("stderr") == ""
        and quota_rows
        and quota_rows[0] == expected_header
        and len(matching_rows) == 1
        and observed_limits
        == [
            quota_mapping.get("block_used"),
            quota_mapping.get("block_soft_limit"),
            quota_mapping.get("block_hard_limit"),
            quota_mapping.get("inode_used"),
            quota_mapping.get("inode_soft_limit"),
            quota_mapping.get("inode_hard_limit"),
        ]
        and cleanup == expected_cleanup
    )
    if not raw_valid:
        failures.append(f"{node_name}: dedicated mount/quota raw readback is invalid")
    if (
        value.get("filesystem") != storage["site_filesystem"]
        or value.get("project_quota") is not True
        or value.get("empty_job_root") is not True
        or value.get("cleanup_supported") is not True
        or value.get("mountpoint") != storage["mountpoint"]
        or not isinstance(source, str)
        or not source.startswith("/dev/")
        or source.startswith("/dev/loop")
        or not isinstance(options, list)
        or not set(storage["required_mount_options"]).issubset(options)
        or value.get("dedicated") is not True
    ):
        failures.append(f"{node_name}: dedicated mount/quota readback is invalid")
    return failures


def _kernel_raw_failures(
    value: Mapping[str, Any],
    *,
    policy: PrerequisitePolicy,
    node_name: str,
) -> list[str]:
    readback = value.get("slurm_cgroup_readback")
    raw = value.get("raw")
    if not isinstance(readback, dict) or not isinstance(raw, dict) or set(raw) != {
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
        return [f"{node_name}: cgroup readback is invalid"]
    sysctl = raw.get("unprivileged_user_namespaces")
    cgroup_mount = raw.get("cgroup_mount")
    bpffs_mount = raw.get("bpffs_mount")
    try:
        cgroup_mount_stdout = (
            cgroup_mount.get("stdout") if isinstance(cgroup_mount, dict) else None
        )
        bpffs_mount_stdout = (
            bpffs_mount.get("stdout") if isinstance(bpffs_mount, dict) else None
        )
        cgroup_filesystems = json.loads(str(cgroup_mount_stdout))["filesystems"]
        bpffs_filesystems = json.loads(str(bpffs_mount_stdout))["filesystems"]
    except (AttributeError, KeyError, TypeError, json.JSONDecodeError):
        cgroup_filesystems = []
        bpffs_filesystems = []
    cgroup_item = (
        cgroup_filesystems[0]
        if isinstance(cgroup_filesystems, list) and len(cgroup_filesystems) == 1
        else None
    )
    bpffs_item = (
        bpffs_filesystems[0]
        if isinstance(bpffs_filesystems, list) and len(bpffs_filesystems) == 1
        else None
    )
    controllers_raw = raw.get("controllers")
    delegation_raw = raw.get("delegation")
    pidfd = raw.get("pidfd_open")
    pidfd_pid = pidfd.get("pid") if isinstance(pidfd, dict) else None
    observed_controllers = sorted(str(controllers_raw.get("contents", "")).split()) if isinstance(
        controllers_raw, dict
    ) else []
    observed_delegation = sorted(str(delegation_raw.get("contents", "")).split()) if isinstance(
        delegation_raw, dict
    ) else []
    contents = readback.get("contents")
    required_controllers = {"cpu", "cpuset", "io", "memory", "pids"}
    controllers = value.get("controllers")
    if (
        value.get("cgroup_filesystem") != "cgroup2"
        or not _command_receipt_is_valid(sysctl)
        or not isinstance(sysctl, dict)
        or sysctl.get("command")
        != ["/usr/sbin/sysctl", "--values", "kernel.unprivileged_userns_clone"]
        or sysctl.get("returncode") != 0
        or sysctl.get("stdout") != "1\n"
        or sysctl.get("stderr") != ""
        or not isinstance(pidfd, dict)
        or pidfd != {"pid": pidfd_pid, "flags": 0, "outcome": "opened"}
        or not isinstance(pidfd_pid, int)
        or isinstance(pidfd_pid, bool)
        or pidfd_pid <= 0
        or raw.get("sealed_memfd")
        != {"required_seals": 15, "observed_seals": 15, "outcome": "sealed"}
        or raw.get("clone3_into_cgroup")
        != {
            "flags": "CLONE_INTO_CGROUP",
            "cgroup_fd": -1,
            "returncode": -1,
            "errno": 9,
            "errno_name": "EBADF",
        }
        or not _command_receipt_is_valid(cgroup_mount)
        or not isinstance(cgroup_mount, dict)
        or cgroup_mount.get("command")
        != [
            "/usr/bin/findmnt",
            "--json",
            "--target",
            "/sys/fs/cgroup",
            "--output",
            "TARGET,SOURCE,FSTYPE,OPTIONS",
        ]
        or cgroup_mount.get("returncode") != 0
        or cgroup_mount.get("stderr") != ""
        or not isinstance(cgroup_item, dict)
        or cgroup_item.get("target") != "/sys/fs/cgroup"
        or cgroup_item.get("fstype") != "cgroup2"
        or not _command_receipt_is_valid(bpffs_mount)
        or not isinstance(bpffs_mount, dict)
        or bpffs_mount.get("command")
        != [
            "/usr/bin/findmnt",
            "--json",
            "--target",
            "/sys/fs/bpf",
            "--output",
            "TARGET,SOURCE,FSTYPE,OPTIONS",
        ]
        or bpffs_mount.get("returncode") != 0
        or bpffs_mount.get("stderr") != ""
        or not isinstance(bpffs_item, dict)
        or bpffs_item.get("target") != "/sys/fs/bpf"
        or bpffs_item.get("fstype") != "bpf"
        or "mode=700" not in str(bpffs_item.get("options", "")).split(",")
        or raw.get("bpffs_metadata")
        != {"path": "/sys/fs/bpf", "uid": 0, "gid": 0, "mode": "0700"}
        or not isinstance(controllers_raw, dict)
        or set(controllers_raw) != {"path", "contents", "sha256"}
        or controllers_raw.get("path") != "/sys/fs/cgroup/cgroup.controllers"
        or controllers_raw.get("sha256")
        != hashlib.sha256(str(controllers_raw.get("contents", "")).encode()).hexdigest()
        or not isinstance(delegation_raw, dict)
        or set(delegation_raw) != {"path", "contents", "sha256"}
        or delegation_raw.get("path") != "/sys/fs/cgroup/cgroup.subtree_control"
        or delegation_raw.get("sha256")
        != hashlib.sha256(str(delegation_raw.get("contents", "")).encode()).hexdigest()
        or not isinstance(controllers, list)
        or controllers != observed_controllers
        or not required_controllers.issubset(controllers)
        or value.get("delegated_controllers") != observed_delegation
        or observed_delegation
        != sorted(policy.raw["cgroup"]["required_delegated_controllers"])
        or readback.get("path") != "/etc/slurm/cgroup.conf"
        or contents != DESIRED_CGROUP.decode("utf-8")
        or readback.get("sha256") != hashlib.sha256(str(contents).encode("utf-8")).hexdigest()
    ):
        return [f"{node_name}: cgroup readback is invalid"]
    return []


def _slurm_receipt_failures(
    value: object,
    *,
    cluster: Mapping[str, Any],
    policy: PrerequisitePolicy,
) -> list[str]:
    cluster_id = str(cluster["id"])
    document, failures = _receipt_document(value, label=f"{cluster_id}: Slurm receipt")
    if document is None:
        return failures
    failures.extend(
        _authority_binding_failures(document, policy, label=f"{cluster_id}: Slurm receipt")
    )
    if (
        document.get("schema") != "loom.task-image-builder-slurm-receipt/v1"
        or not _valid_operation_id(document.get("operation_id"))
        or document.get("cluster_id") != cluster_id
        or document.get("candidate_digest") != _slurm_candidate_digest(policy)
        or document.get("policy_digest") != policy.policy_file_digest
        or document.get("cluster_digest") != _fingerprint(cluster)
    ):
        failures.append(f"{cluster_id}: Slurm receipt operation identity or binding is invalid")
    if document.get("terminal_state") != "converged":
        failures.append(f"{cluster_id}: Slurm receipt terminal state is invalid")
    if (
        document.get("production_certification_allowed") is not False
        or document.get("certified_nodes") != []
        or document.get("blockers") != [INERT_BLOCKER]
    ):
        failures.append(f"{cluster_id}: Slurm receipt inert boundary is invalid")
    pre_state = document.get("pre_state")
    post_state = document.get("post_state")
    if not isinstance(pre_state, dict) or not isinstance(post_state, dict):
        failures.append(f"{cluster_id}: Slurm receipt post-state is invalid")
    else:
        required_post = {"partition", "account", "qos", "association", "legacy"}
        if set(post_state) != required_post or any(
            not isinstance(post_state[key], dict) for key in required_post
        ):
            failures.append(f"{cluster_id}: Slurm receipt post-state is invalid")
        pre_legacy = pre_state.get("legacy")
        post_legacy = post_state.get("legacy")
        expected_legacy = _expected_receipt_legacy(cluster, policy)
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
            failures.append(f"{cluster_id}: legacy Slurm fingerprints changed")
        expected_partition = {
            "name": cluster["builder_partition"],
            "line": cluster["builder_partition_line"],
        }
        resources = policy.raw["resource_profile"]
        expected_qos = {
            "name": cluster["slurm_qos"],
            "flags": ["DenyOnLimit"],
            "priority": 0,
            "max_jobs_per_user": resources["max_jobs_per_user"],
            "max_submit_jobs_per_user": resources["max_submit_jobs_per_user"],
            "max_wall": resources["wall_time"],
            "group_tres": {
                "cpu": resources["cpus"],
                "memory_mib": resources["memory_mib"],
                "nodes": 1,
            },
        }
        expected_association = {
            "cluster": cluster["slurm_cluster"],
            "account": cluster["slurm_account"],
            "user": policy.raw["identity"]["user"],
            "partition": cluster["builder_partition"],
            "qos": [cluster["slurm_qos"]],
            "default_qos": cluster["slurm_qos"],
        }
        if (
            post_state.get("partition") != expected_partition
            or post_state.get("account") != {"name": cluster["slurm_account"]}
            or post_state.get("qos") != expected_qos
            or post_state.get("association") != expected_association
        ):
            failures.append(f"{cluster_id}: Slurm receipt raw state is invalid")
    command = document.get("command_outcome")
    created = document.get("created_objects")
    if (
        not isinstance(command, dict)
        or command.get("returncode") != 0
        or document.get("post_readback_error") is not None
        or not isinstance(created, list)
    ):
        failures.append(f"{cluster_id}: Slurm convergence receipt is unsuccessful")
    events, event_failures = _receipt_events(document, label=f"{cluster_id}: Slurm receipt")
    failures.extend(event_failures)
    if events is not None:
        if [event["type"] for event in events] != [
            "pre_state",
            "intent",
            "post_state",
            "converged",
        ]:
            failures.append(f"{cluster_id}: Slurm receipt event chain is invalid")
        elif (
            events[0]["data"] != {"state": pre_state}
            or events[-2]["data"]
            != {
                "state": post_state,
                "readback_error": None,
                "created_objects": created,
            }
            or events[-1]["data"] != {"returncode": 0, "legacy_unchanged": True}
        ):
            failures.append(f"{cluster_id}: Slurm receipt event binding is invalid")
    return failures


def _host_receipt_failures(
    value: object,
    *,
    cluster: Mapping[str, Any],
    policy: PrerequisitePolicy,
    node_name: str,
) -> tuple[dict[str, Any] | None, list[str]]:
    document, failures = _receipt_document(value, label=f"{node_name}: host receipt")
    if document is None:
        return None, failures
    failures.extend(
        _authority_binding_failures(document, policy, label=f"{node_name}: host receipt")
    )
    if (
        document.get("schema") != "loom.task-image-builder-host-receipt/v1"
        or not _valid_operation_id(document.get("operation_id"))
        or document.get("cluster_id") != cluster["id"]
        or document.get("slurm_node") != node_name
        or document.get("candidate_digest") != _host_candidate_digest(policy)
        or document.get("policy_digest") != policy.policy_file_digest
        or document.get("release_digest") != policy.release_digest
        or document.get("cluster_digest") != _fingerprint(cluster)
    ):
        failures.append(f"{node_name}: host receipt operation identity or binding is invalid")
    if (
        document.get("terminal_state") != "host_prepared"
        or document.get("activation_required") is not True
        or document.get("failure") is not None
        or document.get("rollback_verified") is not None
        or document.get("rollback_source_state") is not None
    ):
        failures.append(f"{node_name}: host receipt terminal state is invalid")
    if (
        document.get("production_certification_allowed") is not False
        or document.get("certified_nodes") != []
        or document.get("blockers") != [INERT_BLOCKER]
    ):
        failures.append(f"{node_name}: host receipt inert boundary is invalid")
    pre_state = document.get("pre_state")
    post_state = document.get("post_state")
    bundle_digest = document.get("bundle_digest")
    if (
        not isinstance(pre_state, dict)
        or not isinstance(post_state, dict)
        or not isinstance(bundle_digest, str)
        or re.fullmatch(r"[a-f0-9]{64}", bundle_digest) is None
    ):
        failures.append(f"{node_name}: host receipt post-state is invalid")
    else:
        expected_versions = {
            name: package["version"]
            for name, package in policy.release["packages"][
                policy.release["architecture_map"][cluster["architecture"]]
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
            or post_state.get("packages") != expected_versions
            or any(post_state.get(key) is not True for key in required_true)
        ):
            failures.append(f"{node_name}: host receipt raw facts are invalid")
        failures.extend(
            _quota_raw_failures(post_state.get("quota_state"), policy=policy, node_name=node_name)
        )
    cgroup = document.get("cgroup_poststate")
    if not isinstance(cgroup, dict):
        failures.append(f"{node_name}: host receipt cgroup readback is invalid")
    else:
        try:
            cgroup_payload = base64.b64decode(str(cgroup.get("payload_b64")), validate=True)
        except (TypeError, ValueError):
            cgroup_payload = b""
        if (
            set(cgroup) != {"kind", "payload_b64", "sha256", "mode", "uid", "gid"}
            or cgroup.get("kind") != "regular"
            or cgroup_payload != DESIRED_CGROUP
            or cgroup.get("sha256") != hashlib.sha256(cgroup_payload).hexdigest()
            or cgroup.get("mode") != 0o644
            or cgroup.get("uid") != 0
            or cgroup.get("gid") != 0
        ):
            failures.append(f"{node_name}: host receipt cgroup readback is invalid")
    events, event_failures = _receipt_events(document, label=f"{node_name}: host receipt")
    failures.extend(event_failures)
    if events is not None:
        if [event["type"] for event in events] != [
            "pre_state",
            "intent",
            "post_state",
            "host_prepared",
        ]:
            failures.append(f"{node_name}: host receipt event chain is invalid")
        else:
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
                failures.append(f"{node_name}: host receipt event binding is invalid")
    return document, failures


def _maintenance_receipt_failures(
    value: object,
    *,
    cluster: Mapping[str, Any],
    policy: PrerequisitePolicy,
    node_name: str,
    host_document: Mapping[str, Any] | None,
    kernel: Mapping[str, Any],
    storage: Mapping[str, Any],
) -> list[str]:
    document, failures = _receipt_document(value, label=f"{node_name}: maintenance receipt")
    if document is None:
        return failures
    failures.extend(
        _authority_binding_failures(document, policy, label=f"{node_name}: maintenance receipt")
    )
    if (
        document.get("schema") != "loom.task-image-builder-node-maintenance/v1"
        or not _valid_operation_id(document.get("operation_id"))
        or document.get("cluster_id") != cluster["id"]
        or document.get("slurm_node") != node_name
        or document.get("candidate_digest") != _maintenance_candidate_digest(policy)
        or document.get("policy_digest") != policy.policy_file_digest
        or (
            host_document is not None
            and document.get("operation_id") != host_document.get("operation_id")
        )
    ):
        failures.append(
            f"{node_name}: maintenance receipt operation identity or binding is invalid"
        )
    if document.get("terminal_state") != "prepared" or document.get("failure") is not None:
        failures.append(f"{node_name}: maintenance receipt terminal state is invalid")
    if (
        document.get("production_certification_allowed") is not False
        or document.get("certified_nodes") != []
        or document.get("blockers") != [INERT_BLOCKER]
    ):
        failures.append(f"{node_name}: maintenance receipt inert boundary is invalid")
    events, event_failures = _receipt_events(document, label=f"{node_name}: maintenance receipt")
    failures.extend(event_failures)
    if events is None:
        return failures
    if tuple(str(event["type"]) for event in events) != SUCCESSFUL_MAINTENANCE_EVENTS:
        failures.append(f"{node_name}: maintenance receipt terminal chain is invalid")
        return failures
    pre_state = document.get("pre_state")
    operation_id = document.get("operation_id")
    owned_reason = _owned_reason(str(policy.release["release"]), operation_id)
    if not isinstance(pre_state, dict) or set(pre_state) != {
        "state",
        "reason",
        "allocated_tres",
    }:
        failures.append(f"{node_name}: maintenance pre-state is invalid")
    elif "DRAIN" in str(pre_state.get("state")) and pre_state.get("reason") != owned_reason:
        failures.append(f"{node_name}: foreign drain ownership is forbidden")
    recorded = _event(events, "pre_state_recorded")
    drained = _event(events, "drained")
    if recorded is None or recorded.get("data") != {"pre_state": pre_state}:
        failures.append(f"{node_name}: maintenance pre-state is not receipt-bound")
    if drained is None or drained.get("data") != {"reason": owned_reason}:
        failures.append(f"{node_name}: owned drain receipt is invalid")
    observations = document.get("observations")
    if not isinstance(observations, dict) or set(observations) != {
        "daemon",
        "admission",
        "reservation",
        "smoke",
        "emergency_containment",
    }:
        failures.append(f"{node_name}: maintenance observations are invalid")
        return failures
    if observations.get("emergency_containment") is not None:
        failures.append(f"{node_name}: successful maintenance has emergency containment")
    daemon = observations.get("daemon")
    daemon_valid = False
    if isinstance(daemon, dict) and set(daemon) == {"restart", "check"}:
        restart = daemon.get("restart")
        check = daemon.get("check")
        expected_config = kernel.get("slurm_cgroup_readback")
        daemon_valid = bool(
            isinstance(restart, dict)
            and isinstance(check, dict)
            and restart == check
            and set(check) == {"state", "cgroup_config"}
            and check.get("state") == "active"
            and check.get("cgroup_config") == expected_config
        )
    if not daemon_valid:
        failures.append(f"{node_name}: Slurm cgroup daemon readback is invalid")
    admission = observations.get("admission")
    admission_valid = False
    if isinstance(admission, dict) and set(admission) == {"builder", "rollout_rejected"}:
        builder = admission.get("builder")
        rollout = admission.get("rollout_rejected")
        resources = policy.raw["resource_profile"]
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
        if _command_receipt_is_valid(builder) and _command_receipt_is_valid(rollout):
            assert isinstance(builder, dict) and isinstance(rollout, dict)
            builder_command = builder["command"]
            rollout_command = rollout["command"]
            assert isinstance(builder_command, list) and isinstance(rollout_command, list)
            admission_valid = bool(
                builder.get("returncode") == 0
                and int(rollout["returncode"]) != 0
                and builder_command
                == [
                    "/usr/sbin/runuser",
                    "--user",
                    "loom-builder",
                    "--",
                    "/usr/bin/sbatch",
                    *admission_args,
                ]
                and rollout_command
                == [
                    "/usr/sbin/runuser",
                    "--user",
                    "loom-rollout",
                    "--",
                    "/usr/bin/sbatch",
                    *admission_args,
                ]
            )
    if not admission_valid:
        failures.append(f"{node_name}: Slurm admission evidence is invalid")
    reservation = observations.get("reservation")
    reservation_name = "loom_task_builder_maintenance_" + str(operation_id).replace("-", "")
    reservation_keys = {
        "name",
        "prior_readback",
        "prior_absence",
        "create",
        "create_readback",
        "binding",
        "delete",
        "delete_readback",
        "absence",
    }
    expected_binding = {
        "name": reservation_name,
        "node": node_name,
        "state": "ACTIVE",
        "user": "loom-builder",
    }
    command_keys = (
        "prior_readback",
        "create",
        "create_readback",
        "delete",
        "delete_readback",
    )
    expected_commands = {
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
    }
    prior_rows = (
        _reservation_rows(reservation.get("prior_readback"))
        if isinstance(reservation, dict)
        else None
    )
    created_rows = (
        _reservation_rows(reservation.get("create_readback"))
        if isinstance(reservation, dict)
        else None
    )
    deleted_rows = (
        _reservation_rows(reservation.get("delete_readback"))
        if isinstance(reservation, dict)
        else None
    )
    created_fields = created_rows.get(reservation_name) if created_rows is not None else None
    raw_reservation_valid = bool(
        prior_rows is not None
        and deleted_rows is not None
        and reservation_name not in prior_rows
        and reservation_name not in deleted_rows
        and created_fields is not None
        and created_fields.get("Nodes") == node_name
        and created_fields.get("Users") == "loom-builder"
        and created_fields.get("State") == "ACTIVE"
    )
    reservation_valid = bool(
        isinstance(reservation, dict)
        and set(reservation) == reservation_keys
        and reservation.get("name") == reservation_name
        and reservation.get("prior_absence") == {"name": reservation_name, "absent": True}
        and reservation.get("binding") == expected_binding
        and reservation.get("absence") == {"name": reservation_name, "absent": True}
        and raw_reservation_valid
        and all(
            _command_receipt_is_valid(reservation.get(key))
            and isinstance(reservation.get(key), dict)
            and reservation[key].get("returncode") == 0
            and reservation[key].get("command") == expected_commands[key]
            for key in command_keys
        )
    )
    if not reservation_valid:
        failures.append(f"{node_name}: reservation lifecycle is invalid")
    if isinstance(reservation, dict):
        created = _event(events, "reservation_created")
        deleted = _event(events, "reservation_deleted")
        if (
            created is None
            or created.get("data")
            != {
                "name": reservation_name,
                "create": reservation.get("create"),
                "create_readback": reservation.get("create_readback"),
                "binding": expected_binding,
            }
            or deleted is None
            or deleted.get("data")
            != {
                "name": reservation_name,
                "delete": reservation.get("delete"),
                "delete_readback": reservation.get("delete_readback"),
                "absence": {"name": reservation_name, "absent": True},
            }
        ):
            failures.append(f"{node_name}: reservation event chain is invalid")
    smoke = observations.get("smoke")
    if not isinstance(smoke, dict) or set(smoke) != {
        "job_id",
        "allocation",
        "cgroup",
        "cgroup_path",
        "cleanup",
    }:
        failures.append(f"{node_name}: maintenance smoke evidence is invalid")
        return failures
    job_id = smoke.get("job_id")
    cgroup_path = smoke.get("cgroup_path")
    controls = smoke.get("cgroup")
    allocation = smoke.get("allocation")
    if (
        not isinstance(job_id, str)
        or re.fullmatch(r"[1-9][0-9]*", job_id) is None
        or allocation != {"node": node_name, "sole_first_allocation": True}
        or not isinstance(cgroup_path, str)
        or re.fullmatch(
            rf"/[A-Za-z0-9_./:-]*/job_{re.escape(job_id)}/step_[A-Za-z0-9_.:-]+",
            cgroup_path,
        )
        is None
        or not isinstance(controls, dict)
    ):
        failures.append(f"{node_name}: maintenance smoke allocation is invalid")
        return failures
    resources = policy.raw["resource_profile"]
    devices = controls.get("devices")
    programs = devices.get("programs") if isinstance(devices, dict) else None
    observed_cpu_count = _cpuset_cpu_count(controls.get("cpuset_cpus_effective"))
    if (
        set(controls)
        != {
            "cpuset_cpus_effective",
            "cpuset_cpu_count",
            "memory_max",
            "memory_swap_max",
            "devices",
        }
        or observed_cpu_count is None
        or controls.get("cpuset_cpu_count") != observed_cpu_count
        or observed_cpu_count != resources["cpus"]
        or controls.get("memory_max") != resources["memory_mib"] * 1024 * 1024
        or controls.get("memory_swap_max") != resources["swap_bytes"]
        or not isinstance(devices, dict)
        or set(devices) != {"cgroup_path", "programs"}
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
        failures.append(f"{node_name}: smoke device containment evidence is invalid")
    smoke_observed = _event(events, "smoke_observed")
    expected_observed = {
        "job_id": job_id,
        "evidence": {
            "schema": "loom.task-image-builder-maintenance-smoke/v1",
            "operation_id": operation_id,
            "job_id": job_id,
            "cgroup_path": cgroup_path,
            "controls": controls,
        },
    }
    if smoke_observed is None or smoke_observed.get("data") != expected_observed:
        failures.append(f"{node_name}: smoke device containment is not receipt-bound")
    released = _event(events, "smoke_released")
    released_data = released.get("data") if released is not None else None
    release = released_data.get("release") if isinstance(released_data, dict) else None
    release_command = (
        {key: item for key, item in release.items() if key != "outcome"}
        if isinstance(release, dict)
        else None
    )
    if (
        not isinstance(released_data, dict)
        or released_data.get("job_id") != job_id
        or not isinstance(release, dict)
        or set(release) != {"command", "returncode", "stdout", "stderr", "outcome"}
        or not _command_receipt_is_valid(release_command)
        or release.get("returncode") != 0
        or not _maintenance_command_matches(
            release.get("command"),
            "release",
            job_id,
            operation_id,
        )
        or release.get("stdout") != '{"state":"released"}\n'
        or release.get("outcome") != "released"
    ):
        failures.append(f"{node_name}: smoke release receipt is invalid")
    completed = _event(events, "smoke_completed")
    completed_data = completed.get("data") if completed is not None else None
    completed_mapping = completed_data if isinstance(completed_data, dict) else {}
    accounting = completed_mapping.get("accounting")
    accounting_readback = accounting.get("readback") if isinstance(accounting, dict) else None
    raw_accounting_rows = (
        [
            row
            for row in str(accounting_readback.get("stdout", "")).splitlines()
            if row.split("|", 1)[0] == job_id
        ]
        if isinstance(accounting_readback, dict)
        else []
    )
    if (
        not isinstance(accounting, dict)
        or set(accounting) != {"readback", "top_level"}
        or completed_mapping.get("job_id") != job_id
        or not isinstance(accounting_readback, dict)
        or not _command_receipt_is_valid(accounting_readback)
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
        or raw_accounting_rows != [f"{job_id}|COMPLETED|0:0"]
        or accounting.get("top_level")
        != {"job_id": job_id, "state": "COMPLETED", "exit_code": "0:0"}
    ):
        failures.append(f"{node_name}: smoke accounting is invalid")
    cleaned = _event(events, "smoke_cleaned")
    cleaned_data = cleaned.get("data") if cleaned is not None else None
    cleaned_mapping = cleaned_data if isinstance(cleaned_data, dict) else {}
    cleanup = cleaned_mapping.get("cleanup")
    if (
        smoke.get("cleanup") != CLEANUP_ABSENCE_FACTS
        or not isinstance(cleanup, dict)
        or any(cleanup.get(key) is not True for key in CLEANUP_ABSENCE_FACTS)
        or not _command_receipt_is_valid(
            {key: item for key, item in cleanup.items() if key not in CLEANUP_ABSENCE_FACTS}
        )
        or cleanup.get("returncode") != 0
        or not _maintenance_command_matches(
            cleanup.get("command"),
            "cleanup",
            job_id,
            operation_id,
        )
        or cleanup.get("stdout") != CLEANUP_ABSENCE_STDOUT
        or cleaned_mapping.get("job_id") != job_id
        or storage.get("empty_job_root") is not True
    ):
        failures.append(f"{node_name}: smoke cleanup is invalid")
    if events[-1].get("data") != {"job_id": job_id}:
        failures.append(f"{node_name}: prepared claim is not terminal-receipt-bound")
    return failures


def _node_failures(
    node: Mapping[str, Any],
    *,
    cluster: Mapping[str, Any],
    policy: PrerequisitePolicy,
    collected_at: datetime,
) -> list[str]:
    failures: list[str] = []
    node_name = node["name"]
    failures.extend(_binding_failures(node, policy, label=node_name))
    failures.extend(
        _observation_freshness_failure(
            node.get("observed_at"), collected_at=collected_at, label=node_name
        )
    )
    failures.extend(_slurm_identity_failures(node))
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
        "home": identity_policy["home"],
        "shell": identity_policy["shell"],
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
        or not required_controllers.issubset(kernel["controllers"])
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
    failures.extend(_phase2_guard_failures(node["node_guard"], node_name=node_name))
    failures.extend(
        _package_failures(node["packages"], cluster=cluster, policy=policy, node_name=node_name)
    )
    failures.extend(
        _runtime_raw_failures(runtime, cluster=cluster, policy=policy, node_name=node_name)
    )
    failures.extend(_kernel_raw_failures(kernel, policy=policy, node_name=node_name))
    maintenance_wrapper = node.get("maintenance_receipt")
    maintenance_document = (
        maintenance_wrapper.get("document")
        if isinstance(maintenance_wrapper, Mapping)
        and isinstance(maintenance_wrapper.get("document"), Mapping)
        else None
    )
    failures.extend(
        _storage_raw_failures(
            storage,
            policy=policy,
            node_name=node_name,
            maintenance_document=maintenance_document,
        )
    )
    host_document, host_failures = _host_receipt_failures(
        node["host_receipt"],
        cluster=cluster,
        policy=policy,
        node_name=node_name,
    )
    failures.extend(host_failures)
    failures.extend(
        _maintenance_receipt_failures(
            node["maintenance_receipt"],
            cluster=cluster,
            policy=policy,
            node_name=node_name,
            host_document=host_document,
            kernel=kernel,
            storage=storage,
        )
    )
    if (
        host_document is not None
        and isinstance(host_document.get("post_state"), dict)
        and storage.get("quota") != host_document["post_state"].get("quota_state")
    ):
        failures.append(f"{node_name}: quota readback is not host-receipt-bound")
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
    if item["policy_file_sha256"] != policy.policy_file_digest:
        failures.append("policy digest does not match policy file")
    if item["candidate_sha256"] != policy.candidate_digest:
        failures.append("candidate digest does not match evidence authority")
    failures.extend(_binding_failures(item, policy, label="evidence"))
    if (
        item["release_name"] != policy.release["release"]
        or item["release_sha256"] != policy.release_digest
        or item["runtime_manifest_sha256"] != policy.runtime_digest
    ):
        failures.append("release digest does not match evidence authority")
    if item["production_certification_allowed"] is not False:
        failures.append("production certification must remain disabled in Phase 1")
    if item["certified_nodes"] != []:
        failures.append("certified_nodes must remain empty in Phase 1")
    if item["blockers"] != [INERT_BLOCKER]:
        failures.append("Phase 2 blocker must remain unconditional")
    if item["control_plane_services"] != policy.raw["control_plane_services"]:
        failures.append("control-plane services do not match prerequisite policy")

    collected_at = _parse_collected_at(str(item["collected_at"]))
    age_seconds = (datetime.now(UTC) - collected_at).total_seconds()
    if age_seconds > 3600 or age_seconds < -300:
        failures.append("evidence timestamp is outside the one-hour freshness window")

    policy_clusters = {cluster["id"]: cluster for cluster in policy.raw["clusters"]}
    evidence_clusters = cast(list[dict[str, Any]], item["clusters"])
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
        failures.extend(_binding_failures(observed, policy, label=cluster_id))
        failures.extend(
            _observation_freshness_failure(
                observed.get("observed_at"),
                collected_at=collected_at,
                label=cluster_id,
            )
        )
        failures.extend(
            _controller_identity_failures(
                observed["controller_identity"],
                cluster_id=cluster_id,
                policy=policy,
            )
        )

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
        failures.extend(
            _legacy_builder_failures(
                slurm["legacy_builder"],
                cluster=expected,
                policy=policy,
            )
        )
        failures.extend(
            _slurm_receipt_failures(observed["slurm_receipt"], cluster=expected, policy=policy)
        )

        nodes = observed["nodes"]
        names = [node["name"] for node in nodes]
        if len(names) != len(set(names)):
            failures.append(f"{cluster_id}: duplicate node evidence is forbidden")
        if set(names) != set(expected_nodes) or len(names) != len(expected_nodes):
            failures.append(f"{cluster_id}: node evidence set does not match policy")
        for node in nodes:
            failures.extend(
                _node_failures(
                    node,
                    cluster=expected,
                    policy=policy,
                    collected_at=collected_at,
                )
            )
    return failures


def certification_blockers(_policy: PrerequisitePolicy) -> tuple[str, ...]:
    return (INERT_BLOCKER,)


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
        "blockers": list(certification_blockers(policy)),
        "certified_nodes": [],
    }


def _write_canonical(path: Path, value: Mapping[str, Any]) -> None:
    if path.is_symlink() or path.exists():
        raise ConformanceError("canonical output already exists")
    payload = _canonical_bytes(value) + b"\n"
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except OSError as exc:
        raise ConformanceError("cannot write canonical evidence") from exc
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as exc:
        raise ConformanceError("cannot write canonical evidence") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


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
                        "blockers": list(certification_blockers(policy)),
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
