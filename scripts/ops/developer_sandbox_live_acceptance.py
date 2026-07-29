#!/usr/bin/env python3
"""Persist and verify candidate-bound developer-sandbox live acceptance evidence.

The default command is ``plan`` and is read-only.  This program never submits a
Slurm job, changes a capacity lease, restarts a service, or kills a worker.
Those live actions remain separately authorized operator steps.  The only
mutating commands in this program create a root-owned, crash-safe acceptance
session on the fixed submit host, and every one requires ``--execute``.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import fcntl
import hashlib
import json
import os
import re
import socket
import stat
import sys
import tempfile
import uuid
from collections import defaultdict
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any, cast

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from jsonschema import Draft202012Validator, FormatChecker

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.ops.developer_sandbox_capacity_contract import (  # noqa: E402, I001
    CAPACITY_POLICY_SOURCES as PLATFORM_POLICY_SOURCES,
    CapacityContractError,
    load_capacity_policy,
    load_platform_health_contract,
)
from scripts.ops import nonexclusive_slurm_acceptance as gate6_verifier  # noqa: E402

SCHEMA_VERSION = 2
MAX_ARTIFACT_BYTES = 8 * 1024 * 1024
DEFAULT_SCHEMA = REPO_ROOT / "docs/evidence/developer-sandbox-live-acceptance.schema.json"
STATE_ROOT = Path("/var/lib/loom-developer-sandbox-live-acceptance")
RUNTIME_ATTESTATION_ROOT = Path("/var/lib/loom-shared-capacity/runtime-attestations")
CAPACITY_OBSERVATION_ROOT = Path("/var/lib/loom-shared-capacity/observations")
SERVICE_STATE_ROOT = Path("/srv/loom/developer-sandboxes")
LIVE_AUTHORITY_ROOT = Path(
    "/var/lib/loom-developer-sandbox-live-authority",
)
PLATFORM_HEALTH_AUTHORITY_ROOT = Path(
    "/var/lib/loom-developer-sandbox-platform-health-authority",
)
SLURM_POLICY_STATE_ROOT = Path("/var/lib/loom-developer-sandbox-slurm-policy")
NONEXCLUSIVE_SCHEMA = REPO_ROOT / "docs/evidence/nonexclusive-slurm-acceptance-v1.schema.json"
PROMOTION_AUTHORITY_RECEIPT = Path(
    "/var/lib/loom-staging-rollout/acceptance/promotion.json",
)
PROMOTION_SOURCE_HOST = "trt-eai-oldlab-1"
STAGING_PRESSURE_SOURCE_HOST = "trt-eai-oldlab-1"
STAGING_PRESSURE_PUBLISHED_ROOT = Path(
    "/srv/loom/staging-shared/results/pressure-reclaim",
)
STAGING_PRESSURE_PUBLIC_KEY = Path(
    "/etc/loom/staging-pressure-reclaim/authority-public.pem",
)
REQUIRED_OWNER_UID = 0
REQUIRED_OWNER_GID = 0
SUBMIT_HOST = "trt-eai-oldlab-2"
POOL_AUTHORITY_HOSTS = {"oldlab": SUBMIT_HOST, "gb10": "trt-gb10-1"}
SANDBOXES = ("qianyi", "hongjian", "devansh")
POOLS = ("oldlab", "gb10")
SANDBOX_SERVICE_USERS = {
    "qianyi": "loom-sandbox-qianyi",
    "hongjian": "loom-sandbox-hongjian",
    "devansh": "loom-sandbox-devansh",
}
MAX_OVERLAP_CAPACITY_AGE_SECONDS = 120
MAX_OVERLAP_COLLECTION_SPAN_SECONDS = 30
PLATFORM_HEALTH_EVIDENCE_TTL = timedelta(minutes=15)
POOL_SLOT_BUDGETS = {"oldlab": 20, "gb10": 120}
POOL_PENDING_BUDGETS = {"oldlab": 10, "gb10": 24}
INFRASTRUCTURE_NODES = (
    "trt-eai-oldlab-1",
    "trt-eai-oldlab-2",
    "trt-eai-oldlab-3",
    "trt-eai-oldlab-4",
    "trt-eai-oldlab-5",
    *(f"trt-gb10-{index}" for index in range(1, 16)),
)
RUNTIME_FLEET_INFRASTRUCTURE_NODES = (
    *(f"oldlab-{index}" for index in range(1, 6)),
    *(f"trt-gb10-{index}" for index in range(1, 16)),
)
EXPECTED_NODES = (
    "trt-eai-oldlab-1",
    "trt-eai-oldlab-2",
    "trt-eai-oldlab-3",
    "trt-eai-oldlab-4",
    "trt-eai-oldlab-5",
    "trt-gb10-1",
    "trt-gb10-2",
    "trt-gb10-3",
    "trt-gb10-4",
    "trt-gb10-5",
    "trt-gb10-6",
    "trt-gb10-7",
    "trt-gb10-8",
    "trt-gb10-9",
    "trt-gb10-10",
    "trt-gb10-11",
    "trt-gb10-12",
    "trt-gb10-13",
    "trt-gb10-14",
    "trt-gb10-15",
)
PLATFORM_HEALTH_NODE_KEYS = (
    *(f"oldlab-{index}" for index in range(1, 6)),
    *(f"trt-gb10-{index}" for index in range(1, 16)),
)
PHASES = (
    "preflight",
    "baseline",
    "multi_candidate_overlap",
    "large_batch_burst",
    "fairness_contention",
    "mixed_non_loom",
    "cancel_cleanup",
    "ttl_cleanup",
    "submit_host_restart",
    "worker_crash",
    "final_drain",
)
PHASE_CHECKPOINTS = tuple((phase, sandbox) for phase in PHASES for sandbox in SANDBOXES)
CAPACITY_PHASES = (
    "multi_candidate_overlap",
    "large_batch_burst",
    "fairness_contention",
    "mixed_non_loom",
    "cancel_cleanup",
    "ttl_cleanup",
    "submit_host_restart",
    "worker_crash",
    "final_drain",
)
FAULTS = ("cancel", "ttl_expiry", "submit_host_restart", "worker_crash")
FAULT_PHASES = {
    "cancel": "cancel_cleanup",
    "ttl_expiry": "ttl_cleanup",
    "submit_host_restart": "submit_host_restart",
    "worker_crash": "worker_crash",
}
CROSS_SANDBOX_RESOURCES = ("worker_identity", "object_store", "result_path")
CONTAINER_ROLES = ("worker", "trial", "verifier", "sidecar")
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SESSION_RE = re.compile(r"^[0-9a-f]{32}$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
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
    re.compile(r"\bloom_(?:api|w|admin)_[A-Za-z0-9._-]{8,}", re.IGNORECASE),
    re.compile(r"(?i)(X-Amz-Signature|AWSAccessKeyId|Signature)=[^&\s]+"),
    re.compile(r"://([^:/@\s]+):([^@\s]+)@"),
)


def _platform_policy_contract(pool: str) -> tuple[dict[str, Any], str]:
    try:
        contract = load_capacity_policy(
            REPO_ROOT,
            pool,
            expected_nodes=(EXPECTED_NODES[:5] if pool == "oldlab" else EXPECTED_NODES[5:]),
        )
    except CapacityContractError as exc:
        raise AcceptanceError(str(exc)) from exc
    return dict(contract.values), contract.source_sha256


class AcceptanceError(ValueError):
    """Raised for a controlled, secret-safe acceptance failure."""


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


def _load_schema(path: Path = DEFAULT_SCHEMA) -> dict[str, Any]:
    schema = _json_load(path)
    if not isinstance(schema, dict):
        raise AcceptanceError("schema root must be an object")
    Draft202012Validator.check_schema(schema)
    return schema


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError
    return parsed


def _schema_failures(evidence: Any, schema: Mapping[str, Any]) -> list[str]:
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    failures: list[str] = []
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
    return (
        child_path.is_absolute()
        and parent_path.is_absolute()
        and child_path != parent_path
        and parent_path != PurePosixPath("/")
        and parent_path in child_path.parents
    )


def _matrix(values: Sequence[Mapping[str, Any]], *keys: str) -> set[tuple[Any, ...]]:
    return {tuple(value[key] for key in keys) for value in values}


def _node_in_pool(node: str, pool: str) -> bool:
    prefix = "trt-eai-oldlab-" if pool == "oldlab" else "trt-gb10-"
    return node.startswith(prefix)


def _expected_job_name(sandbox: str, candidate_sha: str, node: str) -> str:
    job_node = re.sub(r"[^A-Za-z0-9_.-]+", "-", node).strip("-") or "worker"
    return f"loom-sandbox-{sandbox}-{candidate_sha[:12]}-{job_node}"[:128]


def _inside_window(value: str, window: tuple[datetime, datetime]) -> bool:
    observed = _timestamp(value)
    return window[0] <= observed <= window[1]


def _interval_inside_window(
    started: str,
    finished: str,
    window: tuple[datetime, datetime],
) -> bool:
    interval_start = _timestamp(started)
    interval_finish = _timestamp(finished)
    return (
        interval_start <= interval_finish
        and window[0] <= interval_start
        and interval_finish <= window[1]
    )


def _candidate_identity(
    evidence: Mapping[str, Any],
    sandbox: str,
) -> tuple[str, str]:
    candidate = evidence["candidates"][sandbox]
    return candidate["sha"], candidate["tree"]


def _candidate_matches(
    evidence: Mapping[str, Any],
    row: Mapping[str, Any],
    sandbox: str,
) -> bool:
    sha, tree = _candidate_identity(evidence, sandbox)
    return bool(row["candidate_sha"] == sha and row["candidate_tree"] == tree)


def _semantic_failures(evidence: Mapping[str, Any]) -> list[str]:
    failures: list[str] = []
    candidates = evidence["candidates"]
    if set(candidates) != set(SANDBOXES):
        failures.append("pre-merge candidate map is not the exact sandbox set")
    candidate_shas = [candidates[sandbox]["sha"] for sandbox in SANDBOXES]
    if len(set(candidate_shas)) != len(SANDBOXES):
        failures.append("pre-merge sandbox candidate SHAs must be distinct")
    session = evidence["session"]
    try:
        started_at = _timestamp(session["started_at"])
        completed_at = _timestamp(session["completed_at"])
        collected_at = _timestamp(session["collected_at"])
    except ValueError:
        return ["session timestamps must be timezone-qualified RFC3339 values"]
    if not started_at < completed_at <= collected_at:
        failures.append("session timestamps are not strictly ordered")
    if (collected_at - completed_at).total_seconds() > session["max_collection_lag_seconds"]:
        failures.append("evidence collection exceeded the freshness window")
    if session["submit_host"] != SUBMIT_HOST:
        failures.append("session submit host is not the fixed acceptance host")
    if not session["execute_acknowledged"]:
        failures.append("live acceptance was not explicitly execute-acknowledged")

    for sandbox in SANDBOXES:
        receipts = candidates[sandbox]["runtime_receipts"]
        if any(receipt["sandbox"] != sandbox for receipt in receipts):
            failures.append(f"{sandbox} runtime receipt is stored under a foreign candidate")
        receipts_by_sandbox: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for receipt in receipts:
            receipts_by_sandbox[receipt["sandbox"]].append(receipt)
        chain = sorted(
            receipts_by_sandbox[sandbox],
            key=lambda item: item["renewal_generation"],
        )
        coverage_until: datetime | None = None
        previous_receipt: Mapping[str, Any] | None = None
        for receipt in chain:
            if not _candidate_matches(evidence, receipt, sandbox):
                failures.append(f"{sandbox} runtime receipt candidate does not match")
            try:
                receipt_collected = _timestamp(receipt["collected_at"])
                receipt_expires = _timestamp(receipt["expires_at"])
            except ValueError:
                failures.append(f"{sandbox} runtime receipt timestamp is invalid")
                continue
            if (
                receipt_expires <= receipt_collected
                or (receipt_expires - receipt_collected).total_seconds() > 900
            ):
                failures.append(f"{sandbox} runtime receipt interval is invalid")
            if previous_receipt is None:
                if receipt_collected > started_at:
                    failures.append(f"{sandbox} runtime receipt chain starts after the session")
            else:
                if receipt["renewal_generation"] <= previous_receipt["renewal_generation"]:
                    failures.append(f"{sandbox} runtime receipt generation does not advance")
                if receipt["previous_payload_sha256"] != previous_receipt["payload_sha256"]:
                    failures.append(f"{sandbox} runtime receipt chain link is invalid")
                if any(
                    receipt["domain_generations"][domain]
                    <= previous_receipt["domain_generations"][domain]
                    for domain in POOLS
                ):
                    failures.append(f"{sandbox} domain receipt generation does not advance")
                if coverage_until is not None and receipt_collected > coverage_until:
                    failures.append(f"{sandbox} runtime receipt chain has a liveness gap")
            coverage_until = receipt_expires
            previous_receipt = receipt
        if not chain or coverage_until is None or coverage_until < completed_at:
            failures.append(f"{sandbox} runtime receipt chain does not cover the session")

    topology = evidence["topology"]
    if tuple(topology["sandboxes"]) != SANDBOXES:
        failures.append("sandbox topology is not the fixed three-sandbox set")
    if tuple(topology["pools"]) != POOLS:
        failures.append("pool topology is not the fixed oldlab/gb10 set")
    if tuple(topology["infrastructure_nodes"]) != INFRASTRUCTURE_NODES:
        failures.append("infrastructure node topology is incomplete or reordered")
    if tuple(topology["eligible_nodes"]) != EXPECTED_NODES:
        failures.append("eligible node topology is incomplete or reordered")
    if topology["excluded_nodes"] != []:
        failures.append("the eligible node topology contains an exclusion")
    if topology["slot_budgets"] != POOL_SLOT_BUDGETS:
        failures.append("pool slot budgets do not match the reviewed contract")
    if topology["pending_slot_budgets"] != POOL_PENDING_BUDGETS:
        failures.append("pool pending budgets do not match the reviewed contract")

    phases = evidence["state_machine"]
    expected_phase_order = [(phase, sandbox) for phase in PHASES for sandbox in SANDBOXES]
    if [(phase["phase"], phase["sandbox"]) for phase in phases] != expected_phase_order:
        failures.append("state-machine phases are incomplete or out of order")
    phase_by_identity: dict[tuple[str, str], Mapping[str, Any]] = {}
    duplicate_phase_identity = False
    for phase in phases:
        phase_identity = (phase["phase"], phase["sandbox"])
        if phase_identity in phase_by_identity:
            failures.append("state-machine phase identity is duplicated")
            duplicate_phase_identity = True
            continue
        phase_by_identity[phase_identity] = phase
    missing_phase_identities = set(expected_phase_order) - set(phase_by_identity)
    if missing_phase_identities:
        failures.append("state-machine phase identity is missing")
    if duplicate_phase_identity or missing_phase_identities:
        return failures

    previous_by_sandbox = {sandbox: started_at for sandbox in SANDBOXES}
    checkpoint_digests: set[str] = set()
    phase_windows: dict[tuple[str, str], tuple[datetime, datetime]] = {}
    for phase_identity in expected_phase_order:
        phase = phase_by_identity[phase_identity]
        sandbox = phase["sandbox"]
        trial_batches = phase.get("trial_batches")
        if (
            (phase["phase"] == "mixed_non_loom")
            != (
                isinstance(trial_batches, dict)
                and set(trial_batches) == set(POOLS)
                and all(
                    isinstance(batch_id, str)
                    and re.fullmatch(
                        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                        r"[0-9a-f]{4}-[0-9a-f]{12}",
                        batch_id,
                    )
                    is not None
                    for batch_id in trial_batches.values()
                )
            )
        ):
            failures.append(f"{phase['phase']} soak trial-batch manifest is invalid")
        if not _candidate_matches(evidence, phase, sandbox):
            failures.append(f"{phase['phase']} is not bound to the exact candidate")
        try:
            phase_started = _timestamp(phase["started_at"])
            phase_finished = _timestamp(phase["finished_at"])
        except ValueError:
            failures.append(f"{phase['phase']} timestamps are invalid")
            continue
        elapsed = (phase_finished - phase_started).total_seconds()
        if phase_started < previous_by_sandbox[sandbox] or phase_finished < phase_started:
            failures.append(f"{phase['phase']} timestamps regress")
        if elapsed > phase["deadline_seconds"]:
            failures.append(f"{phase['phase']} exceeded its bounded deadline")
        phase_windows[(phase["phase"], sandbox)] = (phase_started, phase_finished)
        if phase["checkpoint_sha256"] in checkpoint_digests:
            failures.append("state-machine checkpoint digest is duplicated")
        checkpoint_digests.add(phase["checkpoint_sha256"])
        previous_by_sandbox[sandbox] = phase_finished
    mixed_batch_ids = [
        batch_id
        for phase in phases
        if phase["phase"] == "mixed_non_loom"
        for batch_id in phase["trial_batches"].values()
    ]
    if len(mixed_batch_ids) != len(set(mixed_batch_ids)):
        failures.append("mixed-workload soak batch identity is duplicated")
    if (
        phase_by_identity
        and max(_timestamp(phase["finished_at"]) for phase in phase_by_identity.values())
        > completed_at
    ):
        failures.append("state-machine completion is later than the session")

    overlap_capacity_samples: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for sample in evidence["capacity_samples"]:
        if sample["phase"] == "multi_candidate_overlap":
            overlap_capacity_samples[(sample["sandbox"], sample["pool"])].append(sample)

    overlap_windows = evidence["overlap_windows"]
    if {window["pool"] for window in overlap_windows} != set(POOLS) or len(overlap_windows) != len(
        POOLS
    ):
        failures.append("multi-candidate overlap must contain exactly one window per pool")
    all_overlap_job_ids: set[str] = set()
    for window in overlap_windows:
        try:
            overlap_start = _timestamp(window["started_at"])
            overlap_finish = _timestamp(window["finished_at"])
        except ValueError:
            failures.append(f"{window['pool']} overlap timestamps are invalid")
            continue
        if overlap_start >= overlap_finish:
            failures.append(f"{window['pool']} overlap window is empty")
        observations = window["observations"]
        if {observation["sandbox"] for observation in observations} != set(SANDBOXES) or len(
            observations
        ) != len(SANDBOXES):
            failures.append(f"{window['pool']} overlap does not cover the exact sandbox set")
        job_ids = [observation["job_id"] for observation in observations]
        job_identities = [
            (
                observation["slurm_account"],
                observation["slurm_user"],
                observation["job_name"],
            )
            for observation in observations
        ]
        if len(set(job_ids)) != len(SANDBOXES) or len(job_ids) != len(SANDBOXES):
            failures.append(f"{window['pool']} overlap job IDs are not unique")
        if len(set(job_identities)) != len(SANDBOXES):
            failures.append(f"{window['pool']} overlap job identities are not unique")
        if any(job_id in all_overlap_job_ids for job_id in job_ids):
            failures.append("overlap job ID is reused across pools")
        all_overlap_job_ids.update(job_ids)
        for observation in observations:
            sandbox = observation["sandbox"]
            pool = window["pool"]
            if not _candidate_matches(evidence, observation, sandbox):
                failures.append(
                    f"{window['pool']}/{sandbox} overlap candidate does not match",
                )
            try:
                active_from = _timestamp(observation["active_from"])
                active_until = _timestamp(observation["active_until"])
                observed_at = _timestamp(observation["observed_at"])
            except ValueError:
                failures.append(f"{window['pool']}/{sandbox} overlap timestamp is invalid")
                continue
            if (
                active_from > overlap_start
                or active_until < overlap_finish
                or not overlap_start <= observed_at <= overlap_finish
                or not _interval_inside_window(
                    window["started_at"],
                    window["finished_at"],
                    phase_windows[("multi_candidate_overlap", sandbox)],
                )
            ):
                failures.append(
                    f"{window['pool']}/{sandbox} does not prove the common active window",
                )
            if not observation["service_active"] or not observation["job_active"]:
                failures.append(
                    f"{window['pool']}/{sandbox} service and job were not simultaneously active",
                )
            if observation["service_unit"] != f"loom-developer-sandbox-{sandbox}.service":
                failures.append(
                    f"{window['pool']}/{sandbox} overlap service identity does not match",
                )
            if not _node_in_pool(observation["node"], pool):
                failures.append(f"{window['pool']}/{sandbox} overlap node is in the wrong pool")
            expected_job_name = _expected_job_name(
                sandbox,
                observation["candidate_sha"],
                observation["node"],
            )
            if (
                observation["slurm_account"] != f"loom-dev-{sandbox}"
                or observation["slurm_user"] != SANDBOX_SERVICE_USERS[sandbox]
                or observation["job_name"] != expected_job_name
            ):
                failures.append(f"{pool}/{sandbox} overlap Slurm identity does not match")

            expected_job_readback = {
                "sandbox": sandbox,
                "pool": pool,
                "candidate_sha": observation["candidate_sha"],
                "candidate_tree": observation["candidate_tree"],
                "job_id": observation["job_id"],
                "account": observation["slurm_account"],
                "user": observation["slurm_user"],
                "job_name": observation["job_name"],
                "node": observation["node"],
                "state": "RUNNING",
                "allocation": observation["job_readback"]["allocation"],
                "observed_at": observation["job_readback"]["observed_at"],
            }
            if observation["job_readback"] != expected_job_readback:
                failures.append(f"{pool}/{sandbox} overlap Slurm readback does not match")
            if (
                hashlib.sha256(_canonical_bytes(observation["job_readback"])).hexdigest()
                != observation["job_readback_sha256"]
            ):
                failures.append(f"{pool}/{sandbox} overlap Slurm readback digest does not match")

            if (
                observation["active_candidate_sha"] != observation["candidate_sha"]
                or observation["active_candidate_tree"] != observation["candidate_tree"]
            ):
                failures.append(f"{pool}/{sandbox} overlap service candidate does not match")
            expected_service_readback = {
                "sandbox": sandbox,
                "candidate_sha": observation["active_candidate_sha"],
                "candidate_tree": observation["active_candidate_tree"],
                "unit": observation["service_unit"],
                "active_state": "active",
                "sub_state": "running",
                "observed_at": observation["service_readback"]["observed_at"],
            }
            if observation["service_readback"] != expected_service_readback:
                failures.append(f"{pool}/{sandbox} overlap service readback does not match")
            if (
                hashlib.sha256(_canonical_bytes(observation["service_readback"])).hexdigest()
                != observation["service_readback_sha256"]
            ):
                failures.append(
                    f"{pool}/{sandbox} overlap service readback digest does not match",
                )
            try:
                job_observed = _timestamp(observation["job_readback"]["observed_at"])
                service_observed = _timestamp(observation["service_readback"]["observed_at"])
            except ValueError:
                failures.append(f"{pool}/{sandbox} overlap authority timestamp is invalid")
            else:
                if (
                    not overlap_start <= job_observed <= overlap_finish
                    or not overlap_start <= service_observed <= overlap_finish
                    or job_observed > observed_at
                    or service_observed > observed_at
                    or (observed_at - job_observed).total_seconds()
                    > MAX_OVERLAP_COLLECTION_SPAN_SECONDS
                    or (observed_at - service_observed).total_seconds()
                    > MAX_OVERLAP_COLLECTION_SPAN_SECONDS
                ):
                    failures.append(
                        f"{pool}/{sandbox} overlap authority timestamps are not fresh",
                    )

            bound_samples = overlap_capacity_samples[(sandbox, pool)]
            if len(bound_samples) != 1:
                failures.append(
                    f"{pool}/{sandbox} overlap capacity binding is missing or duplicated",
                )
            else:
                bound_sample = bound_samples[0]
                capacity_binding = observation["capacity_binding"]
                if (
                    capacity_binding["request_id"] != bound_sample["request_id"]
                    or capacity_binding["lease_epoch"] != bound_sample["lease_epoch"]
                    or capacity_binding["observation_sequence"]
                    != bound_sample["observation_sequence"]
                    or capacity_binding["sample_sha256"]
                    != hashlib.sha256(_canonical_bytes(bound_sample)).hexdigest()
                    or bound_sample["job_id"] != observation["job_id"]
                    or bound_sample["account"] != observation["slurm_account"]
                    or bound_sample["user"] != observation["slurm_user"]
                    or bound_sample["job_name"] != observation["job_name"]
                    or bound_sample["node"] != observation["node"]
                    or bound_sample["allocation"] != observation["job_readback"]["allocation"]
                    or bound_sample["active_slots"] < 1
                    or not _candidate_matches(evidence, bound_sample, sandbox)
                    or not overlap_start
                    <= _timestamp(bound_sample["observed_at"])
                    <= overlap_finish
                ):
                    failures.append(
                        f"{pool}/{sandbox} overlap capacity binding does not match",
                    )

    trusted_sequences = [
        observation["trusted_receipt"]["sequence"]
        for window in overlap_windows
        for observation in window["observations"]
    ]
    if sorted(trusted_sequences) != list(
        range(1, len(SANDBOXES) * len(POOLS) + 1),
    ):
        failures.append("overlap trusted receipt sequence is incomplete or replayed")

    negative = evidence["cross_sandbox_negative"]
    expected_negative = {
        (source, target, resource)
        for source in SANDBOXES
        for target in SANDBOXES
        if source != target
        for resource in CROSS_SANDBOX_RESOURCES
    }
    actual_negative = _matrix(negative, "source", "target", "resource")
    if actual_negative != expected_negative or len(negative) != len(expected_negative):
        failures.append("cross-sandbox negative matrix is incomplete or duplicated")
    for probe in negative:
        if probe["source"] == probe["target"]:
            failures.append("cross-sandbox negative matrix contains a same-sandbox row")
        source_sha, source_tree = _candidate_identity(evidence, probe["source"])
        target_sha, target_tree = _candidate_identity(evidence, probe["target"])
        if (
            probe["source_candidate_sha"] != source_sha
            or probe["source_candidate_tree"] != source_tree
            or probe["target_candidate_sha"] != target_sha
            or probe["target_candidate_tree"] != target_tree
        ):
            failures.append("cross-sandbox negative probe candidate pair does not match")
        if not _inside_window(
            probe["observed_at"],
            phase_windows[(probe["phase"], probe["source"])],
        ):
            failures.append("cross-sandbox negative probe is outside its phase window")
        if not probe["denied"]:
            failures.append("cross-sandbox negative probe unexpectedly succeeded")

    samples = evidence["capacity_samples"]
    expected_pairs = {(sandbox, pool) for sandbox in SANDBOXES for pool in POOLS}
    if _matrix(samples, "sandbox", "pool") != expected_pairs:
        failures.append("capacity samples do not cover all sandbox/pool pairs")
    expected_phase_pairs = {
        (phase, sandbox, pool)
        for phase in CAPACITY_PHASES
        for sandbox in SANDBOXES
        for pool in POOLS
    }
    if _matrix(samples, "phase", "sandbox", "pool") != expected_phase_pairs:
        failures.append("capacity samples do not cover every required phase/pair")
    seen_observations: set[tuple[str, int, int]] = set()
    grouped_slots: dict[tuple[str, str], dict[str, int]] = defaultdict(
        lambda: {"committed": 0, "pending": 0},
    )
    last_sequence: dict[tuple[str, str, str], int] = {}
    for sample in samples:
        sandbox = sample["sandbox"]
        pool = sample["pool"]
        if not _candidate_matches(evidence, sample, sandbox):
            failures.append("capacity sample candidate does not match")
        expected_job_name = _expected_job_name(
            sandbox,
            sample["candidate_sha"],
            sample["node"],
        )
        if (
            sample["account"] != f"loom-dev-{sandbox}"
            or sample["user"] != SANDBOX_SERVICE_USERS[sandbox]
            or sample["job_name"] != expected_job_name
            or not _node_in_pool(sample["node"], pool)
        ):
            failures.append("capacity sample job identity does not match")
        allocation = sample["allocation"]
        if (
            not isinstance(allocation["cpu_cores"], (int, float))
            or isinstance(allocation["cpu_cores"], bool)
            or allocation["cpu_cores"] <= 0
            or allocation["memory_bytes"] <= 0
            or allocation["pids"] <= 0
            or allocation["gpu_count"] < 0
            or allocation["exclusive"]
        ):
            failures.append("capacity sample job allocation is invalid")
        try:
            _timestamp(sample["observed_at"])
        except ValueError:
            failures.append("capacity sample timestamp is invalid")
            continue
        if not _inside_window(
            sample["observed_at"],
            phase_windows[(sample["phase"], sample["sandbox"])],
        ):
            failures.append("capacity sample is outside its phase window")
        observation_identity = (
            sample["request_id"],
            sample["lease_epoch"],
            sample["observation_sequence"],
        )
        if observation_identity in seen_observations:
            failures.append("capacity observation identity is duplicated")
        seen_observations.add(observation_identity)
        stream = (sample["sandbox"], sample["pool"], sample["request_id"])
        if sample["observation_sequence"] <= last_sequence.get(stream, -1):
            failures.append("capacity observation sequence is not monotonic")
        last_sequence[stream] = sample["observation_sequence"]
        committed = sample["pending_slots"] + sample["active_slots"] + sample["draining_slots"]
        if (
            committed > sample["granted_slots"]
            or sample["granted_slots"] > sample["requested_slots"]
        ):
            failures.append("capacity counters violate requested/granted/committed bounds")
        bucket = grouped_slots[(sample["phase"], sample["pool"])]
        bucket["committed"] += committed
        bucket["pending"] += sample["pending_slots"]
        if sample["phase"] == "final_drain" and (sample["granted_slots"] != 0 or committed != 0):
            failures.append("final drain retains granted or committed capacity")
    for (phase, pool), counters in grouped_slots.items():
        if counters["committed"] > POOL_SLOT_BUDGETS[pool]:
            failures.append(f"{phase}/{pool} overshoots the slot budget")
        if counters["pending"] > POOL_PENDING_BUDGETS[pool]:
            failures.append(f"{phase}/{pool} overshoots the pending-slot budget")

    bursts = evidence["large_batch_bursts"]
    if _matrix(bursts, "sandbox", "pool") != expected_pairs or len(bursts) != len(
        expected_pairs,
    ):
        failures.append("large-batch evidence must contain one burst per sandbox/pool")
    for burst in bursts:
        if not _candidate_matches(evidence, burst, burst["sandbox"]):
            failures.append("large-batch burst candidate does not match")
        if not _interval_inside_window(
            burst["started_at"],
            burst["finished_at"],
            phase_windows[(burst["phase"], burst["sandbox"])],
        ):
            failures.append(f"{burst['pool']} large batch is outside its phase window")
        if len(set(burst["nodes"])) < 2:
            failures.append(f"{burst['pool']} large batch did not span multiple nodes")
        if any(not _node_in_pool(node, burst["pool"]) for node in burst["nodes"]):
            failures.append(f"{burst['pool']} large batch contains a foreign-pool node")
        terminal = burst["completed_trials"] + burst["failed_trials"] + burst["cancelled_trials"]
        if terminal != burst["trial_count"]:
            failures.append(f"{burst['pool']} large batch terminal counts are incomplete")
        if sum(burst["node_trial_counts"].values()) != burst["trial_count"]:
            failures.append(f"{burst['pool']} node trial counts do not cover the batch")
        if set(burst["node_trial_counts"]) != set(burst["nodes"]):
            failures.append(f"{burst['pool']} node trial-count keys do not match nodes")
        if burst["peak_active_slots"] > burst["granted_slots"]:
            failures.append(f"{burst['pool']} burst exceeds its granted slots")
        if burst["requested_slots"] != POOL_SLOT_BUDGETS[burst["pool"]]:
            failures.append(f"{burst['pool']} burst did not request its reviewed maximum")
        if burst["granted_slots"] > POOL_SLOT_BUDGETS[burst["pool"]]:
            failures.append(f"{burst['pool']} burst grant exceeds the pool budget")

    fairness = evidence["fairness"]
    if {item["pool"] for item in fairness} != set(POOLS) or len(fairness) != len(POOLS):
        failures.append("fairness evidence must contain exactly one window per pool")
    for window in fairness:
        if not _interval_inside_window(
            window["started_at"],
            window["finished_at"],
            (
                max(phase_windows[(window["phase"], sandbox)][0] for sandbox in SANDBOXES),
                min(phase_windows[(window["phase"], sandbox)][1] for sandbox in SANDBOXES),
            ),
        ):
            failures.append(f"{window['pool']} fairness is outside its phase window")
        elif (
            _timestamp(window["finished_at"]) - _timestamp(window["started_at"])
        ).total_seconds() < window["window_seconds"]:
            failures.append(f"{window['pool']} fairness interval is shorter than policy")
        participants = window["participants"]
        if {item["sandbox"] for item in participants} != set(SANDBOXES):
            failures.append(f"{window['pool']} fairness omits a sandbox")
        if len(participants) != len(SANDBOXES):
            failures.append(f"{window['pool']} fairness duplicates a sandbox")
        if len({item["requested_slots"] for item in participants}) != 1:
            failures.append(f"{window['pool']} fairness requests are not equal")
        granted_totals = [item["granted_slots_total"] for item in participants]
        if granted_totals:
            grant_skew = (max(granted_totals) - min(granted_totals)) / max(
                granted_totals,
            )
            if grant_skew > window["max_grant_skew_ratio"]:
                failures.append(f"{window['pool']} fair-share grant skew exceeds policy")
        for participant in participants:
            if not _candidate_matches(evidence, participant, participant["sandbox"]):
                failures.append(
                    f"{window['pool']}/{participant['sandbox']} fairness candidate does not match",
                )
            if participant["first_grant_wait_seconds"] > window["max_grant_wait_seconds"]:
                failures.append(
                    f"{window['pool']}/{participant['sandbox']} exceeded grant wait",
                )
            if participant["grant_cycles"] < 1 or participant["granted_slots_total"] < 1:
                failures.append(
                    f"{window['pool']}/{participant['sandbox']} never received capacity",
                )
            if participant["indefinite_starvation"]:
                failures.append(
                    f"{window['pool']}/{participant['sandbox']} recorded starvation",
                )

    envelopes = evidence["runtime_envelopes"]
    if _matrix(envelopes, "sandbox", "pool") != expected_pairs:
        failures.append("runtime envelopes do not cover all sandbox/pool pairs")
    for envelope in envelopes:
        if not _candidate_matches(evidence, envelope, envelope["sandbox"]):
            failures.append("runtime envelope candidate does not match")
        if not _inside_window(
            envelope["observed_at"],
            phase_windows[(envelope["phase"], envelope["sandbox"])],
        ):
            failures.append("runtime envelope is outside the mixed-load phase")
        allocation = envelope["allocation"]
        if envelope["account"] != f"loom-dev-{envelope['sandbox']}":
            failures.append("runtime envelope Slurm account does not match sandbox")
        if not _node_in_pool(envelope["node"], envelope["pool"]):
            failures.append("runtime envelope node does not match its pool")
        tres = allocation["tres"]
        if "cpu=" not in tres or "mem=" not in tres:
            failures.append("Slurm TRES is missing CPU or memory")
        if envelope["pool"] == "gb10" and allocation["gpu_count"] > 0:
            if "gres/gpu=" not in tres and "gres/gpu:" not in tres:
                failures.append("GB10 Slurm TRES is missing GPU allocation")
        if allocation["exclusive"]:
            failures.append("developer-sandbox Slurm job used --exclusive")
        if not all(
            controller in envelope["cgroup"]["controllers"]
            for controller in ("cpu", "memory", "pids")
        ):
            failures.append("runtime cgroup is missing a required controller")
        roles = [container["role"] for container in envelope["containers"]]
        if set(roles) != set(CONTAINER_ROLES) or len(roles) != len(CONTAINER_ROLES):
            failures.append("runtime envelope must contain exactly four container roles")
        sums = {"cpu_cores": 0.0, "memory_bytes": 0, "pids": 0}
        container_ids: set[str] = set()
        observed_gpu_ids: set[str] = set()
        for container in envelope["containers"]:
            if container["container_id"] in container_ids:
                failures.append("runtime envelope container ID is duplicated")
            container_ids.add(container["container_id"])
            if container["cgroup_parent"] != envelope["cgroup"]["job_path"]:
                failures.append(f"{container['role']} cgroup parent does not match the job")
            if not _strict_descendant(
                container["observed_cgroup_path"],
                envelope["cgroup"]["job_path"],
            ):
                failures.append(f"{container['role']} escaped the Slurm job cgroup")
            for field in sums:
                sums[field] += container["limits"][field]
            if container["observed_limits"] != container["limits"]:
                failures.append(f"{container['role']} configured/observed limits differ")
            observed_gpu_ids.update(container["gpu_ids"])
        for field, total in sums.items():
            if total > allocation[field]:
                failures.append(f"container aggregate {field} exceeds Slurm allocation")
            if total > envelope["cgroup"][f"{field}_max"]:
                failures.append(f"container aggregate {field} exceeds cgroup maximum")
            if envelope["cgroup"][f"{field}_max"] > allocation[field]:
                failures.append(f"cgroup {field} maximum exceeds Slurm allocation")
        if len(observed_gpu_ids) > allocation["gpu_count"]:
            failures.append("container GPU envelope exceeds the Slurm allocation")
        if envelope["pool"] == "oldlab" and observed_gpu_ids:
            failures.append("OLDLAB runtime envelope unexpectedly exposes a GPU")

    peers = evidence["peer_workloads"]
    if {peer["pool"] for peer in peers} != set(POOLS) or len(peers) != len(POOLS):
        failures.append("non-Loom peer evidence must contain exactly one row per pool")
    for peer in peers:
        baseline = peer["baseline"]
        during = peer["during"]
        after = peer["after"]
        try:
            peer_times = [
                _timestamp(baseline["observed_at"]),
                _timestamp(during["observed_at"]),
                _timestamp(after["observed_at"]),
            ]
        except ValueError:
            failures.append(f"{peer['pool']} peer timestamps are invalid")
            peer_times = []
        if peer_times and (
            peer_times != sorted(peer_times)
            or not _inside_window(
                baseline["observed_at"],
                (
                    min(
                        window[0]
                        for (phase, _), window in phase_windows.items()
                        if phase == "baseline"
                    ),
                    max(
                        window[1]
                        for (phase, _), window in phase_windows.items()
                        if phase == "baseline"
                    ),
                ),
            )
            or not _inside_window(
                during["observed_at"],
                (
                    min(
                        window[0]
                        for (phase, _), window in phase_windows.items()
                        if phase == "mixed_non_loom"
                    ),
                    max(
                        window[1]
                        for (phase, _), window in phase_windows.items()
                        if phase == "mixed_non_loom"
                    ),
                ),
            )
            or not _inside_window(
                after["observed_at"],
                (
                    min(
                        window[0]
                        for (phase, _), window in phase_windows.items()
                        if phase == "final_drain"
                    ),
                    max(
                        window[1]
                        for (phase, _), window in phase_windows.items()
                        if phase == "final_drain"
                    ),
                ),
            )
        ):
            failures.append(f"{peer['pool']} peer checkpoints are outside phase windows")
        if peer["disrupted"] or during["failed_jobs"] > baseline["failed_jobs"]:
            failures.append(f"{peer['pool']} non-Loom peer workload was disrupted")
        if baseline["throughput_per_second"] <= 0:
            failures.append(f"{peer['pool']} peer baseline throughput is not positive")
        else:
            regression = 1 - (during["throughput_per_second"] / baseline["throughput_per_second"])
            if regression > peer["max_throughput_regression_ratio"]:
                failures.append(f"{peer['pool']} peer throughput regression exceeds policy")
        if after["failed_jobs"] != baseline["failed_jobs"]:
            failures.append(f"{peer['pool']} peer failure count did not return to baseline")

    overlap_job_ids = {
        observation["job_id"]
        for window in overlap_windows
        for observation in window["observations"]
    }
    runtime_job_ids = [envelope["job_id"] for envelope in envelopes]
    peer_job_ids = [peer["job_id"] for peer in peers]
    if len(set(runtime_job_ids)) != len(runtime_job_ids):
        failures.append("runtime envelope job ID is reused")
    if len(set(peer_job_ids)) != len(peer_job_ids):
        failures.append("peer workload job ID is reused")
    if overlap_job_ids & set(runtime_job_ids):
        failures.append("overlap job ID is reused by a runtime envelope")
    if overlap_job_ids & set(peer_job_ids):
        failures.append("overlap job ID is reused by a peer workload")
    if set(runtime_job_ids) & set(peer_job_ids):
        failures.append("runtime envelope job ID is reused by a peer workload")

    storage = evidence["storage_io"]
    if {item["domain"] for item in storage} != set(POOLS) or len(storage) != len(POOLS):
        failures.append("storage/cache/I/O evidence must cover both domains")
    for item in storage:
        if not (
            _inside_window(
                item["baseline_observed_at"],
                (
                    min(
                        window[0]
                        for (phase, _), window in phase_windows.items()
                        if phase == "baseline"
                    ),
                    max(
                        window[1]
                        for (phase, _), window in phase_windows.items()
                        if phase == "baseline"
                    ),
                ),
            )
            and _inside_window(
                item["minimum_observed_at"],
                (
                    min(
                        window[0]
                        for (phase, _), window in phase_windows.items()
                        if phase == "mixed_non_loom"
                    ),
                    max(
                        window[1]
                        for (phase, _), window in phase_windows.items()
                        if phase == "mixed_non_loom"
                    ),
                ),
            )
            and _inside_window(
                item["after_observed_at"],
                (
                    min(
                        window[0]
                        for (phase, _), window in phase_windows.items()
                        if phase == "final_drain"
                    ),
                    max(
                        window[1]
                        for (phase, _), window in phase_windows.items()
                        if phase == "final_drain"
                    ),
                ),
            )
        ):
            failures.append(f"{item['domain']} storage observations are outside phase windows")
        if item["minimum_free_bytes"] > item["baseline_free_bytes"]:
            failures.append(f"{item['domain']} minimum free space exceeds its baseline")
        if item["minimum_free_bytes"] < item["required_free_bytes"]:
            failures.append(f"{item['domain']} disk free space dropped below policy")
        if item["after_free_bytes"] < item["required_free_bytes"]:
            failures.append(f"{item['domain']} disk did not recover above policy")
        if item["cache_peak_bytes"] > item["cache_limit_bytes"]:
            failures.append(f"{item['domain']} cache exceeded its reviewed bound")
        if item["read_bytes"] > item["read_limit_bytes"]:
            failures.append(f"{item['domain']} read I/O exceeded its reviewed bound")
        if item["write_bytes"] > item["write_limit_bytes"]:
            failures.append(f"{item['domain']} write I/O exceeded its reviewed bound")
        if item["io_errors"] != 0 or item["enospc_events"] != 0:
            failures.append(f"{item['domain']} recorded storage errors")

    faults = evidence["fault_recovery"]
    if {fault["event"] for fault in faults} != set(FAULTS) or len(faults) != len(FAULTS):
        failures.append("fault recovery evidence is incomplete or duplicated")
    for fault in faults:
        if not _candidate_matches(evidence, fault, fault["sandbox"]):
            failures.append(f"{fault['event']} is not candidate-bound")
        if fault["phase"] != FAULT_PHASES[fault["event"]]:
            failures.append(f"{fault['event']} is bound to the wrong phase")
        try:
            injected_at = _timestamp(fault["injected_at"])
            recovered_at = _timestamp(fault["recovered_at"])
        except ValueError:
            failures.append(f"{fault['event']} timestamps are invalid")
            continue
        if (recovered_at - injected_at).total_seconds() > fault["recovery_deadline_seconds"]:
            failures.append(f"{fault['event']} recovery exceeded its deadline")
        if not _interval_inside_window(
            fault["injected_at"],
            fault["recovered_at"],
            phase_windows[(fault["phase"], fault["sandbox"])],
        ):
            failures.append(f"{fault['event']} recovery is outside its phase")
        retry = fault["retry_attribution"]
        if retry["retryable_trials"] != retry["interrupted_trials"]:
            failures.append(f"{fault['event']} interrupted trials are not fully retryable")
        if retry["retried_trials"] > retry["retryable_trials"]:
            failures.append(f"{fault['event']} retried more trials than were attributable")
        if (
            fault["event"] in {"submit_host_restart", "worker_crash"}
            and not retry["interrupted_trials"]
        ):
            failures.append(f"{fault['event']} did not exercise interrupted-trial recovery")
        if retry["duplicate_retries"] or retry["lost_trials"] or retry["unknown_attribution"]:
            failures.append(f"{fault['event']} retry attribution is not closed")
        if (
            fault["orphan_jobs"]
            or fault["orphan_containers"]
            or fault["orphan_leases"]
            or fault["orphan_trials"]
        ):
            failures.append(f"{fault['event']} left an orphan")

    invariants = evidence["invariants"]
    if any(value != 0 for value in invariants.values()):
        failures.append("one or more global acceptance invariants were violated")

    promotion = evidence["promotion_candidate"]
    promotion_phase = promotion["staging_regression"]
    if (
        promotion_phase["phase"] != "promotion_staging_regression"
        or promotion_phase["candidate_sha"] != promotion["sha"]
        or promotion_phase["candidate_tree"] != promotion["tree"]
        or promotion_phase["status"] != "pass"
    ):
        failures.append("promotion staging regression is not bound to its exact candidate")
    if promotion["sha"] in candidate_shas:
        failures.append("promotion candidate must be distinct from pre-merge sandbox SHAs")
    promotion_unsigned = {
        key: value for key, value in promotion_phase.items() if key != "checkpoint_sha256"
    }
    if (
        hashlib.sha256(_canonical_bytes(promotion_unsigned)).hexdigest()
        != promotion_phase["checkpoint_sha256"]
    ):
        failures.append("promotion staging regression checkpoint digest does not match")
    try:
        promotion_started = _timestamp(promotion_phase["started_at"])
        promotion_finished = _timestamp(promotion_phase["finished_at"])
    except ValueError:
        failures.append("promotion staging regression timestamp is invalid")
    else:
        if not completed_at <= promotion_started <= promotion_finished <= collected_at:
            failures.append("promotion staging regression is outside the post-merge window")
    pressure = evidence["staging_pressure_reclaim"]
    pressure_authority = pressure["authority_evidence"]
    pressure_reference = pressure["trusted_receipt"]
    if (
        pressure_authority["kind"] != "loom.staging-pressure-reclaim.receipt"
        or pressure_authority["environment"] != "staging"
        or pressure_authority["pool"] != "gb10"
        or pressure_authority["partition"] != "gb10"
        or pressure_authority["source_host"] != STAGING_PRESSURE_SOURCE_HOST
        or pressure_authority["acceptance_session_id"] != session["id"]
        or pressure_authority["candidate_sha"] != promotion["sha"]
        or pressure_authority["candidate_tree"] != promotion["tree"]
        or pressure_reference["authority_session_id"] != pressure_authority["session_id"]
        or pressure_reference["sequence"] != pressure_authority["sequence"]
        or pressure_reference["source_host"] != STAGING_PRESSURE_SOURCE_HOST
        or pressure_reference["observed_at"] != pressure_authority["issued_at"]
        or pressure_reference["authority_receipt_sha256"]
        != hashlib.sha256(_canonical_bytes(pressure_authority)).hexdigest()
    ):
        failures.append("staging pressure reclaim evidence is not exactly bound")
    else:
        try:
            pressure_observed = _timestamp(pressure_authority["issued_at"])
            pressure_window_start = _timestamp(promotion_phase["started_at"])
            pressure_window_finish = _timestamp(promotion_phase["finished_at"])
        except ValueError:
            failures.append("staging pressure reclaim timestamp is invalid")
        else:
            if not pressure_window_start <= pressure_observed <= pressure_window_finish:
                failures.append(
                    "staging pressure reclaim is outside the promotion staging window",
                )
    platform_health = evidence["platform_health"]
    platform_authority = platform_health["authority_evidence"]
    try:
        _validate_platform_health_authority(
            platform_authority,
            session_id=evidence["session"]["id"],
            candidates={
                sandbox: {
                    "sha": evidence["candidates"][sandbox]["sha"],
                    "tree": evidence["candidates"][sandbox]["tree"],
                }
                for sandbox in SANDBOXES
            },
        )
    except AcceptanceError:
        failures.append("platform-health authority evidence is invalid")
    else:
        reference = platform_health["trusted_receipt"]
        if reference != {
            "receipt_sha256": reference["receipt_sha256"],
            "authority_payload_sha256": platform_authority["payload_sha256"],
            "source_host": SUBMIT_HOST,
            "observed_at": platform_authority["completed_at"],
        }:
            failures.append("platform-health trusted receipt reference is invalid")
    return failures


def verify_evidence(evidence: Any, schema: Mapping[str, Any]) -> list[str]:
    """Return controlled failures without echoing evidence values."""

    _scan_for_secrets(evidence)
    failures = _schema_failures(evidence, schema)
    if failures or not isinstance(evidence, Mapping):
        return failures
    return _semantic_failures(evidence)


def acceptance_plan() -> dict[str, Any]:
    """Return the fixed, non-mutating #1023 live acceptance plan."""

    return {
        "schema_version": SCHEMA_VERSION,
        "mode": "plan_read_only",
        "live_mutations_supported": False,
        "submit_host": SUBMIT_HOST,
        "sandboxes": list(SANDBOXES),
        "pools": list(POOLS),
        "infrastructure_nodes": list(INFRASTRUCTURE_NODES),
        "eligible_nodes": list(EXPECTED_NODES),
        "excluded_nodes": [],
        "state_machine": list(PHASES),
        "faults": list(FAULTS),
        "requirements": [
            "closed-world qianyi/hongjian/devansh map with three distinct full SHAs, each with a full tree",
            "every sandbox phase and runtime record binds its own candidate-map entry",
            "both pools prove a common interval with all three candidate-distinct services and jobs active",
            "overlap jobs have unique IDs and exact account/user/name plus canonical Slurm readbacks",
            "overlap services have exact active candidate/unit/status readbacks and capacity-sample bindings",
            "all 18 directed cross-sandbox resource probes are denied",
            "large batches span multiple nodes in both pools",
            "all three sandboxes receive fair capacity without overshoot or starvation",
            "non-Loom Slurm peers retain bounded throughput and zero new failures",
            "Slurm allocations are non-exclusive and bind CPU/memory/PID/GPU TRES",
            "all runtime containers remain inside the finite Slurm cgroup envelope",
            "disk, cache, read I/O, and write I/O stay within reviewed bounds",
            "cancel, TTL, submit-host restart, and worker crash leave no orphans",
            "interrupted-trial retries retain complete, unique attribution",
            "every observation and interval falls inside its exact phase window",
            "the exact squash-merged staging regression uses the independent promotion candidate",
        ],
        "stop_rules": [
            "Stop unless all three exact pre-merge candidates are installed and read back on both domains.",
            "Stop if candidate SHAs are reused across sandboxes or a receipt crosses map entries.",
            "Stop unless a real three-candidate overlap window is observed in each pool.",
            "Stop on reused overlap job identity or any readback/capacity digest mismatch.",
            "Stop unless separate live-mutation authority has been recorded.",
            "Stop if submit host, sandbox, pool, or eligible-node identity differs.",
            "Stop on any secret-like evidence field or value.",
            "Stop before pressure if the non-Loom baseline is unhealthy.",
            "Stop on capacity overshoot, duplicate observation, or cgroup escape.",
            "Stop and drain on peer disruption, storage error, or freshness failure.",
            "Never restart or admit new work on a busy node and never add --exclusive.",
        ],
    }


def _canonical_bytes(payload: Any) -> bytes:
    return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _canonical_digest_bytes(payload: Any) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def _write_exclusive(path: Path, payload: Mapping[str, Any]) -> None:
    data = _canonical_bytes(payload)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except OSError as exc:
        raise AcceptanceError("cannot create acceptance artifact") from exc


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _validate_secure_directory(path: Path) -> os.stat_result:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise AcceptanceError("acceptance state directory is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != REQUIRED_OWNER_UID
        or metadata.st_gid != REQUIRED_OWNER_GID
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise AcceptanceError("acceptance state directory has unsafe ownership or mode")
    return metadata


def _ensure_secure_directory(path: Path) -> None:
    try:
        os.mkdir(path, 0o700)
    except FileExistsError:
        pass
    except OSError as exc:
        raise AcceptanceError("cannot create acceptance state directory") from exc
    _validate_secure_directory(path)


def _ensure_state_tree(*, create: bool) -> None:
    if create:
        _ensure_secure_directory(STATE_ROOT)
        _ensure_secure_directory(STATE_ROOT / "sessions")
    else:
        _validate_secure_directory(STATE_ROOT)
        _validate_secure_directory(STATE_ROOT / "sessions")


def _validate_secure_file_metadata(metadata: os.stat_result) -> None:
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != REQUIRED_OWNER_UID
        or metadata.st_gid != REQUIRED_OWNER_GID
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise AcceptanceError("acceptance state file has unsafe ownership or mode")


def _secure_file_metadata(path: Path) -> os.stat_result:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise AcceptanceError("acceptance state file is unavailable") from exc
    _validate_secure_file_metadata(metadata)
    return metadata


def _secure_bytes_load(path: Path) -> bytes:
    _validate_secure_directory(path.parent)
    before = _secure_file_metadata(path)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise AcceptanceError("cannot open acceptance state file safely") from exc
    try:
        opened = os.fstat(descriptor)
        _validate_secure_file_metadata(opened)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise AcceptanceError("acceptance state file changed before read")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            data = handle.read(MAX_ARTIFACT_BYTES + 1)
        if len(data) > MAX_ARTIFACT_BYTES:
            raise AcceptanceError("acceptance state file exceeds the size limit")
        after = path.lstat()
        _validate_secure_file_metadata(after)
        if (after.st_dev, after.st_ino) != (opened.st_dev, opened.st_ino):
            raise AcceptanceError("acceptance state file changed during read")
        return data
    except OSError as exc:
        raise AcceptanceError("cannot read acceptance state file safely") from exc
    finally:
        os.close(descriptor)


def _secure_json_load(path: Path) -> Any:
    try:
        return json.loads(_secure_bytes_load(path))
    except json.JSONDecodeError as exc:
        raise AcceptanceError("acceptance state file contains invalid JSON") from exc


def _runtime_attestation_bytes(path: Path) -> bytes:
    root = RUNTIME_ATTESTATION_ROOT
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise AcceptanceError("runtime receipt is outside the root-owned history") from exc
    if not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
        raise AcceptanceError("runtime receipt path is invalid")
    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    root_parts = root.parts[1:]
    try:
        for index, part in enumerate((*root_parts, *relative.parts[:-1])):
            child = os.open(
                part,
                os.O_RDONLY
                | os.O_DIRECTORY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = child
            if index >= len(root_parts) - 1:
                metadata = os.fstat(descriptor)
                if (
                    metadata.st_uid != REQUIRED_OWNER_UID
                    or metadata.st_gid != REQUIRED_OWNER_GID
                    or stat.S_IMODE(metadata.st_mode) & 0o022
                ):
                    raise AcceptanceError("runtime receipt directory is unsafe")
        leaf = relative.parts[-1]
        receipt_fd = os.open(
            leaf,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=descriptor,
        )
        try:
            opened = os.fstat(receipt_fd)
            rebound = os.stat(leaf, dir_fd=descriptor, follow_symlinks=False)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink != 1
                or opened.st_uid != REQUIRED_OWNER_UID
                or opened.st_gid != REQUIRED_OWNER_GID
                or stat.S_IMODE(opened.st_mode) != 0o600
                or (opened.st_dev, opened.st_ino) != (rebound.st_dev, rebound.st_ino)
            ):
                raise AcceptanceError("runtime receipt file is unsafe")
            chunks: list[bytes] = []
            remaining = MAX_ARTIFACT_BYTES + 1
            while remaining:
                chunk = os.read(receipt_fd, remaining)
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            data = b"".join(chunks)
            if len(data) > MAX_ARTIFACT_BYTES:
                raise AcceptanceError("runtime receipt exceeds the size limit")
            return data
        finally:
            os.close(receipt_fd)
    except AcceptanceError:
        raise
    except OSError as exc:
        raise AcceptanceError("cannot read root-owned runtime receipt") from exc
    finally:
        os.close(descriptor)


def _trusted_authority_bytes(path: Path, root: Path, *, label: str) -> bytes:
    """Read one fixed authority file through a nofollow directory-FD walk."""

    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise AcceptanceError(f"{label} path is outside its fixed authority root") from exc
    if not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
        raise AcceptanceError(f"{label} path is invalid")
    flags = (
        os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(root, flags)
    except OSError as exc:
        raise AcceptanceError(f"{label} authority root is unavailable") from exc
    try:
        root_metadata = os.fstat(descriptor)
        if (
            root_metadata.st_uid != REQUIRED_OWNER_UID
            or root_metadata.st_gid != REQUIRED_OWNER_GID
            or stat.S_IMODE(root_metadata.st_mode) & 0o022
        ):
            raise AcceptanceError(f"{label} authority root is unsafe")
        for part in relative.parts[:-1]:
            child = os.open(part, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
            metadata = os.fstat(descriptor)
            if (
                metadata.st_uid != REQUIRED_OWNER_UID
                or metadata.st_gid != REQUIRED_OWNER_GID
                or stat.S_IMODE(metadata.st_mode) & 0o022
            ):
                raise AcceptanceError(f"{label} authority directory is unsafe")
        leaf = relative.parts[-1]
        before = os.stat(leaf, dir_fd=descriptor, follow_symlinks=False)
        receipt_fd = os.open(
            leaf,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=descriptor,
        )
        try:
            opened = os.fstat(receipt_fd)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink != 1
                or opened.st_uid != REQUIRED_OWNER_UID
                or opened.st_gid != REQUIRED_OWNER_GID
                or stat.S_IMODE(opened.st_mode) != 0o600
                or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
            ):
                raise AcceptanceError(f"{label} authority file is unsafe")
            chunks: list[bytes] = []
            remaining = MAX_ARTIFACT_BYTES + 1
            while remaining:
                chunk = os.read(receipt_fd, remaining)
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            data = b"".join(chunks)
            if len(data) > MAX_ARTIFACT_BYTES:
                raise AcceptanceError(f"{label} authority file exceeds the size limit")
            after = os.stat(leaf, dir_fd=descriptor, follow_symlinks=False)
            if (
                (after.st_dev, after.st_ino) != (opened.st_dev, opened.st_ino)
                or after.st_size != opened.st_size
                or after.st_mtime_ns != opened.st_mtime_ns
            ):
                raise AcceptanceError(f"{label} authority file changed during read")
            return data
        finally:
            os.close(receipt_fd)
    except AcceptanceError:
        raise
    except OSError as exc:
        raise AcceptanceError(f"cannot read {label} authority file safely") from exc
    finally:
        os.close(descriptor)


def _trusted_authority_value(path: Path, root: Path, *, label: str) -> Any:
    raw = _trusted_authority_bytes(path, root, label=label)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AcceptanceError(f"{label} authority file is invalid JSON") from exc
    if raw != _canonical_bytes(payload):
        raise AcceptanceError(f"{label} authority file is not canonical")
    _scan_for_secrets(payload)
    return payload


def _trusted_authority_json(path: Path, root: Path, *, label: str) -> dict[str, Any]:
    payload = _trusted_authority_value(path, root, label=label)
    if not isinstance(payload, dict):
        raise AcceptanceError(f"{label} authority file is not an object")
    return payload


def _verify_trusted_runtime_receipts(evidence: Mapping[str, Any]) -> None:
    for sandbox in SANDBOXES:
        candidate = evidence["candidates"][sandbox]
        sha = candidate["sha"]
        tree = candidate["tree"]
        for reference in candidate["runtime_receipts"]:
            if reference["sandbox"] != sandbox:
                raise AcceptanceError("runtime receipt is stored under a foreign candidate")
            _verify_trusted_runtime_receipt(
                reference,
                sandbox=sandbox,
                sha=sha,
                tree=tree,
            )


def _verify_trusted_runtime_receipt(
    reference: Mapping[str, Any],
    *,
    sandbox: str,
    sha: str,
    tree: str,
) -> None:
    generation = reference["renewal_generation"]
    digest = reference["payload_sha256"]
    expected_path = (
        RUNTIME_ATTESTATION_ROOT / sandbox / sha / "renewals" / f"{generation:020d}-{digest}.json"
    )
    if reference["path"] != str(expected_path):
        raise AcceptanceError("runtime receipt path does not match its identity")
    raw = _runtime_attestation_bytes(expected_path)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AcceptanceError("runtime receipt is invalid JSON") from exc
    if not isinstance(payload, dict) or raw != _canonical_bytes(payload):
        raise AcceptanceError("runtime receipt is not canonical")
    required = {
        "schema_version",
        "kind",
        "sandbox",
        "candidate_sha",
        "candidate_tree",
        "renewal_generation",
        "previous_payload_sha256",
        "collected_at",
        "expires_at",
        "domain_generations",
        "fleet_attestation",
        "combined_receipt",
        "payload_sha256",
    }
    if set(payload) != required:
        raise AcceptanceError("runtime receipt fields are invalid")
    unsigned = {key: value for key, value in payload.items() if key != "payload_sha256"}
    actual_digest = hashlib.sha256(
        json.dumps(
            unsigned,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode(),
    ).hexdigest()
    compared = {
        "sandbox",
        "candidate_sha",
        "candidate_tree",
        "renewal_generation",
        "previous_payload_sha256",
        "collected_at",
        "expires_at",
        "domain_generations",
        "payload_sha256",
    }
    if (
        payload["schema_version"] != 1
        or payload["kind"] != "loom.developer-runtime-attestation-renewal"
        or actual_digest != digest
        or any(payload[key] != reference[key] for key in compared)
    ):
        raise AcceptanceError("runtime receipt identity or digest is invalid")
    combined = payload["combined_receipt"]
    fleet_proof = payload["fleet_attestation"]
    if not isinstance(combined, dict):
        raise AcceptanceError("runtime receipt combined proof is invalid")
    combined_required = {
        "schema_version",
        "kind",
        "sandbox",
        "candidate_sha",
        "candidate_tree",
        "collector",
        "fleet_attestation",
        "domains",
        "payload_sha256",
    }
    if set(combined) != combined_required:
        raise AcceptanceError("runtime receipt combined proof fields are invalid")
    combined_unsigned = {key: value for key, value in combined.items() if key != "payload_sha256"}
    combined_digest = hashlib.sha256(
        json.dumps(
            combined_unsigned,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode(),
    ).hexdigest()
    collector = combined.get("collector")
    domains = combined.get("domains")
    fleet = combined.get("fleet_attestation")
    expected_fleet_nodes = RUNTIME_FLEET_INFRASTRUCTURE_NODES
    if (
        combined["schema_version"] != 1
        or combined["kind"] != "loom.developer-runtime-combined-activation"
        or combined["sandbox"] != sandbox
        or combined["candidate_sha"] != sha
        or combined["candidate_tree"] != tree
        or combined.get("payload_sha256") != combined_digest
        or not isinstance(collector, dict)
        or collector.get("collected_at") != reference["collected_at"]
        or collector.get("expires_at") != reference["expires_at"]
        or not isinstance(domains, dict)
        or set(domains) != set(POOLS)
        or not isinstance(fleet, dict)
        or fleet.get("path")
        != str(
            Path("/var/lib/loom-developer-sandbox-links/attestations")
            / sandbox
            / sha
            / "fleet.json",
        )
    ):
        raise AcceptanceError("runtime receipt combined proof binding is invalid")
    for domain in POOLS:
        row = domains.get(domain)
        base = f"/var/lib/loom-developer-domain-attestations/{sandbox}/{sha}"
        if (
            not isinstance(row, dict)
            or row.get("generation") != reference["domain_generations"][domain]
            or row.get("manifest_path") != f"{base}/{domain}.json"
            or row.get("signature_path") != f"{base}/{domain}.sig"
        ):
            raise AcceptanceError("runtime receipt domain coverage is invalid")
    if not isinstance(fleet_proof, dict):
        raise AcceptanceError("runtime receipt fleet proof is invalid")
    fleet_unsigned = {key: value for key, value in fleet_proof.items() if key != "payload_sha256"}
    fleet_digest = (
        "sha256:"
        + hashlib.sha256(
            json.dumps(
                fleet_unsigned,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode(),
        ).hexdigest()
    )
    fleet_nodes = fleet_proof.get("nodes")
    fleet_server = fleet_proof.get("server")
    fleet_bundle = fleet_proof.get("bundle_generation")
    if (
        fleet_proof.get("sandbox") != sandbox
        or fleet_proof.get("candidate_sha") != sha
        or fleet_proof.get("payload_sha256") != fleet_digest
        or fleet.get("payload_sha256") != fleet_digest
        or fleet_proof.get("generated_at") != fleet.get("generated_at")
        or fleet_proof.get("expires_at") != fleet.get("expires_at")
        or fleet_proof.get("eligible_nodes") != list(expected_fleet_nodes)
        or not isinstance(fleet_nodes, dict)
        or set(fleet_nodes) != set(expected_fleet_nodes)
        or not isinstance(fleet_server, dict)
        or fleet_server.get("node") != "oldlab-2"
        or fleet_server.get("unit_active") is not True
        or fleet_server.get("active_candidate_sha") != sha
        or not isinstance(fleet_bundle, dict)
        or fleet_bundle.get("candidate_sha") != sha
        or any(
            not isinstance(node, dict) or node.get("candidate_sha") != sha
            for node in fleet_nodes.values()
        )
    ):
        raise AcceptanceError("runtime receipt fleet host coverage is invalid")


def _fsync_secure_file(path: Path) -> None:
    """Re-establish leaf and directory durability for an idempotent replay."""

    _validate_secure_directory(path.parent)
    before = _secure_file_metadata(path)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise AcceptanceError("cannot open acceptance state file safely") from exc
    try:
        opened = os.fstat(descriptor)
        _validate_secure_file_metadata(opened)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise AcceptanceError("acceptance state file changed before fsync")
        os.fsync(descriptor)
        after = path.lstat()
        _validate_secure_file_metadata(after)
        if (after.st_dev, after.st_ino) != (opened.st_dev, opened.st_ino):
            raise AcceptanceError("acceptance state file changed during fsync")
        _fsync_directory(path.parent)
    except OSError as exc:
        raise AcceptanceError("cannot fsync acceptance state safely") from exc
    finally:
        os.close(descriptor)


def _prepare_secure_descriptor(descriptor: int) -> None:
    os.fchmod(descriptor, 0o600)
    os.fchown(descriptor, REQUIRED_OWNER_UID, REQUIRED_OWNER_GID)


def _write_secure_exclusive(path: Path, payload: Mapping[str, Any]) -> None:
    _validate_secure_directory(path.parent)
    data = _canonical_bytes(payload)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
        try:
            _prepare_secure_descriptor(descriptor)
            with os.fdopen(descriptor, "wb", closefd=False) as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            os.close(descriptor)
        _secure_file_metadata(path)
        _fsync_directory(path.parent)
    except OSError as exc:
        raise AcceptanceError("cannot create acceptance state file") from exc


def _write_or_verify_secure(path: Path, payload: Mapping[str, Any]) -> None:
    expected = _canonical_bytes(payload)
    _validate_secure_directory(path.parent)
    try:
        path.lstat()
    except FileNotFoundError:
        # Only an artifact that existed before this call is an idempotent crash
        # replay.  A create/fsync/readback failure must propagate even if the
        # leaf became visible, otherwise a failed directory fsync could be
        # mistaken for a durably committed checkpoint.
        _write_secure_exclusive(path, payload)
    except OSError as exc:
        raise AcceptanceError("acceptance state file is unavailable") from exc
    else:
        actual = _secure_bytes_load(path)
        if actual != expected:
            raise AcceptanceError("existing acceptance state file does not match") from None
        _fsync_secure_file(path)


def _atomic_write(path: Path, payload: Mapping[str, Any]) -> None:
    data = _canonical_bytes(payload)
    _validate_secure_directory(path.parent)
    if path.exists() or path.is_symlink():
        _secure_file_metadata(path)
    try:
        descriptor, temporary = tempfile.mkstemp(prefix=".state-", dir=path.parent)
        temporary_path = Path(temporary)
        try:
            _prepare_secure_descriptor(descriptor)
            with os.fdopen(descriptor, "wb", closefd=False) as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.close(descriptor)
            os.replace(temporary_path, path)
            _secure_file_metadata(path)
            _fsync_directory(path.parent)
        finally:
            try:
                os.close(descriptor)
            except OSError:
                pass
            if temporary_path.exists():
                temporary_path.unlink()
    except OSError as exc:
        raise AcceptanceError("cannot persist acceptance session") from exc


def _require_execute(execute: bool) -> None:
    if not execute:
        raise AcceptanceError("state mutation requires explicit --execute")
    if os.geteuid() != 0:
        raise AcceptanceError("state mutation requires root")
    if socket.gethostname().split(".", 1)[0].lower() != SUBMIT_HOST:
        raise AcceptanceError("state mutation requires the fixed submit host")


def _session_dir(session_id: str) -> Path:
    if _SESSION_RE.fullmatch(session_id) is None:
        raise AcceptanceError("invalid session id")
    return STATE_ROOT / "sessions" / session_id


def _validate_session_directory(session_id: str) -> Path:
    _ensure_state_tree(create=False)
    session_dir = _session_dir(session_id)
    _validate_secure_directory(session_dir)
    _validate_secure_directory(session_dir / "checkpoints")
    _validate_secure_directory(session_dir / "trusted-receipts")
    return session_dir


@contextmanager
def _session_lock(
    session_id: str,
    *,
    exclusive: bool,
    create: bool = False,
) -> Iterator[None]:
    session_dir = _validate_session_directory(session_id)
    lock_path = session_dir / "session.lock"
    created = False
    flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
    try:
        if create:
            try:
                descriptor = os.open(
                    lock_path,
                    flags | os.O_CREAT | os.O_EXCL,
                    0o600,
                )
                created = True
            except FileExistsError:
                descriptor = os.open(lock_path, flags)
        else:
            descriptor = os.open(lock_path, flags)
    except OSError as exc:
        raise AcceptanceError("cannot open acceptance session lock safely") from exc
    try:
        if created:
            _prepare_secure_descriptor(descriptor)
            os.fsync(descriptor)
            _fsync_directory(session_dir)
        before = _secure_file_metadata(lock_path)
        opened = os.fstat(descriptor)
        _validate_secure_file_metadata(opened)
        if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
            raise AcceptanceError("acceptance session lock changed during open")
        fcntl.flock(descriptor, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        after = lock_path.lstat()
        _validate_secure_file_metadata(after)
        if (after.st_dev, after.st_ino) != (opened.st_dev, opened.st_ino):
            raise AcceptanceError("acceptance session lock changed while held")
        yield
    except OSError as exc:
        raise AcceptanceError("cannot lock acceptance session safely") from exc
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _session_state_unlocked(session_id: str) -> dict[str, Any]:
    state = _secure_json_load(_session_dir(session_id) / "state.json")
    base_keys = {
        "schema_version",
        "session_id",
        "candidates",
        "submit_host",
        "status",
        "next_phase_index",
        "completed_phases",
        "next_trusted_sequence",
        "trusted_overlap_receipts",
        "promotion_receipt_sha256",
        "platform_health_receipt_sha256",
        "staging_pressure_receipt_sha256",
    }
    if not isinstance(state, dict) or frozenset(state) not in {
        frozenset(base_keys),
        frozenset((*base_keys, "evidence_sha256")),
        frozenset((*base_keys, "evidence_sha256", "gate6_sha256")),
    }:
        raise AcceptanceError("session state has an invalid closed shape")
    completed = state["completed_phases"]
    next_index = state["next_phase_index"]
    trusted_receipts = state["trusted_overlap_receipts"]
    if (
        state["schema_version"] != SCHEMA_VERSION
        or state["session_id"] != session_id
        or not isinstance(state["candidates"], dict)
        or set(state["candidates"]) != set(SANDBOXES)
        or any(
            not isinstance(state["candidates"][sandbox], dict)
            or set(state["candidates"][sandbox]) != {"sha", "tree"}
            or _SHA_RE.fullmatch(str(state["candidates"][sandbox]["sha"])) is None
            or _SHA_RE.fullmatch(str(state["candidates"][sandbox]["tree"])) is None
            for sandbox in SANDBOXES
        )
        or len(
            {state["candidates"][sandbox]["sha"] for sandbox in SANDBOXES},
        )
        != len(SANDBOXES)
        or state["submit_host"] != SUBMIT_HOST
        or state["status"] not in {"running", "complete"}
        or not isinstance(state["next_trusted_sequence"], int)
        or isinstance(state["next_trusted_sequence"], bool)
        or not isinstance(trusted_receipts, list)
        or len(trusted_receipts) > len(SANDBOXES) * len(POOLS)
        or any(
            not isinstance(receipt, dict)
            or set(receipt) != {"sequence", "sandbox", "pool", "receipt_sha256"}
            or receipt["sequence"] != index
            or receipt["sandbox"] not in SANDBOXES
            or receipt["pool"] not in POOLS
            or _DIGEST_RE.fullmatch(str(receipt["receipt_sha256"])) is None
            for index, receipt in enumerate(trusted_receipts, start=1)
        )
        or len(
            {
                (receipt["sandbox"], receipt["pool"])
                for receipt in trusted_receipts
                if isinstance(receipt, dict) and "sandbox" in receipt and "pool" in receipt
            },
        )
        != len(trusted_receipts)
        or state["next_trusted_sequence"] != len(trusted_receipts) + 1
        or (
            state["promotion_receipt_sha256"] is not None
            and _DIGEST_RE.fullmatch(str(state["promotion_receipt_sha256"])) is None
        )
        or (
            state["platform_health_receipt_sha256"] is not None
            and _DIGEST_RE.fullmatch(str(state["platform_health_receipt_sha256"])) is None
        )
        or (
            state["staging_pressure_receipt_sha256"] is not None
            and _DIGEST_RE.fullmatch(str(state["staging_pressure_receipt_sha256"])) is None
        )
        or not isinstance(next_index, int)
        or isinstance(next_index, bool)
        or next_index < 0
        or next_index > len(PHASE_CHECKPOINTS)
        or not isinstance(completed, list)
        or completed != [f"{sandbox}:{phase}" for phase, sandbox in PHASE_CHECKPOINTS[:next_index]]
    ):
        raise AcceptanceError("session state identity or progress is invalid")
    if state["status"] == "complete":
        if (
            next_index != len(PHASE_CHECKPOINTS)
            or _DIGEST_RE.fullmatch(str(state.get("evidence_sha256"))) is None
            or len(trusted_receipts) != len(SANDBOXES) * len(POOLS)
            or state["promotion_receipt_sha256"] is None
            or state["platform_health_receipt_sha256"] is None
            or state["staging_pressure_receipt_sha256"] is None
            or (
                "gate6_sha256" in state
                and _DIGEST_RE.fullmatch(str(state["gate6_sha256"])) is None
            )
        ):
            raise AcceptanceError("complete session state is invalid")
    elif "evidence_sha256" in state or "gate6_sha256" in state:
        raise AcceptanceError("running session state contains a final digest")
    return state


def _session_state(session_id: str) -> dict[str, Any]:
    with _session_lock(session_id, exclusive=False):
        return _session_state_unlocked(session_id)


def start_session(
    candidates: Mapping[str, Mapping[str, str]],
    *,
    execute: bool,
) -> dict[str, Any]:
    """Create a crash-safe, candidate-bound acceptance session."""

    _require_execute(execute)
    if set(candidates) != set(SANDBOXES) or any(
        set(candidates[sandbox]) != {"sha", "tree"}
        or _SHA_RE.fullmatch(candidates[sandbox]["sha"]) is None
        or _SHA_RE.fullmatch(candidates[sandbox]["tree"]) is None
        for sandbox in SANDBOXES
    ):
        raise AcceptanceError("candidate map must contain exact full lowercase Git hashes")
    if len({candidates[sandbox]["sha"] for sandbox in SANDBOXES}) != len(SANDBOXES):
        raise AcceptanceError("sandbox candidate SHA values must be distinct")
    _ensure_state_tree(create=True)
    session_id = uuid.uuid4().hex
    session_dir = _session_dir(session_id)
    try:
        os.mkdir(session_dir, 0o700)
    except OSError as exc:
        raise AcceptanceError("cannot create acceptance session directory") from exc
    _validate_secure_directory(session_dir)
    _ensure_secure_directory(session_dir / "checkpoints")
    _ensure_secure_directory(session_dir / "trusted-receipts")
    _fsync_directory(STATE_ROOT / "sessions")
    state = {
        "schema_version": SCHEMA_VERSION,
        "session_id": session_id,
        "candidates": {
            sandbox: {
                "sha": candidates[sandbox]["sha"],
                "tree": candidates[sandbox]["tree"],
            }
            for sandbox in SANDBOXES
        },
        "submit_host": SUBMIT_HOST,
        "status": "running",
        "next_phase_index": 0,
        "completed_phases": [],
        "next_trusted_sequence": 1,
        "trusted_overlap_receipts": [],
        "promotion_receipt_sha256": None,
        "platform_health_receipt_sha256": None,
        "staging_pressure_receipt_sha256": None,
    }
    with _session_lock(session_id, exclusive=True, create=True):
        _atomic_write(session_dir / "state.json", state)
    return state


def _phase_payload(
    path: Path,
    *,
    phase: str,
    sandbox: str,
    state: Mapping[str, Any],
) -> dict[str, Any]:
    payload = _json_load(path)
    _scan_for_secrets(payload)
    required = {
        "phase",
        "sandbox",
        "candidate_sha",
        "candidate_tree",
        "started_at",
        "finished_at",
        "deadline_seconds",
        "status",
    }
    if phase == "mixed_non_loom":
        required.add("trial_batches")
    if not isinstance(payload, dict) or set(payload) != required:
        raise AcceptanceError("phase evidence has an invalid closed shape")
    if (
        payload["phase"] != phase
        or payload["sandbox"] != sandbox
        or payload["candidate_sha"] != state["candidates"][sandbox]["sha"]
        or payload["candidate_tree"] != state["candidates"][sandbox]["tree"]
        or payload["status"] != "pass"
        or (
            phase == "mixed_non_loom"
            and (
                not isinstance(payload.get("trial_batches"), dict)
                or set(payload["trial_batches"]) != set(POOLS)
                or any(
                    not isinstance(batch_id, str)
                    or re.fullmatch(
                        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                        r"[0-9a-f]{4}-[0-9a-f]{12}",
                        batch_id,
                    )
                    is None
                    for batch_id in payload["trial_batches"].values()
                )
            )
        )
        or not isinstance(payload["deadline_seconds"], int)
        or isinstance(payload["deadline_seconds"], bool)
        or payload["deadline_seconds"] < 1
        or payload["deadline_seconds"] > 7200
    ):
        raise AcceptanceError("phase evidence identity or result does not match")
    try:
        started_at = _timestamp(str(payload["started_at"]))
        finished_at = _timestamp(str(payload["finished_at"]))
    except ValueError as exc:
        raise AcceptanceError("phase evidence timestamp is invalid") from exc
    elapsed = (finished_at - started_at).total_seconds()
    if elapsed < 0 or elapsed > payload["deadline_seconds"]:
        raise AcceptanceError("phase evidence exceeds its bounded deadline")
    return payload


def _checkpoint_payload(
    session_id: str,
    phase_payload: Mapping[str, Any],
    digest: str,
) -> dict[str, Any]:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "session_id": session_id,
        "sandbox": phase_payload["sandbox"],
        "candidate_sha": phase_payload["candidate_sha"],
        "candidate_tree": phase_payload["candidate_tree"],
        "phase": phase_payload["phase"],
        "recorded_at": phase_payload["finished_at"],
        "status": "pass",
        "evidence_sha256": digest,
    }
    if phase_payload["phase"] == "mixed_non_loom":
        payload["trial_batches"] = dict(phase_payload["trial_batches"])
        payload["phase_started_at"] = phase_payload["started_at"]
    return payload


def checkpoint_session(
    session_id: str,
    phase: str,
    sandbox: str,
    phase_evidence_path: Path,
    *,
    execute: bool,
) -> dict[str, Any]:
    """Persist the next exact phase checkpoint and advance the journal."""

    _require_execute(execute)
    with _session_lock(session_id, exclusive=True):
        state = _session_state_unlocked(session_id)
        if state["status"] != "running":
            raise AcceptanceError("session is not running")
        try:
            phase_index = PHASE_CHECKPOINTS.index((phase, sandbox))
        except ValueError as exc:
            raise AcceptanceError("checkpoint phase is invalid") from exc
        if phase_index > state["next_phase_index"]:
            raise AcceptanceError("checkpoint is not the exact next phase")
        phase_payload = _phase_payload(
            phase_evidence_path,
            phase=phase,
            sandbox=sandbox,
            state=state,
        )
        digest = hashlib.sha256(_canonical_bytes(phase_payload)).hexdigest()
        checkpoint = _checkpoint_payload(session_id, phase_payload, digest)
        destination = (
            _session_dir(session_id) / "checkpoints" / f"{phase_index:02d}-{sandbox}-{phase}.json"
        )
        _write_or_verify_secure(destination, checkpoint)
        if phase_index < state["next_phase_index"]:
            return state
        state["completed_phases"].append(f"{sandbox}:{phase}")
        state["next_phase_index"] = phase_index + 1
        _atomic_write(_session_dir(session_id) / "state.json", state)
        return state


def _validate_job_allocation(value: Any) -> dict[str, Any]:
    required = {
        "cpu_cores",
        "memory_bytes",
        "pids",
        "gpu_count",
        "tres",
        "exclusive",
    }
    if (
        not isinstance(value, dict)
        or set(value) != required
        or not isinstance(value["cpu_cores"], (int, float))
        or isinstance(value["cpu_cores"], bool)
        or value["cpu_cores"] <= 0
        or not isinstance(value["memory_bytes"], int)
        or isinstance(value["memory_bytes"], bool)
        or value["memory_bytes"] <= 0
        or not isinstance(value["pids"], int)
        or isinstance(value["pids"], bool)
        or value["pids"] <= 0
        or not isinstance(value["gpu_count"], int)
        or isinstance(value["gpu_count"], bool)
        or value["gpu_count"] < 0
        or not isinstance(value["tres"], str)
        or not value["tres"]
        or value["exclusive"] is not False
    ):
        raise AcceptanceError("trusted job allocation is invalid")
    return value


def _overlap_source_paths(
    *,
    sandbox: str,
    pool: str,
    candidate_sha: str,
    job_id: str,
) -> tuple[Path, Path, Path]:
    if (
        sandbox not in SANDBOXES
        or pool not in POOLS
        or _SHA_RE.fullmatch(candidate_sha) is None
        or re.fullmatch(r"[1-9][0-9]*(?:_[0-9]+)?", job_id) is None
    ):
        raise AcceptanceError("trusted overlap source identity is invalid")
    return (
        CAPACITY_OBSERVATION_ROOT / f"{sandbox}-{pool}.json",
        SERVICE_STATE_ROOT / sandbox / "sandbox-state.json",
        LIVE_AUTHORITY_ROOT / "overlap" / pool / sandbox / candidate_sha / f"{job_id}.json",
    )


def _load_overlap_authority_sources(
    state: Mapping[str, Any],
    *,
    sandbox: str,
    pool: str,
    job_id: str,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any], tuple[Path, Path, Path]]:
    candidate = state["candidates"][sandbox]
    paths = _overlap_source_paths(
        sandbox=sandbox,
        pool=pool,
        candidate_sha=candidate["sha"],
        job_id=job_id,
    )
    capacity_document = _trusted_authority_value(
        paths[0],
        CAPACITY_OBSERVATION_ROOT,
        label="capacity observation",
    )
    sandbox_state = _trusted_authority_json(
        paths[1],
        SERVICE_STATE_ROOT,
        label="sandbox lifecycle state",
    )
    live_observation = _trusted_authority_json(
        paths[2],
        LIVE_AUTHORITY_ROOT,
        label="live overlap observation",
    )
    if (
        not isinstance(capacity_document, list)
        or len(capacity_document) != 1
        or not isinstance(capacity_document[0], dict)
    ):
        raise AcceptanceError("capacity observation must contain exactly one row")
    capacity = capacity_document[0]
    capacity_required = {
        "sandbox",
        "pool_name",
        "candidate_sha",
        "request_id",
        "lease_epoch",
        "capacity_lease_state",
        "observed_at",
        "observation_sequence",
        "pending_slots",
        "active_slots",
        "draining_slots",
        "terminal_slots",
        "payload_sha256",
    }
    sandbox_state_required = {
        "schema_version",
        "sandbox",
        "compose_project",
        "candidate_sha",
        "candidate_tree",
        "source_repo",
        "updated_at",
    }
    live_required = {
        "schema_version",
        "kind",
        "source_host",
        "observed_at",
        "sandbox",
        "pool",
        "candidate_sha",
        "candidate_tree",
        "capacity_observation_sha256",
        "sandbox_state_sha256",
        "capacity_sample",
        "job_readback",
        "service_readback",
    }
    if set(capacity) != capacity_required:
        raise AcceptanceError("capacity observation has an invalid closed shape")
    if set(sandbox_state) != sandbox_state_required:
        raise AcceptanceError("sandbox lifecycle state has an invalid closed shape")
    if set(live_observation) != live_required:
        raise AcceptanceError("live overlap observation has an invalid closed shape")
    unsigned_capacity = dict(capacity)
    capacity_digest = unsigned_capacity.pop("payload_sha256")
    if (
        capacity_digest
        != hashlib.sha256(
            _canonical_digest_bytes(unsigned_capacity),
        ).hexdigest()
    ):
        raise AcceptanceError("capacity observation payload digest is invalid")
    try:
        uuid.UUID(str(capacity["request_id"]))
    except (ValueError, AttributeError) as exc:
        raise AcceptanceError("capacity observation request identity is invalid") from exc
    if (
        _SHA_RE.fullmatch(str(capacity["candidate_sha"])) is None
        or not isinstance(capacity["lease_epoch"], int)
        or isinstance(capacity["lease_epoch"], bool)
        or capacity["lease_epoch"] < 1
        or not isinstance(capacity["observation_sequence"], int)
        or isinstance(capacity["observation_sequence"], bool)
        or capacity["observation_sequence"] < 1
        or any(
            not isinstance(capacity[field], int)
            or isinstance(capacity[field], bool)
            or capacity[field] < 0
            for field in ("pending_slots", "active_slots", "draining_slots", "terminal_slots")
        )
    ):
        raise AcceptanceError("capacity observation values are invalid")
    capacity_document_digest = hashlib.sha256(_canonical_bytes(capacity_document)).hexdigest()
    sandbox_state_digest = hashlib.sha256(_canonical_bytes(sandbox_state)).hexdigest()
    sample = live_observation["capacity_sample"]
    job_readback = live_observation["job_readback"]
    service_readback = live_observation["service_readback"]
    sample_required = {
        "phase",
        "observed_at",
        "sandbox",
        "pool",
        "candidate_sha",
        "candidate_tree",
        "job_id",
        "account",
        "user",
        "job_name",
        "node",
        "allocation",
        "request_id",
        "lease_epoch",
        "observation_sequence",
        "requested_slots",
        "granted_slots",
        "pending_slots",
        "active_slots",
        "draining_slots",
        "terminal_slots",
    }
    job_required = {
        "sandbox",
        "pool",
        "candidate_sha",
        "candidate_tree",
        "job_id",
        "account",
        "user",
        "job_name",
        "node",
        "state",
        "allocation",
        "observed_at",
    }
    service_required = {
        "sandbox",
        "candidate_sha",
        "candidate_tree",
        "unit",
        "active_state",
        "sub_state",
        "observed_at",
    }
    if not isinstance(sample, dict) or set(sample) != sample_required:
        raise AcceptanceError("live capacity sample has an invalid closed shape")
    if not isinstance(job_readback, dict) or set(job_readback) != job_required:
        raise AcceptanceError("live job readback has an invalid closed shape")
    if not isinstance(service_readback, dict) or set(service_readback) != service_required:
        raise AcceptanceError("live service readback has an invalid closed shape")
    _validate_job_allocation(sample["allocation"])
    _validate_job_allocation(job_readback["allocation"])
    expected_job_name = _expected_job_name(sandbox, candidate["sha"], sample["node"])
    sample_job_fields = {
        "sandbox": sandbox,
        "pool": pool,
        "candidate_sha": candidate["sha"],
        "candidate_tree": candidate["tree"],
        "job_id": job_id,
        "account": f"loom-dev-{sandbox}",
        "user": SANDBOX_SERVICE_USERS[sandbox],
        "job_name": expected_job_name,
    }
    adapter_fields = {
        "sandbox": "sandbox",
        "pool": "pool_name",
        "candidate_sha": "candidate_sha",
        "request_id": "request_id",
        "lease_epoch": "lease_epoch",
        "observed_at": "observed_at",
        "observation_sequence": "observation_sequence",
        "pending_slots": "pending_slots",
        "active_slots": "active_slots",
        "draining_slots": "draining_slots",
        "terminal_slots": "terminal_slots",
    }
    if (
        capacity["capacity_lease_state"] != "active"
        or capacity["active_slots"] < 1
        or sandbox_state["schema_version"] != 1
        or sandbox_state["sandbox"] != sandbox
        or sandbox_state["compose_project"] != f"loom-sandbox-{sandbox}"
        or sandbox_state["candidate_sha"] != candidate["sha"]
        or sandbox_state["candidate_tree"] != candidate["tree"]
        or not isinstance(sandbox_state["source_repo"], str)
        or not Path(sandbox_state["source_repo"]).is_absolute()
        or live_observation["schema_version"] != 1
        or live_observation["kind"] != "loom.developer-sandbox.live-overlap-observation"
        or live_observation["source_host"] != POOL_AUTHORITY_HOSTS[pool]
        or live_observation["sandbox"] != sandbox
        or live_observation["pool"] != pool
        or live_observation["candidate_sha"] != candidate["sha"]
        or live_observation["candidate_tree"] != candidate["tree"]
        or live_observation["capacity_observation_sha256"] != capacity_document_digest
        or live_observation["sandbox_state_sha256"] != sandbox_state_digest
        or sample["phase"] != "multi_candidate_overlap"
        or any(sample[key] != value for key, value in sample_job_fields.items())
        or any(sample[key] != capacity[adapter_key] for key, adapter_key in adapter_fields.items())
        or any(job_readback[key] != value for key, value in sample_job_fields.items())
        or job_readback["node"] != sample["node"]
        or job_readback["allocation"] != sample["allocation"]
        or job_readback["state"] != "RUNNING"
        or service_readback["sandbox"] != sandbox
        or service_readback["candidate_sha"] != candidate["sha"]
        or service_readback["candidate_tree"] != candidate["tree"]
        or service_readback["unit"] != f"loom-developer-sandbox-{sandbox}.service"
        or service_readback["active_state"] != "active"
        or service_readback["sub_state"] != "running"
    ):
        raise AcceptanceError("trusted overlap authority sources do not agree")
    try:
        capacity_observed = _timestamp(capacity["observed_at"])
        state_updated = _timestamp(sandbox_state["updated_at"])
        job_observed = _timestamp(job_readback["observed_at"])
        service_observed = _timestamp(service_readback["observed_at"])
        collection_finished = _timestamp(live_observation["observed_at"])
    except ValueError as exc:
        raise AcceptanceError("trusted overlap observed_at is invalid") from exc
    if (
        state_updated > collection_finished
        or capacity_observed > collection_finished
        or job_observed > collection_finished
        or service_observed > collection_finished
        or (collection_finished - capacity_observed).total_seconds()
        > MAX_OVERLAP_CAPACITY_AGE_SECONDS
        or (collection_finished - job_observed).total_seconds()
        > MAX_OVERLAP_COLLECTION_SPAN_SECONDS
        or (collection_finished - service_observed).total_seconds()
        > MAX_OVERLAP_COLLECTION_SPAN_SECONDS
        or abs((job_observed - service_observed).total_seconds())
        > MAX_OVERLAP_COLLECTION_SPAN_SECONDS
    ):
        raise AcceptanceError("trusted overlap authority timestamps are not fresh and ordered")
    return capacity_document, sandbox_state, live_observation, paths


def record_overlap_receipt(
    session_id: str,
    sandbox: str,
    pool: str,
    job_id: str,
    *,
    execute: bool,
) -> dict[str, Any]:
    """Import fixed root-authority overlap facts into an immutable session receipt."""

    _require_execute(execute)
    with _session_lock(session_id, exclusive=True):
        state = _session_state_unlocked(session_id)
        if state["status"] != "running":
            raise AcceptanceError("session is not running")
        if any(
            row["sandbox"] == sandbox and row["pool"] == pool
            for row in state["trusted_overlap_receipts"]
        ):
            raise AcceptanceError("trusted overlap receipt already exists")
        capacity_document, sandbox_state, live_observation, paths = _load_overlap_authority_sources(
            state,
            sandbox=sandbox,
            pool=pool,
            job_id=job_id,
        )
        sample = live_observation["capacity_sample"]
        sequence = state["next_trusted_sequence"]
        receipt_unsigned = {
            "schema_version": SCHEMA_VERSION,
            "kind": "loom.developer-sandbox.overlap-trusted-receipt",
            "session_id": session_id,
            "sequence": sequence,
            "source_host": live_observation["source_host"],
            "observed_at": live_observation["observed_at"],
            "sandbox": sandbox,
            "pool": pool,
            "candidate_sha": sample["candidate_sha"],
            "candidate_tree": sample["candidate_tree"],
            "capacity_observation_document": capacity_document,
            "sandbox_state": sandbox_state,
            "live_observation": live_observation,
            "source_paths": {
                "capacity": str(paths[0]),
                "sandbox_state": str(paths[1]),
                "live_observation": str(paths[2]),
            },
            "source_sha256": {
                "capacity": hashlib.sha256(_canonical_bytes(capacity_document)).hexdigest(),
                "sandbox_state": hashlib.sha256(_canonical_bytes(sandbox_state)).hexdigest(),
                "live_observation": hashlib.sha256(
                    _canonical_bytes(live_observation),
                ).hexdigest(),
            },
        }
        digest = hashlib.sha256(_canonical_bytes(receipt_unsigned)).hexdigest()
        receipt = {**receipt_unsigned, "receipt_sha256": digest}
        destination = (
            _session_dir(session_id)
            / "trusted-receipts"
            / f"{sequence:020d}-{sandbox}-{pool}-{digest}.json"
        )
        _write_secure_exclusive(destination, receipt)
        state["trusted_overlap_receipts"].append(
            {
                "sequence": sequence,
                "sandbox": sandbox,
                "pool": pool,
                "receipt_sha256": digest,
            },
        )
        state["next_trusted_sequence"] = sequence + 1
        _atomic_write(_session_dir(session_id) / "state.json", state)
        return receipt


def record_promotion_receipt(session_id: str, *, execute: bool) -> dict[str, Any]:
    """Import the one fixed staging-rollout authority result into the session."""

    _require_execute(execute)
    with _session_lock(session_id, exclusive=True):
        state = _session_state_unlocked(session_id)
        if state["status"] != "running":
            raise AcceptanceError("session is not running")
        if state["promotion_receipt_sha256"] is not None:
            raise AcceptanceError("promotion receipt already exists")
        authority = _trusted_authority_json(
            PROMOTION_AUTHORITY_RECEIPT,
            PROMOTION_AUTHORITY_RECEIPT.parent,
            label="promotion rollout",
        )
        required = {
            "schema_version",
            "kind",
            "source_host",
            "rollout_id",
            "candidate_sha",
            "candidate_tree",
            "result",
            "observed_at",
        }
        if (
            set(authority) != required
            or authority["schema_version"] != 1
            or authority["kind"] != "loom.staging-rollout.acceptance"
            or authority["source_host"] != PROMOTION_SOURCE_HOST
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", authority["rollout_id"]) is None
            or _SHA_RE.fullmatch(str(authority["candidate_sha"])) is None
            or _SHA_RE.fullmatch(str(authority["candidate_tree"])) is None
            or authority["candidate_sha"]
            in {state["candidates"][sandbox]["sha"] for sandbox in SANDBOXES}
            or authority["result"] != "pass"
        ):
            raise AcceptanceError("promotion rollout authority receipt is invalid")
        try:
            _timestamp(authority["observed_at"])
        except ValueError as exc:
            raise AcceptanceError("promotion rollout observed_at is invalid") from exc
        receipt_unsigned = {
            "schema_version": SCHEMA_VERSION,
            "kind": "loom.developer-sandbox.promotion-trusted-receipt",
            "session_id": session_id,
            "source_host": authority["source_host"],
            "rollout_id": authority["rollout_id"],
            "candidate_sha": authority["candidate_sha"],
            "candidate_tree": authority["candidate_tree"],
            "result": authority["result"],
            "observed_at": authority["observed_at"],
            "authority_receipt": authority,
            "authority_path": str(PROMOTION_AUTHORITY_RECEIPT),
            "authority_sha256": hashlib.sha256(_canonical_bytes(authority)).hexdigest(),
        }
        digest = hashlib.sha256(_canonical_bytes(receipt_unsigned)).hexdigest()
        receipt = {**receipt_unsigned, "receipt_sha256": digest}
        destination = _session_dir(session_id) / "trusted-receipts" / f"promotion-{digest}.json"
        _write_secure_exclusive(destination, receipt)
        state["promotion_receipt_sha256"] = digest
        _atomic_write(_session_dir(session_id) / "state.json", state)
        return receipt


def _pressure_authority_path(
    acceptance_session_id: str,
    authority_session_id: str,
) -> Path:
    try:
        canonical_authority_id = str(uuid.UUID(authority_session_id))
    except ValueError as exc:
        raise AcceptanceError("staging pressure authority session id is invalid") from exc
    if canonical_authority_id != authority_session_id:
        raise AcceptanceError("staging pressure authority session id is not canonical")
    return STAGING_PRESSURE_PUBLISHED_ROOT / acceptance_session_id / f"{authority_session_id}.json"


def _pressure_public_key() -> tuple[Ed25519PublicKey, str]:
    raw = _trusted_authority_bytes(
        STAGING_PRESSURE_PUBLIC_KEY,
        STAGING_PRESSURE_PUBLIC_KEY.parent,
        label="staging pressure public key",
    )
    try:
        key = serialization.load_pem_public_key(raw)
    except ValueError as exc:
        raise AcceptanceError("staging pressure public key is invalid") from exc
    if not isinstance(key, Ed25519PublicKey):
        raise AcceptanceError("staging pressure public key is not Ed25519")
    return key, hashlib.sha256(raw).hexdigest()


def _validate_pressure_snapshot(
    snapshot: Mapping[str, Any],
    *,
    receipt: Mapping[str, Any],
    phase: str,
) -> None:
    required = {
        "schema_version",
        "kind",
        "submit_host",
        "environment",
        "pool",
        "partition",
        "account",
        "qos",
        "phase",
        "session_id",
        "acceptance_session_id",
        "candidate_sha",
        "candidate_tree",
        "observed_at",
        "jobs",
        "snapshot_sha256",
    }
    jobs = snapshot.get("jobs")
    unsigned = {key: value for key, value in snapshot.items() if key != "snapshot_sha256"}
    if (
        set(snapshot) != required
        or snapshot.get("schema_version") != 1
        or snapshot.get("kind") != "loom.staging-pressure-reclaim.observe-result"
        or snapshot.get("submit_host") != "trt-gb10-1"
        or snapshot.get("environment") != "staging"
        or snapshot.get("pool") != "gb10"
        or snapshot.get("partition") != "gb10"
        or snapshot.get("account") != "loom-staging"
        or snapshot.get("qos") != "loom-staging"
        or snapshot.get("phase") != phase
        or snapshot.get("session_id") != receipt["session_id"]
        or snapshot.get("acceptance_session_id") != receipt["acceptance_session_id"]
        or snapshot.get("candidate_sha") != receipt["candidate_sha"]
        or snapshot.get("candidate_tree") != receipt["candidate_tree"]
        or snapshot.get("snapshot_sha256") != hashlib.sha256(_canonical_bytes(unsigned)).hexdigest()
        or not isinstance(jobs, list)
        or any(
            not isinstance(job, dict)
            or set(job) != {"job_id", "user", "account", "qos", "state", "nodes", "name"}
            for job in jobs
        )
    ):
        raise AcceptanceError("staging pressure Slurm snapshot is invalid")
    try:
        _timestamp(str(snapshot["observed_at"]))
    except ValueError as exc:
        raise AcceptanceError("staging pressure snapshot timestamp is invalid") from exc


def _validate_pressure_authority(
    published: Mapping[str, Any],
    *,
    acceptance_session_id: str,
    authority_session_id: str,
    candidate_sha: str,
    candidate_tree: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if set(published) != {
        "schema_version",
        "kind",
        "acceptance_session_id",
        "authority_session_id",
        "candidate_sha",
        "candidate_tree",
        "source_host",
        "published_at",
        "receipt",
        "signature",
    }:
        raise AcceptanceError("staging pressure published receipt shape is invalid")
    receipt = published.get("receipt")
    signature = published.get("signature")
    if not isinstance(receipt, dict) or not isinstance(signature, dict):
        raise AcceptanceError("staging pressure published receipt source is invalid")
    receipt_fields = {
        "schema_version",
        "kind",
        "environment",
        "pool",
        "partition",
        "source_host",
        "submit_host",
        "sequence",
        "session_id",
        "acceptance_session_id",
        "session_sha256",
        "candidate_sha",
        "candidate_tree",
        "issued_at",
        "evidence",
    }
    signature_fields = {
        "schema_version",
        "kind",
        "session_id",
        "receipt_sha256",
        "key_id",
        "signature_base64",
        "signature_sha256",
    }
    evidence = receipt.get("evidence")
    evidence_fields = {
        "registry_before",
        "interrupted_trial_before",
        "claim_probe_before",
        "slurm_before",
        "foreign_peer_snapshot",
        "pressure_on",
        "claim_fence",
        "registry_terminal",
        "interrupted_trial_retryable",
        "slurm_during",
        "pressure_off",
        "claim_recovered",
        "claim_probe_requeued",
        "slurm_after",
        "foreign_peer_zero_impact",
    }
    if (
        published.get("schema_version") != 1
        or published.get("kind") != "loom.staging-pressure-reclaim.published-receipt"
        or published.get("acceptance_session_id") != acceptance_session_id
        or published.get("authority_session_id") != authority_session_id
        or published.get("candidate_sha") != candidate_sha
        or published.get("candidate_tree") != candidate_tree
        or published.get("source_host") != STAGING_PRESSURE_SOURCE_HOST
        or set(receipt) != receipt_fields
        or receipt.get("schema_version") != 1
        or receipt.get("kind") != "loom.staging-pressure-reclaim.receipt"
        or receipt.get("environment") != "staging"
        or receipt.get("pool") != "gb10"
        or receipt.get("partition") != "gb10"
        or receipt.get("source_host") != STAGING_PRESSURE_SOURCE_HOST
        or receipt.get("submit_host") != "trt-gb10-1"
        or receipt.get("session_id") != authority_session_id
        or receipt.get("acceptance_session_id") != acceptance_session_id
        or receipt.get("candidate_sha") != candidate_sha
        or receipt.get("candidate_tree") != candidate_tree
        or published.get("published_at") != receipt.get("issued_at")
        or not isinstance(receipt.get("sequence"), int)
        or isinstance(receipt.get("sequence"), bool)
        or int(receipt["sequence"]) < 1
        or not isinstance(evidence, dict)
        or set(evidence) != evidence_fields
        or set(signature) != signature_fields
        or signature.get("schema_version") != 1
        or signature.get("kind") != "loom.staging-pressure-reclaim.receipt.signature"
        or signature.get("session_id") != authority_session_id
        or signature.get("receipt_sha256") != hashlib.sha256(_canonical_bytes(receipt)).hexdigest()
    ):
        raise AcceptanceError("staging pressure published receipt binding is invalid")
    key, key_id = _pressure_public_key()
    if signature.get("key_id") != key_id:
        raise AcceptanceError("staging pressure receipt key identity is invalid")
    try:
        signature_bytes = base64.b64decode(
            str(signature["signature_base64"]),
            validate=True,
        )
    except (binascii.Error, ValueError) as exc:
        raise AcceptanceError("staging pressure signature encoding is invalid") from exc
    if len(signature_bytes) != 64 or hashlib.sha256(signature_bytes).hexdigest() != signature.get(
        "signature_sha256"
    ):
        raise AcceptanceError("staging pressure signature digest is invalid")
    try:
        key.verify(signature_bytes, _canonical_bytes(receipt))
    except InvalidSignature as exc:
        raise AcceptanceError("staging pressure receipt signature is invalid") from exc
    for phase, key_name in (
        ("before", "slurm_before"),
        ("during", "slurm_during"),
        ("after", "slurm_after"),
    ):
        snapshot = evidence[key_name]
        if not isinstance(snapshot, dict):
            raise AcceptanceError("staging pressure Slurm snapshot is invalid")
        _validate_pressure_snapshot(snapshot, receipt=receipt, phase=phase)
    registry_before = evidence["registry_before"]
    registry_terminal = evidence["registry_terminal"]
    owned_ids = {str(row.get("job_id")) for row in registry_before if isinstance(row, dict)}
    before_peers = [
        row for row in evidence["slurm_before"]["jobs"] if row["job_id"] not in owned_ids
    ]
    if (
        not isinstance(registry_before, list)
        or not registry_before
        or not isinstance(registry_terminal, list)
        or len(registry_before) != len(registry_terminal)
        or any(
            not isinstance(row, dict)
            or row.get("acceptance_owned") is not True
            or row.get("candidate_sha") != candidate_sha
            or row.get("state") not in {"pending", "running"}
            for row in registry_before
        )
        or any(
            not isinstance(row, dict)
            or row.get("state") not in {"completed", "failed", "cancelled", "stale"}
            or row.get("pending_reason")
            not in {
                "cancelled by prod-pressure reclaim",
                "released during prod-pressure reclaim",
            }
            for row in registry_terminal
        )
        or evidence["foreign_peer_snapshot"] != before_peers
        or evidence["slurm_during"]["jobs"] != before_peers
        or evidence["slurm_after"]["jobs"] != before_peers
        or evidence.get("foreign_peer_zero_impact") is not True
        or evidence.get("claim_fence") != {"status": 204, "trial_id": None}
        or evidence.get("claim_recovered", {}).get("state") != "claimed"
        or evidence.get("claim_probe_requeued", {}).get("state") != "queued"
        or evidence.get("interrupted_trial_retryable", {}).get("state") != "queued"
        or evidence.get("interrupted_trial_retryable", {}).get("failure_reason")
        != "prod_capacity_pressure"
        or evidence.get("pressure_on", {}).get("has_pressure") is not True
        or evidence.get("pressure_on", {}).get("drain_intent_active") is not True
        or evidence.get("pressure_on", {}).get("new_staging_claims_allowed") is not False
        or evidence.get("pressure_off", {}).get("has_pressure") is not False
        or evidence.get("pressure_off", {}).get("drain_intent_active") is not False
        or evidence.get("pressure_off", {}).get("new_staging_claims_allowed") is not True
    ):
        raise AcceptanceError("staging pressure reclaim proof is incomplete")
    try:
        _timestamp(str(receipt["issued_at"]))
    except ValueError as exc:
        raise AcceptanceError("staging pressure receipt timestamp is invalid") from exc
    return receipt, signature


def record_staging_pressure_receipt(
    session_id: str,
    authority_session_id: str,
    *,
    execute: bool,
) -> dict[str, Any]:
    """Import one signed staging-only pressure receipt into the live session."""

    _require_execute(execute)
    with _session_lock(session_id, exclusive=True):
        state = _session_state_unlocked(session_id)
        if state["status"] != "running":
            raise AcceptanceError("session is not running")
        if state["staging_pressure_receipt_sha256"] is not None:
            raise AcceptanceError("staging pressure receipt already exists")
        promotion_digest = state["promotion_receipt_sha256"]
        if not isinstance(promotion_digest, str):
            raise AcceptanceError("promotion receipt must exist before staging pressure")
        promotion = _load_session_receipt(
            _session_dir(session_id) / "trusted-receipts" / f"promotion-{promotion_digest}.json",
            expected_digest=promotion_digest,
        )
        candidate_sha = str(promotion["candidate_sha"])
        candidate_tree = str(promotion["candidate_tree"])
        authority_path = _pressure_authority_path(session_id, authority_session_id)
        published = _trusted_authority_json(
            authority_path,
            STAGING_PRESSURE_PUBLISHED_ROOT,
            label="staging pressure",
        )
        receipt, signature = _validate_pressure_authority(
            published,
            acceptance_session_id=session_id,
            authority_session_id=authority_session_id,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
        )
        receipt_unsigned = {
            "schema_version": SCHEMA_VERSION,
            "kind": "loom.developer-sandbox.staging-pressure-trusted-receipt",
            "session_id": session_id,
            "source_host": STAGING_PRESSURE_SOURCE_HOST,
            "observed_at": receipt["issued_at"],
            "authority_session_id": authority_session_id,
            "candidate_sha": candidate_sha,
            "candidate_tree": candidate_tree,
            "sequence": receipt["sequence"],
            "authority_path": str(authority_path),
            "authority_payload_sha256": hashlib.sha256(
                _canonical_bytes(published),
            ).hexdigest(),
            "authority_receipt_sha256": signature["receipt_sha256"],
            "authority_signature_sha256": signature["signature_sha256"],
            "authority_key_id": signature["key_id"],
            "authority_evidence": receipt,
        }
        digest = hashlib.sha256(_canonical_bytes(receipt_unsigned)).hexdigest()
        trusted = {**receipt_unsigned, "receipt_sha256": digest}
        destination = (
            _session_dir(session_id) / "trusted-receipts" / f"staging-pressure-{digest}.json"
        )
        _write_secure_exclusive(destination, trusted)
        state["staging_pressure_receipt_sha256"] = digest
        _atomic_write(_session_dir(session_id) / "state.json", state)
        return trusted


def _validate_platform_health_authority(
    authority: Mapping[str, Any],
    *,
    session_id: str,
    candidates: Mapping[str, Any],
) -> None:
    required = {
        "schema_version",
        "kind",
        "session_id",
        "candidates",
        "collector_host",
        "checkpoints",
        "mixed_jobs",
        "cancelled_jobs",
        "crashed_jobs",
        "node_intervals",
        "policy_capacity",
        "oldlab_capacity_recommendation",
        "zero_orphans",
        "completed_at",
        "expires_at",
        "payload_sha256",
    }
    gate6_observations = authority.get("gate6_observations")
    gate6_enabled = gate6_observations is not None
    unsigned = {key: value for key, value in authority.items() if key != "payload_sha256"}
    checkpoints = authority.get("checkpoints")
    mixed_jobs = authority.get("mixed_jobs")
    intervals = authority.get("node_intervals")
    policy_capacity = authority.get("policy_capacity")
    recommendation = authority.get("oldlab_capacity_recommendation")
    expected_policy = {pool: _platform_policy_contract(pool) for pool in POOLS}
    try:
        health_contract = load_platform_health_contract(REPO_ROOT)
    except CapacityContractError as exc:
        raise AcceptanceError(str(exc)) from exc
    oldlab_capacity = policy_capacity.get("oldlab") if isinstance(policy_capacity, dict) else None
    gb10_capacity = policy_capacity.get("gb10") if isinstance(policy_capacity, dict) else None
    capacity_extra_fields = {
        "minimum_node_cpu_cores",
        "minimum_node_memory_bytes",
        "reserved_cpu_cores_per_node",
        "reserved_memory_mib_per_node",
    }
    expected_capacity_fields = set(expected_policy["oldlab"][0]) | capacity_extra_fields
    derivation_fields = {
        "method",
        "measured_node_count",
        "minimum_observed_node_cpu_cores",
        "minimum_observed_node_memory_bytes",
        "minimum_observed_free_cpu_cores",
        "minimum_observed_free_memory_bytes",
        "minimum_required_free_cpu_cores",
        "minimum_required_free_memory_bytes",
        "maximum_allowed_cpu_busy_ratio",
        "all_nodes_passed",
    }
    derivation = recommendation.get("derivation") if isinstance(recommendation, dict) else None
    gb10_minimum_cpu = (
        gb10_capacity.get("minimum_node_cpu_cores") if isinstance(gb10_capacity, dict) else None
    )
    gb10_minimum_memory = (
        gb10_capacity.get("minimum_node_memory_bytes") if isinstance(gb10_capacity, dict) else None
    )
    gb10_headroom_is_typed = (
        isinstance(gb10_minimum_cpu, int)
        and not isinstance(gb10_minimum_cpu, bool)
        and isinstance(gb10_minimum_memory, int)
        and not isinstance(gb10_minimum_memory, bool)
    )
    expected_gb10_reserved_cpu = (
        cast(int, gb10_minimum_cpu) - expected_policy["gb10"][0]["requested_cpus"]
        if gb10_headroom_is_typed
        else None
    )
    expected_gb10_reserved_memory_mib = (
        cast(int, gb10_minimum_memory) // 1024**2
        - expected_policy["gb10"][0]["requested_memory_mib"]
        if gb10_headroom_is_typed
        else None
    )
    expected_checkpoints = (
        (
            "baseline",
            "mixed_non_loom",
            "cancel_cleanup",
            "ttl_cleanup",
            "submit_host_restart",
            "worker_crash",
            "final_drain",
        )
        if gate6_enabled
        else (
            "baseline",
            "mixed_non_loom",
            "cancel_cleanup",
            "worker_crash",
            "final_drain",
        )
    )
    if (
        set(authority) != (required | ({"gate6_observations"} if gate6_enabled else set()))
        or authority.get("schema_version") != 1
        or authority.get("kind") != "loom.developer-sandbox.platform-health-evidence"
        or authority.get("session_id") != session_id
        or authority.get("candidates") != candidates
        or authority.get("collector_host") != SUBMIT_HOST
        or authority.get("payload_sha256") != hashlib.sha256(_canonical_bytes(unsigned)).hexdigest()
        or not isinstance(checkpoints, list)
        or tuple(row.get("checkpoint") for row in checkpoints if isinstance(row, dict))
        != expected_checkpoints
        or [row.get("sequence") for row in checkpoints if isinstance(row, dict)]
        != list(range(1, len(expected_checkpoints) + 1))
        or not isinstance(mixed_jobs, list)
        or len(mixed_jobs) != len(SANDBOXES) * len(POOLS)
        or not isinstance(intervals, dict)
        or set(intervals) != set(PLATFORM_HEALTH_NODE_KEYS)
        or not isinstance(policy_capacity, dict)
        or set(policy_capacity) != set(POOLS)
        or not isinstance(oldlab_capacity, dict)
        or not isinstance(gb10_capacity, dict)
        or set(oldlab_capacity) != expected_capacity_fields
        or set(gb10_capacity) != expected_capacity_fields
        or any(
            capacity.get(key) != expected_policy[pool][0][key]
            for pool, capacity in (("oldlab", oldlab_capacity), ("gb10", gb10_capacity))
            for key in expected_policy[pool][0]
        )
        or oldlab_capacity.get("reserved_cpu_cores_per_node")
        != health_contract.minimum_oldlab_free_cpu_cores
        or oldlab_capacity.get("reserved_memory_mib_per_node")
        != health_contract.minimum_oldlab_free_memory_bytes // 1024**2
        or not gb10_headroom_is_typed
        or gb10_capacity.get("reserved_cpu_cores_per_node") != expected_gb10_reserved_cpu
        or gb10_capacity.get("reserved_memory_mib_per_node") != expected_gb10_reserved_memory_mib
        or not isinstance(recommendation, dict)
        or set(recommendation)
        != {"schema_version", "pool", "source", "source_sha256", "values", "derivation"}
        or recommendation.get("schema_version") != 1
        or recommendation.get("pool") != "oldlab"
        or recommendation.get("source") != PLATFORM_POLICY_SOURCES["oldlab"]
        or recommendation.get("source_sha256") != expected_policy["oldlab"][1]
        or recommendation.get("values") != oldlab_capacity
        or not isinstance(derivation, dict)
        or set(derivation) != derivation_fields
        or derivation.get("method") != "installed-shared-capacity-policy-v1"
        or derivation.get("measured_node_count") != len(health_contract.oldlab_nodes)
        or derivation.get("minimum_observed_node_cpu_cores")
        != oldlab_capacity.get("minimum_node_cpu_cores")
        or derivation.get("minimum_observed_node_memory_bytes")
        != oldlab_capacity.get("minimum_node_memory_bytes")
        or derivation.get("minimum_required_free_cpu_cores")
        != health_contract.minimum_oldlab_free_cpu_cores
        or derivation.get("minimum_required_free_memory_bytes")
        != health_contract.minimum_oldlab_free_memory_bytes
        or derivation.get("maximum_allowed_cpu_busy_ratio")
        != health_contract.maximum_cpu_busy_ratio
        or derivation.get("all_nodes_passed") is not True
        or not isinstance(derivation.get("minimum_observed_free_cpu_cores"), (int, float))
        or isinstance(derivation.get("minimum_observed_free_cpu_cores"), bool)
        or derivation["minimum_observed_free_cpu_cores"]
        < health_contract.minimum_oldlab_free_cpu_cores
        or not isinstance(derivation.get("minimum_observed_free_memory_bytes"), int)
        or isinstance(derivation.get("minimum_observed_free_memory_bytes"), bool)
        or derivation["minimum_observed_free_memory_bytes"]
        < health_contract.minimum_oldlab_free_memory_bytes
        or authority.get("zero_orphans") is not True
    ):
        raise AcceptanceError("platform-health authority evidence is invalid")
    combinations: set[tuple[str, str]] = set()
    compose_projects: set[str] = set()
    compose_networks: set[str] = set()
    for job in mixed_jobs:
        if not isinstance(job, dict):
            raise AcceptanceError("platform-health mixed job evidence is invalid")
        sandbox = job.get("sandbox")
        node = job.get("node")
        pool = "oldlab" if node in EXPECTED_NODES[:5] else "gb10"
        candidate = candidates.get(sandbox) if isinstance(sandbox, str) else None
        allocation = job.get("allocation")
        cgroup = job.get("cgroup")
        containers = job.get("containers")
        policy = expected_policy[pool][0]
        job_id = job.get("job_id")
        job_path = cgroup.get("job_path") if isinstance(cgroup, dict) else None
        networks = job.get("compose_networks")
        project = job.get("compose_project")
        if (
            sandbox not in SANDBOXES
            or node not in EXPECTED_NODES
            or not isinstance(candidate, Mapping)
            or job.get("candidate_sha") != candidate.get("sha")
            or not isinstance(allocation, dict)
            or allocation.get("exclusive") is not False
            or allocation.get("cpu_cores") != policy["requested_cpus"]
            or allocation.get("memory_bytes") != policy["requested_memory_mib"] * 1024**2
            or allocation.get("pids") != policy["job_pids_max"]
            or allocation.get("gpu_count") != (1 if policy["gpu_tres"] else 0)
            or any(
                not isinstance(allocation.get(field), int)
                or isinstance(allocation[field], bool)
                or allocation[field] < minimum
                for field, minimum in (
                    ("cpu_cores", 1),
                    ("memory_bytes", 1),
                    ("pids", 1),
                    ("gpu_count", 0),
                )
            )
            or not isinstance(cgroup, dict)
            or set(cgroup.get("controllers", ())) != {"cpu", "memory", "pids"}
            or cgroup.get("slurm_job_id") != job_id
            or not isinstance(job_path, str)
            or f"job_{job_id}" not in PurePosixPath(job_path).parts
            or not isinstance(cgroup.get("slurm_pid_cgroup_paths"), list)
            or not cgroup["slurm_pid_cgroup_paths"]
            or any(
                not isinstance(path, str) or not _strict_descendant(path, job_path)
                for path in cgroup["slurm_pid_cgroup_paths"]
            )
            or cgroup.get("cpu_cores_max") != allocation.get("cpu_cores")
            or cgroup.get("memory_bytes_max") != allocation.get("memory_bytes")
            or cgroup.get("pids_max") != allocation.get("pids")
            or not isinstance(containers, list)
            or {container.get("role") for container in containers if isinstance(container, dict)}
            != set(CONTAINER_ROLES)
            or len(containers) != len(CONTAINER_ROLES)
            or any(
                not isinstance(container, dict)
                or container.get("cgroup_parent") != job_path
                or not _strict_descendant(
                    str(container.get("observed_cgroup_path")),
                    job_path,
                )
                or container.get("limits", {}).get("cpu_cores") != policy["container_cpus"]
                or container.get("limits", {}).get("memory_bytes")
                != policy["container_memory_mib"] * 1024**2
                or container.get("limits", {}).get("pids") != policy["container_pids"]
                for container in containers
            )
            or (
                gate6_enabled
                and (
                    not isinstance(job.get("host"), str)
                    or not job["host"]
                    or cgroup.get("delegated") is not True
                    or set(cgroup.get("delegated_controllers", ()))
                    != {"cpu", "memory", "pids"}
                    or not isinstance(cgroup.get("pids_current"), int)
                    or isinstance(cgroup.get("pids_current"), bool)
                    or cgroup["pids_current"] < 0
                    or cgroup["pids_current"] > cgroup["pids_max"]
                    or not isinstance(job.get("device_probe"), dict)
                    or any(
                        not isinstance(container, dict)
                        or set(container.get("identity_labels", {}))
                        != {
                            "loom.sandbox",
                            "loom.candidate_sha",
                            "loom.slurm_job_id",
                            "loom.compose_project",
                        }
                        or not isinstance(container.get("name"), str)
                        or not container["name"]
                        for container in containers
                    )
                )
            )
            or not isinstance(project, str)
            or not project
            or project in compose_projects
            or not isinstance(networks, list)
            or not networks
            or any(
                not isinstance(network, str)
                or not network.startswith(f"{project}_")
                or network in compose_networks
                for network in networks
            )
        ):
            raise AcceptanceError("platform-health mixed job evidence is invalid")
        combinations.add((sandbox, pool))
        compose_projects.add(project)
        compose_networks.update(networks)
    if combinations != {(sandbox, pool) for sandbox in SANDBOXES for pool in POOLS}:
        raise AcceptanceError("platform-health mixed job coverage is incomplete")
    if gate6_enabled:
        if (
            not isinstance(gate6_observations, dict)
            or set(gate6_observations) != {"soak", "device_isolation", "cleanup"}
            or not isinstance(gate6_observations.get("soak"), dict)
            or gate6_observations["soak"].get("required_duration_seconds") != 14_400
            or gate6_observations["soak"].get("required_sample_count") != 120
            or gate6_observations["soak"].get("minimum_trial_success_ratio") != 0.95
            or gate6_observations["soak"].get("duration_seconds", 0) < 14_400
            or gate6_observations["soak"].get("sample_count", 0) < 120
            or gate6_observations["soak"].get("resource_envelope_breaches") != 0
            or not isinstance(gate6_observations.get("device_isolation"), list)
            or {
                (row.get("sandbox"), row.get("pool"))
                for row in gate6_observations["device_isolation"]
                if isinstance(row, dict)
            }
            != {(sandbox, pool) for sandbox in SANDBOXES for pool in POOLS}
            or len(gate6_observations["device_isolation"]) != len(SANDBOXES) * len(POOLS)
            or not isinstance(gate6_observations.get("cleanup"), list)
            or {row.get("event") for row in gate6_observations["cleanup"] if isinstance(row, dict)}
            != {"cancellation", "ttl_expiry", "worker_crash", "submit_host_restart"}
            or len(gate6_observations["cleanup"]) != 4
        ):
            raise AcceptanceError("platform-health gate-6 evidence is invalid")
        soak = gate6_observations["soak"]
        outcomes = soak.get("trial_outcomes")
        numerator = soak.get("trial_success_numerator")
        denominator = soak.get("trial_success_denominator")
        if (
            not isinstance(outcomes, list)
            or len(outcomes) != len(SANDBOXES) * len(POOLS)
            or {
                (row.get("sandbox"), row.get("pool"))
                for row in outcomes
                if isinstance(row, dict)
            }
            != {(sandbox, pool) for sandbox in SANDBOXES for pool in POOLS}
            or not isinstance(numerator, int)
            or isinstance(numerator, bool)
            or not isinstance(denominator, int)
            or isinstance(denominator, bool)
            or denominator <= 0
            or any(
                not isinstance(row, dict)
                or any(
                    not isinstance(row.get(field), int)
                    or isinstance(row.get(field), bool)
                    or row[field] < 0
                    for field in (
                        "terminal_trial_count",
                        "succeeded_trial_count",
                        "failed_trial_count",
                        "cancelled_trial_count",
                        "retried_trial_count",
                        "retry_attempt_count",
                    )
                )
                for row in outcomes
            )
            or any(
                row.get("terminal_trial_count")
                != row.get("succeeded_trial_count", 0)
                + row.get("failed_trial_count", 0)
                + row.get("cancelled_trial_count", 0)
                or row.get("terminal_trial_count", 0) <= 0
                or row.get("success_ratio")
                != row.get("succeeded_trial_count", 0) / row.get("terminal_trial_count", 1)
                for row in outcomes
            )
            or numerator
            != sum(row.get("succeeded_trial_count", 0) for row in outcomes)
            or denominator != sum(row.get("terminal_trial_count", 0) for row in outcomes)
            or soak.get("trial_success_ratio") != numerator / denominator
            or soak["trial_success_ratio"] < soak["minimum_trial_success_ratio"]
        ):
            raise AcceptanceError("platform-health trial outcome accounting is invalid")
    if not authority.get("cancelled_jobs") or not authority.get("crashed_jobs"):
        raise AcceptanceError("platform-health cleanup evidence is incomplete")
    try:
        completed_at = _timestamp(str(authority["completed_at"]))
        expires_at = _timestamp(str(authority["expires_at"]))
    except ValueError as exc:
        raise AcceptanceError("platform-health evidence timestamp is invalid") from exc
    if expires_at - completed_at != PLATFORM_HEALTH_EVIDENCE_TTL:
        raise AcceptanceError("platform-health evidence expiry is invalid")


def record_platform_health_receipt(session_id: str, *, execute: bool) -> dict[str, Any]:
    """Import the fixed root-owned platform-health result into the session."""

    _require_execute(execute)
    with _session_lock(session_id, exclusive=True):
        state = _session_state_unlocked(session_id)
        if state["status"] != "running":
            raise AcceptanceError("session is not running")
        if state["platform_health_receipt_sha256"] is not None:
            raise AcceptanceError("platform-health receipt already exists")
        authority_path = PLATFORM_HEALTH_AUTHORITY_ROOT / "sessions" / session_id / "evidence.json"
        authority = _trusted_authority_json(
            authority_path,
            PLATFORM_HEALTH_AUTHORITY_ROOT,
            label="platform-health",
        )
        _validate_platform_health_authority(
            authority,
            session_id=session_id,
            candidates=state["candidates"],
        )
        receipt_unsigned = {
            "schema_version": SCHEMA_VERSION,
            "kind": "loom.developer-sandbox.platform-health-trusted-receipt",
            "session_id": session_id,
            "source_host": SUBMIT_HOST,
            "observed_at": authority["completed_at"],
            "authority_path": str(authority_path),
            "authority_payload_sha256": authority["payload_sha256"],
            "authority_evidence": authority,
        }
        digest = hashlib.sha256(_canonical_bytes(receipt_unsigned)).hexdigest()
        receipt = {**receipt_unsigned, "receipt_sha256": digest}
        destination = (
            _session_dir(session_id) / "trusted-receipts" / f"platform-health-{digest}.json"
        )
        _write_secure_exclusive(destination, receipt)
        state["platform_health_receipt_sha256"] = digest
        _atomic_write(_session_dir(session_id) / "state.json", state)
        return receipt


def _load_session_receipt(path: Path, *, expected_digest: str) -> dict[str, Any]:
    payload = _secure_json_load(path)
    if not isinstance(payload, dict) or payload.get("receipt_sha256") != expected_digest:
        raise AcceptanceError("trusted session receipt identity is invalid")
    unsigned = {key: value for key, value in payload.items() if key != "receipt_sha256"}
    if hashlib.sha256(_canonical_bytes(unsigned)).hexdigest() != expected_digest:
        raise AcceptanceError("trusted session receipt digest is invalid")
    return payload


def _verify_overlap_session_receipts(
    session_id: str,
    evidence: Mapping[str, Any],
    state: Mapping[str, Any],
) -> None:
    observations = {
        (observation["sandbox"], window["pool"]): (window, observation)
        for window in evidence["overlap_windows"]
        for observation in window["observations"]
    }
    samples = {
        (sample["sandbox"], sample["pool"]): sample
        for sample in evidence["capacity_samples"]
        if sample["phase"] == "multi_candidate_overlap"
    }
    descriptors = state["trusted_overlap_receipts"]
    if (
        len(descriptors) != len(SANDBOXES) * len(POOLS)
        or set(observations) != {(sandbox, pool) for sandbox in SANDBOXES for pool in POOLS}
        or set(samples) != set(observations)
    ):
        raise AcceptanceError("trusted overlap receipt coverage is incomplete")
    for descriptor in descriptors:
        sandbox = descriptor["sandbox"]
        pool = descriptor["pool"]
        sequence = descriptor["sequence"]
        digest = descriptor["receipt_sha256"]
        window, observation = observations[(sandbox, pool)]
        if observation["trusted_receipt"] != {
            "sequence": sequence,
            "receipt_sha256": digest,
        }:
            raise AcceptanceError("final overlap evidence does not bind its trusted receipt")
        path = (
            _session_dir(session_id)
            / "trusted-receipts"
            / f"{sequence:020d}-{sandbox}-{pool}-{digest}.json"
        )
        receipt = _load_session_receipt(path, expected_digest=digest)
        required = {
            "schema_version",
            "kind",
            "session_id",
            "sequence",
            "source_host",
            "observed_at",
            "sandbox",
            "pool",
            "candidate_sha",
            "candidate_tree",
            "capacity_observation_document",
            "sandbox_state",
            "live_observation",
            "source_paths",
            "source_sha256",
            "receipt_sha256",
        }
        if set(receipt) != required:
            raise AcceptanceError("trusted overlap receipt has an invalid closed shape")
        candidate = evidence["candidates"][sandbox]
        capacity_document = receipt["capacity_observation_document"]
        sandbox_state = receipt["sandbox_state"]
        live_observation = receipt["live_observation"]
        if (
            not isinstance(capacity_document, list)
            or len(capacity_document) != 1
            or not isinstance(capacity_document[0], dict)
            or not isinstance(live_observation, dict)
        ):
            raise AcceptanceError("trusted overlap receipt source shape is invalid")
        live_sample = live_observation["capacity_sample"]
        job_readback = live_observation["job_readback"]
        service_readback = live_observation["service_readback"]
        sample = samples[(sandbox, pool)]
        expected_paths = _overlap_source_paths(
            sandbox=sandbox,
            pool=pool,
            candidate_sha=candidate["sha"],
            job_id=observation["job_id"],
        )
        if (
            receipt["schema_version"] != SCHEMA_VERSION
            or receipt["kind"] != "loom.developer-sandbox.overlap-trusted-receipt"
            or receipt["session_id"] != session_id
            or receipt["sequence"] != sequence
            or receipt["source_host"] != POOL_AUTHORITY_HOSTS[pool]
            or receipt["sandbox"] != sandbox
            or receipt["pool"] != pool
            or receipt["candidate_sha"] != candidate["sha"]
            or receipt["candidate_tree"] != candidate["tree"]
            or receipt["source_paths"]
            != {
                "capacity": str(expected_paths[0]),
                "sandbox_state": str(expected_paths[1]),
                "live_observation": str(expected_paths[2]),
            }
            or receipt["source_sha256"]
            != {
                "capacity": hashlib.sha256(_canonical_bytes(capacity_document)).hexdigest(),
                "sandbox_state": hashlib.sha256(_canonical_bytes(sandbox_state)).hexdigest(),
                "live_observation": hashlib.sha256(
                    _canonical_bytes(live_observation),
                ).hexdigest(),
            }
            or live_observation["capacity_observation_sha256"]
            != hashlib.sha256(_canonical_bytes(capacity_document)).hexdigest()
            or live_observation["sandbox_state_sha256"]
            != hashlib.sha256(_canonical_bytes(sandbox_state)).hexdigest()
            or sample != live_sample
            or observation["job_readback"] != job_readback
            or observation["service_readback"] != service_readback
            or receipt["observed_at"] != observation["observed_at"]
            or receipt["observed_at"] != live_observation["observed_at"]
            or not _inside_window(
                receipt["observed_at"],
                (_timestamp(window["started_at"]), _timestamp(window["finished_at"])),
            )
        ):
            raise AcceptanceError("trusted overlap receipt does not match final evidence")


def _verify_promotion_session_receipt(
    session_id: str,
    evidence: Mapping[str, Any],
    state: Mapping[str, Any],
) -> None:
    digest = state["promotion_receipt_sha256"]
    if not isinstance(digest, str):
        raise AcceptanceError("trusted promotion receipt is missing")
    reference = evidence["promotion_candidate"]["trusted_receipt"]
    if reference["receipt_sha256"] != digest:
        raise AcceptanceError("promotion evidence does not bind its trusted receipt")
    path = _session_dir(session_id) / "trusted-receipts" / f"promotion-{digest}.json"
    receipt = _load_session_receipt(path, expected_digest=digest)
    required = {
        "schema_version",
        "kind",
        "session_id",
        "source_host",
        "rollout_id",
        "candidate_sha",
        "candidate_tree",
        "result",
        "observed_at",
        "authority_receipt",
        "authority_path",
        "authority_sha256",
        "receipt_sha256",
    }
    promotion = evidence["promotion_candidate"]
    regression = promotion["staging_regression"]
    if (
        set(receipt) != required
        or receipt["schema_version"] != SCHEMA_VERSION
        or receipt["kind"] != "loom.developer-sandbox.promotion-trusted-receipt"
        or receipt["session_id"] != session_id
        or receipt["source_host"] != PROMOTION_SOURCE_HOST
        or receipt["authority_path"] != str(PROMOTION_AUTHORITY_RECEIPT)
        or receipt["authority_sha256"]
        != hashlib.sha256(_canonical_bytes(receipt["authority_receipt"])).hexdigest()
        or receipt["candidate_sha"] != promotion["sha"]
        or receipt["candidate_tree"] != promotion["tree"]
        or receipt["rollout_id"] != reference["rollout_id"]
        or receipt["source_host"] != reference["source_host"]
        or receipt["result"] != reference["result"]
        or receipt["observed_at"] != reference["observed_at"]
        or receipt["result"] != "pass"
        or not _inside_window(
            receipt["observed_at"],
            (_timestamp(regression["started_at"]), _timestamp(regression["finished_at"])),
        )
    ):
        raise AcceptanceError("trusted promotion receipt does not match final evidence")


def _verify_platform_health_session_receipt(
    session_id: str,
    evidence: Mapping[str, Any],
    state: Mapping[str, Any],
) -> None:
    digest = state["platform_health_receipt_sha256"]
    if not isinstance(digest, str):
        raise AcceptanceError("trusted platform-health receipt is missing")
    platform_health = evidence["platform_health"]
    reference = platform_health["trusted_receipt"]
    if reference.get("receipt_sha256") != digest:
        raise AcceptanceError("platform-health evidence does not bind its trusted receipt")
    path = _session_dir(session_id) / "trusted-receipts" / f"platform-health-{digest}.json"
    receipt = _load_session_receipt(path, expected_digest=digest)
    required = {
        "schema_version",
        "kind",
        "session_id",
        "source_host",
        "observed_at",
        "authority_path",
        "authority_payload_sha256",
        "authority_evidence",
        "receipt_sha256",
    }
    authority = receipt.get("authority_evidence")
    if not isinstance(authority, dict):
        raise AcceptanceError("trusted platform-health source is invalid")
    _validate_platform_health_authority(
        authority,
        session_id=session_id,
        candidates=state["candidates"],
    )
    if (
        set(receipt) != required
        or receipt["schema_version"] != SCHEMA_VERSION
        or receipt["kind"] != "loom.developer-sandbox.platform-health-trusted-receipt"
        or receipt["session_id"] != session_id
        or receipt["source_host"] != SUBMIT_HOST
        or receipt["authority_path"]
        != str(PLATFORM_HEALTH_AUTHORITY_ROOT / "sessions" / session_id / "evidence.json")
        or receipt["authority_payload_sha256"] != authority["payload_sha256"]
        or receipt["observed_at"] != authority["completed_at"]
        or platform_health["authority_evidence"] != authority
        or reference
        != {
            "receipt_sha256": digest,
            "authority_payload_sha256": authority["payload_sha256"],
            "source_host": SUBMIT_HOST,
            "observed_at": authority["completed_at"],
        }
    ):
        raise AcceptanceError("trusted platform-health receipt does not match final evidence")


def _verify_staging_pressure_session_receipt(
    session_id: str,
    evidence: Mapping[str, Any],
    state: Mapping[str, Any],
) -> None:
    digest = state["staging_pressure_receipt_sha256"]
    if not isinstance(digest, str):
        raise AcceptanceError("trusted staging pressure receipt is missing")
    pressure = evidence["staging_pressure_reclaim"]
    reference = pressure["trusted_receipt"]
    if reference.get("receipt_sha256") != digest:
        raise AcceptanceError("staging pressure evidence does not bind its trusted receipt")
    path = _session_dir(session_id) / "trusted-receipts" / f"staging-pressure-{digest}.json"
    receipt = _load_session_receipt(path, expected_digest=digest)
    required = {
        "schema_version",
        "kind",
        "session_id",
        "source_host",
        "observed_at",
        "authority_session_id",
        "candidate_sha",
        "candidate_tree",
        "sequence",
        "authority_path",
        "authority_payload_sha256",
        "authority_receipt_sha256",
        "authority_signature_sha256",
        "authority_key_id",
        "authority_evidence",
        "receipt_sha256",
    }
    authority = receipt.get("authority_evidence")
    promotion = evidence["promotion_candidate"]
    if (
        not isinstance(authority, dict)
        or set(receipt) != required
        or receipt["schema_version"] != SCHEMA_VERSION
        or receipt["kind"] != "loom.developer-sandbox.staging-pressure-trusted-receipt"
        or receipt["session_id"] != session_id
        or receipt["source_host"] != STAGING_PRESSURE_SOURCE_HOST
        or receipt["candidate_sha"] != promotion["sha"]
        or receipt["candidate_tree"] != promotion["tree"]
        or receipt["authority_path"]
        != str(
            _pressure_authority_path(
                session_id,
                str(receipt["authority_session_id"]),
            ),
        )
        or receipt["authority_receipt_sha256"]
        != hashlib.sha256(_canonical_bytes(authority)).hexdigest()
        or pressure["authority_evidence"] != authority
        or reference
        != {
            "receipt_sha256": digest,
            "authority_session_id": receipt["authority_session_id"],
            "authority_receipt_sha256": receipt["authority_receipt_sha256"],
            "authority_signature_sha256": receipt["authority_signature_sha256"],
            "authority_key_id": receipt["authority_key_id"],
            "sequence": receipt["sequence"],
            "source_host": STAGING_PRESSURE_SOURCE_HOST,
            "observed_at": receipt["observed_at"],
        }
    ):
        raise AcceptanceError("trusted staging pressure receipt does not match final evidence")
    if not _inside_window(
        receipt["observed_at"],
        (
            _timestamp(promotion["staging_regression"]["started_at"]),
            _timestamp(promotion["staging_regression"]["finished_at"]),
        ),
    ):
        raise AcceptanceError("trusted staging pressure receipt is outside staging window")


def finalize_session(
    session_id: str,
    evidence_path: Path,
    schema: Mapping[str, Any],
    *,
    execute: bool,
) -> dict[str, Any]:
    """Verify and seal the final artifact into a complete session."""

    _require_execute(execute)
    evidence = _json_load(evidence_path)
    failures = verify_evidence(evidence, schema)
    if failures:
        raise AcceptanceError("final evidence failed verification")
    with _session_lock(session_id, exclusive=True):
        state = _session_state_unlocked(session_id)
        if state["completed_phases"] != [
            f"{sandbox}:{phase}" for phase, sandbox in PHASE_CHECKPOINTS
        ]:
            raise AcceptanceError("all bounded phases must pass before finalization")
        if {
            sandbox: {
                "sha": evidence["candidates"][sandbox]["sha"],
                "tree": evidence["candidates"][sandbox]["tree"],
            }
            for sandbox in SANDBOXES
        } != state["candidates"] or evidence["session"]["id"] != session_id:
            raise AcceptanceError("final evidence does not match the session identity")
        _verify_trusted_runtime_receipts(evidence)
        _verify_overlap_session_receipts(session_id, evidence, state)
        _verify_promotion_session_receipt(session_id, evidence, state)
        _verify_platform_health_session_receipt(session_id, evidence, state)
        _verify_staging_pressure_session_receipt(session_id, evidence, state)
        for index, (phase, sandbox) in enumerate(PHASE_CHECKPOINTS):
            checkpoint = _secure_json_load(
                _session_dir(session_id) / "checkpoints" / f"{index:02d}-{sandbox}-{phase}.json",
            )
            phase_evidence = evidence["state_machine"][index]
            canonical_phase = {
                key: value for key, value in phase_evidence.items() if key != "checkpoint_sha256"
            }
            actual_digest = hashlib.sha256(_canonical_bytes(canonical_phase)).hexdigest()
            expected_checkpoint = _checkpoint_payload(
                session_id,
                canonical_phase,
                actual_digest,
            )
            if (
                checkpoint != expected_checkpoint
                or phase_evidence["checkpoint_sha256"] != actual_digest
            ):
                raise AcceptanceError("final evidence does not match the checkpoint journal")
        evidence_digest = hashlib.sha256(_canonical_bytes(evidence)).hexdigest()
        _write_or_verify_secure(_session_dir(session_id) / "evidence.json", evidence)
        if state["status"] == "complete":
            if state.get("evidence_sha256") != evidence_digest:
                raise AcceptanceError("complete session evidence digest does not match")
            return state
        state["status"] = "complete"
        state["evidence_sha256"] = evidence_digest
        _atomic_write(_session_dir(session_id) / "state.json", state)
        return state


def _gate6_matrix_path(sandbox: str, pool: str, candidate_sha: str) -> Path:
    cluster = {"oldlab": "trt-oldlab", "gb10": "trt-gb10"}[pool]
    return (
        SLURM_POLICY_STATE_ROOT
        / "allocation-probes"
        / cluster
        / sandbox
        / f"{candidate_sha}.json"
    )


def _gate6_runtime_domain_bindings(
    evidence: Mapping[str, Any],
) -> dict[tuple[str, str], set[tuple[str, str, str, int]]]:
    bindings: dict[tuple[str, str], set[tuple[str, str, str, int]]] = {
        (sandbox, pool): set() for sandbox in SANDBOXES for pool in POOLS
    }
    for sandbox in SANDBOXES:
        for reference in evidence["candidates"][sandbox]["runtime_receipts"]:
            raw = _runtime_attestation_bytes(Path(reference["path"]))
            try:
                wrapper = json.loads(raw)
                combined = wrapper["combined_receipt"]
            except (KeyError, json.JSONDecodeError, TypeError) as exc:
                raise AcceptanceError("trusted runtime receipt cannot bind gate 6") from exc
            if raw != _canonical_bytes(wrapper):
                raise AcceptanceError("trusted runtime receipt is not canonical")
            for pool in POOLS:
                domain = combined["domains"][pool]
                bindings[(sandbox, pool)].add(
                    (
                        combined["payload_sha256"],
                        domain["payload_sha256"],
                        domain["signature_sha256"],
                        domain["generation"],
                    ),
                )
    return bindings


def seal_gate6(
    session_id: str,
    schema: Mapping[str, Any],
    nonexclusive_schema: Mapping[str, Any],
    *,
    execute: bool,
) -> dict[str, Any]:
    """Seal the exact finalized session and native authorities into gate 6."""

    _require_execute(execute)
    with _session_lock(session_id, exclusive=True):
        state = _session_state_unlocked(session_id)
        if state["status"] != "complete":
            raise AcceptanceError("gate 6 requires a finalized acceptance session")
        evidence = _secure_json_load(_session_dir(session_id) / "evidence.json")
        failures = verify_evidence(evidence, schema)
        if failures:
            raise AcceptanceError("finalized evidence failed gate-6 verification")
        evidence_digest = hashlib.sha256(_canonical_bytes(evidence)).hexdigest()
        if state["evidence_sha256"] != evidence_digest:
            raise AcceptanceError("finalized evidence digest drifted before gate 6")
        _verify_trusted_runtime_receipts(evidence)
        _verify_overlap_session_receipts(session_id, evidence, state)
        _verify_promotion_session_receipt(session_id, evidence, state)
        _verify_platform_health_session_receipt(session_id, evidence, state)
        _verify_staging_pressure_session_receipt(session_id, evidence, state)

        platform_path = (
            PLATFORM_HEALTH_AUTHORITY_ROOT / "sessions" / session_id / "evidence.json"
        )
        platform = _trusted_authority_json(
            platform_path,
            PLATFORM_HEALTH_AUTHORITY_ROOT,
            label="platform-health",
        )
        if platform != evidence["platform_health"]["authority_evidence"]:
            raise AcceptanceError("platform-health gate-6 authority drifted")

        matrices: dict[tuple[str, str], dict[str, Any]] = {}
        for sandbox in SANDBOXES:
            candidate_sha = state["candidates"][sandbox]["sha"]
            for pool in POOLS:
                path = _gate6_matrix_path(sandbox, pool, candidate_sha)
                matrices[(sandbox, pool)] = _trusted_authority_json(
                    path,
                    SLURM_POLICY_STATE_ROOT,
                    label="allocation-matrix",
                )
        runtime_bindings = _gate6_runtime_domain_bindings(evidence)
        for pair, matrix in matrices.items():
            runtime = matrix.get("runtime_attestation")
            if not isinstance(runtime, dict) or (
                runtime.get("receipt_sha256"),
                runtime.get("domain_payload_sha256"),
                runtime.get("domain_signature_sha256"),
                runtime.get("domain_generation"),
            ) not in runtime_bindings[pair]:
                raise AcceptanceError("allocation matrix is not bound to a trusted runtime receipt")
        try:
            bundle, pair_artifacts = gate6_verifier.build_gate6_bundle(
                evidence,
                platform,
                matrices,
                nonexclusive_schema,
            )
        except gate6_verifier.AcceptanceError as exc:
            raise AcceptanceError(str(exc)) from exc

        gate_root = _session_dir(session_id) / "gate6"
        _ensure_secure_directory(gate_root)
        for (sandbox, pool), artifact in sorted(pair_artifacts.items()):
            _write_or_verify_secure(
                gate_root / f"{sandbox}-{pool}.nonexclusive.json",
                artifact,
            )
        _write_or_verify_secure(gate_root / "acceptance.json", bundle)
        gate6_digest = bundle["payload_sha256"]
        if state.get("gate6_sha256") is not None:
            if state["gate6_sha256"] != gate6_digest:
                raise AcceptanceError("sealed gate-6 digest does not match")
            return state
        state["gate6_sha256"] = gate6_digest
        _atomic_write(_session_dir(session_id) / "state.json", state)
        return state


def _report(evidence: Any, schema: Mapping[str, Any]) -> dict[str, Any]:
    failures = verify_evidence(evidence, schema)
    if failures:
        return {"status": "fail", "failures": failures}
    return {
        "status": "pass",
        "schema_version": SCHEMA_VERSION,
        "session_id": evidence["session"]["id"],
        "candidates": {
            sandbox: {
                "sha": evidence["candidates"][sandbox]["sha"],
                "tree": evidence["candidates"][sandbox]["tree"],
            }
            for sandbox in SANDBOXES
        },
        "promotion_candidate": {
            "sha": evidence["promotion_candidate"]["sha"],
            "tree": evidence["promotion_candidate"]["tree"],
        },
    }


def _emit(payload: Mapping[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("plan", allow_abbrev=False)

    verify = subparsers.add_parser("verify", allow_abbrev=False)
    verify.add_argument("--evidence", type=Path, required=True)

    collect = subparsers.add_parser("collect", allow_abbrev=False)
    collect.add_argument("--input", type=Path, required=True)
    collect.add_argument("--output", type=Path, required=True)

    start = subparsers.add_parser("session-start", allow_abbrev=False)
    for sandbox in SANDBOXES:
        start.add_argument(f"--{sandbox}-sha", required=True)
        start.add_argument(f"--{sandbox}-tree", required=True)
    start.add_argument("--execute", action="store_true")

    status = subparsers.add_parser("session-status", allow_abbrev=False)
    status.add_argument("--session-id", required=True)

    checkpoint = subparsers.add_parser("session-checkpoint", allow_abbrev=False)
    checkpoint.add_argument("--session-id", required=True)
    checkpoint.add_argument("--phase", choices=PHASES, required=True)
    checkpoint.add_argument("--sandbox", choices=SANDBOXES, required=True)
    checkpoint.add_argument("--phase-evidence", type=Path, required=True)
    checkpoint.add_argument("--execute", action="store_true")

    record_overlap = subparsers.add_parser(
        "session-record-overlap",
        allow_abbrev=False,
    )
    record_overlap.add_argument("--session-id", required=True)
    record_overlap.add_argument("--sandbox", choices=SANDBOXES, required=True)
    record_overlap.add_argument("--pool", choices=POOLS, required=True)
    record_overlap.add_argument("--job-id", required=True)
    record_overlap.add_argument("--execute", action="store_true")

    record_promotion = subparsers.add_parser(
        "session-record-promotion",
        allow_abbrev=False,
    )
    record_promotion.add_argument("--session-id", required=True)
    record_promotion.add_argument("--execute", action="store_true")

    record_platform_health = subparsers.add_parser(
        "session-record-platform-health",
        allow_abbrev=False,
    )
    record_platform_health.add_argument("--session-id", required=True)
    record_platform_health.add_argument("--execute", action="store_true")

    record_pressure = subparsers.add_parser(
        "session-record-staging-pressure",
        allow_abbrev=False,
    )
    record_pressure.add_argument("--session-id", required=True)
    record_pressure.add_argument("--authority-session-id", required=True)
    record_pressure.add_argument("--execute", action="store_true")

    finalize = subparsers.add_parser("session-finalize", allow_abbrev=False)
    finalize.add_argument("--session-id", required=True)
    finalize.add_argument("--evidence", type=Path, required=True)
    finalize.add_argument("--execute", action="store_true")
    gate6 = subparsers.add_parser("session-seal-gate6", allow_abbrev=False)
    gate6.add_argument("--session-id", required=True)
    gate6.add_argument("--execute", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    command = args.command or "plan"
    try:
        if command == "plan":
            _emit(acceptance_plan())
            return 0
        if command == "session-status":
            _emit(_session_state(args.session_id))
            return 0
        if command == "session-start":
            _emit(
                start_session(
                    {
                        sandbox: {
                            "sha": getattr(args, f"{sandbox}_sha"),
                            "tree": getattr(args, f"{sandbox}_tree"),
                        }
                        for sandbox in SANDBOXES
                    },
                    execute=args.execute,
                ),
            )
            return 0
        if command == "session-checkpoint":
            _emit(
                checkpoint_session(
                    args.session_id,
                    args.phase,
                    args.sandbox,
                    args.phase_evidence,
                    execute=args.execute,
                ),
            )
            return 0
        if command == "session-record-overlap":
            _emit(
                record_overlap_receipt(
                    args.session_id,
                    args.sandbox,
                    args.pool,
                    args.job_id,
                    execute=args.execute,
                ),
            )
            return 0
        if command == "session-record-promotion":
            _emit(
                record_promotion_receipt(
                    args.session_id,
                    execute=args.execute,
                ),
            )
            return 0
        if command == "session-record-platform-health":
            _emit(
                record_platform_health_receipt(
                    args.session_id,
                    execute=args.execute,
                ),
            )
            return 0
        if command == "session-record-staging-pressure":
            _emit(
                record_staging_pressure_receipt(
                    args.session_id,
                    args.authority_session_id,
                    execute=args.execute,
                ),
            )
            return 0

        schema = _load_schema()
        if command == "session-finalize":
            _emit(
                finalize_session(
                    args.session_id,
                    args.evidence,
                    schema,
                    execute=args.execute,
                ),
            )
            return 0
        if command == "session-seal-gate6":
            _emit(
                seal_gate6(
                    args.session_id,
                    schema,
                    gate6_verifier._load_schema(NONEXCLUSIVE_SCHEMA),
                    execute=args.execute,
                ),
            )
            return 0
        source = args.input if command == "collect" else args.evidence
        evidence = _json_load(source)
        report = _report(evidence, schema)
        if report["status"] != "pass":
            _emit(report)
            return 1
        if command == "collect":
            _write_exclusive(args.output, evidence)
        _emit(report)
        return 0
    except AcceptanceError as exc:
        _emit({"status": "fail", "failures": [str(exc)]})
        return 1
    except (KeyError, OSError, ValueError):
        _emit({"status": "fail", "failures": ["acceptance processing failed safely"]})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
