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
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

SCHEMA_VERSION = 1
MAX_ARTIFACT_BYTES = 8 * 1024 * 1024
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCHEMA = REPO_ROOT / "docs/evidence/developer-sandbox-live-acceptance.schema.json"
STATE_ROOT = Path("/var/lib/loom-developer-sandbox-live-acceptance")
REQUIRED_OWNER_UID = 0
REQUIRED_OWNER_GID = 0
SUBMIT_HOST = "trt-eai-oldlab-2"
SANDBOXES = ("qianyi", "hongjian", "devansh")
POOLS = ("oldlab", "gb10")
POOL_SLOT_BUDGETS = {"oldlab": 20, "gb10": 140}
POOL_PENDING_BUDGETS = {"oldlab": 10, "gb10": 30}
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
    "trt-gb10-8",
    "trt-gb10-9",
    "trt-gb10-10",
    "trt-gb10-11",
    "trt-gb10-12",
    "trt-gb10-13",
    "trt-gb10-14",
    "trt-gb10-15",
)
PHASES = (
    "preflight",
    "baseline",
    "large_batch_burst",
    "fairness_contention",
    "mixed_non_loom",
    "cancel_cleanup",
    "ttl_cleanup",
    "submit_host_restart",
    "worker_crash",
    "final_drain",
)
CAPACITY_PHASES = (
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


def _semantic_failures(evidence: Mapping[str, Any]) -> list[str]:
    failures: list[str] = []
    candidate_sha = evidence["candidate"]["sha"]
    candidate_tree = evidence["candidate"]["tree"]
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

    receipts = evidence["candidate"]["runtime_receipts"]
    if {receipt["sandbox"] for receipt in receipts} != set(SANDBOXES) or len(
        receipts,
    ) != len(SANDBOXES):
        failures.append("runtime receipts do not cover the exact sandbox set")
    for receipt in receipts:
        if receipt["candidate_sha"] != candidate_sha or receipt["candidate_tree"] != candidate_tree:
            failures.append(f"{receipt['sandbox']} runtime receipt candidate does not match")
        try:
            receipt_collected = _timestamp(receipt["collected_at"])
            receipt_expires = _timestamp(receipt["expires_at"])
        except ValueError:
            failures.append(f"{receipt['sandbox']} runtime receipt timestamp is invalid")
            continue
        if receipt_collected > started_at or receipt_expires < completed_at:
            failures.append(
                f"{receipt['sandbox']} runtime receipt does not cover the session",
            )

    topology = evidence["topology"]
    if tuple(topology["sandboxes"]) != SANDBOXES:
        failures.append("sandbox topology is not the fixed three-sandbox set")
    if tuple(topology["pools"]) != POOLS:
        failures.append("pool topology is not the fixed oldlab/gb10 set")
    if tuple(topology["eligible_nodes"]) != EXPECTED_NODES:
        failures.append("eligible node topology is incomplete or reordered")
    if topology["excluded_nodes"] != ["trt-gb10-7"]:
        failures.append("the quarantined GB10 node exclusion is not exact")
    if topology["slot_budgets"] != POOL_SLOT_BUDGETS:
        failures.append("pool slot budgets do not match the reviewed contract")
    if topology["pending_slot_budgets"] != POOL_PENDING_BUDGETS:
        failures.append("pool pending budgets do not match the reviewed contract")

    phases = evidence["state_machine"]
    if [phase["phase"] for phase in phases] != list(PHASES):
        failures.append("state-machine phases are incomplete or out of order")
    previous = started_at
    checkpoint_digests: set[str] = set()
    phase_windows: dict[str, tuple[datetime, datetime]] = {}
    for phase in phases:
        if phase["candidate_sha"] != candidate_sha or phase["candidate_tree"] != candidate_tree:
            failures.append(f"{phase['phase']} is not bound to the exact candidate")
        try:
            phase_started = _timestamp(phase["started_at"])
            phase_finished = _timestamp(phase["finished_at"])
        except ValueError:
            failures.append(f"{phase['phase']} timestamps are invalid")
            continue
        elapsed = (phase_finished - phase_started).total_seconds()
        if phase_started < previous or phase_finished < phase_started:
            failures.append(f"{phase['phase']} timestamps regress")
        if elapsed > phase["deadline_seconds"]:
            failures.append(f"{phase['phase']} exceeded its bounded deadline")
        phase_windows[phase["phase"]] = (phase_started, phase_finished)
        if phase["checkpoint_sha256"] in checkpoint_digests:
            failures.append("state-machine checkpoint digest is duplicated")
        checkpoint_digests.add(phase["checkpoint_sha256"])
        previous = phase_finished
    if phases and _timestamp(phases[-1]["finished_at"]) > completed_at:
        failures.append("state-machine completion is later than the session")

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
        if probe["candidate_sha"] != candidate_sha or probe["candidate_tree"] != candidate_tree:
            failures.append("cross-sandbox negative probe candidate does not match")
        if not _inside_window(probe["observed_at"], phase_windows[probe["phase"]]):
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
        if sample["candidate_sha"] != candidate_sha or sample["candidate_tree"] != candidate_tree:
            failures.append("capacity sample candidate does not match")
        try:
            _timestamp(sample["observed_at"])
        except ValueError:
            failures.append("capacity sample timestamp is invalid")
            continue
        if not _inside_window(sample["observed_at"], phase_windows[sample["phase"]]):
            failures.append("capacity sample is outside its phase window")
        identity = (
            sample["request_id"],
            sample["lease_epoch"],
            sample["observation_sequence"],
        )
        if identity in seen_observations:
            failures.append("capacity observation identity is duplicated")
        seen_observations.add(identity)
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
    if {burst["pool"] for burst in bursts} != set(POOLS) or len(bursts) != len(POOLS):
        failures.append("large-batch evidence must contain exactly one burst per pool")
    for burst in bursts:
        if burst["candidate_sha"] != candidate_sha or burst["candidate_tree"] != candidate_tree:
            failures.append("large-batch burst candidate does not match")
        if not _interval_inside_window(
            burst["started_at"],
            burst["finished_at"],
            phase_windows[burst["phase"]],
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
        if window["candidate_sha"] != candidate_sha or window["candidate_tree"] != candidate_tree:
            failures.append(f"{window['pool']} fairness candidate does not match")
        if not _interval_inside_window(
            window["started_at"],
            window["finished_at"],
            phase_windows[window["phase"]],
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
        if (
            envelope["candidate_sha"] != candidate_sha
            or envelope["candidate_tree"] != candidate_tree
        ):
            failures.append("runtime envelope candidate does not match")
        if not _inside_window(
            envelope["observed_at"],
            phase_windows[envelope["phase"]],
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
        if peer["candidate_sha"] != candidate_sha or peer["candidate_tree"] != candidate_tree:
            failures.append(f"{peer['pool']} peer candidate does not match")
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
                phase_windows["baseline"],
            )
            or not _inside_window(
                during["observed_at"],
                phase_windows["mixed_non_loom"],
            )
            or not _inside_window(
                after["observed_at"],
                phase_windows["final_drain"],
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

    storage = evidence["storage_io"]
    if {item["domain"] for item in storage} != set(POOLS) or len(storage) != len(POOLS):
        failures.append("storage/cache/I/O evidence must cover both domains")
    for item in storage:
        if item["candidate_sha"] != candidate_sha or item["candidate_tree"] != candidate_tree:
            failures.append(f"{item['domain']} storage candidate does not match")
        if not (
            _inside_window(
                item["baseline_observed_at"],
                phase_windows["baseline"],
            )
            and _inside_window(
                item["minimum_observed_at"],
                phase_windows["mixed_non_loom"],
            )
            and _inside_window(
                item["after_observed_at"],
                phase_windows["final_drain"],
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
        if fault["candidate_sha"] != candidate_sha or fault["candidate_tree"] != candidate_tree:
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
            phase_windows[fault["phase"]],
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
        "eligible_nodes": list(EXPECTED_NODES),
        "excluded_nodes": ["trt-gb10-7"],
        "state_machine": list(PHASES),
        "faults": list(FAULTS),
        "requirements": [
            "exact candidate SHA and tree on every phase and runtime record",
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
        ],
        "stop_rules": [
            "Stop unless the exact candidate is installed and read back on both domains.",
            "Stop unless separate live-mutation authority has been recorded.",
            "Stop if submit host, sandbox, pool, or eligible-node identity differs.",
            "Stop on any secret-like evidence field or value.",
            "Stop before pressure if the non-Loom baseline is unhealthy.",
            "Stop on capacity overshoot, duplicate observation, or cgroup escape.",
            "Stop and drain on peer disruption, storage error, or freshness failure.",
            "Never use trt-gb10-7 and never add --exclusive.",
        ],
    }


def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()


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
        "candidate_sha",
        "candidate_tree",
        "submit_host",
        "status",
        "next_phase_index",
        "completed_phases",
    }
    if not isinstance(state, dict) or frozenset(state) not in {
        frozenset(base_keys),
        frozenset((*base_keys, "evidence_sha256")),
    }:
        raise AcceptanceError("session state has an invalid closed shape")
    completed = state["completed_phases"]
    next_index = state["next_phase_index"]
    if (
        state["schema_version"] != SCHEMA_VERSION
        or state["session_id"] != session_id
        or _SHA_RE.fullmatch(str(state["candidate_sha"])) is None
        or _SHA_RE.fullmatch(str(state["candidate_tree"])) is None
        or state["submit_host"] != SUBMIT_HOST
        or state["status"] not in {"running", "complete"}
        or not isinstance(next_index, int)
        or isinstance(next_index, bool)
        or next_index < 0
        or next_index > len(PHASES)
        or not isinstance(completed, list)
        or completed != list(PHASES[:next_index])
    ):
        raise AcceptanceError("session state identity or progress is invalid")
    if state["status"] == "complete":
        if (
            next_index != len(PHASES)
            or _DIGEST_RE.fullmatch(str(state.get("evidence_sha256"))) is None
        ):
            raise AcceptanceError("complete session state is invalid")
    elif "evidence_sha256" in state:
        raise AcceptanceError("running session state contains a final digest")
    return state


def _session_state(session_id: str) -> dict[str, Any]:
    with _session_lock(session_id, exclusive=False):
        return _session_state_unlocked(session_id)


def start_session(candidate_sha: str, candidate_tree: str, *, execute: bool) -> dict[str, Any]:
    """Create a crash-safe, candidate-bound acceptance session."""

    _require_execute(execute)
    if _SHA_RE.fullmatch(candidate_sha) is None or _SHA_RE.fullmatch(candidate_tree) is None:
        raise AcceptanceError("candidate SHA and tree must be full lowercase Git hashes")
    _ensure_state_tree(create=True)
    session_id = uuid.uuid4().hex
    session_dir = _session_dir(session_id)
    try:
        os.mkdir(session_dir, 0o700)
    except OSError as exc:
        raise AcceptanceError("cannot create acceptance session directory") from exc
    _validate_secure_directory(session_dir)
    _ensure_secure_directory(session_dir / "checkpoints")
    _fsync_directory(STATE_ROOT / "sessions")
    state = {
        "schema_version": SCHEMA_VERSION,
        "session_id": session_id,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "submit_host": SUBMIT_HOST,
        "status": "running",
        "next_phase_index": 0,
        "completed_phases": [],
    }
    with _session_lock(session_id, exclusive=True, create=True):
        _atomic_write(session_dir / "state.json", state)
    return state


def _phase_payload(
    path: Path,
    *,
    phase: str,
    state: Mapping[str, Any],
) -> dict[str, Any]:
    payload = _json_load(path)
    _scan_for_secrets(payload)
    required = {
        "phase",
        "candidate_sha",
        "candidate_tree",
        "started_at",
        "finished_at",
        "deadline_seconds",
        "status",
    }
    if not isinstance(payload, dict) or set(payload) != required:
        raise AcceptanceError("phase evidence has an invalid closed shape")
    if (
        payload["phase"] != phase
        or payload["candidate_sha"] != state["candidate_sha"]
        or payload["candidate_tree"] != state["candidate_tree"]
        or payload["status"] != "pass"
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
    return {
        "schema_version": SCHEMA_VERSION,
        "session_id": session_id,
        "candidate_sha": phase_payload["candidate_sha"],
        "candidate_tree": phase_payload["candidate_tree"],
        "phase": phase_payload["phase"],
        "recorded_at": phase_payload["finished_at"],
        "status": "pass",
        "evidence_sha256": digest,
    }


def checkpoint_session(
    session_id: str,
    phase: str,
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
            phase_index = PHASES.index(phase)
        except ValueError as exc:
            raise AcceptanceError("checkpoint phase is invalid") from exc
        if phase_index > state["next_phase_index"]:
            raise AcceptanceError("checkpoint is not the exact next phase")
        phase_payload = _phase_payload(
            phase_evidence_path,
            phase=phase,
            state=state,
        )
        digest = hashlib.sha256(_canonical_bytes(phase_payload)).hexdigest()
        checkpoint = _checkpoint_payload(session_id, phase_payload, digest)
        destination = _session_dir(session_id) / "checkpoints" / f"{phase_index:02d}-{phase}.json"
        _write_or_verify_secure(destination, checkpoint)
        if phase_index < state["next_phase_index"]:
            return state
        state["completed_phases"].append(phase)
        state["next_phase_index"] = phase_index + 1
        _atomic_write(_session_dir(session_id) / "state.json", state)
        return state


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
        if state["completed_phases"] != list(PHASES):
            raise AcceptanceError("all bounded phases must pass before finalization")
        if (
            evidence["candidate"]["sha"] != state["candidate_sha"]
            or evidence["candidate"]["tree"] != state["candidate_tree"]
            or evidence["session"]["id"] != session_id
        ):
            raise AcceptanceError("final evidence does not match the session identity")
        for index, phase in enumerate(PHASES):
            checkpoint = _secure_json_load(
                _session_dir(session_id) / "checkpoints" / f"{index:02d}-{phase}.json",
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


def _report(evidence: Any, schema: Mapping[str, Any]) -> dict[str, Any]:
    failures = verify_evidence(evidence, schema)
    if failures:
        return {"status": "fail", "failures": failures}
    return {
        "status": "pass",
        "schema_version": SCHEMA_VERSION,
        "session_id": evidence["session"]["id"],
        "candidate_sha": evidence["candidate"]["sha"],
        "candidate_tree": evidence["candidate"]["tree"],
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
    start.add_argument("--candidate-sha", required=True)
    start.add_argument("--candidate-tree", required=True)
    start.add_argument("--execute", action="store_true")

    status = subparsers.add_parser("session-status", allow_abbrev=False)
    status.add_argument("--session-id", required=True)

    checkpoint = subparsers.add_parser("session-checkpoint", allow_abbrev=False)
    checkpoint.add_argument("--session-id", required=True)
    checkpoint.add_argument("--phase", choices=PHASES, required=True)
    checkpoint.add_argument("--phase-evidence", type=Path, required=True)
    checkpoint.add_argument("--execute", action="store_true")

    finalize = subparsers.add_parser("session-finalize", allow_abbrev=False)
    finalize.add_argument("--session-id", required=True)
    finalize.add_argument("--evidence", type=Path, required=True)
    finalize.add_argument("--execute", action="store_true")
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
                    args.candidate_sha,
                    args.candidate_tree,
                    execute=args.execute,
                ),
            )
            return 0
        if command == "session-checkpoint":
            _emit(
                checkpoint_session(
                    args.session_id,
                    args.phase,
                    args.phase_evidence,
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
    except (OSError, ValueError):
        _emit({"status": "fail", "failures": ["acceptance processing failed safely"]})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
