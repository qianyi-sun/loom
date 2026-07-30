"""Persistently reconcile and publish shared sandbox capacity handoffs.

One invocation collects the configured sandbox/pool adapter observations,
validates their request and lease-epoch bindings against the broker's durable
state, executes exactly one broker reconcile transaction, independently
rechecks all global/pool slot and pending-slot budgets, then atomically
publishes the exact broker-produced handoffs.  It never reads a sandbox
credential.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import sqlite3
import stat
import sys
import tomllib
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

_INSTALLED_SELF = Path("/usr/local/libexec/scripts/ops/shared_capacity_supervisor.py")
_INSTALLED_IMPORT_ROOT = Path("/usr/local/libexec/loom-runtime-python")
_RUNNING_INSTALLED = Path(__file__).absolute() == _INSTALLED_SELF
if _RUNNING_INSTALLED:
    for _path, _mode in {
        _INSTALLED_SELF: 0o444,
        _INSTALLED_IMPORT_ROOT / "loom_control_plane/__init__.py": 0o444,
        _INSTALLED_IMPORT_ROOT / "loom_control_plane/shared_capacity_broker.py": 0o444,
    }.items():
        _current = Path("/")
        for _part in _path.relative_to("/").parts[:-1]:
            _current /= _part
            try:
                _metadata = _current.lstat()
            except OSError as exc:
                raise RuntimeError("installed supervisor import ancestry is unavailable") from exc
            if (
                not stat.S_ISDIR(_metadata.st_mode)
                or stat.S_ISLNK(_metadata.st_mode)
                or _metadata.st_uid != 0
                or _metadata.st_gid != 0
                or _metadata.st_mode & 0o022
            ):
                raise RuntimeError("installed supervisor import ancestry is unsafe")
        try:
            _metadata = _path.lstat()
        except OSError as exc:
            raise RuntimeError("installed supervisor import asset is unavailable") from exc
        if (
            not stat.S_ISREG(_metadata.st_mode)
            or stat.S_ISLNK(_metadata.st_mode)
            or _metadata.st_uid != 0
            or _metadata.st_gid != 0
            or _metadata.st_nlink != 1
            or stat.S_IMODE(_metadata.st_mode) != _mode
        ):
            raise RuntimeError("installed supervisor import asset is unsafe")
        _cache = _path.parent / "__pycache__"
        if _cache.exists() or _cache.is_symlink():
            raise RuntimeError("installed supervisor import inventory is not closed")
    sys.path.insert(0, str(_INSTALLED_IMPORT_ROOT))
elif __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).absolute().parents[2]))

from loom_control_plane.shared_capacity_broker import (  # noqa: E402
    BrokerBudgets,
    BrokerError,
    LeaseObservation,
    SandboxId,
    SharedCapacityBroker,
)

if _RUNNING_INSTALLED:
    _broker_module = sys.modules["loom_control_plane.shared_capacity_broker"]
    if Path(str(_broker_module.__file__)).absolute() != (
        _INSTALLED_IMPORT_ROOT / "loom_control_plane/shared_capacity_broker.py"
    ):
        raise RuntimeError("installed supervisor import closure escaped fixed assets")

_SCHEMA_VERSION = 1
_REVIEWED_POOL_SLOT_BOUNDS = {"gb10": 120, "oldlab": 20}
_REVIEWED_POOL_PENDING_BOUNDS = {"gb10": 24, "oldlab": 10}
_REVIEWED_GLOBAL_SLOT_BOUND = 140
_REVIEWED_GLOBAL_PENDING_BOUND = 34
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
)
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_GENERATION_RE = re.compile(r"^generation-[0-9]{20}-[0-9a-f]{16}$")
_STAGING_GENERATION_RE = re.compile(r"^\.generation-[0-9]{20}-[0-9a-f]{16}\.tmp-[0-9]+$")
_OBSERVATION_FIELDS = {
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
_OBSERVATION_MAX_AGE = timedelta(seconds=60)
_OBSERVATION_MAX_FUTURE_SKEW = timedelta(seconds=30)
_MAX_AUDIT_BYTES = 16 * 1024 * 1024
_DRAINED_LEASE_FIELDS = (
    "granted_slots",
    "committed_slots",
    "pending_slots",
    "active_slots",
    "draining_slots",
    "terminal_slots",
)
_HANDOFF_FIELDS = {
    "schema_version",
    "request_id",
    "lease_epoch",
    "sandbox",
    "environment",
    "candidate_sha",
    "pool_name",
    "enabled",
    "min_slots",
    "max_slots",
    "expires_at",
    "preemptible",
}


class SupervisorError(RuntimeError):
    """The supervisor cannot safely reconcile or publish this cycle."""


@dataclass(frozen=True, slots=True)
class SupervisorConfig:
    state_db: Path
    handoff_dir: Path
    observation_dir: Path
    supervisor_state_path: Path
    audit_path: Path
    evidence_path: Path
    global_slot_budget: int
    global_pending_slot_budget: int
    pool_slot_budgets: dict[str, int]
    pool_pending_slot_budgets: dict[str, int]
    instances: tuple[str, ...]

    @property
    def digest(self) -> str:
        payload = {
            "schema_version": _SCHEMA_VERSION,
            "state_db": str(self.state_db),
            "handoff_dir": str(self.handoff_dir),
            "observation_dir": str(self.observation_dir),
            "supervisor_state_path": str(self.supervisor_state_path),
            "audit_path": str(self.audit_path),
            "evidence_path": str(self.evidence_path),
            "global_slot_budget": self.global_slot_budget,
            "global_pending_slot_budget": self.global_pending_slot_budget,
            "pool_slot_budgets": self.pool_slot_budgets,
            "pool_pending_slot_budgets": self.pool_pending_slot_budgets,
            "instances": list(self.instances),
        }
        return _digest(payload)


@contextmanager
def _exclusive_supervisor_lock(config: SupervisorConfig) -> Iterator[None]:
    lock_path = config.supervisor_state_path.with_name(
        f".{config.supervisor_state_path.name}.lock",
    )
    _private_parent(lock_path.parent)
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as exc:
        raise SupervisorError("supervisor lock is unavailable") from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise SupervisorError("supervisor lock file is unsafe")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise SupervisorError("supervisor invocation is already active") from exc
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _digest(payload: object) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


def _private_parent(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    metadata = path.lstat()
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISDIR(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise SupervisorError("supervisor output directory must be private")


def _secure_regular_file(
    path: Path,
    *,
    label: str,
    required: bool = True,
) -> Path | None:
    if not path.is_absolute():
        raise SupervisorError(f"{label} must be an absolute path")
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        if required:
            raise SupervisorError(f"{label} is unavailable") from None
        return None
    except OSError as exc:
        raise SupervisorError(f"{label} is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise SupervisorError(f"{label} must be an owner-only regular file")
    return path.resolve(strict=True)


def _absolute_path(payload: Mapping[str, Any], field: str) -> Path:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise SupervisorError(f"supervisor config {field} is invalid")
    path = Path(value.strip())
    if not path.is_absolute() or ".." in path.parts:
        raise SupervisorError(f"supervisor config {field} must be absolute")
    return path


def _budget_table(
    payload: Mapping[str, Any],
    field: str,
    reviewed: Mapping[str, int],
) -> dict[str, int]:
    raw = payload.get(field)
    if not isinstance(raw, dict) or set(raw) != set(reviewed):
        raise SupervisorError(f"supervisor config {field} pools are invalid")
    result: dict[str, int] = {}
    for pool, reviewed_bound in reviewed.items():
        value = raw.get(pool)
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or not 0 <= value <= reviewed_bound
        ):
            raise SupervisorError(
                f"supervisor config {field}.{pool} exceeds the reviewed bound",
            )
        result[pool] = value
    return result


def _instance_parts(instance: str) -> tuple[SandboxId, str]:
    sandbox, separator, pool = instance.rpartition("-")
    if not separator or pool not in _REVIEWED_POOL_SLOT_BOUNDS:
        raise SupervisorError(f"unknown supervisor instance {instance!r}")
    try:
        normalized_sandbox = SandboxId(sandbox)
    except ValueError as exc:
        raise SupervisorError(f"supervisor instance sandbox id is invalid: {instance!r}") from exc
    if instance != f"{normalized_sandbox.value}-{pool}":
        raise SupervisorError(f"unknown supervisor instance {instance!r}")
    return normalized_sandbox, pool


def _normalize_instances(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or not value or not all(isinstance(item, str) for item in value):
        raise SupervisorError("supervisor instances must be a non-empty string list")
    if len(value) != len(set(value)):
        raise SupervisorError("supervisor instances contain duplicates")
    sandbox_pools: dict[SandboxId, set[str]] = {}
    normalized: list[tuple[SandboxId, str]] = []
    for instance in value:
        sandbox, pool = _instance_parts(instance)
        sandbox_pools.setdefault(sandbox, set()).add(pool)
        normalized.append((sandbox, pool))
    reviewed_pools = set(_REVIEWED_POOL_SLOT_BOUNDS)
    if any(pools != reviewed_pools for pools in sandbox_pools.values()):
        raise SupervisorError(
            "each supervisor sandbox must cover every reviewed pool exactly once",
        )
    return tuple(
        f"{sandbox.value}-{pool}"
        for sandbox, pool in sorted(
            normalized,
            key=lambda item: (item[0].value, item[1]),
        )
    )


def load_config(path: Path) -> SupervisorConfig:
    resolved = _secure_regular_file(path, label="supervisor config")
    assert resolved is not None
    try:
        payload = tomllib.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        raise SupervisorError("supervisor config is invalid") from exc
    expected = {
        "schema_version",
        "state_db",
        "handoff_dir",
        "observation_dir",
        "supervisor_state_path",
        "audit_path",
        "evidence_path",
        "global_slot_budget",
        "global_pending_slot_budget",
        "instances",
        "pool_slot_budgets",
        "pool_pending_slot_budgets",
    }
    if set(payload) != expected or payload.get("schema_version") != _SCHEMA_VERSION:
        raise SupervisorError("supervisor config fields do not match the closed schema")
    global_slots = payload.get("global_slot_budget")
    global_pending = payload.get("global_pending_slot_budget")
    if (
        isinstance(global_slots, bool)
        or not isinstance(global_slots, int)
        or not 0 <= global_slots <= _REVIEWED_GLOBAL_SLOT_BOUND
    ):
        raise SupervisorError("global_slot_budget exceeds the reviewed bound")
    if (
        isinstance(global_pending, bool)
        or not isinstance(global_pending, int)
        or not 0 <= global_pending <= _REVIEWED_GLOBAL_PENDING_BOUND
    ):
        raise SupervisorError("global_pending_slot_budget exceeds the reviewed bound")
    pool_slots = _budget_table(
        payload,
        "pool_slot_budgets",
        _REVIEWED_POOL_SLOT_BOUNDS,
    )
    pool_pending = _budget_table(
        payload,
        "pool_pending_slot_budgets",
        _REVIEWED_POOL_PENDING_BOUNDS,
    )
    if global_slots > sum(pool_slots.values()):
        raise SupervisorError("global slot budget exceeds the sum of pool budgets")
    if global_pending > global_slots or global_pending > sum(pool_pending.values()):
        raise SupervisorError("global pending budget is inconsistent")
    if any(pool_pending[pool] > pool_slots[pool] for pool in pool_slots):
        raise SupervisorError("pool pending budget exceeds its slot budget")
    instances = _normalize_instances(payload.get("instances"))
    return SupervisorConfig(
        state_db=_absolute_path(payload, "state_db"),
        handoff_dir=_absolute_path(payload, "handoff_dir"),
        observation_dir=_absolute_path(payload, "observation_dir"),
        supervisor_state_path=_absolute_path(payload, "supervisor_state_path"),
        audit_path=_absolute_path(payload, "audit_path"),
        evidence_path=_absolute_path(payload, "evidence_path"),
        global_slot_budget=global_slots,
        global_pending_slot_budget=global_pending,
        pool_slot_budgets=pool_slots,
        pool_pending_slot_budgets=pool_pending,
        instances=instances,
    )


def _read_json(path: Path, *, label: str) -> object:
    resolved = _secure_regular_file(path, label=label)
    assert resolved is not None
    try:
        return json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SupervisorError(f"{label} is invalid") from exc


def _load_supervisor_state(path: Path) -> dict[str, Any] | None:
    resolved = _secure_regular_file(
        path,
        label="supervisor state",
        required=False,
    )
    if resolved is None:
        return None
    payload = _read_json(resolved, label="supervisor state")
    required = {
        "schema_version",
        "cycle_sequence",
        "config_digest",
        "report_digest",
        "published",
        "generation",
        "updated_at",
    }
    if not isinstance(payload, dict) or set(payload) != required:
        raise SupervisorError("supervisor state fields do not match the closed schema")
    if payload.get("schema_version") != _SCHEMA_VERSION:
        raise SupervisorError("supervisor state schema is unsupported")
    if (
        isinstance(payload.get("cycle_sequence"), bool)
        or not isinstance(payload.get("cycle_sequence"), int)
        or payload["cycle_sequence"] < 0
        or _DIGEST_RE.fullmatch(str(payload.get("config_digest"))) is None
        or _DIGEST_RE.fullmatch(str(payload.get("report_digest"))) is None
        or not isinstance(payload.get("published"), dict)
        or _GENERATION_RE.fullmatch(str(payload.get("generation"))) is None
    ):
        raise SupervisorError("supervisor state is invalid")
    return payload


def _canonical_handoff_bytes(handoff: Mapping[str, Any]) -> bytes:
    return (json.dumps(dict(handoff), sort_keys=True, separators=(",", ":")) + "\n").encode()


def _prepare_committed_cohort_migration(
    *,
    state: Mapping[str, Any],
    config: SupervisorConfig,
    report: Mapping[str, Any],
) -> dict[str, bytes | None]:
    published = state.get("published")
    if not isinstance(published, dict):
        raise SupervisorError("supervisor state publication registry is invalid")
    try:
        old_instances = _normalize_instances(list(published))
    except SupervisorError as exc:
        raise SupervisorError("supervisor state cohort is invalid") from exc
    old_config = replace(config, instances=old_instances)
    if old_config.digest != state["config_digest"]:
        raise SupervisorError(
            "supervisor non-cohort config changed while capacity is committed",
        )

    old_set = set(old_instances)
    new_set = set(config.instances)
    added = new_set - old_set
    removed = old_set - new_set
    if (added and removed) or (not added and not removed):
        raise SupervisorError(
            "supervisor cohort rebind is forbidden while capacity is committed",
        )

    if removed:
        requests = report.get("requests")
        if not isinstance(requests, list):
            raise SupervisorError("broker request surface is invalid")
        for item in requests:
            if not isinstance(item, dict):
                raise SupervisorError("broker request record is invalid")
            request = item.get("request")
            lease = item.get("lease")
            if not isinstance(request, dict) or not isinstance(lease, dict):
                raise SupervisorError("broker request record is invalid")
            instance = f"{request.get('sandbox')}-{request.get('pool')}"
            if instance in removed and not _outside_scope_request_is_drained_terminal(
                request,
                lease,
            ):
                raise SupervisorError(
                    "supervisor cohort deletion requires drained terminal capacity",
                )

    current = _current_generation(config.handoff_dir)
    if current != state["generation"]:
        raise SupervisorError("supervisor current generation differs from durable state")
    current_path = config.handoff_dir / current
    manifest = _generation_manifest(
        generation=current,
        report_digest=str(state["report_digest"]),
        published=published,
    )
    selected = _publication_handoffs(report, old_config)
    _validate_generation_contents(
        current_path,
        manifest=manifest,
        selected=selected,
        config=old_config,
    )
    _validated_published_generation_files(current_path)

    retained: dict[str, bytes | None] = {}
    for instance in sorted(old_set & new_set):
        handoff = selected[instance]
        if handoff is None:
            retained[instance] = None
            continue
        path = _secure_generation_file(
            current_path / f"{instance}.json",
            label="retained generation handoff",
        )
        retained[instance] = path.read_bytes()
    return retained


def _validate_committed_cohort_migration(
    *,
    retained: Mapping[str, bytes | None],
    report: Mapping[str, Any],
    config: SupervisorConfig,
) -> None:
    selected = _publication_handoffs(report, config)
    for instance, before in retained.items():
        handoff = selected[instance]
        after = None if handoff is None else _canonical_handoff_bytes(handoff)
        if after != before:
            raise SupervisorError(
                "existing supervisor binding changed during cohort migration",
            )


def _outside_scope_request_is_drained_terminal(
    request: Mapping[str, Any],
    lease: Mapping[str, Any],
) -> bool:
    return (
        request.get("state") == "terminal"
        and lease.get("state") == "terminal"
        and all(
            type(lease.get(field)) is int and lease[field] == 0 for field in _DRAINED_LEASE_FIELDS
        )
    )


def _request_maps(
    report: Mapping[str, Any],
    config: SupervisorConfig,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    raw_requests = report.get("requests")
    raw_handoffs = report.get("handoffs")
    if not isinstance(raw_requests, list) or not isinstance(raw_handoffs, list):
        raise SupervisorError("broker report request/handoff surfaces are invalid")
    requests: dict[str, dict[str, Any]] = {}
    for item in raw_requests:
        if not isinstance(item, dict):
            raise SupervisorError("broker request record is invalid")
        request = item.get("request")
        lease = item.get("lease")
        if not isinstance(request, dict) or not isinstance(lease, dict):
            raise SupervisorError("broker request record is invalid")
        request_id = request.get("id")
        if not isinstance(request_id, str) or _UUID_RE.fullmatch(request_id) is None:
            raise SupervisorError("broker request id is invalid")
        if request_id in requests:
            raise SupervisorError("broker report contains duplicate request ids")
        sandbox_value = request.get("sandbox")
        pool_value = request.get("pool")
        if not isinstance(sandbox_value, str) or not isinstance(pool_value, str):
            raise SupervisorError("broker request instance is invalid")
        try:
            sandbox = SandboxId(sandbox_value)
        except ValueError as exc:
            raise SupervisorError("broker request sandbox id is invalid") from exc
        instance = f"{sandbox.value}-{pool_value}"
        try:
            _instance_parts(instance)
        except SupervisorError as exc:
            raise SupervisorError("broker request is outside supervisor scope") from exc
        if instance not in config.instances and not _outside_scope_request_is_drained_terminal(
            request,
            lease,
        ):
            raise SupervisorError("broker request is outside supervisor scope")
        requests[request_id] = {"request": request, "lease": lease, "instance": instance}
    handoffs: dict[str, dict[str, Any]] = {}
    for handoff in raw_handoffs:
        if not isinstance(handoff, dict):
            raise SupervisorError("broker handoff is invalid")
        request_id = handoff.get("request_id")
        if not isinstance(request_id, str) or request_id not in requests:
            raise SupervisorError("broker handoff request binding is invalid")
        if request_id in handoffs:
            raise SupervisorError("broker report contains duplicate handoffs")
        _validate_handoff(handoff, requests[request_id])
        handoffs[request_id] = handoff
    return requests, handoffs


def _validate_handoff(
    handoff: Mapping[str, Any],
    record: Mapping[str, Any],
) -> None:
    request = record["request"]
    lease = record["lease"]
    if (
        handoff.get("request_id") != request.get("id")
        or handoff.get("lease_epoch") != lease.get("lease_epoch")
        or handoff.get("sandbox") != request.get("sandbox")
        or handoff.get("environment") != f"sandbox-{request.get('sandbox')}"
        or handoff.get("candidate_sha") != request.get("candidate_sha")
        or handoff.get("pool_name") != request.get("pool")
        or handoff.get("max_slots") != lease.get("granted_slots")
        or handoff.get("preemptible") != request.get("preemptible")
    ):
        raise SupervisorError("broker handoff is not exactly request/lease bound")
    if _SHA_RE.fullmatch(str(handoff.get("candidate_sha"))) is None:
        raise SupervisorError("broker handoff candidate is invalid")
    enabled = int(lease.get("granted_slots") or 0) > 0 and not bool(request.get("cancel_requested"))
    if handoff.get("enabled") is not enabled:
        raise SupervisorError("broker handoff enabled state is invalid")


def _current_bindings(
    requests: Mapping[str, Mapping[str, Any]],
    handoffs: Mapping[str, dict[str, Any]],
    config: SupervisorConfig,
) -> dict[str, dict[str, Any] | None]:
    result: dict[str, dict[str, Any] | None] = {}
    for instance in config.instances:
        active = [
            record
            for record in requests.values()
            if record["instance"] == instance and record["request"].get("state") != "terminal"
        ]
        if len(active) > 1:
            raise SupervisorError(f"multiple nonterminal requests target {instance}")
        if not active:
            result[instance] = None
            continue
        request_id = str(active[0]["request"]["id"])
        handoff = handoffs.get(request_id)
        if handoff is None:
            raise SupervisorError("nonterminal request has no broker handoff")
        result[instance] = handoff
    return result


def _parse_observation(path: Path) -> LeaseObservation:
    payload = _read_json(path, label="adapter observation")
    if not isinstance(payload, list) or len(payload) != 1:
        raise SupervisorError("adapter observation must contain exactly one item")
    observation = payload[0]
    if not isinstance(observation, dict) or set(observation) != _OBSERVATION_FIELDS:
        raise SupervisorError("adapter observation fields do not match the closed schema")
    if _UUID_RE.fullmatch(str(observation.get("request_id"))) is None:
        raise SupervisorError("adapter observation request_id is invalid")
    try:
        return LeaseObservation.from_mapping(observation)
    except BrokerError as exc:
        raise SupervisorError(f"adapter observation is invalid: {exc}") from exc


def _collect_observations(
    config: SupervisorConfig,
    report: Mapping[str, Any],
    *,
    now: datetime,
) -> tuple[tuple[LeaseObservation, ...], dict[str, dict[str, Any]]]:
    if config.observation_dir.exists() or config.observation_dir.is_symlink():
        allowed = {f"{instance}.json" for instance in config.instances}
        try:
            unexpected = sorted(
                path.name
                for path in config.observation_dir.iterdir()
                if path.name.endswith(".json") and path.name not in allowed
            )
        except OSError as exc:
            raise SupervisorError("adapter observation directory is unavailable") from exc
        if unexpected:
            raise SupervisorError("adapter observation is outside the config snapshot")
    requests, handoffs = _request_maps(report, config)
    current = _current_bindings(requests, handoffs, config)
    accepted: list[LeaseObservation] = []
    evidence: dict[str, dict[str, Any]] = {}
    for instance in config.instances:
        path = config.observation_dir / f"{instance}.json"
        if not path.exists() and not path.is_symlink():
            evidence[instance] = {"status": "missing"}
            continue
        observation = _parse_observation(path)
        request_id = observation.request_id
        record = requests.get(request_id)
        if record is None:
            raise SupervisorError("adapter observation references an unknown request")
        binding = current[instance]
        if binding is None or request_id != binding["request_id"]:
            request = record["request"]
            lease = record["lease"]
            terminal_tombstone = (
                request.get("state") == "terminal"
                and int(lease.get("committed_slots") or 0) == 0
                and observation.sandbox == request.get("sandbox")
                and observation.pool_name == request.get("pool")
                and observation.candidate_sha == request.get("candidate_sha")
                and observation.lease_epoch == int(lease.get("lease_epoch") or 0)
                and observation.capacity_lease_state in {"retiring", "retired"}
                and observation.observation_sequence
                >= int(lease.get("last_observation_sequence") or 0)
                and observation.pending_slots == 0
                and observation.active_slots == 0
                and observation.draining_slots == 0
                and observation.terminal_slots >= int(lease.get("terminal_slots") or 0)
            )
            if terminal_tombstone:
                evidence[instance] = {
                    "status": "terminal_tombstone_ignored",
                    "request_id": request_id,
                    "lease_epoch": observation.lease_epoch,
                    "observation_sequence": observation.observation_sequence,
                    "digest": observation.payload_sha256,
                }
                continue
            raise SupervisorError("adapter observation request is not the current binding")
        expected_epoch = int(binding["lease_epoch"])
        observed_epoch = observation.lease_epoch
        if observed_epoch != expected_epoch:
            raise SupervisorError("adapter observation lease_epoch is not current")
        expected_state = {"active"} if bool(binding["enabled"]) else {"retiring", "retired"}
        if (
            observation.sandbox != record["request"]["sandbox"]
            or observation.pool_name != binding["pool_name"]
            or observation.candidate_sha != binding["candidate_sha"]
            or observation.capacity_lease_state not in expected_state
        ):
            raise SupervisorError("adapter observation binding is not current")
        observed_at = datetime.fromisoformat(
            observation.observed_at.replace("Z", "+00:00"),
        ).astimezone(UTC)
        if observed_at > now + _OBSERVATION_MAX_FUTURE_SKEW:
            raise SupervisorError("adapter observation timestamp is in the future")
        if now - observed_at > _OBSERVATION_MAX_AGE:
            raise SupervisorError("adapter observation is stale")
        lease = record["lease"]
        previous_sequence = lease.get("last_observation_sequence")
        previous_digest = lease.get("last_observation_digest")
        if previous_sequence is not None:
            if observation.observation_sequence < int(previous_sequence):
                raise SupervisorError("adapter observation sequence regressed")
            if observation.observation_sequence == int(previous_sequence):
                if observation.payload_sha256 != previous_digest:
                    raise SupervisorError(
                        "adapter observation sequence was rebound to another payload",
                    )
                evidence[instance] = {
                    "status": "duplicate_ignored",
                    "request_id": request_id,
                    "lease_epoch": observed_epoch,
                    "observation_sequence": observation.observation_sequence,
                    "digest": observation.payload_sha256,
                }
                continue
        accepted.append(observation)
        evidence[instance] = {
            "status": "accepted",
            "request_id": request_id,
            "lease_epoch": observed_epoch,
            "observation_sequence": observation.observation_sequence,
            "observed_at": observation.observed_at,
            "digest": observation.payload_sha256,
        }
    return tuple(accepted), evidence


def _validate_report_budgets(
    report: Mapping[str, Any],
    config: SupervisorConfig,
) -> None:
    budgets = report.get("budgets")
    aggregate = report.get("aggregate")
    if not isinstance(budgets, dict) or not isinstance(aggregate, dict):
        raise SupervisorError("broker budget report is invalid")
    expected_budgets = {
        "global_slots": config.global_slot_budget,
        "global_pending_slots": config.global_pending_slot_budget,
        "pool_slots": config.pool_slot_budgets,
        "pool_pending_slots": config.pool_pending_slot_budgets,
    }
    if budgets != expected_budgets:
        raise SupervisorError("broker persisted budgets differ from supervisor config")
    requests, handoffs = _request_maps(report, config)
    _current_bindings(requests, handoffs, config)
    pool_committed = {pool: 0 for pool in config.pool_slot_budgets}
    pool_pending = {pool: 0 for pool in config.pool_slot_budgets}
    recomputed = {
        "requested_slots": 0,
        "granted_slots": 0,
        "active_slots": 0,
        "pending_slots": 0,
        "draining_slots": 0,
        "terminal_slots": 0,
        "committed_slots": 0,
    }
    for record in requests.values():
        request = record["request"]
        lease = record["lease"]
        pool = request.get("pool")
        if pool not in pool_committed:
            if request.get("state") == "terminal":
                continue
            raise SupervisorError("broker report contains an unbudgeted pool")
        target = int(request.get("target_slots") or 0)
        granted = int(lease.get("granted_slots") or 0)
        committed = int(lease.get("committed_slots") or 0)
        pending = int(lease.get("pending_slots") or 0)
        expected_committed = max(
            granted,
            pending + int(lease.get("active_slots") or 0) + int(lease.get("draining_slots") or 0),
        )
        if committed != expected_committed:
            raise SupervisorError("broker committed slots do not match lease counters")
        if granted > target or granted > config.pool_slot_budgets[pool]:
            raise SupervisorError("broker grant exceeds request or reviewed pool budget")
        pool_committed[pool] += committed
        pool_pending[pool] += pending
        recomputed["requested_slots"] += target
        for field in (
            "granted_slots",
            "active_slots",
            "pending_slots",
            "draining_slots",
            "terminal_slots",
            "committed_slots",
        ):
            recomputed[field] += int(lease.get(field) or 0)
    if aggregate != recomputed:
        raise SupervisorError("broker aggregate does not match request records")
    if recomputed["committed_slots"] > config.global_slot_budget:
        raise SupervisorError("broker committed slots exceed the global budget")
    if recomputed["pending_slots"] > config.global_pending_slot_budget:
        raise SupervisorError("broker pending slots exceed the global pending budget")
    for pool in config.pool_slot_budgets:
        if pool_committed[pool] > config.pool_slot_budgets[pool]:
            raise SupervisorError(f"broker committed slots exceed {pool} budget")
        if pool_pending[pool] > config.pool_pending_slot_budgets[pool]:
            raise SupervisorError(f"broker pending slots exceed {pool} pending budget")


def _publication_handoffs(
    report: Mapping[str, Any],
    config: SupervisorConfig,
) -> dict[str, dict[str, Any] | None]:
    requests, handoffs = _request_maps(report, config)
    current = _current_bindings(requests, handoffs, config)
    result: dict[str, dict[str, Any] | None] = {}
    for instance in config.instances:
        if current[instance] is not None:
            result[instance] = current[instance]
            continue
        terminal = [
            record
            for record in requests.values()
            if record["instance"] == instance
            and record["request"].get("state") == "terminal"
            and record["request"].get("id") in handoffs
        ]
        if not terminal:
            result[instance] = None
            continue
        record = max(
            terminal,
            key=lambda item: (
                str(item["lease"].get("updated_at") or ""),
                str(item["request"].get("id") or ""),
            ),
        )
        handoff = handoffs[str(record["request"]["id"])]
        if handoff.get("enabled") is not False or handoff.get("max_slots") != 0:
            raise SupervisorError("terminal publication is not a zero handoff")
        result[instance] = handoff
    return result


def _atomic_json_write(path: Path, payload: object) -> str:
    _private_parent(path.parent)
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    digest = hashlib.sha256(canonical.encode()).hexdigest()
    existing = _secure_regular_file(path, label="supervisor output", required=False)
    if existing is not None:
        try:
            if existing.read_text(encoding="utf-8") == canonical:
                return digest
        except (OSError, UnicodeError) as exc:
            raise SupervisorError("supervisor output is unreadable") from exc
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(canonical)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        path.chmod(0o600)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()
    return digest


def _secure_generation_dir(path: Path, *, label: str) -> Path:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise SupervisorError(f"{label} is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISDIR(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or metadata.st_uid != os.geteuid()
        or metadata.st_gid != os.getegid()
    ):
        raise SupervisorError(f"{label} must be an owner-private regular directory")
    return path


def _secure_generation_file(path: Path, *, label: str) -> Path:
    resolved = _secure_regular_file(path, label=label)
    assert resolved is not None
    metadata = resolved.stat()
    if metadata.st_uid != os.geteuid() or metadata.st_gid != os.getegid():
        raise SupervisorError(f"{label} must be owner-bound")
    return resolved


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _current_generation(handoff_dir: Path) -> str | None:
    current = handoff_dir / "current"
    if not current.exists() and not current.is_symlink():
        return None
    try:
        metadata = current.lstat()
        target = os.readlink(current)
    except OSError as exc:
        raise SupervisorError("current handoff generation is unreadable") from exc
    if (
        not stat.S_ISLNK(metadata.st_mode)
        or Path(target).is_absolute()
        or len(Path(target).parts) != 1
        or _GENERATION_RE.fullmatch(target) is None
    ):
        raise SupervisorError("current handoff generation link is unsafe")
    _secure_generation_dir(
        handoff_dir / target,
        label="current handoff generation",
    )
    return target


def _generation_manifest(
    *,
    generation: str,
    report_digest: str,
    published: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": _SCHEMA_VERSION,
        "artifact_type": "shared-capacity-handoff-generation",
        "generation": generation,
        "report_digest": report_digest,
        "instances": dict(published),
    }


def _validate_generation_contents(
    path: Path,
    *,
    manifest: Mapping[str, Any],
    selected: Mapping[str, Mapping[str, Any] | None],
    config: SupervisorConfig,
) -> None:
    _secure_generation_dir(path, label="handoff generation")
    actual_manifest = _read_json(path / "manifest.json", label="handoff manifest")
    if actual_manifest != manifest:
        raise SupervisorError("existing handoff generation manifest drifted")
    allowed = {"manifest.json"} | {f"{item}.json" for item in config.instances}
    actual_names = {item.name for item in path.iterdir()}
    if not actual_names <= allowed:
        raise SupervisorError("handoff generation contains unexpected files")
    for instance in config.instances:
        handoff_path = path / f"{instance}.json"
        handoff = selected[instance]
        if handoff is None:
            if handoff_path.exists() or handoff_path.is_symlink():
                raise SupervisorError("absent handoff contract contains a file")
            continue
        actual = _read_json(handoff_path, label="generation handoff")
        if actual != handoff:
            raise SupervisorError("generation handoff differs from broker output")


def _validated_published_generation_files(path: Path) -> tuple[Path, ...]:
    _secure_generation_dir(path, label="obsolete handoff generation")
    manifest_path = _secure_generation_file(
        path / "manifest.json",
        label="obsolete handoff manifest",
    )
    manifest = _read_json(manifest_path, label="obsolete handoff manifest")
    required = {
        "schema_version",
        "artifact_type",
        "generation",
        "report_digest",
        "instances",
    }
    if (
        not isinstance(manifest, dict)
        or set(manifest) != required
        or manifest.get("schema_version") != _SCHEMA_VERSION
        or manifest.get("artifact_type") != "shared-capacity-handoff-generation"
        or manifest.get("generation") != path.name
        or _GENERATION_RE.fullmatch(path.name) is None
        or _DIGEST_RE.fullmatch(str(manifest.get("report_digest"))) is None
        or path.name.rpartition("-")[2] != str(manifest.get("report_digest"))[:16]
        or not isinstance(manifest.get("instances"), dict)
        or not manifest["instances"]
    ):
        raise SupervisorError("obsolete handoff manifest is invalid")
    instances = manifest["instances"]
    try:
        normalized_instances = _normalize_instances(list(instances))
    except SupervisorError as exc:
        raise SupervisorError("obsolete handoff manifest instance registry is invalid") from exc
    if set(normalized_instances) != set(instances):
        raise SupervisorError("obsolete handoff manifest instance registry is invalid")
    published_files: list[Path] = []
    expected_names = {"manifest.json"}
    for instance in normalized_instances:
        entry = instances[instance]
        if not isinstance(entry, dict):
            raise SupervisorError("obsolete handoff manifest entry is invalid")
        status_value = entry.get("status")
        if status_value == "absent":
            if set(entry) != {"status"}:
                raise SupervisorError("obsolete absent handoff entry is invalid")
            continue
        if (
            status_value != "published"
            or set(entry) != {"status", "request_id", "lease_epoch", "digest"}
            or _UUID_RE.fullmatch(str(entry.get("request_id"))) is None
            or type(entry.get("lease_epoch")) is not int
            or entry["lease_epoch"] < 0
            or _DIGEST_RE.fullmatch(str(entry.get("digest"))) is None
        ):
            raise SupervisorError("obsolete published handoff entry is invalid")
        handoff_path = _secure_generation_file(
            path / f"{instance}.json",
            label="obsolete generation handoff",
        )
        handoff = _read_json(handoff_path, label="obsolete generation handoff")
        sandbox, pool = _instance_parts(instance)
        if (
            not isinstance(handoff, dict)
            or set(handoff) != _HANDOFF_FIELDS
            or handoff.get("request_id") != entry["request_id"]
            or handoff.get("lease_epoch") != entry["lease_epoch"]
            or handoff.get("sandbox") != sandbox.value
            or handoff.get("environment") != sandbox.environment
            or handoff.get("pool_name") != pool
            or _SHA_RE.fullmatch(str(handoff.get("candidate_sha"))) is None
            or type(handoff.get("enabled")) is not bool
            or type(handoff.get("min_slots")) is not int
            or type(handoff.get("max_slots")) is not int
            or type(handoff.get("preemptible")) is not bool
        ):
            raise SupervisorError("obsolete generation handoff binding is invalid")
        canonical = json.dumps(handoff, sort_keys=True, separators=(",", ":")) + "\n"
        if hashlib.sha256(canonical.encode()).hexdigest() != entry["digest"]:
            raise SupervisorError("obsolete generation handoff digest drifted")
        expected_names.add(handoff_path.name)
        published_files.append(handoff_path)
    try:
        actual_names = {item.name for item in path.iterdir()}
    except OSError as exc:
        raise SupervisorError("obsolete handoff generation is unreadable") from exc
    if actual_names != expected_names:
        raise SupervisorError("obsolete handoff generation contains unexpected files")
    return (*published_files, manifest_path)


def _validated_staging_generation_files(
    path: Path,
    config: SupervisorConfig,
) -> tuple[Path, ...]:
    _secure_generation_dir(path, label="staging handoff generation")
    allowed = {"manifest.json"} | {f"{item}.json" for item in config.instances}
    entries = list(path.iterdir())
    if any(item.name not in allowed for item in entries):
        raise SupervisorError("staging handoff generation contains unexpected files")
    return tuple(_secure_generation_file(item, label="staging generation file") for item in entries)


def _remove_staging_generation_dir(path: Path, config: SupervisorConfig) -> None:
    for resolved in _validated_staging_generation_files(path, config):
        resolved.unlink()
    path.rmdir()


def _materialize_generation(
    *,
    generation: str,
    selected: Mapping[str, Mapping[str, Any] | None],
    published: Mapping[str, Mapping[str, Any]],
    report_digest: str,
    config: SupervisorConfig,
) -> None:
    final = config.handoff_dir / generation
    manifest = _generation_manifest(
        generation=generation,
        report_digest=report_digest,
        published=published,
    )
    if final.exists() or final.is_symlink():
        _validate_generation_contents(
            final,
            manifest=manifest,
            selected=selected,
            config=config,
        )
        return
    staging = config.handoff_dir / f".{generation}.tmp-{os.getpid()}"
    if staging.exists() or staging.is_symlink():
        raise SupervisorError("handoff generation staging path already exists")
    staging.mkdir(mode=0o700)
    try:
        for instance in config.instances:
            handoff = selected[instance]
            if handoff is not None:
                _atomic_json_write(staging / f"{instance}.json", handoff)
        _atomic_json_write(staging / "manifest.json", manifest)
        _fsync_directory(staging)
        os.rename(staging, final)
        _fsync_directory(config.handoff_dir)
    finally:
        if staging.exists():
            _remove_staging_generation_dir(staging, config)


def _flip_current_generation(handoff_dir: Path, generation: str) -> None:
    temporary = handoff_dir / f".current.tmp-{os.getpid()}"
    if temporary.exists() or temporary.is_symlink():
        raise SupervisorError("temporary current-generation link already exists")
    try:
        os.symlink(generation, temporary)
        os.replace(temporary, handoff_dir / "current")
        _fsync_directory(handoff_dir)
    finally:
        if temporary.is_symlink():
            temporary.unlink()


def _prune_generations(
    config: SupervisorConfig,
    *,
    current: str,
    previous: str | None,
) -> None:
    keep = {current}
    if previous is not None:
        keep.add(previous)
    validated: list[tuple[Path, tuple[Path, ...]]] = []
    for path in sorted(config.handoff_dir.iterdir()):
        if path.name in keep or path.name == "current":
            continue
        if (
            _GENERATION_RE.fullmatch(path.name) is None
            and _STAGING_GENERATION_RE.fullmatch(path.name) is None
        ):
            continue
        if _GENERATION_RE.fullmatch(path.name) is not None:
            files = _validated_published_generation_files(path)
        else:
            files = _validated_staging_generation_files(path, config)
        validated.append((path, files))
    for path, files in validated:
        for item in files:
            item.unlink()
        path.rmdir()
    _fsync_directory(config.handoff_dir)


def _validate_prunable_generations(
    config: SupervisorConfig,
    *,
    current: str,
    previous: str | None,
) -> None:
    keep = {current}
    if previous is not None:
        keep.add(previous)
    for path in sorted(config.handoff_dir.iterdir()):
        if path.name in keep or path.name == "current":
            continue
        if _GENERATION_RE.fullmatch(path.name) is not None:
            _validated_published_generation_files(path)
        elif _STAGING_GENERATION_RE.fullmatch(path.name) is not None:
            _validated_staging_generation_files(path, config)


def _previous_generation(handoff_dir: Path, current: str) -> str | None:
    candidates = sorted(
        (
            path.name
            for path in handoff_dir.iterdir()
            if path.name != current and _GENERATION_RE.fullmatch(path.name) is not None
        ),
        reverse=True,
    )
    return candidates[0] if candidates else None


def _publish_handoffs(
    report: Mapping[str, Any],
    config: SupervisorConfig,
    *,
    sequence: int,
    report_digest: str,
) -> tuple[dict[str, dict[str, Any]], str, str | None]:
    selected = _publication_handoffs(report, config)
    published: dict[str, dict[str, Any]] = {}
    for instance in config.instances:
        handoff = selected[instance]
        if handoff is None:
            published[instance] = {"status": "absent"}
            continue
        canonical = json.dumps(handoff, sort_keys=True, separators=(",", ":")) + "\n"
        published[instance] = {
            "status": "published",
            "request_id": handoff["request_id"],
            "lease_epoch": handoff["lease_epoch"],
            "digest": hashlib.sha256(canonical.encode()).hexdigest(),
        }
    generation = f"generation-{sequence:020d}-{report_digest[:16]}"
    _private_parent(config.handoff_dir)
    previous = _current_generation(config.handoff_dir)
    if previous is not None:
        previous_path = config.handoff_dir / previous
        previous_manifest = _read_json(
            previous_path / "manifest.json",
            label="current handoff manifest",
        )
        if not isinstance(previous_manifest, dict):
            raise SupervisorError("current handoff manifest is invalid")
        if (
            previous_manifest.get("report_digest") == report_digest
            and previous_manifest.get("instances") == published
        ):
            _validate_generation_contents(
                previous_path,
                manifest=previous_manifest,
                selected=selected,
                config=config,
            )
            retained_previous = _previous_generation(config.handoff_dir, previous)
            _validate_prunable_generations(
                config,
                current=previous,
                previous=retained_previous,
            )
            _prune_generations(
                config,
                current=previous,
                previous=retained_previous,
            )
            return published, previous, retained_previous
    _validate_prunable_generations(
        config,
        current=generation,
        previous=previous,
    )
    _materialize_generation(
        generation=generation,
        selected=selected,
        published=published,
        report_digest=report_digest,
        config=config,
    )
    _flip_current_generation(config.handoff_dir, generation)
    _prune_generations(
        config,
        current=generation,
        previous=previous,
    )
    return published, generation, previous


def _last_audit_sequence(path: Path) -> int:
    resolved = _secure_regular_file(path, label="supervisor audit", required=False)
    if resolved is None:
        return 0
    try:
        if resolved.stat().st_size > _MAX_AUDIT_BYTES:
            raise SupervisorError("supervisor audit exceeds the bounded size")
        lines = resolved.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise SupervisorError("supervisor audit is unreadable") from exc
    if not lines:
        return 0
    try:
        event = json.loads(lines[-1])
    except json.JSONDecodeError as exc:
        raise SupervisorError("supervisor audit tail is invalid") from exc
    sequence = event.get("sequence") if isinstance(event, dict) else None
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
        raise SupervisorError("supervisor audit sequence is invalid")
    return sequence


def _append_audit(path: Path, event: Mapping[str, Any]) -> None:
    _private_parent(path.parent)
    if path.exists() or path.is_symlink():
        _secure_regular_file(path, label="supervisor audit")
    line = json.dumps(dict(event), sort_keys=True, separators=(",", ":")) + "\n"
    descriptor = os.open(
        path,
        os.O_WRONLY
        | os.O_APPEND
        | os.O_CREAT
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        os.write(descriptor, line.encode())
        os.fsync(descriptor)
        os.fchmod(descriptor, 0o600)
    finally:
        os.close(descriptor)


def _run_once_unlocked(
    config: SupervisorConfig,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = (now or datetime.now(UTC)).astimezone(UTC)
    broker = SharedCapacityBroker(config.state_db, clock=lambda: now)
    before = broker.status()
    before_aggregate = before.get("aggregate")
    if not isinstance(before_aggregate, dict):
        raise SupervisorError("broker aggregate report is invalid")
    state = _load_supervisor_state(config.supervisor_state_path)
    retained_handoffs: dict[str, bytes | None] | None = None
    if (
        state is not None
        and state["config_digest"] != config.digest
        and int(before_aggregate["committed_slots"]) != 0
    ):
        retained_handoffs = _prepare_committed_cohort_migration(
            state=state,
            config=config,
            report=before,
        )
    observations, observation_evidence = _collect_observations(
        config,
        before,
        now=now,
    )
    report = broker.reconcile(
        BrokerBudgets(
            global_slots=config.global_slot_budget,
            pool_slots=config.pool_slot_budgets,
            global_pending_slots=config.global_pending_slot_budget,
            pool_pending_slots=config.pool_pending_slot_budgets,
        ),
        observations=observations,
    )
    _validate_report_budgets(report, config)
    if retained_handoffs is not None:
        _validate_committed_cohort_migration(
            retained=retained_handoffs,
            report=report,
            config=config,
        )
    report_digest = _digest(report)
    previous_sequence = int(state["cycle_sequence"]) if state is not None else 0
    sequence = max(previous_sequence, _last_audit_sequence(config.audit_path)) + 1
    published, generation, previous_generation = _publish_handoffs(
        report,
        config,
        sequence=sequence,
        report_digest=report_digest,
    )
    timestamp = now.isoformat().replace("+00:00", "Z")
    event = {
        "schema_version": _SCHEMA_VERSION,
        "sequence": sequence,
        "occurred_at": timestamp,
        "config_digest": config.digest,
        "report_digest": report_digest,
        "generation": generation,
        "previous_generation": previous_generation,
        "observations": observation_evidence,
        "published": published,
        "aggregate": report["aggregate"],
        "budgets": report["budgets"],
    }
    _append_audit(config.audit_path, event)
    supervisor_state = {
        "schema_version": _SCHEMA_VERSION,
        "cycle_sequence": sequence,
        "config_digest": config.digest,
        "report_digest": report_digest,
        "generation": generation,
        "published": published,
        "updated_at": timestamp,
    }
    _atomic_json_write(config.supervisor_state_path, supervisor_state)
    _atomic_json_write(
        config.evidence_path,
        {
            "schema_version": _SCHEMA_VERSION,
            "artifact_type": "shared-capacity-supervisor-cycle",
            "cycle": event,
            "broker_report": report,
        },
    )
    return {
        "schema_version": _SCHEMA_VERSION,
        "artifact_type": "shared-capacity-supervisor-result",
        "status": "reconciled",
        "cycle_sequence": sequence,
        "report_digest": report_digest,
        "generation": generation,
        "observations": observation_evidence,
        "published": published,
        "aggregate": report["aggregate"],
        "budgets": report["budgets"],
    }


def run_once(
    config: SupervisorConfig,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    with _exclusive_supervisor_lock(config):
        return _run_once_unlocked(config, now=now)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("command", choices=("run",))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        report = run_once(load_config(args.config))
    except (
        BrokerError,
        SupervisorError,
        OSError,
        UnicodeError,
        ValueError,
        sqlite3.Error,
    ):
        sys.stderr.write('{"error":"shared-capacity-supervisor-failed-safely"}\n')
        return 1
    sys.stdout.write(json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
