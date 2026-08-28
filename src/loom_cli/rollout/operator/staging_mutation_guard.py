"""Request-bound coordination between protected rollout and staging lifecycle GC."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import pwd
import re
import signal
import stat
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import Any, Literal, Protocol, cast, overload
from uuid import uuid4

from loom.staging_mutation_coordination import (
    STAGING_MUTATION_TRY_LOCK_SQL,
    STAGING_MUTATION_UNLOCK_SQL,
)
from loom_cli.cluster_backup_guard import DEFAULT_BACKUP_MAX_ELAPSED_SECONDS
from loom_cli.rollout.credential_authority import (
    converge_new_private_file,
    read_trusted_file,
)
from loom_cli.rollout.final_gate_command_runner import FINAL_GATE_MAX_ELAPSED_SECONDS
from loom_cli.rollout.readonly_database_authority import DatabaseQuery

from .config import OperatorConfig, candidate_sha_from_runner_repo
from .envelope import fixed_operator_config_path
from .model import validate_safe_identifier
from .policy import sanitized_child_environment
from .readonly_database_client import (
    READONLY_DATABASE_STATEMENT_TIMEOUT_SECONDS,
    READONLY_DATABASE_TUNNEL_TEARDOWN_BOUND_SECONDS,
    open_readonly_database_guard_query,
)

_NAMESPACE = "loom-staging"
_CRONJOB_NAME = "loom-staging-data-lifecycle"
_CRONJOB_RESOURCE = f"cronjob/{_CRONJOB_NAME}"
_SCHEDULE = "*/5 * * * *"
_CONCURRENCY_POLICY = "Forbid"
_FIELD_MANAGER = "loom-staging-rollout"
_REQUEST_ANNOTATION = "loom.carin.dev/staging-mutation-guard-request"
_CANDIDATE_ANNOTATION = "loom.carin.dev/staging-mutation-guard-candidate-sha"
_TREE_ANNOTATION = "loom.carin.dev/staging-mutation-guard-candidate-tree"
_GUARD_ANNOTATIONS = frozenset({_REQUEST_ANNOTATION, _CANDIDATE_ANNOTATION, _TREE_ANNOTATION})
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_GENERATION_RE = re.compile(r"^[0-9a-f]{32}$")
_UID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_RESOURCE_VERSION_RE = re.compile(r"^[1-9][0-9]*$")
_JOB_NAME_RE = re.compile(r"^loom-staging-data-lifecycle-[1-9][0-9]*$")
_MAX_KUBERNETES_OUTPUT = 1024 * 1024
_MAX_EVIDENCE_BYTES = 64 * 1024
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
MUTATION_GUARD_KUBERNETES_COMMAND_TIMEOUT_SECONDS = 120
MUTATION_GUARD_SYSTEMD_COMMAND_TIMEOUT_SECONDS = 30
_KUBERNETES_COMMAND_TIMEOUT_SECONDS = MUTATION_GUARD_KUBERNETES_COMMAND_TIMEOUT_SECONDS
_SYSTEMD_COMMAND_TIMEOUT_SECONDS = MUTATION_GUARD_SYSTEMD_COMMAND_TIMEOUT_SECONDS
_CANDIDATE_COMMAND_TIMEOUT_SECONDS = 15
_STOP_REACTION_DELAY_SECONDS = 1
_EVIDENCE_PUBLICATION_MARGIN_SECONDS = 30
MUTATION_GUARD_NORMAL_RELEASE_BOUND_SECONDS = (
    MUTATION_GUARD_SYSTEMD_COMMAND_TIMEOUT_SECONDS  # in-flight owner inventory
    + _STOP_REACTION_DELAY_SECONDS  # steady-state poll sleep
    + READONLY_DATABASE_STATEMENT_TIMEOUT_SECONDS  # next lock-health query
    + 2 * MUTATION_GUARD_KUBERNETES_COMMAND_TIMEOUT_SECONDS  # CronJob GET plus PATCH
    + READONLY_DATABASE_STATEMENT_TIMEOUT_SECONDS  # advisory unlock query
    + READONLY_DATABASE_TUNNEL_TEARDOWN_BOUND_SECONDS
    + _EVIDENCE_PUBLICATION_MARGIN_SECONDS
)
_DEFAULT_ACTIVE_WAITS = 1320
_DEFAULT_LOCK_ATTEMPTS = 120
_DEFAULT_OWNER_LAUNCH_GRACE_SECONDS = 30
_GUARD_READINESS_SECONDS = 1_500
_GUARD_OPERATIONAL_MARGIN_SECONDS = 5 * 60 * 60
MUTATION_GUARD_RUNTIME_SECONDS = (
    DEFAULT_BACKUP_MAX_ELAPSED_SECONDS
    + FINAL_GATE_MAX_ELAPSED_SECONDS
    + _GUARD_READINESS_SECONDS
    + _GUARD_OPERATIONAL_MARGIN_SECONDS
)
_GUARD_INTERNAL_LIFETIME_SECONDS = MUTATION_GUARD_RUNTIME_SECONDS - 5 * 60

_TRY_LOCK_SQL = f"{STAGING_MUTATION_TRY_LOCK_SQL} AS acquired"
_READ_EPOCH_SQL = (
    "SELECT epoch AS mutation_epoch FROM staging_mutation_epochs "
    "WHERE environment = 'staging' AND namespace = 'loom-staging'"
)
_UNLOCK_SQL = f"{STAGING_MUTATION_UNLOCK_SQL} AS released"
_HEALTH_SQL = (
    "SELECT pg_backend_pid() AS backend_pid, count(*) = 1 AS owns_lock "
    "FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid() "
    "AND classid = 1280263818 AND objid = 1621151599 AND objsubid = 1 "
    "AND mode = 'ExclusiveLock' AND granted"
)


class MutationGuardError(RuntimeError):
    """Raised when request-bound mutation coordination cannot be proven safe."""


class CommandResult(Protocol):
    @property
    def returncode(self) -> int: ...

    @property
    def stdout(self) -> str: ...

    @property
    def stderr(self) -> str: ...


KubernetesRunner = Callable[[list[str]], CommandResult]
QueryContext = Callable[..., AbstractContextManager[DatabaseQuery]]
CandidateResolver = Callable[[OperatorConfig], tuple[str, str]]
GuardState = Literal["ready", "released"]


def validate_mutation_guard_generation(generation: str) -> str:
    """Return one exact non-secret per-acquisition generation."""

    if not isinstance(generation, str) or _GENERATION_RE.fullmatch(generation) is None:
        raise MutationGuardError("mutation guard generation identity is invalid")
    return generation


def _hash_json(value: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(dict(value), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class MutationGuardEvidence:
    schema_version: int
    request_id: str
    candidate_sha: str
    candidate_tree: str
    generation: str
    mutation_epoch: int
    guard_pid: int
    database_backend_pid: int
    deadline_unix_seconds: int
    cronjob_uid: str
    suspended_resource_version: str
    state: GuardState
    evidence_digest: str

    def __post_init__(self) -> None:
        try:
            validate_safe_identifier(self.request_id, "request_id")
        except ValueError as exc:
            raise MutationGuardError("mutation guard evidence request identity is invalid") from exc
        if (
            self.schema_version != 1
            or _SHA_RE.fullmatch(self.candidate_sha) is None
            or _SHA_RE.fullmatch(self.candidate_tree) is None
            or _GENERATION_RE.fullmatch(self.generation) is None
            or type(self.mutation_epoch) is not int
            or self.mutation_epoch < 0
            or type(self.guard_pid) is not int
            or self.guard_pid < 1
            or type(self.database_backend_pid) is not int
            or self.database_backend_pid < 1
            or type(self.deadline_unix_seconds) is not int
            or self.deadline_unix_seconds < 1
            or _UID_RE.fullmatch(self.cronjob_uid) is None
            or _RESOURCE_VERSION_RE.fullmatch(self.suspended_resource_version) is None
            or self.state not in {"ready", "released"}
            or not re.fullmatch(r"[0-9a-f]{64}", self.evidence_digest)
        ):
            raise MutationGuardError("mutation guard evidence identity is invalid")
        if _hash_json(self._record(include_digest=False)) != self.evidence_digest:
            raise MutationGuardError("mutation guard evidence digest is invalid")

    def _record(self, *, include_digest: bool) -> dict[str, object]:
        value: dict[str, object] = {
            "candidate_sha": self.candidate_sha,
            "candidate_tree": self.candidate_tree,
            "cronjob_uid": self.cronjob_uid,
            "database_backend_pid": self.database_backend_pid,
            "deadline_unix_seconds": self.deadline_unix_seconds,
            "generation": self.generation,
            "guard_pid": self.guard_pid,
            "mutation_epoch": self.mutation_epoch,
            "request_id": self.request_id,
            "schema_version": self.schema_version,
            "state": self.state,
            "suspended_resource_version": self.suspended_resource_version,
        }
        if include_digest:
            value["evidence_digest"] = self.evidence_digest
        return value

    def to_dict(self) -> dict[str, object]:
        return self._record(include_digest=True)

    @classmethod
    def build(
        cls,
        *,
        request_id: str,
        candidate_sha: str,
        candidate_tree: str,
        generation: str,
        mutation_epoch: int,
        guard_pid: int,
        database_backend_pid: int,
        deadline_unix_seconds: int,
        cronjob_uid: str,
        suspended_resource_version: str,
        state: GuardState,
    ) -> MutationGuardEvidence:
        value: dict[str, object] = {
            "candidate_sha": candidate_sha,
            "candidate_tree": candidate_tree,
            "cronjob_uid": cronjob_uid,
            "database_backend_pid": database_backend_pid,
            "deadline_unix_seconds": deadline_unix_seconds,
            "generation": generation,
            "guard_pid": guard_pid,
            "mutation_epoch": mutation_epoch,
            "request_id": request_id,
            "schema_version": 1,
            "state": state,
            "suspended_resource_version": suspended_resource_version,
        }
        return cls(
            schema_version=1,
            request_id=request_id,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            generation=generation,
            mutation_epoch=mutation_epoch,
            guard_pid=guard_pid,
            database_backend_pid=database_backend_pid,
            deadline_unix_seconds=deadline_unix_seconds,
            cronjob_uid=cronjob_uid,
            suspended_resource_version=suspended_resource_version,
            state=state,
            evidence_digest=_hash_json(value),
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> MutationGuardEvidence:
        if set(value) != {
            "candidate_sha",
            "candidate_tree",
            "cronjob_uid",
            "database_backend_pid",
            "deadline_unix_seconds",
            "evidence_digest",
            "generation",
            "guard_pid",
            "mutation_epoch",
            "request_id",
            "schema_version",
            "state",
            "suspended_resource_version",
        }:
            raise MutationGuardError("mutation guard evidence fields are invalid")
        try:
            return cls(**cast(Any, dict(value)))
        except TypeError as exc:
            raise MutationGuardError("mutation guard evidence fields are invalid") from exc


@dataclass(frozen=True, slots=True)
class _ActiveJob:
    name: str
    uid: str


@dataclass(frozen=True, slots=True)
class _CronJob:
    uid: str
    resource_version: str
    annotations: Mapping[str, str]
    suspended: bool
    active_jobs: tuple[_ActiveJob, ...]


class _MutationGuardUnitStatus(Protocol):
    @property
    def is_running(self) -> bool: ...

    @property
    def main_pid(self) -> int: ...


class _MutationGuardSystemd(Protocol):
    def start_mutation_guard(self, request_id: str) -> MutationGuardEvidence: ...

    def show_mutation_guard(self, request_id: str) -> _MutationGuardUnitStatus | None: ...

    def stop_mutation_guard(self, request_id: str) -> MutationGuardEvidence | None: ...


def _validate_config(config: OperatorConfig) -> None:
    if (
        config.short_name != "staging"
        or config.environment != "staging"
        or config.namespace != _NAMESPACE
        or not config.runtime_root.is_absolute()
        or ".." in config.runtime_root.parts
    ):
        raise MutationGuardError("mutation guard configuration authority is invalid")


def guard_evidence_path(config: OperatorConfig, request_id: str) -> Path:
    _validate_config(config)
    try:
        validate_safe_identifier(request_id, "request_id")
    except ValueError as exc:
        raise MutationGuardError("mutation guard request identity is invalid") from exc
    return config.runtime_root / "mutation-guards" / f"{request_id}.json"


def _decode_json(payload: str, label: str) -> dict[str, object]:
    if not payload or len(payload.encode()) > _MAX_KUBERNETES_OUTPUT:
        raise MutationGuardError(f"{label} output is invalid")
    try:
        value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
    except (json.JSONDecodeError, ValueError) as exc:
        raise MutationGuardError(f"{label} output is invalid") from exc
    if not isinstance(value, dict):
        raise MutationGuardError(f"{label} output is invalid")
    return cast(dict[str, object], value)


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise MutationGuardError(f"{label} is invalid")
    return cast(dict[str, object], value)


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("JSON contains duplicate fields")
        value[key] = item
    return value


def _run_json(run: KubernetesRunner, argv: list[str], label: str) -> dict[str, object]:
    try:
        result = run(argv)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise MutationGuardError(f"{label} command failed safely") from exc
    if (
        type(result.returncode) is not int
        or result.returncode != 0
        or not isinstance(result.stdout, str)
        or not isinstance(result.stderr, str)
        or len(result.stderr.encode()) > _MAX_KUBERNETES_OUTPUT
    ):
        raise MutationGuardError(f"{label} command failed safely")
    return _decode_json(result.stdout, label)


def _kubectl_prefix(config: OperatorConfig) -> list[str]:
    return [
        "kubectl",
        "--kubeconfig",
        str(config.kubeconfig_path),
        "--namespace",
        _NAMESPACE,
    ]


def _load_cronjob(config: OperatorConfig, run: KubernetesRunner) -> _CronJob:
    value = _run_json(
        run,
        [
            *_kubectl_prefix(config),
            "get",
            _CRONJOB_RESOURCE,
            "--output=json",
            "--request-timeout=60s",
        ],
        "lifecycle CronJob",
    )
    return _parse_cronjob_value(value)


def _parse_cronjob_value(value: dict[str, object]) -> _CronJob:
    metadata = _mapping(value.get("metadata"), "lifecycle CronJob metadata")
    spec = _mapping(value.get("spec"), "lifecycle CronJob spec")
    status = _mapping(value.get("status", {}), "lifecycle CronJob status")
    labels = _mapping(metadata.get("labels"), "lifecycle CronJob labels")
    raw_annotations = _mapping(metadata.get("annotations", {}), "lifecycle CronJob annotations")
    annotations: dict[str, str] = {}
    for key, item in raw_annotations.items():
        if not isinstance(item, str) or len(key) > 256 or len(item) > 1024:
            raise MutationGuardError("lifecycle CronJob annotation authority is invalid")
        annotations[key] = item
    uid = metadata.get("uid")
    resource_version = metadata.get("resourceVersion")
    suspended = spec.get("suspend")
    if (
        value.get("apiVersion") != "batch/v1"
        or value.get("kind") != "CronJob"
        or metadata.get("name") != _CRONJOB_NAME
        or metadata.get("namespace") != _NAMESPACE
        or labels != {"app": _CRONJOB_NAME}
        or not isinstance(uid, str)
        or _UID_RE.fullmatch(uid) is None
        or not isinstance(resource_version, str)
        or _RESOURCE_VERSION_RE.fullmatch(resource_version) is None
        or spec.get("schedule") != _SCHEDULE
        or spec.get("concurrencyPolicy") != _CONCURRENCY_POLICY
        or type(suspended) is not bool
    ):
        if spec.get("schedule") != _SCHEDULE:
            raise MutationGuardError("lifecycle CronJob schedule authority drifted")
        if spec.get("concurrencyPolicy") != _CONCURRENCY_POLICY:
            raise MutationGuardError("lifecycle CronJob concurrency authority drifted")
        raise MutationGuardError("lifecycle CronJob identity authority drifted")
    active = status.get("active", [])
    if not isinstance(active, list) or len(active) > 16:
        raise MutationGuardError("lifecycle CronJob active state is invalid")
    active_jobs: list[_ActiveJob] = []
    for raw in active:
        reference = _mapping(raw, "lifecycle CronJob active reference")
        name = reference.get("name")
        job_uid = reference.get("uid")
        if (
            reference.get("apiVersion") != "batch/v1"
            or reference.get("kind") != "Job"
            or reference.get("namespace") != _NAMESPACE
            or not isinstance(name, str)
            or _JOB_NAME_RE.fullmatch(name) is None
            or not isinstance(job_uid, str)
            or _UID_RE.fullmatch(job_uid) is None
        ):
            raise MutationGuardError("lifecycle CronJob active reference is invalid")
        active_jobs.append(_ActiveJob(name=name, uid=job_uid))
    return _CronJob(
        uid=uid,
        resource_version=resource_version,
        annotations=annotations,
        suspended=suspended,
        active_jobs=tuple(active_jobs),
    )


def _guard_annotations(request_id: str, candidate_sha: str, candidate_tree: str) -> dict[str, str]:
    return {
        _REQUEST_ANNOTATION: request_id,
        _CANDIDATE_ANNOTATION: candidate_sha,
        _TREE_ANNOTATION: candidate_tree,
    }


def _guard_annotation_state(cronjob: _CronJob) -> dict[str, str]:
    return {key: value for key, value in cronjob.annotations.items() if key in _GUARD_ANNOTATIONS}


def _require_held(
    cronjob: _CronJob,
    *,
    uid: str,
    request_id: str,
    candidate_sha: str,
    candidate_tree: str,
) -> None:
    if cronjob.uid != uid:
        raise MutationGuardError("lifecycle CronJob UID authority drifted")
    if not cronjob.suspended or _guard_annotation_state(cronjob) != _guard_annotations(
        request_id, candidate_sha, candidate_tree
    ):
        raise MutationGuardError("lifecycle CronJob guard annotation authority drifted")


def _patch_cronjob(
    config: OperatorConfig,
    run: KubernetesRunner,
    *,
    resource_version: str,
    suspend: bool,
    annotations: Mapping[str, str | None],
) -> _CronJob:
    patch = json.dumps(
        {
            "metadata": {
                "annotations": dict(annotations),
                "resourceVersion": resource_version,
            },
            "spec": {"suspend": suspend},
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    value = _run_json(
        run,
        [
            *_kubectl_prefix(config),
            "patch",
            _CRONJOB_RESOURCE,
            "--type=merge",
            f"--field-manager={_FIELD_MANAGER}",
            "--patch",
            patch,
            "--output=json",
            "--request-timeout=60s",
        ],
        "lifecycle CronJob patch",
    )
    return _parse_cronjob_value(value)


def _list_nonterminal_owned_jobs(
    config: OperatorConfig,
    run: KubernetesRunner,
    *,
    cronjob_uid: str,
) -> tuple[_ActiveJob, ...]:
    value = _run_json(
        run,
        [
            *_kubectl_prefix(config),
            "get",
            "jobs",
            f"--selector=batch.kubernetes.io/controller-uid={cronjob_uid}",
            "--output=json",
            "--request-timeout=60s",
        ],
        "lifecycle owned Job inventory",
    )
    list_metadata = _mapping(value.get("metadata"), "lifecycle owned Job inventory metadata")
    resource_version = list_metadata.get("resourceVersion")
    items = value.get("items")
    if (
        value.get("apiVersion") != "v1"
        or value.get("kind") != "List"
        or not isinstance(resource_version, str)
        or (resource_version and _RESOURCE_VERSION_RE.fullmatch(resource_version) is None)
        or not isinstance(items, list)
        or len(items) > 256
    ):
        raise MutationGuardError("lifecycle owned Job inventory is invalid")
    active: list[_ActiveJob] = []
    for raw_job in items:
        job = _mapping(raw_job, "lifecycle owned Job")
        metadata = _mapping(job.get("metadata"), "lifecycle owned Job metadata")
        labels = _mapping(metadata.get("labels"), "lifecycle owned Job labels")
        owners = metadata.get("ownerReferences")
        name = metadata.get("name")
        uid = metadata.get("uid")
        if not isinstance(owners, list) or len(owners) != 1:
            raise MutationGuardError("lifecycle owned Job owner authority drifted")
        owner = _mapping(owners[0], "lifecycle owned Job owner")
        if (
            job.get("apiVersion") != "batch/v1"
            or job.get("kind") != "Job"
            or not isinstance(name, str)
            or _JOB_NAME_RE.fullmatch(name) is None
            or metadata.get("namespace") != _NAMESPACE
            or not isinstance(uid, str)
            or _UID_RE.fullmatch(uid) is None
            or labels.get("batch.kubernetes.io/controller-uid") != cronjob_uid
            or labels.get("batch.kubernetes.io/job-name") != name
            or owner
            != {
                "apiVersion": "batch/v1",
                "blockOwnerDeletion": True,
                "controller": True,
                "kind": "CronJob",
                "name": _CRONJOB_NAME,
                "uid": cronjob_uid,
            }
        ):
            raise MutationGuardError("lifecycle owned Job owner authority drifted")
        status = _mapping(job.get("status", {}), "lifecycle owned Job status")
        conditions = status.get("conditions", [])
        if not isinstance(conditions, list) or len(conditions) > 32:
            raise MutationGuardError("lifecycle owned Job terminal state is invalid")
        terminal = False
        for raw_condition in conditions:
            condition = _mapping(raw_condition, "lifecycle owned Job condition")
            condition_type = condition.get("type")
            condition_status = condition.get("status")
            if not isinstance(condition_type, str) or not isinstance(condition_status, str):
                raise MutationGuardError("lifecycle owned Job terminal state is invalid")
            if condition_type in {"Complete", "Failed"} and condition_status == "True":
                terminal = True
        if not terminal:
            active.append(_ActiveJob(name=name, uid=uid))
    return tuple(active)


def _require_before_readiness_deadline(
    monotonic: Callable[[], float],
    *,
    deadline_monotonic: float,
) -> None:
    observed = monotonic()
    if not math.isfinite(observed):
        raise MutationGuardError("mutation guard clock authority was lost")
    if observed >= deadline_monotonic:
        raise MutationGuardError("mutation guard absolute deadline expired before readiness")


def _wait_until_inactive(
    config: OperatorConfig,
    run: KubernetesRunner,
    held: _CronJob,
    *,
    request_id: str,
    candidate_sha: str,
    candidate_tree: str,
    sleep: Callable[[float], None],
    stop_requested: Callable[[], bool],
    monotonic: Callable[[], float],
    deadline_monotonic: float,
    max_waits: int,
) -> _CronJob:
    current = held
    consecutive_empty = 0
    remaining_waits = max_waits
    while True:
        _require_before_readiness_deadline(
            monotonic,
            deadline_monotonic=deadline_monotonic,
        )
        if stop_requested():
            raise MutationGuardError("mutation guard stop requested before readiness")
        _require_held(
            current,
            uid=held.uid,
            request_id=request_id,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
        )
        active_jobs = _list_nonterminal_owned_jobs(
            config,
            run,
            cronjob_uid=held.uid,
        )
        _require_before_readiness_deadline(
            monotonic,
            deadline_monotonic=deadline_monotonic,
        )
        consecutive_empty = consecutive_empty + 1 if not active_jobs else 0
        if consecutive_empty == 2:
            return current
        if remaining_waits == 0:
            break
        remaining_waits -= 1
        sleep(1.0)
        _require_before_readiness_deadline(
            monotonic,
            deadline_monotonic=deadline_monotonic,
        )
        current = _load_cronjob(config, run)
        _require_before_readiness_deadline(
            monotonic,
            deadline_monotonic=deadline_monotonic,
        )
    raise MutationGuardError("lifecycle active Job did not finish within the guard bound")


def _restore_cronjob(
    config: OperatorConfig,
    run: KubernetesRunner,
    *,
    uid: str,
    request_id: str,
    candidate_sha: str,
    candidate_tree: str,
) -> None:
    current = _load_cronjob(config, run)
    if current.uid != uid:
        raise MutationGuardError("lifecycle CronJob UID authority drifted during release")
    guard_state = _guard_annotation_state(current)
    if not current.suspended and not guard_state:
        return
    if guard_state != _guard_annotations(request_id, candidate_sha, candidate_tree):
        raise MutationGuardError("lifecycle CronJob guard annotation authority drifted")
    restored = _patch_cronjob(
        config,
        run,
        resource_version=current.resource_version,
        suspend=False,
        annotations={key: None for key in _GUARD_ANNOTATIONS},
    )
    if restored.uid != uid or restored.suspended or _guard_annotation_state(restored):
        raise MutationGuardError("lifecycle CronJob release verification failed")


@overload
def _parse_scalar(
    rows: tuple[Mapping[str, object], ...],
    *,
    key: str,
    expected_type: type[bool],
) -> bool: ...


@overload
def _parse_scalar(
    rows: tuple[Mapping[str, object], ...],
    *,
    key: str,
    expected_type: type[int],
) -> int: ...


def _parse_scalar(
    rows: tuple[Mapping[str, object], ...],
    *,
    key: str,
    expected_type: type[bool] | type[int],
) -> bool | int:
    if len(rows) != 1 or set(rows[0]) != {key}:
        raise MutationGuardError("mutation guard database evidence is invalid")
    value = rows[0][key]
    if type(value) is not expected_type:
        raise MutationGuardError("mutation guard database evidence is invalid")
    return value


def _parse_lock_health(
    rows: tuple[Mapping[str, object], ...],
) -> tuple[int, bool]:
    if len(rows) != 1 or set(rows[0]) != {"backend_pid", "owns_lock"}:
        raise MutationGuardError("mutation guard database lock health is invalid")
    backend_pid = rows[0]["backend_pid"]
    owns_lock = rows[0]["owns_lock"]
    if type(backend_pid) is not int or backend_pid < 1 or type(owns_lock) is not bool:
        raise MutationGuardError("mutation guard database lock health is invalid")
    return backend_pid, owns_lock


def _require_lock_health(query: DatabaseQuery, *, backend_pid: int | None = None) -> int:
    observed_pid, owns_lock = _parse_lock_health(query(_HEALTH_SQL))
    if not owns_lock or (backend_pid is not None and observed_pid != backend_pid):
        raise MutationGuardError("mutation guard database ownership was lost")
    return observed_pid


def _ensure_evidence_directory(config: OperatorConfig, *, service_uid: int) -> Path:
    root = config.runtime_root
    metadata = root.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != service_uid
        or stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE
    ):
        raise MutationGuardError("mutation guard runtime authority is unsafe")
    directory = root / "mutation-guards"
    try:
        directory.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
    except FileExistsError:
        pass
    metadata = directory.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != service_uid
        or stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE
    ):
        raise MutationGuardError("mutation guard evidence directory is unsafe")
    return directory


def _publish_evidence(
    config: OperatorConfig,
    evidence: MutationGuardEvidence,
    *,
    service_uid: int,
) -> Path:
    directory = _ensure_evidence_directory(config, service_uid=service_uid)
    path = guard_evidence_path(config, evidence.request_id)
    payload = (
        json.dumps(evidence.to_dict(), sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()
    if len(payload) > _MAX_EVIDENCE_BYTES:
        raise MutationGuardError("mutation guard evidence exceeds its bound")
    temporary = f".{path.name}.{uuid4().hex}.tmp"
    directory_fd = os.open(
        directory,
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    created = False
    try:
        fd = os.open(
            temporary,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            _PRIVATE_FILE_MODE,
            dir_fd=directory_fd,
        )
        created = True
        try:
            converge_new_private_file(fd, service_uid=service_uid)
            offset = 0
            while offset < len(payload):
                written = os.write(fd, payload[offset:])
                if written <= 0:  # pragma: no cover - os.write contract
                    raise OSError("mutation guard evidence write made no progress")
                offset += written
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(temporary, path.name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
        created = False
        os.fsync(directory_fd)
    except (OSError, ValueError) as exc:
        raise MutationGuardError("mutation guard evidence could not be published safely") from exc
    finally:
        if created:
            try:
                os.unlink(temporary, dir_fd=directory_fd)
            except OSError:
                pass
        os.close(directory_fd)
    return path


def read_mutation_guard_evidence(
    path: Path,
    *,
    service_uid: int,
) -> MutationGuardEvidence:
    try:
        trusted = read_trusted_file(
            path,
            service_uid=service_uid,
            private=True,
            max_bytes=_MAX_EVIDENCE_BYTES,
            require_nonempty=True,
        )
        value = json.loads(trusted.payload, object_pairs_hook=_reject_duplicate_keys)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise MutationGuardError("mutation guard evidence is unsafe") from exc
    if not isinstance(value, dict):
        raise MutationGuardError("mutation guard evidence is unsafe")
    return MutationGuardEvidence.from_dict(cast(dict[str, object], value))


def _resolve_candidate(config: OperatorConfig) -> tuple[str, str]:
    expected_sha = candidate_sha_from_runner_repo(config.runner_repo)
    environment = sanitized_child_environment(config, service_uid=os.geteuid())

    def git(revision: str) -> str:
        try:
            result = subprocess.run(
                [
                    "/usr/bin/git",
                    "-C",
                    str(config.runner_repo),
                    "rev-parse",
                    "--verify",
                    revision,
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=_CANDIDATE_COMMAND_TIMEOUT_SECONDS,
                env=environment,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise MutationGuardError("mutation guard candidate identity is unavailable") from exc
        value = result.stdout.strip()
        if result.returncode != 0 or _SHA_RE.fullmatch(value) is None:
            raise MutationGuardError("mutation guard candidate identity is unavailable")
        return value

    observed_sha = git("HEAD")
    observed_tree = git("HEAD^{tree}")
    if observed_sha != expected_sha:
        raise MutationGuardError("mutation guard candidate identity drifted")
    return observed_sha, observed_tree


@dataclass(slots=True)
class MutationGuardManager:
    """Bind broker and worker guard operations to one installed candidate."""

    config: OperatorConfig
    service_uid: int
    systemd: _MutationGuardSystemd
    resolve_candidate: CandidateResolver = _resolve_candidate
    wall_time: Callable[[], float] = time.time

    def _validate(
        self,
        evidence: MutationGuardEvidence,
        *,
        request_id: str,
        state: GuardState,
    ) -> MutationGuardEvidence:
        _validate_config(self.config)
        if self.service_uid < 1:
            raise MutationGuardError("mutation guard manager authority is invalid")
        candidate_sha, candidate_tree = self.resolve_candidate(self.config)
        observed_wall_time = self.wall_time()
        if (
            evidence.request_id != request_id
            or evidence.candidate_sha != candidate_sha
            or evidence.candidate_tree != candidate_tree
            or evidence.state != state
            or (
                state == "ready"
                and (
                    not math.isfinite(observed_wall_time)
                    or evidence.deadline_unix_seconds <= math.floor(observed_wall_time)
                )
            )
        ):
            raise MutationGuardError("mutation guard evidence binding drifted")
        return evidence

    def acquire(self, request_id: str) -> MutationGuardEvidence:
        evidence = self.systemd.start_mutation_guard(request_id)
        try:
            return self._validate(evidence, request_id=request_id, state="ready")
        except Exception as validation_error:
            try:
                released = self.systemd.stop_mutation_guard(request_id)
            except Exception as release_error:
                raise MutationGuardError(
                    "drifted mutation guard could not be released safely"
                ) from release_error
            if (
                released is None
                or released.request_id != request_id
                or released.generation != evidence.generation
                or released.state != "released"
            ):
                raise MutationGuardError(
                    "drifted mutation guard release was not verified"
                ) from validation_error
            raise

    def assert_ready(self, request_id: str) -> MutationGuardEvidence:
        status = self.systemd.show_mutation_guard(request_id)
        if status is None or not status.is_running or status.main_pid < 1:
            raise MutationGuardError("mutation guard unit is not ready")
        evidence = read_mutation_guard_evidence(
            guard_evidence_path(self.config, request_id),
            service_uid=self.service_uid,
        )
        if evidence.guard_pid != status.main_pid:
            raise MutationGuardError("mutation guard process identity drifted")
        return self._validate(evidence, request_id=request_id, state="ready")

    def release(self, request_id: str) -> MutationGuardEvidence:
        evidence = self.systemd.stop_mutation_guard(request_id)
        if evidence is None:
            raise MutationGuardError("mutation guard release evidence is absent")
        return self._validate(evidence, request_id=request_id, state="released")


def _validate_reconcile_evidence(
    evidence: MutationGuardEvidence,
    *,
    cronjob: _CronJob,
    request_id: str,
    candidate_sha: str,
    candidate_tree: str,
) -> None:
    if (
        evidence.request_id != request_id
        or evidence.candidate_sha != candidate_sha
        or evidence.candidate_tree != candidate_tree
        or evidence.cronjob_uid != cronjob.uid
    ):
        raise MutationGuardError("orphaned mutation guard evidence binding drifted")


def _read_reconcile_evidence(
    *,
    config: OperatorConfig,
    service_uid: int,
    cronjob: _CronJob,
    request_id: str,
    candidate_sha: str,
    candidate_tree: str,
) -> MutationGuardEvidence | None:
    evidence_path = guard_evidence_path(config, request_id)
    try:
        os.lstat(evidence_path)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise MutationGuardError("orphaned mutation guard evidence is unavailable") from exc
    try:
        evidence = read_mutation_guard_evidence(
            evidence_path,
            service_uid=service_uid,
        )
    except MutationGuardError as exc:
        raise MutationGuardError("orphaned mutation guard evidence is unsafe") from exc
    _validate_reconcile_evidence(
        evidence,
        cronjob=cronjob,
        request_id=request_id,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
    )
    return evidence


def _fence_and_require_stable_owner_absence(
    *,
    request_id: str,
    fence_owners: Callable[[str], None] | None,
    owner_running: Callable[[str], bool] | None,
    sleep: Callable[[float], None],
) -> None:
    if fence_owners is None or owner_running is None:
        raise MutationGuardError("orphaned mutation guard owner recovery is unavailable")
    try:
        fence_owners(request_id)
    except Exception as exc:
        raise MutationGuardError("orphaned mutation guard owner fence failed safely") from exc
    for observation in range(2):
        try:
            live_owner = owner_running(request_id)
        except Exception as exc:
            raise MutationGuardError("orphaned mutation guard owner absence is unsafe") from exc
        if type(live_owner) is not bool:
            raise MutationGuardError("orphaned mutation guard owner absence is unsafe")
        if live_owner:
            raise MutationGuardError("orphaned mutation guard owner remains live")
        if observation == 0:
            try:
                sleep(1.0)
            except Exception as exc:
                raise MutationGuardError("orphaned mutation guard owner absence is unsafe") from exc


def reconcile_orphaned_guard(
    *,
    config: OperatorConfig,
    service_uid: int,
    run: KubernetesRunner,
    show_guard: Callable[[str], _MutationGuardUnitStatus | None],
    fence_owners: Callable[[str], None] | None = None,
    owner_running: Callable[[str], bool] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, str]:
    """Restore only an exact annotated freeze whose request unit is absent."""

    _validate_config(config)
    if service_uid < 1 or os.geteuid() != service_uid:
        raise MutationGuardError("mutation guard reconciler authority is invalid")
    cronjob = _load_cronjob(config, run)
    annotations = _guard_annotation_state(cronjob)
    if not annotations:
        if cronjob.suspended:
            raise MutationGuardError("lifecycle CronJob is suspended without guard annotations")
        return {"status": "idle"}
    if set(annotations) != _GUARD_ANNOTATIONS:
        raise MutationGuardError("lifecycle CronJob guard annotations are incomplete")
    request_id = annotations[_REQUEST_ANNOTATION]
    candidate_sha = annotations[_CANDIDATE_ANNOTATION]
    candidate_tree = annotations[_TREE_ANNOTATION]
    try:
        validate_safe_identifier(request_id, "request_id")
    except ValueError as exc:
        raise MutationGuardError("lifecycle CronJob guard annotation is invalid") from exc
    if _SHA_RE.fullmatch(candidate_sha) is None or _SHA_RE.fullmatch(candidate_tree) is None:
        raise MutationGuardError("lifecycle CronJob guard annotation is invalid")
    if not cronjob.suspended:
        raise MutationGuardError("lifecycle CronJob guard annotations require suspension")
    installed_candidate_sha, installed_candidate_tree = _resolve_candidate(config)
    if (candidate_sha, candidate_tree) != (installed_candidate_sha, installed_candidate_tree):
        raise MutationGuardError("lifecycle CronJob guard candidate authority drifted")
    status = show_guard(request_id)
    if status is not None:
        if not status.is_running or status.main_pid < 1:
            raise MutationGuardError("orphaned mutation guard unit is not absent")
        try:
            evidence = read_mutation_guard_evidence(
                guard_evidence_path(config, request_id),
                service_uid=service_uid,
            )
        except MutationGuardError as exc:
            raise MutationGuardError("active mutation guard evidence is unsafe") from exc
        _validate_reconcile_evidence(
            evidence,
            cronjob=cronjob,
            request_id=request_id,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
        )
        if evidence.state != "ready" or evidence.guard_pid != status.main_pid:
            raise MutationGuardError("active mutation guard evidence binding drifted")
        return {"request_id": request_id, "status": "active"}

    initial_evidence: MutationGuardEvidence | None = None
    evidence_error: MutationGuardError | None = None
    try:
        initial_evidence = _read_reconcile_evidence(
            config=config,
            service_uid=service_uid,
            cronjob=cronjob,
            request_id=request_id,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
        )
    except MutationGuardError as exc:
        evidence_error = exc
    if initial_evidence is not None and initial_evidence.state == "released":
        raise MutationGuardError("orphaned mutation guard released evidence contradicts suspension")
    _fence_and_require_stable_owner_absence(
        request_id=request_id,
        fence_owners=fence_owners,
        owner_running=owner_running,
        sleep=sleep,
    )
    if evidence_error is not None:
        raise MutationGuardError("orphaned mutation guard evidence is unsafe") from evidence_error
    final_evidence = _read_reconcile_evidence(
        config=config,
        service_uid=service_uid,
        cronjob=cronjob,
        request_id=request_id,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
    )
    if final_evidence is not None and final_evidence.state == "released":
        raise MutationGuardError("orphaned mutation guard released evidence contradicts suspension")
    if final_evidence != initial_evidence:
        raise MutationGuardError("orphaned mutation guard evidence changed during recovery")
    _restore_cronjob(
        config,
        run,
        uid=cronjob.uid,
        request_id=request_id,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
    )
    return {"request_id": request_id, "status": "restored"}


def hold_request_guard(
    *,
    config: OperatorConfig,
    request_id: str,
    generation: str,
    service_uid: int,
    run: KubernetesRunner,
    query_context: QueryContext = open_readonly_database_guard_query,
    resolve_candidate: CandidateResolver = _resolve_candidate,
    stop_requested: Callable[[], bool],
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    wall_time: Callable[[], float] = time.time,
    owner_running: Callable[[str], bool] = lambda _request_id: True,
    owner_launch_grace_seconds: int = _DEFAULT_OWNER_LAUNCH_GRACE_SECONDS,
    max_active_waits: int = _DEFAULT_ACTIVE_WAITS,
    max_lock_attempts: int = _DEFAULT_LOCK_ATTEMPTS,
) -> MutationGuardEvidence:
    """Freeze the legacy CronJob and hold the shared lock until stopped."""

    _validate_config(config)
    if (
        service_uid < 1
        or os.geteuid() != service_uid
        or not 1 <= max_active_waits <= _DEFAULT_ACTIVE_WAITS
        or not 1 <= max_lock_attempts <= _DEFAULT_LOCK_ATTEMPTS
        or not 0 <= owner_launch_grace_seconds <= 300
    ):
        raise MutationGuardError("mutation guard process authority is invalid")
    try:
        validate_safe_identifier(request_id, "request_id")
    except ValueError as exc:
        raise MutationGuardError("mutation guard request identity is invalid") from exc
    validate_mutation_guard_generation(generation)
    candidate_sha, candidate_tree = resolve_candidate(config)
    if _SHA_RE.fullmatch(candidate_sha) is None or _SHA_RE.fullmatch(candidate_tree) is None:
        raise MutationGuardError("mutation guard candidate identity is invalid")
    started_monotonic = monotonic()
    started_wall_time = wall_time()
    if (
        not math.isfinite(started_monotonic)
        or not math.isfinite(started_wall_time)
        or started_wall_time <= 0
    ):
        raise MutationGuardError("mutation guard clock authority is invalid")
    deadline_monotonic = started_monotonic + _GUARD_INTERNAL_LIFETIME_SECONDS
    deadline_unix_seconds = math.ceil(started_wall_time) + _GUARD_INTERNAL_LIFETIME_SECONDS
    _require_before_readiness_deadline(
        monotonic,
        deadline_monotonic=deadline_monotonic,
    )
    initial = _load_cronjob(config, run)
    _require_before_readiness_deadline(
        monotonic,
        deadline_monotonic=deadline_monotonic,
    )
    expected_annotations = _guard_annotations(request_id, candidate_sha, candidate_tree)
    observed_annotations = _guard_annotation_state(initial)
    if observed_annotations:
        raise MutationGuardError(
            "lifecycle CronJob is already annotated; annotation authority is occupied"
        )
    if initial.suspended:
        raise MutationGuardError("lifecycle CronJob is already suspended")
    restored = False
    acquired = False
    unsafe_loss = False
    ready_published = False
    ready: MutationGuardEvidence | None = None
    try:
        _require_before_readiness_deadline(
            monotonic,
            deadline_monotonic=deadline_monotonic,
        )
        held = _patch_cronjob(
            config,
            run,
            resource_version=initial.resource_version,
            suspend=True,
            annotations=expected_annotations,
        )
        _require_before_readiness_deadline(
            monotonic,
            deadline_monotonic=deadline_monotonic,
        )
        _require_held(
            held,
            uid=initial.uid,
            request_id=request_id,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
        )
        held = _wait_until_inactive(
            config,
            run,
            held,
            request_id=request_id,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            sleep=sleep,
            stop_requested=stop_requested,
            monotonic=monotonic,
            deadline_monotonic=deadline_monotonic,
            max_waits=max_active_waits,
        )
        with query_context(service_uid=service_uid) as query:
            try:
                for attempt in range(max_lock_attempts):
                    _require_before_readiness_deadline(
                        monotonic,
                        deadline_monotonic=deadline_monotonic,
                    )
                    if stop_requested():
                        raise MutationGuardError("mutation guard stop requested before readiness")
                    acquired = _parse_scalar(
                        query(_TRY_LOCK_SQL), key="acquired", expected_type=bool
                    )
                    _require_before_readiness_deadline(
                        monotonic,
                        deadline_monotonic=deadline_monotonic,
                    )
                    if acquired:
                        break
                    if attempt + 1 < max_lock_attempts:
                        sleep(1.0)
                        _require_before_readiness_deadline(
                            monotonic,
                            deadline_monotonic=deadline_monotonic,
                        )
                if not acquired:
                    raise MutationGuardError("staging mutation advisory lock is unavailable")
                backend_pid = _require_lock_health(query)
                _require_before_readiness_deadline(
                    monotonic,
                    deadline_monotonic=deadline_monotonic,
                )
                post_lock = _load_cronjob(config, run)
                _require_before_readiness_deadline(
                    monotonic,
                    deadline_monotonic=deadline_monotonic,
                )
                _require_held(
                    post_lock,
                    uid=initial.uid,
                    request_id=request_id,
                    candidate_sha=candidate_sha,
                    candidate_tree=candidate_tree,
                )
                held = _wait_until_inactive(
                    config,
                    run,
                    post_lock,
                    request_id=request_id,
                    candidate_sha=candidate_sha,
                    candidate_tree=candidate_tree,
                    sleep=sleep,
                    stop_requested=stop_requested,
                    monotonic=monotonic,
                    deadline_monotonic=deadline_monotonic,
                    max_waits=max_active_waits,
                )
                _require_lock_health(query, backend_pid=backend_pid)
                _require_before_readiness_deadline(
                    monotonic,
                    deadline_monotonic=deadline_monotonic,
                )
                mutation_epoch = _parse_scalar(
                    query(_READ_EPOCH_SQL), key="mutation_epoch", expected_type=int
                )
                _require_before_readiness_deadline(
                    monotonic,
                    deadline_monotonic=deadline_monotonic,
                )
                if mutation_epoch < 0:
                    raise MutationGuardError("mutation guard epoch evidence is invalid")
                ready = MutationGuardEvidence.build(
                    request_id=request_id,
                    candidate_sha=candidate_sha,
                    candidate_tree=candidate_tree,
                    generation=generation,
                    mutation_epoch=mutation_epoch,
                    guard_pid=os.getpid(),
                    database_backend_pid=backend_pid,
                    deadline_unix_seconds=deadline_unix_seconds,
                    cronjob_uid=initial.uid,
                    suspended_resource_version=held.resource_version,
                    state="ready",
                )
                _require_before_readiness_deadline(
                    monotonic,
                    deadline_monotonic=deadline_monotonic,
                )
                _publish_evidence(config, ready, service_uid=service_uid)
                ready_published = True
                launch_grace_deadline = monotonic() + owner_launch_grace_seconds
                try:
                    while True:
                        try:
                            _require_lock_health(query, backend_pid=backend_pid)
                        except BaseException as exc:
                            unsafe_loss = True
                            raise MutationGuardError(
                                "mutation guard database ownership was lost"
                            ) from exc
                        now = monotonic()
                        if not math.isfinite(now):
                            unsafe_loss = True
                            raise MutationGuardError("mutation guard clock authority was lost")
                        if stop_requested():
                            break
                        if now >= deadline_monotonic:
                            try:
                                live_owner = owner_running(request_id)
                            except BaseException as exc:
                                unsafe_loss = True
                                raise MutationGuardError(
                                    "mutation guard owner liveness is unsafe"
                                ) from exc
                            if live_owner:
                                unsafe_loss = True
                                raise MutationGuardError(
                                    "mutation guard absolute deadline expired with a live owner"
                                )
                            break
                        if now >= launch_grace_deadline:
                            try:
                                live_owner = owner_running(request_id)
                            except BaseException as exc:
                                unsafe_loss = True
                                raise MutationGuardError(
                                    "mutation guard owner liveness is unsafe"
                                ) from exc
                            if not live_owner:
                                break
                        sleep(1.0)
                except BaseException:
                    if ready_published:
                        unsafe_loss = True
                    raise
            finally:
                release_error: BaseException | None = None
                if not unsafe_loss:
                    try:
                        _restore_cronjob(
                            config,
                            run,
                            uid=initial.uid,
                            request_id=request_id,
                            candidate_sha=candidate_sha,
                            candidate_tree=candidate_tree,
                        )
                        restored = True
                    except BaseException as exc:
                        release_error = exc
                    if acquired:
                        try:
                            unlocked = _parse_scalar(
                                query(_UNLOCK_SQL), key="released", expected_type=bool
                            )
                            if unlocked is not True:
                                raise MutationGuardError(
                                    "staging mutation advisory unlock was not confirmed"
                                )
                        except BaseException as exc:
                            if release_error is None:
                                release_error = exc
                    if release_error is not None:
                        raise MutationGuardError(
                            "mutation guard release failed safely"
                        ) from release_error
    finally:
        if not restored and not unsafe_loss:
            _restore_cronjob(
                config,
                run,
                uid=initial.uid,
                request_id=request_id,
                candidate_sha=candidate_sha,
                candidate_tree=candidate_tree,
            )
    if ready is None:
        raise MutationGuardError("mutation guard readiness evidence was not published")
    released_evidence = MutationGuardEvidence.build(
        request_id=ready.request_id,
        candidate_sha=ready.candidate_sha,
        candidate_tree=ready.candidate_tree,
        generation=ready.generation,
        mutation_epoch=ready.mutation_epoch,
        guard_pid=ready.guard_pid,
        database_backend_pid=ready.database_backend_pid,
        deadline_unix_seconds=ready.deadline_unix_seconds,
        cronjob_uid=ready.cronjob_uid,
        suspended_resource_version=ready.suspended_resource_version,
        state="released",
    )
    _publish_evidence(config, released_evidence, service_uid=service_uid)
    return released_evidence


def _run(
    argv: list[str],
    *,
    environment: Mapping[str, str],
    timeout_seconds: int,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        env=dict(environment),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="command", required=True)
    hold = subparsers.add_parser("hold", allow_abbrev=False)
    hold.add_argument("--request-id", required=True)
    hold.add_argument("--generation", required=True)
    fence = subparsers.add_parser("fence", allow_abbrev=False)
    fence.add_argument("--request-id", required=True)
    fence.add_argument("--generation", required=True)
    subparsers.add_parser("reconcile", allow_abbrev=False)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(list(argv) if argv is not None else None)
    except SystemExit as exc:
        return exc.code if type(exc.code) is int else 2
    stopped = Event()

    def request_stop(_signum: int, _frame: object) -> None:
        stopped.set()

    previous_term = signal.signal(signal.SIGTERM, request_stop)
    previous_int = signal.signal(signal.SIGINT, request_stop)
    try:
        config = OperatorConfig.load(fixed_operator_config_path())
        service_uid = pwd.getpwnam(config.service_user).pw_uid
        if os.geteuid() != service_uid:
            raise MutationGuardError("mutation guard effective UID is invalid")
        environment = sanitized_child_environment(config, service_uid=service_uid)

        def run(command: list[str]) -> subprocess.CompletedProcess[str]:
            return _run(
                command,
                environment=environment,
                timeout_seconds=_KUBERNETES_COMMAND_TIMEOUT_SECONDS,
            )

        def run_systemd(command: list[str]) -> subprocess.CompletedProcess[str]:
            return _run(
                command,
                environment=environment,
                timeout_seconds=_SYSTEMD_COMMAND_TIMEOUT_SECONDS,
            )

        from .systemd import SystemdUserManager

        systemd = SystemdUserManager(
            config,
            service_uid=service_uid,
            run=run_systemd,
        )
        if args.command == "hold":
            hold_request_guard(
                config=config,
                request_id=args.request_id,
                generation=args.generation,
                service_uid=service_uid,
                run=run,
                stop_requested=stopped.is_set,
                owner_running=systemd.mutation_guard_owner_running,
            )
        elif args.command == "fence":
            systemd.fence_mutation_guard_owners(args.request_id, args.generation)
        else:
            result = reconcile_orphaned_guard(
                config=config,
                service_uid=service_uid,
                run=run,
                show_guard=systemd.show_mutation_guard,
                fence_owners=systemd.fence_mutation_guard_owners,
                owner_running=systemd.mutation_guard_owner_running,
            )
            sys.stdout.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
        return 0
    except Exception:
        sys.stderr.write("error: staging mutation guard failed safely\n")
        return 1
    finally:
        signal.signal(signal.SIGTERM, previous_term)
        signal.signal(signal.SIGINT, previous_int)


if __name__ == "__main__":  # pragma: no cover - service entrypoint
    raise SystemExit(main())


__all__ = [
    "MUTATION_GUARD_KUBERNETES_COMMAND_TIMEOUT_SECONDS",
    "MUTATION_GUARD_NORMAL_RELEASE_BOUND_SECONDS",
    "MUTATION_GUARD_RUNTIME_SECONDS",
    "MUTATION_GUARD_SYSTEMD_COMMAND_TIMEOUT_SECONDS",
    "MutationGuardError",
    "MutationGuardEvidence",
    "MutationGuardManager",
    "guard_evidence_path",
    "hold_request_guard",
    "main",
    "read_mutation_guard_evidence",
    "reconcile_orphaned_guard",
    "validate_mutation_guard_generation",
]
