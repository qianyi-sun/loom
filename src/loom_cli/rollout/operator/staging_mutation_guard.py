"""Request-bound coordination between protected rollout and staging lifecycle GC."""

from __future__ import annotations

import argparse
import hashlib
import json
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
from loom_cli.rollout.credential_authority import (
    converge_new_private_file,
    read_trusted_file,
)
from loom_cli.rollout.readonly_database_authority import DatabaseQuery

from .config import OperatorConfig, candidate_sha_from_runner_repo
from .envelope import fixed_operator_config_path
from .model import validate_safe_identifier
from .policy import sanitized_child_environment
from .readonly_database_client import open_readonly_database_query

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
_UID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_RESOURCE_VERSION_RE = re.compile(r"^[1-9][0-9]*$")
_JOB_NAME_RE = re.compile(r"^loom-staging-data-lifecycle-[1-9][0-9]*$")
_MAX_KUBERNETES_OUTPUT = 1024 * 1024
_MAX_EVIDENCE_BYTES = 64 * 1024
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_DEFAULT_ACTIVE_WAITS = 1320
_DEFAULT_LOCK_ATTEMPTS = 120

_TRY_LOCK_SQL = f"{STAGING_MUTATION_TRY_LOCK_SQL} AS acquired"
_READ_EPOCH_SQL = (
    "SELECT epoch AS mutation_epoch FROM staging_mutation_epochs "
    "WHERE environment = 'staging' AND namespace = 'loom-staging'"
)
_UNLOCK_SQL = f"{STAGING_MUTATION_UNLOCK_SQL} AS released"


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
    mutation_epoch: int
    guard_pid: int
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
            or type(self.mutation_epoch) is not int
            or self.mutation_epoch < 0
            or type(self.guard_pid) is not int
            or self.guard_pid < 1
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
        mutation_epoch: int,
        guard_pid: int,
        cronjob_uid: str,
        suspended_resource_version: str,
        state: GuardState,
    ) -> MutationGuardEvidence:
        value: dict[str, object] = {
            "candidate_sha": candidate_sha,
            "candidate_tree": candidate_tree,
            "cronjob_uid": cronjob_uid,
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
            mutation_epoch=mutation_epoch,
            guard_pid=guard_pid,
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
            "evidence_digest",
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


def _validate_active_job(
    config: OperatorConfig,
    run: KubernetesRunner,
    *,
    cronjob_uid: str,
    reference: _ActiveJob,
) -> None:
    value = _run_json(
        run,
        [
            *_kubectl_prefix(config),
            "get",
            f"job/{reference.name}",
            "--output=json",
            "--request-timeout=60s",
        ],
        "lifecycle active Job",
    )
    metadata = _mapping(value.get("metadata"), "lifecycle active Job metadata")
    owners = metadata.get("ownerReferences")
    if not isinstance(owners, list) or len(owners) != 1:
        raise MutationGuardError("lifecycle active Job owner authority drifted")
    owner = _mapping(owners[0], "lifecycle active Job owner")
    if (
        value.get("apiVersion") != "batch/v1"
        or value.get("kind") != "Job"
        or metadata.get("name") != reference.name
        or metadata.get("namespace") != _NAMESPACE
        or metadata.get("uid") != reference.uid
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
        raise MutationGuardError("lifecycle active Job owner authority drifted")


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
    max_waits: int,
) -> _CronJob:
    current = held
    for attempt in range(max_waits + 1):
        if stop_requested():
            raise MutationGuardError("mutation guard stop requested before readiness")
        _require_held(
            current,
            uid=held.uid,
            request_id=request_id,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
        )
        if not current.active_jobs:
            return current
        for reference in current.active_jobs:
            _validate_active_job(
                config,
                run,
                cronjob_uid=held.uid,
                reference=reference,
            )
        if attempt == max_waits:
            break
        sleep(1.0)
        current = _load_cronjob(config, run)
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
                timeout=15,
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
        if (
            evidence.request_id != request_id
            or evidence.candidate_sha != candidate_sha
            or evidence.candidate_tree != candidate_tree
            or evidence.state != state
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


def reconcile_orphaned_guard(
    *,
    config: OperatorConfig,
    service_uid: int,
    run: KubernetesRunner,
    show_guard: Callable[[str], _MutationGuardUnitStatus | None],
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
    evidence_path = guard_evidence_path(config, request_id)
    if status is not None:
        if not status.is_running or status.main_pid < 1:
            raise MutationGuardError("orphaned mutation guard unit is not absent")
        try:
            evidence = read_mutation_guard_evidence(
                evidence_path,
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
    try:
        os.lstat(evidence_path)
    except FileNotFoundError:
        pass
    except OSError as exc:
        raise MutationGuardError("orphaned mutation guard evidence is unavailable") from exc
    else:
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
    service_uid: int,
    run: KubernetesRunner,
    query_context: QueryContext = open_readonly_database_query,
    resolve_candidate: CandidateResolver = _resolve_candidate,
    stop_requested: Callable[[], bool],
    sleep: Callable[[float], None] = time.sleep,
    max_active_waits: int = _DEFAULT_ACTIVE_WAITS,
    max_lock_attempts: int = _DEFAULT_LOCK_ATTEMPTS,
) -> MutationGuardEvidence:
    """Freeze the legacy CronJob and hold the shared lock until stopped."""

    _validate_config(config)
    if (
        service_uid < 1
        or os.geteuid() != service_uid
        or not 0 <= max_active_waits <= _DEFAULT_ACTIVE_WAITS
        or not 1 <= max_lock_attempts <= _DEFAULT_LOCK_ATTEMPTS
    ):
        raise MutationGuardError("mutation guard process authority is invalid")
    try:
        validate_safe_identifier(request_id, "request_id")
    except ValueError as exc:
        raise MutationGuardError("mutation guard request identity is invalid") from exc
    candidate_sha, candidate_tree = resolve_candidate(config)
    if _SHA_RE.fullmatch(candidate_sha) is None or _SHA_RE.fullmatch(candidate_tree) is None:
        raise MutationGuardError("mutation guard candidate identity is invalid")
    initial = _load_cronjob(config, run)
    expected_annotations = _guard_annotations(request_id, candidate_sha, candidate_tree)
    observed_annotations = _guard_annotation_state(initial)
    if observed_annotations:
        if observed_annotations != expected_annotations:
            raise MutationGuardError("lifecycle CronJob guard annotation authority drifted")
    elif initial.suspended:
        raise MutationGuardError("lifecycle CronJob is already suspended")
    restored = False
    acquired = False
    ready: MutationGuardEvidence | None = None
    try:
        held = (
            initial
            if initial.suspended
            else _patch_cronjob(
                config,
                run,
                resource_version=initial.resource_version,
                suspend=True,
                annotations=expected_annotations,
            )
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
            max_waits=max_active_waits,
        )
        with query_context(service_uid=service_uid) as query:
            try:
                for attempt in range(max_lock_attempts):
                    if stop_requested():
                        raise MutationGuardError("mutation guard stop requested before readiness")
                    acquired = _parse_scalar(
                        query(_TRY_LOCK_SQL), key="acquired", expected_type=bool
                    )
                    if acquired:
                        break
                    if attempt + 1 < max_lock_attempts:
                        sleep(1.0)
                if not acquired:
                    raise MutationGuardError("staging mutation advisory lock is unavailable")
                mutation_epoch = _parse_scalar(
                    query(_READ_EPOCH_SQL), key="mutation_epoch", expected_type=int
                )
                if mutation_epoch < 0:
                    raise MutationGuardError("mutation guard epoch evidence is invalid")
                ready = MutationGuardEvidence.build(
                    request_id=request_id,
                    candidate_sha=candidate_sha,
                    candidate_tree=candidate_tree,
                    mutation_epoch=mutation_epoch,
                    guard_pid=os.getpid(),
                    cronjob_uid=initial.uid,
                    suspended_resource_version=held.resource_version,
                    state="ready",
                )
                _publish_evidence(config, ready, service_uid=service_uid)
                while not stop_requested():
                    sleep(1.0)
            finally:
                release_error: BaseException | None = None
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
        if not restored:
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
        mutation_epoch=ready.mutation_epoch,
        guard_pid=ready.guard_pid,
        cronjob_uid=ready.cronjob_uid,
        suspended_resource_version=ready.suspended_resource_version,
        state="released",
    )
    _publish_evidence(config, released_evidence, service_uid=service_uid)
    return released_evidence


def _run(argv: list[str], *, environment: Mapping[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
        env=dict(environment),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="command", required=True)
    hold = subparsers.add_parser("hold", allow_abbrev=False)
    hold.add_argument("--request-id", required=True)
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
            return _run(command, environment=environment)

        if args.command == "hold":
            hold_request_guard(
                config=config,
                request_id=args.request_id,
                service_uid=service_uid,
                run=run,
                stop_requested=stopped.is_set,
            )
        else:
            from .systemd import SystemdUserManager

            systemd = SystemdUserManager(
                config,
                service_uid=service_uid,
                run=run,
            )
            result = reconcile_orphaned_guard(
                config=config,
                service_uid=service_uid,
                run=run,
                show_guard=systemd.show_mutation_guard,
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
    "MutationGuardError",
    "MutationGuardEvidence",
    "MutationGuardManager",
    "guard_evidence_path",
    "hold_request_guard",
    "main",
    "read_mutation_guard_evidence",
    "reconcile_orphaned_guard",
]
