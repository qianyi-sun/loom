"""Atomic oldlab-first placement leases for GitHub Actions jobs.

The broker is deliberately independent from workflow-controlled code. A trusted
router submits immutable job identities and receives one frozen placement. The
SQLite transaction is the capacity authority across concurrent workflow runs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import sys
import tomllib
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import cast

SCHEMA_VERSION = "2"
EXPECTED_REPOSITORY = "qianyi-sun/loom"
WORK_CLASSES = ("normal", "image", "smoke")
EXPECTED_CAPACITIES = {"normal": 5, "image": 4, "smoke": 2}
CLASS_LABELS = {
    "normal": "loom-ci-normal",
    "image": "loom-ci-image",
    "smoke": "loom-ci-smoke",
}
HOSTED_RUNS_ON = {
    "normal": ("ubuntu-latest",),
    "image": ("ubuntu-24.04",),
    "smoke": ("ubuntu-latest",),
}
WORKFLOW_CLASS_CONTRACTS = {
    "CI": (
        302898379,
        "normal",
        (
            "lint-and-static",
            "tests-root-1-of-2",
            "tests-root-2-of-2",
            "tests-packages",
            "runtime-payload",
            "go-checks",
            "web-checks",
            "integration-1-of-2",
            "integration-2-of-2",
            "integration-docker",
        ),
        300,
    ),
    "images": (
        302898384,
        "image",
        (
            "agent-sandbox",
            "control-plane",
            "egress-xds",
            "family-orchestrator",
            "pipeline-orchestrator",
            "llm-gateway",
            "llm-gateway-sandbox",
            "service",
            "web",
            "staging-admin-browser-smoke",
            "rehearsal-postgres",
            "worker",
            "behavior-stage1-sim",
        ),
        900,
    ),
    "cluster-smoke": (302898381, "smoke", ("cluster-contract",), 300),
    "staging-smoke": (302898388, "smoke", ("system-smoke",), 300),
}
RELEASE_REASONS = {
    "completed",
    "cancelled",
    "skipped",
    "superseded",
    "expired",
}
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_JOB_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+@()-]{0,199}$")
_MAX_RUN_ID = 2**63 - 1
_MIN_TTL_SECONDS = 60
_MAX_TTL_SECONDS = 24 * 60 * 60


class LeaseBrokerError(ValueError):
    """A bounded, secret-free capacity broker failure."""


class PlacementTarget(StrEnum):
    OLDLAB = "oldlab"
    GITHUB_HOSTED = "github_hosted"


class AssignmentState(StrEnum):
    ASSIGNED = "assigned"
    RELEASED = "released"


class RouteDecisionState(StrEnum):
    PENDING = "pending"
    PUBLISHED = "published"
    ABANDONED = "abandoned"


@dataclass(frozen=True, slots=True)
class LeaseBrokerConfig:
    repository: str
    oldlab_labels: tuple[str, ...]
    capacities: Mapping[str, int]

    @classmethod
    def from_profile(cls, path: Path) -> LeaseBrokerConfig:
        try:
            value = tomllib.loads(path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as exc:
            raise LeaseBrokerError("runner profile is unreadable or invalid") from exc
        if value.get("schema_version") != 2:
            raise LeaseBrokerError("runner profile schema_version must be 2")
        repository = _exact_text(value.get("repository"), "repository")
        labels_value = value.get("labels")
        if not isinstance(labels_value, list) or not labels_value:
            raise LeaseBrokerError("runner profile labels must be a non-empty array")
        labels = tuple(_exact_text(item, "labels[]") for item in labels_value)
        if len(labels) != len(set(labels)):
            raise LeaseBrokerError("runner profile labels must be unique")
        work_classes = value.get("work_classes")
        if not isinstance(work_classes, list):
            raise LeaseBrokerError("runner profile work_classes must be an array")
        capacities: dict[str, int] = {}
        for item in work_classes:
            if not isinstance(item, dict):
                raise LeaseBrokerError("runner profile work class must be an object")
            name = _exact_text(item.get("name"), "work_classes[].name")
            if name not in WORK_CLASSES or name in capacities:
                raise LeaseBrokerError("runner profile work classes are invalid")
            label = _exact_text(item.get("label"), "work_classes[].label")
            if label != CLASS_LABELS[name]:
                raise LeaseBrokerError(f"runner profile label is invalid for {name}")
            capacities[name] = _bounded_int(
                item.get("slots"),
                f"work_classes[{name}].slots",
                minimum=1,
                maximum=11,
            )
        config = cls(repository=repository, oldlab_labels=labels, capacities=capacities)
        config.validate()
        return config

    def validate(self) -> None:
        if self.repository != EXPECTED_REPOSITORY:
            raise LeaseBrokerError(f"repository must be {EXPECTED_REPOSITORY}")
        if dict(self.capacities) != EXPECTED_CAPACITIES:
            raise LeaseBrokerError("class capacities must remain exactly 5/4/2")
        for work_class, capacity in self.capacities.items():
            _bounded_int(capacity, f"capacity.{work_class}", minimum=1, maximum=11)
        required_labels = {"self-hosted", "linux", "x64", "loom-ci", "oldlab-5"}
        if not required_labels.issubset(self.oldlab_labels):
            raise LeaseBrokerError("oldlab labels do not preserve the isolation boundary")
        if len(self.oldlab_labels) != len(set(self.oldlab_labels)):
            raise LeaseBrokerError("oldlab labels must be unique")


@dataclass(frozen=True, slots=True)
class AssignmentRequest:
    repository: str
    workflow_run_id: int
    run_attempt: int
    job_key: str
    head_sha: str
    work_class: str
    lease_ttl_seconds: int

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> AssignmentRequest:
        request = cls(
            repository=_exact_text(value.get("repository"), "repository"),
            workflow_run_id=_bounded_int(
                value.get("workflow_run_id"),
                "workflow_run_id",
                minimum=1,
                maximum=_MAX_RUN_ID,
            ),
            run_attempt=_bounded_int(
                value.get("run_attempt"), "run_attempt", minimum=1, maximum=1_000_000
            ),
            job_key=_exact_text(value.get("job_key"), "job_key"),
            head_sha=_exact_text(value.get("head_sha"), "head_sha"),
            work_class=_exact_text(value.get("work_class"), "work_class"),
            lease_ttl_seconds=_bounded_int(
                value.get("lease_ttl_seconds"),
                "lease_ttl_seconds",
                minimum=_MIN_TTL_SECONDS,
                maximum=_MAX_TTL_SECONDS,
            ),
        )
        request.validate()
        return request

    def validate(self) -> None:
        if self.repository != EXPECTED_REPOSITORY:
            raise LeaseBrokerError(f"repository must be {EXPECTED_REPOSITORY}")
        if _JOB_KEY_RE.fullmatch(self.job_key) is None:
            raise LeaseBrokerError("job_key contains unsupported characters")
        if _SHA_RE.fullmatch(self.head_sha) is None:
            raise LeaseBrokerError("head_sha must be a full lowercase commit SHA")
        if self.work_class not in WORK_CLASSES:
            raise LeaseBrokerError("work_class must be normal, image, or smoke")


@dataclass(frozen=True, slots=True)
class RouteRequest:
    repository: str
    workflow_name: str
    workflow_id: int
    workflow_run_id: int
    run_attempt: int
    head_sha: str
    job_keys: tuple[str, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> RouteRequest:
        expected_keys = {
            "schema_version",
            "repository",
            "workflow_name",
            "workflow_id",
            "workflow_run_id",
            "run_attempt",
            "head_sha",
            "job_keys",
        }
        if set(value) != expected_keys:
            raise LeaseBrokerError("route request fields do not match schema 1")
        if value.get("schema_version") != 1:
            raise LeaseBrokerError("route request schema_version must be 1")
        job_keys_value = value.get("job_keys")
        if not isinstance(job_keys_value, list):
            raise LeaseBrokerError("route request job_keys must be an array")
        request = cls(
            repository=_exact_text(value.get("repository"), "repository"),
            workflow_name=_exact_text(value.get("workflow_name"), "workflow_name"),
            workflow_id=_bounded_int(
                value.get("workflow_id"), "workflow_id", minimum=1, maximum=_MAX_RUN_ID
            ),
            workflow_run_id=_bounded_int(
                value.get("workflow_run_id"),
                "workflow_run_id",
                minimum=1,
                maximum=_MAX_RUN_ID,
            ),
            run_attempt=_bounded_int(
                value.get("run_attempt"), "run_attempt", minimum=1, maximum=1_000_000
            ),
            head_sha=_exact_text(value.get("head_sha"), "head_sha"),
            job_keys=tuple(_exact_text(item, "job_keys[]") for item in job_keys_value),
        )
        request.validate()
        return request

    @property
    def work_class(self) -> str:
        return WORKFLOW_CLASS_CONTRACTS[self.workflow_name][1]

    @property
    def lease_ttl_seconds(self) -> int:
        return WORKFLOW_CLASS_CONTRACTS[self.workflow_name][3]

    def validate(self) -> None:
        if self.repository != EXPECTED_REPOSITORY:
            raise LeaseBrokerError(f"repository must be {EXPECTED_REPOSITORY}")
        contract = WORKFLOW_CLASS_CONTRACTS.get(self.workflow_name)
        if contract is None:
            raise LeaseBrokerError("route request workflow is not eligible")
        expected_id, _, allowed_job_keys, _ = contract
        if self.workflow_id != expected_id:
            raise LeaseBrokerError("route request workflow id does not match its name")
        if _SHA_RE.fullmatch(self.head_sha) is None:
            raise LeaseBrokerError("head_sha must be a full lowercase commit SHA")
        if not self.job_keys:
            raise LeaseBrokerError("route request must contain at least one job")
        if len(self.job_keys) != len(set(self.job_keys)):
            raise LeaseBrokerError("route request job_keys must be unique")
        if any(_JOB_KEY_RE.fullmatch(job_key) is None for job_key in self.job_keys):
            raise LeaseBrokerError("route request job_key contains unsupported characters")
        if not set(self.job_keys) <= set(allowed_job_keys):
            raise LeaseBrokerError(
                f"route request contains a job outside the {self.workflow_name} contract"
            )

    def assignment_requests(self) -> tuple[AssignmentRequest, ...]:
        return tuple(
            AssignmentRequest(
                repository=self.repository,
                workflow_run_id=self.workflow_run_id,
                run_attempt=self.run_attempt,
                job_key=job_key,
                head_sha=self.head_sha,
                work_class=self.work_class,
                lease_ttl_seconds=self.lease_ttl_seconds,
            )
            for job_key in self.job_keys
        )

    def public_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "repository": self.repository,
            "workflow_name": self.workflow_name,
            "workflow_id": self.workflow_id,
            "workflow_run_id": self.workflow_run_id,
            "run_attempt": self.run_attempt,
            "head_sha": self.head_sha,
            "job_keys": list(self.job_keys),
        }


@dataclass(frozen=True, slots=True)
class PlacementAssignment:
    assignment_id: int
    repository: str
    workflow_run_id: int
    run_attempt: int
    job_key: str
    head_sha: str
    work_class: str
    target: PlacementTarget
    slot: int | None
    lease_epoch: int
    state: AssignmentState
    runs_on: tuple[str, ...]
    created_at: str
    lease_expires_at: str | None
    released_at: str | None
    release_reason: str | None

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> PlacementAssignment:
        expected = {
            "assignment_id",
            "repository",
            "workflow_run_id",
            "run_attempt",
            "job_key",
            "head_sha",
            "work_class",
            "target",
            "slot",
            "lease_epoch",
            "state",
            "runs_on",
            "created_at",
            "lease_expires_at",
            "released_at",
            "release_reason",
        }
        if set(value) != expected:
            raise LeaseBrokerError("stored route assignment fields do not match")
        runs_on_value = value.get("runs_on")
        if not isinstance(runs_on_value, list) or any(
            not isinstance(item, str) or not item for item in runs_on_value
        ):
            raise LeaseBrokerError("stored route assignment labels are invalid")
        try:
            target = PlacementTarget(_exact_text(value.get("target"), "target"))
            state = AssignmentState(_exact_text(value.get("state"), "state"))
        except ValueError as exc:
            raise LeaseBrokerError("stored route assignment enum is invalid") from exc
        slot_value = value.get("slot")
        slot = (
            None
            if slot_value is None
            else _bounded_int(slot_value, "slot", minimum=0, maximum=10)
        )
        lease_expires_at = value.get("lease_expires_at")
        released_at = value.get("released_at")
        release_reason = value.get("release_reason")
        if lease_expires_at is not None and not isinstance(lease_expires_at, str):
            raise LeaseBrokerError("stored route assignment expiry is invalid")
        if released_at is not None and not isinstance(released_at, str):
            raise LeaseBrokerError("stored route assignment release time is invalid")
        if release_reason is not None and not isinstance(release_reason, str):
            raise LeaseBrokerError("stored route assignment release reason is invalid")
        assignment = cls(
            assignment_id=_bounded_int(
                value.get("assignment_id"), "assignment_id", minimum=1, maximum=_MAX_RUN_ID
            ),
            repository=_exact_text(value.get("repository"), "repository"),
            workflow_run_id=_bounded_int(
                value.get("workflow_run_id"),
                "workflow_run_id",
                minimum=1,
                maximum=_MAX_RUN_ID,
            ),
            run_attempt=_bounded_int(
                value.get("run_attempt"), "run_attempt", minimum=1, maximum=1_000_000
            ),
            job_key=_exact_text(value.get("job_key"), "job_key"),
            head_sha=_exact_text(value.get("head_sha"), "head_sha"),
            work_class=_exact_text(value.get("work_class"), "work_class"),
            target=target,
            slot=slot,
            lease_epoch=_bounded_int(
                value.get("lease_epoch"), "lease_epoch", minimum=1, maximum=_MAX_RUN_ID
            ),
            state=state,
            runs_on=tuple(runs_on_value),
            created_at=_exact_text(value.get("created_at"), "created_at"),
            lease_expires_at=lease_expires_at,
            released_at=released_at,
            release_reason=release_reason,
        )
        if assignment.repository != EXPECTED_REPOSITORY:
            raise LeaseBrokerError("stored route assignment repository is invalid")
        if _JOB_KEY_RE.fullmatch(assignment.job_key) is None:
            raise LeaseBrokerError("stored route assignment job key is invalid")
        if _SHA_RE.fullmatch(assignment.head_sha) is None:
            raise LeaseBrokerError("stored route assignment head is invalid")
        if assignment.work_class not in WORK_CLASSES:
            raise LeaseBrokerError("stored route assignment work class is invalid")
        if assignment.state is not AssignmentState.ASSIGNED:
            raise LeaseBrokerError("stored route assignment must be frozen while assigned")
        if assignment.target is PlacementTarget.OLDLAB:
            if assignment.slot is None or assignment.lease_expires_at is None:
                raise LeaseBrokerError("stored oldlab route assignment is incomplete")
            if assignment.slot >= EXPECTED_CAPACITIES[assignment.work_class]:
                raise LeaseBrokerError("stored oldlab route assignment slot is invalid")
            if not {
                "self-hosted",
                "linux",
                "x64",
                "loom-ci",
                "oldlab-5",
                CLASS_LABELS[assignment.work_class],
            }.issubset(assignment.runs_on):
                raise LeaseBrokerError("stored oldlab route labels are invalid")
        elif assignment.slot is not None or assignment.lease_expires_at is not None:
            raise LeaseBrokerError("stored hosted route assignment is inconsistent")
        elif assignment.runs_on != HOSTED_RUNS_ON[assignment.work_class]:
            raise LeaseBrokerError("stored hosted route labels are invalid")
        return assignment

    def public_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["target"] = self.target.value
        value["state"] = self.state.value
        value["runs_on"] = list(self.runs_on)
        return value


@dataclass(frozen=True, slots=True)
class RouteAssignmentDocument:
    schema_version: int
    repository: str
    workflow_name: str
    workflow_id: int
    workflow_run_id: int
    run_attempt: int
    head_sha: str
    request_sha256: str
    assignments: tuple[PlacementAssignment, ...]
    oldlab_eligible: bool

    @classmethod
    def create(
        cls,
        request: RouteRequest,
        assignments: Sequence[PlacementAssignment],
        *,
        oldlab_eligible: bool,
    ) -> RouteAssignmentDocument:
        canonical_request = _canonical_json(request.public_dict()).encode()
        document = cls(
            schema_version=1,
            repository=request.repository,
            workflow_name=request.workflow_name,
            workflow_id=request.workflow_id,
            workflow_run_id=request.workflow_run_id,
            run_attempt=request.run_attempt,
            head_sha=request.head_sha,
            request_sha256=hashlib.sha256(canonical_request).hexdigest(),
            assignments=tuple(assignments),
            oldlab_eligible=oldlab_eligible,
        )
        document.validate()
        return document

    def validate(self) -> None:
        if _SHA256_RE.fullmatch(self.request_sha256) is None:
            raise LeaseBrokerError("stored route request digest is invalid")
        assignment_ids = [assignment.assignment_id for assignment in self.assignments]
        job_keys = [assignment.job_key for assignment in self.assignments]
        if (
            len(assignment_ids) != len(set(assignment_ids))
            or len(job_keys) != len(set(job_keys))
        ):
            raise LeaseBrokerError("stored route response assignments are not unique")
        for assignment in self.assignments:
            if (
                assignment.repository != self.repository
                or assignment.workflow_run_id != self.workflow_run_id
                or assignment.run_attempt != self.run_attempt
                or assignment.head_sha != self.head_sha
            ):
                raise LeaseBrokerError("stored route assignment identity does not match response")
        if not self.oldlab_eligible and any(
            assignment.target is PlacementTarget.OLDLAB
            for assignment in self.assignments
        ):
            raise LeaseBrokerError("ineligible stored route response selects oldlab")

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> RouteAssignmentDocument:
        expected = {
            "schema_version",
            "repository",
            "workflow_name",
            "workflow_id",
            "workflow_run_id",
            "run_attempt",
            "head_sha",
            "request_sha256",
            "assignments",
            "oldlab_eligible",
        }
        if set(value) != expected or value.get("schema_version") != 1:
            raise LeaseBrokerError("stored route response fields do not match schema 1")
        assignments_value = value.get("assignments")
        if not isinstance(assignments_value, list) or not assignments_value:
            raise LeaseBrokerError("stored route response assignments are invalid")
        assignments = tuple(
            PlacementAssignment.from_mapping(item)
            for item in assignments_value
            if isinstance(item, dict)
        )
        if len(assignments) != len(assignments_value):
            raise LeaseBrokerError("stored route response assignment is invalid")
        oldlab_eligible = value.get("oldlab_eligible")
        if not isinstance(oldlab_eligible, bool):
            raise LeaseBrokerError("stored route response eligibility is invalid")
        document = cls(
            schema_version=1,
            repository=_exact_text(value.get("repository"), "repository"),
            workflow_name=_exact_text(value.get("workflow_name"), "workflow_name"),
            workflow_id=_bounded_int(
                value.get("workflow_id"), "workflow_id", minimum=1, maximum=_MAX_RUN_ID
            ),
            workflow_run_id=_bounded_int(
                value.get("workflow_run_id"),
                "workflow_run_id",
                minimum=1,
                maximum=_MAX_RUN_ID,
            ),
            run_attempt=_bounded_int(
                value.get("run_attempt"), "run_attempt", minimum=1, maximum=1_000_000
            ),
            head_sha=_exact_text(value.get("head_sha"), "head_sha"),
            request_sha256=_exact_text(value.get("request_sha256"), "request_sha256"),
            assignments=assignments,
            oldlab_eligible=oldlab_eligible,
        )
        document.validate()
        return document

    def public_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["assignments"] = [item.public_dict() for item in self.assignments]
        return value


@dataclass(frozen=True, slots=True)
class RouteDecision:
    decision_id: int
    repository: str
    workflow_name: str
    workflow_id: int
    workflow_run_id: int
    run_attempt: int
    head_sha: str
    request_sha256: str
    request_json: str
    response_json: str
    oldlab_eligible: bool
    state: RouteDecisionState
    created_at: str
    dispatch_attempted_at: str | None
    dispatch_attempts: int
    published_at: str | None
    abandoned_at: str | None

    def response_dict(self) -> dict[str, object]:
        try:
            value = json.loads(self.response_json)
        except json.JSONDecodeError as exc:
            raise LeaseBrokerError("stored route response is invalid JSON") from exc
        if not isinstance(value, dict) or _canonical_json(value) != self.response_json:
            raise LeaseBrokerError("stored route response is not canonical")
        return value

    def document(self) -> RouteAssignmentDocument:
        document = RouteAssignmentDocument.from_mapping(self.response_dict())
        if (
            document.repository != self.repository
            or document.workflow_name != self.workflow_name
            or document.workflow_id != self.workflow_id
            or document.workflow_run_id != self.workflow_run_id
            or document.run_attempt != self.run_attempt
            or document.head_sha != self.head_sha
            or document.request_sha256 != self.request_sha256
            or document.oldlab_eligible is not self.oldlab_eligible
        ):
            raise LeaseBrokerError("stored route decision identity does not match response")
        return document


def _canonical_json(value: Mapping[str, object]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise LeaseBrokerError("clock must return a timezone-aware datetime")
    return value.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LeaseBrokerError("stored broker timestamp is invalid") from exc
    return _utc(parsed)


def _exact_text(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise LeaseBrokerError(f"{field} must be exact non-empty text")
    return value


def _bounded_int(value: object, field: str, *, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise LeaseBrokerError(f"{field} must be an integer in {minimum}..{maximum}")
    return value


class CiRunnerLeaseBroker:
    """Durable per-job assignment authority with transactional class limits."""

    def __init__(self, state_db: Path, config: LeaseBrokerConfig) -> None:
        self.state_db = state_db
        self.config = config
        self.config.validate()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        if self.state_db.is_symlink():
            raise LeaseBrokerError("state database must not be a symlink")
        self.state_db.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        connection = sqlite3.connect(self.state_db, timeout=30.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA busy_timeout = 30000")
        if self.state_db.exists():
            self.state_db.chmod(0o600)
        return connection

    def _initialize(self) -> None:
        connection = self._connect()
        try:
            connection.executescript(
                """
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS class_capacity (
                    work_class TEXT PRIMARY KEY,
                    capacity INTEGER NOT NULL CHECK (capacity > 0)
                );
                CREATE TABLE IF NOT EXISTS assignments (
                    assignment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    repository TEXT NOT NULL,
                    workflow_run_id INTEGER NOT NULL,
                    run_attempt INTEGER NOT NULL,
                    job_key TEXT NOT NULL,
                    head_sha TEXT NOT NULL,
                    work_class TEXT NOT NULL,
                    target TEXT NOT NULL CHECK (target IN ('oldlab', 'github_hosted')),
                    slot INTEGER,
                    lease_epoch INTEGER NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('assigned', 'released')),
                    created_at TEXT NOT NULL,
                    lease_expires_at TEXT,
                    released_at TEXT,
                    release_reason TEXT,
                    UNIQUE (repository, workflow_run_id, run_attempt, job_key),
                    CHECK (
                        (target = 'oldlab' AND slot IS NOT NULL AND lease_expires_at IS NOT NULL)
                        OR
                        (target = 'github_hosted' AND slot IS NULL AND lease_expires_at IS NULL)
                    )
                );
                CREATE UNIQUE INDEX IF NOT EXISTS active_oldlab_slot
                    ON assignments(work_class, slot)
                    WHERE target = 'oldlab' AND state = 'assigned';
                CREATE INDEX IF NOT EXISTS assignment_state
                    ON assignments(state, work_class, target);
                CREATE TABLE IF NOT EXISTS route_decisions (
                    decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    repository TEXT NOT NULL,
                    workflow_name TEXT NOT NULL,
                    workflow_id INTEGER NOT NULL,
                    workflow_run_id INTEGER NOT NULL,
                    run_attempt INTEGER NOT NULL,
                    head_sha TEXT NOT NULL,
                    request_sha256 TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    response_json TEXT NOT NULL,
                    oldlab_eligible INTEGER NOT NULL CHECK (oldlab_eligible IN (0, 1)),
                    state TEXT NOT NULL CHECK (state IN ('pending', 'published', 'abandoned')),
                    created_at TEXT NOT NULL,
                    dispatch_attempted_at TEXT,
                    dispatch_attempts INTEGER NOT NULL DEFAULT 0 CHECK (dispatch_attempts >= 0),
                    published_at TEXT,
                    abandoned_at TEXT,
                    UNIQUE (repository, workflow_run_id, run_attempt),
                    UNIQUE (request_sha256),
                    CHECK (
                        (state = 'pending' AND published_at IS NULL AND abandoned_at IS NULL)
                        OR (state = 'published' AND published_at IS NOT NULL AND abandoned_at IS NULL)
                        OR (state = 'abandoned' AND abandoned_at IS NOT NULL)
                    )
                );
                CREATE INDEX IF NOT EXISTS route_decision_state
                    ON route_decisions(state, created_at);
                """
            )
            self._initialize_contract(connection)
            connection.commit()
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize_contract(self, connection: sqlite3.Connection) -> None:
        expected_metadata = {
            "schema_version": SCHEMA_VERSION,
            "repository": self.config.repository,
            "oldlab_labels": json.dumps(list(self.config.oldlab_labels), separators=(",", ":")),
            "next_lease_epoch": "1",
        }
        existing = {
            str(row["key"]): str(row["value"])
            for row in connection.execute("SELECT key, value FROM metadata")
        }
        if existing:
            if existing.get("schema_version") not in {"1", SCHEMA_VERSION}:
                raise LeaseBrokerError("stored broker schema_version does not match config")
            for key in ("repository", "oldlab_labels"):
                if existing.get(key) != expected_metadata[key]:
                    raise LeaseBrokerError(f"stored broker {key} does not match config")
            if "next_lease_epoch" not in existing:
                raise LeaseBrokerError("stored broker lease epoch is missing")
            if existing["schema_version"] == "1":
                connection.execute(
                    "UPDATE metadata SET value = ? WHERE key = 'schema_version'",
                    (SCHEMA_VERSION,),
                )
        else:
            connection.executemany(
                "INSERT INTO metadata(key, value) VALUES (?, ?)",
                expected_metadata.items(),
            )
        stored_capacities = {
            str(row["work_class"]): int(row["capacity"])
            for row in connection.execute("SELECT work_class, capacity FROM class_capacity")
        }
        expected_capacities = dict(self.config.capacities)
        if stored_capacities and stored_capacities != expected_capacities:
            raise LeaseBrokerError("stored class capacities do not match config")
        if not stored_capacities:
            connection.executemany(
                "INSERT INTO class_capacity(work_class, capacity) VALUES (?, ?)",
                sorted(expected_capacities.items()),
            )

    @staticmethod
    def _next_epoch(connection: sqlite3.Connection) -> int:
        row = connection.execute(
            "SELECT value FROM metadata WHERE key = 'next_lease_epoch'"
        ).fetchone()
        if row is None:
            raise LeaseBrokerError("stored broker lease epoch is missing")
        try:
            epoch = int(row["value"])
        except (TypeError, ValueError) as exc:
            raise LeaseBrokerError("stored broker lease epoch is invalid") from exc
        if epoch < 1:
            raise LeaseBrokerError("stored broker lease epoch is invalid")
        connection.execute(
            "UPDATE metadata SET value = ? WHERE key = 'next_lease_epoch'",
            (str(epoch + 1),),
        )
        return epoch

    def allocate(
        self, request: AssignmentRequest, *, now: datetime | None = None
    ) -> PlacementAssignment:
        return self.allocate_many((request,), now=now)[0]

    def allocate_many(
        self,
        requests: Sequence[AssignmentRequest],
        *,
        now: datetime | None = None,
        allow_oldlab: bool = True,
    ) -> tuple[PlacementAssignment, ...]:
        if not requests or len(requests) > 100:
            raise LeaseBrokerError("allocation batch must contain 1..100 requests")
        identities: set[tuple[str, int, int, str]] = set()
        for request in requests:
            request.validate()
            if request.repository != self.config.repository:
                raise LeaseBrokerError("request repository does not match broker config")
            identity = (
                request.repository,
                request.workflow_run_id,
                request.run_attempt,
                request.job_key,
            )
            if identity in identities:
                raise LeaseBrokerError("allocation batch identities must be unique")
            identities.add(identity)
        observed_at = _utc(now or datetime.now(UTC))
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            assignments = tuple(
                self._allocate_in_transaction(
                    connection,
                    request,
                    observed_at,
                    allow_oldlab=allow_oldlab,
                )
                for request in requests
            )
            connection.commit()
            return assignments
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def allocate_route(
        self,
        request: RouteRequest,
        *,
        now: datetime | None = None,
        allow_oldlab: bool = True,
    ) -> RouteAssignmentDocument:
        return self.decide_route(
            request,
            now=now,
            allow_oldlab=allow_oldlab,
        ).document()

    def decide_route(
        self,
        request: RouteRequest,
        *,
        now: datetime | None = None,
        allow_oldlab: bool = True,
    ) -> RouteDecision:
        """Atomically freeze one route response with its capacity assignments."""
        request.validate()
        if request.repository != self.config.repository:
            raise LeaseBrokerError("request repository does not match broker config")
        observed_at = _utc(now or datetime.now(UTC))
        request_json = _canonical_json(request.public_dict())
        request_sha256 = hashlib.sha256(request_json.encode()).hexdigest()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT * FROM route_decisions
                WHERE repository = ? AND workflow_run_id = ? AND run_attempt = ?
                """,
                (request.repository, request.workflow_run_id, request.run_attempt),
            ).fetchone()
            if existing is not None:
                decision = self._route_decision_from_row(existing)
                if (
                    decision.request_json != request_json
                    or decision.request_sha256 != request_sha256
                ):
                    raise LeaseBrokerError(
                        "route decision identity was replayed with different inputs"
                    )
                connection.commit()
                return decision

            assignments = tuple(
                self._allocate_in_transaction(
                    connection,
                    assignment_request,
                    observed_at,
                    allow_oldlab=allow_oldlab,
                )
                for assignment_request in request.assignment_requests()
            )
            document = RouteAssignmentDocument.create(
                request,
                assignments,
                oldlab_eligible=allow_oldlab,
            )
            if document.request_sha256 != request_sha256:
                raise LeaseBrokerError("route request digest is inconsistent")
            response_json = _canonical_json(document.public_dict())
            cursor = connection.execute(
                """
                INSERT INTO route_decisions(
                    repository, workflow_name, workflow_id, workflow_run_id,
                    run_attempt, head_sha, request_sha256, request_json,
                    response_json, oldlab_eligible, state, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                """,
                (
                    request.repository,
                    request.workflow_name,
                    request.workflow_id,
                    request.workflow_run_id,
                    request.run_attempt,
                    request.head_sha,
                    request_sha256,
                    request_json,
                    response_json,
                    int(allow_oldlab),
                    _timestamp(observed_at),
                ),
            )
            stored = connection.execute(
                "SELECT * FROM route_decisions WHERE decision_id = ?",
                (cursor.lastrowid,),
            ).fetchone()
            if stored is None:
                raise LeaseBrokerError("stored route decision could not be read back")
            decision = self._route_decision_from_row(stored)
            connection.commit()
            return decision
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def _allocate_in_transaction(
        self,
        connection: sqlite3.Connection,
        request: AssignmentRequest,
        observed_at: datetime,
        *,
        allow_oldlab: bool,
    ) -> PlacementAssignment:
        existing = connection.execute(
            """
            SELECT * FROM assignments
            WHERE repository = ? AND workflow_run_id = ?
              AND run_attempt = ? AND job_key = ?
            """,
            (
                request.repository,
                request.workflow_run_id,
                request.run_attempt,
                request.job_key,
            ),
        ).fetchone()
        if existing is not None:
            assignment = self._assignment_from_row(existing)
            self._validate_replay(assignment, request)
            return assignment

        used_slots = {
            int(row["slot"])
            for row in connection.execute(
                """
                SELECT slot FROM assignments
                WHERE work_class = ? AND target = 'oldlab' AND state = 'assigned'
                """,
                (request.work_class,),
            )
        }
        capacity = self.config.capacities[request.work_class]
        free_slot = (
            next((slot for slot in range(capacity) if slot not in used_slots), None)
            if allow_oldlab
            else None
        )
        target = PlacementTarget.OLDLAB if free_slot is not None else PlacementTarget.GITHUB_HOSTED
        epoch = self._next_epoch(connection)
        created_at = _timestamp(observed_at)
        expires_at = (
            _timestamp(observed_at + timedelta(seconds=request.lease_ttl_seconds))
            if target is PlacementTarget.OLDLAB
            else None
        )
        cursor = connection.execute(
            """
            INSERT INTO assignments(
                repository, workflow_run_id, run_attempt, job_key, head_sha,
                work_class, target, slot, lease_epoch, state, created_at,
                lease_expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'assigned', ?, ?)
            """,
            (
                request.repository,
                request.workflow_run_id,
                request.run_attempt,
                request.job_key,
                request.head_sha,
                request.work_class,
                target.value,
                free_slot,
                epoch,
                created_at,
                expires_at,
            ),
        )
        row = connection.execute(
            "SELECT * FROM assignments WHERE assignment_id = ?",
            (cursor.lastrowid,),
        ).fetchone()
        if row is None:
            raise LeaseBrokerError("stored assignment could not be read back")
        return self._assignment_from_row(row)

    def release(
        self,
        *,
        assignment_id: int,
        lease_epoch: int,
        reason: str,
        terminal_observed: bool,
        now: datetime | None = None,
    ) -> PlacementAssignment:
        _bounded_int(assignment_id, "assignment_id", minimum=1, maximum=_MAX_RUN_ID)
        _bounded_int(lease_epoch, "lease_epoch", minimum=1, maximum=_MAX_RUN_ID)
        if reason not in RELEASE_REASONS:
            raise LeaseBrokerError("release reason is invalid")
        if terminal_observed is not True:
            raise LeaseBrokerError("release requires an exact terminal observation")
        observed_at = _utc(now or datetime.now(UTC))
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM assignments WHERE assignment_id = ?", (assignment_id,)
            ).fetchone()
            if row is None:
                raise LeaseBrokerError("assignment does not exist")
            assignment = self._assignment_from_row(row)
            if assignment.lease_epoch != lease_epoch:
                raise LeaseBrokerError("stale lease epoch cannot release assignment")
            if assignment.state is AssignmentState.RELEASED:
                if assignment.release_reason != reason:
                    raise LeaseBrokerError("assignment was released for another reason")
                connection.commit()
                return assignment
            connection.execute(
                """
                UPDATE assignments
                SET state = 'released', released_at = ?, release_reason = ?
                WHERE assignment_id = ? AND lease_epoch = ? AND state = 'assigned'
                """,
                (_timestamp(observed_at), reason, assignment_id, lease_epoch),
            )
            updated = connection.execute(
                "SELECT * FROM assignments WHERE assignment_id = ?", (assignment_id,)
            ).fetchone()
            if updated is None:
                raise LeaseBrokerError("released assignment could not be read back")
            released = self._assignment_from_row(updated)
            connection.commit()
            return released
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def status(self, *, now: datetime | None = None) -> dict[str, object]:
        observed_at = _utc(now or datetime.now(UTC))
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT * FROM assignments WHERE state = 'assigned' ORDER BY assignment_id"
            ).fetchall()
        finally:
            connection.close()
        assignments = [self._assignment_from_row(row) for row in rows]
        classes: dict[str, dict[str, object]] = {}
        for work_class in WORK_CLASSES:
            class_assignments = [item for item in assignments if item.work_class == work_class]
            oldlab = [item for item in class_assignments if item.target is PlacementTarget.OLDLAB]
            hosted = [
                item for item in class_assignments if item.target is PlacementTarget.GITHUB_HOSTED
            ]
            overdue = [
                item
                for item in oldlab
                if item.lease_expires_at is not None
                and _parse_timestamp(item.lease_expires_at) <= observed_at
            ]
            capacity = self.config.capacities[work_class]
            classes[work_class] = {
                "capacity": capacity,
                "oldlab_assigned": len(oldlab),
                "hosted_assigned": len(hosted),
                "available": capacity - len(oldlab),
                "overdue_oldlab_assignments": len(overdue),
            }
        return {
            "schema_version": int(SCHEMA_VERSION),
            "repository": self.config.repository,
            "observed_at": _timestamp(observed_at),
            "classes": classes,
            "healthy": all(
                cast(int, item["oldlab_assigned"]) <= cast(int, item["capacity"])
                for item in classes.values()
            ),
        }

    def active_assignments(self) -> tuple[PlacementAssignment, ...]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT * FROM assignments WHERE state = 'assigned' ORDER BY assignment_id"
            ).fetchall()
        finally:
            connection.close()
        return tuple(self._assignment_from_row(row) for row in rows)

    def route_decisions(
        self, *, states: Sequence[RouteDecisionState] | None = None
    ) -> tuple[RouteDecision, ...]:
        selected = tuple(states or tuple(RouteDecisionState))
        if not selected or len(selected) != len(set(selected)):
            raise LeaseBrokerError("route decision state filter is invalid")
        placeholders = ",".join("?" for _ in selected)
        connection = self._connect()
        try:
            rows = connection.execute(
                f"SELECT * FROM route_decisions WHERE state IN ({placeholders}) "
                "ORDER BY decision_id",
                tuple(state.value for state in selected),
            ).fetchall()
        finally:
            connection.close()
        return tuple(self._route_decision_from_row(row) for row in rows)

    def record_route_dispatch(
        self, request_sha256: str, *, now: datetime | None = None
    ) -> RouteDecision:
        return self._update_route_decision(
            request_sha256,
            """
            UPDATE route_decisions
            SET dispatch_attempted_at = ?, dispatch_attempts = dispatch_attempts + 1
            WHERE request_sha256 = ? AND state = 'pending'
            """,
            (_timestamp(_utc(now or datetime.now(UTC))), request_sha256),
        )

    def mark_route_published(
        self, request_sha256: str, *, now: datetime | None = None
    ) -> RouteDecision:
        observed = _timestamp(_utc(now or datetime.now(UTC)))
        return self._update_route_decision(
            request_sha256,
            """
            UPDATE route_decisions
            SET state = CASE WHEN state = 'pending' THEN 'published' ELSE state END,
                published_at = COALESCE(published_at, ?)
            WHERE request_sha256 = ?
            """,
            (observed, request_sha256),
        )

    def abandon_route(
        self, request_sha256: str, *, now: datetime | None = None
    ) -> RouteDecision:
        observed = _timestamp(_utc(now or datetime.now(UTC)))
        return self._update_route_decision(
            request_sha256,
            """
            UPDATE route_decisions
            SET state = 'abandoned', abandoned_at = ?
            WHERE request_sha256 = ? AND state = 'pending'
            """,
            (observed, request_sha256),
        )

    def _update_route_decision(
        self,
        request_sha256: str,
        statement: str,
        parameters: tuple[object, ...],
    ) -> RouteDecision:
        if _SHA256_RE.fullmatch(request_sha256) is None:
            raise LeaseBrokerError("route request digest must be SHA-256")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM route_decisions WHERE request_sha256 = ?",
                (request_sha256,),
            ).fetchone()
            if row is None:
                raise LeaseBrokerError("route decision does not exist")
            connection.execute(statement, parameters)
            updated = connection.execute(
                "SELECT * FROM route_decisions WHERE request_sha256 = ?",
                (request_sha256,),
            ).fetchone()
            if updated is None:
                raise LeaseBrokerError("updated route decision could not be read back")
            decision = self._route_decision_from_row(updated)
            connection.commit()
            return decision
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def prune_route_decisions(
        self,
        *,
        before: datetime,
        limit: int = 100,
    ) -> int:
        cutoff = _timestamp(_utc(before))
        _bounded_int(limit, "limit", minimum=1, maximum=1_000)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """
                SELECT decision_id FROM route_decisions AS decision
                WHERE decision.state IN ('published', 'abandoned')
                  AND COALESCE(decision.published_at, decision.abandoned_at) < ?
                  AND NOT EXISTS (
                      SELECT 1 FROM assignments AS assignment
                      WHERE assignment.repository = decision.repository
                        AND assignment.workflow_run_id = decision.workflow_run_id
                        AND assignment.run_attempt = decision.run_attempt
                        AND assignment.state = 'assigned'
                  )
                ORDER BY decision.decision_id
                LIMIT ?
                """,
                (cutoff, limit),
            ).fetchall()
            ids = [int(row["decision_id"]) for row in rows]
            if ids:
                placeholders = ",".join("?" for _ in ids)
                connection.execute(
                    f"DELETE FROM route_decisions WHERE decision_id IN ({placeholders})",
                    ids,
                )
            connection.commit()
            return len(ids)
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def _route_decision_from_row(self, row: sqlite3.Row) -> RouteDecision:
        try:
            state = RouteDecisionState(str(row["state"]))
        except ValueError as exc:
            raise LeaseBrokerError("stored route decision state is invalid") from exc
        decision = RouteDecision(
            decision_id=int(row["decision_id"]),
            repository=str(row["repository"]),
            workflow_name=str(row["workflow_name"]),
            workflow_id=int(row["workflow_id"]),
            workflow_run_id=int(row["workflow_run_id"]),
            run_attempt=int(row["run_attempt"]),
            head_sha=str(row["head_sha"]),
            request_sha256=str(row["request_sha256"]),
            request_json=str(row["request_json"]),
            response_json=str(row["response_json"]),
            oldlab_eligible=bool(row["oldlab_eligible"]),
            state=state,
            created_at=str(row["created_at"]),
            dispatch_attempted_at=(
                str(row["dispatch_attempted_at"])
                if row["dispatch_attempted_at"] is not None
                else None
            ),
            dispatch_attempts=int(row["dispatch_attempts"]),
            published_at=(
                str(row["published_at"]) if row["published_at"] is not None else None
            ),
            abandoned_at=(
                str(row["abandoned_at"]) if row["abandoned_at"] is not None else None
            ),
        )
        try:
            request_value = json.loads(decision.request_json)
        except json.JSONDecodeError as exc:
            raise LeaseBrokerError("stored route request is invalid JSON") from exc
        if (
            not isinstance(request_value, dict)
            or _canonical_json(request_value) != decision.request_json
        ):
            raise LeaseBrokerError("stored route request is not canonical")
        request = RouteRequest.from_mapping(request_value)
        request_sha256 = hashlib.sha256(decision.request_json.encode()).hexdigest()
        if (
            request.repository != decision.repository
            or request.workflow_name != decision.workflow_name
            or request.workflow_id != decision.workflow_id
            or request.workflow_run_id != decision.workflow_run_id
            or request.run_attempt != decision.run_attempt
            or request.head_sha != decision.head_sha
            or request_sha256 != decision.request_sha256
        ):
            raise LeaseBrokerError("stored route decision identity does not match request")
        document = decision.document()
        if tuple(assignment.job_key for assignment in document.assignments) != request.job_keys:
            raise LeaseBrokerError("stored route assignments do not match request jobs")
        if any(assignment.work_class != request.work_class for assignment in document.assignments):
            raise LeaseBrokerError("stored route assignment class does not match request")
        expected_oldlab_labels = (*self.config.oldlab_labels, CLASS_LABELS[request.work_class])
        if any(
            assignment.target is PlacementTarget.OLDLAB
            and assignment.runs_on != expected_oldlab_labels
            for assignment in document.assignments
        ):
            raise LeaseBrokerError("stored oldlab route labels do not match broker config")
        return decision

    def _assignment_from_row(self, row: sqlite3.Row) -> PlacementAssignment:
        target = PlacementTarget(str(row["target"]))
        work_class = str(row["work_class"])
        runs_on = (
            (*self.config.oldlab_labels, CLASS_LABELS[work_class])
            if target is PlacementTarget.OLDLAB
            else HOSTED_RUNS_ON[work_class]
        )
        return PlacementAssignment(
            assignment_id=int(row["assignment_id"]),
            repository=str(row["repository"]),
            workflow_run_id=int(row["workflow_run_id"]),
            run_attempt=int(row["run_attempt"]),
            job_key=str(row["job_key"]),
            head_sha=str(row["head_sha"]),
            work_class=work_class,
            target=target,
            slot=int(row["slot"]) if row["slot"] is not None else None,
            lease_epoch=int(row["lease_epoch"]),
            state=AssignmentState(str(row["state"])),
            runs_on=runs_on,
            created_at=str(row["created_at"]),
            lease_expires_at=(
                str(row["lease_expires_at"]) if row["lease_expires_at"] is not None else None
            ),
            released_at=(str(row["released_at"]) if row["released_at"] is not None else None),
            release_reason=(
                str(row["release_reason"]) if row["release_reason"] is not None else None
            ),
        )

    @staticmethod
    def _validate_replay(assignment: PlacementAssignment, request: AssignmentRequest) -> None:
        if (
            assignment.head_sha != request.head_sha
            or assignment.work_class != request.work_class
            or assignment.repository != request.repository
        ):
            raise LeaseBrokerError("assignment identity was replayed with different inputs")


def _request_file(path: Path) -> AssignmentRequest:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LeaseBrokerError("request file is unreadable or invalid JSON") from exc
    if not isinstance(value, dict):
        raise LeaseBrokerError("request file must contain one JSON object")
    return AssignmentRequest.from_mapping(value)


def _route_request_file(path: Path) -> RouteRequest:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LeaseBrokerError("route request file is unreadable or invalid JSON") from exc
    if not isinstance(value, dict):
        raise LeaseBrokerError("route request file must contain one JSON object")
    return RouteRequest.from_mapping(value)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--state-db",
        type=Path,
        default=Path("/var/lib/loom-ci-runner-pool/leases.sqlite3"),
    )
    parser.add_argument(
        "--profile",
        type=Path,
        default=Path("/etc/loom-ci-runner-pool/profile.toml"),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    allocate = subparsers.add_parser("allocate")
    allocate.add_argument("--request-file", type=Path, required=True)
    allocate_route = subparsers.add_parser("allocate-route")
    allocate_route.add_argument("--request-file", type=Path, required=True)
    release = subparsers.add_parser("release")
    release.add_argument("--assignment-id", type=int, required=True)
    release.add_argument("--lease-epoch", type=int, required=True)
    release.add_argument("--reason", choices=sorted(RELEASE_REASONS), required=True)
    release.add_argument("--terminal-observed", action="store_true")
    subparsers.add_parser("status")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        config = LeaseBrokerConfig.from_profile(args.profile)
        broker = CiRunnerLeaseBroker(args.state_db, config)
        if args.command == "allocate":
            result: object = broker.allocate(_request_file(args.request_file)).public_dict()
        elif args.command == "allocate-route":
            result = broker.allocate_route(_route_request_file(args.request_file)).public_dict()
        elif args.command == "release":
            result = broker.release(
                assignment_id=args.assignment_id,
                lease_epoch=args.lease_epoch,
                reason=args.reason,
                terminal_observed=args.terminal_observed,
            ).public_dict()
        else:
            result = broker.status()
    except (LeaseBrokerError, OSError, sqlite3.Error) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
