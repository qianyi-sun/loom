"""Private ordered migration phases beneath the original protected component.

Records carry historical authority and dispatch/recovery intent. Live checks and
the enclosing installed component still own SQL/Job/Secret effects. Every append
requires that component's active apply and original acknowledged guard. An
incomplete retirement cannot be skipped by creating another credential generation.
"""

from __future__ import annotations

import base64
import binascii
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from loom.staging_mutation_coordination import rollout_guard_application_name
from loom_cli.rollout.application_migration_contract import APPLICATION_OWNER_ROLE

from .final_gate_plan import FinalGatePlan
from .protected_application_admission_recovery import (
    ApplicationAdmissionRecoveryRecord,
    _backend,
    _integer,
    _string,
    admission_record_digest,
)
from .protected_apply_journal import (
    ComponentIntent,
    ProtectedApplyComponent,
    ProtectedApplyJournal,
    _require_directory,
)
from .protected_cnpg_writer_configuration import _mapping
from .staging_mutation_guard import MutationGuardEvidence

_MAX_EVENTS = 256
_NEXT = {"role": "generation", "secret-dispatch": "role", "secret": "secret-dispatch",
    "job-dispatch": "secret", "job": "job-dispatch", "job-stopped": "retirement", "closed": "job-stopped",
    "role-retire": "closed", "role-retired": "role-retire", "secret-deleted": "role-retired",
    "reopen": "secret-deleted", "reopened": "reopen", "complete": "reopened"}
_EMPTY = {"job-stopped", "closed", "role-retire", "role-retired", "secret-deleted", "reopen", "reopened", "abandoned"}


def _sha(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


@dataclass(frozen=True, slots=True)
class ApplicationMigrationEvent:
    sequence: int
    phase: str
    payload: Mapping[str, object] = field(repr=False)
    intent_digest: str
    guard_digest: str
    previous_digest: str
    event_digest: str

    def to_dict(self) -> dict[str, object]:
        return {"schema_version": 1, "sequence": self.sequence, "phase": self.phase, "payload": dict(self.payload),
            "intent_digest": self.intent_digest, "guard_digest": self.guard_digest,
            "previous_digest": self.previous_digest, "event_digest": self.event_digest}

    @classmethod
    def build(cls, *, sequence: int, phase: str, payload: Mapping[str, object], intent_digest: str,
              guard_digest: str, previous_digest: str) -> ApplicationMigrationEvent:
        record = {"schema_version": 1, "sequence": sequence, "phase": phase, "payload": dict(payload),
            "intent_digest": intent_digest, "guard_digest": guard_digest, "previous_digest": previous_digest}
        return cls.from_dict({**record, "event_digest": admission_record_digest(record)})

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ApplicationMigrationEvent:
        if (set(value) != {"schema_version", "sequence", "phase", "payload", "intent_digest", "guard_digest", "previous_digest", "event_digest"}
                or type(value.get("schema_version")) is not int or value["schema_version"] != 1
                or type(value.get("sequence")) is not int or not 0 <= _integer(value, "sequence") < _MAX_EVENTS
                or not isinstance(value.get("phase"), str)
                or value.get("phase") not in {"authority", "generation", "retirement", "maintenance-peer", "abandoned", "noop", *_NEXT}
                or any(not _sha(value.get(key)) for key in ("intent_digest", "guard_digest", "previous_digest", "event_digest"))
                or admission_record_digest({key: item for key, item in value.items() if key != "event_digest"}) != value["event_digest"]):
            raise ValueError("application migration event binding is invalid")
        return cls(_integer(value, "sequence"), _string(value, "phase"), _mapping(value["payload"]),
            _string(value, "intent_digest"), _string(value, "guard_digest"), _string(value, "previous_digest"), _string(value, "event_digest"))


@dataclass(frozen=True, slots=True)
class ApplicationMigrationJournal:
    journal: ProtectedApplyJournal
    plan: FinalGatePlan
    component: ProtectedApplyComponent
    ordinal: int

    def __post_init__(self) -> None:
        if (self.component.component_id not in {"database-migration", "staging-capacity-database"} or type(self.ordinal) is not int
                or not 0 <= self.ordinal < 32 or self.plan.request_id != self.journal.request_id
                or self.plan.attempt_number != self.journal.attempt_number
                or FinalGatePlan.from_dict(self.plan.to_dict()) != self.plan):
            raise ValueError("application migration journal binding is invalid")

    @property
    def root(self) -> Path:
        return self.journal.root / f"{self.ordinal:02d}-{self.component.component_id}"

    @property
    def capacity_bootstrap(self) -> bool:
        return self.component.component_id == "staging-capacity-database"

    @property
    def source_revision(self) -> str:
        return "pending" if self.capacity_bootstrap else self.plan.schema_revision

    @property
    def target_revision(self) -> str:
        # Capacity completion includes the exact desired authority and runtime
        # credentials, not only its guard Alembic version.
        return "exact" if self.capacity_bootstrap else self.plan.migration_target_revision

    @property
    def intent(self) -> ComponentIntent:
        return ComponentIntent.build(self.plan, self.component, self.ordinal)

    def read(self) -> tuple[ApplicationMigrationEvent, ...]:
        for directory in (self.journal.attempt_root, self.journal.root, self.root):
            try:
                _require_directory(directory, uid=self.journal.service_uid)
            except FileNotFoundError:
                return ()
        if ComponentIntent.from_dict(self.journal._read(self.root / "intent.json")) != self.intent:
            raise ValueError("application migration original component intent changed")
        paths = sorted(self.root.glob("migration-event-*.json"))
        if len(paths) > _MAX_EVENTS:
            raise ValueError("application migration history is unbounded")
        events = []
        previous = self.intent.intent_digest
        for index, path in enumerate(paths):
            if path.name != f"migration-event-{index:04d}.json":
                raise ValueError("application migration history has a missing or invalid event")
            event = ApplicationMigrationEvent.from_dict(self.journal._read(path))
            if (event.sequence != index or event.intent_digest != self.intent.intent_digest
                    or event.previous_digest != previous):
                raise ValueError("application migration history binding changed")
            events.append(event)
            previous = event.event_digest
        _validate_history(self.plan, events, capacity=self.capacity_bootstrap)
        for path in paths:
            self.journal._sync_application_recovery(self.root, path.name)
        if sorted(self.root.glob("migration-event-*.json")) != paths:
            raise ValueError("application migration history changed during observation")
        return tuple(events)

    def append(self, phase: str, payload: Mapping[str, object], *, guard: MutationGuardEvidence) -> ApplicationMigrationEvent:
        self.journal.require_application_guard_retained(self.plan, guard=guard)
        root, intent = self.journal._application_admission_context()
        if root != self.root or intent != self.intent:
            raise ValueError("application migration active component changed")
        events = self.read()
        if events and events[-1].phase == phase and events[-1].payload == payload:
            return events[-1]
        event = ApplicationMigrationEvent.build(sequence=len(events), phase=phase, payload=payload,
            intent_digest=intent.intent_digest, guard_digest=guard.evidence_digest,
            previous_digest=events[-1].event_digest if events else intent.intent_digest)
        _validate_history(self.plan, (*events, event), capacity=self.capacity_bootstrap)
        path = self.root / f"migration-event-{event.sequence:04d}.json"
        self.journal._publish_or_match(path, event.to_dict())
        observed = self.read()
        if observed != (*events, event):
            raise ValueError("application migration append readback changed")
        return event


def _fields(payload: Mapping[str, object], keys: set[str]) -> None:
    if set(payload) != keys:
        raise ValueError("application migration phase fields are invalid")


def _validate_history(plan: FinalGatePlan, events: Sequence[ApplicationMigrationEvent], *, capacity: bool = False) -> None:
    if not events:
        return
    first = events[0]
    if first.phase != "authority":
        raise ValueError("application migration requires original authority first")
    _fields(first.payload, {"admission", "guard", "handoff_digest", "credential_digest", "inputs_digest"}
        | ({"guard_owner", "guard_migrator", "runtime_role_oids", "seed_digest", "migration_digest"} if capacity else set()))
    admission = ApplicationAdmissionRecoveryRecord.from_dict(_mapping(first.payload["admission"]))
    guard = MutationGuardEvidence.from_dict(_mapping(first.payload["guard"]))
    coordination = admission.coordination_guard
    if (admission.intent_digest != first.intent_digest or coordination is None
            or admission.target.database != "loom" or admission.target.owner_role != "loom"
            or admission.target.successor_role != APPLICATION_OWNER_ROLE
            or first.guard_digest != guard.evidence_digest or guard.state != "ready"
            or guard.request_id != plan.request_id or guard.candidate_sha != plan.candidate_sha
            or guard.candidate_tree != plan.candidate_tree
            or guard.mutation_epoch not in {plan.starting_mutation_epoch, plan.starting_mutation_epoch + 1}
            or coordination.backend.pid != guard.database_backend_pid
            or coordination.application_name != rollout_guard_application_name(request_id=guard.request_id,
                candidate_sha=guard.candidate_sha, candidate_tree=guard.candidate_tree, generation=guard.generation)
            or any(not _sha(first.payload[key]) for key in ("handoff_digest", "credential_digest", "inputs_digest"))):
        raise ValueError("application migration original authority changed")
    capacity_oid = _capacity_authority(first, admission) if capacity else None
    source_revision = "pending" if capacity else plan.schema_revision
    target_revision = "exact" if capacity else plan.migration_target_revision
    previous = "authority"
    generations = 0
    phases: set[str] = set()
    successful = False
    maintenance_peers: list[object] = []
    for event in events[1:]:
        phase, payload = event.phase, event.payload
        if event.guard_digest != guard.evidence_digest:
            raise ValueError("application migration original guard changed")
        if phase == "generation":
            if previous not in {"authority", "abandoned", "complete"} or (previous == "complete" and successful):
                raise ValueError("application migration cannot rearm before retirement")
            generations += 1
            phases = set()
            maintenance_peers = []
            _fields(payload, {"ordinal", "nonce", "password", "expires_at", "creation_backend", "ca_certificate"})
            backend = _backend(payload["creation_backend"])
            expires = datetime.fromisoformat(_string(payload, "expires_at"))
            if (type(payload["ordinal"]) is not int or payload["ordinal"] != generations or generations > 8
                    or re.fullmatch(r"[0-9a-f]{32}", _string(payload, "nonce")) is None
                    or re.fullmatch(r"[A-Za-z0-9_-]{64}", _string(payload, "password")) is None
                    or expires.utcoffset() is None or expires.astimezone(UTC).isoformat() != payload["expires_at"]
                    or backend.system_identifier != admission.target.system_identifier
                    or backend.database_oid != admission.target.database_oid or backend.pid == coordination.backend.pid
                    or backend.server_started_at != coordination.backend.server_started_at):
                raise ValueError("application migration credential generation is invalid")
            try:
                ca = base64.b64decode(_string(payload, "ca_certificate"), validate=True)
            except (ValueError, binascii.Error):
                raise ValueError("application migration CA encoding is invalid") from None
            if not 64 <= len(ca) <= 65536:
                raise ValueError("application migration CA is unbounded")
        elif phase == "retirement":
            if previous not in {"generation", "role", "secret-dispatch", "secret", "job-dispatch", "job"}:
                raise ValueError("application migration retirement ordering changed")
            _fields(payload, {"successful", "maintenance_backend"})
            backend = _backend(payload["maintenance_backend"])
            if (type(payload["successful"]) is not bool or (payload["successful"] and previous != "job")
                    or backend.system_identifier != admission.target.system_identifier
                    or backend.database_oid == admission.target.database_oid or backend.pid == coordination.backend.pid
                    or backend.server_started_at != coordination.backend.server_started_at):
                raise ValueError("application migration retirement authority changed")
            successful = bool(payload["successful"])
            maintenance_peers.append(backend)
        elif phase == "maintenance-peer":
            _fields(payload, {"backend"})
            backend = _backend(payload["backend"])
            if ("retirement" not in phases or previous in {"complete", "abandoned"}
                    or backend in maintenance_peers or len(maintenance_peers) >= 8
                    or backend.system_identifier != admission.target.system_identifier
                    or backend.database_oid == admission.target.database_oid
                    or backend.pid == coordination.backend.pid
                    or backend.server_started_at != coordination.backend.server_started_at):
                raise ValueError("application migration replacement maintenance peer changed")
            maintenance_peers.append(backend)
            continue
        elif phase == "abandoned":
            if capacity or previous not in {"generation", "role", "retirement"} or "secret-dispatch" in phases:
                raise ValueError("application migration cannot abandon delivered credentials")
            _fields(payload, set())
        elif phase == "noop":
            _fields(payload, {"revision"})
            if previous not in {"authority", "abandoned", "complete"} or payload["revision"] != target_revision:
                raise ValueError("application migration no-op binding changed")
        else:
            if _NEXT.get(phase) != previous:
                raise ValueError("application migration phase skipped an original prerequisite")
            if phase in _EMPTY:
                _fields(payload, set())
                if phase == "closed" and "role" not in phases:
                    raise ValueError("application migration closure lacks a recorded role")
            elif phase == "role":
                _fields(payload, {"oid"})
                if (type(payload["oid"]) is not int or not 0 < _integer(payload, "oid") < 2**32
                        or payload["oid"] in {admission.target.owner_oid, admission.target.successor_oid, coordination.role_oid}
                        or (capacity_oid is not None and payload["oid"] != capacity_oid)):
                    raise ValueError("application migration role OID is invalid")
            elif phase in {"secret-dispatch", "job-dispatch"}:
                _fields(payload, {"manifest_sha256"})
                if not _sha(payload["manifest_sha256"]):
                    raise ValueError("application migration dispatch fingerprint is invalid")
            elif phase in {"secret", "job"}:
                _fields(payload, {"uid"})
                if re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", _string(payload, "uid")) is None:
                    raise ValueError("application migration resource UID is invalid")
            elif phase == "complete":
                _fields(payload, {"successful", "revision"})
                if (type(payload["successful"]) is not bool or payload["successful"] != successful
                        or payload["revision"] not in {source_revision, target_revision}
                        or (successful and payload["revision"] != target_revision)):
                    raise ValueError("application migration completion binding changed")
        phases.add(phase)
        previous = phase


def _capacity_authority(event: ApplicationMigrationEvent, admission: ApplicationAdmissionRecoveryRecord) -> int:
    """Bind permanent role OIDs before any capacity credential can be armed."""
    owner = _mapping(event.payload["guard_owner"])
    migrator = _mapping(event.payload["guard_migrator"])
    runtime = _mapping(event.payload["runtime_role_oids"])
    _fields(owner, {"role_name", "role_oid"})
    _fields(migrator, {"role_name", "role_oid"})
    _fields(runtime, {"loom_cap_staging_agent", "loom_cap_staging_executor",
        "loom_cap_staging_observer", "loom_cap_staging_runtime"})
    assert admission.coordination_guard is not None
    oids = [owner["role_oid"], migrator["role_oid"], *runtime.values(), admission.target.owner_oid,
        admission.target.successor_oid, admission.coordination_guard.role_oid]
    if (owner["role_name"] != "loom_cap_staging_owner" or migrator["role_name"] != "loom_cap_staging_migrator"
            or any(type(oid) is not int or not 0 < oid < 2**32 for oid in oids)
            or len(set(oids)) != 9
            or any(not _sha(event.payload[key]) for key in ("seed_digest", "migration_digest"))):
        raise ValueError("application capacity original role authority changed")
    return _integer(migrator, "role_oid")
