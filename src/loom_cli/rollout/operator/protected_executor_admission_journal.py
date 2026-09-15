"""One durable executor credential beneath its protected component and guard.

The authority file precedes SQL issuance. A separate immutable completion marker
records readback; neither record alone admits live SQL or a completed bootstrap.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path

from loom.application_executor_admission import ApplicationExecutorAdmissionIdentity, _input
from loom.staging_mutation_coordination import rollout_guard_application_name

from .final_gate_plan import FinalGatePlan
from .protected_application_admission_recovery import (
    ApplicationAdmissionRecoveryRecord,
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

EXECUTOR_ADMISSION_COMPONENT_ID = "executor-database-admission"
_AUTHORITY = "executor-admission.json"
_ISSUED = "executor-admission-issued.json"


@dataclass(frozen=True, slots=True)
class ExecutorAdmissionRecord:
    admission: ApplicationAdmissionRecoveryRecord
    guard: MutationGuardEvidence
    bootstrap_terminal_digest: str
    bootstrap_event_digest: str
    inputs_digest: str
    seed_digest: str
    identity: ApplicationExecutorAdmissionIdentity
    password: str = field(repr=False)

    def __post_init__(self) -> None:
        _input(self.identity, self.password)
        coordination = self.admission.coordination_guard
        if (coordination is None or self.guard.state != "ready"
                or self.admission.target.database != "loom" or self.admission.target.owner_role != "loom"
                or self.admission.target.successor_role != "loom_app_staging_owner"
                or coordination.backend.pid != self.guard.database_backend_pid
                or coordination.application_name != rollout_guard_application_name(
                    request_id=self.guard.request_id, candidate_sha=self.guard.candidate_sha,
                    candidate_tree=self.guard.candidate_tree, generation=self.guard.generation)
                or self.identity.role_oid in {self.admission.target.owner_oid,
                    self.admission.target.successor_oid, coordination.role_oid}
                or any(not isinstance(value, str) or len(value) != 64
                    or any(c not in "0123456789abcdef" for c in value) or value == "0" * 64
                    for value in (self.bootstrap_terminal_digest, self.bootstrap_event_digest,
                        self.inputs_digest, self.seed_digest))):
            raise ValueError("executor admission retained authority is invalid")

    def to_dict(self) -> dict[str, object]:
        return {"schema_version": 1, "admission": self.admission.to_dict(), "guard": self.guard.to_dict(),
            "bootstrap_terminal_digest": self.bootstrap_terminal_digest, "bootstrap_event_digest": self.bootstrap_event_digest,
            "inputs_digest": self.inputs_digest, "seed_digest": self.seed_digest,
            "identity": asdict(self.identity), "password": self.password}

    @property
    def digest(self) -> str:
        return admission_record_digest(self.to_dict())

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ExecutorAdmissionRecord:
        if (set(value) != {"schema_version", "admission", "guard", "bootstrap_terminal_digest", "bootstrap_event_digest",
                "inputs_digest", "seed_digest", "identity", "password"}
                or type(value["schema_version"]) is not int or value["schema_version"] != 1):
            raise ValueError("executor admission record fields are invalid")
        identity = _mapping(value["identity"])
        if set(identity) != {"role_oid", "privilege_sha256"}:
            raise ValueError("executor admission identity fields are invalid")
        return cls(ApplicationAdmissionRecoveryRecord.from_dict(_mapping(value["admission"])),
            MutationGuardEvidence.from_dict(_mapping(value["guard"])),
            _string(value, "bootstrap_terminal_digest"), _string(value, "bootstrap_event_digest"),
            _string(value, "inputs_digest"), _string(value, "seed_digest"),
            ApplicationExecutorAdmissionIdentity(_integer(identity, "role_oid"), _string(identity, "privilege_sha256")),
            _string(value, "password"))


@dataclass(frozen=True, slots=True)
class ExecutorAdmissionJournal:
    journal: ProtectedApplyJournal
    plan: FinalGatePlan
    component: ProtectedApplyComponent
    ordinal: int

    def __post_init__(self) -> None:
        if (self.component.component_id != EXECUTOR_ADMISSION_COMPONENT_ID or type(self.ordinal) is not int
                or not 0 <= self.ordinal < 32 or self.plan.request_id != self.journal.request_id
                or self.plan.attempt_number != self.journal.attempt_number
                or FinalGatePlan.from_dict(self.plan.to_dict()) != self.plan):
            raise ValueError("executor admission journal binding is invalid")

    @property
    def root(self) -> Path:
        return self.journal.root / f"{self.ordinal:02d}-{self.component.component_id}"

    @property
    def intent(self) -> ComponentIntent:
        return ComponentIntent.build(self.plan, self.component, self.ordinal)

    def _validate(self, record: ExecutorAdmissionRecord) -> None:
        if (record.admission.intent_digest != self.intent.intent_digest or record.guard.request_id != self.plan.request_id
                or record.guard.candidate_sha != self.plan.candidate_sha or record.guard.candidate_tree != self.plan.candidate_tree
                or record.guard.mutation_epoch not in {self.plan.starting_mutation_epoch, self.plan.starting_mutation_epoch + 1}):
            raise RuntimeError("executor admission original plan or guard changed")

    def read(self) -> tuple[ExecutorAdmissionRecord, bool] | None:
        for directory in (self.journal.attempt_root, self.journal.root, self.root):
            try:
                _require_directory(directory, uid=self.journal.service_uid)
            except FileNotFoundError:
                return None
        if ComponentIntent.from_dict(self.journal._read(self.root / "intent.json")) != self.intent:
            raise RuntimeError("executor admission component intent changed")
        try:
            record = ExecutorAdmissionRecord.from_dict(self.journal._read(self.root / _AUTHORITY))
        except FileNotFoundError:
            if os.path.lexists(self.root / _ISSUED):
                raise RuntimeError("executor admission issuance lost its retained credential") from None
            return None
        self._validate(record)
        self.journal._sync_application_recovery(self.root, _AUTHORITY)
        try:
            issued = self.journal._read(self.root / _ISSUED)
        except FileNotFoundError:
            return record, False
        if issued != self._receipt(record):
            raise RuntimeError("executor admission issuance binding changed")
        self.journal._sync_application_recovery(self.root, _ISSUED)
        return record, True

    def _active(self, guard: MutationGuardEvidence) -> None:
        self.journal.require_application_guard_retained(self.plan, guard=guard)
        if self.journal._application_admission_context() != (self.root, self.intent):
            raise RuntimeError("executor admission active component changed")

    def retain(self, record: ExecutorAdmissionRecord, *, guard: MutationGuardEvidence) -> None:
        self._active(guard)
        self._validate(record)
        if record.guard != guard:
            raise RuntimeError("executor admission original guard changed")
        existing = self.read()
        if existing is not None and existing[0] != record:
            raise RuntimeError("executor admission retained credential changed")
        self.journal._publish_or_match(self.root / _AUTHORITY, record.to_dict())
        if self.read() != (record, existing[1] if existing else False):
            raise RuntimeError("executor admission credential readback changed")

    def mark_issued(self, record: ExecutorAdmissionRecord, *, guard: MutationGuardEvidence) -> None:
        self._active(guard)
        if record.guard != guard or self.read() not in ((record, False), (record, True)):
            raise RuntimeError("executor admission issuance lacks original retained authority")
        self.journal._publish_or_match(self.root / _ISSUED, self._receipt(record))
        if self.read() != (record, True):
            raise RuntimeError("executor admission issuance readback changed")

    def _receipt(self, record: ExecutorAdmissionRecord) -> dict[str, object]:
        return {"schema_version": 1, "intent_digest": self.intent.intent_digest, "admission_digest": record.digest}
