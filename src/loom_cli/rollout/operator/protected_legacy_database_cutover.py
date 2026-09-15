"""Retain runtime cutover separately from completed ownership-handoff recovery.

The enclosing installed operation supplies an admitted candidate/target, original
credential binding, actual guard and continuously checked host/workload/policy
exclusion. This operation owns only SQL closure and its bounded peer recovery.
It cannot publish fleet closure or start successors, and never edits the original
handoff journal. There is deliberately no standalone destructive CLI.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import ClassVar, Protocol, get_args

from loom.application_database_admission import (
    ApplicationDatabaseAdmissionTarget,
    ApplicationDatabaseCoordinationGuard,
    ApplicationDatabaseHandoffBackend,
    _maintenance_transaction,
    reclose_application_database_for_handoff_recovery,
    reopen_application_database_for_handoff_recovery,
    require_application_database_recovery_drained,
)
from loom.application_database_connection import application_sql
from loom.application_handoff_completion import (
    _require_retired_client_work,
    application_handoff_recovery_login_enabled,
)
from loom.application_runtime_cutover import close_application_runtime_for_cutover
from loom.application_runtime_retirement import (
    _require_boundary,
    _require_no_foreign_work,
    retire_application_runtime_sessions,
)
from loom.application_schema_reference import ApplicationSchemaAclProfile, ApplicationSchemaRevision

from .protected_application_admission_recovery import (
    ApplicationAdmissionRecoveryRecord,
    ApplicationHandoffReplacementReceipt,
    admission_record_digest,
    require_replacement_identity,
)
from .protected_legacy_writer_fence_installation import LegacyWriterFenceJournal
from .protected_peer_database_connection import PeerDatabaseConnection

_PEERS = 16


@dataclass(frozen=True, slots=True)
class LegacyDatabaseCutoverJournal(LegacyWriterFenceJournal):
    allowed_records: ClassVar[frozenset[str]] = frozenset({"cutover.intent.json", "cutover.terminal.json"} | {
        f"peer-{index:02d}.{phase}.json" for index in range(_PEERS) for phase in ("intent", "terminal")})

    @property
    def root(self) -> Path:
        return self.state_root / "protected-capacity" / "legacy-database-cutover-journals" / self.request_id / str(self.attempt_number)


class LegacyDatabaseCutoverRunner(Protocol):
    def open_staging_peer_database(self) -> PeerDatabaseConnection: ...
    def open_staging_peer_maintenance_database(self) -> PeerDatabaseConnection: ...


@dataclass(frozen=True, slots=True)
class LegacyDatabaseCutover:
    journal: LegacyDatabaseCutoverJournal
    plan_digest: str
    credential_binding_sha256: str
    target: ApplicationDatabaseAdmissionTarget
    coordination_guard: ApplicationDatabaseCoordinationGuard
    role_bindings: Mapping[str, str]
    password: str = field(repr=False)
    schema_acl_profile: ApplicationSchemaAclProfile
    schema_revision: ApplicationSchemaRevision
    runner: LegacyDatabaseCutoverRunner
    authority_check: Callable[[], None]

    def __post_init__(self) -> None:
        if (any(not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None or value == "0" * 64
                for value in (self.plan_digest, self.credential_binding_sha256))
            or self.schema_acl_profile not in get_args(ApplicationSchemaAclProfile)
            or self.schema_revision not in get_args(ApplicationSchemaRevision)
            or not callable(self.authority_check)
            or len([role for role, alias in self.role_bindings.items() if alias == "provisioner"]) != 1):
            raise ValueError("legacy database cutover binding is invalid")

    @property
    def provisioner(self) -> str:
        return next(role for role, alias in self.role_bindings.items() if alias == "provisioner")

    def _scope(self) -> dict[str, object]:
        return {"schema_version": 1, "request_id": self.journal.request_id,
            "attempt_number": self.journal.attempt_number, "plan_digest": self.plan_digest,
            "credential_binding_sha256": self.credential_binding_sha256,
            "target": asdict(self.target), "coordination_guard": asdict(self.coordination_guard),
            "role_bindings": dict(self.role_bindings), "schema_acl_profile": self.schema_acl_profile,
            "schema_revision": self.schema_revision}

    def _history(self, scope_digest: str) -> tuple[list[ApplicationHandoffReplacementReceipt], int]:
        receipts: list[ApplicationHandoffReplacementReceipt] = []
        missing = False
        for index in range(_PEERS):
            intent = self.journal.read(f"peer-{index:02d}.intent.json")
            terminal = self.journal.read(f"peer-{index:02d}.terminal.json")
            if intent is None:
                missing = True
                if terminal is not None:
                    raise RuntimeError("legacy database cutover peer has no intent")
                continue
            if missing or intent != self._peer_intent(scope_digest, receipts):
                raise RuntimeError("legacy database cutover peer history binding changed")
            if terminal is None:
                missing = True
                continue
            receipt = ApplicationHandoffReplacementReceipt.from_dict(terminal)
            if receipt.recovery_intent_digest != admission_record_digest(intent):
                raise RuntimeError("legacy database cutover peer receipt binding changed")
            self._validate_peer(receipt.handoff_backend, receipts, scope_digest)
            receipts.append(receipt)
        return receipts, len(receipts)

    @staticmethod
    def _peer_intent(scope_digest: str, receipts: list[ApplicationHandoffReplacementReceipt]) -> dict[str, object]:
        return {"schema_version": 1, "scope_digest": scope_digest,
            "previous_peer_digest": receipts[-1].digest if receipts else None}

    def _validate_peer(self, backend: ApplicationDatabaseHandoffBackend,
                       receipts: list[ApplicationHandoffReplacementReceipt], scope_digest: str) -> None:
        original = ApplicationAdmissionRecoveryRecord(scope_digest, self.target,
            receipts[0].handoff_backend if receipts else backend, self.coordination_guard)
        if receipts:
            require_replacement_identity(original, [item.handoff_backend for item in receipts], backend)

    def _record_peer(self, peer: PeerDatabaseConnection, index: int, scope_digest: str,
                     receipts: list[ApplicationHandoffReplacementReceipt]) -> ApplicationHandoffReplacementReceipt:
        self.authority_check()
        identity = peer.backend_identity
        if identity.database != self.target.database or identity.session_user != self.provisioner:
            raise RuntimeError("legacy database cutover fixed peer identity changed")
        backend = ApplicationDatabaseHandoffBackend(identity.backend_pid, identity.backend_started_at,
            identity.system_identifier, identity.server_started_at, identity.database_oid)
        self._validate_peer(backend, receipts, scope_digest)
        receipt = ApplicationHandoffReplacementReceipt(
            admission_record_digest(self._peer_intent(scope_digest, receipts)), backend)
        self.journal.retain(f"peer-{index:02d}.terminal.json", receipt.to_dict())
        return receipt

    def _close(self, peer: PeerDatabaseConnection, receipt: ApplicationHandoffReplacementReceipt) -> None:
        self.authority_check()
        with self.runner.open_staging_peer_maintenance_database() as maintenance:
            close_application_runtime_for_cutover(peer, maintenance=maintenance, target=self.target,
                handoff_backend=receipt.handoff_backend, coordination_guard=self.coordination_guard,
                role_bindings=self.role_bindings, password=self.password,
                schema_acl_profile=self.schema_acl_profile, schema_revision=self.schema_revision)
        self.authority_check()

    def _observe_closed(self, backend: ApplicationDatabaseHandoffBackend) -> None:
        self.authority_check()
        with self.runner.open_staging_peer_maintenance_database() as maintenance:
            with _maintenance_transaction(maintenance, database=self.target.database, provisioner_role=self.provisioner):
                maintenance.execute("SET TRANSACTION READ ONLY")
                if maintenance.execute("SELECT current_database()='postgres'").fetchone() != (True,):
                    raise RuntimeError("legacy database cutover maintenance peer changed")
                _require_boundary(maintenance, self.target, self.coordination_guard, self.password)
                _require_no_foreign_work(maintenance, self.target)
                if maintenance.execute(application_sql(
                    "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE usesysid={})",
                    self.target.owner_oid,
                )).fetchone() != (False,):
                    raise RuntimeError("legacy database cutover runtime sessions remain")
            require_application_database_recovery_drained(maintenance, target=self.target,
                provisioner_role=self.provisioner, handoff_backend=backend,
                runtime_password=self.password, coordination_guard=self.coordination_guard)
            _require_retired_client_work(maintenance, target=self.target, handoff_backend=backend,
                coordination_guard=self.coordination_guard, provisioner=self.provisioner)
        self.authority_check()

    def _recover(self, *, index: int, scope_digest: str,
                 receipts: list[ApplicationHandoffReplacementReceipt]) -> ApplicationHandoffReplacementReceipt:
        lost = receipts[-1].handoff_backend
        reopened = False
        try:
            with self.runner.open_staging_peer_maintenance_database() as maintenance:
                self.authority_check()
                login = application_handoff_recovery_login_enabled(maintenance, target=self.target,
                    handoff_backend=lost, coordination_guard=self.coordination_guard,
                    provisioner_role=self.provisioner)
                if not login:
                    reclose_application_database_for_handoff_recovery(maintenance, target=self.target,
                        provisioner_role=self.provisioner, handoff_backend=lost,
                        runtime_password=self.password, coordination_guard=self.coordination_guard)
                    retire_application_runtime_sessions(maintenance, target=self.target,
                        coordination_guard=self.coordination_guard, provisioner_role=self.provisioner,
                        password=self.password)
                    _require_retired_client_work(maintenance, target=self.target, handoff_backend=lost,
                        coordination_guard=self.coordination_guard, provisioner=self.provisioner)
                    # Set before issuing SQL: even a lost reopen ACK requires
                    # fresh serialized reclosure in finally.
                    reopened = True
                    reopen_application_database_for_handoff_recovery(maintenance, target=self.target,
                        provisioner_role=self.provisioner, handoff_backend=lost,
                        runtime_password=self.password, coordination_guard=self.coordination_guard)
                with self.runner.open_staging_peer_database() as peer:
                    receipt = self._record_peer(peer, index, scope_digest, receipts)
                    # The complete closure checks ALL clients. Do not leave our
                    # recovery maintenance connection as an unknown second peer.
                    maintenance.close()
                    self._close(peer, receipt)
                    return receipt
        finally:
            if reopened:
                self.authority_check()
                with self.runner.open_staging_peer_maintenance_database() as cleanup:
                    reclose_application_database_for_handoff_recovery(cleanup, target=self.target,
                        provisioner_role=self.provisioner, handoff_backend=lost,
                        runtime_password=self.password, coordination_guard=self.coordination_guard)

    def retire(self) -> dict[str, object]:
        self.authority_check()
        scope = self._scope()
        digest = admission_record_digest(scope)
        with self.journal.exclusive():
            saved = self.journal.read("cutover.intent.json")
            if saved is not None and saved != scope:
                raise RuntimeError("legacy database cutover retained binding changed")
            self.journal.retain("cutover.intent.json", scope)
            receipts, index = self._history(digest)
            terminal = self.journal.read("cutover.terminal.json")
            if terminal is not None:
                if (not receipts or terminal != {"schema_version": 1, "scope_digest": digest, "peer_digest": receipts[-1].digest}
                    or (index < _PEERS and self.journal.read(f"peer-{index:02d}.intent.json") is not None)):
                    raise RuntimeError("legacy database cutover terminal binding changed")
                self._observe_closed(receipts[-1].handoff_backend)
                return terminal
            if index >= _PEERS:
                raise RuntimeError("legacy database cutover peer recovery limit reached")
            self.authority_check()
            self.journal.retain(f"peer-{index:02d}.intent.json", self._peer_intent(digest, receipts))
            if receipts:
                receipt = self._recover(index=index, scope_digest=digest, receipts=receipts)
            else:
                with self.runner.open_staging_peer_database() as peer:
                    receipt = self._record_peer(peer, index, digest, receipts)
                    self._close(peer, receipt)
            self._observe_closed(receipt.handoff_backend)
            terminal = {"schema_version": 1, "scope_digest": digest, "peer_digest": receipt.digest}
            self.journal.retain("cutover.terminal.json", terminal)
            return terminal
