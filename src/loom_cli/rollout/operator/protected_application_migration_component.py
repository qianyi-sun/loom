"""Protected post-handoff migration with same-guard closed-admission recovery."""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, replace
from typing import Protocol

from loom.application_completed_authority import (
    ApplicationOwnerSuccessor,
    observe_completed_application_authority,
)
from loom.application_migrator_recovery import observe_application_migrator_role

from .final_gate_plan import FinalGatePlan
from .protected_application_admission_recovery import (
    ApplicationAdmissionRecoveryRecord,
    _integer,
    admission_record_digest,
)
from .protected_application_credential_recovery import ApplicationCredentialObservation
from .protected_application_handoff_component import ApplicationHandoffRunner, _admit_sql_profiles
from .protected_application_migration_ca import ApplicationMigrationCA
from .protected_application_migration_documents import (
    application_migration_documents,
    application_migration_role,
)
from .protected_application_migration_journal import (
    ApplicationMigrationEvent,
    ApplicationMigrationJournal,
)
from .protected_application_migration_lifecycle import ApplicationMigrationLifecycle
from .protected_application_migration_resources import ProtectedApplicationMigrationResources
from .protected_application_migration_runtime import (
    ApplicationMigrationRunner,
    ProtectedApplicationMigrationRuntime,
)
from .protected_application_owner_preparation import APPLICATION_OWNER_ROLE, _observe
from .protected_apply_journal import (
    ApplicationRecoveryView,
    ComponentObservation,
    ComponentState,
    ComponentTerminal,
    ProtectedApplyComponent,
    ProtectedApplyJournal,
    ProtectedApplyJournalError,
)
from .protected_cnpg_runtime_admission import CNPGPrimaryRuntime
from .protected_cnpg_sql_admission import require_cnpg_effective_sql_profile
from .protected_cnpg_writer_configuration import _mapping
from .protected_migration_component import KubernetesProtectedMigrationComponent
from .staging_mutation_guard import MutationGuardEvidence


@dataclass(frozen=True, slots=True)
class ApplicationMigrationInputs:
    credential: ApplicationCredentialObservation
    runtime: CNPGPrimaryRuntime
    external_sha256: str
    ca: ApplicationMigrationCA

    @property
    def credential_digest(self) -> str:
        return admission_record_digest(asdict(self.credential.binding))

    def digest(self) -> str:
        if self.runtime.cluster_uid != self.credential.configuration.cluster_uid or self.ca.cluster_uid != self.runtime.cluster_uid:
            raise RuntimeError("application migration input cluster identity changed")
        return admission_record_digest({"credential": self.credential_digest,
            "configuration": asdict(self.credential.configuration), "runtime": self.runtime.to_dict(),
            "external_sha256": self.external_sha256, "ca": self.ca.binding()})


class ApplicationMigrationComponentRunner(ApplicationMigrationRunner, ApplicationHandoffRunner, Protocol):
    pass


@dataclass(frozen=True, slots=True)
class ProtectedApplicationMigrationComponent:
    plan: FinalGatePlan
    journal: ProtectedApplyJournal
    runner: ApplicationMigrationComponentRunner
    ordinal: int
    guard_source: Callable[[], MutationGuardEvidence]
    epoch_source: Callable[[], int]
    inputs_source: Callable[[], ApplicationMigrationInputs]
    handoff_source: Callable[[], tuple[ApplicationRecoveryView, ComponentTerminal]]
    successor_source: Callable[[], ApplicationOwnerSuccessor | None]
    container_registry: str

    def __post_init__(self) -> None:
        if (self.plan.environment != "staging" or self.plan.namespace != "loom-staging"
                or self.plan.checkpoint_schema_version != 3 or type(self.ordinal) is not int or not 1 <= self.ordinal < 32
                or self.journal.request_id != self.plan.request_id or self.journal.attempt_number != self.plan.attempt_number
                or not self.container_registry):
            raise ValueError("application migration component binding is invalid")

    def component(self) -> ProtectedApplyComponent:
        return ProtectedApplyComponent("database-migration", hashlib.sha256(b"loom-protected-owner-migration-v1").hexdigest(),
            admission_record_digest({"plan_digest": self.plan.plan_digest, "ordinal": self.ordinal,
                "container_registry": self.container_registry}), self.classify, self.apply)

    def _journal(self) -> ApplicationMigrationJournal:
        return ApplicationMigrationJournal(self.journal, self.plan, self.component(), self.ordinal)

    def _guard(self) -> MutationGuardEvidence:
        guard = self.guard_source()
        epoch = self.epoch_source()
        if (guard.state != "ready" or guard.request_id != self.plan.request_id
                or guard.candidate_sha != self.plan.candidate_sha or guard.candidate_tree != self.plan.candidate_tree
                or guard.mutation_epoch not in {self.plan.starting_mutation_epoch, self.plan.starting_mutation_epoch + 1}
                or type(epoch) is not int or epoch != self.plan.starting_mutation_epoch + 1 or self.guard_source() != guard):
            raise RuntimeError("application migration guard or claimed epoch changed")
        return guard

    def _handoff(self) -> tuple[ApplicationRecoveryView, ComponentTerminal]:
        view, terminal = self.handoff_source()
        if (view.admission is None or view.credential_binding is None or view.restoration is None or not view.fences_retiring
                or view.intent.component_id != "application-ownership-handoff" or view.intent.ordinal >= self.ordinal
                or view.intent.plan_digest != self.plan.plan_digest or terminal.intent_digest != view.intent.intent_digest
                or terminal.observed_epoch != self.plan.starting_mutation_epoch + 1):
            raise RuntimeError("application migration lacks completed original handoff")
        return view, terminal

    def _inputs(self) -> ApplicationMigrationInputs:
        inputs = self.inputs_source()
        view, _ = self._handoff()
        assert view.credential_binding is not None and view.cnpg_runtime is not None
        expected = view.credential_binding
        if (replace(inputs.credential.binding, application_resource_version=expected.application_resource_version,
                    cnpg_resource_version=expected.cnpg_resource_version) != expected
                or inputs.runtime.cluster_uid != view.cnpg_runtime.cluster_uid):
            raise RuntimeError("application migration original runtime credential or cluster changed")
        inputs.digest()
        return inputs

    def _terminal(self, events: Sequence[ApplicationMigrationEvent]) -> ComponentTerminal | None:
        migration = self._journal()
        try:
            terminal = ComponentTerminal.from_dict(self.journal._read(migration.root / "terminal.json"))
        except FileNotFoundError:
            return None
        if (not _finished(events) or terminal.intent_digest != migration.intent.intent_digest
                or terminal.component_id != self.component().component_id or terminal.observed_epoch != self.plan.starting_mutation_epoch + 1
                or terminal.evidence_digest != self._evidence(events)):
            raise RuntimeError("application migration terminal has no completed retirement")
        from .protected_application_guard_retention import _read_pending_retention
        if _read_pending_retention(self.journal.attempt_root.parents[3], request_id=self.plan.request_id,
                service_uid=self.journal.service_uid, require_record=True, component_id=self.component().component_id) is not None:
            raise RuntimeError("application migration completed guard retention is still pending")
        self.journal._sync_application_recovery(migration.root, "terminal.json")
        return terminal

    def _context(self, events: Sequence[ApplicationMigrationEvent]) -> tuple[MutationGuardEvidence, ApplicationMigrationInputs]:
        guard = self._guard()
        _, handoff = self._handoff()
        inputs = self._inputs()
        if events:
            authority = events[0]
            if authority.payload["handoff_digest"] != handoff.terminal_digest:
                raise RuntimeError("application migration original handoff terminal changed")
            if self._terminal(events) is None and (authority.guard_digest != guard.evidence_digest
                    or authority.payload["credential_digest"] != inputs.credential_digest
                    or authority.payload["inputs_digest"] != inputs.digest()):
                raise RuntimeError("application migration original guard or inputs changed")
        if self._guard() != guard:
            raise RuntimeError("application migration guard changed during observation")
        return guard, inputs

    def _evidence(self, events: Sequence[ApplicationMigrationEvent]) -> str:
        return admission_record_digest({"migration_event_digest": events[-1].event_digest,
            "target_revision": self._journal().target_revision, "plan_digest": self.plan.plan_digest})

    def classify(self, plan: FinalGatePlan) -> ComponentObservation:
        if plan != self.plan:
            raise RuntimeError("application migration original plan changed")
        events = self._journal().read()
        guard, inputs = self._context(events)
        if _finished(events):
            self._observe_completed(events, guard, inputs)
            return ComponentObservation(ComponentState.EXACT, self._evidence(events), self.plan.starting_mutation_epoch + 1)
        # Never open an ordinary application peer for a pending closure. The
        # original active lifecycle must restore admission before such reads.
        return ComponentObservation(ComponentState.READY, admission_record_digest({"inputs": inputs.digest(),
            "last_event": events[-1].event_digest if events else None}), self.plan.starting_mutation_epoch + 1)

    def _template(self) -> bytes:
        return KubernetesProtectedMigrationComponent(runner=self.runner, environment=self.runner.environment,
            service_uid=self.journal.service_uid, container_registry=self.container_registry)._read_manifest(self.plan)

    def _retain(self, guard: MutationGuardEvidence) -> None:
        self.journal.retain_application_guard(self.plan, guard=guard)
        deadline = time.monotonic() + 35
        while True:
            if self._guard() != guard:
                raise RuntimeError("application migration original guard changed during retention")
            try:
                self.journal.require_application_guard_retained(self.plan, guard=guard)
                return
            except ProtectedApplyJournalError as exc:
                if str(exc) != "application guard acknowledgement is absent" or time.monotonic() >= deadline:
                    raise
                time.sleep(0.1)

    def apply(self, plan: FinalGatePlan) -> None:
        if plan != self.plan:
            raise RuntimeError("application migration original plan changed")
        migration = self._journal()
        events = migration.read()
        guard, inputs = self._context(events)
        template = self._template()
        self._retain(guard)
        view, handoff = self._handoff()
        assert view.admission is not None
        if not events:
            with self.runner.open_staging_peer_database() as peer:
                _admit_sql_profiles(self.runner, peer, guard, separated_owner=True)
                observe_completed_application_authority(peer, target=view.admission.target,
                    runtime_password=inputs.credential.credential.password)
                backend, coordination = _observe(peer, guard, owner_role=APPLICATION_OWNER_ROLE)
                admission = ApplicationAdmissionRecoveryRecord(migration.intent.intent_digest, view.admission.target, backend, coordination)
                if self._context(())[0] != guard or self._inputs() != inputs:
                    raise RuntimeError("application migration initial authority changed")
                migration.append("authority", {"admission": admission.to_dict(), "guard": guard.to_dict(),
                    "handoff_digest": handoff.terminal_digest, "credential_digest": inputs.credential_digest,
                    "inputs_digest": inputs.digest()}, guard=guard)
            events = migration.read()
        original = ApplicationAdmissionRecoveryRecord.from_dict(_mapping(events[0].payload["admission"]))
        assert original.coordination_guard is not None
        def checkpoint() -> None:
            current_guard, current = self._context(migration.read())
            if current_guard != guard or current != inputs or self._template() != template:
                raise RuntimeError("application migration original active authority changed")
            for database, opener in (("postgres", self.runner.open_staging_peer_maintenance_database),
                                     ("template1", self.runner.open_staging_peer_template_database)):
                with opener() as connection:
                    require_cnpg_effective_sql_profile(connection, database=database, original=original.handoff_backend)
        with ProtectedApplicationMigrationRuntime(plan=self.plan, guard=guard, target=original.target,
                coordination_guard=original.coordination_guard, runner=self.runner, template=template,
                ca_certificate=inputs.ca.certificate, runtime_password=inputs.credential.credential.password,
                container_registry=self.container_registry, assert_guard=self._guard, assert_inputs=checkpoint,
                intent_digest=migration.intent.intent_digest) as runtime:
            ApplicationMigrationLifecycle(migration, guard, runtime).run()

    def _observe_completed(self, events: Sequence[ApplicationMigrationEvent], guard: MutationGuardEvidence,
                           inputs: ApplicationMigrationInputs) -> None:
        original = ApplicationAdmissionRecoveryRecord.from_dict(_mapping(events[0].payload["admission"]))
        historical_guard = MutationGuardEvidence.from_dict(_mapping(events[0].payload["guard"]))
        template = self._template()
        successor = self.successor_source()
        with self.runner.open_staging_peer_database() as peer:
            _admit_sql_profiles(self.runner, peer, guard, separated_owner=True)
            _, coordination = _observe(peer, guard, owner_role=APPLICATION_OWNER_ROLE)
            observe_completed_application_authority(peer, target=original.target,
                runtime_password=inputs.credential.credential.password, successor=successor)
            generation = None
            oid = None
            for event in events:
                if event.phase == "generation":
                    generation, oid = event, None
                elif event.phase == "role":
                    oid = _integer(event.payload, "oid")
                elif event.phase in {"complete", "abandoned"}:
                    assert generation is not None
                    job, secret = application_migration_documents(self.plan, template=template, generation=generation,
                        guard=historical_guard, container_registry=self.container_registry)
                    ProtectedApplicationMigrationResources(runner=self.runner, job=job, secret=secret, guard=guard,
                        assert_guard=self._guard).require_retired()
                    if observe_application_migrator_role(peer, target=original.target, coordination_guard=coordination,
                            provisioner_role="postgres", migrator_role=application_migration_role(generation), migrator_oid=oid):
                        raise RuntimeError("application migration completed role returned")
            with peer.transaction():
                peer.execute("SET TRANSACTION READ ONLY")
                peer.execute("SET LOCAL search_path=pg_catalog,pg_temp")
                peer.execute("SET LOCAL statement_timeout='30s'")
                if peer.execute("SELECT version_num FROM public.alembic_version").fetchall() != [(self.plan.migration_target_revision,)]:
                    raise RuntimeError("application migration completed schema changed")
        if self._context(events) != (guard, inputs) or self.successor_source() != successor or self._journal().read() != tuple(events):
            raise RuntimeError("application migration completed inputs changed during observation")


def _finished(events: Sequence[ApplicationMigrationEvent]) -> bool:
    return bool(events) and (events[-1].phase == "noop" or (events[-1].phase == "complete" and events[-1].payload["successful"] is True))
