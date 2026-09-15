"""Retained executor SQL admission after the original completed capacity bootstrap."""

from __future__ import annotations

import hashlib
import secrets
import time
from dataclasses import dataclass, replace

from loom.application_database_connection import ApplicationDatabaseConnection
from loom.application_executor_admission import (
    admit_sealed_executor,
    issue_executor_admission,
    require_issued_executor,
)
from loom.application_schema_inventory import read_application_schema_inventory
from loom.application_schema_reference import (
    ApplicationSchemaProfile,
    require_application_schema_reference,
)
from loom_capacity_manager.contracts import SubjectConfigurationV1

from .final_gate_plan import FinalGatePlan
from .protected_application_admission_recovery import (
    ApplicationAdmissionRecoveryRecord,
    admission_record_digest,
)
from .protected_application_handoff_component import _admit_sql_profiles
from .protected_application_owner_preparation import APPLICATION_OWNER_ROLE, _observe
from .protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ComponentTerminal,
    ProtectedApplyComponent,
    ProtectedApplyJournalError,
)
from .protected_capacity_bootstrap_component import ProtectedCapacityBootstrapComponent
from .protected_controller_admission import (
    ControllerAdmissionBundle,
    build_controller_admission_bundle,
)
from .protected_executor_admission_journal import (
    EXECUTOR_ADMISSION_COMPONENT_ID,
    ExecutorAdmissionJournal,
    ExecutorAdmissionRecord,
)
from .staging_mutation_guard import MutationGuardEvidence


@dataclass(frozen=True, slots=True)
class ProtectedExecutorAdmissionComponent:
    bootstrap: ProtectedCapacityBootstrapComponent
    ordinal: int

    def __post_init__(self) -> None:
        if type(self.ordinal) is not int or not self.bootstrap.ordinal < self.ordinal < 32:
            raise ValueError("executor admission must follow its original bootstrap")

    def component(self) -> ProtectedApplyComponent:
        return ProtectedApplyComponent(EXECUTOR_ADMISSION_COMPONENT_ID,
            hashlib.sha256(b"loom-protected-executor-admission-v1").hexdigest(),
            admission_record_digest({"plan_digest": self.bootstrap.plan.plan_digest, "ordinal": self.ordinal,
                "bootstrap_intent": self.bootstrap._journal().intent.intent_digest}), self.classify, self.apply)

    def _journal(self) -> ExecutorAdmissionJournal:
        return ExecutorAdmissionJournal(self.bootstrap.journal, self.bootstrap.plan, self.component(), self.ordinal)

    def completed_bootstrap(self) -> ProtectedCapacityBootstrapComponent:
        return replace(self.bootstrap, executor_admission_source=self._journal().read)

    def _terminal(self, record: ExecutorAdmissionRecord) -> ComponentTerminal | None:
        saved = self._journal()
        try:
            terminal = ComponentTerminal.from_dict(self.bootstrap.journal._read(saved.root / "terminal.json"))
        except FileNotFoundError:
            return None
        if (saved.read() != (record, True) or terminal.intent_digest != saved.intent.intent_digest
                or terminal.component_id != EXECUTOR_ADMISSION_COMPONENT_ID
                or terminal.observed_epoch != self.bootstrap.plan.starting_mutation_epoch + 1
                or terminal.evidence_digest != self._evidence(record)):
            raise RuntimeError("executor admission terminal lacks exact issued authority")
        from .protected_application_guard_retention import _read_pending_retention
        if _read_pending_retention(self.bootstrap.journal.attempt_root.parents[3],
                request_id=self.bootstrap.plan.request_id, service_uid=self.bootstrap.journal.service_uid,
                require_record=True, component_id=EXECUTOR_ADMISSION_COMPONENT_ID) is not None:
            raise RuntimeError("executor admission completed guard retention is still pending")
        self.bootstrap.journal._sync_application_recovery(saved.root, "terminal.json")
        return terminal

    def _context(self, plan: FinalGatePlan) -> tuple[MutationGuardEvidence, str]:
        if plan != self.bootstrap.plan:
            raise RuntimeError("executor admission original plan changed")
        bootstrap = self.completed_bootstrap()
        events = bootstrap._journal().read()
        terminal = bootstrap._terminal(events)
        if terminal is None:
            raise RuntimeError("executor admission original bootstrap is not complete")
        guard, inputs = bootstrap._context(events)
        saved = self._journal().read()
        if saved is not None:
            record, _ = saved
            if (record.bootstrap_terminal_digest != terminal.terminal_digest
                    or record.bootstrap_event_digest != events[-1].event_digest
                    or record.seed_digest != bootstrap._seed_digest()
                    or (self._terminal(record) is None and (record.inputs_digest != inputs.digest() or record.guard != guard))):
                raise RuntimeError("executor admission original bootstrap inputs or guard changed")
        if bootstrap.classify(plan).state is not ComponentState.EXACT or bootstrap._guard() != guard:
            raise RuntimeError("executor admission completed bootstrap observation changed")
        return guard, inputs.digest()

    def classify(self, plan: FinalGatePlan) -> ComponentObservation:
        guard, inputs_digest = self._context(plan)
        saved = self._journal().read()
        if saved is None or not saved[1]:
            return ComponentObservation(ComponentState.READY,
                admission_record_digest({"inputs_digest": inputs_digest, "guard_digest": guard.evidence_digest,
                    "retained_admission": None if saved is None else saved[0].digest}),
                plan.starting_mutation_epoch + 1)
        return ComponentObservation(ComponentState.EXACT, self._evidence(saved[0]), plan.starting_mutation_epoch + 1)

    def _evidence(self, record: ExecutorAdmissionRecord) -> str:
        return admission_record_digest({"plan_digest": self.bootstrap.plan.plan_digest,
            "admission_digest": record.digest, "profile": "cnpg-staging-executor-admission"})

    def controller_admission(self, plan: FinalGatePlan, *, subject: SubjectConfigurationV1,
                             state_directory: str, protected_admission_sha256: str) -> ControllerAdmissionBundle:
        """Read completed issuance and current bound CA without issuing or rotating."""
        context = self._context(plan)
        saved = self._journal().read()
        if saved is None or not saved[1] or self._terminal(saved[0]) is None:
            raise RuntimeError("controller admission requires completed executor issuance")
        if self.classify(plan).state is not ComponentState.EXACT:
            raise RuntimeError("controller admission executor authority is not exact")
        inputs = self.bootstrap.inputs_source()
        seed = self.bootstrap.seed_source()
        if (inputs.digest() != context[1]
                or seed.get("subject_id") != str(subject.subject_id)
                or seed.get("subject_incarnation") != str(subject.subject_incarnation)
                or seed.get("reporter_incarnation") != str(subject.demand_reporter_incarnation)):
            raise RuntimeError("controller admission source binding changed")
        bundle = build_controller_admission_bundle(saved[0], subject=subject,
            state_directory=state_directory, protected_admission_sha256=protected_admission_sha256,
            ca_certificate=inputs.ca.certificate)
        if (self._context(plan) != context or self._journal().read() != saved
                or self._terminal(saved[0]) is None or self.bootstrap.inputs_source() != inputs
                or self.bootstrap.seed_source() != seed):
            raise RuntimeError("controller admission source changed during derivation")
        return bundle

    def _retain(self, guard: MutationGuardEvidence) -> None:
        journal, plan = self.bootstrap.journal, self.bootstrap.plan
        journal.retain_application_guard(plan, guard=guard)
        deadline = time.monotonic() + 35
        while True:
            if self.bootstrap._guard() != guard:
                raise RuntimeError("executor admission guard changed during retention")
            try:
                journal.require_application_guard_retained(plan, guard=guard)
                return
            except ProtectedApplyJournalError as exc:
                if str(exc) != "application guard acknowledgement is absent" or time.monotonic() >= deadline:
                    raise
                time.sleep(0.1)

    def apply(self, plan: FinalGatePlan) -> None:
        guard, inputs_digest = self._context(plan)
        self._retain(guard)
        journal = self._journal()
        saved = journal.read()
        if saved is None:
            bootstrap = self.bootstrap
            events = bootstrap._journal().read()
            terminal = bootstrap._terminal(events)
            assert terminal is not None
            view, _ = bootstrap._handoff()
            assert view.admission is not None
            with bootstrap.runner.open_staging_peer_database() as peer:
                _admit_sql_profiles(bootstrap.runner, peer, guard, separated_owner=True)
                backend, coordination = _observe(peer, guard, owner_role=APPLICATION_OWNER_ROLE)
                require_executor_schema(peer, issued=False)
                identity = admit_sealed_executor(peer, target=view.admission.target,
                    coordination_guard=coordination, provisioner_role="postgres")
                record = ExecutorAdmissionRecord(
                    ApplicationAdmissionRecoveryRecord(journal.intent.intent_digest, view.admission.target, backend, coordination),
                    guard, terminal.terminal_digest, events[-1].event_digest, inputs_digest, bootstrap._seed_digest(),
                    identity, secrets.token_urlsafe(48))
                if self._context(plan) != (guard, inputs_digest):
                    raise RuntimeError("executor admission authority changed before retention")
                journal.retain(record, guard=guard)
        else:
            record, _ = saved
        assert record.admission.coordination_guard is not None
        if self._context(plan) != (guard, inputs_digest):
            raise RuntimeError("executor admission retained inputs changed before issuance")
        with self.bootstrap.runner.open_staging_peer_database() as peer:
            _admit_sql_profiles(self.bootstrap.runner, peer, guard, separated_owner=True)
            self.bootstrap.journal.require_application_guard_retained(plan, guard=guard)
            issue_executor_admission(peer, target=record.admission.target,
                coordination_guard=record.admission.coordination_guard, provisioner_role="postgres",
                identity=record.identity, password=record.password)
            require_issued_executor(peer, target=record.admission.target,
                coordination_guard=record.admission.coordination_guard, provisioner_role="postgres",
                identity=record.identity, password=record.password)
            require_executor_schema(peer, issued=True)
            if self._context(plan) != (guard, inputs_digest):
                raise RuntimeError("executor admission inputs changed after issuance")
            journal.mark_issued(record, guard=guard)


def require_executor_schema(connection: ApplicationDatabaseConnection, *, issued: bool) -> None:
    profile: ApplicationSchemaProfile = "cnpg-staging-executor-admission" if issued else "cnpg-staging-sealed-owner"
    with connection.transaction():
        connection.execute("SET TRANSACTION READ ONLY")
        connection.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        observed = read_application_schema_inventory(connection, role_bindings={
            "loom": "application-runtime", APPLICATION_OWNER_ROLE: "application-owner", "postgres": "provisioner",
            **{f"loom_cap_staging_{role}": f"guard-{role}" for role in ("owner", "migrator", "agent", "executor", "observer", "runtime")},
        })
        require_application_schema_reference(observed, profile=profile, revision="0146/guard_0034")
