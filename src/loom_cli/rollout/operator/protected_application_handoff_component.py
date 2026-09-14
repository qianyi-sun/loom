"""Compose the application handoff under original guard and external writer admission.

The enclosing installed operator supplies supervised guard/epoch readers and
fresh external operator/process/volume admission. Those are mandatory trusted
capabilities, not user-selected commands or a caller completion boolean. This
component itself observes and binds the primary, inputs, SQL profiles and actual
restoration/retired fences before a component terminal can be published.
"""

from __future__ import annotations

import hashlib
import re
import time
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass, replace
from typing import Protocol

from loom.application_database_connection import ApplicationDatabaseConnection

from .final_gate_plan import FinalGatePlan
from .protected_application_admission_recovery import (
    ApplicationAdmissionRecoveryRecord,
    admission_record_digest,
)
from .protected_application_credential_recovery import (
    ApplicationCredentialObservation,
    observe_application_runtime_credential,
)
from .protected_application_owner_preparation import _observe
from .protected_application_restoration import (
    ApplicationRestorationRunner,
    observe_application_restoration,
)
from .protected_application_workload_runtime import (
    pause_application_workloads,
    restore_application_workloads,
)
from .protected_apply_journal import (
    ApplicationRecoveryView,
    ComponentObservation,
    ComponentState,
    ProtectedApplyComponent,
    ProtectedApplyJournal,
    ProtectedApplyJournalError,
)
from .protected_cnpg_fence_acquisition import acquire_cnpg_input_fence
from .protected_cnpg_fence_retirement import (
    CNPGFenceRetirementRunner,
    observe_application_cnpg_fence_retirement,
    retire_cnpg_input_fence,
)
from .protected_cnpg_manager_replacement import CNPGManagerReplacementReceipt
from .protected_cnpg_runtime_admission import (
    CNPGPrimaryRuntime,
    observe_cnpg_primary_runtime,
    reconcile_cnpg_primary_runtime,
)
from .protected_cnpg_sql_admission import require_cnpg_effective_sql_profile
from .protected_cnpg_writer_configuration import _json, _mapping
from .staging_mutation_guard import MutationGuardEvidence

_COMPONENT = 'application-ownership-handoff'
_IMPLEMENTATION = hashlib.sha256(b'loom-application-authority-handoff-v1').hexdigest()


class ApplicationHandoffRunner(CNPGFenceRetirementRunner, ApplicationRestorationRunner, Protocol):
    def open_staging_peer_template_database(self) -> AbstractContextManager[ApplicationDatabaseConnection]: ...

    def prepare_staging_application_database(
        self, plan: FinalGatePlan, *, journal: ProtectedApplyJournal, connection: ApplicationDatabaseConnection,
        guard: MutationGuardEvidence,
    ) -> ApplicationAdmissionRecoveryRecord: ...

    def recover_staging_peer_database(
        self, plan: FinalGatePlan, *, journal: ProtectedApplyJournal, ordinal: int, runtime_password: str | None = None,
    ) -> AbstractContextManager[ApplicationDatabaseConnection]: ...

    def issue_staging_manager_replacement(
        self, *, journal: ProtectedApplyJournal, runtime_password: str | None = None,
    ) -> bool: ...


def _admit_sql_profiles(
    runner: ApplicationHandoffRunner, peer: ApplicationDatabaseConnection, guard: MutationGuardEvidence,
) -> None:
    original, _ = _observe(peer, guard)
    require_cnpg_effective_sql_profile(peer, database='loom', original=original)
    with runner.open_staging_peer_maintenance_database() as maintenance:
        require_cnpg_effective_sql_profile(maintenance, database='postgres', original=original)
    with runner.open_staging_peer_template_database() as template:
        require_cnpg_effective_sql_profile(template, database='template1', original=original)


@dataclass(frozen=True, slots=True)
class ProtectedApplicationAuthorityHandoffComponent:
    journal: ProtectedApplyJournal
    runner: ApplicationHandoffRunner
    ordinal: int
    guard_source: Callable[[FinalGatePlan], MutationGuardEvidence]
    epoch_source: Callable[[FinalGatePlan], int]
    observe_external_authority: Callable[[FinalGatePlan, CNPGPrimaryRuntime], str]

    def __post_init__(self) -> None:
        if (type(self.ordinal) is not int or not 1 <= self.ordinal < 32
                or not all(callable(value) for value in (self.guard_source, self.epoch_source, self.observe_external_authority))):
            raise ValueError('application handoff enclosing authority is invalid')

    def component(self, plan: FinalGatePlan) -> ProtectedApplyComponent:
        if plan.namespace != 'loom-staging' or plan.checkpoint_schema_version != 3:
            raise ValueError('application handoff requires its staging schema-3 checkpoint')
        return ProtectedApplyComponent(_COMPONENT, _IMPLEMENTATION,
            admission_record_digest({'plan_digest': plan.plan_digest, 'ordinal': self.ordinal}),
            self.classify, self.apply)

    def _guard(self, plan: FinalGatePlan) -> MutationGuardEvidence:
        guard = self.guard_source(plan)
        epoch = self.epoch_source(plan)
        if (type(guard) is not MutationGuardEvidence or guard.state != 'ready'
                or guard.request_id != plan.request_id or guard.candidate_sha != plan.candidate_sha
                or guard.candidate_tree != plan.candidate_tree or guard.mutation_epoch != plan.starting_mutation_epoch
                or type(epoch) is not int or epoch != plan.starting_mutation_epoch + 1):
            raise RuntimeError('application handoff original guard or claimed epoch changed')
        return guard

    def _inputs(
        self, plan: FinalGatePlan, view: ApplicationRecoveryView | None,
    ) -> tuple[ApplicationCredentialObservation, CNPGPrimaryRuntime, str]:
        credential = observe_application_runtime_credential(plan, runner=self.runner, service_uid=self.journal.service_uid)
        if view is not None and view.cnpg_runtime is not None:
            name = view.cnpg_runtime.manager.pod_name
        else:
            cluster = _json(self.runner.capture_stdout(
                ('kubectl', '--namespace', 'loom-staging', 'get', 'cluster.postgresql.cnpg.io/loom-postgres',
                 '--output=json', '--request-timeout=30s'), env=self.runner.environment, timeout_seconds=30))
            name = str(_mapping(cluster.get('status')).get('currentPrimary'))
        runtime = observe_cnpg_primary_runtime(self.runner, cluster_uid=credential.configuration.cluster_uid, pod_name=name)
        external = self.observe_external_authority(plan, runtime)
        if not isinstance(external, str) or re.fullmatch('[0-9a-f]{64}', external) is None:
            raise RuntimeError('application handoff external authority evidence is invalid')
        if view is not None:
            for saved, current in ((view.credential_binding, credential.binding),
                                   (view.cnpg_configuration, credential.configuration),
                                   (view.external_authority_sha256, external)):
                if saved is not None and saved != current:
                    raise RuntimeError('application handoff original inputs changed')
            original = view.cnpg_runtime
            if original is None:
                if view.admission is not None or view.owner_creations or view.manager_replacement is not None:
                    raise RuntimeError('application handoff mutation lacks original runtime')
            else:
                if replace(runtime, manager=original.manager) != original:
                    raise RuntimeError('application handoff original postmaster or Pod changed')
                replacement = view.manager_replacement
                if runtime.manager != original.manager:
                    if replacement is None or not replacement[1]:
                        raise RuntimeError('application handoff manager changed without dispatch')
                    receipt = CNPGManagerReplacementReceipt.validate(replacement[0], runtime.manager)
                    if replacement[2] is not None and replacement[2] != receipt:
                        raise RuntimeError('application handoff acknowledged manager changed')
                elif replacement is not None and replacement[2] is not None:
                    raise RuntimeError('application handoff original manager returned after replacement')
        return credential, runtime, external

    def classify(self, plan: FinalGatePlan) -> ComponentObservation:
        guard = self._guard(plan)
        component = self.component(plan)
        view = self.journal.read_application_recovery_view(plan, component, ordinal=self.ordinal)
        credential, runtime, external = self._inputs(plan, view)
        if self._guard(plan) != guard:
            raise RuntimeError('application handoff original guard changed during classification')
        if view is not None and view.fences_retiring:
            restoration = observe_application_restoration(plan, view=view, runner=self.runner, guard=guard,
                                                          service_uid=self.journal.service_uid)
            retirement = observe_application_cnpg_fence_retirement(plan, journal=self.journal, component=component,
                                                                  ordinal=self.ordinal, runner=self.runner)
            if self._guard(plan) != guard:
                raise RuntimeError('application handoff original guard changed during classification')
            return ComponentObservation(ComponentState.EXACT, admission_record_digest({
                'restoration': restoration.digest, 'retirement': retirement, 'external': external,
            }), plan.starting_mutation_epoch + 1)
        return ComponentObservation(ComponentState.READY, admission_record_digest({
            'runtime': runtime.to_dict(), 'credential': asdict(credential.binding),
            'configuration': asdict(credential.configuration), 'external': external,
        }), plan.starting_mutation_epoch + 1)

    def _retain(self, plan: FinalGatePlan, guard: MutationGuardEvidence) -> None:
        self.journal.retain_application_guard(plan, guard=guard)
        deadline = time.monotonic() + 35
        while True:
            if self._guard(plan) != guard:
                raise RuntimeError('application handoff cannot replace its original guard')
            try:
                self.journal.require_application_guard_retained(plan, guard=guard)
                return
            except ProtectedApplyJournalError as exc:
                if str(exc) != 'application guard acknowledgement is absent' or time.monotonic() >= deadline:
                    raise
            time.sleep(0.1)

    def _checkpoint(self, plan: FinalGatePlan, guard: MutationGuardEvidence) -> None:
        if self._guard(plan) != guard:
            raise RuntimeError('application handoff original guard changed between phases')
        self._inputs(plan, self.journal.read_active_application_recovery_view(plan))
        if self._guard(plan) != guard:
            raise RuntimeError('application handoff original guard changed during observation')

    def apply(self, plan: FinalGatePlan) -> None:
        view = self.journal.read_active_application_recovery_view(plan)
        original = self.journal.read_application_recovery_view(plan, self.component(plan), ordinal=self.ordinal)
        if original is None or view.intent != original.intent:
            raise RuntimeError('application handoff active intent changed')
        guard = self._guard(plan)
        credential, runtime, external = self._inputs(plan, view)
        self._retain(plan, guard)
        if view.cnpg_runtime is None:
            self.journal.record_application_cnpg_runtime(plan, runtime=runtime)
        self.journal.record_application_cnpg_configuration(plan, binding=credential.configuration)
        self.journal.record_application_credential_recovery(plan, binding=credential.binding)
        self.journal.record_application_external_authority(plan, sha256=external)
        if not view.fences_retiring:
            self.journal.prepare_application_cnpg_fence(plan, target_pooler_names=())
            acquire_cnpg_input_fence(plan, journal=self.journal, runner=self.runner)
        self._checkpoint(plan, guard)
        if not view.workloads_restoring:
            if view.admission is None:
                with self.runner.open_staging_peer_database() as peer:
                    _admit_sql_profiles(self.runner, peer, guard)
                    self.runner.prepare_staging_application_database(plan, journal=self.journal, connection=peer, guard=guard)
            self._checkpoint(plan, guard)
            pause_application_workloads(plan, journal=self.journal, runner=self.runner, guard=guard)
            self._checkpoint(plan, guard)
            if self.journal.read_application_manager_replacement() is None:
                original_runtime = self.journal.read_application_cnpg_runtime(plan)
                assert original_runtime is not None
                self.journal.prepare_application_manager_replacement(identity=original_runtime.manager)
            history = self.journal.read_application_handoff_recoveries()
            ordinal = history[-1][0].ordinal if history and history[-1][1] is None else len(history) + 1
            with self.runner.recover_staging_peer_database(plan, journal=self.journal, ordinal=ordinal,
                                                          runtime_password=credential.credential.password) as peer:
                _admit_sql_profiles(self.runner, peer, guard)
                self._checkpoint(plan, guard)
                replacement = self.journal.read_application_manager_replacement()
                assert replacement is not None
                if not replacement[1]:
                    self.runner.issue_staging_manager_replacement(journal=self.journal,
                                                                  runtime_password=credential.credential.password)
                deadline = time.monotonic() + 35
                while True:
                    if self._guard(plan) != guard:
                        raise RuntimeError('application handoff original guard changed during replacement')
                    reconcile_cnpg_primary_runtime(plan, journal=self.journal, runner=self.runner)
                    replacement = self.journal.read_application_manager_replacement()
                    assert replacement is not None
                    if replacement[2] is not None:
                        break
                    if time.monotonic() >= deadline:
                        raise RuntimeError('application handoff manager dispatch is still ambiguous')
                    time.sleep(0.1)
                _admit_sql_profiles(self.runner, peer, guard)
        self._checkpoint(plan, guard)
        if not view.fences_retiring:
            restore_application_workloads(plan, journal=self.journal, runner=self.runner, guard=guard)
        self._checkpoint(plan, guard)
        retire_cnpg_input_fence(plan, journal=self.journal, runner=self.runner, guard=guard)
