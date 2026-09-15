"""Installed migration sources share the admitted handoff and retained guard."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace

from .final_gate_plan import FinalGatePlan
from .installed_application_handoff import InstalledApplicationHandoffFactory
from .protected_application_guard_retention import _read_pending_retention
from .protected_application_handoff_history import (
    CompletedApplicationHandoffOrigin,
    select_completed_handoff,
)
from .protected_application_migration_ca import observe_application_migration_ca
from .protected_application_migration_component import (
    ApplicationMigrationInputs,
    ProtectedApplicationMigrationComponent,
)
from .protected_application_migration_journal import ApplicationMigrationEvent
from .protected_application_owner_preparation import APPLICATION_OWNER_ROLE
from .protected_apply_journal import (
    ApplicationRecoveryView,
    ComponentState,
    ComponentTerminal,
    ProtectedApplyComponent,
    ProtectedApplyJournal,
)
from .protected_capacity_bootstrap_component import ProtectedCapacityBootstrapComponent
from .protected_epoch_component import KubernetesProtectedEpochComponent
from .protected_executor_admission_component import ProtectedExecutorAdmissionComponent
from .protected_staging_capacity_database_component import (
    KubernetesProtectedStagingCapacityDatabaseComponent,
)


@dataclass(frozen=True, slots=True)
class InstalledApplicationMigrationFactory:
    handoff: InstalledApplicationHandoffFactory
    container_registry: str

    def new_journal(self, plan: FinalGatePlan) -> ProtectedApplyJournal:
        return ProtectedApplyJournal(self.handoff.config.state_root, request_id=plan.request_id,
            attempt_number=plan.attempt_number, service_uid=self.handoff.service_uid)

    def components(self, plan: FinalGatePlan, *, journal: ProtectedApplyJournal,
                   ordinal: int) -> tuple[ProtectedApplyComponent, ...]:
        """Construct the identical ordered pair without opening a database peer."""
        if ordinal != 2:
            raise ValueError("installed application handoff ordinal changed")
        origin = self._origin(plan)
        ownership = (self.handoff(plan, journal=journal, ordinal=ordinal) if origin is None
            else self.handoff.historical_component(plan, journal=journal, origin=origin))
        return (ownership, self(plan, journal=journal, ordinal=ordinal + 1, handoff_ordinal=ordinal))

    def _origin(self, plan: FinalGatePlan) -> CompletedApplicationHandoffOrigin | None:
        return select_completed_handoff(plan, state_root=self.handoff.config.state_root,
            service_uid=self.handoff.service_uid, build_component=self.handoff)

    def epoch(self, plan: FinalGatePlan) -> int:
        guard = self.handoff.completed_guard(plan)
        pending = _read_pending_retention(self.handoff.config.state_root, request_id=plan.request_id,
            service_uid=self.handoff.service_uid, guard=guard)
        if pending is not None and pending.acknowledged:
            epoch = self.handoff.manager.observe_retained_epoch(guard, candidate_config=self.handoff.config)
        else:
            observed = KubernetesProtectedEpochComponent(runner=self.handoff.runner,
                environment=self.handoff.runner.environment).classify(plan)
            if observed.state is not ComponentState.EXACT:
                raise RuntimeError("installed application migration epoch is not exact")
            epoch = observed.observed_epoch
        if type(epoch) is not int or epoch != plan.starting_mutation_epoch + 1 or self.handoff.completed_guard(plan) != guard:
            raise RuntimeError("installed application migration guard or epoch changed")
        return epoch

    def __call__(self, plan: FinalGatePlan, *, journal: ProtectedApplyJournal, ordinal: int,
                 handoff_ordinal: int) -> ProtectedApplyComponent:
        return self.build(plan, journal=journal, ordinal=ordinal, handoff_ordinal=handoff_ordinal).component()

    def build(self, plan: FinalGatePlan, *, journal: ProtectedApplyJournal, ordinal: int,
              handoff_ordinal: int) -> ProtectedApplicationMigrationComponent:
        if ordinal != handoff_ordinal + 1:
            raise ValueError("installed application migration must directly follow its handoff")
        handoff = self.handoff.build(plan, journal=journal, ordinal=handoff_ordinal)
        origin = self._origin(plan)
        component = (handoff.component(plan) if origin is None
            else self.handoff.historical_component(plan, journal=journal, origin=origin))
        def history() -> tuple[ApplicationRecoveryView, ComponentTerminal]:
            if origin is not None:
                return origin.admitted_for(plan, journal=journal, component=component)
            terminal = journal.read_application_handoff_terminal(plan, component, ordinal=handoff_ordinal)
            view = journal.read_application_recovery_view(plan, component, ordinal=handoff_ordinal)
            if terminal is None or view is None:
                raise RuntimeError("installed application migration handoff has not completed")
            return view, terminal
        def inputs() -> ApplicationMigrationInputs:
            credential, runtime, external = handoff._inputs(plan, None)
            ca = observe_application_migration_ca(self.handoff.runner, cluster_uid=runtime.cluster_uid)
            return ApplicationMigrationInputs(credential, runtime, external, ca)
        return ProtectedApplicationMigrationComponent(plan=plan, journal=journal, runner=self.handoff.runner,
            ordinal=ordinal, guard_source=lambda: self.handoff.completed_guard(plan), epoch_source=lambda: self.epoch(plan),
            inputs_source=inputs, handoff_source=history,
            successor_source=lambda: self.handoff.successor_source(plan, journal), container_registry=self.container_registry,
            handoff_plan_source=None if origin is None else lambda: origin.plan)

    def capacity(self, plan: FinalGatePlan, *, journal: ProtectedApplyJournal, ordinal: int,
                 handoff_ordinal: int, base: KubernetesProtectedStagingCapacityDatabaseComponent,
                 seed_source: Callable[[], Mapping[str, object]]) -> ProtectedApplyComponent:
        bootstrap = self.build_capacity(plan, journal=journal, ordinal=ordinal, handoff_ordinal=handoff_ordinal,
            base=base, seed_source=seed_source)
        if plan.schema_version == 7:
            return ProtectedExecutorAdmissionComponent(bootstrap, 14).completed_bootstrap().component()
        return bootstrap.component()

    def executor_admission(self, plan: FinalGatePlan, *, journal: ProtectedApplyJournal,
                           base: KubernetesProtectedStagingCapacityDatabaseComponent,
                           seed_source: Callable[[], Mapping[str, object]]) -> ProtectedApplyComponent:
        if plan.schema_version != 7:
            raise ValueError("installed executor admission requires execution preparation")
        bootstrap = self.build_capacity(plan, journal=journal, ordinal=5, handoff_ordinal=2,
            base=base, seed_source=seed_source)
        return ProtectedExecutorAdmissionComponent(bootstrap, 14).component()

    def build_capacity(self, plan: FinalGatePlan, *, journal: ProtectedApplyJournal, ordinal: int,
                       handoff_ordinal: int, base: KubernetesProtectedStagingCapacityDatabaseComponent,
                       seed_source: Callable[[], Mapping[str, object]]) -> ProtectedCapacityBootstrapComponent:
        # Ownership, migration, credential seed, then database bootstrap. Keep
        # this original ordinal for early recovery as well as ordinary apply.
        if handoff_ordinal != 2 or ordinal != handoff_ordinal + 3:
            raise ValueError("installed application capacity ordinal changed")
        migration = self.build(plan, journal=journal, ordinal=handoff_ordinal + 1, handoff_ordinal=handoff_ordinal)
        def history() -> tuple[tuple[ApplicationMigrationEvent, ...], ComponentTerminal]:
            events = migration._journal().read()
            terminal = migration._terminal(events)
            if terminal is None:
                raise RuntimeError("installed application capacity migration has not completed")
            return events, terminal
        return ProtectedCapacityBootstrapComponent(plan=plan, journal=journal, runner=self.handoff.runner,
            ordinal=ordinal, guard_source=migration.guard_source, epoch_source=migration.epoch_source,
            inputs_source=migration.inputs_source, handoff_source=migration.handoff_source,
            successor_source=migration.successor_source, container_registry=self.container_registry,
            base=replace(base, application_owner_role=APPLICATION_OWNER_ROLE), seed_source=seed_source,
            migration_source=history, handoff_plan_source=migration.handoff_plan_source)
