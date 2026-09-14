"""Installed migration sources share the admitted handoff and retained guard."""

from __future__ import annotations

from dataclasses import dataclass

from .final_gate_plan import FinalGatePlan
from .installed_application_handoff import InstalledApplicationHandoffFactory
from .protected_application_guard_retention import _read_pending_retention
from .protected_application_migration_ca import observe_application_migration_ca
from .protected_application_migration_component import (
    ApplicationMigrationInputs,
    ProtectedApplicationMigrationComponent,
)
from .protected_apply_journal import (
    ApplicationRecoveryView,
    ComponentState,
    ComponentTerminal,
    ProtectedApplyComponent,
    ProtectedApplyJournal,
)
from .protected_epoch_component import KubernetesProtectedEpochComponent


@dataclass(frozen=True, slots=True)
class InstalledApplicationMigrationFactory:
    handoff: InstalledApplicationHandoffFactory
    container_registry: str

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
        if ordinal != handoff_ordinal + 1:
            raise ValueError("installed application migration must directly follow its handoff")
        handoff = self.handoff.build(plan, journal=journal, ordinal=handoff_ordinal)
        component = handoff.component(plan)
        def history() -> tuple[ApplicationRecoveryView, ComponentTerminal]:
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
            successor_source=lambda: self.handoff.successor_source(plan, journal), container_registry=self.container_registry).component()
