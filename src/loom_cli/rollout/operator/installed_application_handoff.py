"""Installed original-guard/epoch/process/storage composition for application handoff.

The rollout operator must hold the separately coordinated issue-scoped maintenance
and privileged-writer exclusion throughout this operation. These are real machine
observations under that authority; neither inventory hashes nor journal records
establish the administrator coordination by themselves.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from typing import Protocol

from .config import OperatorConfig
from .final_gate_plan import FinalGatePlan
from .policy import sanitized_child_environment
from .protected_application_guard_retention import _read_pending_retention
from .protected_application_handoff_component import (
    ApplicationHandoffRunner,
    ProtectedApplicationAuthorityHandoffComponent,
)
from .protected_apply_journal import ComponentState, ProtectedApplyComponent, ProtectedApplyJournal
from .protected_cnpg_external_admission import CNPGExternalRunner, observe_cnpg_external_inputs
from .protected_epoch_component import KubernetesProtectedEpochComponent
from .staging_mutation_guard import MutationGuardEvidence, MutationGuardManager
from .systemd import SystemdUserManager


class InstalledApplicationHandoffRunner(ApplicationHandoffRunner, CNPGExternalRunner, Protocol):
    pass


@dataclass(frozen=True, slots=True)
class InstalledApplicationHandoffFactory:
    config: OperatorConfig
    service_uid: int
    runner: InstalledApplicationHandoffRunner
    manager: MutationGuardManager = field(init=False)

    def __post_init__(self) -> None:
        if (self.config.environment != 'staging' or self.config.namespace != 'loom-staging'
                or type(self.service_uid) is not int or self.service_uid < 0):
            raise ValueError('installed application handoff scope is invalid')
        systemd = SystemdUserManager(self.config, service_uid=self.service_uid, run=self._systemd_query)
        object.__setattr__(self, 'manager', MutationGuardManager(
            config=self.config, service_uid=self.service_uid, systemd=systemd))

    def _systemd_query(self, argv: list[str]) -> subprocess.CompletedProcess[str]:
        if argv[:3] != ['systemctl', '--user', 'show']:
            raise ValueError('installed handoff permits only guard supervision reads')
        return subprocess.run(['/usr/bin/systemctl', *argv[1:]], capture_output=True, check=False, text=True,
            timeout=30, env=sanitized_child_environment(self.config, service_uid=self.service_uid))

    def guard(self, plan: FinalGatePlan) -> MutationGuardEvidence:
        guard = self.manager.assert_ready(plan.request_id, candidate_config=self.config)
        if (guard.request_id != plan.request_id or guard.candidate_sha != plan.candidate_sha
                or guard.candidate_tree != plan.candidate_tree or guard.mutation_epoch != plan.starting_mutation_epoch
                or guard.state != 'ready'):
            raise ValueError('installed handoff original guard identity changed')
        return guard

    def epoch(self, plan: FinalGatePlan) -> int:
        guard = self.guard(plan)
        pending = _read_pending_retention(self.config.state_root, request_id=plan.request_id,
                                          service_uid=self.service_uid, guard=guard)
        if pending is not None and pending.acknowledged:
            epoch = self.manager.observe_retained_epoch(guard, candidate_config=self.config)
        else:
            # Before ACK, this component cannot yet seal/close admission. After
            # terminal, login and admission have already been freshly restored.
            # Only the acknowledged pending interval requires the existing peer.
            observed = KubernetesProtectedEpochComponent(runner=self.runner, environment=self.runner.environment).classify(plan)
            if observed.state is not ComponentState.EXACT:
                raise ValueError('installed handoff epoch claim identity is not exact')
            epoch = observed.observed_epoch
        if type(epoch) is not int or epoch != plan.starting_mutation_epoch + 1:
            raise ValueError('installed handoff original claimed epoch changed')
        if self.guard(plan) != guard:
            raise ValueError('installed handoff original guard changed during epoch observation')
        return epoch

    def __call__(
        self, plan: FinalGatePlan, *, journal: ProtectedApplyJournal, ordinal: int,
    ) -> ProtectedApplyComponent:
        if (journal.request_id != plan.request_id or journal.attempt_number != plan.attempt_number
                or journal.service_uid != self.service_uid
                or journal.attempt_root != self.config.state_root / 'requests' / plan.request_id / 'attempts' / str(plan.attempt_number)):
            raise ValueError('installed handoff original journal changed')
        return ProtectedApplicationAuthorityHandoffComponent(journal=journal, runner=self.runner, ordinal=ordinal,
            guard_source=self.guard, epoch_source=self.epoch,
            observe_external_authority=lambda candidate, runtime: observe_cnpg_external_inputs(
                candidate, runtime, runner=self.runner)).component(plan)
