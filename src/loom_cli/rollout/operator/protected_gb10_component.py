"""Journal-ready GB10 convergence through the shared fleet classifier."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from loom_cli.rollout.gb10_convergence import (
    GB10ConvergencePlan,
    GB10ConvergenceState,
    GB10FleetCandidateObservation,
    plan_gb10_candidate_convergence,
)

from .final_gate_plan import FinalGatePlan
from .protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ProtectedApplyComponent,
)

_IMPLEMENTATION_DIGEST = hashlib.sha256(b"loom-protected-gb10-candidate-convergence-v3").hexdigest()


class ProtectedGB10FleetTransport(Protocol):
    """Fixed installed transport; it cannot choose candidate or arbitrary commands."""

    def observe(self, plan: FinalGatePlan) -> GB10FleetCandidateObservation: ...

    def apply(self, plan: FinalGatePlan, convergence: GB10ConvergencePlan) -> None: ...


EpochGuard = Callable[[FinalGatePlan], ComponentObservation]


@dataclass(frozen=True, slots=True)
class ProtectedGB10CandidateComponent:
    transport: ProtectedGB10FleetTransport
    epoch_guard: EpochGuard

    def __post_init__(self) -> None:
        if not callable(self.epoch_guard):
            raise ValueError("protected GB10 epoch authority is invalid")

    def component(self, plan: FinalGatePlan) -> ProtectedApplyComponent:
        return ProtectedApplyComponent(
            component_id="gb10-candidate",
            implementation_digest=_IMPLEMENTATION_DIGEST,
            input_fingerprint=_hash_json(
                {
                    "candidate_sha": plan.candidate_sha,
                    "candidate_tree": plan.candidate_tree,
                    "gb10_boot_ids": dict(plan.gb10_boot_ids),
                    "gb10_candidate_source_digest": plan.gb10_unit_digest,
                    "gb10_inventory_digest": plan.gb10_inventory_digest,
                    "gb10_mount_digest": plan.gb10_mount_digest,
                    "starting_epoch": plan.starting_mutation_epoch,
                }
            ),
            classify=self.classify,
            apply=self.apply,
        )

    def classify(self, plan: FinalGatePlan) -> ComponentObservation:
        epoch = self.epoch_guard(plan)
        if epoch.state is not ComponentState.EXACT:
            return self._observation(plan, ComponentState.DRIFTED, epoch, "0" * 64)
        convergence = self._convergence(plan)
        state = {
            GB10ConvergenceState.READY: ComponentState.READY,
            GB10ConvergenceState.EXACT: ComponentState.EXACT,
            GB10ConvergenceState.DRIFTED: ComponentState.DRIFTED,
        }[convergence.state]
        return self._observation(plan, state, epoch, convergence.evidence_digest)

    def apply(self, plan: FinalGatePlan) -> None:
        epoch = self.epoch_guard(plan)
        if epoch.state is not ComponentState.EXACT:
            raise RuntimeError("protected GB10 epoch ownership changed before apply")
        convergence = self._convergence(plan)
        if convergence.state is not GB10ConvergenceState.READY:
            raise RuntimeError("protected GB10 state changed before apply")
        self.transport.apply(plan, convergence)

    def _convergence(self, plan: FinalGatePlan) -> GB10ConvergencePlan:
        return plan_gb10_candidate_convergence(
            self.transport.observe(plan),
            expected_boot_ids=plan.gb10_boot_ids,
            expected_candidate_source_digest=plan.gb10_unit_digest,
        )

    @staticmethod
    def _observation(
        plan: FinalGatePlan,
        state: ComponentState,
        epoch: ComponentObservation,
        fleet_digest: str,
    ) -> ComponentObservation:
        return ComponentObservation(
            state=state,
            evidence_digest=_hash_json(
                {
                    "candidate_sha": plan.candidate_sha,
                    "candidate_tree": plan.candidate_tree,
                    "epoch_evidence_digest": epoch.evidence_digest,
                    "fleet_digest": fleet_digest,
                    "state": state.value,
                }
            ),
            observed_epoch=plan.starting_mutation_epoch + 1,
        )


def _hash_json(value: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


__all__ = ["ProtectedGB10CandidateComponent", "ProtectedGB10FleetTransport"]
