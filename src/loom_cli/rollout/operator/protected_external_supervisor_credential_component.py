"""Journal-ready controller-local external-supervisor credential convergence."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass

from .final_gate_plan import FinalGatePlan
from .protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ProtectedApplyComponent,
)
from .protected_external_supervisor_credential_transport import (
    ExternalSupervisorCredentialEvidence,
    ProtectedExternalSupervisorCredentialTransport,
)

_IMPLEMENTATION_DIGEST = hashlib.sha256(
    b"loom-protected-external-supervisor-credential-v2"
).hexdigest()
_COMPONENT_IDS = {
    "gx10-01c7": "external-supervisor-credential-gb10",
    "TRT-EAI-OLDLAB-1": "external-supervisor-credential-oldlab",
}

EpochGuard = Callable[[FinalGatePlan], ComponentObservation]


@dataclass(frozen=True, slots=True)
class ProtectedExternalSupervisorCredentialComponent:
    transport: ProtectedExternalSupervisorCredentialTransport
    epoch_guard: EpochGuard
    execution_host: str
    service_uid: int
    service_gid: int

    def __post_init__(self) -> None:
        if (
            self.execution_host not in _COMPONENT_IDS
            or self.service_uid < 0
            or self.service_gid < 0
            or not callable(self.epoch_guard)
        ):
            raise ValueError("protected external supervisor credential authority is invalid")

    def component(self, plan: FinalGatePlan) -> ProtectedApplyComponent:
        return ProtectedApplyComponent(
            component_id=_COMPONENT_IDS[self.execution_host],
            implementation_digest=_IMPLEMENTATION_DIGEST,
            input_fingerprint=_hash_json(
                {
                    "candidate_sha": plan.candidate_sha,
                    "candidate_tree": plan.candidate_tree,
                    "execution_host": self.execution_host,
                    "service_gid": self.service_gid,
                    "service_uid": self.service_uid,
                    "starting_epoch": plan.starting_mutation_epoch,
                }
            ),
            classify=self.classify,
            apply=self.apply,
            preapply_group="external-supervisor-credentials",
        )

    def classify(self, plan: FinalGatePlan) -> ComponentObservation:
        epoch = self.epoch_guard(plan)
        if epoch.state is not ComponentState.EXACT:
            return self._observation(plan, epoch, ComponentState.DRIFTED, None)
        try:
            evidence = self.transport.observe()
        except (OSError, RuntimeError, ValueError):
            return self._observation(plan, epoch, ComponentState.DRIFTED, None)
        if evidence is None:
            state = ComponentState.READY
        elif self._evidence_is_exact(evidence):
            state = ComponentState.EXACT
        else:
            state = ComponentState.DRIFTED
        return self._observation(plan, epoch, state, evidence)

    def apply(self, plan: FinalGatePlan) -> None:
        epoch = self.epoch_guard(plan)
        if epoch.state is not ComponentState.EXACT:
            raise RuntimeError("protected external supervisor credential epoch changed")
        try:
            current = self.transport.observe()
        except (OSError, RuntimeError, ValueError) as exc:
            raise RuntimeError(
                "protected external supervisor credential state changed before apply"
            ) from exc
        if current is not None:
            raise RuntimeError(
                "protected external supervisor credential state changed before apply"
            )
        published = self.transport.publish()
        if not self._evidence_is_exact(published):
            raise RuntimeError("protected external supervisor credential publication drifted")

    def _evidence_is_exact(self, evidence: ExternalSupervisorCredentialEvidence) -> bool:
        return (
            evidence.execution_host == self.execution_host
            and evidence.uid == self.service_uid
            and evidence.gid == self.service_gid
            and evidence.mode == 0o600
            and evidence.database_secret_readable is True
            and evidence.witness_config_map_readable is True
            and evidence.pods_exec_denied is True
        )

    def _observation(
        self,
        plan: FinalGatePlan,
        epoch: ComponentObservation,
        state: ComponentState,
        evidence: ExternalSupervisorCredentialEvidence | None,
    ) -> ComponentObservation:
        return ComponentObservation(
            state=state,
            evidence_digest=_hash_json(
                {
                    "candidate_sha": plan.candidate_sha,
                    "candidate_tree": plan.candidate_tree,
                    "credential": None if evidence is None else evidence.to_dict(),
                    "epoch_evidence_digest": epoch.evidence_digest,
                    "execution_host": self.execution_host,
                    "state": state.value,
                }
            ),
            observed_epoch=plan.starting_mutation_epoch + 1,
        )


def _hash_json(value: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


__all__ = ["ProtectedExternalSupervisorCredentialComponent"]
