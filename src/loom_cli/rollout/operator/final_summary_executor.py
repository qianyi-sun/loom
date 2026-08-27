"""Seal complete normalized final-gate evidence without another live predicate."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from loom_cli.rollout.final_gate_readiness import FinalGateResult
from loom_cli.rollout.preflight_contract import CheckOperation

from .final_gate_plan import FinalGatePlan
from .final_gate_store import FinalGateExecutionStore

_REQUIRED_PREDECESSORS = frozenset(
    {
        "final.protected-apply",
        "final.convergence",
        "final.drift",
        "final.capacity",
        "final.smoke",
        "final.browser",
    }
)


@dataclass(frozen=True, slots=True)
class FinalSummaryExecutor:
    state_root: Path
    service_uid: int

    def __post_init__(self) -> None:
        if (
            not self.state_root.is_absolute()
            or ".." in self.state_root.parts
            or self.service_uid < 0
        ):
            raise ValueError("final summary authority is invalid")

    def __call__(
        self,
        check_id: str,
        operation: CheckOperation,
        plan: FinalGatePlan,
    ) -> FinalGateResult:
        if check_id != "final.summary" or operation is not CheckOperation.VERIFY:
            raise ValueError("final summary operation is invalid")
        executions = FinalGateExecutionStore(
            self.state_root,
            request_id=plan.request_id,
            attempt_number=plan.attempt_number,
            service_uid=self.service_uid,
        ).read_all()
        blockers: dict[str, str] = {}
        if set(executions) != _REQUIRED_PREDECESSORS:
            blockers["coverage"] = "final-gate-evidence-incomplete"
        for predecessor, execution in sorted(executions.items()):
            evidence = execution.evidence
            if (
                predecessor not in _REQUIRED_PREDECESSORS
                or not execution.passed
                or evidence.get("candidate-sha") != plan.candidate_sha
                or evidence.get("attestation-digest") != plan.attestation_digest
                or evidence.get("observed-epoch") != plan.starting_mutation_epoch + 1
            ):
                blockers[predecessor] = "final-gate-evidence-drift"
        payload = {
            "attestation_digest": plan.attestation_digest,
            "candidate_sha": plan.candidate_sha,
            "executions": {
                check_id: {
                    "evidence_hash": execution.evidence_hash,
                    "implementation_digest": execution.implementation_digest,
                }
                for check_id, execution in sorted(executions.items())
            },
            "observed_epoch": plan.starting_mutation_epoch + 1,
        }
        return FinalGateResult(
            check_id="final.summary",
            operation=operation,
            candidate_sha=plan.candidate_sha,
            attestation_digest=plan.attestation_digest,
            observed_epoch=plan.starting_mutation_epoch + 1,
            evidence_digest=hashlib.sha256(
                json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
            protected_mutation=False,
            blockers=blockers,
        )


__all__ = ["FinalSummaryExecutor"]
