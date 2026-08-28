"""Candidate-bound live GB10 Slurm capacity final gate."""

from __future__ import annotations

import hashlib
import json
import subprocess
from collections.abc import Callable
from dataclasses import dataclass

from loom_cli.rollout.final_gate_readiness import FinalGateResult
from loom_cli.rollout.gb10_readiness import FULL_GB10_HOSTS
from loom_cli.rollout.gb10_slurm_acceptance import GB10_SLURM_WORKER_HOSTS
from loom_cli.rollout.preflight_contract import CheckOperation

from .final_gate_plan import FinalGatePlan
from .protected_gb10_external_supervisor_transport import (
    FixedGB10ExternalSupervisorTransport,
)


@dataclass(frozen=True, slots=True)
class FinalCapacityExecutor:
    """Run the fixed real-allocation authority immediately before smoke."""

    transport: FixedGB10ExternalSupervisorTransport | None = None
    transport_factory: Callable[[], FixedGB10ExternalSupervisorTransport] | None = None

    def __post_init__(self) -> None:
        if (self.transport is None) == (self.transport_factory is None):
            raise ValueError("final capacity transport authority is invalid")

    def __call__(
        self,
        check_id: str,
        operation: CheckOperation,
        plan: FinalGatePlan,
    ) -> FinalGateResult:
        if (
            check_id != "final.capacity"
            or operation is not CheckOperation.APPLY
            or set(plan.gb10_boot_ids) != set(FULL_GB10_HOSTS)
        ):
            raise ValueError("final capacity operation is invalid")
        try:
            transport = (
                self.transport_factory() if self.transport_factory is not None else self.transport
            )
            if transport is None:
                raise RuntimeError("final capacity transport is unavailable")
            acceptance = transport.accept_capacity(
                profile_sha256=plan.supervisor_profile_sha256,
                nodes=GB10_SLURM_WORKER_HOSTS,
            )
        except (OSError, RuntimeError, ValueError, subprocess.SubprocessError):
            evidence_digest = hashlib.sha256(
                json.dumps(
                    {
                        "accepted": False,
                        "candidate_sha": plan.candidate_sha,
                        "candidate_tree": plan.candidate_tree,
                        "nodes": GB10_SLURM_WORKER_HOSTS,
                        "profile_sha256": plan.supervisor_profile_sha256,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            blockers = {"capacity": "slurm-acceptance-unavailable"}
        else:
            evidence_digest = acceptance.evidence_digest
            blockers = {}
        return FinalGateResult(
            check_id=check_id,
            operation=operation,
            candidate_sha=plan.candidate_sha,
            attestation_digest=plan.attestation_digest,
            observed_epoch=plan.starting_mutation_epoch + 1,
            evidence_digest=evidence_digest,
            protected_mutation=True,
            blockers=blockers,
        )


__all__ = ["FinalCapacityExecutor"]
