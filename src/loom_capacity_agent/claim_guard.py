"""Disconnected, fail-closed claim-guard contracts for Package 2B3."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Literal, Protocol
from uuid import UUID

from pydantic import model_validator

from loom_capacity_agent.contracts import AgentRegistrationV1
from loom_capacity_guard.contracts import (
    Digest,
    GuardIdentifier,
    NonNegativeSequence,
    PositiveGeneration,
    StrictGuardModel,
)
from loom_capacity_manager.executable_contracts import StrictV2Model

UnclaimedAttemptState = Literal[
    "pending-unassigned",
    "assigned",
    "cancelled-terminal",
]
TransitionOperation = Literal["assign", "withdraw", "cancel"]
PhysicalPool = Literal["oldlab", "gb10"]


class InertAttemptTransitionV1(AgentRegistrationV1):
    """One exact unclaimed lifecycle CAS with no claim authority."""

    transition_id: UUID
    protected_attempt_id: UUID
    execution_generation: PositiveGeneration
    requirements_digest: Digest
    expected_transition_sequence: NonNegativeSequence
    operation: TransitionOperation
    expected_state: UnclaimedAttemptState
    target_state: UnclaimedAttemptState
    allowance_id: UUID | None = None
    plan_id: UUID | None = None
    admission_incarnation: UUID | None = None
    manager_allocation_epoch: PositiveGeneration | None = None
    pool_id: PhysicalPool | None = None
    shape_instance_id: GuardIdentifier | None = None
    submission_intent_id: UUID | None = None
    transition_reason: GuardIdentifier
    executable: Literal[False] = False

    @model_validator(mode="after")
    def _exact_transition(self) -> InertAttemptTransitionV1:
        expected_states = {
            "assign": ("pending-unassigned", "assigned"),
            "withdraw": ("assigned", "pending-unassigned"),
            "cancel": (self.expected_state, "cancelled-terminal"),
        }
        if (self.expected_state, self.target_state) != expected_states[self.operation]:
            raise ValueError("operation does not match its exact state transition")
        if self.operation == "cancel" and self.expected_state not in {
            "pending-unassigned",
            "assigned",
        }:
            raise ValueError("cancel must target one unclaimed attempt state")

        assignment = (
            self.allowance_id,
            self.plan_id,
            self.admission_incarnation,
            self.manager_allocation_epoch,
            self.pool_id,
            self.shape_instance_id,
            self.submission_intent_id,
        )
        requires_assignment = self.operation in {"assign", "withdraw"} or (
            self.operation == "cancel" and self.expected_state == "assigned"
        )
        if requires_assignment and any(value is None for value in assignment):
            raise ValueError("transition requires one complete assignment binding")
        if not requires_assignment and any(value is not None for value in assignment):
            raise ValueError("unassigned transition must not carry an assignment binding")

        identities = {
            self.subject_id,
            self.subject_incarnation,
            self.authority_incarnation,
            self.agent_incarnation,
            self.reporter_incarnation,
            self.transition_id,
            self.protected_attempt_id,
        }
        optional_identities = (
            self.allowance_id,
            self.plan_id,
            self.admission_incarnation,
            self.submission_intent_id,
        )
        identities.update(value for value in optional_identities if value is not None)
        if len(identities) != 7 + sum(value is not None for value in optional_identities):
            raise ValueError("transition identities must be distinct")
        return self


class ClaimProposalV1(AgentRegistrationV1):
    """Candidate proposal shape accepted only by the disconnected trusted guard."""

    proposal_id: UUID
    protected_attempt_id: UUID
    execution_generation: PositiveGeneration
    requirements_digest: Digest
    expected_transition_sequence: NonNegativeSequence
    allowance_id: UUID
    plan_id: UUID
    admission_incarnation: UUID
    manager_allocation_epoch: PositiveGeneration
    pool_id: PhysicalPool
    shape_instance_id: GuardIdentifier
    submission_intent_id: UUID
    worker_id: UUID
    worker_incarnation: UUID
    bootstrap_id: UUID
    proposed_claim_epoch: PositiveGeneration

    @model_validator(mode="after")
    def _distinct_proposal_identities(self) -> ClaimProposalV1:
        identities = {
            self.subject_id,
            self.subject_incarnation,
            self.authority_incarnation,
            self.agent_incarnation,
            self.reporter_incarnation,
            self.proposal_id,
            self.protected_attempt_id,
            self.allowance_id,
            self.plan_id,
            self.admission_incarnation,
            self.submission_intent_id,
            self.worker_id,
            self.worker_incarnation,
            self.bootstrap_id,
        }
        if len(identities) != 14:
            raise ValueError("claim proposal identities must be distinct")
        return self


class ClaimGuardDecisionV1(StrictGuardModel):
    """Only decision representable before activation: no claim and no lease."""

    proposal_id: UUID
    agent_incarnation: UUID
    activation_state: Literal["disabled"] = "disabled"
    activation_epoch: Literal[0] = 0
    executable_new_capacity_ceiling: Literal[0] = 0
    admitted: Literal[False] = False
    reason: Literal["activation-disabled", "not-admitted"] = "activation-disabled"
    claim_id: None = None
    concurrency_lease_id: None = None
    executable: Literal[False] = False


class ClaimGuard(Protocol):
    """Control-plane adapter boundary; no route implements it in Package 2B3."""

    async def evaluate(self, proposal: ClaimProposalV1) -> ClaimGuardDecisionV1: ...


class DisabledClaimGuard:
    """Validate the registered fence and deny every candidate proposal."""

    def __init__(self, *, registration: AgentRegistrationV1) -> None:
        if not isinstance(registration, AgentRegistrationV1):
            raise TypeError("claim guard requires a trusted agent registration")
        self._registration = registration

    async def evaluate(self, proposal: ClaimProposalV1) -> ClaimGuardDecisionV1:
        mismatches = tuple(
            field
            for field in AgentRegistrationV1.model_fields
            if getattr(proposal, field) != getattr(self._registration, field)
        )
        if mismatches:
            raise ValueError(f"claim proposal binding mismatch: {', '.join(mismatches)}")
        return ClaimGuardDecisionV1(
            proposal_id=proposal.proposal_id,
            agent_incarnation=self._registration.agent_incarnation,
        )


class ExecutableClaimProposalV2(StrictV2Model):
    """Minimal protected claim identity checked before any claim mutation."""

    operation_id: UUID
    protected_attempt_id: UUID
    execution_generation: PositiveGeneration
    requirements_digest: Digest
    worker_id: UUID
    worker_incarnation: UUID
    expected_claim_high_water: NonNegativeSequence
    executable: Literal[True] = True


class ExecutableClaimReceiptV2(StrictV2Model):
    """Append-only receipt for one protected executable claim lease."""

    operation_id: UUID
    subject_id: UUID
    subject_incarnation: UUID
    protected_attempt_id: UUID
    execution_generation: PositiveGeneration
    requirements_digest: Digest
    intent_id: UUID
    worker_id: UUID
    worker_incarnation: UUID
    claim_high_water: PositiveGeneration
    request_digest: Digest
    lease_state: Literal["live"] = "live"
    admitted: Literal[True] = True
    executable: Literal[True] = True


AdmitExecutableClaim = Callable[
    [ExecutableClaimProposalV2],
    Awaitable[ExecutableClaimReceiptV2 | None],
]


class ExecutableClaimGate:
    """Stop a draining or revoked worker before a candidate claim transition."""

    def __init__(self, *, admit_claim: AdmitExecutableClaim) -> None:
        if not callable(admit_claim):
            raise TypeError("executable claim gate requires a protected transaction")
        self._admit_claim = admit_claim

    async def evaluate(
        self,
        proposal: ExecutableClaimProposalV2,
    ) -> ExecutableClaimReceiptV2 | None:
        if not isinstance(proposal, ExecutableClaimProposalV2):
            raise TypeError("executable claim proposal is invalid")
        return await self._admit_claim(proposal)


__all__ = [
    "ClaimGuard",
    "ClaimGuardDecisionV1",
    "ClaimProposalV1",
    "DisabledClaimGuard",
    "ExecutableClaimGate",
    "ExecutableClaimProposalV2",
    "ExecutableClaimReceiptV2",
    "InertAttemptTransitionV1",
]
