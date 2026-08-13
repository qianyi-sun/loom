"""Strict disconnected claim-guard contracts and fail-closed adapter."""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import ValidationError

from loom_capacity_agent.claim_guard import (
    ClaimGuardDecisionV1,
    ClaimProposalV1,
    DisabledClaimGuard,
    ExecutableClaimGate,
    ExecutableClaimProposalV2,
    InertAttemptTransitionV1,
)
from loom_capacity_agent.contracts import AgentRegistrationV1


def _registration() -> AgentRegistrationV1:
    return AgentRegistrationV1(
        environment_id="dev-alice",
        subject_id=uuid4(),
        subject_incarnation=uuid4(),
        authority_incarnation=uuid4(),
        agent_incarnation=uuid4(),
        reporter_incarnation=uuid4(),
        candidate_digest="a" * 64,
        deployment_generation=7,
        configuration_generation=11,
    )


def _assignment_fields() -> dict[str, object]:
    return {
        "allowance_id": uuid4(),
        "plan_id": uuid4(),
        "admission_incarnation": uuid4(),
        "manager_allocation_epoch": 3,
        "pool_id": "oldlab",
        "shape_instance_id": "shape-oldlab-0001",
        "submission_intent_id": uuid4(),
    }


def _transition(
    registration: AgentRegistrationV1,
    *,
    operation: str = "assign",
) -> InertAttemptTransitionV1:
    state_fields: dict[str, object]
    if operation == "assign":
        state_fields = {
            "expected_state": "pending-unassigned",
            "target_state": "assigned",
            **_assignment_fields(),
        }
    elif operation == "withdraw":
        state_fields = {
            "expected_state": "assigned",
            "target_state": "pending-unassigned",
            **_assignment_fields(),
        }
    else:
        state_fields = {
            "expected_state": "pending-unassigned",
            "target_state": "cancelled-terminal",
        }
    return InertAttemptTransitionV1(
        **registration.model_dump(mode="python"),
        transition_id=uuid4(),
        protected_attempt_id=uuid4(),
        execution_generation=1,
        requirements_digest="b" * 64,
        expected_transition_sequence=0,
        operation=operation,
        transition_reason="manager-placement" if operation == "assign" else operation,
        **state_fields,
    )


def _proposal(registration: AgentRegistrationV1) -> ClaimProposalV1:
    return ClaimProposalV1(
        **registration.model_dump(mode="python"),
        proposal_id=uuid4(),
        protected_attempt_id=uuid4(),
        execution_generation=1,
        requirements_digest="b" * 64,
        expected_transition_sequence=1,
        **_assignment_fields(),
        worker_id=uuid4(),
        worker_incarnation=uuid4(),
        bootstrap_id=uuid4(),
        proposed_claim_epoch=1,
    )


def test_inert_transitions_have_exact_nonexecutable_state_shapes() -> None:
    registration = _registration()
    for operation in ("assign", "withdraw", "cancel"):
        transition = _transition(registration, operation=operation)
        assert transition.executable is False

    assign = _transition(registration)
    invalid = {
        **assign.model_dump(mode="python"),
        "operation": "withdraw",
    }
    with pytest.raises(ValidationError, match="state transition"):
        InertAttemptTransitionV1.model_validate(invalid)

    missing_allowance = {
        **assign.model_dump(mode="python"),
        "allowance_id": None,
    }
    with pytest.raises(ValidationError, match="assignment binding"):
        InertAttemptTransitionV1.model_validate(missing_allowance)

    with pytest.raises(ValidationError):
        InertAttemptTransitionV1.model_validate(
            {**assign.model_dump(mode="python"), "executable": True}
        )


def test_cancel_from_assigned_requires_the_exact_assignment_binding() -> None:
    registration = _registration()
    assigned_cancel = InertAttemptTransitionV1.model_validate(
        {
            **_transition(registration).model_dump(mode="python"),
            "operation": "cancel",
            "expected_state": "assigned",
            "target_state": "cancelled-terminal",
            "transition_reason": "owner-cancelled-unclaimed",
        }
    )
    assert assigned_cancel.allowance_id is not None
    with pytest.raises(ValidationError, match="assignment binding"):
        InertAttemptTransitionV1.model_validate(
            {**assigned_cancel.model_dump(mode="python"), "plan_id": None}
        )


@pytest.mark.asyncio
async def test_disabled_claim_guard_never_admits_or_returns_claim_authority() -> None:
    registration = _registration()
    proposal = _proposal(registration)
    decision = await DisabledClaimGuard(registration=registration).evaluate(proposal)
    assert decision == ClaimGuardDecisionV1(
        proposal_id=proposal.proposal_id,
        agent_incarnation=registration.agent_incarnation,
    )
    assert decision.activation_state == "disabled"
    assert decision.activation_epoch == 0
    assert decision.executable_new_capacity_ceiling == 0
    assert decision.admitted is False
    assert decision.claim_id is None
    assert decision.concurrency_lease_id is None


@pytest.mark.asyncio
async def test_disabled_claim_guard_rejects_a_mismatched_registration() -> None:
    registration = _registration()
    proposal = _proposal(registration).model_copy(update={"candidate_digest": "c" * 64})
    with pytest.raises(ValueError, match="binding mismatch"):
        await DisabledClaimGuard(registration=registration).evaluate(proposal)


def test_claim_contracts_contain_no_secret_or_token_field() -> None:
    fields = set(ClaimProposalV1.model_fields) | set(ClaimGuardDecisionV1.model_fields)
    assert not {field for field in fields if "secret" in field or "token" in field}


@pytest.mark.asyncio
async def test_executable_claim_gate_delegates_the_complete_protected_transaction() -> None:
    checked: list[ExecutableClaimProposalV2] = []

    async def admit_claim(proposal: ExecutableClaimProposalV2) -> None:
        checked.append(proposal)

    proposal = ExecutableClaimProposalV2(
        operation_id=uuid4(),
        protected_attempt_id=uuid4(),
        execution_generation=7,
        requirements_digest="b" * 64,
        worker_id=uuid4(),
        worker_incarnation=uuid4(),
        expected_claim_high_water=3,
    )
    decision = await ExecutableClaimGate(admit_claim=admit_claim).evaluate(proposal)
    assert decision is None
    assert checked == [proposal]
