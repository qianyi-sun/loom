"""Atomic protected convergence of one manager admission-plan proposal."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, cast
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_agent.admission import (
    AbandonedAdmissionPlanV1,
    NeverConvergedAdmissionPlanV1,
    PreparedAdmissionPlanV1,
    PreparedPlacementAllowanceV1,
    PreparedWorkerShapeV1,
)
from loom_capacity_agent.claim_guard import InertAttemptTransitionV1
from loom_capacity_agent.contracts import (
    AgentRegistrationV1,
    GuardLifecycleDemandAttemptV2,
    GuardLifecycleDemandObservationV2,
    ReporterConfigurationV1,
)
from loom_capacity_agent.lifecycle_store import CapacityAttemptLifecycleStore
from loom_capacity_agent.prepared_store import CapacityPreparedAdmissionStore
from loom_capacity_guard.contracts import canonical_digest as protected_digest
from loom_capacity_manager.executable_contracts import (
    ExecutableAdmissionAcknowledgementV2,
    ExecutableAdmissionPlanClosureAcknowledgementV2,
    ExecutableAdmissionPlanClosureV2,
    ExecutableAdmissionPlanProposalV2,
    ProtectedAdmissionAssignmentV2,
    canonical_executable_digest,
)


class ProtectedAdmissionPlanError(RuntimeError):
    """A manager plan cannot converge with the current protected lifecycle view."""


@dataclass(frozen=True, slots=True)
class ProtectedAdmissionPlanWork:
    """Exact manager acknowledgement authorized by one committed local convergence."""

    acknowledgement: ExecutableAdmissionAcknowledgementV2
    idempotency_key: UUID


@dataclass(frozen=True, slots=True)
class ProtectedAdmissionPlanCleanupWork:
    """Exact replayable guard disposition derived from manager closure evidence."""

    closure: ExecutableAdmissionPlanClosureV2
    disposition: AbandonedAdmissionPlanV1 | NeverConvergedAdmissionPlanV1

    def __post_init__(self) -> None:
        proposal = self.closure.proposal
        anchor = proposal.shapes[0].binding
        if isinstance(self.disposition, NeverConvergedAdmissionPlanV1):
            exact = (
                self.disposition.closure == self.closure
                and self.disposition.closure_digest
                == canonical_executable_digest(self.closure)
                and self.disposition.proposal_digest
                == canonical_executable_digest(proposal)
            )
        else:
            exact = (
                self.disposition.closure_id,
                self.disposition.proposal_id,
                self.disposition.proposal_digest,
                self.disposition.plan_id,
                self.disposition.admission_incarnation,
                self.disposition.subject_id,
                self.disposition.subject_incarnation,
                self.disposition.reporter_incarnation,
                self.disposition.manager_authority_incarnation,
                self.disposition.manager_writer_epoch,
                self.disposition.manager_allocation_epoch,
                self.disposition.manager_input_digest,
                self.disposition.manager_allocation_digest,
                self.disposition.pool_id,
                self.disposition.close_reason,
            ) == (
                self.closure.closure_id,
                proposal.proposal_id,
                canonical_executable_digest(proposal),
                proposal.plan_id,
                proposal.admission_incarnation,
                anchor.subject_id,
                anchor.subject_incarnation,
                proposal.reporter_incarnation,
                anchor.execution.authority_incarnation,
                anchor.execution.writer_epoch,
                anchor.execution.allocation_epoch,
                proposal.manager_input_digest,
                proposal.manager_allocation_digest,
                anchor.pool_id,
                self.closure.close_reason,
            )
        if not exact:
            raise ProtectedAdmissionPlanError(
                "protected admission cleanup evidence changed"
            )

    @property
    def acknowledgement(self) -> ExecutableAdmissionPlanClosureAcknowledgementV2:
        proposal = self.closure.proposal
        anchor = proposal.shapes[0].binding
        return ExecutableAdmissionPlanClosureAcknowledgementV2(
            closure_id=self.closure.closure_id,
            proposal_id=proposal.proposal_id,
            proposal_digest=canonical_executable_digest(proposal),
            plan_id=proposal.plan_id,
            admission_incarnation=proposal.admission_incarnation,
            subject_id=anchor.subject_id,
            subject_incarnation=anchor.subject_incarnation,
            reporter_incarnation=proposal.reporter_incarnation,
            protected_admission_sha256=proposal.protected_admission_sha256,
            close_reason=self.closure.close_reason,
            disposition_kind=(
                "never-converged"
                if isinstance(self.disposition, NeverConvergedAdmissionPlanV1)
                else "abandoned"
            ),
            disposition_digest=protected_digest(self.disposition),
        )

    @property
    def idempotency_key(self) -> UUID:
        return uuid5(
            NAMESPACE_URL,
            "loom:protected-executable-admission-cleanup:"
            f"{canonical_executable_digest(self.acknowledgement)}",
        )


@dataclass(frozen=True, slots=True)
class _ProtectedAdmissionConvergence:
    plan: PreparedAdmissionPlanV1
    transitions: tuple[InertAttemptTransitionV1, ...]
    proposal: ExecutableAdmissionPlanProposalV2

    @property
    def work(self) -> ProtectedAdmissionPlanWork:
        anchor = self.proposal.shapes[0].binding
        assignments = tuple(
            ProtectedAdmissionAssignmentV2(
                transition_id=transition.transition_id,
                allowance_id=allowance.allowance_id,
                protected_attempt_id=allowance.protected_attempt_id,
                execution_generation=transition.execution_generation,
                requirements_digest=transition.requirements_digest,
                shape_instance_id=allowance.shape_instance_id,
                shape_slot_index=allowance.shape_slot_index,
                submission_intent_id=allowance.submission_intent_id,
                lifecycle_sequence=transition.expected_transition_sequence + 1,
            )
            for transition, allowance in zip(
                self.transitions,
                self.plan.placement_allowances,
                strict=True,
            )
        )
        acknowledgement = ExecutableAdmissionAcknowledgementV2(
            execution=anchor.execution,
            tranche_id=anchor.tranche_id,
            proposal_id=self.proposal.proposal_id,
            plan_id=self.proposal.plan_id,
            admission_incarnation=self.proposal.admission_incarnation,
            subject_id=anchor.subject_id,
            subject_incarnation=anchor.subject_incarnation,
            pool_id=cast(Literal["oldlab", "gb10"], anchor.pool_id),
            reporter_incarnation=self.proposal.reporter_incarnation,
            protected_admission_sha256=self.proposal.protected_admission_sha256,
            proposal_digest=canonical_executable_digest(self.proposal),
            prepared_plan_digest=protected_digest(self.plan),
            assignment_count=len(assignments),
            assignments=assignments,
        )
        acknowledgement_digest = canonical_executable_digest(acknowledgement)
        return ProtectedAdmissionPlanWork(
            acknowledgement=acknowledgement,
            idempotency_key=uuid5(
                NAMESPACE_URL,
                f"loom:protected-executable-admission:{acknowledgement_digest}",
            ),
        )


def _registration_values(configuration: ReporterConfigurationV1) -> dict[str, object]:
    return {
        field: getattr(configuration, field) for field in AgentRegistrationV1.model_fields
    }


def _assert_trusted_proposal_binding(
    configuration: ReporterConfigurationV1,
    proposal: ExecutableAdmissionPlanProposalV2,
    *,
    require_unexpired: bool = True,
) -> None:
    anchor = proposal.shapes[0].binding
    proposal_mismatches = tuple(
        name
        for name, actual, expected in (
            ("subject_id", anchor.subject_id, configuration.subject_id),
            (
                "subject_incarnation",
                anchor.subject_incarnation,
                configuration.subject_incarnation,
            ),
            (
                "reporter_incarnation",
                proposal.reporter_incarnation,
                configuration.reporter_incarnation,
            ),
            (
                "deployment_generation",
                anchor.deployment_generation,
                configuration.deployment_generation,
            ),
            (
                "candidate_identity_algorithm",
                anchor.candidate.algorithm,
                configuration.candidate_identity_algorithm,
            ),
            (
                "candidate_identity",
                anchor.candidate.identity,
                configuration.candidate_identity,
            ),
            (
                "candidate_publication_sha256",
                anchor.candidate.publication_sha256,
                configuration.candidate_publication_sha256,
            ),
            (
                "protected_admission_sha256",
                proposal.protected_admission_sha256,
                configuration.protected_admission_sha256,
            ),
        )
        if actual != expected
    )
    if anchor.pool_id not in {
        capability.pool_id for capability in configuration.pool_capabilities
    }:
        proposal_mismatches += ("pool_id",)
    if proposal_mismatches:
        raise ProtectedAdmissionPlanError(
            "protected admission binding differs from trusted configuration: "
            + ", ".join(proposal_mismatches)
        )
    if require_unexpired and proposal.lease_not_after <= datetime.now(UTC):
        raise ProtectedAdmissionPlanError("protected admission proposal is expired")


def _assert_trusted_binding(
    configuration: ReporterConfigurationV1,
    proposal: ExecutableAdmissionPlanProposalV2,
    observation: GuardLifecycleDemandObservationV2,
    *,
    require_unexpired: bool = True,
) -> None:
    observation_mismatches = tuple(
        field
        for field in AgentRegistrationV1.model_fields
        if getattr(observation, field) != getattr(configuration, field)
    )
    if observation_mismatches:
        raise ProtectedAdmissionPlanError(
            "protected admission binding differs from trusted configuration: "
            + ", ".join(observation_mismatches)
        )
    _assert_trusted_proposal_binding(
        configuration,
        proposal,
        require_unexpired=require_unexpired,
    )


def _assigned_attempt_matches(
    attempt: GuardLifecycleDemandAttemptV2,
    *,
    plan: PreparedAdmissionPlanV1,
    shape: PreparedWorkerShapeV1,
    allowance: PreparedPlacementAllowanceV1,
) -> bool:
    return (
        attempt.allowance_id,
        attempt.plan_id,
        attempt.admission_incarnation,
        attempt.manager_allocation_epoch,
        attempt.pool_id,
        attempt.pool_generation,
        attempt.profile_id,
        attempt.profile_generation,
        attempt.profile_digest,
        attempt.shape_id,
        attempt.shape_instance_id,
        attempt.submission_intent_id,
    ) == (
        allowance.allowance_id,
        plan.plan_id,
        plan.admission_incarnation,
        plan.manager_allocation_epoch,
        plan.pool_id,
        plan.pool_generation,
        plan.profile_id,
        plan.profile_generation,
        plan.profile_digest,
        shape.worker_shape.shape_id,
        allowance.shape_instance_id,
        allowance.submission_intent_id,
    )


def _build_protected_admission_convergence(
    configuration: ReporterConfigurationV1,
    proposal: ExecutableAdmissionPlanProposalV2,
    observation: GuardLifecycleDemandObservationV2,
    *,
    require_unexpired: bool = True,
) -> _ProtectedAdmissionConvergence:
    """Purely join manager facts to one exact protected lifecycle observation."""

    if not isinstance(configuration, ReporterConfigurationV1):
        raise ProtectedAdmissionPlanError("protected admission configuration is invalid")
    if not isinstance(proposal, ExecutableAdmissionPlanProposalV2):
        raise ProtectedAdmissionPlanError("protected admission proposal is not schema-v2")
    if not isinstance(observation, GuardLifecycleDemandObservationV2):
        raise ProtectedAdmissionPlanError("protected admission observation is invalid")
    _assert_trusted_binding(
        configuration,
        proposal,
        observation,
        require_unexpired=require_unexpired,
    )
    anchor_shape = proposal.shapes[0]
    anchor = anchor_shape.binding
    pool_id = cast(Literal["oldlab", "gb10"], anchor.pool_id)
    prepared_shapes = tuple(
        PreparedWorkerShapeV1(
            shape_instance_id=shape.binding.shape_instance_id,
            submission_intent_id=shape.binding.intent_id,
            pool_id=pool_id,
            pool_generation=shape.binding.pool_generation,
            profile_id=shape.binding.profile_id,
            profile_generation=shape.binding.profile_generation,
            profile_digest=shape.binding.profile_digest,
            protocol_generation=shape.protocol_generation,
            protocol_digest=shape.protocol_digest,
            worker_shape=shape.worker_shape,
            worker_shape_digest=shape.worker_shape_digest,
            bootstrap_registration_epoch=shape.bootstrap_registration_epoch,
        )
        for shape in proposal.shapes
    )
    shapes_by_id = {item.shape_instance_id: item for item in prepared_shapes}
    attempts_by_id = {item.protected_attempt_id: item for item in observation.attempts}
    prepared_allowances: list[PreparedPlacementAllowanceV1] = []
    attempts: list[GuardLifecycleDemandAttemptV2] = []
    for manager_allowance in proposal.allowances:
        attempt = attempts_by_id.get(manager_allowance.protected_attempt_id)
        if attempt is None:
            raise ProtectedAdmissionPlanError(
                "protected admission allowance references a missing attempt"
            )
        prepared_allowances.append(
            PreparedPlacementAllowanceV1(
                allowance_id=manager_allowance.allowance_id,
                protected_attempt_id=manager_allowance.protected_attempt_id,
                execution_generation=attempt.execution_generation,
                requirements_digest=attempt.requirements_digest,
                pool_id=pool_id,
                shape_instance_id=manager_allowance.shape_instance_id,
                shape_slot_index=manager_allowance.shape_slot_index,
                submission_intent_id=manager_allowance.submission_intent_id,
            )
        )
        attempts.append(attempt)
    plan = PreparedAdmissionPlanV1.model_validate(
        {
            **_registration_values(configuration),
            "plan_id": proposal.plan_id,
            "admission_incarnation": proposal.admission_incarnation,
            "manager_authority_incarnation": anchor.execution.authority_incarnation,
            "manager_writer_epoch": anchor.execution.writer_epoch,
            "manager_allocation_epoch": anchor.execution.allocation_epoch,
            "manager_input_digest": proposal.manager_input_digest,
            "manager_allocation_digest": proposal.manager_allocation_digest,
            "pool_id": pool_id,
            "pool_generation": anchor.pool_generation,
            "profile_id": anchor.profile_id,
            "profile_generation": anchor.profile_generation,
            "profile_digest": anchor.profile_digest,
            "protocol_generation": anchor_shape.protocol_generation,
            "protocol_digest": anchor_shape.protocol_digest,
            "lease_not_after": proposal.lease_not_after,
            "worker_shapes": prepared_shapes,
            "placement_allowances": tuple(prepared_allowances),
        }
    )
    transitions: list[InertAttemptTransitionV1] = []
    proposal_digest = canonical_executable_digest(proposal)
    for allowance, attempt in zip(plan.placement_allowances, attempts, strict=True):
        shape = shapes_by_id[allowance.shape_instance_id]
        if attempt.lifecycle_state == "assigned":
            if attempt.lifecycle_sequence == 0 or not _assigned_attempt_matches(
                attempt,
                plan=plan,
                shape=shape,
                allowance=allowance,
            ):
                raise ProtectedAdmissionPlanError(
                    "protected admission attempt has a different assignment"
                )
            expected_sequence = attempt.lifecycle_sequence - 1
        else:
            expected_sequence = attempt.lifecycle_sequence
        transitions.append(
            InertAttemptTransitionV1.model_validate(
                {
                    **_registration_values(configuration),
                    "transition_id": uuid5(
                        NAMESPACE_URL,
                        "loom:protected-executable-admission-assignment:"
                        f"{proposal_digest}:{allowance.allowance_id}",
                    ),
                    "protected_attempt_id": allowance.protected_attempt_id,
                    "execution_generation": allowance.execution_generation,
                    "requirements_digest": allowance.requirements_digest,
                    "expected_transition_sequence": expected_sequence,
                    "operation": "assign",
                    "expected_state": "pending-unassigned",
                    "target_state": "assigned",
                    "allowance_id": allowance.allowance_id,
                    "plan_id": plan.plan_id,
                    "admission_incarnation": plan.admission_incarnation,
                    "manager_allocation_epoch": plan.manager_allocation_epoch,
                    "pool_id": plan.pool_id,
                    "shape_instance_id": allowance.shape_instance_id,
                    "submission_intent_id": allowance.submission_intent_id,
                    "transition_reason": "manager-placement",
                }
            )
        )
    return _ProtectedAdmissionConvergence(
        plan=plan,
        transitions=tuple(transitions),
        proposal=proposal,
    )


class ProtectedAdmissionPlanCoordinator:
    """Commit a manager plan and every local assignment in one outer transaction."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        configuration: ReporterConfigurationV1,
    ) -> None:
        if not isinstance(session, AsyncSession):
            raise TypeError("protected admission convergence requires an async session")
        if not isinstance(configuration, ReporterConfigurationV1):
            raise TypeError("protected admission convergence requires trusted configuration")
        self._session = session
        self._configuration = configuration

    async def converge(
        self,
        proposal: ExecutableAdmissionPlanProposalV2,
        observation: GuardLifecycleDemandObservationV2,
    ) -> ProtectedAdmissionPlanWork:
        """Persist exact local evidence and only then construct its acknowledgement."""

        convergence = _build_protected_admission_convergence(
            self._configuration,
            proposal,
            observation,
        )
        if not self._session.in_transaction():
            raise ProtectedAdmissionPlanError(
                "protected admission convergence requires an outer transaction"
            )
        isolation = (await self._session.execute(text("SHOW transaction_isolation"))).scalar_one()
        if isolation != "serializable":
            raise ProtectedAdmissionPlanError(
                "protected admission convergence requires SERIALIZABLE isolation"
            )
        await CapacityPreparedAdmissionStore(
            self._session,
            registration=self._configuration,
        ).prepare_plan(convergence.plan)
        lifecycle = CapacityAttemptLifecycleStore(
            self._session,
            registration=self._configuration,
        )
        for transition in convergence.transitions:
            await lifecycle.apply_transition(transition)
            await lifecycle.assert_current_assignment(transition)
        return convergence.work

    async def authorize_publication(
        self,
        work: ProtectedAdmissionPlanWork,
    ) -> ProtectedAdmissionPlanWork:
        """Hold exact local plan and assignment authority through manager receipt."""

        if not isinstance(work, ProtectedAdmissionPlanWork):
            raise TypeError("protected admission publication requires exact local work")
        acknowledgement = work.acknowledgement
        expected_idempotency_key = uuid5(
            NAMESPACE_URL,
            "loom:protected-executable-admission:"
            f"{canonical_executable_digest(acknowledgement)}",
        )
        if work.idempotency_key != expected_idempotency_key:
            raise ProtectedAdmissionPlanError(
                "protected admission publication idempotency binding changed"
            )
        mismatches = tuple(
            name
            for name, actual, expected in (
                ("subject_id", acknowledgement.subject_id, self._configuration.subject_id),
                (
                    "subject_incarnation",
                    acknowledgement.subject_incarnation,
                    self._configuration.subject_incarnation,
                ),
                (
                    "reporter_incarnation",
                    acknowledgement.reporter_incarnation,
                    self._configuration.reporter_incarnation,
                ),
                (
                    "protected_admission_sha256",
                    acknowledgement.protected_admission_sha256,
                    self._configuration.protected_admission_sha256,
                ),
            )
            if actual != expected
        )
        if mismatches:
            raise ProtectedAdmissionPlanError(
                "protected admission publication binding changed: " + ", ".join(mismatches)
            )
        if not self._session.in_transaction():
            raise ProtectedAdmissionPlanError(
                "protected admission publication requires an outer transaction"
            )
        isolation = (await self._session.execute(text("SHOW transaction_isolation"))).scalar_one()
        if isolation != "serializable":
            raise ProtectedAdmissionPlanError(
                "protected admission publication requires SERIALIZABLE isolation"
            )

        await CapacityPreparedAdmissionStore(
            self._session,
            registration=self._configuration,
        ).assert_current_plan(
            plan_id=acknowledgement.plan_id,
            admission_incarnation=acknowledgement.admission_incarnation,
            manager_allocation_epoch=acknowledgement.execution.allocation_epoch,
            pool_id=acknowledgement.pool_id,
            prepared_plan_digest=acknowledgement.prepared_plan_digest,
        )
        lifecycle = CapacityAttemptLifecycleStore(
            self._session,
            registration=self._configuration,
        )
        for assignment in acknowledgement.assignments:
            transition = InertAttemptTransitionV1.model_validate(
                {
                    **_registration_values(self._configuration),
                    "transition_id": assignment.transition_id,
                    "protected_attempt_id": assignment.protected_attempt_id,
                    "execution_generation": assignment.execution_generation,
                    "requirements_digest": assignment.requirements_digest,
                    "expected_transition_sequence": assignment.lifecycle_sequence - 1,
                    "operation": "assign",
                    "expected_state": "pending-unassigned",
                    "target_state": "assigned",
                    "allowance_id": assignment.allowance_id,
                    "plan_id": acknowledgement.plan_id,
                    "admission_incarnation": acknowledgement.admission_incarnation,
                    "manager_allocation_epoch": acknowledgement.execution.allocation_epoch,
                    "pool_id": acknowledgement.pool_id,
                    "shape_instance_id": assignment.shape_instance_id,
                    "submission_intent_id": assignment.submission_intent_id,
                    "transition_reason": "manager-placement",
                }
            )
            await lifecycle.assert_current_assignment(transition)
        return work

    async def abandon(
        self,
        closure: ExecutableAdmissionPlanClosureV2,
        observation: GuardLifecycleDemandObservationV2,
    ) -> ProtectedAdmissionPlanCleanupWork:
        """Withdraw exact unclaimed assignments and durably abandon their plan."""

        if not isinstance(closure, ExecutableAdmissionPlanClosureV2):
            raise TypeError("protected admission cleanup requires manager closure evidence")
        if not self._session.in_transaction():
            raise ProtectedAdmissionPlanError(
                "protected admission cleanup requires an outer transaction"
            )
        isolation = (await self._session.execute(text("SHOW transaction_isolation"))).scalar_one()
        if isolation != "serializable":
            raise ProtectedAdmissionPlanError(
                "protected admission cleanup requires SERIALIZABLE isolation"
            )
        convergence = _build_protected_admission_convergence(
            self._configuration,
            closure.proposal,
            observation,
            require_unexpired=False,
        )
        attempts_by_id = {
            attempt.protected_attempt_id: attempt for attempt in observation.attempts
        }
        shapes_by_id = {
            shape.shape_instance_id: shape for shape in convergence.plan.worker_shapes
        }
        lifecycle = CapacityAttemptLifecycleStore(
            self._session,
            registration=self._configuration,
        )
        closure_digest = canonical_executable_digest(closure)
        for assignment, allowance in zip(
            convergence.transitions,
            convergence.plan.placement_allowances,
            strict=True,
        ):
            attempt = attempts_by_id[allowance.protected_attempt_id]
            if attempt.lifecycle_state == "assigned" and not _assigned_attempt_matches(
                attempt,
                plan=convergence.plan,
                shape=shapes_by_id[allowance.shape_instance_id],
                allowance=allowance,
            ):
                raise ProtectedAdmissionPlanError(
                    "protected admission cleanup found a non-withdrawable assignment"
                )
            if attempt.lifecycle_state not in {"pending-unassigned", "assigned"}:
                raise ProtectedAdmissionPlanError(
                    "protected admission cleanup found a non-withdrawable assignment"
                )
            expected_sequence = (
                attempt.lifecycle_sequence
                if attempt.lifecycle_state == "assigned"
                else assignment.expected_transition_sequence + 1
            )
            withdrawal = assignment.model_copy(
                update={
                    "transition_id": uuid5(
                        NAMESPACE_URL,
                        "loom:protected-executable-admission-abandonment:"
                        f"{closure_digest}:{allowance.allowance_id}",
                    ),
                    "expected_transition_sequence": expected_sequence,
                    "operation": "withdraw",
                    "expected_state": "assigned",
                    "target_state": "pending-unassigned",
                    "transition_reason": "manager-admission-closed",
                }
            )
            await lifecycle.apply_transition(withdrawal)

        proposal = closure.proposal
        anchor = proposal.shapes[0].binding
        abandonment = AbandonedAdmissionPlanV1.model_validate(
            {
                **_registration_values(self._configuration),
                "closure_id": closure.closure_id,
                "proposal_id": proposal.proposal_id,
                "proposal_digest": canonical_executable_digest(proposal),
                "plan_id": proposal.plan_id,
                "admission_incarnation": proposal.admission_incarnation,
                "manager_authority_incarnation": anchor.execution.authority_incarnation,
                "manager_writer_epoch": anchor.execution.writer_epoch,
                "manager_allocation_epoch": anchor.execution.allocation_epoch,
                "manager_input_digest": proposal.manager_input_digest,
                "manager_allocation_digest": proposal.manager_allocation_digest,
                "pool_id": anchor.pool_id,
                "close_reason": closure.close_reason,
            }
        )
        persisted = await CapacityPreparedAdmissionStore(
            self._session,
            registration=self._configuration,
        ).abandon_plan(abandonment)
        return ProtectedAdmissionPlanCleanupWork(
            closure=closure,
            disposition=persisted,
        )

    async def close(
        self,
        closure: ExecutableAdmissionPlanClosureV2,
        observation: GuardLifecycleDemandObservationV2 | None,
    ) -> ProtectedAdmissionPlanCleanupWork:
        """Commit exactly one mutually exclusive guard-owned closure disposition."""

        if not isinstance(closure, ExecutableAdmissionPlanClosureV2):
            raise TypeError("protected admission cleanup requires manager closure evidence")
        if not self._session.in_transaction():
            raise ProtectedAdmissionPlanError(
                "protected admission cleanup requires an outer transaction"
            )
        isolation = (await self._session.execute(text("SHOW transaction_isolation"))).scalar_one()
        if isolation != "serializable":
            raise ProtectedAdmissionPlanError(
                "protected admission cleanup requires SERIALIZABLE isolation"
            )
        _assert_trusted_proposal_binding(
            self._configuration,
            closure.proposal,
            require_unexpired=False,
        )
        registration = AgentRegistrationV1.model_validate(
            _registration_values(self._configuration)
        )
        tombstone = NeverConvergedAdmissionPlanV1(
            **registration.model_dump(mode="python", exclude_none=False),
            registration_digest=protected_digest(registration),
            closure=closure,
            closure_digest=canonical_executable_digest(closure),
            proposal_digest=canonical_executable_digest(closure.proposal),
        )
        try:
            persisted = await CapacityPreparedAdmissionStore(
                self._session,
                registration=self._configuration,
            ).tombstone_never_converged_plan(tombstone)
        except DBAPIError as exc:
            if "prepared admission plan already exists" not in str(exc):
                raise
            if observation is None:
                raise
        else:
            return ProtectedAdmissionPlanCleanupWork(
                closure=closure,
                disposition=persisted,
            )
        return await self.abandon(closure, observation)


__all__ = [
    "ProtectedAdmissionPlanCleanupWork",
    "ProtectedAdmissionPlanCoordinator",
    "ProtectedAdmissionPlanError",
    "ProtectedAdmissionPlanWork",
]
