from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from loom_capacity_agent.admission import (
    AbandonedAdmissionPlanV1,
    NeverConvergedAdmissionPlanV1,
)
from loom_capacity_agent.admission_convergence import (
    ProtectedAdmissionPlanCleanupWork,
    ProtectedAdmissionPlanError,
    _assert_trusted_proposal_binding,
    _build_protected_admission_convergence,
)
from loom_capacity_agent.contracts import (
    AgentPoolCapabilityV1,
    AgentRegistrationV1,
    GuardLifecycleDemandAttemptV2,
    GuardLifecycleDemandObservationV2,
    ReporterConfigurationV1,
)
from loom_capacity_guard.contracts import SealedRequirementsV1
from loom_capacity_guard.contracts import canonical_digest as guard_canonical_digest
from loom_capacity_manager.contracts import ResourceVectorV1, WorkerShapeV1
from loom_capacity_manager.contracts import canonical_digest as manager_canonical_digest
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutableAdmissionAllowanceV2,
    ExecutableAdmissionPlanClosureV2,
    ExecutableAdmissionPlanProposalV2,
    ExecutableAdmissionShapeV2,
    ExecutableIntentBindingV2,
    ExecutionFenceV2,
    canonical_executable_digest,
)


def _configuration(
    *,
    candidate_digest: str = "a" * 64,
    candidate_identity_algorithm: Literal["git-sha1", "source-sha256"] = "source-sha256",
    candidate_identity: str | None = None,
    candidate_publication_sha256: str | None = None,
) -> ReporterConfigurationV1:
    registration = AgentRegistrationV1(
        environment_id="dev-alice",
        subject_id=UUID(int=1),
        subject_incarnation=UUID(int=2),
        authority_incarnation=UUID(int=3),
        agent_incarnation=UUID(int=4),
        reporter_incarnation=UUID(int=5),
        candidate_digest=candidate_digest,
        candidate_identity_algorithm=candidate_identity_algorithm,
        candidate_identity=candidate_identity or candidate_digest,
        candidate_publication_sha256=candidate_publication_sha256 or candidate_digest,
        deployment_generation=7,
        configuration_generation=11,
    )
    return ReporterConfigurationV1(
        **registration.model_dump(mode="python"),
        protected_admission_sha256="b" * 64,
        pool_capabilities=(
            AgentPoolCapabilityV1(
                capability_id="oldlab-x86-none",
                pool_id="oldlab",
                operating_system="linux",
                cpu_architecture="x86_64",
                gpu_vendor="none",
                network_policies=("public",),
            ),
        ),
    )


def _shape(configuration: ReporterConfigurationV1, index: int) -> ExecutableAdmissionShapeV2:
    resources = ResourceVectorV1(
        slots=1,
        cpu_millicores=2000,
        memory_bytes=4_000_000_000,
    )
    worker_shape = WorkerShapeV1(
        shape_id=f"oldlab-x86-none-{index}",
        concurrency_slots=1,
        total_resources=resources,
        node_resources=(resources,),
        compatible_domain_ids=("oldlab-x86",),
        capabilities=(
            "cpu_arch.x86_64",
            "gpu_vendor.none",
            "network.public",
            "os.linux",
        ),
    )
    binding = ExecutableIntentBindingV2(
        execution=ExecutionFenceV2(
            authority_incarnation=UUID(int=10),
            writer_epoch=12,
            configuration_epoch=13,
            execution_epoch=14,
            execution_manifest_sha256="c" * 64,
            execution_state="active",
            executable_new_capacity_ceiling=2,
            executable_new_capacity_rate_per_minute=2,
            trusted_fleet_release_sha256="d" * 64,
            allocation_epoch=15,
        ),
        tranche_id=UUID(int=16),
        intent_id=UUID(int=20 + index),
        shape_instance_id=f"shape-oldlab-{index:04d}",
        subject_id=configuration.subject_id,
        subject_incarnation=configuration.subject_incarnation,
        account_id="dev-alice",
        tier_id="development",
        candidate=CandidateBindingV2(
            algorithm=configuration.candidate_identity_algorithm,
            identity=configuration.candidate_identity,
            publication_sha256=configuration.candidate_publication_sha256,
        ),
        candidate_generation=6,
        deployment_generation=configuration.deployment_generation,
        pool_id="oldlab",
        pool_generation=17,
        executor_id="oldlab-controller",
        executor_incarnation=UUID(int=18),
        shape_id=worker_shape.shape_id,
        profile_id="dev-oldlab",
        profile_generation=19,
        profile_digest="f" * 64,
        concurrency_slots=1,
        resources=resources,
        node_ids=(f"oldlab-{index:02d}",),
    )
    return ExecutableAdmissionShapeV2(
        binding=binding,
        protocol_generation=21,
        protocol_digest="1" * 64,
        worker_shape=worker_shape,
        worker_shape_digest=manager_canonical_digest(worker_shape),
        bootstrap_registration_epoch=1,
    )


def _proposal(
    configuration: ReporterConfigurationV1,
    *,
    allowance_count: int = 2,
) -> ExecutableAdmissionPlanProposalV2:
    shapes = (_shape(configuration, 1), _shape(configuration, 2))
    allowances = tuple(
        ExecutableAdmissionAllowanceV2(
            allowance_id=UUID(int=30 + index),
            protected_attempt_id=UUID(int=40 + index),
            shape_instance_id=shapes[index].binding.shape_instance_id,
            shape_slot_index=0,
            submission_intent_id=shapes[index].binding.intent_id,
        )
        for index in range(allowance_count)
    )
    return ExecutableAdmissionPlanProposalV2(
        proposal_id=UUID(int=50),
        plan_id=UUID(int=51),
        admission_incarnation=UUID(int=52),
        reporter_incarnation=configuration.reporter_incarnation,
        protected_admission_sha256=configuration.protected_admission_sha256,
        manager_input_digest="2" * 64,
        manager_allocation_digest="3" * 64,
        lease_not_after=datetime.now(UTC) + timedelta(minutes=5),
        shapes=shapes,
        allowances=allowances,
    )


def _proposal_with_candidate(
    proposal: ExecutableAdmissionPlanProposalV2,
    candidate: CandidateBindingV2,
) -> ExecutableAdmissionPlanProposalV2:
    return proposal.model_copy(
        update={
            "shapes": tuple(
                shape.model_copy(
                    update={
                        "binding": shape.binding.model_copy(
                            update={"candidate": candidate}
                        )
                    }
                )
                for shape in proposal.shapes
            )
        }
    )


def _attempt(
    protected_attempt_id: UUID,
    *,
    lifecycle_sequence: int = 0,
    assignment: dict[str, object] | None = None,
) -> GuardLifecycleDemandAttemptV2:
    requirements = SealedRequirementsV1(
        os="linux",
        cpu_arch="x86_64",
        gpu_vendor="none",
        network_policies=("public",),
        required_pool="oldlab",
    )
    return GuardLifecycleDemandAttemptV2(
        protected_attempt_id=protected_attempt_id,
        execution_generation=7,
        requirements=requirements,
        requirements_digest=guard_canonical_digest(requirements),
        lifecycle_sequence=lifecycle_sequence,
        lifecycle_state="assigned" if assignment is not None else "pending-unassigned",
        submit_priority=0,
        submitted_at=datetime(2026, 8, 15, tzinfo=UTC),
        **(assignment or {}),
    )


def _observation(
    configuration: ReporterConfigurationV1,
    *attempts: GuardLifecycleDemandAttemptV2,
) -> GuardLifecycleDemandObservationV2:
    return GuardLifecycleDemandObservationV2(
        **{
            field: getattr(configuration, field)
            for field in AgentRegistrationV1.model_fields
        },
        sequence=1,
        source_observed_at=datetime(2026, 8, 15, tzinfo=UTC),
        attempts=attempts,
    )


def test_pending_attempts_build_one_canonical_stable_convergence() -> None:
    configuration = _configuration()
    proposal = _proposal(configuration)
    observation = _observation(
        configuration,
        *(_attempt(item.protected_attempt_id) for item in proposal.allowances),
    )

    first = _build_protected_admission_convergence(configuration, proposal, observation)
    replay = _build_protected_admission_convergence(configuration, proposal, observation)

    assert first == replay
    assert first.plan.plan_id == proposal.plan_id
    assert first.plan.manager_allocation_digest == proposal.manager_allocation_digest
    assert tuple(item.allowance_id for item in first.plan.placement_allowances) == tuple(
        item.allowance_id for item in proposal.allowances
    )
    assert tuple(item.expected_transition_sequence for item in first.transitions) == (0, 0)
    assert tuple(item.lifecycle_sequence for item in first.work.acknowledgement.assignments) == (
        1,
        1,
    )
    assert first.work.acknowledgement.assignment_count == 2


def test_no_allowance_plan_still_prepares_exact_empty_acknowledgement() -> None:
    configuration = _configuration()
    proposal = _proposal(configuration, allowance_count=0)

    prepared = _build_protected_admission_convergence(
        configuration,
        proposal,
        _observation(configuration),
    )

    assert prepared.plan.placement_allowances == ()
    assert prepared.transitions == ()
    assert prepared.work.acknowledgement.assignments == ()
    assert prepared.work.acknowledgement.assignment_count == 0


def test_cleanup_work_builds_exact_stable_manager_acknowledgement() -> None:
    """Cleanup publication must bind the exact closure and guard abandonment digest."""

    configuration = _configuration()
    proposal = _proposal(configuration, allowance_count=0)
    closure = ExecutableAdmissionPlanClosureV2(
        closure_id=UUID(int=60),
        proposal=proposal,
        close_reason="allocation-superseded",
    )
    anchor = proposal.shapes[0].binding
    abandonment = AbandonedAdmissionPlanV1(
        **{
            field: getattr(configuration, field)
            for field in AgentRegistrationV1.model_fields
        },
        closure_id=closure.closure_id,
        proposal_id=proposal.proposal_id,
        proposal_digest=canonical_executable_digest(proposal),
        plan_id=proposal.plan_id,
        admission_incarnation=proposal.admission_incarnation,
        manager_authority_incarnation=anchor.execution.authority_incarnation,
        manager_writer_epoch=anchor.execution.writer_epoch,
        manager_allocation_epoch=anchor.execution.allocation_epoch,
        manager_input_digest=proposal.manager_input_digest,
        manager_allocation_digest=proposal.manager_allocation_digest,
        pool_id="oldlab",
        close_reason=closure.close_reason,
    )
    cleanup = ProtectedAdmissionPlanCleanupWork(closure=closure, disposition=abandonment)

    acknowledgement = cleanup.acknowledgement
    assert acknowledgement.closure_id == closure.closure_id
    assert acknowledgement.proposal_digest == canonical_executable_digest(proposal)
    assert acknowledgement.disposition_kind == "abandoned"
    assert acknowledgement.disposition_digest == guard_canonical_digest(abandonment)
    assert cleanup.idempotency_key == uuid5(
        NAMESPACE_URL,
        "loom:protected-executable-admission-cleanup:"
        f"{canonical_executable_digest(acknowledgement)}",
    )

    with pytest.raises(ProtectedAdmissionPlanError, match="cleanup evidence changed"):
        ProtectedAdmissionPlanCleanupWork(
            closure=closure,
            disposition=abandonment.model_copy(update={"closure_id": UUID(int=61)}),
        )


def test_never_converged_cleanup_binds_full_closure_and_registration_digest() -> None:
    """Negative evidence is exact, replay-stable, and cannot outlive changed inputs."""

    configuration = _configuration()
    proposal = _proposal(configuration, allowance_count=0)
    closure = ExecutableAdmissionPlanClosureV2(
        closure_id=UUID(int=60),
        proposal=proposal,
        close_reason="allocation-superseded",
    )
    registration = AgentRegistrationV1.model_validate(
        {
            field: getattr(configuration, field)
            for field in AgentRegistrationV1.model_fields
        }
    )
    tombstone = NeverConvergedAdmissionPlanV1(
        **registration.model_dump(mode="python", exclude_none=False),
        registration_digest=guard_canonical_digest(registration),
        closure=closure,
        closure_digest=canonical_executable_digest(closure),
        proposal_digest=canonical_executable_digest(proposal),
    )
    cleanup = ProtectedAdmissionPlanCleanupWork(
        closure=closure,
        disposition=tombstone,
    )

    assert cleanup.acknowledgement.disposition_kind == "never-converged"
    assert cleanup.acknowledgement.disposition_digest == guard_canonical_digest(tombstone)
    assert NeverConvergedAdmissionPlanV1.model_validate_json(
        tombstone.model_dump_json()
    ) == tombstone

    with pytest.raises(ValueError, match="closure digest"):
        NeverConvergedAdmissionPlanV1(
            **tombstone.model_dump(
                mode="python",
                exclude={"closure_digest"},
            ),
            closure_digest="f" * 64,
        )
    with pytest.raises(ValueError, match="registration digest"):
        NeverConvergedAdmissionPlanV1(
            **tombstone.model_dump(
                mode="python",
                exclude={"registration_digest"},
            ),
            registration_digest="f" * 64,
        )


@pytest.mark.parametrize(
    "changed_binding",
    (
        {"deployment_generation": 8},
        {
            "candidate": CandidateBindingV2(
                algorithm="git-sha1",
                identity="1" * 40,
                publication_sha256="a" * 64,
            )
        },
        {"pool_id": "gb10"},
    ),
)
def test_never_converged_cleanup_rejects_untrusted_local_binding(
    changed_binding: dict[str, object],
) -> None:
    """Irreversible negative evidence must bind every trusted local fact."""

    configuration = _configuration()
    proposal = _proposal(configuration, allowance_count=0)
    changed_shapes = tuple(
        shape.model_copy(
            update={
                "binding": shape.binding.model_copy(update=changed_binding),
            }
        )
        for shape in proposal.shapes
    )
    changed = proposal.model_copy(update={"shapes": changed_shapes})
    with pytest.raises(ProtectedAdmissionPlanError, match="trusted configuration"):
        _assert_trusted_proposal_binding(
            configuration,
            changed,
            require_unexpired=False,
        )


def test_never_converged_cleanup_rejects_changed_protected_admission_root() -> None:
    """The proposal's protected root is part of the trusted local binding."""

    configuration = _configuration()
    proposal = _proposal(configuration, allowance_count=0).model_copy(
        update={"protected_admission_sha256": "f" * 64}
    )
    with pytest.raises(ProtectedAdmissionPlanError, match="trusted configuration"):
        _assert_trusted_proposal_binding(
            configuration,
            proposal,
            require_unexpired=False,
        )


def test_source_candidate_identity_can_differ_from_publication_sha256() -> None:
    """Comparing publication SHA to the legacy digest rejects valid source candidates."""

    configuration = _configuration(
        candidate_digest="1" * 64,
        candidate_identity="1" * 64,
        candidate_publication_sha256="2" * 64,
    )
    proposal = _proposal(configuration, allowance_count=0)

    prepared = _build_protected_admission_convergence(
        configuration,
        proposal,
        _observation(configuration),
    )

    assert prepared.proposal.shapes[0].binding.candidate.identity == "1" * 64
    assert prepared.proposal.shapes[0].binding.candidate.publication_sha256 == "2" * 64


def test_manager_candidate_identity_forgery_is_rejected() -> None:
    """Checking only publication SHA accepts forged manager algorithm/identity."""

    configuration = _configuration()
    forged = _proposal_with_candidate(
        _proposal(configuration, allowance_count=0),
        CandidateBindingV2(
            algorithm="git-sha1",
            identity="1" * 40,
            publication_sha256=configuration.candidate_publication_sha256,
        ),
    )

    with pytest.raises(ProtectedAdmissionPlanError, match="candidate"):
        _build_protected_admission_convergence(
            configuration,
            forged,
            _observation(configuration),
        )


def test_exact_assigned_observation_replays_original_transition() -> None:
    configuration = _configuration()
    proposal = _proposal(configuration, allowance_count=1)
    pending = _build_protected_admission_convergence(
        configuration,
        proposal,
        _observation(configuration, _attempt(proposal.allowances[0].protected_attempt_id)),
    )
    allowance = pending.plan.placement_allowances[0]
    shape = pending.plan.worker_shapes[0]
    assigned = _attempt(
        allowance.protected_attempt_id,
        lifecycle_sequence=1,
        assignment={
            "allowance_id": allowance.allowance_id,
            "plan_id": pending.plan.plan_id,
            "admission_incarnation": pending.plan.admission_incarnation,
            "manager_allocation_epoch": pending.plan.manager_allocation_epoch,
            "pool_id": pending.plan.pool_id,
            "pool_generation": pending.plan.pool_generation,
            "profile_id": pending.plan.profile_id,
            "profile_generation": pending.plan.profile_generation,
            "profile_digest": pending.plan.profile_digest,
            "shape_id": shape.worker_shape.shape_id,
            "shape_instance_id": allowance.shape_instance_id,
            "submission_intent_id": allowance.submission_intent_id,
        },
    )

    replay = _build_protected_admission_convergence(
        configuration,
        proposal,
        _observation(configuration, assigned),
    )

    assert replay.transitions[0] == pending.transitions[0]
    assert replay.work == pending.work


def test_missing_or_different_assigned_attempt_is_rejected() -> None:
    configuration = _configuration()
    proposal = _proposal(configuration, allowance_count=1)
    with pytest.raises(ProtectedAdmissionPlanError, match="missing"):
        _build_protected_admission_convergence(
            configuration,
            proposal,
            _observation(configuration),
        )

    pending = _build_protected_admission_convergence(
        configuration,
        proposal,
        _observation(configuration, _attempt(proposal.allowances[0].protected_attempt_id)),
    )
    allowance = pending.plan.placement_allowances[0]
    shape = pending.plan.worker_shapes[0]
    conflicting = _attempt(
        allowance.protected_attempt_id,
        lifecycle_sequence=1,
        assignment={
            "allowance_id": allowance.allowance_id,
            "plan_id": UUID(int=99),
            "admission_incarnation": pending.plan.admission_incarnation,
            "manager_allocation_epoch": pending.plan.manager_allocation_epoch,
            "pool_id": pending.plan.pool_id,
            "pool_generation": pending.plan.pool_generation,
            "profile_id": pending.plan.profile_id,
            "profile_generation": pending.plan.profile_generation,
            "profile_digest": pending.plan.profile_digest,
            "shape_id": shape.worker_shape.shape_id,
            "shape_instance_id": allowance.shape_instance_id,
            "submission_intent_id": allowance.submission_intent_id,
        },
    )
    with pytest.raises(ProtectedAdmissionPlanError, match="different assignment"):
        _build_protected_admission_convergence(
            configuration,
            proposal,
            _observation(configuration, conflicting),
        )
