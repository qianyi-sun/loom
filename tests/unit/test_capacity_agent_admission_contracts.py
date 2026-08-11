"""Strict zero-executable prepared-admission contracts."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from loom_capacity_agent.admission import (
    PreparedAdmissionPlanV1,
    PreparedBootstrapBindingV1,
    PreparedPlacementAllowanceV1,
    PreparedWorkerBindingV1,
    PreparedWorkerShapeV1,
)
from loom_capacity_agent.contracts import AgentRegistrationV1
from loom_capacity_manager.contracts import ResourceVectorV1, WorkerShapeV1, canonical_digest


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


def _shape() -> PreparedWorkerShapeV1:
    worker_shape = WorkerShapeV1(
        shape_id="oldlab-x86-none-2",
        concurrency_slots=2,
        total_resources=ResourceVectorV1(
            slots=2,
            cpu_millicores=4000,
            memory_bytes=8_000_000_000,
        ),
        node_resources=(
            ResourceVectorV1(
                slots=2,
                cpu_millicores=4000,
                memory_bytes=8_000_000_000,
            ),
        ),
        compatible_domain_ids=("oldlab-x86",),
        capabilities=(
            "cpu_arch.x86_64",
            "gpu_vendor.none",
            "network.public",
            "os.linux",
        ),
    )
    return PreparedWorkerShapeV1(
        shape_instance_id="shape-oldlab-0001",
        submission_intent_id=uuid4(),
        pool_id="oldlab",
        pool_generation=3,
        profile_id="dev-oldlab",
        profile_generation=5,
        profile_digest="b" * 64,
        protocol_generation=2,
        protocol_digest="c" * 64,
        worker_shape=worker_shape,
        worker_shape_digest=canonical_digest(worker_shape),
        bootstrap_registration_epoch=1,
    )


def _allowance(shape: PreparedWorkerShapeV1, *, slot_index: int = 0):
    return PreparedPlacementAllowanceV1(
        allowance_id=uuid4(),
        protected_attempt_id=uuid4(),
        execution_generation=1,
        requirements_digest="d" * 64,
        pool_id=shape.pool_id,
        shape_instance_id=shape.shape_instance_id,
        shape_slot_index=slot_index,
        submission_intent_id=shape.submission_intent_id,
    )


def _plan() -> PreparedAdmissionPlanV1:
    registration = _registration()
    shape = _shape()
    return PreparedAdmissionPlanV1(
        **registration.model_dump(mode="python"),
        plan_id=uuid4(),
        admission_incarnation=uuid4(),
        manager_authority_incarnation=uuid4(),
        manager_writer_epoch=0,
        manager_allocation_epoch=1,
        manager_input_digest="e" * 64,
        manager_allocation_digest="f" * 64,
        pool_id=shape.pool_id,
        pool_generation=shape.pool_generation,
        profile_id=shape.profile_id,
        profile_generation=shape.profile_generation,
        profile_digest=shape.profile_digest,
        protocol_generation=shape.protocol_generation,
        protocol_digest=shape.protocol_digest,
        lease_not_after=datetime(2026, 8, 10, 13, tzinfo=UTC),
        worker_shapes=(shape,),
        placement_allowances=(_allowance(shape),),
    )


def test_prepared_plan_is_disabled_nonexecutable_and_exactly_bound() -> None:
    plan = _plan()
    assert plan.authority_mode == "disabled"
    assert plan.allocation_epoch == 0
    assert plan.plan_state == "prepared"
    assert plan.executable is False
    assert plan.worker_shapes[0].shape_state == "prepared"
    assert plan.placement_allowances[0].allowance_state == "prepared"
    for field, value in (
        ("authority_mode", "global"),
        ("allocation_epoch", 1),
        ("plan_state", "accepted"),
        ("executable", True),
    ):
        with pytest.raises(ValidationError):
            PreparedAdmissionPlanV1.model_validate(
                {**plan.model_dump(mode="python"), field: value}
            )
    with pytest.raises(ValidationError, match="identities"):
        PreparedAdmissionPlanV1.model_validate(
            {
                **plan.model_dump(mode="python"),
                "admission_incarnation": plan.agent_incarnation,
            }
        )


def test_plan_rejects_cross_pool_profile_shape_or_intent_bindings() -> None:
    plan = _plan()
    shape = plan.worker_shapes[0]
    allowance = plan.placement_allowances[0]
    corruptions = (
        ("worker_shapes", (shape.model_copy(update={"pool_id": "gb10"}),)),
        (
            "worker_shapes",
            (shape.model_copy(update={"profile_digest": "0" * 64}),),
        ),
        (
            "placement_allowances",
            (allowance.model_copy(update={"submission_intent_id": uuid4()}),),
        ),
        (
            "placement_allowances",
            (allowance.model_copy(update={"shape_instance_id": "missing-shape"}),),
        ),
    )
    for field, value in corruptions:
        with pytest.raises(ValidationError):
            PreparedAdmissionPlanV1.model_validate(
                {**plan.model_dump(mode="python"), field: value}
            )


def test_plan_rejects_duplicate_attempts_allowances_and_physical_slots() -> None:
    plan = _plan()
    shape = plan.worker_shapes[0]
    first = plan.placement_allowances[0]
    same_attempt = _allowance(shape, slot_index=1).model_copy(
        update={"protected_attempt_id": first.protected_attempt_id}
    )
    same_slot = _allowance(shape, slot_index=0)
    duplicate_id = _allowance(shape, slot_index=1).model_copy(
        update={"allowance_id": first.allowance_id}
    )
    for allowances in ((first, same_attempt), (first, same_slot), (first, duplicate_id)):
        with pytest.raises(ValidationError, match="duplicate"):
            PreparedAdmissionPlanV1.model_validate(
                {**plan.model_dump(mode="python"), "placement_allowances": allowances}
            )


def test_allowance_slot_must_fit_the_exact_prepared_shape() -> None:
    plan = _plan()
    shape = plan.worker_shapes[0]
    outside = _allowance(shape, slot_index=shape.worker_shape.concurrency_slots)
    with pytest.raises(ValidationError, match="slot"):
        PreparedAdmissionPlanV1.model_validate(
            {**plan.model_dump(mode="python"), "placement_allowances": (outside,)}
        )


def test_worker_shape_digest_is_exact() -> None:
    shape = _shape()
    with pytest.raises(ValidationError, match="digest"):
        PreparedWorkerShapeV1.model_validate(
            {**shape.model_dump(mode="python"), "worker_shape_digest": "0" * 64}
        )


def test_bootstrap_and_worker_bindings_contain_hashes_but_no_usable_secret() -> None:
    plan = _plan()
    shape = plan.worker_shapes[0]
    bootstrap = PreparedBootstrapBindingV1(
        **{
            field: getattr(plan, field)
            for field in AgentRegistrationV1.model_fields
        },
        bootstrap_id=uuid4(),
        plan_id=plan.plan_id,
        admission_incarnation=plan.admission_incarnation,
        manager_allocation_epoch=plan.manager_allocation_epoch,
        pool_id=plan.pool_id,
        shape_instance_id=shape.shape_instance_id,
        submission_intent_id=shape.submission_intent_id,
        bootstrap_registration_epoch=shape.bootstrap_registration_epoch,
        bootstrap_digest="1" * 64,
        expires_at=plan.lease_not_after,
    )
    worker = PreparedWorkerBindingV1(
        **{
            field: getattr(plan, field)
            for field in AgentRegistrationV1.model_fields
        },
        worker_id=uuid4(),
        worker_incarnation=uuid4(),
        bootstrap_id=bootstrap.bootstrap_id,
        plan_id=plan.plan_id,
        admission_incarnation=plan.admission_incarnation,
        manager_allocation_epoch=plan.manager_allocation_epoch,
        pool_id=plan.pool_id,
        shape_instance_id=shape.shape_instance_id,
        submission_intent_id=shape.submission_intent_id,
        bootstrap_registration_epoch=shape.bootstrap_registration_epoch,
        slurm_job_id="oldlab-12345",
        ownership_evidence_digest="2" * 64,
        worker_credential_digest="3" * 64,
    )
    assert bootstrap.executable is False
    assert worker.claim_authorization_epoch == 0
    assert worker.worker_state == "prepared"
    assert "secret" not in PreparedBootstrapBindingV1.model_fields
    assert "token" not in PreparedWorkerBindingV1.model_fields
    with pytest.raises(ValidationError):
        PreparedWorkerBindingV1.model_validate(
            {**worker.model_dump(mode="python"), "claim_authorization_epoch": 1}
        )
