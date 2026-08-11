"""Lifecycle-aware, zero-executable demand projection contracts."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from loom_capacity_agent.contracts import (
    AgentPoolCapabilityV1,
    AgentRegistrationV1,
    GuardLifecycleDemandAttemptV2,
    GuardLifecycleDemandObservationV2,
    ReporterConfigurationV1,
)
from loom_capacity_agent.reporter import (
    DemandReportBlockedError,
    build_lifecycle_demand_snapshot,
)
from loom_capacity_guard.contracts import SealedRequirementsV1, canonical_digest


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


def _requirements(*, cpu_arch: str = "x86_64") -> SealedRequirementsV1:
    return SealedRequirementsV1.model_validate(
        {
            "os": "linux",
            "cpu_arch": cpu_arch,
            "gpu_vendor": "none",
            "network_policies": ("public",),
        }
    )


def _pending(*, cpu_arch: str = "x86_64") -> GuardLifecycleDemandAttemptV2:
    requirements = _requirements(cpu_arch=cpu_arch)
    return GuardLifecycleDemandAttemptV2(
        protected_attempt_id=uuid4(),
        execution_generation=1,
        requirements=requirements,
        requirements_digest=canonical_digest(requirements),
        lifecycle_sequence=0,
        lifecycle_state="pending-unassigned",
        submit_priority=100,
        submitted_at=datetime(2026, 8, 10, 12, tzinfo=UTC),
    )


def _assigned(*, cpu_arch: str = "x86_64") -> GuardLifecycleDemandAttemptV2:
    pending = _pending(cpu_arch=cpu_arch)
    return GuardLifecycleDemandAttemptV2.model_validate(
        {
            **pending.model_dump(mode="python"),
            "lifecycle_sequence": 1,
            "lifecycle_state": "assigned",
            "allowance_id": uuid4(),
            "plan_id": uuid4(),
            "admission_incarnation": uuid4(),
            "manager_allocation_epoch": 5,
            "pool_id": "oldlab",
            "pool_generation": 3,
            "profile_id": "dev-oldlab",
            "profile_generation": 4,
            "profile_digest": "b" * 64,
            "shape_id": "oldlab-x86-none-2",
            "shape_instance_id": "shape-oldlab-0001",
            "submission_intent_id": uuid4(),
        }
    )


def _observation(
    registration: AgentRegistrationV1,
    *attempts: GuardLifecycleDemandAttemptV2,
) -> GuardLifecycleDemandObservationV2:
    return GuardLifecycleDemandObservationV2(
        **registration.model_dump(mode="python"),
        sequence=3,
        source_observed_at=datetime(2026, 8, 10, 13, tzinfo=UTC),
        attempts=attempts,
    )


def _configuration(registration: AgentRegistrationV1) -> ReporterConfigurationV1:
    return ReporterConfigurationV1(
        **registration.model_dump(mode="python"),
        pool_capabilities=(
            AgentPoolCapabilityV1(
                capability_id="oldlab-x86-none",
                pool_id="oldlab",
                operating_system="linux",
                cpu_architecture="x86_64",
                gpu_vendor="none",
                network_policies=("public",),
            ),
            AgentPoolCapabilityV1(
                capability_id="gb10-arm-none",
                pool_id="gb10",
                operating_system="linux",
                cpu_architecture="arm64",
                gpu_vendor="none",
                network_policies=("public",),
            ),
        ),
    )


def test_lifecycle_attempt_state_shapes_are_exact_and_nonexecutable() -> None:
    pending = _pending()
    assigned = _assigned()
    assert pending.view_version == 2
    assert pending.lifecycle_state == "pending-unassigned"
    assert pending.allowance_id is None
    assert pending.executable is False
    assert assigned.lifecycle_state == "assigned"
    assert assigned.allowance_id is not None
    assert assigned.pool_id == "oldlab"
    assert assigned.executable is False

    with pytest.raises(ValidationError, match="assignment binding"):
        GuardLifecycleDemandAttemptV2.model_validate(
            {**assigned.model_dump(mode="python"), "profile_digest": None}
        )
    with pytest.raises(ValidationError, match="must not carry an assignment"):
        GuardLifecycleDemandAttemptV2.model_validate(
            {
                **pending.model_dump(mode="python"),
                "allowance_id": uuid4(),
            }
        )
    with pytest.raises(ValidationError):
        GuardLifecycleDemandAttemptV2.model_validate(
            {**assigned.model_dump(mode="python"), "executable": True}
        )
    with pytest.raises(ValidationError, match="identities must be distinct"):
        GuardLifecycleDemandAttemptV2.model_validate(
            {
                **assigned.model_dump(mode="python"),
                "allowance_id": assigned.plan_id,
            }
        )


def test_lifecycle_observation_is_complete_canonical_and_unique() -> None:
    registration = _registration()
    pending = _pending()
    assigned = _assigned()
    observation = _observation(registration, assigned, pending)
    assert observation.view_version == 2
    assert observation.view_state == "complete"
    assert observation.executable is False
    assert tuple(item.protected_attempt_id for item in observation.attempts) == tuple(
        sorted((pending.protected_attempt_id, assigned.protected_attempt_id), key=lambda item: item.hex)
    )
    with pytest.raises(ValidationError, match="duplicate protected attempt"):
        _observation(registration, pending, pending)


def test_lifecycle_report_separates_pending_demand_and_current_assignment() -> None:
    registration = _registration()
    pending = _pending(cpu_arch="x86_64")
    assigned = _assigned(cpu_arch="x86_64")
    report = build_lifecycle_demand_snapshot(
        _observation(registration, pending, assigned),
        _configuration(registration),
    )
    assert len(report.pending_unassigned) == 1
    assert report.pending_unassigned[0].attempt_ids == (str(pending.protected_attempt_id),)
    assert len(report.current_assignments) == 1
    current = report.current_assignments[0]
    assert current.attempt_id == str(assigned.protected_attempt_id)
    assert current.pool_id == "oldlab"
    assert current.pool_generation == assigned.pool_generation
    assert current.profile_id == assigned.profile_id
    assert current.profile_generation == assigned.profile_generation
    assert current.profile_digest == assigned.profile_digest
    assert current.shape_id == assigned.shape_id
    assert current.allowance_epoch == assigned.manager_allocation_epoch
    assert report.fixed_claims == ()


def test_lifecycle_report_rejects_assignment_incompatible_with_requirements() -> None:
    registration = _registration()
    assigned = _assigned(cpu_arch="arm64").model_copy(update={"pool_id": "oldlab"})
    with pytest.raises(DemandReportBlockedError, match="assigned pool is incompatible"):
        build_lifecycle_demand_snapshot(
            _observation(registration, assigned),
            _configuration(registration),
        )


def test_lifecycle_report_defensively_rejects_incomplete_unchecked_assignment() -> None:
    registration = _registration()
    assigned = _assigned().model_copy(update={"profile_digest": None})
    observation = _observation(registration, _assigned()).model_copy(
        update={"attempts": (assigned,)}
    )
    with pytest.raises(DemandReportBlockedError, match="assignment binding is incomplete"):
        build_lifecycle_demand_snapshot(observation, _configuration(registration))
