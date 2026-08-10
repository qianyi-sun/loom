"""Fail-closed conversion from protected observations to manager demand."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from loom_capacity_agent.contracts import (
    AgentPoolCapabilityV1,
    AgentRegistrationV1,
    GuardDemandAttemptV1,
    GuardDemandObservationV1,
    ReporterConfigurationV1,
)
from loom_capacity_agent.reporter import DemandReportBlockedError, build_demand_snapshot
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
                network_policies=("public", "allowlist"),
            ),
            AgentPoolCapabilityV1(
                capability_id="gb10-arm-none",
                pool_id="gb10",
                operating_system="linux",
                cpu_architecture="arm64",
                gpu_vendor="none",
                network_policies=("public", "allowlist"),
            ),
        ),
    )


def _attempt(
    *,
    cpu_arch: str = "any",
    required_pool: str | None = None,
    priority: int = 100,
    submitted_at: datetime | None = None,
) -> GuardDemandAttemptV1:
    requirements = SealedRequirementsV1.model_validate(
        {
            "os": "linux",
            "cpu_arch": cpu_arch,
            "gpu_vendor": "none",
            "network_policies": ("public",),
            "required_pool": required_pool,
        }
    )
    return GuardDemandAttemptV1(
        protected_attempt_id=uuid4(),
        execution_generation=1,
        requirements=requirements,
        requirements_digest=canonical_digest(requirements),
        submit_priority=priority,
        submitted_at=submitted_at or datetime(2026, 8, 10, 12, tzinfo=UTC),
    )


def _observation(
    registration: AgentRegistrationV1,
    *attempts: GuardDemandAttemptV1,
) -> GuardDemandObservationV1:
    return GuardDemandObservationV1(
        **registration.model_dump(mode="python"),
        sequence=3,
        source_observed_at=datetime(2026, 8, 10, 13, tzinfo=UTC),
        attempts=attempts,
    )


def test_arch_specific_neutral_and_pinned_demand_get_exact_eligible_pools() -> None:
    registration = _registration()
    attempts = (
        _attempt(cpu_arch="x86_64"),
        _attempt(cpu_arch="arm64"),
        _attempt(cpu_arch="any"),
        _attempt(cpu_arch="any", required_pool="gb10"),
    )
    report = build_demand_snapshot(_observation(registration, *attempts), _configuration(registration))
    by_attempt = {
        bucket.attempt_ids[0]: bucket.eligible_pool_ids for bucket in report.pending_unassigned
    }
    assert by_attempt[str(attempts[0].protected_attempt_id)] == ("oldlab",)
    assert by_attempt[str(attempts[1].protected_attempt_id)] == ("gb10",)
    assert by_attempt[str(attempts[2].protected_attempt_id)] == ("gb10", "oldlab")
    assert by_attempt[str(attempts[3].protected_attempt_id)] == ("gb10",)
    neutral_bucket = next(
        bucket
        for bucket in report.pending_unassigned
        if bucket.attempt_ids == (str(attempts[2].protected_attempt_id),)
    )
    pinned_neutral_bucket = next(
        bucket
        for bucket in report.pending_unassigned
        if bucket.attempt_ids == (str(attempts[3].protected_attempt_id),)
    )
    assert "cpu_arch.any" not in neutral_bucket.required_capabilities
    assert "cpu_arch.any" not in pinned_neutral_bucket.required_capabilities
    assert not any(
        capability.startswith("required_pool.")
        for capability in pinned_neutral_bucket.required_capabilities
    )
    x86_bucket = next(
        bucket
        for bucket in report.pending_unassigned
        if bucket.attempt_ids == (str(attempts[0].protected_attempt_id),)
    )
    assert "cpu_arch.x86_64" in x86_bucket.required_capabilities
    assert report.current_assignments == ()
    assert report.fixed_claims == ()


def test_equivalent_attempts_group_deterministically_without_pool_weights() -> None:
    registration = _registration()
    oldest = datetime(2026, 8, 10, 10, tzinfo=UTC)
    attempts = (
        _attempt(submitted_at=oldest + timedelta(minutes=1)),
        _attempt(submitted_at=oldest),
    )
    left = build_demand_snapshot(_observation(registration, *attempts), _configuration(registration))
    right = build_demand_snapshot(
        _observation(registration, *reversed(attempts)), _configuration(registration)
    )
    assert left == right
    assert len(left.pending_unassigned) == 1
    bucket = left.pending_unassigned[0]
    assert bucket.requested_slots == 2
    assert bucket.oldest_submitted_at == oldest
    assert bucket.attempt_ids == tuple(sorted(str(item.protected_attempt_id) for item in attempts))
    assert not any("weight" in capability for capability in bucket.required_capabilities)


def test_distinct_execution_generations_do_not_share_a_demand_bucket() -> None:
    registration = _registration()
    first = _attempt()
    second = GuardDemandAttemptV1.model_validate(
        {
            **_attempt().model_dump(mode="python"),
            "execution_generation": 2,
        }
    )
    report = build_demand_snapshot(
        _observation(registration, first, second), _configuration(registration)
    )
    assert len(report.pending_unassigned) == 2


def test_binding_mismatch_or_unmappable_requirement_produces_no_report() -> None:
    registration = _registration()
    configuration = _configuration(registration)
    observation = _observation(registration, _attempt())
    stale = configuration.model_copy(
        update={"configuration_generation": configuration.configuration_generation + 1}
    )
    with pytest.raises(DemandReportBlockedError, match="binding"):
        build_demand_snapshot(observation, stale)

    windows = _attempt()
    unsupported = GuardDemandAttemptV1.model_validate(
        {
            **windows.model_dump(mode="python"),
            "requirements": windows.requirements.model_copy(update={"os": "windows"}),
            "requirements_digest": canonical_digest(
                windows.requirements.model_copy(update={"os": "windows"})
            ),
        }
    )
    with pytest.raises(DemandReportBlockedError, match="no compatible pool"):
        build_demand_snapshot(
            _observation(registration, unsupported),
            configuration,
        )


def test_capability_offers_do_not_create_false_cross_product_eligibility() -> None:
    registration = _registration()
    base = _configuration(registration)
    cross_product_trap = ReporterConfigurationV1(
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
                capability_id="oldlab-arm-nvidia",
                pool_id="oldlab",
                operating_system="linux",
                cpu_architecture="arm64",
                gpu_vendor="nvidia",
                network_policies=("public",),
            ),
        ),
    )
    requirements = SealedRequirementsV1(
        os="linux",
        cpu_arch="x86_64",
        gpu_vendor="nvidia",
        network_policies=("public",),
    )
    attempt = GuardDemandAttemptV1(
        protected_attempt_id=uuid4(),
        execution_generation=1,
        requirements=requirements,
        requirements_digest=canonical_digest(requirements),
        submit_priority=100,
        submitted_at=datetime(2026, 8, 10, 12, tzinfo=UTC),
    )
    assert base.pool_capabilities
    with pytest.raises(DemandReportBlockedError, match="no compatible pool"):
        build_demand_snapshot(_observation(registration, attempt), cross_product_trap)


def test_complete_empty_observation_is_distinct_from_failure() -> None:
    registration = _registration()
    report = build_demand_snapshot(_observation(registration), _configuration(registration))
    assert report.pending_unassigned == ()
    assert report.sequence == 3
