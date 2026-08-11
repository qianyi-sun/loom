"""Strict contracts for the candidate-independent demand reporter."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from loom_capacity_agent.contracts import (
    AgentPoolCapabilityV1,
    AgentRegistrationV1,
    GuardDemandAttemptV1,
    GuardDemandObservationV1,
    ReporterConfigurationV1,
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
        deployment_generation=1,
        configuration_generation=1,
    )


def _requirements() -> SealedRequirementsV1:
    return SealedRequirementsV1(
        os="linux",
        cpu_arch="any",
        gpu_vendor="none",
        network_policies=("public",),
    )


def _attempt() -> GuardDemandAttemptV1:
    requirements = _requirements()
    return GuardDemandAttemptV1(
        protected_attempt_id=uuid4(),
        execution_generation=1,
        requirements=requirements,
        requirements_digest=canonical_digest(requirements),
        submit_priority=100,
        submitted_at=datetime(2026, 8, 10, 12, tzinfo=UTC),
    )


def test_agent_registration_is_disabled_and_generation_bound() -> None:
    registration = _registration()
    assert registration.authority_mode == "disabled"
    assert registration.allocation_epoch == 0
    assert registration.reporter_high_water == 0
    with pytest.raises(ValidationError):
        AgentRegistrationV1.model_validate(
            {**registration.model_dump(mode="python"), "authority_mode": "global"}
        )


def test_pool_capabilities_are_canonical_and_have_no_weight() -> None:
    value = AgentPoolCapabilityV1(
        capability_id="oldlab-x86-none",
        pool_id="oldlab",
        operating_system="linux",
        cpu_architecture="x86_64",
        gpu_vendor="none",
        network_policies=("public", "allowlist"),
    )
    assert value.network_policies == ("allowlist", "public")
    assert "weight" not in AgentPoolCapabilityV1.model_fields
    with pytest.raises(ValidationError):
        AgentPoolCapabilityV1.model_validate(
            {**value.model_dump(mode="python"), "network_policies": ("public", "public")}
        )


def test_reporter_configuration_allows_multiple_exact_offers_per_pool() -> None:
    registration = _registration()
    pool = AgentPoolCapabilityV1(
        capability_id="gb10-arm-none",
        pool_id="gb10",
        operating_system="linux",
        cpu_architecture="arm64",
        gpu_vendor="none",
        network_policies=("public",),
    )
    other = pool.model_copy(update={"capability_id": "gb10-arm-nvidia", "gpu_vendor": "nvidia"})
    configuration = ReporterConfigurationV1(
        **registration.model_dump(mode="python"),
        pool_capabilities=(other, pool),
    )
    assert tuple(item.capability_id for item in configuration.pool_capabilities) == (
        "gb10-arm-none",
        "gb10-arm-nvidia",
    )
    with pytest.raises(ValidationError, match="duplicate pool"):
        ReporterConfigurationV1(
            **registration.model_dump(mode="python"),
            pool_capabilities=(pool, pool),
        )
    duplicate_offer = pool.model_copy(update={"capability_id": "gb10-arm-none-copy"})
    with pytest.raises(ValidationError, match="semantic pool"):
        ReporterConfigurationV1(
            **registration.model_dump(mode="python"),
            pool_capabilities=(pool, duplicate_offer),
        )


def test_attempt_binds_exact_sealed_requirements_and_utc_time() -> None:
    attempt = _attempt()
    assert attempt.submitted_at.tzinfo == UTC
    assert attempt.claim_state == "queued"
    with pytest.raises(ValidationError, match="digest"):
        GuardDemandAttemptV1.model_validate(
            {**attempt.model_dump(mode="python"), "requirements_digest": "b" * 64}
        )
    with pytest.raises(ValidationError, match="timezone"):
        GuardDemandAttemptV1(
            **{
                **attempt.model_dump(mode="python"),
                "submitted_at": datetime(2026, 8, 10, 12),
            }
        )
    with pytest.raises(ValidationError):
        GuardDemandAttemptV1.model_validate(
            {**attempt.model_dump(mode="python"), "claim_state": "assigned"}
        )
    with pytest.raises(ValidationError):
        GuardDemandAttemptV1.model_validate(
            {**attempt.model_dump(mode="python"), "assigned_pool": "oldlab"}
        )


def test_observation_is_complete_disabled_and_has_unique_attempts() -> None:
    registration = _registration()
    attempt = _attempt()
    values = {
        **registration.model_dump(mode="python"),
        "sequence": 1,
        "source_observed_at": datetime(2026, 8, 10, 13, tzinfo=UTC),
        "attempts": (attempt,),
    }
    observation = GuardDemandObservationV1(**values)
    assert observation.view_state == "complete"
    assert observation.authority_mode == "disabled"
    with pytest.raises(ValidationError, match="duplicate protected attempt"):
        GuardDemandObservationV1(**{**values, "attempts": (attempt, attempt)})
    with pytest.raises(ValidationError):
        GuardDemandObservationV1(**{**values, "sequence": 0})
