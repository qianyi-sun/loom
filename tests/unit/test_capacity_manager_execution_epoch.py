from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import ValidationError

from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutionActivationV2,
    ExecutionAuthorityV2,
    ExecutionPreparationPolicyV2,
    ExecutionPreparationV2,
    LegacyWriterFenceV2,
    PoolControllerAuthorityV2,
    PreparedExecutorBindingV2,
    SubjectExecutionAcknowledgementV2,
    canonical_executable_digest,
)

_SUBJECT_ID = UUID(int=30)


def _executor(pool_id: str) -> PreparedExecutorBindingV2:
    number = 10 if pool_id == "oldlab" else 20
    return PreparedExecutorBindingV2(
        pool_id=pool_id,
        pool_generation=2,
        executor_id=f"{pool_id}-executor",
        executor_incarnation=UUID(int=number),
        signing_key_sha256=("a" if pool_id == "oldlab" else "b") * 64,
        local_authority_sha256=("c" if pool_id == "oldlab" else "d") * 64,
        controller_authority_sha256=("e" if pool_id == "oldlab" else "f") * 64,
    )


def _subject(subject_id: UUID = _SUBJECT_ID) -> SubjectExecutionAcknowledgementV2:
    return SubjectExecutionAcknowledgementV2(
        subject_id=subject_id,
        subject_incarnation=UUID(int=31),
        configuration_generation=3,
        deployment_generation=4,
        candidate=CandidateBindingV2(
            algorithm="source-sha256",
            identity="1" * 64,
            publication_sha256="2" * 64,
        ),
        reporter_incarnation=UUID(int=32),
        protected_admission_sha256="3" * 64,
        legacy_writer_high_water=5,
        acknowledgement_sha256="4" * 64,
    )


def _legacy(
    writer_id: str = "legacy-oldlab",
    *,
    scope_id: str = "oldlab",
) -> LegacyWriterFenceV2:
    return LegacyWriterFenceV2(
        writer_id=writer_id,
        writer_kind="submission",
        scope_kind="pool",
        scope_id=scope_id,
        high_water=8,
        freeze_evidence_sha256="5" * 64,
        state="frozen",
    )


def _preparation() -> ExecutionPreparationV2:
    return ExecutionPreparationV2(
        authority_incarnation=UUID(int=1),
        expected_writer_epoch=2,
        configuration_epoch=3,
        fleet_generation=4,
        fleet_digest="6" * 64,
        trusted_fleet_release_sha256="7" * 64,
        requested_ceiling=1,
        requested_rate_per_minute=1,
        executors=(_executor("oldlab"), _executor("gb10")),
        subject_acknowledgements=(_subject(),),
        legacy_writer_fences=(_legacy(),),
        rollback_evidence_sha256="8" * 64,
    )


def test_execution_preparation_is_canonical_and_binds_both_pools() -> None:
    """Dropping either controller or canonical ordering must fail this test."""

    request = _preparation()
    assert [item.pool_id for item in request.executors] == ["gb10", "oldlab"]
    assert request.requested_ceiling == 1
    assert request.executable is True
    assert canonical_executable_digest(request) == canonical_executable_digest(
        ExecutionPreparationV2.model_validate_json(request.model_dump_json())
    )


def test_execution_preparation_rejects_missing_or_duplicate_pool() -> None:
    """Preparing one controller as the global fleet must fail this test."""

    payload = _preparation().model_dump(mode="python")
    with pytest.raises(ValidationError):
        ExecutionPreparationV2.model_validate(payload | {"executors": (_executor("gb10"),)})
    with pytest.raises(ValidationError, match="exactly gb10 and oldlab"):
        ExecutionPreparationV2.model_validate(
            payload | {"executors": (_executor("oldlab"), _executor("oldlab"))}
        )


def test_execution_preparation_rejects_duplicate_subject_or_legacy_writer() -> None:
    """Incomplete identity sets hidden by duplicates must fail this test."""

    payload = _preparation().model_dump(mode="python")
    with pytest.raises(ValidationError, match="duplicate subject acknowledgement"):
        ExecutionPreparationV2.model_validate(
            payload | {"subject_acknowledgements": (_subject(), _subject())}
        )
    with pytest.raises(ValidationError, match="duplicate legacy writer"):
        ExecutionPreparationV2.model_validate(
            payload | {"legacy_writer_fences": (_legacy(), _legacy())}
        )


def test_legacy_writer_identity_is_scoped_and_policy_is_complete() -> None:
    """Collapsing the same mutation path across environments must fail this test."""

    alice = _legacy("queued-to-claimed", scope_id="dev-alice")
    bob = _legacy("queued-to-claimed", scope_id="dev-bob")
    payload = _preparation().model_dump(mode="python")
    preparation = ExecutionPreparationV2.model_validate(
        payload | {"legacy_writer_fences": (bob, alice)}
    )
    assert [item.scope_id for item in preparation.legacy_writer_fences] == [
        "dev-alice",
        "dev-bob",
    ]

    policy = ExecutionPreparationPolicyV2(
        trusted_fleet_release_sha256="7" * 64,
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
        executors=preparation.executors,
        subject_acknowledgements=(_subject(),),
        rollback_evidence_sha256="8" * 64,
        controller_authorities=(
            PoolControllerAuthorityV2(
                pool_id="oldlab",
                controller_authority_sha256="e" * 64,
            ),
            PoolControllerAuthorityV2(
                pool_id="gb10",
                controller_authority_sha256="f" * 64,
            ),
        ),
        legacy_writer_fences=tuple(reversed(preparation.legacy_writer_fences)),
    )
    assert [item.pool_id for item in policy.controller_authorities] == ["gb10", "oldlab"]
    assert [item.scope_id for item in policy.legacy_writer_fences] == [
        "dev-alice",
        "dev-bob",
    ]

    with pytest.raises(ValidationError):
        ExecutionPreparationPolicyV2(
            trusted_fleet_release_sha256="7" * 64,
            executable_new_capacity_ceiling=1,
            executable_new_capacity_rate_per_minute=1,
            executors=policy.executors,
            subject_acknowledgements=policy.subject_acknowledgements,
            rollback_evidence_sha256="8" * 64,
            controller_authorities=policy.controller_authorities,
            legacy_writer_fences=(),
        )


def test_legacy_writer_must_be_frozen_at_a_nonnegative_high_water() -> None:
    """An inventoried but live writer must never satisfy preparation."""

    payload = _legacy().model_dump(mode="python")
    with pytest.raises(ValidationError):
        LegacyWriterFenceV2.model_validate(payload | {"state": "active"})
    with pytest.raises(ValidationError):
        LegacyWriterFenceV2.model_validate(payload | {"high_water": -1})


def test_activation_names_exact_prepared_manifest_and_positive_ceiling() -> None:
    """Activation without an exact prepared record must fail validation."""

    request = ExecutionActivationV2(
        authority_incarnation=UUID(int=1),
        expected_writer_epoch=2,
        execution_epoch=9,
        execution_manifest_sha256="9" * 64,
        prepared_readiness_sha256="8" * 64,
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
    )
    assert request.executable_new_capacity_ceiling == 1
    assert request.executable is True
    with pytest.raises(ValidationError):
        ExecutionActivationV2.model_validate(
            request.model_dump(mode="python") | {"executable_new_capacity_ceiling": 0}
        )


def test_drain_only_authority_cannot_retain_a_positive_ceiling() -> None:
    """A drain-only checkpoint that can scale up must fail this test."""

    with pytest.raises(ValidationError, match="drain-only execution authority"):
        ExecutionAuthorityV2(
            authority_incarnation=UUID(int=1),
            writer_epoch=2,
            configuration_epoch=3,
            execution_epoch=4,
            execution_manifest_sha256="9" * 64,
            execution_state="drain-only",
            executable_new_capacity_ceiling=1,
            executable_new_capacity_rate_per_minute=0,
            trusted_fleet_release_sha256="8" * 64,
        )
