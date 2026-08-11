"""Strict, inert legacy-compatibility fence contracts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from pydantic import ValidationError

from loom_capacity_agent.contracts import AgentRegistrationV1
from loom_capacity_agent.legacy_fence import (
    LEGACY_MUTATION_INVENTORY_DIGEST,
    LEGACY_MUTATION_PATH_IDS,
    LegacyCompatibilityFreezeV1,
    LegacyCompatibilityPreparationV1,
    LegacyWriterCursorV1,
    LegacyWriterFreezeCursorV1,
)
from loom_capacity_guard.contracts import canonical_digest


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


def _cursors(*, high_water: int = 3) -> tuple[LegacyWriterCursorV1, ...]:
    return tuple(
        LegacyWriterCursorV1(
            mutation_path_id=path_id,
            writer_domain="environment-local",
            writer_incarnation=uuid4(),
            writer_epoch=2,
            high_water=high_water,
            authority_digest="b" * 64,
        )
        for path_id in LEGACY_MUTATION_PATH_IDS
    )


def _preparation() -> LegacyCompatibilityPreparationV1:
    registration = _registration()
    return LegacyCompatibilityPreparationV1(
        **registration.model_dump(mode="python"),
        preparation_id=uuid4(),
        compatibility_incarnation=uuid4(),
        fleet_migration_epoch=1,
        compatibility_not_after=datetime.now(UTC) + timedelta(hours=1),
        mutation_inventory_digest=LEGACY_MUTATION_INVENTORY_DIGEST,
        writer_cursors=_cursors(),
    )


def _freeze(
    preparation: LegacyCompatibilityPreparationV1,
) -> LegacyCompatibilityFreezeV1:
    return LegacyCompatibilityFreezeV1(
        **{field: getattr(preparation, field) for field in AgentRegistrationV1.model_fields},
        freeze_id=uuid4(),
        preparation_id=preparation.preparation_id,
        compatibility_incarnation=preparation.compatibility_incarnation,
        fleet_migration_epoch=preparation.fleet_migration_epoch,
        mutation_inventory_digest=preparation.mutation_inventory_digest,
        preparation_digest=canonical_digest(preparation),
        writer_cursors=tuple(
            LegacyWriterFreezeCursorV1(
                schema_version=cursor.schema_version,
                mutation_path_id=cursor.mutation_path_id,
                writer_domain=cursor.writer_domain,
                writer_incarnation=cursor.writer_incarnation,
                writer_epoch=cursor.writer_epoch,
                high_water=cursor.high_water,
                authority_digest=cursor.authority_digest,
                freeze_acknowledgement_digest="c" * 64,
            )
            for cursor in preparation.writer_cursors
        ),
    )


def test_preparation_requires_the_complete_canonical_inventory() -> None:
    preparation = _preparation()
    assert len(preparation.writer_cursors) == len(LEGACY_MUTATION_PATH_IDS) == 20
    assert tuple(cursor.mutation_path_id for cursor in preparation.writer_cursors) == (
        LEGACY_MUTATION_PATH_IDS
    )
    assert preparation.proposed_authority_mode == "legacy-compatibility"
    assert preparation.compatibility_state == "prepared"
    assert preparation.activation_epoch == 0
    assert preparation.executable is False

    with pytest.raises(ValidationError, match=r"20 items|complete mutation inventory"):
        LegacyCompatibilityPreparationV1.model_validate(
            {
                **preparation.model_dump(mode="python"),
                "writer_cursors": preparation.writer_cursors[:-1],
            }
        )
    with pytest.raises(ValidationError, match="canonical"):
        LegacyCompatibilityPreparationV1.model_validate(
            {
                **preparation.model_dump(mode="python"),
                "writer_cursors": tuple(reversed(preparation.writer_cursors)),
            }
        )


def test_multiple_writer_domains_per_mutation_path_are_preserved() -> None:
    preparation = _preparation()
    extra = preparation.writer_cursors[-1].model_copy(
        update={"writer_domain": "pool-oldlab", "writer_incarnation": uuid4()}
    )
    original = preparation.writer_cursors[-1].model_copy(update={"writer_domain": "pool-gb10"})
    expanded = tuple(
        sorted(
            (*preparation.writer_cursors[:-1], extra, original),
            key=lambda item: (item.mutation_path_id, item.writer_domain),
        )
    )
    value = LegacyCompatibilityPreparationV1.model_validate(
        {**preparation.model_dump(mode="python"), "writer_cursors": expanded}
    )
    assert len(value.writer_cursors) == len(LEGACY_MUTATION_PATH_IDS) + 1
    assert tuple(
        cursor.writer_domain
        for cursor in value.writer_cursors
        if cursor.mutation_path_id == "worker-token-issuance"
    ) == ("pool-gb10", "pool-oldlab")


def test_preparation_rejects_unknown_duplicate_or_drifted_inventory() -> None:
    preparation = _preparation()
    first = preparation.writer_cursors[0]
    with pytest.raises(ValidationError):
        LegacyWriterCursorV1.model_validate(
            {**first.model_dump(mode="python"), "mutation_path_id": "unknown-writer"}
        )
    with pytest.raises(ValidationError):
        LegacyWriterCursorV1.model_validate({**first.model_dump(mode="python"), "writer_epoch": 0})
    with pytest.raises(ValidationError, match="complete mutation inventory"):
        LegacyCompatibilityPreparationV1.model_validate(
            {
                **preparation.model_dump(mode="python"),
                "writer_cursors": (first, *preparation.writer_cursors[1:-1], first),
            }
        )
    with pytest.raises(ValidationError):
        LegacyCompatibilityPreparationV1.model_validate(
            {
                **preparation.model_dump(mode="python"),
                "mutation_inventory_digest": "0" * 64,
            }
        )


def test_preparation_expiry_is_normalized_and_must_be_timezone_aware() -> None:
    preparation = _preparation()
    assert preparation.compatibility_not_after.tzinfo is UTC
    with pytest.raises(ValidationError, match="timezone-aware"):
        LegacyCompatibilityPreparationV1.model_validate(
            {
                **preparation.model_dump(mode="python"),
                "compatibility_not_after": datetime(2026, 8, 10, 12),
            }
        )


def test_preparation_and_freeze_never_carry_runtime_authority() -> None:
    preparation = _preparation()
    freeze = _freeze(preparation)
    false_fields = (
        "new_submission_authority",
        "new_claim_authority",
        "scale_up_authority",
        "cross_pool_placement_authority",
        "global_allowance_authority",
        "new_worker_authority",
        "executable",
    )
    for value in (preparation, freeze):
        assert all(getattr(value, field) is False for field in false_fields)
        for field in false_fields:
            with pytest.raises(ValidationError):
                type(value).model_validate({**value.model_dump(mode="python"), field: True})


def test_freeze_is_exactly_bound_to_its_preparation() -> None:
    preparation = _preparation()
    freeze = _freeze(preparation)
    assert freeze.freeze_state == "frozen"
    assert freeze.preparation_digest == canonical_digest(preparation)
    assert all(cursor.freeze_state == "frozen" for cursor in freeze.writer_cursors)
    assert all(cursor.freeze_acknowledgement_digest == "c" * 64 for cursor in freeze.writer_cursors)

    identities = (
        freeze.subject_id,
        freeze.subject_incarnation,
        freeze.authority_incarnation,
        freeze.agent_incarnation,
        freeze.reporter_incarnation,
        freeze.freeze_id,
        freeze.preparation_id,
        freeze.compatibility_incarnation,
    )
    assert len(identities) == len(set(identities))
    with pytest.raises(ValidationError, match="identities"):
        LegacyCompatibilityFreezeV1.model_validate(
            {
                **freeze.model_dump(mode="python"),
                "freeze_id": freeze.preparation_id,
            }
        )


def test_strict_legacy_contracts_reject_unknown_fields() -> None:
    preparation = _preparation()
    freeze = _freeze(preparation)
    with pytest.raises(ValidationError):
        LegacyCompatibilityPreparationV1.model_validate(
            {**preparation.model_dump(mode="python"), "secret": "forbidden"}
        )
    with pytest.raises(ValidationError):
        LegacyCompatibilityFreezeV1.model_validate(
            {**freeze.model_dump(mode="python"), "rollback_enabled": True}
        )
