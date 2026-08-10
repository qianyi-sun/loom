from __future__ import annotations

from uuid import uuid4

import pytest

from loom.pipeline.budget import (
    BudgetCounter,
    BudgetExceededError,
    BudgetKind,
    BudgetReservation,
    ReservationState,
    TerminalCause,
    checkpoint_artifact_reservation_key,
    provider_reservation_key,
    provider_usd_to_microusd,
    release,
    reservation_request_digest,
    reserve,
    settle,
)

DIGEST = "sha256:" + "a" * 64


def test_provider_budget_conversion_is_lossless() -> None:
    assert provider_usd_to_microusd("0") == 0
    assert provider_usd_to_microusd("12.500001") == 12_500_001


def test_reservation_identity_is_raw_jcs_and_namespaced() -> None:
    attempt_id = uuid4()
    request_id = uuid4()
    key = provider_reservation_key(attempt_id, request_id)

    first = reservation_request_digest(
        kind=BudgetKind.PROVIDER,
        reservation_key=key,
        producer_identity={"attempt_id": str(attempt_id)},
        requested_amount=7,
        governing_digest=DIGEST,
    )
    second = reservation_request_digest(
        kind=BudgetKind.PROVIDER,
        reservation_key=key,
        producer_identity={"attempt_id": str(attempt_id)},
        requested_amount=7,
        governing_digest=DIGEST,
    )
    assert first == second
    assert checkpoint_artifact_reservation_key(attempt_id, 9).endswith(":000000000009")


def test_hard_reserve_settle_and_release() -> None:
    counter = reserve(BudgetCounter(limit=100), kind=BudgetKind.PROVIDER, amount=60)
    reservation = BudgetReservation(
        kind=BudgetKind.PROVIDER,
        key=provider_reservation_key(uuid4(), uuid4()),
        request_digest=DIGEST,
        reserved_amount=60,
    )
    counter, reservation, cause = settle(counter, reservation, actual_amount=45)
    assert counter == BudgetCounter(limit=100, reserved=0, settled=45)
    assert reservation.state is ReservationState.SETTLED
    assert cause is None

    artifact = BudgetReservation(
        kind=BudgetKind.ARTIFACT,
        key=f"artifact:final:{uuid4()}",
        request_digest=DIGEST,
        reserved_amount=10,
    )
    artifact_counter = reserve(BudgetCounter(limit=10), kind=BudgetKind.ARTIFACT, amount=10)
    artifact_counter, artifact = release(artifact_counter, artifact)
    assert artifact_counter.reserved == 0
    assert artifact.state is ReservationState.RELEASED


def test_budget_exhaustion_and_truth_preserving_overage() -> None:
    with pytest.raises(BudgetExceededError) as error:
        reserve(BudgetCounter(limit=4), kind=BudgetKind.GPU, amount=5)
    assert error.value.cause is TerminalCause.GPU_BUDGET

    counter = reserve(BudgetCounter(limit=10), kind=BudgetKind.GPU, amount=10)
    reservation = BudgetReservation(
        kind=BudgetKind.GPU,
        key=f"gpu:{uuid4()}",
        request_digest=DIGEST,
        reserved_amount=10,
    )
    counter, _reservation, cause = settle(counter, reservation, actual_amount=11)
    assert counter.settled == 11
    assert cause is TerminalCause.ACCOUNTING_VIOLATION

    artifact_counter = reserve(BudgetCounter(limit=10), kind=BudgetKind.ARTIFACT, amount=10)
    artifact = BudgetReservation(
        kind=BudgetKind.ARTIFACT,
        key=f"artifact:final:{uuid4()}",
        request_digest=DIGEST,
        reserved_amount=10,
    )
    with pytest.raises(ValueError, match="artifact settlement"):
        settle(artifact_counter, artifact, actual_amount=11)
