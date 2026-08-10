"""Hard Pipeline budget identities and pure accounting transitions (#1212)."""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from decimal import Decimal
from enum import StrEnum
from typing import Literal
from uuid import UUID

from loom.pipeline.keys import MAX_SAFE_INTEGER, canonical_digest
from loom.pipeline.spec import ContainerNodeV1, StageBudgetV1

MAX_INT64 = 2**63 - 1


class BudgetKind(StrEnum):
    PROVIDER = "provider"
    GPU = "gpu"
    ARTIFACT = "artifact"


class TerminalCause(StrEnum):
    USER_CANCEL = "user_cancel"
    PROVIDER_BUDGET = "provider_budget"
    GPU_BUDGET = "gpu_budget"
    ARTIFACT_BUDGET = "artifact_budget"
    STAGE_RUN_BUDGET = "stage_run_budget"
    ATTEMPT_BUDGET = "attempt_budget"
    WALL_BUDGET = "wall_budget"
    ACCOUNTING_VIOLATION = "accounting_violation"


class ReservationState(StrEnum):
    ACTIVE = "active"
    SETTLED = "settled"
    RELEASED = "released"


class BudgetExceededError(ValueError):
    """Raised before work starts when a hard limit cannot fit it."""

    def __init__(self, cause: TerminalCause) -> None:
        self.cause = cause
        super().__init__(cause.value)


class BudgetReservationConflictError(ValueError):
    """Raised when an idempotency key is replayed with another digest."""


class AttemptProviderBudgetExceededError(ValueError):
    """The Attempt-local request/cost slice rejected a dispatch without a run latch."""


_KEY_PATTERNS = {
    BudgetKind.PROVIDER: re.compile(
        r"^provider:[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}:"
        r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    ),
    BudgetKind.GPU: re.compile(
        r"^gpu:[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    ),
    BudgetKind.ARTIFACT: re.compile(
        r"^artifact:(?:final:[0-9a-f-]{36}|checkpoint:[0-9a-f-]{36}:[0-9]{12}|"
        r"control:[a-z][a-z0-9_]{0,62}:[0-9a-f-]{36})$"
    ),
}


def provider_usd_to_microusd(value: str) -> int:
    """Convert the fixed decimal RunBudget value without binary rounding."""

    amount = Decimal(value) * 1_000_000
    if amount != amount.to_integral_value():
        raise ValueError("provider budget is not losslessly representable in micro-USD")
    result = int(amount)
    if not 0 <= result <= MAX_INT64:
        raise ValueError("provider budget exceeds signed 64-bit accounting")
    return result


def provider_reservation_key(attempt_id: UUID, provider_request_id: UUID) -> str:
    return f"provider:{attempt_id}:{provider_request_id}"


def gpu_reservation_key(attempt_id: UUID) -> str:
    return f"gpu:{attempt_id}"


def final_artifact_reservation_key(attempt_id: UUID) -> str:
    return f"artifact:final:{attempt_id}"


def checkpoint_artifact_reservation_key(attempt_id: UUID, sequence: int) -> str:
    if not 0 <= sequence <= 999_999_999_999:
        raise ValueError("checkpoint sequence must fit twelve decimal digits")
    return f"artifact:checkpoint:{attempt_id}:{sequence:012d}"


def control_artifact_reservation_key(producer_kind: str, producer_id: UUID) -> str:
    if re.fullmatch(r"[a-z][a-z0-9_]{0,62}", producer_kind) is None:
        raise ValueError("invalid control producer kind")
    return f"artifact:control:{producer_kind}:{producer_id}"


def validate_reservation_key(kind: BudgetKind, key: str) -> str:
    if not key.isascii() or _KEY_PATTERNS[kind].fullmatch(key) is None:
        raise ValueError(f"invalid {kind.value} reservation key")
    return key


def reservation_request_digest(
    *,
    kind: BudgetKind,
    reservation_key: str,
    producer_identity: dict[str, str | int],
    requested_amount: int,
    governing_digest: str,
) -> str:
    validate_reservation_key(kind, reservation_key)
    _amount(requested_amount)
    return canonical_digest(
        {
            "governing_digest": governing_digest,
            "kind": kind.value,
            "producer_identity": producer_identity,
            "requested_amount": requested_amount,
            "reservation_key": reservation_key,
        },
        persisted=False,
    )


def stage_budget_for_node(
    node: ContainerNodeV1, *, gpu_count_exact: int
) -> StageBudgetV1:
    """Return the exact per-Attempt maxima frozen by readiness."""

    return StageBudgetV1.for_node(node, gpu_count_exact=gpu_count_exact)


@dataclass(frozen=True, slots=True)
class BudgetCounter:
    limit: int
    reserved: int = 0
    settled: int = 0

    def __post_init__(self) -> None:
        _amount(self.limit)
        _amount(self.reserved)
        _amount(self.settled)
        if self.reserved + self.settled > self.limit:
            raise ValueError("reserved plus settled exceeds the hard limit")

    @property
    def remaining(self) -> int:
        return self.limit - self.reserved - self.settled


@dataclass(frozen=True, slots=True)
class BudgetReservation:
    kind: BudgetKind
    key: str
    request_digest: str
    reserved_amount: int
    settled_amount: int | None = None
    state: ReservationState = ReservationState.ACTIVE

    def __post_init__(self) -> None:
        validate_reservation_key(self.kind, self.key)
        _amount(self.reserved_amount)
        if self.settled_amount is not None:
            _amount(self.settled_amount)
        if (self.state is ReservationState.SETTLED) != (self.settled_amount is not None):
            raise ValueError("only settled reservations carry a settled amount")


def reserve(counter: BudgetCounter, *, kind: BudgetKind, amount: int) -> BudgetCounter:
    _amount(amount)
    if amount > counter.remaining:
        raise BudgetExceededError(_cause_for(kind))
    return replace(counter, reserved=counter.reserved + amount)


def settle(
    counter: BudgetCounter,
    reservation: BudgetReservation,
    *,
    actual_amount: int,
) -> tuple[BudgetCounter, BudgetReservation, TerminalCause | None]:
    """Settle authoritative truth, latching accounting violation on allowed overage."""

    if reservation.state is not ReservationState.ACTIVE:
        raise ValueError("reservation is already terminal")
    _amount(actual_amount)
    if reservation.reserved_amount > counter.reserved:
        raise ValueError("reservation exceeds the aggregate reserved counter")
    overage = actual_amount > reservation.reserved_amount or (
        counter.settled + actual_amount > counter.limit
    )
    if overage and reservation.kind is BudgetKind.ARTIFACT:
        raise ValueError("artifact settlement cannot exceed its reservation")
    new_reserved = counter.reserved - reservation.reserved_amount
    new_settled = counter.settled + actual_amount
    updated_counter = object.__new__(BudgetCounter)
    object.__setattr__(updated_counter, "limit", counter.limit)
    object.__setattr__(updated_counter, "reserved", new_reserved)
    object.__setattr__(updated_counter, "settled", new_settled)
    updated_reservation = replace(
        reservation,
        state=ReservationState.SETTLED,
        settled_amount=actual_amount,
    )
    return (
        updated_counter,
        updated_reservation,
        TerminalCause.ACCOUNTING_VIOLATION if overage else None,
    )


def release(
    counter: BudgetCounter, reservation: BudgetReservation
) -> tuple[BudgetCounter, BudgetReservation]:
    if reservation.state is not ReservationState.ACTIVE:
        raise ValueError("reservation is already terminal")
    if reservation.reserved_amount > counter.reserved:
        raise ValueError("reservation exceeds the aggregate reserved counter")
    return (
        replace(counter, reserved=counter.reserved - reservation.reserved_amount),
        replace(reservation, state=ReservationState.RELEASED),
    )


def _cause_for(kind: BudgetKind) -> TerminalCause:
    return {
        BudgetKind.PROVIDER: TerminalCause.PROVIDER_BUDGET,
        BudgetKind.GPU: TerminalCause.GPU_BUDGET,
        BudgetKind.ARTIFACT: TerminalCause.ARTIFACT_BUDGET,
    }[kind]


def _amount(value: int) -> int:
    if isinstance(value, bool) or not 0 <= value <= min(MAX_INT64, MAX_SAFE_INTEGER):
        raise ValueError("accounting amount must be a non-negative interoperable integer")
    return value


CounterName = Literal["provider", "gpu", "artifact"]
