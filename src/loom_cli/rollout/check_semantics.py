"""Typed check semantics + bounded retry for the rollout reconciler (#1097 / #1085 phase 4).

Each check declares whether its failures are TRANSIENT (retry with bounded backoff,
then escalate) or DURABLE (block immediately). This is the reconciler's answer to
two failure modes the pipeline conflated: a flaky rehearsal-browser timeout should
*retry*, not fail a good deploy, while a real migration violation must *block* and
never be retried away. The classification is explicit and owned, and a transient
that keeps failing eventually escalates to a block — so a durable fault
masquerading as transient cannot retry forever.

Pure policy: no I/O and no sleeping. The caller decides how long to wait using
`RetryPolicy.backoff_for`. Wiring specific check ids to their semantics is a
separate, reviewable step (the classification is a judgment, not baked in here).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class FailureSemantics(StrEnum):
    # A flaky / environmental failure — retry with backoff, escalate if persistent.
    TRANSIENT = "transient"
    # A real violation — block immediately, never retried away.
    DURABLE = "durable"


class RetryDecision(StrEnum):
    PASS = "pass"  # check passed — proceed
    RETRY = "retry"  # transient failure with attempts remaining — wait + retry
    BLOCK = "block"  # durable failure, or a transient that exhausted its budget


@dataclass(frozen=True)
class RetryPolicy:
    """A bounded retry budget with exponential backoff (seconds), capped."""

    max_attempts: int = 3
    backoff_base_seconds: float = 2.0
    backoff_max_seconds: float = 60.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.backoff_base_seconds <= 0 or self.backoff_max_seconds <= 0:
            raise ValueError("backoff seconds must be positive")

    def backoff_for(self, attempt: int) -> float:
        """Seconds to wait after a 1-based `attempt` failed, before the next attempt."""
        if attempt < 1:
            raise ValueError("attempt must be >= 1")
        return min(self.backoff_base_seconds * 2.0 ** (attempt - 1), self.backoff_max_seconds)


def classify_outcome(
    *,
    semantics: FailureSemantics,
    passed: bool,
    attempt: int,
    max_attempts: int,
) -> RetryDecision:
    """Decide what to do after a check's 1-based `attempt`.

    - passed → PASS.
    - durable failure → BLOCK (never retried).
    - transient failure with attempts remaining → RETRY.
    - transient failure that exhausted its budget → BLOCK (escalation): a failure
      that claimed to be transient but persists eventually blocks.
    """
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")
    if passed:
        return RetryDecision.PASS
    if semantics is FailureSemantics.DURABLE:
        return RetryDecision.BLOCK
    return RetryDecision.RETRY if attempt < max_attempts else RetryDecision.BLOCK


@dataclass(frozen=True)
class TypedCheck:
    """A check's declared failure semantics + its retry budget."""

    check_id: str
    semantics: FailureSemantics
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)

    def decide(self, *, passed: bool, attempt: int) -> RetryDecision:
        return classify_outcome(
            semantics=self.semantics,
            passed=passed,
            attempt=attempt,
            max_attempts=self.retry_policy.max_attempts,
        )

    def backoff_for(self, attempt: int) -> float:
        return self.retry_policy.backoff_for(attempt)
