"""Closed automatic-retry policy for Pipeline ExecutionAttempts (#1212)."""

from __future__ import annotations

from dataclasses import dataclass

from loom.pipeline.state import RetryClass

RETRY_ALLOWLIST: dict[str, RetryClass] = {
    "worker_lost": RetryClass.INFRASTRUCTURE_TRANSIENT,
    "node_setup_health": RetryClass.INFRASTRUCTURE_TRANSIENT,
    "object_store_transport": RetryClass.INFRASTRUCTURE_TRANSIENT,
    "gateway_transport": RetryClass.PROVIDER_TRANSIENT,
    "provider_429": RetryClass.PROVIDER_TRANSIENT,
    "provider_5xx": RetryClass.PROVIDER_TRANSIENT,
    "container_start_transient": RetryClass.INFRASTRUCTURE_TRANSIENT,
    "stage_helper_transient": RetryClass.INFRASTRUCTURE_TRANSIENT,
}


@dataclass(frozen=True, slots=True)
class RetryDecision:
    retry: bool
    delay_seconds: int | None
    reason: str


def retry_delay_seconds(completed_attempt_number: int) -> int:
    if completed_attempt_number == 1:
        return 30
    if completed_attempt_number == 2:
        return 120
    raise ValueError("v1 has no retry after Attempt 3")


def retry_decision(
    *,
    completed_attempt_number: int,
    max_attempts: int,
    retry_class: RetryClass,
    reason_code: str,
    terminal_cause: str | None,
    immutable_inputs_unchanged: bool = True,
    cleanup_acknowledged: bool = True,
    next_budget_fits: bool = True,
) -> RetryDecision:
    if max_attempts not in {1, 2, 3} or not 1 <= completed_attempt_number <= 3:
        raise ValueError("Attempt bounds are outside v1")
    if completed_attempt_number >= max_attempts:
        return RetryDecision(False, None, "attempts_exhausted")
    if terminal_cause is not None:
        return RetryDecision(False, None, "run_terminal_cause")
    if not immutable_inputs_unchanged:
        return RetryDecision(False, None, "immutable_input_drift")
    if not next_budget_fits:
        return RetryDecision(False, None, "budget_does_not_fit")
    expected = RETRY_ALLOWLIST.get(reason_code)
    if expected is None or expected is not retry_class:
        return RetryDecision(False, None, "not_allowlisted")
    if reason_code == "worker_lost" and not cleanup_acknowledged:
        return RetryDecision(False, None, "worker_cleanup_pending")
    return RetryDecision(True, retry_delay_seconds(completed_attempt_number), "automatic_retry")


def retry_class_for_exit(exit_code: int, reason_code: str) -> RetryClass:
    if exit_code == 0:
        return RetryClass.NONE
    if exit_code == 20:
        return RetryClass.CONTRACT_ERROR
    if exit_code == 21 and RETRY_ALLOWLIST.get(reason_code) is RetryClass.PROVIDER_TRANSIENT:
        return RetryClass.PROVIDER_TRANSIENT
    if exit_code == 22 and RETRY_ALLOWLIST.get(reason_code) is RetryClass.INFRASTRUCTURE_TRANSIENT:
        return RetryClass.INFRASTRUCTURE_TRANSIENT
    if exit_code == 23:
        return RetryClass.INTERNAL_DEFECT
    if exit_code in {130, 143}:
        return RetryClass.CANCELLED
    raise ValueError("exit code and reason_code are not a valid v1 pair")
