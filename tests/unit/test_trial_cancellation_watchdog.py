"""Trial cancellation / ownership-revoke watchdog (#360, #378, #1491)."""

from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

import pytest

from loom.trial.watchdog_cancellation import (
    WatchdogTriggerReason,
    extract_watchdog_cancellation,
)
from loom_worker.trial_cancellation_watchdog import (
    TrialOwnershipSnapshot,
    ownership_revoke_reason,
    resolve_hard_deadline_sec,
    run_with_watchdog,
)

_OWNER = uuid4()


class _FakeCPClient:
    """Return a fixed ownership snapshot each poll; count polls."""

    def __init__(
        self,
        state: str = "running",
        *,
        worker_id: UUID | None = _OWNER,
        attempt_count: int | None = 1,
    ) -> None:
        self.state = state
        self.worker_id = worker_id
        self.attempt_count = attempt_count
        self.polls = 0

    async def get_trial_ownership(self, trial_id: UUID) -> TrialOwnershipSnapshot:
        del trial_id
        self.polls += 1
        return TrialOwnershipSnapshot(
            state=self.state,
            worker_id=self.worker_id,
            attempt_count=self.attempt_count,
        )


class _FlakyCPClient:
    """Raise on the first N polls, then return ownership."""

    def __init__(self, state: str, raise_first_n: int) -> None:
        self.state = state
        self.raise_first_n = raise_first_n
        self.polls = 0

    async def get_trial_ownership(self, trial_id: UUID) -> TrialOwnershipSnapshot:
        del trial_id
        self.polls += 1
        if self.polls <= self.raise_first_n:
            raise RuntimeError("CP transient error")
        return TrialOwnershipSnapshot(
            state=self.state,
            worker_id=_OWNER,
            attempt_count=1,
        )


async def _watch(
    coro: object,
    *,
    cp: object,
    poll_interval_sec: float = 0.01,
    hard_deadline_sec: float | None = None,
    on_cancel: object | None = None,
) -> object:
    return await run_with_watchdog(
        coro,  # type: ignore[arg-type]
        trial_id=uuid4(),
        cp_client=cp,  # type: ignore[arg-type]
        owner_worker_id=_OWNER,
        claimed_attempt_count=1,
        poll_interval_sec=poll_interval_sec,
        hard_deadline_sec=hard_deadline_sec,
        on_cancel=on_cancel,  # type: ignore[arg-type]
    )


def test_hard_deadline_uses_task_default_and_trial_multiplier() -> None:
    deadline = resolve_hard_deadline_sec(
        task_config={
            "agent": {"timeout_sec": 3000.0},
            "steps": [{"name": "main"}],
        },
        trial_config={"agent_timeout_multiplier": 1.25},
        multiplier=2.0,
        grace_sec=300.0,
    )

    assert deadline == 7800.0


def test_hard_deadline_uses_largest_step_override() -> None:
    deadline = resolve_hard_deadline_sec(
        task_config={
            "agent": {"timeout_sec": 1800.0},
            "steps": [
                {"name": "short", "agent": {"timeout_sec": 1200.0}},
                {"name": "long", "agent": {"timeout_sec": 3600.0}},
            ],
        },
        trial_config={"agent_timeout_multiplier": 1.5},
        multiplier=2.0,
        grace_sec=300.0,
    )

    assert deadline == 11100.0


def test_hard_deadline_trial_override_wins_and_prevents_3300_cutoff() -> None:
    deadline = resolve_hard_deadline_sec(
        task_config={
            "agent": {"timeout_sec": 1800.0},
            "steps": [{"name": "main"}],
        },
        trial_config={"override_agent_timeout_sec": 9000.0},
        multiplier=0.25,
        grace_sec=600.0,
    )
    assert deadline == 2850.0


def test_hard_deadline_disabled_when_multiplier_non_positive() -> None:
    assert (
        resolve_hard_deadline_sec(
            task_config={
                "agent": {"timeout_sec": 1800.0},
                "steps": [{"name": "main"}],
            },
            trial_config={"override_agent_timeout_sec": 9000.0},
            multiplier=0.0,
            grace_sec=600.0,
        )
        is None
    )


def test_ownership_revoke_reason_matrix() -> None:
    owner = _OWNER
    assert (
        ownership_revoke_reason(
            TrialOwnershipSnapshot(state="running", worker_id=owner, attempt_count=1),
            owner_worker_id=owner,
            claimed_attempt_count=1,
        )
        is None
    )
    assert (
        ownership_revoke_reason(
            TrialOwnershipSnapshot(state="cancelled", worker_id=owner, attempt_count=1),
            owner_worker_id=owner,
            claimed_attempt_count=1,
        )
        == WatchdogTriggerReason.CP_CANCELLED
    )
    assert (
        ownership_revoke_reason(
            TrialOwnershipSnapshot(state="failed", worker_id=None, attempt_count=1),
            owner_worker_id=owner,
            claimed_attempt_count=1,
        )
        == WatchdogTriggerReason.CP_TERMINAL
    )
    assert (
        ownership_revoke_reason(
            TrialOwnershipSnapshot(state="queued", worker_id=None, attempt_count=1),
            owner_worker_id=owner,
            claimed_attempt_count=1,
        )
        == WatchdogTriggerReason.OWNERSHIP_REVOKED
    )
    assert (
        ownership_revoke_reason(
            TrialOwnershipSnapshot(state="running", worker_id=uuid4(), attempt_count=1),
            owner_worker_id=owner,
            claimed_attempt_count=1,
        )
        == WatchdogTriggerReason.OWNERSHIP_REVOKED
    )
    assert (
        ownership_revoke_reason(
            TrialOwnershipSnapshot(state="running", worker_id=owner, attempt_count=2),
            owner_worker_id=owner,
            claimed_attempt_count=1,
        )
        == WatchdogTriggerReason.OWNERSHIP_REVOKED
    )
    # Mixed deploy: missing worker_id still revokes on queued.
    assert (
        ownership_revoke_reason(
            TrialOwnershipSnapshot(state="queued", worker_id=None, attempt_count=None),
            owner_worker_id=owner,
            claimed_attempt_count=1,
        )
        == WatchdogTriggerReason.OWNERSHIP_REVOKED
    )


async def test_completes_normally_when_no_cancel_or_deadline():
    cp = _FakeCPClient(state="running")

    async def _runner() -> str:
        await asyncio.sleep(0.05)
        return "done"

    trigger = await _watch(_runner(), cp=cp, hard_deadline_sec=None)
    assert trigger is None


async def test_cancels_when_cp_reports_cancelled():
    cp = _FakeCPClient(state="cancelled")
    ran_cleanup = False

    async def _runner() -> None:
        nonlocal ran_cleanup
        try:
            await asyncio.sleep(5.0)
        except asyncio.CancelledError:
            ran_cleanup = True
            raise

    with pytest.raises(asyncio.CancelledError):
        await _watch(_runner(), cp=cp, hard_deadline_sec=None)
    assert cp.polls >= 1
    assert ran_cleanup is True


async def test_cancels_when_cp_reports_queued_reclaim():
    cp = _FakeCPClient(state="queued", worker_id=None)

    async def _runner() -> None:
        await asyncio.sleep(5.0)

    with pytest.raises(asyncio.CancelledError) as exc_info:
        await _watch(_runner(), cp=cp, hard_deadline_sec=None)

    cancellation = extract_watchdog_cancellation(exc_info.value)
    assert cancellation is not None
    assert cancellation.reason == WatchdogTriggerReason.OWNERSHIP_REVOKED
    assert cancellation.cp_state == "queued"


async def test_cancels_when_other_worker_owns_trial():
    cp = _FakeCPClient(state="running", worker_id=uuid4(), attempt_count=1)

    async def _runner() -> None:
        await asyncio.sleep(5.0)

    with pytest.raises(asyncio.CancelledError) as exc_info:
        await _watch(_runner(), cp=cp, hard_deadline_sec=None)

    cancellation = extract_watchdog_cancellation(exc_info.value)
    assert cancellation is not None
    assert cancellation.reason == WatchdogTriggerReason.OWNERSHIP_REVOKED


async def test_cancels_when_attempt_count_advances():
    cp = _FakeCPClient(state="running", worker_id=_OWNER, attempt_count=2)

    async def _runner() -> None:
        await asyncio.sleep(5.0)

    with pytest.raises(asyncio.CancelledError) as exc_info:
        await _watch(_runner(), cp=cp, hard_deadline_sec=None)

    cancellation = extract_watchdog_cancellation(exc_info.value)
    assert cancellation is not None
    assert cancellation.reason == WatchdogTriggerReason.OWNERSHIP_REVOKED


async def test_on_cancel_hook_runs_before_task_cancel():
    cp = _FakeCPClient(state="queued", worker_id=None)
    hooks: list[str] = []

    async def _on_cancel() -> None:
        hooks.append("on_cancel")

    async def _runner() -> None:
        try:
            await asyncio.sleep(5.0)
        except asyncio.CancelledError:
            hooks.append("cancelled")
            raise

    with pytest.raises(asyncio.CancelledError):
        await _watch(_runner(), cp=cp, hard_deadline_sec=None, on_cancel=_on_cancel)

    assert hooks == ["on_cancel", "cancelled"]


async def test_cancels_when_hard_deadline_exceeded():
    cp = _FakeCPClient(state="running")

    async def _runner() -> None:
        await asyncio.sleep(5.0)

    with pytest.raises(asyncio.CancelledError):
        await _watch(_runner(), cp=cp, hard_deadline_sec=0.03)


async def test_hard_deadline_cancellation_carries_watchdog_reason():
    cp = _FakeCPClient(state="running")

    async def _runner() -> None:
        await asyncio.sleep(5.0)

    with pytest.raises(asyncio.CancelledError) as exc_info:
        await _watch(_runner(), cp=cp, hard_deadline_sec=0.03)

    cancellation = extract_watchdog_cancellation(exc_info.value)
    assert cancellation is not None
    assert cancellation.reason == WatchdogTriggerReason.HARD_DEADLINE
    assert cancellation.hard_deadline_sec == 0.03
    assert cancellation.elapsed_sec is not None
    assert cancellation.elapsed_sec >= 0.03


async def test_cp_cancel_cancellation_carries_distinct_watchdog_reason():
    cp = _FakeCPClient(state="cancelled")

    async def _runner() -> None:
        await asyncio.sleep(5.0)

    with pytest.raises(asyncio.CancelledError) as exc_info:
        await _watch(_runner(), cp=cp, hard_deadline_sec=None)

    cancellation = extract_watchdog_cancellation(exc_info.value)
    assert cancellation is not None
    assert cancellation.reason == WatchdogTriggerReason.CP_CANCELLED
    assert cancellation.cp_state == "cancelled"


async def test_cp_poll_errors_do_not_abort_watchdog():
    """A transient GET failure should not tear down the watchdog; the
    trial continues normally and the next poll gets fresh state."""
    cp = _FlakyCPClient(state="cancelled", raise_first_n=3)

    async def _runner() -> None:
        try:
            await asyncio.sleep(5.0)
        except asyncio.CancelledError:
            raise

    with pytest.raises(asyncio.CancelledError):
        await _watch(_runner(), cp=cp, hard_deadline_sec=None)
    assert cp.polls >= 4


async def test_watcher_task_is_cleaned_up_after_normal_completion():
    """Verify no dangling task leaks (asyncio would warn otherwise)."""
    cp = _FakeCPClient(state="running")

    async def _runner() -> str:
        await asyncio.sleep(0.01)
        return "ok"

    all_tasks_before = asyncio.all_tasks()
    await _watch(_runner(), cp=cp, hard_deadline_sec=None)
    await asyncio.sleep(0)
    leaked = asyncio.all_tasks() - all_tasks_before
    leaked = {t for t in leaked if not t.done()}
    assert not leaked


async def test_disabled_hard_deadline_still_allows_cp_cancel():
    cp = _FakeCPClient(state="cancelled")

    async def _runner() -> None:
        await asyncio.sleep(5.0)

    with pytest.raises(asyncio.CancelledError):
        await _watch(_runner(), cp=cp, hard_deadline_sec=None)
