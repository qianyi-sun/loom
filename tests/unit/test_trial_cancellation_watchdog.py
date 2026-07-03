"""Trial cancellation watchdog (#360, #378)."""

from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

import pytest

from loom_worker.trial_cancellation_watchdog import run_with_watchdog


class _FakeCPClient:
    """Return a fixed state each poll; count polls for assertions."""

    def __init__(self, state: str = "running") -> None:
        self.state = state
        self.polls = 0

    async def get_trial_state(self, trial_id: UUID) -> str:
        self.polls += 1
        return self.state


class _FlakyCPClient:
    """Raise on the first N polls, then return ``state``."""

    def __init__(self, state: str, raise_first_n: int) -> None:
        self.state = state
        self.raise_first_n = raise_first_n
        self.polls = 0

    async def get_trial_state(self, trial_id: UUID) -> str:
        self.polls += 1
        if self.polls <= self.raise_first_n:
            raise RuntimeError("CP transient error")
        return self.state


async def test_completes_normally_when_no_cancel_or_deadline():
    cp = _FakeCPClient(state="running")

    async def _runner() -> str:
        await asyncio.sleep(0.05)
        return "done"

    trigger = await run_with_watchdog(
        _runner(),
        trial_id=uuid4(),
        cp_client=cp,
        poll_interval_sec=0.01,
        hard_deadline_sec=None,
    )
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
        await run_with_watchdog(
            _runner(),
            trial_id=uuid4(),
            cp_client=cp,
            poll_interval_sec=0.01,
            hard_deadline_sec=None,
        )
    # Watchdog polled at least once and observed 'cancelled'.
    assert cp.polls >= 1
    # Runner's cancellation cleanup path fired.
    assert ran_cleanup is True


async def test_cancels_when_hard_deadline_exceeded():
    cp = _FakeCPClient(state="running")

    async def _runner() -> None:
        await asyncio.sleep(5.0)

    with pytest.raises(asyncio.CancelledError):
        await run_with_watchdog(
            _runner(),
            trial_id=uuid4(),
            cp_client=cp,
            poll_interval_sec=0.01,
            hard_deadline_sec=0.03,
        )


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
        await run_with_watchdog(
            _runner(),
            trial_id=uuid4(),
            cp_client=cp,
            poll_interval_sec=0.01,
            hard_deadline_sec=None,
        )
    # Watchdog kept polling past the 3 transient errors.
    assert cp.polls >= 4


async def test_watcher_task_is_cleaned_up_after_normal_completion():
    """Verify no dangling task leaks (asyncio would warn otherwise)."""
    cp = _FakeCPClient(state="running")

    async def _runner() -> str:
        await asyncio.sleep(0.01)
        return "ok"

    all_tasks_before = asyncio.all_tasks()
    await run_with_watchdog(
        _runner(),
        trial_id=uuid4(),
        cp_client=cp,
        poll_interval_sec=0.01,
        hard_deadline_sec=None,
    )
    # Give the event loop one tick to clean up.
    await asyncio.sleep(0)
    leaked = asyncio.all_tasks() - all_tasks_before
    # Ignore the currently-running test task.
    leaked = {t for t in leaked if not t.done()}
    assert not leaked


async def test_disabled_hard_deadline_still_allows_cp_cancel():
    cp = _FakeCPClient(state="cancelled")

    async def _runner() -> None:
        await asyncio.sleep(5.0)

    with pytest.raises(asyncio.CancelledError):
        await run_with_watchdog(
            _runner(),
            trial_id=uuid4(),
            cp_client=cp,
            poll_interval_sec=0.01,
            hard_deadline_sec=None,
        )
