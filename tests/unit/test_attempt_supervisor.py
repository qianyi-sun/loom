from __future__ import annotations

import asyncio
import time
from typing import cast

import pytest

from loom.attempt_deadline import AttemptDeadline, AttemptDeadlineExceededError
from loom.trajectory.attempt_guard import (
    AttemptFence,
    AttemptTrajectoryFencedError,
    AttemptTrajectoryGuard,
)
from loom.trajectory.writer import TrajectoryWriter
from loom.trial.attempt_supervisor import supervise_agent_attempt


class _RecordingWriter:
    def __init__(self) -> None:
        self.events: list[object] = []
        self.raw: list[dict[str, object]] = []

    async def append(self, event: object) -> None:
        self.events.append(event)

    async def write_raw_dict(self, data: dict[str, object]) -> None:
        self.raw.append(data)


class _LifecycleAgent:
    def __init__(self) -> None:
        self.deadlines: list[AttemptDeadline] = []
        self.close_calls = 0

    def begin_attempt(self, deadline: AttemptDeadline) -> None:
        self.deadlines.append(deadline)

    async def aclose_attempt(self) -> None:
        self.close_calls += 1


async def test_attempt_guard_rejects_typed_and_raw_writes_after_latch() -> None:
    writer = _RecordingWriter()
    fence = AttemptFence()
    guard = AttemptTrajectoryGuard(cast(TrajectoryWriter, writer), fence)

    await guard.append(cast(object, "before"))  # type: ignore[arg-type]
    await guard.write_raw_dict({"kind": "before"})
    assert fence.latch("agent_timeout")

    with pytest.raises(AttemptTrajectoryFencedError):
        await guard.append(cast(object, "late"))  # type: ignore[arg-type]
    with pytest.raises(AttemptTrajectoryFencedError):
        await guard.write_raw_dict({"kind": "late"})

    await writer.append("platform-timeout-diagnostic")

    assert writer.events == ["before", "platform-timeout-diagnostic"]
    assert writer.raw == [{"kind": "before"}]


async def test_success_uses_one_deadline_and_closes_attempt() -> None:
    agent = _LifecycleAgent()
    writer = cast(TrajectoryWriter, _RecordingWriter())
    received: list[TrajectoryWriter] = []

    async def run(trajectory: TrajectoryWriter) -> None:
        received.append(trajectory)

    diagnostic = await supervise_agent_attempt(
        agent=agent,
        configured_timeout_sec=1.0,
        trajectory=writer,
        run=run,
    )

    assert diagnostic is None
    assert len(agent.deadlines) == 1
    assert agent.close_calls == 1
    assert received[0] is not writer


async def test_deadline_latches_before_cancel_and_closes_transport() -> None:
    agent = _LifecycleAgent()
    writer = cast(TrajectoryWriter, _RecordingWriter())
    cancelled = asyncio.Event()

    async def run(_trajectory: TrajectoryWriter) -> None:
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    diagnostic = await supervise_agent_attempt(
        agent=agent,
        configured_timeout_sec=0.01,
        trajectory=writer,
        run=run,
        cancellation_drain_sec=0.1,
    )

    assert diagnostic is not None
    assert diagnostic.configured_timeout_sec == 0.01
    assert diagnostic.elapsed_monotonic_sec >= 0.01
    assert diagnostic.transport_close_required
    assert diagnostic.task_stopped
    assert diagnostic.cancellation_drain_sec <= 0.1
    assert cancelled.is_set()
    assert agent.close_calls == 1


async def test_cancellation_resistant_task_cannot_extend_drain_budget() -> None:
    agent = _LifecycleAgent()
    writer = cast(TrajectoryWriter, _RecordingWriter())
    release = asyncio.Event()

    async def run(_trajectory: TrajectoryWriter) -> None:
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            await release.wait()

    started = time.monotonic()
    diagnostic = await supervise_agent_attempt(
        agent=agent,
        configured_timeout_sec=0.01,
        trajectory=writer,
        run=run,
        cancellation_drain_sec=0.02,
    )
    elapsed = time.monotonic() - started

    assert diagnostic is not None
    assert not diagnostic.task_stopped
    assert diagnostic.cancellation_drain_sec <= 0.02
    assert elapsed < 0.2
    assert agent.close_calls == 1
    assert agent._loom_worker_unhealthy is True  # type: ignore[attr-defined]

    release.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)


async def test_deadline_exception_always_wins_over_agent_exception() -> None:
    agent = _LifecycleAgent()

    async def run(_trajectory: TrajectoryWriter) -> None:
        raise AttemptDeadlineExceededError("late gateway response")

    diagnostic = await supervise_agent_attempt(
        agent=agent,
        configured_timeout_sec=1.0,
        trajectory=cast(TrajectoryWriter, _RecordingWriter()),
        run=run,
        cancellation_drain_sec=0.1,
    )

    assert diagnostic is not None
    assert diagnostic.task_stopped
    assert agent.close_calls == 1


async def test_caller_cancellation_closes_attempt_exactly_once() -> None:
    agent = _LifecycleAgent()
    entered = asyncio.Event()

    async def run(_trajectory: TrajectoryWriter) -> None:
        entered.set()
        await asyncio.Future()

    supervised = asyncio.create_task(
        supervise_agent_attempt(
            agent=agent,
            configured_timeout_sec=1.0,
            trajectory=cast(TrajectoryWriter, _RecordingWriter()),
            run=run,
            cancellation_drain_sec=0.1,
        )
    )
    await entered.wait()
    supervised.cancel()

    with pytest.raises(asyncio.CancelledError):
        await supervised
    assert agent.close_calls == 1


async def test_each_supervised_retry_gets_a_distinct_deadline() -> None:
    agent = _LifecycleAgent()

    async def run(_trajectory: TrajectoryWriter) -> None:
        return None

    writer = cast(TrajectoryWriter, _RecordingWriter())
    await supervise_agent_attempt(
        agent=agent,
        configured_timeout_sec=1.0,
        trajectory=writer,
        run=run,
    )
    await supervise_agent_attempt(
        agent=agent,
        configured_timeout_sec=1.0,
        trajectory=writer,
        run=run,
    )

    assert len(agent.deadlines) == 2
    assert agent.deadlines[0] is not agent.deadlines[1]


async def test_cancellation_drain_cannot_exceed_thirty_seconds() -> None:
    async def run(_trajectory: TrajectoryWriter) -> None:
        return None

    with pytest.raises(ValueError, match="between 0 and 30"):
        await supervise_agent_attempt(
            agent=_LifecycleAgent(),
            configured_timeout_sec=1.0,
            trajectory=cast(TrajectoryWriter, _RecordingWriter()),
            run=run,
            cancellation_drain_sec=30.01,
        )
