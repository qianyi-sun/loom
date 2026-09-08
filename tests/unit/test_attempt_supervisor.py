from __future__ import annotations

import asyncio
import time
from typing import cast
from uuid import uuid4

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


@pytest.mark.parametrize("blocked_phase", ["begin", "start"])
async def test_attempt_setup_callbacks_share_supervised_deadline(blocked_phase: str) -> None:
    class BlockingAgent(_LifecycleAgent):
        async def begin_attempt(self, deadline: AttemptDeadline) -> None:
            self.deadlines.append(deadline)
            if blocked_phase == "begin":
                await asyncio.Future()

    agent = BlockingAgent()

    async def on_started(*_args: object) -> None:
        if blocked_phase == "start":
            await asyncio.Future()

    async def run(_writer: TrajectoryWriter) -> None:
        pytest.fail("agent must not run after setup deadline")

    diagnostic = await asyncio.wait_for(
        supervise_agent_attempt(
            agent=agent,
            configured_timeout_sec=0.01,
            trajectory=cast(TrajectoryWriter, _RecordingWriter()),
            run=run,
            cancellation_drain_sec=0.01,
            on_attempt_started=on_started,
        ),
        timeout=0.2,
    )
    assert diagnostic is not None
    assert diagnostic.task_stopped
    assert agent.close_calls == 1


async def test_late_cancel_resistant_grant_callback_is_trajectory_fenced() -> None:
    agent = _LifecycleAgent()
    writer = _RecordingWriter()
    release = asyncio.Event()
    finished = asyncio.Event()
    rejected = []

    async def grant_callback(*args: object) -> None:
        # Compatibility form allows the same test to expose the original
        # callback API's unfenced writer as well as the fixed supplied guard.
        guarded_writer = cast(TrajectoryWriter, args[2] if len(args) == 3 else writer)
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            await release.wait()
        try:
            await guarded_writer.write_raw_dict({"kind": "late-grant"})
        except AttemptTrajectoryFencedError:
            rejected.append(True)
        finally:
            finished.set()

    async def run(_writer: TrajectoryWriter) -> None:
        deadline = agent.deadlines[-1]
        await deadline.record_step_token_grant(
            agent_attempt_id=deadline.agent_attempt_id,
            step_jwt_id=uuid4(),
        )

    diagnostic = await supervise_agent_attempt(
        agent=agent,
        configured_timeout_sec=0.01,
        trajectory=cast(TrajectoryWriter, writer),
        run=run,
        cancellation_drain_sec=0.01,
        on_step_token_grant=grant_callback,
    )
    assert diagnostic is not None and not diagnostic.task_stopped
    assert getattr(agent, "_loom_worker_unhealthy", False)
    release.set()
    await asyncio.wait_for(finished.wait(), timeout=0.2)
    await asyncio.sleep(0)
    assert rejected == [True]
    assert writer.raw == []
