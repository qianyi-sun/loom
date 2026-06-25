"""#5 Slice 3b — CpEventSink unit tests.

Pins the buffer-and-flush semantics that TrajectoryWriter relies on
to mirror typed trajectory events through to the `trial_events`
table without ever failing the trial.
"""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import pytest

from loom.models.trajectory import (
    EnvExecEvent,
    StepEndEvent,
    StepStartEvent,
)
from loom.trajectory.cp_event_sink import CpEventSink


def _step_start(seq: int, trial_id, step_id: str = "main") -> StepStartEvent:
    from datetime import UTC, datetime
    return StepStartEvent(
        emitted_at=datetime.now(UTC),
        trial_id=trial_id,
        step_id=step_id,
        seq=seq,
        instruction_excerpt="hi",
    )


def _step_end(seq: int, trial_id, step_id: str = "main") -> StepEndEvent:
    from datetime import UTC, datetime
    return StepEndEvent(
        emitted_at=datetime.now(UTC),
        trial_id=trial_id,
        step_id=step_id,
        seq=seq,
    )


def _env_exec(seq: int, trial_id, step_id: str = "main") -> EnvExecEvent:
    from datetime import UTC, datetime
    return EnvExecEvent(
        emitted_at=datetime.now(UTC),
        trial_id=trial_id,
        step_id=step_id,
        seq=seq,
        cmd="echo hi",
        user=None,
        cwd="/workspace",
        return_code=0,
        stdout_bytes=3,
        stderr_bytes=0,
        truncated=False,
        duration_sec=0.01,
    )


async def test_buffers_until_flush_threshold_then_sends_batch() -> None:
    """Sink buffers events up to flush_event_count, then sends the
    batch through the injected sender. Smaller-than-threshold buffers
    stay in memory until close."""
    sent: list[list[dict[str, Any]]] = []

    async def sender(batch: list[dict[str, Any]]) -> bool:
        sent.append(batch)
        return True

    trial_id = uuid4()
    sink = CpEventSink(
        trial_id=trial_id, worker_id=uuid4(),
        send_batch=sender,
        flush_event_count=3,
        flush_interval_sec=999,  # disable time-based flush
    )

    await sink.observe(_step_start(0, trial_id))
    await sink.observe(_env_exec(1, trial_id))
    assert sent == []  # below threshold

    await sink.observe(_step_end(2, trial_id))  # trips threshold
    assert len(sent) == 1
    assert len(sent[0]) == 3
    assert [r["seq"] for r in sent[0]] == [0, 1, 2]
    assert sent[0][0]["kind"] == "step_start"


async def test_records_source_and_schema_version_per_row() -> None:
    """Every row carries the sink's `source` and `schema_version` so
    the CP route can persist them onto the new columns."""
    sent: list[list[dict[str, Any]]] = []

    async def sender(batch: list[dict[str, Any]]) -> bool:
        sent.append(batch)
        return True

    trial_id = uuid4()
    sink = CpEventSink(
        trial_id=trial_id, worker_id=uuid4(),
        send_batch=sender,
        flush_event_count=1,
        flush_interval_sec=999,
        source="worker",
        schema_version=1,
    )
    await sink.observe(_step_start(0, trial_id))
    assert sent[0][0]["source"] == "worker"
    assert sent[0][0]["schema_version"] == 1


async def test_409_lost_claim_disables_further_writes() -> None:
    """If the sender returns False (the CP route's 409 worker-lost-
    claim response), the sink marks itself lost_claim and silently
    drops subsequent observations. The trial's MinIO writer keeps
    going — sink errors never bubble."""
    sent: list[list[dict[str, Any]]] = []

    async def sender(batch: list[dict[str, Any]]) -> bool:
        sent.append(batch)
        return False  # 409

    trial_id = uuid4()
    sink = CpEventSink(
        trial_id=trial_id, worker_id=uuid4(),
        send_batch=sender,
        flush_event_count=1,
        flush_interval_sec=999,
    )

    await sink.observe(_step_start(0, trial_id))
    assert sink.lost_claim is True

    # Subsequent observations no-op silently — sender NOT called again.
    await sink.observe(_env_exec(1, trial_id))
    await sink.observe(_step_end(2, trial_id))
    assert len(sent) == 1


async def test_sender_exception_does_not_propagate() -> None:
    """If the sender raises (network blip, CP outage, …), the sink
    logs and drops the batch — MinIO remains authoritative in this
    slice so failing the trial would be the wrong tradeoff."""

    async def sender(batch: list[dict[str, Any]]) -> bool:
        raise RuntimeError("network blip")

    trial_id = uuid4()
    sink = CpEventSink(
        trial_id=trial_id, worker_id=uuid4(),
        send_batch=sender,
        flush_event_count=1,
        flush_interval_sec=999,
    )
    # No exception escapes.
    await sink.observe(_step_start(0, trial_id))
    # Sink is NOT marked lost_claim (raise != False); the next
    # observation triggers another attempt.
    assert sink.lost_claim is False


async def test_flush_and_close_drains_remaining_buffer() -> None:
    """End-of-trial close drains whatever's left in the buffer even
    if neither threshold tripped during the trial. Called from
    TrajectoryWriter._close."""
    sent: list[list[dict[str, Any]]] = []

    async def sender(batch: list[dict[str, Any]]) -> bool:
        sent.append(batch)
        return True

    trial_id = uuid4()
    sink = CpEventSink(
        trial_id=trial_id, worker_id=uuid4(),
        send_batch=sender,
        flush_event_count=100,
        flush_interval_sec=999,
    )
    await sink.observe(_step_start(0, trial_id))
    await sink.observe(_step_end(1, trial_id))
    assert sent == []  # below threshold

    await sink.flush_and_close()
    assert len(sent) == 1
    assert [r["seq"] for r in sent[0]] == [0, 1]


async def test_flush_and_close_is_idempotent() -> None:
    sent: list[list[dict[str, Any]]] = []

    async def sender(batch: list[dict[str, Any]]) -> bool:
        sent.append(batch)
        return True

    trial_id = uuid4()
    sink = CpEventSink(
        trial_id=trial_id, worker_id=uuid4(),
        send_batch=sender,
        flush_event_count=100,
        flush_interval_sec=999,
    )
    await sink.observe(_step_start(0, trial_id))
    await sink.flush_and_close()
    # Second call: no extra sends, doesn't raise.
    await sink.flush_and_close()
    await sink.observe(_step_end(1, trial_id))  # after close — silent no-op
    assert len(sent) == 1


async def test_observe_raw_indexable_payload_buffers() -> None:
    """write_raw_dict path: subprocess adapter pre-shapes a dict with
    seq + kind on it; sink's observe_raw treats it like an indexed
    event."""
    sent: list[list[dict[str, Any]]] = []

    async def sender(batch: list[dict[str, Any]]) -> bool:
        sent.append(batch)
        return True

    trial_id = uuid4()
    sink = CpEventSink(
        trial_id=trial_id, worker_id=uuid4(),
        send_batch=sender,
        flush_event_count=1,
        flush_interval_sec=999,
    )
    await sink.observe_raw({
        "seq": 42, "kind": "agent_thought",
        "text": "hello", "step_id": "main",
    })
    assert sent[0][0]["seq"] == 42
    assert sent[0][0]["kind"] == "agent_thought"
    assert sent[0][0]["payload"]["text"] == "hello"


async def test_observe_raw_drops_payload_without_int_seq() -> None:
    """Subprocess adapters that pre-date the typed envelope can emit
    free-form events without `seq`/`kind`; the sink skips those
    rather than indexing them as orphan rows in trial_events."""
    sent: list[list[dict[str, Any]]] = []

    async def sender(batch: list[dict[str, Any]]) -> bool:
        sent.append(batch)
        return True

    sink = CpEventSink(
        trial_id=uuid4(), worker_id=uuid4(),
        send_batch=sender,
        flush_event_count=1,
        flush_interval_sec=999,
    )
    await sink.observe_raw({"some_other_field": "x"})  # no seq/kind
    await sink.observe_raw({"seq": "not_an_int", "kind": "x"})
    await sink.observe_raw({"seq": 0, "kind": 123})  # kind not str
    assert sent == []


async def test_time_based_flush_threshold() -> None:
    """When wall time since last flush exceeds flush_interval_sec, the
    next observe triggers a flush even if the count threshold isn't
    met. Keeps SSE-bound latency bounded."""
    sent: list[list[dict[str, Any]]] = []

    async def sender(batch: list[dict[str, Any]]) -> bool:
        sent.append(batch)
        return True

    trial_id = uuid4()
    sink = CpEventSink(
        trial_id=trial_id, worker_id=uuid4(),
        send_batch=sender,
        flush_event_count=999,  # never trips count threshold
        flush_interval_sec=0.05,
    )
    await sink.observe(_step_start(0, trial_id))
    assert sent == []  # neither threshold yet
    await asyncio.sleep(0.1)
    await sink.observe(_step_end(1, trial_id))  # triggers time flush
    assert len(sent) == 1


async def test_chunks_oversized_drain_at_max_batch_size() -> None:
    """If a single flush would exceed the CP route's 500-event
    per-request cap, the sink chunks the drain into multiple sends."""
    from loom.trajectory.cp_event_sink import _MAX_BATCH_SIZE

    sent: list[list[dict[str, Any]]] = []

    async def sender(batch: list[dict[str, Any]]) -> bool:
        sent.append(batch)
        return True

    trial_id = uuid4()
    sink = CpEventSink(
        trial_id=trial_id, worker_id=uuid4(),
        send_batch=sender,
        flush_event_count=_MAX_BATCH_SIZE + 50,
        flush_interval_sec=999,
    )
    # Backlog enough events that a naive single-batch flush would
    # exceed the CP cap.
    for i in range(_MAX_BATCH_SIZE + 50):
        await sink.observe(_step_start(i, trial_id))
    # flush_and_close should chunk across multiple sends.
    await sink.flush_and_close()
    sizes = [len(b) for b in sent]
    assert max(sizes) <= _MAX_BATCH_SIZE
    assert sum(sizes) == _MAX_BATCH_SIZE + 50


# Pytest-asyncio is auto-mode in the repo's conftest; mark just so the
# discovery doesn't get confused if a future pytest config flips back.
pytestmark = pytest.mark.asyncio
