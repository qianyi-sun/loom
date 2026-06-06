from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from loom.models.trajectory import StepStartEvent
from loom.trajectory.storage import FakeObjectStore
from loom.trajectory.writer import TrajectoryWriter


def _event(seq: int) -> StepStartEvent:
    return StepStartEvent(
        emitted_at=datetime.now(UTC),
        trial_id=uuid4(),
        step_id="main",
        seq=seq,
        instruction_excerpt=f"event {seq}",
    )


@pytest.fixture
def store() -> FakeObjectStore:
    return FakeObjectStore()


async def test_writer_writes_local_first(tmp_path: Path, store: FakeObjectStore):
    local = tmp_path / "events.jsonl"
    writer = TrajectoryWriter(
        local_path=local, store=store,
        bucket="trajectories", key="t/abc/events.jsonl",
        flush_event_count=1000, flush_bytes=10_000_000, flush_sec=3600, min_part_bytes=0,
    )
    async with writer:
        await writer.append(_event(0))
        assert local.read_bytes().count(b"\n") == 1
        assert ("trajectories", "t/abc/events.jsonl") not in store.objects
    assert ("trajectories", "t/abc/events.jsonl") in store.objects
    assert store.objects[("trajectories", "t/abc/events.jsonl")].count(b"\n") == 1


async def test_writer_flush_on_event_count(tmp_path: Path, store: FakeObjectStore):
    local = tmp_path / "events.jsonl"
    writer = TrajectoryWriter(
        local_path=local, store=store,
        bucket="trajectories", key="t/x/events.jsonl",
        flush_event_count=3, flush_bytes=10_000_000, flush_sec=3600, min_part_bytes=0,
    )
    async with writer:
        for i in range(3):
            await writer.append(_event(i))
        assert writer.parts_uploaded >= 1


async def test_writer_flush_on_close(tmp_path: Path, store: FakeObjectStore):
    local = tmp_path / "events.jsonl"
    writer = TrajectoryWriter(
        local_path=local, store=store,
        bucket="trajectories", key="t/x/events.jsonl",
        flush_event_count=1000, flush_bytes=10_000_000, flush_sec=3600, min_part_bytes=0,
    )
    async with writer:
        await writer.append(_event(0))
        await writer.append(_event(1))
    assert ("trajectories", "t/x/events.jsonl") in store.objects
    contents = store.objects[("trajectories", "t/x/events.jsonl")]
    assert contents.count(b"\n") == 2


async def test_writer_remote_uri(tmp_path: Path, store: FakeObjectStore):
    local = tmp_path / "events.jsonl"
    writer = TrajectoryWriter(
        local_path=local, store=store,
        bucket="trajectories", key="t/x/events.jsonl",
    )
    assert writer.remote_uri == "s3://trajectories/t/x/events.jsonl"


async def test_writer_aborts_on_error(tmp_path: Path, store: FakeObjectStore):
    """If the async-with block raises, the multipart upload should abort, not complete."""
    local = tmp_path / "events.jsonl"
    writer = TrajectoryWriter(
        local_path=local, store=store,
        bucket="trajectories", key="t/x/events.jsonl",
        flush_event_count=1000, flush_bytes=10_000_000, flush_sec=3600, min_part_bytes=0,
    )
    with pytest.raises(RuntimeError, match="boom"):
        async with writer:
            await writer.append(_event(0))
            raise RuntimeError("boom")
    # Multipart aborted: object should NOT have landed.
    assert ("trajectories", "t/x/events.jsonl") not in store.objects


async def test_writer_close_reraises_final_flush_failure(tmp_path: Path):
    """Regression: a final-flush failure on the success path must escape, not
    be silently swallowed by an abort. Otherwise the caller thinks the
    trajectory shipped to MinIO when only the local file has it."""

    class BrokenStore(FakeObjectStore):
        async def upload_part(self, upload, *, part_number, body) -> None:  # type: ignore[no-untyped-def]
            raise RuntimeError("upload broken")

    local = tmp_path / "events.jsonl"
    store = BrokenStore()
    writer = TrajectoryWriter(
        local_path=local, store=store,
        bucket="t", key="k",
        flush_event_count=1000, flush_bytes=10_000_000, flush_sec=3600, min_part_bytes=0,
        flush_retries=1,
    )
    from loom.errors import TrajectoryFlushFailedError
    with pytest.raises(TrajectoryFlushFailedError):
        async with writer:
            await writer.append(_event(0))


async def test_writer_append_after_close_raises(tmp_path: Path, store: FakeObjectStore):
    local = tmp_path / "events.jsonl"
    writer = TrajectoryWriter(
        local_path=local, store=store,
        bucket="trajectories", key="t/x/events.jsonl",
    )
    async with writer:
        await writer.append(_event(0))
    with pytest.raises(RuntimeError, match="append after close"):
        await writer.append(_event(1))
