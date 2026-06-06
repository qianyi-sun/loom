"""End-to-end TrajectoryWriter against a real MinIO via testcontainers."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from testcontainers.minio import MinioContainer

from loom.models.trajectory import StepStartEvent
from loom.trajectory.storage import MinioObjectStore
from loom.trajectory.writer import TrajectoryWriter


@pytest.fixture(scope="module")
def minio() -> Iterator[MinioContainer]:
    with MinioContainer() as m:
        yield m


@pytest.fixture
def store(minio: MinioContainer) -> MinioObjectStore:
    cfg = minio.get_config()
    return MinioObjectStore(
        endpoint_url=f"http://{cfg['endpoint']}",
        access_key=cfg["access_key"],
        secret_key=cfg["secret_key"],
    )


def _event(seq: int) -> StepStartEvent:
    return StepStartEvent(
        emitted_at=datetime.now(UTC),
        trial_id=uuid4(),
        step_id="main",
        seq=seq,
        instruction_excerpt=f"event {seq}",
    )


async def test_trajectory_writer_writes_to_minio(
    tmp_path: Path,
    store: MinioObjectStore,
    minio: MinioContainer,
) -> None:
    bucket_name = "trajectories"
    client = minio.get_client()
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    local = tmp_path / "events.jsonl"
    key = f"team/{uuid4()}/events.jsonl"
    writer = TrajectoryWriter(
        local_path=local,
        store=store,
        bucket=bucket_name,
        key=key,
        flush_event_count=2,
        flush_bytes=10_000_000,
        flush_sec=3600,
    )
    async with writer:
        for i in range(5):
            await writer.append(_event(i))

    fetched = await store.get_object(bucket=bucket_name, key=key)
    assert fetched.count(b"\n") == 5
