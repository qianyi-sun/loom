"""S3-artifacts state backend.

Uses the in-memory FakeObjectStore for fast unit coverage; a MinIO
testcontainer round-trip is exercised in the end-to-end integration
suite (Task 19).
"""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from loom.family_run.state_backends import S3ArtifactsStateBackend
from loom.trajectory.storage import FakeObjectStore


@pytest.mark.asyncio
async def test_initialize_provisions_empty_prefix() -> None:
    store = FakeObjectStore()
    await store.ensure_bucket("artifacts")
    backend = S3ArtifactsStateBackend(store=store, bucket="artifacts")
    batch_id = uuid4()
    uri = await backend.initialize(
        batch_id=batch_id,
        family_key="fam",
        params={},
    )
    assert uri.startswith(f"s3://artifacts/family-state/{batch_id}/fam/")


@pytest.mark.asyncio
async def test_upload_download_round_trip(tmp_path: Path) -> None:
    store = FakeObjectStore()
    await store.ensure_bucket("artifacts")
    backend = S3ArtifactsStateBackend(store=store, bucket="artifacts")
    batch_id = uuid4()
    uri = await backend.initialize(
        batch_id=batch_id,
        family_key="fam",
        params={},
    )
    src = tmp_path / "src"
    src.mkdir()
    (src / "skill.txt").write_text("hello\n")

    new_uri = await backend.upload(uri, src, {})
    dst = tmp_path / "dst"
    await backend.download(new_uri, dst, {})
    assert (dst / "skill.txt").read_text() == "hello\n"
