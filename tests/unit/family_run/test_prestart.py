"""Worker pre-start helper: download + bind-mount family state."""

from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import uuid4

import pytest

from loom.family_run.prestart import prepare_family_state_mount
from loom.family_run.state_backends import S3ArtifactsStateBackend
from loom.trajectory.storage import FakeObjectStore


@pytest.mark.asyncio
async def test_prepare_mount_downloads_and_returns_volume_tuple(
    tmp_path: Path,
) -> None:
    store = FakeObjectStore()
    await store.ensure_bucket("artifacts")
    backend = S3ArtifactsStateBackend(store=store, bucket="artifacts")
    batch_id = uuid4()
    uri = await backend.initialize(batch_id=batch_id, family_key="fam", params={})
    # Seed a file into the state.
    src = tmp_path / "src"
    src.mkdir()
    (src / "hello.txt").write_text("world\n")
    new_uri = await backend.upload(uri, src, {})

    mount = await prepare_family_state_mount(
        trial_id="trial-1",
        state_uri=new_uri,
        mount_path="/root/.skills",
        state_backend=backend,
    )
    try:
        assert mount.container_dir == "/root/.skills"
        assert mount.mode == "rw"
        # The tarball round-trip put ``hello.txt`` into the host dir.
        assert (mount.host_dir / "hello.txt").read_text() == "world\n"
        assert mount.as_volume_tuple() == (
            str(mount.host_dir), "/root/.skills", "rw",
        )
    finally:
        mount.cleanup()
    # Cleanup removed the temp dir.
    assert not mount.host_dir.exists()


@pytest.mark.asyncio
async def test_prepare_mount_times_out_and_cleans_up() -> None:
    class _SlowBackend:
        async def download(self, uri, dst, params):
            await asyncio.sleep(5.0)

    with pytest.raises(asyncio.TimeoutError):
        await prepare_family_state_mount(
            trial_id="trial-2",
            state_uri="s3://x/y",
            mount_path="/root/.skills",
            state_backend=_SlowBackend(),
            download_timeout_sec=0.01,
        )
