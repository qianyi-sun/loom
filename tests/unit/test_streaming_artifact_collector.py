from __future__ import annotations

import os

import pytest

from loom.pipeline.streaming_artifact_collector import (
    ArtifactCollectionError,
    StreamingArtifactCollector,
)


async def test_collector_hashes_in_bounded_chunks_without_read_bytes(tmp_path) -> None:
    payload = b"a" * (3 * 1024 * 1024 + 7)
    (tmp_path / "artifact.json").write_bytes(payload)
    collector = StreamingArtifactCollector(tmp_path, chunk_size=1024 * 1024)
    chunks = [chunk async for chunk in collector.stream("artifact.json")]
    assert max(map(len, chunks)) == 1024 * 1024
    observed = await collector.inspect("artifact.json", max_bytes=len(payload))
    assert observed.size_bytes == len(payload)
    assert observed.sha256.startswith("sha256:")


async def test_collector_rejects_links_and_limit_overflow(tmp_path) -> None:
    outside = tmp_path.parent / "outside-artifact"
    outside.write_bytes(b"secret")
    os.symlink(outside, tmp_path / "linked")
    collector = StreamingArtifactCollector(tmp_path, chunk_size=2)
    with pytest.raises(ArtifactCollectionError):
        await collector.inspect("linked", max_bytes=100)
    (tmp_path / "large").write_bytes(b"abc")
    with pytest.raises(ArtifactCollectionError, match="maximum"):
        await collector.inspect("large", max_bytes=2)
