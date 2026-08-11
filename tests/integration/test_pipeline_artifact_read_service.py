from __future__ import annotations

from uuid import uuid4

import pytest
from fastapi import HTTPException

from loom.pipeline.keys import digest_bytes
from loom.trajectory.storage import FakeObjectStore
from loom_control_plane.artifact_read_service import (
    ArtifactReadService,
    ResolvedArtifactInput,
    ResolvedStoredFile,
)


class Resolver:
    def __init__(self, resolved) -> None:
        self.resolved = resolved

    async def resolve(self, **kwargs):
        return self.resolved


async def test_manifest_and_start_only_range_are_locator_free() -> None:
    manifest = b'{"schema_version":"loom.artifact-manifest.v1"}\n'
    payload = b"0123456789"
    manifest_digest = digest_bytes(manifest)
    file_digest = digest_bytes(payload)
    store = FakeObjectStore(objects={("artifacts", "opaque/internal"): payload})
    resolved = ResolvedArtifactInput(
        artifact_id=uuid4(),
        manifest_bytes=manifest,
        manifest_sha256=manifest_digest,
        root_marker_valid=True,
        files=(
            ResolvedStoredFile(
                0, "opaque/internal", "application/octet-stream", len(payload), file_digest
            ),
        ),
    )
    service = ArtifactReadService(resolver=Resolver(resolved), store=store, bucket="artifacts")
    response = await service.read_manifest(
        attempt_id=uuid4(),
        binding_name="input",
        item_key="singleton",
        if_match=f'"{manifest_digest}"',
    )
    assert response.headers["etag"] == f'"{manifest_digest}"'
    ranged = await service.read_file(
        attempt_id=uuid4(),
        binding_name="input",
        item_key="singleton",
        file_index=0,
        if_match=f'"{file_digest}"',
        range_header="bytes=4-",
    )
    assert ranged.status_code == 206 and ranged.headers["content-range"] == "bytes 4-9/10"
    with pytest.raises(HTTPException) as exc:
        await service.read_file(
            attempt_id=uuid4(),
            binding_name="input",
            item_key="singleton",
            file_index=0,
            if_match=f'"{file_digest}"',
            range_header="bytes=-4",
        )
    assert exc.value.status_code == 416
