from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import httpx

from loom.pipeline.artifact_commit import StoredFileV1
from loom_worker.artifact_inputs import ArtifactInputReadClient
from loom_worker.control_plane_client import HttpControlPlaneClient
from tests.pipeline_input_helpers import NeverCancelled, claim, scalar_artifact


async def test_file_read_uses_claim_headers_strong_etag_and_no_initial_range(
    tmp_path: Path,
) -> None:
    payload, manifest, binding = scalar_artifact()
    observed: dict[str, str] = {}

    def transport(request: httpx.Request) -> httpx.Response:
        observed.update(request.headers)
        return httpx.Response(
            200,
            headers={
                "ETag": f'"{manifest.stored_files[0].sha256}"',
                "Content-Length": str(len(payload)),
                "Content-Type": "application/json",
                "Content-Encoding": "identity",
            },
            content=payload,
        )

    async with httpx.AsyncClient(
        base_url="http://cp", transport=httpx.MockTransport(transport)
    ) as client:
        reader = ArtifactInputReadClient(
            HttpControlPlaneClient("http://cp", "worker-token", _client=client)
        )
        execution = claim(binding)
        size, opens = await reader.read_file(
            claim=execution,
            binding_name=binding.binding_name,
            item=binding.items[0],
            stored=StoredFileV1.model_validate(manifest.stored_files[0]),
            destination=tmp_path / str(uuid4()),
            cancellation=NeverCancelled(),
        )

    assert (size, opens) == (len(payload), 1)
    assert observed["if-match"] == f'"{manifest.stored_files[0].sha256}"'
    assert "range" not in observed
    assert observed["x-loom-claim-id"] == str(execution.claim_id)
