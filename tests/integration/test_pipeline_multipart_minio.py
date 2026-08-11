from __future__ import annotations

import pytest
from testcontainers.minio import MinioContainer

from loom.pipeline.artifact_commit import ArtifactCommitService
from loom.trajectory.storage import MinioObjectStore
from tests.integration.pipeline_artifact_testkit import final_producer, plan, upload_all

pytestmark = pytest.mark.docker


async def test_real_minio_multipart_commit_and_readback() -> None:
    with MinioContainer() as container:
        config = container.get_config()
        store = MinioObjectStore(
            endpoint_url="http://" + config["endpoint"],
            access_key=config["access_key"],
            secret_key=config["secret_key"],
        )
        await store.ensure_bucket("artifacts")
        service = ArtifactCommitService(store=store, bucket="artifacts")
        _grant, committed = await upload_all(
            service,
            producer=final_producer(),
            planned=[plan(payload=b'{"ok":true}\n')],
            payloads=[b'{"ok":true}\n'],
        )
        assert committed.state == "committed_ready"
