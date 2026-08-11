from __future__ import annotations

import pytest

from loom.pipeline.artifact_commit import ArtifactCommitError, ArtifactCommitService, UploadAuthV1
from loom.trajectory.storage import FakeObjectStore
from tests.integration.pipeline_artifact_testkit import chunks, digest, final_producer, plan


class ArmedOnce:
    def __init__(self) -> None:
        self.calls = 0

    async def after_part_persisted(self, **kwargs) -> bool:
        self.calls += 1
        return True


async def test_s08_fault_fires_only_after_part_persist_and_aborts() -> None:
    hook = ArmedOnce()
    store = FakeObjectStore()
    service = ArtifactCommitService(store=store, bucket="artifacts", multipart_fault_hook=hook)
    payload = b"first-part"
    grant = await service.prepare_session(
        producer=final_producer(),
        files=[plan(payload=payload)],
        idempotency_key="s08",
        request_digest="sha256:" + "8" * 64,
    )
    with pytest.raises(ArtifactCommitError, match="acceptance_fault_fired"):
        await service.write_part(
            session_id=grant.upload_session_id,
            file_index=0,
            part_number=1,
            content_length=len(payload),
            content_sha256=digest(payload),
            body=chunks(payload),
            auth=UploadAuthV1(upload_token=grant.upload_token),
        )
    assert hook.calls == 1 and not store._multiparts
