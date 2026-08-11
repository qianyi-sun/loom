from __future__ import annotations

from uuid import uuid4

from loom.pipeline.artifact_commit import ArtifactCommitService
from loom.trajectory.storage import FakeObjectStore
from tests.integration.pipeline_artifact_testkit import final_producer, plan, upload_all


async def test_equal_payloads_get_distinct_identity_bound_manifests() -> None:
    store = FakeObjectStore()
    service = ArtifactCommitService(store=store, bucket="artifacts")
    payload = b"same\n"
    producer = final_producer().model_copy(
        update={
            "input_lineage_artifact_ids": [uuid4()],
            "input_lineage_digests": ["sha256:" + "5" * 64],
        }
    )
    _grant, sealed = await upload_all(
        service,
        producer=producer,
        planned=[
            plan(index=0, name="first", path="first/artifact.json", payload=payload),
            plan(index=1, name="second", path="second/artifact.json", payload=payload),
        ],
        payloads=[payload, payload],
    )
    assert sealed.state == "committed_ready"
    manifests = [
        value
        for (_bucket, key), value in store.objects.items()
        if key.endswith("_artifact_manifest.json")
    ]
    assert len(manifests) == 2 and manifests[0] != manifests[1]
