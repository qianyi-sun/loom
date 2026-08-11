from __future__ import annotations

from pathlib import Path

from loom.pipeline.spec import BindingItemV1, BindingSetV1
from loom_worker.artifact_input_journal import ArtifactInputJournal
from loom_worker.artifact_inputs import (
    ArtifactInputMaterializer,
    DeterministicReadOnlyViewMounter,
)
from tests.pipeline_input_helpers import (
    FakeArtifactInputReadClient,
    NeverCancelled,
    claim,
    scalar_artifact,
)


async def test_many_binding_deduplicates_cas_but_keeps_item_leases(tmp_path: Path) -> None:
    payload, manifest, scalar = scalar_artifact()
    original = scalar.items[0]
    many = BindingSetV1(
        binding_name="datasets",
        artifact_type=scalar.artifact_type,
        cardinality="many",
        items=[
            BindingItemV1(**original.model_dump(exclude={"item_key"}), item_key="a"),
            BindingItemV1(**original.model_dump(exclude={"item_key"}), item_key="b"),
        ],
    )
    reader = FakeArtifactInputReadClient(manifest, payload)
    journal = ArtifactInputJournal(
        database_path=tmp_path / "state/cache.sqlite3",
        cas_root=tmp_path / "cas",
        capacity_bytes=1024 * 1024,
    )
    mounter = DeterministicReadOnlyViewMounter()
    materializer = ArtifactInputMaterializer(
        read_client=reader,  # type: ignore[arg-type]
        journal=journal,
        attempt_input_root=tmp_path / "attempts",
        mounter=mounter,
    )
    request = await materializer.materialize_inputs(
        claim=claim(many), cancellation=NeverCancelled()
    )

    async with request as inputs:
        assert inputs.root is not None
        manifest_path = inputs.root / "datasets/binding_manifest.json"
        assert manifest_path.read_bytes().endswith(b"\n")
        assert len(mounter.mounts) == 2
        entry = journal.get_entry(original.manifest_sha256)
        assert entry is not None and entry.refcount == 2

    assert reader.file_opens == 1
