from __future__ import annotations

from pathlib import Path

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


async def test_committed_scalar_is_ready_before_view_and_survives_release(
    tmp_path: Path,
) -> None:
    payload, manifest, binding = scalar_artifact()
    reader = FakeArtifactInputReadClient(manifest, payload)
    journal = ArtifactInputJournal(
        database_path=tmp_path / "state/cache.sqlite3",
        cas_root=tmp_path / "cas",
        capacity_bytes=1024 * 1024,
    )
    materializer = ArtifactInputMaterializer(
        read_client=reader,  # type: ignore[arg-type]
        journal=journal,
        attempt_input_root=tmp_path / "attempts",
        mounter=DeterministicReadOnlyViewMounter(),
    )
    execution = claim(binding)
    request = await materializer.materialize_inputs(
        claim=execution, cancellation=NeverCancelled()
    )

    async with request as inputs:
        entry = journal.get_entry(binding.items[0].manifest_sha256)
        assert entry is not None and entry.state == "ready" and entry.refcount == 1
        assert inputs.counters.file_bytes == len(payload)
    entry = journal.get_entry(binding.items[0].manifest_sha256)
    assert entry is not None and entry.refcount == 0
    assert not (tmp_path / "attempts" / str(execution.execution_attempt_id)).exists()
