from __future__ import annotations

from pathlib import Path

import pytest

from loom_worker.artifact_input_journal import ArtifactInputJournal
from loom_worker.artifact_inputs import (
    ArtifactInputError,
    ArtifactInputMaterializer,
    DeterministicReadOnlyViewMounter,
)
from tests.pipeline_input_helpers import (
    Cancelled,
    FakeArtifactInputReadClient,
    claim,
    scalar_artifact,
)


async def test_cancel_before_file_open_leaves_no_ready_partial_view_or_lease(
    tmp_path: Path,
) -> None:
    payload, manifest, binding = scalar_artifact()
    reader = FakeArtifactInputReadClient(manifest, payload)
    journal = ArtifactInputJournal(
        database_path=tmp_path / "state/cache.sqlite3",
        cas_root=tmp_path / "cas",
        capacity_bytes=1024,
    )
    mounter = DeterministicReadOnlyViewMounter()
    materializer = ArtifactInputMaterializer(
        read_client=reader,  # type: ignore[arg-type]
        journal=journal,
        attempt_input_root=tmp_path / "attempts",
        mounter=mounter,
    )
    request = await materializer.materialize_inputs(
        claim=claim(binding), cancellation=Cancelled()
    )

    with pytest.raises(ArtifactInputError, match="cancelled"):
        await request.__aenter__()

    assert reader.file_opens == 0
    assert journal.get_entry(binding.items[0].manifest_sha256) is None
    assert not list((tmp_path / "cas" / ".partial").glob("*"))
    assert not mounter.mounts
