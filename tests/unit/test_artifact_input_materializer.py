from __future__ import annotations

from pathlib import Path
from uuid import uuid4

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
    NeverCancelled,
    claim,
    scalar_artifact,
)


def _materializer(
    tmp_path: Path, reader: FakeArtifactInputReadClient
) -> tuple[ArtifactInputMaterializer, DeterministicReadOnlyViewMounter]:
    mounter = DeterministicReadOnlyViewMounter()
    return (
        ArtifactInputMaterializer(
            read_client=reader,  # type: ignore[arg-type]
            journal=ArtifactInputJournal(
                database_path=tmp_path / "state/cache.sqlite3",
                cas_root=tmp_path / "cas",
                capacity_bytes=10_000,
            ),
            attempt_input_root=tmp_path / "attempt-inputs",
            mounter=mounter,
        ),
        mounter,
    )


async def test_scalar_materializes_once_then_reuses_ready_entry(tmp_path: Path) -> None:
    payload, manifest, binding = scalar_artifact()
    reader = FakeArtifactInputReadClient(manifest, payload)
    materializer, mounter = _materializer(tmp_path, reader)

    first_set = await materializer.materialize_inputs(
        claim=claim(binding), cancellation=NeverCancelled()
    )
    async with first_set as first:
        assert first.input_view_digest is not None
        assert first.counters.manifest_open_count == 1
        assert first.counters.file_open_count == 1
        source = next(iter(mounter.mounts.values()))
        assert (source / "artifact.json").read_bytes() == payload
    assert not mounter.mounts

    second_set = await materializer.materialize_inputs(
        claim=claim(binding), cancellation=NeverCancelled()
    )
    async with second_set as second:
        assert second.counters.manifest_open_count == 1
        assert second.counters.file_open_count == 0
        assert second.counters.cas_rename_count == 0

    assert reader.manifest_opens == 2
    assert reader.file_opens == 1


async def test_cancelled_materialization_creates_no_view_or_lease(tmp_path: Path) -> None:
    payload, manifest, binding = scalar_artifact()
    reader = FakeArtifactInputReadClient(manifest, payload)
    materializer, mounter = _materializer(tmp_path, reader)
    request = await materializer.materialize_inputs(
        claim=claim(binding, attempt_id=uuid4()), cancellation=Cancelled()
    )

    with pytest.raises(ArtifactInputError, match="cancelled"):
        async with request:
            pass

    assert not mounter.mounts
    assert materializer.journal.capacity_snapshot().input_cache_reserved_bytes == 0


async def test_materialized_set_exit_is_idempotent(tmp_path: Path) -> None:
    payload, manifest, binding = scalar_artifact()
    materializer, _ = _materializer(
        tmp_path, FakeArtifactInputReadClient(manifest, payload)
    )
    request = await materializer.materialize_inputs(
        claim=claim(binding), cancellation=NeverCancelled()
    )

    await request.__aenter__()
    await request.close()
    await request.close()

    assert request._closed is True


async def test_ready_rename_before_journal_commit_leaves_no_orphan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload, manifest, binding = scalar_artifact()
    materializer, _ = _materializer(
        tmp_path, FakeArtifactInputReadClient(manifest, payload)
    )
    item = binding.items[0]

    def fail_mark_ready(**_: object) -> object:
        raise RuntimeError("journal commit interrupted")

    monkeypatch.setattr(materializer.journal, "mark_ready", fail_mark_ready)
    request = await materializer.materialize_inputs(
        claim=claim(binding), cancellation=NeverCancelled()
    )

    with pytest.raises(RuntimeError, match="journal commit interrupted"):
        async with request:
            pass

    assert materializer.journal.get_entry(item.manifest_sha256) is None
    assert not materializer.journal.ready_path(item.manifest_sha256).exists()
