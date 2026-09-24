import io
import tarfile

import pytest

from loom.trial import mutable_snapshot as snapshot
from loom.trial.workspace_snapshot import WorkspaceSnapshotError


def archive(path, *, entries=1, size=1):
    with tarfile.open(path, "w") as stream:
        for index in range(entries):
            entry = tarfile.TarInfo(f"file-{index}")
            entry.size = size
            stream.addfile(entry, io.BytesIO(b"x" * size))


def test_archive_expansion_and_entry_limits_are_enforced(tmp_path, monkeypatch):
    path = tmp_path / "state.tar"
    archive(path, entries=3)
    monkeypatch.setattr(snapshot, "MAX_MUTABLE_ENTRIES", 2)
    with pytest.raises(WorkspaceSnapshotError, match="content limits"):
        snapshot._archive_evidence(path)


def test_total_paths_cannot_multiply_transfer_budget():
    records = [dict(size_bytes=150 * 1024**2, expanded_bytes=150 * 1024**2, entries=1)] * 2
    with pytest.raises(WorkspaceSnapshotError, match="aggregate"):
        snapshot._check_totals(records)


def test_archive_evidence_refuses_symlink(tmp_path):
    target = tmp_path / "target.tar"
    archive(target)
    link = tmp_path / "state.tar"
    link.symlink_to(target)
    with pytest.raises(WorkspaceSnapshotError, match="regular file"):
        snapshot._archive_evidence(link)


async def test_reference_budget_is_shared_and_drivers_without_trusted_inspection_fail(monkeypatch):
    from pathlib import PurePosixPath

    from loom.errors import DriverError

    references = (PurePosixPath('/bin/first'), PurePosixPath('/bin/second'))
    with pytest.raises(WorkspaceSnapshotError, match='trusted'):
        await snapshot._reference_evidence(object(), references)

    inspected = []

    class Inspector:
        async def inspect_reference_file(self, path, *, max_bytes):
            inspected.append((path, max_bytes))
            if max_bytes < 7:
                raise DriverError('reference over budget')
            return {'size_bytes': 7}

    monkeypatch.setattr(snapshot, 'MAX_MUTABLE_BYTES', 10)
    with pytest.raises(WorkspaceSnapshotError, match='reference'):
        await snapshot._reference_evidence(Inspector(), references)
    assert inspected == [(references[0], 10), (references[1], 3)]
