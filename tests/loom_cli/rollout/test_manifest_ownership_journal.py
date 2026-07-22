from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from loom_cli.rollout.manifest_ownership_journal import (
    ManifestOwnershipJournal,
    ManifestOwnershipJournalError,
)


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "state"
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    return root


def test_inventory_is_no_replace_and_events_are_fsynced_append_only(tmp_path: Path) -> None:
    journal = ManifestOwnershipJournal(_root(tmp_path), service_uid=os.geteuid())
    request_id = "req-manifest-ownership-12345678"
    inventory = {"schema_version": 1, "inventory_sha256": "a" * 64}
    journal.publish_inventory(request_id, inventory)
    journal.append(request_id, {"event": "inventory-approved"})
    journal.append(request_id, {"event": "completed"})

    directory = journal.root / request_id
    persisted = json.loads((directory / "inventory.json").read_text())
    assert persisted == {"request_id": request_id, **inventory}
    events = [json.loads(line) for line in (directory / "events.jsonl").read_text().splitlines()]
    assert [event["event"] for event in events] == ["inventory-approved", "completed"]
    assert all(event["request_id"] == request_id for event in events)
    assert (directory / "inventory.json").stat().st_mode & 0o777 == 0o600
    assert (directory / "events.jsonl").stat().st_mode & 0o777 == 0o600

    with pytest.raises(ManifestOwnershipJournalError, match="already exists"):
        journal.publish_inventory(request_id, inventory)


def test_journal_rejects_symlink_mode_and_missing_inventory(tmp_path: Path) -> None:
    state = _root(tmp_path)
    journal = ManifestOwnershipJournal(state, service_uid=os.geteuid())
    maintenance = state / "maintenance"
    maintenance.symlink_to(tmp_path / "escape")
    with pytest.raises(ManifestOwnershipJournalError, match="unsafe"):
        journal.publish_inventory(
            "req-manifest-ownership-12345678",
            {"schema_version": 1},
        )

    maintenance.unlink()
    maintenance.mkdir(mode=0o700)
    maintenance.chmod(0o700)
    journal.root.mkdir(mode=0o700)
    journal.root.chmod(0o700)
    request = journal.root / "req-manifest-ownership-abcdefgh"
    request.mkdir(mode=0o700)
    request.chmod(0o700)
    with pytest.raises(ManifestOwnershipJournalError, match="unavailable"):
        journal.append(request.name, {"event": "invalid"})

    state.chmod(0o755)
    with pytest.raises(ManifestOwnershipJournalError, match="unsafe"):
        journal.publish_inventory(
            "req-manifest-ownership-abcd1234",
            {"schema_version": 1},
        )
