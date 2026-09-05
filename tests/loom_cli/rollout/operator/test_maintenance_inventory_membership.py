"""Directory membership must not depend on filesystem timestamp granularity."""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from loom_cli.rollout.manifest_ownership_journal import ManifestOwnershipJournal
from loom_cli.rollout.operator import preflight_artifact_references as inventory_module
from loom_cli.rollout.operator.preflight_artifact_references import (
    InstalledMaintenanceReferenceInventory,
    PreflightArtifactReferenceInventoryError,
)
from tests.loom_cli.rollout.operator.test_broker import make_config
from tests.loom_cli.rollout.operator.test_preflight_artifact_references import (
    NOW,
    _capacity_plan,
    _manifest_inventory,
    _write_private_json,
)


@pytest.fixture
def colliding_directory_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    original = inventory_module._metadata_identity

    def identity(metadata: os.stat_result) -> tuple[int, ...]:
        value = original(metadata)
        if stat.S_ISDIR(metadata.st_mode):
            # Keep device/inode/type/permissions/owner observations real. Pin
            # directory size/linkcount/times so membership checks cannot rely
            # on a new file happening to advance a filesystem timestamp.
            return (*value[:3], 2, *value[4:6], 4096, 0, 0)
        return value

    monkeypatch.setattr(inventory_module, "_metadata_identity", identity)


def _inventory(
    tmp_path: Path, scope: str
) -> tuple[InstalledMaintenanceReferenceInventory, Path, Path]:
    config = make_config(tmp_path)
    config.state_root.mkdir(parents=True, mode=0o700)
    config.state_root.chmod(0o700)
    if scope == "lifecycle":
        plan = _capacity_plan()
        target = config.state_root / "lifecycle-capacity-jobs" / f"{plan.plan_digest}.claim.json"
        _write_private_json(
            target,
            {
                "approved_plan_digest": plan.plan_digest,
                "claimed_at": NOW.isoformat(),
                "plan": plan.to_dict(),
                "schema_version": 1,
            },
        )
        trigger = target
    else:
        journal = ManifestOwnershipJournal(config.state_root, service_uid=os.geteuid())
        first = "req-manifest-ownership-12345678"
        journal.publish_inventory(first, _manifest_inventory("7" * 64))
        target = journal.root / first / "inventory.json"
        trigger = target
        if scope == "manifest-parent":
            second = "req-manifest-ownership-23456789"
            journal.publish_inventory(second, _manifest_inventory("8" * 64))
            # Change an already-read request while the next request is read.
            # Rechecking only the currently-read request cannot catch this.
            trigger = journal.root / second / "inventory.json"
            target = target.parent
    return (
        InstalledMaintenanceReferenceInventory(config=config, service_uid=os.geteuid()),
        target,
        trigger,
    )


@pytest.mark.parametrize("scope", ["manifest-request", "manifest-parent", "lifecycle"])
@pytest.mark.parametrize("change", ["addition", "removal", "replacement"])
def test_inventory_rejects_membership_drift_with_colliding_directory_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    colliding_directory_metadata: None,
    scope: str,
    change: str,
) -> None:
    inventory, target, trigger = _inventory(tmp_path, scope)
    original = inventory_module._read_private_json
    changed = False

    def read_then_change(path: Path, *, service_uid: int) -> dict[str, object]:
        nonlocal changed
        value = original(path, service_uid=service_uid)
        if path == trigger and not changed:
            changed = True
            if change == "addition":
                extra = target.parent / "unexpected"
                extra.write_text("new entry\n", encoding="utf-8")
                extra.chmod(0o600)
            else:
                # Preserve the old inode outside the journal, so replacement
                # tests do not depend on the filesystem's inode reuse policy.
                held = tmp_path / "retired-entry"
                target.rename(held)
                if change == "replacement":
                    if held.is_dir():
                        target.mkdir(mode=0o700)
                    else:
                        target.write_bytes(held.read_bytes())
                        target.chmod(0o600)
        return value

    monkeypatch.setattr(inventory_module, "_read_private_json", read_then_change)
    with pytest.raises(PreflightArtifactReferenceInventoryError, match="changed"):
        inventory()
    assert changed


@pytest.mark.parametrize("scope", ["manifest-request", "manifest-parent", "lifecycle"])
def test_unchanged_inventory_accepts_colliding_directory_metadata(
    tmp_path: Path,
    colliding_directory_metadata: None,
    scope: str,
) -> None:
    inventory, _, _ = _inventory(tmp_path, scope)
    assert inventory()
