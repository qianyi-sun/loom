from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from loom_cli.rollout.operator import backup as backup_module
from loom_cli.rollout.operator.backup import BackupCreator, BackupError, VerifiedBackup
from loom_cli.rollout.operator.backup_lease import BackupLease
from loom_cli.rollout.operator.backup_retirement import (
    BackupPayloadActivator,
    BackupPayloadRetirer,
)
from loom_cli.rollout.operator.backup_rotation import (
    BackupPayloadPhase,
    BackupPayloadRecord,
    BackupRetirementRecord,
)
from loom_cli.rollout.operator.store import RequestStore
from tests.loom_cli.rollout.operator.test_backup import make_config


def _payload(
    tmp_path: Path, *, with_manifest: bool
) -> tuple[BackupPayloadRetirer, Path, str | None]:
    config = make_config(tmp_path)
    bundle = config.rollout_root / "backups" / "20260719T210000Z-req-retire000"
    (bundle / "postgres").mkdir(parents=True, mode=0o700)
    bundle.parent.chmod(0o700)
    bundle.chmod(0o700)
    payload = bundle / "postgres" / "dump.bin"
    payload.write_bytes(b"payload")
    payload.chmod(0o600)
    digest = None
    if with_manifest:
        manifest = bundle / "backup-manifest.json"
        manifest.write_bytes(b"{}\n")
        manifest.chmod(0o600)
        digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    store = RequestStore(tmp_path / "request-store")
    return (
        BackupPayloadRetirer(
            creator=BackupCreator(config, service_uid=os.geteuid()),
            store=store,
        ),
        bundle,
        digest,
    )


def test_manifest_payload_retirement_persists_compact_evidence_and_receipt(
    tmp_path: Path,
) -> None:
    retirer, bundle, digest = _payload(tmp_path, with_manifest=True)
    assert digest is not None
    record = BackupRetirementRecord(
        payload_id="payload-retire000",
        request_id="req-retire000",
        bundle_name=bundle.name,
        reason="superseded",
        manifest_sha256=digest,
    )

    retirer(record)
    retirer(record)

    assert not bundle.exists()
    root = retirer.store.backup_retirements_root
    assert (root / "payload-retire000.json").is_file()
    assert (root / "payload-retire000.deleted.json").is_file()


def test_incomplete_payload_retirement_is_exact_and_idempotent(tmp_path: Path) -> None:
    retirer, bundle, digest = _payload(tmp_path, with_manifest=False)
    assert digest is None
    record = BackupRetirementRecord(
        payload_id="payload-retire001",
        request_id="req-retire000",
        bundle_name=bundle.name,
        reason="failed",
    )

    retirer(record)
    retirer(record)

    assert not bundle.exists()
    assert (retirer.store.backup_retirements_root / "payload-retire001.json").is_file()


def test_recovery_activator_revalidates_identity_without_requiring_freshness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    creator = BackupCreator(config, service_uid=os.geteuid())
    manifest = (
        config.rollout_root / "backups" / "20260719T210000Z-req-active000" / "backup-manifest.json"
    )
    calls: list[tuple[VerifiedBackup, bool]] = []

    def activate(backup: VerifiedBackup, *, enforce_freshness: bool = True) -> None:
        calls.append((backup, enforce_freshness))

    monkeypatch.setattr(creator, "latest_points_to", lambda _bundle_name: False)
    monkeypatch.setattr(creator, "activate", activate)
    lease = BackupLease(
        lease_id="lease-active00000",
        source_request_id="req-active000",
        manifest_sha256="a" * 64,
        component_sha256={"postgres": "b" * 64},
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=17,
        db_snapshot_identity="pgdump-sha256:" + "b" * 64,
        schema_revision="0072",
        object_inventory_root="c" * 64,
        created_at=datetime(2026, 7, 19, 21, tzinfo=UTC),
        restore_verified_at=datetime(2026, 7, 19, 21, 5, tzinfo=UTC),
        expires_at=datetime(2026, 7, 20, 21, tzinfo=UTC),
    )
    record = BackupPayloadRecord(
        payload_id="payload-active00",
        request_id="req-active000",
        bundle_name=manifest.parent.name,
        phase=BackupPayloadPhase.ACTIVE,
        created_at=datetime(2026, 7, 19, 21, tzinfo=UTC),
        manifest_sha256=lease.manifest_sha256,
        lease=lease,
    )

    BackupPayloadActivator(creator=creator, enforce_freshness=False)(record)

    assert calls == [(VerifiedBackup(manifest, "a" * 64), False)]


def test_already_converged_activator_skips_full_payload_revalidation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    creator = BackupCreator(config, service_uid=os.geteuid())
    lease = BackupLease(
        lease_id="lease-active00000",
        source_request_id="req-active000",
        manifest_sha256="a" * 64,
        component_sha256={"postgres": "b" * 64},
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=17,
        db_snapshot_identity="pgdump-sha256:" + "b" * 64,
        schema_revision="0072",
        object_inventory_root="c" * 64,
        created_at=datetime(2026, 7, 19, 21, tzinfo=UTC),
        restore_verified_at=datetime(2026, 7, 19, 21, 5, tzinfo=UTC),
        expires_at=datetime(2026, 7, 20, 21, tzinfo=UTC),
    )
    record = BackupPayloadRecord(
        payload_id="payload-active00",
        request_id="req-active000",
        bundle_name="20260719T210000Z-req-active000",
        phase=BackupPayloadPhase.ACTIVE,
        created_at=datetime(2026, 7, 19, 21, tzinfo=UTC),
        manifest_sha256=lease.manifest_sha256,
        lease=lease,
    )
    observed: list[str] = []
    monkeypatch.setattr(
        creator,
        "latest_points_to",
        lambda bundle_name: observed.append(bundle_name) or True,
    )
    monkeypatch.setattr(
        creator,
        "activate",
        lambda *_args, **_kwargs: pytest.fail("converged replay must not revalidate payload"),
    )

    BackupPayloadActivator(
        creator=creator,
        enforce_freshness=False,
        allow_metadata_fast_path=True,
    )(record)

    assert observed == [record.bundle_name]


def test_latest_pointer_fast_path_is_exact_and_unsafe_targets_fail_closed(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    backups = config.rollout_root / "backups"
    expected = backups / "20260719T210000Z-req-active000"
    other = backups / "20260719T220000Z-req-other0000"
    expected.mkdir(parents=True, mode=0o700)
    other.mkdir(mode=0o700)
    backups.chmod(0o700)
    expected.chmod(0o700)
    other.chmod(0o700)
    latest = backups / "latest"
    latest.symlink_to(expected.name)
    creator = BackupCreator(config, service_uid=os.geteuid())

    assert creator.latest_points_to(expected.name) is True
    assert creator.latest_points_to(other.name) is False

    latest.unlink()
    latest.symlink_to("../outside")
    with pytest.raises(BackupError, match="latest_publish_failed"):
        creator.latest_points_to(expected.name)


def test_latest_pointer_fast_path_fails_closed_when_target_is_swapped_before_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    backups = config.rollout_root / "backups"
    expected = backups / "20260719T210000Z-req-active000"
    expected.mkdir(parents=True, mode=0o700)
    backups.chmod(0o700)
    expected.chmod(0o700)
    (backups / "latest").symlink_to(expected.name)
    creator = BackupCreator(config, service_uid=os.geteuid())
    original_open = os.open
    swapped = False

    def racing_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal swapped
        if path == expected.name and dir_fd is not None and not swapped:
            swapped = True
            os.rename(
                expected.name,
                "raced-old",
                src_dir_fd=dir_fd,
                dst_dir_fd=dir_fd,
            )
            os.mkdir(expected.name, mode=0o700, dir_fd=dir_fd)
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(backup_module.os, "open", racing_open)

    with pytest.raises(BackupError, match="latest_publish_failed"):
        creator.latest_points_to(expected.name)

    assert swapped is True


def test_latest_pointer_fast_path_fails_closed_when_open_target_name_is_swapped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    backups = config.rollout_root / "backups"
    expected = backups / "20260719T210000Z-req-active000"
    expected.mkdir(parents=True, mode=0o700)
    backups.chmod(0o700)
    expected.chmod(0o700)
    (backups / "latest").symlink_to(expected.name)
    creator = BackupCreator(config, service_uid=os.geteuid())
    original_readlink = os.readlink
    read_count = 0

    def racing_readlink(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        *,
        dir_fd: int | None = None,
    ) -> str | bytes:
        nonlocal read_count
        target = original_readlink(path, dir_fd=dir_fd)
        if path == "latest":
            read_count += 1
            if read_count == 2:
                assert dir_fd is not None
                os.rename(
                    expected.name,
                    "raced-old",
                    src_dir_fd=dir_fd,
                    dst_dir_fd=dir_fd,
                )
                os.mkdir(expected.name, mode=0o700, dir_fd=dir_fd)
        return target

    monkeypatch.setattr(backup_module.os, "readlink", racing_readlink)

    with pytest.raises(BackupError, match="latest_publish_failed"):
        creator.latest_points_to(expected.name)

    assert read_count == 2
