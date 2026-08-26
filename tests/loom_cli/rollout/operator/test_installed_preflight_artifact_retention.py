from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import pytest

from loom_cli.rollout.operator import installed_preflight_artifact_retention as retention_module
from loom_cli.rollout.operator.installed_preflight_artifact_retention import (
    InstalledPreflightArtifactRetentionError,
    InstalledPreflightArtifactRetentionService,
)
from loom_cli.rollout.operator.model import ActivePointer
from loom_cli.rollout.operator.store import RequestStore
from loom_cli.rollout.preflight_artifact_retention import (
    ARTIFACT_FILE_NAMES,
    MAX_RETIREMENTS_PER_PLAN,
    PreflightArtifactProtection,
)
from loom_cli.rollout.preflight_artifact_store import PreflightArtifactStore
from tests.loom_cli.rollout.operator.test_broker import make_config
from tests.loom_cli.rollout.test_preflight_artifact_store import (
    _images,
    _manifests,
    _migration,
    _production_defaults,
)

INVENTORY_AT = datetime(2030, 8, 26, 12, tzinfo=UTC)
SECOND_NS = 1_000_000_000
DAY_NS = 24 * 60 * 60 * SECOND_NS
INVENTORY_NS = int(INVENTORY_AT.timestamp()) * SECOND_NS
CUTOFF_NS = INVENTORY_NS - 7 * DAY_NS


def _publish_bundle(store: PreflightArtifactStore, mutation_epoch: int):
    images = _images()
    return store.publish(
        candidate_sha="a" * 40,
        candidate_tree="f" * 40,
        mutation_epoch=mutation_epoch,
        images=images,
        manifests=_manifests(images),
        migration=_migration(images),
        production_defaults=_production_defaults(),
        migration_plan_sha256="1" * 64,
        migration_target_revision="0067",
        browser_report_schema_sha256="2" * 64,
    )


def _set_bundle_mtime(publication, modified_ns: int) -> None:  # type: ignore[no-untyped-def]
    paths = (
        publication.descriptor_path,
        publication.migration_manifest_path,
        publication.production_defaults_path,
        publication.rendered_manifest_path,
    )
    for path in paths:
        os.utime(path, ns=(modified_ns, modified_ns))
    os.utime(publication.descriptor_path.parent, ns=(modified_ns, modified_ns))


def _stable_file_snapshot(paths: tuple[Path, ...]) -> tuple[tuple[tuple[int, ...], bytes], ...]:
    snapshots: list[tuple[tuple[int, ...], bytes]] = []
    for path in paths:
        metadata = path.stat()
        snapshots.append(
            (
                (
                    metadata.st_dev,
                    metadata.st_ino,
                    metadata.st_uid,
                    metadata.st_gid,
                    metadata.st_mode,
                    metadata.st_nlink,
                    metadata.st_size,
                    metadata.st_mtime_ns,
                    metadata.st_ctime_ns,
                ),
                path.read_bytes(),
            )
        )
    return tuple(snapshots)


def _service(
    tmp_path: Path,
    *,
    references: tuple[PreflightArtifactProtection, ...] = (),
) -> tuple[
    InstalledPreflightArtifactRetentionService,
    PreflightArtifactStore,
]:
    config = make_config(tmp_path)
    artifacts = PreflightArtifactStore(config.state_root, service_uid=os.geteuid())
    return (
        InstalledPreflightArtifactRetentionService(
            config=config,
            service_uid=os.geteuid(),
            store=RequestStore(config.state_root),
            artifact_store=artifacts,
            collect_references=lambda _now: references,
            now=lambda: INVENTORY_AT,
        ),
        artifacts,
    )


def test_inventory_records_exact_files_references_and_grace_boundary(tmp_path: Path) -> None:
    service, store = _service(tmp_path)
    old = _publish_bundle(store, 1)
    referenced = _publish_bundle(store, 2)
    boundary = _publish_bundle(store, 3)
    young = _publish_bundle(store, 4)
    _set_bundle_mtime(old, CUTOFF_NS - 2 * DAY_NS)
    _set_bundle_mtime(referenced, CUTOFF_NS - 3 * DAY_NS)
    _set_bundle_mtime(boundary, CUTOFF_NS)
    _set_bundle_mtime(young, CUTOFF_NS + 1)
    service.collect_references = lambda _now: (
        PreflightArtifactProtection(referenced.bundle_digest, ("active-rollout",)),
    )

    plan = service.inventory()

    assert tuple(item.bundle_digest for item in plan.candidates) == (
        old.bundle_digest,
        boundary.bundle_digest,
    )
    assert tuple(item.bundle_digest for item in plan.protected) == tuple(
        sorted((referenced.bundle_digest, young.bundle_digest))
    )
    assert plan.protections == tuple(
        sorted(
            (
                PreflightArtifactProtection(referenced.bundle_digest, ("active-rollout",)),
                PreflightArtifactProtection(young.bundle_digest, ("grace-period",)),
            ),
            key=lambda item: item.bundle_digest,
        )
    )
    record = next(item for item in plan.candidates if item.bundle_digest == old.bundle_digest)
    assert tuple(item.name for item in record.files) == ARTIFACT_FILE_NAMES
    for identity in record.files:
        path = old.descriptor_path.parent / identity.name
        metadata = path.stat()
        assert identity.sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
        assert (
            identity.device,
            identity.inode,
            identity.owner_uid,
            identity.owner_gid,
            identity.mode,
            identity.link_count,
            identity.size_bytes,
            identity.modified_ns,
            identity.changed_ns,
        ) == (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_uid,
            metadata.st_gid,
            metadata.st_mode & 0o7777,
            metadata.st_nlink,
            metadata.st_size,
            metadata.st_mtime_ns,
            metadata.st_ctime_ns,
        )
    assert plan.grace_cutoff_ns == CUTOFF_NS
    assert plan.root.inode == store.root.stat().st_ino
    assert service.load_claim(plan.plan_digest) == plan


def test_inventory_caps_candidates_and_marks_deferred_records(tmp_path: Path) -> None:
    service, store = _service(tmp_path)
    publications = []
    for index in range(MAX_RETIREMENTS_PER_PLAN + 8):
        publication = _publish_bundle(store, index)
        _set_bundle_mtime(publication, CUTOFF_NS - (index + 1) * SECOND_NS)
        publications.append(publication)

    plan = service.inventory()

    assert len(plan.candidates) == MAX_RETIREMENTS_PER_PLAN
    assert len(plan.protected) == 8
    assert all(item.reasons == ("batch-deferred",) for item in plan.protections)
    assert {item.bundle_digest for item in plan.candidates} | {
        item.bundle_digest for item in plan.protected
    } == {item.bundle_digest for item in publications}


def test_inventory_turns_safe_unknown_root_entry_into_opaque_protection(
    tmp_path: Path,
) -> None:
    service, store = _service(tmp_path)
    publication = _publish_bundle(store, 1)
    _set_bundle_mtime(publication, CUTOFF_NS - DAY_NS)
    unknown = store.root / "operator-note"
    unknown.write_text("preserve\n", encoding="utf-8")
    unknown.chmod(0o600)

    plan = service.inventory()

    assert plan.candidates == ()
    assert plan.protections == (
        PreflightArtifactProtection(publication.bundle_digest, ("opaque-store",)),
    )
    assert len(plan.opaque_evidence) == 1
    assert plan.opaque_evidence[0].name == "operator-note"
    assert plan.opaque_evidence[0].kind == "file"
    assert plan.opaque_evidence[0].reason == "unknown-entry"
    assert plan.opaque_evidence[0].sha256 == hashlib.sha256(b"preserve\n").hexdigest()


@pytest.mark.parametrize(
    "unsafe_kind",
    ("symlink", "hard-link", "fifth-file", "unsafe-directory", "descriptor-drift"),
)
def test_inventory_rejects_unsafe_or_inexact_bundle(
    tmp_path: Path,
    unsafe_kind: str,
) -> None:
    service, store = _service(tmp_path)
    publication = _publish_bundle(store, 1)
    _set_bundle_mtime(publication, CUTOFF_NS - DAY_NS)
    directory = publication.descriptor_path.parent
    if unsafe_kind == "symlink":
        (store.root / "unsafe-link").symlink_to(directory)
    elif unsafe_kind == "hard-link":
        os.link(publication.descriptor_path, directory / "fifth.json")
    elif unsafe_kind == "fifth-file":
        extra = directory / "fifth.json"
        extra.write_text("{}\n", encoding="utf-8")
        extra.chmod(0o600)
    elif unsafe_kind == "unsafe-directory":
        directory.chmod(0o755)
    elif unsafe_kind == "descriptor-drift":
        publication.descriptor_path.write_text("{}\n", encoding="utf-8")

    with pytest.raises(InstalledPreflightArtifactRetentionError):
        service.inventory()


def test_inventory_rejects_entry_changed_after_bounded_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, store = _service(tmp_path)
    publication = _publish_bundle(store, 1)
    _set_bundle_mtime(publication, CUTOFF_NS - DAY_NS)
    original = retention_module._read_inventory_file
    changed = False

    def change_after_read(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal changed
        result = original(*args, **kwargs)
        if kwargs.get("name") == "artifact.json" and not changed:
            changed = True
            os.utime(
                publication.descriptor_path,
                ns=(CUTOFF_NS - DAY_NS + 1, CUTOFF_NS - DAY_NS + 1),
            )
        return result

    monkeypatch.setattr(retention_module, "_read_inventory_file", change_after_read)

    with pytest.raises(InstalledPreflightArtifactRetentionError, match="changed"):
        service.inventory()


def _approved_plan(tmp_path: Path):  # type: ignore[no-untyped-def]
    service, store = _service(tmp_path)
    candidate = _publish_bundle(store, 1)
    protected = _publish_bundle(store, 2)
    _set_bundle_mtime(candidate, CUTOFF_NS - 2 * DAY_NS)
    _set_bundle_mtime(protected, CUTOFF_NS - 3 * DAY_NS)
    service.collect_references = lambda _now: (
        PreflightArtifactProtection(protected.bundle_digest, ("current-release",)),
    )
    return service, store, candidate, protected, service.inventory()


def test_claim_requires_exact_approval_idle_state_and_one_plan(tmp_path: Path) -> None:
    service, _artifacts, _candidate, _protected, plan = _approved_plan(tmp_path)

    with pytest.raises(InstalledPreflightArtifactRetentionError, match="unavailable"):
        service.load_claim("f" * 64)
    pointer = ActivePointer("req-active", 1, "unit-active", "pending")
    service.store.set_active(pointer)
    with pytest.raises(InstalledPreflightArtifactRetentionError, match="active rollout"):
        service.claim(plan)
    assert service.store.clear_active_if_matches(pointer)
    service.store.claim_preflight_artifact_retention("e" * 64, ())
    with pytest.raises(InstalledPreflightArtifactRetentionError, match="another"):
        service.claim(plan)


def test_apply_requires_durable_exact_plan_after_claim(tmp_path: Path) -> None:
    service, _artifacts, candidate, _protected, plan = _approved_plan(tmp_path)
    service.claim(plan)
    plan_path = service.evidence_root / f"{plan.plan_digest}.plan.json"
    plan_path.unlink()
    retention_module._fsync_directory(plan_path.parent)

    with pytest.raises(InstalledPreflightArtifactRetentionError, match="unavailable"):
        service.apply(plan)

    assert service.store.read_preflight_artifact_retention_claim() is not None
    assert candidate.descriptor_path.parent.is_dir()


@pytest.mark.parametrize(
    "drift",
    ("candidate", "protected", "reference", "publication", "opaque"),
)
def test_apply_rejects_all_pre_rename_inventory_drift(
    tmp_path: Path,
    drift: str,
) -> None:
    service, artifacts, candidate, protected, plan = _approved_plan(tmp_path)
    service.claim(plan)
    if drift == "candidate":
        os.utime(
            candidate.descriptor_path,
            ns=(CUTOFF_NS - 2 * DAY_NS + 1, CUTOFF_NS - 2 * DAY_NS + 1),
        )
    elif drift == "protected":
        os.utime(
            protected.descriptor_path,
            ns=(CUTOFF_NS - 3 * DAY_NS + 1, CUTOFF_NS - 3 * DAY_NS + 1),
        )
    elif drift == "reference":
        service.collect_references = lambda _now: (
            PreflightArtifactProtection(candidate.bundle_digest, ("active-rollout",)),
            PreflightArtifactProtection(protected.bundle_digest, ("current-release",)),
        )
    elif drift == "publication":
        _publish_bundle(artifacts, 3)
    elif drift == "opaque":
        opaque = artifacts.root / "new-opaque-evidence"
        opaque.write_text("preserve\n", encoding="utf-8")
        opaque.chmod(0o600)

    with pytest.raises(InstalledPreflightArtifactRetentionError, match="drift"):
        service.apply(plan)

    assert service.store.read_preflight_artifact_retention_claim() is not None
    assert candidate.descriptor_path.parent.is_dir()
    assert protected.descriptor_path.parent.is_dir()


def test_apply_retires_exact_candidates_with_receipt_before_quarantine_removal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, artifacts, candidate, protected, plan = _approved_plan(tmp_path)
    service.claim(plan)
    events: list[tuple[str, str]] = []
    original_receipt = service.store.publish_preflight_artifact_retirement_receipt
    original_rmdir = retention_module.os.rmdir

    def publish_receipt(bundle_digest: str, **kwargs):  # type: ignore[no-untyped-def]
        quarantines = tuple(service.quarantine_root.iterdir())
        assert len(quarantines) == 1
        assert tuple(quarantines[0].iterdir()) == ()
        result = original_receipt(bundle_digest, **kwargs)
        events.append(("receipt", bundle_digest))
        return result

    def remove_quarantine(path, *args, **kwargs):  # type: ignore[no-untyped-def]
        events.append(("rmdir", plan.candidates[0].bundle_digest))
        return original_rmdir(path, *args, **kwargs)

    monkeypatch.setattr(
        service.store,
        "publish_preflight_artifact_retirement_receipt",
        publish_receipt,
    )
    monkeypatch.setattr(retention_module.os, "rmdir", remove_quarantine)

    result = service.apply(plan)

    record = plan.candidates[0]
    assert result == {
        "approved_plan_sha256": plan.plan_digest,
        "environment": "staging",
        "namespace": "loom-staging",
        "retirements": [
            {
                "bundle_digest": record.bundle_digest,
                "inventory_record_sha256": record.record_digest,
            }
        ],
        "schema_version": 1,
    }
    assert events == [
        ("receipt", candidate.bundle_digest),
        ("rmdir", candidate.bundle_digest),
    ]
    assert not candidate.descriptor_path.parent.exists()
    assert artifacts.read(protected.bundle_digest) == protected
    assert service.store.read_preflight_artifact_retirement_receipt(
        candidate.bundle_digest,
        plan_sha256=plan.plan_digest,
        inventory_record_sha256=record.record_digest,
    )
    assert service.store.read_preflight_artifact_retention_claim() is None
    assert not service.quarantine_root.exists() or tuple(service.quarantine_root.iterdir()) == ()
    applied = service.evidence_root / f"{plan.plan_digest}.applied.json"
    assert json.loads(applied.read_bytes()) == result


def test_apply_makes_new_quarantine_root_durable_before_rename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, _artifacts, _candidate, _protected, plan = _approved_plan(tmp_path)
    service.claim(plan)
    fsynced_directories: list[Path] = []
    original_fsync_directory = retention_module._fsync_directory
    original_rename = retention_module.os.rename  # type: ignore[attr-defined]

    def record_fsync(path: Path) -> None:
        original_fsync_directory(path)
        fsynced_directories.append(path)

    def require_durable_parent(*args, **kwargs):  # type: ignore[no-untyped-def]
        assert service.config.state_root in fsynced_directories
        return original_rename(*args, **kwargs)

    monkeypatch.setattr(retention_module, "_fsync_directory", record_fsync)
    monkeypatch.setattr(retention_module.os, "rename", require_durable_parent)

    service.apply(plan)


def test_apply_rechecks_each_quarantine_file_immediately_before_unlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, _artifacts, _candidate, _protected, plan = _approved_plan(tmp_path)
    service.claim(plan)
    record = plan.candidates[0]
    quarantine = service.quarantine_root / f"{plan.plan_digest}.{record.bundle_digest}"
    replacement = quarantine / "migration.yaml"
    original_unlink = retention_module.os.unlink  # type: ignore[attr-defined]
    replaced = False

    def replace_later_file(path, *args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal replaced
        if path == "artifact.json" and not replaced:
            replaced = True
            original_unlink(replacement)
            replacement.write_text("replacement\n", encoding="utf-8")
            replacement.chmod(0o600)
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(retention_module.os, "unlink", replace_later_file)

    with pytest.raises(InstalledPreflightArtifactRetentionError, match="changed"):
        service.apply(plan)

    assert service.store.read_preflight_artifact_retention_claim() is not None


class _InjectedCrash(BaseException):
    pass


@pytest.mark.parametrize(
    "crash_stage",
    ("rename", "one-unlink", "four-unlinks", "receipt", "quarantine-removal"),
)
def test_apply_restart_converges_each_durable_crash_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_stage: str,
) -> None:
    service, _artifacts, candidate, _protected, plan = _approved_plan(tmp_path)
    service.claim(plan)
    if crash_stage == "rename":
        original_fsync = retention_module._fsync_descriptor
        calls = 0

        def crash_after_two_fsyncs(descriptor: int) -> None:
            nonlocal calls
            original_fsync(descriptor)
            calls += 1
            if calls == 2:
                raise _InjectedCrash()

        monkeypatch.setattr(retention_module, "_fsync_descriptor", crash_after_two_fsyncs)
    elif crash_stage in {"one-unlink", "four-unlinks"}:
        original_unlink = retention_module.os.unlink
        target = 1 if crash_stage == "one-unlink" else 4
        calls = 0

        def crash_after_unlink(path, *args, **kwargs):  # type: ignore[no-untyped-def]
            nonlocal calls
            result = original_unlink(path, *args, **kwargs)
            if path in ARTIFACT_FILE_NAMES:
                calls += 1
                if calls == target:
                    raise _InjectedCrash()
            return result

        monkeypatch.setattr(retention_module.os, "unlink", crash_after_unlink)
    elif crash_stage == "receipt":
        original_receipt = service.store.publish_preflight_artifact_retirement_receipt

        def crash_after_receipt(*args, **kwargs):  # type: ignore[no-untyped-def]
            original_receipt(*args, **kwargs)
            raise _InjectedCrash()

        monkeypatch.setattr(
            service.store,
            "publish_preflight_artifact_retirement_receipt",
            crash_after_receipt,
        )
    elif crash_stage == "quarantine-removal":
        original_rmdir = retention_module.os.rmdir

        def crash_after_rmdir(*args, **kwargs):  # type: ignore[no-untyped-def]
            original_rmdir(*args, **kwargs)
            raise _InjectedCrash()

        monkeypatch.setattr(retention_module.os, "rmdir", crash_after_rmdir)

    with pytest.raises(_InjectedCrash):
        service.apply(plan)
    assert service.store.read_preflight_artifact_retention_claim() is not None
    monkeypatch.undo()
    restarted, _store = _service(tmp_path)
    restarted.collect_references = service.collect_references

    result = restarted.apply(plan)

    assert result["approved_plan_sha256"] == plan.plan_digest
    assert not candidate.descriptor_path.parent.exists()
    assert restarted.store.read_preflight_artifact_retention_claim() is None
    assert (
        not restarted.quarantine_root.exists() or tuple(restarted.quarantine_root.iterdir()) == ()
    )


def test_apply_restart_recovers_exact_applied_evidence_link_residue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, _artifacts, candidate, _protected, plan = _approved_plan(tmp_path)
    service.claim(plan)
    original_publish = retention_module._publish_exact_evidence

    def leave_durable_link_residue(
        path: Path,
        value: Mapping[str, object],
        *,
        service_uid: int,
    ) -> None:
        if path.name != f"{plan.plan_digest}.applied.json":
            original_publish(path, value, service_uid=service_uid)
            return
        payload = retention_module._json_bytes(value)
        temporary = path.parent / f".{path.name}.{'f' * 32}.tmp"
        temporary.write_bytes(payload)
        temporary.chmod(0o600)
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        os.link(temporary, path)
        retention_module._fsync_directory(path.parent)
        raise _InjectedCrash()

    monkeypatch.setattr(
        retention_module,
        "_publish_exact_evidence",
        leave_durable_link_residue,
    )

    with pytest.raises(_InjectedCrash):
        service.apply(plan)
    assert service.store.read_preflight_artifact_retention_claim() is not None
    assert not candidate.descriptor_path.parent.exists()
    monkeypatch.undo()
    restarted, _store = _service(tmp_path)
    restarted.collect_references = service.collect_references

    result = restarted.apply(plan)

    applied = restarted.evidence_root / f"{plan.plan_digest}.applied.json"
    assert result["approved_plan_sha256"] == plan.plan_digest
    assert applied.stat().st_nlink == 1
    assert not (applied.parent / f".{applied.name}.{'f' * 32}.tmp").exists()
    assert restarted.store.read_preflight_artifact_retention_claim() is None


def test_apply_restart_recovers_exact_receipt_link_residue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, _artifacts, candidate, _protected, plan = _approved_plan(tmp_path)
    service.claim(plan)
    original_receipt = service.store.publish_preflight_artifact_retirement_receipt
    residue: Path | None = None

    def leave_durable_receipt_link(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal residue
        receipt = original_receipt(*args, **kwargs)
        residue = receipt.with_name(f".{receipt.name}.{'f' * 32}.tmp")
        os.link(receipt, residue)
        retention_module._fsync_directory(receipt.parent)
        raise _InjectedCrash()

    monkeypatch.setattr(
        service.store,
        "publish_preflight_artifact_retirement_receipt",
        leave_durable_receipt_link,
    )

    with pytest.raises(_InjectedCrash):
        service.apply(plan)
    assert residue is not None and residue.exists()
    assert not candidate.descriptor_path.parent.exists()
    assert service.store.read_preflight_artifact_retention_claim() is not None
    monkeypatch.undo()
    restarted, _store = _service(tmp_path)
    restarted.collect_references = service.collect_references

    result = restarted.apply(plan)

    assert result["approved_plan_sha256"] == plan.plan_digest
    assert residue is not None and not residue.exists()
    assert restarted.store.read_preflight_artifact_retention_claim() is None


@pytest.mark.parametrize(
    "impossible",
    ("unreceipted-absence", "mismatched-quarantine", "recreated-source", "other-receipt"),
)
def test_apply_fails_closed_on_impossible_restart_state(
    tmp_path: Path,
    impossible: str,
) -> None:
    service, _artifacts, candidate, _protected, plan = _approved_plan(tmp_path)
    service.claim(plan)
    record = plan.candidates[0]
    source = candidate.descriptor_path.parent
    if impossible in {"unreceipted-absence", "mismatched-quarantine"}:
        service.quarantine_root.mkdir(mode=0o700)
        quarantine = service.quarantine_root / f"{plan.plan_digest}.{candidate.bundle_digest}"
        source.rename(quarantine)
        if impossible == "unreceipted-absence":
            for path in quarantine.iterdir():
                path.unlink()
            quarantine.rmdir()
        else:
            (quarantine / "artifact.json").write_text("{}\n", encoding="utf-8")
    elif impossible == "recreated-source":
        service.store.publish_preflight_artifact_retirement_receipt(
            candidate.bundle_digest,
            plan_sha256=plan.plan_digest,
            inventory_record_sha256=record.record_digest,
        )
    elif impossible == "other-receipt":
        service.store.publish_preflight_artifact_retirement_receipt(
            candidate.bundle_digest,
            plan_sha256="f" * 64,
            inventory_record_sha256=record.record_digest,
        )

    with pytest.raises(InstalledPreflightArtifactRetentionError):
        service.apply(plan)

    assert service.store.read_preflight_artifact_retention_claim() is not None


def test_repeated_bounded_plans_preserve_active_readable_publication(
    tmp_path: Path,
) -> None:
    service, store = _service(tmp_path)
    publications = tuple(_publish_bundle(store, index) for index in range(257))
    for index, publication in enumerate(publications):
        _set_bundle_mtime(publication, CUTOFF_NS - (index + 1) * SECOND_NS)
    protected = publications[128]
    service.collect_references = lambda _now: (
        PreflightArtifactProtection(protected.bundle_digest, ("active-rollout",)),
    )
    protected_paths = (
        protected.descriptor_path,
        protected.migration_manifest_path,
        protected.production_defaults_path,
        protected.rendered_manifest_path,
    )
    protected_snapshot = _stable_file_snapshot(protected_paths)
    retired: set[str] = set()
    batch_sizes: list[int] = []

    while True:
        plan = service.inventory()
        assert protected.bundle_digest not in {record.bundle_digest for record in plan.candidates}
        assert (
            PreflightArtifactProtection(
                protected.bundle_digest,
                ("active-rollout",),
            )
            in plan.protections
        )
        if not plan.candidates:
            break
        batch_sizes.append(len(plan.candidates))
        service.claim(plan)
        result = service.apply(plan)
        retirements = result["retirements"]
        assert isinstance(retirements, list)
        for item in retirements:
            assert isinstance(item, dict)
            bundle_digest = item.get("bundle_digest")
            assert isinstance(bundle_digest, str)
            retired.add(bundle_digest)
        assert service.store.read_preflight_artifact_retention_claim() is None

    assert batch_sizes == [MAX_RETIREMENTS_PER_PLAN] * 8
    assert retired == {
        publication.bundle_digest
        for publication in publications
        if publication.bundle_digest != protected.bundle_digest
    }
    assert store.read(protected.bundle_digest) == protected
    assert _stable_file_snapshot(protected_paths) == protected_snapshot
