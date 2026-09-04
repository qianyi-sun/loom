from __future__ import annotations

import os
import stat
from pathlib import Path
from uuid import UUID

import pytest

import loom_cli.rollout.operator.protected_capacity_manager_configuration_compensation as compensation_module
from loom_cli.rollout.operator.protected_capacity_manager_configuration_compensation import (
    CapacityManagerConfigurationCompensationIntentRecord,
    CapacityManagerConfigurationCompensationRecord,
    CapacityManagerConfigurationCompensationStore,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan


def _intent_record(tmp_path: Path) -> CapacityManagerConfigurationCompensationIntentRecord:
    plan = _plan(tmp_path)
    return CapacityManagerConfigurationCompensationIntentRecord.build(
        request_id=plan.request_id,
        attempt_number=plan.attempt_number,
        plan_digest=plan.plan_digest,
        activation_idempotency_key=UUID("00000000-0000-4000-8000-000000000301"),
        activation_request_digest="1" * 64,
        target_configuration_epoch=10,
        target_configuration_digest="2" * 64,
        target_configuration_evidence_digest="3" * 64,
        predecessor_configuration_epoch=9,
        predecessor_configuration_digest=plan.manager_configuration_digest,
        predecessor_configuration_evidence_digest="4" * 64,
        backup_lease_digest=plan.backup_lease_digest,
        rollback_idempotency_key=UUID("00000000-0000-4000-8000-000000000302"),
        rollback_request_digest="5" * 64,
        rollback_evidence_sha256="6" * 64,
    )


def _record(tmp_path: Path) -> CapacityManagerConfigurationCompensationRecord:
    intent = _intent_record(tmp_path)
    return CapacityManagerConfigurationCompensationRecord.build(
        request_id=intent.request_id,
        attempt_number=intent.attempt_number,
        plan_digest=intent.plan_digest,
        activation_idempotency_key=intent.activation_idempotency_key,
        activation_request_digest=intent.activation_request_digest,
        target_configuration_epoch=intent.target_configuration_epoch,
        target_configuration_digest=intent.target_configuration_digest,
        target_configuration_evidence_digest=intent.target_configuration_evidence_digest,
        predecessor_configuration_epoch=intent.predecessor_configuration_epoch,
        predecessor_configuration_digest=intent.predecessor_configuration_digest,
        predecessor_configuration_evidence_digest=intent.predecessor_configuration_evidence_digest,
        backup_lease_digest=intent.backup_lease_digest,
        rollback_idempotency_key=intent.rollback_idempotency_key,
        rollback_request_digest=intent.rollback_request_digest,
        rollback_evidence_sha256=intent.rollback_evidence_sha256,
        resulting_configuration_epoch=11,
        resulting_configuration_digest="7" * 64,
        resulting_configuration_evidence_digest="8" * 64,
    )


def _write_private(path: Path, payload: bytes) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_bytes(payload)
    os.chmod(path, 0o600)


def _lookup_record(
    tmp_path: Path,
    *,
    request_id: str = "req-alpha",
    attempt_number: int = 1,
    plan_digest: str | None = None,
    predecessor_configuration_epoch: int = 9,
    predecessor_configuration_digest: str | None = None,
    backup_lease_digest: str | None = None,
) -> CapacityManagerConfigurationCompensationRecord | None:
    plan = _plan(tmp_path)
    store = CapacityManagerConfigurationCompensationStore(
        (tmp_path / "compensations").resolve(),
        service_uid=os.geteuid(),
    )
    return store.find_record_for_plan(
        request_id=request_id,
        attempt_number=attempt_number,
        plan_digest=plan.plan_digest if plan_digest is None else plan_digest,
        predecessor_configuration_epoch=predecessor_configuration_epoch,
        predecessor_configuration_digest=(
            plan.manager_configuration_digest
            if predecessor_configuration_digest is None
            else predecessor_configuration_digest
        ),
        backup_lease_digest=plan.backup_lease_digest
        if backup_lease_digest is None
        else backup_lease_digest,
    )


def test_compensation_records_require_exact_field_sets_and_distinct_idempotency_keys(
    tmp_path: Path,
) -> None:
    intent = _intent_record(tmp_path)
    record = _record(tmp_path)

    assert (
        CapacityManagerConfigurationCompensationIntentRecord.from_dict(intent.to_dict()) == intent
    )
    assert CapacityManagerConfigurationCompensationRecord.from_dict(record.to_dict()) == record

    extra = intent.to_dict() | {"unexpected": 1}
    with pytest.raises(ValueError, match="fields"):
        CapacityManagerConfigurationCompensationIntentRecord.from_dict(extra)

    missing = record.to_dict()
    missing.pop("resulting_configuration_digest")
    with pytest.raises(ValueError, match="fields"):
        CapacityManagerConfigurationCompensationRecord.from_dict(missing)

    drifted = record.to_dict()
    drifted["resulting_configuration_digest"] = "9" * 64
    with pytest.raises(ValueError, match="content drifted"):
        CapacityManagerConfigurationCompensationRecord.from_dict(drifted)

    with pytest.raises(ValueError, match="invalid"):
        CapacityManagerConfigurationCompensationIntentRecord.build(
            request_id=intent.request_id,
            attempt_number=intent.attempt_number,
            plan_digest=intent.plan_digest,
            activation_idempotency_key=intent.activation_idempotency_key,
            activation_request_digest=intent.activation_request_digest,
            target_configuration_epoch=10,
            target_configuration_digest=intent.target_configuration_digest,
            target_configuration_evidence_digest=intent.target_configuration_evidence_digest,
            predecessor_configuration_epoch=1,
            predecessor_configuration_digest=intent.predecessor_configuration_digest,
            predecessor_configuration_evidence_digest=intent.predecessor_configuration_evidence_digest,
            backup_lease_digest=intent.backup_lease_digest,
            rollback_idempotency_key=intent.rollback_idempotency_key,
            rollback_request_digest=intent.rollback_request_digest,
            rollback_evidence_sha256=intent.rollback_evidence_sha256,
        )

    with pytest.raises(ValueError, match="invalid"):
        CapacityManagerConfigurationCompensationRecord.build(
            request_id=record.request_id,
            attempt_number=record.attempt_number,
            plan_digest=record.plan_digest,
            activation_idempotency_key=record.activation_idempotency_key,
            activation_request_digest=record.activation_request_digest,
            target_configuration_epoch=record.target_configuration_epoch,
            target_configuration_digest=record.target_configuration_digest,
            target_configuration_evidence_digest=record.target_configuration_evidence_digest,
            predecessor_configuration_epoch=record.predecessor_configuration_epoch,
            predecessor_configuration_digest=record.predecessor_configuration_digest,
            predecessor_configuration_evidence_digest=record.predecessor_configuration_evidence_digest,
            backup_lease_digest=record.backup_lease_digest,
            rollback_idempotency_key=record.rollback_idempotency_key,
            rollback_request_digest=record.rollback_request_digest,
            rollback_evidence_sha256=record.rollback_evidence_sha256,
            resulting_configuration_epoch=999,
            resulting_configuration_digest=record.resulting_configuration_digest,
            resulting_configuration_evidence_digest=record.resulting_configuration_evidence_digest,
        )

    same_key = UUID("00000000-0000-4000-8000-000000000399")
    with pytest.raises(ValueError, match="invalid"):
        CapacityManagerConfigurationCompensationIntentRecord.build(
            request_id=intent.request_id,
            attempt_number=intent.attempt_number,
            plan_digest=intent.plan_digest,
            activation_idempotency_key=same_key,
            activation_request_digest=intent.activation_request_digest,
            target_configuration_epoch=intent.target_configuration_epoch,
            target_configuration_digest=intent.target_configuration_digest,
            target_configuration_evidence_digest=intent.target_configuration_evidence_digest,
            predecessor_configuration_epoch=intent.predecessor_configuration_epoch,
            predecessor_configuration_digest=intent.predecessor_configuration_digest,
            predecessor_configuration_evidence_digest=intent.predecessor_configuration_evidence_digest,
            backup_lease_digest=intent.backup_lease_digest,
            rollback_idempotency_key=same_key,
            rollback_request_digest=intent.rollback_request_digest,
            rollback_evidence_sha256=intent.rollback_evidence_sha256,
        )


def test_compensation_store_persists_intent_before_terminal_and_rejects_drift(
    tmp_path: Path,
) -> None:
    store = CapacityManagerConfigurationCompensationStore(
        (tmp_path / "compensations").resolve(),
        service_uid=os.geteuid(),
    )
    intent = _intent_record(tmp_path)
    record = _record(tmp_path)

    store.record_intent(intent)
    store.record_intent(intent)
    store.record(record)
    store.record(record)

    loaded_intent = store.read_intent(record.activation_idempotency_key)
    loaded = store.read(record.activation_idempotency_key)
    assert loaded_intent == intent
    assert loaded == record
    assert store.root.stat().st_mode & 0o777 == 0o700
    assert store.intent_path_for(record.activation_idempotency_key).name.endswith("-intent.json")
    assert store.path_for(record.activation_idempotency_key).name.endswith("-terminal.json")
    assert store.intent_path_for(record.activation_idempotency_key).stat().st_mode & 0o777 == 0o600
    assert store.path_for(record.activation_idempotency_key).stat().st_mode & 0o777 == 0o600

    drifted = CapacityManagerConfigurationCompensationRecord.build(
        request_id=record.request_id,
        attempt_number=record.attempt_number,
        plan_digest=record.plan_digest,
        activation_idempotency_key=record.activation_idempotency_key,
        activation_request_digest=record.activation_request_digest,
        target_configuration_epoch=record.target_configuration_epoch,
        target_configuration_digest=record.target_configuration_digest,
        target_configuration_evidence_digest=record.target_configuration_evidence_digest,
        predecessor_configuration_epoch=record.predecessor_configuration_epoch,
        predecessor_configuration_digest=record.predecessor_configuration_digest,
        predecessor_configuration_evidence_digest=record.predecessor_configuration_evidence_digest,
        backup_lease_digest=record.backup_lease_digest,
        rollback_idempotency_key=record.rollback_idempotency_key,
        rollback_request_digest=record.rollback_request_digest,
        rollback_evidence_sha256=record.rollback_evidence_sha256,
        resulting_configuration_epoch=record.resulting_configuration_epoch,
        resulting_configuration_digest="9" * 64,
        resulting_configuration_evidence_digest=record.resulting_configuration_evidence_digest,
    )

    with pytest.raises(RuntimeError, match="already drifted"):
        store.record(drifted)


def test_compensation_store_requires_matching_intent_for_terminal_publication_and_read(
    tmp_path: Path,
) -> None:
    store = CapacityManagerConfigurationCompensationStore(
        (tmp_path / "compensations").resolve(),
        service_uid=os.geteuid(),
    )
    intent = _intent_record(tmp_path)
    record = _record(tmp_path)

    with pytest.raises(RuntimeError, match="intent"):
        store.record(record)

    store.record_intent(intent)
    mismatched = CapacityManagerConfigurationCompensationRecord.build(
        request_id=record.request_id,
        attempt_number=record.attempt_number,
        plan_digest=record.plan_digest,
        activation_idempotency_key=record.activation_idempotency_key,
        activation_request_digest=record.activation_request_digest,
        target_configuration_epoch=record.target_configuration_epoch,
        target_configuration_digest=record.target_configuration_digest,
        target_configuration_evidence_digest=record.target_configuration_evidence_digest,
        predecessor_configuration_epoch=record.predecessor_configuration_epoch,
        predecessor_configuration_digest=record.predecessor_configuration_digest,
        predecessor_configuration_evidence_digest=record.predecessor_configuration_evidence_digest,
        backup_lease_digest="9" * 64,
        rollback_idempotency_key=record.rollback_idempotency_key,
        rollback_request_digest=record.rollback_request_digest,
        rollback_evidence_sha256=record.rollback_evidence_sha256,
        resulting_configuration_epoch=record.resulting_configuration_epoch,
        resulting_configuration_digest=record.resulting_configuration_digest,
        resulting_configuration_evidence_digest=record.resulting_configuration_evidence_digest,
    )
    with pytest.raises(RuntimeError, match="intent"):
        store.record(mismatched)

    root = (tmp_path / "manual").resolve()
    manual = CapacityManagerConfigurationCompensationStore(root, service_uid=os.geteuid())
    root.mkdir(mode=0o700)
    _write_private(manual.path_for(record.activation_idempotency_key), record.to_bytes())
    with pytest.raises(RuntimeError, match="intent"):
        manual.read(record.activation_idempotency_key)

    mismatched_intent = CapacityManagerConfigurationCompensationIntentRecord.build(
        request_id=intent.request_id,
        attempt_number=intent.attempt_number,
        plan_digest=intent.plan_digest,
        activation_idempotency_key=intent.activation_idempotency_key,
        activation_request_digest=intent.activation_request_digest,
        target_configuration_epoch=intent.target_configuration_epoch,
        target_configuration_digest=intent.target_configuration_digest,
        target_configuration_evidence_digest=intent.target_configuration_evidence_digest,
        predecessor_configuration_epoch=intent.predecessor_configuration_epoch,
        predecessor_configuration_digest=intent.predecessor_configuration_digest,
        predecessor_configuration_evidence_digest=intent.predecessor_configuration_evidence_digest,
        backup_lease_digest="a" * 64,
        rollback_idempotency_key=intent.rollback_idempotency_key,
        rollback_request_digest=intent.rollback_request_digest,
        rollback_evidence_sha256=intent.rollback_evidence_sha256,
    )
    _write_private(
        manual.intent_path_for(record.activation_idempotency_key), mismatched_intent.to_bytes()
    )
    with pytest.raises(RuntimeError, match="intent"):
        manual.read(record.activation_idempotency_key)


def test_compensation_store_rejects_symlinked_or_non_private_roots(tmp_path: Path) -> None:
    intent = _intent_record(tmp_path)

    redirected_target = (tmp_path / "redirected-target").resolve()
    redirected_target.mkdir(mode=0o700)
    final_symlink_root = tmp_path / "final-symlink-root"
    os.symlink(redirected_target, final_symlink_root)
    with pytest.raises(RuntimeError, match="unsafe"):
        CapacityManagerConfigurationCompensationStore(
            final_symlink_root,
            service_uid=os.geteuid(),
        ).record_intent(intent)

    intermediate_target = (tmp_path / "intermediate-target").resolve()
    intermediate_target.mkdir(mode=0o700)
    intermediate_symlink = tmp_path / "intermediate-link"
    os.symlink(intermediate_target, intermediate_symlink)
    with pytest.raises(RuntimeError, match="unsafe"):
        CapacityManagerConfigurationCompensationStore(
            intermediate_symlink / "nested",
            service_uid=os.geteuid(),
        ).record_intent(intent)

    non_private = (tmp_path / "non-private").resolve()
    non_private.mkdir(mode=0o755)
    with pytest.raises(RuntimeError, match="unsafe"):
        CapacityManagerConfigurationCompensationStore(
            non_private,
            service_uid=os.geteuid(),
        ).record_intent(intent)


def test_compensation_store_fsyncs_exact_private_mode_before_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = (tmp_path / "compensations").resolve()
    root.mkdir(mode=0o700)
    store = CapacityManagerConfigurationCompensationStore(
        root,
        service_uid=os.geteuid(),
    )
    intent = _intent_record(tmp_path)
    file_modes_seen_by_fsync: list[int] = []
    original_fchmod = compensation_module.os.fchmod
    original_fsync = compensation_module.os.fsync

    def tracked_fchmod(descriptor: int, mode: int) -> None:
        original_fchmod(descriptor, mode)

    def tracked_fsync(descriptor: int) -> None:
        metadata = os.fstat(descriptor)
        if stat.S_ISREG(metadata.st_mode):
            file_modes_seen_by_fsync.append(stat.S_IMODE(metadata.st_mode))
        original_fsync(descriptor)

    monkeypatch.setattr(compensation_module.os, "fchmod", tracked_fchmod)
    monkeypatch.setattr(compensation_module.os, "fsync", tracked_fsync)

    previous_umask = os.umask(0o777)
    try:
        store.record_intent(intent)
    finally:
        os.umask(previous_umask)

    assert file_modes_seen_by_fsync == [0o600]
    assert store.intent_path_for(intent.activation_idempotency_key).stat().st_mode & 0o777 == 0o600


def test_compensation_store_terminal_publication_keeps_intent_and_terminal_on_one_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_a = (tmp_path / "compensations-a").resolve()
    root_b = (tmp_path / "compensations-b").resolve()
    store = CapacityManagerConfigurationCompensationStore(root_a, service_uid=os.geteuid())
    intent = _intent_record(tmp_path)
    record = _record(tmp_path)
    store.record_intent(intent)
    root_b.mkdir(mode=0o700)
    original_from_bytes = CapacityManagerConfigurationCompensationIntentRecord.from_bytes

    def rebound_root_after_intent_read(
        cls: type[CapacityManagerConfigurationCompensationIntentRecord],
        payload: bytes,
    ) -> CapacityManagerConfigurationCompensationIntentRecord:
        loaded = original_from_bytes(payload)
        object.__setattr__(store, "root", root_b)
        return loaded

    monkeypatch.setattr(
        CapacityManagerConfigurationCompensationIntentRecord,
        "from_bytes",
        classmethod(rebound_root_after_intent_read),
    )

    store.record(record)

    assert root_a.joinpath(store.terminal_path_for(record.activation_idempotency_key).name).exists()
    assert not root_b.joinpath(
        store.terminal_path_for(record.activation_idempotency_key).name
    ).exists()


def test_compensation_store_terminal_read_keeps_intent_and_terminal_on_one_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_a = (tmp_path / "compensations-a").resolve()
    root_b = (tmp_path / "compensations-b").resolve()
    store = CapacityManagerConfigurationCompensationStore(root_a, service_uid=os.geteuid())
    intent = _intent_record(tmp_path)
    record = _record(tmp_path)
    store.record_intent(intent)
    _write_private(store.path_for(record.activation_idempotency_key), record.to_bytes())
    root_b.mkdir(mode=0o700)
    original_from_bytes = CapacityManagerConfigurationCompensationRecord.from_bytes

    def rebound_root_after_terminal_read(
        cls: type[CapacityManagerConfigurationCompensationRecord],
        payload: bytes,
    ) -> CapacityManagerConfigurationCompensationRecord:
        loaded = original_from_bytes(payload)
        object.__setattr__(store, "root", root_b)
        return loaded

    monkeypatch.setattr(
        CapacityManagerConfigurationCompensationRecord,
        "from_bytes",
        classmethod(rebound_root_after_terminal_read),
    )

    assert store.read(record.activation_idempotency_key) == record


def test_compensation_store_plan_lookup_accepts_directory_inventory_at_entry_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(compensation_module, "_MAX_DIRECTORY_ENTRIES", 6)
    store = CapacityManagerConfigurationCompensationStore(
        (tmp_path / "compensations").resolve(),
        service_uid=os.geteuid(),
    )
    intent = _intent_record(tmp_path)
    record = _record(tmp_path)
    noise_entries = compensation_module._MAX_DIRECTORY_ENTRIES - 2

    for index in range(noise_entries):
        _write_private(store.root / f"ignored-{index:04d}.txt", b"noise")
    store.record_intent(intent)
    store.record(record)

    assert _lookup_record(tmp_path) == record


def test_compensation_store_plan_lookup_rejects_directory_inventory_above_entry_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(compensation_module, "_MAX_DIRECTORY_ENTRIES", 6)
    store = CapacityManagerConfigurationCompensationStore(
        (tmp_path / "compensations").resolve(),
        service_uid=os.geteuid(),
    )
    intent = _intent_record(tmp_path)
    record = _record(tmp_path)
    noise_entries = compensation_module._MAX_DIRECTORY_ENTRIES - 1

    for index in range(noise_entries):
        _write_private(store.root / f"ignored-{index:04d}.txt", b"noise")
    store.record_intent(intent)
    store.record(record)

    with pytest.raises(RuntimeError, match="too many entries"):
        _lookup_record(tmp_path)


@pytest.mark.parametrize("record_terminal", [False, True], ids=["intent-only", "complete"])
def test_compensation_store_plan_lookup_rejects_same_request_attempt_plan_digest_drift(
    tmp_path: Path,
    record_terminal: bool,
) -> None:
    plan = _plan(tmp_path)
    store = CapacityManagerConfigurationCompensationStore(
        (tmp_path / "compensations").resolve(),
        service_uid=os.geteuid(),
    )
    intent = CapacityManagerConfigurationCompensationIntentRecord.build(
        request_id="req-alpha",
        attempt_number=1,
        plan_digest="f" * 64,
        activation_idempotency_key=UUID("00000000-0000-4000-8000-000000000333"),
        activation_request_digest="1" * 64,
        target_configuration_epoch=10,
        target_configuration_digest="2" * 64,
        target_configuration_evidence_digest="3" * 64,
        predecessor_configuration_epoch=9,
        predecessor_configuration_digest=plan.manager_configuration_digest,
        predecessor_configuration_evidence_digest="4" * 64,
        backup_lease_digest=plan.backup_lease_digest,
        rollback_idempotency_key=UUID("00000000-0000-4000-8000-000000000334"),
        rollback_request_digest="5" * 64,
        rollback_evidence_sha256="6" * 64,
    )
    store.record_intent(intent)
    if record_terminal:
        store.record(
            CapacityManagerConfigurationCompensationRecord.build(
                request_id=intent.request_id,
                attempt_number=intent.attempt_number,
                plan_digest=intent.plan_digest,
                activation_idempotency_key=intent.activation_idempotency_key,
                activation_request_digest=intent.activation_request_digest,
                target_configuration_epoch=intent.target_configuration_epoch,
                target_configuration_digest=intent.target_configuration_digest,
                target_configuration_evidence_digest=intent.target_configuration_evidence_digest,
                predecessor_configuration_epoch=intent.predecessor_configuration_epoch,
                predecessor_configuration_digest=intent.predecessor_configuration_digest,
                predecessor_configuration_evidence_digest=intent.predecessor_configuration_evidence_digest,
                backup_lease_digest=intent.backup_lease_digest,
                rollback_idempotency_key=intent.rollback_idempotency_key,
                rollback_request_digest=intent.rollback_request_digest,
                rollback_evidence_sha256=intent.rollback_evidence_sha256,
                resulting_configuration_epoch=11,
                resulting_configuration_digest="7" * 64,
                resulting_configuration_evidence_digest="8" * 64,
            )
        )

    with pytest.raises(RuntimeError, match="binding drifted"):
        _lookup_record(tmp_path)


def test_compensation_store_plan_lookup_ignores_unrelated_request_attempt_records(
    tmp_path: Path,
) -> None:
    plan = _plan(tmp_path)
    store = CapacityManagerConfigurationCompensationStore(
        (tmp_path / "compensations").resolve(),
        service_uid=os.geteuid(),
    )
    intent = CapacityManagerConfigurationCompensationIntentRecord.build(
        request_id="req-other",
        attempt_number=2,
        plan_digest="f" * 64,
        activation_idempotency_key=UUID("00000000-0000-4000-8000-000000000355"),
        activation_request_digest="1" * 64,
        target_configuration_epoch=10,
        target_configuration_digest="2" * 64,
        target_configuration_evidence_digest="3" * 64,
        predecessor_configuration_epoch=9,
        predecessor_configuration_digest=plan.manager_configuration_digest,
        predecessor_configuration_evidence_digest="4" * 64,
        backup_lease_digest=plan.backup_lease_digest,
        rollback_idempotency_key=UUID("00000000-0000-4000-8000-000000000356"),
        rollback_request_digest="5" * 64,
        rollback_evidence_sha256="6" * 64,
    )
    store.record_intent(intent)
    store.record(
        CapacityManagerConfigurationCompensationRecord.build(
            request_id=intent.request_id,
            attempt_number=intent.attempt_number,
            plan_digest=intent.plan_digest,
            activation_idempotency_key=intent.activation_idempotency_key,
            activation_request_digest=intent.activation_request_digest,
            target_configuration_epoch=intent.target_configuration_epoch,
            target_configuration_digest=intent.target_configuration_digest,
            target_configuration_evidence_digest=intent.target_configuration_evidence_digest,
            predecessor_configuration_epoch=intent.predecessor_configuration_epoch,
            predecessor_configuration_digest=intent.predecessor_configuration_digest,
            predecessor_configuration_evidence_digest=intent.predecessor_configuration_evidence_digest,
            backup_lease_digest=intent.backup_lease_digest,
            rollback_idempotency_key=intent.rollback_idempotency_key,
            rollback_request_digest=intent.rollback_request_digest,
            rollback_evidence_sha256=intent.rollback_evidence_sha256,
            resulting_configuration_epoch=11,
            resulting_configuration_digest="7" * 64,
            resulting_configuration_evidence_digest="8" * 64,
        )
    )

    assert _lookup_record(tmp_path) is None


def test_compensation_store_plan_lookup_fails_closed_when_listed_record_disappears(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan(tmp_path)
    store = CapacityManagerConfigurationCompensationStore(
        (tmp_path / "compensations").resolve(),
        service_uid=os.geteuid(),
    )
    intent = _intent_record(tmp_path)
    record = _record(tmp_path)
    store.record_intent(intent)
    store.record(record)
    original_read_at = CapacityManagerConfigurationCompensationStore._read_at

    def disappear(
        self: CapacityManagerConfigurationCompensationStore,
        directory: int,
        name: str,
    ) -> bytes | None:
        if name == store.terminal_path_for(record.activation_idempotency_key).name:
            return None
        return original_read_at(self, directory, name)

    monkeypatch.setattr(CapacityManagerConfigurationCompensationStore, "_read_at", disappear)

    with pytest.raises(RuntimeError, match="record disappeared"):
        store.find_record_for_plan(
            request_id="req-alpha",
            attempt_number=1,
            plan_digest=plan.plan_digest,
            predecessor_configuration_epoch=9,
            predecessor_configuration_digest=plan.manager_configuration_digest,
            backup_lease_digest=plan.backup_lease_digest,
        )
