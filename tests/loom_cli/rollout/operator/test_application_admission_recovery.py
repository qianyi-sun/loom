"""Admission recovery data belongs to one executing protected component intent."""

import json
import os
import stat
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest

from loom.application_database_admission import (
    ApplicationDatabaseAdmissionTarget,
    ApplicationDatabaseCoordinationGuard,
    ApplicationDatabaseHandoffBackend,
)
from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ProtectedApplyComponent,
    ProtectedApplyJournal,
    ProtectedApplyJournalError,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan
from tests.loom_cli.rollout.operator.test_protected_apply_journal import _journal


def _target():
    return ApplicationDatabaseAdmissionTarget("123456", "loom", 42, "loom", 43, "owner", 44)


def _handoff():
    return ApplicationDatabaseHandoffBackend(
        123, "2026-09-09T01:02:03+00:00", "123456", "2026-09-09T00:00:00+00:00", 42
    )


def _guard():
    return ApplicationDatabaseCoordinationGuard(
        replace(_handoff(), pid=124), 45, "loom-rollout-guard-" + "a" * 40,
    )


def test_recovery_binds_the_exact_coordination_guard_and_cannot_replace_it(tmp_path):
    plan, journal = _plan(tmp_path), _journal(tmp_path)

    def apply(_):
        saved = journal.record_application_admission_recovery(
            target=_target(), handoff_backend=_handoff(), coordination_guard=_guard(),
        )
        assert journal.read_application_admission_recovery() == saved
        assert saved.coordination_guard == _guard()
        assert saved.to_dict()["schema_version"] == 2
        for changed in (None, replace(_guard(), backend=replace(_guard().backend, pid=125))):
            with pytest.raises(ProtectedApplyJournalError, match="cannot be replaced"):
                journal.record_application_admission_recovery(
                    target=_target(), handoff_backend=_handoff(), coordination_guard=changed,
                )
        raise RuntimeError("stop after exact guard binding")

    with pytest.raises(RuntimeError, match="exact guard binding"):
        journal.execute(plan, [_component(apply)])


def test_legacy_no_guard_recovery_cannot_silently_admit_a_guard(tmp_path):
    plan, journal = _plan(tmp_path), _journal(tmp_path)

    def apply(_):
        saved = journal.record_application_admission_recovery(target=_target(), handoff_backend=_handoff())
        assert saved.to_dict()["schema_version"] == 1
        assert "coordination_guard" not in saved.to_dict()
        with pytest.raises(ProtectedApplyJournalError, match="cannot be replaced"):
            journal.record_application_admission_recovery(
                target=_target(), handoff_backend=_handoff(), coordination_guard=_guard(),
            )
        raise RuntimeError("stop after legacy refusal")

    with pytest.raises(RuntimeError, match="legacy refusal"):
        journal.execute(plan, [_component(apply)])


@pytest.mark.parametrize("change", ["unknown", "null", "pid-type", "database", "role", "overlap", "server", "version"])
def test_guard_recovery_rejects_malformed_or_rebound_identity(change):
    from loom_cli.rollout.operator.protected_application_admission_recovery import (
        ApplicationAdmissionRecoveryRecord,
    )
    record = ApplicationAdmissionRecoveryRecord("a" * 64, _target(), _handoff(), _guard())
    value = record.to_dict()
    assert ApplicationAdmissionRecoveryRecord.from_dict(value) == record
    guard = value["coordination_guard"]
    if change == "unknown":
        guard["unknown"] = "field"
    elif change == "null":
        value["coordination_guard"] = None
    elif change == "pid-type":
        guard["backend"]["pid"] = True
    elif change == "database":
        guard["backend"]["database_oid"] = 99
    elif change == "role":
        guard["role_oid"] = _target().owner_oid
    elif change == "overlap":
        guard["backend"]["pid"] = _handoff().pid
    elif change == "server":
        guard["backend"]["server_started_at"] = "2000-01-01T00:00:00+00:00"
    else:
        value["schema_version"] = 1
    with pytest.raises(ValueError, match="application"):
        ApplicationAdmissionRecoveryRecord.from_dict(value)


def _component(apply, *, exact=False):
    return ProtectedApplyComponent(
        component_id="application-ownership-handoff",
        implementation_digest="1" * 64,
        input_fingerprint="2" * 64,
        classify=lambda _: ComponentObservation(
            ComponentState.EXACT if exact else ComponentState.READY, "3" * 64, 7
        ),
        apply=apply,
    )


def test_handoff_replacement_chain_preserves_original_and_pending_intent(tmp_path):
    plan, journal = _plan(tmp_path), _journal(tmp_path)
    replacement = replace(_handoff(), pid=126)
    saved = []

    def interrupted(_):
        original = journal.record_application_admission_recovery(
            target=_target(), handoff_backend=_handoff(), coordination_guard=_guard(),
        )
        intent = journal.prepare_application_handoff_recovery(ordinal=1)
        assert journal.prepare_application_handoff_recovery(ordinal=1) == intent
        assert journal.read_application_handoff_recoveries() == ((intent, None),)
        with pytest.raises(ProtectedApplyJournalError, match="pending"):
            journal.prepare_application_handoff_recovery(ordinal=2)
        saved.extend([original, intent])
        raise RuntimeError("lost reopen acknowledgement")

    with pytest.raises(RuntimeError, match="lost reopen"):
        journal.execute(plan, [_component(interrupted)])
    journal = ProtectedApplyJournal(
        tmp_path / "state", request_id=plan.request_id, attempt_number=plan.attempt_number,
    )

    def retry(_):
        original, intent = saved
        assert journal.read_application_admission_recovery() == original
        assert journal.read_application_handoff_recoveries() == ((intent, None),)
        receipt = journal.record_application_handoff_replacement(
            ordinal=1, handoff_backend=replacement,
        )
        assert journal.record_application_handoff_replacement(
            ordinal=1, handoff_backend=replacement,
        ) == receipt
        with pytest.raises(ProtectedApplyJournalError, match="cannot be replaced"):
            journal.record_application_handoff_replacement(
                ordinal=1, handoff_backend=replace(replacement, pid=127),
            )
        successor = journal.prepare_application_handoff_recovery(ordinal=2)
        assert successor.previous_record_digest == receipt.digest
        assert journal.read_application_handoff_recoveries() == (
            (intent, receipt), (successor, None),
        )
        assert journal.read_application_admission_recovery() == original
        raise RuntimeError("stop after successor")

    with pytest.raises(RuntimeError, match="stop after successor"):
        journal.execute(plan, [_component(retry)])


@pytest.mark.parametrize("change", ["same", "guard", "system", "postmaster", "database"])
def test_handoff_replacement_refuses_old_foreign_or_guard_identity(tmp_path, change):
    plan, journal = _plan(tmp_path), _journal(tmp_path)

    def apply(_):
        journal.record_application_admission_recovery(
            target=_target(), handoff_backend=_handoff(), coordination_guard=_guard(),
        )
        journal.prepare_application_handoff_recovery(ordinal=1)
        backend = replace(_handoff(), pid=126)
        if change == "same":
            backend = _handoff()
        elif change == "guard":
            backend = _guard().backend
        elif change == "system":
            backend = replace(backend, system_identifier="999999")
        elif change == "postmaster":
            backend = replace(backend, server_started_at="2026-09-08T00:00:00+00:00")
        else:
            backend = replace(backend, database_oid=99)
        with pytest.raises(ProtectedApplyJournalError, match="identity"):
            journal.record_application_handoff_replacement(ordinal=1, handoff_backend=backend)
        assert journal.read_application_handoff_recoveries()[0][1] is None
        raise RuntimeError("stop after refusal")

    with pytest.raises(RuntimeError, match="stop after refusal"):
        journal.execute(plan, [_component(apply)])


def test_handoff_recovery_requires_original_active_scope_and_order(tmp_path):
    plan, journal = _plan(tmp_path), _journal(tmp_path)
    with pytest.raises(ProtectedApplyJournalError, match="active component"):
        journal.prepare_application_handoff_recovery(ordinal=1)

    def apply(_):
        with pytest.raises(ProtectedApplyJournalError, match="original"):
            journal.prepare_application_handoff_recovery(ordinal=1)
        journal.record_application_admission_recovery(target=_target(), handoff_backend=_handoff())
        for ordinal in (0, True, 2, 17):
            with pytest.raises(ProtectedApplyJournalError, match="ordinal"):
                journal.prepare_application_handoff_recovery(ordinal=ordinal)
        with pytest.raises(ProtectedApplyJournalError, match="intent"):
            journal.record_application_handoff_replacement(
                ordinal=1, handoff_backend=replace(_handoff(), pid=126),
            )
        raise RuntimeError("stop after order")

    with pytest.raises(RuntimeError, match="stop after order"):
        journal.execute(plan, [_component(apply)])


@pytest.mark.parametrize("state", ["absent", "no-guard", "foreign-target", "complete", "old-ordinal"])
def test_fixed_peer_recovery_refuses_unbound_or_completed_state_before_remote_access(tmp_path, monkeypatch, state):
    from loom_cli.rollout.operator.protected_apply_executor import (
        SubprocessProtectedApplyCommandRunner,
    )
    from loom_cli.rollout.operator.protected_peer_database_connection import (
        PeerDatabaseTransportError,
    )

    plan, journal = _plan(tmp_path), _journal(tmp_path)
    runner = SubprocessProtectedApplyCommandRunner()

    def no_remote(*args, **kwargs):
        pytest.fail("refused recovery must not access remote databases")

    monkeypatch.setattr(SubprocessProtectedApplyCommandRunner, "_open_staging_peer", no_remote)
    with pytest.raises(ProtectedApplyJournalError, match="active component"):
        with runner.recover_staging_peer_database(plan, journal=journal, ordinal=1):
            pytest.fail("inactive recovery yielded")

    def apply(_):
        if state != "absent":
            journal.record_application_admission_recovery(
                target=replace(_target(), database="foreign") if state == "foreign-target" else _target(),
                handoff_backend=_handoff(), coordination_guard=None if state == "no-guard" else _guard(),
            )
        if state in {"complete", "old-ordinal"}:
            journal.prepare_application_handoff_recovery(ordinal=1)
            journal.record_application_handoff_replacement(ordinal=1, handoff_backend=replace(_handoff(), pid=126))
            if state == "old-ordinal":
                journal.prepare_application_handoff_recovery(ordinal=2)
        with pytest.raises(PeerDatabaseTransportError, match="protected peer recovery"):
            with runner.recover_staging_peer_database(plan, journal=journal, ordinal=1):
                pytest.fail("unbound recovery yielded")
        raise RuntimeError("stop after remote refusal")

    with pytest.raises(RuntimeError, match="stop after remote refusal"):
        journal.execute(plan, [_component(apply)])


@pytest.mark.parametrize("change", ["gap", "orphan", "unknown", "rebound", "boolean", "symlink", "mode"])
def test_handoff_recovery_read_rejects_changed_chain(tmp_path, change):
    plan, journal = _plan(tmp_path), _journal(tmp_path)

    def apply(_):
        journal.record_application_admission_recovery(target=_target(), handoff_backend=_handoff())
        journal.prepare_application_handoff_recovery(ordinal=1)
        journal.record_application_handoff_replacement(ordinal=1, handoff_backend=replace(_handoff(), pid=126))
        journal.prepare_application_handoff_recovery(ordinal=2)
        raise RuntimeError("interrupt")

    with pytest.raises(RuntimeError, match="interrupt"):
        journal.execute(plan, [_component(apply)])
    root = journal.root / "00-application-ownership-handoff"
    path = root / "application-handoff-01-peer.json"
    if change == "gap":
        path.unlink()
    elif change == "orphan":
        (root / "application-handoff-01-intent.json").unlink()
    elif change == "unknown":
        path.rename(root / "application-handoff-17-peer.json")
    elif change == "symlink":
        path.rename(root / "saved-peer.json")
        path.symlink_to(root / "saved-peer.json")
    elif change == "mode":
        path.chmod(0o644)
    else:
        value = json.loads(path.read_text())
        if change == "rebound":
            value["recovery_intent_digest"] = "f" * 64
        else:
            value["schema_version"] = True
        path.write_text(json.dumps(value))
    with pytest.raises((ProtectedApplyJournalError, OSError)):
        journal.execute(plan, [_component(lambda _: journal.read_application_handoff_recoveries())])


@pytest.mark.parametrize("stage", ["intent", "peer"])
def test_handoff_recovery_reflushes_visible_publication_after_sync_failure(tmp_path, monkeypatch, stage):
    plan, journal = _plan(tmp_path), _journal(tmp_path)
    real_sync = journal._sync_application_recovery
    sync_failure = False

    def sync(root, filename):
        if sync_failure and filename == f"application-handoff-01-{stage}.json":
            raise OSError("injected durability failure")
        real_sync(root, filename)

    monkeypatch.setattr(journal, "_sync_application_recovery", sync)

    def apply(_):
        nonlocal sync_failure
        journal.record_application_admission_recovery(target=_target(), handoff_backend=_handoff())
        if stage == "peer":
            journal.prepare_application_handoff_recovery(ordinal=1)
        sync_failure = True
        with pytest.raises(OSError, match="durability failure"):
            if stage == "intent":
                journal.prepare_application_handoff_recovery(ordinal=1)
            else:
                journal.record_application_handoff_replacement(
                    ordinal=1, handoff_backend=replace(_handoff(), pid=126),
                )
        with pytest.raises(OSError, match="durability failure"):
            journal.read_application_handoff_recoveries()
        sync_failure = False
        records = journal.read_application_handoff_recoveries()
        assert len(records) == 1
        assert (records[0][1] is not None) == (stage == "peer")
        raise RuntimeError("stop after durability")

    with pytest.raises(RuntimeError, match="stop after durability"):
        journal.execute(plan, [_component(apply)])


def test_handoff_recovery_is_bounded_and_never_reuses_a_historical_backend(tmp_path):
    plan, journal = _plan(tmp_path), _journal(tmp_path)

    def apply(_):
        journal.record_application_admission_recovery(target=_target(), handoff_backend=_handoff())
        for ordinal in range(1, 17):
            journal.prepare_application_handoff_recovery(ordinal=ordinal)
            if ordinal > 1:
                with pytest.raises(ProtectedApplyJournalError, match="identity"):
                    journal.record_application_handoff_replacement(
                        ordinal=ordinal, handoff_backend=replace(_handoff(), pid=126),
                    )
            journal.record_application_handoff_replacement(
                ordinal=ordinal, handoff_backend=replace(_handoff(), pid=125 + ordinal),
            )
        with pytest.raises(ProtectedApplyJournalError, match="ordinal"):
            journal.prepare_application_handoff_recovery(ordinal=17)
        assert len(journal.read_application_handoff_recoveries()) == 16
        raise RuntimeError("stop after bounded history")

    with pytest.raises(RuntimeError, match="stop after bounded history"):
        journal.execute(plan, [_component(apply)])


def test_admission_recovery_requires_active_apply_and_survives_a_new_journal(tmp_path):
    plan, journal = _plan(tmp_path), _journal(tmp_path)
    with pytest.raises(ProtectedApplyJournalError, match="active component"):
        journal.record_application_admission_recovery(target=_target(), handoff_backend=_handoff())

    def interrupted(_):
        assert journal.read_application_admission_recovery() is None
        saved = journal.record_application_admission_recovery(
            target=_target(), handoff_backend=_handoff()
        )
        assert journal.read_application_admission_recovery() == saved
        assert (
            journal.record_application_admission_recovery(
                target=_target(), handoff_backend=_handoff()
            )
            == saved
        )
        raise RuntimeError("simulated interrupted handoff")

    with pytest.raises(RuntimeError, match="interrupted handoff"):
        journal.execute(plan, [_component(interrupted)])
    with pytest.raises(ProtectedApplyJournalError, match="active component"):
        journal.read_application_admission_recovery()
    recovered = ProtectedApplyJournal(
        tmp_path / "state", request_id=plan.request_id, attempt_number=plan.attempt_number
    )
    seen = []

    def retry(_):
        record = recovered.read_application_admission_recovery()
        assert record.target == _target()
        assert record.handoff_backend == _handoff()
        seen.append(record)
        raise RuntimeError("stop after readback")

    with pytest.raises(RuntimeError, match="readback"):
        recovered.execute(plan, [_component(retry)])
    assert len(seen) == 1
    path = journal.root / "00-application-ownership-handoff/application-admission.json"
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.stat().st_uid == os.geteuid()
    assert path.stat().st_nlink == 1
    assert not (path.parent / "terminal.json").exists()


def test_admission_recovery_is_write_once_and_rejects_cross_intent_payload(tmp_path):
    plan, journal = _plan(tmp_path), _journal(tmp_path)

    def interrupted(_):
        journal.record_application_admission_recovery(target=_target(), handoff_backend=_handoff())
        with pytest.raises(ProtectedApplyJournalError, match="cannot be replaced"):
            journal.record_application_admission_recovery(
                target=replace(_target(), owner_oid=45), handoff_backend=_handoff()
            )
        raise RuntimeError("interrupted handoff")

    with pytest.raises(RuntimeError, match="interrupted handoff"):
        journal.execute(plan, [_component(interrupted)])
    path = journal.root / "00-application-ownership-handoff/application-admission.json"
    data = json.loads(path.read_text())
    data["intent_digest"] = "f" * 64
    path.write_text(json.dumps(data))

    def retry(_):
        journal.read_application_admission_recovery()

    with pytest.raises(ProtectedApplyJournalError, match="intent"):
        journal.execute(plan, [_component(retry)])


@pytest.mark.parametrize("change", ["symlink", "mode", "unknown_field", "wrong_type"])
def test_admission_recovery_rejects_unsafe_or_invalid_record(tmp_path, change):
    plan, journal = _plan(tmp_path), _journal(tmp_path)

    def interrupted(_):
        journal.record_application_admission_recovery(target=_target(), handoff_backend=_handoff())
        raise RuntimeError("interrupted handoff")

    with pytest.raises(RuntimeError, match="interrupted handoff"):
        journal.execute(plan, [_component(interrupted)])
    path = journal.root / "00-application-ownership-handoff/application-admission.json"
    if change == "symlink":
        destination = path.with_suffix(".old")
        path.rename(destination)
        path.symlink_to(destination)
    elif change == "mode":
        path.chmod(0o644)
    else:
        data = json.loads(path.read_text())
        if change == "unknown_field":
            data["credential"] = "unexpected"
        else:
            data["target"]["database_oid"] = True
        path.write_text(json.dumps(data))
    with pytest.raises((ProtectedApplyJournalError, OSError)):
        journal.execute(plan, [_component(lambda _: journal.read_application_admission_recovery())])


def test_admission_recovery_cannot_be_published_by_classification(tmp_path):
    plan, journal = _plan(tmp_path), _journal(tmp_path)
    component = replace(
        _component(lambda _: None),
        classify=lambda _: journal.record_application_admission_recovery(
            target=_target(), handoff_backend=_handoff()
        ),
    )
    with pytest.raises(ProtectedApplyJournalError, match="active component"):
        journal.execute(plan, [component])


def test_admission_recovery_scope_does_not_leak_to_other_threads(tmp_path):
    plan, journal = _plan(tmp_path), _journal(tmp_path)

    def apply(_):
        journal.record_application_admission_recovery(target=_target(), handoff_backend=_handoff())
        with ThreadPoolExecutor(max_workers=1) as pool:
            with pytest.raises(ProtectedApplyJournalError, match="active component"):
                pool.submit(journal.read_application_admission_recovery).result(timeout=2)
        raise RuntimeError("stop after scope check")

    with pytest.raises(RuntimeError, match="stop after scope check"):
        journal.execute(plan, [_component(apply)])


@pytest.mark.parametrize("retry", [False, True])
def test_admission_recovery_read_requires_durability_before_return(tmp_path, monkeypatch, retry):
    plan, journal = _plan(tmp_path), _journal(tmp_path)
    real_fsync = os.fsync
    fail_sync = False
    synced = []

    def fsync(fd):
        metadata = os.fstat(fd)
        if fail_sync and stat.S_ISDIR(metadata.st_mode):
            raise OSError("injected durability failure")
        synced.append((metadata.st_ino, stat.S_ISDIR(metadata.st_mode)))
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", fsync)

    def apply(_):
        nonlocal fail_sync
        journal.record_application_admission_recovery(target=_target(), handoff_backend=_handoff())
        fail_sync = True
        try:
            with pytest.raises(OSError, match="durability failure"):
                if retry:
                    journal.record_application_admission_recovery(
                        target=_target(), handoff_backend=_handoff()
                    )
                else:
                    journal.read_application_admission_recovery()
        finally:
            fail_sync = False
        synced.clear()
        assert journal.read_application_admission_recovery().target == _target()
        directory = journal.root / "00-application-ownership-handoff"
        for path in (directory, journal.root, journal.attempt_root):
            assert (path.stat().st_ino, True) in synced
        for name in ("intent.json", "application-admission.json"):
            assert ((directory / name).stat().st_ino, False) in synced
        raise RuntimeError("stop after durability check")

    with pytest.raises(RuntimeError, match="stop after durability check"):
        journal.execute(plan, [_component(apply)])
