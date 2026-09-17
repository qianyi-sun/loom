"""Fence parameters and object identity survive only under their active intent."""

import json
import os

import pytest

from loom_cli.rollout.operator.protected_apply_journal import ProtectedApplyJournal
from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
from tests.loom_cli.rollout.operator.test_application_credential_recovery import _sources
from tests.loom_cli.rollout.operator.test_protected_apply_journal import _journal

_UID = "22222222-2222-4222-8222-222222222222"


def test_guarded_fence_refuses_legacy_restart_authority():
    from loom_cli.rollout.operator.protected_cnpg_fence_recovery import CNPGFenceRequest

    legacy = {
        "schema_version": 1, "intent_digest": "a" * 64,
        "restart_principal": "system:admin", "restart_timestamp": "2026-09-09T17:00:00Z",
        "target_pooler_names": ["existing-target"],
        # Exact old renderer output at ff8f06f6f; do not derive a legacy artifact
        # from the current renderer, whose semantics deliberately differ.
        "documents_sha256": "4ae05ddb098b6058fd7315db2c4910fad7c382a30bb64504af68d9c4f122efc8",
    }
    with pytest.raises(ValueError, match="CNPG fence"):
        CNPGFenceRequest.from_dict(legacy)


@pytest.mark.parametrize("change", ["version", "restart-field"])
def test_guarded_fence_requires_exact_version_two_contract(change):
    from loom_cli.rollout.operator.protected_cnpg_fence_recovery import CNPGFenceRequest

    request = CNPGFenceRequest("a" * 64, ())
    value = request.to_dict()
    assert value["schema_version"] == 2
    assert CNPGFenceRequest.from_dict(value) == request
    if change == "version":
        value["schema_version"] = 1
    else:
        value["restart_principal"] = "system:admin"
    with pytest.raises(ValueError, match="CNPG fence"):
        CNPGFenceRequest.from_dict(value)


def _prepare(plan, journal, **overrides):
    return journal.prepare_application_cnpg_fence(plan, **{
        "target_pooler_names": (), **overrides,
    })


def test_fence_records_require_active_intent_and_recover_exactly(tmp_path):
    plan, _ = _sources(tmp_path)
    journal = _journal(tmp_path)
    with pytest.raises(RuntimeError, match="active component"):
        _prepare(plan, journal)
    seen = []

    def apply(_):
        request = _prepare(plan, journal)
        assert journal.read_application_cnpg_fence(plan) == request
        receipt = journal.record_application_cnpg_fence_object(plan, ordinal=0, uid=_UID)
        assert journal.read_application_cnpg_fence_object(plan, ordinal=0) == receipt
        assert receipt.intent_digest == request.intent_digest
        seen.append((request, receipt))
        raise RuntimeError("interrupted after fence identity")

    with pytest.raises(RuntimeError, match="interrupted"):
        journal.execute(plan, [_component(apply)])
    journal = ProtectedApplyJournal(tmp_path / "state", request_id=plan.request_id,
                                    attempt_number=plan.attempt_number)
    with pytest.raises(RuntimeError, match="interrupted"):
        journal.execute(plan, [_component(apply)])
    assert len(seen) == 2 and seen[0] == seen[1]
    for path in journal.root.rglob("application-cnpg-fence*.json"):
        assert path.stat().st_mode & 0o777 == 0o600
    assert not list(journal.root.rglob("terminal.json"))


@pytest.mark.parametrize("change", ["poolers", "uid"])
def test_recovery_cannot_rebind_request_or_object_identity(tmp_path, change):
    plan, _ = _sources(tmp_path)
    journal = _journal(tmp_path)

    def initial(_):
        _prepare(plan, journal)
        journal.record_application_cnpg_fence_object(plan, ordinal=0, uid=_UID)
        raise RuntimeError("interrupted")

    with pytest.raises(RuntimeError, match="interrupted"):
        journal.execute(plan, [_component(initial)])
    before = {p: p.read_bytes() for p in journal.root.rglob("application-cnpg-fence*.json")}

    def changed(_):
        if change == "uid":
            journal.record_application_cnpg_fence_object(
                plan, ordinal=0, uid="33333333-3333-4333-8333-333333333333")
        else:
            _prepare(plan, journal, target_pooler_names=("another",))

    with pytest.raises(RuntimeError, match="cannot be replaced"):
        journal.execute(plan, [_component(changed)])
    assert {p: p.read_bytes() for p in before} == before


@pytest.mark.parametrize("which", ["request", "object"])
@pytest.mark.parametrize("replay", [False, True])
def test_fence_identity_is_fsynced_before_fresh_or_replayed_return(tmp_path, monkeypatch, which, replay):
    plan, _ = _sources(tmp_path)
    journal = _journal(tmp_path)
    filename = "application-cnpg-fence-" + ("request" if which == "request" else "00-object") + ".json"
    path = journal.root / "00-application-ownership-handoff" / filename

    def apply(_):
        _prepare(plan, journal)
        journal.record_application_cnpg_fence_object(plan, ordinal=0, uid=_UID)
        raise RuntimeError("interrupted")

    if replay:
        with pytest.raises(RuntimeError, match="interrupted"):
            journal.execute(plan, [_component(apply)])
    fsync = os.fsync

    def fail(fd):
        if path.exists() and os.fstat(fd).st_ino == path.stat().st_ino:
            raise OSError("fence fsync failure")
        fsync(fd)

    monkeypatch.setattr(os, "fsync", fail)
    with pytest.raises(OSError, match="fence fsync"):
        journal.execute(plan, [_component(apply)])
    assert not list(journal.root.rglob("terminal.json"))


@pytest.mark.parametrize("field,value", [("ordinal", True), ("intent_digest", "f" * 64),
                                        ("document_sha256", "f" * 64), ("uid", "not-a-uid")])
def test_saved_object_receipt_is_typed_and_bound_to_exact_rendered_object(tmp_path, field, value):
    plan, _ = _sources(tmp_path)
    journal = _journal(tmp_path)

    def initial(_):
        _prepare(plan, journal)
        journal.record_application_cnpg_fence_object(plan, ordinal=0, uid=_UID)
        raise RuntimeError("interrupted")

    with pytest.raises(RuntimeError, match="interrupted"):
        journal.execute(plan, [_component(initial)])
    path = journal.root / "00-application-ownership-handoff/application-cnpg-fence-00-object.json"
    value_dict = json.loads(path.read_text())
    value_dict[field] = value
    path.write_text(json.dumps(value_dict))
    with pytest.raises((RuntimeError, ValueError), match="CNPG"):
        journal.execute(plan, [_component(lambda _: journal.read_application_cnpg_fence_object(plan, ordinal=0))])


@pytest.mark.parametrize("field,value", [("schema_version", True), ("documents_sha256", "f" * 64),
                                        ("intent_digest", "f" * 64),
                                        ("target_pooler_names", ["duplicate", "duplicate"]),
                                        ("unknown", "field")])
def test_saved_request_rejects_invalid_fields_or_changed_rendering(tmp_path, field, value):
    plan, _ = _sources(tmp_path)
    journal = _journal(tmp_path)

    def initial(_):
        _prepare(plan, journal)
        raise RuntimeError("interrupted")

    with pytest.raises(RuntimeError, match="interrupted"):
        journal.execute(plan, [_component(initial)])
    path = journal.root / "00-application-ownership-handoff/application-cnpg-fence-request.json"
    saved = json.loads(path.read_text())
    saved[field] = value
    path.write_text(json.dumps(saved))
    with pytest.raises(ValueError, match="CNPG"):
        journal.execute(plan, [_component(lambda _: journal.read_application_cnpg_fence(plan))])


@pytest.mark.parametrize("ordinal", [True, -1, 10, "../../other"])
def test_invalid_object_ordinal_cannot_select_a_journal_path(tmp_path, ordinal):
    plan, _ = _sources(tmp_path)
    journal = _journal(tmp_path)

    def apply(_):
        _prepare(plan, journal)
        journal.read_application_cnpg_fence_object(plan, ordinal=ordinal)

    with pytest.raises(ValueError, match="CNPG"):
        journal.execute(plan, [_component(apply)])
    assert not list(journal.root.rglob("*-object.json"))


def test_object_identity_cannot_precede_durable_request(tmp_path):
    plan, _ = _sources(tmp_path)
    journal = _journal(tmp_path)
    with pytest.raises(RuntimeError, match="request must precede"):
        journal.execute(plan, [_component(lambda _: journal.record_application_cnpg_fence_object(
            plan, ordinal=0, uid=_UID))])
    assert not list(journal.root.rglob("*-object.json"))
