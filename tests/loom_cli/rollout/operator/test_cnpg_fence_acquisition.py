"""A journaled absent-to-create transition, not a name match, permits recovery."""

import copy
import json
import os
from uuid import uuid4

import pytest

from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
from tests.loom_cli.rollout.operator.test_application_credential_recovery import _sources
from tests.loom_cli.rollout.operator.test_cnpg_fence_recovery import _prepare
from tests.loom_cli.rollout.operator.test_protected_apply_journal import _journal


class FenceRunner:
    def __init__(self):
        self.environment = {"KUBECONFIG": "/fixture/protected-config"}
        self.objects = {}
        self.creates = []
        self.calls = []
        self.lose_reply = False
        self.fail_read = False
        self.enforced = True
        self.before_create = lambda value: None
        self.during_probe = lambda: None

    def capture_stdout(self, argv, *, env, timeout_seconds):
        self.calls.append(tuple(argv))
        assert env == self.environment and timeout_seconds == 30
        assert tuple(argv[:2]) == ("kubectl", "get") and "--ignore-not-found=true" in argv
        if self.fail_read:
            raise RuntimeError("simulated transport failure")
        value = self.objects.get((argv[2], argv[3]))
        return b"" if value is None else json.dumps(value).encode()

    def capture_stdout_with_input(self, argv, *, env, input_payload, timeout_seconds):
        assert env == self.environment and timeout_seconds == 30
        assert tuple(argv[:2]) == ("kubectl", "create")
        assert "--field-manager=loom-cnpg-fence" in argv
        value = json.loads(input_payload)
        self.before_create(value)
        metadata = value["metadata"]
        metadata.update(uid=str(uuid4()), resourceVersion="101", generation=1,
                        creationTimestamp="2026-09-09T17:30:00Z",
                        managedFields=[{"manager": "loom-cnpg-fence", "operation": "Update",
                                        "apiVersion": value["apiVersion"], "fieldsType": "FieldsV1",
                                        "fieldsV1": {"f:spec": {}, "f:metadata": {"f:annotations": {
                                            "f:loom.dev/handoff-intent": {}, "f:loom.dev/fence-create-nonce": {}}}}}])
        resource = ("validatingadmissionpolicies" if value["kind"] == "ValidatingAdmissionPolicy"
                    else "validatingadmissionpolicybindings") + ".admissionregistration.k8s.io"
        assert (resource, metadata["name"]) not in self.objects
        self.objects[resource, metadata["name"]] = value
        if value["kind"] == "ValidatingAdmissionPolicy":
            value["status"] = {"observedGeneration": 1, "typeChecking": {}}
        self.creates.append(copy.deepcopy(value))
        if self.lose_reply:
            self.lose_reply = False
            raise RuntimeError("simulated lost create reply")
        return json.dumps(value).encode()

    def probe_cnpg_input_fence(self, **kwargs):
        self.during_probe()
        return self.enforced


def _acquire(plan, journal, runner):
    from loom_cli.rollout.operator.protected_cnpg_fence_acquisition import acquire_cnpg_input_fence
    return acquire_cnpg_input_fence(plan, journal=journal, runner=runner)


def _apply(plan, journal, runner):
    def apply(_):
        _prepare(plan, journal)
        receipts = _acquire(plan, journal, runner)
        assert len(receipts) == 10
        raise RuntimeError("stop after acquired fence")
    return apply


def test_acquisition_requires_active_request_and_write_ahead_create_intents(tmp_path):
    plan, _ = _sources(tmp_path)
    journal, runner = _journal(tmp_path), FenceRunner()
    with pytest.raises(RuntimeError, match="active component"):
        _acquire(plan, journal, runner)
    assert runner.calls == []

    def before(value):
        ordinal = len(runner.creates)
        intent = journal.read_application_cnpg_fence_create(plan, ordinal=ordinal)
        assert intent is not None
        assert value["metadata"]["annotations"]["loom.dev/fence-create-nonce"] == intent.nonce

    runner.before_create = before
    for _ in range(2):
        with pytest.raises(RuntimeError, match="stop after acquired"):
            journal.execute(plan, [_component(_apply(plan, journal, runner))])
    assert len(runner.creates) == 10


def test_lost_create_reply_recovers_from_exact_pending_marker_without_duplicate(tmp_path):
    plan, _ = _sources(tmp_path)
    journal, runner = _journal(tmp_path), FenceRunner()
    runner.lose_reply = True
    with pytest.raises(RuntimeError, match="lost create reply"):
        journal.execute(plan, [_component(_apply(plan, journal, runner))])
    original = copy.deepcopy(runner.creates[0])
    with pytest.raises(RuntimeError, match="stop after acquired"):
        journal.execute(plan, [_component(_apply(plan, journal, runner))])
    assert len(runner.creates) == 10 and runner.creates[0] == original


def test_legacy_restart_request_blocks_retry_before_any_api_access(tmp_path):
    plan, _ = _sources(tmp_path)
    journal, runner = _journal(tmp_path), FenceRunner()
    with pytest.raises(RuntimeError, match="stop after acquired"):
        journal.execute(plan, [_component(_apply(plan, journal, runner))])
    path = journal.root / "00-application-ownership-handoff/application-cnpg-fence-request.json"
    legacy = json.loads(path.read_text())
    legacy.update(schema_version=1, restart_principal="system:admin",
                  restart_timestamp="2026-09-09T17:00:00Z")
    path.write_text(json.dumps(legacy))
    before = {p: p.read_bytes() for p in path.parent.glob("application-cnpg-fence*.json")}
    objects = copy.deepcopy(runner.objects)
    runner.calls.clear()
    runner.creates.clear()
    with pytest.raises(ValueError, match="CNPG fence request fields"):
        journal.execute(plan, [_component(lambda _: _acquire(plan, journal, runner))])
    assert runner.calls == runner.creates == []
    assert runner.objects == objects
    assert before == {p: p.read_bytes() for p in before}


@pytest.mark.parametrize("pending_reply,change", [(False, change) for change in
                                                 ("missing", "uid", "spec", "nonce", "manager", "generation")] +
                                                [(True, change) for change in ("spec", "nonce", "manager", "generation")])
def test_known_or_pending_changed_object_is_never_replaced(tmp_path, pending_reply, change):
    plan, _ = _sources(tmp_path)
    journal, runner = _journal(tmp_path), FenceRunner()
    runner.lose_reply = pending_reply
    with pytest.raises(RuntimeError, match="lost create reply" if pending_reply else "stop after acquired"):
        journal.execute(plan, [_component(_apply(plan, journal, runner))])
    key = next(iter(runner.objects))
    value = runner.objects[key]
    if change == "missing":
        del runner.objects[key]
    elif change == "spec":
        value["spec"]["failurePolicy"] = "Ignore"
    elif change == "nonce":
        value["metadata"]["annotations"]["loom.dev/fence-create-nonce"] = "f" * 32
    elif change == "manager":
        value["metadata"]["managedFields"][0]["manager"] = "foreign"
    else:
        value["metadata"][change] = str(uuid4()) if change == "uid" else True
    before = copy.deepcopy(runner.objects)
    with pytest.raises((RuntimeError, ValueError), match="CNPG"):
        journal.execute(plan, [_component(_apply(plan, journal, runner))])
    assert runner.objects == before and len(runner.creates) == (1 if pending_reply else 10)


def test_existing_object_without_pending_create_intent_is_not_adopted(tmp_path):
    plan, _ = _sources(tmp_path)
    journal, runner = _journal(tmp_path), FenceRunner()

    def apply(_):
        request = _prepare(plan, journal)
        value = request.documents()[0]
        runner.objects["validatingadmissionpolicies.admissionregistration.k8s.io", value["metadata"]["name"]] = value
        _acquire(plan, journal, runner)

    with pytest.raises(RuntimeError, match="CNPG"):
        journal.execute(plan, [_component(apply)])
    assert runner.creates == []


def test_read_failure_is_not_absence_and_non_enforcement_is_not_success(tmp_path):
    plan, _ = _sources(tmp_path)
    journal, runner = _journal(tmp_path), FenceRunner()
    runner.fail_read = True
    with pytest.raises(RuntimeError, match="transport"):
        journal.execute(plan, [_component(_apply(plan, journal, runner))])
    assert runner.creates == [] and not list(journal.root.rglob("*-create.json"))
    runner.fail_read = False
    runner.enforced = False
    with pytest.raises(RuntimeError, match=r"CNPG.*not enforcing"):
        journal.execute(plan, [_component(_apply(plan, journal, runner))])
    assert len(runner.creates) == 10


@pytest.mark.parametrize("replay", [False, True])
def test_create_intent_fsync_failure_prevents_external_create(tmp_path, monkeypatch, replay):
    plan, _ = _sources(tmp_path)
    journal, runner = _journal(tmp_path), FenceRunner()
    path = journal.root / "00-application-ownership-handoff/application-cnpg-fence-00-create.json"
    if replay:
        def prepared(_):
            _prepare(plan, journal)
            journal.prepare_application_cnpg_fence_create(plan, ordinal=0)
            raise RuntimeError("stop after prepared create")
        with pytest.raises(RuntimeError, match="prepared create"):
            journal.execute(plan, [_component(prepared)])
    fsync = os.fsync

    def fail(fd):
        if path.exists() and os.fstat(fd).st_ino == path.stat().st_ino:
            raise OSError("create intent fsync failure")
        fsync(fd)

    monkeypatch.setattr(os, "fsync", fail)
    with pytest.raises(OSError, match="create intent fsync"):
        journal.execute(plan, [_component(_apply(plan, journal, runner))])
    assert runner.creates == []


@pytest.mark.parametrize("foreign_spec_field", [False, True])
def test_policy_status_apply_is_allowed_but_cannot_claim_input_fields(tmp_path, foreign_spec_field):
    plan, _ = _sources(tmp_path)
    journal, runner = _journal(tmp_path), FenceRunner()
    with pytest.raises(RuntimeError, match="stop after acquired"):
        journal.execute(plan, [_component(_apply(plan, journal, runner))])
    fields = {"f:status": {"f:observedGeneration": {}}}
    if foreign_spec_field:
        fields["f:spec"] = {}
    next(iter(runner.objects.values()))["metadata"]["managedFields"].append({
        "manager": "validatingadmissionpolicy-status", "operation": "Apply", "subresource": "status",
        "apiVersion": "admissionregistration.k8s.io/v1", "fieldsType": "FieldsV1", "fieldsV1": fields,
    })
    with pytest.raises((ValueError, RuntimeError), match="CNPG" if foreign_spec_field else "stop after acquired"):
        journal.execute(plan, [_component(_apply(plan, journal, runner))])
    assert len(runner.creates) == 10


@pytest.mark.parametrize("change", ["missing-status", "stale-status", "warnings", "uid", "spec"])
def test_final_readback_rejects_bad_type_checking_and_changes_during_probes(tmp_path, change):
    plan, _ = _sources(tmp_path)
    journal, runner = _journal(tmp_path), FenceRunner()

    def drift():
        value = next(iter(runner.objects.values()))
        if change == "missing-status":
            value.pop("status")
        elif change == "stale-status":
            value["status"]["observedGeneration"] = 0
        elif change == "warnings":
            value["status"]["typeChecking"] = {"expressionWarnings": [{"warning": "type mismatch"}]}
        elif change == "uid":
            value["metadata"]["uid"] = str(uuid4())
        else:
            value["spec"]["failurePolicy"] = "Ignore"

    runner.during_probe = drift
    with pytest.raises((ValueError, RuntimeError), match="CNPG"):
        journal.execute(plan, [_component(_apply(plan, journal, runner))])
    assert len(runner.creates) == 10 and not list(journal.root.rglob("terminal.json"))


@pytest.mark.parametrize("tamper", [False, True])
def test_prepared_but_not_sent_create_resumes_only_with_exact_record(tmp_path, tamper):
    plan, _ = _sources(tmp_path)
    journal, runner = _journal(tmp_path), FenceRunner()
    original = []

    def prepare(_):
        _prepare(plan, journal)
        original.append(journal.prepare_application_cnpg_fence_create(plan, ordinal=0))
        raise RuntimeError("stop before create")

    with pytest.raises(RuntimeError, match="before create"):
        journal.execute(plan, [_component(prepare)])
    if tamper:
        path = journal.root / "00-application-ownership-handoff/application-cnpg-fence-00-create.json"
        saved = json.loads(path.read_text())
        saved["nonce"] = "f" * 32
        path.write_text(json.dumps(saved))
    with pytest.raises((RuntimeError, ValueError), match="CNPG" if tamper else "stop after acquired"):
        journal.execute(plan, [_component(_apply(plan, journal, runner))])
    if tamper:
        assert runner.creates == []
    else:
        assert len(runner.creates) == 10
        assert runner.creates[0]["metadata"]["annotations"]["loom.dev/fence-create-nonce"] == original[0].nonce
