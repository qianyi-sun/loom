"""Retained fence creation survives lost replies without adopting foreign objects."""

import copy
import json
import os
from uuid import uuid4

import pytest

from loom_cli.rollout.operator.protected_legacy_writer_fence_installation import (
    LegacyWriterFenceInstallation,
    LegacyWriterFenceJournal,
)

_IMAGE = "registry.example.test/loom-control-plane@sha256:" + "a" * 64


class Runner:
    environment = {}

    def __init__(self):
        self.objects = {}
        self.creates = 0
        self.lose_reply = False

    def capture_stdout(self, argv, **kwargs):
        assert argv[:2] == ("kubectl", "get")
        value = self.objects.get((argv[2], argv[3]))
        return json.dumps(value).encode() if value else b""

    def capture_stdout_with_input(self, argv, *, input_payload, **kwargs):
        assert argv[:2] == ("kubectl", "create")
        value = json.loads(input_payload)
        name = (value["kind"], value["metadata"]["name"])
        assert name not in self.objects
        value["metadata"].update(uid=str(uuid4()), resourceVersion="12", generation=1)
        if value["kind"] == "ValidatingAdmissionPolicy":
            value["status"] = {"observedGeneration": 1, "typeChecking": {}}
        self.objects[name] = value
        self.creates += 1
        if self.lose_reply:
            self.lose_reply = False
            raise RuntimeError("lost create reply")
        return json.dumps(value).encode()


def fixture(tmp_path):
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    journal = LegacyWriterFenceJournal(state, "request-one", 1, os.geteuid())
    runner = Runner()
    owner = LegacyWriterFenceInstallation(journal, runner, "b" * 64, _IMAGE, lambda: None)
    return owner, runner


def test_lost_create_reply_reconciles_retained_intent_and_never_removes_fence(tmp_path):
    owner, runner = fixture(tmp_path)
    runner.lose_reply = True
    with pytest.raises(RuntimeError, match="lost create reply"):
        owner.install()
    evidence = owner.install()
    assert len(evidence) == runner.creates == 12
    assert owner.install() == evidence
    assert runner.creates == 12
    assert owner.observe() == evidence


@pytest.mark.parametrize("drift", ["uid", "spec", "generation", "deletion", "missing", "typecheck"])
def test_known_fence_drift_never_causes_recreation_or_overwrite(tmp_path, drift):
    owner, runner = fixture(tmp_path)
    owner.install()
    name = next(iter(runner.objects))
    value = runner.objects[name]
    if drift == "uid":
        value["metadata"]["uid"] = str(uuid4())
    elif drift == "spec":
        value["spec"]["failurePolicy"] = "Ignore"
    elif drift == "generation":
        value["metadata"]["generation"] = 2
    elif drift == "deletion":
        value["metadata"]["deletionTimestamp"] = "2026-09-15T00:00:00Z"
    elif drift == "missing":
        del runner.objects[name]
    else:
        value["status"]["typeChecking"] = {"expressionWarnings": [{"warning": "bad"}]}
    before = copy.deepcopy(runner.objects)
    with pytest.raises((ValueError, RuntimeError)):
        owner.install()
    assert runner.creates == 12 and runner.objects == before


def test_existing_object_without_creation_intent_is_not_adopted(tmp_path):
    owner, runner = fixture(tmp_path)
    document = owner.documents()[0]
    runner.capture_stdout_with_input(("kubectl", "create"), input_payload=json.dumps(document).encode())
    with pytest.raises(RuntimeError, match="unowned"):
        owner.install()
    assert runner.creates == 1


def test_guard_failure_prevents_creation_and_observation_cannot_install(tmp_path):
    from dataclasses import replace
    owner, runner = fixture(tmp_path)
    with pytest.raises(RuntimeError, match="not installed"):
        owner.observe()
    def refuse():
        raise RuntimeError("guard lost")
    with pytest.raises(RuntimeError, match="guard lost"):
        replace(owner, guard=refuse).install()
    assert runner.creates == 0


def test_new_image_cannot_rebind_the_retained_installation(tmp_path):
    from dataclasses import replace
    owner, runner = fixture(tmp_path)
    owner.install()
    with pytest.raises(RuntimeError, match="drifted"):
        replace(owner, control_plane_image=_IMAGE.replace("a" * 64, "c" * 64)).install()
    assert runner.creates == 12
