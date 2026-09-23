"""Concrete cutover transport submits only one fixed UID/RV-fenced mutation."""
from __future__ import annotations

import copy
import importlib
import json
from uuid import uuid4

import pytest
from scripts.ops.nebius_ingress_gateway import TLSBinding
from tests.ops.test_nebius_ingress_cutover import API


def module():
    return importlib.import_module("scripts.ops.nebius_ingress_cutover")


@pytest.fixture
def transport(tmp_path):
    binding = TLSBinding(str(uuid4()), str(uuid4()), "loom-platform", str(uuid4()), str(uuid4()),
                         "dev.example.test", "management.example.test")
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.touch(mode=0o600)

    class Transport(module().KubectlCutoverAPI):
        def __init__(self):
            super().__init__(kubeconfig, binding=binding, executable=tmp_path / "kubectl", candidate="a" * 40)
            self.calls = []
            self.responses = []

        def _run(self, arguments, *, payload=None):
            self.calls.append((arguments, payload))
            if arguments[:2] == ["get", "namespace"]:
                name = arguments[2]
                uid = binding.namespace_uid if name == binding.namespace else binding.kube_system_uid
                return json.dumps({"kind": "Namespace", "metadata": {"name": name, "uid": uid}}).encode()
            return self.responses.pop(0)

    return Transport()


@pytest.mark.parametrize("kind", ["Service", "ConfigMap"])
def test_patch_guards_full_identity_and_changes_only_routing_field_and_operation_marker(transport, kind):
    fixture = API()
    before = fixture.service if kind == "Service" else fixture.config
    after = copy.deepcopy(before)
    owner = str(uuid4())
    after["metadata"]["annotations"] = {module().MARKER: owner}
    if kind == "Service":
        after["spec"]["selector"] = {"app": "loom-shared-ingress"}
    else:
        after["data"]["environment.json"] = json.dumps({"shared_ingress_enabled": True, "keep": "unchanged"})
    transport.responses = [b"patched"]
    transport.patch(before, after)
    calls = [(args, payload) for args, payload in transport.calls if args[0] == "patch"]
    assert len(calls) == 1
    arguments, payload = calls[0]
    assert arguments == ["patch", kind, before["metadata"]["name"], "-n", "loom-platform",
                         "--type=json", "--patch-file=/dev/stdin", "-o", "name"]
    patch = json.loads(payload)
    assert patch[:2] == [{"op": "test", "path": "/metadata/uid", "value": before["metadata"]["uid"]},
                         {"op": "test", "path": "/metadata/resourceVersion", "value": before["metadata"]["resourceVersion"]}]
    replacements = [item for item in patch if item["op"] != "test"]
    assert replacements == [
        {"op": "replace", "path": "/spec/selector" if kind == "Service" else "/data/environment.json",
         "value": {"app": "loom-shared-ingress"} if kind == "Service" else after["data"]["environment.json"]},
        {"op": "add", "path": "/metadata/annotations", "value": {module().MARKER: owner}},
    ]


@pytest.mark.parametrize("drift", ["namespace", "name", "allocation", "ports", "annotations", "marker", "candidate", "extra-setting"])
def test_transport_rejects_any_extra_authority_before_mutation(transport, drift):
    fixture = API()
    before = copy.deepcopy(fixture.config if drift in {"candidate", "extra-setting"} else fixture.service)
    after = copy.deepcopy(before)
    after["metadata"]["annotations"] = {module().MARKER: str(uuid4())}
    if before["kind"] == "Service":
        after["spec"]["selector"] = {"app": "loom-shared-ingress"}
    else:
        after["data"]["environment.json"] = json.dumps({"shared_ingress_enabled": True, "keep": "unchanged"})
    if drift == "namespace":
        before["metadata"]["namespace"] = after["metadata"]["namespace"] = "foreign"
    elif drift == "name":
        before["metadata"]["name"] = after["metadata"]["name"] = "foreign"
    elif drift == "allocation":
        after["spec"]["clusterIP"] = "10.0.0.22"
    elif drift == "ports":
        after["spec"]["ports"][0]["port"] = 444
    elif drift == "annotations":
        after["metadata"]["annotations"]["foreign"] = "extra"
    elif drift == "marker":
        after["metadata"]["annotations"][module().MARKER] = "not-an-operation"
    elif drift == "candidate":
        before["data"]["profile.json"] = after["data"]["profile.json"] = json.dumps({"candidate_sha": "b" * 40})
    else:
        after["data"]["environment.json"] = json.dumps({"shared_ingress_enabled": True, "keep": "changed"})
    with pytest.raises(module().CutoverError):
        transport.patch(before, after)
    assert not any(args[0] == "patch" for args, _payload in transport.calls)


@pytest.mark.parametrize("action,status", [("acquire", "acquired"), ("observe", "held"), ("release", "released")])
def test_guard_executes_only_fixed_module_and_exact_owner_candidate(transport, action, status):
    owner = str(uuid4())
    transport.responses = [json.dumps({"status": status}).encode()]
    assert transport.guard(action, owner, "a" * 40) == {"status": status}
    assert transport.calls[-1] == (["exec", "-n", "loom-platform", "deployment/loom-control-plane", "--",
                                  "python", "-m", "loom.nebius_rollout_guard", action, "--owner", owner,
                                  "--candidate", "a" * 40], None)


@pytest.mark.parametrize("action,owner,candidate", [("delete", "valid", "a" * 40), ("acquire", "--anything", "a" * 40),
                                                   ("observe", str(uuid4()), "b" * 40)])
def test_guard_transport_denies_unbound_commands(transport, action, owner, candidate):
    with pytest.raises(module().CutoverError):
        transport.guard(action, owner, candidate)
    assert not transport.calls


def test_transport_never_retries_a_lost_patch_response(transport):
    before = API().service
    after = copy.deepcopy(before)
    after["spec"]["selector"] = {"app": "loom-shared-ingress"}
    after["metadata"]["annotations"] = {module().MARKER: str(uuid4())}
    with pytest.raises(module().CutoverError):
        transport.patch(before, after)  # Empty response fixture simulates lost transport.
    assert sum(args[0] == "patch" for args, _payload in transport.calls) == 1
