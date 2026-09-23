"""Two Kubernetes writes are journaled; uncertainty never resumes dispatch."""
from __future__ import annotations

import copy
import importlib
import json
from uuid import uuid4

import pytest


def module():
    return importlib.import_module("scripts.ops.nebius_ingress_cutover")


class API:
    def __init__(self):
        self.service = {
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": "loom-web", "namespace": "loom-platform", "uid": str(uuid4()), "resourceVersion": "1"},
            "spec": {"type": "LoadBalancer", "selector": {"app": "loom-web"}, "clusterIP": "10.0.0.20",
                     "ports": [{"port": 443, "targetPort": 8443, "nodePort": 30443}]},
            "status": {"loadBalancer": {"ingress": [{"ip": "192.0.2.12"}]}},
        }
        self.config = {
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": "loom-platform-config", "namespace": "loom-platform", "uid": str(uuid4()), "resourceVersion": "2"},
            "data": {"environment.json": '{"shared_ingress_enabled":false,"keep":"unchanged"}',
                     "profile.json": json.dumps({"candidate_sha": "a" * 40}), "keyring.json": "private-keyring"},
        }
        self.owner = None
        self.writes = []
        self.failure = None
        self.denied = None
        self.probe_ok = True
        self.preflight_ok = True

    def read(self):
        return copy.deepcopy(self.service), copy.deepcopy(self.config)

    def qualify(self):
        if not self.preflight_ok:
            raise RuntimeError("private preflight detail")

    def guard(self, action, owner, candidate):
        assert candidate == "a" * 40
        if action == "observe":
            return {"status": "open" if self.owner is None else "held" if self.owner == owner else "skipped_locked"}
        self.writes.append(action)
        if self.failure == action + "-before":
            raise TimeoutError("private guard detail")
        if action == "acquire":
            if self.denied:
                return {"status": self.denied}
            assert self.owner is None
            self.owner = owner
        else:
            assert action == "release" and self.owner == owner
            self.owner = None
        if self.failure == action + "-after":
            raise TimeoutError("private guard detail")
        return {"status": "acquired" if action == "acquire" else "released"}

    def patch(self, before, after):
        field = "service" if before["kind"] == "Service" else "config"
        assert getattr(self, field) == before
        assert self.owner is not None
        self.writes.append(field)
        if self.failure == field + "-before":
            raise TimeoutError("private write detail")
        result = copy.deepcopy(after)
        result["metadata"]["resourceVersion"] = str(int(before["metadata"]["resourceVersion"]) + 10)
        setattr(self, field, result)
        if self.failure == field + "-after":
            raise TimeoutError("private write detail")

    def public_probe(self):
        if not self.probe_ok:
            raise RuntimeError("private health detail")


@pytest.fixture
def args(tmp_path):
    return {"api": API(), "state_dir": tmp_path / "operation", "installation_id": str(uuid4()),
            "candidate": "a" * 40, "namespace": "loom-platform"}


def test_cutover_preserves_allocation_and_other_configuration_then_releases_once(args):
    api = args["api"]
    before = copy.deepcopy(api.service)
    result = module().cutover(**args)
    assert result["status"] == "complete"
    assert api.service["metadata"]["uid"] == before["metadata"]["uid"]
    assert api.service["status"] == before["status"]
    assert api.service["spec"] == {**before["spec"], "selector": {"app": "loom-shared-ingress"}}
    assert json.loads(api.config["data"]["environment.json"]) == {"shared_ingress_enabled": True, "keep": "unchanged"}
    assert api.config["data"]["keyring.json"] == "private-keyring"
    assert api.writes == ["acquire", "service", "config", "release"]
    assert api.owner is None
    assert module().cutover(**args) == result
    assert api.writes == ["acquire", "service", "config", "release"]
    assert "private-keyring" not in json.dumps(result)
    assert all(path.stat().st_mode & 0o077 == 0 for path in args["state_dir"].glob("*.json"))


@pytest.mark.parametrize("failure", ["acquire-before", "service-before", "config-before"])
def test_unknown_write_not_observed_is_never_repeated_or_released(args, failure):
    api = args["api"]
    api.failure = failure
    with pytest.raises(module().CutoverError):
        module().cutover(**args)
    written = list(api.writes)
    api.failure = None
    with pytest.raises(module().CutoverError):
        module().cutover(**args)
    assert api.writes == written and "release" not in written


@pytest.mark.parametrize("failure", ["acquire-after", "service-after", "config-after", "release-after"])
def test_lost_reply_reconciles_exact_readback_without_repeating_write(args, failure):
    args["api"].failure = failure
    assert module().cutover(**args)["status"] == "complete"
    assert args["api"].writes == ["acquire", "service", "config", "release"]


def test_failed_public_probe_retains_owned_pause_and_resumes_by_readback(args):
    api = args["api"]
    api.probe_ok = False
    with pytest.raises(module().CutoverError) as error:
        module().cutover(**args)
    assert "private health" not in str(error.value)
    assert api.writes == ["acquire", "service"] and api.owner is not None
    api.probe_ok = True
    assert module().cutover(**args)["status"] == "complete"
    assert api.writes == ["acquire", "service", "config", "release"]


@pytest.mark.parametrize("denied", ["skipped_busy", "skipped_locked"])
def test_busy_or_foreign_guard_never_changes_routing_or_releases(args, denied):
    args["api"].denied = denied
    assert module().cutover(**args)["status"] == denied
    assert args["api"].writes == ["acquire"]


@pytest.mark.parametrize("drift", ["candidate", "service-uid", "port", "allocation", "config", "owner", "marker"])
def test_recovery_rejects_concurrent_drift_without_more_writes(args, drift):
    api = args["api"]
    api.probe_ok = False
    with pytest.raises(module().CutoverError):
        module().cutover(**args)
    api.probe_ok = True
    if drift == "candidate":
        api.config["data"]["profile.json"] = json.dumps({"candidate_sha": "b" * 40})
    elif drift == "service-uid":
        api.service["metadata"]["uid"] = str(uuid4())
    elif drift == "port":
        api.service["spec"]["ports"][0]["targetPort"] = 9443
    elif drift == "allocation":
        api.service["status"]["loadBalancer"]["ingress"][0]["ip"] = "192.0.2.13"
    elif drift == "config":
        api.config["data"]["keyring.json"] = "foreign-change"
    elif drift == "marker":
        api.service["metadata"]["annotations"] = {}
    else:
        api.owner = "foreign"
    written = list(api.writes)
    with pytest.raises(module().CutoverError):
        module().cutover(**args)
    assert api.writes == written


def test_failed_live_capacity_or_identity_check_prevents_any_write(args):
    args["api"].preflight_ok = False
    with pytest.raises(module().CutoverError):
        module().cutover(**args)
    assert args["api"].writes == []


def test_old_installed_guard_without_observation_support_is_rejected_before_pause(args):
    api = args["api"]
    guard = api.guard

    def old_guard(action, owner, candidate):
        if action == "observe":
            raise RuntimeError("unknown installed CLI action")
        return guard(action, owner, candidate)

    api.guard = old_guard
    with pytest.raises(module().CutoverError):
        module().cutover(**args)
    assert api.writes == [] and api.owner is None
