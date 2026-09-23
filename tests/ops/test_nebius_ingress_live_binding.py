"""Installation derives rendering inputs from current protected config, not handoff copies."""
from __future__ import annotations

import copy
import importlib
import json
from uuid import uuid4

import pytest
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

from scripts.ops.nebius_ingress_gateway import TLSBinding
from scripts.ops.nebius_ingress_image import DIGEST


def module():
    return importlib.import_module("scripts.ops.nebius_ingress_operation")


@pytest.fixture
def live(tmp_path, platform_inputs):
    config, candidate, profile = copy.deepcopy(platform_inputs)
    tls = TLSBinding(str(uuid4()), str(uuid4()), config["namespace"], str(uuid4()), str(uuid4()),
                     "dev.example.test", "management.example.test")
    metadata = {"binding": tls, "cluster_id": config["cluster_id"], "candidate": candidate["candidate_sha"],
                "api_server": config["kubernetes_api_server"], "ingress_class": "loom-shared",
                "image": "cr.eu-north1.nebius.cloud/test/loom-shared-ingress@" + DIGEST}
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.touch(mode=0o600)

    class Live(module().LiveIngressAPI):
        def __init__(self):
            super().__init__(kubeconfig, executable=tmp_path / "kubectl", **metadata)
            self.config = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {
                "name": "loom-platform-config", "namespace": config["namespace"], "uid": str(uuid4()), "resourceVersion": "5"},
                "data": {"environment.json": json.dumps(config), "profile.json": json.dumps(profile), "keyring.json": "{}"}}
            self.view = {"clusters": [{"name": "nebius-" + config["cluster_id"], "cluster": {
                "server": config["kubernetes_api_server"], "certificate-authority-data": "test-ca"}}]}
            self.namespace_uid = tls.namespace_uid
            self.calls = []

        def _run(self, arguments, *, payload=None):
            self.calls.append(arguments)
            assert payload is None
            if arguments[:2] == ["get", "namespace"]:
                name = arguments[2]
                return json.dumps({"kind": "Namespace", "metadata": {"name": name, "uid": (
                    tls.kube_system_uid if name == "kube-system" else self.namespace_uid)}}).encode()
            if arguments[:2] == ["config", "view"]:
                return json.dumps(self.view).encode()
            if arguments[:3] == ["get", "configmap", "loom-platform-config"]:
                return json.dumps(self.config).encode()
            raise AssertionError(arguments)

    return Live(), config


def test_foundation_uses_fresh_config_and_normalizes_only_own_cutover_flag(live):
    api, config = live
    first = api.foundation()
    assert first.platform_config == {**config, "shared_ingress_enabled": False}
    changed = {**config, "shared_ingress_enabled": True, "service_execution_scheduler_max_deadline_sec": 14400}
    api.config["data"]["environment.json"] = json.dumps(changed)
    second = api.foundation()
    assert second.platform_config == {**changed, "shared_ingress_enabled": False}
    assert first.platform_config != second.platform_config
    assert second.ingress_namespace == config["namespace"]
    assert second.public_dns_zone == "dev.example.test"
    assert not any(call[0] in {"create", "patch", "apply", "exec"} for call in api.calls)


@pytest.mark.parametrize("drift", ["candidate", "cluster", "namespace", "server", "view-server", "view-name", "view-no-ca", "view-insecure", "namespace-uid", "invalid-config", "config-name"])
def test_foundation_rejects_untrusted_or_mismatched_live_binding_before_writes(live, drift):
    api, config = live
    if drift == "candidate":
        api.config["data"]["profile.json"] = json.dumps({"candidate_sha": "f" * 40})
    elif drift == "cluster":
        config["cluster_id"] = "foreign"
    elif drift == "namespace":
        config["namespace"] = "loom-nebius-foreign"
    elif drift == "server":
        config["kubernetes_api_server"] = "https://foreign.example.test"
    elif drift == "view-server":
        api.view["clusters"][0]["cluster"]["server"] = "https://foreign.example.test"
    elif drift == "view-name":
        api.view["clusters"][0]["name"] = "foreign"
    elif drift == "view-no-ca":
        del api.view["clusters"][0]["cluster"]["certificate-authority-data"]
    elif drift == "view-insecure":
        api.view["clusters"][0]["cluster"]["insecure-skip-tls-verify"] = True
    elif drift == "namespace-uid":
        api.namespace_uid = str(uuid4())
    elif drift == "invalid-config":
        config["capacity_policy"]["max_nodes"] = 0
    else:
        api.config["metadata"]["name"] = "foreign"
    api.config["data"]["environment.json"] = json.dumps(config)
    with pytest.raises(module().OperationError):
        api.foundation()
    assert not any(call[0] in {"create", "patch", "apply", "exec"} for call in api.calls)


def test_image_must_match_fixed_qualified_manifest_in_live_region(live):
    api, _ = live
    api.image = "cr.eu-north1.nebius.cloud/test/loom-shared-ingress@sha256:" + "a" * 64
    with pytest.raises(module().OperationError):
        api.foundation()
