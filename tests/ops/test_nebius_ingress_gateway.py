"""TLS publication owns exact immutable Secrets; it never cuts public traffic."""
from __future__ import annotations

import copy
import importlib
import json
import subprocess
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from tests.ops.test_nebius_certificates import NOW, installation, material, publish


def module():
    return importlib.import_module("scripts.ops.nebius_ingress_gateway")


class API:
    def __init__(self, binding):
        self.binding = binding
        self.secrets = {}
        self.creates = 0
        self.failure = None
        self.identity_matches = True

    def verify_identity(self, binding):
        assert binding == self.binding
        if not self.identity_matches:
            raise RuntimeError("wrong cluster")

    def get_secret(self, namespace, name):
        assert namespace == self.binding.namespace
        return copy.deepcopy(self.secrets.get(name))

    def create_secret(self, document):
        self.creates += 1
        if self.failure == "before":
            raise TimeoutError("private-payload-must-not-escape")
        self.secrets[document["metadata"]["name"]] = copy.deepcopy(document)
        self.secrets[document["metadata"]["name"]]["metadata"]["uid"] = str(uuid4())
        if self.failure == "after":
            raise TimeoutError("private-payload-must-not-escape")


@pytest.fixture
def inputs(tmp_path):
    config = json.loads(installation(tmp_path).read_text())
    root = tmp_path / "certificate-state"
    chain, key, roots = material()
    selected = publish(root, chain, key, roots)
    from scripts.ops.nebius_certificates import _bind_installation

    _bind_installation(root, config)
    binding = module().TLSBinding(
        installation_id=str(uuid4()), certificate_installation_id=config["installation_id"],
        namespace="loom-platform", namespace_uid=str(uuid4()), kube_system_uid=str(uuid4()),
        child_domain=config["child_domain"], management_host=config["management_host"],
    )
    return config, binding, API(binding), roots, selected, root


def deliver(inputs, **kwargs):
    config, binding, api, roots, _, _ = inputs
    return module().deliver_tls(config, binding=binding, api=api, roots=roots, now=kwargs.pop("now", NOW), **kwargs)


def test_tls_secret_is_immutable_private_and_exactly_replayable(inputs):
    result = deliver(inputs)
    _config, binding, api, _roots, selected, root = inputs
    assert result["certificate_generation"] == selected["generation"]
    assert result["fingerprint_sha256"] == selected["fingerprint_sha256"]
    secret = api.secrets[result["secret_name"]]
    assert secret["type"] == "kubernetes.io/tls" and secret["immutable"] is True
    assert set(secret["data"]) == {"tls.crt", "tls.key"}
    assert secret["metadata"]["namespace"] == binding.namespace
    assert deliver(inputs) == result and api.creates == 1
    assert "PRIVATE KEY" not in json.dumps(result) and "tls.key" not in json.dumps(result)
    receipt = list((root / "deliveries").glob("*.json"))
    assert len(receipt) == 1 and receipt[0].stat().st_mode & 0o077 == 0
    assert json.loads(receipt[0].read_text())["secret_uid"] == secret["metadata"]["uid"]


@pytest.mark.parametrize("change", ["owner", "data", "mutable", "uid", "deleting", "namespace"])
def test_replay_never_overwrites_foreign_changed_or_recreated_secret(inputs, change):
    result = deliver(inputs)
    api = inputs[2]
    secret = api.secrets[result["secret_name"]]
    if change == "owner":
        secret["metadata"]["labels"] = {}
    elif change == "data":
        secret["data"]["tls.key"] = "Zm9yZWlnbg=="
    elif change == "mutable":
        secret["immutable"] = False
    elif change == "uid":
        secret["metadata"]["uid"] = str(uuid4())
    elif change == "namespace":
        secret["metadata"]["namespace"] = "foreign"
    else:
        secret["metadata"]["deletionTimestamp"] = NOW.isoformat()
    before = copy.deepcopy(api.secrets)
    with pytest.raises(module().IngressError):
        deliver(inputs)
    assert api.secrets == before and api.creates == 1


@pytest.mark.parametrize("failure", ["before", "after"])
def test_unknown_create_reply_is_read_back_without_write_retry(inputs, failure):
    api = inputs[2]
    api.failure = failure
    if failure == "after":
        assert deliver(inputs)["status"] == "tls_delivered"
        assert deliver(inputs)["status"] == "tls_delivered"
    else:
        for _ in range(2):
            with pytest.raises(module().IngressError) as error:
                deliver(inputs)
            assert "private-payload" not in str(error.value)
    assert api.creates == 1


@pytest.mark.parametrize("change", ["expired", "selection", "missing", "identity", "config", "symlink"])
def test_invalid_certificate_or_binding_blocks_before_kubernetes_write(inputs, change):
    config, _binding, api, _roots, selected, root = inputs
    kwargs = {}
    if change == "expired":
        kwargs["now"] = NOW + timedelta(days=65)
    elif change == "selection":
        selected["fingerprint_sha256"] = "0" * 64
        (root / "selected.json").write_text(json.dumps(selected))
    elif change == "missing":
        (root / "selected.json").unlink()
    elif change == "identity":
        api.identity_matches = False
    elif change == "config":
        config["installation_id"] = str(uuid4())
    else:
        original = root / "generations" / selected["generation"] / "privkey.pem"
        original.rename(original.with_suffix(".retained"))
        original.symlink_to(original.with_suffix(".retained"))
    with pytest.raises(module().IngressError):
        deliver(inputs, **kwargs)
    assert api.creates == 0


def test_rotation_retains_previous_secret_and_never_changes_selected_certificate(inputs):
    first = deliver(inputs)
    config, binding, api, _roots, selected, root = inputs
    before = copy.deepcopy(api.secrets[first["secret_name"]])
    chain, key, new_roots = material()
    publish(root, chain, key, new_roots)
    second = module().deliver_tls(config, binding=binding, api=api, roots=new_roots, now=NOW)
    assert first["secret_name"] != second["secret_name"]
    assert api.secrets[first["secret_name"]] == before and api.creates == 2
    assert json.loads((root / "selected.json").read_text())["previous_generation"] == selected["generation"]


@pytest.mark.parametrize("missing", ["receipt", "secret"])
def test_lost_tracking_never_adopts_or_recreates_tls_secret(inputs, missing):
    result = deliver(inputs)
    api, root = inputs[2], inputs[5]
    if missing == "receipt":
        next((root / "deliveries").glob("*.json")).unlink()
    else:
        del api.secrets[result["secret_name"]]
    with pytest.raises(module().IngressError):
        deliver(inputs)
    assert api.creates == 1


@pytest.mark.parametrize("case", ["matching", "foreign-cluster", "foreign-namespace", "api-failure"])
def test_kubectl_transport_checks_identity_before_secret_create(inputs, tmp_path, monkeypatch, case):
    binding = inputs[1]
    config = tmp_path / "kubeconfig"
    config.write_text("private-unused-fixture")
    config.chmod(0o600)
    writes = []

    def run(argv, **kwargs):
        assert argv[:4] == ["/usr/bin/kubectl", "--kubeconfig", str(config), "--request-timeout=30s"]
        assert kwargs["capture_output"] and kwargs["timeout"] == 40
        assert kwargs["env"] == {"PATH": "/bin:/usr/bin", "LANG": "C.UTF-8"}
        if "create" in argv:
            writes.append(json.loads(kwargs["input"]))
            return subprocess.CompletedProcess(argv, 0, b"secret/fixture", b"")
        name = argv[6]
        uid = binding.kube_system_uid if name == "kube-system" else binding.namespace_uid
        if case == "foreign-cluster" and name == "kube-system":
            uid = str(uuid4())
        if case == "foreign-namespace" and name == binding.namespace:
            uid = str(uuid4())
        return subprocess.CompletedProcess(argv, int(case == "api-failure"),
            json.dumps({"kind": "Namespace", "metadata": {"name": name, "uid": uid}}).encode(), b"private-api-error")

    monkeypatch.setattr(subprocess, "run", run)
    api = module().KubectlTLSAPI(config, binding=binding, executable=Path("/usr/bin/kubectl"))
    document = {"metadata": {"namespace": binding.namespace}}
    if case == "matching":
        api.create_secret(document)
        assert writes == [document]
    else:
        with pytest.raises(module().IngressError) as error:
            api.create_secret(document)
        assert not writes and "private-api-error" not in str(error.value)
