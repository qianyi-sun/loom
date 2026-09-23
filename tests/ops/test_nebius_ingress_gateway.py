"""TLS publication owns exact immutable Secrets; it never cuts public traffic."""
from __future__ import annotations

import copy
import importlib
import json
import subprocess
import sys
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
        assert argv[:6] == ["/usr/bin/kubectl", "--kubeconfig", str(config), "--request-timeout=30s",
                           "--cache-dir", str(tmp_path / ".loom-ingress-kubectl-cache")]
        assert kwargs["capture_output"] and kwargs["timeout"] == 40
        assert kwargs["env"] == {"PATH": "/bin:/usr/bin", "LANG": "C.UTF-8"}
        if "create" in argv:
            writes.append(json.loads(kwargs["input"]))
            return subprocess.CompletedProcess(argv, 0, b"secret/fixture", b"")
        name = argv[8]
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
        assert (tmp_path / ".loom-ingress-kubectl-cache").stat().st_mode & 0o077 == 0
    else:
        with pytest.raises(module().IngressError) as error:
            api.create_secret(document)
        assert not writes and "private-api-error" not in str(error.value)


@pytest.fixture
def controller(inputs):
    receipt = deliver(inputs)
    binding, api = inputs[1], inputs[2]
    image = "cr.eu-north1.nebius.cloud/test/traefik@sha256:" + "d" * 64
    deployment_uid, rs_uid, pod_uid = (str(uuid4()) for _ in range(3))
    spec = {"containers": [{"name": "main", "image": image}],
            "volumes": [{"name": "tls", "secret": {"secretName": receipt["secret_name"]}}]}
    deployment = {"metadata": {"name": "loom-shared-ingress", "namespace": binding.namespace,
                                "uid": deployment_uid, "generation": 3,
                                "labels": {"loom.nebius/ingress-installation-id": binding.installation_id}},
                  "spec": {"replicas": 1, "selector": {"matchLabels": {"app": "loom-shared-ingress"}},
                           "template": {"spec": copy.deepcopy(spec)}},
                  "status": {"observedGeneration": 3, "replicas": 1, "updatedReplicas": 1,
                             "readyReplicas": 1, "availableReplicas": 1}}
    replicas = [{"metadata": {"uid": rs_uid, "ownerReferences": [
        {"kind": "Deployment", "uid": deployment_uid, "controller": True}]}}]
    pods = [{"metadata": {"name": "ingress-current", "namespace": binding.namespace, "uid": pod_uid,
                         "resourceVersion": "pod-v1",
                         "labels": {"app": "loom-shared-ingress"}, "ownerReferences": [
                             {"kind": "ReplicaSet", "uid": rs_uid, "controller": True}]},
             "spec": copy.deepcopy(spec), "status": {"phase": "Running", "conditions": [
                 {"type": "Ready", "status": "True"}]}}]
    probes = []
    api.get_deployment = lambda namespace, name: copy.deepcopy(deployment)
    api.list_controller_pods = lambda namespace: copy.deepcopy(pods)
    api.list_controller_replicasets = lambda namespace: copy.deepcopy(replicas)
    api.get_pod = lambda namespace, name: copy.deepcopy(pods[0])

    def probe(namespace, name, uid, server_name):
        assert (namespace, name, uid, server_name) == (
            binding.namespace, "ingress-current", pod_uid, binding.management_host)
        probes.append(uid)
        return receipt["fingerprint_sha256"]

    api.probe_tls = probe
    arguments = {"binding": binding, "api": api, "deployment_uid": deployment_uid,
                 "image": image, "tls_receipt": receipt}
    return arguments, deployment, replicas, pods, probes


def test_controller_qualification_proves_the_current_pod_certificate(controller):
    arguments, deployment, _replicas, pods, probes = controller
    result = module().qualify_controller(**arguments)
    assert result["status"] == "controller_qualified"
    assert result["deployment_uid"] == deployment["metadata"]["uid"]
    assert result["pod_uids"] == probes == [pods[0]["metadata"]["uid"]]
    assert result["fingerprint_sha256"] == arguments["tls_receipt"]["fingerprint_sha256"]


@pytest.mark.parametrize("case", ["stale-controller", "stale-status", "old-pod", "wrong-owner",
                                  "wrong-image", "wrong-secret", "not-ready", "tls-mismatch", "pod-recreated"])
def test_controller_qualification_rejects_stale_mixed_or_unverified_pods(controller, case):
    arguments, deployment, replicas, pods, probes = controller
    api = arguments["api"]
    if case == "stale-controller":
        deployment["metadata"]["uid"] = str(uuid4())
    elif case == "stale-status":
        deployment["status"]["observedGeneration"] = 2
    elif case == "old-pod":
        pods.append(copy.deepcopy(pods[0]))
        pods[-1]["metadata"]["uid"] = str(uuid4())
        pods[-1]["metadata"]["deletionTimestamp"] = NOW.isoformat()
    elif case == "wrong-owner":
        replicas[0]["metadata"]["ownerReferences"][0]["uid"] = str(uuid4())
    elif case == "wrong-image":
        pods[0]["spec"]["containers"][0]["image"] = "foreign:latest"
    elif case == "wrong-secret":
        pods[0]["spec"]["volumes"][0]["secret"]["secretName"] = "previous-generation"
    elif case == "not-ready":
        pods[0]["status"]["conditions"][0]["status"] = "False"
    elif case == "tls-mismatch":
        api.probe_tls = lambda *args: "0" * 64
    else:
        api.get_pod = lambda *args: {"metadata": {"uid": str(uuid4())}}
    with pytest.raises(module().IngressError):
        module().qualify_controller(**arguments)
    if case not in {"tls-mismatch", "pod-recreated"}:
        assert not probes


def test_new_selected_pod_during_tls_probe_prevents_qualification(controller):
    arguments, _deployment, _replicas, pods, _probes = controller
    api = arguments["api"]
    original_probe = api.probe_tls

    def probe(*args):
        result = original_probe(*args)
        extra = copy.deepcopy(pods[0])
        extra["metadata"]["name"] = "unqualified-new-pod"
        extra["metadata"]["uid"] = str(uuid4())
        pods.append(extra)
        return result

    api.probe_tls = probe
    with pytest.raises(module().IngressError):
        module().qualify_controller(**arguments)


@pytest.mark.parametrize("change", ["uid", "owner"])
def test_secret_identity_must_remain_owned_during_tls_probe(controller, change):
    arguments, _deployment, _replicas, _pods, _probes = controller
    api = arguments["api"]
    original_probe = api.probe_tls

    def probe(*args):
        result = original_probe(*args)
        metadata = api.secrets[arguments["tls_receipt"]["secret_name"]]["metadata"]
        if change == "uid":
            metadata["uid"] = str(uuid4())
        else:
            metadata["labels"] = {}
        return result

    api.probe_tls = probe
    with pytest.raises(module().IngressError):
        module().qualify_controller(**arguments)


@pytest.mark.parametrize("case", ["matching", "api-error", "foreign-owner", "foreign-namespace",
                                  "foreign-name", "missing-version", "deleting", "duplicate-tls"])
def test_tls_switch_is_one_exact_identity_conditioned_patch(inputs, controller, tmp_path, monkeypatch, case):
    arguments, deployment, _replicas, _pods, _probes = controller
    binding = inputs[1]
    deployment["metadata"].update(resourceVersion="version-before", name="loom-shared-ingress")
    if case == "foreign-owner":
        deployment["metadata"]["labels"]["loom.nebius/ingress-installation-id"] = str(uuid4())
    elif case == "foreign-namespace":
        deployment["metadata"]["namespace"] = "another-owner"
    elif case == "foreign-name":
        deployment["metadata"]["name"] = "other-controller"
    elif case == "missing-version":
        del deployment["metadata"]["resourceVersion"]
    elif case == "deleting":
        deployment["metadata"]["deletionTimestamp"] = NOW.isoformat()
    elif case == "duplicate-tls":
        deployment["spec"]["template"]["spec"]["volumes"].append(
            copy.deepcopy(deployment["spec"]["template"]["spec"]["volumes"][0]))
    kubeconfig = tmp_path / "switch-kubeconfig"
    kubeconfig.write_text("fixture")
    kubeconfig.chmod(0o600)
    api = module().KubectlControllerAPI(kubeconfig, binding=binding, executable=Path("/usr/bin/kubectl"))
    calls = []
    verified = []
    monkeypatch.setattr(api, "verify_identity", lambda value: verified.append(value))

    def execute(args, *, payload=None):
        calls.append((args, json.loads(payload)))
        if case == "api-error":
            raise module().IngressError("protected Kubernetes outcome unavailable")
        return b"deployment.apps/loom-shared-ingress"

    monkeypatch.setattr(api, "_run", execute)
    before = copy.deepcopy(deployment)
    if case == "matching":
        api.switch_controller_tls(deployment, "loom-ingress-tls-next")
        assert verified == [binding]
        assert len(calls) == 1
        assert calls[0] == (["patch", "deployment", "loom-shared-ingress", "-n", binding.namespace,
                             "--type=json", "--patch-file=/dev/stdin", "-o", "name"], [
            {"op": "test", "path": "/metadata/uid", "value": arguments["deployment_uid"]},
            {"op": "test", "path": "/metadata/resourceVersion", "value": "version-before"},
            {"op": "test", "path": "/spec/template/spec/volumes/0/name", "value": "tls"},
            {"op": "test", "path": "/spec/template/spec/volumes/0/secret/secretName",
             "value": arguments["tls_receipt"]["secret_name"]},
            {"op": "replace", "path": "/spec/template/spec/volumes/0/secret/secretName", "value": "loom-ingress-tls-next"},
        ])
    else:
        with pytest.raises(module().IngressError):
            api.switch_controller_tls(deployment, "loom-ingress-tls-next")
        assert len(calls) == int(case == "api-error")
    assert deployment == before


@pytest.mark.parametrize("wrong_certificate", [False, True])
def test_real_tls_probe_uses_verified_hostname_and_stops_forwarder(inputs, tmp_path, monkeypatch, wrong_certificate):
    import hashlib
    import ssl

    from cryptography import x509
    from cryptography.hazmat.primitives import serialization

    binding = inputs[1]
    chain, key, roots = material(names=("*.dev.example.test", "foreign.example.test") if wrong_certificate else (
        "*.dev.example.test", "management.example.test"))
    certificate, private = tmp_path / "server.crt", tmp_path / "server.key"
    certificate.write_bytes(chain)
    private.write_bytes(key)
    private.chmod(0o600)
    fixture = tmp_path / "kubectl-fixture"
    fixture.write_text(f'''#!{sys.executable}
import socket, ssl, time
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain({str(certificate)!r}, {str(private)!r})
with socket.socket() as server:
    server.bind(("127.0.0.1", 0)); server.listen()
    print("Forwarding from 127.0.0.1:%d -> 8443" % server.getsockname()[1], flush=True)
    connection, address = server.accept()
    try:
        with context.wrap_socket(connection, server_side=True) as stream:
            stream.recv(1)
    except ssl.SSLError:
        connection.close()
    time.sleep(30)
''')
    fixture.chmod(0o700)
    config = tmp_path / "kubeconfig"
    config.write_text("private fixture configuration")
    config.chmod(0o600)
    context = ssl.create_default_context(cadata=roots[0].public_bytes(serialization.Encoding.PEM).decode())
    monkeypatch.setattr(ssl, "create_default_context", lambda: context)
    api = module().KubectlControllerAPI(config, binding=binding, executable=fixture)
    uid = str(uuid4())
    monkeypatch.setattr(api, "get_pod", lambda namespace, name: {"metadata": {"uid": uid}})
    processes = []
    original = subprocess.Popen

    def start(argv, **kwargs):
        assert argv[6:] == ["port-forward", "--address=127.0.0.1", "-n", binding.namespace, "pod/ingress", ":8443"]
        child = original(argv, **kwargs)
        processes.append(child)
        return child

    monkeypatch.setattr(subprocess, "Popen", start)
    if wrong_certificate:
        with pytest.raises(module().IngressError):
            api.probe_tls(binding.namespace, "ingress", uid, binding.management_host)
    else:
        expected = hashlib.sha256(x509.load_pem_x509_certificate(chain).public_bytes(serialization.Encoding.DER)).hexdigest()
        assert api.probe_tls(binding.namespace, "ingress", uid, binding.management_host) == expected
    assert len(processes) == 1 and processes[0].returncode is not None
