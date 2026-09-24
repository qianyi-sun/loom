"""Protected diagnostics reuse real ingress checks without granting writes."""
from __future__ import annotations

import importlib
import json
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest
from tests.ops.test_nebius_ingress_bootstrap import configuration
from tests.ops.test_nebius_ingress_operation import inventory as inventory
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def module():
    return importlib.import_module("scripts.ops.nebius_ingress_preflight")


@pytest.fixture
def wire(tmp_path, platform_inputs, inventory):
    config = configuration(tmp_path)
    platform, _, profile = platform_inputs
    platform["cluster_id"] = "mk8scluster-test"
    config.update(cluster_id=platform["cluster_id"], api_server=platform["kubernetes_api_server"],
                  image="cr.eu-north1.nebius.cloud/registry/loom-shared-ingress@sha256:3429c14149401de2ac82fc72ddc6a92642332b90deb3012301ff211b9d2d0f18")
    config["binding"]["namespace"] = platform["namespace"]
    namespace = platform["namespace"]
    documents = {}
    for name, uid in (("kube-system", config["binding"]["kube_system_uid"]),
                      (namespace, config["binding"]["namespace_uid"])):
        documents[("get", "namespace", name)] = {"apiVersion": "v1", "kind": "Namespace",
            "metadata": {"name": name, "uid": uid, "resourceVersion": "1"}}
    documents[("get", "configmap", "loom-platform-config", "-n", namespace)] = {
        "apiVersion": "v1", "kind": "ConfigMap", "metadata": {
            "name": "loom-platform-config", "namespace": namespace, "uid": str(uuid4()), "resourceVersion": "1"},
        "data": {"environment.json": json.dumps(platform), "profile.json": json.dumps({
            **profile, "candidate_sha": config["candidate"]}), "secret": "private-config"}}
    documents[("config", "view", "--minify", "-o", "json")] = {"clusters": [{
        "name": "nebius-" + platform["cluster_id"], "cluster": {"server": config["api_server"],
            "certificate-authority-data": "private-ca"}}], "users": [{"token": "private-token"}]}
    for name, kind, args in (("nodes", "Node", ("get", "nodes")),
                             ("pods", "Pod", ("get", "pods", "--all-namespaces"))):
        for row in inventory[name]:
            row.update(apiVersion="v1", kind=kind)
        documents[args] = {"apiVersion": "v1", "kind": kind + "List", "metadata": {}, "items": inventory[name]}

    class Wire:
        kubeconfig = Path(config["kubeconfig"])

        def __init__(self):
            self.calls = []
            self.error = None

        def run(self, *args, **kwargs):
            self.calls.append(args)
            if self.error:
                raise self.error
            if args[-3:] == ("--ignore-not-found", "-o", "json"):
                args = args[:-3]
            assert args in documents, "diagnostic attempted an unapproved request"
            value = documents[args]
            return value if isinstance(value, str) else json.dumps(value)

    return Wire(), config, documents, inventory


def inspect(wire):
    kube, config, _, _ = wire
    return module().inspect_ingress(kube, json.dumps(config), namespace=config["binding"]["namespace"],
                                    expected_cluster_id=config["cluster_id"])


def test_real_foundation_and_capacity_checks_are_read_only_and_payload_free(wire):
    result = inspect(wire)
    assert result["status"] == "passed"
    assert result["checks"] == {"foundation": "passed", "capacity": "passed"}
    assert result["unverified"] == ["gateway_local_execution", "certificate_delivery", "staging", "cutover"]
    assert result["source_sha"] == "a" * 40 and result["candidate"] == "b" * 40
    assert "private-" not in json.dumps(result)
    assert all(call[0] in {"get", "config"} for call in wire[0].calls)


@pytest.mark.parametrize("change", ["candidate", "namespace_uid", "server", "foundation"])
def test_foundation_drift_is_localized_before_capacity_reads(wire, change):
    _, config, documents, _ = wire
    if change == "candidate":
        config["candidate"] = "c" * 40
    elif change == "namespace_uid":
        config["binding"]["namespace_uid"] = str(uuid4())
    elif change == "server":
        config["api_server"] = "https://other.example.test"
    else:
        row = documents[("get", "configmap", "loom-platform-config", "-n", config["binding"]["namespace"])]
        platform = json.loads(row["data"]["environment.json"])
        platform["shared_ingress_enabled"] = "private-invalid"
        row["data"]["environment.json"] = json.dumps(platform)
    result = inspect(wire)
    assert result["status"] == "blocked" and result["phase"] == "foundation"
    assert result["checks"] == {}
    assert not any(call[:2] == ("get", "pods") for call in wire[0].calls)
    assert "private-" not in json.dumps(result)


def test_size_limit_failure_is_distinct_from_api_failure_or_insufficient_capacity(wire):
    wire[2][("get", "pods", "--all-namespaces")] = " " * (4 * 1024 * 1024 + 1)
    result = inspect(wire)
    assert result["status"] == "blocked" and result["phase"] == "capacity"
    assert result["reason"] == "response_too_large"
    assert result["checks"] == {"foundation": "passed"}
    assert result["reads"][-1] == {"resource": "pods", "bytes": 4194305, "status": "response_too_large"}


def test_full_capacity_accounting_reports_exhaustion_without_payload(wire):
    wire[3]["pods"][0]["spec"]["containers"][0]["resources"]["requests"]["cpu"] = "900m"
    result = inspect(wire)
    assert result["phase"] == "capacity" and result["reason"] == "insufficient_capacity"


def test_incomplete_inventory_does_not_become_insufficient_capacity(wire):
    wire[2][("get", "pods", "--all-namespaces")]["metadata"]["continue"] = "private-next-page"
    result = inspect(wire)
    assert result["phase"] == "capacity" and result["reason"] == "inventory_unqualified"
    assert "private-" not in json.dumps(result)


def test_transport_failure_drops_all_exception_details(wire):
    wire[0].error = RuntimeError("private-provider-token")
    result = inspect(wire)
    assert result["phase"] == "foundation" and result["reason"] == "read_failed"
    assert "private-" not in json.dumps(result)


@pytest.mark.parametrize("raw", ["", "null", "{", "[]", '{"private":"credential"}'])
def test_absent_or_invalid_binding_never_queries_cluster(wire, raw):
    kube, config, _, _ = wire
    result = module().inspect_ingress(kube, raw, namespace=config["binding"]["namespace"],
                                     expected_cluster_id=config["cluster_id"])
    assert result["status"] == ("not_configured" if not raw else "blocked")
    assert kube.calls == []
    assert "credential" not in json.dumps(result)


@pytest.mark.parametrize("args,payload", [(["get", "secret", "private"], None),
    (["get", "pods", "-n", "foreign"], None), (["apply", "-f", "-"], b"private"),
    (["get", "nodes", "--ignore-not-found", "-o", "json"], b"private")])
def test_diagnostic_transport_rejects_non_fixed_reads_and_every_payload(wire, args, payload):
    kube, config, _, _ = wire
    adapter = module().ReadOnlyIngressAPI(kube, config)
    with pytest.raises(RuntimeError):
        adapter._run(args, payload=payload)
    assert kube.calls == []


def test_diagnostic_preserves_wire_size_at_gateway_limit(wire, monkeypatch):
    from scripts.ops.deploy_nebius_platform import Kubectl

    _, config, _, _ = wire
    monkeypatch.delenv("LOOM_DEPLOY_SSH_TARGET", raising=False)
    raw = "x" * (4 * 1024 * 1024) + "\n"
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: subprocess.CompletedProcess(a, 0, raw, ""))
    adapter = module().ReadOnlyIngressAPI(Kubectl(Path(config["kubeconfig"])), config)
    with pytest.raises(RuntimeError):
        adapter._get(["get", "pods", "--all-namespaces"])
    assert adapter.failure == "response_too_large"
    assert adapter.reads[-1]["bytes"] == 4194305
