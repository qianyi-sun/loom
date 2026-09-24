"""Authority qualification must use the authenticated runtime subject, not operator identity."""
from __future__ import annotations

import json
import ssl
from uuid import uuid4

import httpx
import pytest

from loom.nebius_management_authority import ManagementNamespaceAuthority


@pytest.fixture
def probe():
    from scripts.ops.nebius_management_authority_probe import HTTPSManagementAuthorityProbe

    authority = ManagementNamespaceAuthority(installation_id=uuid4(), namespace="loom-nebius-management")
    uid = str(uuid4())
    return authority, uid, HTTPSManagementAuthorityProbe(authority=authority, service_account_uid=uid,
        api_server="https://cluster.example", ssl_context=ssl.create_default_context(), token="runtime-only-token")


def responder(authority, uid, requests, *, change=None):
    def handle(request):
        requests.append(request)
        doc = json.loads(request.content)
        path = request.url.path
        if path.endswith("selfsubjectreviews"):
            value = {"status": {"userInfo": {"username": "system:serviceaccount:" + authority.namespace + ":loom-management-provisioner",
                "uid": uid, "groups": ["system:serviceaccounts", "system:serviceaccounts:" + authority.namespace, "system:authenticated"]}}}
            if change == "operator":
                value["status"]["userInfo"]["username"] = "operator"
            if change == "replaced_account":
                value["status"]["userInfo"]["uid"] = str(uuid4())
            return httpx.Response(201, json=value)
        if path.endswith("selfsubjectaccessreviews"):
            attrs = doc["spec"]["resourceAttributes"]
            allowed = (attrs["verb"], attrs["resource"]) in {("create", "namespaces"), ("get", "namespaces"), ("create", "rolebindings"), ("bind", "clusterroles")}
            if change == "global_secrets" and attrs["resource"] == "secrets":
                allowed = True
            return httpx.Response(201, json={"status": {"allowed": allowed}})
        assert request.url.params["dryRun"] == "All"
        assert request.method == "POST"
        if path == "/api/v1/namespaces":
            if doc["metadata"]["name"].startswith("loom-dev-"):
                return httpx.Response(201, json=doc)
            message = "management namespace boundary"
            suffix = "namespaces"
        else:
            assert path.endswith("/rolebindings")
            message = "management namespace binding boundary"
            suffix = "bindings"
        if change == "policy_absent":
            return httpx.Response(201, json=doc)
        if change == "unrelated_denial":
            message = "denied by unrelated permission"
        return httpx.Response(403, json={"kind": "Status", "reason": "Forbidden", "code": 403,
            "message": "ValidatingAdmissionPolicy '" + authority.name + "-" + suffix + "': " + message})
    return handle


def test_probe_checks_real_subject_dry_run_admission_and_forbidden_privileges(probe):
    authority, uid, api = probe
    requests = []
    api.client.close()
    api.client = httpx.Client(base_url="https://cluster.example", transport=httpx.MockTransport(responder(authority, uid, requests)))
    with api:
        assert api.qualify() is True
    assert any(r.url.path.endswith("selfsubjectreviews") for r in requests)
    assert any(r.url.path.endswith("selfsubjectaccessreviews") for r in requests)
    assert all(r.url.params.get("dryRun") == "All" for r in requests if r.url.path.endswith(("namespaces", "rolebindings")))


@pytest.mark.parametrize("change", ["operator", "replaced_account", "global_secrets", "unrelated_denial"])
def test_wrong_subject_or_excess_privilege_cannot_qualify(probe, change):
    from scripts.ops.nebius_management_stage import ManagementStageError

    authority, uid, api = probe
    requests = []
    api.client.close()
    api.client = httpx.Client(base_url="https://cluster.example", transport=httpx.MockTransport(responder(authority, uid, requests, change=change)))
    with api, pytest.raises(ManagementStageError):
        api.qualify()
    if change in {"operator", "replaced_account"}:
        assert len(requests) == 1


def test_unpropagated_admission_returns_pending_without_a_persisted_probe(probe):
    authority, uid, api = probe
    requests = []
    api.client.close()
    api.client = httpx.Client(base_url="https://cluster.example", transport=httpx.MockTransport(responder(authority, uid, requests, change="policy_absent")))
    with api:
        assert api.qualify() is False
    assert all(r.url.params.get("dryRun") == "All" for r in requests if r.url.path.endswith(("namespaces", "rolebindings")))
