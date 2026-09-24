"""Real API authorization/admission fences the manager away from foreign data."""
from __future__ import annotations

import base64
import copy
import os
import ssl
import time

import httpx
import pytest
import yaml

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="requires explicitly disposable Kubernetes")


@pytest.mark.timeout(180)
def test_management_bootstrap_and_owned_permissions_are_enforced_by_actual_api():
    from kubernetes import client

    from loom.nebius_management_authority import (
        ManagementNamespaceAuthority,
        namespace_binding,
        render_namespace_authority,
    )

    authority = ManagementNamespaceAuthority(
        installation_id="30000000-0000-4000-8000-000000000001", namespace="loom-nebius-management",
    )
    container = _start_k3s(ephemeral_storage_floor="2Gi")
    try:
        _, core, _ = _load_client(container)
        rbac = client.RbacAuthorizationV1Api(core.api_client)
        admission = client.AdmissionregistrationV1Api(core.api_client)
        for name in (authority.namespace, "loom-dev-foreign"):
            core.create_namespace({"metadata": {"name": name}})
        core.create_namespaced_secret("loom-dev-foreign", {"metadata": {"name": "private"}, "stringData": {"value": "foreign"}})
        core.create_namespaced_service_account(authority.namespace, {"metadata": {"name": "loom-management-provisioner"}})
        constructors = {
            "ValidatingAdmissionPolicy": admission.create_validating_admission_policy,
            "ValidatingAdmissionPolicyBinding": admission.create_validating_admission_policy_binding,
            "ClusterRole": rbac.create_cluster_role,
            "ClusterRoleBinding": rbac.create_cluster_role_binding,
        }
        policies = []
        for document in render_namespace_authority(authority):
            constructors[document["kind"]](document)
            if document["kind"] == "ValidatingAdmissionPolicy":
                policies.append(document["metadata"]["name"])
        deadline = time.monotonic() + 20
        while True:
            observed = [admission.read_validating_admission_policy(name) for name in policies]
            if all(item.status and item.status.type_checking for item in observed):
                assert all(not item.status.type_checking.expression_warnings for item in observed)
                break
            assert time.monotonic() < deadline, "admission policy was not type checked"
            time.sleep(0.1)
        token = core.create_namespaced_service_account_token("loom-management-provisioner", authority.namespace,
            client.AuthenticationV1TokenRequest(spec=client.V1TokenRequestSpec(audiences=[], expiration_seconds=600))).status.token
        config = yaml.safe_load(container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"]).output)
        trust = ssl.create_default_context(cadata=base64.b64decode(
            config["clusters"][0]["cluster"]["certificate-authority-data"]).decode())
        endpoint = "https://127.0.0.1:" + str(container.get_exposed_port(6443))
        with httpx.Client(base_url=endpoint, verify=trust, headers={"Authorization": "Bearer " + token},
                          trust_env=False, follow_redirects=False, timeout=20) as http:
            own = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "loom-dev-alice", "labels": {
                "loom.nebius/namespace-installation": str(authority.installation_id),
                "loom.nebius/environment-id": "10000000-0000-4000-8000-000000000001",
                "loom.nebius/incarnation": "20000000-0000-4000-8000-000000000001",
                "pod-security.kubernetes.io/enforce": "restricted",
            }}}
            # Negative dry-run establishes policy-cache readiness without ever
            # creating an invalid namespace during admission propagation.
            invalid = copy.deepcopy(own)
            invalid["metadata"]["name"] = "foreign-created"
            deadline = time.monotonic() + 20
            while True:
                result = http.post("/api/v1/namespaces?dryRun=All", json=invalid)
                if result.status_code == 403 and "management namespace boundary" in result.text:
                    break
                assert result.status_code in (201, 403), result.text
                assert time.monotonic() < deadline, "namespace admission did not become effective"
                time.sleep(0.1)
            for key, value in (("loom.nebius/namespace-installation", "foreign"),
                               ("pod-security.kubernetes.io/enforce", "privileged"),
                               ("loom.nebius/environment-id", "invalid"),
                               ("loom.nebius/incarnation", "00000000-0000-0000-0000-000000000000")):
                invalid = copy.deepcopy(own)
                invalid["metadata"]["labels"][key] = value
                assert http.post("/api/v1/namespaces", json=invalid).status_code == 403
            invalid = copy.deepcopy(own)
            del invalid["metadata"]["labels"]
            assert http.post("/api/v1/namespaces", json=invalid).status_code == 403
            assert http.post("/api/v1/namespaces", json=own).status_code == 201
            bindings = "/apis/rbac.authorization.k8s.io/v1/namespaces/loom-dev-alice/rolebindings"
            binding = namespace_binding(authority, "loom-dev-alice")
            foreign = namespace_binding(authority, "loom-dev-foreign")
            assert http.post(bindings.replace("loom-dev-alice", "loom-dev-foreign"), json=foreign).status_code == 403
            for field in ("subject", "role"):
                invalid = copy.deepcopy(binding)
                if field == "subject":
                    invalid["subjects"][0]["namespace"] = "loom-dev-alice"
                else:
                    invalid["roleRef"]["name"] = "cluster-admin"
                denied = http.post(bindings, json=invalid)
                assert denied.status_code == 403
                if field == "subject":
                    assert "management namespace binding boundary" in denied.text
            assert http.post(bindings, json=binding).status_code == 201
            for name in ("loom-run-20000000000040008000000000000001", "loom-run-20000000000040008000000000000001-build"):
                run = copy.deepcopy(own)
                run["metadata"]["name"] = name
                assert http.post("/api/v1/namespaces", json=run).status_code == 201
                assert http.post(bindings.replace("loom-dev-alice", name), json=namespace_binding(authority, name)).status_code == 201
            secrets = "/api/v1/namespaces/loom-dev-alice/secrets"
            assert http.post(secrets, json={"metadata": {"name": "own"}, "stringData": {"value": "own"}}).status_code == 201
            assert http.get(secrets + "/own").status_code == 200
            assert http.get("/api/v1/namespaces/loom-dev-foreign/secrets/private").status_code == 403
            assert http.get("/api/v1/secrets").status_code == 403
            assert http.patch("/api/v1/namespaces/loom-dev-foreign", json={"metadata": {"labels": own["metadata"]["labels"]}},
                              headers={"Content-Type": "application/merge-patch+json"}).status_code == 403
            assert http.delete("/api/v1/namespaces/loom-dev-alice/persistentvolumeclaims/data").status_code == 403
            roles = bindings.replace("rolebindings", "roles")
            role = {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
                "metadata": {"name": "loom-execution-observer", "namespace": "loom-dev-alice"},
                "rules": [{"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["get", "list", "watch"]},
                          {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list", "watch"]}]}
            assert http.post(roles, json=role).status_code == 201
            observer = copy.deepcopy(binding)
            observer["metadata"]["name"] = "loom-execution-observer"
            observer["roleRef"].update(kind="Role", name="loom-execution-observer")
            observer["subjects"] = [{"kind": "ServiceAccount", "namespace": "loom-dev-alice", "name": "loom-execution-actuator"}]
            assert http.post(bindings, json=observer).status_code == 201
            role["metadata"]["name"] = "escalation"
            role["rules"] = [{"apiGroups": ["*"], "resources": ["*"], "verbs": ["*"]}]
            assert http.post(roles, json=role).status_code == 403
            # Retained cleanup patches routing rather than deleting the Ingress.
            ingress_path = "/apis/networking.k8s.io/v1/namespaces/loom-dev-alice/ingresses"
            ingress = {"apiVersion": "networking.k8s.io/v1", "kind": "Ingress",
                "metadata": {"name": "route", "namespace": "loom-dev-alice"},
                "spec": {"defaultBackend": {"service": {"name": "service", "port": {"number": 8090}}}}}
            assert http.post(ingress_path, json=ingress).status_code == 201
            assert http.patch(ingress_path + "/route", json=[{
                "op": "replace", "path": "/spec/defaultBackend/service/name", "value": "loom-retained-alice",
            }], headers={"Content-Type": "application/json-patch+json"}).status_code == 200
            assert http.delete(ingress_path + "/route").status_code == 403
    finally:
        container.stop()
