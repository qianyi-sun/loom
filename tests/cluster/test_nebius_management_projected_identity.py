"""Actual short-lived Kubernetes identity stays within its namespace binding."""
from __future__ import annotations

import base64
import os

import httpx
import pytest
import yaml

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="requires explicitly disposable Kubernetes")


@pytest.mark.timeout(180)
async def test_native_management_token_rotation_preserves_actual_namespace_rbac(tmp_path):
    from kubernetes import client

    from loom_service.environment_management.kubernetes_credentials import (
        ProjectedKubernetesConnection,
        ProjectedKubernetesCredentials,
    )
    from loom_service.environment_management.runtime import NebiusManagementAuth

    container = _start_k3s(ephemeral_storage_floor="2Gi")
    try:
        _, core, _ = _load_client(container)
        admin = client.RbacAuthorizationV1Api(core.api_client)
        for namespace in ("manager", "owned-child", "foreign-child"):
            core.create_namespace(client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace)))
        for namespace in ("owned-child", "foreign-child"):
            core.create_namespaced_secret(namespace, client.V1Secret(
                metadata=client.V1ObjectMeta(name="proof"), string_data={"value": namespace},
            ))
        for account in ("provisioner", "unprivileged"):
            core.create_namespaced_service_account("manager", client.V1ServiceAccount(
                metadata=client.V1ObjectMeta(name=account), automount_service_account_token=False,
            ))
        admin.create_namespaced_role("owned-child", client.V1Role(
            metadata=client.V1ObjectMeta(name="read-proof"), rules=[client.V1PolicyRule(
                api_groups=[""], resources=["secrets"], resource_names=["proof"], verbs=["get"],
            )],
        ))
        admin.create_namespaced_role_binding("owned-child", client.V1RoleBinding(
            metadata=client.V1ObjectMeta(name="provisioner"),
            role_ref=client.V1RoleRef(api_group="rbac.authorization.k8s.io", kind="Role", name="read-proof"),
            subjects=[client.RbacV1Subject(kind="ServiceAccount", name="provisioner", namespace="manager")],
        ))
        config = yaml.safe_load(container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"]).output)
        ca, token = tmp_path / "ca.crt", tmp_path / "token"
        ca.write_bytes(base64.b64decode(config["clusters"][0]["cluster"]["certificate-authority-data"]))

        def issue(account):
            generation = tmp_path / (account + "-token")
            generation.write_text(core.create_namespaced_service_account_token(account, "manager", client.AuthenticationV1TokenRequest(
                spec=client.V1TokenRequestSpec(audiences=[], expiration_seconds=600),
            )).status.token)
            generation.chmod(0o440)
            pending = tmp_path / "next-token"
            pending.symlink_to(generation)
            pending.replace(token)

        issue("provisioner")
        binding = ProjectedKubernetesConnection(kind="projected_service_account",
            endpoint="https://127.0.0.1:" + str(container.get_exposed_port(6443)), ca_file=ca, token_file=token)
        credentials = ProjectedKubernetesCredentials(binding)
        try:
            async with httpx.AsyncClient(base_url=binding.endpoint, verify=credentials.ssl_context,
                                         auth=NebiusManagementAuth(binding, credentials), trust_env=False,
                                         timeout=20, follow_redirects=False) as http:
                own = "/api/v1/namespaces/owned-child/secrets/proof"
                response = await http.get(own)
                assert response.status_code == 200
                assert base64.b64decode(response.json()["data"]["value"]) == b"owned-child"
                assert (await http.get("/api/v1/namespaces/foreign-child/secrets/proof")).status_code == 403
                issue("unprivileged")
                assert (await http.get(own)).status_code == 403
        finally:
            await credentials.close()
    finally:
        container.stop()
