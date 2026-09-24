"""Actual Secret defaulting and ownership for initial management installation."""
from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from uuid import uuid4

import pytest
import yaml

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="requires explicitly disposable Kubernetes")


@pytest.mark.timeout(180)
def test_real_secret_delivery_replay_and_namespace_ownership(tmp_path):
    from kubernetes import client
    from scripts.ops.nebius_management_material import (
        KubectlMaterialAPI,
        ManagementBinding,
        MaterialError,
        deliver_material,
    )

    container = _start_k3s(ephemeral_storage_floor="2Gi")
    try:
        _, core, _ = _load_client(container)
        installation = str(uuid4())
        namespace = "loom-nebius-management"
        ns = core.create_namespace(client.V1Namespace(metadata=client.V1ObjectMeta(
            name=namespace, labels={"loom.nebius/management-installation": installation})))
        context = yaml.safe_load(container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"]).output)
        endpoint = "https://127.0.0.1:" + str(container.get_exposed_port(6443))
        context["clusters"][0]["cluster"]["server"] = endpoint
        kubeconfig = tmp_path / "kubeconfig"
        kubeconfig.write_text(yaml.safe_dump(context))
        kubeconfig.chmod(0o600)
        binding = ManagementBinding(installation, namespace, ns.metadata.uid,
                                    core.read_namespace("kube-system").metadata.uid)
        api = KubectlMaterialAPI(kubeconfig, binding=binding, executable=Path(shutil.which("kubectl")), api_server=endpoint)
        state = tmp_path / "material"
        first = deliver_material(binding=binding, api=api, state_dir=state)
        journal = (state / "material.json").read_bytes()
        second = deliver_material(binding=binding, api=api, state_dir=state)
        assert first == second and (state / "material.json").read_bytes() == journal
        secrets = core.list_namespaced_secret(namespace).items
        assert len(secrets) == 4 and all(secret.immutable is True for secret in secrets)
        assert {secret.metadata.name: secret.metadata.uid for secret in secrets} == first["secret_uids"]
        # Even with the same namespace UID, ownership removal closes delivery.
        core.patch_namespace(namespace, {"metadata": {"labels": {"loom.nebius/management-installation": "foreign"}}})
        with pytest.raises(MaterialError):
            deliver_material(binding=binding, api=api, state_dir=state)
        assert (state / "material.json").read_bytes() == journal
        assert json.loads(journal)["status"] == "delivered"
    finally:
        container.stop()
