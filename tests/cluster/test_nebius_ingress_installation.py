"""Qualify private TLS publication against an explicitly disposable API."""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest
from scripts.ops.nebius_certificates import _bind_installation
from scripts.ops.nebius_ingress_gateway import IngressError, KubectlTLSAPI, TLSBinding, deliver_tls

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.ops.test_nebius_certificates import NOW, installation, material, publish

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="requires an explicitly disposable Kubernetes API")


@pytest.mark.timeout(150)
def test_immutable_tls_delivery_replays_and_rotates_without_replacing_old_secret(tmp_path):
    from kubernetes import client

    container = _start_k3s(ephemeral_storage_floor="2Gi")
    try:
        _, core, _ = _load_client(container)
        namespace = "loom-tls-" + uuid4().hex[:8]
        ns = core.create_namespace(client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace)))
        config = json.loads(installation(tmp_path).read_text())
        root = Path(config["state_dir"])
        chain, key, roots = material()
        publish(root, chain, key, roots)
        _bind_installation(root, config)
        binding = TLSBinding(
            installation_id=str(uuid4()), certificate_installation_id=config["installation_id"],
            namespace=namespace, namespace_uid=ns.metadata.uid,
            kube_system_uid=core.read_namespace("kube-system").metadata.uid,
            child_domain=config["child_domain"], management_host=config["management_host"],
        )
        # Only execution is adapted for the container's private kubectl; real
        # production namespace binding, JSON readback and Secret creation run.
        class ContainerAPI(KubectlTLSAPI):
            def _run(self, arguments, *, payload=None):
                result = subprocess.run(
                    ["docker", "exec", "-i", container.get_wrapped_container().id,
                     "kubectl", "--request-timeout=30s", *arguments],
                    input=payload, capture_output=True, timeout=40, check=False,
                )
                if result.returncode:
                    raise IngressError("disposable Kubernetes operation failed")
                return result.stdout

        # Adapter validates private input ownership; container uses its own config.
        kubeconfig = tmp_path / "private-kubeconfig"
        kubeconfig.write_bytes(b"disposable container owns actual configuration")
        kubeconfig.chmod(0o600)
        api = ContainerAPI(kubeconfig, binding=binding, executable=Path("/bin/kubectl"))
        first = deliver_tls(config, binding=binding, api=api, roots=roots, now=NOW)
        assert deliver_tls(config, binding=binding, api=api, roots=roots, now=NOW) == first
        old = core.read_namespaced_secret(first["secret_name"], namespace)
        assert old.immutable and old.metadata.uid == first["secret_uid"]
        with pytest.raises(client.ApiException) as error:
            core.patch_namespaced_secret(first["secret_name"], namespace, {"data": {"tls.key": "Zm9yZWlnbg=="}})
        assert error.value.status == 422
        new_chain, new_key, new_roots = material()
        publish(root, new_chain, new_key, new_roots)
        second = deliver_tls(config, binding=binding, api=api, roots=new_roots, now=NOW)
        assert second["secret_uid"] != first["secret_uid"]
        assert core.read_namespaced_secret(first["secret_name"], namespace).data == old.data
        assert len(core.list_namespaced_secret(namespace).items) == 2
        core.delete_namespaced_secret(second["secret_name"], namespace)
        with pytest.raises(IngressError, match="unresolved"):
            deliver_tls(config, binding=binding, api=api, roots=new_roots, now=NOW)
        assert len(core.list_namespaced_secret(namespace).items) == 1
    finally:
        container.stop()
