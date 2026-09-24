"""Real Namespace defaulting, generated Secret delivery and bootstrap replay."""
from __future__ import annotations

import base64
import json
import os
import ssl
from uuid import uuid4

import pytest
import yaml

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="requires explicitly disposable Kubernetes")


@pytest.mark.timeout(180)
def test_real_namespace_bootstrap_and_replay_preserve_credentials(tmp_path):
    from scripts.ops.nebius_management_bootstrap import (
        BootstrapBinding,
        BootstrapError,
        HTTPSBootstrapAPI,
        bootstrap_management,
    )

    container = _start_k3s(ephemeral_storage_floor="2Gi")
    try:
        _, core, _ = _load_client(container)
        config = yaml.safe_load(container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"]).output)
        endpoint = "https://127.0.0.1:" + str(container.get_exposed_port(6443))
        trust = ssl.create_default_context(cadata=base64.b64decode(
            config["clusters"][0]["cluster"]["certificate-authority-data"]).decode())
        user = config["users"][0]["user"]
        certificate, key = tmp_path / "client.crt", tmp_path / "client.key"
        certificate.write_bytes(base64.b64decode(user["client-certificate-data"]))
        key.write_bytes(base64.b64decode(user["client-key-data"]))
        key.chmod(0o600)
        trust.load_cert_chain(certificate, key)
        binding = BootstrapBinding(str(uuid4()), "loom-nebius-management", core.read_namespace("kube-system").metadata.uid)
        with HTTPSBootstrapAPI(binding=binding, api_server=endpoint, ssl_context=trust) as api:
            state = tmp_path / "bootstrap"
            first = bootstrap_management(binding=binding, api=api, state_dir=state)
            original = (state / "material/material.json").read_bytes()
            assert bootstrap_management(binding=binding, api=api, state_dir=state) == first
            ns = core.read_namespace(binding.namespace)
            assert ns.metadata.uid == first["namespace_uid"]
            assert ns.metadata.labels["pod-security.kubernetes.io/enforce"] == "restricted"
            secrets = core.list_namespaced_secret(binding.namespace).items
            assert len(secrets) == 4 and all(secret.immutable is True for secret in secrets)
            assert {secret.metadata.name: secret.metadata.uid for secret in secrets} == first["secret_uids"]
            core.patch_namespace(binding.namespace, {"metadata": {"labels": {
                "pod-security.kubernetes.io/enforce": "privileged",
            }}})
            with pytest.raises(BootstrapError):
                bootstrap_management(binding=binding, api=api, state_dir=state)
            assert (state / "material/material.json").read_bytes() == original
            assert json.loads((state / "bootstrap.json").read_bytes())["stage"] == "bootstrapped"
    finally:
        container.stop()
