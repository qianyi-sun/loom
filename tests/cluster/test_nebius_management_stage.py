"""Real API defaulting/readback for each fixed management manifest phase."""
from __future__ import annotations

import base64
import os
import ssl

import pytest
import yaml

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_management_render import render
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="requires explicitly disposable Kubernetes")


@pytest.mark.timeout(180)
def test_all_fixed_management_phases_preserve_real_defaulting_and_uids(tmp_path, management_inputs):
    from scripts.ops.nebius_management_bootstrap import (
        BootstrapBinding,
        HTTPSBootstrapAPI,
        bootstrap_management,
    )
    from scripts.ops.nebius_management_material import ManagementBinding
    from scripts.ops.nebius_management_stage import (
        HTTPSManagementStageAPI,
        ManagementStageError,
        management_phase_ready,
        stage_management_resources,
    )

    rendered = render(management_inputs)
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
        bootstrap = BootstrapBinding(management_inputs[0]["installation_id"],
                                     "loom-nebius-management", core.read_namespace("kube-system").metadata.uid)
        with HTTPSBootstrapAPI(binding=bootstrap, api_server=endpoint, ssl_context=trust) as api:
            receipt = bootstrap_management(binding=bootstrap, api=api, state_dir=tmp_path / "bootstrap")
        binding = ManagementBinding(bootstrap.installation_id, bootstrap.namespace,
                                    receipt["namespace_uid"], bootstrap.kube_system_uid)
        for phase in ("10-config-network.yaml", "20-database.yaml", "30-migrate.yaml",
                      "40-services.yaml", "80-backup.yaml", "70-public.yaml"):
            with HTTPSManagementStageAPI(binding=binding, rendered=rendered, phase=phase,
                                         api_server=endpoint, ssl_context=trust) as api:
                arguments = dict(rendered=rendered, phase=phase, binding=binding, api=api, state_dir=tmp_path / phase)
                first = stage_management_resources(**arguments)
                assert stage_management_resources(**arguments) == first
                assert all(first["resource_uids"].values())
                if phase in {"20-database.yaml", "30-migrate.yaml", "40-services.yaml"}:
                    # This disposable API has no integration platform node or
                    # runtime images. Creating resources cannot prove readiness.
                    assert management_phase_ready(**arguments) is False
        service = core.read_namespaced_service("loom-postgres", binding.namespace)
        assert service.spec.cluster_ip and service.spec.cluster_ip != "None"
        core.patch_namespace(binding.namespace, {"metadata": {"labels": {
            "pod-security.kubernetes.io/enforce": "privileged",
        }}})
        with HTTPSManagementStageAPI(binding=binding, rendered=rendered, phase="10-config-network.yaml",
                                     api_server=endpoint, ssl_context=trust) as api:
            with pytest.raises(ManagementStageError):
                stage_management_resources(rendered=rendered, phase="10-config-network.yaml", binding=binding,
                                           api=api, state_dir=tmp_path / "new-state")
        assert core.read_namespace(binding.namespace).metadata.uid == binding.namespace_uid
    finally:
        container.stop()
