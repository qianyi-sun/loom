"""Actual API cutover CAS/allocation proof; TLS and DB guard have separate lanes.

Only readiness/probes/guard responses are adapted here. JSON Patch transport,
cluster/namespace identities, full before/after readback and journal replay are
real. This is not full installed or end-to-end acceptance.
"""
from __future__ import annotations

import copy
import json
import os
import shutil
from pathlib import Path
from uuid import uuid4

import pytest
from scripts.ops.nebius_ingress_cutover import CutoverError, cutover, rollback
from scripts.ops.nebius_ingress_gateway import TLSBinding
from scripts.ops.nebius_ingress_operation import LiveIngressAPI

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="requires an explicitly disposable Kubernetes API")


@pytest.mark.timeout(180)
def test_real_api_cutover_preserves_allocation_and_rejects_stale_versions(tmp_path):
    from kubernetes import client

    container = _start_k3s(ephemeral_storage_floor="2Gi")
    try:
        _, core, _ = _load_client(container)
        namespace = "loom-nebius-cutover"
        ns = core.create_namespace(client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace)))
        public = core.create_namespaced_service(namespace, {
            "apiVersion": "v1", "kind": "Service", "metadata": {"name": "loom-web"},
            "spec": {"type": "LoadBalancer", "selector": {"app": "loom-web"},
                     "ports": [{"port": 443, "targetPort": 8443}]},
        })
        core.patch_namespaced_service_status("loom-web", namespace, {
            "status": {"loadBalancer": {"ingress": [{"ip": "192.0.2.12"}]}},
        })
        core.create_namespaced_config_map(namespace, {
            "apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "loom-platform-config"},
            "data": {"environment.json": '{"shared_ingress_enabled":false,"untouched":1}',
                     "profile.json": json.dumps({"candidate_sha": "a" * 40}), "other": "retained"},
        })
        binding = TLSBinding(str(uuid4()), str(uuid4()), namespace, ns.metadata.uid,
                             core.read_namespace("kube-system").metadata.uid, "dev.example.test", "management.example.test")
        raw = container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"])
        assert raw.exit_code == 0
        kubeconfig = tmp_path / "kubeconfig"
        endpoint = f"https://127.0.0.1:{container.get_exposed_port(6443)}"
        kubeconfig.write_text(raw.output.decode().replace("https://127.0.0.1:6443", endpoint))
        kubeconfig.chmod(0o600)
        executable = shutil.which("kubectl")
        assert executable is not None

        class API(LiveIngressAPI):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.owner = None
                self.transitions = []
                self.interrupt_after_config = True

            def qualify(self):
                self.verify_identity(self.binding)
                if self.interrupt_after_config and json.loads(self.read()[1]["data"]["environment.json"])["shared_ingress_enabled"]:
                    raise RuntimeError("simulated interruption before guard release")

            def public_probe(self):
                self.verify_identity(self.binding)

            def probe_original_backend(self):
                self.verify_identity(self.binding)

            def probe_original_public(self):
                assert self.read()[0]["spec"]["selector"] == {"app": "loom-web"}

            def guard(self, action, owner, candidate):
                assert candidate == self.candidate
                if action == "observe":
                    return {"status": "open" if self.owner is None else "held" if self.owner == owner else "skipped_locked"}
                self.transitions.append(action)
                if action == "acquire":
                    assert self.owner is None
                    self.owner = owner
                    return {"status": "acquired"}
                assert action == "release" and self.owner == owner
                self.owner = None
                return {"status": "released"}

        api = API(kubeconfig, binding=binding, executable=Path(executable), candidate="a" * 40,
                  cluster_id="disposable", api_server=endpoint, ingress_class="loom-shared", image="unused-by-CAS-test")
        before, _ = api.read()
        desired = copy.deepcopy(before)
        desired["spec"]["selector"] = {"app": "loom-shared-ingress"}
        desired["metadata"].setdefault("annotations", {})["loom.nebius/ingress-cutover-id"] = str(uuid4())
        core.patch_namespaced_service("loom-web", namespace, {"metadata": {"annotations": {"concurrent": "preserve"}}})
        with pytest.raises(CutoverError):
            api.patch(before, desired)
        assert core.read_namespaced_service("loom-web", namespace).spec.selector == {"app": "loom-web"}
        args = {"api": api, "state_dir": tmp_path / "cutover", "installation_id": binding.installation_id,
                "candidate": api.candidate, "namespace": namespace}
        with pytest.raises(CutoverError):
            cutover(**args)
        assert api.owner is not None
        assert rollback(**args)["status"] == "rolled_back"
        assert core.read_namespaced_service("loom-web", namespace).spec.selector == {"app": "loom-web"}
        restored = core.read_namespaced_config_map("loom-platform-config", namespace)
        assert json.loads(restored.data["environment.json"]) == {"shared_ingress_enabled": False, "untouched": 1}
        assert api.owner is None and api.transitions == ["acquire", "release"]
        api.interrupt_after_config = False
        args["state_dir"] = tmp_path / "second-cutover"
        result = cutover(**args)
        assert result["status"] == "complete" and cutover(**args) == result
        after = core.read_namespaced_service("loom-web", namespace)
        assert after.metadata.uid == public.metadata.uid
        assert after.spec.cluster_ip == public.spec.cluster_ip
        assert after.spec.ports == public.spec.ports
        assert after.status.load_balancer.ingress[0].ip == "192.0.2.12"
        assert after.metadata.annotations["concurrent"] == "preserve"
        assert after.spec.selector == {"app": "loom-shared-ingress"}
        config = core.read_namespaced_config_map("loom-platform-config", namespace)
        assert json.loads(config.data["environment.json"]) == {"shared_ingress_enabled": True, "untouched": 1}
        assert config.data["other"] == "retained" and api.transitions == ["acquire", "release", "acquire", "release"]
    finally:
        container.stop()
