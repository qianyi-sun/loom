"""Exercise root-policy admission against a disposable API, without model calls."""

from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from loom.nebius_task_identity_policy import identity_namespace_labels, identity_policy_documents
from loom_execution_actuator.renderer import ExecutionTargetRuntime, _sidecar, render_execution_job
from tests.integration.test_execution_actuator_k3s import _lease, _load_client, _start_k3s
from tests.unit.test_service_execution_materialization import _provenance
from tests.unit.test_task_sandbox_identity import _identity_task


def _pod(namespace: str) -> dict:
    from loom.service_execution_materialization import compile_service_execution_plan

    lease = _lease(namespace)
    job = render_execution_job(lease, target=ExecutionTargetRuntime(target_id=lease.target_id, namespace=namespace))
    template = job["spec"]["template"]
    task, trial, profile = _identity_task("root")
    plan = compile_service_execution_plan(
        task=task, trial=trial, profile=profile.model_copy(update={"supports_task_identity": True}),
        source_provenance=_provenance(), task_revision_sha256="sha256:" + "c" * 64,
    )
    template["spec"]["initContainers"].extend(_sidecar(sidecar) for sidecar in plan.sidecars)
    template["spec"]["volumes"].extend(
        {"name": sidecar.role_name + "-socket", "emptyDir": {}} for sidecar in plan.sidecars
    )
    return {"apiVersion": "v1", "kind": "Pod", "metadata": {
        **template["metadata"], "name": "identity-admission", "namespace": namespace,
    }, "spec": template["spec"]}


@pytest.mark.timeout(240)
def test_private_root_policy_accepts_only_the_constrained_pod_shape(tmp_path: Path):
    import json
    import os
    import subprocess
    import time

    if os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1":
        pytest.skip("set LOOM_RUN_DISPOSABLE_K3S=1 for actual isolated admission checks")
    namespace = "loom-identity-policy-test"
    container = _start_k3s()

    def apply(documents, *, dry_run=False):
        path = tmp_path / "policy-probe.yaml"
        path.write_text(yaml.safe_dump_all(documents))
        subprocess.run(["docker", "cp", str(path), container.get_wrapped_container().id + ":/tmp/probe.yaml"],
                       check=True, capture_output=True)
        return container.exec(["kubectl", "apply", "-f", "/tmp/probe.yaml", *(["--dry-run=server"] if dry_run else [])])

    try:
        _load_client(container)
        result = apply([
            {"apiVersion": "v1", "kind": "Namespace", "metadata": {
                "name": namespace, "labels": {"pod-security.kubernetes.io/enforce": "restricted"},
            }},
            {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {
                "name": "loom-execution-attempt", "namespace": namespace,
            }, "automountServiceAccountToken": False},
        ])
        assert result.exit_code == 0, result.output.decode()
        pod = _pod(namespace)
        assert apply([pod], dry_run=True).exit_code != 0  # Existing default refuses root.
        policies = identity_policy_documents(namespace, "disposable-k3s")
        from scripts.ops.deploy_nebius_platform import Kubectl, install_task_identity_policy

        kubeconfig = tmp_path / "kubeconfig"
        kubeconfig.write_text(container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"]).output.decode().replace(
            "https://127.0.0.1:6443", f"https://127.0.0.1:{container.get_exposed_port(6443)}",
        ))
        kubeconfig.chmod(0o600)
        (tmp_path / "00-task-identity-policy.yaml").write_text(yaml.safe_dump_all(policies))
        install_task_identity_policy(Kubectl(kubeconfig), {"execution_namespace": namespace}, tmp_path)
        assert apply([pod], dry_run=True).exit_code != 0  # Policy setup retains restricted PSS.
        result = apply([{"apiVersion": "v1", "kind": "Namespace", "metadata": {
            "name": namespace, "labels": identity_namespace_labels(),
        }}])
        assert result.exit_code == 0, result.output.decode()
        for _ in range(60):
            result = apply([pod], dry_run=True)
            if result.exit_code == 0:
                break
            time.sleep(0.2)
        assert result.exit_code == 0, result.output.decode()
        result = container.exec(["kubectl", "get", "validatingadmissionpolicy", policies[0]["metadata"]["name"], "-o", "json"])
        assert not json.loads(result.output).get("status", {}).get("typeChecking", {}).get("expressionWarnings")
        nonroot = deepcopy(pod)
        for sandbox in nonroot["spec"]["initContainers"][1:]:
            sandbox["securityContext"].update(runAsNonRoot=True)
            sandbox["securityContext"].pop("runAsUser")
            sandbox["securityContext"].pop("runAsGroup")
            sandbox["securityContext"]["capabilities"].pop("add")
        assert apply([nonroot], dry_run=True).exit_code == 0
        mutations = [
            lambda p: p["spec"]["containers"][0]["securityContext"].update(runAsUser=0, runAsNonRoot=False),
            lambda p: p["spec"]["initContainers"][1]["securityContext"]["capabilities"]["add"].append("SYS_ADMIN"),
            lambda p: p["spec"]["initContainers"][1]["securityContext"].update(allowPrivilegeEscalation=True),
            lambda p: p["spec"]["initContainers"][1]["volumeMounts"][0].update(name="workspace"),
            lambda p: p["spec"]["initContainers"][1].update(envFrom=[{"secretRef": {"name": "controller-token"}}]),
            lambda p: p["spec"].update(hostPID=True),
            lambda p: p["spec"].update(shareProcessNamespace=True),
            lambda p: p["spec"]["initContainers"][1].update(command=["/bin/sh"]),
            lambda p: p["spec"]["initContainers"][0]["securityContext"]["capabilities"].update(add=["NET_BIND_SERVICE"]),
            lambda p: p["metadata"]["annotations"].update({"loom.openai.com/target-id": "other-target"}),
            lambda p: p["spec"]["volumes"][0].update(emptyDir=None, hostPath={"path": "/"}),
        ]
        for mutate in mutations:
            bad = deepcopy(pod)
            mutate(bad)
            result = apply([bad], dry_run=True)
            assert result.exit_code != 0, "unsafe variation was admitted"
        # A policy bound to this namespace must not affect another namespace.
        result = apply([{"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace + "-other"}}])
        assert result.exit_code == 0
        other = deepcopy(pod)
        other["metadata"]["namespace"] = namespace + "-other"
        other["spec"]["serviceAccountName"] = "default"
        other["spec"]["containers"][0]["securityContext"].update(runAsUser=0, runAsNonRoot=False)
        assert apply([other], dry_run=True).exit_code == 0

        # Ephemeral containers are admitted on a separate subresource. Park a
        # Pod on a nonexistent node so this test never pulls an image or runs it.
        parked = deepcopy(pod)
        parked["spec"]["nodeName"] = "loom-admission-nonexistent-node"
        assert apply([parked]).exit_code == 0
        from kubernetes import client
        from kubernetes.client.exceptions import ApiException

        core = client.CoreV1Api()
        ephemeral = {"name": "diagnostic", "image": "invalid.local/admission-only:unused", "securityContext": {
            "runAsNonRoot": True, "allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]},
        }}
        core.patch_namespaced_pod_ephemeralcontainers(
            parked["metadata"]["name"], namespace, {"spec": {"ephemeralContainers": [ephemeral]}}, dry_run="All",
        )
        ephemeral["securityContext"].update(runAsUser=0, runAsNonRoot=False)
        with pytest.raises(ApiException) as rejected:
            core.patch_namespaced_pod_ephemeralcontainers(
                parked["metadata"]["name"], namespace, {"spec": {"ephemeralContainers": [ephemeral]}}, dry_run="All",
            )
        assert rejected.value.status in {403, 422}
        assert "private-root-v1" in rejected.value.body
    finally:
        container.stop()
