"""Agent-enabled Kubernetes proofs, separate from API-only admission tests."""

import asyncio
import json
from copy import deepcopy
from uuid import uuid4

import pytest
from testcontainers.core.container import DockerContainer

from loom.dev_instance_runtime import DevInstanceRuntimeError, KubectlClient
from loom.personal_dev_incarnation_storage import personal_dev_secret_name
from loom.personal_dev_storage_secret_write import write_storage_secret
from tests.integration.test_personal_dev_storage_namespace import _K3S, _ContainerKubectl
from tests.integration.test_personal_dev_storage_workload_recovery import _reconcile_workload
from tests.integration.test_personal_dev_storage_workload_write import _namespace, _workload
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim

# Immutable multi-platform manifest already used by the local disposable runtime.
_BUSYBOX = "docker.io/library/busybox@sha256:dc2d74b28e4cf8984fa52af1f39bc7c3d9c73760b41a74d629f5d11b1ab28616"


@pytest.fixture
async def running_storage_kubectl():
    container = DockerContainer(_K3S).with_command([
        "server", "--disable=traefik", "--disable=servicelb", "--disable=metrics-server",
        "--disable=local-storage", "--disable=coredns",
    ]).with_kwargs(privileged=True)
    try:
        await asyncio.to_thread(container.start)
        kubectl = KubectlClient("kubectl", runner=_ContainerKubectl(container.get_wrapped_container().id))
        async with asyncio.timeout(120):
            while True:
                try:
                    reply = await kubectl.runner.run(kubectl._argv("get", "nodes", "-o", "json"), timeout_seconds=10)
                    nodes = json.loads(reply.stdout)["items"]
                    if nodes and all(any(condition["type"] == "Ready" and condition["status"] == "True"
                        for condition in node.get("status", {}).get("conditions", [])) for node in nodes):
                        break
                except DevInstanceRuntimeError:
                    pass
                await asyncio.sleep(0.5)
        yield kubectl
    finally:
        await asyncio.to_thread(container.stop)


async def _eventually(kubectl, identity, kind, name, predicate):
    async with asyncio.timeout(150):
        while True:
            observed = await kubectl.read_resource_json(namespace=identity.namespace, kind=kind, name=name)
            if predicate(observed):
                return observed
            await asyncio.sleep(0.5)


@pytest.mark.docker
@pytest.mark.timeout(360)
async def test_current_secret_mount_runs_but_delayed_old_pod_cannot_mount_successor(running_storage_kubectl):
    kubectl = running_storage_kubectl
    old = _bound_claim().operation.storage_binding.identity
    await _namespace(kubectl, old)
    await kubectl.delete_storage_namespace(old)
    current = old.storage_binding.model_copy(update={"subject_incarnation": uuid4()}).identity
    await _namespace(kubectl, current)
    await write_storage_secret(kubectl, current, {
        "apiVersion": "v1", "kind": "Secret", "type": "Opaque",
        "metadata": {"name": personal_dev_secret_name(current, "loom-secrets"), "namespace": current.namespace},
        "stringData": {"marker": "successor-fixture"},
    })
    pod = {
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "current-mount", "namespace": current.namespace},
        "spec": {
            "restartPolicy": "Never", "automountServiceAccountToken": False,
            "containers": [{"name": "probe", "image": _BUSYBOX,
                "command": ["sh", "-c", 'test "$(cat /fixture/marker)" = successor-fixture'],
                "volumeMounts": [{"name": "fixture", "mountPath": "/fixture", "readOnly": True}]}],
            "volumes": [{"name": "fixture", "secret": {"secretName": personal_dev_secret_name(current, "loom-secrets")}}],
        },
    }
    await kubectl.runner.run(kubectl._argv("create", "-f", "-"), stdin=json.dumps(pod))
    completed = await _eventually(kubectl, current, "pod", "current-mount", lambda value: value.get("status", {}).get("phase") in {"Succeeded", "Failed"})
    assert completed["status"]["phase"] == "Succeeded"
    stale = deepcopy(pod)
    stale["metadata"]["name"] = "delayed-old-mount"
    stale["spec"]["volumes"][0]["secret"]["secretName"] = personal_dev_secret_name(old, "loom-secrets")
    await kubectl.runner.run(kubectl._argv("create", "-f", "-"), stdin=json.dumps(stale))
    async with asyncio.timeout(60):
        while True:
            reply = await kubectl.runner.run(kubectl._argv("get", "events", "-n", current.namespace,
                "--field-selector=involvedObject.name=delayed-old-mount", "-o", "json"))
            if any(event.get("reason") == "FailedMount" and personal_dev_secret_name(old, "loom-secrets") in event.get("message", "")
                   and "not found" in event.get("message", "") for event in json.loads(reply.stdout)["items"]):
                break
            await asyncio.sleep(0.5)
    observed = await kubectl.read_resource_json(namespace=current.namespace, kind="pod", name="delayed-old-mount")
    assert observed["spec"].get("nodeName")
    assert observed["status"]["phase"] == "Pending"
    assert all("running" not in status.get("state", {}) and "terminated" not in status.get("state", {})
               for status in observed["status"].get("containerStatuses", []))


@pytest.mark.docker
@pytest.mark.timeout(360)
async def test_failed_job_new_attempt_actually_executes_again(running_storage_kubectl):
    kubectl = running_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    await _namespace(kubectl, identity)
    document = _workload(identity, "Job")
    document["metadata"]["labels"] = {"loom.dev/attempt": str(uuid4()), "loom.dev/attempt-sequence": "0"}
    document["spec"]["template"]["spec"]["containers"][0].update(image=_BUSYBOX, command=["sh", "-c", "exit 1"])
    await _reconcile_workload(kubectl, identity, document, operation_epoch=1)
    failed = await _eventually(kubectl, identity, "job", document["metadata"]["name"], lambda value:
        any(item["type"] == "Failed" and item["status"] == "True" for item in value.get("status", {}).get("conditions", [])))
    document["metadata"]["labels"]["loom.dev/attempt"] = str(uuid4())
    document["metadata"]["labels"]["loom.dev/attempt-sequence"] = "1"
    await _reconcile_workload(kubectl, identity, document, operation_epoch=1)
    retried = await _eventually(kubectl, identity, "job", document["metadata"]["name"], lambda value:
        any(item["type"] == "Failed" and item["status"] == "True" for item in value.get("status", {}).get("conditions", [])))
    assert failed["metadata"]["uid"] != retried["metadata"]["uid"]
    assert failed["status"]["failed"] == retried["status"]["failed"] == 1
