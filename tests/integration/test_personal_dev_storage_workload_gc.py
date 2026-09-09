"""Namespace owners collect stale inert writes without adopting successor state."""

import asyncio
import json
from uuid import uuid4

import pytest

from loom.dev_instance_runtime import DevInstanceRuntimeError, KubectlClient
from loom.personal_dev_storage_workload_write import write_storage_workload
from tests.integration.test_personal_dev_storage_namespace import (
    disposable_storage_kubectl,  # noqa: F401
)
from tests.integration.test_personal_dev_storage_workload_recovery import _reconcile_workload
from tests.integration.test_personal_dev_storage_workload_write import _namespace, _workload
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim


@pytest.mark.parametrize("kind", ("Job", "Deployment"))
async def test_delayed_inert_create_is_collected_and_successor_workload_survives(
    disposable_storage_kubectl, kind,  # noqa: F811
):
    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    await _namespace(kubectl, identity)
    old_uid = (await kubectl.read_storage_namespace(identity))["metadata"]["uid"]
    document = _workload(identity, kind)
    paused, resume = asyncio.Event(), asyncio.Event()
    created = []

    class DelayedCreate:
        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            if "create" in argv:
                paused.set()
                await resume.wait()
            reply = await kubectl.runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)
            if "create" in argv:
                created.append(json.loads(reply.stdout))
            return reply

    task = asyncio.create_task(write_storage_workload(
        KubectlClient("kubectl", runner=DelayedCreate()), identity, document, operation_epoch=1,
    ))
    try:
        await asyncio.wait_for(paused.wait(), 30)
        await kubectl.delete_storage_namespace(identity)
        successor = identity.storage_binding.model_copy(update={"subject_incarnation": uuid4()}).identity
        await _namespace(kubectl, successor)
        resume.set()
        with pytest.raises(DevInstanceRuntimeError):
            await asyncio.wait_for(task, 30)
        assert len(created) == 1
        stale = created[0]
        assert stale["spec"]["suspend" if kind == "Job" else "replicas"] == (True if kind == "Job" else 0)
        assert stale["metadata"].get("ownerReferences") == [{
            "apiVersion": "v1", "kind": "Namespace", "name": identity.namespace, "uid": old_uid,
        }]
        async with asyncio.timeout(60):
            while True:
                reply = await kubectl.runner.run(kubectl._argv(
                    "get", kind.lower(), document["metadata"]["name"], "-n", identity.namespace,
                    "--ignore-not-found", "-o", "json",
                ))
                if not reply.stdout.strip():
                    break
                assert json.loads(reply.stdout)["metadata"]["uid"] == stale["metadata"]["uid"]
                await asyncio.sleep(0.2)
        await _reconcile_workload(kubectl, successor, document, operation_epoch=1)
        current = await kubectl.read_resource_json(namespace=identity.namespace, kind=kind.lower(), name=document["metadata"]["name"])
        assert current["metadata"]["uid"] != stale["metadata"]["uid"]
        successor_uid = (await kubectl.read_storage_namespace(successor))["metadata"]["uid"]
        assert current["metadata"]["ownerReferences"][0]["uid"] == successor_uid
        await _reconcile_workload(kubectl, successor, document, operation_epoch=1)
        replay = await kubectl.read_resource_json(namespace=identity.namespace, kind=kind.lower(), name=document["metadata"]["name"])
        assert replay["metadata"]["uid"] == current["metadata"]["uid"]
    finally:
        resume.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
