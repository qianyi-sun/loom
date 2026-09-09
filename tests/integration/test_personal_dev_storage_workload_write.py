"""Delayed personal controllers cannot CREATE runnable successor workloads."""

import asyncio
import json
from uuid import uuid4

import pytest
import yaml

from loom.dev_instance_runtime import DevInstanceRuntimeError, KubectlClient
from loom.personal_dev_capacity_runtime import KubectlPersonalDevCapacityInstaller
from loom.personal_dev_incarnation_storage import personal_dev_storage_annotations
from tests.integration.test_personal_dev_storage_namespace import (
    disposable_storage_kubectl,  # noqa: F401
)
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim


def _workload(identity, kind="Deployment"):
    template = {
        "metadata": {"labels": {"app": "storage-workload-probe"}},
        "spec": {
            "automountServiceAccountToken": False,
            "containers": [{"name": "probe", "image": "registry.example/probe@sha256:" + "a" * 64}],
        },
    }
    spec = {"template": template}
    if kind == "Job":
        template["spec"]["restartPolicy"] = "Never"
        spec["backoffLimit"] = 0
    else:
        spec.update(replicas=1, selector={"matchLabels": template["metadata"]["labels"]})
    return {
        "apiVersion": "batch/v1" if kind == "Job" else "apps/v1",
        "kind": kind,
        "metadata": {"name": "storage-workload-probe", "namespace": identity.namespace},
        "spec": spec,
    }


async def _namespace(kubectl, identity):
    await kubectl.apply(json.dumps({
        "apiVersion": "v1", "kind": "Namespace",
        "metadata": {"name": identity.namespace, "annotations": personal_dev_storage_annotations(identity)},
    }))


async def test_delayed_capacity_workload_creation_is_inert_after_namespace_replacement(
    disposable_storage_kubectl,  # noqa: F811
):
    kubectl = disposable_storage_kubectl
    claim = _bound_claim()
    identity = claim.operation.storage_binding.identity
    await _namespace(kubectl, identity)
    paused, resume = asyncio.Event(), asyncio.Event()

    class PausedWriter:
        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            documents = tuple(yaml.safe_load_all(stdin)) if stdin is not None else ()
            if any(isinstance(item, dict) and item.get("kind") == "Deployment" for item in documents):
                paused.set()
                await resume.wait()
            return await kubectl.runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)

    installer = KubectlPersonalDevCapacityInstaller(
        kubectl=KubectlClient("kubectl", runner=PausedWriter()), database=None, config=None,
    )
    task = asyncio.create_task(installer._apply_manifests(claim, identity, (_workload(identity),)))
    try:
        await asyncio.wait_for(paused.wait(), timeout=30)
        await kubectl.delete_storage_namespace(identity)
        successor = identity.storage_binding.model_copy(update={"subject_incarnation": uuid4()}).identity
        await _namespace(kubectl, successor)
        resume.set()
        with pytest.raises(DevInstanceRuntimeError):
            await asyncio.wait_for(task, timeout=30)
        observed = await kubectl.read_resource_json(
            namespace=identity.namespace, kind="deployment", name="storage-workload-probe",
        )
        assert observed["spec"]["replicas"] == 0
    finally:
        resume.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
