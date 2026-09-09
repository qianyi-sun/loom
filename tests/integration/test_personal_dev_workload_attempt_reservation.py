"""A newer attempt keeps its ordering authority while a failed Job is absent."""

import asyncio
import json
from copy import deepcopy
from datetime import UTC, datetime
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


async def test_failed_job_replacement_gap_cannot_reacquire_older_attempt(disposable_storage_kubectl):  # noqa: F811
    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    await _namespace(kubectl, identity)
    old = _workload(identity, "Job")
    old["metadata"]["labels"] = {"loom.dev/attempt": str(uuid4()), "loom.dev/attempt-sequence": "0"}
    # No live Pods are needed to expose the object-authority deletion gap.
    old["spec"]["parallelism"] = 0
    await _reconcile_workload(kubectl, identity, old, operation_epoch=1)
    await kubectl.runner.run(kubectl._argv(
        "patch", "job", old["metadata"]["name"], "-n", identity.namespace,
        "--subresource=status", "--type=merge", "-p", json.dumps({"status": {
            "startTime": datetime.now(UTC).isoformat(), "failed": 1, "active": 0,
            "conditions": [{"type": "FailureTarget", "status": "True"}, {"type": "Failed", "status": "True"}],
        }}),
    ))
    newer = deepcopy(old)
    newer["metadata"]["labels"].update({"loom.dev/attempt": str(uuid4()), "loom.dev/attempt-sequence": "1"})
    paused, resume = asyncio.Event(), asyncio.Event()

    class PausedReplacement:
        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            payload = json.loads(stdin) if stdin else None
            if "create" in argv and isinstance(payload, dict) and payload.get("kind") == "Job":
                paused.set()
                await resume.wait()
            return await kubectl.runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)

    task = asyncio.create_task(_reconcile_workload(
        KubectlClient("kubectl", runner=PausedReplacement()), identity, newer, operation_epoch=1,
    ))
    try:
        await asyncio.wait_for(paused.wait(), 30)
        absent = await kubectl.runner.run(kubectl._argv(
            "get", "job", old["metadata"]["name"], "-n", identity.namespace, "--ignore-not-found", "-o", "json",
        ))
        assert not absent.stdout.strip()
        with pytest.raises(DevInstanceRuntimeError, match="attempt|reservation"):
            await write_storage_workload(kubectl, identity, old, operation_epoch=1)
        resume.set()
        await asyncio.wait_for(task, 30)
        current = await kubectl.read_resource_json(namespace=identity.namespace, kind="job", name=old["metadata"]["name"])
        assert current["metadata"]["labels"]["loom.dev/attempt"] == newer["metadata"]["labels"]["loom.dev/attempt"]
    finally:
        resume.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
