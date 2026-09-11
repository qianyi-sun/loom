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
        with pytest.raises(DevInstanceRuntimeError, match=r"attempt|reservation"):
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


async def test_current_attempt_recovers_old_inert_create_in_replacement_gap(disposable_storage_kubectl):  # noqa: F811
    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    await _namespace(kubectl, identity)
    old = _workload(identity, "Job")
    old["spec"]["parallelism"] = 0
    old_paused, old_resume = asyncio.Event(), asyncio.Event()
    new_paused, new_resume = asyncio.Event(), asyncio.Event()

    class DelayedCreate:
        def __init__(self, paused, resume):
            self.paused, self.resume = paused, resume
            self.creates = 0

        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            payload = json.loads(stdin) if stdin else None
            if "create" in argv and isinstance(payload, dict) and payload.get("kind") == "Job":
                self.creates += 1
                self.paused.set()
                await self.resume.wait()
            return await kubectl.runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)

    old_runner, new_runner = DelayedCreate(old_paused, old_resume), DelayedCreate(new_paused, new_resume)
    tasks = []
    try:
        async with asyncio.timeout(60):
            stale = asyncio.create_task(write_storage_workload(
                KubectlClient("kubectl", runner=old_runner), identity, old, operation_epoch=1,
            ))
            tasks.append(stale)
            await old_paused.wait()
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
            current = asyncio.create_task(write_storage_workload(
                KubectlClient("kubectl", runner=new_runner), identity, newer, operation_epoch=1,
            ))
            tasks.append(current)
            await new_paused.wait()
            old_resume.set()
            with pytest.raises(DevInstanceRuntimeError, match="reservation attempt was superseded"):
                await stale
            inert = await kubectl.read_resource_json(namespace=identity.namespace, kind="job", name=old["metadata"]["name"])
            assert inert["spec"]["suspend"] is True
            new_resume.set()
            await current
            active = await kubectl.read_resource_json(namespace=identity.namespace, kind="job", name=old["metadata"]["name"])
            assert active["metadata"]["uid"] == inert["metadata"]["uid"]
            assert active["spec"]["suspend"] is False
            assert active["metadata"]["labels"]["loom.dev/attempt"] == newer["metadata"]["labels"]["loom.dev/attempt"]
            assert old_runner.creates == new_runner.creates == 1
    finally:
        old_resume.set()
        new_resume.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
