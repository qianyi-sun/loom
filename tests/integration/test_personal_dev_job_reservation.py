"""API-backed reservation CAS and namespace lifetime behavior."""

import asyncio
import json
from uuid import uuid4

from loom.dev_instance_runtime import DevInstanceRuntimeError, KubectlClient
from loom.personal_dev_job_reservation import reserve_job_attempt
from tests.integration.test_personal_dev_storage_namespace import (
    disposable_storage_kubectl,  # noqa: F401
)
from tests.integration.test_personal_dev_storage_workload_write import _namespace
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim


async def test_concurrent_reservation_updates_use_one_shot_resource_version_cas(disposable_storage_kubectl):  # noqa: F811
    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    await _namespace(kubectl, identity)
    namespace = await kubectl.read_storage_namespace(identity)
    namespace_uid = namespace["metadata"]["uid"]

    async def reserve(client, sequence, attempt):
        return await reserve_job_attempt(
            client, identity, namespace_uid=namespace_uid,
            job_name="loom-migrate-abcdef0-g1", intent='{"template":{}}',
            operation_epoch=1, attempt=(sequence, attempt),
        )

    initial = await reserve(kubectl, 0, uuid4())
    both_ready = asyncio.Event()
    writers = 0
    puts = []

    class ConcurrentUpdates:
        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            nonlocal writers
            if "replace" in argv:
                writers += 1
                puts.append(json.loads(stdin))
                if writers == 2:
                    both_ready.set()
                await both_ready.wait()
            return await kubectl.runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)

    client = KubectlClient("kubectl", runner=ConcurrentUpdates())
    async with asyncio.timeout(30):
        results = await asyncio.gather(
            reserve(client, 1, uuid4()), reserve(client, 2, uuid4()), return_exceptions=True,
        )
    failures = [result for result in results if isinstance(result, BaseException)]
    assert len(failures) == 1 and isinstance(failures[0], DevInstanceRuntimeError)
    assert writers == 2
    assert puts[0]["metadata"]["resourceVersion"] == puts[1]["metadata"]["resourceVersion"]
    winner = next(result for result in results if not isinstance(result, BaseException))
    await winner.verify(kubectl)
    assert winner.document["metadata"]["uid"] == initial.document["metadata"]["uid"]


async def test_late_reservation_create_is_collected_after_namespace_uid_replacement(disposable_storage_kubectl):  # noqa: F811
    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    await _namespace(kubectl, identity)
    old_uid = (await kubectl.read_storage_namespace(identity))["metadata"]["uid"]
    paused, resume = asyncio.Event(), asyncio.Event()

    class DelayedCreate:
        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            if "create" in argv:
                paused.set()
                await resume.wait()
            return await kubectl.runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)

    task = asyncio.create_task(reserve_job_attempt(
        KubectlClient("kubectl", runner=DelayedCreate()), identity,
        namespace_uid=old_uid, job_name="loom-migrate-abcdef0-g1",
        intent='{"template":{}}', operation_epoch=1, attempt=(0, uuid4()),
    ))
    try:
        async with asyncio.timeout(60):
            await paused.wait()
            await kubectl.delete_storage_namespace(identity)
            await _namespace(kubectl, identity)
            new_uid = (await kubectl.read_storage_namespace(identity))["metadata"]["uid"]
            assert new_uid != old_uid
            resume.set()
            result = (await asyncio.gather(task, return_exceptions=True))[0]
            assert isinstance(result, DevInstanceRuntimeError)
            while True:
                record = await kubectl.runner.run(kubectl._argv(
                    "get", "configmap", "loom-workload-fence-loom-migrate-abcdef0-g1",
                    "-n", identity.namespace, "--ignore-not-found", "-o", "json",
                ))
                if not record.stdout.strip():
                    break
                assert json.loads(record.stdout)["metadata"]["ownerReferences"][0]["uid"] == old_uid
                await asyncio.sleep(0.1)
            current = await reserve_job_attempt(
                kubectl, identity, namespace_uid=new_uid,
                job_name="loom-migrate-abcdef0-g1", intent='{"template":{}}',
                operation_epoch=1, attempt=(0, uuid4()),
            )
            await current.verify(kubectl)
    finally:
        resume.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
