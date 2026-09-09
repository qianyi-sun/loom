"""A delayed old installer must not inject credentials into a new namespace."""

import asyncio
import json
from dataclasses import replace
from uuid import uuid4

import pytest

from loom.dev_instance_runtime import DevInstanceRuntimeError, KubectlClient, KubectlSecretVault
from loom.personal_dev_incarnation_storage import personal_dev_storage_annotations
from loom.personal_dev_capacity_runtime import KubectlPersonalDevCapacityInstaller
from tests.integration.test_personal_dev_storage_namespace import (
    disposable_storage_kubectl,  # noqa: F401
)
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim


async def test_delayed_capacity_seed_cannot_overwrite_recreated_namespace(
    disposable_storage_kubectl,  # noqa: F811
):
    kubectl = disposable_storage_kubectl
    old = _bound_claim()
    identity = old.operation.storage_binding.identity
    vault = KubectlSecretVault(
        kubectl, "postgresql://admin:fixture@database.example/postgres",
        protected_worker_runtime=True,
    )
    installer = KubectlPersonalDevCapacityInstaller(kubectl=kubectl, database=None, config=None)
    await vault.store(identity, "b" * 32)
    old_credentials = await installer._credentials(old, identity)
    await installer._persist_credentials(old, identity, old_credentials)
    await kubectl.delete_storage_namespace(identity)

    storage = old.operation.storage_binding.model_copy(update={"subject_incarnation": uuid4()})
    current = replace(
        old,
        environment=replace(old.environment, storage_binding=storage,
                            subject_incarnation=storage.subject_incarnation),
        operation=replace(old.operation, storage_binding=storage,
                          subject_incarnation=storage.subject_incarnation),
        attempt=replace(old.attempt, subject_incarnation=storage.subject_incarnation),
    )
    await vault.store(storage.identity, "c" * 32)
    credentials = await installer._credentials(current, storage.identity)
    await installer._persist_credentials(current, storage.identity, credentials)
    before = await kubectl.read_secret_optional(identity.namespace, "loom-capacity-agent-credentials")

    # Resume the old invocation after its namespace/credential preflight has
    # already returned. New read checks at the caller cannot fence this write.
    with pytest.raises(DevInstanceRuntimeError):
        await installer._persist_credentials(old, identity, old_credentials)
    assert await kubectl.read_secret_optional(
        identity.namespace, "loom-capacity-agent-credentials",
    ) == before


@pytest.mark.parametrize("pause_at", ("before_create", "after_create", "before_replace", "before_replace_absent"))
async def test_secret_two_phase_write_fences_namespace_replacement(
    disposable_storage_kubectl, pause_at,  # noqa: F811
):
    from loom.personal_dev_storage_secret_write import write_storage_secret

    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    await kubectl.apply(json.dumps({"apiVersion": "v1", "kind": "Namespace", "metadata": {
        "name": identity.namespace, "annotations": personal_dev_storage_annotations(identity),
    }}))
    paused, resume = asyncio.Event(), asyncio.Event()
    transmitted = []

    class PausedWriter:
        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            document = json.loads(stdin) if stdin is not None else None
            is_secret = isinstance(document, dict) and document.get("kind") == "Secret"
            if is_secret and "create" in argv:
                # No credential data may cross the unguarded CREATE boundary.
                assert document.get("data", {}) == {}
                assert document.get("stringData", {}) == {}
                transmitted.append(document)
            if is_secret and ((pause_at == "before_create" and "create" in argv)
                              or (pause_at.startswith("before_replace") and "replace" in argv)):
                paused.set()
                await resume.wait()
            result = await kubectl.runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)
            if is_secret and pause_at == "after_create" and "create" in argv:
                paused.set()
                await resume.wait()
            return result

    writer = KubectlClient("kubectl", runner=PausedWriter())
    document = {"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
                "metadata": {"name": "storage-write-probe", "namespace": identity.namespace},
                "stringData": {"password": "must-not-cross-incarnations"}}
    task = asyncio.create_task(write_storage_secret(writer, identity, document))
    try:
        await asyncio.wait_for(paused.wait(), timeout=15)
        await kubectl.delete_storage_namespace(identity)
        current = identity.storage_binding.model_copy(update={"subject_incarnation": uuid4()}).identity
        await kubectl.apply(json.dumps({"apiVersion": "v1", "kind": "Namespace", "metadata": {
            "name": current.namespace, "annotations": personal_dev_storage_annotations(current),
        }}))
        before = None
        if pause_at not in ("before_create", "before_replace_absent"):
            await kubectl.apply(json.dumps({**document, "stringData": {"password": "successor"}}))
            before = await kubectl.read_secret_optional(current.namespace, "storage-write-probe")
        resume.set()
        with pytest.raises(DevInstanceRuntimeError):
            await asyncio.wait_for(task, timeout=15)
        after = await kubectl.read_secret_optional(current.namespace, "storage-write-probe")
        assert after == ({} if pause_at == "before_create" else before)
        assert len(transmitted) == 1
    finally:
        resume.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
