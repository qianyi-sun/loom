"""A delayed old installer must not inject credentials into a new namespace."""

from dataclasses import replace
from uuid import uuid4

import pytest

from loom.dev_instance_runtime import DevInstanceRuntimeError, KubectlSecretVault
from loom.personal_dev_capacity_runtime import KubectlPersonalDevCapacityInstaller
from tests.integration.test_personal_dev_storage_namespace import (
    disposable_storage_kubectl,  # noqa: F401
)
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim


async def test_delayed_capacity_seed_cannot_overwrite_recreated_namespace(
    disposable_storage_kubectl,
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
