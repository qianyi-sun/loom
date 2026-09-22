"""The real durable registry advances only after verified external effects."""

from __future__ import annotations

import asyncio

import pytest

from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


class ExternalProvider:
    def __init__(self):
        self.resources = {}
        self.fail_after_create = False
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.pause = False

    async def apply(self, context, step):
        self.entered.set()
        if self.pause:
            await self.release.wait()
        identity = self.resources.setdefault(step.key, "provider-" + str(len(self.resources)))
        if self.fail_after_create:
            self.fail_after_create = False
            from loom_service.environment_management.provider import ProviderRetryError

            raise ProviderRetryError("provider_unavailable")
        return identity


async def test_worker_recovers_lost_reply_and_finishes_existing_intents(environment_registry):
    from loom_service.environment_management.worker import EnvironmentWorker

    registry, _, (alice, _), prepare = environment_registry
    operation = await registry.create(principal=alice, idempotency_key="worker", prepared=prepare())
    provider = ExternalProvider()
    provider.fail_after_create = True
    worker = EnvironmentWorker(registry, provider)
    await worker.reconcile_once(operation.operation_id)
    first = await registry.get_operation(operation.operation_id, principal=alice)
    assert first.phase == "pending" and first.error_code == "provider_unavailable"
    assert len(provider.resources) == 1
    await EnvironmentWorker(registry, provider).reconcile_once(operation.operation_id)
    assert (await registry.get_operation(operation.operation_id, principal=alice)).phase == "completed"
    assert "ready:application" in provider.resources


async def test_concurrent_worker_does_not_duplicate_mutations(environment_registry):
    from loom_service.environment_management.worker import EnvironmentWorker

    registry, _, (alice, _), prepare = environment_registry
    operation = await registry.create(principal=alice, idempotency_key="worker", prepared=prepare())
    provider = ExternalProvider()
    provider.pause = True
    first = asyncio.create_task(EnvironmentWorker(registry, provider).reconcile_once(operation.operation_id))
    await provider.entered.wait()
    await EnvironmentWorker(registry, ExternalProvider()).reconcile_once(operation.operation_id)
    assert (await registry.get_operation(operation.operation_id, principal=alice)).phase == "running"
    provider.release.set()
    await first
    assert (await registry.get_operation(operation.operation_id, principal=alice)).phase == "completed"


async def test_worker_has_bounded_retries_and_scrubs_unexpected_errors(environment_registry):
    from loom_service.environment_management.provider import ProviderRetryError
    from loom_service.environment_management.worker import EnvironmentWorker

    registry, _, (alice, _), prepare = environment_registry
    operation = await registry.create(principal=alice, idempotency_key="worker", prepared=prepare())

    class Unavailable:
        async def apply(self, context, step):
            raise ProviderRetryError("provider_unavailable")

    worker = EnvironmentWorker(registry, Unavailable(), max_attempts=2)
    await worker.reconcile_once(operation.operation_id)
    await worker.reconcile_once(operation.operation_id)
    state = await registry.get_operation(operation.operation_id, principal=alice)
    assert state.phase == "blocked" and state.error_code == "provider_unavailable"
    assert await registry.claim(operation.operation_id) is None
    assert await registry.runnable_operations() == []

    other = await registry.create(principal=alice, idempotency_key="worker2", prepared=prepare("other"))

    class Broken:
        async def apply(self, context, step):
            raise RuntimeError("password=must-not-be-journaled")

    await EnvironmentWorker(registry, Broken()).reconcile_once(other.operation_id)
    state = await registry.get_operation(other.operation_id, principal=alice)
    assert state.phase == "blocked" and state.error_code == "provider_internal_error"


async def test_shutdown_does_not_report_completion_or_release_ambiguous_intent(environment_registry):
    from loom_service.environment_management.worker import EnvironmentWorker

    registry, _, (alice, _), prepare = environment_registry
    operation = await registry.create(principal=alice, idempotency_key="worker", prepared=prepare())
    provider = ExternalProvider()
    provider.pause = True
    task = asyncio.create_task(EnvironmentWorker(registry, provider).reconcile_once(operation.operation_id))
    await provider.entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    state = await registry.get_operation(operation.operation_id, principal=alice)
    assert state.phase == "running"
    assert await registry.claim(operation.operation_id) is None


async def test_credential_material_is_encrypted_atomic_and_stable_across_restart(environment_registry, monkeypatch):
    import base64
    import json

    from sqlalchemy import select

    from loom.db.nebius_environment_schema import NebiusEnvironmentOperation
    from loom.db.schema import Secret
    from loom_service.environment_management.registry import ManagementError

    monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", base64.b64encode(b"m" * 32).decode())
    monkeypatch.delenv("LOOM_SECRET_STORE_MASTER_KEYS", raising=False)
    registry, factory, (alice, _), prepare = environment_registry
    operation = await registry.create(principal=alice, idempotency_key="worker", prepared=prepare())
    lease = await registry.claim(operation.operation_id)
    assert lease is not None
    with pytest.raises(ManagementError, match="resource_step_out_of_order"):
        await registry.store_material(lease, "credentials:material", {"private": "not-in-journal"})
    while (step := await registry.next_step(lease)).key != "credentials:material":
        await registry.confirm_step(lease, step.key, provider_identity="owned:" + step.key)
    ref = await registry.store_material(lease, step.key, {"private": "not-in-journal"})
    assert await registry.store_material(lease, step.key, {"private": "different"}) == ref
    assert await registry.load_material(lease, step.key) == {"private": "not-in-journal"}
    async with factory() as session:
        row = await session.get(NebiusEnvironmentOperation, operation.operation_id)
        assert "not-in-journal" not in json.dumps(row.plan_json)
        secrets = (await session.scalars(select(Secret))).all()
        assert len(secrets) == 1
        assert b"not-in-journal" not in secrets[0].ciphertext


async def test_readiness_wait_does_not_exhaust_transient_retry_budget(environment_registry):
    from loom_service.environment_management.provider import ProviderWaitingError
    from loom_service.environment_management.worker import EnvironmentWorker

    registry, _, (alice, _), prepare = environment_registry
    operation = await registry.create(principal=alice, idempotency_key="worker", prepared=prepare())

    class Starting(ExternalProvider):
        calls = 0

        async def apply(self, context, step):
            self.calls += 1
            if self.calls < 3:
                raise ProviderWaitingError("kubernetes_not_ready")
            return await super().apply(context, step)

    await EnvironmentWorker(registry, Starting(), max_attempts=1, readiness_poll_seconds=0.01).reconcile_once(operation.operation_id)
    assert (await registry.get_operation(operation.operation_id, principal=alice)).phase == "completed"


async def test_worker_renews_lease_and_stops_external_work_if_lease_is_lost(environment_registry):
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import update

    from loom.db.nebius_environment_schema import NebiusEnvironmentOperation
    from loom_service.environment_management.worker import EnvironmentWorker

    registry, factory, (alice, _), prepare = environment_registry
    operation = await registry.create(principal=alice, idempotency_key="worker", prepared=prepare())
    provider = ExternalProvider()
    provider.pause = True
    task = asyncio.create_task(EnvironmentWorker(registry, provider, lease_seconds=3).reconcile_once(operation.operation_id))
    await provider.entered.wait()
    # Waiting across the original lease verifies that actual renewals happen.
    await asyncio.sleep(3.1)
    assert await registry.claim(operation.operation_id) is None
    async with factory.begin() as session:
        await session.execute(update(NebiusEnvironmentOperation).where(
            NebiusEnvironmentOperation.operation_id == operation.operation_id,
        ).values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=1)))
    successor = await registry.claim(operation.operation_id)
    assert successor is not None
    await asyncio.wait_for(task, timeout=2)
    provider.release.set()
    assert provider.resources == {}
    assert (await registry.provisioning_context(successor)).identities == {}
