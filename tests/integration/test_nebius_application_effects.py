"""Durable application write intent is distinct from provider/runtime fencing."""
from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest
from sqlalchemy import text

from loom.db.nebius_application_operation_schema import (
    NebiusApplicationOperation,
    NebiusApplicationReservation,
)
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def intent(plan, **changes):
    return dict(api_version="apps/v1", kind="Deployment", namespace=plan["prepared"].registration.application_namespace,
                name="loom-service", action="create", request_sha256="a" * 64) | changes


async def started(applications):
    registry, factory, (alice, _), prepare, _, _ = applications
    plan = prepare()
    operation = await registry.create(principal=alice, idempotency_key="create", **plan)
    return registry, factory, alice, plan, operation, await registry.claim(operation.operation_id)


async def expire(factory, lease):
    async with factory.begin() as session:
        row = await session.get(NebiusApplicationOperation, lease.operation_id)
        row.lease_expires_at = await session.scalar(text("SELECT clock_timestamp() - interval '1 second'"))


async def test_prepare_is_atomic_immutable_and_never_sends_a_write(applications):
    registry, _, _, plan, operation, lease = await started(applications)
    values = await asyncio.gather(*[
        registry.prepare_effect(lease, "api-create", intent(plan)) for _ in range(3)
    ])
    assert all(value == values[0] for value in values)
    first = values[0]
    assert first.phase == "prepared" and first.dispatch_epoch is None
    assert first.sequence == 1 and first.operation_id == operation.operation_id
    with pytest.raises(ManagementError, match="application_effect_conflict"):
        await registry.prepare_effect(lease, "api-create", intent(plan, request_sha256="b" * 64))
    with pytest.raises(ManagementError, match="application_effect_unresolved"):
        await registry.prepare_effect(lease, "web-create", intent(plan, name="loom-web"))
    assert len(await registry.effect_history(lease)) == 1


@pytest.mark.parametrize("change", [
    {"namespace": "loom-shared"}, {"name": "foreign"}, {"kind": "Job", "api_version": "batch/v1"},
    {"kind": "StatefulSet"}, {"api_version": "v1"}, {"action": "delete"},
    {"action": "patch", "uid": "exact-uid"}, {"action": "create", "uid": "exact-uid", "resource_version": "4"},
    {"request_sha256": "not-a-digest"}, {"body": {"password": "must-not-be-stored"}},
    {"kind": "Secret", "api_version": "v1", "name": "management-key"},
    {"kind": "ResourceQuota", "api_version": "v1", "name": "arbitrary"},
    {"kind": "Pod", "api_version": "v1", "name": "api-pod"},
])
async def test_invalid_or_foreign_write_intent_is_rejected_without_journal_row(applications, change):
    registry, _, _, plan, _, lease = await started(applications)
    with pytest.raises(ManagementError, match="invalid_application_effect"):
        await registry.prepare_effect(lease, "invalid", intent(plan, **change))
    assert await registry.effect_history(lease) == []


@pytest.mark.parametrize("kind,api,name", [
    ("Namespace", "v1", None), ("RoleBinding", "rbac.authorization.k8s.io/v1", "authority"),
])
async def test_bootstrap_identity_cannot_be_deleted_or_patched(applications, kind, api, name):
    registry, _, _, plan, _, lease = await started(applications)
    for action in ("patch", "delete"):
        with pytest.raises(ManagementError, match="invalid_application_effect"):
            await registry.prepare_effect(lease, "identity", intent(plan, kind=kind, api_version=api,
                namespace=None if kind == "Namespace" else plan["prepared"].registration.application_namespace,
                name=name or plan["prepared"].registration.application_namespace,
                action=action, uid="owned-uid", resource_version="4"))


async def test_dispatch_has_one_winner_and_expiry_does_not_authorize_resend(applications):
    registry, factory, _, plan, operation, lease = await started(applications)
    await registry.prepare_effect(lease, "api", intent(plan))
    winners = await asyncio.gather(*[registry.dispatch_effect(lease, "api") for _ in range(3)])
    assert winners.count(True) == 1 and winners.count(False) == 2
    await expire(factory, lease)
    newer = await registry.claim(operation.operation_id)
    assert await registry.dispatch_effect(newer, "api") is False
    effect = (await registry.effect_history(newer))[0]
    assert effect.phase == "dispatched" and effect.dispatch_epoch == lease.runner_epoch
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await registry.observe_effect(lease, "api", uid="created-uid", resource_version="9")
    await registry.observe_effect(newer, "api", uid="created-uid", resource_version="9")
    assert (await registry.effect_history(newer))[0].phase == "observed"


async def test_prepared_effect_can_be_dispatched_after_takeover_but_not_observed_before_dispatch(applications):
    registry, factory, _, plan, operation, lease = await started(applications)
    await registry.prepare_effect(lease, "api", intent(plan))
    with pytest.raises(ManagementError, match="application_effect_not_dispatched"):
        await registry.observe_effect(lease, "api", uid="created-uid", resource_version="9")
    await expire(factory, lease)
    newer = await registry.claim(operation.operation_id)
    assert await registry.dispatch_effect(newer, "api") is True
    assert (await registry.effect_history(newer))[0].dispatch_epoch == newer.runner_epoch


async def test_observation_is_immutable_and_must_match_exact_mutation_target(applications):
    registry, _, _, plan, _, lease = await started(applications)
    await registry.prepare_effect(lease, "stop", intent(plan, action="patch", uid="owned-uid", resource_version="4"))
    await registry.dispatch_effect(lease, "stop")
    for uid, rv in (("replacement-uid", "5"), ("owned-uid", None), ("owned-uid", "bad\nversion")):
        with pytest.raises(ManagementError, match="invalid_application_effect_observation"):
            await registry.observe_effect(lease, "stop", uid=uid, resource_version=rv)
    await registry.observe_effect(lease, "stop", uid="owned-uid", resource_version="5")
    await registry.observe_effect(lease, "stop", uid="owned-uid", resource_version="5")
    with pytest.raises(ManagementError, match="application_effect_observation_conflict"):
        await registry.observe_effect(lease, "stop", uid="owned-uid", resource_version="6")
    assert await registry.dispatch_effect(lease, "stop") is False
    second = await registry.prepare_effect(lease, "delete-pod", intent(plan, api_version="v1", kind="Pod",
        name="api-old-pod", action="delete", uid="pod-uid", resource_version="7"))
    assert second.sequence == 2
    await registry.dispatch_effect(lease, "delete-pod")
    with pytest.raises(ManagementError, match="invalid_application_effect_observation"):
        await registry.observe_effect(lease, "delete-pod", uid="pod-uid", resource_version="8")
    await registry.observe_effect(lease, "delete-pod", uid="pod-uid", resource_version=None)
    assert [item.phase for item in await registry.effect_history(lease)] == ["observed", "observed"]


async def test_stop_retains_uncertain_predecessor_and_capacity_without_foreign_history(applications):
    registry, factory, alice, plan, first, lease = await started(applications)
    await registry.prepare_effect(lease, "late-create", intent(plan))
    await registry.dispatch_effect(lease, "late-create")
    stopped = await registry.transition(first.application_id, principal=alice, idempotency_key="stop",
        action="suspend", expected_generation=1)
    stop_lease = await registry.claim(stopped.operation_id)
    history = await registry.effect_history(stop_lease)
    assert len(history) == 1 and history[0].operation_id == first.operation_id
    assert history[0].phase == "dispatched"
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await registry.effect_history(lease)
    with pytest.raises(ManagementError, match="application_effect_missing"):
        await registry.observe_effect(stop_lease, "late-create", uid="late-uid", resource_version="9")
    _, _, (_, bob), prepare, _, _ = applications
    other = await registry.create(principal=bob, idempotency_key="bob", **prepare("bob", bob))
    bob_lease = await registry.claim(other.operation_id)
    assert await registry.effect_history(bob_lease) == []
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await registry.effect_history(replace(bob_lease, application_id=first.application_id))
    async with factory() as session:
        hold = await session.get(NebiusApplicationReservation, first.application_id)
        assert hold.cpu_millis == plan["prepared"].platform_envelope.cpu_millis
        assert (await session.get(NebiusApplicationOperation, stopped.operation_id)).phase == "running"


async def test_retry_preserves_dispatched_effect_and_missing_keys_fail_closed(applications):
    registry, _, alice, plan, first, lease = await started(applications)
    for call in (registry.dispatch_effect(lease, "missing"),
                 registry.observe_effect(lease, "missing", uid="uid", resource_version="1")):
        with pytest.raises(ManagementError, match="application_effect_missing"):
            await call
    await registry.prepare_effect(lease, "api", intent(plan))
    await registry.dispatch_effect(lease, "api")
    await registry.finish_attempt(lease, error_code="uncertain_write", retry=False)
    await registry.retry(first.operation_id, principal=alice)
    newer = await registry.claim(first.operation_id)
    assert await registry.dispatch_effect(newer, "api") is False
    assert (await registry.effect_history(newer))[0].phase == "dispatched"


@pytest.mark.parametrize("kind,name", [
    ("Namespace", None), ("Secret", "loom-application-db"), ("Secret", "loom-application-storage"),
    ("Secret", "loom-application-auth"), ("ResourceQuota", "loom-application-retired"),
])
async def test_exact_bootstrap_and_runtime_material_targets_are_recorded_without_contents(applications, kind, name):
    registry, _, _, plan, _, lease = await started(applications)
    ns = plan["prepared"].registration.application_namespace
    effect = await registry.prepare_effect(lease, "setup", intent(plan, kind=kind, api_version="v1",
        name=name or ns, namespace=None if kind == "Namespace" else ns))
    assert effect.intent.name == (name or ns)
    assert set(effect.intent.model_dump()) == {
        "api_version", "kind", "namespace", "name", "action", "request_sha256", "uid", "resource_version",
    }
    assert await registry.dispatch_effect(lease, "setup") is True


async def test_concurrent_distinct_intents_cannot_both_prepare_before_reconciliation(applications):
    registry, _, _, plan, _, lease = await started(applications)
    results = await asyncio.gather(*[
        registry.prepare_effect(lease, key, intent(plan, name=key)) for key in ("loom-service", "loom-web")
    ], return_exceptions=True)
    assert sum(not isinstance(result, Exception) for result in results) == 1
    failure = next(result for result in results if isinstance(result, Exception))
    assert isinstance(failure, ManagementError) and failure.code == "application_effect_unresolved"
    assert len(await registry.effect_history(lease)) == 1
