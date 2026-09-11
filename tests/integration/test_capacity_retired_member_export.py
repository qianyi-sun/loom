"""Retired member export authenticates real history without admitting a successor."""

from importlib import import_module

import pytest
from sqlalchemy import func, select, text

from loom_capacity_manager.models import CapacityConfigGeneration, CapacityConfigurationEpoch
from loom_capacity_manager.store import ConfigurationConflictError, ExecutionConflictError
from loom_capacity_manager.typed_membership_store import CapacityTypedMembershipStore
from tests.capacity_build_membership_fixtures import (
    application_request,
    build_request,
    managed_application_request,
    typed_sql_execution,
)
from tests.integration.test_capacity_mixed_membership_store import apply, transition
from tests.integration.test_capacity_retired_application_import import retire
from tests.integration.test_capacity_typed_managed_base_history import prepared
from tests.integration.test_capacity_typed_recreation_store import recreate


async def export(session, execution, snapshot):
    module = import_module("loom_capacity_manager.retired_member_export")
    return await module.export_retired_member_origins(session, execution_epoch=execution.execution_epoch,
        expected_snapshot=snapshot)


@pytest.mark.parametrize("build", (False, True))
@pytest.mark.parametrize("operation", ("create", "capacity", "destroy", "recreate"))
async def test_export_preserves_real_last_event_root_and_installation(capacity_session, build, operation):
    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    request = (build_request if build else application_request)(preparation, execution)
    created = latest = await apply(capacity_session, request)
    if operation != "create":
        next_request = transition(request, "capacity" if operation == "capacity" else "destroy", revision=1)
        latest = await apply(capacity_session, next_request, key=970001)
        if operation == "recreate":
            latest = await apply(capacity_session, recreate(next_request, revision=2), key=970002)
    # The final global event belongs to a different owner and purpose.
    other_request = (application_request if build else build_request)(preparation, execution,
        owner=88011, revision=latest.revision)
    other = await apply(capacity_session, other_request, key=970003)
    snapshot = await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)
    await retire(capacity_session, management, preparation, execution)
    counts = tuple([await capacity_session.scalar(select(func.count()).select_from(model))
        for model in (CapacityConfigGeneration, CapacityConfigurationEpoch)])
    result = await export(capacity_session, execution, snapshot)
    origins = {origin.configuration.subject_id: origin for origin in (*result.applications, *result.builds)}
    assert set(origins) == {created.member.configuration.subject_id, other.member.configuration.subject_id}
    origin = origins[created.member.configuration.subject_id]
    assert origin.inherited.anchor.member == latest.member
    assert origin.inherited.anchor.revision == latest.revision
    assert origin.inherited.anchor.head_sha256 == latest.head_sha256
    assert origin.inherited.source.revision == other.revision
    assert origin.inherited.source.head_sha256 == other.head_sha256
    assert origin.inherited.original_origin.subject_incarnation == created.member.configuration.subject_incarnation
    assert origin.inherited.original_origin.generation == created.member.configuration.configuration_generation
    assert origin.installation_projection.operation_kind == "create"
    assert origin.installation_projection.subject_incarnation == latest.member.configuration.subject_incarnation
    if build:
        assert origin.readiness_state == "pending"
        assert origin.template == preparation.personal_builds
        assert origin.trusted_fleet_release_sha256 == preparation.trusted_fleet_release_sha256
    assert await export(capacity_session, execution, snapshot) == result
    assert tuple([await capacity_session.scalar(select(func.count()).select_from(model))
        for model in (CapacityConfigGeneration, CapacityConfigurationEpoch)]) == counts


async def test_export_untouched_operator_base_does_not_invent_member_event(capacity_session):
    management, preparation, _fleet, execution = await prepared(capacity_session)
    snapshot = await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)
    await retire(capacity_session, management, preparation, execution)
    result = await export(capacity_session, execution, snapshot)
    assert result.applications == preparation.managed_application_origins
    assert not result.builds


@pytest.mark.parametrize("boundary", ("active", "old-prefix", "head", "members", "installation"))
async def test_export_rejects_unretired_or_incomplete_or_changed_source(capacity_session, boundary):
    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    request = build_request(preparation, execution)
    created = await apply(capacity_session, request)
    snapshot = await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)
    if boundary == "old-prefix":
        await apply(capacity_session, transition(request, "capacity", revision=1), key=970001)
    if boundary != "active":
        await retire(capacity_session, management, preparation, execution)
    if boundary == "head":
        snapshot = snapshot.model_copy(update={"head_sha256": "f" * 64})
    elif boundary == "members":
        snapshot = snapshot.model_copy(update={"members": ()})
    elif boundary == "installation":
        await capacity_session.execute(text("UPDATE capacity_candidates SET attestation_payload='{}'::jsonb WHERE subject_id=:subject"),
            {"subject": created.member.configuration.subject_id})
    with pytest.raises((ConfigurationConflictError, ExecutionConflictError)):
        await export(capacity_session, execution, snapshot)


async def test_export_recreated_managed_application_keeps_original_pinned_root(capacity_session):
    management, preparation, _fleet, execution = await prepared(capacity_session)
    original = preparation.managed_application_origins[0].configuration
    request = managed_application_request(preparation, execution)
    await apply(capacity_session, request)
    disabled = transition(request, "destroy", revision=1)
    await apply(capacity_session, disabled, key=970001)
    latest = await apply(capacity_session, recreate(disabled, revision=2), key=970002)
    snapshot = await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)
    await retire(capacity_session, management, preparation, execution)
    result = await export(capacity_session, execution, snapshot)
    origin = result.applications[0]
    assert origin.inherited.original_origin.subject_incarnation == original.subject_incarnation
    assert origin.inherited.original_origin.generation == original.configuration_generation
    assert origin.inherited.anchor.member == latest.member
    assert origin.inherited.original_origin == latest.member.reincarnation.origin


@pytest.mark.parametrize("edit", ("dirty", "deleted", "new"))
async def test_export_does_not_flush_or_discard_pending_session_edits(capacity_session, edit):
    from uuid import UUID

    management, preparation, _fleet, execution = await prepared(capacity_session)
    snapshot = await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)
    await retire(capacity_session, management, preparation, execution)
    row = (await capacity_session.scalars(select(CapacityConfigGeneration))).first()
    if edit == "dirty":
        row.actor = "pending-local-actor"
    elif edit == "deleted":
        await capacity_session.delete(row)
    else:
        row = CapacityConfigGeneration(scope="subject", subject_id=UUID(int=970010),
            subject_incarnation=UUID(int=970011), scope_generation=1, digest="e" * 64,
            payload={}, state="proposed", actor="pending-local-actor", idempotency_key=UUID(int=970012))
        capacity_session.add(row)
    before = (set(capacity_session.new), set(capacity_session.dirty), set(capacity_session.deleted))
    with pytest.raises(ConfigurationConflictError, match="pending edits"):
        await export(capacity_session, execution, snapshot)
    assert (set(capacity_session.new), set(capacity_session.dirty), set(capacity_session.deleted)) == before
    if edit == "dirty":
        assert row.actor == "pending-local-actor"
