"""Typed base reads authenticate pinned installations against immutable generations."""

import json
from uuid import UUID

import pytest
from sqlalchemy import select, text

from loom_capacity_manager.models import CapacityCandidate, CapacityConfigGeneration, CapacityDevelopmentProjection
from loom_capacity_manager.store import ConfigurationConflictError, WriterFence
from loom_capacity_manager.typed_membership_store import CapacityTypedMembershipStore
from tests.capacity_build_membership_fixtures import build_request, typed_sql_execution
from tests.capacity_fixtures import development_projection


async def prepared(session):
    projection = development_projection(expected_configuration_epoch=1)
    return await typed_sql_execution(session, managed_projection=projection)


async def test_typed_managed_base_retains_real_shadow_installation_without_fabricating_event(capacity_session):
    management, preparation, _fleet, execution = await prepared(capacity_session)
    snapshot = await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)
    assert snapshot.revision == 0 and snapshot.members == ()
    origin = preparation.managed_application_origins[0]
    assert origin.base_projection.expected_configuration_epoch < preparation.configuration_epoch
    value = await management.load_allocation_input(capacity_session,
        WriterFence(authority_incarnation=execution.authority_incarnation, writer_epoch=execution.writer_epoch))
    assert value.managed_base_subjects == (origin.configuration,)
    assert sum(subject.configuration.subject_id == origin.configuration.subject_id for subject in value.subjects) == 1
    build = await CapacityTypedMembershipStore().apply_build(capacity_session, build_request(preparation, execution),
        actor="build-management", idempotency_key=UUID(int=88771))
    assert (await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)).members == (build.member,)


@pytest.mark.parametrize("target", ("candidate", "base-generation", "configuration-root"))
async def test_typed_empty_history_rejects_tampered_managed_base_evidence(capacity_session, target):
    _management, preparation, _fleet, execution = await prepared(capacity_session)
    origin = preparation.managed_application_origins[0]
    if target == "candidate":
        row = (await capacity_session.scalars(select(CapacityCandidate).where(CapacityCandidate.subject_id == origin.configuration.subject_id))).one()
        await capacity_session.execute(text("UPDATE capacity_candidates SET attestation_payload=jsonb_set(attestation_payload,'{operation_epoch}','99') WHERE id=:id"), {"id": row.id})
    elif target == "base-generation":
        row = (await capacity_session.scalars(select(CapacityConfigGeneration).where(CapacityConfigGeneration.subject_id == origin.configuration.subject_id))).one()
        await capacity_session.execute(text("UPDATE capacity_config_generations SET payload=jsonb_set(payload,'{max_slots}','1') WHERE id=:id"), {"id": row.id})
    else:
        await capacity_session.execute(text("UPDATE capacity_configuration_epochs SET canonical_digest=:digest WHERE configuration_epoch=:epoch"), {"digest": "f" * 64, "epoch": execution.configuration_epoch})
    with pytest.raises(ConfigurationConflictError):
        await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)


async def test_typed_managed_origin_cannot_be_replaced_by_consistent_mutable_projection_and_candidate(capacity_session):
    _management, preparation, _fleet, execution = await prepared(capacity_session)
    origin = preparation.managed_application_origins[0]
    changed = str(UUID(int=88779))
    row = (await capacity_session.scalars(select(CapacityDevelopmentProjection).where(CapacityDevelopmentProjection.subject_id == origin.configuration.subject_id))).one()
    payload = dict(row.request_payload, operation_id=changed)
    await capacity_session.execute(text("UPDATE capacity_development_projections SET request_payload=CAST(:payload AS jsonb) WHERE id=:id"), {"payload": json.dumps(payload), "id": row.id})
    await capacity_session.execute(text("UPDATE capacity_candidates SET attestation_payload=jsonb_set(attestation_payload,'{operation_id}',to_jsonb(CAST(:operation AS text))) WHERE subject_id=:subject"), {"operation": changed, "subject": origin.configuration.subject_id})
    with pytest.raises(ConfigurationConflictError):
        await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)
