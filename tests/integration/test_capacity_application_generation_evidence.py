"""Application evidence retains original installation separately from reporter state."""

from importlib import import_module
from uuid import UUID

import pytest
from sqlalchemy import select, text

from loom_capacity_manager.membership_store import CapacityMembershipStore
from loom_capacity_manager.models import (
    CapacityCandidate,
    CapacityDeploymentGeneration,
    CapacityWorkerProfile,
)
from tests.integration.test_capacity_membership import DELEGATE, _active_v3, _projection, _request


async def installation(session, member, projection, origin):
    from loom_capacity_manager.application_origin_contracts import ManagedApplicationOriginV1

    binding = ManagedApplicationOriginV1(configuration=member.configuration,
        acknowledgement=member.acknowledgement, base_projection=projection, installation_projection=origin)
    await import_module("loom_capacity_manager.application_generation_store").require_application_installation_evidence(session, binding)


async def test_application_installation_read_is_independent_of_later_reporter_rotation(capacity_session):
    store, active, origin, created = await initial(capacity_session)
    update_projection = _projection(operation_kind="update", operation_epoch=2, operation_id=UUID(int=87302), reporter_incarnation=UUID(int=87202))
    await store.apply(capacity_session, _request(active, update_projection, expected_revision=1), actor=DELEGATE, idempotency_key=UUID(int=87402))
    await installation(capacity_session, created.member, origin, origin)
    # A successful historical installation read never proves current reporting.
    with pytest.raises(ValueError):
        await require(capacity_session, created.member, origin, origin)


async def test_application_generation_can_retain_original_installation_across_input_epochs(capacity_session):
    store, active, origin, _created = await initial(capacity_session)
    resized_projection = _projection(operation_kind="capacity", operation_epoch=2, operation_id=UUID(int=87502), deployment_generation=1)
    resized_projection = resized_projection.model_copy(update={"demand_reporter_token_sha256": origin.demand_reporter_token_sha256})
    resized = await store.apply(capacity_session, _request(active, resized_projection, expected_revision=1), actor=DELEGATE, idempotency_key=UUID(int=87602))
    # The importing operation's epoch is authenticated by its caller. It must
    # not overwrite the installation operation attested by the candidate row.
    imported = resized_projection.model_copy(update={"expected_configuration_epoch": origin.expected_configuration_epoch + 1})
    await installation(capacity_session, resized.member, imported, origin)
    await require(capacity_session, resized.member, imported, origin)


async def test_historical_installation_still_rejects_mutated_original_attestation(capacity_session):
    _store, _active, origin, created = await initial(capacity_session)
    await capacity_session.execute(text("UPDATE capacity_candidates SET attestation_payload=jsonb_set(attestation_payload,'{operation_epoch}','99') WHERE subject_id=:subject"), {"subject": created.member.configuration.subject_id})
    with pytest.raises(ValueError):
        await installation(capacity_session, created.member, origin, origin)


async def require(session, member, projection, origin, *, state="current"):
    await import_module("loom_capacity_manager.application_generation_store").require_application_generation_evidence(
        session, member, projection, origin, reporter_state=state,
    )


async def initial(session):
    fixture, active = await _active_v3(session)
    store = CapacityMembershipStore(fixture.store)
    origin = _projection()
    created = await store.apply(session, _request(active, origin), actor=DELEGATE, idempotency_key=UUID(int=87001))
    return store, active, origin, created


async def test_application_generation_evidence_matches_real_existing_persistence(capacity_session):
    _store, _active, origin, created = await initial(capacity_session)
    await require(capacity_session, created.member, origin, origin)


async def test_application_generation_tracks_origin_and_last_reporter_independently(capacity_session):
    store, active, origin, created = await initial(capacity_session)
    resized_projection = _projection(operation_kind="capacity", operation_epoch=2, operation_id=UUID(int=87002), deployment_generation=1)
    resized_projection = resized_projection.model_copy(update={"demand_reporter_token_sha256": origin.demand_reporter_token_sha256})
    resized = await store.apply(capacity_session, _request(active, resized_projection, expected_revision=1), actor=DELEGATE, idempotency_key=UUID(int=87102))
    await require(capacity_session, resized.member, resized_projection, origin)
    with pytest.raises(ValueError):
        await require(capacity_session, resized.member, resized_projection, resized_projection)
    update_projection = _projection(operation_kind="update", operation_epoch=3, operation_id=UUID(int=87003), reporter_incarnation=UUID(int=87203))
    updated = await store.apply(capacity_session, _request(active, update_projection, expected_revision=2), actor=DELEGATE, idempotency_key=UUID(int=87103))
    await require(capacity_session, updated.member, update_projection, update_projection)
    await require(capacity_session, resized.member, resized_projection, origin, state="fenced")
    with pytest.raises(ValueError):
        await require(capacity_session, created.member, origin, origin, state="fenced")
    destroy_projection = _projection(operation_kind="destroy", operation_epoch=4, operation_id=UUID(int=87004), reporter_incarnation=UUID(int=87203), deployment_generation=3)
    destroyed = await store.apply(capacity_session, _request(active, destroy_projection, expected_revision=3), actor=DELEGATE, idempotency_key=UUID(int=87104))
    await require(capacity_session, destroyed.member, destroy_projection, update_projection)


@pytest.mark.parametrize("field,document", (
    ("attestation_payload", '{"operation_id":"00000000-0000-0000-0000-000000000999"}'),
    ("attestation_payload", '{"operation_epoch":1.0}'),
    ("artifact_payload", '{"candidate_sha256":"changed"}'),
))
async def test_application_generation_refreshes_and_checks_complete_origin_evidence(capacity_session, field, document):
    _store, _active, origin, created = await initial(capacity_session)
    row = (await capacity_session.scalars(select(CapacityCandidate).where(CapacityCandidate.subject_id == created.member.configuration.subject_id))).one()
    await capacity_session.execute(text(f"UPDATE capacity_candidates SET {field}={field} || CAST(:document AS jsonb) WHERE id=:id"), {"document": document, "id": row.id})
    with pytest.raises(ValueError):
        await require(capacity_session, created.member, origin, origin)


async def test_application_generation_rejects_wrong_original_operation(capacity_session):
    _store, _active, origin, created = await initial(capacity_session)
    changed = origin.model_copy(update={"operation_id": UUID(int=87999)})
    with pytest.raises(ValueError):
        await require(capacity_session, created.member, origin, changed)


@pytest.mark.parametrize("target", ("deployment", "profile", "profile-set"))
async def test_application_generation_checks_profiles_and_deployment_canonically(capacity_session, target):
    _store, _active, origin, created = await initial(capacity_session)
    subject = created.member.configuration
    if target == "deployment":
        row = (await capacity_session.scalars(select(CapacityDeploymentGeneration).where(CapacityDeploymentGeneration.subject_id == subject.subject_id))).one()
        await capacity_session.execute(text("UPDATE capacity_deployment_generations SET required_profiles=jsonb_set(required_profiles,'{0,worker_shapes,0,concurrency_slots}','1.0') WHERE id=:id"), {"id": row.id})
    else:
        row = (await capacity_session.scalars(select(CapacityWorkerProfile).where(CapacityWorkerProfile.subject_id == subject.subject_id))).first()
        if target == "profile":
            await capacity_session.execute(text("UPDATE capacity_worker_profiles SET shape_catalog=jsonb_set(shape_catalog,'{0,concurrency_slots}','1.0') WHERE id=:id"), {"id": row.id})
        else:
            await capacity_session.execute(text("DELETE FROM capacity_worker_profiles WHERE id=:id"), {"id": row.id})
    with pytest.raises(ValueError):
        await require(capacity_session, created.member, origin, origin)
