"""Typed application SQL validates installation origins without Python admission."""

from uuid import UUID

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError

from loom_capacity_manager.models import CapacityCandidate
from tests.capacity_build_membership_fixtures import (
    application_request,
    build_request,
    staged_build_event,
    typed_sql_execution,
)
from tests.integration.test_capacity_build_membership_sql import _reseal
from tests.integration.test_capacity_mixed_membership_store import apply, transition


async def staged(session, operation):
    management, preparation, fleet, execution = await typed_sql_execution(session)
    initial_request = application_request(preparation, execution)
    if operation == "create":
        request, previous, previous_head = initial_request, None, "0" * 64
    else:
        receipt = await apply(session, initial_request)
        previous, previous_head = receipt.member, receipt.head_sha256
        request = transition(initial_request, operation, revision=1)
    return await staged_build_event(session, management, preparation, fleet, request,
        previous=previous, previous_request=initial_request if previous else None,
        previous_head=previous_head, idempotency_key=UUID(int=104001))


@pytest.mark.parametrize("operation", ("create", "update", "capacity", "destroy"))
async def test_sql_accepts_exact_typed_application_lifecycle(capacity_session, operation):
    row = await staged(capacity_session, operation)
    capacity_session.add(row)
    await capacity_session.flush()
    assert row.result_payload["member"]["purpose"] == "personal-application"


@pytest.mark.parametrize("field,changed", (
    ("environment_name", "shared"), ("min_slots", 1.0), ("candidate_sha256", "1" * 64),
    ("local_activation_sha256", "1" * 64), ("capacity_agent_installation_sha256", "1" * 64),
    ("demand_reporter_token_sha256", "1" * 64), ("foreign_field", "injected"),
    ("protocol_versions", {"capacity-agent": "v1", "claim-guard": "v1", "control-plane-worker": "v1", "foreign": "v1"}),
))
async def test_sql_rejects_resealed_application_capacity_substitution(capacity_session, field, changed):
    row = await staged(capacity_session, "capacity")
    row.request_payload["command"]["projection"][field] = changed
    _reseal(row)
    with pytest.raises(DBAPIError) as error:
        async with capacity_session.begin_nested():
            capacity_session.add(row)
            await capacity_session.flush()
    assert error.value.orig.sqlstate == "23514"


@pytest.mark.parametrize("changed", ('{"operation_epoch":2}', '{"operation_epoch":1.0}'))
async def test_sql_application_capacity_requires_original_exact_attestation(capacity_session, changed):
    row = await staged(capacity_session, "capacity")
    candidate = (await capacity_session.scalars(select(CapacityCandidate).where(CapacityCandidate.subject_id == row.subject_id))).one()
    await capacity_session.execute(text("UPDATE capacity_candidates SET attestation_payload=attestation_payload || CAST(:changed AS jsonb) WHERE id=:id"), {"changed": changed, "id": candidate.id})
    with pytest.raises(DBAPIError) as error:
        async with capacity_session.begin_nested():
            capacity_session.add(row)
            await capacity_session.flush()
    assert error.value.orig.sqlstate == "23514"


@pytest.mark.parametrize("build", (False, True))
@pytest.mark.parametrize("field", ("token", "acknowledgement"))
async def test_sql_typed_digest_fields_cannot_be_numeric_json(capacity_session, build, field):
    if build:
        management, preparation, fleet, execution = await typed_sql_execution(capacity_session)
        row = await staged_build_event(capacity_session, management, preparation, fleet, build_request(preparation, execution))
    else:
        row = await staged(capacity_session, "create")
    number = int("1" * 64)
    if field == "token":
        row.request_payload["command"]["projection"]["demand_reporter_token_sha256"] = number
        await capacity_session.execute(text("UPDATE capacity_demand_reporters SET token_sha256=:digest WHERE reporter_incarnation=:reporter"), {"digest": str(number), "reporter": row.reporter_incarnation})
    else:
        row.request_payload["command"]["acknowledgement"]["acknowledgement_sha256"] = number
        row.result_payload["member"]["acknowledgement"]["acknowledgement_sha256"] = number
    _reseal(row)
    with pytest.raises(DBAPIError) as error:
        async with capacity_session.begin_nested():
            capacity_session.add(row)
            await capacity_session.flush()
    assert error.value.orig.sqlstate == "23514"


@pytest.mark.parametrize("operation", ("create", "update"))
@pytest.mark.parametrize("field", ("candidate_sha256", "candidate_publication_sha256"))
async def test_sql_application_rejects_zero_candidate_binding(capacity_session, operation, field):
    row = await staged(capacity_session, operation)
    zero = "0" * 64
    row.request_payload["command"]["projection"][field] = zero
    ack_field = "identity" if field == "candidate_sha256" else "publication_sha256"
    for ack in (row.request_payload["command"]["acknowledgement"], row.result_payload["member"]["acknowledgement"]):
        ack["candidate"][ack_field] = zero
    parameters = {"subject": row.subject_id, "generation": row.deployment_generation, "zero": zero}
    if field == "candidate_sha256":
        await capacity_session.execute(text("UPDATE capacity_candidates SET candidate_digest=:zero,candidate_identity=:zero,artifact_payload=jsonb_build_object('candidate_sha256',CAST(:zero AS text)) WHERE subject_id=:subject AND candidate_generation=:generation"), parameters)
        await capacity_session.execute(text("UPDATE capacity_deployment_generations SET candidate_digest=:zero WHERE subject_id=:subject AND deployment_generation=:generation"), parameters)
    else:
        await capacity_session.execute(text("UPDATE capacity_candidates SET source_payload=jsonb_build_object('publication_sha256',CAST(:zero AS text)) WHERE subject_id=:subject AND candidate_generation=:generation"), parameters)
        await capacity_session.execute(text("UPDATE capacity_deployment_generations SET cutover_payload=jsonb_set(cutover_payload,'{candidate_publication_sha256}',to_jsonb(CAST(:zero AS text))) WHERE subject_id=:subject AND deployment_generation=:generation"), parameters)
    _reseal(row)
    with pytest.raises(DBAPIError) as error:
        async with capacity_session.begin_nested():
            capacity_session.add(row)
            await capacity_session.flush()
    assert error.value.orig.sqlstate == "23514"


async def test_sql_application_name_cannot_be_boolean_json(capacity_session):
    row = await staged(capacity_session, "create")
    row.request_payload["command"]["projection"]["environment_name"] = True
    row.result_payload["member"]["configuration"]["display_name"] = "dev-true"
    await capacity_session.execute(text("UPDATE capacity_subjects SET display_name='dev-true',payload=jsonb_set(payload,'{display_name}','\"dev-true\"') WHERE subject_id=:subject"), {"subject": row.subject_id})
    _reseal(row)
    with pytest.raises(DBAPIError) as error:
        async with capacity_session.begin_nested():
            capacity_session.add(row)
            await capacity_session.flush()
    assert error.value.orig.sqlstate == "23514"


async def test_sql_typed_application_rejects_zero_idempotency_identity(capacity_session):
    row = await staged(capacity_session, "create")
    row.idempotency_key = UUID(int=0)
    _reseal(row)
    with pytest.raises(DBAPIError) as error:
        async with capacity_session.begin_nested():
            capacity_session.add(row)
            await capacity_session.flush()
    assert error.value.orig.sqlstate == "23514"
