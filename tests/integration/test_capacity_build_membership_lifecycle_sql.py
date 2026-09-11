"""Raw SQL lifecycle cannot evade the typed store's ownership/retention rules."""

from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError

from loom_capacity_manager.models import CapacityDemandReporter
from tests.capacity_build_membership_fixtures import (
    build_request,
    staged_build_event,
    typed_sql_execution,
)
from tests.integration.test_capacity_build_membership_sql import _reseal
from tests.integration.test_capacity_typed_membership_store import _apply, _transition


async def _staged_transition(session, operation):
    management, preparation, fleet, execution = await typed_sql_execution(session)
    first_request = build_request(preparation, execution)
    first = await _apply(session, first_request)
    request = _transition(first_request, operation)
    row = await staged_build_event(session, management, preparation, fleet, request,
        previous_head=first.head_sha256, previous=first.member, previous_request=first_request,
        idempotency_key=UUID(int=97000))
    return row, first_request, first


@pytest.mark.parametrize("operation", ("update", "capacity", "destroy"))
async def test_sql_accepts_exact_pending_lifecycle_event(capacity_session, operation):
    row, _request, _result = await _staged_transition(capacity_session, operation)
    capacity_session.add(row)
    await capacity_session.flush()
    assert row.revision == 2


@pytest.mark.parametrize("operation,mutation", (
    ("update", "configuration-regression"),
    ("update", "deployment-regression"),
    ("update", "reporter-reuse"),
    ("update", "token-reuse"),
    ("capacity", "admission-change"),
    ("capacity", "token-change"),
    ("destroy", "admission-change"),
    ("destroy", "token-change"),
    ("destroy", "active-state"),
    ("destroy", "recreate"),
))
async def test_sql_rejects_resealed_lifecycle_changes(capacity_session, operation, mutation):
    row, old_request, _old_result = await _staged_transition(capacity_session, operation)
    projection = row.request_payload["command"]["projection"]
    ack = row.request_payload["command"]["acknowledgement"]
    config = row.result_payload["member"]["configuration"]
    result_ack = row.result_payload["member"]["acknowledgement"]
    if mutation == "configuration-regression":
        row.configuration_generation = 1
        projection["configuration_generation"] = projection["operation_epoch"] = config["configuration_generation"] = ack["configuration_generation"] = result_ack["configuration_generation"] = 1
    elif mutation == "deployment-regression":
        row.deployment_generation = 1
        projection["deployment_generation"] = config["deployment_generation"] = ack["deployment_generation"] = result_ack["deployment_generation"] = 1
    elif mutation == "reporter-reuse":
        row.reporter_incarnation = old_request.command.projection.demand_reporter_incarnation
        projection["demand_reporter_incarnation"] = config["demand_reporter_incarnation"] = ack["reporter_incarnation"] = result_ack["reporter_incarnation"] = str(row.reporter_incarnation)
    elif mutation in {"token-reuse", "token-change"}:
        projection["demand_reporter_token_sha256"] = old_request.command.projection.demand_reporter_token_sha256 if mutation == "token-reuse" else "f" * 64
    elif mutation == "admission-change":
        ack["protected_admission_sha256"] = result_ack["protected_admission_sha256"] = "f" * 64
    elif mutation == "active-state":
        config["lifecycle_state"] = "active"
    else:
        projection["operation_kind"] = "create"
        config["lifecycle_state"] = "active"
        config["max_slots"] = projection["max_slots"]
    _reseal(row)
    with pytest.raises(DBAPIError) as error:
        async with capacity_session.begin_nested():
            capacity_session.add(row)
            await capacity_session.flush()
    assert error.value.orig.sqlstate == "23514"


@pytest.mark.parametrize("field,changed", (("state", "current"), ("state", "equivocal"), ("token_sha256", "f" * 64), ("configuration_generation", 99)))
async def test_sql_update_requires_exact_fenced_retired_reporter(capacity_session, field, changed):
    row, old_request, _old_result = await _staged_transition(capacity_session, "update")
    old = (await capacity_session.scalars(select(CapacityDemandReporter).where(
        CapacityDemandReporter.reporter_incarnation == old_request.command.projection.demand_reporter_incarnation,
    ))).one()
    setattr(old, field, changed)
    await capacity_session.flush()
    with pytest.raises(DBAPIError):
        async with capacity_session.begin_nested():
            capacity_session.add(row)
            await capacity_session.flush()


@pytest.mark.parametrize("target", ("candidate", "deployment", "profile", "subject", "account"))
async def test_sql_lifecycle_rejects_retained_json_numeric_aliases(capacity_session, target):
    from copy import deepcopy

    from sqlalchemy.orm.attributes import flag_modified

    from loom_capacity_manager.models import (
        CapacityAccountPolicy,
        CapacityCandidate,
        CapacityDeploymentGeneration,
        CapacitySubject,
        CapacityWorkerProfile,
    )

    row, _request, _result = await _staged_transition(capacity_session, "update")
    if target == "account":
        retained = (await capacity_session.scalars(select(CapacityAccountPolicy).where(
            CapacityAccountPolicy.account_id == row.result_payload["member"]["configuration"]["account_id"],
        ))).one()
        field = "payload"
    else:
        model, field = {
            "candidate": (CapacityCandidate, "artifact_payload"),
            "deployment": (CapacityDeploymentGeneration, "required_profiles"),
            "profile": (CapacityWorkerProfile, "shape_catalog"),
            "subject": (CapacitySubject, "payload"),
        }[target]
        query = select(model).where(model.subject_id == row.subject_id)
        if target in {"deployment", "profile"}:
            query = query.where(model.deployment_generation == 2)
        retained = (await capacity_session.scalars(query)).first()
    payload = deepcopy(getattr(retained, field))
    if target == "candidate":
        payload["runtime_candidate"]["schema_version"] = 2.0
    elif target == "deployment":
        payload[0]["worker_shapes"][0]["concurrency_slots"] = 1.0
    elif target == "profile":
        payload[0]["concurrency_slots"] = 1.0
    else:
        payload["max_slots"] = float(payload["max_slots"])
    setattr(retained, field, payload)
    flag_modified(retained, field)
    await capacity_session.flush()
    with pytest.raises(DBAPIError) as error:
        async with capacity_session.begin_nested():
            capacity_session.add(row)
            await capacity_session.flush()
    assert error.value.orig.sqlstate == "23514"


async def test_sql_build_canonical_comparator_is_private_and_null_exact(capacity_session):
    from sqlalchemy import text

    row = (await capacity_session.execute(text(
        "SELECT prosecdef,proconfig,EXISTS (SELECT 1 FROM aclexplode(coalesce(proacl,acldefault('f',proowner))) "
        "WHERE grantee=0 AND privilege_type='EXECUTE') FROM pg_proc "
        "WHERE oid='public.capacity_personal_build_json_exact(jsonb,jsonb)'::regprocedure"
    ))).one()
    assert not row[0] and "search_path=pg_catalog" in row[1] and not row[2]
    assert (await capacity_session.execute(text(
        "SELECT public.capacity_personal_build_json_exact(NULL,NULL), "
        "public.capacity_personal_build_json_exact(NULL,'{}'::jsonb), "
        "public.capacity_personal_build_json_exact('1'::jsonb,'1.0'::jsonb), "
        "public.capacity_personal_build_json_exact('1'::jsonb,'true'::jsonb)"
    ))).one() == (True, False, False, False)
