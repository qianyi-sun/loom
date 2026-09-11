"""A registered worker claims only the work already assigned by management."""

import json
from importlib import import_module
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_manager.contracts import canonical_digest
from tests.integration.test_personal_dev_build_guard_claims import claim_input
from tests.integration.test_personal_dev_build_guard_execution import store
from tests.integration.test_personal_dev_build_guard_installations import owner_sessions as owner_sessions
from tests.integration.test_personal_dev_build_guard_migrations import build_guard_database as build_guard_database
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


@pytest.mark.parametrize("boundary", ["exact", "credential", "worker", "incarnation", "binding", "cancelled", "expired"])
async def test_assigned_claim_resolves_only_authenticated_worker_allocation(prepared_input, monkeypatch, boundary):
    factory, engine, installation, _plan, source, platform = prepared_input
    claim = await claim_input(prepared_input, monkeypatch)
    module = import_module("loom_capacity_agent.build_admission")
    payload = claim.model_dump(mode="json", exclude={"request_id"})
    if boundary in {"worker", "incarnation"}:
        payload["worker_id" if boundary == "worker" else "worker_incarnation"] = str(uuid4())
    if boundary == "binding":
        payload["binding"]["account_id"] = "foreign"
    if boundary == "cancelled":
        with engine.begin() as connection:
            connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": platform.id})
    if boundary == "expired":
        with engine.begin() as connection:
            connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=now()-interval '1 second' WHERE id=:id"), {"id": source.build_attempt.id})
    request = module.BuildAllocatedClaimRequestV1.model_validate_json(json.dumps(payload))
    async with factory.begin() as session:
        execution = store(session, installation)
        if boundary != "exact":
            with pytest.raises((ValueError, DBAPIError)):
                await execution.claim_assigned_platform(request,
                    worker_credential="x" * 43 if boundary == "credential" else CREDENTIAL)
        else:
            receipt = await execution.claim_assigned_platform(request, worker_credential=CREDENTIAL)
            assert receipt.request == claim
            assert receipt.request_digest == canonical_digest(claim)
            # Claim retention is not fresh source IO until committed.
            with pytest.raises(DBAPIError, match="committed"):
                await execution.read_source_context(receipt.request, worker_credential=CREDENTIAL)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.platform_claims")) == int(boundary == "exact")
    if boundary == "exact":
        async with factory.begin() as session:
            execution = store(session, installation)
            assert await execution.claim_assigned_platform(request, worker_credential=CREDENTIAL) == receipt
            assert (await execution.read_source_context(receipt.request, worker_credential=CREDENTIAL)).request_id == platform.id


@pytest.mark.parametrize("boundary", ["exact", "lost-reply", "cancelled", "replay-after-cancel", "operation-reuse"])
async def test_assigned_claim_http_commit_and_replay_do_not_renew_authority(prepared_input, tmp_path, monkeypatch, boundary):
    import httpx

    from loom_capacity_agent.build_admission import BuildAllocatedClaimRequestV1
    from tests.integration.test_personal_dev_build_guard_http import application
    from tests.unit.test_capacity_build_admission_client import client_for

    factory, engine, _installation, _plan, _source, platform = prepared_input
    claim = await claim_input(prepared_input, monkeypatch)
    request = BuildAllocatedClaimRequestV1.model_validate_json(claim.model_dump_json(exclude={"request_id"}))
    app = application(prepared_input, tmp_path)
    app.state.personal_dev_build_admission_mode = "native-source"
    received = []
    async def receive(response):
        received.append(response.status_code)
        if boundary == "lost-reply" and len(received) == 1:
            raise httpx.ReadTimeout("reply lost after commit", request=response.request)
    def cancel():
        with engine.begin() as connection:
            connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": platform.id})
    if boundary == "cancelled":
        cancel()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), event_hooks={"response": [receive]}) as http:
        client = client_for(http, request)
        client._token = "executor-secret"
        if boundary in {"cancelled", "lost-reply"}:
            with pytest.raises(RuntimeError):
                await client.claim_assigned_platform(request, worker_credential=CREDENTIAL)
        if boundary == "cancelled":
            assert received == [409]
        else:
            receipt = await client.claim_assigned_platform(request, worker_credential=CREDENTIAL)
            assert receipt.request == claim
            if boundary == "replay-after-cancel":
                cancel()
            if boundary == "operation-reuse":
                with pytest.raises(RuntimeError):
                    await client.claim_assigned_platform(request.model_copy(update={"operation_id": uuid4()}), worker_credential=CREDENTIAL)
            else:
                assert await client.claim_assigned_platform(request, worker_credential=CREDENTIAL) == receipt
            if boundary == "replay-after-cancel":
                with pytest.raises(RuntimeError):
                    await client.read_source_context(receipt.request, worker_credential=CREDENTIAL)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.platform_claims")) == int(boundary != "cancelled")
