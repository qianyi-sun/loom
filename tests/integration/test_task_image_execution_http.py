"""Real authenticated control-plane route and durable commit-before-receipt."""

import hashlib
import importlib
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import select, text, update

from loom.db.schema import TaskImageExecutionGrant, TaskImageExecutionStart, Token, Trial, Worker
from loom_task_image_authority.execution_grant import TaskImageExecutionGrantV2
from loom_worker.control_plane_client import HttpControlPlaneClient
from tests.integration.test_task_image_execution_signer import setup as signer_setup
from tests.integration.test_task_image_execution_store import start_request
from tests.integration.test_task_image_publication_jobs import registry_authority_session as registry_authority_session
from tests.integration.test_task_image_registry_credentials import registry_issuer as registry_issuer

RAW_TOKEN = "disposable-execution-worker-token"


async def setup(factory, issuer, tmp_path, monkeypatch):
    name = "loom_control_plane.task_image_execution"
    assert importlib.util.find_spec(name) is not None, "bounded control-plane execution service missing"
    module = importlib.import_module(name)
    _, policy, _, common, engine = await signer_setup(factory, issuer, tmp_path, monkeypatch)
    token_hash = hashlib.sha256(RAW_TOKEN.encode()).digest()
    async with factory.begin() as session:
        session.add(Token(token_hash=token_hash, type="worker", scopes=["worker:claim", "worker:report"]))
        await session.execute(update(Worker).where(Worker.id == UUID(common["claim"].worker_id)).values(auth_token_hash=token_hash))

    class Signer:
        async def sign_execution(self, request, *, maximum_reply_bytes):
            return await policy.sign_execution(request)

    service = module.TaskImageExecutionService(engine, trust_root=common["trust_root"],
        purpose="production", shadow_campaign_id=None, signer=Signer(), clock=common["clock"])
    delivery = await service.issue(claim=common["claim"], worker_token_hash=token_hash)
    async with factory.begin() as session:
        row = (await session.scalars(select(TaskImageExecutionGrant))).one()
        grant = TaskImageExecutionGrantV2.model_validate_json(row.canonical_grant)
    request = start_request(grant, delivery.grant_envelope.encode())
    app = FastAPI()
    app.state.task_image_execution = service
    app.include_router(importlib.import_module("loom_control_plane.routes.task_image_execution").router)
    return app, service, request, engine


async def test_actual_worker_client_receives_only_committed_fresh_start(
    registry_authority_session, registry_issuer, tmp_path, monkeypatch,
):
    factory = registry_authority_session
    app, _, request, engine = await setup(factory, registry_issuer, tmp_path, monkeypatch)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="https://control-plane.test") as http:
            client = HttpControlPlaneClient(base_url="https://control-plane.test", token=RAW_TOKEN, _client=http)
            receipt = await client.consume_task_image_execution_start(request)
            async with factory.begin() as session:
                retained = (await session.scalars(select(TaskImageExecutionStart))).one()
                assert retained.start_id == UUID(receipt.start_id)
                assert receipt.request_sha256 == request.digest
            with pytest.raises(httpx.HTTPStatusError) as refused:
                await client.consume_task_image_execution_start(request)
            assert refused.value.response.status_code == 409
    finally:
        await engine.dispose()


@pytest.mark.parametrize("change", ["missing-token", "wrong-token", "scope", "revoked", "cancelled", "path", "disabled", "oversize", "duplicate", "commit-failure", "deadline"])
async def test_http_denial_never_acknowledges_or_retains_a_start(
    registry_authority_session, registry_issuer, tmp_path, monkeypatch, change,
):
    factory = registry_authority_session
    app, service, request, engine = await setup(factory, registry_issuer, tmp_path, monkeypatch)
    headers = {"Authorization": "Bearer " + RAW_TOKEN, "Content-Type": "application/json"}
    path = f"/trials/{request.claim.trial_id}/task-image/start"
    import json

    body = json.dumps(request.model_dump(mode="json", by_alias=True, exclude_none=True)).encode()
    blocker = None
    try:
        if change == "missing-token":
            headers.pop("Authorization")
        elif change == "wrong-token":
            headers["Authorization"] = "Bearer wrong-token"
        elif change == "path":
            path = f"/trials/{uuid4()}/task-image/start"
        elif change == "disabled":
            app.state.task_image_execution = None
        elif change == "oversize":
            body = b"x" * 8193
        elif change == "duplicate":
            body = body[:-1] + b',"revision":1}'
        elif change == "deadline":
            service._timeout = 0.2
            blocker = await engine.connect()
            await blocker.begin()
            await blocker.execute(text("SELECT singleton_id FROM task_image_publication_state FOR UPDATE"))
        else:
            async with factory.begin() as session:
                if change == "scope":
                    await session.execute(update(Token).values(scopes=["worker:report"]))
                elif change == "revoked":
                    await session.execute(update(Token).values(revoked_at=service._clock()))
                elif change == "cancelled":
                    await session.execute(update(Trial).values(state="cancelled"))
                else:
                    # Disposable database: force an actual deferred COMMIT
                    # failure after the store has produced its receipt.
                    await session.execute(text("CREATE FUNCTION reject_execution_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test commit refusal'; END $$"))
                    await session.execute(text("CREATE CONSTRAINT TRIGGER reject_execution_commit AFTER INSERT ON task_image_execution_starts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_execution_commit()"))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="https://control-plane.test") as client:
            response = await client.post(path, headers=headers, content=body)
        assert response.status_code in {401, 409, 413, 422, 503}
        async with factory.begin() as session:
            assert not (await session.scalars(select(TaskImageExecutionStart))).all()
    finally:
        if blocker is not None:
            await blocker.rollback()
            await blocker.close()
        await engine.dispose()
