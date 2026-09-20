"""Real authenticated control-plane route and durable commit-before-receipt."""

import hashlib
import importlib
from datetime import timedelta
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
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)

RAW_TOKEN = "disposable-execution-worker-token"


async def test_provisional_scheduler_claim_does_not_wait_on_publication_signing_fence(
    registry_authority_session, registry_issuer, tmp_path, monkeypatch,
):
    import asyncio

    from loom_control_plane.routes.workers import _REQUEUE_TRIAL_RETRY_SQL
    from loom_control_plane.scheduler.claim import claim_work

    factory = registry_authority_session
    _, _, request, engine = await setup(factory, registry_issuer, tmp_path, monkeypatch)
    claim = request.claim
    try:
        async with factory.begin() as session:
            await session.execute(_REQUEUE_TRIAL_RETRY_SQL, dict(
                trial_id=UUID(claim.trial_id), worker_id=UUID(claim.worker_id),
                failure_reason="node_setup_health", failure_message="disposable fixture requeue", retry_after_sec=0,
            ))
            worker = await session.get(Worker, UUID(claim.worker_id))
            digest = worker.capability_snapshot_digest
        async with engine.begin() as blocker:
            await blocker.execute(text("SELECT singleton_id FROM task_image_publication_state FOR UPDATE"))
            async with asyncio.timeout(1), factory.begin() as session:
                result = await claim_work(session, worker_id=UUID(claim.worker_id),
                    capability_snapshot_digest=digest, worker_token_hash=hashlib.sha256(RAW_TOKEN.encode()).digest(),
                    supported_work_kinds=["trial", "execution_attempt"], free_slots=1,
                    worker_os=["linux"], worker_cpu_arches=["x86_64"], worker_gpu_vendors=["none"],
                    worker_network_policies=["public"], allow_signed_task_images=True)
                assert result is not None
                assert result[0]["claim_id"] != UUID(claim.claim_id)
        # Selection is only a provisional claim. It never signs, reads a
        # mutable image snapshot, or consumes runtime authority under its locks.
    finally:
        await engine.dispose()


async def setup(factory, issuer, tmp_path, monkeypatch, *, time_shift=None):
    name = "loom_control_plane.task_image_execution"
    assert importlib.util.find_spec(name) is not None, "bounded control-plane execution service missing"
    module = importlib.import_module(name)
    _, policy, _, common, engine = await signer_setup(factory, issuer, tmp_path, monkeypatch)
    original_clock = common["clock"]
    def clock():
        return original_clock() + timedelta(seconds=time_shift[0] if time_shift else 0)
    policy._clock = clock
    token_hash = hashlib.sha256(RAW_TOKEN.encode()).digest()
    async with factory.begin() as session:
        session.add(Token(token_hash=token_hash, type="worker", scopes=["worker:claim", "worker:report"], issued_at=common["clock"]()))
        await session.execute(update(Worker).where(Worker.id == UUID(common["claim"].worker_id)).values(auth_token_hash=token_hash))

    class Signer:
        def __init__(self):
            self.policy = policy

        async def sign_execution(self, request, *, maximum_reply_bytes):
            return await policy.sign_execution(request)

    service = module.TaskImageExecutionService(engine, trust_root=common["trust_root"],
        purpose="production", shadow_campaign_id=None, signer=Signer(), clock=clock)
    delivery = await service.issue(claim=common["claim"], worker_token_hash=token_hash)
    async with factory.begin() as session:
        row = (await session.scalars(select(TaskImageExecutionGrant))).one()
        grant = TaskImageExecutionGrantV2.model_validate_json(row.canonical_grant)
    request = start_request(grant, delivery.grant_envelope.encode())
    app = FastAPI()
    app.state.task_image_execution = service
    app.state.session_factory = factory
    app.include_router(importlib.import_module("loom_control_plane.routes.task_image_execution").router)
    return app, service, request, engine


@pytest.mark.parametrize("change", ["expired", "near-expiry", "keyset-rollover", "consumed", "stale-claim", "wrong-digest", "revoked-token"])
async def test_authenticated_refresh_preserves_claim_and_never_reopens_consumed_start(
    registry_authority_session, registry_issuer, tmp_path, monkeypatch, change,
):
    from loom_task_image_authority.execution_grant import verify_execution_grant
    from loom_task_image_authority.execution_refresh import ExecutionRefreshRequest

    factory, shift = registry_authority_session, [0]
    app, service, old, engine = await setup(factory, registry_issuer, tmp_path, monkeypatch, time_shift=shift)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="https://control-plane.test") as http:
            client = HttpControlPlaneClient(base_url="https://control-plane.test", token=RAW_TOKEN, _client=http)
            if change == "consumed":
                await client.consume_task_image_execution_start(old)
            elif change == "stale-claim":
                async with factory.begin() as session:
                    await session.execute(update(Trial).values(legacy_claim_id=uuid4()))
            elif change == "wrong-digest":
                old = old.model_copy(update={"envelope_sha256": "a" * 64})
            elif change == "revoked-token":
                async with factory.begin() as session:
                    await session.execute(update(Token).values(revoked_at=service._clock()))
            shift[0] = 100 if change == "near-expiry" else 121
            if change == "keyset-rollover":
                from loom_task_image_authority.publication_keyset_store import (
                    finalize_keyset,
                    prepare_keyset,
                )
                from tests.unit.test_task_image_publication_keyset import _sign, _time

                shift[0] = 1801
                async with factory.begin() as session:
                    prepared = await prepare_keyset(session, trust_root=service._root)
                wire = _sign(dict(schema="loom.task-image-publication-keyset/v1", environment=service._root.environment,
                    keyset_version=prepared.proposed_state.keyset_version, revocation_epoch=prepared.proposed_state.revocation_epoch,
                    issued_at=_time(service._clock()), expires_at=_time(service._clock() + timedelta(minutes=5)),
                    keys=[key.model_dump(mode="json", exclude_none=True) for key in prepared.keys]), service._signer.policy._execution.private)
                async with factory.begin() as session:
                    await finalize_keyset(session, preparation=prepared, wire=wire, trust_root=service._root, clock=service._clock)
            request = ExecutionRefreshRequest.model_validate(dict(schema="loom.task-image-execution-refresh-request/v1", previous=old))
            if change not in {"expired", "near-expiry", "keyset-rollover"}:
                with pytest.raises(httpx.HTTPStatusError):
                    await client.refresh_task_image_execution(request)
                return
            delivery = await client.refresh_task_image_execution(request)
            current = verify_execution_grant(wire=delivery.grant_envelope.encode(), plan_wire=delivery.frozen_plan.encode(),
                publication_wires=tuple(item.encode() for item in delivery.publications), keyset_wire=delivery.keyset.encode(),
                trust_root=service._root, expected_claim=old.claim, expected_purpose="production", expected_shadow_campaign_id=None, now=service._clock())
            assert current.grant.grant_id == old.grant_id and current.grant.revision == old.revision + 1
            if change == "keyset-rollover":
                assert current.grant.keyset_version > old.keyset_version
            # A lost refresh acknowledgement may replay current issuance, but
            # cannot consume the previous revision or recover a lost start.
            assert await client.refresh_task_image_execution(request) == delivery
            with pytest.raises(httpx.HTTPStatusError):
                await client.consume_task_image_execution_start(old)
            await client.consume_task_image_execution_start(start_request(current.grant, delivery.grant_envelope.encode()))
            with pytest.raises(httpx.HTTPStatusError):
                await client.refresh_task_image_execution(request)
    finally:
        await engine.dispose()


@pytest.mark.parametrize("reader", ["v2", "trial-only", "legacy", "disabled", "digest-drift", "signer-unavailable"])
async def test_shared_claim_delivers_signed_native_images_only_to_registered_v2_reader(
    registry_authority_session, registry_issuer, tmp_path, monkeypatch, reader,
):
    from loom.pipeline.keys import canonical_digest
    from loom_control_plane.routes.workers import _REQUEUE_TRIAL_RETRY_SQL, router
    from loom_task_image_authority.execution_delivery import SignedWorkClaim

    factory = registry_authority_session
    app, service, old_request, engine = await setup(factory, registry_issuer, tmp_path, monkeypatch)
    app.include_router(router)
    claim = old_request.claim
    try:
        async with factory.begin() as session:
            await session.execute(_REQUEUE_TRIAL_RETRY_SQL, dict(
                trial_id=UUID(claim.trial_id), worker_id=UUID(claim.worker_id),
                failure_reason="node_setup_health", failure_message="disposable fixture requeue",
                retry_after_sec=0,
            ))
            worker = await session.get(Worker, UUID(claim.worker_id))
            if reader == "trial-only":
                worker.supported_work_kinds = ["trial"]
            if reader in {"legacy", "digest-drift"}:
                snapshot = dict(worker.capability_snapshot_json)
                if reader == "legacy":
                    snapshot["container_runtime_features"] = []
                else:
                    snapshot["cpu_cores"] += 1
                worker.capability_snapshot_json = snapshot
                if reader == "legacy":
                    worker.capability_snapshot_digest = canonical_digest(snapshot)
            digest = worker.capability_snapshot_digest
        if reader == "disabled":
            app.state.task_image_execution = None
        if reader == "signer-unavailable":
            async def unavailable(*args, **kwargs):
                raise ConnectionError("disposable signer unavailable")

            service._signer.sign_execution = unavailable
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="https://control-plane.test") as http:
            client = HttpControlPlaneClient(base_url="https://control-plane.test", token=RAW_TOKEN, _client=http)
            if reader in {"legacy", "disabled"}:
                assert await client.claim_work(worker_id=UUID(claim.worker_id), capability_snapshot_digest=digest, free_slots=1) is None
            elif reader in {"digest-drift", "signer-unavailable"}:
                with pytest.raises(httpx.HTTPStatusError):
                    await client.claim_work(worker_id=UUID(claim.worker_id), capability_snapshot_digest=digest, free_slots=1)
                async with factory.begin() as session:
                    trial = await session.get(Trial, UUID(claim.trial_id))
                    assert trial.state == "queued" and trial.attempt_count == 0
            else:
                body = await client.claim_work(worker_id=UUID(claim.worker_id), capability_snapshot_digest=digest, free_slots=1,
                    supported_work_kinds=["trial"] if reader == "trial-only" else ["trial", "execution_attempt"])
                import json

                signed = SignedWorkClaim.model_validate_json(json.dumps(body))
                delivery = signed.payload.task_image_execution
                assert delivery.claim.claim_id != claim.claim_id
                assert delivery.claim.trial_attempt_count == claim.trial_attempt_count
                assert signed.payload.task_image_materialization is None
                envelope = json.loads(delivery.grant_envelope)
                grant = TaskImageExecutionGrantV2.model_validate_json(envelope["canonical_grant"])
                receipt = await client.consume_task_image_execution_start(start_request(grant, delivery.grant_envelope.encode()))
                assert receipt.request_sha256 != old_request.digest
                # The previous claim's otherwise valid signed evidence cannot
                # consume authority after a refundable scheduler re-claim.
                with pytest.raises(httpx.HTTPStatusError):
                    await client.consume_task_image_execution_start(old_request)
    finally:
        await engine.dispose()


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
