"""Actual HTTP/SQL concurrency: bundle storage never owns heartbeat locks."""

import asyncio
from concurrent.futures import CancelledError, ThreadPoolExecutor
from datetime import timedelta
from threading import Event
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, insert, select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Secret, TaskImageMaterialization, TaskImageMaterializationOperationEvent
from loom.security.secret_store import LocalEncryptedSecretStore
from loom_task_image_authority.api import create_app
from loom_task_image_authority.bundle_capability import (
    AsyncTaskImageBundleCapabilityProvider,
    TaskImageBundleCapabilityV1,
    TaskImageBundleObject,
)
from loom_task_image_authority.contracts import TaskImageBuildSessionV2, TaskImageSessionRenewalV1
from tests.integration.test_task_image_authority_api import (
    _HEADERS,
    TaskImageMaterializationClaimResponseV1,
    TestClient,
    _claim_request,
    _FakeBundleBackend,
    _operation_request,
    _post,
    _put,
    _renewed_session,
    _seed_materialization,
)
from tests.integration.test_task_image_authority_api import authority_api as authority_api
from tests.integration.test_task_image_authority_api import (
    registry_token_issuer as registry_token_issuer,
)
from tests.integration.test_task_image_projection_store import (
    GRANT_ID,
    _attestation,
    _proof,
    _revocation,
)
from tests.integration.test_task_image_retired_credential_ingress import _retire

pytestmark = pytest.mark.usefixtures("authority_api")


class WaitingBackend:
    def __init__(self, clock):
        self.entered, self.two_entered, self.release = Event(), Event(), Event()
        self.cancelled = Event()
        self.calls = 0
        self.signer = _FakeBundleBackend(clock=clock)

    async def list_objects(self, *, bucket, prefix, maximum_objects, maximum_bytes, expires_at):
        self.calls += 1
        self.entered.set()
        if self.calls >= 2:
            self.two_entered.set()
        try:
            while not self.release.is_set():
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            self.cancelled.set()
            raise
        return (TaskImageBundleObject(key=prefix + "task.toml", size_bytes=20),)

    def presign_get(self, **options):
        return self.signer.presign_get(**options)


def _app(context, backend):
    return create_app(
        context.settings, now_factory=lambda: context.now[0],
        bundle_capability_provider=AsyncTaskImageBundleCapabilityProvider(
            backend=backend, public_https_origin="https://objects.example", expected_bucket="loom-bundles",
            maximum_objects=2000, maximum_bytes=536870912, url_expiry_seconds=600,
            clock=lambda: context.now[0], addressing_style="bucket-host",
        ),
    )


async def _claim(context, database_url):
    materialization_id = await _seed_materialization(database_url)
    build_session = _renewed_session(context)
    response = _post(context, f"/v1/projections/{GRANT_ID}/materializations/claim", _claim_request(build_session))
    assert response.status_code == 200
    claim = TaskImageMaterializationClaimResponseV1.model_validate_json(response.content)
    response = _put(context, f"/v1/projections/{GRANT_ID}/materializations/{materialization_id}/start", _operation_request(build_session, claim, operation_id=uuid4()))
    assert response.status_code == 200
    return build_session, claim


async def _counts(database_url, operation_id):
    engine = create_async_engine(database_url)
    try:
        async with async_sessionmaker(engine)() as session:
            events = await session.scalar(select(func.count()).select_from(TaskImageMaterializationOperationEvent).where(TaskImageMaterializationOperationEvent.operation_id == operation_id))
            secrets = await session.scalar(select(func.count()).select_from(Secret).where(Secret.ref.startswith("loom://task-image-bundle-capability/")))
            return events, secrets
    finally:
        await engine.dispose()


@pytest.mark.parametrize("during_io", ["heartbeat", "release", "source_changed", "expired", "retired", "revoked"])
async def test_storage_wait_releases_authority_and_rechecks_before_persistence(authority_api, isolated_migration_postgres_url, during_io):
    context = authority_api
    build_session, claim = await _claim(context, isolated_migration_postgres_url)
    operation_id = uuid4()
    body = _operation_request(build_session, claim, operation_id=operation_id)
    base = f"/v1/projections/{GRANT_ID}/materializations/{claim.materialization_id}"
    backend = WaitingBackend(lambda: context.now[0])
    with TestClient(_app(context, backend)) as client, ThreadPoolExecutor(max_workers=2) as pool:
        pending = pool.submit(client.put, base + "/bundle", headers=_HEADERS, json=body.model_dump(mode="json"))
        try:
            assert backend.entered.wait(5), "HTTP route never awaited the async listing backend"
            if during_io in {"heartbeat", "release"}:
                other = pool.submit(_put, context, base + "/" + during_io, _operation_request(build_session, claim, operation_id=uuid4()))
                assert other.result(timeout=5).status_code == 200, "storage I/O retained authority locks"
            elif during_io == "source_changed":
                engine = create_async_engine(isolated_migration_postgres_url)
                try:
                    async with async_sessionmaker(engine)() as session:
                        await session.execute(update(TaskImageMaterialization).where(TaskImageMaterialization.id == claim.materialization_id).values(task_source="s3://loom-bundles/changed/"))
                        await session.commit()
                finally:
                    await engine.dispose()
            elif during_io == "retired":
                engine = create_async_engine(isolated_migration_postgres_url)
                try:
                    await _retire(async_sessionmaker(engine, expire_on_commit=False), claim.attempt_id)
                finally:
                    await engine.dispose()
            elif during_io == "revoked":
                revoked = pool.submit(_put, context, f"/v1/projections/{GRANT_ID}/revocation", _revocation(observed_at=context.now[0]))
                assert revoked.result(timeout=5).status_code == 204
            else:
                context.now[0] += timedelta(seconds=1000)
        finally:
            backend.release.set()
        response = pending.result(timeout=10)
    assert response.status_code == (200 if during_io == "heartbeat" else 409 if during_io in {"release", "source_changed", "retired"} else 403 if during_io == "revoked" else 503)
    assert await _counts(isolated_migration_postgres_url, operation_id) == ((1, 1) if during_io == "heartbeat" else (0, 0))


async def test_concurrent_bundle_operation_has_one_encrypted_winner_and_replay(authority_api, isolated_migration_postgres_url):
    context = authority_api
    build_session, claim = await _claim(context, isolated_migration_postgres_url)
    operation_id = uuid4()
    body = _operation_request(build_session, claim, operation_id=operation_id).model_dump(mode="json")
    path = f"/v1/projections/{GRANT_ID}/materializations/{claim.materialization_id}/bundle"
    backend = WaitingBackend(lambda: context.now[0])
    with TestClient(_app(context, backend)) as client, ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(client.put, path, headers=_HEADERS, json=body)
        second = pool.submit(client.put, path, headers=_HEADERS, json=body)
        try:
            assert backend.two_entered.wait(5), "concurrent listing blocked behind a database lock"
        finally:
            backend.release.set()
        responses = [first.result(timeout=10), second.result(timeout=10)]
        assert [response.status_code for response in responses] == [200, 200]
        assert responses[0].json() == responses[1].json()
        replay = client.put(path, headers=_HEADERS, json=body)
        assert replay.status_code == 200 and replay.json() == responses[0].json()
        assert backend.calls == 2
    assert await _counts(isolated_migration_postgres_url, operation_id) == (1, 1)


@pytest.mark.parametrize("boundary", ["put", "commit", "replay_get", "response"])
async def test_expiry_at_persistence_commit_replay_or_response_never_returns_capability(authority_api, isolated_migration_postgres_url, monkeypatch, boundary):
    from sqlalchemy.ext.asyncio import AsyncSession

    context = authority_api
    build_session, claim = await _claim(context, isolated_migration_postgres_url)
    operation_id = uuid4()
    body = _operation_request(build_session, claim, operation_id=operation_id)
    path = f"/v1/projections/{GRANT_ID}/materializations/{claim.materialization_id}/bundle"
    if boundary == "replay_get":
        assert _put(context, path, body).status_code == 200
    original_put, original_get = LocalEncryptedSecretStore.put, LocalEncryptedSecretStore.get
    original_commit = AsyncSession.commit
    original_dump = TaskImageBundleCapabilityV1.model_dump_json
    committed = []

    async def put(store, *, namespace, value):
        ref = await original_put(store, namespace=namespace, value=value)
        if namespace == "task-image-bundle-capability":
            deadline = TaskImageBundleCapabilityV1.model_validate_json(value).expires_at
            store._session.info["bundle_test_deadline"] = deadline
            if boundary == "put":
                context.now[0] = deadline
        return ref

    async def get(store, ref):
        value = await original_get(store, ref)
        if ref.startswith("loom://task-image-bundle-capability/") and boundary == "replay_get":
            context.now[0] = TaskImageBundleCapabilityV1.model_validate_json(value).expires_at
        return value

    async def commit(session):
        await original_commit(session)
        deadline = session.info.get("bundle_test_deadline")
        if deadline is not None:
            committed.append(deadline)
            if boundary == "commit":
                context.now[0] = deadline

    def dump(capability, *args, **kwargs):
        result = original_dump(capability, *args, **kwargs)
        if committed and boundary == "response":
            context.now[0] = committed[-1]
        return result

    monkeypatch.setattr(LocalEncryptedSecretStore, "put", put)
    monkeypatch.setattr(LocalEncryptedSecretStore, "get", get)
    monkeypatch.setattr(AsyncSession, "commit", commit)
    monkeypatch.setattr(TaskImageBundleCapabilityV1, "model_dump_json", dump)
    response = _put(context, path, body)
    assert response.status_code == 503
    assert response.json() == {"detail": "task-image authority unavailable"}
    assert await _counts(isolated_migration_postgres_url, operation_id) == ((0, 0) if boundary == "put" else (1, 1))


@pytest.mark.parametrize("collision", ["operation_id", "primary_id"])
async def test_real_unique_constraint_failure_rolls_back_secret_and_maps_only_operation_collision(authority_api, isolated_migration_postgres_url, monkeypatch, collision):
    from sqlalchemy.ext.asyncio import AsyncSession

    context = authority_api
    build_session, claim = await _claim(context, isolated_migration_postgres_url)
    operation_id = uuid4()
    body = _operation_request(build_session, claim, operation_id=operation_id)
    original = AsyncSession.flush
    injected = []

    async def collide(session, *args, **kwargs):
        events = [row for row in session.new if isinstance(row, TaskImageMaterializationOperationEvent) and row.operation_id == operation_id]
        await original(session, *args, **kwargs)
        if events and not injected:
            injected.append(True)
            values = {key: value for key, value in vars(events[0]).items() if not key.startswith("_")}
            values["id" if collision == "operation_id" else "operation_id"] = uuid4()
            # Exercise the actual PostgreSQL constraint/asyncpg exception, not a
            # synthetic IntegrityError. This is a collision-path fault injection,
            # not evidence of a complete second-grant HTTP setup.
            await session.execute(insert(TaskImageMaterializationOperationEvent).values(**values))

    monkeypatch.setattr(AsyncSession, "flush", collide)
    response = _put(context, f"/v1/projections/{GRANT_ID}/materializations/{claim.materialization_id}/bundle", body)
    assert injected
    assert response.status_code == (409 if collision == "operation_id" else 503)
    assert await _counts(isolated_migration_postgres_url, operation_id) == (0, 0)


@pytest.mark.parametrize("clock_change", ["expired", "regressed"])
async def test_final_admission_refreshes_clock_after_real_parent_lock_wait(authority_api, isolated_migration_postgres_url, clock_change):
    context = authority_api
    build_session, claim = await _claim(context, isolated_migration_postgres_url)
    operation_id = uuid4()
    body = _operation_request(build_session, claim, operation_id=operation_id)
    path = f"/v1/projections/{GRANT_ID}/materializations/{claim.materialization_id}/bundle"
    backend = WaitingBackend(lambda: context.now[0])
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        with TestClient(_app(context, backend)) as client, ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(client.put, path, headers=_HEADERS, json=body.model_dump(mode="json"))
            try:
                assert backend.entered.wait(5)
                async with async_sessionmaker(engine)() as holder:
                    await holder.scalar(select(TaskImageMaterialization).where(TaskImageMaterialization.id == claim.materialization_id).with_for_update())
                    blocker = await holder.scalar(text("SELECT pg_backend_pid()"))
                    backend.release.set()
                    async with async_sessionmaker(engine)() as probe, asyncio.timeout(5):
                        while not await probe.scalar(text("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE :blocker = ANY(pg_blocking_pids(pid)))"), {"blocker": blocker}):
                            assert not pending.done(), "bundle did not wait on the final parent fence"
                            await asyncio.sleep(0.01)
                    context.now[0] += timedelta(seconds=1000 if clock_change == "expired" else -1)
                    await holder.commit()
            finally:
                backend.release.set()
            response = pending.result(timeout=10)
        assert response.status_code == 503
        assert await _counts(isolated_migration_postgres_url, operation_id) == (0, 0)
    finally:
        await engine.dispose()


async def test_renewal_invalidates_inflight_bearer_but_successor_can_issue_for_original_attempt(authority_api, isolated_migration_postgres_url):
    context = authority_api
    original_session, claim = await _claim(context, isolated_migration_postgres_url)
    old_operation, new_operation = uuid4(), uuid4()
    path = f"/v1/projections/{GRANT_ID}/materializations/{claim.materialization_id}/bundle"
    backend = WaitingBackend(lambda: context.now[0])
    with TestClient(_app(context, backend)) as client, ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(client.put, path, headers=_HEADERS, json=_operation_request(original_session, claim, operation_id=old_operation).model_dump(mode="json"))
        try:
            assert backend.entered.wait(5)
            context.now[0] += timedelta(seconds=1)
            renewal = TaskImageSessionRenewalV1(
                renewal_id=uuid4(), grant_id=GRANT_ID, session_id=original_session.session_id,
                session_generation=original_session.generation, session_token=original_session.session_token,
                attestation=_attestation(_proof(), generation=3, attestation_id=uuid4(), issued_at=context.now[0]), observed_at=context.now[0],
            )
            renewed = client.put(f"/v1/projections/{GRANT_ID}/sessions/{original_session.generation}/renew", headers=_HEADERS, json=renewal.model_dump(mode="json"))
            assert renewed.status_code == 200
            successor = TaskImageBuildSessionV2.model_validate_json(renewed.content)
        finally:
            backend.release.set()
        assert pending.result(timeout=10).status_code == 403
        response = client.put(path, headers=_HEADERS, json=_operation_request(successor, claim, operation_id=new_operation).model_dump(mode="json"))
        assert response.status_code == 200
        capability = TaskImageBundleCapabilityV1.model_validate_json(response.content)
        assert capability.session_id == successor.session_id
        assert capability.session_generation == successor.generation
        assert capability.materialization_id == claim.materialization_id
    assert await _counts(isolated_migration_postgres_url, old_operation) == (0, 1)
    assert await _counts(isolated_migration_postgres_url, new_operation) == (1, 1)


async def test_cancelled_http_issuance_leaves_no_secret_or_operation(authority_api, isolated_migration_postgres_url):
    context = authority_api
    build_session, claim = await _claim(context, isolated_migration_postgres_url)
    operation_id = uuid4()
    body = _operation_request(build_session, claim, operation_id=operation_id).model_dump(mode="json")
    path = f"/v1/projections/{GRANT_ID}/materializations/{claim.materialization_id}/bundle"
    backend = WaitingBackend(lambda: context.now[0])
    app = _app(context, backend)
    with TestClient(app) as client:
        async def send():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://testserver") as request_client:
                return await request_client.put(path, headers=_HEADERS, json=body)

        pending = client.portal.start_task_soon(send)
        try:
            assert backend.entered.wait(5)
            pending.cancel()
            with pytest.raises(CancelledError):
                pending.result(timeout=5)
            assert backend.cancelled.wait(5)
        finally:
            backend.release.set()
    assert await _counts(isolated_migration_postgres_url, operation_id) == (0, 0)
