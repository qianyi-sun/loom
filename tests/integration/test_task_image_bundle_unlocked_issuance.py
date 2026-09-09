"""Actual HTTP/SQL concurrency: bundle storage never owns heartbeat locks."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event
from uuid import uuid4

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Secret, TaskImageMaterialization, TaskImageMaterializationOperationEvent
from loom_task_image_authority.api import create_app
from loom_task_image_authority.bundle_capability import AsyncTaskImageBundleCapabilityProvider, TaskImageBundleObject
from tests.integration.test_task_image_authority_api import (
    TaskImageMaterializationClaimResponseV1,
    TestClient,
    _HEADERS,
    _FakeBundleBackend,
    _claim_request,
    _operation_request,
    _post,
    _put,
    _renewed_session,
    _seed_materialization,
)
from tests.integration.test_task_image_authority_api import authority_api as authority_api
from tests.integration.test_task_image_authority_api import registry_token_issuer as registry_token_issuer
from tests.integration.test_task_image_projection_store import GRANT_ID

pytestmark = pytest.mark.usefixtures("authority_api")


class WaitingBackend:
    def __init__(self, clock):
        self.entered, self.two_entered, self.release = Event(), Event(), Event()
        self.calls = 0
        self.signer = _FakeBundleBackend(clock=clock)

    async def list_objects(self, *, bucket, prefix, maximum_objects, maximum_bytes, expires_at):
        self.calls += 1
        self.entered.set()
        if self.calls >= 2:
            self.two_entered.set()
        while not self.release.is_set():
            await asyncio.sleep(0.01)
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


@pytest.mark.parametrize("during_io", ["heartbeat", "release", "source_changed", "expired"])
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
            else:
                context.now[0] += timedelta(seconds=1000)
        finally:
            backend.release.set()
        response = pending.result(timeout=10)
    assert response.status_code == (200 if during_io == "heartbeat" else 409 if during_io in {"release", "source_changed"} else 503)
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
