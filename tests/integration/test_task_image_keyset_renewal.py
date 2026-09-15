"""Runtime renewal must keep real signed claims usable without extending old bytes."""

import asyncio
import importlib
from datetime import timedelta

import httpx
import pytest
from sqlalchemy import select, text

from loom.db.schema import TaskImagePublicationKeyset, TaskImagePublicationState
from loom_task_image_authority.execution_grant import verify_execution_grant
from loom_task_image_authority.execution_refresh import ExecutionRefreshRequest
from loom_worker.control_plane_client import HttpControlPlaneClient
from tests.integration.test_task_image_execution_http import RAW_TOKEN, setup
from tests.integration.test_task_image_execution_store import start_request
from tests.integration.test_task_image_publication_jobs import registry_authority_session as registry_authority_session
from tests.integration.test_task_image_registry_credentials import registry_issuer as registry_issuer


def publisher(service, engine):
    name = "loom_control_plane.task_image_keyset_renewal"
    assert importlib.util.find_spec(name) is not None, "runtime keyset renewal missing"
    module = importlib.import_module(name)

    class Signer:
        async def sign_keyset(self, request, *, maximum_reply_bytes):
            return await service._signer.policy.sign_keyset(request)

    return module.TaskImageKeysetPublisher(engine, trust_root=service._root, signer=Signer(), clock=service._clock)


@pytest.mark.parametrize("delay", [151, 1801])
async def test_renewed_keyset_recovers_actual_worker_claim_before_and_after_expiry(
    registry_authority_session, registry_issuer, tmp_path, monkeypatch, delay,
):
    shift = [0]
    app, service, old, engine = await setup(registry_authority_session, registry_issuer, tmp_path, monkeypatch, time_shift=shift)
    try:
        subject = publisher(service, engine)
        assert not subject.ready
        assert await subject.refresh_if_needed() is False
        assert subject.ready
        shift[0] = delay
        assert await subject.refresh_if_needed() is True
        assert subject.ready
        assert await subject.refresh_if_needed() is False
        async with registry_authority_session.begin() as session:
            rows = (await session.scalars(select(TaskImagePublicationKeyset).order_by(TaskImagePublicationKeyset.keyset_version))).all()
            assert len(rows) == 2
            assert rows[1].keyset_version == rows[0].keyset_version + 1
            assert rows[1].issued_at == service._clock()
            assert rows[0].expires_at < rows[1].expires_at
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="https://control-plane.test") as http:
            client = HttpControlPlaneClient("https://control-plane.test", RAW_TOKEN, _client=http)
            delivery = await client.refresh_task_image_execution(ExecutionRefreshRequest.model_validate(dict(
                schema="loom.task-image-execution-refresh-request/v1", previous=old)))
            current = verify_execution_grant(wire=delivery.grant_envelope.encode(), plan_wire=delivery.frozen_plan.encode(),
                publication_wires=tuple(item.encode() for item in delivery.publications), keyset_wire=delivery.keyset.encode(),
                trust_root=service._root, expected_claim=old.claim, expected_purpose="production", expected_shadow_campaign_id=None, now=service._clock())
            await client.consume_task_image_execution_start(start_request(current.grant, delivery.grant_envelope.encode()))
    finally:
        await engine.dispose()


async def test_keyset_signer_outage_disables_stale_readiness_and_can_recover(
    registry_authority_session, registry_issuer, tmp_path, monkeypatch,
):
    shift = [0]
    _, service, _, engine = await setup(registry_authority_session, registry_issuer, tmp_path, monkeypatch, time_shift=shift)
    try:
        subject = publisher(service, engine)
        await subject.refresh_if_needed()
        assert subject.ready
        shift[0] = 1801
        assert not subject.ready
        original = subject._signer.sign_keyset

        async def unavailable(*args, **kwargs):
            raise ConnectionError("disposable signer outage")

        subject._signer.sign_keyset = unavailable
        with pytest.raises(ConnectionError):
            await subject.refresh_if_needed()
        assert not subject.ready
        async with registry_authority_session.begin() as session:
            assert len((await session.scalars(select(TaskImagePublicationKeyset))).all()) == 1
        subject._signer.sign_keyset = original
        assert await subject.refresh_if_needed()
        assert subject.ready
    finally:
        await engine.dispose()


@pytest.mark.parametrize("cancel", [False, True])
async def test_concurrent_or_cancelled_keyset_signing_holds_no_database_locks(
    registry_authority_session, registry_issuer, tmp_path, monkeypatch, cancel,
):
    shift = [0]
    _, service, old, engine = await setup(registry_authority_session, registry_issuer, tmp_path, monkeypatch, time_shift=shift)
    tasks = []
    try:
        shift[0] = 151
        provider = service._signer.policy._execution
        provider.entered.clear()
        provider.release.clear()
        tasks = [asyncio.create_task(publisher(service, engine).refresh_if_needed()) for _ in range(1 if cancel else 2)]
        await asyncio.wait_for(provider.entered.wait(), 2)
        async with asyncio.timeout(2), registry_authority_session.begin() as session:
            await session.execute(text("SELECT singleton_id FROM task_image_publication_state FOR UPDATE NOWAIT"))
        if cancel:
            tasks[0].cancel()
        provider.release.set()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        if cancel:
            assert isinstance(results[0], asyncio.CancelledError)
        else:
            assert any(result is True for result in results)
            assert all(result is True or result is False or isinstance(result, ValueError) for result in results)
        async with registry_authority_session.begin() as session:
            state = await session.get(TaskImagePublicationState, 1)
            assert state.keyset_version == old.keyset_version + (0 if cancel else 1)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await engine.dispose()
