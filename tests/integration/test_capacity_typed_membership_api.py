"""Typed onboarding HTTP uses real manager authority; never certifies execution."""

import json
from uuid import UUID

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom_capacity_manager.api import create_app
from loom_capacity_manager.auth import CapacityPrincipalVerifier
from loom_capacity_manager.config import CapacityManagerSettings
from loom_capacity_manager.contracts import canonical_bytes
from loom_capacity_manager.models import (
    CapacityDeploymentGeneration,
    CapacityPersonalMembershipEvent,
)
from loom_capacity_manager.store import WriterFence
from tests.capacity_build_membership_fixtures import (
    application_request,
    build_request,
    typed_sql_execution,
)
from tests.integration.test_capacity_manager_api import _owner_file, _principal
from tests.integration.test_capacity_typed_membership_execution import typed_management

TOKEN = "test-only-typed-membership-management"


@pytest.fixture
async def typed_api(tmp_path, isolated_capacity_postgres_url):
    engine = create_async_engine(isolated_capacity_postgres_url, isolation_level="SERIALIZABLE")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            _legacy, preparation, fleet, execution = await typed_sql_execution(session)
        registry = _owner_file(tmp_path / "typed-principals.json", json.dumps({"schema_version": 1, "principals": [
            _principal("build-management", TOKEN, ["capacity:membership:manage"]),
            _principal("wrong-delegate", "test-wrong-delegate", ["capacity:membership:manage"]),
            _principal("reader", "test-reader", ["capacity:read", "capacity:reconcile"]),
            _principal("bound", "test-bound", ["capacity:report:demand"],
                subject_id=UUID(int=1), subject_incarnation=UUID(int=2), demand_reporter_incarnation=UUID(int=3)),
        ]}))
        settings = CapacityManagerSettings(principals_file=registry, db_url_file=tmp_path / "unused-db",
            expected_authority_incarnation=execution.authority_incarnation,
            tls_cert_file=tmp_path / "unused-cert", tls_key_file=tmp_path / "unused-key", tls_client_ca_file=tmp_path / "unused-ca")
        management = typed_management(preparation)
        app = create_app(settings, verifier=CapacityPrincipalVerifier.from_file(registry), management_store=management)
        app.state.ready, app.state.session_factory, app.state.store = True, sessions, management
        app.state.writer = WriterFence(authority_incarnation=execution.authority_incarnation, writer_epoch=execution.writer_epoch)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://capacity.test") as http:
            yield http, preparation, fleet, execution, sessions, app
    finally:
        await engine.dispose()


def headers(key=700001, token=TOKEN):
    return {"Authorization": f"Bearer {token}", "Idempotency-Key": str(UUID(int=key)), "Content-Type": "application/json"}


async def test_typed_http_two_owner_builds_and_application_share_revision(typed_api):
    http, preparation, _fleet, execution, sessions, _app = typed_api
    checkpoint = await http.get("/v2/personal-memberships/checkpoint", headers=headers())
    assert checkpoint.status_code == 200, checkpoint.text
    assert checkpoint.json()["revision"] == 0
    requests = [build_request(preparation, execution), build_request(preparation, execution, owner=88011, revision=1),
        application_request(preparation, execution, revision=2)]
    for index, request in enumerate(requests):
        path = f"/v2/personal-memberships/{request.command.acknowledgement.subject_id}"
        response = await http.put(path, headers=headers(700001 + index), content=canonical_bytes(request))
        assert response.status_code == 200, response.text
        assert response.json()["revision"] == index + 1
        replay = await http.put(path, headers=headers(700001 + index), content=canonical_bytes(request))
        assert replay.status_code == 200, replay.text
        assert replay.json() == response.json() | {"replayed": True}
    checkpoint = await http.get("/v2/personal-memberships/checkpoint", headers=headers())
    assert checkpoint.status_code == 200, checkpoint.text
    assert checkpoint.json()["revision"] == 3
    assert checkpoint.json()["head_sha256"] == response.json()["head_sha256"]
    async with sessions() as session:
        for request in requests[:2]:
            deployment = await session.scalar(select(CapacityDeploymentGeneration).where(
                CapacityDeploymentGeneration.subject_id == request.command.acknowledgement.subject_id))
            assert deployment.readiness_state == "pending"


@pytest.mark.parametrize("boundary,expected", [
    ("unknown", 401), ("reader", 403), ("delegate", 409), ("bound", 403), ("path", 403),
    ("version", 422), ("duplicate", 422), ("revision", 409), ("operator", 503),
])
async def test_typed_http_rejects_invalid_authority_without_membership_write(typed_api, boundary, expected):
    http, preparation, _fleet, execution, sessions, app = typed_api
    request = build_request(preparation, execution, revision=1 if boundary == "revision" else 0)
    token = {"unknown": "unknown", "reader": "test-reader", "delegate": "test-wrong-delegate", "bound": "test-bound"}.get(boundary, TOKEN)
    subject = UUID(int=999) if boundary == "path" else request.command.acknowledgement.subject_id
    wire = canonical_bytes(request)
    if boundary == "version":
        wire = wire.replace(b'"schema_version":2', b'"schema_version":2.0', 1)
    elif boundary == "duplicate":
        wire = b'{"schema_version":2,' + wire[1:]
    elif boundary == "operator":
        app.state.store = typed_management(preparation.model_copy(update={"personal_builds":
            preparation.personal_builds.model_copy(update={"max_slots_per_subject": 1})}))
    response = await http.put(f"/v2/personal-memberships/{subject}", headers=headers(token=token), content=wire)
    assert response.status_code == expected, response.text
    if boundary == "revision":
        assert response.json()["detail"] == {"code": "membership_revision_conflict"}
    if boundary in {"unknown", "reader", "delegate", "bound", "operator"}:
        checkpoint = await http.get("/v2/personal-memberships/checkpoint", headers=headers(token=token))
        assert checkpoint.status_code == expected, checkpoint.text
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(CapacityPersonalMembershipEvent)) == 0


async def test_typed_client_recovers_committed_lost_reply_after_other_owner_advances(typed_api):
    from loom.personal_dev_membership_client import PersonalDevMembershipError
    from loom.personal_dev_typed_membership_client import (
        CapacityManagerPersonalDevTypedMembershipClient,
        PersonalDevTypedMembershipEnvelopeV1,
    )

    http, preparation, fleet, execution, sessions, _app = typed_api
    sent = []

    async def transport(request):
        response = await http.send(request)
        if request.method == "PUT":
            sent.append((request.content, request.headers["Idempotency-Key"]))
            if len(sent) == 1:
                assert response.status_code == 200, response.text
                await response.aclose()
                raise httpx.ReadTimeout("test reply lost after manager commit")
        return response

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as wire:
        client = CapacityManagerPersonalDevTypedMembershipClient(manager_origin="https://capacity.test", bearer_token=TOKEN, http_client=wire)
        original = PersonalDevTypedMembershipEnvelopeV1(request=build_request(preparation, execution),
            expected_checkpoint=await client.membership_checkpoint(), idempotency_key=UUID(int=900001))
        with pytest.raises(PersonalDevMembershipError, match="unconfirmed"):
            await client.mutate_membership(original, preparation=preparation, fleet=fleet)
        checkpoint = await client.membership_checkpoint()
        assert checkpoint.revision == 1
        other = PersonalDevTypedMembershipEnvelopeV1(request=build_request(preparation, execution, owner=88011, revision=1),
            expected_checkpoint=checkpoint, idempotency_key=UUID(int=900002))
        assert (await client.mutate_membership(other, preparation=preparation, fleet=fleet)).revision == 2
        recovered = await client.mutate_membership(original, preparation=preparation, fleet=fleet)
        assert recovered.replayed and recovered.revision == 1
        assert recovered.head_sha256 == checkpoint.head_sha256
        assert (await client.membership_checkpoint()).revision == 2
    assert sent[0] == sent[2] and sent[0] != sent[1]
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(CapacityPersonalMembershipEvent)) == 2
