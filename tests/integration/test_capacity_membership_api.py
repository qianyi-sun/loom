"""Personal membership HTTP authorization against the real migrated manager store."""

import json
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom_capacity_manager.api import create_app
from loom_capacity_manager.auth import CapacityPrincipalVerifier
from loom_capacity_manager.config import CapacityManagerSettings
from loom_capacity_manager.contracts import canonical_digest
from loom_capacity_manager.executable_contracts import (
    ExecutionContextV2,
    canonical_executable_digest,
)
from loom_capacity_manager.membership_contracts import PersonalMembershipPolicyV1
from loom_capacity_manager.models import CapacityExecutionEpoch, CapacityPersonalMembershipEvent
from tests.capacity_execution_fixtures import (
    execution_acknowledgement,
    execution_policy,
    setup_execution,
)
from tests.capacity_fixtures import fleet_with_development_template, subject_configuration
from tests.integration.test_capacity_manager_api import _owner_file, _principal
from tests.integration.test_capacity_membership import DELEGATE, NAMESPACE_ID, _active_v3, _request

MEMBERSHIP_TOKEN = "test-personal-membership-manager"
OTHER_TOKEN = "test-other-membership-manager"
READ_TOKEN = "test-read-operator"
PREPARE_TOKEN = "test-prepare-operator"


@pytest.fixture
async def membership_api(
    tmp_path: Path, isolated_capacity_postgres_url: str, request: pytest.FixtureRequest
):
    engine = create_async_engine(isolated_capacity_postgres_url, isolation_level="SERIALIZABLE")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            if getattr(request, "param", "active") == "shadow":
                fleet = fleet_with_development_template()
                subject = subject_configuration(fleet)
                policy = execution_policy(
                    subject_acknowledgements=(execution_acknowledgement(subject=subject),),
                    personal_membership=PersonalMembershipPolicyV1(
                        namespace_id=NAMESPACE_ID,
                        management_principal_id=DELEGATE,
                        development_template_sha256=canonical_digest(
                            fleet.development_subject_template
                        ),
                        max_subjects=2,
                        managed_base_subject_ids=(),
                    ),
                )
                fixture = await setup_execution(
                    session, execution_policy=policy, fleet=fleet, subjects=(subject,)
                )
                active = None
            else:
                fixture, active = await _active_v3(session)
        registry = _owner_file(
            tmp_path / "principals.json",
            json.dumps(
                {
                    "schema_version": 1,
                    "principals": [
                        _principal(DELEGATE, MEMBERSHIP_TOKEN, ["capacity:membership:manage"]),
                        _principal("another-manager", OTHER_TOKEN, ["capacity:membership:manage"]),
                        _principal(
                            "preparation-operator", PREPARE_TOKEN, ["capacity:execution:prepare"]
                        ),
                        _principal(
                            "read-operator", READ_TOKEN, ["capacity:read", "capacity:reconcile"]
                        ),
                    ],
                }
            ),
        )
        settings = CapacityManagerSettings(
            principals_file=registry,
            db_url_file=tmp_path / "unused-db-url",
            expected_authority_incarnation=fixture.writer.authority_incarnation,
            tls_cert_file=tmp_path / "unused-cert",
            tls_key_file=tmp_path / "unused-key",
            tls_client_ca_file=tmp_path / "unused-ca",
        )
        app = create_app(
            settings,
            verifier=CapacityPrincipalVerifier.from_file(registry),
            management_store=fixture.store,
        )
        # Exercise HTTP dependencies/store with the already prepared test DB. This
        # fixture deliberately does not claim to exercise startup or TLS rollout.
        app.state.ready = True
        app.state.session_factory = sessions
        app.state.store = fixture.store
        app.state.writer = fixture.writer
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="https://capacity.test"
        ) as client:
            yield client, active, sessions, fixture.request
    finally:
        await engine.dispose()


async def test_membership_http_checkpoint_and_mutation(membership_api) -> None:
    client, active, _, _ = membership_api
    headers = {"Authorization": f"Bearer {MEMBERSHIP_TOKEN}"}
    checkpoint = await client.get("/v1/personal-memberships/checkpoint", headers=headers)
    assert checkpoint.status_code == 200, checkpoint.text
    assert checkpoint.json()["revision"] == 0
    request = _request(active)
    response = await client.put(
        f"/v1/personal-memberships/{request.projection.subject_id}",
        headers=headers | {"Idempotency-Key": str(UUID(int=22100))},
        json=request.model_dump(mode="json"),
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["checkpoint"]["revision"] == payload["result"]["revision"] == 1
    assert payload["checkpoint"]["head_sha256"] == payload["result"]["head_sha256"]
    assert payload["checkpoint"]["execution"] == active.model_dump(mode="json")
    replay = await client.put(
        f"/v1/personal-memberships/{request.projection.subject_id}",
        headers=headers | {"Idempotency-Key": str(UUID(int=22100))},
        json=request.model_dump(mode="json"),
    )
    assert replay.status_code == 200, replay.text
    assert replay.json()["result"]["replayed"] is True
    assert replay.json()["checkpoint"] == payload["checkpoint"]


@pytest.mark.parametrize(
    "token,expected", ((None, 401), ("unknown", 401), (READ_TOKEN, 403), (OTHER_TOKEN, 409))
)
async def test_membership_http_rejects_unauthorized_writes(membership_api, token, expected) -> None:
    client, active, sessions, _ = membership_api
    request = _request(active)
    headers = {"Idempotency-Key": str(UUID(int=22101))}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    response = await client.put(
        f"/v1/personal-memberships/{request.projection.subject_id}",
        headers=headers,
        json=request.model_dump(mode="json"),
    )
    assert response.status_code == expected, response.text
    async with sessions() as session:
        assert (
            await session.scalar(select(func.count()).select_from(CapacityPersonalMembershipEvent))
            == 0
        )


async def test_membership_http_revision_conflict_is_machine_distinguishable(membership_api) -> None:
    client, active, _, _ = membership_api
    request = _request(active, expected_revision=1)
    response = await client.put(
        f"/v1/personal-memberships/{request.projection.subject_id}",
        headers={
            "Authorization": f"Bearer {MEMBERSHIP_TOKEN}",
            "Idempotency-Key": str(UUID(int=22102)),
        },
        json=request.model_dump(mode="json"),
    )
    assert response.status_code == 409, response.text
    assert response.json()["detail"] == {"code": "membership_revision_conflict"}


async def test_membership_http_cannot_substitute_subject_path(membership_api) -> None:
    client, active, sessions, _ = membership_api
    response = await client.put(
        f"/v1/personal-memberships/{UUID(int=999)}",
        headers={
            "Authorization": f"Bearer {MEMBERSHIP_TOKEN}",
            "Idempotency-Key": str(UUID(int=22103)),
        },
        json=_request(active).model_dump(mode="json"),
    )
    assert response.status_code == 403, response.text
    async with sessions() as session:
        assert (
            await session.scalar(select(func.count()).select_from(CapacityPersonalMembershipEvent))
            == 0
        )


@pytest.mark.parametrize("tag", (3.0, "3", 2, None))
async def test_v3_preparation_route_rejects_nonexact_version(membership_api, tag) -> None:
    client, _, _, preparation = membership_api
    payload = preparation.model_dump(mode="json")
    payload["schema_version"] = tag
    response = await client.post(
        "/v3/execution-preparations",
        headers={
            "Authorization": f"Bearer {PREPARE_TOKEN}",
            "Idempotency-Key": str(UUID(int=20010)),
        },
        json=payload,
    )
    assert response.status_code == 422, response.text


@pytest.mark.parametrize("membership_api", ("shadow",), indirect=True)
async def test_v3_preparation_http_success_and_exact_replay(membership_api) -> None:
    client, active, sessions, preparation = membership_api
    assert active is None
    payload = preparation.model_dump(mode="json")
    assert payload["schema_version"] == 3
    key = UUID(int=22104)
    headers = {
        "Authorization": f"Bearer {PREPARE_TOKEN}",
        "Idempotency-Key": str(key),
    }
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(CapacityExecutionEpoch)) == 0
    response = await client.post("/v3/execution-preparations", headers=headers, json=payload)
    assert response.status_code == 200, response.text
    prepared = ExecutionContextV2.model_validate_json(response.content)
    assert prepared.execution_state == "prepared"
    assert prepared.execution_epoch == 1
    assert prepared.executable_new_capacity_ceiling == 0
    assert prepared.executable_new_capacity_rate_per_minute == 0
    assert prepared.authority_incarnation == preparation.authority_incarnation
    assert prepared.writer_epoch == preparation.expected_writer_epoch
    assert prepared.execution_manifest_sha256 == canonical_executable_digest(preparation)

    replay = await client.post("/v3/execution-preparations", headers=headers, json=payload)
    assert replay.status_code == 200, replay.text
    assert replay.json() == response.json()
    changed = payload | {"rollback_evidence_sha256": "9" * 64}
    assert changed != payload
    conflict = await client.post("/v3/execution-preparations", headers=headers, json=changed)
    assert conflict.status_code == 409, conflict.text
    async with sessions() as session:
        rows = (await session.scalars(select(CapacityExecutionEpoch))).all()
        assert len(rows) == 1
        row = rows[0]
        assert row.state == "prepared"
        assert row.actor == "preparation-operator"
        assert row.idempotency_key == key
        assert row.manifest_payload == payload
        assert (
            row.request_digest
            == row.execution_manifest_sha256
            == canonical_executable_digest(preparation)
        )
