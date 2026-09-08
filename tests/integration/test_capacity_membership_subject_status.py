"""Historical subject status and release observations cannot confer new admission."""

from hashlib import sha256
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.auth import CapacityPrincipalVerifier
from loom_capacity_manager.membership_contracts import (
    PersonalApplicationMembershipResponseV1,
    PersonalMembershipCheckpointV1,
)
from loom_capacity_manager.membership_release import predecessor_release_sha256
from loom_capacity_manager.membership_store import CapacityMembershipStore
from loom_capacity_manager.models import CapacityExecutableIntent
from tests.integration.test_capacity_membership import DELEGATE, _active_v3, _request
from tests.integration.test_capacity_membership_agent_auth import _http_client
from tests.integration.test_capacity_membership_outcomes import READ_TOKEN, _principal, _retire

STATUS = "/v1/personal-memberships/subjects/status/query"
RELEASE = "/v1/personal-memberships/subjects/release/query"
HEADERS = {"Authorization": f"Bearer {READ_TOKEN}"}


def _receipt(active, result):  # type: ignore[no-untyped-def]
    return PersonalApplicationMembershipResponseV1(
        checkpoint=PersonalMembershipCheckpointV1(
            execution=active,
            namespace_id=_request(active).namespace_id,
            revision=result.revision,
            head_sha256=result.head_sha256,
        ),
        result=result,
    )


async def _disable(session, fixture, active, request):  # type: ignore[no-untyped-def]
    disabled = request.projection.model_copy(
        update={
            "operation_kind": "destroy",
            "operation_epoch": 2,
            "configuration_generation": 2,
            "operation_id": UUID(int=27002),
        }
    )
    result = await CapacityMembershipStore(fixture.store).apply(
        session,
        _request(active, disabled, expected_revision=1),
        actor=DELEGATE,
        idempotency_key=UUID(int=27003),
    )
    return _receipt(active, result)


async def _empty_disabled(session):  # type: ignore[no-untyped-def]
    fixture, active = await _active_v3(session)
    request = _request(active)
    created = await CapacityMembershipStore(fixture.store).apply(
        session,
        request,
        actor=DELEGATE,
        idempotency_key=UUID(int=27001),
    )
    disabled = await _disable(session, fixture, active, request)
    return fixture, active, request, _receipt(active, created), disabled


async def test_empty_disabled_release_is_exact_read_only_historical_evidence(
    capacity_session: AsyncSession,
) -> None:
    fixture, active, _request_value, created, disabled = await _empty_disabled(capacity_session)
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    query = {"schema_version": 1, "membership_receipt": disabled.model_dump(mode="json")}
    async with _http_client(capacity_session, fixture, active, verifier) as (client, _app):
        response = await client.post(RELEASE, json=query, headers=HEADERS)
        assert response.status_code == 200, response.text
        value = response.json()
        assert value["outcome"] == "verified"
        assert value["historical"] is True and value["worker_available"] is False
        assert value["release_set_sha256"] == await predecessor_release_sha256(
            capacity_session, disabled.result.member.configuration
        )
        assert value["membership_receipt"] == disabled.model_dump(mode="json")
        assert value["current"]["subject"]["configuration_generation"] == 2
        assert value["current"]["subject"]["lifecycle_state"] == "disabled"
        repeated = await client.post(RELEASE, json=query, headers=HEADERS)
        assert repeated.status_code == 200 and repeated.json() == value
        response = await client.post(
            STATUS,
            json={"schema_version": 1, "membership_receipt": created.model_dump(mode="json")},
            headers=HEADERS,
        )
        assert response.status_code == 200, response.text
        assert response.json()["membership_receipt"] == created.model_dump(mode="json")
        assert response.json()["historical"] and not response.json()["worker_available"]
        assert response.json()["incarnation_work"]["unreleased_executable_intents"] == 0
        response = await client.post(
            RELEASE,
            json={"schema_version": 1, "membership_receipt": created.model_dump(mode="json")},
            headers=HEADERS,
        )
        assert response.status_code == 409, response.text
    assert (await capacity_session.scalars(select(CapacityExecutableIntent))).all() == []


async def test_verified_old_identity_survives_authenticated_recreation(
    capacity_session: AsyncSession,
) -> None:
    fixture, active, request, _created, disabled = await _empty_disabled(capacity_session)
    recreated = request.projection.model_copy(
        update={
            "operation_kind": "create",
            "operation_id": UUID(int=27004),
            "operation_epoch": 3,
            "configuration_generation": 3,
            "subject_incarnation": UUID(int=27005),
            "demand_reporter_incarnation": UUID(int=27006),
            "demand_reporter_token_sha256": "3" * 64,
        }
    )
    result = await CapacityMembershipStore(fixture.store).apply(
        capacity_session,
        _request(active, recreated, expected_revision=2),
        actor=DELEGATE,
        idempotency_key=UUID(int=27007),
    )
    assert result.member.reincarnation is not None
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    async with _http_client(capacity_session, fixture, active, verifier) as (client, _app):
        response = await client.post(
            RELEASE,
            json={"schema_version": 1, "membership_receipt": disabled.model_dump(mode="json")},
            headers=HEADERS,
        )
        assert response.status_code == 200, response.text
        value = response.json()
        assert value["outcome"] == "verified"
        assert value["membership_receipt"]["result"]["member"]["configuration"][
            "subject_incarnation"
        ] == str(request.projection.subject_incarnation)
        assert value["current"]["subject"]["subject_incarnation"] == str(
            recreated.subject_incarnation
        )
        assert value["release_set_sha256"] == result.member.reincarnation.release_set_sha256


async def test_release_after_retirement_establishes_current_absence_from_new_base(
    capacity_session: AsyncSession,
) -> None:
    import json

    from loom_capacity_manager.contracts import (
        ConfigurationActivationV1,
        ConfigurationGenerationRefV1,
    )
    from loom_capacity_manager.models import CapacityConfigurationEpoch

    fixture, active, _request_value, _created, disabled = await _empty_disabled(capacity_session)
    await _retire(capacity_session, fixture, active)
    base = await capacity_session.get(CapacityConfigurationEpoch, active.configuration_epoch)
    assert base is not None
    await fixture.store.activate_configuration(
        capacity_session,
        ConfigurationActivationV1(
            expected_configuration_epoch=base.configuration_epoch,
            fleet=ConfigurationGenerationRefV1(
                scope="fleet", generation=base.fleet_generation, digest=base.fleet_digest
            ),
            subjects=tuple(
                ConfigurationGenerationRefV1.model_validate_json(json.dumps(item))
                for item in base.subject_generation_manifest
            ),
        ),
        actor="configuration-operator",
        idempotency_key=UUID(int=27008),
    )
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    async with _http_client(capacity_session, fixture, active, verifier) as (client, _app):
        response = await client.post(
            RELEASE,
            json={"schema_version": 1, "membership_receipt": disabled.model_dump(mode="json")},
            headers=HEADERS,
        )
        assert response.status_code == 200, response.text
        assert response.json()["outcome"] == "verified"
        assert response.json()["current"]["subject"] is None
        assert response.json()["current"]["configuration_epoch"] == 2
        assert response.json()["current"]["execution_state"] == "shadow"


@pytest.mark.parametrize("change_incarnation", (False, True))
async def test_later_shadow_projection_cannot_authorize_old_release_by_row_inequality(
    capacity_session: AsyncSession, change_incarnation: bool
) -> None:
    fixture, active, request, _created, disabled = await _empty_disabled(capacity_session)
    await _retire(capacity_session, fixture, active)
    projected = request.projection.model_copy(
        update={
            "operation_kind": "update",
            "operation_id": UUID(int=27009),
            "operation_epoch": 3,
            "configuration_generation": 3,
            "candidate_generation": 2,
            "deployment_generation": 2,
            "demand_reporter_incarnation": UUID(int=27010),
            "demand_reporter_token_sha256": "4" * 64,
            "subject_incarnation": UUID(int=27011)
            if change_incarnation
            else request.projection.subject_incarnation,
        }
    )
    await fixture.store.project_development_subject(
        capacity_session,
        projected,
        actor="configuration-operator",
        idempotency_key=UUID(int=27012),
    )
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    async with _http_client(capacity_session, fixture, active, verifier) as (client, _app):
        response = await client.post(
            RELEASE,
            json={"schema_version": 1, "membership_receipt": disabled.model_dump(mode="json")},
            headers=HEADERS,
        )
        if change_incarnation:
            assert response.status_code == 409, response.text
        else:
            assert response.status_code == 200, response.text
            assert response.json()["outcome"] == "pending"
            assert response.json()["blockers"] == ["same-incarnation-enabled"]
            assert response.json()["incarnation_work"]["executable_intents"] == 0
            assert "release_set_sha256" not in response.json()
            assert response.json()["current"]["subject"]["configuration_generation"] == 3


@pytest.mark.parametrize("accepted", (False, True))
async def test_release_requires_existing_authenticated_cleanup_path(
    capacity_session: AsyncSession, accepted: bool
) -> None:
    from loom_capacity_manager.executable_contracts import (
        ExecutableIntentCloseV2,
        ExecutablePartialReleaseV2,
        ExecutableProtectedReleaseV2,
        ExecutableReservationProposalV2,
    )
    from loom_capacity_manager.execution_store import CapacityExecutionStore
    from tests.capacity_execution_fixtures import executor_binding
    from tests.integration.test_capacity_manager_execution_store import _heartbeat
    from tests.integration.test_capacity_membership_admission import (
        _bootstrap_ack,
        _personal_bootstrap,
        _personal_plan,
    )

    if accepted:
        fixture, active, request, store, executor, bootstrap = await _personal_bootstrap(
            capacity_session
        )
        await store.acknowledge_bootstrap(
            capacity_session,
            _bootstrap_ack(request, bootstrap),
            actor="subject-agent",
            idempotency_key=UUID(int=27013),
        )
    else:
        fixture, active, request = await _personal_plan(capacity_session)
        store = CapacityExecutionStore()
        executor = executor_binding("gb10")
        await _heartbeat(store, capacity_session, active, pool_id="gb10")
        assert isinstance(
            await store.next_pool_work(capacity_session, executor), ExecutableReservationProposalV2
        )
    disabled = await _disable(capacity_session, fixture, active, request)
    query = {"schema_version": 1, "membership_receipt": disabled.model_dump(mode="json")}
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    async with _http_client(capacity_session, fixture, active, verifier) as (client, _app):
        response = await client.post(RELEASE, json=query, headers=HEADERS)
        assert response.status_code == 200, response.text
        assert response.json()["outcome"] == "pending"
        assert response.json()["blockers"] == ["executable-intents"]
        row = (await capacity_session.scalars(select(CapacityExecutableIntent))).one()
        assert row.state != "released" and row.released_at is None
        if accepted:
            close = await store.next_pool_work(capacity_session, executor)
            assert isinstance(close, ExecutableIntentCloseV2)
            await store.begin_intent_close(capacity_session, close)
            await store.acknowledge_protected_release(
                capacity_session,
                ExecutableProtectedReleaseV2(
                    binding=bootstrap.binding,
                    reporter_incarnation=request.acknowledgement.reporter_incarnation,
                    bootstrap_registration_epoch=1,
                    protected_registration_epoch=2,
                    bootstrap_revoked=True,
                    protected_release_sha256="b" * 64,
                ),
                actor="subject-agent",
                idempotency_key=UUID(int=27014),
            )
            release = await store.next_pool_work(capacity_session, executor)
            assert isinstance(release, ExecutablePartialReleaseV2)
            await store.release_shapes(capacity_session, release)
        else:
            assert await store.next_pool_work(capacity_session, executor) is None
        await capacity_session.refresh(row)
        assert row.state == "released" and row.released_at is not None
        response = await client.post(RELEASE, json=query, headers=HEADERS)
        assert response.status_code == 200, response.text
        assert response.json()["outcome"] == "verified"
        assert response.json()["incarnation_work"]["executable_intents"] == 1
        assert response.json()["incarnation_work"]["unreleased_executable_intents"] == 0
        assert response.json()["release_set_sha256"] == await predecessor_release_sha256(
            capacity_session, disabled.result.member.configuration
        )
        await _retire(capacity_session, fixture, active)
        response = await client.post(RELEASE, json=query, headers=HEADERS)
        assert response.status_code == 200, response.text
        assert response.json()["outcome"] == "verified"
        assert response.json()["incarnation_work"]["executable_intents"] == 1
        assert response.json()["current"]["execution_state"] == "shadow"


async def test_subject_observers_require_current_unbound_read_authorization(
    capacity_session: AsyncSession,
) -> None:
    from dataclasses import replace

    fixture, active, _request_value, _created, disabled = await _empty_disabled(capacity_session)
    principals = {
        "current": _principal(),
        "subject": replace(_principal(), subject_id=UUID(int=27020)),
        "pool": replace(_principal(), pool_id="gb10"),
        "executor": replace(_principal(), executor_id="executor"),
        "old-delegate": replace(
            _principal(), principal_id=DELEGATE, scopes=frozenset({"capacity:membership:manage"})
        ),
    }
    verifier = CapacityPrincipalVerifier(
        tuple(
            (sha256(token.encode()).digest(), principal) for token, principal in principals.items()
        )
    )
    query = {"schema_version": 1, "membership_receipt": disabled.model_dump(mode="json")}
    async with _http_client(capacity_session, fixture, active, verifier) as (client, _app):
        for path in (STATUS, RELEASE):
            for token in (*principals, "revoked", "unknown"):
                response = await client.post(
                    path, json=query, headers={"Authorization": f"Bearer {token}"}
                )
                expected = 200 if token == "current" else 403 if token in principals else 401
                assert response.status_code == expected, (path, token, response.text)


async def test_historical_queries_reject_substituted_receipt_bindings(
    capacity_session: AsyncSession,
) -> None:
    fixture, active, _request_value, _created, disabled = await _empty_disabled(capacity_session)
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    async with _http_client(capacity_session, fixture, active, verifier) as (client, _app):
        for field in (
            "head",
            "revision",
            "namespace",
            "execution",
            "configuration",
            "incarnation",
            "deployment",
            "generation",
        ):
            receipt = disabled.model_dump(mode="json")
            checkpoint, result = receipt["checkpoint"], receipt["result"]
            member = result["member"]
            if field == "head":
                checkpoint["head_sha256"] = result["head_sha256"] = "f" * 64
            elif field == "revision":
                checkpoint["revision"] = result["revision"] = member["revision"] = 99
            elif field == "namespace":
                checkpoint["namespace_id"] = str(UUID(int=27021))
            elif field == "execution":
                checkpoint["execution"]["execution_manifest_sha256"] = "f" * 64
            elif field == "configuration":
                checkpoint["execution"]["configuration_epoch"] += 1
            else:
                name = {
                    "incarnation": "subject_incarnation",
                    "deployment": "deployment_generation",
                    "generation": "configuration_generation",
                }[field]
                changed = (
                    str(UUID(int=27021))
                    if field == "incarnation"
                    else member["configuration"][name] + 1
                )
                member["configuration"][name] = member["acknowledgement"][name] = changed
            for path in (STATUS, RELEASE):
                response = await client.post(
                    path, json={"schema_version": 1, "membership_receipt": receipt}, headers=HEADERS
                )
                assert response.status_code == 409, (field, path, response.text)


@pytest.mark.parametrize("missing", (False, True))
async def test_retired_missing_or_corrupt_materialization_is_not_release_evidence(
    capacity_session: AsyncSession,
    missing: bool,
) -> None:
    from sqlalchemy import delete, update

    from loom_capacity_manager.models import CapacitySubject

    fixture, active, _request_value, _created, disabled = await _empty_disabled(capacity_session)
    await _retire(capacity_session, fixture, active)
    subject_id = disabled.result.member.configuration.subject_id
    if missing:
        await capacity_session.execute(
            delete(CapacitySubject).where(CapacitySubject.subject_id == subject_id)
        )
    else:
        await capacity_session.execute(
            update(CapacitySubject)
            .where(CapacitySubject.subject_id == subject_id)
            .values(max_slots=7)
        )
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    async with _http_client(capacity_session, fixture, active, verifier) as (client, _app):
        response = await client.post(
            RELEASE,
            json={"schema_version": 1, "membership_receipt": disabled.model_dump(mode="json")},
            headers=HEADERS,
        )
        assert response.status_code == 409, response.text


@pytest.mark.parametrize("json_only", (False, True))
async def test_observed_commitments_remain_charged_including_quarantined_payloads(
    capacity_session: AsyncSession,
    json_only: bool,
) -> None:
    from datetime import UTC, datetime

    from loom_capacity_manager.contracts import ObservedCommitmentV1
    from loom_capacity_manager.models import CapacityObservedCommitment

    fixture, active, _request_value, _created, disabled = await _empty_disabled(capacity_session)
    predecessor = disabled.result.member.configuration
    profile, now = predecessor.profiles[0], datetime.now(UTC)
    shape = profile.worker_shapes[0]
    observed = ObservedCommitmentV1(
        kind="physical",
        commitment_id="retained-predecessor",
        physical_identity="retained-predecessor",
        subject_id=predecessor.subject_id,
        subject_incarnation=predecessor.subject_incarnation,
        deployment_generation=predecessor.deployment_generation,
        pool_id=profile.pool_id,
        pool_generation=profile.pool_generation,
        profile_id=shape.shape_id,
        profile_generation=profile.profile_generation,
        profile_digest=profile.profile_digest,
        shape_id=shape.shape_id,
        resources=shape.total_resources,
        state="quarantined",
    )
    row = CapacityObservedCommitment(
        kind="physical",
        commitment_identity=observed.commitment_id,
        source_incarnation=UUID(int=27022),
        subject_id=None if json_only else predecessor.subject_id,
        subject_incarnation=None if json_only else predecessor.subject_incarnation,
        deployment_generation=None if json_only else predecessor.deployment_generation,
        profile_id=None if json_only else observed.profile_id,
        profile_generation=None if json_only else observed.profile_generation,
        profile_digest=None if json_only else observed.profile_digest,
        shape_id=None if json_only else observed.shape_id,
        pool_id=profile.pool_id,
        pool_generation=profile.pool_generation,
        binding_payload={"observed_contract": observed.model_dump(mode="json")},
        resource_vector=observed.resources.model_dump(mode="json"),
        state="quarantined",
        first_reporter_high_water=1,
        last_reporter_high_water=1,
        first_receipt_time=now,
        last_receipt_time=now,
    )
    capacity_session.add(row)
    await capacity_session.flush()
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    async with _http_client(capacity_session, fixture, active, verifier) as (client, _app):
        for path in (STATUS, RELEASE):
            response = await client.post(
                path,
                json={"schema_version": 1, "membership_receipt": disabled.model_dump(mode="json")},
                headers=HEADERS,
            )
            assert response.status_code == 200, response.text
            assert response.json()["incarnation_work"]["observed_commitments"] == 1
            if path == RELEASE:
                assert response.json()["outcome"] == "pending"
                assert response.json()["blockers"] == ["observed-commitments"]
                assert "release_set_sha256" not in response.json()
            else:
                assert response.json()["deployment_work"]["observed_commitments"] == 1
    await capacity_session.refresh(row)
    assert row.state == "quarantined" and row.binding_payload[
        "observed_contract"
    ] == observed.model_dump(mode="json")


async def test_old_deployment_intent_blocks_new_disabled_generation_and_corrupt_release_fails_closed(
    capacity_session: AsyncSession,
) -> None:
    from sqlalchemy import update

    from loom_capacity_manager.execution_store import CapacityExecutionStore
    from tests.capacity_execution_fixtures import executor_binding
    from tests.integration.test_capacity_manager_execution_store import (
        _heartbeat,
        _test_only_update_without_guard,
    )
    from tests.integration.test_capacity_membership_admission import _personal_plan, _supersede

    fixture, active, request = await _personal_plan(capacity_session)
    store, executor = CapacityExecutionStore(), executor_binding("gb10")
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    assert await store.next_pool_work(capacity_session, executor) is not None
    updated = await _supersede(capacity_session, fixture, active, request, "update")
    projected = request.projection.model_copy(
        update={
            "operation_kind": "destroy",
            "operation_epoch": 3,
            "configuration_generation": 3,
            "operation_id": UUID(int=27023),
            "candidate_generation": 2,
            "deployment_generation": 2,
            "demand_reporter_incarnation": updated.member.configuration.demand_reporter_incarnation,
            "demand_reporter_token_sha256": "2" * 64,
        }
    )
    disabled = _receipt(
        active,
        await CapacityMembershipStore(fixture.store).apply(
            capacity_session,
            _request(active, projected, expected_revision=2),
            actor=DELEGATE,
            idempotency_key=UUID(int=27024),
        ),
    )
    query = {"schema_version": 1, "membership_receipt": disabled.model_dump(mode="json")}
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    async with _http_client(capacity_session, fixture, active, verifier) as (client, _app):
        response = await client.post(STATUS, json=query, headers=HEADERS)
        assert response.status_code == 200, response.text
        assert response.json()["incarnation_work"]["unreleased_executable_intents"] == 1
        assert response.json()["deployment_work"]["executable_intents"] == 0
        response = await client.post(RELEASE, json=query, headers=HEADERS)
        assert response.status_code == 200 and response.json()["outcome"] == "pending", (
            response.text
        )
        assert await store.next_pool_work(capacity_session, executor) is None
        response = await client.post(RELEASE, json=query, headers=HEADERS)
        assert response.status_code == 200 and response.json()["outcome"] == "verified", (
            response.text
        )
        # Fixture-only corruption, after real cleanup established its durable release.
        row = (await capacity_session.scalars(select(CapacityExecutableIntent))).one()
        await _test_only_update_without_guard(
            capacity_session,
            table_name="capacity_executable_intents",
            trigger_name="capacity_executable_intent_mutation_guard",
            statement=update(CapacityExecutableIntent)
            .where(CapacityExecutableIntent.intent_id == row.intent_id)
            .values(binding_digest="f" * 64),
        )
        response = await client.post(RELEASE, json=query, headers=HEADERS)
        assert response.status_code == 409, response.text


async def test_shadow_release_rejects_corrupt_current_retirement_evidence(
    capacity_session: AsyncSession,
) -> None:
    from sqlalchemy import text, update

    from loom_capacity_manager.models import CapacityExecutionEpoch
    from tests.integration.test_capacity_manager_execution_store import (
        _test_only_update_without_guard,
    )

    fixture, active, _request_value, _created, disabled = await _empty_disabled(capacity_session)
    await _retire(capacity_session, fixture, active)
    await capacity_session.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
    await _test_only_update_without_guard(
        capacity_session,
        table_name="capacity_execution_epochs",
        trigger_name="capacity_execution_epoch_transition_guard",
        statement=update(CapacityExecutionEpoch)
        .where(CapacityExecutionEpoch.execution_epoch == active.execution_epoch)
        .values(retirement_request_digest="f" * 64),
    )
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    async with _http_client(capacity_session, fixture, active, verifier) as (client, _app):
        response = await client.post(
            RELEASE,
            json={"schema_version": 1, "membership_receipt": disabled.model_dump(mode="json")},
            headers=HEADERS,
        )
        assert response.status_code == 409, response.text
