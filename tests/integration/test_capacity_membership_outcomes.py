"""Historical outcome queries use current observer auth, never old mutation authority."""

from dataclasses import replace
from hashlib import sha256
from uuid import UUID

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.auth import CapacityPrincipal, CapacityPrincipalVerifier
from loom_capacity_manager.contracts import canonical_digest
from loom_capacity_manager.executable_contracts import (
    ExecutionAuthorityV2,
    ExecutionDrainV2,
    ExecutionPreparationAbortV2,
    ExecutionRetirementV2,
)
from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.membership_contracts import (
    ExecutionPreparationPolicyV3,
    PersonalMembershipPolicyV1,
)
from loom_capacity_manager.membership_store import CapacityMembershipStore
from loom_capacity_manager.models import CapacityExecutionEpoch, CapacityPersonalMembershipEvent
from loom_capacity_manager.store import CapacityManagementStore
from tests.capacity_execution_fixtures import (
    execution_acknowledgement,
    execution_policy,
    setup_execution,
)
from tests.capacity_fixtures import fleet_with_development_template, subject_configuration
from tests.integration.test_capacity_membership import DELEGATE, NAMESPACE_ID, _active_v3, _request
from tests.integration.test_capacity_membership_agent_auth import _http_client

PATH = "/v1/personal-memberships/operation-outcomes/query"
OLD_TOKEN = "historical-delegate-revoked"
READ_TOKEN = "current-outcome-observer"
KEY = UUID(int=26001)


def _principal() -> CapacityPrincipal:
    return CapacityPrincipal(
        principal_id="current-observer",
        scopes=frozenset({"capacity:read"}),
        subject_id=None,
        subject_incarnation=None,
        demand_reporter_incarnation=None,
        pool_id=None,
        pool_reporter_incarnation=None,
        executor_id=None,
        executor_incarnation=None,
        executor_pool_generation=None,
    )


def _query(request, *, key=KEY, actor=DELEGATE):  # type: ignore[no-untyped-def]
    return {
        "schema_version": 1,
        "original_actor": actor,
        "idempotency_key": str(key),
        "request": request.model_dump(mode="json"),
    }


async def _retire(session, fixture, active):  # type: ignore[no-untyped-def]
    from tests.integration.test_capacity_manager_execution_store import (
        _heartbeat,
        _mark_retirement_safe,
        _post_inventory_heartbeat,
    )

    store = CapacityExecutionStore()
    for pool in ("gb10", "oldlab"):
        await _heartbeat(store, session, active, pool_id=pool)
    drained = await fixture.store.begin_execution_drain(
        session,
        ExecutionDrainV2(
            authority_incarnation=active.authority_incarnation,
            expected_writer_epoch=active.writer_epoch,
            execution_epoch=active.execution_epoch,
            execution_manifest_sha256=active.execution_manifest_sha256,
            expected_executable_new_capacity_ceiling=active.executable_new_capacity_ceiling,
            expected_executable_new_capacity_rate_per_minute=active.executable_new_capacity_rate_per_minute,
        ),
        actor="retirement-operator",
        idempotency_key=UUID(int=26002),
    )
    checkpoints = []
    for pool in ("gb10", "oldlab"):
        await _post_inventory_heartbeat(store, session, active, pool_id=pool)
        checkpoints.append(await _mark_retirement_safe(session, pool_id=pool))
    return await fixture.store.retire_execution_epoch(
        session,
        ExecutionRetirementV2(
            authority_incarnation=drained.authority_incarnation,
            expected_writer_epoch=drained.writer_epoch,
            execution_epoch=drained.execution_epoch,
            execution_manifest_sha256=drained.execution_manifest_sha256,
            executor_checkpoints=tuple(checkpoints),
        ),
        actor="retirement-operator",
        idempotency_key=UUID(int=26003),
    )


async def test_lost_response_recovers_exact_committed_outcome_after_retirement(
    capacity_session: AsyncSession,
) -> None:
    fixture, active = await _active_v3(capacity_session)
    request = _request(active)
    old = replace(
        _principal(), principal_id=DELEGATE, scopes=frozenset({"capacity:membership:manage"})
    )
    async with _http_client(
        capacity_session,
        fixture,
        active,
        CapacityPrincipalVerifier(((sha256(OLD_TOKEN.encode()).digest(), old),)),
    ) as (client, _app):
        committed = await client.put(
            f"/v1/personal-memberships/{request.projection.subject_id}",
            json=request.model_dump(mode="json"),
            headers={"Authorization": f"Bearer {OLD_TOKEN}", "Idempotency-Key": str(KEY)},
        )
        assert committed.status_code == 200, committed.text
    successor = request.projection.model_copy(
        update={
            "operation_kind": "capacity",
            "operation_epoch": 2,
            "configuration_generation": 2,
            "operation_id": UUID(int=26011),
        }
    )
    await CapacityMembershipStore(fixture.store).apply(
        capacity_session,
        _request(active, successor, expected_revision=1),
        actor=DELEGATE,
        idempotency_key=UUID(int=26012),
    )
    await _retire(capacity_session, fixture, active)
    policy = fixture.store.execution_policy
    assert isinstance(policy, ExecutionPreparationPolicyV3)
    fixture = replace(
        fixture,
        store=CapacityManagementStore(
            execution_policy=policy.model_copy(
                update={
                    "personal_membership": policy.personal_membership.model_copy(
                        update={"management_principal_id": "new-delegate"}
                    ),
                }
            )
        ),
    )
    # The caller retained the request, not the lost response. Its old token is gone.
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    async with _http_client(capacity_session, fixture, active, verifier) as (client, _app):
        old_response = await client.post(
            PATH, json=_query(request), headers={"Authorization": f"Bearer {OLD_TOKEN}"}
        )
        assert old_response.status_code == 401, old_response.text
        response = await client.post(
            PATH, json=_query(request), headers={"Authorization": f"Bearer {READ_TOKEN}"}
        )
        assert response.status_code == 200, response.text
        assert response.json()["outcome"] == "committed"
        assert response.json()["receipt"] == committed.json()
        assert response.json()["request_sha256"] == canonical_digest(request)
        assert response.json()["receipt"]["result"]["replayed"] is False
        replay = await client.post(
            PATH, json=_query(request), headers={"Authorization": f"Bearer {READ_TOKEN}"}
        )
        assert replay.status_code == 200 and replay.json() == response.json()
        collision = await client.post(
            PATH,
            json=_query(request, key=UUID(int=26012)),
            headers={"Authorization": f"Bearer {READ_TOKEN}"},
        )
        assert collision.status_code == 409, collision.text
        mutation = await client.put(
            f"/v1/personal-memberships/{request.projection.subject_id}",
            json=request.model_dump(mode="json"),
            headers={"Authorization": f"Bearer {READ_TOKEN}", "Idempotency-Key": str(KEY)},
        )
        assert mutation.status_code == 403
    assert (
        await capacity_session.scalar(
            select(func.count()).select_from(CapacityPersonalMembershipEvent)
        )
        == 2
    )


async def test_absence_is_unresolved_until_real_retirement(capacity_session: AsyncSession) -> None:
    fixture, active = await _active_v3(capacity_session)
    request = _request(active)
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    async with _http_client(capacity_session, fixture, active, verifier) as (client, _app):
        response = await client.post(
            PATH, json=_query(request), headers={"Authorization": f"Bearer {READ_TOKEN}"}
        )
        assert response.status_code == 200, response.text
        assert response.json()["outcome"] == "unresolved"
        assert response.json()["epoch_state"] == "active"
        retired = await _retire(capacity_session, fixture, active)
        response = await client.post(
            PATH, json=_query(request), headers={"Authorization": f"Bearer {READ_TOKEN}"}
        )
        assert response.status_code == 200, response.text
        assert response.json()["outcome"] == "terminal-not-committed"
        assert response.json()["execution_epoch"] == retired.execution_epoch
        assert response.json()["execution_manifest_sha256"] == retired.execution_manifest_sha256
        assert "receipt" not in response.json()
    assert (
        await capacity_session.scalar(
            select(func.count()).select_from(CapacityPersonalMembershipEvent)
        )
        == 0
    )


async def test_existing_zero_uuid_idempotency_key_remains_queryable(
    capacity_session: AsyncSession,
) -> None:
    fixture, active = await _active_v3(capacity_session)
    request = _request(active)
    result = await CapacityMembershipStore(fixture.store).apply(
        capacity_session,
        request,
        actor=DELEGATE,
        idempotency_key=UUID(int=0),
    )
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    async with _http_client(capacity_session, fixture, active, verifier) as (client, _app):
        response = await client.post(
            PATH,
            json=_query(request, key=UUID(int=0)),
            headers={"Authorization": f"Bearer {READ_TOKEN}"},
        )
        assert response.status_code == 200, response.text
        assert response.json()["outcome"] == "committed"
        assert response.json()["receipt"]["result"] == result.model_dump(mode="json")


@pytest.mark.parametrize(
    "substitution",
    (
        "actor",
        "key",
        "operation",
        "request",
        "namespace",
        "manifest",
        "execution_epoch",
        "writer_epoch",
        "authority_incarnation",
        "ceiling",
        "configuration_epoch",
    ),
)
async def test_outcome_query_substitutions_conflict(
    capacity_session: AsyncSession, substitution: str
) -> None:
    fixture, active = await _active_v3(capacity_session)
    request = _request(active)
    await CapacityMembershipStore(fixture.store).apply(
        capacity_session, request, actor=DELEGATE, idempotency_key=KEY
    )
    await _retire(capacity_session, fixture, active)
    query = _query(request)
    if substitution == "actor":
        query["original_actor"] = "different-delegate"
    elif substitution == "key":
        query["idempotency_key"] = str(UUID(int=26004))
    elif substitution == "operation":
        query["request"]["projection"]["operation_id"] = str(UUID(int=26005))
    elif substitution == "request":
        query["request"]["projection"]["max_slots"] = 1
    elif substitution == "namespace":
        query["request"]["namespace_id"] = str(UUID(int=26006))
    elif substitution == "manifest":
        query["request"]["execution"]["execution_manifest_sha256"] = "f" * 64
    elif substitution == "authority_incarnation":
        query["request"]["execution"]["authority_incarnation"] = str(UUID(int=26018))
    elif substitution == "ceiling":
        query["request"]["execution"]["executable_new_capacity_ceiling"] += 1
    else:
        query["request"]["execution"][substitution] += 1
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    async with _http_client(capacity_session, fixture, active, verifier) as (client, _app):
        response = await client.post(
            PATH, json=query, headers={"Authorization": f"Bearer {READ_TOKEN}"}
        )
        assert response.status_code == 409, response.text


@pytest.mark.parametrize("binding", ("subject", "pool", "executor", "scope"))
async def test_outcome_observer_must_be_current_unbound_reader(
    capacity_session: AsyncSession, binding: str
) -> None:
    fixture, active = await _active_v3(capacity_session)
    principal = _principal()
    if binding == "subject":
        principal = replace(principal, subject_id=UUID(int=26007))
    elif binding == "pool":
        principal = replace(principal, pool_id="gb10")
    elif binding == "executor":
        principal = replace(principal, executor_id="gb10-executor")
    else:
        principal = replace(principal, scopes=frozenset({"capacity:membership:manage"}))
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), principal),))
    async with _http_client(capacity_session, fixture, active, verifier) as (client, _app):
        response = await client.post(
            PATH, json=_query(_request(active)), headers={"Authorization": f"Bearer {READ_TOKEN}"}
        )
        assert response.status_code == 403, response.text


async def test_v2_epoch_does_not_acquire_membership_outcome_authority(
    capacity_session: AsyncSession,
) -> None:
    from tests.integration.test_capacity_membership_demand import _active_v2

    fixture, active, _subject, _ack = await _active_v2(capacity_session)
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    async with _http_client(capacity_session, fixture, active, verifier) as (client, _app):
        response = await client.post(
            PATH, json=_query(_request(active)), headers={"Authorization": f"Bearer {READ_TOKEN}"}
        )
        assert response.status_code == 409, response.text


@pytest.mark.parametrize("absent", (False, True))
async def test_outcome_authenticates_complete_history_before_result_or_absence(
    capacity_session: AsyncSession, absent: bool
) -> None:
    fixture, active = await _active_v3(capacity_session)
    request = _request(active)
    await CapacityMembershipStore(fixture.store).apply(
        capacity_session, request, actor=DELEGATE, idempotency_key=KEY
    )
    successor = request.projection.model_copy(
        update={
            "operation_kind": "capacity",
            "operation_epoch": 2,
            "configuration_generation": 2,
            "operation_id": UUID(int=26011),
        }
    )
    await CapacityMembershipStore(fixture.store).apply(
        capacity_session,
        _request(active, successor, expected_revision=1),
        actor=DELEGATE,
        idempotency_key=UUID(int=26012),
    )
    await _retire(capacity_session, fixture, active)
    await capacity_session.execute(
        text(
            "ALTER TABLE capacity_personal_membership_events DISABLE TRIGGER capacity_personal_membership_append_only_guard"
        )
    )
    try:
        await capacity_session.execute(
            update(CapacityPersonalMembershipEvent)
            .where(CapacityPersonalMembershipEvent.revision == 2)
            .values(head_sha256="f" * 64)
        )
    finally:
        await capacity_session.execute(
            text(
                "ALTER TABLE capacity_personal_membership_events ENABLE TRIGGER capacity_personal_membership_append_only_guard"
            )
        )
    if absent:
        request = _request(
            active, request.projection.model_copy(update={"operation_id": UUID(int=26013)})
        )
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    async with _http_client(capacity_session, fixture, active, verifier) as (client, _app):
        response = await client.post(
            PATH,
            json=_query(request, key=UUID(int=26014) if absent else KEY),
            headers={"Authorization": f"Bearer {READ_TOKEN}"},
        )
        assert response.status_code == 409, response.text


async def test_absent_outcome_during_drain_is_not_terminal(capacity_session: AsyncSession) -> None:
    fixture, active = await _active_v3(capacity_session)
    await fixture.store.begin_execution_drain(
        capacity_session,
        ExecutionDrainV2(
            authority_incarnation=active.authority_incarnation,
            expected_writer_epoch=active.writer_epoch,
            execution_epoch=active.execution_epoch,
            execution_manifest_sha256=active.execution_manifest_sha256,
            expected_executable_new_capacity_ceiling=active.executable_new_capacity_ceiling,
            expected_executable_new_capacity_rate_per_minute=active.executable_new_capacity_rate_per_minute,
        ),
        actor="retirement-operator",
        idempotency_key=UUID(int=26015),
    )
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    async with _http_client(capacity_session, fixture, active, verifier) as (client, _app):
        response = await client.post(
            PATH, json=_query(_request(active)), headers={"Authorization": f"Bearer {READ_TOKEN}"}
        )
        assert response.status_code == 200, response.text
        assert response.json()["outcome"] == "unresolved"
        assert response.json()["epoch_state"] == "drain-only"


async def test_absent_outcome_rejects_corrupt_retirement_evidence(
    capacity_session: AsyncSession,
) -> None:
    fixture, active = await _active_v3(capacity_session)
    await _retire(capacity_session, fixture, active)
    epoch = await capacity_session.get(CapacityExecutionEpoch, active.execution_epoch)
    assert epoch is not None
    # Corruption injection; never used to authorize the real retirement fixture.
    await capacity_session.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
    await capacity_session.execute(
        text(
            "ALTER TABLE capacity_execution_epochs DISABLE TRIGGER capacity_execution_epoch_transition_guard"
        )
    )
    try:
        epoch.retirement_request_digest = "f" * 64
        await capacity_session.flush()
    finally:
        await capacity_session.execute(
            text(
                "ALTER TABLE capacity_execution_epochs ENABLE TRIGGER capacity_execution_epoch_transition_guard"
            )
        )
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    async with _http_client(capacity_session, fixture, active, verifier) as (client, _app):
        response = await client.post(
            PATH, json=_query(_request(active)), headers={"Authorization": f"Bearer {READ_TOKEN}"}
        )
        assert response.status_code == 409, response.text


async def test_absent_prepared_epoch_is_unresolved_until_abort(
    capacity_session: AsyncSession,
) -> None:
    fleet = fleet_with_development_template()
    subject = subject_configuration(fleet)
    policy = execution_policy(
        subject_acknowledgements=(execution_acknowledgement(subject=subject),),
        personal_membership=PersonalMembershipPolicyV1(
            namespace_id=NAMESPACE_ID,
            management_principal_id=DELEGATE,
            development_template_sha256=canonical_digest(fleet.development_subject_template),
            max_subjects=2,
            managed_base_subject_ids=(),
        ),
    )
    fixture = await setup_execution(
        capacity_session, execution_policy=policy, fleet=fleet, subjects=(subject,)
    )
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="preparation-operator",
        idempotency_key=UUID(int=26016),
    )
    # An original active-form mutation is not evidence that activation happened.
    execution = ExecutionAuthorityV2.model_validate(
        prepared.model_dump(mode="python")
        | {
            "execution_state": "active",
            "executable_new_capacity_ceiling": fixture.request.requested_ceiling,
            "executable_new_capacity_rate_per_minute": fixture.request.requested_rate_per_minute,
        }
    )
    verifier = CapacityPrincipalVerifier(((sha256(READ_TOKEN.encode()).digest(), _principal()),))
    async with _http_client(capacity_session, fixture, execution, verifier) as (client, _app):
        query = _query(_request(execution))
        response = await client.post(
            PATH, json=query, headers={"Authorization": f"Bearer {READ_TOKEN}"}
        )
        assert response.status_code == 200, response.text
        assert response.json()["outcome"] == "unresolved"
        assert response.json()["epoch_state"] == "prepared"
        await fixture.store.abort_prepared_execution_epoch(
            capacity_session,
            ExecutionPreparationAbortV2(
                authority_incarnation=prepared.authority_incarnation,
                expected_writer_epoch=prepared.writer_epoch,
                execution_epoch=prepared.execution_epoch,
                execution_manifest_sha256=prepared.execution_manifest_sha256,
            ),
            actor="preparation-operator",
            idempotency_key=UUID(int=26017),
        )
        response = await client.post(
            PATH, json=query, headers={"Authorization": f"Bearer {READ_TOKEN}"}
        )
        assert response.status_code == 200, response.text
        assert response.json()["outcome"] == "terminal-not-committed"
