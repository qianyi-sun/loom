"""Real HTTP credential boundaries for personal protected subject agents."""

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from hashlib import sha256
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_manager.api import create_app
from loom_capacity_manager.auth import (
    AuthorizationError,
    CapacityPrincipal,
    CapacityPrincipalVerifier,
)
from loom_capacity_manager.config import CapacityManagerSettings
from loom_capacity_manager.contracts import (
    ConfigurationActivationV1,
    ConfigurationGenerationRefV1,
    DynamicDevelopmentSubjectProjectionV1,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableAdmissionPlanClosureAcknowledgementV2,
    ExecutableAdmissionPlanClosureV2,
    ExecutableAdmissionPlanProposalV2,
    ExecutableIntentCloseV2,
    ExecutableLaunchPermitV2,
    ExecutablePartialReleaseV2,
    ExecutableProtectedReleaseV2,
    ExecutionAuthorityV2,
    ExecutionDrainV2,
    ExecutionRetirementV2,
)
from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.membership_store import CapacityMembershipStore
from loom_capacity_manager.models import (
    CapacityAllocationEpoch,
    CapacityConfigurationEpoch,
    CapacityDemandReporter,
    CapacityPersonalMembershipEvent,
)
from tests.capacity_execution_fixtures import PreparedExecutionFixture
from tests.capacity_fixtures import demand_snapshot, development_projection
from tests.integration.test_capacity_membership import (
    BOB_SUBJECT_ID,
    DELEGATE,
    _active_v3,
    _active_v3_with_managed_base,
    _projection,
    _request,
)

TOKEN = "personal-agent-auth-token"
NEXT_TOKEN = "personal-agent-successor-token"


@asynccontextmanager
async def _client(
    session: AsyncSession,
    *,
    state: str = "current",
    static_scope: bool = False,
) -> AsyncIterator[
    tuple[
        AsyncClient,
        FastAPI,
        PreparedExecutionFixture,
        ExecutionAuthorityV2,
        DynamicDevelopmentSubjectProjectionV1,
    ]
]:
    fixture, active = await _active_v3(session)
    projection = _projection().model_copy(
        update={"demand_reporter_token_sha256": sha256(TOKEN.encode()).hexdigest()}
    )
    await CapacityMembershipStore(fixture.store).apply(
        session, _request(active, projection), actor=DELEGATE, idempotency_key=UUID(int=24001)
    )
    if state != "current":
        await session.execute(
            update(CapacityDemandReporter)
            .where(CapacityDemandReporter.subject_id == BOB_SUBJECT_ID)
            .values(state=state)
        )
    principal = CapacityPrincipal(
        principal_id="read-only",
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
    verifier = CapacityPrincipalVerifier(
        ((sha256(TOKEN.encode()).digest(), principal),) if static_scope else ()
    )
    async with _http_client(session, fixture, active, verifier) as (client, app):
        yield client, app, fixture, active, projection


@asynccontextmanager
async def _http_client(
    session: AsyncSession,
    fixture: PreparedExecutionFixture,
    active: ExecutionAuthorityV2,
    verifier: CapacityPrincipalVerifier,
    *,
    execution_store: CapacityExecutionStore | None = None,
) -> AsyncIterator[tuple[AsyncClient, FastAPI]]:
    settings = CapacityManagerSettings(
        principals_file=Path("unused"),
        db_url_file=Path("unused"),
        tls_cert_file=Path("unused"),
        tls_key_file=Path("unused"),
        tls_client_ca_file=Path("unused"),
        expected_authority_incarnation=active.authority_incarnation,
    )
    app = create_app(settings, verifier=verifier, management_store=fixture.store)
    # Keep the test's migrated transaction; process startup would rotate the writer.
    app.state.ready = True
    app.state.session_factory = async_sessionmaker(
        bind=await session.connection(),
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )
    app.state.store = fixture.store
    app.state.writer = fixture.writer
    app.state.execution_store = execution_store or CapacityExecutionStore()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client, app


@pytest.mark.parametrize("suffix", ("bootstrap-work", "admission-work"))
async def test_dynamic_agent_reaches_real_empty_polling(
    capacity_session: AsyncSession, suffix: str
) -> None:
    async with _client(capacity_session) as (client, _app, _fixture, _active, _projection):
        response = await client.get(
            f"/v2/subjects/{BOB_SUBJECT_ID}/{suffix}",
            headers={"Authorization": f"Bearer {TOKEN}"},
        )
        assert response.status_code == 200, response.text
        assert response.json() is None


@pytest.mark.parametrize("state", ("equivocal", "fenced"))
async def test_arbitrary_fence_or_equivocation_does_not_authenticate(
    capacity_session: AsyncSession,
    state: str,
) -> None:
    async with _client(capacity_session, state=state) as (client, *_):
        response = await client.get(
            f"/v2/subjects/{BOB_SUBJECT_ID}/bootstrap-work",
            headers={"Authorization": f"Bearer {TOKEN}"},
        )
        assert response.status_code == 401


async def test_known_static_wrong_scope_cannot_fall_back(capacity_session: AsyncSession) -> None:
    async with _client(capacity_session, static_scope=True) as (client, *_):
        response = await client.get(
            f"/v2/subjects/{BOB_SUBJECT_ID}/bootstrap-work",
            headers={"Authorization": f"Bearer {TOKEN}"},
        )
        assert response.status_code == 403


async def test_dynamic_subject_path_and_authority_boundaries(
    capacity_session: AsyncSession,
) -> None:
    async with _client(capacity_session) as (client, *_):
        for path, expected in (
            (f"/v2/subjects/{UUID(int=24002)}/bootstrap-work", 403),
            ("/v1/personal-memberships/checkpoint", 401),
            ("/v2/executors/gb10/work", 401),
            ("/v1/status", 401),
        ):
            response = await client.get(path, headers={"Authorization": f"Bearer {TOKEN}"})
            assert response.status_code == expected, (path, response.text)
        response = await client.get(
            f"/v2/subjects/{BOB_SUBJECT_ID}/bootstrap-work",
            headers={"Authorization": "Bearer unknown-token"},
        )
        assert response.status_code == 401
        assert (
            len(
                (await capacity_session.execute(select(CapacityPersonalMembershipEvent)))
                .scalars()
                .all()
            )
            == 1
        )


async def test_legitimate_rotated_reporter_authenticates_identity_only(
    capacity_session: AsyncSession,
) -> None:
    from loom_capacity_manager.membership_auth import authenticate_personal_subject_agent

    async with _client(capacity_session) as (client, _app, fixture, active, initial):
        successor = _projection(
            operation_kind="update",
            operation_epoch=2,
            operation_id=UUID(int=24003),
            reporter_incarnation=UUID(int=24004),
        ).model_copy(
            update={"demand_reporter_token_sha256": sha256(NEXT_TOKEN.encode()).hexdigest()}
        )
        await CapacityMembershipStore(fixture.store).apply(
            capacity_session,
            _request(active, successor, expected_revision=1),
            actor=DELEGATE,
            idempotency_key=UUID(int=24005),
        )
        old = await authenticate_personal_subject_agent(
            capacity_session, fixture.store, token_sha256=sha256(TOKEN.encode()).hexdigest()
        )
        new = await authenticate_personal_subject_agent(
            capacity_session, fixture.store, token_sha256=sha256(NEXT_TOKEN.encode()).hexdigest()
        )
        assert old.subject_id == new.subject_id == BOB_SUBJECT_ID
        assert old.demand_reporter_incarnation != new.demand_reporter_incarnation
        assert old.scopes == new.scopes == frozenset({"capacity:report:demand"})
        stale_demand = demand_snapshot(
            subject_id=initial.subject_id,
            subject_incarnation=initial.subject_incarnation,
            reporter_incarnation=initial.demand_reporter_incarnation,
        )
        response = await client.put(
            f"/v1/reports/demand/{BOB_SUBJECT_ID}",
            json=stale_demand.model_dump(mode="json"),
            headers={"Authorization": f"Bearer {TOKEN}"},
        )
        assert response.status_code == 401


@pytest.mark.parametrize(
    "suffix",
    (
        "intents/{id}/bootstrap-acknowledgements",
        "admission-acknowledgements/{id}",
        "admission-closures/{id}/acknowledgements",
        "protected-release",
    ),
)
async def test_all_protected_write_routes_use_only_local_identity_auth(
    capacity_session: AsyncSession,
    suffix: str,
) -> None:
    async with _client(capacity_session) as (client, *_):
        path = (
            f"/v2/reports/protected-releases/{BOB_SUBJECT_ID}/shape-1"
            if suffix == "protected-release"
            else f"/v2/subjects/{BOB_SUBJECT_ID}/{suffix.format(id=UUID(int=24010))}"
        )
        for token, expected in ((TOKEN, 422), ("unknown-token", 401)):
            response = await client.put(
                path,
                json={},
                headers={
                    "Authorization": f"Bearer {token}",
                    "Idempotency-Key": str(UUID(int=24011)),
                },
            )
            # 422 proves credential acceptance only; no valid write contract was supplied.
            assert response.status_code == expected, response.text


async def test_terminal_evidence_route_rejects_unknown_agent(
    capacity_session: AsyncSession,
) -> None:
    async with _client(capacity_session) as (client, *_):
        response = await client.get(
            f"/v2/subjects/{BOB_SUBJECT_ID}/intents/{UUID(int=24012)}/terminal-inventory-evidence",
            headers={"Authorization": "Bearer unknown-token"},
        )
        assert response.status_code == 401


async def test_reporter_hash_rebinding_has_no_membership_authority(
    capacity_session: AsyncSession,
) -> None:
    async with _client(capacity_session) as (client, *_):
        await capacity_session.execute(
            update(CapacityDemandReporter)
            .where(CapacityDemandReporter.subject_id == BOB_SUBJECT_ID)
            .values(token_sha256=sha256(NEXT_TOKEN.encode()).hexdigest())
        )
        response = await client.get(
            f"/v2/subjects/{BOB_SUBJECT_ID}/bootstrap-work",
            headers={"Authorization": f"Bearer {NEXT_TOKEN}"},
        )
        assert response.status_code == 401


async def test_prepared_personal_base_reporter_retains_proven_origin(
    capacity_session: AsyncSession,
) -> None:
    from loom_capacity_manager.membership_auth import authenticate_personal_subject_agent

    fixture, active, base = await _active_v3_with_managed_base(capacity_session)
    original_token = development_projection().demand_reporter_token_sha256
    original = await authenticate_personal_subject_agent(
        capacity_session,
        fixture.store,
        token_sha256=original_token,
    )
    assert original.subject_id == base.subject_id
    successor = _projection(
        operation_kind="update",
        operation_epoch=2,
        operation_id=UUID(int=24013),
        subject_id=base.subject_id,
        subject_incarnation=base.subject_incarnation,
        owner_id=UUID(hex=base.account_id.removeprefix("dev-owner-")),
        environment_name=base.display_name.removeprefix("dev-"),
        expected_configuration_epoch=2,
        reporter_incarnation=UUID(int=24014),
    )
    await CapacityMembershipStore(fixture.store).apply(
        capacity_session,
        _request(active, successor),
        actor=DELEGATE,
        idempotency_key=UUID(int=24015),
    )
    assert (
        await authenticate_personal_subject_agent(
            capacity_session,
            fixture.store,
            token_sha256=original_token,
        )
        == original
    )


async def test_new_owner_completes_real_authenticated_http_admission(
    capacity_session: AsyncSession,
) -> None:
    from tests.integration import test_capacity_membership_admission as admission_helpers
    from tests.integration.test_capacity_manager_execution_store import _admission_acknowledgement

    (
        fixture,
        active,
        request,
        store,
        executor,
        bootstrap,
    ) = await admission_helpers._personal_bootstrap(
        capacity_session, reporter_token_sha256=sha256(TOKEN.encode()).hexdigest()
    )
    verifier = CapacityPrincipalVerifier(())
    headers = {"Authorization": f"Bearer {TOKEN}"}
    async with _http_client(
        capacity_session,
        fixture,
        active,
        verifier,
        execution_store=store,
    ) as (client, _app):
        response = await client.get(
            f"/v2/subjects/{BOB_SUBJECT_ID}/bootstrap-work",
            headers=headers,
        )
        assert response.status_code == 200, response.text
        assert response.json() == bootstrap.model_dump(mode="json")
        bootstrap_ack = admission_helpers._bootstrap_ack(request, bootstrap)
        response = await client.put(
            f"/v2/subjects/{BOB_SUBJECT_ID}/intents/{bootstrap.binding.intent_id}/bootstrap-acknowledgements",
            headers={**headers, "Idempotency-Key": str(UUID(int=24020))},
            json=bootstrap_ack.model_dump(mode="json"),
        )
        assert response.status_code == 200, response.text
        response = await client.get(
            f"/v2/subjects/{BOB_SUBJECT_ID}/admission-work",
            headers=headers,
        )
        assert response.status_code == 200, response.text
        plan = ExecutableAdmissionPlanProposalV2.model_validate_json(response.content)
        response = await client.put(
            f"/v2/subjects/{BOB_SUBJECT_ID}/admission-acknowledgements/{plan.proposal_id}",
            headers={**headers, "Idempotency-Key": str(UUID(int=24021))},
            json=_admission_acknowledgement(plan).model_dump(mode="json"),
        )
        assert response.status_code == 200, response.text
        permit = await store.next_pool_work(capacity_session, executor)
        assert isinstance(permit, ExecutableLaunchPermitV2)
        assert permit.binding.subject_id == BOB_SUBJECT_ID
        with pytest.raises(AuthorizationError):
            verifier.verify_bearer(headers["Authorization"])


@pytest.mark.parametrize("epoch_state", ("active", "shadow", "prepared"))
async def test_historical_http_cleanup_after_update_without_next_allocation(
    capacity_session: AsyncSession, epoch_state: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.integration.test_capacity_manager_execution_store import (
        _admission_acknowledgement,
        _heartbeat,
        _mark_retirement_safe,
        _post_inventory_heartbeat,
    )
    from tests.integration.test_capacity_membership_admission import (
        _bootstrap_ack,
        _personal_bootstrap,
    )

    fixture, active, request, store, executor, bootstrap = await _personal_bootstrap(
        capacity_session, reporter_token_sha256=sha256(TOKEN.encode()).hexdigest()
    )
    retire = epoch_state != "active"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    next_headers = {"Authorization": f"Bearer {NEXT_TOKEN}"}
    async with _http_client(
        capacity_session, fixture, active, CapacityPrincipalVerifier(()), execution_store=store
    ) as (client, _app):
        bootstrap_ack = _bootstrap_ack(request, bootstrap)
        bootstrap_path = f"/v2/subjects/{BOB_SUBJECT_ID}/intents/{bootstrap.binding.intent_id}/bootstrap-acknowledgements"
        # Deliver a real plan before supersession so its durable closure survives retirement.
        plan = None
        if retire:
            response = await client.put(
                bootstrap_path,
                json=bootstrap_ack.model_dump(mode="json"),
                headers={**headers, "Idempotency-Key": str(UUID(int=24030))},
            )
            assert response.status_code == 200, response.text
            response = await client.get(
                f"/v2/subjects/{BOB_SUBJECT_ID}/admission-work", headers=headers
            )
            assert response.status_code == 200, response.text
            plan = ExecutableAdmissionPlanProposalV2.model_validate_json(response.content)
        successor = request.projection.model_copy(
            update={
                "operation_kind": "update",
                "operation_epoch": 2,
                "configuration_generation": 2,
                "operation_id": UUID(int=24031),
                "candidate_generation": 2,
                "deployment_generation": 2,
                "demand_reporter_incarnation": UUID(int=24032),
                "demand_reporter_token_sha256": sha256(NEXT_TOKEN.encode()).hexdigest(),
            }
        )
        await CapacityMembershipStore(fixture.store).apply(
            capacity_session,
            _request(active, successor, expected_revision=1),
            actor=DELEGATE,
            idempotency_key=UUID(int=24033),
        )
        response = await client.put(
            bootstrap_path,
            json=bootstrap_ack.model_dump(mode="json"),
            headers={**next_headers, "Idempotency-Key": str(UUID(int=24034))},
        )
        assert response.status_code == 403, response.text
        if not retire:
            response = await client.put(
                bootstrap_path,
                json=bootstrap_ack.model_dump(mode="json"),
                headers={**headers, "Idempotency-Key": str(UUID(int=24035))},
            )
            assert response.status_code == 200, response.text
            response = await client.get(
                f"/v2/subjects/{BOB_SUBJECT_ID}/admission-work", headers=headers
            )
            assert response.status_code == 200 and response.json() is None, response.text
        else:
            assert plan is not None
            response = await client.put(
                f"/v2/subjects/{BOB_SUBJECT_ID}/admission-acknowledgements/{plan.proposal_id}",
                json=_admission_acknowledgement(plan).model_dump(mode="json"),
                headers={**headers, "Idempotency-Key": str(UUID(int=24036))},
            )
            assert response.status_code == 409, response.text
        close = await store.next_pool_work(capacity_session, executor)
        assert isinstance(close, ExecutableIntentCloseV2)
        await store.begin_intent_close(capacity_session, close)
        protected = ExecutableProtectedReleaseV2(
            binding=bootstrap.binding,
            reporter_incarnation=request.acknowledgement.reporter_incarnation,
            bootstrap_registration_epoch=1,
            protected_registration_epoch=2,
            bootstrap_revoked=True,
            protected_release_sha256="b" * 64,
        )
        protected_path = (
            f"/v2/reports/protected-releases/{BOB_SUBJECT_ID}/{bootstrap.binding.shape_instance_id}"
        )
        for credential, expected, key in ((next_headers, 403, 24037), (headers, 200, 24038)):
            response = await client.put(
                protected_path,
                json=protected.model_dump(mode="json"),
                headers={**credential, "Idempotency-Key": str(UUID(int=key))},
            )
            assert response.status_code == expected, response.text
        release = await store.next_pool_work(capacity_session, executor)
        assert isinstance(release, ExecutablePartialReleaseV2)
        await store.release_shapes(capacity_session, release)
        assert len((await capacity_session.scalars(select(CapacityAllocationEpoch))).all()) == 1
        if not retire:
            return
        await _heartbeat(store, capacity_session, active, pool_id="oldlab")
        drained = await fixture.store.begin_execution_drain(
            capacity_session,
            ExecutionDrainV2(
                authority_incarnation=active.authority_incarnation,
                expected_writer_epoch=active.writer_epoch,
                execution_epoch=active.execution_epoch,
                execution_manifest_sha256=active.execution_manifest_sha256,
                expected_executable_new_capacity_ceiling=active.executable_new_capacity_ceiling,
                expected_executable_new_capacity_rate_per_minute=active.executable_new_capacity_rate_per_minute,
            ),
            actor="activation-operator",
            idempotency_key=UUID(int=24039),
        )
        checkpoints = []
        for pool in ("gb10", "oldlab"):
            await _post_inventory_heartbeat(store, capacity_session, active, pool_id=pool)
            checkpoints.append(await _mark_retirement_safe(capacity_session, pool_id=pool))
        await fixture.store.retire_execution_epoch(
            capacity_session,
            ExecutionRetirementV2(
                authority_incarnation=drained.authority_incarnation,
                expected_writer_epoch=drained.writer_epoch,
                execution_epoch=drained.execution_epoch,
                execution_manifest_sha256=drained.execution_manifest_sha256,
                executor_checkpoints=tuple(checkpoints),
            ),
            actor="activation-operator",
            idempotency_key=UUID(int=24040),
        )
        if epoch_state == "prepared":
            base = await capacity_session.get(
                CapacityConfigurationEpoch, active.configuration_epoch
            )
            assert base is not None
            # A new preparation requires a freshly materialized immutable base.
            configured = await fixture.store.activate_configuration(
                capacity_session,
                ConfigurationActivationV1(
                    expected_configuration_epoch=base.configuration_epoch,
                    fleet=ConfigurationGenerationRefV1(
                        scope="fleet",
                        generation=base.fleet_generation,
                        digest=base.fleet_digest,
                    ),
                    subjects=tuple(
                        ConfigurationGenerationRefV1.model_validate_json(json.dumps(item))
                        for item in base.subject_generation_manifest
                    ),
                ),
                actor="configuration-operator",
                idempotency_key=UUID(int=24047),
            )
            await fixture.store.prepare_execution_epoch(
                capacity_session,
                fixture.request.model_copy(
                    update={"configuration_epoch": configured.configuration_epoch}
                ),
                actor="preparation-operator",
                idempotency_key=UUID(int=24044),
            )
        savepoint = await capacity_session.begin_nested()
        await capacity_session.execute(
            update(CapacityDemandReporter)
            .where(
                CapacityDemandReporter.reporter_incarnation
                == request.acknowledgement.reporter_incarnation
            )
            .values(state="equivocal")
        )
        response = await client.get(
            f"/v2/subjects/{BOB_SUBJECT_ID}/admission-work", headers=headers
        )
        assert response.status_code == 401, response.text
        await savepoint.rollback()
        response = await client.get(
            f"/v2/subjects/{BOB_SUBJECT_ID}/admission-work", headers=headers
        )
        assert response.status_code == 200, response.text
        closure = ExecutableAdmissionPlanClosureV2.model_validate_json(response.content)
        assert closure.proposal == plan
        # Fail closed if closure receipt races the separate authentication/store transactions.
        with monkeypatch.context() as race:
            race.setattr(
                store, "next_subject_admission_plan", AsyncMock(return_value=closure.proposal)
            )
            response = await client.get(
                f"/v2/subjects/{BOB_SUBJECT_ID}/admission-work", headers=headers
            )
            assert response.status_code == 401, response.text
        acknowledgement = ExecutableAdmissionPlanClosureAcknowledgementV2(
            closure_id=closure.closure_id,
            proposal_id=closure.proposal.proposal_id,
            proposal_digest=store.contract_digest(closure.proposal),
            plan_id=closure.proposal.plan_id,
            admission_incarnation=closure.proposal.admission_incarnation,
            subject_id=BOB_SUBJECT_ID,
            subject_incarnation=request.projection.subject_incarnation,
            reporter_incarnation=request.acknowledgement.reporter_incarnation,
            protected_admission_sha256=closure.proposal.protected_admission_sha256,
            close_reason=closure.close_reason,
            disposition_kind="never-converged",
            disposition_digest="e" * 64,
        )
        closure_path = f"/v2/subjects/{BOB_SUBJECT_ID}/admission-closures/{closure.closure_id}/acknowledgements"
        response = await client.put(
            f"/v2/subjects/{BOB_SUBJECT_ID}/admission-closures/{UUID(int=24045)}/acknowledgements",
            json=acknowledgement.model_dump(mode="json"),
            headers={**headers, "Idempotency-Key": str(UUID(int=24046))},
        )
        assert response.status_code == 401, response.text
        for credential, expected, key in ((next_headers, 401, 24041), (headers, 200, 24042)):
            response = await client.put(
                closure_path,
                json=acknowledgement.model_dump(mode="json"),
                headers={**credential, "Idempotency-Key": str(UUID(int=key))},
            )
            assert response.status_code == expected, response.text
        response = await client.put(
            closure_path,
            json=acknowledgement.model_dump(mode="json"),
            headers={**headers, "Idempotency-Key": str(UUID(int=24042))},
        )
        assert response.status_code == 200 and response.json()["replayed"], response.text
        response = await client.get(
            f"/v2/subjects/{BOB_SUBJECT_ID}/admission-work", headers=headers
        )
        assert response.status_code == 401, response.text
        # Archive-only credentials do not reactivate any increase or bootstrap authority.
        response = await client.put(
            f"/v2/subjects/{BOB_SUBJECT_ID}/admission-acknowledgements/{closure.proposal.proposal_id}",
            json=_admission_acknowledgement(closure.proposal).model_dump(mode="json"),
            headers={**headers, "Idempotency-Key": str(UUID(int=24043))},
        )
        assert response.status_code == 401, response.text
        response = await client.get(
            f"/v2/subjects/{BOB_SUBJECT_ID}/bootstrap-work", headers=headers
        )
        assert response.status_code == 401, response.text
