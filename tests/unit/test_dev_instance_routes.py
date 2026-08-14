from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import pytest
from fastapi import FastAPI, HTTPException, Response
from starlette.requests import Request

from loom.auth import AuthContext
from loom.dev_instance_provisioner import (
    DevInstanceProvisioner,
    DevInstanceRecord,
    InstanceReservation,
    OwnerAccessSnapshot,
)
from loom.personal_dev_activation import (
    PersonalDevActivationIntent,
    PersonalDevActivationIntentRequest,
    PersonalDevActivationSigner,
    PersonalDevActivationVerifier,
)
from loom.personal_dev_candidate import PERSONAL_DEV_COMPONENTS
from loom.personal_dev_environment import (
    PersonalDevApplyReservation,
    PersonalDevEnvironmentRecord,
    PersonalDevLifecycleOperationRecord,
)
from loom.personal_dev_capacity import PersonalDevCapacityAvailability
from loom_service.routes.dev_instances import (
    DevInstanceCreateRequest,
    PersonalDevActivationAcknowledgementPayload,
    PersonalDevActivationIntentRequestPayload,
    PersonalDevEnvironmentApplyPayload,
    acknowledge_personal_dev_activation,
    apply_personal_dev_environment,
    create_dev_instance,
    delete_dev_instance,
    get_dev_instance,
    list_dev_instances,
    next_personal_dev_activation_intent,
)

_NOW = datetime(2026, 8, 6, tzinfo=UTC)
_OWNER = UUID("00000000-0000-0000-0000-000000000001")
_OTHER = UUID("00000000-0000-0000-0000-000000000002")
_TEAM = UUID("00000000-0000-0000-0000-000000000003")
_OPERATION = UUID("00000000-0000-0000-0000-000000000004")


def _access(user_id: UUID = _OWNER) -> OwnerAccessSnapshot:
    return OwnerAccessSnapshot(
        user_id=user_id,
        email="owner@example.test",
        username="owner",
        username_normalized="owner",
        display_name="Owner",
        password_hash="hash",
        password_set_at=_NOW,
        user_status="active",
        user_disabled_at=None,
        user_created_at=_NOW,
        user_last_login_at=_NOW,
        team_id=_TEAM,
        team_name="owner-team",
        team_created_at=_NOW,
        membership_role="owner",
        membership_created_at=_NOW,
        fair_share_weight=1.0,
        max_attempts_ceiling=3,
        license_allowlist=("MIT",),
        taskset_max_count=None,
        taskset_max_storage_bytes=None,
        allow_private_endpoints=False,
    )


def _ctx(user_id: UUID, *, admin: bool = False) -> AuthContext:
    return AuthContext(
        token_hash=b"x" * 32,
        type="admin" if admin else "user",
        scopes=["admin:platform"] if admin else ["read:own", "submit"],
        team_id=_TEAM,
        expires_at=None,
        user_id=user_id,
        role="platform_admin" if admin else "member",
    )


class _Store:
    def __init__(self) -> None:
        self.rows: dict[str, DevInstanceRecord] = {}

    async def get(self, name: str) -> DevInstanceRecord | None:
        return self.rows.get(name)

    async def list_active(self) -> list[DevInstanceRecord]:
        return [row for row in self.rows.values() if row.status != "deleted"]

    async def list_visible(
        self,
        *,
        owner_user_id: UUID | None,
        include_deleted: bool = False,
    ) -> list[DevInstanceRecord]:
        rows = list(self.rows.values())
        if owner_user_id is not None:
            rows = [row for row in rows if row.owner_user_id == owner_user_id]
        if not include_deleted:
            rows = [row for row in rows if row.status != "deleted"]
        return sorted(rows, key=lambda row: row.name)

    async def claim_create(self, record: DevInstanceRecord) -> InstanceReservation:
        current = self.rows.get(record.name)
        if current is not None and current.status in {"ready", "provisioning", "deleting"}:
            return InstanceReservation(current, acquired=False)
        self.rows[record.name] = record
        return InstanceReservation(record, acquired=True)

    async def claim_destroy(
        self,
        name: str,
        *,
        operation_id: UUID,
        keep_data: bool,
        now: datetime,
    ) -> InstanceReservation | None:
        current = self.rows.get(name)
        if current is None or current.status == "deleted":
            return None
        claimed = DevInstanceRecord(
            **{
                **current.__dict__,
                "status": "deleting",
                "operation_id": operation_id,
                "operation_epoch": current.operation_epoch + 1,
                "operation_step": (
                    current.operation_step
                    if current.failure_reason == "deletion_failed"
                    else "claimed"
                ),
                "keep_data": keep_data,
                "updated_at": now,
            },
        )
        self.rows[name] = claimed
        return InstanceReservation(claimed, acquired=True)

    async def assert_operation(self, name: str, operation_id: UUID) -> None:
        assert self.rows[name].operation_id == operation_id

    async def set_secret_ref(self, name: str, operation_id: UUID, secret_ref: str) -> None:
        await self.assert_operation(name, operation_id)
        self.rows[name] = DevInstanceRecord(
            **{**self.rows[name].__dict__, "secret_ref": secret_ref},
        )

    async def set_operation_step(self, name: str, operation_id: UUID, step: str) -> None:
        await self.assert_operation(name, operation_id)
        self.rows[name] = DevInstanceRecord(
            **{**self.rows[name].__dict__, "operation_step": step},
        )

    async def complete_operation(
        self,
        name: str,
        operation_id: UUID,
        *,
        status: str,
        now: datetime,
        failure_reason: str | None = None,
    ) -> DevInstanceRecord:
        await self.assert_operation(name, operation_id)
        values = {
            **self.rows[name].__dict__,
            "status": status,
            "updated_at": now,
            "failure_reason": failure_reason,
        }
        if status == "ready":
            values["ready_at"] = now
            values["operation_step"] = "complete"
        if status == "deleted":
            values["deleted_at"] = now
            values["operation_step"] = "complete"
        row = DevInstanceRecord(**values)
        self.rows[name] = row
        return row

    async def checkpoint(self) -> None:
        return None


class _Effects:
    async def apply_role_and_database(self, identity, *, role_sql, create_database_sql):
        return None

    async def drop_database_and_role(self, identity):
        return None

    async def ensure_buckets(self, identity, buckets):
        return None

    async def remove_buckets(self, identity, buckets):
        return None

    async def converge(self, identity):
        return None

    async def upsert_dev_policy(self, identity, requested):
        return None

    async def drop_dev_policy(self, identity):
        return None

    async def deploy(self, identity, *, deployment_generation, candidate_sha):
        return None

    async def destroy(self, identity, *, keep_data):
        return None

    async def store(self, identity, password):
        return f"secret://{identity.name}"

    async def delete(self, identity):
        return None

    async def bootstrap(self, identity, *, password, access):
        return None


class _Runner:
    def __init__(self) -> None:
        self.created: list[DevInstanceRecord] = []
        self.destroyed: list[DevInstanceRecord] = []

    def submit_create(self, record, access):
        assert access.user_id == record.owner_user_id
        self.created.append(record)
        return True

    def submit_destroy(self, record):
        self.destroyed.append(record)
        return True


def _request(
    store: _Store,
    *,
    configured: bool = True,
    runner: _Runner | None = None,
) -> Request:
    app = FastAPI()
    app.state.dev_instance_store_factory = lambda _session: store

    async def access_factory(_session, ctx):
        assert ctx.user_id is not None
        return _access(ctx.user_id)

    app.state.dev_instance_access_snapshot_factory = access_factory
    if runner is not None:
        app.state.dev_instance_lifecycle_runner = runner
    if configured:
        effects = _Effects()
        app.state.dev_instance_provisioner_factory = lambda bound_store: DevInstanceProvisioner(
            store=bound_store,
            sql=effects,
            buckets=effects,
            object_store_tenant=effects,
            policy=effects,
            cluster=effects,
            vault=effects,
            access=effects,
            candidate_sha="a" * 40,
            password_factory=lambda: "b" * 20,
        )
    return Request({"type": "http", "method": "GET", "path": "/", "app": app})


async def test_create_list_get_destroy_owner_lifecycle() -> None:
    store = _Store()
    request = _request(store)
    sc = (object(), _ctx(_OWNER))

    created = await create_dev_instance(
        DevInstanceCreateRequest(name="alice", min_slots=0, max_slots=2),
        request,
        sc,  # type: ignore[arg-type]
    )
    assert created.status == "ready"
    assert created.identity.environment == "dev-alice"
    assert created.identity.namespace == "loom-dev-alice"
    assert "secret" not in created.model_dump()

    listed = await list_dev_instances(
        request,
        sc,  # type: ignore[arg-type]
        mine=False,
        include_deleted=False,
    )
    assert [item.name for item in listed.items] == ["alice"]
    assert (await get_dev_instance("alice", request, sc)).owner_user_id == _OWNER  # type: ignore[arg-type]

    response = Response()
    deleted = await delete_dev_instance(
        "alice",
        request,
        sc,  # type: ignore[arg-type]
        response,
        keep_data=True,
    )
    assert deleted.status == "deleted"
    assert deleted.keep_data is True
    assert response.status_code == 202


async def test_cross_owner_detail_is_hidden() -> None:
    store = _Store()
    request = _request(store)
    await create_dev_instance(
        DevInstanceCreateRequest(name="alice"),
        request,
        (object(), _ctx(_OWNER)),  # type: ignore[arg-type]
    )
    with pytest.raises(HTTPException) as exc:
        await get_dev_instance(
            "alice",
            request,
            (object(), _ctx(_OTHER)),  # type: ignore[arg-type]
        )
    assert exc.value.status_code == 404


async def test_mutation_fails_closed_when_runtime_is_not_configured() -> None:
    store = _Store()
    with pytest.raises(HTTPException) as exc:
        await create_dev_instance(
            DevInstanceCreateRequest(name="alice"),
            _request(store, configured=False),
            (object(), _ctx(_OWNER)),  # type: ignore[arg-type]
        )
    assert exc.value.status_code == 503
    assert store.rows == {}


async def test_configured_runner_makes_accepted_lifecycle_non_blocking() -> None:
    store = _Store()
    runner = _Runner()
    request = _request(store, runner=runner)
    sc = (object(), _ctx(_OWNER))

    created = await create_dev_instance(
        DevInstanceCreateRequest(name="alice"),
        request,
        sc,  # type: ignore[arg-type]
    )
    assert created.status == "provisioning"
    assert [record.name for record in runner.created] == ["alice"]

    store.rows["alice"] = DevInstanceRecord(
        **{**store.rows["alice"].__dict__, "status": "ready"},
    )
    deleted = await delete_dev_instance(
        "alice",
        request,
        sc,  # type: ignore[arg-type]
        Response(),
        keep_data=False,
    )
    assert deleted.status == "deleting"
    assert [record.name for record in runner.destroyed] == ["alice"]


async def test_admin_can_list_all_but_owner_list_is_scoped() -> None:
    store = _Store()
    request = _request(store)
    for name, owner in (("alice", _OWNER), ("bob", _OTHER)):
        store.rows[name] = DevInstanceRecord(
            name=name,
            owner_user_id=owner,
            owner_team_id=_TEAM,
            min_slots=0,
            max_slots=2,
            status="ready",
            deployment_generation=1,
            candidate_sha="a" * 40,
            operation_epoch=1,
            operation_id=_OPERATION,
            created_at=_NOW,
            updated_at=_NOW,
        )
    mine = await list_dev_instances(
        request,
        (object(), _ctx(_OWNER)),  # type: ignore[arg-type]
        mine=False,
        include_deleted=False,
    )
    all_rows = await list_dev_instances(
        request,
        (object(), _ctx(_OWNER, admin=True)),  # type: ignore[arg-type]
        mine=False,
        include_deleted=False,
    )
    assert [row.name for row in mine.items] == ["alice"]
    assert [row.name for row in all_rows.items] == ["alice", "bob"]


async def test_personal_apply_binds_authenticated_owner_and_returns_operation() -> None:
    candidate_id = UUID("00000000-0000-0000-0000-000000000010")
    idempotency_key = UUID("00000000-0000-0000-0000-000000000011")
    subject_id = UUID("00000000-0000-0000-0000-000000000012")
    incarnation = UUID("00000000-0000-0000-0000-000000000013")
    operation_id = UUID("00000000-0000-0000-0000-000000000014")
    captured = []

    class _Authority:
        async def apply(self, requested, *, access_binding, now=None):
            captured.append((requested, access_binding))
            environment = PersonalDevEnvironmentRecord(
                name=requested.name,
                subject_id=subject_id,
                subject_incarnation=incarnation,
                owner_user_id=requested.owner_user_id,
                owner_team_id=requested.owner_team_id,
                min_slots=requested.min_slots,
                max_slots=requested.max_slots,
                status="provisioning",
                deployment_generation=1,
                candidate_id=requested.candidate_id,
                candidate_sha=requested.candidate_sha,
                operation_epoch=1,
                operation_id=operation_id,
                operation_step="candidate_build",
                created_at=_NOW,
                updated_at=_NOW,
            )
            operation = PersonalDevLifecycleOperationRecord(
                id=operation_id,
                idempotency_key=requested.idempotency_key,
                environment_name=requested.name,
                subject_id=subject_id,
                subject_incarnation=incarnation,
                owner_user_id=requested.owner_user_id,
                owner_team_id=requested.owner_team_id,
                operation_epoch=1,
                expected_operation_epoch=0,
                kind="create",
                state="running",
                attempt_id=UUID("00000000-0000-0000-0000-000000000015"),
                attempt_sequence=0,
                request_sha256=requested.request_sha256,
                candidate_id=requested.candidate_id,
                candidate_sha=requested.candidate_sha,
                min_slots=requested.min_slots,
                max_slots=requested.max_slots,
                deployment_generation=1,
                checkpoint="candidate_build",
                created_at=_NOW,
                updated_at=_NOW,
                started_at=_NOW,
            )
            return PersonalDevApplyReservation(
                environment=environment,
                operation=operation,
                acquired=True,
                requires_build_binding=True,
            )

    request = _request(_Store(), configured=False)
    request.app.state.settings = type("Settings", (), {"dev_instances_enabled": True})()
    request.app.state.personal_dev_builder_available = True
    authority = _Authority()
    request.app.state.personal_dev_environment_authority_factory = lambda _session: authority
    response = Response()
    result = await apply_personal_dev_environment(
        "alice",
        PersonalDevEnvironmentApplyPayload(
            candidate_id=candidate_id,
            candidate_sha="a" * 64,
            min_slots=0,
            max_slots=2,
            expected_operation_epoch=0,
            idempotency_key=idempotency_key,
        ),
        request,
        (object(), _ctx(_OWNER)),  # type: ignore[arg-type]
        response,
    )

    assert response.status_code == 202
    assert result.environment.subject_id == subject_id
    assert result.environment.application_status == "provisioning"
    assert result.environment.capacity_status == "shadow"
    assert result.environment.capacity_prepared is False
    assert result.environment.worker_available is False
    assert result.operation.id == operation_id
    assert result.operation.promotable is False
    assert captured[0][0].owner_user_id == _OWNER
    assert captured[0][0].owner_team_id == _TEAM
    assert captured[0][1].auth_kind == "bearer"
    assert captured[0][1].credential_hash == b"x" * 32


async def test_personal_apply_fails_before_mutation_when_builder_is_inert() -> None:
    request = _request(_Store(), configured=False)
    request.app.state.personal_dev_builder_available = False

    with pytest.raises(HTTPException) as exc:
        await apply_personal_dev_environment(
            "alice",
            PersonalDevEnvironmentApplyPayload(
                candidate_id=UUID("00000000-0000-0000-0000-000000000010"),
                candidate_sha="a" * 64,
                min_slots=0,
                max_slots=2,
                expected_operation_epoch=0,
                idempotency_key=UUID("00000000-0000-0000-0000-000000000011"),
            ),
            request,
            (object(), _ctx(_OWNER)),  # type: ignore[arg-type]
            Response(),
        )

    assert exc.value.status_code == 503


async def test_personal_destroy_requires_epoch_and_submits_durable_authority() -> None:
    candidate_id = UUID("00000000-0000-0000-0000-000000000010")
    operation_id = UUID("00000000-0000-0000-0000-000000000014")
    destroy_id = UUID("00000000-0000-0000-0000-000000000015")
    key = UUID("00000000-0000-0000-0000-000000000016")
    store = _Store()
    store.rows["alice"] = DevInstanceRecord(
        name="alice",
        owner_user_id=_OWNER,
        owner_team_id=_TEAM,
        min_slots=0,
        max_slots=2,
        status="ready",
        deployment_generation=1,
        candidate_sha="a" * 64,
        candidate_id=candidate_id,
        subject_id=UUID("00000000-0000-0000-0000-000000000012"),
        subject_incarnation=UUID("00000000-0000-0000-0000-000000000013"),
        operation_epoch=4,
        operation_id=operation_id,
        created_at=_NOW,
        updated_at=_NOW,
        ready_at=_NOW,
    )
    captured = []

    class _Authority:
        async def destroy(self, requested, *, access_binding, now=None):
            captured.append((requested, access_binding))
            environment = PersonalDevEnvironmentRecord(
                name="alice",
                subject_id=store.rows["alice"].subject_id,  # type: ignore[arg-type]
                subject_incarnation=store.rows["alice"].subject_incarnation,  # type: ignore[arg-type]
                owner_user_id=_OWNER,
                owner_team_id=_TEAM,
                min_slots=0,
                max_slots=2,
                status="deleting",
                deployment_generation=1,
                candidate_id=candidate_id,
                candidate_sha="a" * 64,
                operation_epoch=5,
                operation_id=destroy_id,
                operation_step="capacity_retirement_requested",
                keep_data=True,
                created_at=_NOW,
                updated_at=_NOW,
            )
            operation = PersonalDevLifecycleOperationRecord(
                id=destroy_id,
                idempotency_key=key,
                environment_name="alice",
                subject_id=environment.subject_id,
                subject_incarnation=environment.subject_incarnation,
                owner_user_id=_OWNER,
                owner_team_id=_TEAM,
                operation_epoch=5,
                expected_operation_epoch=4,
                kind="destroy",
                state="running",
                attempt_id=UUID("00000000-0000-0000-0000-000000000017"),
                attempt_sequence=0,
                request_sha256=requested.request_sha256,
                candidate_id=candidate_id,
                candidate_sha="a" * 64,
                min_slots=0,
                max_slots=2,
                deployment_generation=1,
                checkpoint="capacity_retirement_requested",
                keep_data=True,
                created_at=_NOW,
                updated_at=_NOW,
                started_at=_NOW,
            )
            return PersonalDevApplyReservation(
                environment=environment,
                operation=operation,
                acquired=True,
                requires_build_binding=False,
            )

    request = _request(store, configured=False)
    request.app.state.settings = type("Settings", (), {"dev_instances_enabled": True})()
    request.app.state.personal_dev_environment_authority_factory = lambda _session: _Authority()
    with pytest.raises(HTTPException) as missing:
        await delete_dev_instance(
            "alice",
            request,
            (object(), _ctx(_OWNER)),  # type: ignore[arg-type]
            Response(),
            keep_data=True,
            expected_operation_epoch=None,
            idempotency_key=None,
        )
    assert missing.value.status_code == 400

    result = await delete_dev_instance(
        "alice",
        request,
        (object(), _ctx(_OWNER)),  # type: ignore[arg-type]
        Response(),
        keep_data=True,
        expected_operation_epoch=4,
        idempotency_key=key,
    )
    assert result.status == "deleting"
    assert result.keep_data is True
    assert result.operation_id == destroy_id
    assert captured[0][0].expected_operation_epoch == 4
    assert captured[0][1].credential_hash == b"x" * 32


async def test_candidate_less_create_is_retired_when_personal_lifecycle_is_enabled() -> None:
    store = _Store()
    request = _request(store)
    request.app.state.settings = type("Settings", (), {"dev_instances_enabled": True})()
    with pytest.raises(HTTPException) as exc:
        await create_dev_instance(
            DevInstanceCreateRequest(name="alice"),
            request,
            (object(), _ctx(_OWNER)),  # type: ignore[arg-type]
        )
    assert exc.value.status_code == 410
    assert store.rows == {}


async def test_activation_ack_route_rejects_unsigned_agent_payload_before_database() -> None:
    request = _request(_Store(), configured=False)
    request.app.state.personal_dev_activation_verifier = PersonalDevActivationVerifier(
        keys={"personal-dev-agent-v1": b"k" * 32},
    )
    request.app.state.session_factory = object()
    payload = PersonalDevActivationAcknowledgementPayload(
        environment_name="alice",
        subject_id=UUID("00000000-0000-0000-0000-000000000010"),
        subject_incarnation=UUID("00000000-0000-0000-0000-000000000011"),
        operation_id=UUID("00000000-0000-0000-0000-000000000012"),
        operation_epoch=1,
        attempt_id=UUID("00000000-0000-0000-0000-000000000013"),
        candidate_id=UUID("00000000-0000-0000-0000-000000000014"),
        candidate_sha="a" * 64,
        deployment_generation=1,
        readiness_evidence_sha256="b" * 64,
        local_activation_sha256="c" * 64,
        agent_key_id="personal-dev-agent-v1",
        observed_at=datetime.now(UTC),
    )

    with pytest.raises(HTTPException) as exc:
        await acknowledge_personal_dev_activation(
            payload,
            request,
            signature="0" * 64,
        )

    assert exc.value.status_code == 403


async def test_activation_intent_route_requires_agent_signature_and_returns_exact_binding(
    monkeypatch,
) -> None:
    private_key = bytes(range(32))
    signer = PersonalDevActivationSigner(keys={"personal-dev-agent-v1": private_key})
    verifier = PersonalDevActivationVerifier(
        keys={
            "personal-dev-agent-v1": signer.public_key_bytes("personal-dev-agent-v1"),
        },
    )
    intent = PersonalDevActivationIntent(
        environment_name="alice",
        subject_id=UUID("00000000-0000-0000-0000-000000000010"),
        subject_incarnation=UUID("00000000-0000-0000-0000-000000000011"),
        operation_id=UUID("00000000-0000-0000-0000-000000000012"),
        operation_epoch=1,
        attempt_id=UUID("00000000-0000-0000-0000-000000000013"),
        attempt_sequence=0,
        candidate_id=UUID("00000000-0000-0000-0000-000000000014"),
        candidate_sha="a" * 64,
        candidate_publication_sha256="b" * 64,
        deployment_generation=1,
        readiness_evidence_sha256="c" * 64,
        min_slots=0,
        max_slots=2,
        images={
            component: f"registry.test/{component}@sha256:{str(index % 10) * 64}"
            for index, component in enumerate(PERSONAL_DEV_COMPONENTS, start=1)
        },
        intent_created_at=_NOW,
    )

    class _SessionContext:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *_args):
            return None

    request = _request(_Store(), configured=False)
    request.app.state.personal_dev_activation_verifier = verifier
    request.app.state.session_factory = lambda: _SessionContext()

    async def next_intent(_self, **_kwargs):
        return intent

    monkeypatch.setattr(
        "loom_service.routes.dev_instances.SqlAlchemyPersonalDevActivationIntentReader.next_intent",
        next_intent,
    )
    poll = PersonalDevActivationIntentRequest(
        agent_key_id="personal-dev-agent-v1",
        request_nonce=UUID("00000000-0000-0000-0000-000000000099"),
        requested_at=datetime.now(UTC),
    )
    payload = PersonalDevActivationIntentRequestPayload(
        agent_key_id=poll.agent_key_id,
        request_nonce=poll.request_nonce,
        requested_at=poll.requested_at,
    )

    with pytest.raises(HTTPException) as exc:
        await next_personal_dev_activation_intent(payload, request, signature="0" * 128)
    assert exc.value.status_code == 403

    response = await next_personal_dev_activation_intent(
        payload,
        request,
        signature=signer.sign_intent_request(poll),
    )
    assert response.operation_id == intent.operation_id
    assert response.intent_sha256 == intent.intent_sha256


async def test_personal_status_uses_persisted_authority_coordinates_not_display_name() -> None:
    store = _Store()
    record = DevInstanceRecord(
        name="renamed-display",
        owner_user_id=_OWNER,
        owner_team_id=_TEAM,
        min_slots=0,
        max_slots=2,
        status="ready",
        deployment_generation=7,
        candidate_sha="a" * 64,
        candidate_id=UUID("00000000-0000-0000-0000-000000000010"),
        subject_id=UUID("00000000-0000-0000-0000-000000000012"),
        subject_incarnation=UUID("00000000-0000-0000-0000-000000000013"),
        capacity_namespace="loom-dev-bound-owner",
        capacity_database="loom_dev_bound_owner",
        operation_epoch=1,
        operation_id=_OPERATION,
        created_at=_NOW,
        updated_at=_NOW,
    )
    store.rows[record.name] = record
    request = _request(store)

    class _Reader:
        async def read(self, **kwargs):
            assert kwargs["namespace"] == "loom-dev-bound-owner"
            assert kwargs["database"] == "loom_dev_bound_owner"
            assert "environment_name" not in kwargs
            return PersonalDevCapacityAvailability("available", True, True)

    request.app.state.personal_dev_capacity_status_reader = _Reader()
    response = await get_dev_instance(record.name, request, (object(), _ctx(_OWNER)))  # type: ignore[arg-type]
    assert response.capacity_status == "available"
    assert response.worker_available is True
