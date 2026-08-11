"""Idempotent orchestration for provisioning one ``dev-<name>`` instance.

The guarded ``dev-instances`` endpoint and the ``loom dev`` CLI both drive the
*same* create/destroy sequence through this module, so the guardrail and the
step order live in exactly one place. Every side-effecting step is a small
``Protocol`` — SQL (role + database on the shared dev fixture), object-store
buckets, the capped autoscaler policy, the per-instance cluster deploy, and the
instance registry — so the orchestration is unit-testable with in-memory fakes
and the live executors (psycopg / boto3 / control-plane httpx / kube) plug in
behind the same seams.

Create is **fail-closed** (``validate_dev_instance`` runs before any mutation)
"""

from __future__ import annotations

import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal, Protocol
from uuid import UUID, uuid4

from loom.dev_instance import (
    DevInstanceIdentity,
    DevInstanceRef,
    RequestedPolicy,
    derive_identity,
    validate_dev_instance,
)
from loom.dev_instance_provision import provisioning_plan

#: Dev instances only ever run on the Slurm actuator (the envelope the
#: autoscaler admission also enforces). Fixed here so a caller cannot request a
#: different actuator through the guarded path.
DEV_ACTUATOR = "slurm"
DevInstanceStatus = Literal[
    "provisioning",
    "ready",
    "updating",
    "activating",
    "deleting",
    "draining",
    "failed",
    "deleted",
]
_DESTROY_STEP_RANK = {
    "claimed": 0,
    "drained": 1,
    "namespace_deleted": 2,
    "database_deleted": 3,
    "buckets_deleted": 4,
    "tenant_deleted": 5,
}


class DevInstanceRejectedError(ValueError):
    """The requested instance is outside the dev envelope — no mutation ran.

    Carries the human-readable reasons from :func:`validate_dev_instance` so
    the endpoint can surface them as a 400.
    """

    def __init__(self, reasons: list[str]) -> None:
        self.reasons = list(reasons)
        super().__init__("; ".join(reasons) or "dev instance rejected")


class DevInstanceConflictError(ValueError):
    """The name is owned by someone else or has an incompatible live shape."""


class DevInstanceOperationFencedError(RuntimeError):
    """A newer lifecycle operation superseded this provisioner invocation."""


@dataclass(frozen=True)
class DevInstanceRecord:
    """The registry view of one instance (what GET/list return)."""

    name: str
    owner_user_id: UUID
    owner_team_id: UUID
    min_slots: int
    max_slots: int
    status: DevInstanceStatus
    deployment_generation: int
    candidate_sha: str
    operation_epoch: int
    operation_id: UUID
    created_at: datetime
    updated_at: datetime
    operation_step: str = "claimed"
    secret_ref: str | None = None
    keep_data: bool = False
    failure_reason: str | None = None
    ready_at: datetime | None = None
    deleted_at: datetime | None = None
    subject_id: UUID | None = None
    subject_incarnation: UUID | None = None
    candidate_id: UUID | None = None


@dataclass(frozen=True)
class InstanceReservation:
    record: DevInstanceRecord
    acquired: bool


@dataclass(frozen=True, slots=True)
class BearerAccessSnapshot:
    """The already-verified user-owned bearer row to mirror into the instance."""

    token_hash: bytes
    name: str | None
    type: str
    scopes: tuple[str, ...]
    issued_at: datetime
    expires_at: datetime | None
    created_by_actor: str | None


@dataclass(frozen=True, slots=True)
class SessionAccessSnapshot:
    """The already-verified browser-session row to mirror into the instance."""

    session_hash: bytes
    csrf_hash: bytes
    issued_at: datetime
    expires_at: datetime
    last_seen_at: datetime | None


@dataclass(frozen=True, slots=True)
class OwnerAccessSnapshot:
    """Minimal owner/team state required to use a newly migrated instance.

    The snapshot contains hashes, never raw bearer/session/CSRF credentials.
    The caller keeps the raw credential it already owns, so the same CLI or
    browser principal can authenticate to the new isolated database.
    """

    user_id: UUID
    email: str | None
    username: str
    username_normalized: str
    display_name: str | None
    password_hash: str | None
    password_set_at: datetime | None
    user_status: str
    user_disabled_at: datetime | None
    user_created_at: datetime
    user_last_login_at: datetime | None
    team_id: UUID
    team_name: str
    team_created_at: datetime
    membership_role: str
    membership_created_at: datetime
    fair_share_weight: float
    max_attempts_ceiling: int
    license_allowlist: tuple[str, ...]
    taskset_max_count: int | None
    taskset_max_storage_bytes: int | None
    allow_private_endpoints: bool
    bearer: BearerAccessSnapshot | None = None
    session: SessionAccessSnapshot | None = None


# ── Step seams (live executors implement these; fakes back the tests) ────────


class SqlExecutor(Protocol):
    """Runs the per-instance role + database SQL on the dev-fixture Postgres."""

    async def apply_role_and_database(
        self, identity: DevInstanceIdentity, *, role_sql: str, create_database_sql: str
    ) -> None: ...

    async def drop_database_and_role(self, identity: DevInstanceIdentity) -> None: ...


class BucketEnsurer(Protocol):
    """Creates (idempotently) / removes the instance's object-store buckets."""

    async def ensure_buckets(self, identity: DevInstanceIdentity, buckets: list[str]) -> None: ...

    async def remove_buckets(self, identity: DevInstanceIdentity, buckets: list[str]) -> None: ...


class ObjectStoreTenant(Protocol):
    """Converges a credential restricted to exactly one instance's buckets."""

    async def converge(self, identity: DevInstanceIdentity) -> None: ...

    async def delete(self, identity: DevInstanceIdentity) -> None: ...


class PolicyRegistrar(Protocol):
    """Upserts / drops the instance's capped dev autoscaler policy (via the CP).

    The control-plane upsert independently re-enforces the dev envelope, so this
    is defense-in-depth over the endpoint's own ``validate_dev_instance`` call.
    """

    async def upsert_dev_policy(
        self, identity: DevInstanceIdentity, requested: RequestedPolicy
    ) -> None: ...

    async def drop_dev_policy(self, identity: DevInstanceIdentity) -> None: ...


class ClusterProvisioner(Protocol):
    """Deploys / tears down the per-instance namespace + control-plane/service."""

    async def deploy(
        self,
        identity: DevInstanceIdentity,
        *,
        deployment_generation: int,
        candidate_sha: str,
    ) -> None: ...

    async def destroy(self, identity: DevInstanceIdentity, *, keep_data: bool) -> None: ...


class SecretVault(Protocol):
    """Stores the generated role password; returns an opaque ref."""

    async def store(self, identity: DevInstanceIdentity, password: str) -> str: ...

    async def delete(self, identity: DevInstanceIdentity) -> None: ...


class AccessBootstrap(Protocol):
    """Mirrors the authenticated owner into the newly migrated database."""

    async def bootstrap(
        self,
        identity: DevInstanceIdentity,
        *,
        password: str,
        access: OwnerAccessSnapshot,
    ) -> None: ...


class InstanceStore(Protocol):
    """Persists the registry rows the budget accounting + listing read."""

    async def get(self, name: str) -> DevInstanceRecord | None: ...

    async def list_active(self) -> list[DevInstanceRecord]: ...

    async def claim_create(self, record: DevInstanceRecord) -> InstanceReservation: ...

    async def claim_destroy(
        self,
        name: str,
        *,
        operation_id: UUID,
        keep_data: bool,
        now: datetime,
    ) -> InstanceReservation | None: ...

    async def assert_operation(self, name: str, operation_id: UUID) -> None: ...

    async def set_secret_ref(self, name: str, operation_id: UUID, secret_ref: str) -> None: ...

    async def set_operation_step(self, name: str, operation_id: UUID, step: str) -> None: ...

    async def complete_operation(
        self,
        name: str,
        operation_id: UUID,
        *,
        status: DevInstanceStatus,
        now: datetime,
        failure_reason: str | None = None,
    ) -> DevInstanceRecord: ...

    async def checkpoint(self) -> None: ...


# ── Orchestrator ─────────────────────────────────────────────────────────────


@dataclass
class DevInstanceProvisioner:
    """Composes the step seams into idempotent create / destroy sequences."""

    store: InstanceStore
    sql: SqlExecutor
    buckets: BucketEnsurer
    object_store_tenant: ObjectStoreTenant
    policy: PolicyRegistrar
    cluster: ClusterProvisioner
    vault: SecretVault
    access: AccessBootstrap
    deployment_generation: int = 1
    candidate_sha: str = "0" * 40
    # Injectable so tests are deterministic; default = a 20-hex-char secret that
    # satisfies dev_instance_provision's _HEX_PASSWORD guard.
    password_factory: Callable[[], str] = field(
        default=lambda: secrets.token_hex(10),
    )

    def __post_init__(self) -> None:
        if self.deployment_generation <= 0:
            raise ValueError("deployment_generation must be positive")
        if len(self.candidate_sha) != 40 or any(
            char not in "0123456789abcdef" for char in self.candidate_sha
        ):
            raise ValueError("candidate_sha must be a full lowercase Git SHA")

    async def create(
        self,
        name: str,
        *,
        owner_user_id: UUID,
        owner_team_id: UUID,
        min_slots: int,
        max_slots: int,
        access: OwnerAccessSnapshot,
        now: datetime | None = None,
    ) -> DevInstanceRecord:
        """Provision (or converge) ``dev-<name>`` and return its registry record.

        Fail-closed: the envelope check runs against every *other* live instance
        before any mutation. Idempotent: an already-``ready`` instance with the
        same shape is returned unchanged.
        """
        reservation = await self.claim_create(
            name,
            owner_user_id=owner_user_id,
            owner_team_id=owner_team_id,
            min_slots=min_slots,
            max_slots=max_slots,
            now=now,
        )
        if not reservation.acquired:
            return reservation.record
        return await self.converge_create(reservation.record, access=access)

    async def claim_create(
        self,
        name: str,
        *,
        owner_user_id: UUID,
        owner_team_id: UUID,
        min_slots: int,
        max_slots: int,
        now: datetime | None = None,
    ) -> InstanceReservation:
        """Validate and durably claim creation without running external effects."""
        now = now or datetime.now(UTC)
        requested = RequestedPolicy(actuator=DEV_ACTUATOR, min_slots=min_slots, max_slots=max_slots)

        others = [
            DevInstanceRef(name=r.name, max_slots=r.max_slots)
            for r in await self.store.list_active()
            if r.name != name
        ]
        errors = validate_dev_instance(name, requested, others)
        if errors:
            raise DevInstanceRejectedError(errors)

        operation_id = uuid4()
        existing = await self.store.get(name)
        reservation = await self.store.claim_create(
            DevInstanceRecord(
                name=name,
                owner_user_id=owner_user_id,
                owner_team_id=owner_team_id,
                min_slots=min_slots,
                max_slots=max_slots,
                status="provisioning",
                deployment_generation=self.deployment_generation,
                candidate_sha=self.candidate_sha,
                operation_epoch=(existing.operation_epoch + 1 if existing else 1),
                operation_id=operation_id,
                created_at=(existing.created_at if existing else now),
                updated_at=now,
                secret_ref=(existing.secret_ref if existing else None),
            ),
        )
        await self.store.checkpoint()
        return reservation

    async def converge_create(
        self,
        record: DevInstanceRecord,
        *,
        access: OwnerAccessSnapshot,
    ) -> DevInstanceRecord:
        """Run idempotent external effects for one previously claimed create."""
        name = record.name
        owner_user_id = record.owner_user_id
        owner_team_id = record.owner_team_id
        operation_id = record.operation_id
        requested = RequestedPolicy(
            actuator=DEV_ACTUATOR,
            min_slots=record.min_slots,
            max_slots=record.max_slots,
        )
        identity = derive_identity(name)

        try:
            password = self.password_factory()
            plan = provisioning_plan(name, password)
            await self.store.assert_operation(name, operation_id)
            await self.sql.apply_role_and_database(
                identity,
                role_sql=str(plan["role_sql"]),
                create_database_sql=str(plan["create_database_sql"]),
            )
            await self.store.assert_operation(name, operation_id)
            await self.buckets.ensure_buckets(identity, dev_buckets(identity))
            await self.store.assert_operation(name, operation_id)
            secret_ref = await self.vault.store(identity, password)
            await self.store.set_secret_ref(name, operation_id, secret_ref)
            await self.store.checkpoint()
            await self.store.assert_operation(name, operation_id)
            await self.object_store_tenant.converge(identity)
            await self.store.assert_operation(name, operation_id)
            await self.cluster.deploy(
                identity,
                deployment_generation=self.deployment_generation,
                candidate_sha=self.candidate_sha,
            )
            await self.store.assert_operation(name, operation_id)
            if access.user_id != owner_user_id or access.team_id != owner_team_id:
                raise DevInstanceConflictError(
                    "authenticated owner changed during provisioning",
                )
            await self.access.bootstrap(identity, password=password, access=access)
            await self.store.assert_operation(name, operation_id)
            await self.policy.upsert_dev_policy(identity, requested)
            result = await self.store.complete_operation(
                name,
                operation_id,
                status="ready",
                now=datetime.now(UTC),
            )
            await self.store.checkpoint()
            return result
        except DevInstanceOperationFencedError:
            raise
        except Exception:
            await self.store.complete_operation(
                name,
                operation_id,
                status="failed",
                now=datetime.now(UTC),
                failure_reason="provisioning_failed",
            )
            await self.store.checkpoint()
            raise

    async def destroy(self, name: str, *, keep_data: bool = False) -> DevInstanceRecord | None:
        """Tear down ``dev-<name>`` idempotently; ``None`` if it doesn't exist.

        Reverse of create: drop the policy (draining the pool) → destroy the
        namespace → soft-delete the registry row → drop DB/buckets/secret unless
        ``keep_data``.
        """
        reservation = await self.claim_destroy(name, keep_data=keep_data)
        if reservation is None:
            return None
        if not reservation.acquired:
            return reservation.record
        return await self.converge_destroy(reservation.record)

    async def claim_destroy(
        self,
        name: str,
        *,
        keep_data: bool = False,
    ) -> InstanceReservation | None:
        """Durably claim deletion without running external effects."""
        reservation = await self.store.claim_destroy(
            name,
            operation_id=uuid4(),
            keep_data=keep_data,
            now=datetime.now(UTC),
        )
        await self.store.checkpoint()
        return reservation

    async def converge_destroy(self, record: DevInstanceRecord) -> DevInstanceRecord:
        """Run drain-first external cleanup for one previously claimed delete."""
        name = record.name
        keep_data = record.keep_data
        operation_id = record.operation_id
        try:
            step_rank = _DESTROY_STEP_RANK[record.operation_step]
        except KeyError as exc:
            raise DevInstanceConflictError("dev instance deletion checkpoint is invalid") from exc

        identity = derive_identity(name)
        try:
            if step_rank < _DESTROY_STEP_RANK["drained"]:
                await self.store.assert_operation(name, operation_id)
                await self.policy.drop_dev_policy(identity)
                await self.store.set_operation_step(name, operation_id, "drained")
                await self.store.checkpoint()
                step_rank = _DESTROY_STEP_RANK["drained"]
            if step_rank < _DESTROY_STEP_RANK["namespace_deleted"]:
                await self.store.assert_operation(name, operation_id)
                await self.cluster.destroy(identity, keep_data=keep_data)
                await self.store.set_operation_step(name, operation_id, "namespace_deleted")
                await self.store.checkpoint()
                step_rank = _DESTROY_STEP_RANK["namespace_deleted"]
            if not keep_data:
                if step_rank < _DESTROY_STEP_RANK["database_deleted"]:
                    await self.store.assert_operation(name, operation_id)
                    await self.sql.drop_database_and_role(identity)
                    await self.store.set_operation_step(name, operation_id, "database_deleted")
                    await self.store.checkpoint()
                    step_rank = _DESTROY_STEP_RANK["database_deleted"]
                if step_rank < _DESTROY_STEP_RANK["buckets_deleted"]:
                    await self.store.assert_operation(name, operation_id)
                    await self.buckets.remove_buckets(identity, list(dev_buckets(identity)))
                    await self.store.set_operation_step(name, operation_id, "buckets_deleted")
                    await self.store.checkpoint()
                    step_rank = _DESTROY_STEP_RANK["buckets_deleted"]
                if step_rank < _DESTROY_STEP_RANK["tenant_deleted"]:
                    await self.store.assert_operation(name, operation_id)
                    await self.object_store_tenant.delete(identity)
                    await self.store.set_operation_step(name, operation_id, "tenant_deleted")
                    await self.store.checkpoint()
                await self.vault.delete(identity)
            result = await self.store.complete_operation(
                name,
                operation_id,
                status="deleted",
                now=datetime.now(UTC),
            )
            await self.store.checkpoint()
            return result
        except DevInstanceOperationFencedError:
            raise
        except Exception:
            await self.store.complete_operation(
                name,
                operation_id,
                status="failed",
                now=datetime.now(UTC),
                failure_reason="deletion_failed",
            )
            await self.store.checkpoint()
            raise


def dev_buckets(identity: DevInstanceIdentity) -> list[str]:
    """The instance's buckets (thin re-export so destroy needn't rebuild them)."""
    return [
        identity.task_bucket,
        identity.trajectories_bucket,
        identity.artifacts_bucket,
    ]
