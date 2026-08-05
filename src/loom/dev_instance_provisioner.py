"""Idempotent orchestration for provisioning one ``dev-<name>`` instance.

The guarded ``dev-instances`` endpoint and the ``loom dev`` CLI both drive the
*same* create/destroy sequence through this module, so the guardrail and the
step order live in exactly one place. Every side-effecting step is a small
``Protocol`` — SQL (role + database on the shared dev fixture), object-store
buckets, the capped autoscaler policy, the per-instance cluster deploy, and the
instance registry — so the orchestration is unit-testable with in-memory fakes
and the live executors (psycopg / boto3 / control-plane httpx / kube) plug in
behind the same seams.

``NoOp`` cluster/secret defaults let the endpoint run and be exercised before
the shared dev fixture (Postgres + MinIO) and the per-instance k8s deploy land
(design phases 2b/3b) — see ``docs/architecture/multi-dev-env-design.md``.

Create is **fail-closed** (``validate_dev_instance`` runs before any mutation)
and **idempotent** (get-or-converge on the registry), so a re-run after a
partial failure converges rather than half-applying.
"""

from __future__ import annotations

import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

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


class DevInstanceRejectedError(ValueError):
    """The requested instance is outside the dev envelope — no mutation ran.

    Carries the human-readable reasons from :func:`validate_dev_instance` so
    the endpoint can surface them as a 400.
    """

    def __init__(self, reasons: list[str]) -> None:
        self.reasons = list(reasons)
        super().__init__("; ".join(reasons) or "dev instance rejected")


@dataclass(frozen=True)
class DevInstanceRecord:
    """The registry view of one instance (what GET/list return)."""

    name: str
    owner_user_id: str | None
    min_slots: int
    max_slots: int
    status: str  # provisioning | ready | deleting | deleted
    created_at: datetime
    secret_ref: str | None = None


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

    async def deploy(self, identity: DevInstanceIdentity) -> None: ...

    async def destroy(self, identity: DevInstanceIdentity, *, keep_data: bool) -> None: ...


class SecretVault(Protocol):
    """Stores the generated role password; returns an opaque ref."""

    async def store(self, identity: DevInstanceIdentity, password: str) -> str: ...

    async def delete(self, identity: DevInstanceIdentity) -> None: ...


class InstanceStore(Protocol):
    """Persists the registry rows the budget accounting + listing read."""

    async def get(self, name: str) -> DevInstanceRecord | None: ...

    async def list_active(self) -> list[DevInstanceRecord]: ...

    async def upsert(self, record: DevInstanceRecord) -> None: ...

    async def set_status(self, name: str, status: str) -> None: ...

    async def soft_delete(self, name: str) -> None: ...


# ── Orchestrator ─────────────────────────────────────────────────────────────


@dataclass
class DevInstanceProvisioner:
    """Composes the step seams into idempotent create / destroy sequences."""

    store: InstanceStore
    sql: SqlExecutor
    buckets: BucketEnsurer
    policy: PolicyRegistrar
    cluster: ClusterProvisioner
    vault: SecretVault
    # Injectable so tests are deterministic; default = a 20-hex-char secret that
    # satisfies dev_instance_provision's _HEX_PASSWORD guard.
    password_factory: Callable[[], str] = field(
        default=lambda: secrets.token_hex(10),
    )

    async def create(
        self,
        name: str,
        *,
        owner_user_id: str | None,
        min_slots: int,
        max_slots: int,
        now: datetime | None = None,
    ) -> DevInstanceRecord:
        """Provision (or converge) ``dev-<name>`` and return its registry record.

        Fail-closed: the envelope check runs against every *other* live instance
        before any mutation. Idempotent: an already-``ready`` instance with the
        same shape is returned unchanged.
        """
        now = now or datetime.now(UTC)
        requested = RequestedPolicy(actuator=DEV_ACTUATOR, min_slots=min_slots, max_slots=max_slots)

        existing = await self.store.get(name)
        others = [
            DevInstanceRef(name=r.name, max_slots=r.max_slots)
            for r in await self.store.list_active()
            if r.name != name
        ]
        errors = validate_dev_instance(name, requested, others)
        if errors:
            raise DevInstanceRejectedError(errors)

        if existing is not None and existing.status == "ready":
            # Converged already (idempotent re-create with the same envelope).
            return existing

        identity = derive_identity(name)
        password = self.password_factory()
        plan = provisioning_plan(name, password)

        # Order matters: data plane (role+DB, buckets, secret) → registry row →
        # control-plane policy → cluster deploy → ready. Each step is idempotent
        # so a re-run after a mid-sequence failure converges.
        await self.sql.apply_role_and_database(
            identity,
            role_sql=str(plan["role_sql"]),
            create_database_sql=str(plan["create_database_sql"]),
        )
        # `dev_buckets(identity)` == plan["buckets"] but stays typed as list[str]
        # (provisioning_plan returns dict[str, object]).
        await self.buckets.ensure_buckets(identity, dev_buckets(identity))
        secret_ref = await self.vault.store(identity, password)

        record = DevInstanceRecord(
            name=name,
            owner_user_id=owner_user_id,
            min_slots=min_slots,
            max_slots=max_slots,
            status="provisioning",
            created_at=(existing.created_at if existing else now),
            secret_ref=secret_ref,
        )
        await self.store.upsert(record)

        await self.policy.upsert_dev_policy(identity, requested)
        await self.cluster.deploy(identity)

        await self.store.set_status(name, "ready")
        return DevInstanceRecord(**{**record.__dict__, "status": "ready"})

    async def destroy(self, name: str, *, keep_data: bool = False) -> DevInstanceRecord | None:
        """Tear down ``dev-<name>`` idempotently; ``None`` if it doesn't exist.

        Reverse of create: drop the policy (draining the pool) → destroy the
        namespace → soft-delete the registry row → drop DB/buckets/secret unless
        ``keep_data``.
        """
        existing = await self.store.get(name)
        if existing is None or existing.status == "deleted":
            return None

        identity = derive_identity(name)
        await self.store.set_status(name, "deleting")
        await self.policy.drop_dev_policy(identity)
        await self.cluster.destroy(identity, keep_data=keep_data)
        if not keep_data:
            await self.sql.drop_database_and_role(identity)
            await self.buckets.remove_buckets(identity, list(dev_buckets(identity)))
            await self.vault.delete(identity)
        await self.store.soft_delete(name)
        return DevInstanceRecord(**{**existing.__dict__, "status": "deleted"})


def dev_buckets(identity: DevInstanceIdentity) -> list[str]:
    """The instance's buckets (thin re-export so destroy needn't rebuild them)."""
    return [
        identity.task_bucket,
        identity.trajectories_bucket,
        identity.artifacts_bucket,
    ]
