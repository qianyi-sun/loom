"""Prepare inventory outside the catalog barrier; recheck only under its caller's fence.

These internal database helpers are neither reference eligibility nor retirement,
execution-start, maintenance, or deletion authority. No caller-supplied snapshot
may replace preparation in the owned retirement transaction.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from uuid import UUID

import rfc8785
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import load_only

from loom.db.schema import (
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImageRegistryCredentialGeneration,
)
from loom.task_image_build_plan import MAX_TASK_IMAGE_BUILD_PLAN_BYTES
from loom_task_image_authority.retention_inventory import (
    AttemptRepositoryInventory,
    derive_attempt_repository_inventory,
)

MAX_RETIREMENT_CREDENTIALS = 128 * 512
_MATERIALIZATION_FIELDS = ("id", "materialization_key", "task_id", "task_checksum", "cpu_arch")
_ATTEMPT_FIELDS = (
    "id", "materialization_id", "attempt_number", "lease_epoch", "builder_id", "grant_id",
    "session_id", "session_generation", "claim_id", "claim_plan_sha256",
)
_PUBLIC_CREDENTIAL_FIELDS = tuple(
    column.name for column in TaskImageRegistryCredentialGeneration.__table__.columns
    if column.name not in ("secret_response_ref", "request_sha256", "recorded_at")
)


class RetirementInventoryUnavailableError(ValueError):
    """A bounded committed inventory with active immutability could not be established."""


class RetirementInventoryChangedError(RuntimeError):
    """Release the whole retirement fence and prepare again outside it."""


@dataclass(frozen=True)
class PreparedAttemptRetirementInventory:
    inventory: AttemptRepositoryInventory
    credential_count: int
    materialization_values: tuple[object, ...]
    attempt_values: tuple[object, ...]
    canonical_plan: bytes


async def _require_credential_guards(session: AsyncSession) -> None:
    # Schema administration is trusted throughout both phases. Checking active
    # guards at each endpoint cannot detect an administrator bypassing them in
    # between; a migration-version string alone does not even establish this much.
    immutable, ingress = (await session.execute(text("""
        SELECT pg_catalog.current_setting('session_replication_role') = 'origin' AND EXISTS (
          SELECT 1 FROM pg_catalog.pg_trigger
          WHERE tgrelid = pg_catalog.to_regclass('public.task_image_registry_credentials')
            AND tgname = 'task_image_registry_credentials_preserve'
            AND tgfoid = pg_catalog.to_regprocedure('public.task_image_registry_preserve_audit()')
            AND tgenabled IN ('O', 'A') AND tgtype = 58 AND NOT tgisinternal
            AND tgqual IS NULL AND tgattr = ''::pg_catalog.int2vector AND tgnargs = 0
        ), EXISTS (
          SELECT 1 FROM pg_catalog.pg_trigger AS t
          JOIN pg_catalog.pg_proc AS p ON p.oid = t.tgfoid
          WHERE t.tgrelid = pg_catalog.to_regclass('public.task_image_registry_credentials')
            AND t.tgname = 'task_image_registry_credentials_not_retired'
            AND t.tgfoid = pg_catalog.to_regprocedure('public.task_image_registry_reject_retired_attempt()')
            AND t.tgenabled IN ('O', 'A') AND t.tgtype = 5 AND NOT t.tgisinternal
            AND t.tgqual IS NULL AND t.tgattr = ''::pg_catalog.int2vector AND t.tgnargs = 0
            AND NOT t.tgdeferrable AND NOT t.tginitdeferred
            AND p.provolatile = 'v' AND p.prosecdef
            AND p.proconfig @> ARRAY['search_path=pg_catalog', 'row_security=off']::pg_catalog.text[]
            AND pg_catalog.cardinality(p.proconfig) = 2
        )
    """))).one()
    if not immutable:
        raise RetirementInventoryUnavailableError("credential immutability guard unavailable")
    if not ingress:
        raise RetirementInventoryUnavailableError("credential retirement guard unavailable")


def _prepare_detached(
    row: TaskImageMaterialization,
    attempt: TaskImageMaterializationAttempt,
    credentials: list[TaskImageRegistryCredentialGeneration],
    registry_origin: str,
) -> PreparedAttemptRetirementInventory:
    try:
        plan = rfc8785.dumps(attempt.claim_plan_json)
        if len(plan) > MAX_TASK_IMAGE_BUILD_PLAN_BYTES:
            raise ValueError("frozen plan exceeds bound")
        inventory = derive_attempt_repository_inventory(
            materialization=row, attempt=attempt, credentials=credentials,
            registry_origin=registry_origin,
        )
    except (TypeError, ValueError) as exc:
        raise RetirementInventoryUnavailableError("retirement inventory unavailable") from exc
    return PreparedAttemptRetirementInventory(
        inventory,
        len(credentials),
        tuple(getattr(row, field) for field in _MATERIALIZATION_FIELDS),
        tuple(getattr(attempt, field) for field in _ATTEMPT_FIELDS),
        plan,
    )


async def prepare_attempt_retirement_inventory(
    engine: AsyncEngine, *, attempt_id: UUID, registry_origin: str,
) -> PreparedAttemptRetirementInventory:
    """Load a committed, bounded, owned snapshot, then validate without DB locks.

    The fresh session cannot autoflush or reuse another caller's identity map.
    Loaded ORM/JSON values are owned here, detached on close, and never returned.
    Expensive public-schema/canonical validation runs off the event loop only
    AFTER transaction and connection release. Preparation acquires no catalog
    barrier or row locks and does not observe references or write retirement.
    """
    if type(attempt_id) is not UUID or attempt_id.int == 0:
        raise RetirementInventoryUnavailableError("retirement attempt identity unavailable")
    async with engine.connect() as connection:
        # Set characteristics AFTER checkout: inherited engine connection hooks
        # can override a derived engine's options, including retaining autocommit.
        await connection.execution_options(isolation_level="READ COMMITTED")
        async with AsyncSession(
            connection, expire_on_commit=False, autoflush=False,
        ) as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(text("SET LOCAL statement_timeout = '5s'"))
            await session.execute(text("SET LOCAL idle_in_transaction_session_timeout = '5s'"))
            await _require_credential_guards(session)
            attempt = await session.scalar(
                select(TaskImageMaterializationAttempt)
                .options(load_only(
                    *(getattr(TaskImageMaterializationAttempt, field) for field in _ATTEMPT_FIELDS),
                    TaskImageMaterializationAttempt.claim_plan_json, raiseload=True,
                ))
                .where(TaskImageMaterializationAttempt.id == attempt_id)
            )
            if attempt is None:
                raise RetirementInventoryUnavailableError("retirement attempt unavailable")
            row = await session.scalar(
                select(TaskImageMaterialization)
                .options(load_only(
                    *(getattr(TaskImageMaterialization, field) for field in _MATERIALIZATION_FIELDS),
                    raiseload=True,
                ))
                .where(TaskImageMaterialization.id == attempt.materialization_id)
            )
            if row is None:
                raise RetirementInventoryUnavailableError("retirement materialization unavailable")
            credentials = list(await session.scalars(
                select(TaskImageRegistryCredentialGeneration)
                .options(load_only(
                    *(getattr(TaskImageRegistryCredentialGeneration, field)
                      for field in _PUBLIC_CREDENTIAL_FIELDS), raiseload=True,
                ))
                .where(TaskImageRegistryCredentialGeneration.materialization_attempt_id == attempt_id)
                .limit(MAX_RETIREMENT_CREDENTIALS + 1)
            ))
            if len(credentials) > MAX_RETIREMENT_CREDENTIALS:
                raise RetirementInventoryUnavailableError("retirement credential inventory exceeds bound")
    return await asyncio.to_thread(_prepare_detached, row, attempt, credentials, registry_origin)


async def revalidate_retirement_inventory(
    session: AsyncSession, *, prepared: PreparedAttemptRetirementInventory,
) -> None:
    """Recheck exact evidence and bounded cardinality under an ALREADY-HELD fence.

    Caller owns a fresh READ COMMITTED transaction, catalog barrier and exact
    materialization -> attempt FOR UPDATE locks, with total/statement/idle bounds.
    It must abort the entire transaction on mismatch, then prepare again outside
    it. This helper neither acquires that fence nor authorizes any mutation.

    Immutable prepared rows S cannot leave committed set F. After the attempt
    fence stops insertion, S subset F and equal cardinality imply equality.
    An active INSERT trigger now permanently rejects raw credential insertion
    after retirement. This does not deny registry requests using existing tokens
    or establish writer quiescence; FK locking alone supplies neither guarantee.
    """
    if session.new or session.dirty or session.deleted:
        raise RetirementInventoryUnavailableError("retirement recheck contains unflushed changes")
    if (
        type(prepared.credential_count) is not int
        or not 0 <= prepared.credential_count <= MAX_RETIREMENT_CREDENTIALS
        or len(prepared.canonical_plan) > MAX_TASK_IMAGE_BUILD_PLAN_BYTES
    ):
        raise RetirementInventoryUnavailableError("prepared retirement inventory exceeds bounds")
    await _require_credential_guards(session)
    identity = (
        select(1).select_from(TaskImageMaterialization)
        .join(TaskImageMaterializationAttempt,
              TaskImageMaterializationAttempt.materialization_id == TaskImageMaterialization.id)
        .where(
            *(getattr(TaskImageMaterialization, field) == value for field, value in
              zip(_MATERIALIZATION_FIELDS, prepared.materialization_values, strict=True)),
            *(getattr(TaskImageMaterializationAttempt, field) == value for field, value in
              zip(_ATTEMPT_FIELDS, prepared.attempt_values, strict=True)),
            TaskImageMaterializationAttempt.claim_plan_json == json.loads(prepared.canonical_plan),
        )
    )
    if not await session.scalar(select(identity.exists())):
        raise RetirementInventoryChangedError("retirement input identity changed")
    bounded_credentials = (
        select(TaskImageRegistryCredentialGeneration.credential_id)
        .where(TaskImageRegistryCredentialGeneration.materialization_attempt_id
               == prepared.inventory.attempt_id)
        .limit(prepared.credential_count + 1)
        .subquery()
    )
    count = await session.scalar(select(func.count()).select_from(bounded_credentials))
    if count != prepared.credential_count:
        raise RetirementInventoryChangedError("retirement credential inventory changed")
