"""Bounded online execution admission; no private keys or signer I/O under locks."""

from __future__ import annotations

import asyncio
import math
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Protocol

from pydantic import TypeAdapter
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from loom.auth import verify_bearer_token
from loom.db.schema import Token
from loom_task_image_authority.contracts import BuildPurpose
from loom_task_image_authority.execution_delivery import TaskImageExecutionDelivery
from loom_task_image_authority.execution_grant import (
    MAX_EXECUTION_GRANT_ENVELOPE_BYTES,
    LegacyExecutionClaim,
)
from loom_task_image_authority.execution_start import ExecutionStartReceipt, ExecutionStartRequest
from loom_task_image_authority.execution_store import (
    consume_execution_start,
    finalize_execution_grant,
    lock_execution_claim,
    prepare_execution_signing_request,
)
from loom_task_image_authority.publication_contracts import CanonicalUUID
from loom_task_image_authority.publication_keyset import ExecutionGrantTrustRoot, _instant


class ExecutionSigner(Protocol):
    async def sign_execution(self, canonical_request: bytes, *, maximum_reply_bytes: int) -> bytes: ...


def _clock() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


class TaskImageExecutionService:
    """Explicit release-owned composition, disabled unless installed by the host.

    One deadline includes connection checkout, authentication, state-lock waits,
    signer I/O and commit/cleanup. A signature is prepared and committed in one
    transaction, then finalized in another. Start never retries a durable consume.
    The caller owns the engine and signer lifecycle.
    """

    def __init__(
        self, engine: AsyncEngine, *, trust_root: ExecutionGrantTrustRoot,
        purpose: BuildPurpose, shadow_campaign_id: str | None, signer: ExecutionSigner,
        clock: Callable[[], datetime] = _clock, timeout_seconds: float = 10.0,
    ) -> None:
        trust_root.__post_init__()
        TypeAdapter(BuildPurpose).validate_python(purpose, strict=True)
        if shadow_campaign_id is not None:
            TypeAdapter(CanonicalUUID).validate_python(shadow_campaign_id, strict=True)
        if (
            (purpose == "production") != (shadow_campaign_id is None)
            or type(timeout_seconds) is not float or not math.isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= 10
        ):
            raise ValueError("invalid fixed execution admission configuration")
        self._engine, self._root, self._purpose = engine, trust_root, purpose
        self._campaign, self._signer, self._clock, self._timeout = shadow_campaign_id, signer, clock, timeout_seconds

    @property
    def timeout_seconds(self) -> float:
        return self._timeout

    @property
    def native_ready_enabled(self) -> bool:
        # The shared production readiness journal does not advertise shadows.
        return self._purpose == "production"

    @asynccontextmanager
    async def _connection(self) -> AsyncIterator[AsyncSession]:
        async with self._engine.connect() as connection:
            await connection.execution_options(isolation_level="READ COMMITTED")
            async with AsyncSession(connection, expire_on_commit=False) as session:
                yield session

    async def _limits(self, session: AsyncSession) -> None:
        await session.execute(text("SET LOCAL search_path=pg_catalog,public,pg_temp"))
        await session.execute(text(
            "SELECT pg_catalog.set_config('statement_timeout', :bound, true), "
            "pg_catalog.set_config('idle_in_transaction_session_timeout', :bound, true)"
        ), {"bound": f"{math.ceil(self._timeout * 1000)}ms"})

    @asynccontextmanager
    async def _transaction(self, worker_token_hash: bytes) -> AsyncIterator[AsyncSession]:
        if type(worker_token_hash) is not bytes or len(worker_token_hash) != 32:
            raise PermissionError("worker token required")
        async with self._connection() as session, session.begin():
            await self._limits(session)
            # Token revocation is serialized before state -> worker -> Trial.
            # Do not use verify_bearer_token here: it owns a commit internally
            # and would release the authority locks before consumption.
            token = await session.scalar(select(Token).where(Token.token_hash == worker_token_hash).with_for_update(read=True))
            if (
                token is None or token.type != "worker" or "worker:claim" not in token.scopes
                or any(scope.startswith("admin:") for scope in token.scopes)
                or token.revoked_at is not None
                or (token.expires_at is not None and token.expires_at <= self._clock())
            ):
                raise PermissionError("current worker token is unavailable")
            yield session
            # Bounds are checked again before context-manager COMMIT; callers
            # additionally verify signed/receipt expiry after it returns.
            if token.expires_at is not None and token.expires_at <= self._clock():
                raise PermissionError("worker token expired during admission")

    async def issue(self, *, claim: LegacyExecutionClaim, worker_token_hash: bytes) -> TaskImageExecutionDelivery:
        async with asyncio.timeout(self._timeout):
            async with self._transaction(worker_token_hash) as session:
                request = await prepare_execution_signing_request(
                    session, claim=claim, worker_token_hash=worker_token_hash, trust_root=self._root,
                    purpose=self._purpose, shadow_campaign_id=self._campaign, clock=self._clock,
                )
            wire = await self._signer.sign_execution(request.canonical_bytes(), maximum_reply_bytes=MAX_EXECUTION_GRANT_ENVELOPE_BYTES)
            async with self._transaction(worker_token_hash) as session:
                delivery = await finalize_execution_grant(
                    session, wire=wire, claim=claim, worker_token_hash=worker_token_hash, trust_root=self._root,
                    purpose=self._purpose, shadow_campaign_id=self._campaign, clock=self._clock,
                )
            # The consumer verifies every attachment again; never deliver an
            # envelope whose validity was spent waiting for database COMMIT.
            from loom_task_image_authority.execution_grant import verify_execution_grant

            verify_execution_grant(
                wire=wire, plan_wire=delivery.frozen_plan.encode(),
                publication_wires=tuple(item.encode() for item in delivery.publications), keyset_wire=delivery.keyset.encode(),
                trust_root=self._root, expected_claim=claim, expected_purpose=self._purpose,
                expected_shadow_campaign_id=self._campaign, now=self._clock(),
            )
            return delivery

    async def consume(self, *, request: ExecutionStartRequest, authorization: str | None) -> ExecutionStartReceipt:
        if not isinstance(request.claim, LegacyExecutionClaim):
            raise ValueError("protected execution starts are not enabled")
        async with asyncio.timeout(self._timeout):
            async with self._connection() as session:
                await self._limits(session)
                auth = await verify_bearer_token(session, authorization)
                if auth is None or auth.type != "worker" or "worker:claim" not in auth.scopes:
                    raise PermissionError("worker start is unauthenticated")
            async with self._transaction(auth.token_hash) as session:
                receipt = await consume_execution_start(
                    session, request=request, claim=request.claim, worker_token_hash=auth.token_hash,
                    trust_root=self._root, purpose=self._purpose, shadow_campaign_id=self._campaign, clock=self._clock,
                )
            if not _instant(receipt.consumed_at) <= self._clock() < _instant(receipt.expires_at):
                raise ValueError("execution start acknowledgement expired during commit")
            return receipt

    async def refund_undelivered_claim(self, *, claim: LegacyExecutionClaim, worker_token_hash: bytes) -> None:
        """Return only this exact pre-start claim after failed issuance, without burning an attempt."""
        from uuid import UUID

        from loom_control_plane.routes.workers import _REQUEUE_TRIAL_RETRY_SQL

        async with asyncio.timeout(self._timeout), self._connection() as session, session.begin():
            await self._limits(session)
            # Internal compensation, not a worker-authenticated route. Token
            # revocation must not prevent returning an unstarted claimed slot.
            await lock_execution_claim(session, claim=claim, worker_token_hash=worker_token_hash)
            await session.execute(_REQUEUE_TRIAL_RETRY_SQL, dict(
                trial_id=UUID(claim.trial_id), worker_id=UUID(claim.worker_id),
                failure_reason="task_image_admission_unavailable",
                failure_message="Task-image grant issuance unavailable before worker delivery",
                retry_after_sec=30,
            ))
