"""Shared claim fencing and mutation idempotency for ExecutionAttempts."""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom.db.schema import ExecutionAttempt, ExecutionAttemptRequest, Worker
from loom.pipeline.keys import canonical_digest


@dataclass(frozen=True)
class AttemptFenceError(Exception):
    reason: str
    not_found: bool = False


async def verify_attempt_claim(
    session: AsyncSession,
    *,
    attempt_id: UUID,
    auth: AuthContext,
    claim_id: UUID,
    lease_epoch: int,
    lease_token: str | None,
    require_live_lease: bool = True,
    lock: bool = True,
) -> ExecutionAttempt:
    query = (
        select(ExecutionAttempt, Worker)
        .join(Worker, Worker.id == ExecutionAttempt.worker_id)
        .where(ExecutionAttempt.id == attempt_id)
    )
    if lock:
        query = query.with_for_update(of=ExecutionAttempt)
    row = (await session.execute(query)).one_or_none()
    if row is None:
        raise AttemptFenceError("attempt_not_found", not_found=True)
    attempt: ExecutionAttempt = row[0]
    worker: Worker = row[1]

    # The worker token accepted at registration is bound to the durable worker
    # row.  The Attempt then identifies that exact registered worker.  The
    # deployment mTLS principal remains the outer transport identity.
    if worker.auth_token_hash is None or not hmac.compare_digest(
        bytes(worker.auth_token_hash), auth.token_hash
    ):
        raise AttemptFenceError("claim_fenced")
    if attempt.claim_id != claim_id or attempt.lease_epoch != lease_epoch:
        raise AttemptFenceError("claim_fenced")
    if require_live_lease or lease_token is not None:
        if lease_token is None or attempt.lease_token_digest is None:
            raise AttemptFenceError("claim_fenced")
        supplied_digest = hashlib.sha256(lease_token.encode()).hexdigest()
        if not hmac.compare_digest(supplied_digest, attempt.lease_token_digest):
            raise AttemptFenceError("claim_fenced")
    if require_live_lease:
        now = datetime.now(UTC)
        if (
            attempt.state not in {"claimed", "running"}
            or attempt.lease_expires_at is None
            or attempt.lease_expires_at <= now
        ):
            raise AttemptFenceError("claim_fenced")
    return attempt


async def replay_or_conflict(
    session: AsyncSession,
    *,
    attempt_id: UUID,
    route: str,
    request_id: UUID,
    payload: Any,
) -> dict[str, Any] | None:
    request_digest = canonical_digest(payload)
    row = (
        await session.execute(
            select(ExecutionAttemptRequest).where(
                ExecutionAttemptRequest.execution_attempt_id == attempt_id,
                ExecutionAttemptRequest.route == route,
                ExecutionAttemptRequest.request_id == request_id,
            )
        )
    ).scalar_one_or_none()
    if row is None:
        return None
    if not hmac.compare_digest(row.request_digest, request_digest):
        raise AttemptFenceError("idempotency_conflict")
    return dict(row.response_json)


def idempotency_values(
    *,
    attempt_id: UUID,
    route: str,
    request_id: UUID,
    payload: Any,
    response: dict[str, Any],
    status_code: int = 200,
) -> dict[str, Any]:
    return {
        "execution_attempt_id": attempt_id,
        "route": route,
        "request_id": request_id,
        "request_digest": canonical_digest(payload),
        "response_json": response,
        "status_code": status_code,
    }
