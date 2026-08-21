"""Worker-facing durable Daytona sandbox lifecycle and cleanup journal."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from sqlalchemy import text

from loom.auth import verify_bearer_token

router = APIRouter()

_CANDIDATE_RE = re.compile(r"^[0-9a-f]{40}$")
_DIGEST_IMAGE_RE = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")
_PROVIDER_SCOPE_RE = re.compile(r"^[0-9a-f]{64}$")
_SANDBOX_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
_PER_SECOND_USD = Decimal("0.0001")


async def _require_worker(session: Any, authorization: str | None) -> None:
    ctx = await verify_bearer_token(session, authorization)
    if ctx is None or "worker:report" not in ctx.scopes:
        raise HTTPException(status_code=401, detail="not authorized to report")


def _as_utc(value: object, *, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"{field} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise HTTPException(status_code=400, detail=f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _row_payload(row: Any) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "trial_id": str(row["trial_id"]),
        "attempt_count": row["attempt_count"],
        "team_id": str(row["team_id"]),
        "worker_id": str(row["worker_id"]),
        "candidate_sha": row["candidate_sha"],
        "provider_scope": row["provider_scope"],
        "artifact_ref": row["artifact_ref"],
        "sandbox_name": row["sandbox_name"],
        "sandbox_id": row["sandbox_id"],
        "state": row["state"],
        "deadline_at": row["deadline_at"].isoformat(),
        "started_at": row["started_at"].isoformat() if row["started_at"] else None,
    }


@router.post("/workers/{worker_id}/daytona-sandboxes/reserve")
async def reserve_daytona_sandbox(
    worker_id: UUID,
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    try:
        trial_id = UUID(str(payload["trial_id"]))
        team_id = UUID(str(payload["team_id"]))
        attempt_count = int(payload["attempt_count"])
        candidate_sha = str(payload["candidate_sha"])
        provider_scope = str(payload["provider_scope"])
        artifact_ref = str(payload["artifact_ref"])
        sandbox_name = str(payload["sandbox_name"])
        deadline_at = _as_utc(payload["deadline_at"], field="deadline_at")
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"invalid reservation: {exc}") from exc
    if attempt_count <= 0:
        raise HTTPException(status_code=400, detail="attempt_count must be positive")
    if _CANDIDATE_RE.fullmatch(candidate_sha) is None:
        raise HTTPException(status_code=400, detail="candidate_sha must be an exact commit SHA")
    if _PROVIDER_SCOPE_RE.fullmatch(provider_scope) is None:
        raise HTTPException(status_code=400, detail="provider_scope must be a SHA-256 digest")
    if _DIGEST_IMAGE_RE.fullmatch(artifact_ref) is None:
        raise HTTPException(
            status_code=400, detail="artifact_ref must be an immutable image digest"
        )
    if _SANDBOX_NAME_RE.fullmatch(sandbox_name) is None:
        raise HTTPException(status_code=400, detail="sandbox_name is invalid")
    if deadline_at <= datetime.now(tz=UTC):
        raise HTTPException(status_code=400, detail="deadline_at must be in the future")

    async with request.app.state.session_factory() as session:
        await _require_worker(session, authorization)
        trial = (
            (
                await session.execute(
                    text(
                        """
                    SELECT id, team_id, worker_id, attempt_count, state
                      FROM trials
                     WHERE id=CAST(:trial_id AS uuid)
                       AND team_id=CAST(:team_id AS uuid)
                       AND worker_id=CAST(:worker_id AS uuid)
                       AND attempt_count=:attempt_count
                       AND state IN ('claimed','running')
                     FOR UPDATE
                    """
                    ),
                    {
                        "trial_id": trial_id,
                        "team_id": team_id,
                        "worker_id": worker_id,
                        "attempt_count": attempt_count,
                    },
                )
            )
            .mappings()
            .one_or_none()
        )
        if trial is None:
            raise HTTPException(status_code=409, detail="worker lost trial attempt ownership")
        await session.execute(
            text(
                """
                INSERT INTO daytona_sandboxes
                  (trial_id, attempt_count, team_id, worker_id, candidate_sha,
                   provider_scope, artifact_ref, sandbox_name, state, deadline_at)
                VALUES
                  (CAST(:trial_id AS uuid), :attempt_count, CAST(:team_id AS uuid),
                   CAST(:worker_id AS uuid), :candidate_sha, :provider_scope,
                   :artifact_ref, :sandbox_name, 'reserved', :deadline_at)
                ON CONFLICT (trial_id, attempt_count) DO NOTHING
                """
            ),
            {
                "trial_id": trial_id,
                "attempt_count": attempt_count,
                "team_id": team_id,
                "worker_id": worker_id,
                "candidate_sha": candidate_sha,
                "provider_scope": provider_scope,
                "artifact_ref": artifact_ref,
                "sandbox_name": sandbox_name,
                "deadline_at": deadline_at,
            },
        )
        row = (
            (
                await session.execute(
                    text(
                        """
                    SELECT * FROM daytona_sandboxes
                     WHERE trial_id=CAST(:trial_id AS uuid) AND attempt_count=:attempt_count
                     FOR UPDATE
                    """
                    ),
                    {"trial_id": trial_id, "attempt_count": attempt_count},
                )
            )
            .mappings()
            .one()
        )
        immutable = {
            "team_id": team_id,
            "worker_id": worker_id,
            "candidate_sha": candidate_sha,
            "provider_scope": provider_scope,
            "artifact_ref": artifact_ref,
            "sandbox_name": sandbox_name,
        }
        if any(str(row[key]) != str(value) for key, value in immutable.items()):
            raise HTTPException(status_code=409, detail="Daytona reservation identity drift")
        await session.commit()
    return _row_payload(row)


@router.post("/workers/{worker_id}/daytona-sandboxes/{ledger_id}/started")
async def mark_daytona_sandbox_started(
    worker_id: UUID,
    ledger_id: UUID,
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    sandbox_id = str(payload.get("sandbox_id") or "")
    if not sandbox_id or len(sandbox_id) > 512:
        raise HTTPException(status_code=400, detail="sandbox_id is required")
    started_at = _as_utc(payload.get("started_at"), field="started_at")
    async with request.app.state.session_factory() as session:
        await _require_worker(session, authorization)
        row = (
            (
                await session.execute(
                    text(
                        """
                    UPDATE daytona_sandboxes
                       SET sandbox_id=COALESCE(sandbox_id, :sandbox_id), state='running',
                           started_at=COALESCE(started_at, :started_at), last_error=NULL,
                           updated_at=now()
                     WHERE id=CAST(:ledger_id AS uuid)
                       AND worker_id=CAST(:worker_id AS uuid)
                       AND state IN ('reserved','running')
                       AND (sandbox_id IS NULL OR sandbox_id=:sandbox_id)
                 RETURNING *
                    """
                    ),
                    {
                        "ledger_id": ledger_id,
                        "worker_id": worker_id,
                        "sandbox_id": sandbox_id,
                        "started_at": started_at,
                    },
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise HTTPException(status_code=409, detail="Daytona start identity conflict")
        await session.commit()
    return _row_payload(row)


@router.post("/workers/{worker_id}/daytona-sandboxes/claim-cleanup")
async def claim_daytona_cleanup(
    worker_id: UUID,
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, Any] | None:
    provider_scope = str(payload.get("provider_scope") or "")
    if _PROVIDER_SCOPE_RE.fullmatch(provider_scope) is None:
        raise HTTPException(status_code=400, detail="provider_scope must be a SHA-256 digest")
    async with request.app.state.session_factory() as session:
        await _require_worker(session, authorization)
        row = (
            (
                await session.execute(
                    text(
                        """
                    WITH candidate AS (
                      SELECT d.id
                        FROM daytona_sandboxes d
                        JOIN trials t ON t.id=d.trial_id
                       WHERE d.provider_scope=:provider_scope
                         AND d.state <> 'deleted'
                         AND (d.cleanup_lease_expires_at IS NULL OR d.cleanup_lease_expires_at < now())
                         AND (
                           d.state='delete_pending' OR d.deadline_at <= now()
                           OR t.state IN ('succeeded','failed','cancelled')
                           OR t.attempt_count <> d.attempt_count
                           OR t.worker_id IS DISTINCT FROM d.worker_id
                         )
                       ORDER BY d.deadline_at, d.created_at
                       FOR UPDATE OF d SKIP LOCKED
                       LIMIT 1
                    )
                    UPDATE daytona_sandboxes d
                       SET cleanup_lease_worker_id=CAST(:worker_id AS uuid),
                           cleanup_lease_expires_at=now() + interval '2 minutes',
                           updated_at=now()
                      FROM candidate c
                     WHERE d.id=c.id
                 RETURNING d.*
                    """
                    ),
                    {"worker_id": worker_id, "provider_scope": provider_scope},
                )
            )
            .mappings()
            .one_or_none()
        )
        await session.commit()
    return _row_payload(row) if row is not None else None


@router.post("/workers/{worker_id}/daytona-sandboxes/{ledger_id}/deleted")
async def report_daytona_deleted(
    worker_id: UUID,
    ledger_id: UUID,
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    deleted = payload.get("deleted") is True
    stopped_at = _as_utc(payload.get("stopped_at"), field="stopped_at")
    error = str(payload.get("error") or "")[:2000] or None
    async with request.app.state.session_factory() as session:
        await _require_worker(session, authorization)
        row = (
            (
                await session.execute(
                    text(
                        """
                    SELECT * FROM daytona_sandboxes
                     WHERE id=CAST(:ledger_id AS uuid)
                       AND (
                         worker_id=CAST(:worker_id AS uuid)
                         OR cleanup_lease_worker_id=CAST(:worker_id AS uuid)
                       )
                     FOR UPDATE
                    """
                    ),
                    {"ledger_id": ledger_id, "worker_id": worker_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise HTTPException(status_code=409, detail="Daytona cleanup lease lost")
        if deleted and row["state"] != "deleted":
            if row["sandbox_id"] and row["started_at"] and row["usage_reported_at"] is None:
                seconds = max(0.0, (stopped_at - row["started_at"]).total_seconds())
                await session.execute(
                    text(
                        """
                        INSERT INTO cloud_compute_records
                          (team_id, trial_id, cloud_provider, sandbox_id, image,
                           started_at, stopped_at, compute_seconds, cost_usd)
                        VALUES
                          (CAST(:team_id AS uuid), CAST(:trial_id AS uuid), 'daytona',
                           :sandbox_id, :image, :started_at, :stopped_at,
                           :seconds, :cost)
                        """
                    ),
                    {
                        "team_id": row["team_id"],
                        "trial_id": row["trial_id"],
                        "sandbox_id": row["sandbox_id"],
                        "image": row["artifact_ref"],
                        "started_at": row["started_at"],
                        "stopped_at": stopped_at,
                        "seconds": seconds,
                        "cost": Decimal(str(seconds)) * _PER_SECOND_USD,
                    },
                )
            await session.execute(
                text(
                    """
                    UPDATE daytona_sandboxes
                       SET state='deleted', deleted_at=:stopped_at,
                           usage_reported_at=CASE
                             WHEN sandbox_id IS NOT NULL AND started_at IS NOT NULL
                             THEN COALESCE(usage_reported_at, now()) ELSE usage_reported_at END,
                           cleanup_lease_worker_id=NULL, cleanup_lease_expires_at=NULL,
                           last_error=NULL, updated_at=now()
                     WHERE id=CAST(:ledger_id AS uuid)
                    """
                ),
                {"ledger_id": ledger_id, "stopped_at": stopped_at},
            )
        elif not deleted and row["state"] != "deleted":
            await session.execute(
                text(
                    """
                    UPDATE daytona_sandboxes
                       SET state='delete_pending', last_error=:error,
                           cleanup_lease_worker_id=NULL, cleanup_lease_expires_at=NULL,
                           updated_at=now()
                     WHERE id=CAST(:ledger_id AS uuid)
                    """
                ),
                {"ledger_id": ledger_id, "error": error or "provider delete failed"},
            )
        await session.commit()
    final_state = "deleted" if row["state"] == "deleted" or deleted else "delete_pending"
    return {"id": str(ledger_id), "state": final_state}
