"""Shared Control Plane authority for trial cancellation."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from uuid import UUID

from sqlalchemy import text

from loom_control_plane.protected_worker_session import ProtectedWorkerSessionStore
from loom_control_plane.service_execution import request_trial_execution_cancellation

_REQUEST_CANCEL_SQL = text("""
UPDATE trials
   SET state = CASE WHEN state = 'queued' THEN 'cancelled' ELSE state END,
       cancellation_requested_at = NOW(),
       cancellation_observed_at = CASE WHEN state = 'queued' THEN NOW()
                                       ELSE cancellation_observed_at END,
       finished_at = CASE WHEN state = 'queued' THEN COALESCE(finished_at, NOW())
                          ELSE finished_at END
 WHERE id = (:trial_id)::uuid
   AND ((:team_id)::uuid IS NULL OR team_id = (:team_id)::uuid)
   AND state IN ('queued', 'claimed', 'running')
   AND cancellation_requested_at IS NULL
 RETURNING id, state;
""")

_READ_CANCEL_REPLAY_SQL = text("""
SELECT id, state
  FROM trials
 WHERE id = (:trial_id)::uuid
   AND ((:team_id)::uuid IS NULL OR team_id = (:team_id)::uuid)
   AND cancellation_requested_at IS NOT NULL
   AND state IN ('claimed', 'running', 'cancelled');
""")


async def cancel_trial_under_authority(
    *,
    session_factory: Any,
    protected_store: ProtectedWorkerSessionStore | None,
    trial_id: UUID,
    team_id: UUID | None,
) -> Mapping[str, Any] | None:
    """Cancel one trial through protected authority with ordinary fallback."""

    row: Mapping[str, Any] | None = None
    if protected_store is not None:
        protected_cancellation = await protected_store.cancel_pending_trial(
            trial_id=trial_id,
            team_id=team_id,
        )
        if protected_cancellation is not None:
            row = {
                "id": protected_cancellation["trial_id"],
                "state": protected_cancellation["state"],
            }

    async with session_factory() as session:
        if row is None:
            row = (
                (
                    await session.execute(
                        _REQUEST_CANCEL_SQL,
                        {"trial_id": trial_id, "team_id": team_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                row = (
                    (
                        await session.execute(
                            _READ_CANCEL_REPLAY_SQL,
                            {"trial_id": trial_id, "team_id": team_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        if row is not None:
            await request_trial_execution_cancellation(session, trial_id=trial_id)
        await session.commit()
    return row
