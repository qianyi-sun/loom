"""Terminus-2 execution reclaim.

Scheduler retry still claims the same trial. This module only records:
one execution (same K1/K2), a new run_attempt, and the latest episode
checkpoint.

Harbor cannot resume mid-session. The runtime therefore **fails closed**
when a checkpoint exists (`resumed` or bad checksum) so a second
episode-1 Harbor run is never appended onto the same trajectory.
A first start with no checkpoint remains `fresh`.
"""

from __future__ import annotations

import hashlib
from uuid import UUID, uuid4

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    EpisodeCheckpoint,
    ModelSwitchPlan,
    TerminusAgentExecution,
    TerminusAgentRunAttempt,
)
from loom.models.model_switch_plan import TerminusRecoveryState


def checkpoint_checksum(
    *,
    execution_id: UUID,
    episode: int,
    active_role: str,
    last_call_ordinal: int,
    last_seq: int,
    tmux_session_id: str | None,
) -> str:
    payload = (
        f"{execution_id}:{episode}:{active_role}:"
        f"{last_call_ordinal}:{last_seq}:{tmux_session_id or ''}"
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def verify_checkpoint(row: EpisodeCheckpoint) -> bool:
    expected = checkpoint_checksum(
        execution_id=row.execution_id,
        episode=row.episode,
        active_role=row.active_role,
        last_call_ordinal=row.last_call_ordinal,
        last_seq=row.last_seq,
        tmux_session_id=row.tmux_session_id,
    )
    return row.checksum == expected


async def reclaim_terminus_execution(
    session: AsyncSession,
    *,
    trial_id: UUID,
    step_id: str,
    worker_id: UUID | None = None,
) -> TerminusRecoveryState:
    plan = (
        await session.execute(
            select(ModelSwitchPlan).where(ModelSwitchPlan.trial_id == trial_id),
        )
    ).scalar_one_or_none()
    execution = (
        await session.execute(
            select(TerminusAgentExecution).where(
                TerminusAgentExecution.trial_id == trial_id,
                TerminusAgentExecution.step_id == step_id,
            ),
        )
    ).scalar_one_or_none()
    if execution is None:
        execution = TerminusAgentExecution(
            id=uuid4(),
            trial_id=trial_id,
            step_id=step_id,
            model_switch_plan_id=None if plan is None else plan.id,
        )
        session.add(execution)
        await session.flush()
        attempt = TerminusAgentRunAttempt(
            id=uuid4(),
            execution_id=execution.id,
            attempt_number=1,
            worker_id=worker_id,
            state="running",
        )
        session.add(attempt)
        await session.flush()
        return TerminusRecoveryState(
            agent_execution_id=execution.id,
            agent_run_attempt_id=attempt.id,
            attempt_number=1,
            recovery="fresh",
        )

    latest = (
        await session.execute(
            select(EpisodeCheckpoint)
            .where(EpisodeCheckpoint.execution_id == execution.id)
            .order_by(EpisodeCheckpoint.version.desc())
            .limit(1),
        )
    ).scalar_one_or_none()
    max_attempt = (
        await session.execute(
            select(func.max(TerminusAgentRunAttempt.attempt_number)).where(
                TerminusAgentRunAttempt.execution_id == execution.id,
            ),
        )
    ).scalar_one()
    next_number = int(max_attempt or 0) + 1

    if latest is not None and not verify_checkpoint(latest):
        attempt = TerminusAgentRunAttempt(
            id=uuid4(),
            execution_id=execution.id,
            attempt_number=next_number,
            worker_id=worker_id,
            state="recovery_failed",
        )
        session.add(attempt)
        await session.flush()
        return TerminusRecoveryState(
            agent_execution_id=execution.id,
            agent_run_attempt_id=attempt.id,
            attempt_number=next_number,
            recovery="recovery_failed",
            last_episode=latest.episode,
            active_role=latest.active_role,
            last_call_ordinal=latest.last_call_ordinal,
            last_seq=latest.last_seq,
            checksum=latest.checksum,
        )

    attempt = TerminusAgentRunAttempt(
        id=uuid4(),
        execution_id=execution.id,
        attempt_number=next_number,
        worker_id=worker_id,
        state="running",
    )
    session.add(attempt)
    await session.flush()
    if latest is None:
        return TerminusRecoveryState(
            agent_execution_id=execution.id,
            agent_run_attempt_id=attempt.id,
            attempt_number=next_number,
            recovery="fresh",
        )
    return TerminusRecoveryState(
        agent_execution_id=execution.id,
        agent_run_attempt_id=attempt.id,
        attempt_number=next_number,
        recovery="resumed",
        last_episode=latest.episode,
        active_role=latest.active_role,
        last_call_ordinal=latest.last_call_ordinal,
        last_seq=latest.last_seq,
        checksum=latest.checksum,
    )


async def write_episode_checkpoint(
    session: AsyncSession,
    *,
    execution_id: UUID,
    run_attempt_id: UUID,
    episode: int,
    active_role: str,
    last_call_ordinal: int,
    last_seq: int,
    tmux_session_id: str | None,
) -> EpisodeCheckpoint:
    max_version = (
        await session.execute(
            select(func.max(EpisodeCheckpoint.version)).where(
                EpisodeCheckpoint.execution_id == execution_id,
            ),
        )
    ).scalar_one()
    version = int(max_version or 0) + 1
    row = EpisodeCheckpoint(
        id=uuid4(),
        execution_id=execution_id,
        run_attempt_id=run_attempt_id,
        version=version,
        episode=episode,
        tmux_session_id=tmux_session_id,
        active_role=active_role,
        last_call_ordinal=last_call_ordinal,
        last_seq=last_seq,
        checksum=checkpoint_checksum(
            execution_id=execution_id,
            episode=episode,
            active_role=active_role,
            last_call_ordinal=last_call_ordinal,
            last_seq=last_seq,
            tmux_session_id=tmux_session_id,
        ),
    )
    session.add(row)
    await session.flush()
    return row


async def mark_run_attempt_state(
    session: AsyncSession,
    run_attempt_id: UUID,
    state: str,
) -> None:
    await session.execute(
        update(TerminusAgentRunAttempt)
        .where(TerminusAgentRunAttempt.id == run_attempt_id)
        .values(state=state),
    )
