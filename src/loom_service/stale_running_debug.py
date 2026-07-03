"""Service-side stale-running debug context.

The control plane owns the reclaim decision. The service mirrors the same
conservative default thresholds for debug payloads so operators can see why a
running trial would be kept or reclaimed without scraping CP logs.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

from sqlalchemy import func, select

from loom.db.schema import LlmCall, Task, TrialEvent, Worker
from loom.trial.stale_running import (
    StaleRunningDecision,
    evaluate_stale_running_trial,
)

DEBUG_WORKER_HEARTBEAT_EXPIRY_SEC = 120.0
DEBUG_STALE_RUNNING_TIMEOUT_MULTIPLIER = 3.0
DEBUG_STALE_RUNNING_GRACE_SEC = 900.0
DEBUG_STALE_RUNNING_SILENCE_SEC = 900.0


@dataclass(frozen=True)
class StaleRunningDebugPolicy:
    worker_heartbeat_expiry_sec: float
    reclaim_enabled: bool
    timeout_multiplier: float
    grace_sec: float
    silence_sec: float


def _float_setting(settings: Any | None, name: str, default: float) -> float:
    if settings is None:
        return default
    raw = getattr(settings, name, default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _bool_setting(settings: Any | None, name: str, default: bool) -> bool:
    if settings is None:
        return default
    raw = getattr(settings, name, default)
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        return raw.strip().lower() not in {"0", "false", "no", "off"}
    return bool(raw)


def stale_running_debug_policy(settings: Any | None = None) -> StaleRunningDebugPolicy:
    return StaleRunningDebugPolicy(
        worker_heartbeat_expiry_sec=_float_setting(
            settings,
            "worker_heartbeat_expiry_sec",
            DEBUG_WORKER_HEARTBEAT_EXPIRY_SEC,
        ),
        reclaim_enabled=_bool_setting(
            settings,
            "stale_running_trial_reclaim_enabled",
            True,
        ),
        timeout_multiplier=_float_setting(
            settings,
            "stale_running_trial_timeout_multiplier",
            DEBUG_STALE_RUNNING_TIMEOUT_MULTIPLIER,
        ),
        grace_sec=_float_setting(
            settings,
            "stale_running_trial_grace_sec",
            DEBUG_STALE_RUNNING_GRACE_SEC,
        ),
        silence_sec=_float_setting(
            settings,
            "stale_running_trial_silence_sec",
            DEBUG_STALE_RUNNING_SILENCE_SEC,
        ),
    )


async def trial_stale_running_debug_context(
    session: Any,
    trial: Any,
    *,
    task: Any | None,
    llm_calls: Sequence[LlmCall],
    settings: Any | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return keyword args for ``build_trial_debug_evidence``."""

    observed_at = now or datetime.now(UTC)
    worker = await _worker_for_trial(session, trial)
    last_event = await _latest_trial_event(session, trial.id)
    decision = _decision_for_trial(
        trial,
        task_config=(getattr(task, "config", None) if task is not None else None),
        last_event_at=(last_event.created_at if last_event is not None else None),
        last_llm_call_at=_latest_llm_call_at(llm_calls),
        worker=worker,
        policy=stale_running_debug_policy(settings),
        now=observed_at,
    )
    return {
        "worker": worker,
        "last_event": last_event,
        "stale_running_decision": decision,
        "now": observed_at,
    }


async def batch_stale_running_decisions(
    session: Any,
    trials: Sequence[Any],
    *,
    llm_calls: Sequence[LlmCall],
    settings: Any | None = None,
    now: datetime | None = None,
) -> dict[UUID, StaleRunningDecision]:
    """Evaluate stale-running diagnostics for a batch's trial projections."""

    if not trials:
        return {}
    observed_at = now or datetime.now(UTC)
    task_configs = await _task_configs_by_id(session, trials)
    workers = await _workers_by_id(session, trials)
    last_events = await _latest_events_by_trial_id(session, trials)
    latest_llm_calls = _latest_llm_call_at_by_trial_id(llm_calls)
    policy = stale_running_debug_policy(settings)
    decisions: dict[UUID, StaleRunningDecision] = {}
    for trial in trials:
        trial_id = getattr(trial, "id", None)
        if trial_id is None:
            continue
        worker_id = getattr(trial, "worker_id", None)
        last_event = last_events.get(trial_id)
        decisions[trial_id] = _decision_for_trial(
            trial,
            task_config=task_configs.get(str(getattr(trial, "task_id", ""))),
            last_event_at=(last_event.created_at if last_event is not None else None),
            last_llm_call_at=latest_llm_calls.get(trial_id),
            worker=workers.get(worker_id) if isinstance(worker_id, UUID) else None,
            policy=policy,
            now=observed_at,
        )
    return decisions


async def _worker_for_trial(session: Any, trial: Any) -> Worker | None:
    worker_id = getattr(trial, "worker_id", None)
    if worker_id is None:
        return None
    result = await session.execute(select(Worker).where(Worker.id == worker_id))
    return cast(Worker | None, result.scalar_one_or_none())


async def _workers_by_id(session: Any, trials: Sequence[Any]) -> dict[UUID, Worker]:
    worker_ids = sorted(
        {trial.worker_id for trial in trials if getattr(trial, "worker_id", None) is not None},
        key=str,
    )
    if not worker_ids:
        return {}
    return {
        worker.id: worker
        for worker in (
            await session.execute(select(Worker).where(Worker.id.in_(worker_ids)))
        )
        .scalars()
        .all()
    }


async def _task_configs_by_id(
    session: Any,
    trials: Sequence[Any],
) -> dict[str, dict[str, Any]]:
    task_ids = sorted(
        {str(trial.task_id) for trial in trials if getattr(trial, "task_id", None)},
    )
    if not task_ids:
        return {}
    rows = (
        await session.execute(
            select(Task.id, Task.config).where(Task.id.in_(task_ids)),
        )
    ).all()
    return {
        str(task_id): config
        for task_id, config in rows
        if isinstance(config, dict)
    }


async def _latest_trial_event(session: Any, trial_id: UUID) -> TrialEvent | None:
    result = await session.execute(
        select(TrialEvent)
        .where(TrialEvent.trial_id == trial_id)
        .order_by(
            TrialEvent.created_at.desc(),
            TrialEvent.seq.desc(),
            TrialEvent.id.desc(),
        )
        .limit(1),
    )
    return cast(TrialEvent | None, result.scalar_one_or_none())


async def _latest_events_by_trial_id(
    session: Any,
    trials: Sequence[Any],
) -> dict[UUID, TrialEvent]:
    trial_ids = [trial.id for trial in trials if getattr(trial, "id", None) is not None]
    if not trial_ids:
        return {}
    ranked = (
        select(
            TrialEvent.id,
            func.row_number()
            .over(
                partition_by=TrialEvent.trial_id,
                order_by=(
                    TrialEvent.created_at.desc(),
                    TrialEvent.seq.desc(),
                    TrialEvent.id.desc(),
                ),
            )
            .label("rank"),
        )
        .where(TrialEvent.trial_id.in_(trial_ids))
        .subquery()
    )
    rows = (
        await session.execute(
            select(TrialEvent)
            .join(ranked, TrialEvent.id == ranked.c.id)
            .where(ranked.c.rank == 1),
        )
    ).scalars()
    return {event.trial_id: event for event in rows}


def _latest_llm_call_at(llm_calls: Sequence[LlmCall]) -> datetime | None:
    return max(
        (
            captured_at
            for call in llm_calls
            if (captured_at := getattr(call, "captured_at", None)) is not None
        ),
        default=None,
    )


def _latest_llm_call_at_by_trial_id(
    llm_calls: Sequence[LlmCall],
) -> dict[UUID, datetime]:
    latest: dict[UUID, datetime] = {}
    for call in llm_calls:
        trial_id = getattr(call, "trial_id", None)
        captured_at = getattr(call, "captured_at", None)
        if trial_id is None or captured_at is None:
            continue
        previous = latest.get(trial_id)
        if previous is None or captured_at > previous:
            latest[trial_id] = captured_at
    return latest


def _decision_for_trial(
    trial: Any,
    *,
    task_config: Any,
    last_event_at: datetime | None,
    last_llm_call_at: datetime | None,
    worker: Worker | None,
    policy: StaleRunningDebugPolicy,
    now: datetime,
) -> StaleRunningDecision:
    decision = evaluate_stale_running_trial(
        state=str(getattr(trial, "state", "")),
        started_at=getattr(trial, "started_at", None),
        finished_at=getattr(trial, "finished_at", None),
        trial_config=getattr(trial, "config", None),
        task_config=task_config,
        last_event_at=last_event_at,
        last_llm_call_at=last_llm_call_at,
        worker_last_seen_at=(worker.last_seen_at if worker is not None else None),
        now=now,
        worker_heartbeat_expiry_sec=policy.worker_heartbeat_expiry_sec,
        timeout_multiplier=policy.timeout_multiplier,
        grace_sec=policy.grace_sec,
        silence_sec=policy.silence_sec,
    )
    if not policy.reclaim_enabled and str(getattr(trial, "state", "")) == "running":
        return replace(
            decision,
            decision="keep",
            reason="stale_running_reclaim_disabled",
            reclaimable=False,
            hard_deadline_sec=None,
        )
    return decision
