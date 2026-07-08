"""Reconstruct downloadable trajectory streams from canonical DB state."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal, cast
from uuid import UUID

from sqlalchemy import select

from loom.db.schema import LlmCall, Trial, TrialEvent
from loom.models.trajectory import TrialEndEvent
from loom.trajectory.llm_call_events import llm_call_row_to_event

_TERMINAL_TRIAL_STATES = frozenset({"succeeded", "failed", "cancelled"})


async def read_all_events_from_postgres(
    session: Any, *, trial_id: UUID,
) -> list[dict[str, Any]]:
    rows = (
        await session.execute(
            select(TrialEvent.payload)
            .where(TrialEvent.trial_id == trial_id)
            .order_by(TrialEvent.seq.asc(), TrialEvent.created_at.asc()),
        )
    ).all()
    return [row[0] for row in rows]


async def read_llm_calls_from_postgres(
    session: Any, *, trial_id: UUID,
) -> Sequence[LlmCall]:
    rows = (
        (
            await session.execute(
                select(LlmCall)
                .where(LlmCall.trial_id == trial_id)
                .order_by(LlmCall.captured_at.asc(), LlmCall.id.asc()),
            )
        )
        .scalars()
        .all()
    )
    return cast(Sequence[LlmCall], rows)


def reconstruct_postgres_trajectory_events(
    events: Sequence[dict[str, Any]],
    *,
    trial: Trial,
    llm_calls: Sequence[LlmCall],
) -> list[dict[str, Any]]:
    """Build the analysis-ready event stream used when the object is absent.

    `trial_events` is the durable event table, but older writers could emit
    duplicate local seq values before reaching the CP mirror. The CP table then
    kept only the first row for a seq. For object-missing trials, recover the
    canonical facts still present in the relational tables: gateway LLM usage
    rows and trial terminal state/result.
    """

    copied = [dict(event) for event in events]
    non_terminal = [event for event in copied if event.get("kind") != "trial_end"]
    terminal_events = [event for event in copied if event.get("kind") == "trial_end"]
    has_llm_call = any(event.get("kind") == "llm_call" for event in copied)

    synthesized: list[dict[str, Any]] = []
    if not has_llm_call:
        for call in llm_calls:
            synthesized.append(
                llm_call_row_to_event(
                    _llm_call_mapping(call),
                    trial_id=trial.id,
                    seq=0,
                ).model_dump(mode="json"),
            )

    terminal_event: dict[str, Any] | None = None
    if terminal_events:
        terminal_event = dict(terminal_events[-1])
    else:
        terminal_event = _trial_end_from_trial(trial)

    ordered = [*non_terminal, *synthesized]
    if terminal_event is not None:
        ordered.append(terminal_event)
    return _renumber(ordered)


def _llm_call_mapping(call: LlmCall) -> dict[str, Any]:
    return {
        "id": call.id,
        "trial_id": call.trial_id,
        "step_id": call.step_id,
        "model": call.model,
        "dialect": call.dialect,
        "input_tokens": call.input_tokens,
        "output_tokens": call.output_tokens,
        "provider_extras": call.provider_extras,
        "request_params": call.request_params,
        "cost_usd": call.cost_usd,
        "rate_card_hash": call.rate_card_hash,
        "captured_at": call.captured_at,
        "attempt": call.attempt,
    }


def _trial_end_from_trial(trial: Trial) -> dict[str, Any] | None:
    if trial.state not in _TERMINAL_TRIAL_STATES:
        return None
    final_state = cast(Literal["succeeded", "failed", "cancelled"], trial.state)
    event = TrialEndEvent(
        emitted_at=trial.finished_at or datetime.now(UTC),
        trial_id=trial.id,
        step_id="__trial__",
        seq=0,
        final_state=final_state,
        reward=_reward_from_result(trial.result),
        failure_reason=trial.failure_reason,
    )
    return event.model_dump(mode="json")


def _reward_from_result(result: dict[str, Any] | None) -> dict[str, float] | None:
    if not isinstance(result, dict):
        return None
    raw = result.get("reward")
    if isinstance(raw, dict):
        out = {
            str(key): float(value)
            for key, value in raw.items()
            if isinstance(value, int | float | Decimal) and not isinstance(value, bool)
        }
        return out or None
    if isinstance(raw, int | float | Decimal) and not isinstance(raw, bool):
        return {"reward": float(raw)}

    aggregate = result.get("aggregate_reward")
    if isinstance(aggregate, int | float | Decimal) and not isinstance(aggregate, bool):
        return {"aggregate_reward": float(aggregate)}
    return None


def _renumber(events: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for seq, event in enumerate(events):
        copied = dict(event)
        copied["seq"] = seq
        out.append(copied)
    return out
