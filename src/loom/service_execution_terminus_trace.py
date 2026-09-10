"""Typed Harbor trace accounting shared by execution and canonical projection."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import TypeAdapter

from loom.models.trajectory import LLMCallEvent, TrajectoryEvent
from loom.models.trial import TrialConfig

_EVENT: TypeAdapter[TrajectoryEvent] = TypeAdapter(TrajectoryEvent)
_COUNTERS = (
    "input_tokens", "cached_input_tokens", "cache_write_tokens", "output_tokens", "thinking_tokens",
)


def parse_terminus_events(
    body: bytes | None, *, trial: TrialConfig, trial_id: UUID | None = None,
) -> list[TrajectoryEvent]:
    events: list[TrajectoryEvent] = []
    call_ids: set[str] = set()
    for line in (body or b"").splitlines():
        event = _EVENT.validate_json(line)
        if event.seq != len(events) or event.step_id != "agent":
            raise ValueError("Terminus trace order or step identity is invalid")
        if trial_id is None:
            trial_id = event.trial_id
        if event.trial_id != trial_id:
            raise ValueError("Terminus trace has another Trial identity")
        if not (event.kind.startswith("terminus2_") or isinstance(event, LLMCallEvent)):
            raise ValueError("Terminus source cannot author lifecycle events")
        if isinstance(event, LLMCallEvent):
            if event.model != trial.agent_model:
                raise ValueError("Terminus trace has another model identity")
            if not event.gateway_request_id or event.gateway_request_id in call_ids:
                raise ValueError("Terminus trace has missing or duplicate Gateway calls")
            call_ids.add(event.gateway_request_id)
        events.append(event)
    return events


def terminus_usage(events: list[TrajectoryEvent], trial: TrialConfig) -> dict[str, Any]:
    calls = [event for event in events if isinstance(event, LLMCallEvent)]
    return {
        "schema_version": "loom.service-execution-terminus-usage.v1",
        "model": trial.agent_model.to_gateway_model_string() if trial.agent_model else None,
        "call_count": len(calls),
        "gateway_request_ids": [call.gateway_request_id for call in calls],
        "totals": {
            **{name: sum(getattr(call, name) for call in calls) for name in _COUNTERS},
            "cost_usd": sum(call.cost_usd_snapshot for call in calls),
            "duration_sec": sum(call.duration_sec for call in calls),
        },
    }
