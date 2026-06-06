"""Excerpt strategies for LLMJudgeVerifier / dashboards / replay (spec §3.6 + §4.10).

A strategy is a small frozen dataclass selecting which events to include.
`TrajectoryReader.excerpt(strategy, max_tokens=...)` applies the strategy
then prunes oldest-first to fit a token budget.

Token estimation: 4 bytes ≈ 1 token, computed against JSON-serialized event
payload. Coarse but adequate for budget gating.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from loom.models.trajectory import EventKind, TrajectoryEvent


@dataclass(frozen=True)
class TailExcerpt:
    kind: Literal["tail"] = "tail"
    n: int = 50


@dataclass(frozen=True)
class AllExcerpt:
    kind: Literal["all"] = "all"


@dataclass(frozen=True)
class ToolUseOnlyExcerpt:
    kind: Literal["tool_use_only"] = "tool_use_only"


@dataclass(frozen=True)
class StepSummaryExcerpt:
    kind: Literal["step_summary"] = "step_summary"


ExcerptStrategy = TailExcerpt | AllExcerpt | ToolUseOnlyExcerpt | StepSummaryExcerpt


_BYTES_PER_TOKEN_ESTIMATE = 4


def _estimate_tokens(event: TrajectoryEvent) -> int:
    payload = event.model_dump_json().encode("utf-8")
    return max(1, len(payload) // _BYTES_PER_TOKEN_ESTIMATE)


def trim_to_budget(
    events: list[TrajectoryEvent], *, max_tokens: int,
) -> list[TrajectoryEvent]:
    """Drop oldest events until total estimated tokens ≤ max_tokens."""
    total = sum(_estimate_tokens(e) for e in events)
    out = list(events)
    while out and total > max_tokens:
        dropped = out.pop(0)
        total -= _estimate_tokens(dropped)
    return out


def apply_strategy(
    events: list[TrajectoryEvent], strategy: ExcerptStrategy,
) -> list[TrajectoryEvent]:
    if isinstance(strategy, TailExcerpt):
        return events[-strategy.n:]
    if isinstance(strategy, AllExcerpt):
        return list(events)
    if isinstance(strategy, ToolUseOnlyExcerpt):
        return [
            e for e in events
            if e.kind in (EventKind.TOOL_USE, EventKind.LLM_CALL)
        ]
    if isinstance(strategy, StepSummaryExcerpt):
        return [
            e for e in events
            if e.kind in (EventKind.STEP_START, EventKind.STEP_END)
        ]
    raise ValueError(f"unknown excerpt strategy: {strategy!r}")
