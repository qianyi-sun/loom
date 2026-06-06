"""TrajectoryReader — read-side accessor for a finished or in-progress
trajectory file (spec §3.6 + §4.10)."""

from __future__ import annotations

from collections import deque
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import TypeAdapter

from loom.models.trajectory import EventKind, TrajectoryEvent

if TYPE_CHECKING:
    from loom.trajectory.excerpt import ExcerptStrategy

_event_adapter: TypeAdapter[TrajectoryEvent] = TypeAdapter(TrajectoryEvent)


class TrajectoryReader:
    """Iterate trajectory events from a local JSONL file."""

    def __init__(self, source: Path) -> None:
        self._source = source

    def iter_all(self) -> Iterator[TrajectoryEvent]:
        with self._source.open("rb") as fh:
            for raw in fh:
                line = raw.strip()
                if not line:
                    continue
                yield _event_adapter.validate_json(line)

    def iter_kind(self, kind: EventKind) -> Iterator[TrajectoryEvent]:
        for event in self.iter_all():
            if event.kind == kind:
                yield event

    def tail(self, n: int) -> list[TrajectoryEvent]:
        buf: deque[TrajectoryEvent] = deque(maxlen=n)
        for event in self.iter_all():
            buf.append(event)
        return list(buf)

    def excerpt(
        self,
        strategy: ExcerptStrategy,
        *,
        max_tokens: int,
    ) -> list[TrajectoryEvent]:
        """Apply `strategy` to all events, then prune oldest-first to fit max_tokens."""
        from loom.trajectory.excerpt import apply_strategy, trim_to_budget
        events = list(self.iter_all())
        selected = apply_strategy(events, strategy)
        return trim_to_budget(selected, max_tokens=max_tokens)
