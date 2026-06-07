"""stream_stdout_jsonl — parse a JSONL-on-stdout stream into events.

Used by adapters whose agent CLI emits one JSON object per line on
stdout (claude-code with `--output-format stream-json`, gemini-cli with
`--output json`, mini-swe-agent, opencode, openhands-sdk).
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from typing import Any

from loom_launcher.adapter import ExecHandle, TrajectoryEventLike

logger = logging.getLogger(__name__)


class _DictEvent:
    """Lightweight wrapper that exposes `.model_dump()` over a plain dict.
    Used when the adapter doesn't import the full TrajectoryEvent union —
    keeps `loom-launcher` decoupled from `loom.models.trajectory`."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def model_dump(self) -> dict[str, Any]:
        return dict(self._data)


async def stream_stdout_jsonl(
    handle: ExecHandle,
    *,
    event_factory: type | None = None,
    skip_malformed: bool = True,
) -> AsyncIterator[TrajectoryEventLike]:
    """Yield one event per JSONL line on `handle.stdout`.

    `event_factory`: if provided, called with the parsed dict to build
    the event (e.g. `LLMCallEvent.model_validate`). Defaults to a thin
    `_DictEvent` wrapper that re-exposes the raw dict via `.model_dump()`.

    `skip_malformed`: if True (default), JSON parse errors are logged
    at WARNING and the offending line is dropped. If False, the
    `json.JSONDecodeError` propagates and aborts the iteration.
    """
    buf = b""
    async for chunk in handle.stdout:
        buf += chunk
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                if skip_malformed:
                    logger.warning(
                        "stream_stdout_jsonl: skipping malformed line "
                        "(%d bytes): %s", len(line), exc,
                    )
                    continue
                raise
            if event_factory is None:
                yield _DictEvent(obj)
            else:
                yield event_factory(**obj) if not hasattr(event_factory, "model_validate") else event_factory.model_validate(obj)
    # Flush the tail (process exited without a trailing newline).
    tail = buf.strip()
    if tail:
        try:
            obj = json.loads(tail)
            yield _DictEvent(obj) if event_factory is None else (
                event_factory(**obj)
                if not hasattr(event_factory, "model_validate")
                else event_factory.model_validate(obj)
            )
        except json.JSONDecodeError as exc:
            if not skip_malformed:
                raise
            logger.warning(
                "stream_stdout_jsonl: tail %d bytes not valid JSON: %s",
                len(tail), exc,
            )
