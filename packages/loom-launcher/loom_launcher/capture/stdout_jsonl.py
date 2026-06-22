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

# Maximum sample size of a single malformed JSONL line included in
# the end-of-stream synthetic event. Long enough to spot the issue
# (truncated JSON, raw shell error, etc.) without blowing up the
# trajectory if an agent dumps megabytes of garbage.
_MALFORMED_SAMPLE_BYTES = 512
# How many distinct skipped samples to keep. Last-N strategy — we
# want the failure tail (most likely to contain the root cause),
# capped so a 10k-line garbage stream still fits in ~5KB.
_MALFORMED_SAMPLE_COUNT = 10


class _DictEvent:
    """Lightweight wrapper that exposes `.model_dump()` over a plain dict.
    Used when the adapter doesn't import the full TrajectoryEvent union —
    keeps `loom-launcher` decoupled from `loom.models.trajectory`."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def model_dump(self) -> dict[str, Any]:
        return dict(self._data)


def _make_event(
    obj: dict[str, Any],
    event_factory: type | None,
) -> TrajectoryEventLike:
    if event_factory is None:
        return _DictEvent(obj)
    if hasattr(event_factory, "model_validate"):
        return event_factory.model_validate(obj)  # type: ignore[no-any-return]
    return event_factory(**obj)  # type: ignore[no-any-return]


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

    When `skip_malformed=True` and at least one line was skipped, the
    iterator yields a terminal synthetic event:

        {
            "kind": "stream_capture_warning",
            "skipped_lines": N,
            "last_skip_reason": "<JSON error from the last bad line>",
            "last_skip_sample": "<first bytes of the last bad line>",
            "skip_samples": [
                {"reason": "...", "sample": "..."},   # up to 10 most recent
                ...
            ],
        }

    SubprocessAgent uses `last_skip_sample` for the failure_message
    summary and persists the full `skip_samples` list to the trajectory
    so post-hoc debugging doesn't depend on retained worker logs (#321).
    """
    buf = b""
    skipped = 0
    last_reason: str = ""
    last_sample: str = ""
    # Ring buffer of recent skipped samples — preserved in the
    # trajectory so operators can debug after worker logs are gone.
    samples: list[dict[str, str]] = []

    def _note_skip(line: bytes, exc: json.JSONDecodeError) -> None:
        nonlocal skipped, last_reason, last_sample
        skipped += 1
        last_reason = str(exc)
        last_sample = line[:_MALFORMED_SAMPLE_BYTES].decode(
            "utf-8", errors="replace",
        )
        samples.append({"reason": last_reason, "sample": last_sample})
        if len(samples) > _MALFORMED_SAMPLE_COUNT:
            del samples[0]

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
                    _note_skip(line, exc)
                    continue
                raise
            yield _make_event(obj, event_factory)
    # Flush the tail (process exited without a trailing newline).
    tail = buf.strip()
    if tail:
        try:
            obj = json.loads(tail)
            yield _make_event(obj, event_factory)
        except json.JSONDecodeError as exc:
            if not skip_malformed:
                raise
            logger.warning(
                "stream_stdout_jsonl: tail %d bytes not valid JSON: %s",
                len(tail), exc,
            )
            _note_skip(tail, exc)

    if skipped > 0:
        yield _DictEvent({
            "kind": "stream_capture_warning",
            "skipped_lines": skipped,
            "last_skip_reason": last_reason,
            "last_skip_sample": last_sample,
            "skip_samples": list(samples),
        })
