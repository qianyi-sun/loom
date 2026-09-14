"""Read projection for historical native starts missing from the Trial column."""

from datetime import datetime
from typing import Any


def trial_started_at(started_at: datetime | None, result: Any) -> datetime | None:
    """Reuse persisted, server-validated attempt timing without rewriting history."""
    if started_at is not None:
        return started_at
    runtime = result.get("runtime_result") if isinstance(result, dict) else None
    if not isinstance(runtime, dict) or (
        runtime.get("schema_version") != "loom.execution-runtime-result.v1"
        or runtime.get("execution_role") != "attempt"
    ):
        return None
    try:
        actual_start = datetime.fromisoformat(runtime["started_at"])
        actual_finish = datetime.fromisoformat(runtime["finished_at"])
    except (KeyError, TypeError, ValueError):
        return None
    if (
        actual_start.utcoffset() is None
        or actual_finish.utcoffset() is None
        or actual_finish < actual_start
    ):
        return None
    return actual_start
