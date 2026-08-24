"""Worker startup orphan cleanup (spec §3.2).

Scans the local trajectory cache for JSONL files left from previous worker
incarnations. For each, asks the Control Plane for the trial's state.
Deletes if terminal, or unknown, or non-terminal but very stale
(fallback for the case where the reclaim sweep never fired).

#416 Slice 1: the predicate used to also delete when `owner != ours`,
which is the post-reclaim shape (`owner=None`) for a trial the local
file is the only forensic record of. Reclaim re-queues the trial; the
new attempt writes its own JSONL. Deleting the old one mid-rollout
killed the only data available to triage the previous attempt and
manifested as `trajectory_flush_failed` rows with no message in the
staging evidence on this issue.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from pathlib import Path
from uuid import UUID

logger = logging.getLogger(__name__)

_TERMINAL_STATES = frozenset({"succeeded", "failed", "cancelled"})

# Fallback delete window for non-terminal JSONLs. Reclaim sweeps trials
# whose worker heartbeat is more than `worker_heartbeat_expiry_sec`
# (default 120s) stale, so under normal operation a still-running trial
# either transitions to terminal or gets re-queued within minutes. A
# JSONL that's still tagged non-terminal a full day later means the
# reclaim sweep is broken or the trial is wedged in CP — delete the
# file to bound disk growth and log loudly so operators can dig in.
_NON_TERMINAL_FALLBACK_SEC: float = 24 * 60 * 60


def _trial_id_from_filename(path: Path) -> UUID | None:
    name = path.name
    attempt_suffix = ".events.jsonl"
    if name.endswith(attempt_suffix):
        trial_and_attempt = name.removesuffix(attempt_suffix)
        trial_text, separator, raw_attempt = trial_and_attempt.rpartition(
            ".attempt-",
        )
        if not separator:
            return None
        try:
            attempt_count = int(raw_attempt)
        except ValueError:
            return None
        if attempt_count <= 0 or str(attempt_count) != raw_attempt:
            return None
        try:
            return UUID(trial_text)
        except ValueError:
            return None
    try:
        return UUID(name.removesuffix(".jsonl"))
    except ValueError:
        return None


def cleanup_orphan_trajectories(
    *,
    cache_dir: Path,
    owned_worker_id: UUID,
    state_and_owner_lookup: Callable[[UUID], tuple[str, UUID | None]],
    now_sec: float | None = None,
    non_terminal_fallback_sec: float = _NON_TERMINAL_FALLBACK_SEC,
) -> list[UUID]:
    """Delete trajectory files whose trial is terminal, unknown, or
    very stale and still non-terminal.

    Returns the list of deleted trial IDs.

    `state_and_owner_lookup(trial_id)` returns `(state, worker_id_or_none)`
    or raises `LookupError` for unknown trials (which are deleted too).
    `owned_worker_id` is kept for log attribution; the predicate no
    longer branches on owner identity — see module docstring (#416).
    """
    if not cache_dir.exists():
        return []
    if now_sec is None:
        now_sec = time.time()

    deleted: list[UUID] = []
    for entry in cache_dir.iterdir():
        if not entry.is_file() or not entry.name.endswith(".jsonl"):
            continue
        trial_id = _trial_id_from_filename(entry)
        if trial_id is None:
            continue
        try:
            state, owner = state_and_owner_lookup(trial_id)
        except LookupError:
            entry.unlink(missing_ok=True)
            deleted.append(trial_id)
            logger.info(
                "orphan_trajectory_deleted trial=%s reason=unknown", trial_id,
            )
            continue
        terminal = state in _TERMINAL_STATES
        if terminal:
            entry.unlink(missing_ok=True)
            deleted.append(trial_id)
            logger.info(
                "orphan_trajectory_deleted trial=%s state=%s owner=%s "
                "reason=terminal",
                trial_id, state, owner,
            )
            continue
        # Non-terminal: keep the JSONL so the next reclaim attempt or
        # an operator can read it. Fallback safety-valve: if the file
        # is older than the configured window, the reclaim sweep
        # either never fired or the trial is wedged — delete the file
        # and log loudly so the operator can dig in.
        age_sec = now_sec - entry.stat().st_mtime
        if age_sec > non_terminal_fallback_sec:
            entry.unlink(missing_ok=True)
            deleted.append(trial_id)
            logger.warning(
                "orphan_trajectory_deleted_stale trial=%s state=%s "
                "owner=%s age_sec=%.0f fallback_sec=%.0f — reclaim "
                "sweep may be broken",
                trial_id, state, owner, age_sec, non_terminal_fallback_sec,
            )
            continue
        logger.info(
            "orphan_trajectory_preserved trial=%s state=%s owner=%s "
            "age_sec=%.0f — non-terminal, keeping for reclaim/forensics",
            trial_id, state, owner, age_sec,
        )
    return deleted
