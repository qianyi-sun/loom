"""Worker startup orphan cleanup (spec §3.2).

Scans the local trajectory cache for JSONL files left from previous worker
incarnations. For each, asks the Control Plane for the trial's state +
owner. Deletes if terminal OR owned by a different worker OR unknown.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from uuid import UUID

logger = logging.getLogger(__name__)

_TERMINAL_STATES = frozenset({"succeeded", "failed", "cancelled"})


def _trial_id_from_filename(path: Path) -> UUID | None:
    try:
        return UUID(path.stem)
    except ValueError:
        return None


def cleanup_orphan_trajectories(
    *,
    cache_dir: Path,
    owned_worker_id: UUID,
    state_and_owner_lookup: Callable[[UUID], tuple[str, UUID | None]],
) -> list[UUID]:
    """Delete trajectory files whose trial is terminal or not owned by us.

    Returns the list of deleted trial IDs.

    `state_and_owner_lookup(trial_id)` returns `(state, worker_id_or_none)`
    or raises `LookupError` for unknown trials (which are deleted too).
    """
    if not cache_dir.exists():
        return []

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
        not_ours = owner != owned_worker_id
        if terminal or not_ours:
            entry.unlink(missing_ok=True)
            deleted.append(trial_id)
            logger.info(
                "orphan_trajectory_deleted trial=%s state=%s owner=%s",
                trial_id, state, owner,
            )
    return deleted
