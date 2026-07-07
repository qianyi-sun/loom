"""Worker startup orphan sandbox-container cleanup (#605).

Sibling to `orphan_cleanup.py` (which handles trajectory JSONLs). Docker
containers spawned via the Docker socket outlive their spawner: if a
worker crashes, its host reboots, or the kind cluster drops, the sandbox
containers keep running until someone reaps them. Observed 2026-07-07:
GB10-2 and GB10-15 each had 10 sleep-infinity trial-cache containers
still up 3 days after the worker died.

Predicate mirrors `cleanup_orphan_trajectories`:

- Terminal state (succeeded/failed/cancelled) → remove
- Unknown to CP → remove
- Non-terminal, container older than the fallback window → remove
- Non-terminal, container fresh → preserve (a live worker somewhere may
  own it; wait for the terminal transition or the fallback window)

We do not label containers with a `loom.worker_id` at spawn (see
`task_sidecars.py:265`), so we can't distinguish "mine" from "someone
else's" at the container level. That's fine at worker startup — a fresh
worker owns no pre-existing trials, so every labeled container found is
by definition orphan from some prior incarnation. The fallback window
protects against deleting a container a live worker on the same host is
still using for a long-running trial.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import datetime
from typing import Any
from uuid import UUID

logger = logging.getLogger(__name__)

_TERMINAL_STATES = frozenset({"succeeded", "failed", "cancelled"})
_TRIAL_ID_LABEL = "loom.trial_id"

# Mirror of `orphan_cleanup._NON_TERMINAL_FALLBACK_SEC`. See that module
# for the reasoning — the fallback exists to bound resource growth when
# reclaim is broken, not as the primary cleanup mechanism.
_NON_TERMINAL_FALLBACK_SEC: float = 24 * 60 * 60


def _trial_id_from_labels(labels: dict[str, str]) -> UUID | None:
    raw = labels.get(_TRIAL_ID_LABEL)
    if raw is None:
        return None
    try:
        return UUID(raw)
    except ValueError:
        return None


def _container_started_at_epoch(container: Any) -> float | None:
    """Best-effort read of `.attrs.State.StartedAt` → epoch seconds.

    Docker returns RFC3339 with nanoseconds; datetime.fromisoformat
    accepts that on 3.11+. Malformed or missing → None (caller treats
    as "unknown age" and skips the age-based branch)."""
    state = container.attrs.get("State") or {}
    started_at = state.get("StartedAt")
    if not started_at or started_at.startswith("0001-01-01"):
        return None
    try:
        return datetime.fromisoformat(started_at.replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def cleanup_orphan_sandbox_containers(
    *,
    docker_client: Any,
    state_lookup: Callable[[UUID], str],
    now_sec: float | None = None,
    non_terminal_fallback_sec: float = _NON_TERMINAL_FALLBACK_SEC,
) -> list[UUID]:
    """Remove Docker containers whose trial is terminal, unknown, or
    non-terminal but older than the fallback window.

    Returns the list of trial IDs whose containers were removed.

    `state_lookup(trial_id)` returns the trial's current state string
    or raises `LookupError` for unknown trials (which are also removed).
    Any other exception from the lookup is treated as CP-unreachable —
    we bail and leave containers alone (fail-safe).
    """
    if now_sec is None:
        now_sec = time.time()

    try:
        containers = docker_client.containers.list(
            all=True, filters={"label": _TRIAL_ID_LABEL},
        )
    except Exception:
        logger.exception("orphan_sandbox_list_failed")
        return []

    removed: list[UUID] = []
    for container in containers:
        trial_id = _trial_id_from_labels(container.labels)
        if trial_id is None:
            logger.debug(
                "orphan_sandbox_skipped container=%s reason=missing_or_malformed_label",
                container.id,
            )
            continue

        try:
            state = state_lookup(trial_id)
        except LookupError:
            if _remove(container, trial_id, reason="unknown"):
                removed.append(trial_id)
            continue
        except Exception:
            # CP unreachable / transient error — do NOT delete. Log and
            # move on; next startup will retry.
            logger.warning(
                "orphan_sandbox_lookup_failed trial=%s container=%s — preserving",
                trial_id, container.id, exc_info=True,
            )
            continue

        if state in _TERMINAL_STATES:
            if _remove(container, trial_id, reason=f"terminal:{state}"):
                removed.append(trial_id)
            continue

        started = _container_started_at_epoch(container)
        age_sec = now_sec - started if started is not None else None
        if age_sec is not None and age_sec > non_terminal_fallback_sec:
            if _remove(
                container,
                trial_id,
                reason=(
                    f"stale:state={state} age_sec={age_sec:.0f} "
                    f"fallback_sec={non_terminal_fallback_sec:.0f}"
                ),
                warn=True,
            ):
                removed.append(trial_id)
            continue

        logger.info(
            "orphan_sandbox_preserved trial=%s container=%s state=%s age_sec=%s",
            trial_id, container.id, state,
            f"{age_sec:.0f}" if age_sec is not None else "unknown",
        )
    return removed


def _remove(
    container: Any,
    trial_id: UUID,
    *,
    reason: str,
    warn: bool = False,
) -> bool:
    try:
        container.remove(force=True)
    except Exception:
        logger.warning(
            "orphan_sandbox_remove_failed trial=%s container=%s reason=%s",
            trial_id, container.id, reason, exc_info=True,
        )
        return False
    log = logger.warning if warn else logger.info
    log(
        "orphan_sandbox_removed trial=%s container=%s reason=%s",
        trial_id, container.id, reason,
    )
    return True
