"""Agent ↔ task capability matching (#320).

Filters incompatible (agent, task) pairs at `POST /api/v1/batches`
preflight so the matrix runner / SPA / CLI never launch a trial that
will deterministically fail at the worker with
`AgentError: OracleAgent requires .../solution/solve.sh; not found`.

Capability vocabulary is intentionally small in V1 — only
`solution_solve_sh` (the oracle case from #320 evidence). Adding a
capability is two steps: declare it in `AgentEntry.requires_capabilities`
on the consuming agent, and teach `task_provides_capability` how to
recognize it on a `TaskConfig`-shaped dict.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

# Heuristic key: tasks whose verifier is `pytest` are produced by
# adapters that always co-emit `solution/solve.sh` next to a
# `solution/_reference.py` (the post-#388/#414 stub layout).
# Adapters with a different verifier (`script`, `default`, ...) ship
# no solve.sh, so oracle has nothing to upload.
_PYTEST_VERIFIER = "pytest"


def task_provides_capability(
    task_config: Mapping[str, Any], capability: str,
) -> bool:
    """Best-effort derivation: does the materialized bundle expose
    the named agent capability?

    `solution_solve_sh` is granted iff the task's verifier is `pytest`.
    Every current adapter that emits `solution/solve.sh` pairs it with
    a pytest verifier (mbpp, humaneval, livecodebench); the inverse
    also holds — non-pytest verifiers (script, default, ...) don't
    co-emit solve.sh. A future adapter that breaks the convention can
    add an explicit task tag override (out of V1 scope).

    Unknown capabilities return False — fail closed so a typo in
    `AgentEntry.requires_capabilities` doesn't silently permit
    everything.
    """
    if capability == "solution_solve_sh":
        verifier = task_config.get("verifier") or {}
        if isinstance(verifier, Mapping):
            return verifier.get("name") == _PYTEST_VERIFIER
        return False
    return False


def task_supports_agent(
    task_config: Mapping[str, Any], required: Iterable[str],
) -> bool:
    """True iff the task provides EVERY capability the agent requires.
    Agents with empty `requires_capabilities` always pass."""
    return all(
        task_provides_capability(task_config, cap) for cap in required
    )


def filter_tasks_by_agent_capability(
    *,
    task_configs: Mapping[str, Mapping[str, Any]],
    required: Iterable[str],
) -> tuple[list[str], list[str]]:
    """Split `task_configs` (id → config dict) into `(compatible_ids,
    incompatible_ids)` in input-order. Used by the batch-create
    preflight to surface counts + concrete examples without leaking
    raw configs back to the caller.
    """
    required_set = frozenset(required)
    if not required_set:
        return list(task_configs.keys()), []
    compatible: list[str] = []
    incompatible: list[str] = []
    for task_id, config in task_configs.items():
        if task_supports_agent(config, required_set):
            compatible.append(task_id)
        else:
            incompatible.append(task_id)
    return compatible, incompatible
