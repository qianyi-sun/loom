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

# Heuristic key: tasks whose verifier is `pytest` are produced by adapters
# that always co-emit `solution/solve.sh` next to a `solution/_reference.py`
# (the post-#388/#414 stub layout). Terminal-Bench-2 is the known V1
# exception: it uses the script verifier, but its adapter wraps upstream
# `solution.sh` / `solution.yaml` as `solution/solve.sh`.
_PYTEST_VERIFIER = "pytest"
_TASK_ID_SOLUTION_SOLVE_SH_PREFIXES = ("terminal-bench-2/",)


def task_provides_capability(
    task_config: Mapping[str, Any], capability: str,
) -> bool:
    """Best-effort derivation: does the materialized bundle expose
    the named agent capability?

    `solution_solve_sh` is granted when the task's verifier is `pytest`,
    or when a known adapter emits `solution/solve.sh` under a different
    verifier contract. A future adapter that breaks the convention should
    add an explicit capability marker instead of broadening every script
    verifier task.

    Unknown capabilities return False — fail closed so a typo in
    `AgentEntry.requires_capabilities` doesn't silently permit
    everything.
    """
    if capability == "solution_solve_sh":
        verifier = task_config.get("verifier") or {}
        if isinstance(verifier, Mapping):
            if verifier.get("name") == _PYTEST_VERIFIER:
                return True
        task = task_config.get("task") or {}
        task_id = task.get("id") if isinstance(task, Mapping) else None
        return isinstance(task_id, str) and task_id.startswith(
            _TASK_ID_SOLUTION_SOLVE_SH_PREFIXES,
        )
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
