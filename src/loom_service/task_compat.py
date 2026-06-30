"""Agent ↔ task capability matching (#320).

Filters incompatible (agent, task) pairs at `POST /api/v1/batches`
preflight so the matrix runner / SPA / CLI never launch a trial that
will deterministically fail at the worker with
`AgentError: OracleAgent requires .../solution/solve.sh; not found`.

Capability vocabulary is intentionally small in V1 — only
`solution_solve_sh` (the oracle case from #320 evidence). Adding a
capability is two steps: declare it in `AgentEntry.requires_capabilities`
on the consuming agent, and teach `task_provides_capability` how to
recognize it on a `TaskConfig`-shaped dict (or `tags` for adapters
that publish a per-instance marker — e.g. SkillLearnBench's
`oracle_eligible` tag covers a benchmark whose 100 tasks include 27
without an upstream `solve.sh`).
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
_ORACLE_ELIGIBLE_TAG = "oracle_eligible"


def task_provides_capability(
    task_config: Mapping[str, Any],
    capability: str,
    *,
    tags: Mapping[str, str] | None = None,
) -> bool:
    """Best-effort derivation: does the materialized bundle expose
    the named agent capability?

    `solution_solve_sh` is granted when (a) the task's verifier is `pytest`,
    (b) the per-task `oracle_eligible` tag is `"true"`, or (c) the task id
    matches a known adapter prefix whose adapter unconditionally emits
    `solution/solve.sh`. An explicit `oracle_eligible="false"` tag wins
    over the prefix fallback so a heterogeneous oracle slate (e.g.
    SkillLearnBench: 73 of 100 upstream tasks ship `solve.sh`, or
    Terminal-Bench-2 tasks that lack upstream `solution.sh`/
    `solution.yaml`) is honored once the adapter starts emitting tags.
    A future adapter that breaks the convention should add an explicit
    capability marker instead of broadening every script verifier task.

    Unknown capabilities return False — fail closed so a typo in
    `AgentEntry.requires_capabilities` doesn't silently permit
    everything.
    """
    if capability == "solution_solve_sh":
        verifier = task_config.get("verifier") or {}
        if isinstance(verifier, Mapping):
            if verifier.get("name") == _PYTEST_VERIFIER:
                return True
        if tags is not None:
            tag_value = tags.get(_ORACLE_ELIGIBLE_TAG)
            if tag_value == "true":
                return True
            if tag_value == "false":
                return False
        task = task_config.get("task") or {}
        task_id = task.get("id") if isinstance(task, Mapping) else None
        if isinstance(task_id, str) and task_id.startswith(
            _TASK_ID_SOLUTION_SOLVE_SH_PREFIXES,
        ):
            return True
        return False
    return False


def task_supports_agent(
    task_config: Mapping[str, Any],
    required: Iterable[str],
    *,
    tags: Mapping[str, str] | None = None,
) -> bool:
    """True iff the task provides EVERY capability the agent requires.
    Agents with empty `requires_capabilities` always pass."""
    return all(
        task_provides_capability(task_config, cap, tags=tags)
        for cap in required
    )


def filter_tasks_by_agent_capability(
    *,
    task_configs: Mapping[str, Mapping[str, Any]],
    required: Iterable[str],
    task_tags: Mapping[str, Mapping[str, str]] | None = None,
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
        tags = task_tags.get(task_id) if task_tags else None
        if task_supports_agent(config, required_set, tags=tags):
            compatible.append(task_id)
        else:
            incompatible.append(task_id)
    return compatible, incompatible
