"""Shared tenant-scoped agent/task admission for service submissions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from uuid import UUID

from fastapi import HTTPException

from loom.db.schema import Task
from loom.db.task_set_visibility import visible_tasks
from loom.models.batch import Combination
from loom_service.agent_catalog import AgentEntry, get_agent
from loom_service.task_compat import agent_task_compatibility


class AgentTaskIncompatibilityError(HTTPException):
    """A capability mismatch that batch creation records as a rejection metric."""

    def __init__(self, detail: str) -> None:
        super().__init__(status_code=400, detail=detail)


def _submission_agent_names(
    *,
    combinations: Sequence[Combination | Mapping[str, Any]],
    trial_config: Mapping[str, Any],
) -> list[str]:
    """Extract requested names without normalizing persisted configuration."""
    if combinations:
        names: list[str] = []
        for index, combination in enumerate(combinations):
            raw_name = (
                combination.agent_name
                if isinstance(combination, Combination)
                else combination.get("agent_name")
            )
            if not isinstance(raw_name, str) or not raw_name:
                raise HTTPException(
                    status_code=400,
                    detail=f"combinations[{index}].agent_name must be a non-empty string",
                )
            names.append(raw_name)
        return names

    raw_name = trial_config.get("agent_name")
    return [raw_name] if isinstance(raw_name, str) and raw_name else []


def _resolved_submission_agents(
    names: Sequence[str],
) -> list[tuple[str, AgentEntry]]:
    """Resolve aliases to catalog entries, retaining request names for diagnostics."""
    resolved: list[tuple[str, AgentEntry]] = []
    canonical_names: set[str] = set()
    for name in names:
        entry = get_agent(name)
        if entry is None:
            raise HTTPException(
                status_code=400,
                detail=(f"unknown agent_name {name!r}. GET /api/v1/agents for the catalog."),
            )
        if entry.name not in canonical_names:
            canonical_names.add(entry.name)
            resolved.append((name, entry))
    return resolved


async def validate_submission_agent_task_compatibility(
    session: Any,
    *,
    team_id: UUID | None,
    task_ids: Sequence[str],
    combinations: Sequence[Combination | Mapping[str, Any]] = (),
    trial_config: Mapping[str, Any],
) -> None:
    """Reject missing/invisible tasks and incompatible agent/task pairs.

    Every lookup is constrained by ``visible_tasks`` so an inaccessible
    TaskSet task is indistinguishable from a missing task. The original
    requested agent spelling is retained in errors; aliases are resolved only
    for their immutable catalog capabilities.
    """
    requested_task_ids = list(dict.fromkeys(task_ids))
    requested_agents = _submission_agent_names(
        combinations=combinations,
        trial_config=trial_config,
    )
    agents = _resolved_submission_agents(requested_agents)
    if not requested_task_ids:
        return

    rows = (
        await session.execute(
            visible_tasks(team_id=team_id)
            .with_only_columns(Task.id, Task.config, Task.tags)
            .where(Task.id.in_(requested_task_ids)),
        )
    ).all()
    tasks = {
        str(task_id): (config if isinstance(config, Mapping) else {}, tags)
        for task_id, config, tags in rows
    }
    if set(tasks) != set(requested_task_ids):
        raise HTTPException(status_code=404, detail="task not found")
    if not agents:
        return

    offenders: list[tuple[str, list[tuple[str, frozenset[str], frozenset[str]]]]] = []
    for requested_name, entry in agents:
        failures: list[tuple[str, frozenset[str], frozenset[str]]] = []
        for task_id in requested_task_ids:
            config, raw_tags = tasks[task_id]
            compatibility = agent_task_compatibility(
                config,
                agent_requires=entry.requires_capabilities,
                agent_provides=entry.provides_capabilities,
                tags=dict(raw_tags) if isinstance(raw_tags, Mapping) else None,
            )
            if not compatibility.compatible:
                failures.append(
                    (
                        task_id,
                        compatibility.missing_from_task,
                        compatibility.missing_from_agent,
                    ),
                )
        if failures:
            offenders.append((requested_name, failures))

    if not offenders:
        return

    pairs: list[str] = []
    for name, failures in offenders:
        task_id, missing_from_task, missing_from_agent = failures[0]
        reasons: list[str] = []
        if missing_from_task:
            reasons.append(f"task missing {sorted(missing_from_task)}")
        if missing_from_agent:
            reasons.append(f"agent missing {sorted(missing_from_agent)}")
        pairs.append(f"{name}: {len(failures)} task(s) (e.g. {task_id}; {', '.join(reasons)})")
    raise AgentTaskIncompatibilityError(
        f"agent×task capability mismatch — {'; '.join(pairs)}. The listed agents "
        "cannot run these tasks at the platform level (e.g. oracle requires a "
        "benchmark adapter that ships `solution/solve.sh`). Choose a compatible "
        "agent or task.",
    )
