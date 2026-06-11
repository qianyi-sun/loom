"""Workflows CRUD + launch (Plan 22).

A Workflow is a GLOBAL saved-recipe — admin-managed, all-teams-launchable.
It pins every config field needed for reproducibility:

- benchmark (FK to benchmarks.id)
- agent name + agent version (pinned together)
- model provider + model name
- backend
- concurrency
- task_filter (which tasks within the benchmark)
- trial_config (the per-trial knobs sent to TrialConfig)

Launching a workflow deep-copies its `task_filter` + `trial_config`
into a Campaign with `workflow_id` set as the back-reference. The
Campaign carries the FROZEN config — edits to the workflow after
launch don't retroactively change the run.

Auth split:
- GET routes: any signed-in team user can list / read.
- POST / PATCH / DELETE / launch:
  - mutations require `admin:workflows`
  - launch requires `submit` (same as POST /campaigns; the launcher's
    team gets the new Campaign and is billed for it)
"""

from __future__ import annotations

import copy
from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from loom.auth import verify_bearer_token
from loom.db.schema import Benchmark, Campaign, Workflow
from loom_service.auth_guards import (
    require_human_or_admin,
    require_scope,
)
from loom_service.routes.campaigns import _resolve_task_filter

router = APIRouter()


_BACKENDS: frozenset[str] = frozenset(
    {"docker", "fake", "daytona", "modal"},
)


def _serialize(w: Workflow) -> dict[str, Any]:
    return {
        "id": str(w.id),
        "name": w.name,
        "description": w.description,
        "benchmark_id": w.benchmark_id,
        "agent_name": w.agent_name,
        "agent_version": w.agent_version,
        "model_provider": w.model_provider,
        "model_name": w.model_name,
        "backend": w.backend,
        "concurrency": w.concurrency,
        "n_per_task": w.n_per_task,
        "task_filter": dict(w.task_filter),
        "trial_config": dict(w.trial_config),
        "created_at": w.created_at.isoformat(),
        "updated_at": w.updated_at.isoformat(),
        "created_by_token_prefix": w.created_by_token_prefix,
    }


class _WorkflowPayload(BaseModel):
    """Shared body shape for POST and PATCH. PATCH ignores missing
    keys; POST requires the full slate."""
    name: str = Field(min_length=1, max_length=200)
    description: str | None = None
    benchmark_id: str
    agent_name: str = Field(min_length=1, max_length=80)
    agent_version: str = Field(min_length=1, max_length=80)
    model_provider: str = Field(min_length=1, max_length=80)
    model_name: str = Field(min_length=1, max_length=200)
    backend: str = "docker"
    concurrency: int = Field(default=1, ge=1, le=64)
    n_per_task: int = Field(default=1, ge=1, le=100)
    task_filter: dict[str, Any] = Field(default_factory=dict)
    trial_config: dict[str, Any] = Field(default_factory=dict)


class _LaunchPayload(BaseModel):
    """Optional `name` override; defaults to the workflow's name +
    timestamp so multiple launches don't collide on a unique name."""
    name: str | None = None


async def _validate_benchmark(
    request: Request, benchmark_id: str,
) -> None:
    async with request.app.state.session_factory() as s:
        exists = await s.execute(
            select(Benchmark.id).where(Benchmark.id == benchmark_id),
        )
        if exists.scalar_one_or_none() is None:
            raise HTTPException(
                status_code=400,
                detail=f"unknown benchmark_id: {benchmark_id!r}",
            )


def _validate_payload_field_choices(payload: _WorkflowPayload) -> None:
    if payload.backend not in _BACKENDS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"unknown backend: {payload.backend!r}. "
                f"valid: {sorted(_BACKENDS)}"
            ),
        )


def _token_prefix(ctx: Any) -> str:
    return ctx.token_hash.hex()[:8] if ctx.token_hash else "00000000"


@router.get("/workflows")
async def list_workflows(
    request: Request,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(gt=0, le=200)] = 50,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        require_human_or_admin(ctx)

        stmt = (
            select(Workflow)
            .where(Workflow.deleted_at.is_(None))
            .order_by(Workflow.name.asc(), Workflow.id.asc())
            .limit(limit + 1)
        )
        if cursor:
            stmt = stmt.where(Workflow.name > cursor)
        rows = (await s.execute(stmt)).scalars().all()
        next_cursor = None
        if len(rows) > limit:
            next_cursor = rows[limit - 1].name
            rows = rows[:limit]
        return {
            "items": [_serialize(w) for w in rows],
            "next_cursor": next_cursor,
        }


@router.post("/workflows", status_code=201)
async def create_workflow(
    request: Request,
    payload: _WorkflowPayload,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)
        require_scope(ctx, "admin:workflows")
    _validate_payload_field_choices(payload)
    await _validate_benchmark(request, payload.benchmark_id)

    async with request.app.state.session_factory() as s:
        # Re-fetch ctx in the same session as the insert so token_hash
        # is accessible from a fresh context.
        ctx = await verify_bearer_token(s, authorization)
        existing = await s.execute(
            select(Workflow).where(
                Workflow.name == payload.name,
                Workflow.deleted_at.is_(None),
            ),
        )
        if existing.scalar_one_or_none() is not None:
            raise HTTPException(
                status_code=409,
                detail=f"workflow {payload.name!r} already exists",
            )
        w = Workflow(
            name=payload.name,
            description=payload.description,
            benchmark_id=payload.benchmark_id,
            agent_name=payload.agent_name,
            agent_version=payload.agent_version,
            model_provider=payload.model_provider,
            model_name=payload.model_name,
            backend=payload.backend,
            concurrency=payload.concurrency,
            n_per_task=payload.n_per_task,
            task_filter=payload.task_filter,
            trial_config=payload.trial_config,
            created_by_token_prefix=_token_prefix(ctx),
        )
        s.add(w)
        try:
            await s.commit()
        except IntegrityError as exc:
            # Two admins racing on the same name → the partial unique
            # index catches the loser. Translate to a clean 409 so
            # clients don't see a vague 500.
            await s.rollback()
            raise HTTPException(
                status_code=409,
                detail=f"workflow {payload.name!r} already exists",
            ) from exc
        await s.refresh(w)
        return _serialize(w)


@router.get("/workflows/{workflow_id}")
async def get_workflow(
    request: Request,
    workflow_id: UUID,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        require_human_or_admin(ctx)
        w = await s.get(Workflow, workflow_id)
        if w is None or w.deleted_at is not None:
            raise HTTPException(status_code=404, detail="workflow not found")
        return _serialize(w)


@router.patch("/workflows/{workflow_id}")
async def update_workflow(
    request: Request,
    workflow_id: UUID,
    payload: _WorkflowPayload,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)
        require_scope(ctx, "admin:workflows")
    _validate_payload_field_choices(payload)
    await _validate_benchmark(request, payload.benchmark_id)

    async with request.app.state.session_factory() as s:
        w = await s.get(Workflow, workflow_id)
        if w is None or w.deleted_at is not None:
            raise HTTPException(status_code=404, detail="workflow not found")

        # Reject renames that would collide with another active workflow.
        if payload.name != w.name:
            clash = await s.execute(
                select(Workflow).where(
                    Workflow.name == payload.name,
                    Workflow.deleted_at.is_(None),
                    Workflow.id != workflow_id,
                ),
            )
            if clash.scalar_one_or_none() is not None:
                raise HTTPException(
                    status_code=409,
                    detail=f"workflow {payload.name!r} already exists",
                )

        w.name = payload.name
        w.description = payload.description
        w.benchmark_id = payload.benchmark_id
        w.agent_name = payload.agent_name
        w.agent_version = payload.agent_version
        w.model_provider = payload.model_provider
        w.model_name = payload.model_name
        w.backend = payload.backend
        w.concurrency = payload.concurrency
        w.n_per_task = payload.n_per_task
        w.task_filter = payload.task_filter
        w.trial_config = payload.trial_config
        w.updated_at = datetime.now(UTC)
        try:
            await s.commit()
        except IntegrityError as exc:
            await s.rollback()
            raise HTTPException(
                status_code=409,
                detail=f"workflow {payload.name!r} already exists",
            ) from exc
        await s.refresh(w)
        return _serialize(w)


@router.delete("/workflows/{workflow_id}", status_code=204)
async def delete_workflow(
    request: Request,
    workflow_id: UUID,
    authorization: Annotated[str | None, Header()] = None,
) -> None:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)
        require_scope(ctx, "admin:workflows")
        w = await s.get(Workflow, workflow_id)
        if w is None or w.deleted_at is not None:
            raise HTTPException(status_code=404, detail="workflow not found")
        w.deleted_at = datetime.now(UTC)
        await s.commit()


@router.post("/workflows/{workflow_id}/launch", status_code=201)
async def launch_workflow(
    request: Request,
    workflow_id: UUID,
    payload: _LaunchPayload,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    """Create a Campaign from the workflow's frozen config.

    The caller's team is debited for the Campaign; admin tokens
    without a team_id are rejected because every Campaign must be
    attributable. The launch deep-copies task_filter + trial_config
    from the workflow so subsequent edits don't retroactively change
    the historical run.
    """
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)
        require_scope(ctx, "submit")
        if ctx.team_id is None:
            raise HTTPException(
                status_code=400,
                detail="admin tokens must scope launches to a team — "
                       "use a team token to launch a workflow",
            )

        w = await s.get(Workflow, workflow_id)
        if w is None or w.deleted_at is not None:
            raise HTTPException(status_code=404, detail="workflow not found")

        # Deep-copy task_filter + trial_config so the Campaign carries
        # a snapshot wholly independent of the workflow row. Subsequent
        # admin edits to the workflow do NOT retroactively change the
        # historical run; nested mutation in either object is also
        # isolated. An empty `task_filter` falls back to the
        # benchmark-id default so a launch never auto-resolves the
        # entire task catalogue.
        frozen_filter: dict[str, Any] = copy.deepcopy(w.task_filter) or {
            "benchmark_id": w.benchmark_id,
        }
        frozen_config: dict[str, Any] = copy.deepcopy(w.trial_config)
        # Plan 23: TrialConfig requires agent_name + agent_model. Inject
        # them from the workflow's pinned columns so the campaign runner
        # can submit valid trial configs without each Workflow author
        # having to repeat agent/model inside trial_config too.
        frozen_config["agent_name"] = w.agent_name
        frozen_config["agent_model"] = {
            "provider": w.model_provider,
            "name": w.model_name,
        }

        task_ids = await _resolve_task_filter(s, frozen_filter)
        if not task_ids:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"workflow {w.name!r} task_filter {frozen_filter} "
                    "matched zero tasks; refusing to launch empty campaign"
                ),
            )

        name = payload.name or (
            f"{w.name} — {datetime.now(UTC).isoformat(timespec='seconds')}"
        )
        token_prefix = _token_prefix(ctx)
        # Plan 23: n_per_task is frozen onto the Campaign at launch
        # (same reasoning as task_filter + trial_config — historical
        # runs don't move under a later admin edit). expected_trial_count
        # is the total trial fan-out, not the matched-task count.
        expected = len(task_ids) * w.n_per_task
        c = Campaign(
            team_id=ctx.team_id,
            name=name,
            description=f"Launched from workflow {w.name!r} ({w.id})",
            task_filter=frozen_filter,
            trial_config=frozen_config,
            state="submitted",
            created_by_token_prefix=token_prefix,
            expected_trial_count=expected,
            n_per_task=w.n_per_task,
            workflow_id=w.id,
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        return {
            "campaign_id": str(c.id),
            "workflow_id": str(w.id),
            "expected_trial_count": expected,
            "state": c.state,
            "created_at": c.created_at.isoformat(),
        }
