"""Batches CRUD (spec §5.3 / Plan 19, renamed in Plan 28).

Routes:
- POST /api/v1/batches          — create + immediately materialize
                                  expected_trial_count from task_filter
- GET  /api/v1/batches          — list with cursor pagination
- GET  /api/v1/batches/{id}     — detail + trial roll-up (state summary,
                                  aggregate reward from Trial.result JSONB,
                                  and token totals from llm_calls)
- POST /api/v1/batches/{id}/cancel — terminate the batch + cascade-cancel
                                  its still-active trials
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Annotated, Any, Literal, NoReturn, cast

if TYPE_CHECKING:
    from loom.family_run.spec import FamilyRunSpec
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import and_, func, or_, select, update

from loom.auth import AuthContext
from loom.db.schema import (
    Batch,
    Benchmark,
    LlmCall,
    ProviderConnection,
    ProviderModelCache,
    Task,
    Team,
    TeamMembership,
    Trial,
    User,
    Worker,
)
from loom.models.batch import Combination
from loom.models.types import ModelSpec
from loom.request_params import sanitize_request_extras
from loom.security.redaction import redact_mapping, redact_text
from loom_llm_gateway.rate_card import (
    COST_META_CONFIDENCE_KEY,
    COST_META_SOURCE_KEY,
)
from loom_service.admin_audit import (
    actor_from_context,
    write_admin_audit_event,
)
from loom_service.agent_catalog import (
    get_agent,
    known_names,
    validate_agent_model_compat,
)
from loom_service.auth_guards import (
    is_admin,
    require_scope,
    require_submitting_user,
    require_team_or_admin,
)
from loom_service.batch_identity import build_batch_identity
from loom_service.combination_summary import combination_summary_for_batch
from loom_service.debug_evidence import build_batch_debug_evidence
from loom_service.dependencies import AdminSessionAndCtx, SessionAndCtx
from loom_service.diagnosis import build_batch_diagnosis, trial_failure_records
from loom_service.failure_taxonomy import (
    build_supplemental_rerun_plan,
    is_auto_safe_rerun,
    is_replaceable_by_successful_supplemental,
)
from loom_service.family_run_seed import prepare_family_run_state
from loom_service.metrics import SUBMISSION_REJECTS_TOTAL
from loom_service.monitor_filters import (
    apply_batch_monitor_filters,
    resolve_monitor_team_filter,
)
from loom_service.pagination import Cursor, decode_cursor, encode_cursor
from loom_service.provider_connection_lookup import validate_provider_connection
from loom_service.stale_running_debug import batch_stale_running_decisions
from loom_service.task_compat import task_supports_agent
from loom_service.task_config_validation import (
    expected_trial_count,
    invalid_task_config_detail,
    split_valid_task_configs,
)
from loom_service.task_filter import resolve_task_filter_with_diagnostics
from loom_service.usage_accounting import (
    PreRunBudgetEstimate,
    empty_usage_projection,
    estimate_pre_run_batch_budget,
    price_snapshots_for_trials,
    project_batch_budget,
    summarize_llm_evidence_for_trials,
    summarize_usage_counts,
    usage_status_filter,
)
from loom_service.worker_backends import get_active_backends

router = APIRouter()


def _catalog_family_run_defaults(benchmark_id: str | None) -> FamilyRunSpec | None:
    """Fetch the ``family_run_defaults`` block from the benchmark
    catalog (JSON shipped in ``loom_benchmarks``). None when the
    benchmark has no defaults or when the entry is missing.
    """
    from loom.family_run.spec import FamilyRunSpec

    if benchmark_id is None:
        return None
    try:
        from loom_benchmarks.catalog import CATALOG
    except ImportError:
        return None
    entry = CATALOG.get(benchmark_id)
    if entry is None or entry.family_run_defaults is None:
        return None
    return FamilyRunSpec.model_validate(entry.family_run_defaults)


def _build_service_state_backend(request: Request) -> Any:
    """Materialise a state backend on the service side.

    Wraps the boto3 MinIO client into a :class:`MinioObjectStore` +
    :class:`S3ArtifactsStateBackend`. Kept as a helper so tests can
    monkey-patch it with a fake without touching the route body.
    """
    from loom.family_run.state_backends import S3ArtifactsStateBackend
    from loom.trajectory.storage import MinioObjectStore

    settings = request.app.state.settings
    store = MinioObjectStore(
        endpoint_url=settings.minio_endpoint,
        access_key=settings.minio_access_key.get_secret_value(),
        secret_key=settings.minio_secret_key.get_secret_value(),
        region=settings.minio_region,
    )
    return S3ArtifactsStateBackend(store=store, bucket=settings.artifacts_bucket)


_RERUNNABLE_FAILURE_REASONS: frozenset[str] = frozenset(
    {
        "gateway_error",
        "provider_transport_disconnect",
        "retry_exhausted",
        "exhausted_retries",
    }
)


@dataclass(frozen=True)
class _BatchTrialProjection:
    id: UUID
    batch_id: UUID | None
    team_id: UUID
    task_id: str
    config: dict[str, Any]
    state: str
    failure_reason: str | None
    failure_message: str | None
    result: dict[str, Any] | None
    claimed_at: datetime | None
    pre_start_heartbeat_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    sample_idx: int
    combination_idx: int
    provider_connection_id: UUID | None
    provider_model_id: str | None
    worker_id: UUID | None


class _CreateBatch(BaseModel):
    name: str | None = Field(default=None, max_length=200)
    name_suffix: str | None = Field(default=None, max_length=80)
    description: str | None = None
    task_filter: dict[str, Any]
    trial_config: dict[str, Any]
    # Plan 23: n-sampling for single-combination batches. Ignored
    # when `combinations` is non-empty (each Combination carries
    # its own n_per_task).
    n_per_task: int = Field(default=1, ge=1, le=100)
    # Plan 28 PR-3: backend selection at the batch level. Optional;
    # defaults to "docker" so single-backend deployments don't have
    # to send it.
    backend: str = "docker"
    # Plan 28 PR-3: multi-(agent, model) combinations. Empty list
    # ⇒ single-combination behavior (agent + model come from
    # trial_config).
    combinations: list[Combination] = Field(default_factory=list)
    # cluster-deploy.md §Schema additions: batch-level provider
    # override. Carries through fan-out into Trial.provider_connection_id
    # for every trial spawned from this batch (unless overridden on
    # a per-trial basis at submission). Validated against the team
    # before insertion.
    provider_connection_id: UUID | None = None
    provider_model_id: str | None = None
    # Explicit on-behalf-of team for platform admins. Non-admin callers
    # may omit this or pass their own team id; cross-team values require
    # admin scope and are used for provider validation + Batch.team_id.
    team_id: UUID | None = None
    # Issue #188: operator/release canaries can require deterministic
    # terminal coverage from named worker pools. The batch runner adds
    # one extra pool-pinned coverage trial per normalized entry.
    required_worker_pools: list[str] = Field(default_factory=list, max_length=20)
    budget_usd: float | None = Field(default=None, ge=0)
    budget_policy: Literal["none", "soft", "hard"] = "none"
    budget_confirmed: bool = False


class _AdminCreateBatchOnBehalf(_CreateBatch):
    represented_user_id: UUID | None = None
    represented_username: str | None = Field(default=None, max_length=64)


class _RerunFailedBatch(BaseModel):
    task_ids: list[str] = Field(default_factory=list, max_length=5000)
    include_operator_approval: bool = False


def _sanitize_trial_config(config: dict[str, Any]) -> dict[str, Any]:
    out = dict(config)
    if "request_params" in out:
        out["request_params"] = sanitize_request_extras(out.get("request_params"))
    return out


def _extract_family_run_override(
    trial_config: dict[str, Any],
) -> FamilyRunSpec | None:
    """Read the optional ``family_run`` block off the trial_config.

    Returns ``None`` when the field is absent (classic mode) or the
    value is ``None``. Any parse error is surfaced to the caller as a
    :class:`ValueError` — the batches route translates that into a 400.
    """
    from loom.family_run.spec import FamilyRunSpec

    raw = trial_config.get("family_run")
    if raw is None:
        return None
    if isinstance(raw, FamilyRunSpec):
        return raw
    return FamilyRunSpec.model_validate(raw)


def _normalize_required_worker_pools(values: Sequence[str]) -> list[str]:
    pools: list[str] = []
    seen: set[str] = set()
    for raw in values:
        pool = str(raw).strip()
        if not pool:
            _reject_submission(
                reason="invalid_input",
                status_code=400,
                detail="required_worker_pools entries must be non-empty strings",
            )
        if len(pool) > 80 or any(ch.isspace() for ch in pool):
            _reject_submission(
                reason="invalid_input",
                status_code=400,
                detail=(
                    "required_worker_pools entries must be 1-80 characters "
                    "and contain no whitespace"
                ),
            )
        if pool not in seen:
            seen.add(pool)
            pools.append(pool)
    return pools


def _reject_if_k8s_worker_unavailable(
    request: Request,
    required_worker_pools: Sequence[str],
) -> None:
    """Fail-loudly gate for #383. When the cluster's
    `k8s_worker.enabled` render toggle is false, the k8s loom-worker
    Deployment is not rendered, so `k8s-worker` pool coverage cannot
    be satisfied. Reject up front instead of accepting a submission
    whose coverage trial will queue forever.
    """
    if "k8s-worker" not in required_worker_pools:
        return
    settings = request.app.state.settings
    if settings.k8s_worker_enabled:
        return
    _reject_submission(
        reason="k8s_worker_unavailable",
        status_code=400,
        detail=(
            "required_worker_pool 'k8s-worker' is not available on this "
            "cluster: k8s_worker.enabled=false in the deployed profile "
            "(#383). Use 'oldlab' for x86_64 coverage or 'gb10-arm64' "
            "for arm64 coverage."
        ),
    )


def _reject_submission(
    *,
    reason: str,
    status_code: int,
    detail: Any,
) -> NoReturn:
    SUBMISSION_REJECTS_TOTAL.labels(reason=reason).inc()
    raise HTTPException(status_code=status_code, detail=detail)


async def _reject_if_team_paused(session: Any, team_id: UUID) -> None:
    paused_at = (
        await session.execute(
            select(Team.submissions_paused_at).where(Team.id == team_id),
        )
    ).scalar_one_or_none()
    if paused_at is not None:
        _reject_submission(
            reason="team_paused",
            status_code=403,
            detail="team submissions are paused",
        )


async def _resolve_on_behalf_submitter(
    session: Any,
    payload: _AdminCreateBatchOnBehalf,
) -> User:
    if payload.team_id is None:
        _reject_submission(
            reason="invalid_input",
            status_code=400,
            detail="team_id is required for admin on-behalf batch submission",
        )
    represented_user_id = payload.represented_user_id
    represented_username = (
        payload.represented_username.strip() if payload.represented_username is not None else None
    )
    if bool(represented_user_id) == bool(represented_username):
        _reject_submission(
            reason="invalid_input",
            status_code=400,
            detail=("set exactly one of represented_user_id or represented_username"),
        )

    team = (
        await session.execute(
            select(Team).where(Team.id == payload.team_id),
        )
    ).scalar_one_or_none()
    if team is None:
        _reject_submission(
            reason="invalid_input",
            status_code=404,
            detail="team not found",
        )
    if team.disabled_at is not None:
        _reject_submission(
            reason="permission",
            status_code=403,
            detail="represented team is disabled",
        )

    if represented_user_id is not None:
        stmt = select(User).where(User.id == represented_user_id)
    else:
        assert represented_username is not None
        stmt = select(User).where(
            User.username_normalized == represented_username.casefold(),
        )
    user = (await session.execute(stmt)).scalar_one_or_none()
    if user is None:
        _reject_submission(
            reason="invalid_input",
            status_code=404,
            detail="represented user not found",
        )
    if user.status != "active" or user.disabled_at is not None:
        _reject_submission(
            reason="permission",
            status_code=403,
            detail="represented user is not active",
        )

    membership = (
        await session.execute(
            select(TeamMembership).where(
                TeamMembership.team_id == payload.team_id,
                TeamMembership.user_id == user.id,
            ),
        )
    ).scalar_one_or_none()
    if membership is None:
        _reject_submission(
            reason="permission",
            status_code=403,
            detail="represented user is not a member of the represented team",
        )
    return cast(User, user)


async def _resolve_submission_team_id(
    session: Any,
    ctx: AuthContext,
    requested_team_id: UUID | None,
) -> UUID:
    if requested_team_id is None:
        if ctx.team_id is None:
            _reject_submission(
                reason="invalid_input",
                status_code=400,
                detail="admin tokens must scope batches to a team — "
                "set team_id or use a team-scoped user token",
            )
        assert ctx.team_id is not None
        return ctx.team_id

    if not is_admin(ctx):
        if ctx.team_id != requested_team_id:
            _reject_submission(
                reason="permission",
                status_code=403,
                detail="cross-team batch submission requires admin scope",
            )
        return requested_team_id

    exists = (
        await session.execute(
            select(Team.id).where(Team.id == requested_team_id),
        )
    ).scalar_one_or_none()
    if exists is None:
        _reject_submission(
            reason="invalid_input",
            status_code=404,
            detail="team not found",
        )
    return requested_team_id


async def _reject_if_known_failed_provider_model(
    session: Any,
    *,
    provider_connection_id: UUID | None,
    provider_model_id: str | None,
    context: str | None = None,
) -> None:
    if provider_connection_id is None or not provider_model_id:
        return
    row = (
        await session.execute(
            select(ProviderModelCache).where(
                ProviderModelCache.provider_connection_id == provider_connection_id,
                ProviderModelCache.model_id == provider_model_id,
            ),
        )
    ).scalar_one_or_none()
    if row is None:
        prefix = f"{context}: " if context else ""
        _reject_submission(
            reason="provider_model_cache",
            status_code=400,
            detail=(
                f"{prefix}provider model {provider_model_id!r} is not in the model cache "
                "for this provider connection; run `loom providers models "
                "NAME --refresh` or choose a cached model"
            ),
        )
    if row.last_preflight_status != "failed":
        return
    prefix = f"{context}: " if context else ""
    detail = f"{prefix}provider model {provider_model_id!r} last preflight failed for this provider connection"
    if row.last_preflight_error_code:
        detail += f" ({row.last_preflight_error_code})"
    detail += "; run provider model preflight again or choose another model"
    _reject_submission(
        reason="provider_model_preflight",
        status_code=400,
        detail=detail,
    )


def _effective_provider_fields(
    payload: _CreateBatch,
    combo: Combination | None,
) -> tuple[UUID | None, str | None]:
    if combo is None:
        return payload.provider_connection_id, payload.provider_model_id
    return (
        combo.provider_connection_id or payload.provider_connection_id,
        combo.provider_model_id or payload.provider_model_id,
    )


def _combination_context(index: int, combo: Combination) -> str:
    label = combo.label or _derive_combination_label(combo)
    return f"combinations[{index}] {label!r}"


def _merge_budget_source(values: set[str]) -> str:
    if not values:
        return "none"
    if len(values) == 1:
        return next(iter(values))
    return "mixed"


def _merge_budget_confidence(values: set[str]) -> str:
    if not values:
        return "unavailable"
    if "unavailable" in values:
        return "unavailable"
    if len(values) == 1:
        return next(iter(values))
    return "mixed"


async def _estimate_pre_run_budget_for_payload(
    session: Any,
    *,
    payload: _CreateBatch,
    provider_connections_by_id: dict[UUID, ProviderConnection],
    task_count: int,
    expected_trial_count: int,
    required_worker_pool_count: int,
    settings: Any,
    budget_usd: float | None,
    budget_policy: str,
    provider_connection: ProviderConnection | None,
) -> tuple[PreRunBudgetEstimate, list[dict[str, Any]]]:
    if not payload.combinations:
        estimate = await estimate_pre_run_batch_budget(
            session,
            provider_connection=provider_connection,
            provider_model_id=payload.provider_model_id,
            expected_trial_count=expected_trial_count,
            settings=settings,
            budget_usd=budget_usd,
            budget_policy=budget_policy,
        )
        return estimate, []

    estimates: list[PreRunBudgetEstimate] = []
    items: list[dict[str, Any]] = []
    for i, combo in enumerate(payload.combinations):
        conn_id, model_id = _effective_provider_fields(payload, combo)
        combo_expected = task_count * int(combo.n_per_task)
        if i == 0:
            combo_expected += required_worker_pool_count
        estimate = await estimate_pre_run_batch_budget(
            session,
            provider_connection=(
                provider_connections_by_id.get(conn_id) if conn_id is not None else None
            ),
            provider_model_id=model_id,
            expected_trial_count=combo_expected,
            settings=settings,
            budget_usd=budget_usd,
            budget_policy=budget_policy,
        )
        estimates.append(estimate)
        items.append(
            {
                "combination_idx": i,
                "label": combo.label or _derive_combination_label(combo),
                "provider_connection_id": str(conn_id) if conn_id else None,
                "provider_model_id": model_id,
                "expected_trial_count": combo_expected,
                "pre_run_estimated_cost_usd": (estimate.pre_run_estimated_cost_usd),
                "cost_estimate_source": estimate.cost_estimate_source,
                "cost_estimate_confidence": estimate.cost_estimate_confidence,
                "unpriced_reason": estimate.unpriced_reason,
                "pre_run_estimated_llm_calls_count": (estimate.pre_run_estimated_llm_calls_count),
                "pre_run_estimated_prompt_tokens": (estimate.pre_run_estimated_prompt_tokens),
                "pre_run_estimated_completion_tokens": (
                    estimate.pre_run_estimated_completion_tokens
                ),
            }
        )

    costs = [estimate.pre_run_estimated_cost_usd for estimate in estimates]
    total_cost = (
        sum(cast(float, cost) for cost in costs)
        if all(cost is not None for cost in costs)
        else None
    )
    aggregate = PreRunBudgetEstimate(
        budget_usd=estimates[0].budget_usd if estimates else budget_usd,
        budget_policy=budget_policy,
        pre_run_estimated_cost_usd=total_cost,
        cost_estimate_source=_merge_budget_source(
            {estimate.cost_estimate_source for estimate in estimates}
        ),
        cost_estimate_confidence=_merge_budget_confidence(
            {estimate.cost_estimate_confidence for estimate in estimates}
        ),
        pre_run_estimated_llm_calls_count=sum(
            estimate.pre_run_estimated_llm_calls_count for estimate in estimates
        ),
        pre_run_estimated_prompt_tokens=sum(
            estimate.pre_run_estimated_prompt_tokens for estimate in estimates
        ),
        pre_run_estimated_completion_tokens=sum(
            estimate.pre_run_estimated_completion_tokens for estimate in estimates
        ),
        unpriced_reason=(None if total_cost is not None else "one_or_more_combinations_unpriced"),
    )
    return aggregate, [
        {
            "reason": "combination_budget_estimate",
            "items": items,
        }
    ]


def _agents_in_batch(
    combinations: Sequence[Combination],
    single_agent_name: Any,
) -> list[str]:
    """Collect the agent names involved in this batch: every combo's
    `agent_name` for multi-combination batches, else the single
    `trial_config.agent_name`. Empty when the request used neither
    surface (legacy default-agent path)."""
    if combinations:
        return list({c.agent_name for c in combinations})
    if isinstance(single_agent_name, str) and single_agent_name:
        return [single_agent_name]
    return []


async def _reject_agent_task_incompat(
    session: Any,
    *,
    valid_task_ids: Sequence[str],
    combinations: Sequence[Combination],
    single_agent_name: Any,
) -> None:
    """#320 preflight. For every agent in the batch, drop any task
    that doesn't expose every capability the agent requires
    (currently only `solution_solve_sh` for oracle). Reject the whole
    batch with a structured 400 listing the offending (agent, task)
    pairs so the caller can resubmit with a per-agent task slate."""
    agents = _agents_in_batch(combinations, single_agent_name)
    if not agents or not valid_task_ids:
        return
    # Skip the DB read entirely if every agent is capability-permissive.
    requirements: dict[str, frozenset[str]] = {}
    for name in agents:
        entry = get_agent(name)
        if entry is None or not entry.requires_capabilities:
            continue
        requirements[name] = entry.requires_capabilities
    if not requirements:
        return

    rows = (
        await session.execute(
            select(Task.id, Task.config, Task.tags).where(
                Task.id.in_(list(valid_task_ids)),
            ),
        )
    ).all()
    configs: dict[str, Any] = {str(tid): cfg for tid, cfg, _ in rows}
    tags_by_id: dict[str, dict[str, str]] = {str(tid): dict(tags or {}) for tid, _, tags in rows}

    offenders: dict[str, list[str]] = {}
    for agent_name, required in requirements.items():
        bad = [
            tid
            for tid in valid_task_ids
            if not task_supports_agent(
                configs.get(tid) or {},
                required,
                tags=tags_by_id.get(tid),
            )
        ]
        if bad:
            offenders[agent_name] = bad

    if not offenders:
        return

    pairs = "; ".join(
        f"{name}: {len(ids)} task(s) (e.g. {sorted(ids)[0]})"
        for name, ids in sorted(offenders.items())
    )
    detail = (
        f"agent×task capability mismatch — {pairs}. The listed agents "
        f"cannot run these tasks at the platform level (e.g. oracle "
        f"requires a benchmark adapter that ships `solution/solve.sh`). "
        f"Submit per-agent batches with the "
        f"compatible task slate, or drop the incompatible agent."
    )
    _reject_submission(
        reason="agent_task_incompat",
        status_code=400,
        detail=detail,
    )


def _serialize(
    b: Batch,
    *,
    summary: dict[str, int] | None = None,
    aggregate_reward: float | None = None,
    usage: dict[str, int] | None = None,
    owner_team: Team | None = None,
    submitted_by_user: User | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "id": str(b.id),
        "team_id": str(b.team_id),
        "name": b.name,
        "description": b.description,
        "task_filter": b.task_filter,
        "trial_config": b.trial_config,
        "state": b.state,
        "result_status": b.result_status,
        "failure_reason": b.failure_reason,
        "failure_message": (redact_text(b.failure_message) if b.failure_message else None),
        "fanout_errors": redact_mapping(b.fanout_errors),
        "rerun_of_batch_id": (str(b.rerun_of_batch_id) if b.rerun_of_batch_id else None),
        "rerun_targets": b.rerun_targets,
        "created_at": b.created_at.isoformat(),
        "finished_at": (b.finished_at.isoformat() if b.finished_at else None),
        "created_by_token_prefix": b.created_by_token_prefix,
        "expected_trial_count": b.expected_trial_count,
        "n_per_task": b.n_per_task,
        "backend": b.backend,
        "combinations": b.combinations,
        "required_worker_pools": b.required_worker_pools,
        "visibility": b.visibility,
        "share_status": b.share_status,
        "source_provenance": b.source_provenance,
        "resolved_task_ids": b.resolved_task_ids,
        "submitted_by_user": (
            {
                "id": str(submitted_by_user.id),
                "username": submitted_by_user.username,
                "team_id": str(b.team_id),
                "team_name": owner_team.name if owner_team else None,
            }
            if submitted_by_user is not None
            else None
        ),
    }
    if owner_team is not None:
        out["team_name"] = owner_team.name
        out["owner_team"] = {
            "id": str(owner_team.id),
            "name": owner_team.name,
        }
    usage_projection = dict(usage or _empty_usage_projection())
    usage_projection.pop("total_cost_usd", None)
    out.update(usage_projection)
    out.update(project_batch_budget(b, usage_projection))
    if summary is not None:
        out["trial_summary"] = summary
        out["aggregate_reward"] = aggregate_reward
    if extra:
        out.update(extra)
    return out


async def _create_batch_record(
    request: Request,
    s: Any,
    ctx: AuthContext,
    payload: _CreateBatch,
    *,
    submitted_by_user_id: UUID | None,
    usage_attributed_user_id: UUID | None,
    usage_attributed_actor: str | None,
) -> dict[str, Any]:
    submission_team_id = await _resolve_submission_team_id(
        s,
        ctx,
        payload.team_id,
    )
    await _reject_if_team_paused(s, submission_team_id)

    catalog = known_names()
    trial_config = _sanitize_trial_config(payload.trial_config)
    required_worker_pools = _normalize_required_worker_pools(
        payload.required_worker_pools,
    )
    _reject_if_k8s_worker_unavailable(request, required_worker_pools)

    if payload.combinations:
        # Multi-combination batch. trial_config MUST NOT carry
        # agent_name / agent_model / n_per_task in this shape —
        # those live on each Combination.
        for forbidden in ("agent_name", "agent_model"):
            if forbidden in trial_config:
                _reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=(
                        f"trial_config.{forbidden} must be absent when "
                        "`combinations` is supplied — each combination "
                        "carries its own agent/model"
                    ),
                )
        # Catalog membership + agent⇄model compatibility per
        # Combination.
        for i, combo in enumerate(payload.combinations):
            if combo.agent_name not in catalog:
                _reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=(
                        f"combinations[{i}].agent_name "
                        f"{combo.agent_name!r} is not in the agent "
                        "catalog. GET /api/v1/agents."
                    ),
                )
            err = validate_agent_model_compat(
                combo.agent_name,
                combo.agent_model,
            )
            if err is not None:
                _reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=f"combinations[{i}]: {err}",
                )
        # Labels unique within the batch (after computing the
        # derived label for those without one).
        seen_labels: set[str] = set()
        for i, combo in enumerate(payload.combinations):
            label = combo.label or _derive_combination_label(combo)
            if label in seen_labels:
                _reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=(f"combinations[{i}] label {label!r} is duplicated within the batch"),
                )
            seen_labels.add(label)
    else:
        # Single-combination batch. Catalog check on the agent
        # embedded in trial_config + agent⇄model compatibility.
        agent_name = trial_config.get("agent_name")
        if isinstance(agent_name, str) and agent_name:
            if agent_name not in catalog:
                _reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=(
                        f"unknown agent_name {agent_name!r} in trial_config. GET /api/v1/agents."
                    ),
                )
            if "agent_model" not in trial_config:
                _reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=(
                        "trial_config.agent_model is required when "
                        "trial_config.agent_name is supplied; use null "
                        "for agents that do not call a model"
                    ),
                )
            model_raw = trial_config["agent_model"]
            model: ModelSpec | None
            if model_raw is None:
                model = None
            else:
                try:
                    model = ModelSpec.model_validate(model_raw)
                except Exception as exc:
                    _reject_submission(
                        reason="invalid_input",
                        status_code=400,
                        detail=(f"trial_config.agent_model failed to validate: {exc}"),
                    )
            err = validate_agent_model_compat(agent_name, model)
            if err is not None:
                _reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=f"trial_config: {err}",
                )

    # cluster-deploy.md §POST /batches: reject when no live worker
    # advertises the requested backend. Saves operators from creating
    # batches that would stall in 'submitted' forever (no worker will
    # ever claim them). Backend catalog is owned by /api/v1/backends;
    # this check uses the same predicate so a backend that shows
    # `available=false` there also rejects here.
    active_backends = await get_active_backends(s)
    if payload.backend not in active_backends:
        available_str = (
            ", ".join(sorted(active_backends)) if active_backends else "(none — no active workers)"
        )
        _reject_submission(
            reason="no_workers",
            status_code=400,
            detail=(
                f"no active worker advertises backend "
                f"{payload.backend!r}. Currently available: "
                f"{available_str}. See `GET /api/v1/backends`."
            ),
        )

    # Validate provider_connection_id before task materialization/fan-out
    # work so known bad provider/model input returns a direct actionable error.
    # Combination-level provider fields override the batch-level value; the
    # batch-level value remains the backward-compatible default.
    provider_connection: ProviderConnection | None = None
    provider_connection_ids: set[UUID] = set()
    provider_model_checks: list[tuple[UUID | None, str | None, str | None]] = []
    if payload.combinations:
        for i, combo in enumerate(payload.combinations):
            conn_id, model_id = _effective_provider_fields(payload, combo)
            if conn_id is not None:
                provider_connection_ids.add(conn_id)
            provider_model_checks.append((conn_id, model_id, _combination_context(i, combo)))
    else:
        conn_id, model_id = _effective_provider_fields(payload, None)
        if conn_id is not None:
            provider_connection_ids.add(conn_id)
        provider_model_checks.append((conn_id, model_id, None))

    for conn_id in sorted(provider_connection_ids, key=str):
        try:
            await validate_provider_connection(
                s,
                conn_id,
                team_id=submission_team_id,
            )
        except HTTPException:
            SUBMISSION_REJECTS_TOTAL.labels(
                reason="provider_connection",
            ).inc()
            raise

    for conn_id, model_id, context in provider_model_checks:
        await _reject_if_known_failed_provider_model(
            s,
            provider_connection_id=conn_id,
            provider_model_id=model_id,
            context=context,
        )

    if payload.provider_connection_id is not None:
        provider_connection = (
            await s.execute(
                select(ProviderConnection).where(
                    ProviderConnection.id == payload.provider_connection_id,
                    ProviderConnection.deleted_at.is_(None),
                ),
            )
        ).scalar_one_or_none()
    provider_connections_by_id: dict[UUID, ProviderConnection] = {}
    if provider_connection_ids:
        provider_rows = (
            (
                await s.execute(
                    select(ProviderConnection).where(
                        ProviderConnection.id.in_(list(provider_connection_ids)),
                        ProviderConnection.deleted_at.is_(None),
                    ),
                )
            )
            .scalars()
            .all()
        )
        provider_connections_by_id = {row.id: row for row in provider_rows}

    task_result = await resolve_task_filter_with_diagnostics(
        s,
        payload.task_filter,
        team_id=submission_team_id,
    )
    task_ids = task_result.task_ids
    # Audit M2: a filter materializing to zero tasks creates a
    # batch stuck in `submitted` forever — reject up front.
    if not task_ids:
        _reject_submission(
            reason="empty_filter",
            status_code=400,
            detail=(
                f"task_filter {payload.task_filter} matched zero "
                "tasks; refusing to create empty batch"
            ),
        )

    valid_task_ids, invalid_tasks = await split_valid_task_configs(
        s,
        task_ids,
    )
    if invalid_tasks:
        _reject_submission(
            reason="invalid_task_config",
            status_code=400,
            detail=invalid_task_config_detail(invalid_tasks),
        )

    # #320 preflight: skip launching trials for (agent, task) combos
    # where the agent's `requires_capabilities` doesn't match what the
    # task bundle exposes. The current case is oracle, which needs
    # `solution/solve.sh` (granted by the pytest-verifier heuristic).
    # Reject upfront with a structured detail instead of fanning out
    # into trials that deterministically AgentError mid-run.
    await _reject_agent_task_incompat(
        s,
        valid_task_ids=valid_task_ids,
        combinations=payload.combinations,
        single_agent_name=trial_config.get("agent_name"),
    )

    token_prefix = ctx.token_hash.hex()[:8] if ctx.token_hash else "00000000"

    # expected_trial_count = sum over combinations × tasks.
    if payload.combinations:
        combinations_jsonb = [c.model_dump(mode="json") for c in payload.combinations]
        expected = expected_trial_count(
            task_count=len(valid_task_ids),
            n_per_task=payload.n_per_task,
            combinations=combinations_jsonb,
        ) + len(required_worker_pools)
    else:
        expected = expected_trial_count(
            task_count=len(valid_task_ids),
            n_per_task=payload.n_per_task,
            combinations=None,
        ) + len(required_worker_pools)
        combinations_jsonb = []

    budget_policy = payload.budget_policy
    if payload.budget_usd is None:
        budget_policy = "none"
    elif budget_policy == "none":
        budget_policy = "hard"

    budget_estimate, budget_diagnostics = await _estimate_pre_run_budget_for_payload(
        s,
        payload=payload,
        provider_connections_by_id=provider_connections_by_id,
        task_count=len(valid_task_ids),
        expected_trial_count=expected,
        required_worker_pool_count=len(required_worker_pools),
        settings=request.app.state.settings,
        budget_usd=payload.budget_usd,
        budget_policy=budget_policy,
        provider_connection=provider_connection,
    )
    if budget_policy != "none":
        pre_run_cost = budget_estimate.pre_run_estimated_cost_usd
        budget_value = budget_estimate.budget_usd
        if pre_run_cost is None:
            reason = (
                "batch_budget_unpriced"
                if budget_policy == "hard"
                else "batch_budget_unpriced_confirmation_required"
            )
            if budget_policy == "hard" or not payload.budget_confirmed:
                _reject_submission(
                    reason=reason,
                    status_code=400 if budget_policy == "hard" else 409,
                    detail={
                        "reason": reason,
                        "budget": budget_estimate.as_api_dict(),
                    },
                )
        elif budget_value is not None and pre_run_cost > budget_value:
            if budget_policy == "hard":
                _reject_submission(
                    reason="batch_budget_exceeded",
                    status_code=400,
                    detail={
                        "reason": "batch_budget_exceeded",
                        "budget": budget_estimate.as_api_dict(),
                    },
                )
            if not payload.budget_confirmed:
                _reject_submission(
                    reason="batch_budget_confirmation_required",
                    status_code=409,
                    detail={
                        "reason": "batch_budget_confirmation_required",
                        "budget": budget_estimate.as_api_dict(),
                    },
                )

    explicit_name = payload.name.strip() if payload.name else ""
    explicit_description = payload.description.strip() if payload.description else ""
    generated_identity = build_batch_identity(
        task_filter=dict(payload.task_filter),
        trial_config=trial_config,
        combinations=combinations_jsonb or payload.combinations,
        n_per_task=payload.n_per_task,
        backend=payload.backend,
        suffix=payload.name_suffix,
        provider_model_id=payload.provider_model_id,
    )
    batch_name = explicit_name or generated_identity.name
    batch_description = explicit_description or generated_identity.description

    b = Batch(
        team_id=submission_team_id,
        name=batch_name,
        description=batch_description,
        task_filter=dict(payload.task_filter),
        resolved_task_ids=list(task_result.task_ids),
        source_provenance=list(task_result.benchmark_selection_provenance),
        trial_config=trial_config,
        state="submitted",
        created_by_token_prefix=token_prefix,
        submitted_by_user_id=submitted_by_user_id,
        usage_attributed_user_id=usage_attributed_user_id,
        usage_attributed_actor=usage_attributed_actor,
        expected_trial_count=expected,
        n_per_task=payload.n_per_task,
        backend=payload.backend,
        combinations=combinations_jsonb,
        required_worker_pools=required_worker_pools,
        provider_connection_id=payload.provider_connection_id,
        provider_model_id=payload.provider_model_id,
        budget_usd=payload.budget_usd if budget_policy != "none" else None,
        budget_policy=budget_policy,
        budget_confirmed_at=(
            datetime.now(UTC) if budget_policy == "soft" and payload.budget_confirmed else None
        ),
        pre_run_estimated_cost_usd=budget_estimate.pre_run_estimated_cost_usd,
        pre_run_cost_estimate_source=budget_estimate.cost_estimate_source,
        pre_run_cost_estimate_confidence=(budget_estimate.cost_estimate_confidence),
        budget_diagnostics=budget_diagnostics,
    )
    s.add(b)
    await s.flush()
    await s.refresh(b)

    # #672 PR-3 hot-path: if the trial_config opted the batch into
    # family-run mode (or the benchmark catalog carries defaults),
    # resolve the spec, seed per-family state, and record the resolved
    # spec on the batch. The batch_runner reads batch_family_state to
    # stamp trials.family_key at CP-submit time; the scheduler's claim
    # query then gates trial claims by family sequence position.
    override_family_run = _extract_family_run_override(trial_config)
    # Resolve the catalog default off the tasks' shared benchmark_id.
    # Family-run is rejected for multi-benchmark batches unless the
    # trial_config carries a fully-formed override — mixing benchmark
    # defaults across tasks would silently pick one arbitrary benchmark
    # and apply it to foreign tasks.
    task_rows = (
        (
            await s.execute(
                select(Task).where(Task.id.in_(list(valid_task_ids))),
            )
        )
        .scalars()
        .all()
    )
    distinct_benchmarks = {t.benchmark_id for t in task_rows if t.benchmark_id is not None}
    catalog_default = None
    if len(distinct_benchmarks) == 1:
        catalog_default = _catalog_family_run_defaults(
            next(iter(distinct_benchmarks)),
        )
    if override_family_run is not None or catalog_default is not None:
        try:
            state_backend = _build_service_state_backend(request)
            seeded = await prepare_family_run_state(
                session=s,
                batch_id=b.id,
                tasks=task_rows,
                catalog_default=catalog_default,
                override=override_family_run,
                state_backend=state_backend,
            )
        except ValueError as exc:
            _reject_submission(
                reason="invalid_family_run_spec",
                status_code=400,
                detail=f"family_run spec resolution failed: {exc}",
            )
        if seeded is not None:
            b.family_run_spec = seeded.resolved_spec.model_dump(mode="json")
            await s.flush()

    usage_projection = _empty_usage_projection()
    budget_projection = project_batch_budget(b, usage_projection)
    return {
        "batch_id": str(b.id),
        "team_id": str(b.team_id),
        "name": b.name,
        "description": b.description,
        "expected_trial_count": expected,
        "n_per_task": b.n_per_task,
        "backend": b.backend,
        "combinations": b.combinations,
        "required_worker_pools": b.required_worker_pools,
        "state": b.state,
        "created_at": b.created_at.isoformat(),
        **budget_projection,
    }


@router.post("/batches", status_code=201)
async def create_batch(
    request: Request,
    sc: SessionAndCtx,
    payload: _CreateBatch,
) -> dict[str, Any]:
    s, ctx = sc
    try:
        require_scope(ctx, "submit")
        require_submitting_user(ctx)
    except HTTPException:
        SUBMISSION_REJECTS_TOTAL.labels(reason="permission").inc()
        raise
    response = await _create_batch_record(
        request,
        s,
        ctx,
        payload,
        submitted_by_user_id=ctx.user_id,
        usage_attributed_user_id=ctx.user_id,
        usage_attributed_actor=(f"user:{ctx.user_id}" if ctx.user_id is not None else None),
    )
    await s.commit()
    return response


@router.post("/admin/batches/on-behalf", status_code=201)
async def admin_create_batch_on_behalf(
    request: Request,
    sc: AdminSessionAndCtx,
    payload: _AdminCreateBatchOnBehalf,
    x_loom_admin_actor: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    s, ctx = sc
    actor = await actor_from_context(s, ctx, x_loom_admin_actor)
    represented_user = await _resolve_on_behalf_submitter(s, payload)
    response = await _create_batch_record(
        request,
        s,
        ctx,
        payload,
        submitted_by_user_id=represented_user.id,
        usage_attributed_user_id=ctx.user_id,
        usage_attributed_actor=actor,
    )
    await write_admin_audit_event(
        s,
        actor=actor,
        action="batch.submit_on_behalf",
        target_type="batch",
        target_id=response["batch_id"],
        request=request,
        metadata={
            "represented_user_id": str(represented_user.id),
            "represented_username": represented_user.username,
            "represented_team_id": str(payload.team_id),
            "expected_trial_count": response["expected_trial_count"],
            "backend": response["backend"],
        },
    )
    await s.commit()
    return response


def _derive_combination_label(combo: Combination) -> str:
    """Default label `"{agent_name}"` or
    `"{agent_name}/{provider}/{name}"` when a model is set."""
    if combo.agent_model is None:
        return combo.agent_name
    return f"{combo.agent_name}/{combo.agent_model.provider}/{combo.agent_model.name}"


@router.get("/batches")
async def list_batches(
    request: Request,
    sc: SessionAndCtx,
    team_id: Annotated[UUID | None, Query()] = None,
    q: Annotated[str | None, Query()] = None,
    benchmark_id: Annotated[str | None, Query()] = None,
    agent_name: Annotated[str | None, Query()] = None,
    agent: Annotated[str | None, Query()] = None,
    model_provider: Annotated[str | None, Query()] = None,
    model_name: Annotated[str | None, Query()] = None,
    model: Annotated[str | None, Query()] = None,
    provider_connection_id: Annotated[UUID | None, Query()] = None,
    provider_model_id: Annotated[str | None, Query()] = None,
    state: Annotated[
        str | None,
        Query(description="comma-separated state filter"),
    ] = None,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(gt=0, le=200)] = 50,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "read:own")

    target_team = resolve_monitor_team_filter(ctx, team_id)

    stmt = select(Batch).order_by(
        Batch.created_at.desc(),
        Batch.id.desc(),
    )
    stmt = apply_batch_monitor_filters(
        stmt,
        target_team=target_team,
        q=q,
        benchmark_id=benchmark_id,
        agent_name=agent_name or agent,
        model_provider=model_provider,
        model_name=model_name or model,
        provider_connection_id=provider_connection_id,
        provider_model_id=provider_model_id,
        state=state,
    )
    if cursor:
        try:
            cur = decode_cursor(cursor)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail=str(exc),
            ) from exc
        stmt = stmt.where(
            or_(
                Batch.created_at < cur.submitted_at,
                and_(
                    Batch.created_at == cur.submitted_at,
                    Batch.id < cur.id,
                ),
            ),
        )
    stmt = stmt.limit(limit + 1)
    rows: Sequence[Batch] = (await s.execute(stmt)).scalars().all()

    items = list(rows)
    if len(items) > limit:
        items = items[:limit]
        last = items[-1]
        next_cursor: str | None = encode_cursor(
            Cursor(submitted_at=last.created_at, id=last.id),
        )
    else:
        next_cursor = None
    usage_by_batch = await _usage_by_batch_ids(s, [r.id for r in items])
    teams_by_id: dict[UUID, Team] = {}
    users_by_id: dict[UUID, User] = {}
    if items:
        team_rows = (
            (
                await s.execute(
                    select(Team).where(Team.id.in_({r.team_id for r in items})),
                )
            )
            .scalars()
            .all()
        )
        teams_by_id = {team.id: team for team in team_rows}
        user_ids = {r.submitted_by_user_id for r in items if r.submitted_by_user_id is not None}
        if user_ids:
            user_rows = (
                (
                    await s.execute(
                        select(User).where(User.id.in_(user_ids)),
                    )
                )
                .scalars()
                .all()
            )
            users_by_id = {user.id: user for user in user_rows}
    return {
        "items": [
            _serialize(
                r,
                usage=usage_by_batch.get(r.id),
                owner_team=teams_by_id.get(r.team_id),
                submitted_by_user=(
                    users_by_id.get(r.submitted_by_user_id)
                    if r.submitted_by_user_id is not None
                    else None
                ),
            )
            for r in items
        ],
        "next_cursor": next_cursor,
    }


def _rollup_from_result(result: dict[str, Any] | None) -> float | None:
    """Pull reward out of a Trial.result JSONB. Same logic as
    routes/trials.py — kept inline here so the rollup query can flow
    naturally."""
    if not result:
        return None
    reward = result.get("aggregate_reward")
    if reward is None:
        reward = result.get("reward")
    try:
        return float(reward) if reward is not None else None
    except (TypeError, ValueError):
        return None


def _empty_usage_projection() -> dict[str, Any]:
    return empty_usage_projection()


def _priced_call_filter() -> Any:
    return (
        ~LlmCall.rate_card_hash.like("facade:tokens-only%")
        & ~_price_unknown_call_filter()
        & (LlmCall.rate_card_hash != "failed-upstream")
    )


def _price_unknown_call_filter() -> Any:
    return LlmCall.rate_card_hash.like("facade:rate-card:missing%") | _cost_meta_filter(
        COST_META_SOURCE_KEY, "unpriced"
    )


def _cost_meta_filter(key: str, value: str) -> Any:
    return func.coalesce(LlmCall.provider_extras.op("->>")(key), "") == value


def _cost_source_counts(row: Any) -> dict[str, int]:
    return {
        "operator-supplied": int(row.cost_source_operator_supplied_count or 0),
        "rate-card": int(row.cost_source_rate_card_count or 0),
        "tokens-only": int(row.cost_source_tokens_only_count or 0),
        "unpriced": int(row.cost_source_unpriced_count or 0),
    }


def _cost_confidence_counts(row: Any) -> dict[str, int]:
    return {
        "configured": int(row.cost_confidence_configured_count or 0),
        "not_applicable": int(row.cost_confidence_not_applicable_count or 0),
        "unavailable": int(row.cost_confidence_unavailable_count or 0),
    }


async def _usage_totals_for_trials(
    session: Any,
    trials: Sequence[Any],
) -> dict[str, Any]:
    trial_ids = [trial.id for trial in trials]
    if not trial_ids:
        return _empty_usage_projection()
    row = (
        await session.execute(
            select(
                func.coalesce(
                    func.sum(LlmCall.input_tokens),
                    0,
                ).label("total_prompt_tokens"),
                func.coalesce(
                    func.sum(LlmCall.output_tokens),
                    0,
                ).label("total_completion_tokens"),
                func.count(LlmCall.id).label("llm_calls_count"),
                func.coalesce(
                    func.sum(LlmCall.cost_usd),
                    0,
                ).label("total_cost_usd"),
                func.count(LlmCall.id)
                .filter(_priced_call_filter())
                .label("priced_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(LlmCall.rate_card_hash.like("facade:tokens-only%"))
                .label("token_only_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(_price_unknown_call_filter())
                .label("price_unknown_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(LlmCall.rate_card_hash == "failed-upstream")
                .label("failed_upstream_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_SOURCE_KEY, "operator-supplied"))
                .label("cost_source_operator_supplied_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_SOURCE_KEY, "rate-card"))
                .label("cost_source_rate_card_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_SOURCE_KEY, "tokens-only"))
                .label("cost_source_tokens_only_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_SOURCE_KEY, "unpriced"))
                .label("cost_source_unpriced_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_CONFIDENCE_KEY, "configured"))
                .label("cost_confidence_configured_count"),
                func.count(LlmCall.id)
                .filter(
                    _cost_meta_filter(COST_META_CONFIDENCE_KEY, "not_applicable"),
                )
                .label("cost_confidence_not_applicable_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_CONFIDENCE_KEY, "unavailable"))
                .label("cost_confidence_unavailable_count"),
                func.count(LlmCall.id)
                .filter(usage_status_filter("partial"))
                .label("partial_usage_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(usage_status_filter("missing"))
                .label("missing_usage_llm_calls_count"),
            ).where(LlmCall.trial_id.in_(trial_ids)),
        )
    ).one()
    return summarize_usage_counts(
        llm_calls_count=int(row.llm_calls_count or 0),
        total_prompt_tokens=int(row.total_prompt_tokens or 0),
        total_completion_tokens=int(row.total_completion_tokens or 0),
        total_cost_usd=row.total_cost_usd,
        priced_llm_calls_count=int(row.priced_llm_calls_count or 0),
        token_only_llm_calls_count=int(row.token_only_llm_calls_count or 0),
        price_unknown_llm_calls_count=int(
            row.price_unknown_llm_calls_count or 0,
        ),
        failed_upstream_llm_calls_count=int(
            row.failed_upstream_llm_calls_count or 0,
        ),
        partial_usage_llm_calls_count=int(
            row.partial_usage_llm_calls_count or 0,
        ),
        missing_usage_llm_calls_count=int(
            row.missing_usage_llm_calls_count or 0,
        ),
        cost_source_counts=_cost_source_counts(row),
        cost_confidence_counts=_cost_confidence_counts(row),
    )


async def _llm_calls_for_trials(
    session: Any,
    trials: Sequence[Any],
) -> list[LlmCall]:
    trial_ids = [trial.id for trial in trials]
    if not trial_ids:
        return []
    return list(
        (
            await session.execute(
                select(LlmCall)
                .where(LlmCall.trial_id.in_(trial_ids))
                .order_by(LlmCall.captured_at.asc(), LlmCall.id.asc()),
            )
        )
        .scalars()
        .all()
    )


async def _worker_pool_names_for_trials(
    session: Any,
    trials: Sequence[Any],
) -> dict[UUID, str]:
    worker_ids = sorted(
        {trial.worker_id for trial in trials if trial.worker_id is not None},
        key=str,
    )
    if not worker_ids:
        return {}
    result = await session.execute(
        select(Worker.id, Worker.pool_name).where(Worker.id.in_(worker_ids)),
    )
    return {worker_id: pool_name for worker_id, pool_name in result.all()}


async def _llm_call_counts_for_trials(
    session: Any,
    trials: Sequence[Any],
) -> dict[UUID, int]:
    trial_ids = [trial.id for trial in trials]
    if not trial_ids:
        return {}
    rows = (
        await session.execute(
            select(LlmCall.trial_id, func.count(LlmCall.id))
            .where(LlmCall.trial_id.in_(trial_ids))
            .group_by(LlmCall.trial_id),
        )
    ).all()
    return {trial_id: int(count or 0) for trial_id, count in rows if trial_id is not None}


async def _trial_projections_for_batch_ids(
    session: Any,
    batch_ids: Sequence[UUID],
) -> list[_BatchTrialProjection]:
    if not batch_ids:
        return []
    rows = (
        await session.execute(
            select(
                Trial.id,
                Trial.batch_id,
                Trial.team_id,
                Trial.task_id,
                Trial.config,
                Trial.state,
                Trial.failure_reason,
                Trial.failure_message,
                Trial.result,
                Trial.claimed_at,
                Trial.pre_start_heartbeat_at,
                Trial.started_at,
                Trial.finished_at,
                Trial.sample_idx,
                Trial.combination_idx,
                Trial.provider_connection_id,
                Trial.provider_model_id,
                Trial.worker_id,
            ).where(Trial.batch_id.in_(list(batch_ids))),
        )
    ).all()
    return [
        _BatchTrialProjection(
            id=row.id,
            batch_id=row.batch_id,
            team_id=row.team_id,
            task_id=row.task_id,
            config=row.config,
            state=row.state,
            failure_reason=row.failure_reason,
            failure_message=row.failure_message,
            result=row.result,
            claimed_at=row.claimed_at,
            pre_start_heartbeat_at=row.pre_start_heartbeat_at,
            started_at=row.started_at,
            finished_at=row.finished_at,
            sample_idx=row.sample_idx,
            combination_idx=row.combination_idx,
            provider_connection_id=row.provider_connection_id,
            provider_model_id=row.provider_model_id,
            worker_id=row.worker_id,
        )
        for row in rows
    ]


async def _trial_projections_for_batch(
    session: Any,
    batch_id: UUID,
) -> list[_BatchTrialProjection]:
    return await _trial_projections_for_batch_ids(session, [batch_id])


async def _usage_by_batch_ids(
    session: Any,
    batch_ids: Sequence[UUID],
) -> dict[UUID, dict[str, Any]]:
    if not batch_ids:
        return {}
    rows = (
        await session.execute(
            select(
                Trial.batch_id.label("batch_id"),
                func.coalesce(
                    func.sum(LlmCall.input_tokens),
                    0,
                ).label("total_prompt_tokens"),
                func.coalesce(
                    func.sum(LlmCall.output_tokens),
                    0,
                ).label("total_completion_tokens"),
                func.count(LlmCall.id).label("llm_calls_count"),
                func.coalesce(
                    func.sum(LlmCall.cost_usd),
                    0,
                ).label("total_cost_usd"),
                func.count(LlmCall.id)
                .filter(_priced_call_filter())
                .label("priced_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(LlmCall.rate_card_hash.like("facade:tokens-only%"))
                .label("token_only_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(_price_unknown_call_filter())
                .label("price_unknown_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(LlmCall.rate_card_hash == "failed-upstream")
                .label("failed_upstream_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_SOURCE_KEY, "operator-supplied"))
                .label("cost_source_operator_supplied_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_SOURCE_KEY, "rate-card"))
                .label("cost_source_rate_card_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_SOURCE_KEY, "tokens-only"))
                .label("cost_source_tokens_only_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_SOURCE_KEY, "unpriced"))
                .label("cost_source_unpriced_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_CONFIDENCE_KEY, "configured"))
                .label("cost_confidence_configured_count"),
                func.count(LlmCall.id)
                .filter(
                    _cost_meta_filter(COST_META_CONFIDENCE_KEY, "not_applicable"),
                )
                .label("cost_confidence_not_applicable_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_CONFIDENCE_KEY, "unavailable"))
                .label("cost_confidence_unavailable_count"),
                func.count(LlmCall.id)
                .filter(usage_status_filter("partial"))
                .label("partial_usage_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(usage_status_filter("missing"))
                .label("missing_usage_llm_calls_count"),
            )
            .join(LlmCall, LlmCall.trial_id == Trial.id)
            .where(Trial.batch_id.in_(batch_ids))
            .group_by(Trial.batch_id),
        )
    ).all()
    return {
        row.batch_id: summarize_usage_counts(
            llm_calls_count=int(row.llm_calls_count or 0),
            total_prompt_tokens=int(row.total_prompt_tokens or 0),
            total_completion_tokens=int(row.total_completion_tokens or 0),
            total_cost_usd=row.total_cost_usd,
            priced_llm_calls_count=int(row.priced_llm_calls_count or 0),
            token_only_llm_calls_count=int(
                row.token_only_llm_calls_count or 0,
            ),
            price_unknown_llm_calls_count=int(
                row.price_unknown_llm_calls_count or 0,
            ),
            failed_upstream_llm_calls_count=int(
                row.failed_upstream_llm_calls_count or 0,
            ),
            partial_usage_llm_calls_count=int(
                row.partial_usage_llm_calls_count or 0,
            ),
            missing_usage_llm_calls_count=int(
                row.missing_usage_llm_calls_count or 0,
            ),
            cost_source_counts=_cost_source_counts(row),
            cost_confidence_counts=_cost_confidence_counts(row),
        )
        for row in rows
    }


def _empty_trial_summary() -> dict[str, int]:
    return {
        k: 0
        for k in (
            "queued",
            "claimed",
            "running",
            "succeeded",
            "failed",
            "cancelled",
        )
    }


def _summary_from_trials(trials: Sequence[Any]) -> dict[str, int]:
    summary = _empty_trial_summary()
    for trial in trials:
        state = str(trial.state)
        summary[state] = summary.get(state, 0) + 1
    return summary


def _rollup_from_trials(trials: Sequence[Any]) -> float | None:
    reward_sum = 0.0
    reward_n = 0
    for trial in trials:
        if str(trial.state) not in {"succeeded", "failed"}:
            continue
        rew = _rollup_from_result(trial.result)
        if rew is not None:
            reward_sum += rew
            reward_n += 1
    return (reward_sum / reward_n) if reward_n > 0 else None


async def _benchmark_summary_from_trials(
    session: Any,
    trials: Sequence[Any],
) -> list[dict[str, Any]]:
    task_ids = sorted({trial.task_id for trial in trials})
    if not task_ids:
        return []

    rows = (
        await session.execute(
            select(
                Task.id,
                Task.benchmark_id,
                Benchmark.display_name,
            )
            .outerjoin(Benchmark, Benchmark.id == Task.benchmark_id)
            .where(Task.id.in_(task_ids)),
        )
    ).all()
    task_lookup = {
        str(row.id): {
            "benchmark_id": row.benchmark_id,
            "display_name": row.display_name,
        }
        for row in rows
    }

    grouped: dict[str, list[Any]] = defaultdict(list)
    labels: dict[str, tuple[str | None, str]] = {}
    for trial in trials:
        meta = task_lookup.get(str(trial.task_id), {})
        benchmark_id = meta.get("benchmark_id")
        group_key = str(benchmark_id) if benchmark_id else "__unbenchmarked__"
        display_name = (
            str(meta.get("display_name"))
            if meta.get("display_name")
            else (str(benchmark_id) if benchmark_id else "Unbenchmarked tasks")
        )
        grouped[group_key].append(trial)
        labels[group_key] = (
            str(benchmark_id) if benchmark_id else None,
            display_name,
        )

    summaries: list[dict[str, Any]] = []
    for group_key, group_trials in grouped.items():
        summary = _summary_from_trials(group_trials)
        benchmark_id, display_name = labels[group_key]
        summaries.append(
            {
                "benchmark_id": benchmark_id,
                "display_name": display_name,
                "metric_name": "score",
                "expected_trial_count": len(group_trials),
                "completed_trial_count": sum(
                    summary.get(state, 0) for state in ("succeeded", "failed", "cancelled")
                ),
                "platform_failed_count": summary.get("failed", 0),
                "trial_summary": summary,
                "aggregate_reward": _rollup_from_trials(group_trials),
            }
        )

    return sorted(
        summaries,
        key=lambda row: (
            str(row["display_name"]).casefold(),
            str(row["benchmark_id"] or ""),
        ),
    )


def _trial_key(trial: Any) -> tuple[str, int, int]:
    return (trial.task_id, int(trial.sample_idx), int(trial.combination_idx))


def _is_rerunnable_failure(trial: Any) -> bool:
    return is_auto_safe_rerun(trial)


def _effective_trials(
    original_trials: Sequence[Any],
    rerun_trials: Sequence[Any],
) -> list[Any]:
    original_by_key = {_trial_key(trial): trial for trial in original_trials}
    effective = dict(original_by_key)
    for trial in rerun_trials:
        if str(trial.state) != "succeeded":
            continue
        key = _trial_key(trial)
        original = original_by_key.get(key)
        if original is None or not is_replaceable_by_successful_supplemental(original):
            continue
        effective[key] = trial
    return list(effective.values())


def _result_status_from_trials(trials: Sequence[Any]) -> str | None:
    if not trials:
        return None
    states = [str(trial.state) for trial in trials]
    if any(state in {"queued", "claimed", "running"} for state in states):
        return None
    succeeded = sum(1 for state in states if state == "succeeded")
    failed = len(states) - succeeded
    if failed == 0 and succeeded > 0:
        return "succeeded"
    if succeeded == 0:
        return "all_failed"
    return "partial_failed"


@router.get("/batches/{batch_id}")
async def get_batch(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
    include_debug: Annotated[
        bool,
        Query(
            description=(
                "Include heavyweight debug_evidence and diagnosis payloads. "
                "Defaults to false so large batch detail reads stay bounded; "
                "use /debug or /diagnosis for targeted diagnostics."
            )
        ),
    ] = False,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "read:own")
    b = (
        await s.execute(
            select(Batch).where(Batch.id == batch_id),
        )
    ).scalar_one_or_none()
    if b is None:
        raise HTTPException(
            status_code=404,
            detail="batch not found",
        )
    require_team_or_admin(ctx, b.team_id)
    owner_team = (
        await s.execute(
            select(Team).where(Team.id == b.team_id),
        )
    ).scalar_one_or_none()
    submitted_by_user = None
    if b.submitted_by_user_id is not None:
        submitted_by_user = (
            await s.execute(
                select(User).where(User.id == b.submitted_by_user_id),
            )
        ).scalar_one_or_none()

    original_trials: list[Any] = list(await _trial_projections_for_batch(s, batch_id))
    summary = _summary_from_trials(original_trials)
    avg_reward = _rollup_from_trials(original_trials)
    usage = await _usage_totals_for_trials(s, original_trials)
    price_snapshots = await price_snapshots_for_trials(
        s,
        {trial.id for trial in original_trials},
    )
    llm_call_counts = await _llm_call_counts_for_trials(s, original_trials)
    llm_evidence = summarize_llm_evidence_for_trials(
        original_trials,
        llm_call_counts=llm_call_counts,
    )
    benchmark_summary = await _benchmark_summary_from_trials(
        s,
        original_trials,
    )
    combination_summary = await combination_summary_for_batch(
        s,
        combinations=b.combinations,
        trials=original_trials,
        expected_trial_count=b.expected_trial_count,
        required_worker_pool_count=len(b.required_worker_pools or []),
        fanout_errors=b.fanout_errors,
    )

    rerun_batches = (
        (
            await s.execute(
                select(Batch)
                .where(Batch.rerun_of_batch_id == batch_id)
                .order_by(Batch.created_at.asc(), Batch.id.asc()),
            )
        )
        .scalars()
        .all()
    )
    rerun_batch_ids = [child.id for child in rerun_batches]
    rerun_trials: list[Any] = []
    if rerun_batch_ids:
        rerun_trials = await _trial_projections_for_batch_ids(
            s,
            rerun_batch_ids,
        )
    rerun_plan = build_supplemental_rerun_plan(
        b,
        original_trials,
        supplemental_trials=rerun_trials,
    )
    effective_trials = _effective_trials(original_trials, rerun_trials)
    effective_summary = _summary_from_trials(effective_trials)
    effective_reward = _rollup_from_trials(effective_trials)
    effective_usage = await _usage_totals_for_trials(s, effective_trials)
    effective_combination_summary = await combination_summary_for_batch(
        s,
        combinations=b.combinations,
        trials=effective_trials,
        expected_trial_count=b.expected_trial_count,
        required_worker_pool_count=len(b.required_worker_pools or []),
        fanout_errors=b.fanout_errors,
    )
    effective_llm_call_counts = await _llm_call_counts_for_trials(
        s,
        effective_trials,
    )
    effective_llm_evidence = summarize_llm_evidence_for_trials(
        effective_trials,
        llm_call_counts=effective_llm_call_counts,
    )
    effective_price_snapshots = await price_snapshots_for_trials(
        s,
        {trial.id for trial in effective_trials},
    )
    rerunnable_failed_count = sum(1 for trial in original_trials if _is_rerunnable_failure(trial))
    extra = {
        "rerun_batches": [
            {
                "id": str(child.id),
                "name": child.name,
                "state": child.state,
                "result_status": child.result_status,
                "expected_trial_count": child.expected_trial_count,
                "created_at": child.created_at.isoformat(),
                "finished_at": (child.finished_at.isoformat() if child.finished_at else None),
            }
            for child in rerun_batches
        ],
        "rerunnable_failed_count": rerunnable_failed_count,
        "rerun_plan": rerun_plan,
        "final_trial_selection": rerun_plan["final_trial_selection"],
        "effective_trial_summary": effective_summary,
        "effective_result_status": _result_status_from_trials(effective_trials),
        "effective_aggregate_reward": effective_reward,
        "effective_total_prompt_tokens": effective_usage["total_prompt_tokens"],
        "effective_total_completion_tokens": effective_usage["total_completion_tokens"],
        "effective_total_tokens": effective_usage["total_tokens"],
        "effective_llm_calls_count": effective_usage["llm_calls_count"],
        "effective_estimated_cost_usd": effective_usage["estimated_cost_usd"],
        "effective_cost_currency": effective_usage["cost_currency"],
        "effective_cost_status": effective_usage["cost_status"],
        "effective_pricing_modes": effective_usage["pricing_modes"],
        "effective_priced_llm_calls_count": effective_usage["priced_llm_calls_count"],
        "effective_token_only_llm_calls_count": effective_usage["token_only_llm_calls_count"],
        "effective_price_unknown_llm_calls_count": effective_usage["price_unknown_llm_calls_count"],
        "effective_failed_upstream_llm_calls_count": effective_usage[
            "failed_upstream_llm_calls_count"
        ],
        "effective_partial_usage_llm_calls_count": effective_usage["partial_usage_llm_calls_count"],
        "effective_missing_usage_llm_calls_count": effective_usage["missing_usage_llm_calls_count"],
        "effective_usage_reporting_status": effective_usage["usage_reporting_status"],
        "effective_usage_estimate_confidence": effective_usage["usage_estimate_confidence"],
        "no_call_trial_count": llm_evidence["no_call_trial_count"],
        "no_call_reason_counts": llm_evidence["no_call_reason_counts"],
        "llm_evidence_status": llm_evidence["llm_evidence_status"],
        "model_backed_terminal_trial_count": llm_evidence["model_backed_terminal_trial_count"],
        "effective_no_call_trial_count": effective_llm_evidence["no_call_trial_count"],
        "effective_no_call_reason_counts": effective_llm_evidence["no_call_reason_counts"],
        "effective_llm_evidence_status": effective_llm_evidence["llm_evidence_status"],
        "effective_model_backed_terminal_trial_count": effective_llm_evidence[
            "model_backed_terminal_trial_count"
        ],
        "price_snapshots": price_snapshots,
        "effective_price_snapshots": effective_price_snapshots,
        "benchmark_summary": benchmark_summary,
        "combination_summary": combination_summary,
        "effective_combination_summary": effective_combination_summary,
    }
    if include_debug:
        llm_calls = await _llm_calls_for_trials(s, original_trials)
        worker_pool_names = await _worker_pool_names_for_trials(s, original_trials)
        stale_running_decisions = await batch_stale_running_decisions(
            s,
            original_trials,
            llm_calls=llm_calls,
            settings=request.app.state.settings,
        )
        debug_evidence = build_batch_debug_evidence(
            b,
            trials=original_trials,
            llm_calls=llm_calls,
            worker_pool_names_by_id=worker_pool_names,
            stale_running_decisions_by_trial_id=stale_running_decisions,
        )
        extra["debug_evidence"] = debug_evidence
        extra["diagnosis"] = build_batch_diagnosis(
            debug_evidence,
            trial_failures=trial_failure_records(original_trials),
        )

    return _serialize(
        b,
        summary=summary,
        aggregate_reward=avg_reward,
        usage=usage,
        owner_team=owner_team,
        submitted_by_user=submitted_by_user,
        extra=extra,
    )


@router.get("/batches/{batch_id}/debug")
async def get_batch_debug(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "read:own")
    b = (
        await s.execute(
            select(Batch).where(Batch.id == batch_id),
        )
    ).scalar_one_or_none()
    if b is None:
        raise HTTPException(
            status_code=404,
            detail="batch not found",
        )
    require_team_or_admin(ctx, b.team_id)
    trials = await _trial_projections_for_batch(s, batch_id)
    llm_calls = await _llm_calls_for_trials(s, trials)
    worker_pool_names = await _worker_pool_names_for_trials(s, trials)
    stale_running_decisions = await batch_stale_running_decisions(
        s,
        trials,
        llm_calls=llm_calls,
        settings=request.app.state.settings,
    )
    return build_batch_debug_evidence(
        b,
        trials=trials,
        llm_calls=llm_calls,
        worker_pool_names_by_id=worker_pool_names,
        stale_running_decisions_by_trial_id=stale_running_decisions,
    )


@router.get("/batches/{batch_id}/diagnosis")
async def get_batch_diagnosis(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "read:own")
    b = (
        await s.execute(
            select(Batch).where(Batch.id == batch_id),
        )
    ).scalar_one_or_none()
    if b is None:
        raise HTTPException(
            status_code=404,
            detail="batch not found",
        )
    require_team_or_admin(ctx, b.team_id)
    trials = await _trial_projections_for_batch(s, batch_id)
    llm_calls = await _llm_calls_for_trials(s, trials)
    worker_pool_names = await _worker_pool_names_for_trials(s, trials)
    stale_running_decisions = await batch_stale_running_decisions(
        s,
        trials,
        llm_calls=llm_calls,
        settings=request.app.state.settings,
    )
    debug_evidence = build_batch_debug_evidence(
        b,
        trials=trials,
        llm_calls=llm_calls,
        worker_pool_names_by_id=worker_pool_names,
        stale_running_decisions_by_trial_id=stale_running_decisions,
    )
    return build_batch_diagnosis(
        debug_evidence,
        trial_failures=trial_failure_records(trials),
    )


@router.get("/batches/{batch_id}/rerun-plan")
async def get_batch_rerun_plan(
    sc: SessionAndCtx,
    batch_id: UUID,
    task_id: Annotated[
        list[str] | None,
        Query(
            description=(
                "Optional task id filter. Repeat task_id to generate a plan "
                "for an explicit supplemental task list."
            )
        ),
    ] = None,
    include_operator_approval: Annotated[
        bool,
        Query(
            description=(
                "Include operator-approval rows in supplemental_task_ids. "
                "Defaults to false so only auto-safe rows are emitted."
            )
        ),
    ] = False,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "read:own")
    b = (
        await s.execute(
            select(Batch).where(Batch.id == batch_id),
        )
    ).scalar_one_or_none()
    if b is None:
        raise HTTPException(
            status_code=404,
            detail="batch not found",
        )
    require_team_or_admin(ctx, b.team_id)
    original_trials = await _trial_projections_for_batch(s, batch_id)
    child_batch_ids = (
        (
            await s.execute(
                select(Batch.id).where(Batch.rerun_of_batch_id == batch_id),
            )
        )
        .scalars()
        .all()
    )
    supplemental_trials: list[Any] = []
    if child_batch_ids:
        supplemental_trials = await _trial_projections_for_batch_ids(
            s,
            child_batch_ids,
        )
    return build_supplemental_rerun_plan(
        b,
        original_trials,
        task_ids=task_id,
        supplemental_trials=supplemental_trials,
        include_operator_approval=include_operator_approval,
    )


@router.post("/batches/{batch_id}/rerun-failed", status_code=201)
async def rerun_failed_batch(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
    payload: _RerunFailedBatch | None = None,
) -> dict[str, Any]:
    s, ctx = sc
    try:
        require_scope(ctx, "submit")
        require_submitting_user(ctx)
    except HTTPException:
        SUBMISSION_REJECTS_TOTAL.labels(reason="permission").inc()
        raise
    b = (
        await s.execute(
            select(Batch).where(Batch.id == batch_id),
        )
    ).scalar_one_or_none()
    if b is None:
        _reject_submission(
            reason="invalid_input",
            status_code=404,
            detail="batch not found",
        )
    require_team_or_admin(ctx, b.team_id)
    await _reject_if_team_paused(s, b.team_id)
    _reject_if_k8s_worker_unavailable(request, b.required_worker_pools or [])

    active_backends = await get_active_backends(s)
    if b.backend not in active_backends:
        available_str = (
            ", ".join(sorted(active_backends)) if active_backends else "(none -- no active workers)"
        )
        _reject_submission(
            reason="no_workers",
            status_code=400,
            detail=(
                f"no active worker advertises backend {b.backend!r}. "
                f"Currently available: {available_str}."
            ),
        )

    original_trials = await _trial_projections_for_batch(s, batch_id)
    child_batch_ids = (
        (
            await s.execute(
                select(Batch.id).where(Batch.rerun_of_batch_id == batch_id),
            )
        )
        .scalars()
        .all()
    )
    supplemental_trials: list[Any] = []
    if child_batch_ids:
        supplemental_trials = await _trial_projections_for_batch_ids(
            s,
            child_batch_ids,
        )
    request_payload = payload or _RerunFailedBatch()
    plan = build_supplemental_rerun_plan(
        b,
        original_trials,
        task_ids=(request_payload.task_ids or None),
        supplemental_trials=supplemental_trials,
        include_operator_approval=request_payload.include_operator_approval,
    )
    selected_targets = list(plan["auto_safe"])
    if request_payload.include_operator_approval:
        selected_targets.extend(plan["operator_approval"])
    if not selected_targets:
        _reject_submission(
            reason="invalid_input",
            status_code=400,
            detail="batch has no rerunnable failed trials",
        )

    targets = [
        {
            "task_id": target["task_id"],
            "sample_idx": int(target["sample_idx"]),
            "combination_idx": int(target["combination_idx"]),
            "original_trial_id": target["original_trial_id"],
            "failure_reason": target["failure_reason"],
        }
        for target in selected_targets
    ]
    task_ids = sorted({target["task_id"] for target in selected_targets})
    rerun_task_result = await resolve_task_filter_with_diagnostics(
        s,
        {"subset_kind": "explicit", "task_ids": task_ids},
        team_id=b.team_id,
        require_runnable=True,
    )
    if set(rerun_task_result.task_ids) != set(task_ids):
        _reject_submission(
            reason="invalid_input",
            status_code=400,
            detail="rerun target tasks are missing or no longer runnable",
        )
    token_prefix = ctx.token_hash.hex()[:8] if ctx.token_hash else "00000000"
    rerun = Batch(
        team_id=b.team_id,
        name=f"{b.name} failed-case rerun",
        description=(f"Reruns {len(targets)} transient failed case(s) from batch {b.id}."),
        task_filter={"subset_kind": "explicit", "task_ids": task_ids},
        resolved_task_ids=list(rerun_task_result.task_ids),
        trial_config=dict(b.trial_config),
        state="submitted",
        created_by_token_prefix=token_prefix,
        submitted_by_user_id=ctx.user_id,
        usage_attributed_user_id=ctx.user_id,
        usage_attributed_actor=(f"user:{ctx.user_id}" if ctx.user_id is not None else None),
        expected_trial_count=len(targets),
        n_per_task=1,
        backend=b.backend,
        combinations=list(b.combinations or []),
        provider_connection_id=b.provider_connection_id,
        provider_model_id=b.provider_model_id,
        rerun_of_batch_id=b.id,
        rerun_targets=targets,
        source_provenance=[
            {
                "kind": "supplemental_rerun",
                "source_batch_id": str(b.id),
            },
            *rerun_task_result.benchmark_selection_provenance,
        ],
    )
    s.add(rerun)
    await s.commit()
    await s.refresh(rerun)
    return {
        "batch_id": str(rerun.id),
        "rerun_of_batch_id": str(b.id),
        "expected_trial_count": rerun.expected_trial_count,
        "state": rerun.state,
        "created_at": rerun.created_at.isoformat(),
        "rerun_target_count": len(targets),
        "rerun_plan": plan,
    }


@router.post("/batches/{batch_id}/cancel")
async def cancel_batch(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "submit")
    b = (
        await s.execute(
            select(Batch).where(Batch.id == batch_id),
        )
    ).scalar_one_or_none()
    if b is None:
        raise HTTPException(
            status_code=404,
            detail="batch not found",
        )
    require_team_or_admin(ctx, b.team_id)
    now = datetime.now(UTC)
    await s.execute(
        update(Batch).where(Batch.id == batch_id).values(state="cancelled", finished_at=now),
    )
    # Cascade-cancel still-active trials in this batch. We do
    # NOT cancel queued trials whose worker may already be partway
    # through claim; the CP's existing cancel endpoint (Plan 5)
    # handles graceful interruption when called per-trial. Here we
    # just transition the rows to `cancelled` so the SPA stops
    # showing them as in-flight.
    await s.execute(
        update(Trial)
        .where(
            and_(
                Trial.batch_id == batch_id,
                Trial.state.in_(["queued", "claimed", "running"]),
            ),
        )
        .values(
            state="cancelled",
            cancellation_requested_at=now,
            finished_at=now,
        ),
    )
    await s.commit()
    return {"batch_id": str(batch_id), "state": "cancelled"}
