"""Org-wide Run Library for completed shared work (#336)."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Annotated, Any, cast
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import and_, func, or_, select

from loom.db.schema import Artifact, ArtifactLineageEdge, Batch, LlmCall, Team, Trial
from loom.security.redaction import redact_mapping, redact_text
from loom_service.auth_guards import (
    is_admin,
    require_scope,
    require_submitting_user,
    require_team_or_admin,
)
from loom_service.debug_evidence import build_batch_debug_evidence
from loom_service.dependencies import SessionAndCtx
from loom_service.diagnosis import build_batch_diagnosis, trial_failure_records
from loom_service.monitor_filters import apply_batch_monitor_filters
from loom_service.pagination import Cursor, decode_cursor, encode_cursor
from loom_service.provider_connection_lookup import validate_provider_connection
from loom_service.routes.object_downloads import stream_object_response

router = APIRouter()

_ORG_VISIBLE_BATCH_STATES = frozenset({"finished", "cancelled"})
_ORG_VISIBLE_TRIAL_STATES = frozenset({"succeeded", "failed", "cancelled"})
_ARTIFACT_GROUPS = (
    "reports",
    "trajectories",
    "reusable_outputs",
    "logs_diagnostics",
    "raw_diagnostics",
)
_ARTIFACT_TYPE_GROUPS = {
    "atif_projection": "reports",
    "metric_table": "reports",
    "trajectory": "trajectories",
    "trajectory_bundle": "trajectories",
    "completion_set": "reusable_outputs",
    "task_set": "reusable_outputs",
    "task_split": "reusable_outputs",
    "skill_markdown": "reusable_outputs",
    "workflow_spec": "reusable_outputs",
    "verifier_replay": "logs_diagnostics",
    "debug_bundle": "raw_diagnostics",
    "evidence_bundle": "reusable_outputs",
    "training_data_export": "reusable_outputs",
}
_DOWNLOAD_REDACTION_STATES = frozenset({"not_required", "redacted"})
_TRIAL_SUMMARY_STATES = (
    "queued",
    "claimed",
    "running",
    "succeeded",
    "failed",
    "cancelled",
)


class _CloneConfigRequest(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    description: str | None = None
    provider_connection_id: UUID | None = None
    provider_model_id: str | None = None


class _ReuseArtifactRequest(BaseModel):
    key: str = Field(min_length=1)
    name: str = Field(min_length=1, max_length=200)
    description: str | None = None
    provider_connection_id: UUID | None = None
    provider_model_id: str | None = None


class _VisibilityPatch(BaseModel):
    visibility: str
    share_status: str


def _is_owner_or_admin(ctx: Any, team_id: UUID) -> bool:
    return is_admin(ctx) or ctx.team_id == team_id


def _batch_is_org_visible(batch: Batch) -> bool:
    return (
        batch.visibility == "org"
        and batch.share_status == "shared"
        and batch.state in _ORG_VISIBLE_BATCH_STATES
    )


def _trial_is_org_visible(
    trial: Trial,
    batch: Batch | None = None,
) -> bool:
    if batch is not None:
        return _batch_is_org_visible(batch) and trial.state in _ORG_VISIBLE_TRIAL_STATES

    trial_shared = (
        trial.visibility == "org"
        and trial.share_status == "shared"
        and trial.state in _ORG_VISIBLE_TRIAL_STATES
    )
    return trial_shared


def _can_read_batch(ctx: Any, batch: Batch) -> bool:
    return _is_owner_or_admin(ctx, batch.team_id) or _batch_is_org_visible(batch)


def _can_read_trial(
    ctx: Any,
    trial: Trial,
    batch: Batch | None = None,
) -> bool:
    return _is_owner_or_admin(ctx, trial.team_id) or _trial_is_org_visible(
        trial,
        batch,
    )


def _artifact_role(item: dict[str, Any]) -> str:
    raw = item.get("role") or item.get("artifact_role")
    if isinstance(raw, str):
        normalized = raw.strip().lower().replace("-", "_")
        aliases = {
            "report": "reports",
            "reports": "reports",
            "atif": "reports",
            "trajectory": "trajectories",
            "trajectories": "trajectories",
            "output": "reusable_outputs",
            "outputs": "reusable_outputs",
            "reusable_output": "reusable_outputs",
            "reusable_outputs": "reusable_outputs",
            "log": "logs_diagnostics",
            "logs": "logs_diagnostics",
            "diagnostic": "logs_diagnostics",
            "diagnostics": "logs_diagnostics",
            "logs_diagnostics": "logs_diagnostics",
            "raw": "raw_diagnostics",
            "raw_diagnostic": "raw_diagnostics",
            "raw_diagnostics": "raw_diagnostics",
            "internal_diagnostics": "raw_diagnostics",
        }
        role = aliases.get(normalized)
        if role is not None:
            return role

    key = item.get("key")
    key_text = key.lower() if isinstance(key, str) else ""
    if key_text.endswith("atif.json") or "report" in key_text:
        return "reports"
    if key_text.endswith("events.jsonl") or "trajectory" in key_text:
        return "trajectories"
    if "debug" in key_text or "raw" in key_text or "internal" in key_text:
        return "raw_diagnostics"
    if "log" in key_text or "diagnostic" in key_text:
        return "logs_diagnostics"
    return "reusable_outputs"


def _artifact_bucket(item: dict[str, Any], default_bucket: str) -> str:
    bucket = item.get("bucket")
    if not isinstance(bucket, str) or not bucket:
        return default_bucket
    return bucket


def _artifact_filename(key: str) -> str:
    name = key.rstrip("/").rsplit("/", 1)[-1]
    return name or "artifact"


def _artifact_type_label(artifact_type: str) -> str:
    return artifact_type.replace("_", " ").capitalize()


def _artifact_group_for_type(artifact_type: str) -> str:
    return _ARTIFACT_TYPE_GROUPS.get(artifact_type, "reusable_outputs")


def _artifact_storage_key(artifact: Artifact) -> str | None:
    storage = artifact.storage if isinstance(artifact.storage, dict) else {}
    key = storage.get("key")
    return key if isinstance(key, str) and key else None


def _artifact_storage_bucket(
    artifact: Artifact,
    default_bucket: str,
) -> str:
    storage = artifact.storage if isinstance(artifact.storage, dict) else {}
    bucket = storage.get("bucket")
    return bucket if isinstance(bucket, str) and bucket else default_bucket


def _artifact_storage_size(artifact: Artifact) -> int:
    storage = artifact.storage if isinstance(artifact.storage, dict) else {}
    raw = storage.get("size_bytes") or storage.get("size")
    try:
        return max(int(raw), 0) if raw is not None else 0
    except (TypeError, ValueError):
        return 0


def _artifact_source(artifact: Artifact) -> dict[str, str | None]:
    created_by = artifact.created_by if isinstance(artifact.created_by, dict) else {}
    kind = created_by.get("kind")
    if not isinstance(kind, str) or not kind:
        kind = "trial" if artifact.trial_id is not None else "batch"
    return {
        "kind": kind,
        "batch_id": str(artifact.batch_id) if artifact.batch_id else None,
        "trial_id": str(artifact.trial_id) if artifact.trial_id else None,
    }


def _safe_artifact_blocked_reason(artifact: Artifact) -> str:
    if artifact.blocked_reason:
        return redact_text(artifact.blocked_reason)
    if artifact.safety_state not in {"safe", "unknown"}:
        return "blocked by artifact safety policy"
    if artifact.redaction_state not in _DOWNLOAD_REDACTION_STATES:
        return "blocked by artifact redaction policy"
    return "blocked by artifact sharing policy"


def _artifact_parent_visible(
    artifact: Artifact,
    *,
    batch: Batch | None = None,
    trial: Trial | None = None,
) -> bool:
    if batch is not None:
        return _batch_is_org_visible(batch)
    if trial is not None:
        return _trial_is_org_visible(trial)
    return artifact.visibility == "org"


def _artifact_content_allowed(
    artifact: Artifact,
    *,
    batch: Batch | None = None,
    trial: Trial | None = None,
) -> bool:
    return (
        artifact.visibility == "org"
        and artifact.share_status == "shared"
        and artifact.safety_state == "safe"
        and artifact.redaction_state in _DOWNLOAD_REDACTION_STATES
        and _artifact_parent_visible(artifact, batch=batch, trial=trial)
    )


def _artifact_metadata_visible(
    ctx: Any,
    artifact: Artifact,
    *,
    batch: Batch | None = None,
    trial: Trial | None = None,
) -> bool:
    return _is_owner_or_admin(ctx, artifact.team_id) or (
        artifact.visibility == "org"
        and artifact.share_status == "shared"
        and _artifact_parent_visible(artifact, batch=batch, trial=trial)
    )


def _artifact_items(
    trajectory_index: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not trajectory_index:
        return []
    artifacts = trajectory_index.get("artifacts")
    if not isinstance(artifacts, list):
        return []
    return [item for item in artifacts if isinstance(item, dict)]


def _find_artifact(
    trajectory_index: dict[str, Any] | None,
    key: str,
) -> dict[str, Any] | None:
    for item in _artifact_items(trajectory_index):
        if item.get("key") == key:
            return item
    return None


def _share_status(item: dict[str, Any]) -> str:
    status = item.get("share_status")
    if status in {"pending_scan", "shared", "blocked"}:
        return str(status)
    return "pending_scan"


def _blocked_reason(item: dict[str, Any]) -> str:
    reason = item.get("blocked_reason")
    if isinstance(reason, str) and reason.strip():
        return redact_text(reason)
    return "blocked by artifact sharing policy"


async def _typed_artifacts_for_trials(
    session: Any,
    trials: Sequence[Trial],
) -> dict[UUID, list[Artifact]]:
    trial_ids = [trial.id for trial in trials]
    if not trial_ids:
        return {}
    rows = list(
        (
            await session.execute(
                select(Artifact)
                .where(Artifact.trial_id.in_(trial_ids))
                .order_by(Artifact.created_at.asc(), Artifact.id.asc()),
            )
        )
        .scalars()
        .all()
    )
    out: dict[UUID, list[Artifact]] = {trial.id: [] for trial in trials}
    for artifact in rows:
        if artifact.trial_id is not None:
            out.setdefault(artifact.trial_id, []).append(artifact)
    return out


async def _parents_for_artifacts(
    session: Any,
    artifacts: Sequence[Artifact],
) -> dict[UUID, list[dict[str, Any]]]:
    artifact_ids = [artifact.id for artifact in artifacts]
    if not artifact_ids:
        return {}
    rows = list(
        (
            await session.execute(
                select(ArtifactLineageEdge)
                .where(ArtifactLineageEdge.child_artifact_id.in_(artifact_ids))
                .order_by(
                    ArtifactLineageEdge.created_at.asc(),
                    ArtifactLineageEdge.id.asc(),
                ),
            )
        )
        .scalars()
        .all()
    )
    out: dict[UUID, list[dict[str, Any]]] = {artifact.id: [] for artifact in artifacts}
    for edge in rows:
        out.setdefault(edge.child_artifact_id, []).append(
            {
                "artifact_id": (
                    str(edge.parent_artifact_id) if edge.parent_artifact_id is not None else None
                ),
                "relation": edge.relation,
                "metadata": edge.edge_metadata,
            }
        )
    return out


def _serialize_typed_artifact(
    request: Request | None,
    artifact: Artifact,
    owner_team: Team,
    *,
    ctx: Any,
    batch: Batch | None = None,
    trial: Trial | None = None,
    parents: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    key = _artifact_storage_key(artifact)
    if key is None:
        return None
    role = _artifact_group_for_type(artifact.artifact_type)
    owner_or_admin = _is_owner_or_admin(ctx, artifact.team_id)
    content_allowed = _artifact_content_allowed(
        artifact,
        batch=batch,
        trial=trial,
    )
    full_metadata = owner_or_admin or content_allowed
    can_download = (
        artifact.trial_id is not None
        and request is not None
        and (owner_or_admin or content_allowed)
    )
    entry: dict[str, Any] = {
        "id": str(artifact.id),
        "trial_id": str(artifact.trial_id) if artifact.trial_id else None,
        "key": key if full_metadata else f"redacted-artifact:{artifact.id}",
        "size": _artifact_storage_size(artifact) if full_metadata else 0,
        "role": role,
        "artifact_type": artifact.artifact_type,
        "artifact_type_label": _artifact_type_label(artifact.artifact_type),
        "artifact_schema_version": artifact.artifact_schema_version,
        "owner_team": {"id": str(owner_team.id), "name": owner_team.name},
        "source": _artifact_source(artifact),
        "share_status": artifact.share_status,
        "safety_state": artifact.safety_state,
        "redaction_state": artifact.redaction_state,
        "blocked_reason": (
            _safe_artifact_blocked_reason(artifact)
            if artifact.safety_state != "safe"
            or artifact.redaction_state not in _DOWNLOAD_REDACTION_STATES
            or artifact.share_status != "shared"
            else None
        ),
        "content_hash": artifact.content_hash if full_metadata else None,
        "storage": artifact.storage if full_metadata else None,
        "provenance": artifact.provenance if full_metadata else {},
        "metadata": artifact.artifact_metadata if full_metadata else {},
        "parents": (parents or []) if full_metadata else [],
    }
    if can_download and request is not None:
        entry["download_url"] = str(
            request.url_for(
                "download_run_library_artifact",
                trial_id=str(artifact.trial_id),
            ).include_query_params(key=key),
        )
    else:
        entry["download_url"] = None
    return entry


def _legacy_artifact_type(item: dict[str, Any]) -> str:
    raw = item.get("artifact_type")
    if isinstance(raw, str) and raw:
        return raw
    role = _artifact_role(item)
    if role == "reports":
        return "atif_projection"
    if role == "trajectories":
        return "trajectory"
    if role in {"logs_diagnostics", "raw_diagnostics"}:
        return "debug_bundle"
    return "evidence_bundle"


def _legacy_safety_state(item: dict[str, Any]) -> str:
    status = _share_status(item)
    if status == "shared":
        return "safe"
    if status == "blocked":
        return "unsafe"
    return "unknown"


def _legacy_redaction_state(item: dict[str, Any]) -> str:
    status = _share_status(item)
    if status == "shared":
        return "not_required"
    if status == "blocked":
        return "blocked"
    return "pending"


def _serialize_legacy_artifact(
    request: Request,
    trial: Trial,
    owner_team: Team,
    item: dict[str, Any],
) -> dict[str, Any] | None:
    key = item.get("key")
    if not isinstance(key, str) or not key:
        return None
    size = item.get("size")
    try:
        size_int = int(size) if size is not None else 0
    except (TypeError, ValueError):
        size_int = 0
    role = _artifact_role(item)
    status = _share_status(item)
    artifact_type = _legacy_artifact_type(item)
    return {
        "trial_id": str(trial.id),
        "key": key,
        "size": max(size_int, 0),
        "role": role,
        "artifact_type": artifact_type,
        "artifact_type_label": _artifact_type_label(artifact_type),
        "artifact_schema_version": "1.0",
        "owner_team": {"id": str(owner_team.id), "name": owner_team.name},
        "source": {
            "kind": "trial",
            "batch_id": str(trial.batch_id) if trial.batch_id else None,
            "trial_id": str(trial.id),
        },
        "share_status": status,
        "safety_state": _legacy_safety_state(item),
        "redaction_state": _legacy_redaction_state(item),
        "blocked_reason": (_blocked_reason(item) if status == "blocked" else None),
        "content_hash": item.get("content_hash") or "pending:legacy-unhashed",
        "storage": {
            "backend": "object_store",
            "bucket": _artifact_bucket(item, "artifacts"),
            "key": key,
            "media_type": item.get("media_type") or "application/octet-stream",
            "size_bytes": max(size_int, 0),
        },
        "provenance": {
            "batch_id": str(trial.batch_id) if trial.batch_id else None,
            "trial_id": str(trial.id),
            "source_trial_ids": [str(trial.id)],
            "relation": "produced_from",
        },
        "metadata": {
            "legacy_role": item.get("role") or item.get("artifact_role"),
            "step_name": item.get("step_name"),
        },
        "parents": [],
        "download_url": str(
            request.url_for(
                "download_run_library_artifact",
                trial_id=str(trial.id),
            ).include_query_params(key=key),
        ),
    }


def _artifact_summary(
    trials: Sequence[Trial],
    typed_by_trial: dict[UUID, list[Artifact]],
) -> dict[str, int]:
    summary = _empty_artifact_summary()
    for trial in trials:
        typed = typed_by_trial.get(trial.id) or []
        if typed:
            for artifact in typed:
                summary[_artifact_group_for_type(artifact.artifact_type)] += 1
            continue
        for item in _artifact_items(trial.trajectory_index):
            summary[_artifact_role(item)] += 1
    return summary


def _empty_artifact_summary() -> dict[str, int]:
    return {role: 0 for role in _ARTIFACT_GROUPS}


def _artifact_inventory(
    request: Request,
    ctx: Any,
    trials: Sequence[Trial],
    batch: Batch,
    owner_team: Team,
    typed_by_trial: dict[UUID, list[Artifact]],
    parents_by_artifact: dict[UUID, list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {role: [] for role in _ARTIFACT_GROUPS}
    for trial in trials:
        typed = typed_by_trial.get(trial.id) or []
        if typed:
            for artifact in typed:
                entry = _serialize_typed_artifact(
                    request,
                    artifact,
                    owner_team,
                    ctx=ctx,
                    batch=batch,
                    trial=trial,
                    parents=parents_by_artifact.get(artifact.id, []),
                )
                if entry is not None:
                    grouped[entry["role"]].append(entry)
            continue
        for item in _artifact_items(trial.trajectory_index):
            entry = _serialize_legacy_artifact(request, trial, owner_team, item)
            if entry is not None:
                grouped[entry["role"]].append(entry)
    return grouped


def _trial_summary(trials: Sequence[Trial]) -> dict[str, int]:
    summary = _empty_trial_summary()
    for trial in trials:
        state = str(trial.state)
        summary[state] = summary.get(state, 0) + 1
    return summary


def _empty_trial_summary() -> dict[str, int]:
    return {state: 0 for state in _TRIAL_SUMMARY_STATES}


def _rollup_result(result: dict[str, Any] | None) -> tuple[float | None, float]:
    if not result:
        return None, 0.0
    reward = result.get("aggregate_reward")
    if reward is None:
        reward = result.get("reward")
    try:
        reward_f = float(reward) if reward is not None else None
    except (TypeError, ValueError):
        reward_f = None
    try:
        cost_f = float(result.get("cost_usd", 0) or 0)
    except (TypeError, ValueError):
        cost_f = 0.0
    return reward_f, cost_f


def _trial_rollup(trials: Sequence[Trial]) -> tuple[float | None, float]:
    reward_sum = 0.0
    reward_count = 0
    cost_total = 0.0
    for trial in trials:
        if trial.state not in _ORG_VISIBLE_TRIAL_STATES:
            continue
        reward, cost = _rollup_result(trial.result)
        cost_total += cost
        if reward is not None:
            reward_sum += reward
            reward_count += 1
    return (
        reward_sum / reward_count if reward_count else None,
        cost_total,
    )


def _artifact_filter_active(filters: dict[str, Any]) -> bool:
    return any(value is not None for value in filters.values())


def _typed_artifact_matches_filters(
    artifact: Artifact,
    filters: dict[str, Any],
) -> bool:
    artifact_type = filters.get("artifact_type")
    if artifact_type is not None and artifact_type not in {
        artifact.artifact_type,
        _artifact_group_for_type(artifact.artifact_type),
    }:
        return False
    owner_team_id = filters.get("owner_team_id")
    if owner_team_id is not None and artifact.team_id != owner_team_id:
        return False
    source_trial_id = filters.get("source_trial_id")
    provenance = artifact.provenance if isinstance(artifact.provenance, dict) else {}
    source_trial_ids = provenance.get("source_trial_ids")
    if source_trial_id is not None:
        if artifact.trial_id != source_trial_id and (
            not isinstance(source_trial_ids, list)
            or str(source_trial_id) not in {str(item) for item in source_trial_ids}
        ):
            return False
    source_batch_id = filters.get("source_batch_id")
    if source_batch_id is not None:
        if artifact.batch_id != source_batch_id and str(provenance.get("batch_id")) != str(
            source_batch_id
        ):
            return False
    safety_state = filters.get("safety_state")
    if safety_state is not None and artifact.safety_state != safety_state:
        return False
    provenance_relation = filters.get("provenance_relation")
    if provenance_relation is not None and provenance.get("relation") != provenance_relation:
        return False
    return True


def _legacy_artifact_matches_filters(
    item: dict[str, Any],
    trial: Trial,
    filters: dict[str, Any],
) -> bool:
    artifact_type = filters.get("artifact_type")
    legacy_type = _legacy_artifact_type(item)
    if artifact_type is not None and artifact_type not in {
        legacy_type,
        _artifact_role(item),
    }:
        return False
    owner_team_id = filters.get("owner_team_id")
    if owner_team_id is not None and trial.team_id != owner_team_id:
        return False
    source_trial_id = filters.get("source_trial_id")
    if source_trial_id is not None and trial.id != source_trial_id:
        return False
    source_batch_id = filters.get("source_batch_id")
    if source_batch_id is not None and trial.batch_id != source_batch_id:
        return False
    safety_state = filters.get("safety_state")
    if safety_state is not None and _legacy_safety_state(item) != safety_state:
        return False
    provenance_relation = filters.get("provenance_relation")
    if provenance_relation is not None and provenance_relation != "produced_from":
        return False
    return True


async def _batch_has_matching_artifact(
    session: Any,
    trials: Sequence[Trial],
    filters: dict[str, Any],
) -> bool:
    if not _artifact_filter_active(filters):
        return True
    typed_by_trial = await _typed_artifacts_for_trials(session, trials)
    for trial in trials:
        typed = typed_by_trial.get(trial.id) or []
        if typed:
            if any(_typed_artifact_matches_filters(artifact, filters) for artifact in typed):
                return True
            continue
        if any(
            _legacy_artifact_matches_filters(item, trial, filters)
            for item in _artifact_items(trial.trajectory_index)
        ):
            return True
    return False


async def _batch_trials(session: Any, batch_id: UUID) -> list[Trial]:
    return list(
        (
            await session.execute(
                select(Trial).where(Trial.batch_id == batch_id),
            )
        )
        .scalars()
        .all()
    )


async def _llm_calls_for_trials(
    session: Any,
    trials: Sequence[Trial],
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


def _serialize_batch_base(batch: Batch, owner_team: Team) -> dict[str, Any]:
    return {
        "id": str(batch.id),
        "team_id": str(batch.team_id),
        "owner_team": {
            "id": str(owner_team.id),
            "name": owner_team.name,
        },
        "name": batch.name,
        "description": batch.description,
        "task_filter": batch.task_filter,
        "trial_config": batch.trial_config,
        "backend": batch.backend,
        "combinations": batch.combinations,
        "provider_connection_id": (
            str(batch.provider_connection_id) if batch.provider_connection_id else None
        ),
        "provider_model_id": batch.provider_model_id,
        "state": batch.state,
        "result_status": batch.result_status,
        "visibility": batch.visibility,
        "share_status": batch.share_status,
        "source_provenance": batch.source_provenance,
        "expected_trial_count": batch.expected_trial_count,
        "created_by_token_prefix": batch.created_by_token_prefix,
        "created_at": batch.created_at.isoformat(),
        "finished_at": batch.finished_at.isoformat() if batch.finished_at else None,
    }


async def _serialize_batch(
    request: Request,
    session: Any,
    ctx: Any,
    batch: Batch,
    owner_team: Team,
    *,
    include_inventory: bool = False,
    include_debug: bool = False,
) -> dict[str, Any]:
    trials = await _batch_trials(session, batch.id)
    typed_by_trial = await _typed_artifacts_for_trials(session, trials)
    typed_artifacts = [artifact for artifacts in typed_by_trial.values() for artifact in artifacts]
    parents_by_artifact = await _parents_for_artifacts(session, typed_artifacts)
    reward, cost = _trial_rollup(trials)
    out = {
        **_serialize_batch_base(batch, owner_team),
        "trial_summary": _trial_summary(trials),
        "aggregate_reward": reward,
        "total_cost_usd": cost,
        "artifact_summary": _artifact_summary(trials, typed_by_trial),
    }
    if include_debug:
        llm_calls = await _llm_calls_for_trials(session, trials)
        debug_evidence = build_batch_debug_evidence(
            batch,
            trials=trials,
            llm_calls=llm_calls,
        )
        out["debug_evidence"] = debug_evidence
        out["diagnosis"] = build_batch_diagnosis(
            debug_evidence,
            trial_failures=trial_failure_records(trials),
        )
    if include_inventory:
        out["artifact_inventory"] = _artifact_inventory(
            request,
            ctx,
            trials,
            batch,
            owner_team,
            typed_by_trial,
            parents_by_artifact,
        )
    return out


async def _batch_list_trial_rollups(
    session: Any,
    batch_ids: Sequence[UUID],
) -> dict[UUID, tuple[dict[str, int], float | None, float]]:
    summaries = {batch_id: _empty_trial_summary() for batch_id in batch_ids}
    reward_sums = {batch_id: 0.0 for batch_id in batch_ids}
    reward_counts = {batch_id: 0 for batch_id in batch_ids}
    cost_totals = {batch_id: 0.0 for batch_id in batch_ids}
    if not batch_ids:
        return {}

    rows = (
        await session.execute(
            select(Trial.batch_id, Trial.state, Trial.result).where(
                Trial.batch_id.in_(batch_ids),
            ),
        )
    ).all()
    for batch_id, state, result in rows:
        if batch_id is None:
            continue
        summary = summaries.setdefault(batch_id, _empty_trial_summary())
        state_text = str(state)
        summary[state_text] = summary.get(state_text, 0) + 1
        if state_text not in _ORG_VISIBLE_TRIAL_STATES:
            continue
        reward, cost = _rollup_result(result)
        cost_totals[batch_id] = cost_totals.get(batch_id, 0.0) + cost
        if reward is not None:
            reward_sums[batch_id] = reward_sums.get(batch_id, 0.0) + reward
            reward_counts[batch_id] = reward_counts.get(batch_id, 0) + 1

    return {
        batch_id: (
            summaries.get(batch_id, _empty_trial_summary()),
            (
                reward_sums.get(batch_id, 0.0) / reward_counts[batch_id]
                if reward_counts.get(batch_id, 0)
                else None
            ),
            cost_totals.get(batch_id, 0.0),
        )
        for batch_id in batch_ids
    }


async def _batch_list_artifact_summaries(
    session: Any,
    batch_ids: Sequence[UUID],
) -> dict[UUID, dict[str, int]]:
    summaries = {batch_id: _empty_artifact_summary() for batch_id in batch_ids}
    if not batch_ids:
        return {}

    rows = (
        await session.execute(
            select(Artifact.batch_id, Artifact.artifact_type, func.count(Artifact.id))
            .where(Artifact.batch_id.in_(batch_ids))
            .group_by(Artifact.batch_id, Artifact.artifact_type),
        )
    ).all()
    for batch_id, artifact_type, count in rows:
        if batch_id is None:
            continue
        summary = summaries.setdefault(batch_id, _empty_artifact_summary())
        summary[_artifact_group_for_type(str(artifact_type))] += int(count)
    return summaries


def _serialize_batch_list_item(
    batch: Batch,
    owner_team: Team,
    trial_rollup: tuple[dict[str, int], float | None, float],
    artifact_summary: dict[str, int],
) -> dict[str, Any]:
    trial_summary, reward, cost = trial_rollup
    return {
        **_serialize_batch_base(batch, owner_team),
        "trial_summary": trial_summary,
        "aggregate_reward": reward,
        "total_cost_usd": cost,
        "artifact_summary": artifact_summary,
    }


async def _load_batch_with_team(
    session: Any,
    batch_id: UUID,
) -> tuple[Batch, Team]:
    row = (
        await session.execute(
            select(Batch, Team).join(Team, Team.id == Batch.team_id).where(Batch.id == batch_id),
        )
    ).one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="batch not found")
    batch, team = row
    return batch, team


async def _load_trial_with_batch(
    session: Any,
    trial_id: UUID,
) -> tuple[Trial, Batch | None]:
    trial = (
        await session.execute(
            select(Trial).where(Trial.id == trial_id),
        )
    ).scalar_one_or_none()
    if trial is None:
        raise HTTPException(status_code=404, detail="trial not found")
    batch: Batch | None = None
    if trial.batch_id is not None:
        batch = (
            await session.execute(
                select(Batch).where(Batch.id == trial.batch_id),
            )
        ).scalar_one_or_none()
    return trial, batch


async def _typed_artifact_for_trial_key(
    session: Any,
    trial_id: UUID,
    key: str,
) -> Artifact | None:
    rows = cast(
        list[Artifact],
        list(
            (
                await session.execute(
                    select(Artifact)
                    .where(Artifact.trial_id == trial_id)
                    .order_by(Artifact.created_at.asc(), Artifact.id.asc()),
                )
            )
            .scalars()
            .all()
        ),
    )
    for artifact in rows:
        if _artifact_storage_key(artifact) == key:
            return artifact
    return None


def _apply_read_filter(
    stmt: Any,
    *,
    ctx: Any,
    scope: str,
    team_id: UUID | None,
) -> Any:
    if team_id is not None:
        if _is_owner_or_admin(ctx, team_id):
            return stmt.where(Batch.team_id == team_id)
        return stmt.where(
            and_(
                Batch.team_id == team_id,
                Batch.visibility == "org",
                Batch.share_status == "shared",
                Batch.state.in_(sorted(_ORG_VISIBLE_BATCH_STATES)),
            ),
        )

    if scope == "all":
        if is_admin(ctx):
            return stmt
        return stmt.where(
            or_(
                Batch.team_id == ctx.team_id,
                and_(
                    Batch.visibility == "org",
                    Batch.share_status == "shared",
                    Batch.state.in_(sorted(_ORG_VISIBLE_BATCH_STATES)),
                ),
            ),
        )

    if ctx.team_id is None:
        return stmt
    return stmt.where(Batch.team_id == ctx.team_id)


@router.get("/run-library/batches")
async def list_run_library_batches(
    request: Request,
    sc: SessionAndCtx,
    scope: Annotated[str, Query(pattern="^(my|all)$")] = "my",
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
    state: Annotated[str | None, Query()] = None,
    visibility: Annotated[str | None, Query(pattern="^(team|org|private)$")] = None,
    artifact_type: Annotated[str | None, Query()] = None,
    owner_team_id: Annotated[UUID | None, Query()] = None,
    source_batch_id: Annotated[UUID | None, Query()] = None,
    source_trial_id: Annotated[UUID | None, Query()] = None,
    safety_state: Annotated[str | None, Query()] = None,
    provenance_relation: Annotated[str | None, Query()] = None,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(gt=0, le=200)] = 50,
) -> dict[str, Any]:
    session, ctx = sc
    require_scope(ctx, "read:own")

    stmt = (
        select(Batch, Team)
        .join(Team, Team.id == Batch.team_id)
        .order_by(Batch.created_at.desc(), Batch.id.desc())
    )
    stmt = _apply_read_filter(stmt, ctx=ctx, scope=scope, team_id=team_id)
    stmt = apply_batch_monitor_filters(
        stmt,
        target_team=None,
        q=q,
        benchmark_id=benchmark_id,
        agent_name=agent_name or agent,
        model_provider=model_provider,
        model_name=model_name or model,
        provider_connection_id=provider_connection_id,
        provider_model_id=provider_model_id,
        state=state,
    )
    if visibility:
        stmt = stmt.where(Batch.visibility == visibility)
    if cursor:
        try:
            cur = decode_cursor(cursor)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        stmt = stmt.where(
            or_(
                Batch.created_at < cur.submitted_at,
                and_(Batch.created_at == cur.submitted_at, Batch.id < cur.id),
            ),
        )
    artifact_filters = {
        "artifact_type": artifact_type,
        "owner_team_id": owner_team_id,
        "source_batch_id": source_batch_id,
        "source_trial_id": source_trial_id,
        "safety_state": safety_state,
        "provenance_relation": provenance_relation,
    }
    artifact_filtering = _artifact_filter_active(artifact_filters)
    if not artifact_filtering:
        stmt = stmt.limit(limit + 1)
    rows = list((await session.execute(stmt)).all())

    serialized: list[dict[str, Any]] = []
    if not artifact_filtering:
        batch_ids = [batch.id for batch, _team in rows]
        trial_rollups = await _batch_list_trial_rollups(session, batch_ids)
        artifact_summaries = await _batch_list_artifact_summaries(session, batch_ids)
        for batch, team in rows:
            item = _serialize_batch_list_item(
                batch,
                team,
                trial_rollups.get(
                    batch.id,
                    (_empty_trial_summary(), None, 0.0),
                ),
                artifact_summaries.get(batch.id, _empty_artifact_summary()),
            )
            serialized.append(item)
    else:
        for batch, team in rows:
            trials = await _batch_trials(session, batch.id)
            if not await _batch_has_matching_artifact(
                session,
                trials,
                artifact_filters,
            ):
                continue
            item = await _serialize_batch(request, session, ctx, batch, team)
            serialized.append(item)

    next_cursor: str | None = None
    if len(serialized) > limit:
        serialized = serialized[:limit]
        last_id = UUID(serialized[-1]["id"])
        last_created = next(batch.created_at for batch, _team in rows if batch.id == last_id)
        next_cursor = encode_cursor(
            Cursor(submitted_at=last_created, id=last_id),
        )
    return {"items": serialized, "next_cursor": next_cursor}


async def _artifact_rows_for_library(
    session: Any,
    ctx: Any,
    *,
    request: Request | None,
    scope: str,
    artifact_filters: dict[str, Any],
    safe_content_only: bool,
    limit: int,
) -> list[dict[str, Any]]:
    stmt = (
        select(Artifact, Team, Batch, Trial)
        .join(Team, Team.id == Artifact.team_id)
        .outerjoin(Batch, Batch.id == Artifact.batch_id)
        .outerjoin(Trial, Trial.id == Artifact.trial_id)
        .order_by(Artifact.created_at.desc(), Artifact.id.desc())
    )
    owner_team_id = artifact_filters.get("owner_team_id")
    if owner_team_id is not None:
        stmt = stmt.where(Artifact.team_id == owner_team_id)

    artifact_type = artifact_filters.get("artifact_type")
    if artifact_type in _ARTIFACT_TYPE_GROUPS:
        stmt = stmt.where(Artifact.artifact_type == artifact_type)
    # Source filters may match execution columns or provenance JSON, so they
    # stay in the Python filter below instead of narrowing SQL too early.
    safety_state = artifact_filters.get("safety_state")
    if safety_state is not None:
        stmt = stmt.where(Artifact.safety_state == safety_state)

    if scope != "all":
        if ctx.team_id is None:
            return []
        stmt = stmt.where(Artifact.team_id == ctx.team_id)
    elif not is_admin(ctx):
        stmt = stmt.where(
            or_(
                Artifact.team_id == ctx.team_id,
                and_(
                    Batch.visibility == "org",
                    Batch.share_status == "shared",
                    Batch.state.in_(sorted(_ORG_VISIBLE_BATCH_STATES)),
                ),
                and_(
                    Batch.id.is_(None),
                    Trial.visibility == "org",
                    Trial.share_status == "shared",
                    Trial.state.in_(sorted(_ORG_VISIBLE_TRIAL_STATES)),
                ),
            ),
        )

    rows = list((await session.execute(stmt)).all())
    selected: list[tuple[Artifact, Team, Batch | None, Trial | None]] = []
    for artifact, owner_team, batch, trial in rows:
        if not _artifact_metadata_visible(
            ctx,
            artifact,
            batch=batch,
            trial=trial,
        ):
            continue
        if not _typed_artifact_matches_filters(artifact, artifact_filters):
            continue
        if safe_content_only and not _artifact_content_allowed(
            artifact,
            batch=batch,
            trial=trial,
        ):
            continue
        selected.append((artifact, owner_team, batch, trial))
        if len(selected) >= limit:
            break

    parents_by_artifact = await _parents_for_artifacts(
        session,
        [artifact for artifact, _owner_team, _batch, _trial in selected],
    )
    out: list[dict[str, Any]] = []
    for artifact, owner_team, batch, trial in selected:
        item = _serialize_typed_artifact(
            request,
            artifact,
            owner_team,
            ctx=ctx,
            batch=batch,
            trial=trial,
            parents=parents_by_artifact.get(artifact.id, []),
        )
        if item is not None:
            out.append(redact_mapping(item) if safe_content_only else item)
    return out


@router.get("/run-library/artifacts")
async def list_run_library_artifacts(
    request: Request,
    sc: SessionAndCtx,
    scope: Annotated[str, Query(pattern="^(my|all)$")] = "my",
    artifact_type: Annotated[str | None, Query()] = None,
    owner_team_id: Annotated[UUID | None, Query()] = None,
    source_batch_id: Annotated[UUID | None, Query()] = None,
    source_trial_id: Annotated[UUID | None, Query()] = None,
    safety_state: Annotated[str | None, Query()] = None,
    provenance_relation: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(gt=0, le=500)] = 200,
) -> dict[str, Any]:
    session, ctx = sc
    require_scope(ctx, "read:own")
    filters = {
        "artifact_type": artifact_type,
        "owner_team_id": owner_team_id,
        "source_batch_id": source_batch_id,
        "source_trial_id": source_trial_id,
        "safety_state": safety_state,
        "provenance_relation": provenance_relation,
    }
    stmt_items = await _artifact_rows_for_library(
        session,
        ctx,
        request=request,
        scope=scope,
        artifact_filters=filters,
        safe_content_only=False,
        limit=limit,
    )
    return {"items": stmt_items, "next_cursor": None}


@router.get("/run-library/artifacts/export")
async def export_run_library_artifacts(
    sc: SessionAndCtx,
    scope: Annotated[str, Query(pattern="^(my|all)$")] = "my",
    artifact_type: Annotated[str | None, Query()] = None,
    owner_team_id: Annotated[UUID | None, Query()] = None,
    source_batch_id: Annotated[UUID | None, Query()] = None,
    source_trial_id: Annotated[UUID | None, Query()] = None,
    safety_state: Annotated[str | None, Query()] = None,
    provenance_relation: Annotated[str | None, Query()] = None,
    format: Annotated[str, Query(pattern="^(json|jsonl)$")] = "jsonl",
    limit: Annotated[int, Query(gt=0, le=5000)] = 1000,
) -> Response:
    session, ctx = sc
    require_scope(ctx, "read:own")
    filters = {
        "artifact_type": artifact_type,
        "owner_team_id": owner_team_id,
        "source_batch_id": source_batch_id,
        "source_trial_id": source_trial_id,
        "safety_state": safety_state,
        "provenance_relation": provenance_relation,
    }
    items = await _artifact_rows_for_library(
        session,
        ctx,
        request=None,
        scope=scope,
        artifact_filters=filters,
        safe_content_only=True,
        limit=limit,
    )
    if format == "json":
        return Response(
            content=json.dumps({"items": items}, separators=(",", ":")),
            media_type="application/json",
        )
    content = "".join(json.dumps(item, separators=(",", ":")) + "\n" for item in items)
    return Response(content=content, media_type="application/x-ndjson")


@router.get("/run-library/batches/{batch_id}")
async def get_run_library_batch(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
    include_debug: Annotated[
        bool,
        Query(
            description=(
                "Include heavyweight debug_evidence and diagnosis payloads. "
                "Defaults to false so Run Library detail stays bounded."
            )
        ),
    ] = False,
) -> dict[str, Any]:
    session, ctx = sc
    require_scope(ctx, "read:own")
    batch, team = await _load_batch_with_team(session, batch_id)
    if not _can_read_batch(ctx, batch):
        raise HTTPException(status_code=403, detail="batch is not shared")
    return await _serialize_batch(
        request,
        session,
        ctx,
        batch,
        team,
        include_inventory=True,
        include_debug=include_debug,
    )


@router.patch("/run-library/batches/{batch_id}/visibility")
async def update_run_library_batch_visibility(
    sc: SessionAndCtx,
    batch_id: UUID,
    payload: _VisibilityPatch,
) -> dict[str, Any]:
    session, ctx = sc
    require_scope(ctx, "submit")
    if payload.visibility not in {"team", "org", "private"}:
        raise HTTPException(status_code=400, detail="invalid visibility")
    if payload.share_status not in {"pending_scan", "shared", "blocked"}:
        raise HTTPException(status_code=400, detail="invalid share_status")
    batch, _team = await _load_batch_with_team(session, batch_id)
    require_team_or_admin(ctx, batch.team_id)
    batch.visibility = payload.visibility
    batch.share_status = payload.share_status
    await session.commit()
    await session.refresh(batch)
    return {
        "batch_id": str(batch.id),
        "visibility": batch.visibility,
        "share_status": batch.share_status,
    }


@router.post("/run-library/batches/{batch_id}/clone-config", status_code=201)
async def clone_run_library_batch_config(
    sc: SessionAndCtx,
    batch_id: UUID,
    payload: _CloneConfigRequest,
) -> dict[str, Any]:
    session, ctx = sc
    require_scope(ctx, "submit")
    require_submitting_user(ctx)
    if ctx.team_id is None:
        raise HTTPException(status_code=400, detail="team context required")
    source, _team = await _load_batch_with_team(session, batch_id)
    if not _can_read_batch(ctx, source):
        raise HTTPException(status_code=403, detail="batch is not shared")
    if source.provider_connection_id is not None and payload.provider_connection_id is None:
        raise HTTPException(
            status_code=400,
            detail="choose a provider_connection_id owned by your team",
        )
    if payload.provider_connection_id is not None:
        await validate_provider_connection(
            session,
            payload.provider_connection_id,
            team_id=ctx.team_id,
        )

    token_prefix = ctx.token_hash.hex()[:8] if ctx.token_hash else "00000000"
    provenance = [
        {
            "kind": "cloned_batch_config",
            "source_batch_id": str(source.id),
            "source_team_id": str(source.team_id),
            "source_visibility": source.visibility,
        }
    ]
    clone = Batch(
        team_id=ctx.team_id,
        name=payload.name,
        description=payload.description or (f"Cloned config from shared batch {source.id}."),
        task_filter=dict(source.task_filter),
        trial_config=dict(source.trial_config),
        state="submitted",
        created_by_token_prefix=token_prefix,
        submitted_by_user_id=ctx.user_id,
        expected_trial_count=source.expected_trial_count,
        n_per_task=source.n_per_task,
        backend=source.backend,
        combinations=list(source.combinations or []),
        provider_connection_id=payload.provider_connection_id,
        provider_model_id=payload.provider_model_id or source.provider_model_id,
        source_provenance=provenance,
    )
    session.add(clone)
    await session.commit()
    await session.refresh(clone)
    return {
        "batch_id": str(clone.id),
        "cloned_from_batch_id": str(source.id),
        "provider_connection_id": (
            str(clone.provider_connection_id) if clone.provider_connection_id else None
        ),
        "provider_model_id": clone.provider_model_id,
        "source_provenance": clone.source_provenance,
        "state": clone.state,
        "created_at": clone.created_at.isoformat(),
    }


@router.get("/run-library/trials/{trial_id}/artifacts/download")
async def download_run_library_artifact(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
    key: Annotated[str, Query(min_length=1)],
) -> StreamingResponse:
    settings = request.app.state.settings
    session, ctx = sc
    require_scope(ctx, "read:own")
    trial, batch = await _load_trial_with_batch(session, trial_id)
    if not _can_read_trial(ctx, trial, batch):
        raise HTTPException(status_code=403, detail="trial is not shared")
    typed_artifact = await _typed_artifact_for_trial_key(session, trial.id, key)
    if typed_artifact is not None:
        if not (
            _is_owner_or_admin(ctx, typed_artifact.team_id)
            or _artifact_content_allowed(
                typed_artifact,
                batch=batch,
                trial=trial,
            )
        ):
            raise HTTPException(
                status_code=403,
                detail=_safe_artifact_blocked_reason(typed_artifact),
            )
        return stream_object_response(
            client=request.app.state.minio_client,
            bucket=_artifact_storage_bucket(
                typed_artifact,
                settings.artifacts_bucket,
            ),
            key=key,
            filename=_artifact_filename(key),
            artifact_kind="artifact",
        )
    artifact = _find_artifact(trial.trajectory_index, key)
    if artifact is None:
        raise HTTPException(status_code=404, detail="artifact not found")
    if _share_status(artifact) != "shared":
        raise HTTPException(status_code=403, detail=_blocked_reason(artifact))
    return stream_object_response(
        client=request.app.state.minio_client,
        bucket=_artifact_bucket(artifact, settings.artifacts_bucket),
        key=key,
        filename=_artifact_filename(key),
        artifact_kind="artifact",
    )


@router.post("/run-library/trials/{trial_id}/artifacts/reuse", status_code=201)
async def reuse_run_library_artifact(
    sc: SessionAndCtx,
    trial_id: UUID,
    payload: _ReuseArtifactRequest,
) -> dict[str, Any]:
    session, ctx = sc
    require_scope(ctx, "submit")
    require_submitting_user(ctx)
    if ctx.team_id is None:
        raise HTTPException(status_code=400, detail="team context required")
    trial, batch = await _load_trial_with_batch(session, trial_id)
    if not _can_read_trial(ctx, trial, batch):
        raise HTTPException(status_code=403, detail="trial is not shared")
    typed_artifact = await _typed_artifact_for_trial_key(
        session,
        trial.id,
        payload.key,
    )
    artifact = _find_artifact(trial.trajectory_index, payload.key)
    if typed_artifact is None and artifact is None:
        raise HTTPException(status_code=404, detail="artifact not found")
    if typed_artifact is not None and not _artifact_content_allowed(
        typed_artifact,
        batch=batch,
        trial=trial,
    ):
        raise HTTPException(
            status_code=403,
            detail=_safe_artifact_blocked_reason(typed_artifact),
        )
    if typed_artifact is None and artifact is not None and _share_status(artifact) != "shared":
        raise HTTPException(status_code=403, detail=_blocked_reason(artifact))
    if payload.provider_connection_id is not None:
        await validate_provider_connection(
            session,
            payload.provider_connection_id,
            team_id=ctx.team_id,
        )

    token_prefix = ctx.token_hash.hex()[:8] if ctx.token_hash else "00000000"
    role = (
        _artifact_group_for_type(typed_artifact.artifact_type)
        if typed_artifact is not None
        else _artifact_role(artifact or {})
    )
    source_batch_id = str(batch.id) if batch is not None else None
    provenance_item: dict[str, Any] = {
        "kind": "reused_artifact",
        "relation": "reused_as_input",
        "source_batch_id": source_batch_id,
        "source_trial_id": str(trial.id),
        "source_team_id": str(trial.team_id),
        "source_artifact_key": payload.key,
        "source_artifact_role": role,
    }
    if typed_artifact is not None:
        provenance_item.update(
            {
                "source_artifact_id": str(typed_artifact.id),
                "source_artifact_type": typed_artifact.artifact_type,
                "source_artifact_schema_version": (typed_artifact.artifact_schema_version),
                "source_content_hash": typed_artifact.content_hash,
                "source_visibility": typed_artifact.visibility,
                "source_share_status": typed_artifact.share_status,
                "source_safety_state": typed_artifact.safety_state,
                "source_redaction_state": typed_artifact.redaction_state,
            }
        )
    provenance = [provenance_item]
    task_filter = (
        dict(batch.task_filter)
        if batch is not None
        else {"subset_kind": "explicit", "task_ids": [trial.task_id]}
    )
    trial_config = dict(batch.trial_config) if batch is not None else dict(trial.config)
    derived = Batch(
        team_id=ctx.team_id,
        name=payload.name,
        description=payload.description
        or (f"Reuses shared artifact {payload.key} from trial {trial.id}."),
        task_filter=task_filter,
        trial_config=trial_config,
        state="submitted",
        created_by_token_prefix=token_prefix,
        submitted_by_user_id=ctx.user_id,
        expected_trial_count=batch.expected_trial_count if batch else 1,
        n_per_task=batch.n_per_task if batch else 1,
        backend=batch.backend if batch else "docker",
        combinations=list(batch.combinations or []) if batch else [],
        provider_connection_id=payload.provider_connection_id,
        provider_model_id=(
            payload.provider_model_id
            or trial.provider_model_id
            or (batch.provider_model_id if batch else None)
        ),
        source_provenance=provenance,
    )
    session.add(derived)
    await session.commit()
    await session.refresh(derived)
    return {
        "batch_id": str(derived.id),
        "source_artifact": {
            **(
                {
                    "id": str(typed_artifact.id),
                    "artifact_type": typed_artifact.artifact_type,
                    "artifact_schema_version": typed_artifact.artifact_schema_version,
                    "content_hash": typed_artifact.content_hash,
                }
                if typed_artifact is not None
                else {}
            ),
            "trial_id": str(trial.id),
            "key": payload.key,
            "role": role,
        },
        "source_provenance": derived.source_provenance,
        "state": derived.state,
        "created_at": derived.created_at.isoformat(),
    }
