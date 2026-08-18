"""Correlate Terminus-2 LLM calls by client_call_id before upstream I/O (#1380)."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.agent.terminus2.model_switch import role_for_episode
from loom.db.schema import LlmCallIntent, ModelSwitchPlan

LOOM_PAYLOAD_PREFIX = "loom_"
LOOM_FIELD_NAMES = (
    "loom_client_call_id",
    "loom_agent_execution_id",
    "loom_agent_run_attempt_id",
    "loom_episode",
    "loom_call_ordinal",
    "loom_requested_model",
    "loom_role",
)
# Terminus always talks to this gateway's OpenAI facade (/openai/v1), so Harbor
# LiteLLM ids are ``openai/<name>`` even when the plan provider is Anthropic.
# The HTTP body ``model`` field and model_switch_plans snapshots store the bare
# ``<name>``. Strip only this LiteLLM dialect prefix; do not treat it as the
# real upstream provider. Native /anthropic/v1 does not use this correlation.
_OPENAI_COMPAT_PREFIX = "openai/"


def canonical_facade_model_id(model: str) -> str:
    """Map Harbor LiteLLM ``openai/<name>`` onto the OpenAI facade ``model`` id.

    ``loom_requested_model`` is stamped from ``_harbor_model_name``
    (always ``openai/`` + ``ModelSpec.name``). LiteLLM then POSTs
    ``model: <name>`` to ``/openai/v1/chat/completions``. Exact string
    equality of those two fields 400s a real student call; this helper
    is the identity for that facade contract.
    """
    if model.startswith(_OPENAI_COMPAT_PREFIX):
        return model[len(_OPENAI_COMPAT_PREFIX) :]
    return model


def extract_and_strip_loom_fields(payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Copy payload, pull Loom correlation keys, strip them before upstream."""
    out = dict(payload)
    extras: dict[str, Any] = {}
    extra_body = out.get("extra_body")
    sources: list[dict[str, Any]] = [out]
    nested: dict[str, Any] | None = None
    if isinstance(extra_body, dict):
        nested = dict(extra_body)
        out["extra_body"] = nested
        sources.append(nested)
    for src in sources:
        for key in list(src.keys()):
            if key.startswith(LOOM_PAYLOAD_PREFIX):
                extras[key] = src.pop(key)
    if isinstance(out.get("extra_body"), dict) and not out["extra_body"]:
        out.pop("extra_body", None)
    return out, extras


def _as_uuid(value: Any, field: str) -> UUID:
    try:
        return UUID(str(value))
    except (ValueError, TypeError) as exc:
        raise HTTPException(
            status_code=400,
            detail=f"{field} is not a UUID",
        ) from exc


async def persist_correlated_intent(
    session: AsyncSession,
    *,
    trial_id: UUID,
    step_id: str,
    extras: dict[str, Any],
    jwt_connection_id: UUID | None,
    requested_model: str,
) -> dict[str, Any]:
    """Verify extras against the immutable plan and persist intent.

    Returns correlation kwargs for ``record_call``. Empty extras → legacy.
    """
    if not extras:
        return {
            "correlation_status": "legacy_uncorrelated",
            "client_call_id": None,
            "agent_execution_id": None,
            "agent_run_attempt_id": None,
            "episode": None,
            "call_ordinal": None,
            "requested_model": requested_model,
            "role": None,
        }

    missing = [name for name in LOOM_FIELD_NAMES if extras.get(name) in (None, "")]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"incomplete Loom correlation fields: {missing}",
        )

    client_call_id = _as_uuid(extras["loom_client_call_id"], "loom_client_call_id")
    execution_id = _as_uuid(
        extras["loom_agent_execution_id"], "loom_agent_execution_id",
    )
    attempt_id = _as_uuid(
        extras["loom_agent_run_attempt_id"], "loom_agent_run_attempt_id",
    )
    try:
        episode = int(extras["loom_episode"])
        call_ordinal = int(extras["loom_call_ordinal"])
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail="loom_episode and loom_call_ordinal must be integers",
        ) from exc
    role = str(extras["loom_role"])
    if role not in {"student", "teacher"}:
        raise HTTPException(status_code=400, detail="loom_role is invalid")
    declared_model = canonical_facade_model_id(str(extras["loom_requested_model"]))
    requested_model = canonical_facade_model_id(requested_model)
    if declared_model != requested_model:
        raise HTTPException(
            status_code=400,
            detail="requested model does not match loom_requested_model",
        )

    plan = (
        await session.execute(
            select(ModelSwitchPlan).where(ModelSwitchPlan.trial_id == trial_id),
        )
    ).scalar_one_or_none()
    if plan is None:
        raise HTTPException(
            status_code=400,
            detail="Loom correlation fields require a model_switch_plan",
        )
    if (
        jwt_connection_id is not None
        and plan.provider_connection_id is not None
        and jwt_connection_id != plan.provider_connection_id
    ):
        raise HTTPException(
            status_code=403,
            detail="provider_connection_id does not match model_switch_plan",
        )

    student_name = canonical_facade_model_id(
        str((plan.student_model_snapshot or {}).get("name") or ""),
    )
    teacher_name = canonical_facade_model_id(
        str((plan.teacher_model_snapshot or {}).get("name") or ""),
    )
    allowed = {student_name, teacher_name} - {""}
    if requested_model not in allowed:
        raise HTTPException(
            status_code=403,
            detail=(
                f"requested model {requested_model!r} is not in the "
                "student/teacher plan allowlist"
            ),
        )
    expected_role = role_for_episode(
        episode,
        first_switch_episode=plan.k1,
        return_switch_episode=plan.k2,
    )
    if role != expected_role:
        raise HTTPException(
            status_code=400,
            detail=(
                f"loom_role {role!r} does not match plan role "
                f"{expected_role!r} for episode {episode}"
            ),
        )
    expected_name = teacher_name if expected_role == "teacher" else student_name
    if requested_model != expected_name:
        raise HTTPException(
            status_code=400,
            detail=(
                f"requested model {requested_model!r} does not match "
                f"{expected_role} snapshot {expected_name!r}"
            ),
        )

    stmt = (
        pg_insert(LlmCallIntent)
        .values(
            id=client_call_id,
            client_call_id=client_call_id,
            trial_id=trial_id,
            step_id=step_id,
            agent_execution_id=execution_id,
            agent_run_attempt_id=attempt_id,
            episode=episode,
            call_ordinal=call_ordinal,
            requested_model=requested_model,
            role=role,
            status="registered",
        )
        .on_conflict_do_nothing(index_elements=["client_call_id"])
    )
    await session.execute(stmt)
    await session.commit()
    return {
        "correlation_status": "correlated",
        "client_call_id": client_call_id,
        "agent_execution_id": execution_id,
        "agent_run_attempt_id": attempt_id,
        "episode": episode,
        "call_ordinal": call_ordinal,
        "requested_model": requested_model,
        "role": role,
    }
