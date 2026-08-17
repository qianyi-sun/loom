"""Persist/load immutable model_switch_plans rows (#1380)."""

from __future__ import annotations

import secrets
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import ModelSwitchPlan, ProviderConnection
from loom.models.model_switch_plan import PRNG_VERSION, ModelSwitchPlanSnapshot
from loom.models.trial import MultiModelSwitchSpec
from loom.models.types import ModelSpec


def plan_snapshot_from_row(row: ModelSwitchPlan) -> ModelSwitchPlanSnapshot:
    return ModelSwitchPlanSnapshot(
        id=row.id,
        trial_id=row.trial_id,
        combination_idx=row.combination_idx,
        k1=row.k1,
        k2=row.k2,
        teacher_episodes=row.teacher_episodes,
        seed=row.seed,
        prng_version=row.prng_version,
        student_model=ModelSpec.model_validate(row.student_model_snapshot),
        teacher_model=ModelSpec.model_validate(row.teacher_model_snapshot),
        provider_connection_id=row.provider_connection_id,
        pricing_snapshot=dict(row.pricing_snapshot or {}),
        capability_snapshot=dict(row.capability_snapshot or {}),
        inherited_from_plan_id=row.inherited_from_plan_id,
    )


async def persist_model_switch_plan(
    session: AsyncSession,
    *,
    trial_id: UUID,
    trial_config: dict[str, Any] | None,
    agent_model: ModelSpec | None,
    provider_connection_id: UUID | None,
    combination_idx: int = 0,
    inherit_from_trial_id: UUID | None = None,
    provider_connection: ProviderConnection | None = None,
) -> ModelSwitchPlan | None:
    if not trial_config:
        return None
    raw = trial_config.get("multi_model")
    if not raw:
        return None
    spec = MultiModelSwitchSpec.model_validate(raw)
    if not spec.enabled or spec.secondary_model is None:
        return None
    if spec.switch_episode is None or spec.return_switch_episode is None:
        return None
    if agent_model is None:
        return None

    inherited_from: UUID | None = None
    seed = secrets.token_hex(16)
    mode = trial_config.get("model_switch_plan_mode") or "inherit"
    if inherit_from_trial_id is not None and mode != "resample":
        source = (
            await session.execute(
                select(ModelSwitchPlan).where(
                    ModelSwitchPlan.trial_id == inherit_from_trial_id,
                ),
            )
        ).scalar_one_or_none()
        if source is not None:
            inherited_from = source.id
            seed = source.seed

    pricing_snapshot: dict[str, Any] = {}
    capability_snapshot: dict[str, Any] = {}
    if provider_connection is not None:
        pricing_snapshot = {
            "pricing_source": provider_connection.pricing_source,
            "pricing_data": provider_connection.pricing_data,
            "rate_card_provider": provider_connection.rate_card_provider,
        }
        capability_snapshot = {
            "allowed_models": list(provider_connection.allowed_models or []),
            "provider_type": provider_connection.provider_type,
        }

    plan_id = uuid4()
    stmt = (
        pg_insert(ModelSwitchPlan)
        .values(
            id=plan_id,
            trial_id=trial_id,
            combination_idx=combination_idx,
            k1=int(spec.switch_episode),
            k2=int(spec.return_switch_episode),
            teacher_episodes=int(spec.teacher_episodes),
            seed=seed,
            prng_version=PRNG_VERSION,
            student_model_snapshot=agent_model.model_dump(mode="json"),
            teacher_model_snapshot=spec.secondary_model.model_dump(mode="json"),
            provider_connection_id=provider_connection_id,
            pricing_snapshot=pricing_snapshot,
            capability_snapshot=capability_snapshot,
            inherited_from_plan_id=inherited_from,
        )
        .on_conflict_do_nothing(index_elements=["trial_id"])
    )
    await session.execute(stmt)
    return (
        await session.execute(
            select(ModelSwitchPlan).where(ModelSwitchPlan.trial_id == trial_id),
        )
    ).scalar_one_or_none()


async def load_model_switch_plan(
    session: AsyncSession,
    trial_id: UUID,
) -> ModelSwitchPlan | None:
    return (
        await session.execute(
            select(ModelSwitchPlan).where(ModelSwitchPlan.trial_id == trial_id),
        )
    ).scalar_one_or_none()
