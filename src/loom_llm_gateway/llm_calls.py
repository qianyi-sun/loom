"""llm_calls insert helper — one writer for all dialect endpoints
(Plan 9 Task 6)."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import LlmCall
from loom_llm_gateway.dialect import TokenUsage


async def record_call(
    session: AsyncSession,
    *,
    team_id: UUID,
    trial_id: UUID,
    step_id: str,
    dialect: str,
    model: str,
    usage: TokenUsage,
    cost_usd: float,
    rate_card_hash: str,
) -> None:
    """Insert one row into `llm_calls`. Called by every dialect endpoint
    (chat / messages / responses / gemini) AFTER the upstream provider
    returns successfully. The trial's worker reads these rows at finalize
    (via the CP endpoint) and projects each into an LLMCallEvent."""
    await session.execute(insert(LlmCall).values(
        team_id=team_id,
        trial_id=trial_id,
        step_id=step_id,
        dialect=dialect,
        model=model,
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        provider_extras=usage.provider_extras,
        cost_usd=cost_usd,
        rate_card_hash=rate_card_hash,
    ))
    await session.commit()
