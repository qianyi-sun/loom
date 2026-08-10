from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from sqlalchemy import text

from loom.pipeline.budget import (
    BudgetExceededError,
    BudgetKind,
    TerminalCause,
    provider_reservation_key,
)
from loom.pipeline.keys import canonical_digest

if TYPE_CHECKING:
    from tests.integration.pipeline_orchestrator_fixtures import OrchestratorSeed


@pytest.mark.asyncio
async def test_concurrent_provider_reservations_latch_one_terminal_cause(
    orchestrator_seed: OrchestratorSeed,
) -> None:
    seed = orchestrator_seed
    lease = (await seed.repository.claim_runs(controller_id="controller-a"))[0]
    await seed.repository.initialize_run(lease)
    attempt_id = await _seed_attempt(seed)

    async def reserve_one() -> object:
        key = provider_reservation_key(attempt_id, uuid4())
        try:
            return await seed.repository.reserve_budget(
                lease,
                kind=BudgetKind.PROVIDER,
                reservation_key=key,
                request_digest=canonical_digest({"key": key}, persisted=False),
                amount=7,
                execution_attempt_id=attempt_id,
            )
        except BudgetExceededError as error:
            return error.cause

    outcomes = await asyncio.gather(reserve_one(), reserve_one())
    assert TerminalCause.PROVIDER_BUDGET in outcomes
    async with seed.sessions() as session:
        row = (
            await session.execute(
                text("""
                    SELECT l.terminal_cause, l.provider_reserved_microusd, r.state,
                           (SELECT state FROM pipeline_budget_reservations
                             WHERE pipeline_run_id=r.id)
                      FROM pipeline_budget_ledgers l
                      JOIN pipeline_runs r ON r.id=l.pipeline_run_id
                     WHERE r.id=:id
                """),
                {"id": seed.run_id},
            )
        ).one()
    assert row == ("provider_budget", 0, "cancelling", "released")
    await seed.repository.release(lease)


async def _seed_attempt(seed: OrchestratorSeed):
    stage_id = uuid4()
    attempt_id = uuid4()
    digest = "sha256:" + "d" * 64
    async with seed.sessions() as session, session.begin():
        await session.execute(
            text("""
                INSERT INTO pipeline_stage_runs (
                    id,pipeline_run_id,node_key,shard_key,node_kind,state,
                    resource_profile_json,resource_profile_digest,failure_policy
                ) VALUES (:id,:run_id,'budget_stage','singleton','container','blocked',
                          '{}'::jsonb,:digest,'fail_run')
            """),
            {"id": stage_id, "run_id": seed.run_id, "digest": digest},
        )
        await session.execute(
            text("""
                INSERT INTO execution_attempts (
                    id,stage_run_id,attempt_number,state,queued_at,
                    stage_request_json,stage_request_bytes,stage_request_digest
                ) VALUES (:id,:stage_id,1,'queued',now(),'{}'::jsonb,:bytes,:digest)
            """),
            {"id": attempt_id, "stage_id": stage_id, "bytes": b"{}\n", "digest": digest},
        )
    return attempt_id
