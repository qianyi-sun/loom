from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from sqlalchemy import text

from loom.pipeline.budget import (
    AttemptProviderBudgetExceededError,
    BudgetKind,
    gpu_reservation_key,
)
from loom.pipeline.keys import canonical_digest, canonical_document
from loom_pipeline_orchestrator.repository import (
    AttemptProviderBudgetSpec,
    AttemptReservationSpec,
    BudgetReservationConflictError,
    FrozenReadiness,
    ReservationRecord,
)

if TYPE_CHECKING:
    from tests.integration.pipeline_orchestrator_fixtures import OrchestratorSeed


@pytest.mark.asyncio
async def test_attempt_and_gpu_reservation_commit_atomically(
    orchestrator_seed: OrchestratorSeed,
) -> None:
    seed = orchestrator_seed
    lease = (await seed.repository.claim_runs(controller_id="controller-a"))[0]
    await seed.repository.initialize_run(lease)
    stage_id = await _stage_id(seed)
    frozen = _frozen()
    await seed.repository.freeze_readiness(lease, stage_run_id=stage_id, frozen=frozen)
    attempt_id = uuid4()
    request = {"schema_version": "test.stage-request.v1"}
    key = gpu_reservation_key(attempt_id)
    reservation_digest = canonical_digest({"key": key, "amount": 30}, persisted=False)
    attempt = await seed.repository.create_attempt(
        lease,
        stage_run_id=stage_id,
        attempt_id=attempt_id,
        stage_request_json=request,
        stage_request_bytes=canonical_document(request),
        stage_request_digest=canonical_digest(request),
        reservations=(AttemptReservationSpec(BudgetKind.GPU, key, reservation_digest, 30, {}),),
    )
    replay = await seed.repository.create_attempt(
        lease,
        stage_run_id=stage_id,
        attempt_id=attempt_id,
        stage_request_json=request,
        stage_request_bytes=canonical_document(request),
        stage_request_digest=canonical_digest(request),
        reservations=(AttemptReservationSpec(BudgetKind.GPU, key, reservation_digest, 30, {}),),
    )
    assert replay == attempt
    with pytest.raises(BudgetReservationConflictError, match="reservation replay drift"):
        await seed.repository.create_attempt(
            lease,
            stage_run_id=stage_id,
            attempt_id=attempt_id,
            stage_request_json=request,
            stage_request_bytes=canonical_document(request),
            stage_request_digest=canonical_digest(request),
            reservations=(AttemptReservationSpec(BudgetKind.GPU, key, reservation_digest, 31, {}),),
        )
    async with seed.sessions() as session:
        values = (
            await session.execute(
                text("""
                    SELECT gpu_reserved_seconds, attempts_created,
                           (SELECT count(*) FROM pipeline_budget_reservations
                             WHERE pipeline_run_id=:id)
                      FROM pipeline_budget_ledgers WHERE pipeline_run_id=:id
                """),
                {"id": seed.run_id},
            )
        ).one()
    assert values == (30, 1, 1)
    await seed.repository.release(lease)


@pytest.mark.asyncio
async def test_attempt_local_provider_slice_serializes_concurrent_dispatches(
    orchestrator_seed: OrchestratorSeed,
) -> None:
    seed = orchestrator_seed
    lease = (await seed.repository.claim_runs(controller_id="controller-a"))[0]
    await seed.repository.initialize_run(lease)
    stage_id = await _stage_id(seed)
    await seed.repository.freeze_readiness(lease, stage_run_id=stage_id, frozen=_frozen())
    attempt_id = uuid4()
    request = {"schema_version": "test.stage-request.v1"}
    await seed.repository.create_attempt(
        lease,
        stage_run_id=stage_id,
        attempt_id=attempt_id,
        stage_request_json=request,
        stage_request_bytes=canonical_document(request),
        stage_request_digest=canonical_digest(request),
        reservations=(),
        provider_budget=AttemptProviderBudgetSpec(
            binding_snapshot_sha256="sha256:" + "b" * 64,
            request_limit=1,
            cost_limit_microusd=20,
            per_call_timeout_seconds=30,
        ),
    )
    worker_id = uuid4()
    async with seed.sessions() as session, session.begin():
        await session.execute(
            text("""
                INSERT INTO workers (
                    id, hostname, version, capabilities, registered_at, last_seen_at, status
                ) VALUES (:id, :hostname, 'test', '[]'::jsonb, now(), now(), 'active')
            """),
            {"id": worker_id, "hostname": f"provider-budget-{worker_id}"},
        )
        await session.execute(
            text("""
                UPDATE execution_attempts
                   SET state='running', worker_id=:worker_id, claim_id=:claim_id,
                       lease_token_digest=:digest, lease_expires_at=now()+interval '1 minute',
                       claimed_at=now(), started_at=now()
                 WHERE id=:attempt_id
            """),
            {
                "worker_id": worker_id,
                "claim_id": uuid4(),
                "digest": "sha256:" + "c" * 64,
                "attempt_id": attempt_id,
            },
        )
        await session.execute(
            text("UPDATE pipeline_stage_runs SET state='running' WHERE id=:stage_id"),
            {"stage_id": stage_id},
        )

    async def reserve() -> object:
        provider_request_id = uuid4()
        try:
            return await seed.repository.reserve_provider_dispatch(
                lease,
                attempt_id=attempt_id,
                provider_request_id=provider_request_id,
                request_digest=canonical_digest(
                    {"provider_request_id": str(provider_request_id)}, persisted=False
                ),
                worst_case_cost_microusd=10,
            )
        except AttemptProviderBudgetExceededError as error:
            return error

    outcomes = await asyncio.gather(reserve(), reserve())
    reservations = [item for item in outcomes if not isinstance(item, Exception)]
    failures = [item for item in outcomes if isinstance(item, AttemptProviderBudgetExceededError)]
    assert len(reservations) == len(failures) == 1
    async with seed.sessions() as session:
        values = (
            await session.execute(
                text("""
                    SELECT b.requests_reserved, b.requests_settled,
                           b.cost_reserved_microusd, l.provider_reserved_microusd,
                           l.terminal_cause, a.reason_code
                      FROM execution_attempt_provider_budgets b
                      JOIN execution_attempts a ON a.id=b.attempt_id
                      JOIN pipeline_stage_runs s ON s.id=a.stage_run_id
                      JOIN pipeline_budget_ledgers l ON l.pipeline_run_id=s.pipeline_run_id
                     WHERE b.attempt_id=:attempt_id
                """),
                {"attempt_id": attempt_id},
            )
        ).one()
    assert values == (1, 0, 10, 10, None, "provider_attempt_budget_exhausted")
    assert isinstance(reservations[0], ReservationRecord)
    await seed.repository.release_provider_dispatch(lease, reservation_id=reservations[0].id)
    await seed.repository.release(lease)


async def _stage_id(seed: OrchestratorSeed):
    async with seed.sessions() as session:
        return (
            await session.execute(
                text("SELECT id FROM pipeline_stage_runs WHERE pipeline_run_id=:id"),
                {"id": seed.run_id},
            )
        ).scalar_one()


def _frozen() -> FrozenReadiness:
    bindings: list[dict[str, object]] = []
    spec = {"schema_version": "test.execution-spec.v1"}
    return FrozenReadiness(
        bindings,
        canonical_digest(bindings),
        spec,
        canonical_document(spec),
        canonical_digest(spec),
    )
