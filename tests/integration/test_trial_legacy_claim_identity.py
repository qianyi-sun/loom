"""A refundable attempt number must never be the identity of a worker claim."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import insert, select, text
from sqlalchemy.engine import RowMapping
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import ExecutionAdmissionPolicy, Task, Team, TeamQuota, Trial, Worker
from loom_control_plane.execution_admission import upsert_execution_admission_policy
from loom_control_plane.routes.workers import _REQUEUE_TRIAL_RETRY_SQL
from loom_control_plane.scheduler.claim import WorkClaimConflictError, claim_one, claim_work

_TOKEN_HASH = b"w" * 32
_CAPABILITY = "sha256:" + "c" * 64


async def _seed(session: AsyncSession) -> tuple[UUID, UUID]:
    team_id, worker_id, trial_id = uuid4(), uuid4(), uuid4()
    task_id = f"claim-identity-{trial_id}"
    now = datetime.now(UTC)
    await session.execute(insert(Team).values(id=team_id, name=f"claim-{team_id}"))
    await session.execute(insert(TeamQuota).values(team_id=team_id))
    await upsert_execution_admission_policy(
        session, scope_kind="team", scope_key=str(team_id), max_concurrent=1,
        enabled=True, reason="exercise refundable slot accounting",
    )
    await session.execute(insert(Task).values(id=task_id, checksum="0" * 64, config={}))
    await session.execute(insert(Worker).values(
        id=worker_id, hostname="claim-identity", version="test", status="active",
        capabilities=[{"os": "linux", "cpu_arch": "x86_64", "gpu_vendor": "none",
                       "network_policies": ["public"]}],
        supported_work_kinds=["trial"], capability_snapshot_digest=_CAPABILITY,
        capability_snapshot_json={}, auth_token_hash=_TOKEN_HASH,
        registered_at=now, last_seen_at=now, max_concurrent=1,
    ))
    await session.execute(insert(Trial).values(
        id=trial_id, team_id=team_id, task_id=task_id, config={}, state="queued",
        requires_caps={"os": "linux", "cpu_arch": "x86_64", "gpu_vendor": "none",
                       "network_policies": ["public"]},
    ))
    return trial_id, worker_id


async def _claim(session: AsyncSession, worker_id: UUID, *, shared: bool) -> RowMapping | None:
    if shared:
        result = await claim_work(
            session, worker_id=worker_id, capability_snapshot_digest=_CAPABILITY,
            worker_token_hash=_TOKEN_HASH, supported_work_kinds=["trial"], free_slots=1,
            worker_os=["linux"], worker_cpu_arches=["x86_64"],
            worker_gpu_vendors=["none"], worker_network_policies=["public"],
        )
        if result is None:
            return None
        row, token = result
        assert token is None  # Trial identities are not pipeline lease tokens.
        return row
    return await claim_one(
        session, worker_id=worker_id, worker_os=["linux"],
        worker_cpu_arches=["x86_64"], worker_gpu_vendors=["none"],
        worker_network_policies=["public"], enforce_shared_slot=True,
    )


@pytest.mark.parametrize("shared", [False, True], ids=["trial-claim", "work-claim"])
async def test_refunded_trial_gets_new_committed_claim_identity(
    isolated_migration_postgres_url: str, shared: bool,
) -> None:
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            trial_id, worker_id = await _seed(session)
        identities = []
        for _ in range(2):
            async with sessions() as session, session.begin():
                row = await _claim(session, worker_id, shared=shared)
                assert row is not None and row["id"] == trial_id
                assert row["attempt_count"] == 1
                identity = row.get("claim_id")
                assert isinstance(identity, UUID) and identity.int != 0
                identities.append(identity)
            async with sessions() as session, session.begin():
                stored = (await session.execute(text(
                    "SELECT legacy_claim_id FROM trials WHERE id=:id"
                ), {"id": trial_id})).scalar_one()
                assert stored == identity
                assert await session.scalar(select(ExecutionAdmissionPolicy.active_count)) == 1
                # Exercise the actual pre-start health refund, not a test-only
                # update of attempt_count. The next claim is the same worker,
                # same Trial, and same attempt count.
                returned = await session.scalar(_REQUEUE_TRIAL_RETRY_SQL, {
                    "trial_id": trial_id, "worker_id": worker_id,
                    "failure_reason": "node_setup_health", "failure_message": "fixture",
                    "retry_after_sec": 0,
                })
                assert returned == trial_id
                assert await session.scalar(select(ExecutionAdmissionPolicy.active_count)) == 0
                states = (await session.scalars(text(
                    "SELECT state FROM execution_admission_reservations WHERE trial_id=:id"
                ), {"id": trial_id})).all()
                assert states and set(states) == {"released"}
        assert identities[0] != identities[1]
        async with sessions() as session:
            assert await session.scalar(text(
                "SELECT count(*) FROM execution_admission_reservations WHERE trial_id=:id"
            ), {"id": trial_id}) == 2
    finally:
        await engine.dispose()


@pytest.mark.parametrize("shared", [False, True], ids=["trial-claim", "work-claim"])
async def test_concurrent_reclaim_has_one_identity_and_one_reservation(
    isolated_migration_postgres_url: str, shared: bool,
) -> None:
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            trial_id, worker_id = await _seed(session)
        async with sessions() as session, session.begin():
            original = await _claim(session, worker_id, shared=shared)
            assert original is not None
            await session.execute(_REQUEUE_TRIAL_RETRY_SQL, {
                "trial_id": trial_id, "worker_id": worker_id,
                "failure_reason": "node_setup_health", "failure_message": "fixture",
                "retry_after_sec": 0,
            })

        async def contender() -> UUID | None:
            async with sessions() as session, session.begin():
                try:
                    row = await _claim(session, worker_id, shared=shared)
                except WorkClaimConflictError as error:
                    assert error.reason == "worker_capacity_exhausted"
                    return None
                return None if row is None else row["claim_id"]

        results = await asyncio.gather(contender(), contender())
        winners = [result for result in results if result is not None]
        assert len(winners) == 1 and winners[0] != original["claim_id"]
        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            assert trial is not None and trial.legacy_claim_id == winners[0]
            assert trial.attempt_count == 1
            assert await session.scalar(select(ExecutionAdmissionPolicy.active_count)) == 1
            assert (await session.scalars(text(
                "SELECT state FROM execution_admission_reservations "
                "WHERE trial_id=:id ORDER BY acquired_at"
            ), {"id": trial_id})).all() == ["released", "active"]
    finally:
        await engine.dispose()


@pytest.mark.parametrize("shared", [False, True], ids=["trial-claim", "work-claim"])
async def test_rollback_does_not_persist_or_reuse_a_claim_identity(
    isolated_migration_postgres_url: str, shared: bool,
) -> None:
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            trial_id, worker_id = await _seed(session)
        async with sessions() as session:
            row = await _claim(session, worker_id, shared=shared)
            assert row is not None
            abandoned = row.get("claim_id")
            assert isinstance(abandoned, UUID) and abandoned.int != 0
            await session.rollback()
        async with sessions() as session, session.begin():
            trial = await session.get(Trial, trial_id)
            assert trial is not None and trial.state == "queued" and trial.attempt_count == 0
            assert await session.scalar(text(
                "SELECT legacy_claim_id FROM trials WHERE id=:id"
            ), {"id": trial_id}) is None
            row = await _claim(session, worker_id, shared=shared)
            assert row is not None and row["attempt_count"] == 1
            assert isinstance(row.get("claim_id"), UUID) and row["claim_id"] != abandoned
        async with sessions() as session:
            assert await session.scalar(select(Trial.attempt_count).where(Trial.id == trial_id)) == 1
    finally:
        await engine.dispose()


@pytest.mark.parametrize("owner,reason,reusable", [
    ("legacy_worker_claim", None, False),
    ("legacy_worker_claim", "trial_left_active_state", False),
    ("legacy_worker_claim", "trial_setup_refund", True),
    ("service_execution_lease", "trial_setup_refund", False),
    ("protected_worker_claim", "trial_setup_refund", False),
])
async def test_only_explicit_legacy_refund_releases_historical_uniqueness(
    isolated_migration_postgres_url: str, owner: str, reason: str | None, reusable: bool,
) -> None:
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        async with async_sessionmaker(engine)() as session, session.begin():
            trial_id, worker_id = await _seed(session)
            params = {"trial": trial_id, "worker": worker_id, "owner": owner}
            statement = text("""
                INSERT INTO execution_admission_reservations (
                    trial_id, attempt, execution_role, team_id, pool_id, owner_kind, owner_id
                ) SELECT id, 1, 'attempt', team_id, 'oldlab', :owner, :worker
                    FROM trials WHERE id=:trial
            """)
            await session.execute(statement, params)
            # Isolate the index contract. NULL release reason is permitted by
            # the historical CHECK, but is NOT explicit refund evidence.
            await session.execute(text(
                "UPDATE execution_admission_reservations SET state='released', "
                "released_at=now(), release_reason=:reason WHERE trial_id=:trial"
            ), {"reason": reason, "trial": trial_id})
            if reusable:
                await session.execute(statement, params)
            else:
                with pytest.raises(IntegrityError, match="trial_attempt_role_uidx"):
                    async with session.begin_nested():
                        await session.execute(statement, params)
    finally:
        await engine.dispose()
