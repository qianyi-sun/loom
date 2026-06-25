"""#416 Slice 4: the `trials_succeeded_has_result` CHECK constraint
ensures `state='succeeded'` always carries a non-NULL `result`. Pins
the writeback invariant at the DB so a regression in the
`trial_runner` deferred-success patch can't quietly produce trial
rows the SPA/ATIF/#426 reward gate cannot consume."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, delete, insert, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Task, Team, TeamQuota, Token, Trial, Worker


@pytest.fixture
def seed(postgres_url: str) -> Iterator[tuple[UUID, UUID, UUID]]:
    """Minimal team + task + worker so we can insert trial rows. The
    test-side Alembic upgrade run by `postgres_url` already applied
    migration 0039, so the constraint is live."""
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()
    raw = f"w_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Team).values(id=team_id, name=f"x-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="worker", scopes=["worker:report"], team_id=None,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.execute(insert(Worker).values(
            id=worker_id, hostname="h", version="v", capabilities=[],
            registered_at=datetime.now(UTC),
            last_seen_at=datetime.now(UTC), status="active",
        ))
        s.execute(insert(Task).values(
            id="constraint-task", checksum="0" * 64, config={},
        ))
        s.commit()
    try:
        yield team_id, worker_id, trial_id
    finally:
        with session_factory() as s:
            s.execute(delete(Trial))
            s.execute(delete(Worker))
            s.execute(delete(Token))
            s.execute(delete(Task))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        engine.dispose()


def _new_trial(
    team_id: UUID, worker_id: UUID, trial_id: UUID, *,
    state: str, result: dict | None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "id": trial_id, "team_id": team_id, "task_id": "constraint-task",
        "config": {}, "requires_caps": {}, "state": state,
        "worker_id": worker_id,
    }
    if result is not None:
        payload["result"] = result
    return payload


def test_insert_succeeded_with_result_is_allowed(
    seed: tuple[UUID, UUID, UUID], postgres_url: str,
) -> None:
    """Happy path: a terminal-successful trial carrying its TrialResult
    inserts cleanly. Sanity check that the constraint isn't blocking
    valid writes."""
    team_id, worker_id, trial_id = seed
    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    try:
        with sl() as s:
            s.execute(insert(Trial).values(
                **_new_trial(
                    team_id, worker_id, trial_id,
                    state="succeeded",
                    result={"aggregate_reward": 1.0},
                ),
            ))
            s.commit()
    finally:
        engine.dispose()


def test_insert_succeeded_with_null_result_is_blocked(
    seed: tuple[UUID, UUID, UUID], postgres_url: str,
) -> None:
    """The bug shape from the public-beta evidence on #416. A trial
    being persisted as `state=succeeded` without its `result` MUST
    error rather than silently succeed."""
    team_id, worker_id, trial_id = seed
    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    try:
        with sl() as s, pytest.raises(IntegrityError) as exc:
            s.execute(insert(Trial).values(
                **_new_trial(
                    team_id, worker_id, trial_id,
                    state="succeeded", result=None,
                ),
            ))
            s.commit()
        assert "trials_succeeded_has_result" in str(exc.value)
    finally:
        engine.dispose()


def test_update_to_succeeded_without_result_is_blocked(
    seed: tuple[UUID, UUID, UUID], postgres_url: str,
) -> None:
    """The actual writeback ordering bug: a running trial that gets
    PATCHed to `state=succeeded` BEFORE its result lands must be
    rejected at PATCH time so the worker sees a 5xx and retries the
    correct ordering."""
    team_id, worker_id, trial_id = seed
    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    try:
        with sl() as s:
            s.execute(insert(Trial).values(
                **_new_trial(
                    team_id, worker_id, trial_id,
                    state="running", result=None,
                ),
            ))
            s.commit()
        with sl() as s, pytest.raises(IntegrityError) as exc:
            s.execute(
                update(Trial).where(Trial.id == trial_id).values(
                    state="succeeded",
                ),
            )
            s.commit()
        assert "trials_succeeded_has_result" in str(exc.value)
    finally:
        engine.dispose()


def test_failed_and_cancelled_states_dont_require_result(
    seed: tuple[UUID, UUID, UUID], postgres_url: str,
) -> None:
    """The constraint is scoped specifically to `succeeded` — `failed`
    and `cancelled` rows correctly carry NULL result (the agent never
    produced one). Don't accidentally widen the constraint."""
    team_id, worker_id, trial_id = seed
    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    try:
        with sl() as s:
            s.execute(insert(Trial).values(
                **_new_trial(
                    team_id, worker_id, trial_id,
                    state="failed", result=None,
                ),
            ))
            cancelled_id = uuid4()
            s.execute(insert(Trial).values(
                **_new_trial(
                    team_id, worker_id, cancelled_id,
                    state="cancelled", result=None,
                ),
            ))
            s.commit()
    finally:
        engine.dispose()


def test_constraint_was_added_not_valid_so_existing_violations_survive(
    seed: tuple[UUID, UUID, UUID], postgres_url: str,
) -> None:
    """Defensive guard against accidentally tightening the migration
    to VALIDATE — operators may still have legacy succeeded-with-NULL
    rows from before #416 ships, and the upgrade path must not refuse
    to start the service. The follow-up `VALIDATE` migration runs
    only after operators run the cleanup query documented in the
    runbook.

    We assert by reading `pg_constraint.convalidated`: NOT VALID =
    False, VALIDATE = True. If a future migration legitimately
    validates, this test should be deleted along with that change."""
    engine = create_engine(postgres_url)
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT convalidated FROM pg_constraint "
                "WHERE conname = 'trials_succeeded_has_result'",
            )).first()
        assert row is not None, (
            "trials_succeeded_has_result constraint missing — "
            "migration 0039 may not have run"
        )
        assert row[0] is False, (
            "constraint is VALIDATEd; this migration intentionally "
            "ships as NOT VALID so legacy violations don't block "
            "service startup. See migration 0039."
        )
    finally:
        engine.dispose()
