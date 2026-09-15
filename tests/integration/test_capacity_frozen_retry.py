"""Real authenticated retry continuity without reopening frozen legacy writes."""

from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom_control_plane.protected_worker_session import (
    ProtectedWorkerSessionRejected,
    ProtectedWorkerSessionStore,
)
from tests.integration.test_capacity_agent_store import _seed_trial, _value
from tests.integration.test_capacity_protected_worker_session import (
    _WORKER_CREDENTIAL,
    _seed_claimed_protected_trial,
)
from tests.integration.test_capacity_trial_writer_fence import (
    _control_session,
    _freeze,
    _initialize,
    _legacy_engine,
)
from tests.integration.test_capacity_trial_writer_retirement import _downgrade


@pytest.mark.parametrize(
    "failure_reason,expected_attempt_count",
    [("env_start_failure", 1), ("node_setup_health", 0)],
)
@pytest.mark.parametrize("freeze_before_retry", [False, True])
def test_authenticated_retry_survives_trial_writer_freeze(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure_reason: str,
    expected_attempt_count: int,
    freeze_before_retry: bool,
) -> None:
    database = capacity_guard_database
    seeded = _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    initial = asyncio.run(_initialize(database, registration=seeded.worker.registration))
    freeze_operation = uuid4()
    frozen = None
    if freeze_before_retry:
        frozen = asyncio.run(_freeze(database, initial["writer_incarnation"], freeze_operation))
        assert frozen["frozen"] is True

    before_engine = create_engine(_value(database, "admin_url"))
    try:
        with before_engine.connect() as connection:
            ledger_before = connection.execute(
                text("SELECT count(*) FROM loom_capacity_guard.trial_writer_mutations")
            ).scalar_one()
    finally:
        before_engine.dispose()

    database_errors: list[tuple[object, object]] = []
    retry = ProtectedWorkerSessionStore.retry_claimed_trial

    async def observed_retry(self, **kwargs):
        try:
            return await retry(self, **kwargs)
        except ProtectedWorkerSessionRejected as exc:
            original = getattr(exc.__cause__, "orig", None)
            diagnostic = getattr(original, "diag", None)
            database_errors.append(
                (getattr(original, "sqlstate", None), getattr(diagnostic, "message_primary", None))
            )
            raise

    monkeypatch.setattr(ProtectedWorkerSessionStore, "retry_claimed_trial", observed_retry)

    with TestClient(seeded.app) as client:
        response = client.post(
            f"/trials/{seeded.trial_id}/retry",
            headers=seeded.claim_headers,
            json={
                "worker_id": str(seeded.worker.worker.worker_id),
                "failure_reason": failure_reason,
                "failure_message": "retry under the protected writer",
                "retry_after_sec": 0,
            },
        )
    assert response.status_code == 200, (response.text, database_errors)
    assert response.json()["state"] == "protected-pending"

    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.connect() as connection:
            result = (
                connection.execute(
                    text(
                        "SELECT trial.state, trial.worker_id, trial.attempt_count, "
                        "trial.failure_reason, "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_attempts AS item "
                        " WHERE item.trial_id = trial.id) AS attempt_rows, "
                        "(SELECT count(*) FROM "
                        " loom_capacity_guard.protected_runtime_trial_submissions AS item "
                        " WHERE item.trial_id = trial.id) AS runtime_rows, "
                        "(SELECT count(*) FROM "
                        " loom_capacity_guard.executable_claim_terminal_events AS item "
                        " WHERE item.protected_attempt_id = :attempt_id) AS terminal_rows "
                        "FROM public.trials AS trial WHERE trial.id = :trial_id"
                    ),
                    {
                        "trial_id": seeded.trial_id,
                        "attempt_id": seeded.first_attempt["protected_attempt_id"],
                    },
                )
                .mappings()
                .one()
            )
            ledger_after = connection.execute(
                text("SELECT count(*) FROM loom_capacity_guard.trial_writer_mutations")
            ).scalar_one()
            permits = (
                connection.execute(
                    text(
                        "SELECT permit_id, state, writer_incarnation, freeze_operation_id, trial_id, "
                        "protected_attempt_id, worker_id, operation "
                        "FROM loom_capacity_guard.trial_mutation_permits ORDER BY operation"
                    )
                )
                .mappings()
                .all()
            )
    finally:
        engine.dispose()
    assert dict(result) == {
        "state": "protected-pending",
        "worker_id": None,
        "attempt_count": expected_attempt_count,
        "failure_reason": failure_reason,
        "attempt_rows": 2,
        "runtime_rows": 2,
        "terminal_rows": 1,
    }
    final = asyncio.run(_freeze(database, initial["writer_incarnation"], freeze_operation))
    if frozen is not None:
        assert final == frozen
        assert ledger_after == ledger_before
        assert [permit["operation"] for permit in permits] == (
            ["refund", "retry"] if failure_reason == "node_setup_health" else ["retry"]
        )
        for permit in permits:
            assert permit["state"] == "consumed"
            assert str(permit["writer_incarnation"]) == frozen["writer_incarnation"]
            assert permit["freeze_operation_id"] == freeze_operation
            assert permit["trial_id"] == seeded.trial_id
            assert permit["protected_attempt_id"] == seeded.first_attempt["protected_attempt_id"]
            assert permit["worker_id"] == seeded.worker.worker.worker_id

        async def reject_old_transaction_permission() -> None:
            with pytest.raises(DBAPIError) as refusal:
                async with _control_session(database, isolation="SERIALIZABLE") as owner:
                    await owner.execute(
                        text(
                            "SELECT loom_capacity_guard.assert_frozen_trial_mutation_consumed(CAST(:permit AS uuid))"
                        ),
                        {"permit": permits[0]["permit_id"]},
                    )
            assert refusal.value.orig.sqlstate == "55000"

        asyncio.run(reject_old_transaction_permission())
        with pytest.raises(RuntimeError, match="permission evidence requires protected retirement"):
            _downgrade(database, monkeypatch)
    else:
        assert final["high_water"] == (2 if failure_reason == "node_setup_health" else 1)
        assert ledger_after - ledger_before == final["high_water"]
        assert permits == []


@pytest.mark.parametrize(
    "boundary", ["direct-update", "private-read", "private-write", "private-issuer"]
)
def test_frozen_retry_does_not_grant_legacy_or_private_permission(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    boundary: str,
) -> None:
    database = capacity_guard_database
    seeded = _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    initial = asyncio.run(_initialize(database, registration=seeded.worker.registration))
    asyncio.run(_freeze(database, initial["writer_incarnation"], uuid4()))
    queries = {
        "direct-update": "UPDATE public.trials SET submit_priority = 999 WHERE id = :trial",
        "private-read": "SELECT * FROM loom_capacity_guard.trial_mutation_permits",
        "private-write": "DELETE FROM loom_capacity_guard.trial_mutation_permits",
        "private-issuer": (
            "SELECT loom_capacity_guard.authorize_frozen_trial_update("
            "CAST(:trial AS uuid), CAST(:attempt AS uuid), 1, CAST(:worker AS uuid), "
            "CAST(:worker AS uuid), CAST(:attempt AS uuid), 'retry', '{}'::jsonb)"
        ),
    }
    legacy = _legacy_engine(database)
    try:
        with pytest.raises(DBAPIError) as refusal:
            with legacy.begin() as connection:
                connection.execute(
                    text(queries[boundary]),
                    {
                        "trial": seeded.trial_id,
                        "attempt": seeded.first_attempt["protected_attempt_id"],
                        "worker": seeded.worker.worker.worker_id,
                    },
                )
        assert refusal.value.orig.sqlstate == ("55000" if boundary == "direct-update" else "42501")
    finally:
        legacy.dispose()


@pytest.mark.parametrize(
    "fault", ["rollback", "extra-before", "extra-after", "extra-row", "suppressed-row"]
)
def test_frozen_retry_refuses_unapproved_changes_and_rolls_back_atomically(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fault: str,
) -> None:
    database = capacity_guard_database
    seeded = _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    other_trial = _seed_trial(database)
    initial = asyncio.run(_initialize(database, registration=seeded.worker.registration))
    frozen = asyncio.run(_freeze(database, initial["writer_incarnation"], uuid4()))
    engine = create_engine(_value(database, "admin_url"))
    # Test-only trigger injection in this disposable database. No live DDL.
    trigger = "aaa_retry_fault" if fault == "extra-before" else "zzzz_retry_fault"
    timing = "AFTER" if fault in {"rollback", "extra-row"} else "BEFORE"
    body = (
        "RAISE EXCEPTION 'injected retry rollback' USING ERRCODE = '55000';"
        if fault == "rollback"
        else "NEW.submit_priority := OLD.submit_priority + 1; RETURN NEW;"
    )
    if fault == "extra-row":
        body = f"UPDATE public.trials SET submit_priority = 999 WHERE id = '{other_trial}'::uuid; RETURN NEW;"
    elif fault == "suppressed-row":
        body = "RETURN NULL;"
    accounting_sql = text(
        "SELECT quota.in_flight_count, reservation.state "
        "FROM public.trials AS trial "
        "JOIN public.team_quotas AS quota ON quota.team_id = trial.team_id "
        "JOIN public.execution_admission_reservations AS reservation ON reservation.trial_id = trial.id "
        "WHERE trial.id = :trial ORDER BY reservation.id"
    )
    policy_sql = text(
        "SELECT id, active_count, counter_updated_at FROM public.execution_admission_policies ORDER BY id"
    )
    try:
        with engine.begin() as connection:
            accounting_before = connection.execute(accounting_sql, {"trial": seeded.trial_id}).all()
            policy_before = connection.execute(policy_sql).all()
            connection.exec_driver_sql(
                "CREATE FUNCTION public.inject_retry_fault() RETURNS trigger "
                f"LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$ BEGIN {body} END $$"
            )
            connection.exec_driver_sql(
                f"CREATE TRIGGER {trigger} {timing} UPDATE ON public.trials "
                "FOR EACH ROW EXECUTE FUNCTION public.inject_retry_fault()"
            )
        with TestClient(seeded.app) as client:
            response = client.post(
                f"/trials/{seeded.trial_id}/retry",
                headers=seeded.claim_headers,
                json={
                    "worker_id": str(seeded.worker.worker.worker_id),
                    "failure_reason": "node_setup_health",
                    "retry_after_sec": 0,
                },
            )
        assert response.status_code == 409, response.text
        with engine.begin() as connection:
            assert (
                connection.execute(accounting_sql, {"trial": seeded.trial_id}).all()
                == accounting_before
            )
            assert connection.execute(policy_sql).all() == policy_before
            row = (
                connection.execute(
                    text(
                        "SELECT state, worker_id, attempt_count FROM public.trials WHERE id = :trial"
                    ),
                    {"trial": seeded.trial_id},
                )
                .mappings()
                .one()
            )
            assert dict(row) == {
                "state": "claimed",
                "worker_id": seeded.worker.worker.worker_id,
                "attempt_count": 1,
            }
            assert (
                connection.execute(
                    text("SELECT count(*) FROM loom_capacity_guard.trial_mutation_permits")
                ).scalar_one()
                == 0
            )
            assert (
                connection.execute(
                    text("SELECT count(*) FROM loom_capacity_guard.trial_writer_mutations")
                ).scalar_one()
                == frozen["high_water"]
            )
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM loom_capacity_guard.trial_attempts WHERE trial_id = :trial"
                    ),
                    {"trial": seeded.trial_id},
                ).scalar_one()
                == 1
            )
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM loom_capacity_guard.executable_claim_terminal_events "
                        "WHERE protected_attempt_id = :attempt"
                    ),
                    {"attempt": seeded.first_attempt["protected_attempt_id"]},
                ).scalar_one()
                == 0
            )
            connection.exec_driver_sql(f"DROP TRIGGER {trigger} ON public.trials")
            connection.exec_driver_sql("DROP FUNCTION public.inject_retry_fault()")
        with TestClient(seeded.app) as client:
            clean_retry = client.post(
                f"/trials/{seeded.trial_id}/retry",
                headers=seeded.claim_headers,
                json={
                    "worker_id": str(seeded.worker.worker.worker_id),
                    "failure_reason": "node_setup_health",
                    "retry_after_sec": 0,
                },
            )
        assert clean_retry.status_code == 200, clean_retry.text
    finally:
        engine.dispose()


def test_frozen_retry_wrong_credential_cannot_create_a_permission(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    database = capacity_guard_database
    seeded = _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    initial = asyncio.run(_initialize(database, registration=seeded.worker.registration))
    asyncio.run(_freeze(database, initial["writer_incarnation"], uuid4()))
    headers = {**seeded.claim_headers, "X-Loom-Executor-Worker-Credential": "wrong-credential"}
    with TestClient(seeded.app) as client:
        response = client.post(
            f"/trials/{seeded.trial_id}/retry",
            headers=headers,
            json={
                "worker_id": str(seeded.worker.worker.worker_id),
                "failure_reason": "env_start_failure",
                "retry_after_sec": 0,
            },
        )
    assert response.status_code == 401, response.text
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT count(*) FROM loom_capacity_guard.trial_mutation_permits")
                ).scalar_one()
                == 0
            )
            assert (
                connection.execute(
                    text("SELECT state FROM public.trials WHERE id = :trial"),
                    {"trial": seeded.trial_id},
                ).scalar_one()
                == "claimed"
            )
    finally:
        engine.dispose()


def test_frozen_retry_is_exactly_once_under_concurrency(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    database = capacity_guard_database
    seeded = _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    initial = asyncio.run(_initialize(database, registration=seeded.worker.registration))
    asyncio.run(_freeze(database, initial["writer_incarnation"], uuid4()))
    request = {
        "schema_version": 1,
        "trial_id": str(seeded.trial_id),
        "worker_id": str(seeded.worker.worker.worker_id),
        "failure_reason": "env_start_failure",
        "failure_message": None,
        "retry_after_sec": 0,
    }

    async def race() -> list[object]:
        engine = create_async_engine(
            _value(database, "runtime_url"), isolation_level="SERIALIZABLE"
        )
        store = ProtectedWorkerSessionStore(async_sessionmaker(engine, expire_on_commit=False))

        async def retry() -> object:
            try:
                return await store.retry_claimed_trial(
                    worker_id=seeded.worker.worker.worker_id,
                    worker_credential=_WORKER_CREDENTIAL,
                    retry_request=request,
                )
            except ProtectedWorkerSessionRejected as exc:
                original = getattr(exc.__cause__, "orig", None)
                assert getattr(original, "sqlstate", None) != "40P01", "retry deadlocked"
                return exc

        try:
            return list(await asyncio.wait_for(asyncio.gather(retry(), retry()), timeout=30))
        finally:
            await engine.dispose()

    results = asyncio.run(race())
    assert sum(isinstance(result, dict) for result in results) == 1
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM loom_capacity_guard.trial_attempts WHERE trial_id = :trial"
                    ),
                    {"trial": seeded.trial_id},
                ).scalar_one()
                == 2
            )
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM loom_capacity_guard.executable_claim_terminal_events "
                        "WHERE protected_attempt_id = :attempt"
                    ),
                    {"attempt": seeded.first_attempt["protected_attempt_id"]},
                ).scalar_one()
                == 1
            )
            assert connection.execute(
                text("SELECT state FROM loom_capacity_guard.trial_mutation_permits")
            ).scalars().all() == ["consumed"]
            assert (
                connection.execute(
                    text("SELECT count(*) FROM loom_capacity_guard.trial_writer_mutations")
                ).scalar_one()
                == 0
            )
    finally:
        engine.dispose()
