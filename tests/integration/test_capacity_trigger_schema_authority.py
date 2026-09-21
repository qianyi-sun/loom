"""Real application triggers must work with a non-superuser definer."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from loom.db.schema import Worker
from tests.integration.test_capacity_agent_migrations import _guard_config, _value
from tests.support.historical_capacity import _seed_claimed_protected_trial, seed_unprotected_trial

_BRIDGES = (
    (
        "public.loom_close_protected_runtime_trial_claim()",
        "loom_capacity_guard.close_protected_runtime_trial_claim(uuid,text,text,uuid,integer)",
    ),
    (
        "public.loom_transform_protected_runtime_trial_requeue()",
        "loom_capacity_guard.transform_protected_runtime_trial_requeue"
        "(uuid,text,uuid,integer,uuid,integer,text,text,timestamp with time zone)",
    ),
)


@pytest.mark.parametrize("operation", ["terminal", "requeue"])
def test_schema_resolution_does_not_authorize_false_live_claim_closure(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    operation: str,
) -> None:
    """The newly resolvable helpers still enforce real state and terminal evidence."""
    seeded = _seed_claimed_protected_trial(capacity_guard_database, monkeypatch, tmp_path)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    role = f"application_trigger_{uuid4().hex[:16]}"
    quote = engine.dialect.identifier_preparer.quote
    quoted_role = quote(role)
    owners: dict[str, str] = {}
    snapshot = text(
        "SELECT trial.state, trial.worker_id, trial.attempt_count, "
        "(SELECT count(*) FROM loom_capacity_guard.trial_attempts AS attempt "
        " WHERE attempt.trial_id=trial.id) AS attempts, "
        "(SELECT count(*) FROM loom_capacity_guard.executable_claim_terminal_events "
        " WHERE protected_attempt_id=:attempt) AS terminals "
        "FROM public.trials AS trial WHERE trial.id=:trial"
    )
    parameters = {
        "trial": seeded.trial_id,
        "attempt": seeded.first_attempt["protected_attempt_id"],
    }
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f"CREATE ROLE {quoted_role} NOLOGIN NOSUPERUSER NOINHERIT NOBYPASSRLS"
            )
            for bridge, guarded in _BRIDGES:
                owners[bridge] = connection.execute(
                    text(
                        "SELECT pg_get_userbyid(proowner) FROM pg_proc "
                        "WHERE oid=to_regprocedure(:signature)"
                    ),
                    {"signature": bridge},
                ).scalar_one()
                connection.exec_driver_sql(f"ALTER FUNCTION {bridge} OWNER TO {quoted_role}")
                connection.exec_driver_sql(f"GRANT EXECUTE ON FUNCTION {guarded} TO {quoted_role}")

        cfg = _guard_config(capacity_guard_database)
        command.downgrade(cfg, "guard_0029")
        command.upgrade(cfg, "head")
        with engine.connect() as connection:
            before = tuple(connection.execute(snapshot, parameters).one())
            assert before == ("claimed", seeded.worker.worker.worker_id, 1, 1, 0)
            assert connection.execute(
                text("SELECT has_schema_privilege(:role, 'loom_capacity_guard', 'USAGE')"),
                {"role": role},
            ).scalar_one()

        with engine.begin() as connection:
            connection.exec_driver_sql(f"SET LOCAL ROLE {quoted_role}")
            call_parameters = {"trial": seeded.trial_id, "worker": before[1]}
            if operation == "terminal":
                with pytest.raises(DBAPIError, match="protected terminal transition is not exact"):
                    with connection.begin_nested():
                        connection.execute(
                            text(
                                "SELECT loom_capacity_guard.close_protected_runtime_trial_claim("
                                ":trial, 'claimed', 'succeeded', :worker, 1)"
                            ),
                            call_parameters,
                        )
            else:
                retained = connection.execute(
                    text(
                        "SELECT loom_capacity_guard.transform_protected_runtime_trial_requeue("
                        ":trial, 'claimed', :worker, 1, NULL, 1, 'worker_lost', "
                        "'unproven caller assertion', NOW() + interval '1 minute')"
                    ),
                    call_parameters,
                ).scalar_one()
                assert retained["state"] == "retained"
                assert retained["executable"] is False

        with engine.connect() as connection:
            assert tuple(connection.execute(snapshot, parameters).one()) == before

        # The negative cases must not pass merely because the bridge is broken:
        # a real public terminal update invokes the same non-superuser definer.
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE public.trials SET result='{}'::jsonb, state='succeeded' WHERE id=:trial"
                ),
                {"trial": seeded.trial_id},
            )
        with engine.connect() as connection:
            assert tuple(connection.execute(snapshot, parameters).one()) == (
                "succeeded",
                before[1],
                1,
                1,
                1,
            )
    finally:
        with engine.begin() as connection:
            for bridge, guarded in _BRIDGES:
                if bridge in owners:
                    connection.exec_driver_sql(
                        f"ALTER FUNCTION {bridge} OWNER TO {quote(owners[bridge])}"
                    )
                    connection.exec_driver_sql(
                        f"REVOKE EXECUTE ON FUNCTION {guarded} FROM {quoted_role}"
                    )
            connection.exec_driver_sql(
                f"REVOKE USAGE ON SCHEMA loom_capacity_guard FROM {quoted_role}"
            )
            connection.exec_driver_sql(f"DROP ROLE IF EXISTS {quoted_role}")
        engine.dispose()




@pytest.mark.asyncio
@pytest.mark.parametrize("next_state", ["succeeded", "queued"])
async def test_application_trigger_owner_can_finish_or_requeue_without_table_authority(
    capacity_guard_database: dict[str, object], next_state: str
) -> None:
    """Catch missing schema resolution authority masked by superuser migration fixtures."""
    trial_id = seed_unprotected_trial(capacity_guard_database)
    worker_id = uuid4()
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    role = f"application_trigger_{uuid4().hex[:16]}"
    quote = engine.dialect.identifier_preparer.quote
    quoted_role = quote(role)
    owners: dict[str, str] = {}
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f"CREATE ROLE {quoted_role} NOLOGIN NOSUPERUSER NOINHERIT NOBYPASSRLS"
            )
            connection.execute(
                Worker.__table__.insert().values(
                    id=worker_id,
                    hostname="trigger-test",
                    version="test",
                    capabilities=[],
                    registered_at=datetime.now(UTC),
                    last_seen_at=datetime.now(UTC),
                    status="active",
                )
            )
            connection.execute(
                text(
                    "UPDATE public.trials SET state='running', worker_id=:worker, "
                    "attempt_count=1 WHERE id=:trial"
                ),
                {"worker": worker_id, "trial": trial_id},
            )
            for bridge, guarded in _BRIDGES:
                owners[bridge] = connection.execute(
                    text(
                        "SELECT pg_get_userbyid(proowner) FROM pg_proc "
                        "WHERE oid=to_regprocedure(:signature)"
                    ),
                    {"signature": bridge},
                ).scalar_one()
                connection.exec_driver_sql(f"ALTER FUNCTION {bridge} OWNER TO {quoted_role}")
                # Match installed guard_0029: EXECUTE was granted, but USAGE was not.
                connection.exec_driver_sql(f"GRANT EXECUTE ON FUNCTION {guarded} TO {quoted_role}")
            assert not connection.execute(
                text("SELECT has_schema_privilege(:role, 'loom_capacity_guard', 'USAGE')"),
                {"role": role},
            ).scalar_one()

        cfg = _guard_config(capacity_guard_database)
        command.downgrade(cfg, "guard_0029")
        command.upgrade(cfg, "head")

        with engine.begin() as connection:
            observed = connection.execute(
                text(
                    "UPDATE public.trials SET state=CAST(:state AS text), result='{}'::jsonb "
                    " ,worker_id=CASE WHEN :state='queued' THEN NULL ELSE worker_id END "
                    " ,next_attempt_at=CASE WHEN :state='queued' "
                    " THEN NOW() + interval '1 minute' ELSE NULL END "
                    "WHERE id=:trial RETURNING state"
                ),
                {"state": next_state, "trial": trial_id},
            ).scalar_one()
            assert observed == next_state

        # Resolving the two already-authorized calls must not authorize guard data
        # access or any general admission/claim procedure.
        with engine.begin() as connection:
            connection.exec_driver_sql(f"SET LOCAL ROLE {quoted_role}")
            with pytest.raises(DBAPIError, match="permission denied for table"):
                with connection.begin_nested():
                    connection.exec_driver_sql("SELECT * FROM loom_capacity_guard.trial_attempts")
            assert not connection.execute(
                text(
                    "SELECT has_function_privilege(current_user, "
                    "'loom_capacity_guard.admit_executable_claim(uuid,uuid,jsonb,bytea,text)', "
                    "'EXECUTE')"
                )
            ).scalar_one()
    finally:
        with engine.begin() as connection:
            for bridge, guarded in _BRIDGES:
                if bridge in owners:
                    connection.exec_driver_sql(
                        f"ALTER FUNCTION {bridge} OWNER TO {quote(owners[bridge])}"
                    )
                    connection.exec_driver_sql(
                        f"REVOKE EXECUTE ON FUNCTION {guarded} FROM {quoted_role}"
                    )
            connection.exec_driver_sql(
                f"REVOKE USAGE ON SCHEMA loom_capacity_guard FROM {quoted_role}"
            )
            connection.exec_driver_sql(f"DROP ROLE IF EXISTS {quoted_role}")
        engine.dispose()
