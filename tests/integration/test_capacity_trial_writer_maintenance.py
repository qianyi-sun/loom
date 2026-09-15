"""Maintenance must not hold terminal authority while waiting for a writer."""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_cli.rollout.operator.protected_staging_capacity_database_component import (
    _AUTHORITY_REBIND_FOUNDATION_SQL,
    _AUTHORITY_REBIND_LOCK_STATEMENT,
)
from tests.integration.test_capacity_agent_store import (
    _initialize_and_register,
    _seed_trial,
    _value,
)
from tests.integration.test_capacity_guard_migrations import _owner_connection
from tests.integration.test_capacity_trial_writer_fence import _legacy_engine


def _maintenance_locks(database: dict[str, object]) -> str:
    return _AUTHORITY_REBIND_LOCK_STATEMENT.replace(
        "'loom_cap_staging_owner'", "'" + _value(database, "owner_role") + "'"
    )


def test_maintenance_refuses_writer_without_waiting_or_retaining_partial_locks(
    capacity_guard_database: dict[str, object],
) -> None:
    database = capacity_guard_database
    trial = _seed_trial(database)
    engine = _legacy_engine(database)
    try:
        with engine.begin() as writer:
            writer.execute(
                text("UPDATE public.trials SET submit_priority=101 WHERE id=:id"),
                {"id": trial},
            )
            with pytest.raises(DBAPIError) as busy:
                with _owner_connection(database) as maintenance:
                    # Bound the pre-fix blocking regression. A genuine NOWAIT
                    # refusal has a different message than this lock timeout.
                    maintenance.execute(text("SET LOCAL lock_timeout='250ms'"))
                    maintenance.execute(text(_maintenance_locks(database)))
            assert busy.value.orig.sqlstate == "55P03"
            assert "could not obtain lock" in str(busy.value.orig)
            # Failed maintenance must release earlier locks in the set, allowing
            # the terminal bridge's authority lock to make progress.
            with _owner_connection(database) as terminal:
                terminal.execute(
                    text(
                        "SELECT singleton_id FROM loom_capacity_guard.agent_runtime_authority "
                        "FOR UPDATE NOWAIT"
                    )
                ).scalar_one()
    finally:
        engine.dispose()


def test_maintenance_cannot_cross_an_inflight_retry_permission(
    capacity_guard_database: dict[str, object],
) -> None:
    # Hold the same relation lock acquired when a retry inserts its private
    # permission. Maintenance must refuse rather than ignore this new evidence.
    with _owner_connection(capacity_guard_database) as retry:
        retry.execute(text(
            "LOCK TABLE loom_capacity_guard.trial_retry_mutation_permits IN ROW EXCLUSIVE MODE"
        ))
        with pytest.raises(DBAPIError) as busy:
            with _owner_connection(capacity_guard_database) as maintenance:
                maintenance.execute(text("SET LOCAL lock_timeout='250ms'"))
                maintenance.execute(text(_maintenance_locks(capacity_guard_database)))
        assert busy.value.orig.sqlstate == "55P03"
        assert "could not obtain lock" in str(busy.value.orig)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["observe", "lock"])
@pytest.mark.parametrize("drift", ["descendant", "ancestor", "owner"])
async def test_retry_maintenance_does_not_read_or_lock_foreign_descendants(
    capacity_guard_database: dict[str, object], operation: str, drift: str,
) -> None:
    from sqlalchemy import create_engine

    database = capacity_guard_database
    await _initialize_and_register(database)
    statement = _AUTHORITY_REBIND_FOUNDATION_SQL if operation == "observe" else _AUTHORITY_REBIND_LOCK_STATEMENT
    for kind in ("owner", "agent", "executor", "observer", "runtime"):
        statement = statement.replace(f"'loom_cap_staging_{kind}'", "'" + _value(database, f"{kind}_role") + "'")
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.begin() as admin:
            admin.exec_driver_sql("CREATE SCHEMA foreign_scope")
            if drift == "descendant":
                admin.exec_driver_sql(
                    "CREATE TABLE foreign_scope.retry_child () INHERITS "
                    "(loom_capacity_guard.trial_retry_mutation_permits)"
                )
            else:
                admin.exec_driver_sql("CREATE TABLE foreign_scope.retry_child ()")
                if drift == "ancestor":
                    admin.exec_driver_sql(
                        "ALTER TABLE loom_capacity_guard.trial_retry_mutation_permits "
                        "INHERIT foreign_scope.retry_child"
                    )
                else:
                    admin.exec_driver_sql(
                        "ALTER TABLE loom_capacity_guard.trial_retry_mutation_permits OWNER TO CURRENT_USER"
                    )
        with engine.begin() as foreign:
            foreign.exec_driver_sql("LOCK TABLE ONLY foreign_scope.retry_child IN ACCESS EXCLUSIVE MODE")
            with engine.begin() as maintenance:
                maintenance.exec_driver_sql("SET LOCAL lock_timeout='250ms'")
                if operation == "observe":
                    assert maintenance.exec_driver_sql(statement).scalar_one() == "drifted"
                else:
                    with pytest.raises(DBAPIError) as refusal:
                        maintenance.exec_driver_sql(statement)
                    assert refusal.value.orig.sqlstate == "55000"
    finally:
        engine.dispose()
