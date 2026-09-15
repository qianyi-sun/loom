"""Maintenance must not hold terminal authority while waiting for a writer."""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_cli.rollout.operator.protected_staging_capacity_database_component import (
    _AUTHORITY_REBIND_LOCK_STATEMENT,
)
from tests.integration.test_capacity_agent_store import _seed_trial
from tests.integration.test_capacity_guard_migrations import _owner_connection
from tests.integration.test_capacity_trial_writer_fence import _legacy_engine


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
                    maintenance.execute(text(_AUTHORITY_REBIND_LOCK_STATEMENT))
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
                maintenance.execute(text(_AUTHORITY_REBIND_LOCK_STATEMENT))
        assert busy.value.orig.sqlstate == "55P03"
        assert "could not obtain lock" in str(busy.value.orig)
