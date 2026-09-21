"""Scoped database admission closure and ordered non-signalling drain checks."""

import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta

import psycopg
import pytest
from psycopg import sql

from loom.application_database_admission import (
    ApplicationDatabaseHandoffBackend,
    capture_application_database_admission,
    close_application_database_admission,
    reopen_application_database_admission,
    require_application_database_drained,
)
from loom.application_login_sealing import seal_application_login
from tests.integration.test_application_login_sealing import login_database  # noqa: F401
from tests.integration.test_application_ownership_transfer import (
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)

pytestmark = pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)


def _maintenance(admin):
    return psycopg.connect(
        admin.info.dsn, password=admin.info.password, dbname="postgres", autocommit=True
    )


def _handoff(admin):
    row = admin.execute(
        "SELECT a.pid,a.backend_start::text,s.system_identifier::text,pg_postmaster_start_time()::text,a.datid::bigint "
        "FROM pg_catalog.pg_stat_activity a CROSS JOIN pg_catalog.pg_control_system() s WHERE a.pid=pg_backend_pid()"
    ).fetchone()
    return ApplicationDatabaseHandoffBackend(
        pid=row[0],
        started_at=row[1],
        system_identifier=row[2],
        server_started_at=row[3],
        database_oid=row[4],
    )


def _target(admin, maintenance, database, role, successor, provisioner):
    # This fixture's spare role defaults to INHERIT; a protected owner may not.
    admin.execute(sql.SQL("ALTER ROLE {} NOINHERIT").format(sql.Identifier(successor)))
    seal_application_login(admin, database=database, role=role, provisioner_role=provisioner)
    return capture_application_database_admission(
        maintenance,
        database=database,
        owner_role=role,
        successor_role=successor,
        provisioner_role=provisioner,
        handoff_backend=_handoff(admin),
    )


def test_admission_closes_only_target_and_recovers_from_a_fresh_maintenance_connection(
    login_database,  # noqa: F811
):
    admin, database, role, successor, _, provisioner = login_database
    with _maintenance(admin) as maintenance:
        target = _target(admin, maintenance, database, role, successor, provisioner)
        handoff = _handoff(admin)
        close_application_database_admission(
            maintenance, target=target, provisioner_role=provisioner
        )
        close_application_database_admission(
            maintenance, target=target, provisioner_role=provisioner
        )
        with pytest.raises(psycopg.OperationalError):
            with psycopg.connect(admin.info.dsn, password=admin.info.password, connect_timeout=2):
                pass
        assert admin.execute("SELECT 42").fetchone() == (42,)
        with _maintenance(admin) as unrelated:
            assert unrelated.execute("SELECT 43").fetchone() == (43,)
        require_application_database_drained(
            maintenance, target=target, provisioner_role=provisioner, handoff_backend=handoff
        )
    # Local control loss cannot strand the target: exact saved identity is reused.
    with _maintenance(admin) as recovered:
        reopen_application_database_admission(
            recovered, target=target, provisioner_role=provisioner
        )
        reopen_application_database_admission(
            recovered, target=target, provisioner_role=provisioner
        )
        with psycopg.connect(admin.info.dsn, password=admin.info.password) as fresh:
            assert fresh.execute("SELECT 44").fetchone() == (44,)


def test_drain_refuses_existing_client_without_terminating_it(login_database):  # noqa: F811
    admin, database, role, successor, client_url, provisioner = login_database
    with (
        psycopg.connect(client_url, autocommit=True) as existing,
        _maintenance(admin) as maintenance,
    ):
        target = _target(admin, maintenance, database, role, successor, provisioner)
        handoff = _handoff(admin)
        close_application_database_admission(
            maintenance, target=target, provisioner_role=provisioner
        )
        try:
            with pytest.raises(RuntimeError, match="sessions"):
                require_application_database_drained(
                    maintenance,
                    target=target,
                    provisioner_role=provisioner,
                    handoff_backend=handoff,
                )
            assert existing.execute("SELECT 42").fetchone() == (42,)
            existing.close()
            require_application_database_drained(
                maintenance, target=target, provisioner_role=provisioner, handoff_backend=handoff
            )
        finally:
            reopen_application_database_admission(
                maintenance, target=target, provisioner_role=provisioner
            )


def test_recovery_drain_requires_lost_handoff_and_keeps_admission_closed(login_database):  # noqa: F811
    from loom.application_database_admission import require_application_database_recovery_drained

    admin, database, role, successor, _, provisioner = login_database
    with _maintenance(admin) as maintenance:
        target = _target(admin, maintenance, database, role, successor, provisioner)
        handoff = _handoff(admin)
        arguments = dict(target=target, provisioner_role=provisioner, handoff_backend=handoff)
        with pytest.raises(RuntimeError, match="not closed"):
            require_application_database_recovery_drained(maintenance, **arguments)
        close_application_database_admission(maintenance, target=target, provisioner_role=provisioner)
        with pytest.raises(RuntimeError, match="sessions"):
            require_application_database_recovery_drained(maintenance, **arguments)
        assert admin.execute("SELECT 42").fetchone() == (42,)
        admin.close()
        require_application_database_recovery_drained(maintenance, **arguments)
        require_application_database_recovery_drained(maintenance, **arguments)
        assert maintenance.execute("SELECT datallowconn FROM pg_database WHERE datname=%s", (database,)).fetchone() == (False,)
        # Loss recovery must not make the ordinary transfer drain accept a dead peer.
        with pytest.raises(RuntimeError, match="sessions"):
            require_application_database_drained(maintenance, **arguments)
        with pytest.raises(RuntimeError, match="server identity"):
            require_application_database_recovery_drained(
                maintenance, **{**arguments, "handoff_backend": replace(handoff, server_started_at="2000-01-01T00:00:00+00:00")},
            )


def test_recovery_reopen_admits_new_peer_without_restoring_runtime_login(login_database):  # noqa: F811
    from loom.application_database_admission import reopen_application_database_for_handoff_recovery

    admin, database, role, successor, client_url, provisioner = login_database
    dsn, password = admin.info.dsn, admin.info.password
    with _maintenance(admin) as maintenance:
        target = _target(admin, maintenance, database, role, successor, provisioner)
        original = _handoff(admin)
        arguments = dict(target=target, provisioner_role=provisioner, handoff_backend=original)
        close_application_database_admission(maintenance, target=target, provisioner_role=provisioner)
        with pytest.raises(RuntimeError, match="sessions"):
            reopen_application_database_for_handoff_recovery(maintenance, **arguments)
        admin.close()
        reopen_application_database_for_handoff_recovery(maintenance, **arguments)
        assert maintenance.execute("SELECT datallowconn FROM pg_database WHERE datname=%s", (database,)).fetchone() == (True,)
        assert maintenance.execute("SELECT count(*) FROM pg_roles WHERE rolname=ANY(%s) AND rolcanlogin", ([role, successor],)).fetchone() == (0,)
        with pytest.raises(psycopg.OperationalError):
            psycopg.connect(client_url, connect_timeout=2).close()
        # Open admission is not silently adopted on an uncertain retry.
        with pytest.raises(RuntimeError, match="not closed"):
            reopen_application_database_for_handoff_recovery(maintenance, **arguments)
        with psycopg.connect(dsn, password=password, autocommit=True) as replacement:
            current = _handoff(replacement)
            assert current != original
            close_application_database_admission(maintenance, target=target, provisioner_role=provisioner)
            require_application_database_drained(
                maintenance, target=target, provisioner_role=provisioner, handoff_backend=current,
            )


@pytest.mark.parametrize("reclose", [False, True])
@pytest.mark.parametrize("lose_guard", [False, True])
def test_recovery_reopen_preserves_guard_or_rolls_back_admission(login_database, lose_guard, reclose):  # noqa: F811
    from loom.application_database_admission import (
        capture_application_coordination_guard,
        reopen_application_database_for_handoff_recovery,
    )
    from loom.staging_mutation_coordination import (
        STAGING_MUTATION_TRY_LOCK_SQL,
        rollout_guard_application_name,
        rollout_guard_bind_sql,
    )

    admin, database, role, successor, _, provisioner = login_database
    admin.execute("CREATE ROLE loom_rollout_readonly LOGIN NOINHERIT PASSWORD 'test-only-guard'")
    request = dict(request_id="req-test-reopen", candidate_sha="a" * 40,
                   candidate_tree="b" * 40, generation="c" * 32)
    with _maintenance(admin) as maintenance:
        try:
            with psycopg.connect(admin.info.dsn, user="loom_rollout_readonly", password="test-only-guard", autocommit=True) as guard:
                guard.execute(rollout_guard_bind_sql(rollout_guard_application_name(**request)))
                assert guard.execute(STAGING_MUTATION_TRY_LOCK_SQL).fetchone() == (True,)
                target = _target(admin, maintenance, database, role, successor, provisioner)
                original = _handoff(admin)
                captured = capture_application_coordination_guard(
                    maintenance, target=target, provisioner_role=provisioner,
                    backend_pid=guard.info.backend_pid, **request,
                )
                close_application_database_admission(maintenance, target=target, provisioner_role=provisioner)
                admin.close()

                class Connection:
                    @property
                    def info(self):
                        return maintenance.info

                    def transaction(self):
                        return maintenance.transaction()

                    def execute(self, query):
                        result = maintenance.execute(query)
                        if lose_guard and isinstance(query, sql.Composed) and query.as_string(maintenance).startswith("ALTER DATABASE"):
                            guard.execute("SELECT pg_advisory_unlock(5498691230183247727)")
                        return result

                arguments = dict(target=target, provisioner_role=provisioner,
                                 handoff_backend=original, coordination_guard=captured)
                operation = reopen_application_database_for_handoff_recovery
                if reclose:
                    from loom.application_database_admission import (
                        reclose_application_database_for_handoff_recovery,
                    )
                    reopen_application_database_for_handoff_recovery(maintenance, **arguments)
                    operation = reclose_application_database_for_handoff_recovery
                if lose_guard:
                    with pytest.raises(RuntimeError, match="coordination guard"):
                        operation(Connection(), **arguments)
                else:
                    operation(Connection(), **arguments)
                    assert guard.execute("SELECT pg_advisory_unlock(5498691230183247727)").fetchone() == (True,)
                assert maintenance.execute("SELECT datallowconn FROM pg_database WHERE datname=%s", (database,)).fetchone() == (lose_guard if reclose else not lose_guard,)
                assert maintenance.execute("SELECT count(*) FROM pg_roles WHERE rolname=ANY(%s) AND rolcanlogin", ([role, successor],)).fetchone() == (0,)
        finally:
            maintenance.execute("DROP ROLE loom_rollout_readonly")


def test_recovery_reclose_preserves_unknown_peer_but_refuses_to_drain_it(login_database):  # noqa: F811
    from loom.application_database_admission import (
        reclose_application_database_for_handoff_recovery,
        reopen_application_database_for_handoff_recovery,
        require_application_database_recovery_drained,
    )

    admin, database, role, successor, _, provisioner = login_database
    dsn, password = admin.info.dsn, admin.info.password
    with _maintenance(admin) as maintenance:
        target = _target(admin, maintenance, database, role, successor, provisioner)
        original = _handoff(admin)
        arguments = dict(target=target, provisioner_role=provisioner, handoff_backend=original)
        close_application_database_admission(maintenance, target=target, provisioner_role=provisioner)
        admin.close()
        reopen_application_database_for_handoff_recovery(maintenance, **arguments)
        with psycopg.connect(dsn, password=password, autocommit=True) as unknown:
            with pytest.raises(RuntimeError, match="server identity"):
                reclose_application_database_for_handoff_recovery(
                    maintenance, **{**arguments, "handoff_backend": replace(
                        original, server_started_at="2000-01-01T00:00:00+00:00",
                    )},
                )
            assert maintenance.execute("SELECT datallowconn FROM pg_database WHERE datname=%s", (database,)).fetchone() == (True,)
            reclose_application_database_for_handoff_recovery(maintenance, **arguments)
            reclose_application_database_for_handoff_recovery(maintenance, **arguments)
            assert maintenance.execute("SELECT datallowconn FROM pg_database WHERE datname=%s", (database,)).fetchone() == (False,)
            with pytest.raises(RuntimeError, match="sessions"):
                require_application_database_recovery_drained(maintenance, **arguments)
            assert unknown.execute("SELECT 42").fetchone() == (42,)
        require_application_database_recovery_drained(maintenance, **arguments)


def test_recovery_reclose_cannot_race_an_uncommitted_prior_reopen(login_database):  # noqa: F811
    from loom.application_database_admission import (
        reclose_application_database_for_handoff_recovery,
    )

    admin, database, role, successor, _, provisioner = login_database
    with _maintenance(admin) as maintenance, _maintenance(admin) as prior:
        target = _target(admin, maintenance, database, role, successor, provisioner)
        handoff = _handoff(admin)
        arguments = dict(target=target, provisioner_role=provisioner, handoff_backend=handoff)
        close_application_database_admission(maintenance, target=target, provisioner_role=provisioner)
        with prior.transaction():
            prior.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS true").format(sql.Identifier(database)))
            # Another maintenance connection sees the old committed false. That
            # snapshot is NOT closure evidence while this reopen can still commit.
            assert maintenance.execute("SELECT datallowconn FROM pg_database WHERE datname=%s", (database,)).fetchone() == (False,)
            with pytest.raises(psycopg.errors.LockNotAvailable):
                reclose_application_database_for_handoff_recovery(maintenance, **arguments)
        assert maintenance.execute("SELECT datallowconn FROM pg_database WHERE datname=%s", (database,)).fetchone() == (True,)
        reclose_application_database_for_handoff_recovery(maintenance, **arguments)
        assert maintenance.execute("SELECT datallowconn FROM pg_database WHERE datname=%s", (database,)).fetchone() == (False,)


def test_recovery_drain_preserves_exact_guard_and_refuses_other_clients(login_database):  # noqa: F811
    from loom.application_database_admission import (
        capture_application_coordination_guard,
        require_application_database_recovery_drained,
    )
    from loom.staging_mutation_coordination import (
        STAGING_MUTATION_TRY_LOCK_SQL,
        rollout_guard_application_name,
        rollout_guard_bind_sql,
    )

    admin, database, role, successor, client_url, provisioner = login_database
    admin.execute("CREATE ROLE loom_rollout_readonly LOGIN NOINHERIT PASSWORD 'test-only-guard'")
    request = dict(request_id="req-test-recovery", candidate_sha="a" * 40,
                   candidate_tree="b" * 40, generation="c" * 32)
    with _maintenance(admin) as maintenance:
        try:
            with psycopg.connect(admin.info.dsn, user="loom_rollout_readonly", password="test-only-guard", autocommit=True) as guard, psycopg.connect(client_url, autocommit=True) as existing:
                guard.execute(rollout_guard_bind_sql(rollout_guard_application_name(**request)))
                assert guard.execute(STAGING_MUTATION_TRY_LOCK_SQL).fetchone() == (True,)
                target = _target(admin, maintenance, database, role, successor, provisioner)
                handoff = _handoff(admin)
                captured = capture_application_coordination_guard(
                    maintenance, target=target, provisioner_role=provisioner,
                    backend_pid=guard.info.backend_pid, **request,
                )
                arguments = dict(target=target, provisioner_role=provisioner,
                                 handoff_backend=handoff, coordination_guard=captured)
                close_application_database_admission(maintenance, target=target, provisioner_role=provisioner)
                admin.close()
                with pytest.raises(RuntimeError, match="sessions"):
                    require_application_database_recovery_drained(maintenance, **arguments)
                assert existing.execute("SELECT 42").fetchone() == (42,)
                existing.close()
                require_application_database_recovery_drained(maintenance, **arguments)
                assert guard.execute("SELECT 43").fetchone() == (43,)

                class LoseGuardAfterSessionInventory:
                    @property
                    def info(self):
                        return maintenance.info

                    def transaction(self):
                        return maintenance.transaction()

                    def execute(self, query):
                        result = maintenance.execute(query)
                        if "pg_prepared_xacts" in str(query):
                            guard.execute("SELECT pg_advisory_unlock(5498691230183247727)")
                        return result

                with pytest.raises(RuntimeError, match="coordination guard"):
                    require_application_database_recovery_drained(LoseGuardAfterSessionInventory(), **arguments)
                with pytest.raises(RuntimeError, match="coordination guard"):
                    require_application_database_recovery_drained(maintenance, **arguments)
                assert guard.execute("SELECT pg_try_advisory_lock(1280263818,1621151599)").fetchone() == (True,)
                with pytest.raises(RuntimeError, match="coordination guard"):
                    require_application_database_recovery_drained(maintenance, **arguments)
                guard.close()
                with pytest.raises(RuntimeError, match="coordination guard"):
                    require_application_database_recovery_drained(maintenance, **arguments)
        finally:
            maintenance.execute("DROP ROLE loom_rollout_readonly")


def test_drain_preserves_the_exact_rollout_coordination_guard(login_database):  # noqa: F811
    from loom.application_database_admission import capture_application_coordination_guard
    from loom.staging_mutation_coordination import (
        STAGING_MUTATION_HEALTH_SQL,
        STAGING_MUTATION_TRY_LOCK_SQL,
        rollout_guard_application_name,
        rollout_guard_bind_sql,
    )

    admin, database, role, successor, _, provisioner = login_database
    guard_role = "loom_rollout_readonly"
    admin.execute("CREATE ROLE loom_rollout_readonly LOGIN NOINHERIT PASSWORD 'test-only-guard'")
    request = dict(
        request_id="req-test-guard", candidate_sha="a" * 40, candidate_tree="b" * 40,
        generation="c" * 32,
    )
    application_name = rollout_guard_application_name(**request)
    try:
        with psycopg.connect(admin.info.dsn, user=guard_role, password="test-only-guard", autocommit=True) as guard, _maintenance(admin) as maintenance:
            guard.execute(rollout_guard_bind_sql(application_name))
            assert guard.execute(STAGING_MUTATION_TRY_LOCK_SQL).fetchone() == (True,)
            target = _target(admin, maintenance, database, role, successor, provisioner)
            handoff = _handoff(admin)
            captured = capture_application_coordination_guard(
                maintenance, target=target, provisioner_role=provisioner,
                backend_pid=guard.info.backend_pid, **request,
            )
            saved = captured
            extra = psycopg.connect(admin.info.dsn, user=guard_role, password="test-only-guard", autocommit=True)
            close_application_database_admission(maintenance, target=target, provisioner_role=provisioner)
            try:
                with pytest.raises(RuntimeError, match="requires open admission"):
                    capture_application_coordination_guard(
                        maintenance, target=target, provisioner_role=provisioner,
                        backend_pid=guard.info.backend_pid, **request,
                    )
                with pytest.raises(RuntimeError, match="sessions"):
                    require_application_database_drained(
                        maintenance, target=target, provisioner_role=provisioner,
                        handoff_backend=handoff, coordination_guard=saved,
                    )
                assert extra.execute("SELECT 41").fetchone() == (41,)
                extra.close()
                # Keeping the actual request coordination lock is mandatory; it
                # cannot be dropped just to make the database look empty.
                require_application_database_drained(
                    maintenance, target=target, provisioner_role=provisioner, handoff_backend=handoff,
                    coordination_guard=saved,
                )
                assert guard.execute("SELECT 42").fetchone() == (42,)
                for wrong in (
                    replace(saved, role_oid=saved.role_oid + 1),
                    replace(saved, application_name="loom-rollout-guard-" + "d" * 40),
                    replace(saved, backend=replace(saved.backend, pid=saved.backend.pid + 100000)),
                    replace(saved, backend=replace(saved.backend, started_at="2000-01-01T00:00:00+00:00")),
                    replace(saved, backend=replace(saved.backend, server_started_at="2000-01-01T00:00:00+00:00")),
                ):
                    with pytest.raises(RuntimeError, match="coordination guard"):
                        require_application_database_drained(
                            maintenance, target=target, provisioner_role=provisioner,
                            handoff_backend=handoff, coordination_guard=wrong,
                        )
                admin.execute("CREATE TABLE public.coordination_lock_test(id integer)")
                with admin.transaction():
                    admin.execute("LOCK TABLE public.coordination_lock_test IN ACCESS EXCLUSIVE MODE")
                    guard.execute("SET statement_timeout='1s'")
                    assert guard.execute(STAGING_MUTATION_HEALTH_SQL).fetchone() == (saved.backend.pid, True)
                guard.execute("SELECT pg_advisory_unlock(5498691230183247727)")
                with pytest.raises(RuntimeError, match="coordination guard"):
                    require_application_database_drained(
                        maintenance, target=target, provisioner_role=provisioner,
                        handoff_backend=handoff, coordination_guard=saved,
                    )
                # The two-int key shares classid/objid but is a DIFFERENT lock
                # (objsubid=2); it must never substitute for lost coordination.
                assert guard.execute("SELECT pg_try_advisory_lock(1280263818,1621151599)").fetchone() == (True,)
                with pytest.raises(RuntimeError, match="coordination guard"):
                    require_application_database_drained(
                        maintenance, target=target, provisioner_role=provisioner,
                        handoff_backend=handoff, coordination_guard=saved,
                    )
                guard.close()
                with pytest.raises(RuntimeError, match="coordination guard"):
                    require_application_database_drained(
                        maintenance, target=target, provisioner_role=provisioner,
                        handoff_backend=handoff, coordination_guard=saved,
                    )
            finally:
                extra.close()
                reopen_application_database_admission(maintenance, target=target, provisioner_role=provisioner)
    finally:
        admin.execute("DROP ROLE loom_rollout_readonly")


@pytest.mark.parametrize(
    "drift", ["database_oid", "system_identifier", "owner_oid", "successor_oid", "database"]
)
def test_recovery_refuses_identity_drift_without_opening_the_database(login_database, drift):  # noqa: F811
    admin, database, role, successor, _, provisioner = login_database
    with _maintenance(admin) as maintenance:
        target = _target(admin, maintenance, database, role, successor, provisioner)
        close_application_database_admission(
            maintenance, target=target, provisioner_role=provisioner
        )
        before = maintenance.execute(
            "SELECT datallowconn FROM pg_database WHERE datname=%s", (database,)
        ).fetchone()
        value = getattr(target, drift)
        changed = replace(target, **{drift: value + 1000 if type(value) is int else value + "1"})
        try:
            with pytest.raises(RuntimeError):
                reopen_application_database_admission(
                    maintenance, target=changed, provisioner_role=provisioner
                )
            assert (
                maintenance.execute(
                    "SELECT datallowconn FROM pg_database WHERE datname=%s", (database,)
                ).fetchone()
                == before
                == (False,)
            )
        finally:
            reopen_application_database_admission(
                maintenance, target=target, provisioner_role=provisioner
            )


def test_capture_does_not_adopt_an_already_closed_database(login_database):  # noqa: F811
    admin, database, role, successor, _, provisioner = login_database
    with _maintenance(admin) as maintenance:
        target = _target(admin, maintenance, database, role, successor, provisioner)
        close_application_database_admission(
            maintenance, target=target, provisioner_role=provisioner
        )
        try:
            with pytest.raises(RuntimeError, match="already closed"):
                capture_application_database_admission(
                    maintenance,
                    database=database,
                    owner_role=role,
                    successor_role=successor,
                    provisioner_role=provisioner,
                    handoff_backend=_handoff(admin),
                )
        finally:
            reopen_application_database_admission(
                maintenance, target=target, provisioner_role=provisioner
            )


@pytest.mark.parametrize("recovery", [False, True])
def test_drain_refuses_prepublication_startup_then_accepts_only_after_it_exits(login_database, recovery):  # noqa: F811
    from loom.application_database_admission import require_application_database_recovery_drained

    drain = require_application_database_recovery_drained if recovery else require_application_database_drained
    admin, database, role, successor, client_url, provisioner = login_database
    with _maintenance(admin) as maintenance, ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            psycopg.connect,
            client_url,
            autocommit=True,
            connect_timeout=10,
            options="-c post_auth_delay=3",
        )
        target = None
        try:
            deadline = time.monotonic() + 2
            while not maintenance.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='object' AND classid='pg_database'::regclass AND objid=(SELECT oid FROM pg_database WHERE datname=%s) AND mode='RowExclusiveLock' AND granted)",
                (database,),
            ).fetchone()[0]:
                assert time.monotonic() < deadline, "startup did not acquire its database lock"
                time.sleep(0.01)
            target = _target(admin, maintenance, database, role, successor, provisioner)
            handoff = _handoff(admin)
            close_application_database_admission(
                maintenance, target=target, provisioner_role=provisioner
            )
            if recovery:
                admin.close()
            with pytest.raises(RuntimeError, match="startup"):
                drain(
                    maintenance,
                    target=target,
                    provisioner_role=provisioner,
                    handoff_backend=handoff,
                )
            with future.result(timeout=10) as late:
                assert late.execute("SELECT 42").fetchone() == (42,)
                with pytest.raises(RuntimeError, match="sessions"):
                    drain(
                        maintenance,
                        target=target,
                        provisioner_role=provisioner,
                        handoff_backend=handoff,
                    )
            drain(
                maintenance, target=target, provisioner_role=provisioner, handoff_backend=handoff
            )
        finally:
            if not future.cancel():
                future.result(timeout=10).close()
            if target is not None:
                reopen_application_database_admission(
                    maintenance, target=target, provisioner_role=provisioner
                )


def test_recovery_accepts_only_the_recorded_successor_owner(login_database):  # noqa: F811
    admin, database, role, successor, _, provisioner = login_database
    with _maintenance(admin) as maintenance:
        target = _target(admin, maintenance, database, role, successor, provisioner)
        close_application_database_admission(
            maintenance, target=target, provisioner_role=provisioner
        )
        admin.execute(
            sql.SQL("ALTER DATABASE {} OWNER TO {}").format(
                sql.Identifier(database), sql.Identifier(successor)
            )
        )
        reopen_application_database_admission(
            maintenance, target=target, provisioner_role=provisioner
        )


def test_lost_close_commit_acknowledgement_is_reconciled_from_saved_identity(login_database):  # noqa: F811
    admin, database, role, successor, _, provisioner = login_database
    with _maintenance(admin) as maintenance:
        target = _target(admin, maintenance, database, role, successor, provisioner)

        class LostAcknowledgement:
            @property
            def info(self):
                return maintenance.info

            def execute(self, query):
                return maintenance.execute(query)

            @contextmanager
            def transaction(self):
                with maintenance.transaction():
                    yield
                raise RuntimeError("lost commit acknowledgement")

        with pytest.raises(RuntimeError, match="lost commit"):
            close_application_database_admission(
                LostAcknowledgement(), target=target, provisioner_role=provisioner
            )
    with _maintenance(admin) as recovered:
        assert recovered.execute(
            "SELECT datallowconn FROM pg_database WHERE datname=%s", (database,)
        ).fetchone() == (False,)
        reopen_application_database_admission(
            recovered, target=target, provisioner_role=provisioner
        )


def test_partial_close_rolls_back_and_outer_transactions_are_refused(login_database):  # noqa: F811
    admin, database, role, successor, _, provisioner = login_database
    with _maintenance(admin) as maintenance:
        target = _target(admin, maintenance, database, role, successor, provisioner)

        class FailedAfterAlter:
            @property
            def info(self):
                return maintenance.info

            def transaction(self):
                return maintenance.transaction()

            def execute(self, query):
                result = maintenance.execute(query)
                if isinstance(query, sql.Composed) and query.as_string().startswith(
                    "ALTER DATABASE"
                ):
                    raise RuntimeError("injected post-alter failure")
                return result

        with maintenance.transaction(), pytest.raises(RuntimeError, match="idle"):
            close_application_database_admission(
                maintenance, target=target, provisioner_role=provisioner
            )
        with pytest.raises(RuntimeError, match="maintenance administrator"):
            close_application_database_admission(admin, target=target, provisioner_role=provisioner)
        with pytest.raises(RuntimeError, match="post-alter"):
            close_application_database_admission(
                FailedAfterAlter(), target=target, provisioner_role=provisioner
            )
        assert maintenance.execute(
            "SELECT datallowconn FROM pg_database WHERE datname=%s", (database,)
        ).fetchone() == (True,)


@pytest.mark.parametrize(
    "field", ["started_at", "server_started_at", "system_identifier", "database_oid"]
)
def test_drain_refuses_wrong_handoff_backend_identity(login_database, field):  # noqa: F811
    admin, database, role, successor, _, provisioner = login_database
    with _maintenance(admin) as maintenance:
        target = _target(admin, maintenance, database, role, successor, provisioner)
        handoff = _handoff(admin)
        close_application_database_admission(
            maintenance, target=target, provisioner_role=provisioner
        )
        value = getattr(handoff, field)
        if field in {"started_at", "server_started_at"}:
            value = (datetime.fromisoformat(value) + timedelta(seconds=1)).isoformat()
        else:
            value = value + 1000 if type(value) is int else str(int(value) + 1)
        changed = replace(handoff, **{field: value})
        try:
            with pytest.raises(RuntimeError):
                require_application_database_drained(
                    maintenance,
                    target=target,
                    provisioner_role=provisioner,
                    handoff_backend=changed,
                )
            assert admin.execute("SELECT 42").fetchone() == (42,)
        finally:
            reopen_application_database_admission(
                maintenance, target=target, provisioner_role=provisioner
            )


@pytest.mark.parametrize(
    "field", ["pid", "started_at", "server_started_at", "system_identifier", "database_oid"]
)
def test_capture_refuses_wrong_handoff_identity(login_database, field):  # noqa: F811
    admin, database, role, successor, _, provisioner = login_database
    with _maintenance(admin) as maintenance:
        _target(admin, maintenance, database, role, successor, provisioner)
        handoff = _handoff(admin)
        value = getattr(handoff, field)
        if field in {"started_at", "server_started_at"}:
            value = (datetime.fromisoformat(value) + timedelta(seconds=1)).isoformat()
        else:
            value = value + 1000 if type(value) is int else str(int(value) + 1)
        with pytest.raises(RuntimeError):
            capture_application_database_admission(
                maintenance,
                database=database,
                owner_role=role,
                successor_role=successor,
                provisioner_role=provisioner,
                handoff_backend=replace(handoff, **{field: value}),
            )
        assert maintenance.execute(
            "SELECT datallowconn FROM pg_database WHERE datname=%s", (database,)
        ).fetchone() == (True,)


def test_handoff_identity_compares_instants_across_session_timezones(login_database):  # noqa: F811
    admin, database, role, successor, _, provisioner = login_database
    admin.execute("SET TIME ZONE 'Pacific/Honolulu'")
    with _maintenance(admin) as maintenance:
        maintenance.execute("SET TIME ZONE 'Asia/Kolkata'")
        maintenance.execute("SET DateStyle='SQL, DMY'")
        target = _target(admin, maintenance, database, role, successor, provisioner)
        handoff = _handoff(admin)
        close_application_database_admission(
            maintenance, target=target, provisioner_role=provisioner
        )
        try:
            require_application_database_drained(
                maintenance, target=target, provisioner_role=provisioner, handoff_backend=handoff
            )
        finally:
            reopen_application_database_admission(
                maintenance, target=target, provisioner_role=provisioner
            )
