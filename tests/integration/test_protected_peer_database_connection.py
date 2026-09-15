"""Real psql transaction transport for installed protected code, not candidate Jobs."""

import os
import subprocess
from collections.abc import Iterator
from datetime import datetime
from uuid import uuid4

import psycopg
import pytest
from psycopg.pq import TransactionStatus
from sqlalchemy.engine import make_url
from testcontainers.postgres import PostgresContainer

from loom.application_database_admission import (
    ApplicationDatabaseHandoffBackend,
    capture_application_database_admission,
    close_application_database_admission,
    reopen_application_database_admission,
    require_application_database_drained,
)
from loom.application_login_sealing import seal_application_login
from loom.application_ownership_transfer import transfer_application_ownership
from loom.application_runtime_login import restore_application_runtime_login
from loom.application_schema_inventory import read_application_schema_inventory
from loom.application_schema_reference import (
    application_schema_reference,
    require_application_schema_reference,
)
from loom_cli.rollout.operator.protected_apply_executor import (
    _STAGING_PEER_DATABASE_COMMAND,
    _STAGING_PEER_MAINTENANCE_COMMAND,
)
from loom_cli.rollout.operator.protected_peer_database_connection import (
    PeerDatabaseConnection,
    PeerDatabaseTransportError,
)
from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)


@pytest.fixture(scope="module", params=[16, 17])
def peer_postgres(request: pytest.FixtureRequest) -> Iterator[PostgresContainer]:
    with PostgresContainer(
        application_schema_reference(postgres_major=request.param).postgres_image,
        driver="psycopg", username="postgres", password=uuid4().hex,
    ).with_bind_ports(5432, ("127.0.0.1", None)) as postgres:
        yield postgres


def _peer(
    postgres: PostgresContainer, database: str | None = None, *, startup_options: str | None = None
) -> PeerDatabaseConnection:
    options = startup_options or ""
    if postgres.image == application_schema_reference(postgres_major=17).postgres_image:
        options += " -c event_triggers=off"
    process = subprocess.Popen(
        [
            "docker",
            "exec",
            "-i",
            *([] if not options else ["-e", f"PGOPTIONS={options}"]),
            postgres.get_wrapped_container().id,
            "psql",
            "-U",
            postgres.username,
            "-d",
            database or postgres.dbname,
            "-qAtX",
            "-v",
            "ON_ERROR_STOP=0",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )
    return PeerDatabaseConnection(process, query_timeout_seconds=5)


def test_peer_preserves_json_types_duplicate_columns_and_transaction_state(peer_postgres):
    with _peer(peer_postgres) as connection:
        assert connection.info.server_version // 10000 in {16, 17}
        assert connection.info.transaction_status == TransactionStatus.IDLE
        assert connection.execute(
            "SELECT true AS same, false AS same, 7, NULL, "
            "'line1' || chr(10) || 'line2', '[1,{\"key\":false}]'::jsonb"
        ).fetchone() == (True, False, 7, None, "line1\nline2", [1, {"key": False}])
        with connection.transaction():
            connection.execute("CREATE TEMP TABLE peer_rows (id integer PRIMARY KEY)")
            connection.execute("INSERT INTO peer_rows VALUES (1)")
            assert connection.info.transaction_status == TransactionStatus.INTRANS
            with pytest.raises(psycopg.errors.UniqueViolation):
                with connection.transaction():
                    connection.execute("INSERT INTO peer_rows VALUES (2)")
                    connection.execute("INSERT INTO peer_rows VALUES (1)")
            assert connection.info.transaction_status == TransactionStatus.INTRANS
            assert connection.execute("SELECT id FROM peer_rows ORDER BY id").fetchall() == [(1,)]
        assert connection.info.transaction_status == TransactionStatus.IDLE
        with pytest.raises(RuntimeError, match="rollback me"):
            with connection.transaction():
                connection.execute("INSERT INTO peer_rows VALUES (3)")
                raise RuntimeError("rollback me")
        assert connection.execute("SELECT id FROM peer_rows").fetchall() == [(1,)]


@pytest.mark.parametrize("peer_postgres", [17], indirect=True)
@pytest.mark.parametrize("failure", ["none", "intent", "reopen", "peer", "receipt", "unknown", "extra", "lost-maintenance", "lost-initial-close"])
def test_journaled_fixed_peer_recovery_retries_without_adopting_old_connections(
    peer_postgres, tmp_path, monkeypatch, failure,
):
    from loom.application_database_admission import capture_application_coordination_guard
    from loom.staging_mutation_coordination import (
        STAGING_MUTATION_TRY_LOCK_SQL,
        rollout_guard_application_name,
        rollout_guard_bind_sql,
    )
    from loom_cli.rollout.operator import protected_apply_executor as executor
    from loom_cli.rollout.operator.protected_apply_journal import ProtectedApplyJournal
    from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
    from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan
    from tests.loom_cli.rollout.operator.test_protected_apply_journal import _journal

    runner = executor.SubprocessProtectedApplyCommandRunner()
    plan, journal = _plan(tmp_path), _journal(tmp_path)
    url = peer_postgres.get_connection_url().replace("postgresql+psycopg://", "postgresql://", 1)
    request = dict(request_id=plan.request_id, candidate_sha="a" * 40,
                   candidate_tree="b" * 40, generation="c" * 32)
    with psycopg.connect(url, autocommit=True) as admin:
        admin.execute("CREATE ROLE recovery_source LOGIN NOINHERIT")
        admin.execute("CREATE ROLE recovery_owner NOLOGIN NOINHERIT")
        admin.execute("CREATE ROLE loom_rollout_readonly LOGIN NOINHERIT PASSWORD 'test-only-guard'")
        admin.execute("CREATE DATABASE loom OWNER recovery_source")
        try:
            with _peer(peer_postgres, "loom") as original, psycopg.connect(
                url, dbname="loom", user="loom_rollout_readonly", password="test-only-guard", autocommit=True,
            ) as guard:
                guard.execute(rollout_guard_bind_sql(rollout_guard_application_name(**request)))
                assert guard.execute(STAGING_MUTATION_TRY_LOCK_SQL).fetchone() == (True,)
                identity = original.backend_identity
                backend = ApplicationDatabaseHandoffBackend(
                    identity.backend_pid, identity.backend_started_at, identity.system_identifier,
                    identity.server_started_at, identity.database_oid,
                )
                seal_application_login(original, database="loom", role="recovery_source", provisioner_role="postgres")
                target = capture_application_database_admission(
                    admin, database="loom", owner_role="recovery_source", successor_role="recovery_owner",
                    provisioner_role="postgres", handoff_backend=backend,
                )
                saved_guard = capture_application_coordination_guard(
                    admin, target=target, provisioner_role="postgres", backend_pid=guard.info.backend_pid, **request,
                )
                opened = []
                unknown = []
                reopening = False
                lost_ack_fd = None
                lost_maintenance = []
                real_read = os.read

                def read(fd, size):
                    chunk = real_read(fd, size)
                    if fd == lost_ack_fd and chunk:
                        assert admin.execute("SELECT datallowconn FROM pg_database WHERE datname='loom'").fetchone() == (failure != "lost-initial-close",)
                        raise OSError("injected commit acknowledgement loss")
                    return chunk

                monkeypatch.setattr(os, "read", read)

                def open_peer(_runner, *, database):
                    assert database in {"postgres", "loom"}
                    maintenance = database == "postgres"
                    peer = _peer(peer_postgres, database)
                    exchange = peer._exchange

                    def exchange_with_lost_ack(statement, *, returns_rows):
                        nonlocal lost_ack_fd
                        lose_ack = ((failure == "lost-maintenance" and reopening)
                                    or (failure == "lost-initial-close" and not lost_maintenance))
                        if maintenance and lose_ack and statement == "COMMIT":
                            lost_ack_fd = peer._process.stdout.fileno()
                            lost_maintenance.append(peer)
                        try:
                            return exchange(statement, returns_rows=returns_rows)
                        finally:
                            lost_ack_fd = None

                    monkeypatch.setattr(peer, "_exchange", exchange_with_lost_ack)
                    if not maintenance:
                        opened.append(peer)
                        if failure in {"unknown", "extra"} and not unknown:
                            unknown.append(psycopg.connect(url, dbname="loom", autocommit=True))
                    return peer

                monkeypatch.setattr(executor.SubprocessProtectedApplyCommandRunner, "_open_staging_peer", open_peer)
                publish = journal._publish_or_match

                def injected_publish(path, value):
                    if failure == "intent" and path.name == "application-handoff-01-intent.json":
                        raise OSError("injected recovery interruption")
                    if failure in {"peer", "unknown"} and path.name == "application-handoff-01-peer.json":
                        raise OSError("injected recovery interruption")
                    publish(path, value)
                    if failure == "receipt" and path.name == "application-handoff-01-peer.json":
                        raise OSError("injected recovery interruption")

                monkeypatch.setattr(journal, "_publish_or_match", injected_publish)
                real_reopen = executor.reopen_application_database_for_handoff_recovery

                def reopen(*args, **kwargs):
                    nonlocal reopening
                    reopening = True
                    try:
                        real_reopen(*args, **kwargs)
                    finally:
                        reopening = False
                    if failure == "reopen":
                        raise OSError("injected recovery interruption")

                monkeypatch.setattr(executor, "reopen_application_database_for_handoff_recovery", reopen)

                def apply(_):
                    journal.record_application_admission_recovery(
                        target=target, handoff_backend=backend, coordination_guard=saved_guard,
                    )
                    close_application_database_admission(admin, target=target, provisioner_role="postgres")
                    original.close()
                    with runner.recover_staging_peer_database(plan, journal=journal, ordinal=1) as replacement:
                        assert replacement.execute("SELECT 42").fetchone() == (42,)
                        assert admin.execute("SELECT datallowconn FROM pg_database WHERE datname='loom'").fetchone() == (False,)
                        assert replacement.backend_identity.backend_pid != backend.pid
                    raise RuntimeError("stop after replacement")

                expected = "stop after replacement" if failure == "none" else "injected recovery interruption"
                if failure == "extra":
                    expected = "sessions are not drained"
                if failure in {"lost-maintenance", "lost-initial-close"}:
                    expected = "transport failed safely"
                with pytest.raises((OSError, RuntimeError), match=expected):
                    journal.execute(plan, [_component(apply)])
                assert admin.execute("SELECT datallowconn FROM pg_database WHERE datname='loom'").fetchone() == (False,)
                assert admin.execute("SELECT rolcanlogin FROM pg_roles WHERE rolname='recovery_source'").fetchone() == (False,)
                assert all(peer._process.poll() is not None for peer in opened)
                if failure in {"lost-maintenance", "lost-initial-close"}:
                    assert len(lost_maintenance) == 1
                    assert lost_maintenance[0]._poisoned
                    assert lost_maintenance[0].info.transaction_status == TransactionStatus.UNKNOWN
                monkeypatch.setattr(executor, "reopen_application_database_for_handoff_recovery", real_reopen)
                journal = ProtectedApplyJournal(tmp_path / "state", request_id=plan.request_id, attempt_number=plan.attempt_number)
                ordinal = 2 if failure in {"none", "receipt", "extra"} else 1

                def retry(_):
                    with runner.recover_staging_peer_database(plan, journal=journal, ordinal=ordinal) as replacement:
                        assert replacement.execute("SELECT 43").fetchone() == (43,)
                        records = journal.read_application_handoff_recoveries()
                        assert len(records) == ordinal
                        assert records[-1][1].handoff_backend.pid == replacement.backend_identity.backend_pid
                        assert journal.read_application_admission_recovery().handoff_backend == backend
                    raise RuntimeError("stop after recovered successor")

                if failure in {"unknown", "extra"}:
                    try:
                        peer_count = len(opened)
                        with pytest.raises(RuntimeError, match="sessions are not drained"):
                            journal.execute(plan, [_component(retry)])
                        assert len(opened) == peer_count
                        assert unknown[0].execute("SELECT 44").fetchone() == (44,)
                    finally:
                        unknown[0].close()
                with pytest.raises(RuntimeError, match="stop after recovered successor"):
                    journal.execute(plan, [_component(retry)])
                assert admin.execute("SELECT datallowconn FROM pg_database WHERE datname='loom'").fetchone() == (False,)
                assert guard.execute("SELECT pg_advisory_unlock(5498691230183247727)").fetchone() == (True,)
        finally:
            admin.execute("DROP DATABASE loom WITH (FORCE)")
            admin.execute("DROP ROLE recovery_source, recovery_owner, loom_rollout_readonly")


@pytest.mark.parametrize("enabled", [True, False])
def test_peer_refuses_even_disabled_database_event_triggers(peer_postgres, enabled):
    url = peer_postgres.get_connection_url().replace("postgresql+psycopg://", "postgresql://", 1)
    with psycopg.connect(url, autocommit=True) as admin:
        admin.execute(
            "CREATE FUNCTION public.peer_event_policy() RETURNS event_trigger LANGUAGE plpgsql "
            "AS $$BEGIN RAISE EXCEPTION 'private callback must not execute'; END$$"
        )
        admin.execute(
            "CREATE EVENT TRIGGER peer_event_policy ON ddl_command_start "
            "WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION public.peer_event_policy()"
        )
        try:
            if not enabled:
                admin.execute("ALTER EVENT TRIGGER peer_event_policy DISABLE")
            with pytest.raises(PeerDatabaseTransportError, match="event trigger"):
                with _peer(peer_postgres):
                    pass
            assert admin.execute(
                "SELECT evtenabled FROM pg_event_trigger WHERE evtname='peer_event_policy'"
            ).fetchone() == ("O" if enabled else "D",)
        finally:
            admin.execute("DROP EVENT TRIGGER peer_event_policy")
            admin.execute("DROP FUNCTION public.peer_event_policy()")


@pytest.mark.parametrize("peer_postgres", [17], indirect=True)
@pytest.mark.parametrize("maintenance_peer", [False, True])
def test_fixed_staging_startup_never_executes_a_login_trigger(peer_postgres, maintenance_peer):
    url = peer_postgres.get_connection_url().replace("postgresql+psycopg://", "postgresql://", 1)
    with psycopg.connect(url, autocommit=True) as maintenance:
        maintenance.execute("CREATE DATABASE loom")
        try:
            database = "postgres" if maintenance_peer else "loom"
            command = _STAGING_PEER_MAINTENANCE_COMMAND if maintenance_peer else _STAGING_PEER_DATABASE_COMMAND
            with psycopg.connect(url, dbname=database, autocommit=True) as admin:
                admin.execute("CREATE TABLE public.peer_login_log (role_name text)")
                admin.execute(
                    "CREATE FUNCTION public.peer_login_policy() RETURNS event_trigger LANGUAGE plpgsql "
                    "AS $$BEGIN INSERT INTO public.peer_login_log VALUES (current_user); END$$"
                )
                admin.execute(
                    "CREATE EVENT TRIGGER peer_login_policy ON login "
                    "EXECUTE FUNCTION public.peer_login_policy()"
                )
                try:
                    process = subprocess.Popen(
                        ["docker", "exec", "-i", peer_postgres.get_wrapped_container().id,
                         *command[-3:]],
                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0,
                    )
                    with pytest.raises(PeerDatabaseTransportError) as caught:
                        with PeerDatabaseConnection(process, query_timeout_seconds=5):
                            pass
                    # The old script executes the LOGIN callback as postgres BEFORE
                    # even the server-version query. A post-connect setting is too late.
                    assert admin.execute("SELECT role_name FROM public.peer_login_log").fetchall() == []
                    assert "event trigger" in str(caught.value)
                    assert process.poll() is not None
                    assert admin.execute(
                        "SELECT evtenabled FROM pg_event_trigger WHERE evtname='peer_login_policy'"
                    ).fetchone() == ("O",)
                finally:
                    admin.execute("DROP EVENT TRIGGER peer_login_policy")
                    admin.execute("DROP FUNCTION public.peer_login_policy()")
                    admin.execute("DROP TABLE public.peer_login_log")
        finally:
            maintenance.execute("DROP DATABASE loom WITH (FORCE)")


@pytest.mark.parametrize("peer_postgres", [17], indirect=True)
def test_pg17_startup_restores_normal_ddl_event_handling(peer_postgres):
    with _peer(peer_postgres) as connection:
        assert connection.execute("SELECT current_setting('event_triggers')").fetchone() == ("on",)
        with connection.transaction():
            connection.execute(
                "CREATE FUNCTION public.peer_ddl_policy() RETURNS event_trigger LANGUAGE plpgsql "
                "AS $$BEGIN RAISE EXCEPTION 'DDL remains protected'; END$$"
            )
            connection.execute(
                "CREATE EVENT TRIGGER peer_ddl_policy ON ddl_command_start "
                "WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION public.peer_ddl_policy()"
            )
            try:
                with pytest.raises(psycopg.errors.RaiseException):
                    with connection.transaction():
                        connection.execute("CREATE TABLE public.must_not_exist (id integer)")
            finally:
                connection.execute("DROP EVENT TRIGGER peer_ddl_policy")
                connection.execute("DROP FUNCTION public.peer_ddl_policy()")


@pytest.mark.parametrize("peer_postgres", [17], indirect=True)
def test_pg17_transaction_timeout_poisoning_is_unknown_not_rollback(peer_postgres):
    with _peer(peer_postgres) as connection:
        with pytest.raises(PeerDatabaseTransportError):
            with connection.transaction():
                connection.execute("SET LOCAL transaction_timeout='100ms'")
                # A server-side timed query deterministically crosses its own
                # transaction deadline; no guessed client sleep or restart.
                connection.execute("SELECT pg_sleep(1)")
        assert connection.info.transaction_status == TransactionStatus.UNKNOWN
        with pytest.raises(PeerDatabaseTransportError, match="unavailable"):
            connection.execute("SELECT 1")


def test_peer_composable_literals_cannot_execute_psql_commands(peer_postgres):
    hostile = "quoted ';\n\\echo spoofed\n\\q\n-- \\ end"
    with _peer(peer_postgres) as connection:
        assert connection.execute(
            psycopg.sql.SQL("SELECT {}::text").format(psycopg.sql.Literal(hostile))
        ).fetchone() == (hostile,)
        assert connection.execute("SELECT 8").fetchone() == (8,)


def test_peer_close_rolls_back_and_reaps_real_backend(peer_postgres):
    table = "peer_retirement_" + uuid4().hex
    url = peer_postgres.get_connection_url().replace("postgresql+psycopg://", "postgresql://", 1)
    connection = _peer(peer_postgres)
    transaction = connection.transaction()
    with psycopg.connect(url, autocommit=True) as admin:
        admin.execute(
            psycopg.sql.SQL("CREATE TABLE {} (id integer)").format(
                psycopg.sql.Identifier("public", table)
            )
        )
        try:
            backend = connection.execute("SELECT pg_backend_pid()").fetchone()[0]
            transaction.__enter__()
            connection.execute(
                psycopg.sql.SQL("INSERT INTO {} VALUES (1)").format(
                    psycopg.sql.Identifier("public", table)
                )
            )
            connection.close()
            assert (
                admin.execute(
                    psycopg.sql.SQL("SELECT id FROM {}").format(
                        psycopg.sql.Identifier("public", table)
                    )
                ).fetchall()
                == []
            )
            assert admin.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE pid=%s", (backend,)
            ).fetchone() == (0,)
        finally:
            connection.close()
            admin.execute(
                psycopg.sql.SQL("DROP TABLE {}").format(psycopg.sql.Identifier("public", table))
            )


def test_peer_error_does_not_expose_sql_or_private_diagnostics(peer_postgres):
    private = "test-private-diagnostic-" + uuid4().hex
    with _peer(peer_postgres) as connection:
        with pytest.raises(psycopg.errors.RaiseException) as caught:
            with connection.transaction():
                connection.execute(
                    psycopg.sql.SQL("DO $body$ BEGIN RAISE EXCEPTION {}; END $body$").format(
                        psycopg.sql.Literal(private)
                    )
                )
        assert private not in str(caught.value)
        assert private not in repr(caught.value)
        assert connection.execute("SELECT 9").fetchone() == (9,)


@pytest.mark.parametrize("tail", ["INSERT INTO peer_single VALUES (2)", "SELECT 1/0"])
@pytest.mark.parametrize("separator", [";", "-- comment\r;"])
def test_peer_rejects_multiple_statements_before_any_effect(peer_postgres, tail, separator):
    with _peer(peer_postgres) as connection:
        with connection.transaction():
            connection.execute("CREATE TEMP TABLE peer_single (id integer)")
            with pytest.raises(PeerDatabaseTransportError, match="single statement"):
                connection.execute("INSERT INTO peer_single VALUES (1) " + separator + " " + tail)
            assert connection.execute("SELECT id FROM peer_single").fetchall() == []


def test_peer_identity_covers_backend_and_server_for_recovery(peer_postgres):
    with _peer(peer_postgres) as connection:
        expected = connection.execute(
            "SELECT s.system_identifier::text, pg_catalog.pg_postmaster_start_time()::text, "
            "a.pid, a.backend_start::text, a.datid::bigint, a.datname, a.usename "
            "FROM pg_catalog.pg_stat_activity a CROSS JOIN pg_catalog.pg_control_system() s "
            "WHERE a.pid=pg_catalog.pg_backend_pid()"
        ).fetchone()
        identity = connection.backend_identity
        assert (
            identity.system_identifier,
            identity.server_started_at,
            identity.backend_pid,
            identity.backend_started_at,
            identity.database_oid,
            identity.database,
            identity.session_user,
        ) == expected


def test_peer_identity_timestamps_are_iso_despite_inherited_date_style(peer_postgres):
    with _peer(
        peer_postgres, startup_options="-c datestyle=German,DMY -c timezone=Europe/Berlin"
    ) as connection:
        identity = connection.backend_identity
        assert datetime.fromisoformat(identity.server_started_at).utcoffset() is not None
        assert datetime.fromisoformat(identity.backend_started_at).utcoffset() is not None
        assert connection.execute("SELECT current_setting('DateStyle')").fetchone() == ("ISO, YMD",)


@pytest.mark.asyncio
@pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)
async def test_peer_runs_real_ownership_transfer_to_the_independent_profile(
    transfer_database,  # noqa: F811
    transfer_postgres,  # noqa: F811
):
    url, target, bindings = transfer_database
    previous = next(role for role, alias in bindings.items() if alias == "application-owner")
    provisioner = next(role for role, alias in bindings.items() if alias == "provisioner")
    with (
        _peer(transfer_postgres, make_url(url).database) as connection,
        _peer(transfer_postgres, "postgres") as maintenance,
    ):
        # Exercise the separate committed sealing phase on the actual peer path.
        seal_application_login(
            connection,
            database=make_url(url).database,
            role=previous,
            provisioner_role=provisioner,
        )
        handoff = ApplicationDatabaseHandoffBackend(
            pid=connection.backend_identity.backend_pid,
            started_at=connection.backend_identity.backend_started_at,
            system_identifier=connection.backend_identity.system_identifier,
            server_started_at=connection.backend_identity.server_started_at,
            database_oid=connection.backend_identity.database_oid,
        )
        admission = capture_application_database_admission(
            maintenance,
            database=make_url(url).database,
            owner_role=previous,
            successor_role=target,
            provisioner_role=provisioner,
            handoff_backend=handoff,
        )
        close_application_database_admission(
            maintenance, target=admission, provisioner_role=provisioner
        )
        try:
            for _ in range(2):
                require_application_database_drained(
                    maintenance,
                    target=admission,
                    provisioner_role=provisioner,
                    handoff_backend=handoff,
                )
                with connection.transaction():
                    transfer_application_ownership(
                        connection, owner_role=target, role_bindings=bindings
                    )
            with connection.transaction():
                observed = read_application_schema_inventory(
                    connection,
                    role_bindings={
                        **bindings,
                        previous: "application-runtime",
                        target: "application-owner",
                    },
                )
                require_application_schema_reference(observed, profile="sealed-owner")
        finally:
            reopen_application_database_admission(
                maintenance, target=admission, provisioner_role=provisioner
            )
        password = uuid4().hex
        for _ in range(2):
            restore_application_runtime_login(
                connection,
                target=admission,
                owner_role=target,
                role_bindings=bindings,
                password=password,
            )
        runtime_url = (
            make_url(url)
            .set(username=previous, password=password)
            .render_as_string(hide_password=False)
        )
        with psycopg.connect(runtime_url, autocommit=True) as runtime:
            assert runtime.execute("SELECT count(*) FROM public.trials").fetchone() == (0,)


@pytest.mark.parametrize(
    "control",
    [
        "COMMIT",
        "/* leading */ END WORK",
        "ROLLBACK AND CHAIN",
        "SAVEPOINT injected",
        "BEGIN",
        "START TRANSACTION",
        "ABORT",
        "RELEASE SAVEPOINT injected",
        "PREPARE TRANSACTION 'injected'",
        "-- comment\rCOMMIT",
    ],
)
def test_peer_transaction_control_is_exclusive_to_the_context_manager(peer_postgres, control):
    with _peer(peer_postgres) as connection:
        with connection.transaction():
            connection.execute("CREATE TEMP TABLE peer_control (id integer)")
        with pytest.raises(RuntimeError, match="rollback application"):
            with connection.transaction():
                connection.execute("INSERT INTO peer_control VALUES (1)")
                with pytest.raises(PeerDatabaseTransportError, match="transaction control"):
                    connection.execute(control)
                raise RuntimeError("rollback application")
        assert connection.execute("SELECT id FROM peer_control").fetchall() == []


def test_peer_trailing_comment_cannot_leave_a_pending_query_buffer(peer_postgres):
    with _peer(peer_postgres) as connection:
        with connection.transaction():
            connection.execute("CREATE TEMP TABLE peer_commented (id integer) -- trailing comment")
            connection.execute("INSERT INTO peer_commented VALUES (1) -- another comment")
            assert connection.execute(
                "SELECT id FROM peer_commented -- read comment"
            ).fetchone() == (1,)


def test_peer_leading_comments_preserve_query_rows(peer_postgres):
    with _peer(peer_postgres) as connection:
        assert connection.execute(
            "/* outer /* nested */ comment */ -- line\rSELECT 42"
        ).fetchone() == (42,)
