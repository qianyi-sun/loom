"""Complete and resume the database phases against real, isolated PostgreSQL."""

from contextlib import contextmanager

import psycopg
import pytest
from psycopg import sql

from loom.application_database_admission import (
    capture_application_coordination_guard,
    capture_application_database_admission,
    close_application_database_admission,
)
from loom.application_login_sealing import seal_application_login
from loom.staging_mutation_coordination import (
    STAGING_MUTATION_TRY_LOCK_SQL,
    rollout_guard_application_name,
    rollout_guard_bind_sql,
)
from tests.integration.test_application_database_admission import _handoff, _maintenance
from tests.integration.test_application_ownership_transfer import (
    _install_staging_readonly,
    transfer_database,  # noqa: F401
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)

pytestmark = pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)


@contextmanager
def _closed(database_fixture, *, request=None):
    url, owner, bindings = database_fixture
    runtime = next(role for role, alias in bindings.items() if alias == "application-owner")
    provisioner = next(role for role, alias in bindings.items() if alias == "provisioner")
    request = request or dict(request_id="req-complete-handoff", candidate_sha="a" * 40,
                   candidate_tree="b" * 40, generation="c" * 32)
    with psycopg.connect(url, autocommit=True) as peer, _maintenance(peer) as maintenance:
        database = peer.execute("SELECT current_database()").fetchone()[0]
        guard_password = _install_staging_readonly(peer)
        try:
            with psycopg.connect(url, user="loom_rollout_readonly", password=guard_password,
                                 autocommit=True) as guard:
                guard.execute(rollout_guard_bind_sql(rollout_guard_application_name(**request)))
                assert guard.execute(STAGING_MUTATION_TRY_LOCK_SQL).fetchone() == (True,)
                seal_application_login(peer, database=database, role=runtime, provisioner_role=provisioner)
                handoff = _handoff(peer)
                target = capture_application_database_admission(
                    maintenance, database=database, owner_role=runtime, successor_role=owner,
                    provisioner_role=provisioner, handoff_backend=handoff,
                )
                saved_guard = capture_application_coordination_guard(
                    maintenance, target=target, provisioner_role=provisioner,
                    backend_pid=guard.info.backend_pid, **request,
                )
                close_application_database_admission(maintenance, target=target, provisioner_role=provisioner)
                try:
                    yield peer, maintenance, guard, dict(
                        target=target, handoff_backend=handoff, coordination_guard=saved_guard,
                        role_bindings=bindings, password="the-original-runtime-password",
                        schema_acl_profile="staging-readonly",
                    )
                finally:
                    # Disposable fixture cleanup, including intentionally lost guard cases.
                    maintenance.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS true").format(sql.Identifier(database)))
        finally:
            with psycopg.connect(url, autocommit=True) as cleanup:
                cleanup.execute("DROP OWNED BY loom_rollout_readonly")
                cleanup.execute("DROP ROLE loom_rollout_readonly")


class LostAcknowledgementError(RuntimeError):
    pass


class InterruptCommit:
    """Lose the client acknowledgement of exactly one actual committed phase."""

    def __init__(self, connection, phase):
        self.connection, self.phase, self.armed = connection, phase, False
        self.interrupted = False

    @property
    def info(self):
        return self.connection.info

    def execute(self, query):
        rendered = query if isinstance(query, str) else query.as_string(self.connection)
        result = self.connection.execute(query)
        if ((self.phase == "transfer" and rendered.startswith("ALTER DATABASE ") and " OWNER TO " in rendered)
                or (self.phase == "reopen" and rendered.startswith("ALTER DATABASE ") and "ALLOW_CONNECTIONS true" in rendered)
                or (self.phase == "login" and rendered.startswith("ALTER ROLE ") and " LOGIN PASSWORD " in rendered)):
            self.armed = True
        return result

    @contextmanager
    def transaction(self):
        outer = self.connection.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        with self.connection.transaction():
            yield
        if outer and self.armed and not self.interrupted:
            self.interrupted = True
            raise LostAcknowledgementError(self.phase)


@pytest.mark.asyncio
@pytest.mark.parametrize("interruption", [None, "transfer", "reopen", "login"])
async def test_completion_recovers_each_committed_phase_with_original_guard(transfer_database, interruption):  # noqa: F811
    from loom.application_handoff_completion import complete_application_handoff_database

    url, owner, _bindings = transfer_database
    with _closed(transfer_database) as (peer, maintenance, guard, arguments):
        original_backend = guard.info.backend_pid
        original_server = guard.execute("SELECT pg_postmaster_start_time()").fetchone()
        if interruption:
            channel = InterruptCommit(maintenance if interruption == "reopen" else peer, interruption)
            with pytest.raises(LostAcknowledgementError, match=interruption):
                complete_application_handoff_database(
                    peer if interruption == "reopen" else channel,
                    maintenance=channel if interruption == "reopen" else maintenance, **arguments,
                )
            assert channel.interrupted
        for _ in range(2):
            outcome = complete_application_handoff_database(peer, maintenance=maintenance, **arguments)
            assert outcome.target == arguments["target"]
            assert outcome.coordination_guard == arguments["coordination_guard"]
            assert guard.info.backend_pid == original_backend
            assert guard.execute("SELECT pg_postmaster_start_time()").fetchone() == original_server
            from loom_cli.rollout.operator.staging_mutation_guard import _HEALTH_SQL
            assert guard.execute(_HEALTH_SQL).fetchone() == (original_backend, True)
        with psycopg.connect(url, user=arguments["target"].owner_role,
                             password=arguments["password"], autocommit=True) as runtime:
            assert runtime.execute("SELECT count(*) FROM public.trials").fetchone() == (0,)
            for query in (
                "ALTER TABLE public.trials DISABLE TRIGGER ALL",
                "UPDATE public.alembic_version SET version_num='wrong'",
                sql.SQL("SET ROLE {}").format(sql.Identifier(owner)),
            ):
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    runtime.execute(query)


@pytest.mark.asyncio
async def test_completion_refuses_lost_guard_before_database_mutation(transfer_database):  # noqa: F811
    from loom.application_handoff_completion import complete_application_handoff_database

    with _closed(transfer_database) as (peer, maintenance, guard, arguments):
        assert guard.execute("SELECT pg_advisory_unlock(5498691230183247727)").fetchone() == (True,)
        with pytest.raises(RuntimeError, match="coordination guard"):
            complete_application_handoff_database(peer, maintenance=maintenance, **arguments)
        assert peer.execute("SELECT pg_get_userbyid(datdba),datallowconn FROM pg_database WHERE datname=current_database()").fetchone() == (arguments["target"].owner_role, False)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["reopen", "login"])
async def test_guard_loss_inside_final_mutation_rolls_back_that_phase(transfer_database, phase):  # noqa: F811
    from loom.application_handoff_completion import complete_application_handoff_database

    with _closed(transfer_database) as (peer, maintenance, guard, arguments):
        class LoseGuard(InterruptCommit):
            def execute(self, query):
                result = super().execute(query)
                if self.armed:
                    self.armed = False
                    assert guard.execute("SELECT pg_advisory_unlock(5498691230183247727)").fetchone() == (True,)
                return result

        channel = LoseGuard(maintenance if phase == "reopen" else peer, phase)
        with pytest.raises(RuntimeError, match="coordination guard"):
            complete_application_handoff_database(
                peer if phase == "reopen" else channel,
                maintenance=channel if phase == "reopen" else maintenance, **arguments,
            )
        assert peer.execute("SELECT pg_get_userbyid(datdba),datallowconn FROM pg_database WHERE datname=current_database()").fetchone() == (arguments["target"].successor_role, phase == "login")
        assert peer.execute("SELECT rolcanlogin,rolpassword FROM pg_authid WHERE oid=%s", (arguments["target"].owner_oid,)).fetchone() == (False, None)


@pytest.mark.asyncio
async def test_restored_replay_preserves_unknown_password_and_rejects_schema_drift(transfer_database):  # noqa: F811
    from loom.application_handoff_completion import complete_application_handoff_database

    with _closed(transfer_database) as (peer, maintenance, _guard, arguments):
        complete_application_handoff_database(peer, maintenance=maintenance, **arguments)
        before = peer.execute("SELECT rolcanlogin,rolpassword FROM pg_authid WHERE oid=%s", (arguments["target"].owner_oid,)).fetchone()
        with pytest.raises(RuntimeError, match="credential"):
            complete_application_handoff_database(peer, maintenance=maintenance,
                                                  **{**arguments, "password": "a-different-password"})
        peer.execute("CREATE TABLE public.unexpected_handoff_object(id integer)")
        with pytest.raises(RuntimeError, match="trusted reference"):
            complete_application_handoff_database(peer, maintenance=maintenance, **arguments)
        assert peer.execute("SELECT rolcanlogin,rolpassword FROM pg_authid WHERE oid=%s", (arguments["target"].owner_oid,)).fetchone() == before
        assert peer.execute("SELECT datallowconn FROM pg_database WHERE datname=current_database()").fetchone() == (True,)
