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
                    # Recovery may retire the original maintenance peer before
                    # handing authority to its own fixed transport.
                    with psycopg.connect(url, dbname="postgres", autocommit=True) as cleanup:
                        cleanup.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS true").format(sql.Identifier(database)))
        finally:
            with psycopg.connect(url, autocommit=True) as cleanup:
                cleanup.execute("DROP OWNED BY loom_rollout_readonly")
                cleanup.execute("DROP ROLE loom_rollout_readonly")


class LostAcknowledgementError(RuntimeError):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize("observation", ["autovacuum", "retired", "unretired"])
@pytest.mark.parametrize("refusal", ["trigger", "trigger-code", "unrelated-code", "sessions", "prepared"])
async def test_handoff_retries_rolled_back_quiescence_when_current_work_is_admitted(transfer_database, monkeypatch, observation, refusal):  # noqa: F811
    from loom import application_handoff_completion as module
    from loom.application_ownership_transfer import ApplicationOwnershipTransferError

    transfer = module.transfer_application_ownership
    calls = []
    def interrupted(connection, **kwargs):
        transfer(connection, **kwargs)
        calls.append(1)
        if len(calls) == 1:
            if refusal in {"trigger-code", "unrelated-code"}:
                code = "55L01" if refusal == "trigger-code" else "55000"
                connection.execute(f"DO $$ BEGIN RAISE EXCEPTION 'unclassified private diagnostic' USING ERRCODE='{code}'; END $$")
            elif refusal != "trigger":
                raise ApplicationOwnershipTransferError(
                    "application ownership requires reconciled sessions" if refusal == "sessions"
                    else "application ownership has prepared transactions"
                )
            connection.execute("DO $$ BEGIN RAISE EXCEPTION 'application trigger handoff requires quiescent legacy authority' "
                "USING ERRCODE='55000'; END $$")
    monkeypatch.setattr(module, "transfer_application_ownership", interrupted)
    if observation != "retired":
        monkeypatch.setattr(module, "_quiescence_retry_admitted", lambda *args, **kwargs: observation == "autovacuum")
    with _closed(transfer_database) as (peer, maintenance, _guard_peer, arguments):
        if observation != "unretired" and refusal not in {"prepared", "unrelated-code"}:
            module.complete_application_handoff_database(peer, maintenance=maintenance, **arguments)
            assert len(calls) == 2
        else:
            error = psycopg.Error if refusal in {"trigger", "trigger-code", "unrelated-code"} else ApplicationOwnershipTransferError
            with pytest.raises(error):
                module.complete_application_handoff_database(peer, maintenance=maintenance, **arguments)
            assert len(calls) == 1
            assert peer.execute("SELECT datdba FROM pg_database WHERE datname=current_database()").fetchone() == (arguments["target"].owner_oid,)


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
@pytest.mark.parametrize("transfer_database", ["release", "cnpg", "baseline"], indirect=True)
@pytest.mark.parametrize("interruption", [None, "transfer", "reopen", "login"])
async def test_completion_recovers_each_committed_phase_with_original_guard(transfer_database, interruption, request):  # noqa: F811
    from loom.application_handoff_completion import complete_application_handoff_database

    url, owner, _bindings = transfer_database
    with _closed(transfer_database) as (peer, maintenance, guard, arguments):
        arguments["schema_revision"] = ("0134/guard_0030" if request.node.callspec.params["transfer_database"] == "baseline" else "0146/guard_0035")
        if request.node.callspec.params["transfer_database"] in {"cnpg", "baseline"}:
            arguments["schema_acl_profile"] = "cnpg-staging"
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


@pytest.mark.asyncio
@pytest.mark.parametrize("loss", [None, "before", "after-alter", "wrong-peer", "wrong-password"])
async def test_guarded_seal_preserves_original_login_when_guard_or_peer_changes(transfer_database, loss):  # noqa: F811
    from dataclasses import replace

    from loom.application_login_sealing import seal_guarded_application_login

    with _closed(transfer_database) as (peer, _maintenance_peer, guard, arguments):
        target = arguments["target"]
        peer.execute(sql.SQL("ALTER ROLE {} LOGIN INHERIT PASSWORD {}").format(
            sql.Identifier(target.owner_role), sql.Literal(arguments["password"])))
        before = peer.execute("SELECT oid,rolcanlogin,rolinherit,rolpassword FROM pg_authid WHERE rolname=%s",
                              (target.owner_role,)).fetchone()
        changed = []

        class LoseGuard:
            info = peer.info
            transaction = peer.transaction

            def execute(self, query):
                rendered = query if isinstance(query, str) else query.as_string(peer)
                result = peer.execute(query)
                if rendered.startswith("ALTER ROLE "):
                    changed.append(True)
                    if loss == "after-alter":
                        assert guard.execute("SELECT pg_advisory_unlock(5498691230183247727)").fetchone() == (True,)
                return result

        if loss == "before":
            assert guard.execute("SELECT pg_advisory_unlock(5498691230183247727)").fetchone() == (True,)
        backend = arguments["handoff_backend"]
        if loss == "wrong-peer":
            backend = replace(backend, pid=backend.pid + 1)
        kwargs = dict(database=target.database, role=target.owner_role,
                      provisioner_role=next(role for role, alias in arguments["role_bindings"].items() if alias == "provisioner"),
                      handoff_backend=backend, coordination_guard=arguments["coordination_guard"],
                      runtime_password="unrelated-password" if loss == "wrong-password" else arguments["password"])
        if loss:
            with pytest.raises(RuntimeError, match=r"guard|peer|password"):
                seal_guarded_application_login(LoseGuard(), **kwargs)
            assert peer.execute("SELECT oid,rolcanlogin,rolinherit,rolpassword FROM pg_authid WHERE rolname=%s",
                                (target.owner_role,)).fetchone() == before
            assert changed == ([True] if loss == "after-alter" else [])
        else:
            for _ in range(2):
                seal_guarded_application_login(LoseGuard(), **kwargs)
            assert peer.execute("SELECT oid,rolcanlogin,rolinherit,rolpassword FROM pg_authid WHERE rolname=%s",
                                (target.owner_role,)).fetchone() == (before[0], False, False, None)
            assert guard.execute("SELECT pg_backend_pid()").fetchone() == (arguments["coordination_guard"].backend.pid,)


@pytest.mark.asyncio
@pytest.mark.parametrize("loss", [None, "before", "after-alter", "wrong-peer"])
async def test_guarded_closure_rolls_back_on_original_guard_loss(transfer_database, loss):  # noqa: F811
    from dataclasses import replace

    from loom.application_database_admission import close_guarded_application_database_admission

    with _closed(transfer_database) as (peer, maintenance, guard, arguments):
        target = arguments["target"]
        maintenance.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS true").format(sql.Identifier(target.database)))
        changed = []

        class LoseGuard:
            info = maintenance.info
            transaction = maintenance.transaction

            def execute(self, query):
                rendered = query if isinstance(query, str) else query.as_string(maintenance)
                result = maintenance.execute(query)
                if rendered.startswith("ALTER DATABASE "):
                    changed.append(True)
                    if loss == "after-alter":
                        assert guard.execute("SELECT pg_advisory_unlock(5498691230183247727)").fetchone() == (True,)
                return result

        if loss == "before":
            assert guard.execute("SELECT pg_advisory_unlock(5498691230183247727)").fetchone() == (True,)
        backend = arguments["handoff_backend"]
        if loss == "wrong-peer":
            backend = replace(backend, pid=backend.pid + 10000)
        kwargs = dict(target=target,
                      provisioner_role=next(role for role, alias in arguments["role_bindings"].items() if alias == "provisioner"),
                      handoff_backend=backend, coordination_guard=arguments["coordination_guard"],
                      runtime_password=arguments["password"])
        if loss:
            with pytest.raises(RuntimeError, match=r"guard|peer"):
                close_guarded_application_database_admission(LoseGuard(), **kwargs)
            assert peer.execute("SELECT datallowconn FROM pg_database WHERE datname=%s", (target.database,)).fetchone() == (True,)
            assert changed == ([True] if loss == "after-alter" else [])
        else:
            for _ in range(2):
                close_guarded_application_database_admission(LoseGuard(), **kwargs)
            assert peer.execute("SELECT datallowconn FROM pg_database WHERE datname=%s", (target.database,)).fetchone() == (False,)
            assert changed == [True]


@pytest.mark.asyncio
@pytest.mark.parametrize("transfer_database", ["cnpg", "baseline"], indirect=True)
@pytest.mark.parametrize("marker", ["public.alembic_version", "loom_capacity_guard.capacity_guard_alembic_version"])
async def test_revision_marker_drift_refuses_before_ownership_change(transfer_database, marker, request):  # noqa: F811
    from loom.application_handoff_completion import complete_application_handoff_database

    with _closed(transfer_database) as (peer, maintenance, _guard, arguments):
        arguments.update(schema_acl_profile="cnpg-staging", schema_revision=(
            "0134/guard_0030" if request.node.callspec.params["transfer_database"] == "baseline" else "0146/guard_0035"))
        peer.execute("UPDATE " + marker + " SET version_num='unexpected'")
        with pytest.raises(RuntimeError, match="revision"):
            complete_application_handoff_database(peer, maintenance=maintenance, **arguments)
        target = arguments["target"]
        assert peer.execute("SELECT datdba,datallowconn FROM pg_database WHERE oid=%s", (target.database_oid,)).fetchone() == (target.owner_oid, False)


@pytest.mark.asyncio
async def test_quiescence_retry_requires_retirement_in_other_databases(transfer_database):  # noqa: F811
    from loom.application_handoff_completion import _quiescence_retry_admitted

    with _closed(transfer_database) as (_peer, maintenance, _guard, arguments):
        authority = {key: arguments[key] for key in ("target", "handoff_backend", "coordination_guard")}
        authority["provisioner"] = next(role for role, alias in arguments["role_bindings"].items() if alias == "provisioner")
        with psycopg.connect(transfer_database[0], dbname=maintenance.info.dbname, autocommit=True):
            with pytest.raises(RuntimeError, match="client work"):
                _quiescence_retry_admitted(maintenance, **authority)
        assert _quiescence_retry_admitted(maintenance, **authority)
