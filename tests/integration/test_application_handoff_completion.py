"""Complete and resume the database phases against real, isolated PostgreSQL."""

import os
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from uuid import uuid4

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
async def test_handoff_retries_rolled_back_quiescence_when_current_work_is_admitted(transfer_database, monkeypatch, observation, refusal, early_quiescence=False):  # noqa: F811
    from loom import application_handoff_completion as module
    from loom.application_ownership_transfer import ApplicationOwnershipTransferError

    transfer = module.transfer_application_ownership
    calls = []
    injected_refusal_pending = False
    early_refusals = []
    if early_quiescence:
        from loom.application_database_admission import ApplicationDatabaseAdmissionError

        drained = module.require_application_database_drained

        def initial_quiescence(connection, **kwargs):
            drained(connection, **kwargs)
            if not early_refusals:
                early_refusals.append(1)
                raise ApplicationDatabaseAdmissionError("application database sessions are not drained")

        monkeypatch.setattr(module, "require_application_database_drained", initial_quiescence)

    def interrupted(connection, **kwargs):
        nonlocal injected_refusal_pending
        transfer(connection, **kwargs)
        calls.append(1)
        if len(calls) == 1:
            injected_refusal_pending = True
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
        retry_admitted = module._quiescence_retry_admitted

        def classify_injected_refusal(*args, **kwargs):
            nonlocal injected_refusal_pending
            if injected_refusal_pending:
                injected_refusal_pending = False
                return observation == "autovacuum"
            # Startup/retirement observations outside our injected fault still
            # use the real bounded classifier and original authority checks.
            return retry_admitted(*args, **kwargs)

        monkeypatch.setattr(module, "_quiescence_retry_admitted", classify_injected_refusal)
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
    assert early_refusals == ([1] if early_quiescence else [])


@pytest.mark.asyncio
@pytest.mark.parametrize("observation,refusal", [
    ("unretired", "unrelated-code"),
    ("unretired", "trigger-code"),
    ("autovacuum", "trigger-code"),
])
async def test_injected_transfer_refusal_does_not_classify_preliminary_drain(
    transfer_database, monkeypatch, observation, refusal,  # noqa: F811
):
    # A legitimate initial drain retry must reach the separately injected
    # transfer failure. The classifier double represents that failure only.
    await test_handoff_retries_rolled_back_quiescence_when_current_work_is_admitted(
        transfer_database, monkeypatch, observation, refusal, early_quiescence=True,
    )


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
        arguments["schema_revision"] = ("0134/guard_0030" if request.node.callspec.params["transfer_database"] == "baseline" else "0148/guard_0036")
