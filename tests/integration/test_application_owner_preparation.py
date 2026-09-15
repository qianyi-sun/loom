"""Create the saved staging owner through the real component journal and SQL."""

import os
from contextlib import contextmanager
from uuid import uuid4

import pytest
from psycopg import sql
from testcontainers.postgres import PostgresContainer

from loom.application_schema_reference import application_schema_reference
from loom_cli.rollout.operator.protected_application_guard_retention import (
    application_guard_is_retained,
)
from loom_cli.rollout.operator.staging_mutation_guard import MutationGuardEvidence
from tests.integration.test_application_handoff_completion import _closed
from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)
from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
from tests.loom_cli.rollout.operator.test_application_guard_retention import _guard, _setup


@pytest.fixture(scope="module")
def transfer_postgres(request):
    with PostgresContainer(application_schema_reference(postgres_major=request.param).postgres_image,
                           driver="psycopg", username="postgres", password=uuid4().hex).with_bind_ports(
                               5432, ("127.0.0.1", None)) as postgres:
        yield postgres


@pytest.mark.asyncio
@pytest.mark.parametrize("transfer_postgres", [17], indirect=True)
@pytest.mark.parametrize("transfer_database", ["protected-staging"], indirect=True)
@pytest.mark.parametrize("interruption", [None, "receipt", "commit", "foreign-role", "guard-after-receipt"])
async def test_staging_owner_creation_recovers_only_its_saved_oid(
    transfer_database, tmp_path, monkeypatch, interruption,  # noqa: F811
):
    from loom_cli.rollout.operator.protected_application_owner_preparation import (
        APPLICATION_OWNER_ROLE,
        prepare_application_owner,
    )

    plan, journal = _setup(tmp_path)
    evidence = _guard(plan)
    request = dict(request_id=plan.request_id, candidate_sha=plan.candidate_sha,
                   candidate_tree=plan.candidate_tree, generation=evidence.generation)
    with _closed(transfer_database, request=request) as (peer, maintenance, db_guard, _arguments):
        maintenance.execute("ALTER DATABASE loom ALLOW_CONNECTIONS true")
        evidence = MutationGuardEvidence.build(**{
            k: v for k, v in evidence.to_dict().items()
            if k not in {"schema_version", "evidence_digest", "database_backend_pid"}
        }, database_backend_pid=db_guard.info.backend_pid)
        armed = [True]
        seen = []
        original = journal.record_application_owner_oid

        def record(*args, **kwargs):
            original(*args, **kwargs)
            seen.append(kwargs["role_oid"])
            if interruption == "guard-after-receipt":
                assert db_guard.execute("SELECT pg_advisory_unlock(5498691230183247727)").fetchone() == (True,)
            if interruption == "receipt" and armed[0]:
                armed[0] = False
                raise RuntimeError("receipt persisted before rollback")
        monkeypatch.setattr(journal, "record_application_owner_oid", record)

        class CommitLoss:
            info = peer.info
            def execute(self, statement):
                return peer.execute(statement)
            @contextmanager
            def transaction(self):
                with peer.transaction():
                    yield
                if interruption == "commit" and armed[0] and seen:
                    armed[0] = False
                    raise RuntimeError("owner commit acknowledgement lost")

        outcome = []
        def apply(_):
            journal.retain_application_guard(plan, guard=evidence)
            assert application_guard_is_retained(tmp_path / "state", request_id=plan.request_id,
                                                service_uid=os.getuid(), guard=evidence, acknowledge=True)
            outcome.append(prepare_application_owner(plan, journal=journal, connection=CommitLoss(), guard=evidence))
            raise RuntimeError("owner creation section verified")

        role = sql.Identifier(APPLICATION_OWNER_ROLE)
        try:
            if interruption == "foreign-role":
                peer.execute(sql.SQL("CREATE ROLE {} NOLOGIN NOINHERIT").format(role))
                with pytest.raises(RuntimeError, match="unrecorded"):
                    journal.execute(plan, [_component(apply)])
                assert seen == [] and outcome == []
                return
            if interruption == "guard-after-receipt":
                with pytest.raises(RuntimeError, match="coordination guard"):
                    journal.execute(plan, [_component(apply)])
                assert peer.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (APPLICATION_OWNER_ROLE,)).fetchone() is None
                return
            if interruption:
                with pytest.raises(RuntimeError, match=r"rollback|acknowledgement"):
                    journal.execute(plan, [_component(apply)])
            for _ in range(2):
                with pytest.raises(RuntimeError, match="section verified"):
                    journal.execute(plan, [_component(apply)])
            assert outcome[0] == outcome[1] == seen[-1]
            assert len(seen) == (2 if interruption == "receipt" else 1)
            if interruption == "receipt":
                assert seen[0] != seen[1]
            row = peer.execute(sql.SQL("SELECT oid::bigint,NOT (rolcanlogin OR rolinherit OR rolsuper "
                "OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls) AND rolpassword IS NULL "
                "FROM pg_authid WHERE rolname={}").format(sql.Literal(APPLICATION_OWNER_ROLE))).fetchone()
            assert row == (outcome[0], True)
            assert not list(journal.root.rglob("terminal.json"))
        finally:
            peer.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(role))


@pytest.mark.asyncio
@pytest.mark.parametrize("transfer_postgres", [17], indirect=True)
@pytest.mark.parametrize("transfer_database", ["protected-staging"], indirect=True)
@pytest.mark.parametrize("interruption", [None, "owner", "seal", "admission-record", "close", "schema-drift"])
@pytest.mark.parametrize("transport", ["psycopg", "installed-peer"])
async def test_initial_database_phase_recovers_each_commit_without_recapturing_closed_target(
    transfer_database, transfer_postgres, tmp_path, monkeypatch, interruption, transport,  # noqa: F811
):
    from contextlib import ExitStack, nullcontext

    from loom_cli.rollout.operator.protected_application_owner_preparation import (
        APPLICATION_OWNER_ROLE,
    )
    from loom_cli.rollout.operator.protected_apply_executor import (
        SubprocessProtectedApplyCommandRunner,
    )
    from tests.integration.test_protected_peer_database_connection import _peer
    from tests.loom_cli.rollout.operator.test_application_credential_recovery import (
        _Runner,
        _sources,
    )

    _, journal = _setup(tmp_path)
    password = "the-original-runtime-password"
    plan, live = _sources(tmp_path, password=password, schema_revision="0146/guard_0034")
    evidence = _guard(plan)
    request = dict(request_id=plan.request_id, candidate_sha=plan.candidate_sha,
                   candidate_tree=plan.candidate_tree, generation=evidence.generation)
    with _closed(transfer_database, request=request) as (peer, maintenance, db_guard, _arguments), ExitStack() as stack:
        maintenance.execute("ALTER DATABASE loom ALLOW_CONNECTIONS true")
        peer.execute(sql.SQL("ALTER ROLE loom LOGIN INHERIT PASSWORD {}").format(sql.Literal(password)))
        initial_peer = peer if transport == "psycopg" else stack.enter_context(_peer(transfer_postgres, "loom"))
        initial_maintenance = maintenance if transport == "psycopg" else stack.enter_context(_peer(transfer_postgres, "postgres"))
        evidence = MutationGuardEvidence.build(**{
            k: v for k, v in evidence.to_dict().items()
            if k not in {"schema_version", "evidence_digest", "database_backend_pid"}
        }, database_backend_pid=db_guard.info.backend_pid)
        interrupted = []
        mutations = []

        class InterruptCommit:
            def __init__(self, connection):
                self.connection, self.armed = connection, None
                self.info = connection.info
            def execute(self, query):
                text = query if isinstance(query, str) else query.as_string()
                result = self.connection.execute(query)
                phase = None
                if text.startswith("CREATE ROLE "):
                    phase = "owner"
                elif text.startswith("ALTER ROLE "):
                    phase = "seal"
                elif text.startswith("ALTER DATABASE "):
                    phase = "close"
                if phase:
                    mutations.append(phase)
                    self.armed = phase
                return result
            @contextmanager
            def transaction(self):
                with self.connection.transaction():
                    yield
                phase, self.armed = self.armed, None
                if phase is not None and phase == interruption and not interrupted:
                    interrupted.append(phase)
                    raise RuntimeError("phase acknowledgement lost")

        wrapped_peer, wrapped_maintenance = InterruptCommit(initial_peer), InterruptCommit(initial_maintenance)
        fixture = _Runner(live)
        class Runner(SubprocessProtectedApplyCommandRunner):
            def capture_stdout(self, *args, **kwargs):
                return fixture.capture_stdout(*args, **kwargs)
            def open_staging_peer_maintenance_database(self):
                return nullcontext(wrapped_maintenance)
        runner = Runner()
        fixture.environment = runner.environment
        record = journal.record_application_admission_recovery
        publications = []
        def publish(**kwargs):
            result = record(**kwargs)
            publications.append(result)
            if interruption == "admission-record" and not interrupted:
                interrupted.append(interruption)
                raise RuntimeError("phase acknowledgement lost")
            return result
        monkeypatch.setattr(journal, "record_application_admission_recovery", publish)
        outcomes = []
        def apply(_):
            journal.retain_application_guard(plan, guard=evidence)
            assert application_guard_is_retained(tmp_path / "state", request_id=plan.request_id,
                                                service_uid=os.getuid(), guard=evidence, acknowledge=True)
            outcomes.append(runner.prepare_staging_application_database(
                plan, journal=journal, connection=wrapped_peer, guard=evidence))
            raise RuntimeError("initial database phase verified")
        try:
            if interruption == "schema-drift":
                peer.execute("ALTER TABLE public.teams ADD COLUMN unadmitted integer")
                original_login = peer.execute("SELECT rolcanlogin,rolinherit,rolpassword FROM pg_authid WHERE rolname='loom'").fetchone()
                with pytest.raises(RuntimeError, match="schema"):
                    journal.execute(plan, [_component(apply)])
                assert mutations == [] and publications == []
                assert peer.execute("SELECT rolcanlogin,rolinherit,rolpassword FROM pg_authid WHERE rolname='loom'").fetchone() == original_login
                assert peer.execute("SELECT datallowconn FROM pg_database WHERE datname='loom'").fetchone() == (True,)
                return
            if interruption:
                with pytest.raises(RuntimeError, match="acknowledgement lost"):
                    journal.execute(plan, [_component(apply)])
            for _ in range(2):
                with pytest.raises(RuntimeError, match="initial database phase verified"):
                    journal.execute(plan, [_component(apply)])
            assert outcomes[0] == outcomes[1] == publications[0]
            assert len(publications) == 1
            assert mutations.count("owner") == 1
            assert mutations.count("close") == 1
            assert mutations.count("seal") == (2 if interruption == "seal" else 1)
            assert peer.execute("SELECT rolcanlogin,rolinherit,rolpassword FROM pg_authid WHERE rolname='loom'").fetchone() == (False, False, None)
            assert peer.execute("SELECT datallowconn,pg_get_userbyid(datdba) FROM pg_database WHERE datname='loom'").fetchone() == (False, "loom")
            assert outcomes[0].target.successor_role == APPLICATION_OWNER_ROLE
            assert outcomes[0].coordination_guard.backend.pid == db_guard.info.backend_pid
            assert not list(journal.root.rglob("terminal.json"))
        finally:
            maintenance.execute("ALTER DATABASE loom ALLOW_CONNECTIONS true")
            peer.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(APPLICATION_OWNER_ROLE)))


@pytest.mark.asyncio
@pytest.mark.parametrize("transfer_postgres", [17], indirect=True)
@pytest.mark.parametrize("transfer_database", ["protected-staging"], indirect=True)
async def test_completed_sql_profile_uses_separated_database_owner(transfer_database, tmp_path, monkeypatch):  # noqa: F811
    from contextlib import nullcontext
    from types import SimpleNamespace

    from loom_cli.rollout.operator import protected_application_handoff_component as module

    plan, _ = _setup(tmp_path)
    evidence = _guard(plan)
    request = dict(request_id=plan.request_id, candidate_sha=plan.candidate_sha,
        candidate_tree=plan.candidate_tree, generation=evidence.generation)
    with _closed(transfer_database, request=request) as (peer, maintenance, db_guard, _args):
        evidence = MutationGuardEvidence.build(**{key: value for key, value in evidence.to_dict().items()
            if key not in {"schema_version", "evidence_digest", "database_backend_pid"}}, database_backend_pid=db_guard.info.backend_pid)
        runner = SimpleNamespace(open_staging_peer_maintenance_database=lambda: nullcontext(maintenance),
            open_staging_peer_template_database=lambda: nullcontext(object()))
        seen = []
        monkeypatch.setattr(module, "require_cnpg_effective_sql_profile", lambda *args, **kwargs: seen.append(kwargs["database"]))
        peer.execute("CREATE ROLE loom_app_staging_owner NOLOGIN NOINHERIT")
        try:
            maintenance.execute("ALTER DATABASE loom OWNER TO loom_app_staging_owner")
            with pytest.raises(RuntimeError, match="database authority"):
                module._admit_sql_profiles(runner, peer, evidence)
            module._admit_sql_profiles(runner, peer, evidence, separated_owner=True)
            assert seen == ["loom", "postgres", "template1"]
        finally:
            maintenance.execute("ALTER DATABASE loom OWNER TO loom")
            peer.execute("DROP ROLE loom_app_staging_owner")
