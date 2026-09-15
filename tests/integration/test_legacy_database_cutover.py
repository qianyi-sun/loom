"""Journaled cutover on actual PostgreSQL using the installed peer transport."""

import os
from contextlib import contextmanager
from dataclasses import replace

import psycopg
import pytest

from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)
from tests.integration.test_application_runtime_retirement import _runtime
from tests.integration.test_protected_peer_database_connection import _peer

pytestmark = pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)


def _operation(tmp_path, postgres, guard, arguments):
    from loom_cli.rollout.operator.protected_legacy_database_cutover import (
        LegacyDatabaseCutover,
        LegacyDatabaseCutoverJournal,
    )

    class Runner:
        def open_staging_peer_database(self):
            return _peer(postgres, database=arguments["target"].database)

        def open_staging_peer_maintenance_database(self):
            return _peer(postgres, database="postgres")

    def check():
        assert guard.execute("SELECT 1").fetchone() == (1,)

    return LegacyDatabaseCutover(
        journal=LegacyDatabaseCutoverJournal(tmp_path, "req-cutover", 1, os.getuid()),
        plan_digest="a" * 64, credential_binding_sha256="b" * 64,
        target=arguments["target"], coordination_guard=arguments["coordination_guard"],
        role_bindings=arguments["role_bindings"], password=arguments["password"],
        schema_acl_profile="staging-readonly", schema_revision="0148/guard_0036",
        runner=Runner(), authority_check=check,
    )


@pytest.mark.parametrize("interruption", [None, "peer-00.terminal.json", "cutover.terminal.json"])
def test_cutover_journal_recovers_peer_loss_and_never_reuses_handoff_journal(
    transfer_database, transfer_postgres, tmp_path, monkeypatch, interruption,  # noqa: F811
):
    with _runtime(transfer_database) as (peer, maintenance, guard, active, arguments, _):
        operation = _operation(tmp_path, transfer_postgres, guard, arguments)
        peer.close()
        maintenance.close()
        original = type(operation.journal).retain
        injected = []

        def retain(journal, name, value):
            original(journal, name, value)
            if name == interruption and not injected:
                injected.append(name)
                raise RuntimeError("retained cutover reply lost")

        monkeypatch.setattr(type(operation.journal), "retain", retain)
        if interruption is not None:
            with pytest.raises(RuntimeError, match="reply lost"):
                operation.retire()
        terminal = operation.retire()
        files = {p.name: p.read_bytes() for p in operation.journal.root.iterdir() if p.is_file()}
        assert operation.retire() == terminal
        assert {p.name: p.read_bytes() for p in operation.journal.root.iterdir() if p.is_file()} == files
        assert operation.journal.root.parts[-3] == "legacy-database-cutover-journals"
        with pytest.raises(psycopg.OperationalError):
            active.execute("SELECT 1")
        with pytest.raises(RuntimeError, match="binding"):
            replace(operation, plan_digest="c" * 64).retire()
        assert guard.execute("SELECT 1").fetchone() == (1,)


def test_cutover_recovery_refuses_foreign_peer_without_signalling_it(
    transfer_database, transfer_postgres, tmp_path,  # noqa: F811
):
    with _runtime(transfer_database) as (peer, maintenance, guard, _, arguments, _options):
        operation = _operation(tmp_path, transfer_postgres, guard, arguments)
        peer.close()
        maintenance.close()
        with psycopg.connect(transfer_database[0], dbname="postgres", autocommit=True) as foreign:
            with pytest.raises(RuntimeError, match="surviving or unknown"):
                operation.retire()
            assert foreign.execute("SELECT 1").fetchone() == (1,)
            with pytest.raises(RuntimeError, match="surviving or unknown"):
                operation.retire()
            assert foreign.execute("SELECT 1").fetchone() == (1,)
        operation.retire()


def test_terminal_cutover_replay_refuses_a_resumed_runtime_without_signalling(
    transfer_database, transfer_postgres, tmp_path,  # noqa: F811
):
    from psycopg import sql

    with _runtime(transfer_database) as (peer, maintenance, guard, _, arguments, _options):
        operation = _operation(tmp_path, transfer_postgres, guard, arguments)
        peer.close()
        maintenance.close()
        operation.retire()
        files = {p.name: p.read_bytes() for p in operation.journal.root.iterdir() if p.is_file()}
        with psycopg.connect(transfer_database[0], dbname="postgres", autocommit=True) as admin:
            admin.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS true").format(sql.Identifier(operation.target.database)))
            admin.execute(sql.SQL("ALTER ROLE {} LOGIN").format(sql.Identifier(operation.target.owner_role)))
            with psycopg.connect(transfer_database[0], user=operation.target.owner_role,
                                 password=operation.password, autocommit=True) as resumed:
                admin.execute(sql.SQL("ALTER ROLE {} NOLOGIN").format(sql.Identifier(operation.target.owner_role)))
                admin.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(sql.Identifier(operation.target.database)))
                admin.close()
                with pytest.raises(RuntimeError, match="sessions remain"):
                    operation.retire()
                assert resumed.execute("SELECT 1").fetchone() == (1,)
        assert {p.name: p.read_bytes() for p in operation.journal.root.iterdir() if p.is_file()} == files


def test_cutover_recovery_recloses_after_lost_reopen_commit_reply(
    transfer_database, transfer_postgres, tmp_path, monkeypatch,  # noqa: F811
):
    with _runtime(transfer_database) as (peer, maintenance, guard, _, arguments, _options):
        operation = _operation(tmp_path, transfer_postgres, guard, arguments)
        peer.close()
        maintenance.close()
        observe = type(operation)._observe_closed
        interrupted = []

        def lose_observation(self, backend):
            if not interrupted:
                interrupted.append(True)
                raise RuntimeError("cutover stopped before terminal publication")
            return observe(self, backend)

        monkeypatch.setattr(type(operation), "_observe_closed", lose_observation)
        with pytest.raises(RuntimeError, match="before terminal"):
            operation.retire()
        opener = operation.runner.open_staging_peer_maintenance_database
        lost_reopen = []

        class LostReply:
            def __init__(self):
                self.connection = opener()
                self.reopened = False

            @property
            def info(self):
                return self.connection.info

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return self.connection.__exit__(*args)

            def close(self):
                self.connection.close()

            def execute(self, query):
                result = self.connection.execute(query)
                rendered = query if isinstance(query, str) else query.as_string()
                if rendered.startswith("ALTER DATABASE ") and "ALLOW_CONNECTIONS true" in rendered:
                    self.reopened = True
                return result

            @contextmanager
            def transaction(self):
                with self.connection.transaction():
                    yield
                if self.reopened and not lost_reopen:
                    lost_reopen.append(True)
                    raise RuntimeError("reopen commit reply lost")

        monkeypatch.setattr(operation.runner, "open_staging_peer_maintenance_database", LostReply)
        with pytest.raises(RuntimeError, match="reopen commit reply lost"):
            operation.retire()
        assert lost_reopen == [True]
        with opener() as check:
            assert check.execute("SELECT datallowconn FROM pg_database WHERE datname='" + operation.target.database + "'").fetchone() == (False,)
        operation.retire()


@pytest.mark.parametrize("interruption", [
    None, "peer-00.terminal.json", "freeze-commit", "trial-writer.terminal.json", "cutover.terminal.json",
])
async def test_cutover_freezes_real_trial_ledger_before_peer_exit_and_recovers(
    transfer_database, transfer_postgres, tmp_path, monkeypatch, interruption,  # noqa: F811
):
    from contextlib import nullcontext
    from uuid import uuid4

    from loom_cli.rollout.operator.protected_trial_writer_control import (
        ProtectedTrialWriterControl,
        TrialWriterControlBinding,
    )
    from tests.integration.test_capacity_agent_store import _initialize_and_register, _seed_trial

    url, _, bindings = transfer_database
    database = {
        "admin_url": url.replace("postgresql://", "postgresql+psycopg://", 1),
        "migrator_url": url.replace("postgresql://", "postgresql+psycopg://", 1),
        "owner_role": next(role for role, alias in bindings.items() if alias == "guard-owner"),
        "agent_role": next(role for role, alias in bindings.items() if alias == "guard-agent"),
    }
    trial = _seed_trial(database)
    _, registration = await _initialize_and_register(database)
    binding = TrialWriterControlBinding(registration, uuid4(), uuid4())
    with _runtime(transfer_database) as (peer, maintenance, guard, active, arguments, _):
        operation = replace(_operation(tmp_path, transfer_postgres, guard, arguments), trial_writer_binding=binding)
        # Account a real committed legacy mutation, rather than supplying a cursor.
        control = ProtectedTrialWriterControl(lambda: nullcontext(peer), binding)
        control.initialize()
        active.execute("UPDATE public.trials SET submit_priority=101 WHERE id=%s", (trial,))
        assert control.capture().high_water == 1
        peer.close()
        maintenance.close()
        original_retain = type(operation.journal).retain
        original_freeze = ProtectedTrialWriterControl.freeze
        injected = []

        def retain(journal, name, value):
            original_retain(journal, name, value)
            if name == interruption and not injected:
                injected.append(name)
                raise RuntimeError("trial cutover reply lost")

        def freeze(self):
            result = original_freeze(self)
            if interruption == "freeze-commit" and not injected:
                injected.append(interruption)
                raise RuntimeError("trial cutover reply lost")
            return result

        monkeypatch.setattr(type(operation.journal), "retain", retain)
        monkeypatch.setattr(ProtectedTrialWriterControl, "freeze", freeze)
        if interruption:
            with pytest.raises(RuntimeError, match="trial cutover reply lost"):
                operation.retire()
        terminal = operation.retire()
        evidence = operation.journal.read("trial-writer.terminal.json")
        assert evidence["observation"]["high_water"] == 1
        assert evidence["observation"]["frozen"] is True
        assert evidence["observation"]["writer_incarnation"] == str(binding.writer_incarnation)
        assert evidence["observation"]["freeze_operation_id"] == str(binding.freeze_operation_id)
        from loom_cli.rollout.operator.protected_application_admission_recovery import (
            admission_record_digest,
        )
        assert terminal["trial_writer_digest"] == admission_record_digest(evidence)
        assert operation.journal.read("cutover.intent.json")["trial_writer_binding"]["registration"] == registration.model_dump(mode="json")
        files = {p.name: p.read_bytes() for p in operation.journal.root.iterdir() if p.is_file()}

        def refuse_peer():
            raise AssertionError("terminal replay must not reopen the application database")

        monkeypatch.setattr(operation.runner, "open_staging_peer_database", refuse_peer)
        assert operation.retire() == terminal
        with pytest.raises(RuntimeError, match="binding"):
            replace(operation, trial_writer_binding=replace(binding, freeze_operation_id=uuid4())).retire()
        with pytest.raises(RuntimeError, match="binding"):
            replace(operation, trial_writer_binding=None).retire()
        assert {p.name: p.read_bytes() for p in operation.journal.root.iterdir() if p.is_file()} == files
        with pytest.raises(psycopg.OperationalError):
            active.execute("SELECT 1")


@pytest.mark.parametrize("registration_drift", [False, True])
async def test_cutover_initializes_only_the_actual_registered_trial_writer(
    transfer_database, transfer_postgres, tmp_path, registration_drift,  # noqa: F811
):
    from uuid import uuid4

    from loom_cli.rollout.operator.protected_trial_writer_control import TrialWriterControlBinding
    from tests.integration.test_capacity_agent_store import _initialize_and_register

    url, _, bindings = transfer_database
    _, registration = await _initialize_and_register({
        "migrator_url": url.replace("postgresql://", "postgresql+psycopg://", 1),
        "owner_role": next(role for role, alias in bindings.items() if alias == "guard-owner"),
        "agent_role": next(role for role, alias in bindings.items() if alias == "guard-agent"),
    })
    if registration_drift:
        registration = registration.model_copy(update={"configuration_generation": registration.configuration_generation + 1})
    binding = TrialWriterControlBinding(registration, uuid4(), uuid4())
    with _runtime(transfer_database) as (peer, maintenance, guard, _, arguments, _):
        operation = replace(_operation(tmp_path, transfer_postgres, guard, arguments), trial_writer_binding=binding)
        peer.close()
        maintenance.close()
        if registration_drift:
            with pytest.raises(ValueError, match="registration drifted"):
                operation.retire()
            assert operation.journal.read("cutover.terminal.json") is None
            assert operation.journal.read("trial-writer.terminal.json") is None
        else:
            terminal = operation.retire()
            evidence = operation.journal.read("trial-writer.terminal.json")
            assert evidence["observation"]["high_water"] == 0
            assert evidence["observation"]["frozen"] is True
            assert operation.retire() == terminal
