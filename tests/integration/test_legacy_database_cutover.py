"""Journaled cutover on actual PostgreSQL using the installed peer transport."""

import os
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
        schema_acl_profile="staging-readonly", schema_revision="0147/guard_0036",
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
