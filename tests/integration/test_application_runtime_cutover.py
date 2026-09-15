"""Compose runtime login, admission and SQL retirement with retained identities."""

from contextlib import contextmanager

import psycopg
import pytest

from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)
from tests.integration.test_application_runtime_retirement import _identity, _runtime

pytestmark = pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)


@pytest.mark.parametrize("lost_ack", [False, True])
def test_runtime_cutover_recovers_closed_admission_without_credential_rotation(transfer_database, lost_ack):  # noqa: F811
    from loom.application_runtime_cutover import close_application_runtime_for_cutover

    with _runtime(transfer_database) as (peer, maintenance, guard, active, arguments, options):
        before = _identity(peer, options["target"])

        class Interrupted:
            pending = False
            raised = False

            @property
            def info(self):
                return maintenance.info

            @contextmanager
            def transaction(self):
                with maintenance.transaction():
                    yield
                if self.pending and not self.raised:
                    self.raised = True
                    raise RuntimeError("closed admission commit acknowledgement lost")

            def execute(self, query):
                result = maintenance.execute(query)
                rendered = query if isinstance(query, str) else query.as_string(maintenance)
                if rendered.startswith("ALTER DATABASE "):
                    self.pending = True
                return result

        if lost_ack:
            with pytest.raises(RuntimeError, match="acknowledgement lost"):
                close_application_runtime_for_cutover(peer, maintenance=Interrupted(), **arguments)
            assert active.execute("SELECT 1").fetchone() == (1,)
            assert maintenance.execute("SELECT datallowconn FROM pg_database WHERE oid=%s",
                (options["target"].database_oid,)).fetchone() == (False,)
        outcome = close_application_runtime_for_cutover(peer, maintenance=maintenance, **arguments)
        assert close_application_runtime_for_cutover(peer, maintenance=maintenance, **arguments) == outcome
        assert outcome.target == options["target"]
        with pytest.raises(psycopg.OperationalError):
            active.execute("SELECT 1")
        after = _identity(peer, options["target"])
        before[0][0][0]["rolcanlogin"] = False
        assert after == before
        assert guard.execute("SELECT 1").fetchone() == (1,)


def test_runtime_cutover_refuses_privileged_foreign_client_before_completion(transfer_database):  # noqa: F811
    from loom.application_runtime_cutover import close_application_runtime_for_cutover

    with _runtime(transfer_database) as (peer, maintenance, guard, _, arguments, _options):
        with psycopg.connect(transfer_database[0], dbname="postgres", autocommit=True) as foreign:
            with pytest.raises(RuntimeError, match="surviving or unknown"):
                close_application_runtime_for_cutover(peer, maintenance=maintenance, **arguments)
            assert foreign.execute("SELECT 1").fetchone() == (1,)
        close_application_runtime_for_cutover(peer, maintenance=maintenance, **arguments)
        assert guard.execute("SELECT 1").fetchone() == (1,)


def test_runtime_cutover_refuses_unrecorded_peer_before_sealing(transfer_database):  # noqa: F811
    from loom.application_runtime_cutover import close_application_runtime_for_cutover

    with _runtime(transfer_database) as (peer, maintenance, _, active, arguments, options):
        before = _identity(peer, options["target"])
        with psycopg.connect(transfer_database[0], autocommit=True) as replacement:
            with pytest.raises(RuntimeError, match="peer changed"):
                close_application_runtime_for_cutover(replacement, maintenance=maintenance, **arguments)
        assert _identity(peer, options["target"]) == before
        assert active.execute("SELECT 1").fetchone() == (1,)
        assert maintenance.execute("SELECT datallowconn FROM pg_database WHERE oid=%s",
            (options["target"].database_oid,)).fetchone() == (True,)
