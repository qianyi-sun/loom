"""Recover transient migrator cleanup while ordinary database admission is closed."""

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest

from loom.application_handoff_completion import complete_application_handoff_database
from loom.application_migrator_provision import (
    arm_application_migrator,
    create_application_migrator,
    seal_application_migrator,
)
from loom.application_migrator_retirement import retire_application_migrator
from tests.integration.test_application_handoff_completion import _closed
from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)

pytestmark = pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)


class LostAdmissionReplyError(RuntimeError):
    pass


class LoseCommit:
    def __init__(self, connection):
        self.connection, self.armed, self.interrupted = connection, False, False

    @property
    def info(self):
        return self.connection.info

    def execute(self, query):
        rendered = query if isinstance(query, str) else query.as_string(self.connection)
        result = self.connection.execute(query)
        if rendered.startswith("ALTER DATABASE "):
            self.armed = True
        return result

    @contextmanager
    def transaction(self):
        with self.connection.transaction():
            yield
        if self.armed and not self.interrupted:
            self.interrupted = True
            raise LostAdmissionReplyError("committed admission reply lost")


@pytest.mark.asyncio
@pytest.mark.parametrize("interruption", [None, "close", "reopen"])
async def test_cleanup_admission_recovers_commits_and_preserves_original_runtime(transfer_database, interruption):  # noqa: F811
    from loom.application_migrator_admission import (
        close_application_migrator_admission,
        reopen_application_migrator_admission,
    )

    with _closed(transfer_database) as (peer, maintenance, guard, args):
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        target = args["target"]
        authority = dict(target=target, coordination_guard=args["coordination_guard"],
            provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"))
        identity = create_application_migrator(peer, **authority, migrator_role="app_migrator_" + uuid4().hex,
            persist_identity=lambda _: None)
        arm_application_migrator(peer, **authority, identity=identity, password=uuid4().hex,
            expires_at=datetime.now(UTC) + timedelta(minutes=45))
        with pytest.raises(RuntimeError, match="sealed"):
            close_application_migrator_admission(maintenance, **authority, identity=identity)
        seal_application_migrator(maintenance, **authority, identity=identity)
        if interruption == "close":
            with pytest.raises(LostAdmissionReplyError):
                close_application_migrator_admission(LoseCommit(maintenance), **authority, identity=identity)
        close_application_migrator_admission(maintenance, **authority, identity=identity)
        assert peer.execute("SELECT datallowconn FROM pg_database WHERE oid=%s", (target.database_oid,)).fetchone() == (False,)
        with pytest.raises(RuntimeError, match="retired"):
            reopen_application_migrator_admission(maintenance, **authority, identity=identity, runtime_password=args["password"])
        retire_application_migrator(maintenance, **authority, migrator_role=identity.role_name, migrator_oid=identity.role_oid)
        with pytest.raises(RuntimeError, match="runtime"):
            reopen_application_migrator_admission(maintenance, **authority, identity=identity, runtime_password="wrong-original-password")
        if interruption == "reopen":
            with pytest.raises(LostAdmissionReplyError):
                reopen_application_migrator_admission(LoseCommit(maintenance), **authority, identity=identity, runtime_password=args["password"])
        for _ in range(2):
            reopen_application_migrator_admission(maintenance, **authority, identity=identity, runtime_password=args["password"])
        with psycopg.connect(transfer_database[0], user=target.owner_role, password=args["password"], autocommit=True) as runtime:
            assert runtime.execute("SELECT count(*) FROM trials").fetchone() == (0,)
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                runtime.execute("CREATE TABLE must_remain_restricted(id int)")
        assert guard.execute("SELECT 1").fetchone() == (1,)
