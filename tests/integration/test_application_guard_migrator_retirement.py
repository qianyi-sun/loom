"""Both elevated owner sessions retire while the permanent guard role stays sealed."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from loom.application_completed_authority import ApplicationGuardOwner, ApplicationOwnerSuccessor
from loom.application_handoff_completion import complete_application_handoff_database
from tests.integration.test_application_handoff_completion import _closed
from tests.integration.test_application_migrator_admission import (
    LoseCommit,
    LostAdmissionReplyError,
)
from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)

pytestmark = pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("selected_owner", ["application", "guard"])
@pytest.mark.parametrize("interruption", [None, "close", "reopen"])
async def test_guard_migrator_retires_both_owner_sessions_without_dropping_guard_roles(transfer_database, selected_owner, interruption):  # noqa: F811
    from loom.application_guard_migrator_retirement import (
        close_application_guard_migrator_admission,
        reopen_application_guard_migrator_admission,
        retire_application_guard_migrator,
    )

    with _closed(transfer_database) as (peer, maintenance, active_guard, args):
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        target = args["target"]
        owner = next(role for role, alias in args["role_bindings"].items() if alias == "guard-owner")
        migrator = next(role for role, alias in args["role_bindings"].items() if alias == "guard-migrator")
        owner_oid = peer.execute("SELECT oid FROM pg_roles WHERE rolname=%s", (owner,)).fetchone()[0]
        migrator_oid = peer.execute("SELECT oid FROM pg_roles WHERE rolname=%s", (migrator,)).fetchone()[0]
        identity = ApplicationOwnerSuccessor(migrator, migrator_oid, ApplicationGuardOwner(owner, owner_oid))
        password = uuid4().hex
        peer.execute(sql.SQL("GRANT {} TO {} WITH ADMIN FALSE, INHERIT TRUE, SET TRUE").format(
            sql.Identifier(target.successor_role), sql.Identifier(migrator)))
        peer.execute(sql.SQL("GRANT {} TO {} WITH ADMIN FALSE, INHERIT TRUE, SET TRUE").format(
            sql.Identifier(owner), sql.Identifier(migrator)))
        peer.execute(sql.SQL("ALTER ROLE {} LOGIN INHERIT PASSWORD {} VALID UNTIL {}").format(
            sql.Identifier(migrator), sql.Literal(password), sql.Literal((datetime.now(UTC) + timedelta(minutes=45)).isoformat())))
        authority = dict(target=target, coordination_guard=args["coordination_guard"], identity=identity,
            provisioner_role=next(role for role, alias in args["role_bindings"].items() if alias == "provisioner"))
        try:
            with psycopg.connect(transfer_database[0], user=migrator, password=password, autocommit=True) as job:
                job.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(target.successor_role if selected_owner == "application" else owner)))
                with pytest.raises(RuntimeError, match="sealed"):
                    close_application_guard_migrator_admission(maintenance, **authority)
                peer.execute(sql.SQL("ALTER ROLE {} NOLOGIN PASSWORD NULL").format(sql.Identifier(migrator)))
                if interruption == "close":
                    with pytest.raises(LostAdmissionReplyError):
                        close_application_guard_migrator_admission(LoseCommit(maintenance), **authority)
                close_application_guard_migrator_admission(maintenance, **authority)
                with pytest.raises(RuntimeError):
                    reopen_application_guard_migrator_admission(maintenance, **authority, runtime_password=args["password"])
                for _ in range(2):
                    retire_application_guard_migrator(maintenance, **authority)
                with pytest.raises(psycopg.OperationalError):
                    job.execute("SELECT 1")
            with pytest.raises(RuntimeError, match="runtime credential"):
                reopen_application_guard_migrator_admission(maintenance, **authority, runtime_password="wrong-password")
            if interruption == "reopen":
                with pytest.raises(LostAdmissionReplyError):
                    reopen_application_guard_migrator_admission(LoseCommit(maintenance), **authority, runtime_password=args["password"])
            for _ in range(2):
                reopen_application_guard_migrator_admission(maintenance, **authority, runtime_password=args["password"])
            assert peer.execute("SELECT oid,rolcanlogin,rolpassword,rolinherit FROM pg_authid WHERE rolname=%s", (migrator,)).fetchone() == (migrator_oid, False, None, True)
            assert peer.execute("SELECT nspowner FROM pg_namespace WHERE nspname='loom_capacity_guard'").fetchone() == (owner_oid,)
            assert active_guard.execute("SELECT 1").fetchone() == (1,)
        finally:
            peer.execute(sql.SQL("REVOKE {},{} FROM {}; ALTER ROLE {} NOLOGIN PASSWORD NULL").format(
                sql.Identifier(target.successor_role), sql.Identifier(owner), sql.Identifier(migrator), sql.Identifier(migrator)))
