"""Guard bootstrap grants the saved two-owner identity with one fixed lease."""

from dataclasses import replace
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
@pytest.mark.parametrize("lost_reply", [False, True])
async def test_guard_bootstrap_arms_exact_separated_owners_and_retires(transfer_database, lost_reply):  # noqa: F811
    from loom.application_guard_migrator_provision import (
        arm_application_guard_migrator,
        seal_application_guard_migrator,
    )
    from loom.application_guard_migrator_retirement import (
        close_application_guard_migrator_admission,
        reopen_application_guard_migrator_admission,
        require_application_guard_migrator_retired,
        retire_application_guard_migrator,
    )

    with _closed(transfer_database) as (peer, maintenance, guard, args):
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        target = args["target"]
        role = next(n for n, a in args["role_bindings"].items() if a == "guard-migrator")
        owner = next(n for n, a in args["role_bindings"].items() if a == "guard-owner")
        identity = ApplicationOwnerSuccessor(role,
            peer.execute("SELECT oid FROM pg_roles WHERE rolname=%s", (role,)).fetchone()[0],
            ApplicationGuardOwner(owner, peer.execute("SELECT oid FROM pg_roles WHERE rolname=%s", (owner,)).fetchone()[0]))
        authority = dict(target=target, coordination_guard=args["coordination_guard"], identity=identity,
            provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"))
        password, expiry = uuid4().hex, datetime.now(UTC) + timedelta(minutes=45)
        try:
            with pytest.raises(RuntimeError, match="identity"):
                arm_application_guard_migrator(peer, **{**authority, "identity": replace(identity, role_oid=identity.role_oid + 100)},
                    password=password, expires_at=expiry)
            if lost_reply:
                interrupted = LoseCommit(peer)
                interrupted.armed = True
                with pytest.raises(LostAdmissionReplyError):
                    arm_application_guard_migrator(interrupted, **authority, password=password, expires_at=expiry)
            arm_application_guard_migrator(peer, **authority, password=password, expires_at=expiry)
            original = peer.execute("SELECT rolpassword,rolvaliduntil FROM pg_authid WHERE oid=%s", (identity.role_oid,)).fetchone()
            arm_application_guard_migrator(peer, **authority, password=password, expires_at=expiry)
            assert peer.execute("SELECT rolpassword,rolvaliduntil FROM pg_authid WHERE oid=%s", (identity.role_oid,)).fetchone() == original
            for attempted_password, attempted_expiry in (("changed", expiry), (password, expiry + timedelta(minutes=1))):
                with pytest.raises(RuntimeError, match="credential"):
                    arm_application_guard_migrator(peer, **authority, password=attempted_password, expires_at=attempted_expiry)
            memberships = peer.execute("SELECT roleid FROM pg_auth_members WHERE member=%s ORDER BY roleid", (identity.role_oid,)).fetchall()
            assert memberships == sorted([(target.successor_oid,), (identity.guard_owner.role_oid,)])
            assert (target.owner_oid,) not in memberships
            with pytest.raises(RuntimeError):
                require_application_guard_migrator_retired(peer, **authority)
            with psycopg.connect(transfer_database[0], user=role, password=password, autocommit=True) as job:
                for selected in (target.successor_role, owner):
                    job.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(selected)))
                    assert job.execute("SELECT current_user").fetchone() == (selected,)
                for _ in range(2):
                    seal_application_guard_migrator(maintenance, **authority)
                close_application_guard_migrator_admission(maintenance, **authority)
                retire_application_guard_migrator(maintenance, **authority)
                require_application_guard_migrator_retired(maintenance, **authority)
                with pytest.raises(psycopg.OperationalError):
                    job.execute("SELECT 1")
            reopen_application_guard_migrator_admission(maintenance, **authority, runtime_password=args["password"])
            require_application_guard_migrator_retired(peer, **authority)
            assert guard.execute("SELECT 1").fetchone() == (1,)
        finally:
            peer.execute(sql.SQL("REVOKE {},{} FROM {}; ALTER ROLE {} NOLOGIN PASSWORD NULL").format(
                sql.Identifier(target.successor_role), sql.Identifier(owner), sql.Identifier(role), sql.Identifier(role)))
