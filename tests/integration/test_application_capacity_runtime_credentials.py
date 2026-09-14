"""Capacity runtime credentials remain unprivileged and fixed through bootstrap."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from loom.application_handoff_completion import complete_application_handoff_database
from tests.integration.test_application_handoff_completion import _closed
from tests.integration.test_application_migrator_admission import LoseCommit, LostAdmissionReplyError
from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)

pytestmark = [pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True),
    pytest.mark.parametrize("transfer_database", ["protected-staging"], indirect=True)]


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_reply", [False, True])
async def test_capacity_runtime_credentials_arm_and_finalize_saved_roles(transfer_database, lost_reply):  # noqa: F811
    from loom.application_capacity_runtime_credentials import (
        arm_application_capacity_runtime_credentials,
        finalize_application_capacity_runtime_credentials,
    )

    with _closed(transfer_database) as (peer, maintenance, guard, args):
        args["schema_acl_profile"] = "cnpg-staging"
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        roles = {role: peer.execute("SELECT oid FROM pg_roles WHERE rolname=%s", (role,)).fetchone()[0]
            for role, alias in args["role_bindings"].items() if alias in {"guard-agent", "guard-executor", "guard-observer", "guard-runtime"}}
        passwords = {role: uuid4().hex for role in roles if not role.endswith("executor")}
        for role in passwords:
            peer.execute(sql.SQL("ALTER ROLE {} NOLOGIN PASSWORD NULL").format(sql.Identifier(role)))
        authority = dict(target=args["target"], coordination_guard=args["coordination_guard"], role_oids=roles,
            provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"), passwords=passwords)
        expiry = datetime.now(UTC) + timedelta(minutes=45)

        def invoke(operation, **kwargs):
            if lost_reply:
                interrupted = LoseCommit(maintenance)
                interrupted.armed = True
                with pytest.raises(LostAdmissionReplyError):
                    operation(interrupted, **authority, **kwargs)
            operation(maintenance, **authority, **kwargs)

        invoke(arm_application_capacity_runtime_credentials, expires_at=expiry)
        rows = peer.execute("SELECT oid,rolpassword,rolvaliduntil FROM pg_authid WHERE oid=ANY(%s) ORDER BY oid", (list(roles.values()),)).fetchall()
        arm_application_capacity_runtime_credentials(maintenance, **authority, expires_at=expiry)
        assert peer.execute("SELECT oid,rolpassword,rolvaliduntil FROM pg_authid WHERE oid=ANY(%s) ORDER BY oid", (list(roles.values()),)).fetchall() == rows
        changed_passwords = {**passwords, "loom_cap_staging_agent": "wrong-original-password"}
        with pytest.raises(RuntimeError, match="credential"):
            arm_application_capacity_runtime_credentials(maintenance, **{**authority, "passwords": changed_passwords}, expires_at=expiry)
        with pytest.raises(RuntimeError, match="identity"):
            finalize_application_capacity_runtime_credentials(maintenance, **{**authority, "role_oids": {**roles, "loom_cap_staging_runtime": roles["loom_cap_staging_runtime"] + 100}})
        with psycopg.connect(transfer_database[0], user="loom_cap_staging_agent", password=passwords["loom_cap_staging_agent"], autocommit=True) as agent:
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                agent.execute("CREATE TABLE must_not_gain_owner_access(id integer)")
        invoke(finalize_application_capacity_runtime_credentials)
        assert peer.execute("SELECT bool_and(rolvaliduntil='infinity'::timestamptz) FROM pg_authid WHERE rolname=ANY(%s)", (list(passwords),)).fetchone() == (True,)
        # A later bootstrap cannot shorten an already-durable runtime credential.
        arm_application_capacity_runtime_credentials(maintenance, **authority, expires_at=expiry + timedelta(minutes=1))
        assert peer.execute("SELECT bool_and(rolvaliduntil='infinity'::timestamptz) FROM pg_authid WHERE rolname=ANY(%s)", (list(passwords),)).fetchone() == (True,)
        assert guard.execute("SELECT 1").fetchone() == (1,)
