"""Existing executor credentials issue atomically and retain exact identity on replay."""

from dataclasses import replace
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

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

pytestmark = [pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True),
              pytest.mark.parametrize("transfer_database", ["protected-staging"], indirect=True)]


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_reply", [False, True])
async def test_existing_executor_issues_only_connect_and_replays_without_password_reset(transfer_database, lost_reply):  # noqa: F811
    from loom.application_executor_admission import (
        admit_sealed_executor,
        issue_executor_admission,
        require_issued_executor,
    )

    with _closed(transfer_database) as (peer, maintenance, _guard, args):
        args["schema_acl_profile"] = "cnpg-staging"
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        authority = dict(target=args["target"], coordination_guard=args["coordination_guard"],
                         provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"))
        identity = admit_sealed_executor(peer, **authority)
        password = uuid4().hex + uuid4().hex
        if lost_reply:
            interrupted = LoseCommit(peer)
            interrupted.armed = True
            with pytest.raises(LostAdmissionReplyError):
                issue_executor_admission(interrupted, **authority, identity=identity, password=password)
        issue_executor_admission(peer, **authority, identity=identity, password=password)
        original = peer.execute("SELECT oid,rolpassword,rolvaliduntil::text FROM pg_authid WHERE rolname='loom_cap_staging_executor'").fetchone()
        issue_executor_admission(peer, **authority, identity=identity, password=password)
        require_issued_executor(peer, **authority, identity=identity, password=password)
        assert peer.execute("SELECT oid,rolpassword,rolvaliduntil::text FROM pg_authid WHERE rolname='loom_cap_staging_executor'").fetchone() == original
        assert original[0] == identity.role_oid
        with pytest.raises(RuntimeError, match="credential"):
            issue_executor_admission(peer, **authority, identity=identity, password="wrong" * 10)
        with pytest.raises(RuntimeError, match="identity"):
            issue_executor_admission(peer, **authority, identity=replace(identity, role_oid=identity.role_oid + 999), password=password)
        with pytest.raises(RuntimeError, match="sealed"):
            admit_sealed_executor(peer, **authority)
        assert peer.execute("SELECT a.grantor::bigint,a.privilege_type,a.is_grantable FROM pg_database d CROSS JOIN LATERAL aclexplode(d.datacl) a WHERE d.datname=current_database() AND a.grantee=%s", (identity.role_oid,)).fetchall() == [(args["target"].successor_oid, "CONNECT", False)]
        with psycopg.connect(transfer_database[0], user="loom_cap_staging_executor", password=password, autocommit=True) as executor:
            assert executor.execute("SELECT session_user").fetchone() == ("loom_cap_staging_executor",)
            for statement in ("CREATE TABLE public.forbidden(id integer)", "DELETE FROM public.trials", "SET ROLE loom_cap_staging_owner", "CREATE SCHEMA forbidden"):
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    executor.execute(statement)


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["membership", "elevation", "role-setting", "function-grant", "database-grant", "public-grant", "implicit-public-function"])
async def test_executor_admission_refuses_changed_authority_before_issuance(transfer_database, drift):  # noqa: F811
    from loom.application_executor_admission import admit_sealed_executor, issue_executor_admission

    with _closed(transfer_database) as (peer, maintenance, _guard, args):
        args["schema_acl_profile"] = "cnpg-staging"
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        authority = dict(target=args["target"], coordination_guard=args["coordination_guard"],
                         provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"))
        identity = admit_sealed_executor(peer, **authority)
        if drift == "membership":
            peer.execute("GRANT loom_cap_staging_owner TO loom_cap_staging_executor")
        elif drift == "elevation":
            peer.execute("ALTER ROLE loom_cap_staging_executor CREATEDB")
        elif drift == "role-setting":
            peer.execute("ALTER ROLE loom_cap_staging_executor SET search_path=public")
        elif drift == "function-grant":
            peer.execute("REVOKE ALL ON ALL FUNCTIONS IN SCHEMA loom_capacity_guard FROM loom_cap_staging_executor")
        elif drift == "public-grant":
            peer.execute("GRANT SELECT ON public.trials TO PUBLIC")
        elif drift == "implicit-public-function":
            peer.execute("CREATE FUNCTION public.executor_unexpected() RETURNS integer LANGUAGE sql AS 'SELECT 1'")
        else:
            peer.execute(sql.SQL("GRANT TEMPORARY ON DATABASE {} TO loom_cap_staging_executor").format(sql.Identifier(args["target"].database)))
        with pytest.raises(RuntimeError):
            issue_executor_admission(peer, **authority, identity=identity, password=uuid4().hex * 2)
        assert peer.execute("SELECT rolcanlogin,rolpassword FROM pg_authid WHERE oid=%s", (identity.role_oid,)).fetchone() == (False, None)


@pytest.mark.asyncio
async def test_executor_admission_rolls_back_login_and_connect_together(transfer_database):  # noqa: F811
    from loom.application_executor_admission import admit_sealed_executor, issue_executor_admission

    with _closed(transfer_database) as (peer, maintenance, _guard, args):
        args["schema_acl_profile"] = "cnpg-staging"
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        authority = dict(target=args["target"], coordination_guard=args["coordination_guard"],
                         provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"))
        identity = admit_sealed_executor(peer, **authority)
        class InterruptedGrant:
            def __getattr__(self, name):
                return getattr(peer, name)
            def execute(self, statement, *args, **kwargs):
                rendered = statement.as_string(peer) if isinstance(statement, sql.Composable) else statement
                result = peer.execute(statement, *args, **kwargs)
                if rendered.startswith("GRANT CONNECT"):
                    raise RuntimeError("interrupted before issuance commit")
                return result
        password = uuid4().hex * 2
        with pytest.raises(RuntimeError, match="interrupted before issuance commit"):
            issue_executor_admission(InterruptedGrant(), **authority, identity=identity, password=password)
        assert admit_sealed_executor(peer, **authority) == identity
        issue_executor_admission(peer, **authority, identity=identity, password=password)
