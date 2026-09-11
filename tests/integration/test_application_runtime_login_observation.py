"""Read-only recovery distinguishes committed login restoration from a pending step."""

from dataclasses import replace
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from loom.application_runtime_login import (
    ApplicationRuntimeLoginState,
    observe_application_runtime_login,
    restore_application_runtime_login,
)
from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)
from tests.integration.test_application_runtime_login import _prepare

pytestmark = pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)


async def test_readonly_observation_never_restores_a_pending_login(transfer_database):  # noqa: F811
    admin, _url, owner, runtime, bindings, target = await _prepare(transfer_database)
    password = uuid4().hex
    with admin:
        admin.execute("SET default_transaction_read_only=on")
        assert observe_application_runtime_login(
            admin, owner_role=owner, role_bindings=bindings, password=password, target=target,
        ) is ApplicationRuntimeLoginState.SEALED
        assert admin.execute(
            sql.SQL("SELECT rolcanlogin,rolpassword FROM pg_authid WHERE rolname={}").format(
                sql.Literal(runtime),
            ),
        ).fetchone() == (False, None)
        admin.execute("SET default_transaction_read_only=off")
        restore_application_runtime_login(
            admin, owner_role=owner, role_bindings=bindings, password=password, target=target,
        )
        # A fresh classification after a lost acknowledgement must prove the
        # existing credential, with no ALTER ROLE or credential rotation.
        admin.execute("SET default_transaction_read_only=on")
        assert observe_application_runtime_login(
            admin, owner_role=owner, role_bindings=bindings, password=password, target=target,
        ) is ApplicationRuntimeLoginState.RESTORED


@pytest.mark.parametrize("drift", ["credential", "database", "owner", "privileges", "membership", "admission"])
async def test_open_login_alone_is_never_terminal_evidence(transfer_database, drift):  # noqa: F811
    admin, url, owner, runtime, bindings, target = await _prepare(transfer_database)
    password = uuid4().hex
    with admin:
        restore_application_runtime_login(
            admin, owner_role=owner, role_bindings=bindings, password=password, target=target,
        )
        observed_password, observed_target = password, target
        if drift == "credential":
            observed_password = uuid4().hex
        elif drift == "database":
            observed_target = replace(target, database_oid=target.database_oid + 1)
        elif drift == "owner":
            admin.execute(sql.SQL("ALTER ROLE {} LOGIN").format(sql.Identifier(owner)))
        elif drift == "privileges":
            admin.execute(sql.SQL("GRANT TRUNCATE ON public.trials TO {}").format(sql.Identifier(runtime)))
        elif drift == "membership":
            admin.execute(sql.SQL("GRANT {} TO {} WITH INHERIT FALSE").format(
                sql.Identifier(owner), sql.Identifier(runtime),
            ))
        else:
            with psycopg.connect(url, dbname="postgres", autocommit=True) as maintenance:
                maintenance.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(
                    sql.Identifier(target.database),
                ))
        admin.execute("SET default_transaction_read_only=on")
        with pytest.raises(RuntimeError):
            observe_application_runtime_login(
                admin, owner_role=owner, role_bindings=bindings,
                password=observed_password, target=observed_target,
            )
