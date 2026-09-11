"""Internal committed application-login seal, not session or deployment authority.

The protected caller must journal its operation and recoverable credential/Secret
intent before entering, and serialize administrator DDL and credential writers.
This removes no memberships, signals no backend, creates no role and transfers
no objects. It cannot prove quiescence: existing and in-flight logins survive.
An admitted connection-startup barrier and workload shutdown remain required.
"""

from __future__ import annotations

import re

from psycopg import sql
from psycopg.pq import TransactionStatus

from loom.application_database_connection import ApplicationDatabaseConnection, application_sql
from loom.application_ownership_transfer import require_application_role_scope


class ApplicationLoginSealingError(RuntimeError):
    """The exact application login could not be sealed safely."""


def seal_application_login(
    connection: ApplicationDatabaseConnection, *, database: str, role: str, provisioner_role: str
) -> None:
    """Commit NOLOGIN/NOINHERIT/PASSWORD NULL for the ordinary legacy owner.

    Requires an idle administrator connection so return means the sealing
    transaction was acknowledged, not an uncommitted caller savepoint. Exact
    replay is allowed before ownership transfer. Unknown commit outcomes must
    be reconciled by the surrounding protected operation. No live caller yet.
    A membership-free source may have INHERIT; it is explicitly revoked in the
    sealing transaction, not admitted as part of the sealed target state.
    """
    if (
        any(
            re.fullmatch(r"[a-z][a-z0-9_]{0,62}", name) is None
            for name in (database, role, provisioner_role)
        )
        or role == provisioner_role
    ):
        raise ApplicationLoginSealingError("application login administrator identities are invalid")
    if connection.info.transaction_status != TransactionStatus.IDLE:
        raise ApplicationLoginSealingError("application login sealing requires an idle connection")
    if connection.info.server_version // 10000 not in {16, 17}:
        raise ApplicationLoginSealingError("application login sealing requires PostgreSQL 16 or 17")
    with connection.transaction():
        connection.execute("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)")
        if connection.execute(
            "SELECT pg_catalog.current_setting('transaction_isolation')"
        ).fetchone() != ("read committed",):
            raise ApplicationLoginSealingError("application login sealing requires READ COMMITTED")
        if connection.execute(
            application_sql(
                "SELECT current_user=session_user AND current_user={} AND rolsuper "
                "FROM pg_catalog.pg_roles WHERE rolname=current_user",
                provisioner_role,
            )
        ).fetchone() != (True,):
            raise ApplicationLoginSealingError(
                "application login sealing requires protected administrator"
            )
        connection.execute(
            "SELECT pg_catalog.set_config('lock_timeout','1s',true) FROM pg_catalog.pg_settings WHERE name='lock_timeout' AND (setting::integer=0 OR setting::integer>1000)"
        )
        connection.execute(
            "SELECT pg_catalog.set_config('statement_timeout','30s',true) FROM pg_catalog.pg_settings WHERE name='statement_timeout' AND (setting::integer=0 OR setting::integer>30000)"
        )
        if connection.execute(
            application_sql(
                "SELECT d.datname=pg_catalog.current_database() AND r.rolname={} "
                "FROM pg_catalog.pg_database d JOIN pg_catalog.pg_roles r ON r.oid=d.datdba "
                "WHERE d.datname={}",
                role,
                database,
            )
        ).fetchone() != (True,):
            raise ApplicationLoginSealingError("application login database owner changed")
        if connection.execute(
            application_sql(
                "SELECT NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole "
                "AND NOT rolreplication AND NOT rolbypassrls FROM pg_catalog.pg_roles WHERE rolname={}",
                role,
            )
        ).fetchone() != (True,):
            raise ApplicationLoginSealingError("application login role attributes changed")
        if connection.execute(
            application_sql(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles r "
                "ON r.oid=m.roleid OR r.oid=m.member WHERE r.rolname={})",
                role,
            )
        ).fetchone() != (False,):
            raise ApplicationLoginSealingError("application login memberships are not sealed")
        require_application_role_scope(connection, roles=[role])
        connection.execute("SELECT pg_catalog.pg_stat_clear_snapshot()")
        if connection.execute(
            application_sql(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE usename={} "
                "AND datname IS DISTINCT FROM pg_catalog.current_database()) OR EXISTS "
                "(SELECT 1 FROM pg_catalog.pg_prepared_xacts WHERE owner={})",
                role,
                role,
            )
        ).fetchone() != (False,):
            raise ApplicationLoginSealingError(
                "application login has foreign sessions or prepared transactions"
            )
        connection.execute(
            sql.SQL("ALTER ROLE {} NOLOGIN NOINHERIT PASSWORD NULL").format(sql.Identifier(role))
        )
        if connection.execute(
            application_sql(
                "SELECT NOT rolcanlogin AND NOT rolinherit AND rolpassword IS NULL "
                "FROM pg_catalog.pg_authid WHERE rolname={}",
                role,
            )
        ).fetchone() != (True,):
            raise ApplicationLoginSealingError("application login seal was not exact")
