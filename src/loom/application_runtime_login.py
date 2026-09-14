"""Restore an admitted application's credential after exact ownership separation.

Internal phase only: the protected caller must bind this connection and the
preserved credential to its durable operation/Secret recovery authority, admit
private guard definitions, and serialize credential/DDL writers. No deployment
caller, Secret mutation, ownership transfer or workload restart is provided here.
Only an independently admitted same-original-password refresher may overlap;
that permission never covers LOGIN, membership, ownership or another password.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from enum import StrEnum

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus

from loom.application_database_admission import (
    ApplicationDatabaseAdmissionTarget,
    ApplicationDatabaseCoordinationGuard,
    _require_coordination_guard,
)
from loom.application_database_connection import ApplicationDatabaseConnection, application_sql
from loom.application_ownership_transfer import require_application_role_scope
from loom.application_password import application_scram_verifier, matches_application_scram
from loom.application_schema_inventory import read_application_schema_inventory
from loom.application_schema_reference import (
    ApplicationSchemaAclProfile,
    ApplicationSchemaRevision,
    application_schema_profile,
    require_application_migration_revisions,
    require_application_schema_reference,
)


class ApplicationRuntimeLoginError(RuntimeError):
    """Runtime login cannot be restored without changing the admitted authority."""


class ApplicationRuntimeLoginState(StrEnum):
    """Validated credential state; neither value proves workload or fence recovery."""

    SEALED = "sealed"
    RESTORED = "restored"


def observe_application_runtime_login(
    connection: ApplicationDatabaseConnection,
    *,
    owner_role: str,
    role_bindings: Mapping[str, str],
    password: str,
    target: ApplicationDatabaseAdmissionTarget,
    schema_acl_profile: ApplicationSchemaAclProfile = "application-only",
    schema_revision: ApplicationSchemaRevision = "0146/guard_0033",
    coordination_guard: ApplicationDatabaseCoordinationGuard | None = None,
) -> ApplicationRuntimeLoginState:
    """Classify a saved restoration without changing login, password or grants.

    Both states require the exact separated ownership, trusted schema/ACL profile,
    saved database/role identities and open admission. Unknown credentials or
    authority drift refuse. This supplies the database part of recovery evidence;
    it does not authorize fence release or prove administrator exclusion.
    """
    return _application_runtime_login(
        connection, owner_role=owner_role, role_bindings=role_bindings,
        password=password, target=target, schema_acl_profile=schema_acl_profile, schema_revision=schema_revision,
        restore=False, coordination_guard=coordination_guard,
    )


def restore_application_runtime_login(
    connection: ApplicationDatabaseConnection,
    *,
    owner_role: str,
    role_bindings: Mapping[str, str],
    password: str,
    target: ApplicationDatabaseAdmissionTarget,
    schema_acl_profile: ApplicationSchemaAclProfile = "application-only",
    schema_revision: ApplicationSchemaRevision = "0146/guard_0033",
    coordination_guard: ApplicationDatabaseCoordinationGuard | None = None,
) -> None:
    """Commit login for the former owner only after exact sealed-profile admission.

    Bindings retain legacy application-owner alias, as in the transfer phase.
    Replay accepts only the same credential and verified ordinary runtime state;
    it does not rotate an unexpected password. The no-login owner stays sealed.
    A lost acknowledgement must be retried/reconciled with the saved credential.
    A NOLOGIN runtime may already carry that same credential from an admitted
    password-only refresher; unknown credentials are never overwritten.
    The protected caller must carry the same trusted schema_acl_profile used in
    ownership transfer; neither live grants nor caller-provided hashes select it.
    """
    _application_runtime_login(
        connection, owner_role=owner_role, role_bindings=role_bindings,
        password=password, target=target, schema_acl_profile=schema_acl_profile, schema_revision=schema_revision,
        restore=True, coordination_guard=coordination_guard,
    )


def _application_runtime_login(
    connection: ApplicationDatabaseConnection,
    *,
    owner_role: str,
    role_bindings: Mapping[str, str],
    password: str,
    target: ApplicationDatabaseAdmissionTarget,
    schema_acl_profile: ApplicationSchemaAclProfile,
    schema_revision: ApplicationSchemaRevision,
    restore: bool,
    coordination_guard: ApplicationDatabaseCoordinationGuard | None,
) -> ApplicationRuntimeLoginState:
    profile = application_schema_profile(ownership="sealed-owner", acl_profile=schema_acl_profile)
    aliases = {
        "application-owner",
        "guard-owner",
        "guard-migrator",
        "guard-agent",
        "guard-executor",
        "guard-observer",
        "guard-runtime",
        "provisioner",
    }
    bindings = dict(role_bindings)
    if (
        len(bindings) != len(aliases)
        or set(bindings.values()) != aliases
        or owner_role in bindings
        or any(
            re.fullmatch(r"[a-z][a-z0-9_]{0,62}", role) is None for role in (*bindings, owner_role)
        )
        or not isinstance(password, str)
        or not 1 <= len(password) <= 1024
        or any(not 0x21 <= ord(character) <= 0x7E for character in password)
    ):
        raise ApplicationRuntimeLoginError(
            "application runtime login identities or credential are invalid"
        )
    if connection.info.transaction_status != TransactionStatus.IDLE:
        raise ApplicationRuntimeLoginError("application runtime login requires an idle connection")
    if connection.info.server_version // 10000 not in {16, 17}:
        raise ApplicationRuntimeLoginError("application runtime login requires PostgreSQL 16 or 17")
    identities = {alias: role for role, alias in bindings.items()}
    runtime = identities["application-owner"]
    if target.owner_role != runtime or target.successor_role != owner_role:
        raise ApplicationRuntimeLoginError("application runtime login saved role identity changed")
    with connection.transaction():
        if not restore:
            connection.execute("SET TRANSACTION READ ONLY")
        connection.execute("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)")
        if connection.execute(
            "SELECT pg_catalog.current_setting('transaction_isolation')"
        ).fetchone() != ("read committed",):
            raise ApplicationRuntimeLoginError("application runtime login requires READ COMMITTED")
        if connection.execute(
            application_sql(
                "SELECT current_user=session_user AND current_user={} AND rolsuper FROM pg_catalog.pg_roles WHERE rolname=current_user",
                identities["provisioner"],
            )
        ).fetchone() != (True,):
            raise ApplicationRuntimeLoginError(
                "application runtime login requires protected administrator"
            )
        for name, value, limit in (
            ("lock_timeout", "1s", 1000),
            ("statement_timeout", "30s", 30000),
        ):
            connection.execute(
                application_sql(
                    "SELECT pg_catalog.set_config({},{},true) FROM pg_catalog.pg_settings WHERE name={} AND (setting::integer=0 OR setting::integer>{})",
                    name,
                    value,
                    name,
                    limit,
                )
            )
        if connection.execute(
            application_sql(
                "SELECT s.system_identifier::pg_catalog.text={} AND d.oid={} AND d.datname={} "
                "AND d.datallowconn AND d.datdba={} AND a.oid={} AND b.oid={} "
                "FROM pg_catalog.pg_database d CROSS JOIN pg_catalog.pg_control_system() s "
                "JOIN pg_catalog.pg_roles a ON a.rolname={} JOIN pg_catalog.pg_roles b ON b.rolname={} "
                "WHERE d.datname=pg_catalog.current_database()",
                target.system_identifier,
                target.database_oid,
                target.database,
                target.successor_oid,
                target.owner_oid,
                target.successor_oid,
                runtime,
                owner_role,
            )
        ).fetchone() != (True,):
            raise ApplicationRuntimeLoginError(
                "application runtime login saved database identity or admission changed"
            )
        if connection.execute(
            application_sql(
                "SELECT count(*)=2 AND bool_and(NOT rolsuper AND NOT rolinherit AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolreplication AND NOT rolbypassrls AND (rolname={} OR NOT rolcanlogin AND rolpassword IS NULL)) FROM pg_catalog.pg_authid WHERE rolname=ANY({})",
                runtime,
                [runtime, owner_role],
            )
        ).fetchone() != (True,):
            raise ApplicationRuntimeLoginError("application runtime login role authority changed")
        if connection.execute(
            application_sql(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles r ON r.oid=m.roleid OR r.oid=m.member WHERE r.rolname=ANY({}))",
                [runtime, owner_role],
            )
        ).fetchone() != (False,):
            raise ApplicationRuntimeLoginError("application runtime login memberships changed")
        require_application_role_scope(connection, roles=[runtime, owner_role])
        require_application_schema_reference(
            read_application_schema_inventory(
                connection,
                role_bindings={
                    **bindings,
                    runtime: "application-runtime",
                    owner_role: "application-owner",
                },
            ),
            profile=profile, revision=schema_revision,
        )
        require_application_migration_revisions(connection, revision=schema_revision)
        if coordination_guard is not None:
            _require_coordination_guard(connection, target, coordination_guard)
        state = connection.execute(
            application_sql(
                "SELECT rolcanlogin,rolpassword,rolvaliduntil IS NULL OR rolvaliduntil='infinity'::pg_catalog.timestamptz FROM pg_catalog.pg_authid WHERE rolname={}",
                runtime,
            )
        ).fetchone()
        if (
            state is not None
            and state[0] is True
            and state[2] is True
            and matches_application_scram(password, state[1])
        ):
            if coordination_guard is not None:
                _require_coordination_guard(connection, target, coordination_guard)
            return ApplicationRuntimeLoginState.RESTORED
        if state is None or state[0] is not False or (
            state[1] is not None and not matches_application_scram(password, state[1])
        ):
            raise ApplicationRuntimeLoginError("application runtime login credential state changed")
        if not restore:
            return ApplicationRuntimeLoginState.SEALED
        # PostgreSQL accepts precomputed verifiers verbatim. This also treats
        # a verifier-shaped literal password as a password, not as supplied hash.
        verifier = application_scram_verifier(password)
        try:
            connection.execute(
                sql.SQL("ALTER ROLE {} LOGIN PASSWORD {} VALID UNTIL 'infinity'").format(
                    sql.Identifier(runtime), sql.Literal(verifier)
                )
            )
        except psycopg.Error:
            raise ApplicationRuntimeLoginError(
                "application runtime login credential update failed"
            ) from None
        after = connection.execute(
            application_sql(
                "SELECT rolcanlogin,rolpassword,rolvaliduntil='infinity'::pg_catalog.timestamptz FROM pg_catalog.pg_authid WHERE rolname={}",
                runtime,
            )
        ).fetchone()
        if (
            after is None
            or after[0] is not True
            or after[2] is not True
            or not matches_application_scram(password, after[1])
        ):
            raise ApplicationRuntimeLoginError(
                "application runtime login credential update was not exact"
            )
        if coordination_guard is not None:
            _require_coordination_guard(connection, target, coordination_guard)
        return ApplicationRuntimeLoginState.RESTORED
