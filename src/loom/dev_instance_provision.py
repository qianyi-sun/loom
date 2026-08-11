"""Data-plane provisioning for a per-developer dev environment.

Each ``dev-<name>`` instance is strongly isolated on the shared dev fixture:
its own **database** ``loom_dev_<name>`` owned by its own **login role**
``loom_dev_<name>`` (owner ⇒ full access to its own data, no access to any
other instance's database), plus its own object-store **buckets**.

The SQL rendering here is pure and unit-tested (mirroring
``loom_cli.rollout.readonly_database_bootstrap``); the ``CREATE DATABASE`` +
bucket creation are thin live wrappers (integration-tested), since
``CREATE DATABASE`` cannot run inside a transaction and needs an existence
check on the maintenance database.

See ``docs/architecture/multi-dev-environments.md``.
"""

from __future__ import annotations

import re

from loom.dev_instance import DevInstanceIdentity, derive_identity

# Identifiers come from ``derive_identity`` (names are ``[a-z][a-z0-9-]*`` →
# db_slug ``[a-z][a-z0-9_]*``), so they are already safe Postgres identifiers.
# Re-assert here so this module can never emit an injectable identifier even if
# a caller hands it a hand-built identity.
_SAFE_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
# Passwords must be hexadecimal so they cannot escape the single SQL literal
# (same guarantee the readonly-role bootstrap relies on).
_HEX_PASSWORD = re.compile(r"^[0-9a-f]{16,}$")


class UnsafeIdentifierError(ValueError):
    """A database/role identifier or password failed the safety check."""


def _require_safe_identifier(value: str, kind: str) -> None:
    if not _SAFE_IDENTIFIER.fullmatch(value):
        raise UnsafeIdentifierError(f"unsafe {kind} identifier: {value!r}")


def _require_hex_password(password: str) -> None:
    if not _HEX_PASSWORD.fullmatch(password):
        raise UnsafeIdentifierError(
            "dev-instance role password must be >=16 hex chars",
        )


def render_role_convergence_sql(identity: DevInstanceIdentity, password: str) -> str:
    """Render one idempotent role-convergence transaction for the maintenance DB.

    Creates (if absent) the instance's LOGIN role and sets its password +
    least-privilege attributes. Run this on the maintenance database *before*
    ``CREATE DATABASE ... OWNER <role>``. Pure: no I/O.
    """
    _require_safe_identifier(identity.db_role, "role")
    _require_hex_password(password)
    role = identity.db_role
    return (
        f"""
BEGIN;
DO $loom$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = '{role}') THEN
    CREATE ROLE "{role}";
  END IF;
END
$loom$;
ALTER ROLE "{role}" WITH LOGIN NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE
  NOREPLICATION NOBYPASSRLS PASSWORD '{password}';
COMMIT;
SELECT 'dev-instance-role-converged-v1';
""".strip()
        + "\n"
    )


def render_create_database_sql(identity: DevInstanceIdentity) -> str:
    """Render the ``CREATE DATABASE`` statement (owner = the instance role).

    Emitted separately because ``CREATE DATABASE`` cannot run inside a
    transaction; the executor issues it in autocommit only when the database
    is absent (Postgres has no ``CREATE DATABASE IF NOT EXISTS``). Pure.
    """
    _require_safe_identifier(identity.database, "database")
    _require_safe_identifier(identity.db_role, "role")
    return f'CREATE DATABASE "{identity.database}" OWNER "{identity.db_role}";'


def dev_instance_buckets(identity: DevInstanceIdentity) -> list[str]:
    """The object-store buckets an instance owns (created on the shared MinIO)."""
    return [
        identity.task_bucket,
        identity.trajectories_bucket,
        identity.artifacts_bucket,
    ]


def provisioning_plan(name: str, password: str) -> dict[str, object]:
    """Return the full, side-effect-free provisioning plan for ``dev-<name>``.

    A pure summary the guarded endpoint can log/return before executing, and
    that the executor consumes: the role SQL, the create-database SQL, and the
    buckets. Raises if the name or password is unsafe.
    """
    identity = derive_identity(name)
    return {
        "identity": identity,
        "role_sql": render_role_convergence_sql(identity, password),
        "create_database_sql": render_create_database_sql(identity),
        "buckets": dev_instance_buckets(identity),
    }
