"""Exact PostgreSQL role bootstrap and private credential serialization."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

_PASSWORD_RE = re.compile(r"^[0-9a-f]{64}$")
_ROLE = "loom_rollout_readonly"
_DATABASE = "loom"
_TABLES = (
    "agents",
    "alembic_version",
    "data_lifecycle_authorities",
    "data_lifecycle_objects",
    "provider_models_cache",
    "staging_lifecycle_capacity",
    "staging_mutation_epochs",
    "tasks",
    "teams",
    "users",
)


@dataclass(frozen=True, slots=True)
class ReadonlyDatabaseCredential:
    role: str
    database: str
    password: str

    def __post_init__(self) -> None:
        if (
            self.role != _ROLE
            or self.database != _DATABASE
            or _PASSWORD_RE.fullmatch(self.password) is None
        ):
            raise ValueError("readonly database credential is invalid")

    def to_bytes(self) -> bytes:
        return (
            json.dumps(
                {
                    "database": self.database,
                    "password": self.password,
                    "role": self.role,
                    "schema_version": 1,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            + b"\n"
        )

    @classmethod
    def from_bytes(cls, payload: bytes) -> ReadonlyDatabaseCredential:
        if not payload or len(payload) > 1024:
            raise ValueError("readonly database credential is invalid")
        try:
            value = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("readonly database credential is invalid") from exc
        if not isinstance(value, dict) or set(value) != {
            "database",
            "password",
            "role",
            "schema_version",
        }:
            raise ValueError("readonly database credential is invalid")
        if value["schema_version"] != 1:
            raise ValueError("readonly database credential is invalid")
        return cls(
            role=value["role"],
            database=value["database"],
            password=value["password"],
        )


def render_readonly_role_sql(credential: ReadonlyDatabaseCredential) -> str:
    """Render one idempotent exact role convergence transaction.

    The credential alphabet is deliberately hexadecimal, so it cannot escape
    the single SQL literal. Missing post-0065 tables are skipped; once they
    exist a later install grants only their SELECT privilege.
    """

    table_values = ",".join(f"'{table}'" for table in _TABLES)
    return (
        f"""
\\set ON_ERROR_STOP on
BEGIN;
DO $loom$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = '{_ROLE}') THEN
    CREATE ROLE {_ROLE};
  END IF;
END
$loom$;
ALTER ROLE {_ROLE} WITH LOGIN NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE
  NOREPLICATION NOBYPASSRLS PASSWORD '{credential.password}';
ALTER ROLE {_ROLE} SET default_transaction_read_only = 'on';
ALTER ROLE {_ROLE} SET statement_timeout = '15s';
DO $loom$
BEGIN
  EXECUTE format('GRANT CONNECT ON DATABASE %I TO {_ROLE}', current_database());
  EXECUTE format('REVOKE TEMP ON DATABASE %I FROM PUBLIC', current_database());
  EXECUTE format('REVOKE TEMP ON DATABASE %I FROM {_ROLE}', current_database());
END
$loom$;
GRANT USAGE ON SCHEMA public TO {_ROLE};
REVOKE CREATE ON SCHEMA public FROM {_ROLE};
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {_ROLE};
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM {_ROLE};
DO $loom$
DECLARE
  table_name text;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[{table_values}]
  LOOP
    IF to_regclass(format('public.%I', table_name)) IS NOT NULL THEN
      EXECUTE format('GRANT SELECT ON TABLE public.%I TO {_ROLE}', table_name);
    END IF;
  END LOOP;
END
$loom$;
COMMIT;
SELECT 'readonly-role-converged-v1';
""".strip()
        + "\n"
    )


__all__ = [
    "ReadonlyDatabaseCredential",
    "render_readonly_role_sql",
]
