from __future__ import annotations

import json

import pytest

from loom_cli.rollout.readonly_database_bootstrap import (
    ReadonlyDatabaseCredential,
    render_readonly_role_sql,
)


def _credential() -> ReadonlyDatabaseCredential:
    return ReadonlyDatabaseCredential(
        role="loom_rollout_readonly",
        database="loom",
        password="a" * 64,
    )


def test_private_credential_round_trip_is_exact() -> None:
    credential = _credential()

    assert ReadonlyDatabaseCredential.from_bytes(credential.to_bytes()) == credential
    assert json.loads(credential.to_bytes()) == {
        "database": "loom",
        "password": "a" * 64,
        "role": "loom_rollout_readonly",
        "schema_version": 1,
    }


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("role", "loom"),
        ("database", "postgres"),
        ("password", "not-hex"),
        ("password", "a" * 63),
        ("password", "A" * 64),
    ),
)
def test_private_credential_rejects_drift(field: str, value: str) -> None:
    values = {
        "role": "loom_rollout_readonly",
        "database": "loom",
        "password": "a" * 64,
    }
    values[field] = value
    with pytest.raises(ValueError, match="credential is invalid"):
        ReadonlyDatabaseCredential(**values)


def test_role_bootstrap_is_exact_select_only_and_idempotent() -> None:
    sql = render_readonly_role_sql(_credential())

    assert "BEGIN;" in sql and "COMMIT;" in sql
    assert "NOINHERIT" in sql
    assert "NOCREATEDB" in sql
    assert "NOCREATEROLE" in sql
    assert "NOREPLICATION" in sql
    assert "NOBYPASSRLS" in sql
    assert "default_transaction_read_only = 'on'" in sql
    assert "REVOKE TEMP ON DATABASE %I FROM PUBLIC" in sql
    assert "REVOKE TEMP ON DATABASE" in sql
    assert "REVOKE CREATE ON SCHEMA public" in sql
    assert "REVOKE ALL PRIVILEGES ON ALL TABLES" in sql
    assert "REVOKE ALL PRIVILEGES ON ALL SEQUENCES" in sql
    assert "GRANT SELECT ON TABLE public.%I" in sql
    assert "GRANT SELECT ON ALL TABLES" not in sql
    assert "ALTER DEFAULT PRIVILEGES" not in sql
    assert "secrets" not in sql
    assert "tokens" not in sql
    assert "provider_connections" not in sql
    for execution_table in (
        "artifacts",
        "batches",
        "llm_calls",
        "team_memberships",
        "trial_events",
        "trials",
    ):
        assert f"'{execution_table}'" in sql
    for pool_identity_table in (
        "gb10_worker_node_statuses",
        "gb10_worker_pool_desired_states",
        "slurm_worker_jobs",
        "worker_pool_autoscaler_policies",
        "workers",
    ):
        assert f"'{pool_identity_table}'" in sql
    assert sql.count("a" * 64) == 1


def test_role_bootstrap_never_accepts_sql_literal_injection() -> None:
    with pytest.raises(ValueError, match="credential is invalid"):
        ReadonlyDatabaseCredential(
            role="loom_rollout_readonly",
            database="loom",
            password="a' ; DROP ROLE loom; --",
        )
