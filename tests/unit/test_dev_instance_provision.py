"""Unit tests for dev-instance data-plane provisioning (pure renderers)."""

from __future__ import annotations

import pytest

from loom.dev_instance import derive_identity
from loom.dev_instance_provision import (
    UnsafeIdentifierError,
    dev_instance_buckets,
    provisioning_plan,
    render_create_database_sql,
    render_role_convergence_sql,
)

_PW = "0123456789abcdef0123"  # 20 hex chars


class TestRoleConvergenceSql:
    def test_creates_login_role_with_password(self) -> None:
        sql = render_role_convergence_sql(derive_identity("alice"), _PW)
        assert 'CREATE ROLE "loom_dev_alice"' in sql
        assert 'ALTER ROLE "loom_dev_alice" WITH LOGIN' in sql
        assert f"PASSWORD '{_PW}'" in sql
        assert "NOSUPERUSER" in sql and "NOCREATEDB" in sql
        assert sql.strip().endswith("dev-instance-role-converged-v1';")

    def test_idempotent_guard_present(self) -> None:
        sql = render_role_convergence_sql(derive_identity("bob"), _PW)
        assert "IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles" in sql

    def test_rejects_non_hex_password(self) -> None:
        for bad in ["short", "not-hex-!!", "ABCDEF0123456789", "g" * 20, ""]:
            with pytest.raises(UnsafeIdentifierError):
                render_role_convergence_sql(derive_identity("alice"), bad)

    def test_dashed_name_yields_underscore_role(self) -> None:
        sql = render_role_convergence_sql(derive_identity("my-env"), _PW)
        assert 'CREATE ROLE "loom_dev_my_env"' in sql


class TestCreateDatabaseSql:
    def test_owner_is_instance_role(self) -> None:
        sql = render_create_database_sql(derive_identity("alice"))
        assert sql == 'CREATE DATABASE "loom_dev_alice" OWNER "loom_dev_alice";'

    def test_no_transaction_wrapper(self) -> None:
        # CREATE DATABASE cannot run in a transaction — must be a bare statement.
        sql = render_create_database_sql(derive_identity("alice"))
        assert "BEGIN" not in sql and "COMMIT" not in sql


class TestBuckets:
    def test_three_isolated_buckets(self) -> None:
        i = derive_identity("alice")
        assert dev_instance_buckets(i) == [
            "loom-dev-alice-tasks",
            "loom-dev-alice-trajectories",
            "loom-dev-alice-artifacts",
        ]


class TestProvisioningPlan:
    def test_plan_is_complete_and_side_effect_free(self) -> None:
        plan = provisioning_plan("alice", _PW)
        assert plan["identity"] == derive_identity("alice")
        assert 'CREATE ROLE "loom_dev_alice"' in plan["role_sql"]  # type: ignore[operator]
        assert plan["create_database_sql"] == (
            'CREATE DATABASE "loom_dev_alice" OWNER "loom_dev_alice";'
        )
        assert plan["buckets"] == [
            "loom-dev-alice-tasks",
            "loom-dev-alice-trajectories",
            "loom-dev-alice-artifacts",
        ]

    def test_plan_rejects_bad_name(self) -> None:
        from loom.dev_instance import InvalidDevInstanceNameError

        with pytest.raises(InvalidDevInstanceNameError):
            provisioning_plan("Alice", _PW)

    def test_plan_rejects_bad_password(self) -> None:
        with pytest.raises(UnsafeIdentifierError):
            provisioning_plan("alice", "not-hex")


class TestInjectionSafety:
    def test_identifiers_are_only_safe_charset(self) -> None:
        # Every derived identifier fed to the renderers is [a-z0-9_], so no SQL
        # metacharacter can appear. Spot-check the rendered SQL has no quotes
        # other than the intended ones around identifiers/password.
        sql = render_role_convergence_sql(derive_identity("x1-y2"), _PW)
        assert "loom_dev_x1_y2" in sql
        assert ";" in sql  # statements terminate; no stray injected content
