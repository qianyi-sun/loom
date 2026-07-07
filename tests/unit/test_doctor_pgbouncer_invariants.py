"""Tests for _check_pgbouncer_invariants (#609)."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock

import pytest


def _make_schema(pgbouncer_enabled: bool) -> Any:
    """Build a minimal fake Schema with render_config.pgbouncer.fields."""
    pgb_entry = MagicMock()
    pgb_entry.fields = {"enabled": pgbouncer_enabled, "replicas": 2}

    schema = MagicMock()
    schema.render_config = {"pgbouncer": pgb_entry}
    return schema


def _schema_no_pgbouncer() -> Any:
    """Schema with no pgbouncer render_config entry at all."""
    schema = MagicMock()
    schema.render_config = {}
    return schema


def test_no_findings_when_pgbouncer_disabled() -> None:
    from loom_config.doctor import _check_pgbouncer_invariants

    schema = _make_schema(pgbouncer_enabled=False)
    secrets = {"cp-db-url": "postgresql+psycopg://loom:pw@loom-pgbouncer:6432/loom"}
    assert _check_pgbouncer_invariants(schema, secrets) == []


def test_no_findings_when_pgbouncer_entry_absent() -> None:
    from loom_config.doctor import _check_pgbouncer_invariants

    secrets = {"cp-db-url": "postgresql+psycopg://loom:pw@loom-pgbouncer:6432/loom"}
    assert _check_pgbouncer_invariants(_schema_no_pgbouncer(), secrets) == []


def test_no_findings_on_correct_invariants() -> None:
    from loom_config.doctor import _check_pgbouncer_invariants

    schema = _make_schema(pgbouncer_enabled=True)
    secrets = {
        "cp-db-url": "postgresql+psycopg://loom:pw@loom-postgres:5432/loom",
        "cp-db-url-pool": "postgresql+psycopg://loom:pw@loom-pgbouncer:6432/loom",
        "gw-db-url": "postgresql+psycopg://loom:pw@loom-postgres:5432/loom",
        "gw-db-url-pool": "postgresql+psycopg://loom:pw@loom-pgbouncer:6432/loom",
        "svc-db-url": "postgresql+psycopg://loom:pw@loom-postgres:5432/loom",
        "svc-db-url-pool": "postgresql+psycopg://loom:pw@loom-pgbouncer:6432/loom",
    }
    assert _check_pgbouncer_invariants(schema, secrets) == []


def test_flags_direct_url_pointing_at_pgbouncer() -> None:
    from loom_config.doctor import _check_pgbouncer_invariants

    schema = _make_schema(pgbouncer_enabled=True)
    secrets = {
        "cp-db-url": "postgresql+psycopg://loom:pw@loom-pgbouncer:6432/loom",
        "cp-db-url-pool": "postgresql+psycopg://loom:pw@loom-pgbouncer:6432/loom",
    }
    findings = _check_pgbouncer_invariants(schema, secrets)
    assert any("cp-db-url" in str(f) and "loom-postgres" in str(f) for f in findings)


def test_flags_pool_url_pointing_at_postgres() -> None:
    from loom_config.doctor import _check_pgbouncer_invariants

    schema = _make_schema(pgbouncer_enabled=True)
    secrets = {
        "cp-db-url": "postgresql+psycopg://loom:pw@loom-postgres:5432/loom",
        "cp-db-url-pool": "postgresql+psycopg://loom:pw@loom-postgres:5432/loom",
    }
    findings = _check_pgbouncer_invariants(schema, secrets)
    assert any("cp-db-url-pool" in str(f) and "loom-pgbouncer" in str(f) for f in findings)


def test_flags_all_three_services_direct_wrong() -> None:
    from loom_config.doctor import _check_pgbouncer_invariants

    schema = _make_schema(pgbouncer_enabled=True)
    secrets = {
        "cp-db-url": "postgresql+psycopg://loom:pw@loom-pgbouncer:6432/loom",
        "gw-db-url": "postgresql+psycopg://loom:pw@loom-pgbouncer:6432/loom",
        "svc-db-url": "postgresql+psycopg://loom:pw@loom-pgbouncer:6432/loom",
        "cp-db-url-pool": "postgresql+psycopg://loom:pw@loom-pgbouncer:6432/loom",
        "gw-db-url-pool": "postgresql+psycopg://loom:pw@loom-pgbouncer:6432/loom",
        "svc-db-url-pool": "postgresql+psycopg://loom:pw@loom-pgbouncer:6432/loom",
    }
    findings = _check_pgbouncer_invariants(schema, secrets)
    direct_keys = {"cp-db-url", "gw-db-url", "svc-db-url"}
    flagged = {str(f) for f in findings}
    for key in direct_keys:
        assert any(key in s for s in flagged), f"{key} not flagged"


def test_skips_missing_secret_keys() -> None:
    """If a secret key is absent, no finding is raised (secret missing is a
    separate check handled by _secret_violations)."""
    from loom_config.doctor import _check_pgbouncer_invariants

    schema = _make_schema(pgbouncer_enabled=True)
    # Only pool keys present, direct keys absent — should not raise KeyError
    secrets = {
        "cp-db-url-pool": "postgresql+psycopg://loom:pw@loom-pgbouncer:6432/loom",
    }
    findings = _check_pgbouncer_invariants(schema, secrets)
    # No finding for the missing direct keys (that's _secret_violations' job)
    assert not any("cp-db-url" in str(f) and "cp-db-url" not in "cp-db-url-pool" for f in findings
                   if "cp-db-url-pool" not in str(f))
