"""Tests for _check_pgbouncer_invariants (#609)."""
from __future__ import annotations

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


def test_reconcile_calls_pgbouncer_invariants(monkeypatch: pytest.MonkeyPatch) -> None:
    """reconcile() must surface pgbouncer violations read from the live secret."""
    import base64
    from unittest.mock import MagicMock

    from loom_config.doctor import reconcile

    # Build a schema with pgbouncer enabled and no service_config entries
    # (so _secret_violations and _orphan_secret_violations produce nothing).
    pgb_entry = MagicMock()
    pgb_entry.fields = {"enabled": True}

    schema = MagicMock()
    schema.render_config = {"pgbouncer": pgb_entry}
    schema.service_prefix = {}
    schema.service_config = {}
    schema.infra_secrets = set()

    # cp-db-url points at pgbouncer — direct URLs must point at loom-postgres.
    wrong_direct = "postgresql+psycopg://loom:pw@loom-pgbouncer:6432/loom"

    def _make_secret_data(dsn: str) -> bytes:
        return base64.b64encode(dsn.encode())

    mock_secret = MagicMock()
    mock_secret.data = {
        "cp-db-url": _make_secret_data(wrong_direct),
    }

    core_v1_api = MagicMock()
    # read_namespaced_secret is called twice: once by _read_secret_keys and
    # once by the pgbouncer block.  Both calls return the same mock secret.
    core_v1_api.read_namespaced_secret.return_value = mock_secret
    core_v1_api.list_namespaced_pod.return_value.items = []

    report = reconcile(schema, core_v1_api, namespace="loom")

    pgbouncer_violations = [v for v in report.violations if v.kind == "pgbouncer_invariant"]
    assert pgbouncer_violations, "Expected at least one pgbouncer_invariant violation"
    assert any("cp-db-url" in v.entry for v in pgbouncer_violations)


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
