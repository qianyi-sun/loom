from __future__ import annotations

import subprocess
import sys


def test_derive_pool_dsn_prints_rewritten_url() -> None:
    result = subprocess.run(
        [
            sys.executable, "-m", "loom_cli",
            "cluster", "derive-pool-dsn",
            "postgresql+psycopg://loom:pw@loom-postgres:5432/loom",
        ],
        capture_output=True, text=True, check=True,
    )
    assert result.stdout.strip() == (
        "postgresql+psycopg://loom:pw@loom-pgbouncer:6432/loom"
    )


def test_derive_pool_dsn_preserves_query_string() -> None:
    result = subprocess.run(
        [
            sys.executable, "-m", "loom_cli",
            "cluster", "derive-pool-dsn",
            "postgresql://loom:pw@loom-postgres:5432/loom?sslmode=require",
        ],
        capture_output=True, text=True, check=True,
    )
    assert "sslmode=require" in result.stdout


def test_derive_pool_dsn_reports_error_on_malformed_input() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "loom_cli", "cluster", "derive-pool-dsn", "not-a-url"],
        capture_output=True, text=True,
    )
    assert result.returncode != 0
    assert (
        "malformed" in result.stderr.lower()
        or "invalid" in result.stderr.lower()
        or "unsupported" in result.stderr.lower()
    )
