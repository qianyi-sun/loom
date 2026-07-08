"""Integration tests for *-db-url-pool secret derivation in bootstrap.

When pgbouncer is enabled, bootstrap derives pool DSNs from direct DSNs
via _rewrite_dsn_host_port (smoke mode) or shell substitution (real-deploy).
When pgbouncer is disabled, pool keys are omitted entirely.
"""
from __future__ import annotations

from pathlib import Path

from loom_config.bootstrap import render_bootstrap_command
from loom_config.loader import load_schema

_SCHEMA_PATH = Path("config/loom-schema.toml")


def test_bootstrap_emits_derived_pool_secrets_when_pgbouncer_enabled_smoke() -> None:
    schema = load_schema(_SCHEMA_PATH)
    output = render_bootstrap_command(schema, smoke_defaults=True, pgbouncer_enabled=True)

    # Direct keys present
    assert "cp-db-url=" in output
    assert "gw-db-url=" in output
    assert "svc-db-url=" in output

    # Pool keys present
    assert "cp-db-url-pool=" in output
    assert "gw-db-url-pool=" in output
    assert "svc-db-url-pool=" in output

    # Pool URLs point at loom-pgbouncer:6432
    for line in output.splitlines():
        if "-db-url-pool" in line:
            assert "loom-pgbouncer:6432" in line, line


def test_bootstrap_omits_pool_secrets_when_pgbouncer_disabled_smoke() -> None:
    schema = load_schema(_SCHEMA_PATH)
    output = render_bootstrap_command(schema, smoke_defaults=True, pgbouncer_enabled=False)

    assert "cp-db-url=" in output
    assert "-db-url-pool=" not in output


def test_bootstrap_derives_pool_via_shell_substitution_in_real_deploy() -> None:
    """Non-smoke mode: emit shell script using the derive-pool-dsn CLI
    so the operator only edits the direct URL."""
    schema = load_schema(_SCHEMA_PATH)
    output = render_bootstrap_command(schema, smoke_defaults=False, pgbouncer_enabled=True)

    # Find the cp-db-url-pool line
    pool_line = None
    for line in output.splitlines():
        if "cp-db-url-pool=" in line:
            pool_line = line
            break
    assert pool_line is not None, "expected cp-db-url-pool line in output"

    # It references the CLI helper via $(...) shell substitution.
    # In non-smoke mode the direct URL is itself <EDIT_ME>, so the pool line
    # will look like: $(loom cluster derive-pool-dsn '<EDIT_ME>').
    # The key property is that it is wrapped in a shell substitution rather
    # than being a bare <EDIT_ME> with no wrapper.
    assert "derive-pool-dsn" in pool_line, pool_line
    assert "$(" in pool_line, pool_line
    # The <EDIT_ME> placeholder must be inside $(...), not naked.
    assert pool_line.index("$(") < pool_line.index("<EDIT_ME>") if "<EDIT_ME>" in pool_line else True
