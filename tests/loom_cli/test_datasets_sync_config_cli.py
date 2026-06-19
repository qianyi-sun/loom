"""CLI smoke tests for `loom datasets sync-config` (issue #234).

The integration test in `tests/integration/test_benchmarks_sync_local.py`
exercises the engine end-to-end against a real Postgres testcontainer.
The tests here cover the CLI argv parsing + early-exit behavior that
doesn't require a DB.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from loom_cli import datasets_cmd


def test_missing_file_explicit_path_returns_zero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = datasets_cmd.dispatch([
        "sync-config", "--config", str(tmp_path / "nope.toml"),
    ])
    assert rc == 0
    err = capsys.readouterr().err
    assert "not found" in err


def test_missing_default_config_no_op(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LOOM_BENCHMARKS_CONFIG_PATH", raising=False)
    monkeypatch.chdir(tmp_path)  # ensure ./config/benchmarks.toml doesn't exist
    rc = datasets_cmd.dispatch(["sync-config"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "nothing to sync" in out


def test_malformed_toml_exits_one(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    bad = tmp_path / "benchmarks.toml"
    bad.write_text("this is = not valid = toml\n")
    rc = datasets_cmd.dispatch([
        "sync-config", "--config", str(bad),
    ])
    assert rc == 1
    err = capsys.readouterr().err
    assert "invalid" in err


def test_local_entry_without_fixtures_root_errors(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LOOM_WORKER_FIXTURES_ROOT", raising=False)
    cfg = tmp_path / "benchmarks.toml"
    cfg.write_text(
        'schema_version = 1\n'
        '[[local]]\n'
        'id = "team-evals"\n'
        'display_name = "Internal team evaluations"\n'
        'series = "internal"\n'
        'license_spdx = "proprietary"\n',
    )
    rc = datasets_cmd.dispatch([
        "sync-config", "--config", str(cfg),
        "--db-url", "postgresql://x/y",
    ])
    assert rc == 2
    err = capsys.readouterr().err
    assert "fixtures-root" in err.lower() or "fixtures_root" in err.lower()


def test_missing_db_url_errors(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LOOM_DB_URL", raising=False)
    cfg = tmp_path / "benchmarks.toml"
    cfg.write_text("schema_version = 1\n")
    rc = datasets_cmd.dispatch([
        "sync-config", "--config", str(cfg),
    ])
    assert rc == 2
    err = capsys.readouterr().err
    assert "db-url" in err.lower() or "LOOM_DB_URL" in err
