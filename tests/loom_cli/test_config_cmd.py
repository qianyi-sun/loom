"""`loom config set` / `loom config show` integration through the
top-level `main()` entry point."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom_cli.__main__ import main
from loom_cli.config import load_config


def test_set_token_persists(
    tmp_xdg_home: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["config", "set", "token.anthropic", "sk-ant-xyz"])
    assert rc == 0
    cfg = load_config()
    assert cfg.tokens["anthropic"] == "sk-ant-xyz"


def test_set_server_url_persists(tmp_xdg_home: Path) -> None:
    rc = main(["config", "set", "server_url", "https://loom.example.com"])
    assert rc == 0
    cfg = load_config()
    assert cfg.server_url == "https://loom.example.com"


def test_show_prints_config_redacting_token_values(
    tmp_xdg_home: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    main(["config", "set", "token.anthropic", "sk-ant-supersecret"])
    capsys.readouterr()
    rc = main(["config", "show"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "anthropic" in out
    assert "sk-ant-supersecret" not in out
    assert "***" in out


def test_set_rejects_unknown_key(
    tmp_xdg_home: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["config", "set", "frobnicate", "x"])
    assert rc == 2
    assert "unknown key" in capsys.readouterr().err.lower()
