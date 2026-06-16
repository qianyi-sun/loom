from __future__ import annotations

import logging
import os
from pathlib import Path

import pytest

from loom_cli.__main__ import main


def test_main_loads_dotenv_from_cwd_ancestor_without_overriding_env(
    tmp_xdg_home: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"
    nested = project / "src" / "task"
    nested.mkdir(parents=True)
    (project / ".git").mkdir()
    dotenv_path = project / ".env"
    dotenv_path.write_text(
        "ANTHROPIC_API_KEY=from-dotenv\n"
        "OPENAI_API_KEY=from-dotenv\n",
    )

    monkeypatch.chdir(nested)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "from-exported-env")
    caplog.set_level(logging.DEBUG, logger="loom_cli.__main__")

    rc = main(["config", "show"])

    assert rc == 0
    capsys.readouterr()
    assert os.environ["ANTHROPIC_API_KEY"] == "from-dotenv"
    assert os.environ["OPENAI_API_KEY"] == "from-exported-env"
    assert f"loaded .env from {dotenv_path}" in caplog.text


def test_main_does_not_load_dotenv_above_git_root(
    tmp_xdg_home: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    workspace = tmp_path / "workspace"
    repo = workspace / "repo"
    nested = repo / "pkg"
    nested.mkdir(parents=True)
    (repo / ".git").mkdir()
    (workspace / ".env").write_text("ANTHROPIC_API_KEY=from-parent\n")

    monkeypatch.chdir(nested)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    caplog.set_level(logging.DEBUG, logger="loom_cli.__main__")

    rc = main(["config", "show"])

    assert rc == 0
    capsys.readouterr()
    assert os.environ.get("ANTHROPIC_API_KEY") is None
    assert "loaded .env from" not in caplog.text
