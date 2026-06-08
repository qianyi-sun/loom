"""Shared fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture()
def tmp_xdg_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point XDG_CONFIG_HOME at a tmp dir so loom_cli.config doesn't
    touch the developer's real ~/.config/loom."""
    config_root = tmp_path / "xdg-config"
    config_root.mkdir()
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config_root))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    return config_root
