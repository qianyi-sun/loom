"""XDG-aware config loader."""

from __future__ import annotations

from pathlib import Path

from loom_cli.config import (
    CONFIG_FILENAME,
    LoomConfig,
    config_path,
    load_config,
    save_config,
)


def test_config_path_uses_xdg_config_home(tmp_xdg_home: Path) -> None:
    p = config_path()
    assert p == tmp_xdg_home / "loom" / CONFIG_FILENAME


def test_load_returns_empty_config_when_missing(tmp_xdg_home: Path) -> None:
    cfg = load_config()
    assert cfg == LoomConfig()
    assert cfg.tokens == {}
    assert cfg.server_url is None


def test_save_then_load_roundtrip(tmp_xdg_home: Path) -> None:
    cfg = LoomConfig(
        tokens={"anthropic": "sk-ant-xxx", "openai": "sk-oai-yyy"},
        server_url="https://loom.example.com",
    )
    save_config(cfg)
    assert config_path().exists()
    loaded = load_config()
    assert loaded == cfg


def test_save_creates_parent_dir(tmp_xdg_home: Path) -> None:
    save_config(LoomConfig(tokens={"anthropic": "k"}))
    assert config_path().parent.is_dir()


def test_set_token_helper_merges_into_existing(tmp_xdg_home: Path) -> None:
    save_config(LoomConfig(tokens={"openai": "old"}))
    cfg = load_config()
    cfg.tokens["anthropic"] = "new"
    save_config(cfg)
    again = load_config()
    assert again.tokens == {"openai": "old", "anthropic": "new"}
