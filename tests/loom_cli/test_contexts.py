"""Explicit CLI contexts isolate credentials without a mutable global target."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from loom_cli.__main__ import main
from loom_cli.config import LoomConfig, config_path, load_config, save_config


def test_named_auth_dispatch_keeps_default_login_and_resets_selection(tmp_xdg_home, monkeypatch, capsys):
    save_config(LoomConfig(server_url="https://management.example.com", auth_token="management-secret"))
    original = config_path().read_bytes()
    monkeypatch.setenv("TEST_CHILD_TOKEN", "child-secret")
    assert main(["--context", "dev-alice", "auth", "login", "--server", "https://alice.example.com",
                 "--token", "env:TEST_CHILD_TOKEN"]) == 0
    assert config_path().read_bytes() == original
    assert main(["--context", "dev-alice", "auth", "status"]) == 0
    assert "https://alice.example.com" in capsys.readouterr().out
    assert load_config().auth_token == "management-secret"
    assert main(["auth", "status"]) == 0
    assert "https://management.example.com" in capsys.readouterr().out


@pytest.mark.parametrize("name", ["../outside", "/tmp/outside", ".", "", "a/b", "a\\b", "a" * 97])
def test_invalid_context_cannot_escape_config_directory(tmp_xdg_home, name, capsys):
    assert main(["--context", name, "auth", "status"]) == 2
    assert "context" in capsys.readouterr().err.lower()
    assert not (tmp_xdg_home / "loom").exists()


def test_loaded_credentials_keep_their_storage_origin_across_selection(tmp_xdg_home):
    from loom_cli.contexts import selected_context

    save_config(LoomConfig(server_url="https://management.example.com", auth_token="management-secret"))
    with selected_context("alice"):
        save_config(LoomConfig(server_url="https://alice.example.com", auth_token="old-child-secret"))
        cfg = load_config()
        alice_path = config_path()
    cfg.auth_token = "rotated-child-secret"
    save_config(cfg)
    assert load_config().auth_token == "management-secret"
    with selected_context("alice"):
        assert load_config().auth_token == "rotated-child-secret"
    if os.name != "nt":
        assert alice_path.stat().st_mode & 0o777 == 0o600
        assert alice_path.parent.stat().st_mode & 0o777 == 0o700


def test_failed_atomic_replace_preserves_existing_credentials(tmp_xdg_home, monkeypatch):
    save_config(LoomConfig(server_url="https://management.example.com", auth_token="old-secret"))
    original = config_path().read_bytes()

    def fail_replace(*args, **kwargs):
        raise OSError("simulated disk failure")

    monkeypatch.setattr(os, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated disk failure"):
        save_config(LoomConfig(server_url="https://management.example.com", auth_token="new-secret"))
    assert config_path().read_bytes() == original
    assert list(config_path().parent.iterdir()) == [config_path()]


def test_config_symlink_does_not_overwrite_another_file(tmp_xdg_home, tmp_path):
    target = tmp_path / "unrelated.toml"
    target.write_text('auth_token="unrelated-secret"')
    path = config_path()
    path.parent.mkdir(parents=True)
    path.symlink_to(target)
    with pytest.raises(ValueError, match="regular"):
        save_config(LoomConfig(auth_token="replacement"))
    assert target.read_text() == 'auth_token="unrelated-secret"'


def test_context_selection_resets_after_exception(tmp_xdg_home):
    from loom_cli.contexts import selected_context

    default_path = config_path()
    with pytest.raises(RuntimeError):
        with selected_context("alice"):
            assert config_path() != default_path
            raise RuntimeError("failed command")
    assert config_path() == default_path
