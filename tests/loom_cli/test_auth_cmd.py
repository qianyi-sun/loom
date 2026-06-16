"""`loom auth {login,status,logout}` end-to-end."""

from __future__ import annotations

from io import StringIO
from pathlib import Path

import pytest

from loom_cli.__main__ import main
from loom_cli.config import load_config


@pytest.fixture(autouse=True)
def _isolated_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Point XDG_CONFIG_HOME at a tmpdir so tests don't trash a real
    ~/.config/loom/config.toml."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))


# ──────────────────────────────────────────────────────────────────────
# login
# ──────────────────────────────────────────────────────────────────────


def test_login_with_env_persists_token(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("MY_LOOM_TOKEN", "loom_admin_abcdef123456")
    rc = main([
        "auth", "login",
        "--server", "https://loom.example.com",
        "--token", "env:MY_LOOM_TOKEN",
    ])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Logged in to https://loom.example.com" in out
    # Redacted form: prefix + suffix only, no full token.
    assert "loom_a***3456" in out
    assert "loom_admin_abcdef123456" not in out

    cfg = load_config()
    assert cfg.server_url == "https://loom.example.com"
    assert cfg.auth_token == "loom_admin_abcdef123456"


def test_login_strips_trailing_slash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("T", "tok-123456")
    rc = main([
        "auth", "login",
        "--server", "https://loom.example.com/",
        "--token", "env:T",
    ])
    assert rc == 0
    assert load_config().server_url == "https://loom.example.com"


def test_login_with_file_token(tmp_path: Path) -> None:
    f = tmp_path / "loom-token.txt"
    f.write_text("loom_admin_filebased\n")
    rc = main([
        "auth", "login",
        "--server", "https://loom.example.com",
        "--token", f"file:{f}",
    ])
    assert rc == 0
    assert load_config().auth_token == "loom_admin_filebased"


def test_login_with_stdin_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sys.stdin", StringIO("loom_admin_piped123\n"))
    rc = main([
        "auth", "login",
        "--server", "https://loom.example.com",
        "--token", "-",
    ])
    assert rc == 0
    assert load_config().auth_token == "loom_admin_piped123"


def test_login_literal_token_rejected_at_argparse(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc:
        main([
            "auth", "login",
            "--server", "https://loom.example.com",
            "--token", "raw-token-value",
        ])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "literal values are rejected" in err


def test_login_rejects_non_http_server(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("T", "tok-1234567")
    rc = main([
        "auth", "login",
        "--server", "ftp://example.com",
        "--token", "env:T",
    ])
    assert rc == 2
    assert "must start with http://" in capsys.readouterr().err


def test_login_missing_env_var_returns_2_with_clear_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("DEFINITELY_NOT_SET", raising=False)
    rc = main([
        "auth", "login",
        "--server", "https://loom.example.com",
        "--token", "env:DEFINITELY_NOT_SET",
    ])
    assert rc == 2
    err = capsys.readouterr().err
    assert "is not set" in err


# ──────────────────────────────────────────────────────────────────────
# status
# ──────────────────────────────────────────────────────────────────────


def test_status_when_logged_in_returns_0(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("T", "loom_admin_111122223333")
    assert main([
        "auth", "login",
        "--server", "https://loom.example.com",
        "--token", "env:T",
    ]) == 0
    capsys.readouterr()  # drain login output

    rc = main(["auth", "status"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Server:  https://loom.example.com" in out
    assert "set (loom_a***3333)" in out
    # Never the full token.
    assert "loom_admin_111122223333" not in out


def test_status_when_not_logged_in_returns_1(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["auth", "status"])
    assert rc == 1
    out = capsys.readouterr().out
    assert "Server:  (none)" in out
    assert "Token:   (none)" in out


def test_status_with_server_but_no_token_returns_1(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """After `loom auth logout`, server URL is preserved but token is
    cleared. status MUST still exit 1 (logged out)."""
    monkeypatch.setenv("T", "tok-xxxxxxxx")
    main([
        "auth", "login",
        "--server", "https://loom.example.com",
        "--token", "env:T",
    ])
    main(["auth", "logout"])
    capsys.readouterr()  # drain

    rc = main(["auth", "status"])
    assert rc == 1


# ──────────────────────────────────────────────────────────────────────
# logout
# ──────────────────────────────────────────────────────────────────────


def test_logout_clears_token_preserves_server(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("T", "tok-xxxxxxxx")
    main([
        "auth", "login",
        "--server", "https://loom.example.com",
        "--token", "env:T",
    ])
    capsys.readouterr()

    rc = main(["auth", "logout"])
    assert rc == 0
    cfg = load_config()
    assert cfg.auth_token is None
    assert cfg.server_url == "https://loom.example.com"


def test_logout_when_already_logged_out_is_idempotent(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["auth", "logout"])
    assert rc == 0
    assert "Already logged out" in capsys.readouterr().out
