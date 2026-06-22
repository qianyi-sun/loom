"""`loom auth {login,status,logout}` end-to-end."""

from __future__ import annotations

from io import StringIO
from pathlib import Path
from stat import S_IMODE
from typing import Any

import httpx
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


def test_login_writes_config_file_owner_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MY_LOOM_TOKEN", "loom_api_owner_only_abcdef123456")

    rc = main([
        "auth", "login",
        "--server", "https://loom.example.com",
        "--token", "env:MY_LOOM_TOKEN",
    ])

    assert rc == 0
    from loom_cli.config import config_path
    config_file = config_path()
    assert S_IMODE(config_file.stat().st_mode) == 0o600


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


# ──────────────────────────────────────────────────────────────────────
# whoami
# ──────────────────────────────────────────────────────────────────────


class MockAuthServer:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.canned: dict[tuple[str, str], httpx.Response] = {}


@pytest.fixture
def mock_auth_server(monkeypatch: pytest.MonkeyPatch) -> MockAuthServer:
    server = MockAuthServer()

    def _handler(request: httpx.Request) -> httpx.Response:
        server.requests.append(request)
        key = (request.method, request.url.path)
        if key in server.canned:
            return server.canned[key]
        return httpx.Response(404, json={"detail": f"no mock for {key}"})

    transport = httpx.MockTransport(_handler)

    def _patched_authed_client(cfg: Any, *, timeout: float = 30.0) -> httpx.Client:
        return httpx.Client(
            base_url=cfg.server_url,
            headers={"Authorization": f"Bearer {cfg.auth_token}"},
            transport=transport,
            timeout=timeout,
        )

    monkeypatch.setattr(
        "loom_cli.auth_cmd.authed_client", _patched_authed_client,
        raising=False,
    )
    return server


def test_whoami_when_not_logged_in_returns_2(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["auth", "whoami"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "not logged in" in err
    assert "Create a team API token in the web UI" in err
    assert "loom auth login --server URL --token env:LOOM_API_TOKEN" in err


def test_whoami_prints_server_principal_team_scopes_and_prefix(
    monkeypatch: pytest.MonkeyPatch,
    mock_auth_server: MockAuthServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    raw_token = "loom_api_plain_secret_abcdef123456"
    monkeypatch.setenv("TOK", raw_token)
    assert main([
        "auth", "login",
        "--server", "https://loom.test",
        "--token", "env:TOK",
    ]) == 0
    capsys.readouterr()
    mock_auth_server.canned[("GET", "/api/v1/auth/whoami")] = httpx.Response(
        200,
        json={
            "auth_kind": "bearer",
            "principal_type": "team",
            "team_id": "00000000-0000-0000-0000-000000000001",
            "team_name": "Team Alpha",
            "role": "owner",
            "scopes": ["read:own", "submit", "providers:manage"],
            "token_prefix": "loom_api_abcd1234",
            "expires_at": "2026-07-22T00:00:00Z",
        },
    )

    rc = main(["auth", "whoami"])

    assert rc == 0
    assert mock_auth_server.requests[0].method == "GET"
    assert mock_auth_server.requests[0].url.path == "/api/v1/auth/whoami"
    assert mock_auth_server.requests[0].headers["authorization"] == (
        f"Bearer {raw_token}"
    )
    out = capsys.readouterr().out
    assert "Server:    https://loom.test" in out
    assert "Principal: team token" in out
    assert "Team:      Team Alpha (owner)" in out
    assert "Scopes:    providers:manage, read:own, submit" in out
    assert "Token:     loom_api_abcd1234" in out
    assert raw_token not in out


def test_whoami_rejected_token_returns_clear_error(
    monkeypatch: pytest.MonkeyPatch,
    mock_auth_server: MockAuthServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    raw_token = "loom_api_revoked_or_expired_secret"
    monkeypatch.setenv("TOK", raw_token)
    assert main([
        "auth", "login",
        "--server", "https://loom.test",
        "--token", "env:TOK",
    ]) == 0
    capsys.readouterr()
    mock_auth_server.canned[("GET", "/api/v1/auth/whoami")] = httpx.Response(
        401, json={"detail": "token expired"},
    )

    rc = main(["auth", "whoami"])

    assert rc == 1
    err = capsys.readouterr().err
    assert "token rejected by server" in err
    assert "revoked, expired, or missing scope" in err
    assert "loom auth login --server URL --token env:LOOM_API_TOKEN" in err
    assert raw_token not in err


def test_whoami_auth_error_redacts_signed_url_and_token(
    monkeypatch: pytest.MonkeyPatch,
    mock_auth_server: MockAuthServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    raw_token = "loom_api_plain_secret_abcdef123456"
    monkeypatch.setenv("TOK", raw_token)
    assert main([
        "auth", "login",
        "--server", "https://loom.test",
        "--token", "env:TOK",
    ]) == 0
    capsys.readouterr()
    mock_auth_server.canned[("GET", "/api/v1/auth/whoami")] = httpx.Response(
        403,
        json={
            "detail": (
                "denied https://minio.internal/bucket/key?"
                "X-Amz-Signature=secret-sig with token "
                "loom_api_leaked_detail_abcdef"
            ),
        },
    )

    rc = main(["auth", "whoami"])

    assert rc == 1
    err = capsys.readouterr().err
    assert "token rejected by server" in err
    assert "minio.internal" not in err
    assert "X-Amz-Signature=secret-sig" not in err
    assert "loom_api_leaked_detail_abcdef" not in err
    assert raw_token not in err
    assert "[REDACTED:" in err
