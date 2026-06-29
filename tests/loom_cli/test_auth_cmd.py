"""`loom auth {login,status,logout}` end-to-end."""

from __future__ import annotations

from io import StringIO
from pathlib import Path
from stat import S_IMODE
from typing import Any
from uuid import uuid4

import httpx
import pytest

from loom_cli.__main__ import main
from loom_cli.config import LoomConfig, load_config
from loom_cli.server_client import authed_client


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


@pytest.fixture
def mock_public_auth_server(monkeypatch: pytest.MonkeyPatch) -> MockAuthServer:
    server = MockAuthServer()

    def _handler(request: httpx.Request) -> httpx.Response:
        server.requests.append(request)
        key = (request.method, request.url.path)
        if key in server.canned:
            return server.canned[key]
        return httpx.Response(404, json={"detail": f"no mock for {key}"})

    transport = httpx.MockTransport(_handler)

    def _client(server_url: str, *, timeout: float = 30.0) -> httpx.Client:
        return httpx.Client(
            base_url=server_url,
            transport=transport,
            timeout=timeout,
        )

    monkeypatch.setattr("loom_cli.auth_cmd._plain_client", _client, raising=False)
    return server


def test_register_posts_username_and_team(
    mock_public_auth_server: MockAuthServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    team_id = str(uuid4())
    mock_public_auth_server.canned[
        ("POST", "/api/v1/auth/registration-requests")
    ] = httpx.Response(
        202,
        json={
            "id": str(uuid4()),
            "username": "Ada",
            "team_id": team_id,
            "status": "pending",
        },
    )

    rc = main([
        "auth", "register",
        "--server", "https://loom.test",
        "--username", "Ada",
        "--team-id", team_id,
    ])

    assert rc == 0
    req = mock_public_auth_server.requests[0]
    assert req.method == "POST"
    assert req.url.path == "/api/v1/auth/registration-requests"
    assert req.read() == (
        b'{"username":"Ada","team_id":"' + team_id.encode() + b'","metadata":{}}'
    )
    assert "Registration request submitted" in capsys.readouterr().out


def test_teams_lists_public_registration_teams(
    mock_public_auth_server: MockAuthServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    team_id = str(uuid4())
    mock_public_auth_server.canned[
        ("GET", "/api/v1/auth/public-teams")
    ] = httpx.Response(
        200,
        json={"items": [{"id": team_id, "name": "Research"}]},
    )

    rc = main(["auth", "teams", "--server", "https://loom.test"])

    assert rc == 0
    assert mock_public_auth_server.requests[0].method == "GET"
    out = capsys.readouterr().out
    assert "Research" in out
    assert team_id in out


def test_login_with_username_password_persists_session(
    monkeypatch: pytest.MonkeyPatch,
    mock_public_auth_server: MockAuthServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("ADA_PASSWORD", "correct horse battery")
    mock_public_auth_server.canned[("POST", "/api/v1/auth/login")] = httpx.Response(
        200,
        json={
            "csrf_token": "loom_csrf_raw",
            "user": {"id": str(uuid4()), "username": "Ada"},
            "current_team": {"id": str(uuid4()), "name": "Research"},
        },
        headers={"set-cookie": "loom_session=loom_session_raw; Path=/; HttpOnly"},
    )

    rc = main([
        "auth", "login",
        "--server", "https://loom.test",
        "--username", "Ada",
        "--password", "env:ADA_PASSWORD",
    ])

    assert rc == 0
    req = mock_public_auth_server.requests[0]
    assert req.url.path == "/api/v1/auth/login"
    assert req.read() == b'{"username":"Ada","password":"correct horse battery"}'
    cfg = load_config()
    assert cfg.server_url == "https://loom.test"
    assert cfg.auth_token is None
    assert cfg.auth_session_cookie == "loom_session_raw"
    assert cfg.auth_csrf_token == "loom_csrf_raw"
    out = capsys.readouterr().out
    assert "Logged in to https://loom.test as Ada" in out
    assert "correct horse battery" not in out


def test_setup_password_uses_secret_sources(
    monkeypatch: pytest.MonkeyPatch,
    mock_public_auth_server: MockAuthServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("SETUP_TOKEN", "loom_setup_secret")
    monkeypatch.setenv("SETUP_PASSWORD", "new-password-1234")
    mock_public_auth_server.canned[
        ("POST", "/api/v1/auth/setup/complete")
    ] = httpx.Response(200, json={"status": "active", "user": {"username": "Ada"}})

    rc = main([
        "auth", "setup-password",
        "--server", "https://loom.test",
        "--token", "env:SETUP_TOKEN",
        "--password", "env:SETUP_PASSWORD",
        "--confirm-password", "env:SETUP_PASSWORD",
    ])

    assert rc == 0
    assert mock_public_auth_server.requests[0].read() == (
        b'{"token":"loom_setup_secret","password":"new-password-1234",'
        b'"confirm_password":"new-password-1234"}'
    )
    assert "Password set for Ada" in capsys.readouterr().out


def test_forgot_and_reset_password_commands(
    monkeypatch: pytest.MonkeyPatch,
    mock_public_auth_server: MockAuthServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("RESET_TOKEN", "loom_reset_secret")
    monkeypatch.setenv("RESET_PASSWORD", "new-password-5678")
    mock_public_auth_server.canned[
        ("POST", "/api/v1/auth/password-reset-requests")
    ] = httpx.Response(202, json={"status": "pending"})
    mock_public_auth_server.canned[
        ("POST", "/api/v1/auth/reset/complete")
    ] = httpx.Response(200, json={"status": "active", "user": {"username": "Ada"}})

    forgot_rc = main([
        "auth", "forgot-password",
        "--server", "https://loom.test",
        "--username", "Ada",
    ])
    reset_rc = main([
        "auth", "reset-password",
        "--server", "https://loom.test",
        "--token", "env:RESET_TOKEN",
        "--password", "env:RESET_PASSWORD",
        "--confirm-password", "env:RESET_PASSWORD",
    ])

    assert forgot_rc == 0
    assert reset_rc == 0
    assert [request.url.path for request in mock_public_auth_server.requests] == [
        "/api/v1/auth/password-reset-requests",
        "/api/v1/auth/reset/complete",
    ]
    out = capsys.readouterr().out
    assert "Password reset request submitted" in out
    assert "Password reset for Ada" in out


def test_authed_client_uses_session_cookie_and_csrf_without_bearer() -> None:
    cfg = LoomConfig(
        server_url="https://loom.test",
        auth_session_cookie="loom_session_raw",
        auth_csrf_token="loom_csrf_raw",
    )

    with authed_client(cfg) as client:
        assert "authorization" not in client.headers
        assert client.cookies.get("loom_session") == "loom_session_raw"
        assert client.headers["X-Loom-CSRF"] == "loom_csrf_raw"


def test_whoami_when_not_logged_in_returns_2(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["auth", "whoami"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "not logged in" in err
    assert "loom auth login --server URL --username USER --password env:PASS" in err
    assert "loom auth login --server URL --token env:LOOM_API_TOKEN" in err


def test_whoami_prints_legacy_team_token_scopes_and_prefix(
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
    assert "Principal: legacy team token" in out
    assert "Team:      Team Alpha (owner)" in out
    assert "Scopes:    providers:manage, read:own, submit" in out
    assert "Token:     loom_api_abcd1234" in out
    assert raw_token not in out


def test_whoami_prints_user_owned_api_token_identity(
    monkeypatch: pytest.MonkeyPatch,
    mock_auth_server: MockAuthServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    raw_token = "loom_api_user_owned_secret_abcdef123456"
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
            "credential_type": "user_owned_api_token",
            "user_id": "11111111-1111-1111-1111-111111111111",
            "username": "ada",
            "team_id": "00000000-0000-0000-0000-000000000001",
            "team_name": "Team Alpha",
            "role": "owner",
            "scopes": ["read:own", "submit"],
            "token_prefix": "loom_api_user1234",
            "expires_at": "2026-07-22T00:00:00Z",
        },
    )

    rc = main(["auth", "whoami"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "Principal: user-owned API token" in out
    assert "User:      ada (11111111-1111-1111-1111-111111111111)" in out
    assert "Team:      Team Alpha (owner)" in out
    assert "Scopes:    read:own, submit" in out
    assert "Token:     loom_api_user1234" in out
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
