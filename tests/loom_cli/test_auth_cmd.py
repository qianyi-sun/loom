"""`loom auth {login,status,logout}` end-to-end."""

from __future__ import annotations

import json
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from stat import S_IMODE
from typing import Any
from uuid import uuid4

import httpx
import pytest

from loom_cli.__main__ import main
from loom_cli.config import LoomConfig, load_config, save_config
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


@pytest.mark.parametrize("cookie_name", ["loom_session", "__Host-loom_session"])
def test_login_with_username_password_persists_session(
    monkeypatch: pytest.MonkeyPatch,
    mock_public_auth_server: MockAuthServer,
    capsys: pytest.CaptureFixture[str],
    cookie_name: str,
) -> None:
    monkeypatch.setenv("ADA_PASSWORD", "correct horse battery")
    mock_public_auth_server.canned[("POST", "/api/v1/auth/login")] = httpx.Response(
        200,
        json={
            "csrf_token": "loom_csrf_raw",
            "user": {"id": str(uuid4()), "username": "Ada"},
            "current_team": {"id": str(uuid4()), "name": "Research"},
        },
        headers={"set-cookie": f"{cookie_name}=loom_session_raw; Path=/; HttpOnly; Secure"},
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
    assert cfg.auth_session_cookie_name == cookie_name
    assert cfg.auth_csrf_token == "loom_csrf_raw"
    out = capsys.readouterr().out
    assert "Logged in to https://loom.test as Ada" in out
    assert "correct horse battery" not in out


def test_hosted_session_client_sends_persisted_name_and_rejects_other_origins():
    cfg = LoomConfig(
        server_url="https://alice.dev.example.com", auth_session_cookie="private-session",
        auth_session_cookie_name="__Host-loom_session", auth_csrf_token="private-csrf",
    )
    save_config(cfg)
    received = []

    def handler(request):
        received.append(request)
        return httpx.Response(200, json={})

    with authed_client(load_config(), transport=httpx.MockTransport(handler)) as client:
        assert client.get("/api/v1/auth/me").status_code == 200
        assert received[-1].headers["cookie"] == "__Host-loom_session=private-session"
        for url in ("https://bob.dev.example.com/api/v1/auth/me",
                    "http://alice.dev.example.com/api/v1/auth/me"):
            with pytest.raises(ValueError, match="origin"):
                client.get(url)
    assert len(received) == 1


def test_changing_cli_server_clears_credentials_bound_to_old_environment():
    save_config(LoomConfig(
        server_url="https://alice.dev.example.com", auth_session_cookie="alice-session",
        auth_session_cookie_name="__Host-loom_session", auth_csrf_token="alice-csrf",
        auth_token="alice-token",
    ))
    assert main(["config", "set", "server_url", "https://bob.dev.example.com"]) == 0
    config = load_config()
    assert config.server_url == "https://bob.dev.example.com"
    assert config.auth_session_cookie is None
    assert config.auth_csrf_token is None
    assert config.auth_token is None


def test_hosted_session_redirect_does_not_leak_credentials():
    cfg = LoomConfig(
        server_url="https://alice.dev.example.com", auth_session_cookie="private-session",
        auth_session_cookie_name="__Host-loom_session", auth_csrf_token="private-csrf",
    )
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(302, headers={"Location": "https://bob.dev.example.com/stolen"})

    with authed_client(cfg, transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(ValueError, match="origin"):
            client.get("/redirect", follow_redirects=True)
    assert len(requests) == 1


@pytest.mark.parametrize("header", [
    "__Host-loom_session=new; Path=/; HttpOnly",  # missing Secure
    "__Host-loom_session=new; Path=/; Secure; Domain=dev.example.com",
    "__Host-loom_session=new; Path=/api; Secure",
    "loom_session=new; Path=/; Secure",  # cannot downgrade a hosted session
])
def test_cli_rejects_unsafe_hosted_cookie_rotation(header):
    from loom_cli.server_client import persist_session_credentials_from_response

    cfg = LoomConfig(server_url="https://alice.dev.example.com",
                     auth_session_cookie="old", auth_session_cookie_name="__Host-loom_session")
    response = httpx.Response(200, headers={"set-cookie": header}, json={},
                              request=httpx.Request("GET", "https://alice.dev.example.com/api/v1/auth/me"))
    assert not persist_session_credentials_from_response(cfg, response)
    assert cfg.auth_session_cookie == "old"


def test_cli_cookie_jar_cannot_override_verified_hosted_session():
    cfg = LoomConfig(server_url="https://alice.dev.example.com",
                     auth_session_cookie="verified", auth_session_cookie_name="__Host-loom_session")
    received = []

    def handler(request):
        received.append(request.headers.get("cookie"))
        return httpx.Response(200, json={}, headers={
            "set-cookie": "__Host-loom_session=unverified; Domain=dev.example.com; Path=/",
        })

    with authed_client(cfg, transport=httpx.MockTransport(handler)) as client:
        client.get("/api/v1/auth/me")
        client.get("/api/v1/auth/me")
    assert received == ["__Host-loom_session=verified"] * 2


def test_cli_promotes_valid_explicit_session_refresh_before_next_request():
    cfg = LoomConfig(server_url="https://alice.dev.example.com", auth_session_cookie="old",
                     auth_session_cookie_name="__Host-loom_session", auth_csrf_token="csrf-old")

    def handler(request):
        if request.url.path == "/api/v1/auth/me":
            return httpx.Response(200, json={"csrf_token": "csrf-old"})
        if request.url.path == "/api/v1/auth/refresh":
            return httpx.Response(200, json={"csrf_token": "csrf-new"}, headers={
                "set-cookie": "__Host-loom_session=new; Path=/; HttpOnly; Secure",
            })
        if request.headers.get("cookie") == "__Host-loom_session=new":
            return httpx.Response(200, json={})
        return httpx.Response(401, json={})

    with authed_client(cfg, transport=httpx.MockTransport(handler)) as client:
        assert client.post("/api/v1/auth/refresh").status_code == 200
        assert client.get("/api/v1/environments").status_code == 200
    assert load_config().auth_session_cookie == "new"
    assert load_config().auth_csrf_token == "csrf-new"


def test_mutating_client_base_url_does_not_rebind_login_origin():
    cfg = LoomConfig(server_url="https://alice.dev.example.com", auth_session_cookie="private",
                     auth_session_cookie_name="__Host-loom_session")
    with authed_client(cfg, transport=httpx.MockTransport(lambda _: httpx.Response(200))) as client:
        client.base_url = "https://bob.dev.example.com"
        with pytest.raises(ValueError, match="origin"):
            client.get("/api/v1/auth/me")


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


def test_authed_client_refreshes_session_csrf_before_unsafe_request() -> None:
    cfg = LoomConfig(
        server_url="https://loom.test",
        auth_session_cookie="loom_session_old",
        auth_csrf_token="loom_csrf_old",
    )
    save_config(cfg)
    requests: list[httpx.Request] = []

    def _handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/api/v1/auth/me":
            return httpx.Response(
                200,
                json={"csrf_token": "loom_csrf_new"},
                headers={
                    "set-cookie": "loom_session=loom_session_new; Path=/; HttpOnly",
                },
            )
        if request.url.path == "/api/v1/batches":
            assert request.headers["X-Loom-CSRF"] == "loom_csrf_new"
            assert request.headers["cookie"] == "loom_session=loom_session_new"
            return httpx.Response(201, json={"id": "batch-1"})
        return httpx.Response(404, json={"detail": request.url.path})

    with authed_client(cfg, transport=httpx.MockTransport(_handler)) as client:
        response = client.post("/api/v1/batches", json={})

    assert response.status_code == 201
    assert [request.url.path for request in requests] == [
        "/api/v1/auth/me",
        "/api/v1/batches",
    ]
    persisted = load_config()
    assert persisted.auth_session_cookie == "loom_session_new"
    assert persisted.auth_csrf_token == "loom_csrf_new"


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
    assert out == (
        "Server:    https://loom.test\n"
        "Principal: legacy team token\n"
        "Team:      Team Alpha (owner)\n"
        "Scopes:    providers:manage, read:own, submit\n"
        "Token:     loom_api_abcd1234\n"
        "Expires:   2026-07-22T00:00:00Z\n"
    )
    assert raw_token not in out


def test_whoami_json_projects_only_allowlisted_identity_fields(
    mock_auth_server: MockAuthServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    save_config(
        LoomConfig(
            server_url="https://loom.example",
            auth_session_cookie="loom_session_private_value",
            auth_csrf_token="loom_csrf_private_value",
        )
    )
    mock_auth_server.canned[("GET", "/api/v1/auth/whoami")] = httpx.Response(
        200,
        json={
            "auth_kind": "session",
            "credential_type": None,
            "csrf_token": "loom_csrf_rotated_private_value",
            "expires_at": None,
            "full_token": "loom_api_full_private_value_abcdef",
            "principal_type": "user",
            "role": "owner",
            "scopes": ["submit", "read:own", "submit"],
            "team_id": "00000000-0000-0000-0000-000000000002",
            "team_name": "Private Team Name",
            "token_prefix": None,
            "user_id": "00000000-0000-0000-0000-000000000001",
            "username": "private-user-name",
            "unexpected": {"secret": "private-extra-value"},
        },
    )

    rc = main(["auth", "whoami", "--format", "json"])

    assert rc == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == (
        '{"auth_kind":"session","credential_type":null,"expires_at":null,'
        '"principal_type":"user","role":"owner","scopes":["read:own","submit"],'
        '"server":"https://loom.example","team_id":"00000000-0000-0000-0000-'
        '000000000002","token_prefix":null,"user_id":"00000000-0000-0000-0000-'
        '000000000001"}\n'
    )
    record = json.loads(captured.out)
    assert set(record) == {
        "auth_kind", "credential_type", "expires_at", "principal_type", "role",
        "scopes", "server", "team_id", "token_prefix", "user_id",
    }
    for omitted in (
        "loom_session_private_value", "loom_csrf_private_value",
        "loom_csrf_rotated_private_value", "loom_api_full_private_value_abcdef",
        "Private Team Name", "private-user-name", "private-extra-value",
    ):
        assert omitted not in captured.out


@pytest.mark.parametrize(
    ("server_url", "marker"),
    [
        pytest.param(
            "https://loom_api_userinfo_secret@loom.example",
            "loom_api_userinfo_secret",
            id="userinfo",
        ),
        pytest.param(
            "https://loom.example/loom_api_path_secret",
            "loom_api_path_secret",
            id="path",
        ),
        pytest.param(
            "https://loom.example?token=loom_api_query_secret",
            "loom_api_query_secret",
            id="query",
        ),
        pytest.param(
            "https://loom.example#loom_api_fragment_secret",
            "loom_api_fragment_secret",
            id="fragment",
        ),
    ],
)
def test_whoami_json_rejects_credential_markers_in_origin_before_http_without_leak(
    server_url: str,
    marker: str,
    mock_auth_server: MockAuthServer,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    save_config(LoomConfig(server_url=server_url, auth_token="private"))
    evidence = tmp_path / "whoami.json"

    with evidence.open("w") as output, redirect_stdout(output):
        rc = main(["auth", "whoami", "--format", "json"])

    captured = capsys.readouterr()
    assert rc == 2
    assert captured.out == ""
    assert captured.err == "error: invalid whoami JSON projection\n"
    assert evidence.read_text() == ""
    assert mock_auth_server.requests == []
    assert marker not in captured.out
    assert marker not in captured.err
    assert marker not in evidence.read_text()


@pytest.mark.parametrize(
    "server_url",
    [
        pytest.param("ftp://loom.example", id="scheme"),
        pytest.param("https://", id="missing-host"),
        pytest.param(" https://loom.example", id="leading-whitespace"),
        pytest.param("https://loom.example\n", id="control"),
        pytest.param("HTTPS://loom.example", id="uppercase-scheme"),
        pytest.param("https://LOOM.example", id="uppercase-host"),
        pytest.param("https://loom.example/", id="trailing-slash"),
        pytest.param("https://bad_host.example", id="invalid-host"),
        pytest.param("https://" + "a" * 254, id="oversized-host"),
        pytest.param("https://loom.example:0", id="zero-port"),
        pytest.param("https://loom.example:65536", id="oversized-port"),
        pytest.param("https://loom.example:not-a-port", id="invalid-port"),
        pytest.param("https://loom.example:080", id="noncanonical-port"),
    ],
)
def test_whoami_json_rejects_invalid_or_noncanonical_bounded_origin_before_http(
    server_url: str,
    mock_auth_server: MockAuthServer,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    save_config(LoomConfig(server_url=server_url, auth_token="private"))
    evidence = tmp_path / "whoami.json"

    with evidence.open("w") as output, redirect_stdout(output):
        rc = main(["auth", "whoami", "--format", "json"])

    captured = capsys.readouterr()
    assert rc == 2
    assert captured.out == ""
    assert captured.err == "error: invalid whoami JSON projection\n"
    assert evidence.read_text() == ""
    assert mock_auth_server.requests == []


def _valid_whoami_json_payload() -> dict[str, object]:
    return {
        "auth_kind": "bearer",
        "credential_type": "user_owned_api_token",
        "expires_at": "2026-07-22T00:00:00Z",
        "principal_type": "team",
        "role": None,
        "scopes": ["submit", "read:own", "submit"],
        "team_id": "00000000-0000-0000-0000-000000000002",
        "token_prefix": "abcdef12",
        "user_id": "00000000-0000-0000-0000-000000000001",
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("auth_kind", "token", id="auth-kind-enum"),
        pytest.param("auth_kind", "x" * 1024, id="oversized-enum-string"),
        pytest.param("auth_kind", 7, id="auth-kind-type"),
        pytest.param("credential_type", "unknown", id="credential-type-enum"),
        pytest.param("principal_type", "guest", id="principal-type-enum"),
        pytest.param("role", "super-owner", id="role-enum"),
        pytest.param("role", 7, id="role-type"),
        pytest.param(
            "user_id",
            "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
            id="uppercase-user-uuid",
        ),
        pytest.param(
            "team_id",
            "00000000-0000-0000-0000-000000000000",
            id="zero-team-uuid",
        ),
        pytest.param("user_id", 7, id="uuid-type"),
        pytest.param(
            "expires_at",
            "2026-02-30T00:00:00Z",
            id="invalid-timestamp-date",
        ),
        pytest.param(
            "expires_at",
            "2026-07-22T00:00:00Ztrailing",
            id="timestamp-trailing-data",
        ),
        pytest.param(
            "expires_at",
            "2026-07-22T00:00:00Z\nprivate",
            id="timestamp-control",
        ),
        pytest.param("expires_at", "2" * 128, id="oversized-timestamp"),
        pytest.param("expires_at", 7, id="timestamp-type"),
        pytest.param("token_prefix", "ABCDEF12", id="token-prefix-uppercase"),
        pytest.param("token_prefix", 7, id="token-prefix-type"),
        pytest.param("scopes", "read:own", id="scope-container-string"),
        pytest.param("scopes", ["read:own", 7], id="scope-member-type"),
        pytest.param("scopes", ["Read:own"], id="scope-grammar"),
        pytest.param("scopes", ["a" * 129], id="scope-member-oversized"),
        pytest.param(
            "scopes",
            [f"scope{index}" for index in range(65)],
            id="scope-count-oversized",
        ),
    ],
)
def test_whoami_json_rejects_malformed_allowlisted_projection_before_persistence(
    field: str,
    value: object,
    mock_auth_server: MockAuthServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    save_config(
        LoomConfig(
            server_url="https://loom.example",
            auth_session_cookie="loom_session_old",
            auth_csrf_token="loom_csrf_old",
        )
    )
    payload = _valid_whoami_json_payload()
    payload[field] = value
    mock_auth_server.canned[("GET", "/api/v1/auth/whoami")] = httpx.Response(
        200,
        json={**payload, "csrf_token": "loom_csrf_rotated_private"},
        headers={
            "set-cookie": "loom_session=loom_session_rotated_private; Path=/; HttpOnly",
        },
    )

    rc = main(["auth", "whoami", "--format", "json"])

    captured = capsys.readouterr()
    assert rc == 2
    assert captured.out == ""
    assert captured.err == "error: invalid whoami JSON projection\n"
    persisted = load_config()
    assert persisted.auth_session_cookie == "loom_session_old"
    assert persisted.auth_csrf_token == "loom_csrf_old"


def test_whoami_json_rejects_full_token_prefix_without_leak_or_persistence(
    mock_auth_server: MockAuthServer,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    marker = "loom_api_full_private_value_abcdef"
    save_config(
        LoomConfig(
            server_url="https://loom.example",
            auth_session_cookie="loom_session_old",
            auth_csrf_token="loom_csrf_old",
        )
    )
    payload = _valid_whoami_json_payload()
    payload["token_prefix"] = marker
    mock_auth_server.canned[("GET", "/api/v1/auth/whoami")] = httpx.Response(
        200,
        json={**payload, "csrf_token": "loom_csrf_rotated_private"},
        headers={
            "set-cookie": "loom_session=loom_session_rotated_private; Path=/; HttpOnly",
        },
    )
    evidence = tmp_path / "whoami.json"

    with evidence.open("w") as output, redirect_stdout(output):
        rc = main(["auth", "whoami", "--format", "json"])

    captured = capsys.readouterr()
    assert rc == 2
    assert captured.out == ""
    assert captured.err == "error: invalid whoami JSON projection\n"
    assert evidence.read_text() == ""
    persisted = load_config()
    assert persisted.auth_session_cookie == "loom_session_old"
    assert persisted.auth_csrf_token == "loom_csrf_old"
    assert marker not in captured.out
    assert marker not in captured.err
    assert marker not in evidence.read_text()


def test_whoami_json_rejects_invalid_json_response_without_persistence(
    mock_auth_server: MockAuthServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    save_config(
        LoomConfig(
            server_url="https://loom.example",
            auth_session_cookie="loom_session_old",
            auth_csrf_token="loom_csrf_old",
        )
    )
    mock_auth_server.canned[("GET", "/api/v1/auth/whoami")] = httpx.Response(
        200,
        content=b'{"auth_kind":',
        headers={
            "set-cookie": "loom_session=loom_session_rotated_private; Path=/; HttpOnly",
        },
    )

    rc = main(["auth", "whoami", "--format", "json"])

    captured = capsys.readouterr()
    assert rc == 2
    assert captured.out == ""
    assert captured.err == "error: invalid whoami JSON projection\n"
    persisted = load_config()
    assert persisted.auth_session_cookie == "loom_session_old"
    assert persisted.auth_csrf_token == "loom_csrf_old"


def test_whoami_rejects_invalid_format_before_http(
    mock_auth_server: MockAuthServer,
) -> None:
    save_config(LoomConfig(server_url="https://loom.example", auth_token="private"))

    with pytest.raises(SystemExit) as exc:
        main(["auth", "whoami", "--format", "yaml"])

    assert exc.value.code == 2
    assert mock_auth_server.requests == []


def test_whoami_persists_rotated_session_csrf(
    mock_auth_server: MockAuthServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    save_config(
        LoomConfig(
            server_url="https://loom.test",
            auth_session_cookie="loom_session_old",
            auth_csrf_token="loom_csrf_old",
        )
    )
    mock_auth_server.canned[("GET", "/api/v1/auth/whoami")] = httpx.Response(
        200,
        json={
            "auth_kind": "session",
            "principal_type": "user",
            "username": "Ada",
            "team_name": "Research",
            "role": "owner",
            "scopes": ["read:own"],
            "csrf_token": "loom_csrf_rotated",
        },
        headers={
            "set-cookie": "loom_session=loom_session_rotated; Path=/; HttpOnly",
        },
    )

    rc = main(["auth", "whoami"])

    assert rc == 0
    persisted = load_config()
    assert persisted.auth_session_cookie == "loom_session_rotated"
    assert persisted.auth_csrf_token == "loom_csrf_rotated"
    out = capsys.readouterr().out
    assert "Principal: browser session" in out
    assert "loom_session_rotated" not in out
    assert "loom_csrf_rotated" not in out


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


@pytest.mark.parametrize("rotate_cookie", [False, True])
def test_password_login_selects_exported_team_and_saves_rotated_credentials(
    monkeypatch: pytest.MonkeyPatch, mock_public_auth_server: MockAuthServer,
    rotate_cookie: bool,
) -> None:
    monkeypatch.setenv("ADA_PASSWORD", "test-password")
    default_team, target_team = str(uuid4()), str(uuid4())
    mock_public_auth_server.canned[("POST", "/api/v1/auth/login")] = httpx.Response(
        200, json={"csrf_token": "csrf-initial", "current_team": {"id": default_team}},
        headers={"set-cookie": "__Host-loom_session=initial; Path=/; HttpOnly; Secure"},
    )
    mock_public_auth_server.canned[("POST", "/api/v1/auth/team")] = httpx.Response(
        200, json={"csrf_token": "csrf-selected", "current_team": {"id": target_team}},
        headers={"set-cookie": "__Host-loom_session=selected; Path=/; HttpOnly; Secure"}
        if rotate_cookie else {},
    )
    assert main([
        "auth", "login", "--server", "https://loom.test", "--username", "Ada",
        "--password", "env:ADA_PASSWORD", "--team-id", target_team,
    ]) == 0
    assert len(mock_public_auth_server.requests) == 2
    request = mock_public_auth_server.requests[1]
    assert request.url.path == "/api/v1/auth/team"
    assert json.loads(request.content) == {"team_id": target_team}
    assert request.headers["Cookie"] == "__Host-loom_session=initial"
    assert request.headers["X-Loom-CSRF"] == "csrf-initial"
    cfg = load_config()
    assert cfg.auth_session_cookie == ("selected" if rotate_cookie else "initial")
    assert cfg.auth_session_cookie_name == "__Host-loom_session"
    assert cfg.auth_csrf_token == "csrf-selected"
    assert cfg.auth_token is None


@pytest.mark.parametrize("selection_response", [
    httpx.Response(403, json={"detail": "user is not a team member"}),
    httpx.Response(200, json={"csrf_token": "rotated", "current_team": {"id": "wrong-team"}}),
])
def test_failed_team_login_preserves_previous_login_and_returns_nonzero(
    monkeypatch: pytest.MonkeyPatch, mock_public_auth_server: MockAuthServer,
    selection_response: httpx.Response,
) -> None:
    monkeypatch.setenv("ADA_PASSWORD", "test-password")
    save_config(LoomConfig(server_url="https://previous.test", auth_token="previous-token"))
    mock_public_auth_server.canned[("POST", "/api/v1/auth/login")] = httpx.Response(
        200, json={"csrf_token": "csrf-initial"},
        headers={"set-cookie": "__Host-loom_session=initial; Path=/; HttpOnly; Secure"},
    )
    mock_public_auth_server.canned[("POST", "/api/v1/auth/team")] = selection_response
    assert main([
        "auth", "login", "--server", "https://loom.test", "--username", "Ada",
        "--password", "env:ADA_PASSWORD", "--team-id", str(uuid4()),
    ]) != 0
    assert load_config().server_url == "https://previous.test"
    assert load_config().auth_token == "previous-token"
    assert load_config().auth_session_cookie is None
    assert len(mock_public_auth_server.requests) == 2


def test_token_login_rejects_team_selection(
    monkeypatch: pytest.MonkeyPatch, mock_public_auth_server: MockAuthServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("TEST_TOKEN", "test-token")
    assert main([
        "auth", "login", "--server", "https://loom.test", "--token", "env:TEST_TOKEN",
        "--team-id", str(uuid4()),
    ]) == 2
    assert "--team-id requires username/password login" in capsys.readouterr().err
    assert load_config().auth_token is None
    assert not mock_public_auth_server.requests
