"""`loom admin tokens worker {mint, revoke, rotate}` tests (#80)."""

from __future__ import annotations

import io
import json
from typing import Any

import httpx
import pytest

from loom_cli.__main__ import main


class _StubResponse:
    def __init__(self, status_code: int, json_data: Any = None, text: str = "") -> None:
        self.status_code = status_code
        self._json = json_data
        self.text = text

    def json(self) -> Any:
        return self._json


# ──────────────────────────────────────────────────────────────────────
# loom admin tokens worker mint
# ──────────────────────────────────────────────────────────────────────


def test_mint_posts_to_cp_with_bearer(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, Any] = {}

    def _fake_post(url, *, json, headers, timeout):  # type: ignore[no-untyped-def]
        captured["url"] = url
        captured["json"] = json
        captured["headers"] = headers
        return _StubResponse(
            201,
            json_data={
                "token": "loom_w_deadbeef",
                "token_hash_prefix": "ab12cd34",
            },
        )

    monkeypatch.setattr(httpx, "post", _fake_post)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")

    rc = main([
        "admin", "tokens", "worker", "mint",
        "--cp-url", "http://cp.example:8080",
        "--expires-in-days", "30",
    ])
    assert rc == 0
    assert captured["url"] == "http://cp.example:8080/admin/worker-tokens"
    assert captured["json"] == {"expires_in_days": 30}
    assert captured["headers"]["Authorization"] == "Bearer admin-secret"

    out = capsys.readouterr().out
    assert "ab12cd34" in out
    assert "loom_w_deadbeef" in out
    assert "loom-secrets" in out  # rollout hint


def test_mint_strips_trailing_slash_on_cp_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, str] = {}

    def _fake_post(url, **kwargs):  # type: ignore[no-untyped-def]
        captured["url"] = url
        return _StubResponse(
            201, json_data={"token": "x", "token_hash_prefix": "y"},
        )

    monkeypatch.setattr(httpx, "post", _fake_post)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")
    main([
        "admin", "tokens", "worker", "mint",
        "--cp-url", "http://cp:8080/",
    ])
    assert captured["url"] == "http://cp:8080/admin/worker-tokens"


def test_mint_json_format_emits_raw_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _fake_post(url, **kwargs):  # type: ignore[no-untyped-def]
        return _StubResponse(
            201, json_data={"token": "t", "token_hash_prefix": "p"},
        )

    monkeypatch.setattr(httpx, "post", _fake_post)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")
    rc = main([
        "admin", "tokens", "worker", "mint", "--format", "json",
    ])
    assert rc == 0
    out = capsys.readouterr().out
    parsed = json.loads(out)
    assert parsed == {"token": "t", "token_hash_prefix": "p"}
    # Rollout-hint text MUST NOT be present in JSON mode.
    assert "loom-secrets" not in out


def test_mint_admin_token_env_missing_returns_2(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """When env:VAR resolves to an unset variable, exit 2. Test uses a
    deliberately unique var name to dodge the dev `.env` file (loaded
    by `_load_dotenv_from_cwd` at the start of every `main()` call)."""
    monkeypatch.delenv("LOOM_ADMIN_TOKEN_FOR_TEST", raising=False)
    rc = main([
        "admin", "tokens", "worker", "mint",
        "--admin-token", "env:LOOM_ADMIN_TOKEN_FOR_TEST",
    ])
    assert rc == 2
    assert "LOOM_ADMIN_TOKEN_FOR_TEST" in capsys.readouterr().err


def test_mint_cp_unreachable_returns_2(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _raise(url, **kwargs):  # type: ignore[no-untyped-def]
        raise httpx.ConnectError("Connection refused")

    monkeypatch.setattr(httpx, "post", _raise)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")
    rc = main(["admin", "tokens", "worker", "mint"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "could not reach CP" in err
    assert "port-forward" in err


def test_mint_cp_non_201_returns_1(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _fake_post(url, **kwargs):  # type: ignore[no-untyped-def]
        return _StubResponse(403, json_data=None, text="missing scope")

    monkeypatch.setattr(httpx, "post", _fake_post)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "wrong-token")
    rc = main(["admin", "tokens", "worker", "mint"])
    assert rc == 1
    assert "403" in capsys.readouterr().err


def test_mint_admin_token_via_stdin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def _fake_post(url, *, json, headers, timeout):  # type: ignore[no-untyped-def]
        captured["headers"] = headers
        return _StubResponse(
            201, json_data={"token": "x", "token_hash_prefix": "y"},
        )

    monkeypatch.setattr(httpx, "post", _fake_post)
    monkeypatch.setattr("sys.stdin", io.StringIO("from-stdin\n"))
    rc = main([
        "admin", "tokens", "worker", "mint", "--admin-token", "-",
    ])
    assert rc == 0
    assert captured["headers"]["Authorization"] == "Bearer from-stdin"


def test_mint_admin_token_via_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any,
) -> None:
    captured: dict[str, Any] = {}
    token_file = tmp_path / "admin.txt"
    token_file.write_text("from-file\n")

    def _fake_post(url, *, json, headers, timeout):  # type: ignore[no-untyped-def]
        captured["headers"] = headers
        return _StubResponse(
            201, json_data={"token": "x", "token_hash_prefix": "y"},
        )

    monkeypatch.setattr(httpx, "post", _fake_post)
    rc = main([
        "admin", "tokens", "worker", "mint",
        "--admin-token", f"file:{token_file}",
    ])
    assert rc == 0
    assert captured["headers"]["Authorization"] == "Bearer from-file"


# ──────────────────────────────────────────────────────────────────────
# loom admin tokens worker revoke
# ──────────────────────────────────────────────────────────────────────


def test_revoke_deletes_at_correct_url(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, Any] = {}

    def _fake_delete(url, *, headers, timeout):  # type: ignore[no-untyped-def]
        captured["url"] = url
        captured["headers"] = headers
        return _StubResponse(200, json_data={"status": "revoked"})

    monkeypatch.setattr(httpx, "delete", _fake_delete)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")
    rc = main([
        "admin", "tokens", "worker", "revoke", "ab12cd34",
        "--cp-url", "http://cp:8080",
    ])
    assert rc == 0
    assert captured["url"] == (
        "http://cp:8080/admin/worker-tokens/ab12cd34"
    )
    assert "ab12cd34" in capsys.readouterr().out


def test_revoke_rejects_non_hex_prefix(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Server-side regex also rejects non-hex, but we catch client-side
    to avoid a round-trip."""
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")
    rc = main([
        "admin", "tokens", "worker", "revoke", "not-hex!",
    ])
    assert rc == 2
    assert "hex" in capsys.readouterr().err.lower()


def test_revoke_rejects_too_short_prefix(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """3 chars < the 4-char floor that prevents wildcard-revoke
    disasters."""
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")
    rc = main(["admin", "tokens", "worker", "revoke", "abc"])
    assert rc == 2
    assert "4-64" in capsys.readouterr().err


def test_revoke_cp_unreachable_returns_2(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _raise(url, **kwargs):  # type: ignore[no-untyped-def]
        raise httpx.ConnectError("nope")

    monkeypatch.setattr(httpx, "delete", _raise)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")
    rc = main([
        "admin", "tokens", "worker", "revoke", "ab12cd34",
    ])
    assert rc == 2
    assert "could not reach CP" in capsys.readouterr().err


def test_revoke_cp_non_200_returns_1(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _fake_delete(url, **kwargs):  # type: ignore[no-untyped-def]
        return _StubResponse(400, text="prefix must be 4-64 hex characters")

    monkeypatch.setattr(httpx, "delete", _fake_delete)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")
    rc = main([
        "admin", "tokens", "worker", "revoke", "abcd",
    ])
    assert rc == 1


# ──────────────────────────────────────────────────────────────────────
# loom admin tokens worker rotate
# ──────────────────────────────────────────────────────────────────────


def test_rotate_mints_then_prints_checklist(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Rotate is `mint` + a checklist. Must NOT call delete."""
    delete_called = False

    def _fake_post(url, **kwargs):  # type: ignore[no-untyped-def]
        return _StubResponse(
            201,
            json_data={"token": "loom_w_new", "token_hash_prefix": "newpref"},
        )

    def _fake_delete(url, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal delete_called
        delete_called = True
        return _StubResponse(200, json_data={"status": "revoked"})

    monkeypatch.setattr(httpx, "post", _fake_post)
    monkeypatch.setattr(httpx, "delete", _fake_delete)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")

    rc = main(["admin", "tokens", "worker", "rotate"])
    assert rc == 0
    assert not delete_called  # rotate doesn't auto-revoke
    out = capsys.readouterr().out
    assert "newpref" in out
    assert "Rotation checklist" in out
    assert "rollout restart" in out
    assert "loom admin tokens worker revoke" in out


def test_rotate_mint_failure_propagates_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _fake_post(url, **kwargs):  # type: ignore[no-untyped-def]
        return _StubResponse(403, text="missing scope")

    monkeypatch.setattr(httpx, "post", _fake_post)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "wrong")
    rc = main(["admin", "tokens", "worker", "rotate"])
    assert rc == 1
    # Checklist must NOT have been printed when mint failed.
    assert "Rotation checklist" not in capsys.readouterr().out


def test_rotate_json_mode_skips_checklist(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--format json → output is just the mint JSON, no human-readable
    rollout text (would break parsers)."""
    def _fake_post(url, **kwargs):  # type: ignore[no-untyped-def]
        return _StubResponse(
            201, json_data={"token": "t", "token_hash_prefix": "p"},
        )

    monkeypatch.setattr(httpx, "post", _fake_post)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")
    rc = main([
        "admin", "tokens", "worker", "rotate", "--format", "json",
    ])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Rotation checklist" not in out
    assert "rollout restart" not in out


# ──────────────────────────────────────────────────────────────────────
# loom admin slurm-workers status
# ──────────────────────────────────────────────────────────────────────


def test_slurm_workers_status_gets_cp_capacity_without_printing_secrets(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, Any] = {}

    def _fake_get(url, *, headers, timeout):  # type: ignore[no-untyped-def]
        captured["url"] = url
        captured["headers"] = headers
        return _StubResponse(
            200,
            json_data={
                "summary": [
                    {
                        "environment": "production",
                        "pool_name": "oldlab",
                        "desired_slots": 18,
                        "active_slots": 12,
                        "pending_slots": 6,
                        "running_jobs": 2,
                        "pending_jobs": 1,
                        "failed_submissions": 0,
                        "cancelled_pending_jobs": 0,
                        "idle_exits": 1,
                    },
                ],
                "jobs": [
                    {
                        "job_id": "13441",
                        "environment": "production",
                        "pool_name": "oldlab",
                        "state": "running",
                        "nodelist": "oldlab-1",
                        "requested_concurrency": 6,
                        "redacted_env": {"LOOM_WORKER_TOKEN": "<redacted>"},
                    },
                ],
            },
        )

    monkeypatch.setattr(httpx, "get", _fake_get)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")

    rc = main([
        "admin", "slurm-workers", "status",
        "--cp-url", "http://cp:8080/",
    ])
    assert rc == 0
    assert captured["url"] == "http://cp:8080/admin/slurm-worker-jobs/status"
    assert captured["headers"]["Authorization"] == "Bearer admin-secret"

    out = capsys.readouterr().out
    assert "production/oldlab" in out
    assert "desired=18 active=12 pending=6" in out
    assert "13441" in out
    assert "loom_w_secret" not in out
    assert "<redacted>" in out


def test_slurm_workers_status_json_format_emits_raw_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = {"summary": [], "jobs": []}

    def _fake_get(url, **kwargs):  # type: ignore[no-untyped-def]
        return _StubResponse(200, json_data=payload)

    monkeypatch.setattr(httpx, "get", _fake_get)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")

    rc = main(["admin", "slurm-workers", "status", "--format", "json"])
    assert rc == 0
    assert json.loads(capsys.readouterr().out) == payload


# ──────────────────────────────────────────────────────────────────────
# loom admin tokens team — uses loom_service /api/v1/tokens
# ──────────────────────────────────────────────────────────────────────


from pathlib import Path  # noqa: E402

_TEAM_ID = "00000000-0000-0000-0000-000000000aaa"


@pytest.fixture
def _team_logged_in(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Each team-token test starts logged in with an isolated config
    dir so we don't read the dev `~/.config/loom/config.toml`."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    monkeypatch.setenv("MY_TOK", "loom_admin_abcdefgh")
    main([
        "auth", "login",
        "--server", "https://loom.test",
        "--token", "env:MY_TOK",
    ])


class _MockServer:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.canned: dict[tuple[str, str], httpx.Response] = {}


@pytest.fixture
def mock_server(
    monkeypatch: pytest.MonkeyPatch, _team_logged_in: None,
) -> _MockServer:
    server = _MockServer()

    def _handler(request: httpx.Request) -> httpx.Response:
        server.requests.append(request)
        key = (request.method, request.url.path)
        if key in server.canned:
            return server.canned[key]
        return httpx.Response(404, json={"detail": f"no mock for {key}"})

    transport = httpx.MockTransport(_handler)

    def _patched(cfg: Any, *, timeout: float = 30.0) -> httpx.Client:
        return httpx.Client(
            base_url=cfg.server_url,
            headers={"Authorization": f"Bearer {cfg.auth_token}"},
            transport=transport,
            timeout=timeout,
        )

    monkeypatch.setattr("loom_cli.admin_cmd.authed_client", _patched)
    return server


# ── team mint ────────────────────────────────────────────────────────


def test_team_mint_posts_payload(
    mock_server: _MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("POST", "/api/v1/tokens")] = httpx.Response(
        201, json={
            "token": "loom_team_xyz",
            "token_hash_prefix": "01234567",
            "expires_at": "2026-09-01T00:00:00+00:00",
        },
    )
    rc = main([
        "admin", "tokens", "team", "mint",
        "--name", "ci-submit-token",
        "--team-id", _TEAM_ID,
        "--scopes", "read:own,submit,providers:manage",
        "--expires-in-days", "30",
        "--admin-actor", "qianyi",
    ])
    assert rc == 0
    req = mock_server.requests[0]
    body = json.loads(req.content)
    assert body == {
        "name": "ci-submit-token",
        "type": "team",
        "team_id": _TEAM_ID,
        "scopes": ["read:own", "submit", "providers:manage"],
        "expires_in_days": 30,
    }
    assert req.headers["X-Loom-Admin-Actor"] == "qianyi"
    out = capsys.readouterr().out
    assert "01234567" in out
    assert "loom_team_xyz" in out


def test_team_mint_rejects_admin_type_before_request(
    _team_logged_in: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Admin credentials are singleton secret-file credentials now.

    The compatibility CLI surface must reject DB-backed admin token creation
    before issuing an HTTP request.
    """
    with pytest.raises(SystemExit) as exc:
        main([
            "admin", "tokens", "team", "mint",
            "--type", "admin",
            "--admin-actor", "qianyi",
        ])
    assert exc.value.code == 2
    assert "invalid choice" in capsys.readouterr().err


def test_team_mint_rejects_unknown_scope_before_request(
    _team_logged_in: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Unknown scopes fail at argparse-time so a typo doesn't burn an
    HTTP round-trip + audit log entry."""
    with pytest.raises(SystemExit):
        main([
            "admin", "tokens", "team", "mint",
            "--scopes", "submit,bogus",
            "--team-id", _TEAM_ID,
        ])
    err = capsys.readouterr().err
    assert "bogus" in err


def test_team_mint_default_scopes(
    mock_server: _MockServer,
) -> None:
    mock_server.canned[("POST", "/api/v1/tokens")] = httpx.Response(
        201, json={
            "token": "loom_team_xyz",
            "token_hash_prefix": "abc12345",
            "expires_at": "2026-09-01T00:00:00+00:00",
        },
    )
    rc = main([
        "admin", "tokens", "team", "mint",
        "--name", "default-submit-token",
        "--team-id", _TEAM_ID,
    ])
    assert rc == 0
    body = json.loads(mock_server.requests[0].content)
    assert body["scopes"] == ["read:own", "submit"]
    assert body["expires_in_days"] == 90  # default


def test_team_mint_no_admin_actor_omits_header(
    mock_server: _MockServer,
) -> None:
    mock_server.canned[("POST", "/api/v1/tokens")] = httpx.Response(
        201, json={
            "token": "t", "token_hash_prefix": "ab123456",
            "expires_at": "2026-09-01T00:00:00+00:00",
        },
    )
    main([
        "admin", "tokens", "team", "mint",
        "--name", "no-admin-actor-token",
        "--team-id", _TEAM_ID,
    ])
    assert "X-Loom-Admin-Actor" not in mock_server.requests[0].headers


def test_team_mint_not_logged_in_returns_2(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    # No `auth login` — config is empty.
    rc = main([
        "admin", "tokens", "team", "mint",
        "--name", "not-logged-in-token",
        "--team-id", _TEAM_ID,
    ])
    assert rc == 2
    assert "not logged in" in capsys.readouterr().err.lower()


def test_team_mint_server_403_returns_1(
    mock_server: _MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("POST", "/api/v1/tokens")] = httpx.Response(
        403, json={"detail": "X-Loom-Admin-Actor required"},
    )
    rc = main([
        "admin", "tokens", "team", "mint",
        "--name", "forbidden-token",
        "--team-id", _TEAM_ID,
    ])
    assert rc == 1
    err = capsys.readouterr().err
    assert "token rejected by server" in err
    assert "X-Loom-Admin-Actor required" in err


def test_team_mint_json_format(
    mock_server: _MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("POST", "/api/v1/tokens")] = httpx.Response(
        201, json={
            "token": "t", "token_hash_prefix": "01234567",
            "expires_at": "2026-09-01T00:00:00+00:00",
        },
    )
    main([
        "admin", "tokens", "team", "mint",
        "--name", "json-token",
        "--team-id", _TEAM_ID, "--format", "json",
    ])
    out = capsys.readouterr().out
    parsed = json.loads(out)
    assert parsed["token_hash_prefix"] == "01234567"


# ── team revoke ──────────────────────────────────────────────────────


def test_team_revoke_deletes_at_correct_url(
    mock_server: _MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[
        ("DELETE", "/api/v1/tokens/01234567")
    ] = httpx.Response(204)
    rc = main([
        "admin", "tokens", "team", "revoke", "01234567",
        "--admin-actor", "qianyi",
    ])
    assert rc == 0
    req = mock_server.requests[0]
    assert req.method == "DELETE"
    assert req.url.path == "/api/v1/tokens/01234567"
    assert req.headers["X-Loom-Admin-Actor"] == "qianyi"
    assert "01234567" in capsys.readouterr().out


def test_team_revoke_rejects_non_hex_prefix(
    _team_logged_in: None, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main([
        "admin", "tokens", "team", "revoke", "deadbeefX",
    ])
    assert rc == 2
    assert "hex" in capsys.readouterr().err.lower()


def test_team_revoke_rejects_wrong_length_prefix(
    _team_logged_in: None, capsys: pytest.CaptureFixture[str],
) -> None:
    """Server requires exactly 8 hex chars; reject 7 + 9 client-side."""
    for bad in ("abcdef0", "abcdef012"):
        rc = main([
            "admin", "tokens", "team", "revoke", bad,
        ])
        assert rc == 2


def test_team_revoke_uppercase_hex_rejected(
    _team_logged_in: None, capsys: pytest.CaptureFixture[str],
) -> None:
    """Server's check is lowercase hex; client matches."""
    rc = main([
        "admin", "tokens", "team", "revoke", "ABCDEF01",
    ])
    assert rc == 2


def test_team_revoke_404_returns_1(
    mock_server: _MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[
        ("DELETE", "/api/v1/tokens/01234567")
    ] = httpx.Response(404, json={"detail": "token not found"})
    rc = main([
        "admin", "tokens", "team", "revoke", "01234567",
    ])
    assert rc == 1


# ── team rotate ──────────────────────────────────────────────────────


def test_team_rotate_mints_then_prints_checklist(
    mock_server: _MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("POST", "/api/v1/tokens")] = httpx.Response(
        201, json={
            "token": "loom_team_new",
            "token_hash_prefix": "newpref0",
            "expires_at": "2026-09-01T00:00:00+00:00",
        },
    )
    rc = main([
        "admin", "tokens", "team", "rotate",
        "--name", "rotated-token",
        "--team-id", _TEAM_ID,
    ])
    assert rc == 0
    # Only ONE request — rotate does not auto-DELETE.
    assert len(mock_server.requests) == 1
    out = capsys.readouterr().out
    assert "newpref0" in out
    assert "Rotation checklist" in out
    assert "secure channel" in out
    assert "loom admin tokens team revoke" in out


def test_team_rotate_mint_failure_propagates(
    mock_server: _MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("POST", "/api/v1/tokens")] = httpx.Response(
        403, json={"detail": "X-Loom-Admin-Actor required"},
    )
    rc = main([
        "admin", "tokens", "team", "rotate",
        "--name", "blocked-rotate-token",
        "--team-id", _TEAM_ID,
    ])
    assert rc == 1
    out = capsys.readouterr().out
    assert "Rotation checklist" not in out


def test_team_rotate_json_skips_checklist(
    mock_server: _MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("POST", "/api/v1/tokens")] = httpx.Response(
        201, json={
            "token": "t", "token_hash_prefix": "ab123456",
            "expires_at": "2026-09-01T00:00:00+00:00",
        },
    )
    rc = main([
        "admin", "tokens", "team", "rotate",
        "--name", "json-rotate-token",
        "--team-id", _TEAM_ID, "--format", "json",
    ])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Rotation checklist" not in out
