"""`loom admin tokens worker {mint, revoke, rotate}` tests (#80)."""

from __future__ import annotations

import io
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
    import json as _json
    parsed = _json.loads(out)
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
