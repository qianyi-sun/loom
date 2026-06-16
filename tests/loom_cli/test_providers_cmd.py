"""`loom providers {create,list,show,update,delete}` against a mocked
server (httpx MockTransport). The route layer's behavior is exercised
by `tests/integration/test_provider_connections_routes.py`; this file
exercises the CLI surface: argparse, payload shape, output format,
error paths."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from loom_cli.__main__ import main


@pytest.fixture(autouse=True)
def _isolated_logged_in_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Isolated config + already-logged-in. Tests that want to test the
    not-logged-in path clear via `loom auth logout`."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    monkeypatch.setenv("MY_TOK", "loom_admin_test123456")
    main([
        "auth", "login",
        "--server", "https://loom.test",
        "--token", "env:MY_TOK",
    ])


class MockServer:
    """Wraps the MockTransport state so tests can both adjust canned
    responses (via .canned[(method, path)] = httpx.Response(...)) AND
    inspect the recorded requests (via iterating or indexing)."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.canned: dict[tuple[str, str], httpx.Response] = {}

    def __getitem__(self, idx: int) -> httpx.Request:
        return self.requests[idx]

    def __len__(self) -> int:
        return len(self.requests)

    def __eq__(self, other: object) -> bool:
        return self.requests == other


@pytest.fixture
def mock_server(
    monkeypatch: pytest.MonkeyPatch,
) -> MockServer:
    """Patch `loom_cli.providers_cmd.authed_client` to return an
    httpx.Client backed by a MockTransport. Returned MockServer records
    every outgoing request + lets tests inject canned responses."""
    server = MockServer()

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
        "loom_cli.providers_cmd.authed_client", _patched_authed_client,
    )
    return server


def _make_connection(
    *, name: str = "openai-prod", type_: str = "openai-compatible",
    pricing_source: str = "tokens-only",
    pricing_data: dict | None = None,
    status: str = "pending",
    allowed_models: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "id": "00000000-0000-0000-0000-000000000001",
        "team_id": "00000000-0000-0000-0000-000000000000",
        "name": name,
        "type": type_,
        "base_url": "https://api.openai.com/v1",
        "upstream_host": "api.openai.com",
        "resolved_egress_ips": ["104.18.0.1"],
        "allowed_models": allowed_models,
        "status": status,
        "last_validated_at": None,
        "last_validation_error": None,
        "pricing_source": pricing_source,
        "pricing_data": pricing_data,
        "created_by": "admin:abc12345",
        "created_at": "2026-06-16T00:00:00Z",
        "updated_at": "2026-06-16T00:00:00Z",
    }


# ──────────────────────────────────────────────────────────────────────
# create
# ──────────────────────────────────────────────────────────────────────


def test_create_happy_path_posts_correct_payload(
    monkeypatch: pytest.MonkeyPatch,
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("OPENAI_KEY", "sk-test-XXXX")
    mock_server.canned[("POST", "/api/v1/provider-connections")] = httpx.Response(
        201, json=_make_connection(name="openai-prod"),
    )

    rc = main([
        "providers", "create",
        "--name", "openai-prod",
        "--type", "openai-compatible",
        "--base-url", "https://api.openai.com/v1",
        "--api-key", "env:OPENAI_KEY",
    ])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Created provider connection 'openai-prod'" in out
    assert "name:          openai-prod" in out

    # Verify the request shape.
    assert len(mock_server) == 1
    req = mock_server[0]
    assert req.method == "POST"
    body = json.loads(req.content)
    assert body == {
        "name": "openai-prod",
        "type": "openai-compatible",
        "base_url": "https://api.openai.com/v1",
        "api_key": "sk-test-XXXX",  # resolved from env
    }
    # Authorization header carried.
    assert req.headers["Authorization"] == "Bearer loom_admin_test123456"


def test_create_with_operator_pricing_sends_pricing_data(
    monkeypatch: pytest.MonkeyPatch,
    mock_server: MockServer,
) -> None:
    monkeypatch.setenv("K", "k123")
    mock_server.canned[("POST", "/api/v1/provider-connections")] = httpx.Response(
        201, json=_make_connection(
            pricing_source="operator-supplied",
            pricing_data={"input_usd_per_1m": 2.5, "output_usd_per_1m": 10.0},
        ),
    )
    rc = main([
        "providers", "create",
        "--name", "n", "--type", "openai-compatible",
        "--base-url", "https://api.openai.com/v1",
        "--api-key", "env:K",
        "--input-usd-per-1m", "2.5",
        "--output-usd-per-1m", "10.0",
    ])
    assert rc == 0
    body = json.loads(mock_server[0].content)
    assert body["pricing_source"] == "operator-supplied"
    assert body["pricing_data"] == {
        "input_usd_per_1m": 2.5,
        "output_usd_per_1m": 10.0,
    }


def test_create_half_set_pricing_rejected(
    monkeypatch: pytest.MonkeyPatch,
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--input-usd-per-1m without --output-usd-per-1m must fail at the
    CLI layer, BEFORE the HTTP call goes out."""
    monkeypatch.setenv("K", "k123")
    rc = main([
        "providers", "create",
        "--name", "n", "--type", "openai-compatible",
        "--base-url", "https://api.openai.com/v1",
        "--api-key", "env:K",
        "--input-usd-per-1m", "2.5",  # but no --output-usd-per-1m
    ])
    assert rc == 2
    assert "interdependent" in capsys.readouterr().err
    # No HTTP call was made.
    assert mock_server == []


def test_create_api_key_literal_rejected_at_argparse(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc:
        main([
            "providers", "create",
            "--name", "n", "--type", "openai-compatible",
            "--base-url", "https://api.openai.com/v1",
            "--api-key", "sk-literal",  # rejected
        ])
    assert exc.value.code == 2
    assert "literal values are rejected" in capsys.readouterr().err


def test_create_server_error_surfaces_detail(
    monkeypatch: pytest.MonkeyPatch,
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("K", "k1234567")
    mock_server.canned[("POST", "/api/v1/provider-connections")] = httpx.Response(
        409, json={"detail": "a provider_connection named 'n' already exists"},
    )
    rc = main([
        "providers", "create",
        "--name", "n", "--type", "openai-compatible",
        "--base-url", "https://api.openai.com/v1",
        "--api-key", "env:K",
    ])
    assert rc == 1
    err = capsys.readouterr().err
    assert "HTTP 409" in err
    assert "already exists" in err


# ──────────────────────────────────────────────────────────────────────
# list
# ──────────────────────────────────────────────────────────────────────


def test_list_empty(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": []},
    )
    rc = main(["providers", "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "no provider connections" in out


def test_list_table_format(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [
            _make_connection(name="openai-prod"),
            _make_connection(name="anthropic-dev", type_="anthropic",
                             pricing_source="rate-card"),
        ]},
    )
    rc = main(["providers", "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "openai-prod" in out
    assert "anthropic-dev" in out
    assert "openai-compatible" in out
    assert "anthropic" in out


def test_list_json_format(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    items = [_make_connection(name="x")]
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": items},
    )
    rc = main(["providers", "list", "--format", "json"])
    assert rc == 0
    parsed = json.loads(capsys.readouterr().out)
    assert parsed == items


# ──────────────────────────────────────────────────────────────────────
# show / update / delete — exercise the _resolve_by_name lookup path
# ──────────────────────────────────────────────────────────────────────


def test_show_resolves_by_name_then_prints(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    rc = main(["providers", "show", "openai-prod"])
    assert rc == 0
    assert "name:          openai-prod" in capsys.readouterr().out


def test_show_unknown_name_returns_1_with_helpful_message(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": []},
    )
    rc = main(["providers", "show", "nope"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "no provider connection named 'nope'" in err
    assert "loom providers list" in err


def test_update_patches_by_resolved_id(
    monkeypatch: pytest.MonkeyPatch,
    mock_server: MockServer,
) -> None:
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("PATCH", f"/api/v1/provider-connections/{conn['id']}")
    ] = httpx.Response(200, json=conn)

    rc = main([
        "providers", "update", "openai-prod",
        "--base-url", "https://api.openai.com/v2",
    ])
    assert rc == 0
    # First call is the GET (resolution), second is the PATCH.
    assert mock_server[0].method == "GET"
    assert mock_server[1].method == "PATCH"
    patch_body = json.loads(mock_server[1].content)
    assert patch_body == {"base_url": "https://api.openai.com/v2"}


def test_update_with_no_changes_errors(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["providers", "update", "n"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "requires at least one of" in err


def test_update_api_key_resolves_via_source(
    monkeypatch: pytest.MonkeyPatch,
    mock_server: MockServer,
    tmp_path: Path,
) -> None:
    key_file = tmp_path / "new-key.txt"
    key_file.write_text("new-secret-key\n")
    conn = _make_connection(name="x")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("PATCH", f"/api/v1/provider-connections/{conn['id']}")
    ] = httpx.Response(200, json=conn)
    rc = main([
        "providers", "update", "x",
        "--api-key", f"file:{key_file}",
    ])
    assert rc == 0
    patch_body = json.loads(mock_server[1].content)
    assert patch_body == {"api_key": "new-secret-key"}


def test_delete_resolves_then_calls_delete(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("DELETE", f"/api/v1/provider-connections/{conn['id']}")
    ] = httpx.Response(204)
    rc = main(["providers", "delete", "openai-prod"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Soft-deleted provider connection 'openai-prod'" in out
    assert mock_server[1].method == "DELETE"


# ──────────────────────────────────────────────────────────────────────
# test
# ──────────────────────────────────────────────────────────────────────


def test_test_valid_returns_0(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    """A valid probe response → rc=0 with status printed."""
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("POST", f"/api/v1/provider-connections/{conn['id']}/test")
    ] = httpx.Response(200, json={
        "connection_id": conn["id"],
        "status": "valid",
        "http_status": 200,
        "last_validation_error": None,
        "last_validated_at": "2026-06-16T12:00:00Z",
    })
    rc = main(["providers", "test", "openai-prod"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "status:                valid" in out
    assert "http_status:           200" in out
    # 1 GET (resolution) + 1 POST (test).
    assert mock_server[1].method == "POST"
    assert mock_server[1].url.path == (
        f"/api/v1/provider-connections/{conn['id']}/test"
    )


def test_test_invalid_returns_1_and_prints_error(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    """`invalid` status → rc=1 + error printed; CI-greppable."""
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("POST", f"/api/v1/provider-connections/{conn['id']}/test")
    ] = httpx.Response(200, json={
        "connection_id": conn["id"],
        "status": "invalid",
        "http_status": 401,
        "last_validation_error": "HTTP 401 from .../models; body excerpt: 'invalid api key'",
        "last_validated_at": "2026-06-16T12:00:00Z",
    })
    rc = main(["providers", "test", "openai-prod"])
    assert rc == 1
    out = capsys.readouterr().out
    assert "status:                invalid" in out
    assert "http_status:           401" in out
    assert "invalid api key" in out


def test_test_unreachable_omits_http_status(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    """When the probe never reached an HTTP server, http_status=None;
    rendering should NOT print a `http_status:` line."""
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("POST", f"/api/v1/provider-connections/{conn['id']}/test")
    ] = httpx.Response(200, json={
        "connection_id": conn["id"],
        "status": "invalid",
        "http_status": None,
        "last_validation_error": "timeout after 5.0s",
        "last_validated_at": "2026-06-16T12:00:00Z",
    })
    rc = main(["providers", "test", "openai-prod"])
    assert rc == 1
    out = capsys.readouterr().out
    assert "http_status:" not in out
    assert "timeout" in out


def test_test_unknown_name_returns_1(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": []},
    )
    rc = main(["providers", "test", "nope"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "no provider connection named 'nope'" in err


# ──────────────────────────────────────────────────────────────────────
# not-logged-in path
# ──────────────────────────────────────────────────────────────────────


def test_create_not_logged_in_returns_2(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Clear auth via logout.
    main(["auth", "logout"])
    capsys.readouterr()  # drain logout output

    rc = main([
        "providers", "create",
        "--name", "n", "--type", "openai-compatible",
        "--base-url", "https://api.openai.com/v1",
        "--api-key", "env:NEVER_SET",
    ])
    assert rc == 2
    err = capsys.readouterr().err
    assert "not logged in" in err
    assert "loom auth login" in err
