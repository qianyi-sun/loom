"""remote.py — query a connected Loom service when LOOM_SERVER_URL set."""

from __future__ import annotations

import httpx
import pytest

from loom_cli.remote import load_remote_entries


def test_returns_empty_when_no_server_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LOOM_SERVER_URL", raising=False)
    monkeypatch.delenv("LOOM_API_TOKEN", raising=False)
    assert load_remote_entries(server_url=None, token=None) == []


def test_returns_entries_from_service(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "items": [
            {
                "id": "humaneval",
                "display_name": "HumanEval",
                "license_spdx": "MIT",
                "license_url": "https://x.example",
                "splits": ["test"],
            },
            {
                "id": "custom-rl-bench",
                "display_name": "Custom RL Bench",
                "license_spdx": "proprietary",
                "license_url": "",
                "splits": ["train"],
            },
        ],
        "next_cursor": None,
    }

    seen_headers: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen_headers.update(dict(request.headers))
        assert request.url.path == "/api/v1/benchmarks"
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    from loom_cli import remote as rmod
    monkeypatch.setattr(rmod, "_build_client",
                        lambda: httpx.Client(transport=transport))

    entries = load_remote_entries(
        server_url="https://svc.example", token="tok-abc",
    )
    assert seen_headers.get("authorization") == "Bearer tok-abc"
    slugs = [e.slug for e in entries]
    assert slugs == ["custom-rl-bench", "humaneval"]
    for e in entries:
        assert e.source == "remote"
        assert e.status == "remote-only"


def test_service_error_returns_empty_not_raise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    from loom_cli import remote as rmod
    monkeypatch.setattr(rmod, "_build_client",
                        lambda: httpx.Client(transport=transport))

    out = load_remote_entries(server_url="https://svc.example", token="t")
    assert out == []
