"""Tests for `loom_llm_gateway.responses_probe`.

Two exported entry points:

- `classify_probe_status(status_code)` — pure classifier. 200/400/401 →
  supported; 404/501/5xx → unsupported; other 4xx → unknown (surfaces
  to caller as "keep the existing behaviour, don't dispatch to translator").
- `probe_responses_api(upstream_url, api_key, transport)` — runs one
  outbound POST with an empty JSON body; returns
  `(supported: bool | None, error_detail: str | None)`.

Spec: docs/architecture/responses-api-support-probe.md
"""
from __future__ import annotations

import httpx
import pytest

from loom_llm_gateway.responses_probe import (
    ProbeOutcome,
    classify_probe_status,
    probe_responses_api,
)


@pytest.mark.parametrize("code", [200, 400, 401])
def test_classify_supported_codes(code: int) -> None:
    assert classify_probe_status(code) is True


@pytest.mark.parametrize("code", [404, 501, 500, 502, 503, 504])
def test_classify_unsupported_codes(code: int) -> None:
    assert classify_probe_status(code) is False


@pytest.mark.parametrize("code", [403, 429, 422])
def test_classify_ambiguous_codes_return_none(code: int) -> None:
    """4xx that isn't 400/401/404 — the endpoint exists but our probe
    can't tell whether it's a real Responses handler or a proxy that
    rejected the request for policy reasons. Leave as None so the
    caller decides whether to retry or fall back."""
    assert classify_probe_status(code) is None


def _mock_transport(status: int) -> httpx.MockTransport:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json={})
    return httpx.MockTransport(handler)


def _raising_transport(exc: type[BaseException]) -> httpx.MockTransport:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise exc("simulated")
    return httpx.MockTransport(handler)


async def test_probe_returns_supported_on_400() -> None:
    outcome = await probe_responses_api(
        upstream_url="http://mock.example/v1/responses",
        api_key="tok",
        transport=_mock_transport(400),
    )
    assert outcome == ProbeOutcome(supported=True, error_detail=None)


async def test_probe_returns_unsupported_on_504() -> None:
    outcome = await probe_responses_api(
        upstream_url="http://mock.example/v1/responses",
        api_key="tok",
        transport=_mock_transport(504),
    )
    assert outcome == ProbeOutcome(supported=False, error_detail="upstream_504")


async def test_probe_returns_unsupported_on_timeout() -> None:
    outcome = await probe_responses_api(
        upstream_url="http://mock.example/v1/responses",
        api_key="tok",
        transport=_raising_transport(httpx.ReadTimeout),
    )
    assert outcome.supported is False
    assert outcome.error_detail is not None
    assert "timeout" in outcome.error_detail.lower()


async def test_probe_returns_unsupported_on_connect_error() -> None:
    outcome = await probe_responses_api(
        upstream_url="http://mock.example/v1/responses",
        api_key="tok",
        transport=_raising_transport(httpx.ConnectError),
    )
    assert outcome.supported is False
    assert outcome.error_detail is not None


async def test_probe_returns_none_on_ambiguous_4xx() -> None:
    outcome = await probe_responses_api(
        upstream_url="http://mock.example/v1/responses",
        api_key="tok",
        transport=_mock_transport(403),
    )
    assert outcome.supported is None
    assert outcome.error_detail == "ambiguous_403"


async def test_probe_posts_empty_body_to_upstream() -> None:
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["auth"] = request.headers.get("Authorization")
        captured["body"] = request.content
        return httpx.Response(400)

    transport = httpx.MockTransport(handler)
    await probe_responses_api(
        upstream_url="http://mock.example/v1/responses",
        api_key="tok-xyz",
        transport=transport,
    )
    assert captured["method"] == "POST"
    assert captured["url"] == "http://mock.example/v1/responses"
    assert captured["auth"] == "Bearer tok-xyz"
    # Body is a well-formed JSON object with no model reference — the
    # payload cannot be interpreted by the upstream, forcing a routing
    # decision (400 = endpoint present, 404 = absent).
    assert captured["body"] == b"{}"
