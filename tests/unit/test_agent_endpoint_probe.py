"""Unit tests for `loom.agent.endpoint_probe.responses_api_supported`.

The probe answers "does POST {base_url}/responses appear to route to a
real Responses API implementation" — used by SubprocessAgent to pick
`wire_api` for the codex adapter before spawning the codex CLI (#277).

Interpretation:
- 200 / 400 / 401 → endpoint exists (real handler; our stub payload is
  either accepted, validation-rejected, or auth-rejected)
- 404 / 501 → endpoint definitely absent
- 5xx / timeout / connect error → assume absent (fail closed — better
  to fall back to /chat/completions than to hang codex for 40 min)
- Cache: probe runs at most once per base_url per process lifetime.
"""

from __future__ import annotations

import httpx
import pytest

from loom.agent.endpoint_probe import (
    _PROBE_CACHE,
    responses_api_supported,
)


@pytest.fixture(autouse=True)
def _reset_probe_cache() -> None:
    _PROBE_CACHE.clear()
    yield
    _PROBE_CACHE.clear()


def _mock_transport(status_code: int) -> httpx.MockTransport:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json={"ok": status_code == 200})
    return httpx.MockTransport(handler)


def _always_raises(exc: type[BaseException]) -> httpx.MockTransport:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise exc("simulated")
    return httpx.MockTransport(handler)


@pytest.mark.parametrize("code", [200, 400, 401])
async def test_supported_status_codes_return_true(code: int) -> None:
    result = await responses_api_supported(
        base_url="http://gw.example/openai/v1",
        token="tok",
        model="m",
        transport=_mock_transport(code),
    )
    assert result is True


@pytest.mark.parametrize("code", [404, 501, 502, 503, 504])
async def test_unsupported_status_codes_return_false(code: int) -> None:
    result = await responses_api_supported(
        base_url="http://gw.example/openai/v1",
        token="tok",
        model="m",
        transport=_mock_transport(code),
    )
    assert result is False


async def test_timeout_returns_false() -> None:
    result = await responses_api_supported(
        base_url="http://gw.example/openai/v1",
        token="tok",
        model="m",
        transport=_always_raises(httpx.ReadTimeout),
    )
    assert result is False


async def test_connect_error_returns_false() -> None:
    result = await responses_api_supported(
        base_url="http://gw.example/openai/v1",
        token="tok",
        model="m",
        transport=_always_raises(httpx.ConnectError),
    )
    assert result is False


async def test_cache_prevents_second_probe() -> None:
    call_count = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(200)

    transport = httpx.MockTransport(handler)
    await responses_api_supported(
        base_url="http://gw.example/openai/v1",
        token="tok",
        model="m",
        transport=transport,
    )
    await responses_api_supported(
        base_url="http://gw.example/openai/v1",
        token="different-tok",
        model="m2",
        transport=transport,
    )
    assert call_count == 1  # only the first call reached the transport


async def test_different_base_urls_are_probed_independently() -> None:
    call_count = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(200)

    transport = httpx.MockTransport(handler)
    await responses_api_supported(
        base_url="http://gw.example/openai/v1",
        token="tok",
        model="m",
        transport=transport,
    )
    await responses_api_supported(
        base_url="http://other.example/openai/v1",
        token="tok",
        model="m",
        transport=transport,
    )
    assert call_count == 2


async def test_probe_posts_to_responses_endpoint() -> None:
    captured: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(400)

    transport = httpx.MockTransport(handler)
    await responses_api_supported(
        base_url="http://gw.example/openai/v1",
        token="tok-xyz",
        model="glm-5.1-thinking",
        transport=transport,
    )
    assert len(captured) == 1
    req = captured[0]
    assert req.method == "POST"
    assert req.url.path.endswith("/responses")
    assert req.headers["Authorization"] == "Bearer tok-xyz"
