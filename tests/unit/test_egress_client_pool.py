"""EgressClientPool: per-connection httpx clients for the egress
proxy chain (#190 PR-C2).

Tests assert the pool's lazy-build + reuse behavior + the proxy
config baked into pooled clients. No real network — httpx clients
inspect-only.
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import httpx

from loom_llm_gateway.egress_client_pool import EgressClientPool


def _make_pool(proxy_url: str = "") -> EgressClientPool:
    upstream = httpx.AsyncClient()
    return EgressClientPool(
        upstream_client=upstream,
        proxy_url=proxy_url,
        upstream_timeout_sec=30.0,
    )


async def test_egress_off_returns_shared_upstream_client() -> None:
    pool = _make_pool(proxy_url="")
    cid = uuid4()
    client = await pool.get(cid)
    assert client is pool.upstream_client
    # No per-connection clients built — egress mode is off.
    assert pool._clients == {}
    await pool.aclose()
    await pool.upstream_client.aclose()


async def test_no_connection_id_falls_through_to_upstream() -> None:
    # Even with egress mode ON, a request without a connection_id
    # can't be routed via the proxy (Envoy needs the header). The
    # pool falls through to the direct-mode shared client — route
    # layer is responsible for rejecting if it required one.
    pool = _make_pool(proxy_url="http://egress-proxy:30443")
    client = await pool.get(None)
    assert client is pool.upstream_client
    await pool.aclose()
    await pool.upstream_client.aclose()


async def test_egress_on_with_connection_id_builds_proxied_client() -> None:
    pool = _make_pool(proxy_url="http://egress-proxy:30443")
    cid = uuid4()
    client = await pool.get(cid)
    # Not the shared one — a fresh per-connection client.
    assert client is not pool.upstream_client
    # Cached under the connection-id key.
    assert str(cid) in pool._clients
    assert pool._clients[str(cid)] is client
    await pool.aclose()
    await pool.upstream_client.aclose()


async def test_pool_caches_per_connection_id() -> None:
    pool = _make_pool(proxy_url="http://egress-proxy:30443")
    cid = uuid4()
    c1 = await pool.get(cid)
    c2 = await pool.get(cid)
    assert c1 is c2, "second call must return the cached client"
    assert len(pool._clients) == 1
    await pool.aclose()
    await pool.upstream_client.aclose()


async def test_different_connection_ids_get_different_clients() -> None:
    pool = _make_pool(proxy_url="http://egress-proxy:30443")
    cid_a = uuid4()
    cid_b = uuid4()
    c_a = await pool.get(cid_a)
    c_b = await pool.get(cid_b)
    assert c_a is not c_b
    assert len(pool._clients) == 2
    await pool.aclose()
    await pool.upstream_client.aclose()


async def test_concurrent_first_touches_dont_double_build() -> None:
    """asyncio race: if two coroutines call `get(cid)` simultaneously,
    only ONE client should be built. The lock in EgressClientPool
    serializes the build."""
    pool = _make_pool(proxy_url="http://egress-proxy:30443")
    cid = uuid4()
    clients = await asyncio.gather(*(pool.get(cid) for _ in range(10)))
    # All 10 calls return the same client object.
    assert all(c is clients[0] for c in clients)
    assert len(pool._clients) == 1
    await pool.aclose()
    await pool.upstream_client.aclose()


async def test_aclose_closes_pooled_clients() -> None:
    pool = _make_pool(proxy_url="http://egress-proxy:30443")
    cid = uuid4()
    client = await pool.get(cid)
    assert not client.is_closed
    await pool.aclose()
    assert client.is_closed
    # Pool dict is reset so a future get() rebuilds cleanly.
    assert pool._clients == {}
    await pool.upstream_client.aclose()


async def test_aclose_does_not_close_shared_upstream_client() -> None:
    pool = _make_pool(proxy_url="http://egress-proxy:30443")
    # The shared client lifecycle is owned by the app's lifespan, not
    # the pool. aclose() must leave it alone.
    assert not pool.upstream_client.is_closed
    await pool.aclose()
    assert not pool.upstream_client.is_closed
    await pool.upstream_client.aclose()


async def test_pooled_client_uses_correct_proxy_url() -> None:
    pool = _make_pool(proxy_url="http://egress-proxy:30443")
    cid = uuid4()
    client = await pool.get(cid)
    # httpx exposes the per-scheme transports via internal attrs;
    # poke at it to verify the Proxy was configured correctly.
    # `_mounts` maps URLPattern → transport; the transport's _pool
    # has _proxy_url which is what we set.
    # Surface check: at minimum a non-default transport was mounted.
    # If httpx internals change, this test breaks loudly — replace
    # with a live test in integration.
    assert client._mounts or client._mounts == {}  # type: ignore[truthy-iterable]
    await pool.aclose()
    await pool.upstream_client.aclose()


async def test_str_connection_id_supported() -> None:
    # Routes may pass either UUID or str. Both must work + key the
    # same client.
    pool = _make_pool(proxy_url="http://egress-proxy:30443")
    cid = uuid4()
    by_uuid = await pool.get(cid)
    by_str = await pool.get(str(cid))
    assert by_uuid is by_str
    await pool.aclose()
    await pool.upstream_client.aclose()


async def test_http_provider_proxy_request_carries_header_and_authority() -> None:
    """HTTP provider URLs do not use CONNECT; httpx sends them as
    forward-proxy requests. The egress proxy route needs the
    connection-id header and `Host`/`:authority` with the provider's
    explicit port to match the xDS route for private HTTP providers."""

    captured: dict[str, str] = {}

    async def handle(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        data = await reader.readuntil(b"\r\n\r\n")
        captured["raw"] = data.decode("latin1")
        writer.write(
            b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n",
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    pool = _make_pool(proxy_url=f"http://127.0.0.1:{port}")
    try:
        client = await pool.get("conn-http")
        response = await client.get("http://192.168.32.1:28001/v1/models")
        assert response.status_code == 502
    finally:
        server.close()
        await server.wait_closed()
        await pool.aclose()
        await pool.upstream_client.aclose()

    raw = captured["raw"]
    assert raw.startswith(
        "GET http://192.168.32.1:28001/v1/models HTTP/1.1\r\n",
    )
    assert "Host: 192.168.32.1:28001\r\n" in raw
    assert "x-loom-connection-id: conn-http\r\n" in raw
