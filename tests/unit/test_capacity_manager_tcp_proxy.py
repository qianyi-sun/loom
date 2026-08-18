from __future__ import annotations

import asyncio
import ipaddress

from loom_capacity_manager.tcp_proxy import start_tcp_proxy


def test_tcp_proxy_preserves_bidirectional_bytes_and_half_close() -> None:
    async def scenario() -> None:
        async def upstream(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            payload = await reader.read()
            writer.write(b"reply:" + payload)
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        upstream_server = await asyncio.start_server(upstream, "127.0.0.1", 0)
        upstream_port = int(upstream_server.sockets[0].getsockname()[1])
        proxy = await start_tcp_proxy(
            listen_host="127.0.0.1",
            listen_port=0,
            upstream_host="127.0.0.1",
            upstream_port=upstream_port,
            allowed_client_ips=frozenset({ipaddress.ip_address("127.0.0.1")}),
        )
        proxy_port = int(proxy.sockets[0].getsockname()[1])
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            writer.write(b"opaque-tls-records")
            await writer.drain()
            writer.write_eof()
            assert await asyncio.wait_for(reader.read(), timeout=2) == (b"reply:opaque-tls-records")
            writer.close()
            await writer.wait_closed()
        finally:
            proxy.close()
            upstream_server.close()
            await proxy.wait_closed()
            await upstream_server.wait_closed()

    asyncio.run(scenario())


def test_tcp_proxy_rejects_unreviewed_client_before_upstream_connection() -> None:
    async def scenario() -> None:
        upstream_connected = asyncio.Event()

        async def upstream(
            _reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            upstream_connected.set()
            writer.close()
            await writer.wait_closed()

        upstream_server = await asyncio.start_server(upstream, "127.0.0.1", 0)
        upstream_port = int(upstream_server.sockets[0].getsockname()[1])
        proxy = await start_tcp_proxy(
            listen_host="127.0.0.1",
            listen_port=0,
            upstream_host="127.0.0.1",
            upstream_port=upstream_port,
            allowed_client_ips=frozenset({ipaddress.ip_address("127.0.0.2")}),
        )
        proxy_port = int(proxy.sockets[0].getsockname()[1])
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            writer.write(b"must-not-reach-manager")
            await writer.drain()
            try:
                assert await asyncio.wait_for(reader.read(), timeout=2) == b""
            except ConnectionResetError:
                pass
            assert not upstream_connected.is_set()
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionResetError:
                pass
        finally:
            proxy.close()
            upstream_server.close()
            await proxy.wait_closed()
            await upstream_server.wait_closed()

    asyncio.run(scenario())
