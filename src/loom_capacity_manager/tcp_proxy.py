"""Private, allowlisted TCP pass-through for off-cluster capacity controllers."""

from __future__ import annotations

import argparse
import asyncio
import ipaddress
from collections.abc import Sequence

_LISTEN_HOST = "0.0.0.0"
_LISTEN_PORT = 31443
_UPSTREAM_HOST = "loom-capacity-manager.loom-dev.svc.cluster.local"
_UPSTREAM_PORT = 8443
_CONNECT_TIMEOUT_SECONDS = 5.0
_BUFFER_BYTES = 64 * 1024
_PRIVATE_IPV4_NETWORKS = tuple(
    ipaddress.ip_network(value) for value in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
)
_PRIVATE_IPV6_NETWORK = ipaddress.ip_network("fc00::/7")

IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address


def _is_private_address(address: IPAddress) -> bool:
    if isinstance(address, ipaddress.IPv4Address):
        return any(address in network for network in _PRIVATE_IPV4_NETWORKS)
    return address in _PRIVATE_IPV6_NETWORK


async def _close_writer(writer: asyncio.StreamWriter) -> None:
    writer.close()
    try:
        await writer.wait_closed()
    except (ConnectionError, OSError):
        pass


async def _relay(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    while payload := await reader.read(_BUFFER_BYTES):
        writer.write(payload)
        await writer.drain()
    try:
        writer.write_eof()
        await writer.drain()
    except (ConnectionError, NotImplementedError, OSError):
        pass


async def _proxy_connection(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    *,
    upstream_host: str,
    upstream_port: int,
    allowed_client_ips: frozenset[IPAddress],
) -> None:
    peer = client_writer.get_extra_info("peername")
    try:
        peer_address = ipaddress.ip_address(peer[0])
    except (IndexError, TypeError, ValueError):
        await _close_writer(client_writer)
        return
    if peer_address not in allowed_client_ips:
        await _close_writer(client_writer)
        return
    try:
        upstream_reader, upstream_writer = await asyncio.wait_for(
            asyncio.open_connection(upstream_host, upstream_port),
            timeout=_CONNECT_TIMEOUT_SECONDS,
        )
    except (TimeoutError, ConnectionError, OSError):
        await _close_writer(client_writer)
        return
    relays = (
        asyncio.create_task(_relay(client_reader, upstream_writer)),
        asyncio.create_task(_relay(upstream_reader, client_writer)),
    )
    try:
        await asyncio.gather(*relays)
    except (ConnectionError, OSError):
        pass
    finally:
        for relay in relays:
            relay.cancel()
        await asyncio.gather(*relays, return_exceptions=True)
        await _close_writer(upstream_writer)
        await _close_writer(client_writer)


async def start_tcp_proxy(
    *,
    listen_host: str,
    listen_port: int,
    upstream_host: str,
    upstream_port: int,
    allowed_client_ips: frozenset[IPAddress],
) -> asyncio.AbstractServer:
    """Start one byte-transparent proxy server with exact peer admission."""

    if (
        not isinstance(listen_host, str)
        or not listen_host
        or type(listen_port) is not int
        or not 0 <= listen_port <= 65535
        or not isinstance(upstream_host, str)
        or not upstream_host
        or type(upstream_port) is not int
        or not 1 <= upstream_port <= 65535
        or not isinstance(allowed_client_ips, frozenset)
        or not allowed_client_ips
        or len(allowed_client_ips) > 8
        or not all(
            isinstance(address, (ipaddress.IPv4Address, ipaddress.IPv6Address))
            for address in allowed_client_ips
        )
    ):
        raise ValueError("TCP proxy configuration is invalid")

    async def handle(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        await _proxy_connection(
            reader,
            writer,
            upstream_host=upstream_host,
            upstream_port=upstream_port,
            allowed_client_ips=allowed_client_ips,
        )

    return await asyncio.start_server(handle, listen_host, listen_port, backlog=128)


def _private_client_ip(value: str) -> IPAddress:
    try:
        address = ipaddress.ip_address(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("allowed client IP is invalid") from exc
    if value != address.compressed or not _is_private_address(address):
        raise argparse.ArgumentTypeError("allowed client IP must be canonical and private")
    return address


async def _serve(allowed_client_ips: frozenset[IPAddress]) -> None:
    server = await start_tcp_proxy(
        listen_host=_LISTEN_HOST,
        listen_port=_LISTEN_PORT,
        upstream_host=_UPSTREAM_HOST,
        upstream_port=_UPSTREAM_PORT,
        allowed_client_ips=allowed_client_ips,
    )
    async with server:
        await server.serve_forever()


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the private capacity-manager TCP router")
    parser.add_argument(
        "--allowed-client-ip",
        action="append",
        required=True,
        type=_private_client_ip,
    )
    arguments = parser.parse_args(argv)
    allowed_client_ips = frozenset(arguments.allowed_client_ip)
    if len(allowed_client_ips) != len(arguments.allowed_client_ip):
        parser.error("allowed client IPs must be unique")
    try:
        asyncio.run(_serve(allowed_client_ips))
    except KeyboardInterrupt:  # pragma: no cover - process shutdown
        pass


if __name__ == "__main__":  # pragma: no cover - module entry point
    main()


__all__ = ["main", "start_tcp_proxy"]
