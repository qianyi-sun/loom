"""Tiny dependency-free health server for the standalone process."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest


async def _handle_health(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    healthy: Callable[[], bool],
) -> None:
    try:
        request_line = await asyncio.wait_for(reader.readline(), timeout=2.0)
        path = request_line.split(b" ", 2)[1] if b" " in request_line else b""
        if path == b"/metrics":
            status = b"200 OK"
            content_type = CONTENT_TYPE_LATEST.encode("ascii")
            body = generate_latest()
        else:
            ok = path == b"/healthz" and healthy()
            status = b"200 OK" if ok else b"503 Service Unavailable"
            content_type = b"application/json"
            body = b'{"status":"ok"}\n' if ok else b'{"status":"unavailable"}\n'
        writer.write(
            b"HTTP/1.1 "
            + status
            + b"\r\nContent-Type: "
            + content_type
            + b"\r\nConnection: close\r\nContent-Length: "
            + str(len(body)).encode()
            + b"\r\n\r\n"
            + body
        )
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def start_health_server(
    *, host: str, port: int, healthy: Callable[[], bool]
) -> asyncio.AbstractServer:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await _handle_health(reader, writer, healthy)

    return await asyncio.start_server(handler, host=host, port=port)
