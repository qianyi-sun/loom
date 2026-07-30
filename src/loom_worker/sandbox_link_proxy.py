"""Compose-private plaintext to candidate-bound mTLS data-plane proxy."""

from __future__ import annotations

import argparse
import asyncio
import http.client
import os
import re
import ssl
import stat
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from urllib.parse import urlsplit

SHA_RE = re.compile(r"^[0-9a-f]{40}$")
RUNTIME_ID_RE = re.compile(r"^[a-z][a-z0-9-]{1,31}$")
PORT_RE = re.compile(r"^[1-9][0-9]{3,4}$")
SERVER_ADDRESS = "192.168.50.14"
TLS_ROOT = Path("/run/loom-sandbox-link")
TLS_FILES = (TLS_ROOT / "ca.pem", TLS_ROOT / "client.pem", TLS_ROOT / "client-key.pem")


class SandboxLinkError(RuntimeError):
    """The local link cannot satisfy its closed data-plane contract."""


@dataclass(frozen=True, slots=True)
class Link:
    name: str
    local_port: int
    upstream_port: int
    health_path: str
    allow_empty_health: bool


def _secure_file(path: Path, *, private: bool) -> Path:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise SandboxLinkError("sandbox link TLS material is unavailable") from exc
    if (
        not path.is_absolute()
        or ".." in path.parts
        or not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or (metadata.st_mode & 0o022)
        or (private and metadata.st_mode & 0o077)
    ):
        raise SandboxLinkError("sandbox link TLS material is unsafe")
    return path.resolve(strict=True)


def load_links(environ: dict[str, str] | None = None) -> tuple[Link, ...]:
    env = os.environ if environ is None else environ
    sandbox = env.get("LOOM_WORKER_SANDBOX_IDENTITY", "")
    candidate_sha = env.get("LOOM_WORKER_CANDIDATE_SHA", "")
    if RUNTIME_ID_RE.fullmatch(sandbox) is None or SHA_RE.fullmatch(candidate_sha) is None:
        raise SandboxLinkError("sandbox link identity is invalid")
    definitions = (
        (
            "control-plane",
            "LOOM_SANDBOX_LINK_CP_UPSTREAM",
            "LOOM_SANDBOX_LINK_CP_EXPECTED_PORT",
            8080,
            "/healthz",
            False,
        ),
        (
            "gateway",
            "LOOM_SANDBOX_LINK_GATEWAY_UPSTREAM",
            "LOOM_SANDBOX_LINK_GATEWAY_EXPECTED_PORT",
            9100,
            "/healthz",
            False,
        ),
        (
            "minio",
            "LOOM_SANDBOX_LINK_MINIO_UPSTREAM",
            "LOOM_SANDBOX_LINK_MINIO_EXPECTED_PORT",
            9000,
            "/minio/health/live",
            True,
        ),
    )
    links: list[Link] = []
    expected_ports: set[int] = set()
    for definition in definitions:
        name, env_key, expected_port_key, local_port, health_path, allow_empty = definition
        raw_expected_port = env.get(expected_port_key, "")
        if PORT_RE.fullmatch(raw_expected_port) is None:
            raise SandboxLinkError(f"{name} expected port is invalid")
        expected_port = int(raw_expected_port)
        if not 1024 <= expected_port <= 65535 or expected_port in expected_ports:
            raise SandboxLinkError(f"{name} expected port is invalid")
        expected_ports.add(expected_port)
        parsed = urlsplit(env.get(env_key, ""))
        if (
            parsed.scheme != "https"
            or parsed.hostname != SERVER_ADDRESS
            or parsed.port != expected_port
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise SandboxLinkError(f"{name} upstream is not candidate-bound")
        links.append(
            Link(
                name=name,
                local_port=local_port,
                upstream_port=expected_port,
                health_path=health_path,
                allow_empty_health=allow_empty,
            ),
        )
    return tuple(links)


def build_client_context() -> ssl.SSLContext:
    ca, cert, key = TLS_FILES
    context = ssl.create_default_context(cafile=str(_secure_file(ca, private=False)))
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.maximum_version = ssl.TLSVersion.TLSv1_3
    context.load_cert_chain(
        certfile=str(_secure_file(cert, private=False)),
        keyfile=str(_secure_file(key, private=True)),
    )
    return context


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while chunk := await reader.read(64 * 1024):
            writer.write(chunk)
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass
    finally:
        writer.close()


async def _handle(
    link: Link,
    context: ssl.SSLContext,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    try:
        upstream_reader, upstream_writer = await asyncio.open_connection(
            SERVER_ADDRESS,
            link.upstream_port,
            ssl=context,
            server_hostname=SERVER_ADDRESS,
            ssl_handshake_timeout=5.0,
        )
    except (OSError, ssl.SSLError):
        writer.close()
        await writer.wait_closed()
        return
    await asyncio.gather(
        _pipe(reader, upstream_writer),
        _pipe(upstream_reader, writer),
    )


def check(links: tuple[Link, ...], context: ssl.SSLContext) -> None:
    for link in links:
        connection = http.client.HTTPSConnection(
            SERVER_ADDRESS,
            link.upstream_port,
            timeout=5.0,
            context=context,
        )
        try:
            connection.request("GET", link.health_path)
            response = connection.getresponse()
            body = response.read(4096)
        except (OSError, ssl.SSLError, http.client.HTTPException) as exc:
            raise SandboxLinkError(f"{link.name} health probe failed") from exc
        finally:
            connection.close()
        if not 200 <= response.status < 300 or (not body and not link.allow_empty_health):
            raise SandboxLinkError(f"{link.name} health probe was unhealthy")


async def serve(links: tuple[Link, ...], context: ssl.SSLContext) -> None:
    servers = [
        await asyncio.start_server(
            partial(_handle, link, context),
            host="0.0.0.0",
            port=link.local_port,
        )
        for link in links
    ]
    try:
        await asyncio.gather(*(server.serve_forever() for server in servers))
    finally:
        for server in servers:
            server.close()
        await asyncio.gather(*(server.wait_closed() for server in servers))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    links = load_links()
    context = build_client_context()
    if args.check:
        check(links, context)
        return 0
    asyncio.run(serve(links, context))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SandboxLinkError as exc:
        raise SystemExit(str(exc)) from None
