#!/usr/bin/env python3
"""Fail-closed mTLS relay for loopback-only developer sandbox services.

The relay is installed as a root-owned systemd service on oldlab-2.  It accepts
only TLS 1.3 clients whose URI SAN names the configured sandbox and the one
active candidate SHA, then forwards the byte stream to that sandbox's exact
loopback Control Plane port.
"""

from __future__ import annotations

import argparse
import asyncio
import re
import ssl
import stat
import tomllib
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any

SHA_RE = re.compile(r"^[0-9a-f]{40}$")
SANDBOX_RE = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
EXPECTED_BIND_ADDRESS = "192.168.50.14"


class RelayError(RuntimeError):
    """The relay configuration or peer identity is unsafe."""


@dataclass(frozen=True, slots=True)
class ServiceConfig:
    name: str
    bind_port: int
    target_port: int
    health_path: str
    allow_empty_health: bool


@dataclass(frozen=True, slots=True)
class RelayConfig:
    sandbox: str
    candidate_sha: str
    bind_address: str
    target_host: str
    ca_file: Path
    cert_file: Path
    key_file: Path
    services: tuple[ServiceConfig, ...]

    @property
    def client_uri_san(self) -> str:
        return (
            f"spiffe://loom/developer-sandbox/{self.sandbox}/candidate/{self.candidate_sha}/worker"
        )


def _secure_file(path: Path, *, private: bool) -> Path:
    if not path.is_absolute() or ".." in path.parts:
        raise RelayError("TLS paths must be absolute and normalized")
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise RelayError("TLS material is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or (metadata.st_mode & 0o022)
    ):
        raise RelayError("TLS material must be a non-writable single-link file")
    if private and stat.S_IMODE(metadata.st_mode) != 0o600:
        raise RelayError("private TLS keys must have mode 0600")
    if private and metadata.st_uid != 0:
        raise RelayError("private TLS keys must be root-owned")
    return path.resolve(strict=True)


def load_config(path: Path) -> RelayConfig:
    try:
        metadata = path.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_uid != 0
            or (metadata.st_mode & 0o077)
        ):
            raise RelayError("relay config must be a root-owned mode 0600 file")
        with path.open("rb") as handle:
            raw: dict[str, Any] = tomllib.load(handle)
    except OSError as exc:
        raise RelayError("relay config is unavailable") from exc
    except tomllib.TOMLDecodeError as exc:
        raise RelayError("relay config is invalid") from exc

    expected = {
        "schema_version",
        "sandbox",
        "candidate_sha",
        "bind_address",
        "target_host",
        "ca_file",
        "cert_file",
        "key_file",
        "services",
    }
    if set(raw) != expected or raw.get("schema_version") != 1:
        raise RelayError("relay config does not match schema version 1")
    sandbox = raw.get("sandbox")
    candidate_sha = raw.get("candidate_sha")
    if not isinstance(sandbox, str) or SANDBOX_RE.fullmatch(sandbox) is None:
        raise RelayError("sandbox identity is invalid")
    if not isinstance(candidate_sha, str) or SHA_RE.fullmatch(candidate_sha) is None:
        raise RelayError("candidate SHA is invalid")
    bind_address = raw.get("bind_address")
    target_host = raw.get("target_host")
    if bind_address != EXPECTED_BIND_ADDRESS:
        raise RelayError("relay must bind only the oldlab-2 private address")
    if target_host != "127.0.0.1":
        raise RelayError("relay target must remain loopback-only")
    raw_services = raw.get("services")
    if not isinstance(raw_services, dict) or set(raw_services) != {
        "control-plane",
        "gateway",
        "minio",
    }:
        raise RelayError("relay services do not match the closed inventory")
    services: list[ServiceConfig] = []
    for name in ("control-plane", "gateway", "minio"):
        service = raw_services.get(name)
        if not isinstance(service, dict) or set(service) != {
            "bind_port",
            "target_port",
            "health_path",
            "allow_empty_health",
        }:
            raise RelayError("relay service config is invalid")
        bind_port = service.get("bind_port")
        target_port = service.get("target_port")
        health_path = service.get("health_path")
        allow_empty = service.get("allow_empty_health")
        if (
            isinstance(bind_port, bool)
            or not isinstance(bind_port, int)
            or not 1024 <= bind_port <= 65535
            or isinstance(target_port, bool)
            or not isinstance(target_port, int)
            or not 1024 <= target_port <= 65535
            or not isinstance(health_path, str)
            or not health_path.startswith("/")
            or type(allow_empty) is not bool
        ):
            raise RelayError("relay service values are invalid")
        services.append(
            ServiceConfig(
                name=name,
                bind_port=bind_port,
                target_port=target_port,
                health_path=health_path,
                allow_empty_health=allow_empty,
            ),
        )
    if len({service.bind_port for service in services}) != len(services):
        raise RelayError("relay listener ports collide")
    if len({service.target_port for service in services}) != len(services):
        raise RelayError("relay target ports collide")

    return RelayConfig(
        sandbox=sandbox,
        candidate_sha=candidate_sha,
        bind_address=bind_address,
        target_host=target_host,
        ca_file=_secure_file(Path(str(raw["ca_file"])), private=False),
        cert_file=_secure_file(Path(str(raw["cert_file"])), private=False),
        key_file=_secure_file(Path(str(raw["key_file"])), private=True),
        services=tuple(services),
    )


def build_server_context(config: RelayConfig) -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.maximum_version = ssl.TLSVersion.TLSv1_3
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_verify_locations(cafile=str(config.ca_file))
    context.load_cert_chain(
        certfile=str(config.cert_file),
        keyfile=str(config.key_file),
    )
    return context


def _peer_uri_sans(writer: asyncio.StreamWriter) -> frozenset[str]:
    ssl_object = writer.get_extra_info("ssl_object")
    if ssl_object is None:
        return frozenset()
    certificate = ssl_object.getpeercert()
    if not isinstance(certificate, dict):
        return frozenset()
    return frozenset(
        str(value) for kind, value in certificate.get("subjectAltName", ()) if kind == "URI"
    )


async def _pipe(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    try:
        while chunk := await reader.read(64 * 1024):
            writer.write(chunk)
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass
    finally:
        writer.close()


async def _handle_peer(
    config: RelayConfig,
    service: ServiceConfig,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    if _peer_uri_sans(writer) != frozenset({config.client_uri_san}):
        writer.close()
        await writer.wait_closed()
        return
    try:
        target_reader, target_writer = await asyncio.open_connection(
            config.target_host,
            service.target_port,
        )
    except OSError:
        writer.close()
        await writer.wait_closed()
        return
    await asyncio.gather(
        _pipe(reader, target_writer),
        _pipe(target_reader, writer),
    )


async def serve(config: RelayConfig) -> None:
    context = build_server_context(config)
    servers = [
        await asyncio.start_server(
            partial(_handle_peer, config, service),
            host=config.bind_address,
            port=service.bind_port,
            ssl=context,
            ssl_handshake_timeout=5.0,
            start_serving=True,
        )
        for service in config.services
    ]
    try:
        await asyncio.gather(*(server.serve_forever() for server in servers))
    finally:
        for server in servers:
            server.close()
        await asyncio.gather(*(server.wait_closed() for server in servers))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--check", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = load_config(args.config)
    build_server_context(config)
    if args.check:
        return 0
    asyncio.run(serve(config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
