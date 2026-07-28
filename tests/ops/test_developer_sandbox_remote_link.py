from __future__ import annotations

import asyncio
import os
import ssl
import subprocess
from pathlib import Path

import pytest
import scripts.ops.developer_sandbox_remote_link as relay

from loom_worker import sandbox_link_proxy as proxy


class _SslObject:
    def __init__(self, uri: str) -> None:
        self._uri = uri

    def getpeercert(self) -> dict[str, object]:
        return {"subjectAltName": (("URI", self._uri), ("DNS", "oldlab-1"))}


class _Writer:
    def __init__(self, uri: str) -> None:
        self.ssl_object = _SslObject(uri)
        self.closed = False

    def get_extra_info(self, name: str) -> object | None:
        return self.ssl_object if name == "ssl_object" else None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


def _services() -> tuple[relay.ServiceConfig, ...]:
    return (
        relay.ServiceConfig("control-plane", 26080, 20080, "/healthz", False),
        relay.ServiceConfig("gateway", 26100, 20100, "/healthz", False),
        relay.ServiceConfig("minio", 26900, 20900, "/minio/health/live", True),
    )


def _config(tmp_path: Path) -> relay.RelayConfig:
    return relay.RelayConfig(
        sandbox="qianyi",
        candidate_sha="a" * 40,
        bind_address="192.168.50.14",
        target_host="127.0.0.1",
        ca_file=tmp_path / "ca.pem",
        cert_file=tmp_path / "server.pem",
        key_file=tmp_path / "server-key.pem",
        services=_services(),
    )


def test_peer_uri_san_is_exact_not_subset(tmp_path: Path) -> None:
    config = _config(tmp_path)
    good = _Writer(config.client_uri_san)
    wrong_candidate = _Writer(config.client_uri_san.replace("a" * 40, "b" * 40))
    wrong_sandbox = _Writer(config.client_uri_san.replace("/qianyi/", "/devansh/"))

    assert relay._peer_uri_sans(good) == frozenset({config.client_uri_san})
    assert relay._peer_uri_sans(wrong_candidate) != frozenset({config.client_uri_san})
    assert relay._peer_uri_sans(wrong_sandbox) != frozenset({config.client_uri_san})


@pytest.mark.parametrize(
    "uri",
    (
        "spiffe://loom/developer-sandbox/qianyi/candidate/" + "b" * 40 + "/worker",
        "spiffe://loom/developer-sandbox/devansh/candidate/" + "a" * 40 + "/worker",
    ),
)
def test_wrong_identity_is_closed_before_loopback_connect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    uri: str,
) -> None:
    config = _config(tmp_path)
    writer = _Writer(uri)

    async def forbidden_open(*_: object, **__: object) -> None:
        raise AssertionError("loopback target must not be opened")

    monkeypatch.setattr(asyncio, "open_connection", forbidden_open)

    async def scenario() -> None:
        await relay._handle_peer(
            config,
            config.services[0],
            asyncio.StreamReader(),
            writer,  # type: ignore[arg-type]
        )

    asyncio.run(scenario())
    assert writer.closed is True


def _generate_certificates(root: Path, uri: str) -> dict[str, Path]:
    paths = {
        name: root / name
        for name in (
            "ca-key.pem",
            "ca.pem",
            "server-key.pem",
            "server.csr",
            "server.pem",
            "server.ext",
            "client-key.pem",
            "client.csr",
            "client.pem",
            "client.ext",
        )
    }

    def run(*args: str) -> None:
        subprocess.run(["openssl", *args], check=True, capture_output=True)

    run(
        "req",
        "-x509",
        "-newkey",
        "ed25519",
        "-nodes",
        "-days",
        "1",
        "-subj",
        "/CN=test-ca",
        "-keyout",
        str(paths["ca-key.pem"]),
        "-out",
        str(paths["ca.pem"]),
    )
    paths["server.ext"].write_text(
        "subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\n",
        encoding="utf-8",
    )
    run(
        "req",
        "-newkey",
        "ed25519",
        "-nodes",
        "-subj",
        "/CN=127.0.0.1",
        "-keyout",
        str(paths["server-key.pem"]),
        "-out",
        str(paths["server.csr"]),
    )
    run(
        "x509",
        "-req",
        "-in",
        str(paths["server.csr"]),
        "-CA",
        str(paths["ca.pem"]),
        "-CAkey",
        str(paths["ca-key.pem"]),
        "-CAcreateserial",
        "-days",
        "1",
        "-extfile",
        str(paths["server.ext"]),
        "-out",
        str(paths["server.pem"]),
    )
    paths["client.ext"].write_text(
        f"subjectAltName=URI:{uri}\nextendedKeyUsage=clientAuth\n",
        encoding="utf-8",
    )
    run(
        "req",
        "-newkey",
        "ed25519",
        "-nodes",
        "-subj",
        "/CN=test-client",
        "-keyout",
        str(paths["client-key.pem"]),
        "-out",
        str(paths["client.csr"]),
    )
    run(
        "x509",
        "-req",
        "-in",
        str(paths["client.csr"]),
        "-CA",
        str(paths["ca.pem"]),
        "-CAkey",
        str(paths["ca-key.pem"]),
        "-CAcreateserial",
        "-days",
        "1",
        "-extfile",
        str(paths["client.ext"]),
        "-out",
        str(paths["client.pem"]),
    )
    for name in ("server-key.pem", "client-key.pem"):
        os.chmod(paths[name], 0o600)
    return paths


def test_full_data_plane_crosses_tls13_candidate_relay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = _config(tmp_path)
    paths = _generate_certificates(tmp_path, expected.client_uri_san)
    monkeypatch.setattr(proxy, "SERVER_ADDRESS", "127.0.0.1")

    async def scenario() -> None:
        responses = {
            "control-plane": b'{"worker_id":"candidate-worker"}',
            "gateway": b'{"choices":[{"message":{"content":"ok"}}]}',
            "minio": b"object-bytes",
        }

        async def backend(
            name: str,
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            headers = await reader.readuntil(b"\r\n\r\n")
            content_length = 0
            for line in headers.split(b"\r\n"):
                if line.lower().startswith(b"content-length:"):
                    content_length = int(line.split(b":", 1)[1])
            if content_length:
                await reader.readexactly(content_length)
            body = responses[name]
            writer.write(
                b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: "
                + str(len(body)).encode()
                + b"\r\n\r\n"
                + body,
            )
            await writer.drain()
            writer.close()

        backend_servers = []
        backend_ports: dict[str, int] = {}
        for service in expected.services:
            server = await asyncio.start_server(
                lambda reader, writer, name=service.name: backend(name, reader, writer),
                "127.0.0.1",
                0,
            )
            backend_servers.append(server)
            backend_ports[service.name] = server.sockets[0].getsockname()[1]

        active_services = tuple(
            relay.ServiceConfig(
                service.name,
                0,
                backend_ports[service.name],
                service.health_path,
                service.allow_empty_health,
            )
            for service in expected.services
        )
        active = relay.RelayConfig(
            sandbox=expected.sandbox,
            candidate_sha=expected.candidate_sha,
            bind_address="127.0.0.1",
            target_host="127.0.0.1",
            ca_file=paths["ca.pem"],
            cert_file=paths["server.pem"],
            key_file=paths["server-key.pem"],
            services=active_services,
        )
        server_context = relay.build_server_context(active)
        relay_servers = []
        relay_ports: dict[str, int] = {}
        for service in active.services:
            server = await asyncio.start_server(
                lambda reader, writer, service=service: relay._handle_peer(
                    active,
                    service,
                    reader,
                    writer,
                ),
                "127.0.0.1",
                0,
                ssl=server_context,
            )
            relay_servers.append(server)
            relay_ports[service.name] = server.sockets[0].getsockname()[1]

        client_context = ssl.create_default_context(cafile=str(paths["ca.pem"]))
        client_context.minimum_version = ssl.TLSVersion.TLSv1_3
        client_context.maximum_version = ssl.TLSVersion.TLSv1_3
        client_context.load_cert_chain(
            certfile=str(paths["client.pem"]),
            keyfile=str(paths["client-key.pem"]),
        )
        local_servers = []
        local_ports: dict[str, int] = {}
        for service in expected.services:
            link = proxy.Link(
                service.name,
                0,
                relay_ports[service.name],
                service.health_path,
                service.allow_empty_health,
            )
            server = await asyncio.start_server(
                lambda reader, writer, link=link: proxy._handle(
                    link,
                    client_context,
                    reader,
                    writer,
                ),
                "127.0.0.1",
                0,
            )
            local_servers.append(server)
            local_ports[service.name] = server.sockets[0].getsockname()[1]

        requests = {
            "control-plane": b"POST /workers/register HTTP/1.1\r\nHost: local\r\nContent-Length: 2\r\n\r\n{}",
            "gateway": b"POST /openai/v1/chat/completions HTTP/1.1\r\nHost: local\r\nContent-Length: 2\r\n\r\n{}",
            "minio": b"PUT /bucket/object HTTP/1.1\r\nHost: local\r\nContent-Length: 6\r\n\r\nobject",
        }
        for name, request in requests.items():
            reader, writer = await asyncio.open_connection("127.0.0.1", local_ports[name])
            writer.write(request)
            await writer.drain()
            response = await reader.read()
            assert responses[name] in response
            writer.close()
            await writer.wait_closed()

        for server in (*local_servers, *relay_servers, *backend_servers):
            server.close()
        await asyncio.gather(
            *(
                server.wait_closed()
                for server in (*local_servers, *relay_servers, *backend_servers)
            ),
        )

    asyncio.run(scenario())
