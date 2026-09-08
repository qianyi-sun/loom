#!/usr/bin/env python3
"""Exercise the rendered private entry in disposable, network-isolated Docker.

Run with Loom's development Python environment (PyYAML and cryptography).
Uses cached images only, no published ports, live services, or real credentials.
This verifies transport fixtures, not PostgreSQL queries, MinIO signatures,
Kubernetes hostPort/NetworkPolicy enforcement, or live staging acceptance.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import yaml  # type: ignore[import-untyped]
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

ROOT = Path(__file__).resolve().parents[2]
NGINX = "nginx:1.28-alpine@sha256:a8b39bd9cf0f83869a2162827a0caf6137ddf759d50a171451b335cecc87d236"
PYTHON = "python:3.12-slim"
FIXTURE = r"""
import hashlib, json, os, socket, ssl, struct, sys, threading
from pathlib import Path

def receive_http(conn):
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = conn.recv(4096)
        if not chunk:
            raise OSError("client disconnected")
        data += chunk
    header, body = data.split(b"\r\n\r\n", 1)
    size = int(next(x.split(b":", 1)[1] for x in header.split(b"\r\n")
                    if x.lower().startswith(b"content-length:")))
    while len(body) < size:
        chunk = conn.recv(4096)
        if not chunk:
            raise OSError("client disconnected")
        body += chunk
    return header + b"\r\n\r\n" + body

def serve(port):
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("0.0.0.0", port))
    listener.listen()
    while True:
        conn, _ = listener.accept()
        try:
            conn.settimeout(5)
            if port == 5432:
                assert conn.recv(8) == struct.pack("!II", 8, 80877103)
                conn.sendall(b"S")
                context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                context.load_cert_chain("/fixture/pg.crt", "/fixture/pg.key")
                conn = context.wrap_socket(conn, server_side=True)
                assert conn.recv(64) == b"passthrough-fixture"
                conn.sendall(b"pg-tls-fixture-ok")
            else:
                request = receive_http(conn)
                generation = os.environ.get("GENERATION", "1")
                response = ("HTTP/1.1 200 OK\r\nConnection: close\r\n"
                            "X-Fixture-Generation: " + generation + "\r\n"
                            "Content-Length: " + str(len(request)) + "\r\n\r\n")
                conn.sendall(response.encode() + request)
        except (OSError, AssertionError):
            pass
        finally:
            conn.close()

if sys.argv[1] == "serve":
    for port in (5432, 8080, 9000):
        threading.Thread(target=serve, args=(port,), daemon=True).start()
    threading.Event().wait()
else:
    mode, port = sys.argv[1], int(sys.argv[2])
    context = ssl.create_default_context(cafile="/fixture/ca.crt")
    conn = socket.create_connection(("entry", port), timeout=5)
    if mode == "pg":
        conn.sendall(struct.pack("!II", 8, 80877103))
        assert conn.recv(1) == b"S"
    with context.wrap_socket(conn, server_hostname=(
        "pg.smoke.invalid" if mode == "pg" else "smoke.invalid"
    )) as tls:
        result = {"fingerprint": hashlib.sha256(tls.getpeercert(True)).hexdigest()}
        if mode == "pg":
            tls.sendall(b"passthrough-fixture")
            assert tls.recv(64) == b"pg-tls-fixture-ok"
        elif mode == "http":
            request = (b"PUT /bucket/a%2Fb%20c?x=%2B&x=two HTTP/1.1\r\n"
                       b"Host: signed-host.smoke.invalid:19443\r\n"
                       b"Authorization: fixture-signature-not-a-secret\r\n"
                       b"Content-Length: 9\r\nConnection: close\r\n\r\n"
                       b"a\x00b\r\nc\xffde")
            tls.sendall(request)
            response = b""
            while chunk := tls.recv(4096):
                response += chunk
            header, body = response.split(b"\r\n\r\n", 1)
            assert body == request, "proxy changed raw HTTP request bytes"
            result["generation"] = next(x.split(b": ", 1)[1].decode()
                for x in header.split(b"\r\n") if x.startswith(b"X-Fixture-Generation:"))
        print(json.dumps(result))
"""


def command(*args: str, check: bool = True) -> str:
    result = subprocess.run(args, text=True, capture_output=True, timeout=60, check=False)
    if check and result.returncode:
        raise RuntimeError(f"{args[0]} failed: {result.stderr[-2000:]}")
    return result.stdout.strip()


def certificate(directory: Path, name: str, ca_key: rsa.RSAPrivateKey, ca: x509.Certificate) -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    hostname = "pg.smoke.invalid" if name == "pg" else "smoke.invalid"
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, hostname)]))
        .issuer_name(ca.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(UTC) - timedelta(minutes=1))
        .not_valid_after(datetime.now(UTC) + timedelta(hours=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(hostname)]), critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    (directory / f"{name}.crt").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    (directory / f"{name}.key").write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return hashlib.sha256(cert.public_bytes(serialization.Encoding.DER)).hexdigest()


def main() -> None:
    prefix = "loom-private-smoke-" + uuid.uuid4().hex[:12]
    containers: list[str] = []
    network_created = False
    volume_created = False
    os.umask(0o077)
    with tempfile.TemporaryDirectory(prefix=prefix + "-") as temporary:
        work = Path(temporary)
        try:
            for image in (NGINX, PYTHON):
                command("docker", "image", "inspect", image)
            profile = ROOT / "deploy/environments/staging.multinode.cluster.toml"
            config = work / "cluster.toml"
            config.write_text(
                profile.read_text()
                + '\n[nebius_private_entry]\nenabled = true\nnode_name = "staging-control-1"\n'
                + 'wireguard_address = "10.253.71.2"\npeer_address = "10.253.71.1"\n'
                + f"proxy_image = {json.dumps(NGINX)}\n"
            )
            rendered = command(
                sys.executable, "-m", "loom_cli", "cluster", "render", "--config", str(config)
            )
            entry = next(
                doc
                for doc in yaml.safe_load_all(rendered)
                if doc
                and doc["kind"] == "ConfigMap"
                and doc["metadata"]["name"] == "loom-nebius-private-entry"
            )
            (work / "config").mkdir()
            for name, value in entry["data"].items():
                (work / "config" / name).write_text(value)
            ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Disposable smoke CA")])
            ca = (
                x509.CertificateBuilder()
                .subject_name(ca_name)
                .issuer_name(ca_name)
                .public_key(ca_key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(datetime.now(UTC) - timedelta(minutes=1))
                .not_valid_after(datetime.now(UTC) + timedelta(hours=1))
                .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
                .sign(ca_key, hashes.SHA256())
            )
            (work / "ca.crt").write_bytes(ca.public_bytes(serialization.Encoding.PEM))
            fingerprints = {
                name: certificate(work, name, ca_key, ca) for name in ("v1", "v2", "v3", "pg")
            }
            (work / "fixture.py").write_text(FIXTURE)

            def project(version: str, key_version: str | None = None) -> None:
                # Native Linux volume: host-bind symlink swaps on Docker Desktop
                # do not reliably model kubelet's atomic projected-volume rename.
                command(
                    "docker",
                    "exec",
                    client,
                    "python",
                    "-c",
                    """
import os, sys, uuid
from pathlib import Path
os.umask(0o077)
tls = Path("/projection")
target = tls / ("version-" + uuid.uuid4().hex)
target.mkdir()
for name, source in (("tls.crt", sys.argv[1] + ".crt"), ("tls.key", sys.argv[2] + ".key")):
    (target / name).write_bytes((Path("/fixture") / source).read_bytes())
    if not (tls / name).is_symlink():
        (tls / name).symlink_to("..data/" + name)
(tls / "..new").symlink_to(target.name)
(tls / "..new").replace(tls / "..data")
""",
                    version,
                    key_version or version,
                )

            command("docker", "network", "create", "--internal", prefix)
            network_created = True
            command("docker", "volume", "create", prefix)
            volume_created = True

            def launch(name: str, image: str, *args: str, options: tuple[str, ...] = ()) -> str:
                container = prefix + "-" + name
                containers.append(container)
                command(
                    "docker",
                    "run",
                    "--pull=never",
                    "-d",
                    "--name",
                    container,
                    "--network",
                    prefix,
                    "-v",
                    f"{work}:/fixture:ro",
                    *options,
                    image,
                    *args,
                )
                return container

            aliases = tuple(
                value
                for service in ("loom-control-plane", "loom-minio", "loom-postgres-rw")
                for value in ("--network-alias", service + ".loom-staging.svc.cluster.local")
            )
            backend = launch(
                "backend", PYTHON, "python", "/fixture/fixture.py", "serve", options=aliases
            )
            client = launch(
                "client", PYTHON, "sleep", "600", options=("-v", f"{prefix}:/projection")
            )
            project("v1")
            proxy = launch(
                "entry",
                NGINX,
                "/bin/sh",
                "/config/run.sh",
                options=(
                    "--network-alias",
                    "entry",
                    "-v",
                    f"{work / 'config'}:/config:ro",
                    "-v",
                    f"{prefix}:/tls:ro",
                ),
            )
            started = command("docker", "inspect", "--format", "{{.State.StartedAt}}", proxy)

            def probe(mode: str = "fingerprint", port: int = 18443) -> dict[str, str]:
                return cast(
                    dict[str, str],
                    json.loads(
                        command(
                            "docker",
                            "exec",
                            client,
                            "python",
                            "/fixture/fixture.py",
                            mode,
                            str(port),
                        )
                    ),
                )

            def await_probe(
                field: str, expected: str, mode: str = "fingerprint", timeout: int = 25
            ) -> float:
                start = time.monotonic()
                while time.monotonic() - start < timeout:
                    try:
                        if probe(mode)[field] == expected:
                            return round(time.monotonic() - start, 2)
                    except (RuntimeError, KeyError):
                        pass
                    time.sleep(1)
                raise AssertionError(f"timed out waiting for {mode} {field}")

            await_probe("fingerprint", fingerprints["v1"])
            for port in (18443, 19443):
                assert probe("http", port)["fingerprint"] == fingerprints["v1"]
            assert probe("pg", 15432)["fingerprint"] == fingerprints["pg"]
            project("v2")
            rotation = await_probe("fingerprint", fingerprints["v2"])
            project("v3", "v1")
            invalid_start = time.monotonic()
            while time.monotonic() - invalid_start < 12:
                assert probe("http")["fingerprint"] == fingerprints["v2"]
                time.sleep(1)
            assert "retaining active certificate" in command(
                "docker", "logs", proxy, check=False
            ) or (
                "retaining active certificate"
                in subprocess.run(
                    ["docker", "logs", proxy], capture_output=True, text=True, timeout=10
                ).stderr
            )
            project("v3")
            recovery = await_probe("fingerprint", fingerprints["v3"])
            old_ip = command(
                "docker",
                "inspect",
                "--format",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                backend,
            )
            command("docker", "rm", "-f", backend)
            launch("ip-holder", PYTHON, "sleep", "600", options=("--ip", old_ip))
            replacement = launch(
                "replacement",
                PYTHON,
                "python",
                "/fixture/fixture.py",
                "serve",
                options=(*aliases, "-e", "GENERATION=2"),
            )
            new_ip = command(
                "docker",
                "inspect",
                "--format",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                replacement,
            )
            assert new_ip != old_ip
            dns_refresh = await_probe("generation", "2", "http", timeout=35)
            assert probe("http", 19443)["generation"] == "2"
            assert probe("pg", 15432)["fingerprint"] == fingerprints["pg"]
            assert (
                command("docker", "inspect", "--format", "{{.State.StartedAt}}", proxy) == started
            )
            command("docker", "exec", proxy, "sh", "-c", 'kill -TERM "$(cat /tmp/nginx.pid)"')
            stopped = time.monotonic()
            while command("docker", "inspect", "--format", "{{.State.Running}}", proxy) == "true":
                if time.monotonic() - stopped > 15:
                    raise AssertionError("nginx child exit did not terminate container")
                time.sleep(1)
            print(
                json.dumps(
                    {
                        "status": "passed",
                        "http_tls_and_bytes": True,
                        "postgres_sslrequest_tls_passthrough": True,
                        "certificate_rotation_seconds": rotation,
                        "invalid_pair_last_good_retained": True,
                        "recovery_seconds": recovery,
                        "backend_new_ip_dns_refresh_seconds": dns_refresh,
                        "proxy_restarted": False,
                        "nginx_child_exit_terminates_container": True,
                        "scope": "disposable transport fixtures; not live staging acceptance",
                    },
                    indent=2,
                )
            )
        except Exception:
            if prefix + "-entry" in containers:
                result = subprocess.run(
                    ["docker", "logs", prefix + "-entry"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
                print(result.stdout[-2000:] + result.stderr[-2000:], file=sys.stderr)
            raise
        finally:
            for container in reversed(containers):
                command("docker", "rm", "-f", container, check=False)
            if network_created:
                command("docker", "network", "rm", prefix)
            if volume_created:
                command("docker", "volume", "rm", prefix)


if __name__ == "__main__":
    main()
