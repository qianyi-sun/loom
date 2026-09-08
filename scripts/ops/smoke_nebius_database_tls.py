#!/usr/bin/env python3
"""Verify PostgreSQL TLS with Loom's installed async psycopg/SQLAlchemy stack.

Uses cached postgres:16-alpine, a disposable container, ephemeral loopback port,
and generated fixture certificates only. Does not access live services. The
hostaddr argument substitutes for tested Pod hostAliases without changing local
DNS; the native server hostname is still checked by libpq. CA symlink swaps
model a Secret directory update. This is not Kubernetes/live acceptance.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import tempfile
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import sqlalchemy
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from sqlalchemy import URL, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

SERVER_NAME = "loom-postgres-rw.loom-staging.svc.cluster.local"


def command(*args: str, check: bool = True) -> str:
    result = subprocess.run(args, capture_output=True, text=True, timeout=60, check=False)
    if check and result.returncode:
        raise RuntimeError(f"fixture command failed: {args[0]} {args[1]}")
    return result.stdout.strip()


def certificates(directory: Path, generation: str) -> None:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "fixture-ca-" + generation)])
    now = datetime.now(UTC)
    ca = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    server_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    server = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, SERVER_NAME)]))
        .issuer_name(ca.subject)
        .public_key(server_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(SERVER_NAME)]), critical=False)
        .sign(key, hashes.SHA256())
    )
    (directory / f"ca-{generation}.crt").write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    (directory / f"server-{generation}.crt").write_bytes(
        server.public_bytes(serialization.Encoding.PEM)
    )
    path = directory / f"server-{generation}.key"
    path.write_bytes(
        server_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    path.chmod(0o600)


def rotate_ca(directory: Path, generation: str) -> None:
    temporary = directory / "ca-next.crt"
    temporary.symlink_to(f"ca-{generation}.crt")
    temporary.replace(directory / "ca.crt")


async def query(engine: AsyncEngine) -> bool:
    async with engine.connect() as connection:
        return bool(
            (
                await connection.execute(
                    text("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
                )
            ).scalar_one()
        )


async def exercise(port: int, directory: Path, container: str) -> dict[str, object]:
    url = URL.create(
        "postgresql+psycopg", username="postgres", host=SERVER_NAME, port=port, database="loom"
    )
    # Same async dialect used by the Gateway and execution actuator. NullPool
    # ensures each query reads the current CA while preserving the same engine.
    engine = create_async_engine(
        url, connect_args={"hostaddr": "127.0.0.1", "connect_timeout": 3}, poolclass=NullPool
    )
    bad_name = create_async_engine(
        url.set(host="https-entry.smoke.invalid"),
        connect_args={"hostaddr": "127.0.0.1", "connect_timeout": 3},
        poolclass=NullPool,
    )
    try:
        assert await query(engine), "native PostgreSQL TLS query failed"
        try:
            await query(bad_name)
        except OperationalError as exc:
            assert "does not match host name" in str(exc)
        else:
            raise AssertionError("HTTPS name unexpectedly accepted by native PG certificate")
        async with engine.connect() as existing:
            rotate_ca(directory, "wrong")
            try:
                await query(engine)
            except OperationalError as exc:
                assert "certificate verify failed" in str(exc)
            else:
                raise AssertionError("untrusted CA accepted")
            # Existing authenticated sessions are not disrupted by trust updates.
            assert (await existing.execute(text("SELECT 1"))).scalar_one() == 1
            rotate_ca(directory, "trusted")
            assert await query(engine), "CA refresh required an application restart"
            # Rotate the actual server certificate/CA, then update projected
            # trust material. PostgreSQL reloads certs without server restart.
            command(
                "docker",
                "exec",
                container,
                "sh",
                "-ec",
                "cp /fixture/server-wrong.crt /tmp/server.crt; "
                "cp /fixture/server-wrong.key /tmp/server.key; "
                "chown postgres:postgres /tmp/server.key; chmod 600 /tmp/server.key; "
                "kill -HUP 1",
            )
            rotate_ca(directory, "wrong")
            for _ in range(30):
                try:
                    assert await query(engine)
                    break
                except OperationalError:
                    await asyncio.sleep(0.1)
            else:
                raise AssertionError("new server CA did not become usable without app restart")
            assert (await existing.execute(text("SELECT 1"))).scalar_one() == 1
            rotate_ca(directory, "trusted")
            try:
                await query(engine)
            except OperationalError as exc:
                assert "certificate verify failed" in str(exc)
            else:
                raise AssertionError("old-only CA accepted the rotated server certificate")
            rotate_ca(directory, "wrong")
            assert await query(engine)
        return {
            "sql_over_native_tls": True,
            "wrong_hostname_rejected": True,
            "untrusted_ca_rejected": True,
            "ca_refresh_without_engine_restart": True,
            "server_ca_rotation_without_engine_restart": True,
            "existing_connection_preserved": True,
            "psycopg": psycopg.__version__,
            "sqlalchemy": sqlalchemy.__version__,
        }
    finally:
        await bad_name.dispose()
        await engine.dispose()


def main() -> None:
    name = "loom-nebius-db-tls-smoke-" + uuid.uuid4().hex[:12]
    old_env = {key: os.environ.get(key) for key in ("PGSSLMODE", "PGSSLROOTCERT")}
    with tempfile.TemporaryDirectory(prefix="loom-nebius-db-tls-") as raw:
        directory = Path(raw)
        certificates(directory, "trusted")
        certificates(directory, "wrong")
        rotate_ca(directory, "trusted")
        os.environ.update(PGSSLMODE="verify-full", PGSSLROOTCERT=str(directory / "ca.crt"))
        try:
            command(
                "docker",
                "run",
                "--detach",
                "--rm",
                "--pull=never",
                "--name",
                name,
                "--publish",
                "127.0.0.1::5432",
                "--env",
                "POSTGRES_HOST_AUTH_METHOD=trust",
                "--env",
                "POSTGRES_DB=loom",
                "--mount",
                f"type=bind,source={directory},target=/fixture,readonly",
                "postgres:16-alpine",
                "sh",
                "-ec",
                "cp /fixture/server-trusted.key /tmp/server.key; "
                "cp /fixture/server-trusted.crt /tmp/server.crt; "
                "chown postgres:postgres /tmp/server.key; chmod 600 /tmp/server.key; "
                "exec docker-entrypoint.sh postgres -c ssl=on "
                "-c ssl_cert_file=/tmp/server.crt -c ssl_key_file=/tmp/server.key",
            )
            for _ in range(60):
                result = command(
                    "docker", "exec", name, "pg_isready", "-U", "postgres", check=False
                )
                if "accepting connections" in result:
                    # Entry-point bootstrap also briefly accepts Unix connections;
                    # TCP confirms the final server is ready for external clients.
                    result = command(
                        "docker",
                        "exec",
                        name,
                        "pg_isready",
                        "-h",
                        "127.0.0.1",
                        "-U",
                        "postgres",
                        check=False,
                    )
                    if "accepting connections" in result:
                        break
                time.sleep(0.5)
            else:
                raise RuntimeError("disposable PostgreSQL did not become ready")
            port = int(command("docker", "port", name, "5432/tcp").rsplit(":", 1)[1])
            print(json.dumps(asyncio.run(exercise(port, directory, name)), sort_keys=True))
        finally:
            command("docker", "rm", "--force", "--volumes", name, check=False)
            for key, value in old_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


if __name__ == "__main__":
    main()
