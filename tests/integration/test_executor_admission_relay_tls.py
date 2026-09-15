"""Real PostgreSQL TLS and hostname verification through the private byte relay."""

import asyncio
import io
import ipaddress
import tarfile
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from sqlalchemy.engine import make_url
from testcontainers.postgres import PostgresContainer

from loom.application_schema_reference import application_schema_reference
from loom_capacity_executor.admission_client import _database_url_from_bytes
from loom_capacity_manager.tcp_proxy import start_tcp_proxy
from tests.integration.test_capacity_manager_mtls import _new_ca, _private_key_bytes

_HOST = "loom-postgres-rw.loom-staging.svc.cluster.local"


@pytest.mark.parametrize("major", [16, 17])
@pytest.mark.asyncio
async def test_relay_preserves_postgres_verify_full_and_rejects_wrong_trust(tmp_path, monkeypatch, major):
    ca_key, ca = _new_ca("relay-test")
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.now(UTC)
    certificate = (x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, _HOST)]))
        .issuer_name(ca.subject).public_key(key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1)).not_valid_after(now + timedelta(hours=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(_HOST)]), critical=False)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=True)
        .sign(ca_key, hashes.SHA256()))
    ca_path = tmp_path / "ca.pem"
    ca_path.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    wrong_ca_path = tmp_path / "wrong-ca.pem"
    wrong_ca_path.write_bytes(_new_ca("wrong-relay-test")[1].public_bytes(serialization.Encoding.PEM))
    monkeypatch.setenv("PGSSLROOTCERT", str(ca_path))
    with PostgresContainer(application_schema_reference(postgres_major=major).postgres_image,
            driver="psycopg", password=uuid4().hex).with_bind_ports(5432, ("127.0.0.1", None)) as postgres:
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w") as tar:
            for name, data in {"relay.key": _private_key_bytes(key),
                               "relay.crt": certificate.public_bytes(serialization.Encoding.PEM)}.items():
                info = tarfile.TarInfo(name)
                info.size, info.mode = len(data), 0o600
                tar.addfile(info, io.BytesIO(data))
        assert postgres.get_wrapped_container().put_archive("/tmp", archive.getvalue())
        assert postgres.exec(["chown", "postgres:postgres", "/tmp/relay.key", "/tmp/relay.crt"])[0] == 0
        base = make_url(postgres.get_connection_url()).set(drivername="postgresql")
        with psycopg.connect(base.render_as_string(hide_password=False), autocommit=True) as connection:
            connection.execute("ALTER SYSTEM SET ssl_cert_file='/tmp/relay.crt'")
            connection.execute("ALTER SYSTEM SET ssl_key_file='/tmp/relay.key'")
            connection.execute("ALTER SYSTEM SET ssl=on")
            assert connection.execute("SELECT pg_reload_conf()").fetchone() == (True,)
        proxy = await start_tcp_proxy(listen_host="127.0.0.1", listen_port=0,
            upstream_host="127.0.0.1", upstream_port=int(postgres.get_exposed_port(5432)),
            allowed_client_ips=frozenset({ipaddress.ip_address("127.0.0.1")}))
        port = int(proxy.sockets[0].getsockname()[1])
        url = base.set(drivername="postgresql+psycopg", host=_HOST, port=port,
            query={"sslmode": "verify-full", "hostaddr": "127.0.0.1"})
        admitted = make_url(_database_url_from_bytes(url.render_as_string(hide_password=False).encode())).set(drivername="postgresql")
        try:
            async with await psycopg.AsyncConnection.connect(admitted.render_as_string(hide_password=False), connect_timeout=5) as connection:
                row = await (await connection.execute("SELECT ssl, version FROM pg_stat_ssl WHERE pid=pg_backend_pid()")).fetchone()
                assert row[0] is True and row[1] in {"TLSv1.2", "TLSv1.3"}
            with pytest.raises(psycopg.OperationalError, match="does not match host name"):
                await psycopg.AsyncConnection.connect(admitted.set(host="wrong.example.test").render_as_string(hide_password=False), connect_timeout=5)
            monkeypatch.setenv("PGSSLROOTCERT", str(wrong_ca_path))
            with pytest.raises(psycopg.OperationalError, match="certificate verify failed"):
                await psycopg.AsyncConnection.connect(admitted.render_as_string(hide_password=False), connect_timeout=5)
        finally:
            proxy.close()
            await proxy.wait_closed()
            handlers = [task for task in asyncio.all_tasks()
                if task.get_coro().__qualname__ == "start_tcp_proxy.<locals>.handle"]
            await asyncio.wait_for(asyncio.gather(*handlers), timeout=5)
