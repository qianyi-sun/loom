"""Disposable TLS object storage for supported source-bundle tests."""

import ipaddress
import os
import ssl
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import boto3
import docker
import httpx
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from tests.support.minio_images import MINIO_TLS_IMAGE, prepare_test_image

IMAGE = MINIO_TLS_IMAGE
KEYS = ("revision/task.toml", "revision/a space+%.toml", "revision/café.toml", "revision/literal%2Fkey")
FORM_PREFIX_KEY = "revision space+/literal%2Fkey"
PAYLOAD = b"disposable-exact-bundle-content"


@pytest.fixture(scope="module")
def minio_tls(tmp_path_factory):
    root = tmp_path_factory.mktemp("bundle-s3-signing")
    certs, data = root / "certs", root / "data"
    certs.mkdir()
    data.mkdir()
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "disposable-minio")])
    now = datetime.now(UTC)
    cert = (
        x509.CertificateBuilder().subject_name(name).issuer_name(name)
        .public_key(private.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1)).not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), critical=False)
        .sign(private, hashes.SHA256())
    )
    cert_file, key_file = certs / "public.crt", certs / "private.key"
    cert_file.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_file.write_bytes(private.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption(),
    ))
    key_file.chmod(0o600)
    credentials = SimpleNamespace(access_key="minio-access-fixture", secret_key="minio-secret-fixture")
    docker_client, container, admin = docker.from_env(), None, None
    try:
        container = docker_client.containers.run(
            prepare_test_image(IMAGE), ["server", "/data", "--address", ":9000", "--certs-dir", "/certs"],
            detach=True, user=f"{os.getuid()}:{os.getgid()}",
            environment={"MINIO_ROOT_USER": credentials.access_key, "MINIO_ROOT_PASSWORD": credentials.secret_key, "MINIO_BROWSER": "off"},
            volumes={str(certs): {"bind": "/certs", "mode": "ro"}, str(data): {"bind": "/data", "mode": "rw"}},
            ports={"9000/tcp": ("127.0.0.1", None)}, cap_drop=["ALL"],
            security_opt=["no-new-privileges"], mem_limit="512m", nano_cpus=1_000_000_000,
            pids_limit=128, read_only=True,
        )
        container.reload()
        port = container.attrs["NetworkSettings"]["Ports"]["9000/tcp"][0]["HostPort"]
        origin = f"https://127.0.0.1:{port}"
        context = ssl.create_default_context(cafile=str(cert_file))
        with httpx.Client(verify=context, trust_env=False, timeout=2) as client:
            deadline = time.monotonic() + 30
            while True:
                try:
                    if client.get(origin + "/minio/health/live").status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                assert time.monotonic() < deadline, "disposable TLS MinIO did not become ready"
                time.sleep(0.1)
            admin = boto3.client(
                "s3", endpoint_url=origin, aws_access_key_id=credentials.access_key,
                aws_secret_access_key=credentials.secret_key, region_name="us-east-1",
                verify=str(cert_file), config=Config(signature_version="s3v4", proxies={}, retries={"max_attempts": 0}),
            )
            for bucket in ("loom-bundles", "other-bundles"):
                # HTTP liveness can precede storage initialization. Retry only
                # that explicit fixture startup condition under the same budget;
                # permission/signature/other storage failures must still fail.
                while True:
                    try:
                        admin.create_bucket(Bucket=bucket)
                        break
                    except ClientError as error:
                        if error.response.get("Error", {}).get("Code") != "XMinioServerNotInitialized" or time.monotonic() >= deadline:
                            raise
                        time.sleep(0.1)
                for key in (*KEYS, FORM_PREFIX_KEY):
                    admin.put_object(Bucket=bucket, Key=key, Body=PAYLOAD)
            yield origin, credentials, client, admin, cert_file
    finally:
        if admin is not None:
            admin.close()
        if container is not None:
            container.remove(force=True, v=True)
        docker_client.close()
