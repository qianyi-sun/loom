"""Actual S3 signature verification by pinned disposable MinIO over verified TLS."""

import asyncio
import ipaddress
import os
import ssl
import time
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import boto3
import docker
import httpx
import pytest
from botocore.config import Config
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from loom_task_image_authority.bundle_s3_signing import (
    S3SigningCredentials,
    presign_bundle_get,
    presign_bundle_list,
)

pytestmark = [pytest.mark.docker, pytest.mark.timeout(120)]
IMAGE = "minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
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
    credentials = S3SigningCredentials(access_key="minio-access-fixture", secret_key="minio-secret-fixture")
    docker_client, container, admin = docker.from_env(), None, None
    try:
        container = docker_client.containers.run(
            IMAGE, ["server", "/data", "--address", ":9000", "--certs-dir", "/certs"],
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
                admin.create_bucket(Bucket=bucket)
                for key in (*KEYS, FORM_PREFIX_KEY):
                    admin.put_object(Bucket=bucket, Key=key, Body=PAYLOAD)
            yield origin, credentials, client, admin, cert_file
    finally:
        if admin is not None:
            admin.close()
        if container is not None:
            container.remove(force=True, v=True)
        docker_client.close()


def _signed(fixture, key, **changes):
    origin, credentials, _ = fixture[:3]
    now = datetime.now(UTC).replace(microsecond=0)
    options = dict(public_origin=origin, bucket="loom-bundles", key=key, region="us-east-1", credentials=credentials, expires_at=now + timedelta(seconds=60))
    options.update(changes)
    return presign_bundle_get(**options)


@pytest.mark.parametrize("key", KEYS)
def test_actual_minio_accepts_exact_deadline_signature_and_key_bytes(minio_tls, key):
    response = minio_tls[2].get(_signed(minio_tls, key))
    assert response.status_code == 200
    assert response.content == PAYLOAD


@pytest.mark.parametrize("mutation", ["key", "bucket", "method", "host", "signature", "expired", "wrong_secret"])
def test_actual_minio_rejects_modified_or_expired_capability(minio_tls, mutation):
    options = {}
    if mutation == "expired":
        old = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=20)
        options = dict(expires_at=old + timedelta(seconds=60), clock=lambda: old)
    if mutation == "wrong_secret":
        options["credentials"] = S3SigningCredentials(access_key=minio_tls[1].access_key, secret_key="wrong-secret-fixture")
    if mutation == "host":
        options["public_origin"] = "https://127.0.0.1:1"
    url = _signed(minio_tls, KEYS[0], **options)
    if mutation == "key":
        url = url.replace("task.toml", "another.toml")
    elif mutation == "bucket":
        url = url.replace("/loom-bundles/", "/other-bundles/")
    elif mutation == "host":
        # Send only to the disposable server; changing a signed Host/port is
        # exactly the invalid internal-to-public rewriting production forbids.
        url = url.replace("https://127.0.0.1:1/", minio_tls[0] + "/", 1)
    elif mutation == "signature":
        prefix, signature = url.rsplit("=", 1)
        url = prefix + "=" + ("0" if signature[0] != "0" else "1") + signature[1:]
    response = minio_tls[2].request("PUT" if mutation == "method" else "GET", url)
    assert response.status_code == 403


@pytest.mark.parametrize("prefix,expected_keys", [("revision/", KEYS), ("revision space+/", (FORM_PREFIX_KEY,))])
@pytest.mark.parametrize("signer", ["sdk", "production"])
def test_actual_minio_listing_pages_preserve_key_bytes_and_continuation(minio_tls, prefix, expected_keys, signer):
    from loom_task_image_authority.bundle_s3_listing import parse_list_objects_v2

    token = None
    objects = []
    for _ in range(3):
        params = {"Bucket": "loom-bundles", "Prefix": prefix, "MaxKeys": 2, "EncodingType": "url"}
        if token is not None:
            params["ContinuationToken"] = token
        # Retain an independent SDK request path alongside the production signer.
        # Neither path establishes the asynchronous production transport yet.
        if signer == "sdk":
            url = minio_tls[3].generate_presigned_url("list_objects_v2", Params=params, ExpiresIn=60)
        else:
            url = presign_bundle_list(
                public_origin=minio_tls[0], bucket="loom-bundles", prefix=prefix,
                maximum_keys=2, continuation_token=token, region="us-east-1", credentials=minio_tls[1],
                expires_at=datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=60),
            )
        response = minio_tls[2].get(url)
        assert response.status_code == 200
        page = parse_list_objects_v2(
            response.content, expected_bucket="loom-bundles", prefix=prefix,
            maximum_keys=2, continuation_token=token, url_encoding="form",
        )
        objects.extend(page.objects)
        token = page.next_token
        if token is None:
            break
    assert token is None
    assert len(objects) == len(expected_keys)
    assert {obj.key for obj in objects} == set(expected_keys)
    assert {obj.size_bytes for obj in objects} == {len(PAYLOAD)}
    for obj in objects:
        response = minio_tls[2].get(_signed(minio_tls, obj.key))
        assert response.status_code == 200
        assert response.content == PAYLOAD


@pytest.mark.parametrize("parameter,value", [
    ("prefix", "revision space+/"), ("max-keys", "1"), ("encoding-type", ""),
    ("continuation-token", "changed-token"), ("list-type", "1"),
])
def test_actual_minio_rejects_changed_signed_listing_parameters(minio_tls, parameter, value):
    url = presign_bundle_list(
        public_origin=minio_tls[0], bucket="loom-bundles", prefix="revision/",
        maximum_keys=2, region="us-east-1", credentials=minio_tls[1],
        expires_at=datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=60),
    )
    parsed = urlsplit(url)
    query = dict(parse_qsl(parsed.query))
    query[parameter] = value
    changed = urlunsplit(parsed._replace(query=urlencode(query)))
    assert minio_tls[2].get(changed).status_code == 403


@pytest.mark.parametrize("prefix,expected_keys", [("revision/", KEYS), ("revision space+/", (FORM_PREFIX_KEY,))])
async def test_actual_minio_async_transport_lists_exact_signed_pages(minio_tls, prefix, expected_keys):
    from loom_task_image_authority.bundle_s3_listing import parse_list_objects_v2
    from loom_task_image_authority.bundle_s3_transport import HTTPSBundleListingReader

    token = None
    objects = []
    # One shared deadline across pages; production inventory/authorization owner
    # is still a separate integration boundary from this actual transport test.
    deadline = asyncio.get_running_loop().time() + 10.0
    expires_at = datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=60)
    async with HTTPSBundleListingReader(origin=minio_tls[0], bucket="loom-bundles", ca_file=minio_tls[4]) as reader:
        for _ in range(3):
            url = presign_bundle_list(
                public_origin=minio_tls[0], bucket="loom-bundles", prefix=prefix,
                maximum_keys=2, continuation_token=token, region="us-east-1",
                credentials=minio_tls[1], expires_at=expires_at,
            )
            payload = await reader.fetch(url, deadline=deadline)
            page = parse_list_objects_v2(
                payload, expected_bucket="loom-bundles", prefix=prefix,
                maximum_keys=2, continuation_token=token, url_encoding="form",
            )
            objects.extend(page.objects)
            token = page.next_token
            if token is None:
                break
    assert token is None
    assert len(objects) == len(expected_keys)
    assert {obj.key for obj in objects} == set(expected_keys)
    assert {obj.size_bytes for obj in objects} == {len(PAYLOAD)}


@pytest.mark.parametrize("prefix,expected_keys", [("revision/", KEYS), ("revision space+/", (FORM_PREFIX_KEY,))])
async def test_actual_minio_backend_owns_inventory_and_exact_get_signing(minio_tls, prefix, expected_keys):
    from loom_task_image_authority.bundle_s3_backend import (
        MinioTaskImageBundleBackend,
        S3InventoryLimits,
    )

    deadline = datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=60)
    async with MinioTaskImageBundleBackend(
        origin=minio_tls[0], bucket="loom-bundles", region="us-east-1",
        credentials=minio_tls[1], ca_file=minio_tls[4], limits=S3InventoryLimits(page_size=2),
    ) as backend:
        objects = await backend.list_objects(
            bucket="loom-bundles", prefix=prefix, maximum_objects=len(expected_keys),
            maximum_bytes=len(expected_keys) * len(PAYLOAD), expires_at=deadline,
        )
        assert {obj.key for obj in objects} == set(expected_keys)
        assert len(objects) == len(expected_keys)
        for obj in objects:
            url = backend.presign_get(bucket="loom-bundles", key=obj.key, expires_at=deadline)
            response = minio_tls[2].get(url)
            assert response.status_code == 200
            assert response.content == PAYLOAD


async def test_actual_minio_backend_rejects_incomplete_or_excessive_inventory(minio_tls):
    from loom_task_image_authority.bundle_s3_backend import (
        MinioTaskImageBundleBackend,
        S3InventoryLimits,
    )

    async with MinioTaskImageBundleBackend(
        origin=minio_tls[0], bucket="loom-bundles", region="us-east-1",
        credentials=minio_tls[1], ca_file=minio_tls[4], limits=S3InventoryLimits(page_size=2),
    ) as backend:
        for changes in ({"maximum_objects": 3}, {"maximum_bytes": len(PAYLOAD)}, {"prefix": "missing/"}):
            options = dict(bucket="loom-bundles", prefix="revision/", maximum_objects=4, maximum_bytes=4 * len(PAYLOAD), expires_at=datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=60))
            options.update(changes)
            with pytest.raises(RuntimeError):
                await backend.list_objects(**options)
