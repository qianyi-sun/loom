"""Actual S3 signature verification by pinned disposable MinIO over verified TLS."""

import asyncio
import ipaddress
import json
import os
import ssl
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import UUID

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


@pytest.mark.parametrize("prefix,expected_keys", [("revision/", KEYS), ("revision space+/", (FORM_PREFIX_KEY,))])
async def test_actual_minio_async_provider_returns_only_exact_object_capabilities(minio_tls, prefix, expected_keys):
    from loom.task_image_build_plan import TaskImageBuildComponentV1, TaskImageBuildPlanV1
    from loom_task_image_authority.bundle_capability import AsyncTaskImageBundleCapabilityProvider
    from loom_task_image_authority.bundle_s3_backend import (
        MinioTaskImageBundleBackend,
        S3InventoryLimits,
    )

    now = datetime.now(UTC)
    plan = TaskImageBuildPlanV1(
        grant_id=UUID("11111111-1111-4111-8111-111111111111"),
        session_id=UUID("22222222-2222-4222-8222-222222222222"), session_generation=1,
        materialization_id=UUID("33333333-3333-4333-8333-333333333333"),
        builder_id="rootless:22222222222242228222222222222222", task_id="fixture/task",
        task_checksum="4" * 64, cpu_arch="arm64", platform="linux/arm64",
        bundle_bucket="loom-bundles", bundle_prefix=prefix, bundle_file_metadata_sha256="5" * 64,
        bundle_file_limit=2000, bundle_byte_limit=536870912, build_timeout_seconds=900.0,
        authorization_expires_at=now + timedelta(seconds=60),
        components=(TaskImageBuildComponentV1(name="task", dockerfile_path="environment/Dockerfile", context_path=".", oci_output_path="oci/0000.tar"),),
    )
    async with MinioTaskImageBundleBackend(
        origin=minio_tls[0], bucket="loom-bundles", region="us-east-1",
        credentials=minio_tls[1], ca_file=minio_tls[4], limits=S3InventoryLimits(page_size=2),
    ) as backend:
        provider = AsyncTaskImageBundleCapabilityProvider(
            backend=backend, public_https_origin=minio_tls[0], expected_bucket="loom-bundles",
            maximum_objects=2000, maximum_bytes=536870912, url_expiry_seconds=60,
            addressing_style="path",
        )
        capability = await provider.issue(plan, now=now)
    assert capability.file_count == len(expected_keys)
    assert capability.total_bytes == len(expected_keys) * len(PAYLOAD)
    assert capability.expires_at == plan.authorization_expires_at.replace(microsecond=0)
    assert {prefix + obj.relative_path for obj in capability.objects} == set(expected_keys)
    for obj in capability.objects:
        assert "list-type" not in dict(parse_qsl(urlsplit(obj.url).query))
        response = minio_tls[2].get(obj.url)
        assert response.status_code == 200
        assert response.content == PAYLOAD


@pytest.mark.parametrize("state", ["valid", "corrupt", "missing"])
async def test_actual_minio_verifies_exact_registered_manifest(minio_tls, tmp_path, state):
    from loom.task_image_bundle_manifest import (
        capture_task_image_bundle_manifest,
        task_image_bundle_manifest_key,
    )
    from loom_task_image_authority.bundle_s3_backend import MinioTaskImageBundleBackend

    # State-specific captured content gives each case an independent digest key.
    (tmp_path / "Dockerfile").write_bytes(f"FROM scratch\n# {state}\n".encode())
    manifest = capture_task_image_bundle_manifest(tmp_path)
    if state != "missing":
        payload = manifest.canonical_bytes
        if state == "corrupt":
            payload = payload.replace(b"Dockerfile", b"Dockerfild")
        minio_tls[3].put_object(
            Bucket="loom-bundles", Key=task_image_bundle_manifest_key(manifest.digest), Body=payload,
        )
    async with MinioTaskImageBundleBackend(
        origin=minio_tls[0], bucket="loom-bundles", region="us-east-1",
        credentials=minio_tls[1], ca_file=minio_tls[4],
    ) as backend:
        request = backend.get_manifest(
            bucket="loom-bundles", expected_sha256=manifest.digest,
            task_checksum=manifest.task_checksum,
            bundle_file_metadata_sha256=manifest.bundle_file_metadata_sha256,
            expires_at=datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=60),
        )
        if state == "valid":
            assert await request == manifest
        else:
            with pytest.raises(RuntimeError):
                await request


@pytest.mark.parametrize("change", ["none", "missing_data", "extra_data", "sidecar_size"])
async def test_verified_upload_and_real_minio_inventory_match_registered_manifest(minio_tls, tmp_path, change):
    import hashlib

    from loom.task_image_build_plan import TaskImageBuildPlanV2
    from loom.task_image_bundle_manifest import capture_task_image_bundle_manifest
    from loom.trajectory.storage import BUNDLE_FILE_METADATA_NAME
    from loom_benchmark_tool.upload import upload_task_dir
    from loom_task_image_authority.bundle_capability import AsyncTaskImageBundleCapabilityProvider
    from loom_task_image_authority.bundle_s3_backend import (
        MinioTaskImageBundleBackend,
        S3InventoryLimits,
    )
    from tests.unit.test_task_image_bundle_capability import _plan

    (tmp_path / "Dockerfile").write_bytes(f"FROM scratch\n# {change}\n".encode())
    script = tmp_path / "run +%😀.sh"
    script.write_bytes(b"#!/bin/sh\nexit 0\n")
    script.chmod(0o755)
    manifest = capture_task_image_bundle_manifest(tmp_path)
    prefix = f"registered/{manifest.digest}/"

    class FixtureWriter:
        # Only adapt ObjectStore's write call to the fixture's explicitly trusted
        # SDK. Upload/capture and all authority-side signing/TLS/listing are real.
        async def put_object(self, *, bucket, key, body):
            return minio_tls[3].put_object(Bucket=bucket, Key=key, Body=body)["ETag"]

    assert await upload_task_dir(store=FixtureWriter(), bucket="loom-bundles", prefix=prefix, task_dir=tmp_path, content_manifest=manifest) == 2
    if change == "missing_data":
        minio_tls[3].delete_object(Bucket="loom-bundles", Key=prefix + "Dockerfile")
    elif change == "extra_data":
        minio_tls[3].put_object(Bucket="loom-bundles", Key=prefix + "unexpected", Body=b"")
    elif change == "sidecar_size":
        minio_tls[3].put_object(Bucket="loom-bundles", Key=prefix + BUNDLE_FILE_METADATA_NAME, Body=b"bad")
    async with MinioTaskImageBundleBackend(
        origin=minio_tls[0], bucket="loom-bundles", region="us-east-1",
        credentials=minio_tls[1], ca_file=minio_tls[4], limits=S3InventoryLimits(page_size=1),
    ) as backend:
        request = backend.get_verified_bundle_manifest(
            bucket="loom-bundles", prefix=prefix, expected_sha256=manifest.digest,
            task_checksum=manifest.task_checksum, bundle_file_metadata_sha256=manifest.bundle_file_metadata_sha256,
            maximum_objects=2, maximum_bytes=sum(item.size_bytes for item in manifest.files),
            expires_at=datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=60),
        )
        if change == "none":
            assert await request == manifest
        else:
            with pytest.raises(RuntimeError):
                await request
        now = datetime.now(UTC)
        plan = TaskImageBuildPlanV2.model_validate(dict(
            _plan().model_dump(), schema_version="loom.task-image-build-plan.v2",
            bundle_prefix=prefix, bundle_content_manifest_sha256=manifest.digest,
            task_checksum=manifest.task_checksum, bundle_file_metadata_sha256=manifest.bundle_file_metadata_sha256,
            authorization_expires_at=now + timedelta(seconds=60),
        ))
        provider = AsyncTaskImageBundleCapabilityProvider(
            backend=backend, public_https_origin=minio_tls[0], expected_bucket="loom-bundles",
            maximum_objects=2, maximum_bytes=sum(item.size_bytes for item in manifest.files),
            url_expiry_seconds=60, addressing_style="path",
        )
        if change != "none":
            with pytest.raises(RuntimeError):
                await provider.issue(plan, now=now)
        else:
            capability = await provider.issue(plan, now=now)
            assert capability.schema_version == "loom.task-image-bundle-capability.v2"
            assert capability.content_manifest == manifest
            assert capability.file_count == 2  # Sidecar overhead does not consume data quota.
            provider.validate(capability, plan, now=datetime.now(UTC))
            for item in capability.objects:
                # Real signed URLs, including exact plus/percent/Unicode paths.
                response = minio_tls[2].get(item.url)
                assert response.status_code == 200
                assert len(response.content) == item.size_bytes
                assert hashlib.sha256(response.content).hexdigest() == item.sha256


async def test_verified_upload_authority_capability_and_real_go_downloader(minio_tls, tmp_path):
    from loom.task_image_build_plan import TaskImageBuildPlanV2
    from loom.task_image_bundle_manifest import capture_task_image_bundle_manifest
    from loom_benchmark_tool.upload import upload_task_dir
    from loom_task_image_authority.bundle_capability import AsyncTaskImageBundleCapabilityProvider
    from loom_task_image_authority.bundle_s3_backend import MinioTaskImageBundleBackend
    from tests.unit.test_task_image_bundle_capability import _plan

    source = tmp_path / "source"
    source.mkdir()
    (source / "environment").mkdir()
    (source / "environment/Dockerfile").write_bytes(b"FROM scratch\n")
    script = source / 'café +%<"\u2028\u2029😀.sh'
    script.write_bytes(b"#!/bin/sh\nexit 0\n")
    script.chmod(0o755)
    manifest = capture_task_image_bundle_manifest(source)
    prefix = f"native-go/{manifest.digest}/"

    class FixtureWriter:
        async def put_object(self, *, bucket, key, body):
            return minio_tls[3].put_object(Bucket=bucket, Key=key, Body=body)["ETag"]

    assert await upload_task_dir(store=FixtureWriter(), bucket="loom-bundles", prefix=prefix, task_dir=source, content_manifest=manifest) == 2
    now = datetime.now(UTC)
    plan = TaskImageBuildPlanV2.model_validate(dict(
        _plan().model_dump(), schema_version="loom.task-image-build-plan.v2", bundle_prefix=prefix,
        bundle_content_manifest_sha256=manifest.digest, task_checksum=manifest.task_checksum,
        bundle_file_metadata_sha256=manifest.bundle_file_metadata_sha256, authorization_expires_at=now + timedelta(minutes=10),
    ))
    async with MinioTaskImageBundleBackend(origin=minio_tls[0], bucket=plan.bundle_bucket, region="us-east-1", credentials=minio_tls[1], ca_file=minio_tls[4]) as backend:
        provider = AsyncTaskImageBundleCapabilityProvider(
            backend=backend, public_https_origin=minio_tls[0], expected_bucket=plan.bundle_bucket,
            maximum_objects=2, maximum_bytes=1024, url_expiry_seconds=600, addressing_style="path",
        )
        capability = await provider.issue(plan, now=now)
    capability_path = tmp_path / "capability.json"
    capability_path.write_text(capability.model_dump_json())
    capability_path.chmod(0o600)
    fixture = tmp_path / "fixture.json"
    fixture.write_text(json.dumps(dict(
        Plan=dict(GrantID=str(plan.grant_id), MaterializationID=str(plan.materialization_id), TaskChecksum=plan.task_checksum,
                  ManifestSHA256=plan.bundle_content_manifest_sha256, MetadataSHA256=plan.bundle_file_metadata_sha256,
                  Bucket=plan.bundle_bucket, Prefix=plan.bundle_prefix, FileLimit=plan.bundle_file_limit, ByteLimit=plan.bundle_byte_limit),
        Session=dict(SessionID=str(plan.session_id), Generation=plan.session_generation, ExpiresAt=plan.authorization_expires_at.isoformat()),
        Origin=minio_tls[0], CAFile=str(minio_tls[4]), CapabilityFile=str(capability_path),
    )))
    repo = Path(__file__).resolve().parents[2]
    result = subprocess.run([
        "docker", "run", "--rm", "--network", "host", "--user", f"{os.getuid()}:{os.getgid()}",
        "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
        "-v", f"{repo}:/src:ro", "-v", f"{tmp_path}:{tmp_path}:ro", "-v", f"{minio_tls[4]}:{minio_tls[4]}:ro",
        "-e", "GOCACHE=/tmp/native-go-cache", "-e", "GOMODCACHE=/tmp/native-go-mod",
        "-e", f"LOOM_REGISTERED_BUNDLE_FIXTURE={fixture}", "-w", "/src",
        "golang@sha256:95db116434e3f21a2a15600ffc7169bf380c6bfd021b154d106fcb346721c277",
        "go", "test", "./cmd/loom-task-image-builder-supervisor", "-run", "^TestRegisteredBundleExternalMinIO$", "-count=1", "-timeout=30s", "-v",
    ], cwd=repo, capture_output=True, text=True, timeout=100, check=False)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "--- PASS: TestRegisteredBundleExternalMinIO" in result.stdout
