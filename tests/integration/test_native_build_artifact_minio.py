"""Empirical conditional multipart completion on the repository's MinIO fixture."""

import asyncio
import hashlib

import boto3
import pytest
from botocore.config import Config

from loom_capacity_agent.build_admission import BuildArtifactV1, native_build_artifact_key
from tests.unit.test_native_build_artifact_writer import inputs

pytestmark = pytest.mark.docker


async def test_native_artifact_exact_replay_cannot_overwrite_real_minio(shared_minio, monkeypatch):
    config = shared_minio.get_config()
    objects = boto3.client("s3", endpoint_url="http://" + config["endpoint"],
        aws_access_key_id=config["access_key"], aws_secret_access_key=config["secret_key"],
        region_name="us-east-1", config=Config(connect_timeout=5, read_timeout=10, retries={"max_attempts": 1}))
    module, writer, claim, source, _authorize, _fake, _artifact = inputs(monkeypatch)
    monkeypatch.setattr(writer, "_objects", objects)
    objects.create_bucket(Bucket=source.object_bucket)
    data = b"a" * (9 * 1024 * 1024)
    artifact = BuildArtifactV1(archive_size_bytes=len(data), archive_sha256=hashlib.sha256(data).hexdigest())
    async def chunks(payload):
        for offset in range(0, len(payload), module.MAX_ARTIFACT_CHUNK_BYTES):
            yield payload[offset:offset + module.MAX_ARTIFACT_CHUNK_BYTES]
    try:
        results = await asyncio.gather(*(writer.write(claim, worker_credential="x" * 43,
            artifact=artifact, chunks=chunks(data)) for _ in range(2)))
        assert results == [artifact, artifact]
        changed = b"b" * len(data)
        other = BuildArtifactV1(archive_size_bytes=len(changed), archive_sha256=hashlib.sha256(changed).hexdigest())
        results = await asyncio.gather(
            writer.write(claim, worker_credential="x" * 43, artifact=other, chunks=chunks(changed)),
            writer.write(claim, worker_credential="x" * 43, artifact=artifact, chunks=chunks(data)))
        assert results == [other, artifact]
        assert native_build_artifact_key(claim, artifact) != native_build_artifact_key(claim, other)
        response = objects.get_object(Bucket=source.object_bucket, Key=native_build_artifact_key(claim, artifact))
        try:
            assert response["Body"].read() == data
        finally:
            response["Body"].close()
        assert not objects.list_multipart_uploads(Bucket=source.object_bucket).get("Uploads")
    finally:
        await writer.aclose()
        objects.close()
