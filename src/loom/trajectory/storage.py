"""ObjectStore Protocol + FakeObjectStore + boto3-backed MinioObjectStore.

The trajectory writer is the only producer; it uses multipart upload for
flushed-chunk streaming. ATIF docs are uploaded via single-shot put_object.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, cast

import boto3
from botocore.config import Config


def _has_traversal(rel: str) -> bool:
    """True if a relative key contains a `..` segment, an absolute root,
    or a drive-letter prefix. Used by download_prefix to reject keys that
    would escape `out_dir` once joined."""
    parts = Path(rel).parts
    if not parts:
        return False
    if parts[0] in ("/", "\\") or (len(parts[0]) == 2 and parts[0][1] == ":"):
        return True
    return ".." in parts


@dataclass
class MultipartUpload:
    bucket: str
    key: str
    upload_id: str
    parts: list[tuple[int, str]] = field(default_factory=list)  # (part_number, etag)


class ObjectStore(Protocol):
    """Trajectory + artifact storage. Multipart for streaming writes; put_object
    for one-shot uploads; presign_put for client-driven uploads (workers ship
    artifacts to MinIO via signed URLs).
    """

    async def create_multipart_upload(
        self, *, bucket: str, key: str,
    ) -> MultipartUpload: ...

    async def upload_part(
        self, upload: MultipartUpload, *, part_number: int, body: bytes,
    ) -> None: ...

    async def complete_multipart_upload(self, upload: MultipartUpload) -> str:
        """Returns the final object URI (s3://bucket/key)."""
        ...

    async def abort_multipart_upload(self, upload: MultipartUpload) -> None: ...

    async def put_object(self, *, bucket: str, key: str, body: bytes) -> str: ...

    async def get_object(self, *, bucket: str, key: str) -> bytes: ...

    async def presign_put(
        self, *, bucket: str, key: str, expires_sec: int,
    ) -> str: ...

    async def download_prefix(
        self, *, bucket: str, prefix: str, out_dir: Path,
    ) -> int:
        """List every object under `prefix` in `bucket` and stream each to
        `out_dir` preserving the relative path. Returns the count of
        objects downloaded.

        `prefix` must be non-empty — an empty string would match every
        key in the bucket, which would let a misconfigured bundle source
        (`s3://bucket/` or `s3://bucket`) drain the entire bucket into
        one trial workspace. Implementations MUST raise ValueError on
        empty prefix. Keys containing `..` segments after the prefix is
        stripped are skipped to prevent path traversal out of `out_dir`.

        Used by the worker's `_materialize_task_dir` to pull a benchmark-
        imported task's fixture content (task.toml + instruction.md +
        solution/ + tests/ + environment/) from MinIO before the trial
        runs. Plan 13 Task 2 / agent integrations spec §6.3."""
        ...


@dataclass
class FakeObjectStore:
    """In-memory ObjectStore for unit tests. Maps (bucket, key) → bytes."""

    objects: dict[tuple[str, str], bytes] = field(default_factory=dict)
    _multiparts: dict[str, list[tuple[int, bytes]]] = field(default_factory=dict)
    _next_upload_id: int = 0

    async def create_multipart_upload(
        self, *, bucket: str, key: str,
    ) -> MultipartUpload:
        self._next_upload_id += 1
        upload_id = f"upload-{self._next_upload_id}"
        self._multiparts[upload_id] = []
        return MultipartUpload(bucket=bucket, key=key, upload_id=upload_id)

    async def upload_part(
        self, upload: MultipartUpload, *, part_number: int, body: bytes,
    ) -> None:
        self._multiparts[upload.upload_id].append((part_number, body))
        upload.parts.append((part_number, f"etag-{part_number}"))

    async def complete_multipart_upload(self, upload: MultipartUpload) -> str:
        parts = sorted(self._multiparts.pop(upload.upload_id), key=lambda p: p[0])
        self.objects[(upload.bucket, upload.key)] = b"".join(p[1] for p in parts)
        return f"s3://{upload.bucket}/{upload.key}"

    async def abort_multipart_upload(self, upload: MultipartUpload) -> None:
        self._multiparts.pop(upload.upload_id, None)

    async def put_object(self, *, bucket: str, key: str, body: bytes) -> str:
        self.objects[(bucket, key)] = body
        return f"s3://{bucket}/{key}"

    async def get_object(self, *, bucket: str, key: str) -> bytes:
        if (bucket, key) not in self.objects:
            raise KeyError(f"s3://{bucket}/{key}")
        return self.objects[(bucket, key)]

    async def presign_put(
        self, *, bucket: str, key: str, expires_sec: int,
    ) -> str:
        return f"https://fake/{bucket}/{key}?expires_sec={expires_sec}"

    async def download_prefix(
        self, *, bucket: str, prefix: str, out_dir: Path,
    ) -> int:
        """Stream every (bucket, key) where key startswith `prefix` into
        `out_dir`, preserving the suffix path. Used by the worker's
        materialize_task_dir test path."""
        if not prefix:
            raise ValueError(
                "download_prefix requires a non-empty prefix; refusing "
                "to drain entire bucket",
            )
        count = 0
        out_dir.mkdir(parents=True, exist_ok=True)
        for (b, k), body in self.objects.items():
            if b != bucket or not k.startswith(prefix):
                continue
            rel = k[len(prefix):].lstrip("/")
            if not rel or _has_traversal(rel):
                continue
            dest = out_dir / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(body)
            count += 1
        return count


class MinioObjectStore:
    """boto3-backed ObjectStore against a MinIO endpoint.

    Operations are blocking; we use asyncio.to_thread to keep them off the
    event loop. Configure via constructor; do NOT read env vars here (config
    loading lives in `loom_control_plane.config` and `loom_worker.config`).
    """

    def __init__(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
    ) -> None:
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=Config(signature_version="s3v4", retries={"max_attempts": 3}),
        )

    async def create_multipart_upload(
        self, *, bucket: str, key: str,
    ) -> MultipartUpload:
        def _do() -> str:
            return cast(str, self._client.create_multipart_upload(
                Bucket=bucket, Key=key,
            )["UploadId"])
        upload_id = await asyncio.to_thread(_do)
        return MultipartUpload(bucket=bucket, key=key, upload_id=upload_id)

    async def upload_part(
        self, upload: MultipartUpload, *, part_number: int, body: bytes,
    ) -> None:
        def _do() -> str:
            resp = self._client.upload_part(
                Bucket=upload.bucket, Key=upload.key,
                PartNumber=part_number, UploadId=upload.upload_id,
                Body=body,
            )
            return cast(str, resp["ETag"])
        etag = await asyncio.to_thread(_do)
        upload.parts.append((part_number, etag))

    async def complete_multipart_upload(self, upload: MultipartUpload) -> str:
        def _do() -> None:
            self._client.complete_multipart_upload(
                Bucket=upload.bucket, Key=upload.key,
                UploadId=upload.upload_id,
                MultipartUpload={
                    "Parts": [
                        {"PartNumber": pn, "ETag": etag}
                        for pn, etag in sorted(upload.parts, key=lambda p: p[0])
                    ],
                },
            )
        await asyncio.to_thread(_do)
        return f"s3://{upload.bucket}/{upload.key}"

    async def abort_multipart_upload(self, upload: MultipartUpload) -> None:
        def _do() -> None:
            self._client.abort_multipart_upload(
                Bucket=upload.bucket, Key=upload.key, UploadId=upload.upload_id,
            )
        await asyncio.to_thread(_do)

    async def put_object(self, *, bucket: str, key: str, body: bytes) -> str:
        def _do() -> None:
            self._client.put_object(Bucket=bucket, Key=key, Body=body)
        await asyncio.to_thread(_do)
        return f"s3://{bucket}/{key}"

    async def get_object(self, *, bucket: str, key: str) -> bytes:
        def _do() -> bytes:
            resp = self._client.get_object(Bucket=bucket, Key=key)
            return cast(bytes, resp["Body"].read())
        return await asyncio.to_thread(_do)

    async def presign_put(
        self, *, bucket: str, key: str, expires_sec: int,
    ) -> str:
        def _do() -> str:
            return cast(str, self._client.generate_presigned_url(
                "put_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=expires_sec,
            ))
        return await asyncio.to_thread(_do)

    async def download_prefix(
        self, *, bucket: str, prefix: str, out_dir: Path,
    ) -> int:
        """List + download every object under `prefix`. Uses S3's paginator
        because large benchmarks (SWE-Bench Verified is ~500 instances)
        may have thousands of objects per task; the default list_objects
        returns 1000-key pages."""
        if not prefix:
            raise ValueError(
                "download_prefix requires a non-empty prefix; refusing "
                "to drain entire bucket",
            )
        def _do() -> int:
            paginator = self._client.get_paginator("list_objects_v2")
            count = 0
            out_dir.mkdir(parents=True, exist_ok=True)
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    rel = key[len(prefix):].lstrip("/")
                    if not rel or _has_traversal(rel):
                        continue
                    dest = out_dir / rel
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    self._client.download_file(bucket, key, str(dest))
                    count += 1
            return count
        return await asyncio.to_thread(_do)
