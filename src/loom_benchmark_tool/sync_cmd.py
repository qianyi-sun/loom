"""`loom datasets sync-mirror` — one-way object-store sync.

Copies every object from a *source* S3-compatible bucket (typically
the cluster's internal MinIO) to a *destination* S3-compatible bucket
(typically Cloudflare R2 fronting a public custom domain), skipping
objects that already exist in the destination with matching size.

Intended for the "publish to MinIO / sync to R2 nightly" architecture
in #804: workers read benchmarks from in-cluster MinIO (fast,
authenticated); external consumers read from R2 (public, unlimited
egress). This tool bridges the two.

Cheap idempotency: HEAD-then-conditional-PUT. We don't compute
checksums client-side — S3's ETag semantics differ across providers
(MinIO uses MD5 of multipart parts, R2 uses a bespoke content hash),
so an ETag equality check would produce false negatives. Instead we
skip on exact size match; the publish pipeline is content-addressed
(revision = sha256 of task checksums), so a size-matched object under
a content-addressed key is safe to treat as identical.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


@dataclass(frozen=True)
class SyncStats:
    listed: int
    uploaded: int
    skipped_size_match: int
    bytes_uploaded: int
    bytes_skipped: int


def _build_client(*, endpoint_url: str, access_key: str, secret_key: str) -> Any:
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 5, "mode": "adaptive"},
        ),
    )


def _list_source(
    *,
    client: Any,
    bucket: str,
    prefix: str,
) -> list[tuple[str, int]]:
    """Return [(key, size), ...] for every object under `prefix`."""
    paginator = client.get_paginator("list_objects_v2")
    objects: list[tuple[str, int]] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects.append((obj["Key"], int(obj["Size"])))
    return objects


def _dest_size(*, client: Any, bucket: str, key: str) -> int | None:
    """Return the destination object's size, or None if missing.
    Any other error propagates."""
    try:
        head = client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise
    return int(head["ContentLength"])


def _copy_one(
    *,
    source_client: Any,
    source_bucket: str,
    dest_client: Any,
    dest_bucket: str,
    key: str,
) -> None:
    body = source_client.get_object(Bucket=source_bucket, Key=key)["Body"].read()
    dest_client.put_object(Bucket=dest_bucket, Key=key, Body=body)


def _sync_sync(
    *,
    source_endpoint: str,
    source_access_key: str,
    source_secret_key: str,
    source_bucket: str,
    dest_endpoint: str,
    dest_access_key: str,
    dest_secret_key: str,
    dest_bucket: str,
    prefix: str,
    dry_run: bool,
) -> SyncStats:
    """Blocking implementation — called from the async wrapper via
    `asyncio.to_thread` so we don't block the event loop."""
    source_client = _build_client(
        endpoint_url=source_endpoint,
        access_key=source_access_key,
        secret_key=source_secret_key,
    )
    dest_client = _build_client(
        endpoint_url=dest_endpoint,
        access_key=dest_access_key,
        secret_key=dest_secret_key,
    )

    objects = _list_source(
        client=source_client, bucket=source_bucket, prefix=prefix,
    )
    uploaded = 0
    skipped = 0
    bytes_uploaded = 0
    bytes_skipped = 0
    for key, size in objects:
        existing_size = _dest_size(
            client=dest_client, bucket=dest_bucket, key=key,
        )
        if existing_size == size:
            skipped += 1
            bytes_skipped += size
            continue
        if not dry_run:
            _copy_one(
                source_client=source_client,
                source_bucket=source_bucket,
                dest_client=dest_client,
                dest_bucket=dest_bucket,
                key=key,
            )
        uploaded += 1
        bytes_uploaded += size

    return SyncStats(
        listed=len(objects),
        uploaded=uploaded,
        skipped_size_match=skipped,
        bytes_uploaded=bytes_uploaded,
        bytes_skipped=bytes_skipped,
    )


async def run_sync_mirror(
    *,
    source_endpoint: str,
    source_access_key: str,
    source_secret_key: str,
    source_bucket: str,
    dest_endpoint: str,
    dest_access_key: str,
    dest_secret_key: str,
    dest_bucket: str,
    prefix: str = "",
    dry_run: bool = False,
) -> SyncStats:
    """Sync every object from `source_bucket` (S3-compatible) to
    `dest_bucket` (S3-compatible), skipping objects that already exist
    at the destination with the same size.

    `prefix` restricts the source list to keys under that prefix (e.g.
    a single benchmark_id). `dry_run=True` skips the actual PUTs but
    still returns accurate `listed`/`uploaded`/`skipped` counts.
    """
    return await asyncio.to_thread(
        _sync_sync,
        source_endpoint=source_endpoint,
        source_access_key=source_access_key,
        source_secret_key=source_secret_key,
        source_bucket=source_bucket,
        dest_endpoint=dest_endpoint,
        dest_access_key=dest_access_key,
        dest_secret_key=dest_secret_key,
        dest_bucket=dest_bucket,
        prefix=prefix,
        dry_run=dry_run,
    )
