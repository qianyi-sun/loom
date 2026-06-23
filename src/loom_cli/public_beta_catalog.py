"""Public-beta benchmark catalog provisioning.

Copies the production-ready benchmark/task catalog from a known-good source
environment into the public-beta target and reconciles the S3 task bundles the
worker needs at runtime. This is intentionally separate from seed_test_data:
it does not create teams, invites, tokens, runs, or fixture artifacts.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, replace
from typing import Any, Protocol, TypeVar

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.benchmark_readiness import (
    BenchmarkAuditSource,
    TaskAuditSource,
    build_readiness_item,
)
from loom.db.schema import Benchmark
from loom.db.schema import Task as TaskModel
from loom.models.task import TaskConfig
from loom_benchmark_tool.db_url import normalize_db_url

POSTGRES_CATALOG_UPSERT_BATCH_SIZE = 1000
T = TypeVar("T")


def _batched(items: list[T], size: int) -> Iterable[list[T]]:
    if size <= 0:
        raise ValueError("batch size must be positive")
    for start in range(0, len(items), size):
        yield items[start:start + size]


@dataclass(frozen=True)
class BenchmarkRow:
    id: str
    display_name: str
    upstream_kind: str
    upstream_locator: str
    upstream_revision: str
    license_spdx: str
    license_url: str
    splits: list[str]
    series: str | None
    imported_by: str | None


@dataclass(frozen=True)
class TaskRow:
    id: str
    checksum: str
    config: dict[str, Any]
    source: str | None
    license: str | None
    benchmark_id: str | None
    tags: dict[str, str]


@dataclass(frozen=True)
class CatalogRows:
    benchmarks: list[BenchmarkRow]
    tasks: list[TaskRow]


@dataclass(frozen=True)
class ObjectInfo:
    bucket: str
    key: str
    size: int
    etag: str | None = None


@dataclass(frozen=True)
class ProvisionStats:
    ready_benchmarks: int
    ready_tasks: int
    source_objects: int
    target_objects_uploaded: int
    target_objects_skipped: int
    target_objects_missing: int
    bytes_uploaded: int
    bytes_skipped: int


class CatalogStore(Protocol):
    async def load_rows(self) -> CatalogRows: ...

    async def upsert_rows(self, rows: CatalogRows) -> None: ...


class CatalogObjectStore(Protocol):
    async def ensure_bucket(self, bucket: str) -> None: ...

    async def list_objects(self, *, bucket: str, prefix: str) -> list[ObjectInfo]: ...

    async def head_object(self, *, bucket: str, key: str) -> ObjectInfo | None: ...

    async def get_object(self, *, bucket: str, key: str) -> bytes: ...

    async def put_object(self, *, bucket: str, key: str, body: bytes) -> None: ...


class PostgresCatalogStore:
    def __init__(self, db_url: str) -> None:
        self.db_url = normalize_db_url(db_url)

    async def load_rows(self) -> CatalogRows:
        engine = create_async_engine(self.db_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with session_factory() as session:
                benchmark_rows = list((await session.scalars(select(Benchmark))).all())
                task_rows = list(
                    (
                        await session.scalars(
                            select(TaskModel).where(TaskModel.benchmark_id.is_not(None)),
                        )
                    ).all(),
                )
        finally:
            await engine.dispose()

        return CatalogRows(
            benchmarks=[
                BenchmarkRow(
                    id=row.id,
                    display_name=row.display_name,
                    upstream_kind=row.upstream_kind,
                    upstream_locator=row.upstream_locator,
                    upstream_revision=row.upstream_revision,
                    license_spdx=row.license_spdx,
                    license_url=row.license_url,
                    splits=list(row.splits),
                    series=row.series,
                    imported_by=row.imported_by,
                )
                for row in benchmark_rows
            ],
            tasks=[
                TaskRow(
                    id=row.id,
                    checksum=row.checksum,
                    config=dict(row.config),
                    source=row.source,
                    license=row.license,
                    benchmark_id=row.benchmark_id,
                    tags=dict(row.tags or {}),
                )
                for row in task_rows
            ],
        )

    async def upsert_rows(self, rows: CatalogRows) -> None:
        if not rows.benchmarks and not rows.tasks:
            return

        engine = create_async_engine(self.db_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with session_factory() as session:
                if rows.benchmarks:
                    for bench_batch in _batched(
                        rows.benchmarks,
                        POSTGRES_CATALOG_UPSERT_BATCH_SIZE,
                    ):
                        bench_values = [
                            {
                                "id": row.id,
                                "display_name": row.display_name,
                                "upstream_kind": row.upstream_kind,
                                "upstream_locator": row.upstream_locator,
                                "upstream_revision": row.upstream_revision,
                                "license_spdx": row.license_spdx,
                                "license_url": row.license_url,
                                "splits": row.splits,
                                "series": row.series,
                                "imported_by": row.imported_by,
                            }
                            for row in bench_batch
                        ]
                        bench_insert = pg_insert(Benchmark).values(bench_values)
                        await session.execute(
                            bench_insert.on_conflict_do_update(
                                index_elements=["id"],
                                set_={
                                    "display_name": (
                                        bench_insert.excluded.display_name
                                    ),
                                    "upstream_kind": (
                                        bench_insert.excluded.upstream_kind
                                    ),
                                    "upstream_locator": (
                                        bench_insert.excluded.upstream_locator
                                    ),
                                    "upstream_revision": (
                                        bench_insert.excluded.upstream_revision
                                    ),
                                    "license_spdx": (
                                        bench_insert.excluded.license_spdx
                                    ),
                                    "license_url": bench_insert.excluded.license_url,
                                    "splits": bench_insert.excluded.splits,
                                    "series": bench_insert.excluded.series,
                                    "imported_by": bench_insert.excluded.imported_by,
                                },
                            ),
                        )

                if rows.tasks:
                    for task_batch in _batched(
                        rows.tasks,
                        POSTGRES_CATALOG_UPSERT_BATCH_SIZE,
                    ):
                        task_values = [
                            {
                                "id": row.id,
                                "checksum": row.checksum,
                                "config": row.config,
                                "source": row.source,
                                "license": row.license,
                                "benchmark_id": row.benchmark_id,
                                "tags": row.tags,
                            }
                            for row in task_batch
                        ]
                        task_insert = pg_insert(TaskModel).values(task_values)
                        await session.execute(
                            task_insert.on_conflict_do_update(
                                index_elements=["id"],
                                set_={
                                    "checksum": task_insert.excluded.checksum,
                                    "config": task_insert.excluded.config,
                                    "source": task_insert.excluded.source,
                                    "license": task_insert.excluded.license,
                                    "benchmark_id": (
                                        task_insert.excluded.benchmark_id
                                    ),
                                    "tags": task_insert.excluded.tags,
                                },
                            ),
                        )
                await session.commit()
        finally:
            await engine.dispose()


class Boto3CatalogObjectStore:
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
            config=Config(signature_version="s3v4"),
        )

    async def ensure_bucket(self, bucket: str) -> None:
        def _do() -> None:
            try:
                self._client.head_bucket(Bucket=bucket)
                return
            except ClientError as exc:
                code = str(exc.response.get("Error", {}).get("Code", ""))
                if code not in {"404", "NoSuchBucket", "NotFound"}:
                    raise
            try:
                self._client.create_bucket(Bucket=bucket)
            except ClientError as exc:
                code = str(exc.response.get("Error", {}).get("Code", ""))
                if code not in {"BucketAlreadyExists", "BucketAlreadyOwnedByYou"}:
                    raise

        await asyncio.to_thread(_do)

    async def list_objects(self, *, bucket: str, prefix: str) -> list[ObjectInfo]:
        def _do() -> list[ObjectInfo]:
            paginator = self._client.get_paginator("list_objects_v2")
            out: list[ObjectInfo] = []
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    out.append(
                        ObjectInfo(
                            bucket=bucket,
                            key=str(obj["Key"]),
                            size=int(obj.get("Size", 0)),
                            etag=_clean_etag(obj.get("ETag")),
                        ),
                    )
            return out

        return await asyncio.to_thread(_do)

    async def head_object(self, *, bucket: str, key: str) -> ObjectInfo | None:
        def _do() -> ObjectInfo | None:
            try:
                obj = self._client.head_object(Bucket=bucket, Key=key)
            except ClientError as exc:
                code = str(exc.response.get("Error", {}).get("Code", ""))
                if code in {"404", "NoSuchKey", "NotFound"}:
                    return None
                raise
            return ObjectInfo(
                bucket=bucket,
                key=key,
                size=int(obj.get("ContentLength", 0)),
                etag=_clean_etag(obj.get("ETag")),
            )

        return await asyncio.to_thread(_do)

    async def get_object(self, *, bucket: str, key: str) -> bytes:
        def _do() -> bytes:
            obj = self._client.get_object(Bucket=bucket, Key=key)
            return bytes(obj["Body"].read())

        return await asyncio.to_thread(_do)

    async def put_object(self, *, bucket: str, key: str, body: bytes) -> None:
        def _do() -> None:
            self._client.put_object(Bucket=bucket, Key=key, Body=body)

        await asyncio.to_thread(_do)


async def provision_ready_benchmark_catalog(
    *,
    source_catalog: CatalogStore,
    target_catalog: CatalogStore,
    source_objects: CatalogObjectStore,
    target_objects: CatalogObjectStore,
    target_bucket: str,
    imported_by: str | None = None,
) -> ProvisionStats:
    source_rows = await source_catalog.load_rows()
    ready_rows = _ready_catalog_rows(source_rows, target_bucket=target_bucket)
    if imported_by is not None:
        ready_rows = replace(
            ready_rows,
            benchmarks=[replace(row, imported_by=imported_by) for row in ready_rows.benchmarks],
        )

    await target_objects.ensure_bucket(target_bucket)
    stats = await _copy_s3_bundle_objects(
        rows=ready_rows,
        source_objects=source_objects,
        target_objects=target_objects,
        target_bucket=target_bucket,
    )
    if stats.target_objects_missing == 0:
        await target_catalog.upsert_rows(ready_rows)
    return stats


def _ready_catalog_rows(rows: CatalogRows, *, target_bucket: str) -> CatalogRows:
    tasks_by_benchmark: dict[str, list[TaskRow]] = defaultdict(list)
    for task in rows.tasks:
        if task.benchmark_id:
            tasks_by_benchmark[task.benchmark_id].append(task)

    ready_benchmarks: list[BenchmarkRow] = []
    ready_tasks: list[TaskRow] = []
    for benchmark in rows.benchmarks:
        tasks = tasks_by_benchmark.get(benchmark.id, [])
        readiness = build_readiness_item(
            BenchmarkAuditSource(
                id=benchmark.id,
                display_name=benchmark.display_name,
                series=benchmark.series,
                upstream_kind=benchmark.upstream_kind,
                upstream_locator=benchmark.upstream_locator,
                upstream_revision=benchmark.upstream_revision,
            ),
            tasks=[
                TaskAuditSource(id=task.id, config=task.config, source=task.source)
                for task in tasks
            ],
            registry_names={benchmark.id},
        )
        if readiness.readiness_state != "runnable":
            continue
        ready_benchmarks.append(benchmark)
        ready_tasks.extend(
            _rewrite_s3_task_source(task, target_bucket=target_bucket)
            for task in tasks
            if _has_valid_task_config(task)
        )

    return CatalogRows(benchmarks=ready_benchmarks, tasks=ready_tasks)


async def _copy_s3_bundle_objects(
    *,
    rows: CatalogRows,
    source_objects: CatalogObjectStore,
    target_objects: CatalogObjectStore,
    target_bucket: str,
) -> ProvisionStats:
    source_infos: dict[tuple[str, str], ObjectInfo] = {}
    missing_prefixes = 0
    for task in rows.tasks:
        parsed = _parse_s3_uri(task.source)
        if parsed is None:
            continue
        bucket, prefix = parsed
        infos = await source_objects.list_objects(bucket=bucket, prefix=prefix)
        if not infos:
            missing_prefixes += 1
            continue
        for info in infos:
            source_infos[(info.bucket, info.key)] = info

    uploaded = 0
    skipped = 0
    bytes_uploaded = 0
    bytes_skipped = 0
    for (bucket, key), info in sorted(source_infos.items()):
        target_info = await target_objects.head_object(bucket=target_bucket, key=key)
        if target_info is not None and _objects_match(info, target_info):
            skipped += 1
            bytes_skipped += info.size
            continue
        body = await source_objects.get_object(bucket=bucket, key=key)
        await target_objects.put_object(bucket=target_bucket, key=key, body=body)
        uploaded += 1
        bytes_uploaded += len(body)

    return ProvisionStats(
        ready_benchmarks=len(rows.benchmarks),
        ready_tasks=len(rows.tasks),
        source_objects=len(source_infos),
        target_objects_uploaded=uploaded,
        target_objects_skipped=skipped,
        target_objects_missing=missing_prefixes,
        bytes_uploaded=bytes_uploaded,
        bytes_skipped=bytes_skipped,
    )


def _has_valid_task_config(task: TaskRow) -> bool:
    try:
        TaskConfig.model_validate(task.config)
    except ValidationError:
        return False
    return True


def _rewrite_s3_task_source(task: TaskRow, *, target_bucket: str) -> TaskRow:
    parsed = _parse_s3_uri(task.source)
    if parsed is None:
        return task
    _source_bucket, prefix = parsed
    return replace(task, source=f"s3://{target_bucket}/{prefix}")


def _parse_s3_uri(source: str | None) -> tuple[str, str] | None:
    if source is None or not source.startswith("s3://"):
        return None
    rest = source[len("s3://"):]
    if "/" not in rest:
        return None
    bucket, key = rest.split("/", 1)
    if not bucket or not key:
        return None
    return bucket, key


def _objects_match(source: ObjectInfo, target: ObjectInfo) -> bool:
    if source.size != target.size:
        return False
    if source.etag and target.etag:
        return source.etag == target.etag
    return True


def _clean_etag(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text.strip('"')
