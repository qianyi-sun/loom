"""Staging benchmark and agent catalog provisioning.

Copies the production-ready benchmark/task catalog from a known-good source
environment into the staging target and reconciles the S3 task bundles the
worker needs at runtime. It also materializes the service-mode agent catalog
into the target `agents` table as an auditable restore snapshot. This is
intentionally separate from seed_test_data: it does not create teams, invites,
tokens, runs, or fixture artifacts.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from typing import Any, Protocol, TypeVar

from botocore.exceptions import ClientError
from pydantic import ValidationError
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.benchmark_readiness import (
    BenchmarkAuditSource,
    TaskAuditSource,
    build_readiness_item,
)
from loom.db.schema import Agent as AgentModel
from loom.db.schema import Benchmark
from loom.db.schema import Task as TaskModel
from loom.models.task import TaskConfig
from loom_benchmark_tool.db_url import normalize_db_url
from loom_service.agent_catalog import AgentEntry, list_agents

POSTGRES_CATALOG_UPSERT_BATCH_SIZE = 1000
AGENT_CATALOG_VERSION = "service-catalog-v2"
AGENT_CATALOG_SPEC_SCHEMA_VERSION = 2
TB21_PROFILE_ID = "terminal-bench-2@tb2.1-r6"
_WORKSPACE_EXEC_BENCHMARK_STEMS = (
    "skillflow",
    "skilllearnbench",
    "swe-bench",
)
T = TypeVar("T")


def _batched(items: list[T], size: int) -> Iterable[list[T]]:
    if size <= 0:
        raise ValueError("batch size must be positive")
    for start in range(0, len(items), size):
        yield items[start : start + size]


@dataclass(frozen=True)
class AgentRow:
    name: str
    version: str
    mode: str
    spec: dict[str, Any]


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
    execution_state: str = "runnable"
    profile_provenance: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TaskRow:
    id: str
    checksum: str
    config: dict[str, Any]
    source: str | None
    license: str | None
    benchmark_id: str | None
    tags: dict[str, str]
    source_provenance: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CatalogRows:
    benchmarks: list[BenchmarkRow]
    tasks: list[TaskRow]
    agents: list[AgentRow] = field(default_factory=list)


@dataclass(frozen=True)
class ObjectInfo:
    bucket: str
    key: str
    size: int
    etag: str | None = None


@dataclass(frozen=True)
class ProvisionStats:
    ready_agents: int
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


def _immutable_profile_provenance(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if key != "activation_audit"}


def _stable_runtime_tags(value: dict[str, str]) -> dict[str, str]:
    return {key: item for key, item in value.items() if key != "runtime_source_mirrored_at"}


def _tb21_benchmark_identity(row: BenchmarkRow | Benchmark) -> dict[str, Any]:
    return {
        "display_name": row.display_name,
        "upstream_kind": row.upstream_kind,
        "upstream_locator": row.upstream_locator,
        "upstream_revision": row.upstream_revision,
        "license_spdx": row.license_spdx,
        "license_url": row.license_url,
        "splits": list(row.splits),
        "series": row.series,
        "profile_provenance": _immutable_profile_provenance(
            dict(row.profile_provenance or {}),
        ),
    }


def _tb21_task_identity(row: TaskRow | TaskModel) -> dict[str, Any]:
    return {
        "checksum": row.checksum,
        "config": row.config,
        "source": row.source,
        "license": row.license,
        "benchmark_id": row.benchmark_id,
        "tags": _stable_runtime_tags(dict(row.tags or {})),
        "source_provenance": dict(row.source_provenance or {}),
    }


def _tb21_profile_drift(
    *,
    existing_benchmark: BenchmarkRow | Benchmark,
    existing_tasks: Iterable[TaskRow | TaskModel],
    incoming_benchmark: BenchmarkRow,
    incoming_tasks: Iterable[TaskRow],
) -> list[str]:
    drift: list[str] = []
    if _tb21_benchmark_identity(existing_benchmark) != _tb21_benchmark_identity(
        incoming_benchmark,
    ):
        drift.append("benchmark identity/provenance")

    existing_by_id = {row.id: row for row in existing_tasks}
    incoming_by_id = {row.id: row for row in incoming_tasks}
    if set(existing_by_id) != set(incoming_by_id):
        drift.append("task set")
    for task_id in sorted(set(existing_by_id) & set(incoming_by_id)):
        if _tb21_task_identity(existing_by_id[task_id]) != _tb21_task_identity(
            incoming_by_id[task_id],
        ):
            drift.append(f"task {task_id}")
    return drift


def _assert_tb21_catalog_rows_absent_or_exact(
    *,
    existing: CatalogRows,
    incoming: CatalogRows,
) -> None:
    """Fail before object copying if target TB2.1 differs from the restore."""

    incoming_benchmark = next(
        (row for row in incoming.benchmarks if row.id == TB21_PROFILE_ID),
        None,
    )
    if incoming_benchmark is None:
        return
    incoming_tasks = [row for row in incoming.tasks if row.benchmark_id == TB21_PROFILE_ID]
    existing_benchmark = next(
        (row for row in existing.benchmarks if row.id == TB21_PROFILE_ID),
        None,
    )
    if existing_benchmark is None:
        incoming_task_ids = {row.id for row in incoming_tasks}
        if any(row.id in incoming_task_ids for row in existing.tasks):
            raise ValueError(
                "immutable TB2.1 task ID collision during catalog provisioning",
            )
        return

    drift = _tb21_profile_drift(
        existing_benchmark=existing_benchmark,
        existing_tasks=(row for row in existing.tasks if row.benchmark_id == TB21_PROFILE_ID),
        incoming_benchmark=incoming_benchmark,
        incoming_tasks=incoming_tasks,
    )
    if drift:
        raise ValueError(
            "immutable TB2.1 profile drift during catalog provisioning in "
            f"{', '.join(drift)}; register a new physical profile instead",
        )


def _tb21_fail_closed_restore_rows(
    *,
    existing: CatalogRows,
    incoming: CatalogRows,
) -> CatalogRows | None:
    """Build the minimal pre-copy write that disables an existing profile."""

    existing_benchmark = next(
        (row for row in existing.benchmarks if row.id == TB21_PROFILE_ID),
        None,
    )
    incoming_benchmark = next(
        (row for row in incoming.benchmarks if row.id == TB21_PROFILE_ID),
        None,
    )
    if existing_benchmark is None or incoming_benchmark is None:
        return None
    return CatalogRows(
        benchmarks=[incoming_benchmark],
        tasks=[row for row in existing.tasks if row.benchmark_id == TB21_PROFILE_ID],
    )


async def _assert_tb21_target_is_absent_or_exact(
    session: AsyncSession,
    *,
    rows: CatalogRows,
) -> None:
    """Prevent generic catalog restore from rewriting the immutable TB2.1 ID."""

    incoming_benchmark = next(
        (row for row in rows.benchmarks if row.id == TB21_PROFILE_ID),
        None,
    )
    if incoming_benchmark is None:
        return
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:profile_id))"),
        {"profile_id": TB21_PROFILE_ID},
    )
    incoming_tasks = sorted(
        (row for row in rows.tasks if row.benchmark_id == TB21_PROFILE_ID),
        key=lambda row: row.id,
    )
    existing_benchmark = await session.scalar(
        select(Benchmark).where(Benchmark.id == TB21_PROFILE_ID).with_for_update(),
    )
    if existing_benchmark is None:
        colliding = await session.scalar(
            select(TaskModel.id).where(TaskModel.id.in_([row.id for row in incoming_tasks])),
        )
        if colliding is not None:
            raise ValueError(
                "immutable TB2.1 task ID collision during catalog provisioning",
            )
        return

    existing_tasks = list(
        (
            await session.scalars(
                select(TaskModel)
                .where(TaskModel.benchmark_id == TB21_PROFILE_ID)
                .order_by(TaskModel.id)
                .with_for_update(),
            )
        ).all()
    )
    drift = _tb21_profile_drift(
        existing_benchmark=existing_benchmark,
        existing_tasks=existing_tasks,
        incoming_benchmark=incoming_benchmark,
        incoming_tasks=incoming_tasks,
    )
    if drift:
        raise ValueError(
            "immutable TB2.1 profile drift during catalog provisioning in "
            f"{', '.join(drift)}; register a new physical profile instead",
        )


class PostgresCatalogStore:
    def __init__(self, db_url: str) -> None:
        self.db_url = normalize_db_url(db_url)

    async def load_rows(self) -> CatalogRows:
        engine = create_async_engine(self.db_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with session_factory() as session:
                agent_rows = list((await session.scalars(select(AgentModel))).all())
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
            agents=[
                AgentRow(
                    name=row.name,
                    version=row.version,
                    mode=row.mode,
                    spec=dict(row.spec),
                )
                for row in agent_rows
            ],
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
                    execution_state=row.execution_state,
                    profile_provenance=dict(row.profile_provenance or {}),
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
                    source_provenance=dict(row.source_provenance or {}),
                )
                for row in task_rows
            ],
        )

    async def upsert_rows(self, rows: CatalogRows) -> None:
        if not rows.agents and not rows.benchmarks and not rows.tasks:
            return

        engine = create_async_engine(self.db_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with session_factory() as session:
                await _assert_tb21_target_is_absent_or_exact(session, rows=rows)
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
                                "execution_state": row.execution_state,
                                "profile_provenance": row.profile_provenance,
                            }
                            for row in bench_batch
                        ]
                        bench_insert = pg_insert(Benchmark).values(bench_values)
                        await session.execute(
                            bench_insert.on_conflict_do_update(
                                index_elements=["id"],
                                set_={
                                    "display_name": (bench_insert.excluded.display_name),
                                    "upstream_kind": (bench_insert.excluded.upstream_kind),
                                    "upstream_locator": (bench_insert.excluded.upstream_locator),
                                    "upstream_revision": (bench_insert.excluded.upstream_revision),
                                    "license_spdx": (bench_insert.excluded.license_spdx),
                                    "license_url": bench_insert.excluded.license_url,
                                    "splits": bench_insert.excluded.splits,
                                    "series": bench_insert.excluded.series,
                                    "imported_by": bench_insert.excluded.imported_by,
                                    "execution_state": bench_insert.excluded.execution_state,
                                    "profile_provenance": (
                                        bench_insert.excluded.profile_provenance
                                    ),
                                },
                            ),
                        )

                if rows.agents:
                    for agent_batch in _batched(
                        rows.agents,
                        POSTGRES_CATALOG_UPSERT_BATCH_SIZE,
                    ):
                        agent_values = [
                            {
                                "name": row.name,
                                "version": row.version,
                                "mode": row.mode,
                                "spec": row.spec,
                            }
                            for row in agent_batch
                        ]
                        agent_insert = pg_insert(AgentModel).values(agent_values)
                        await session.execute(
                            agent_insert.on_conflict_do_update(
                                index_elements=["name", "version"],
                                set_={
                                    "mode": agent_insert.excluded.mode,
                                    "spec": agent_insert.excluded.spec,
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
                                "source_provenance": row.source_provenance,
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
                                    "benchmark_id": (task_insert.excluded.benchmark_id),
                                    "tags": task_insert.excluded.tags,
                                    "source_provenance": (task_insert.excluded.source_provenance),
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
        auth_kind: str = "static_keys",
    ) -> None:
        from loom.storage_credentials import build_s3_client

        self._client = build_s3_client(
            endpoint_url=endpoint_url,
            auth_kind=auth_kind,
            access_key=access_key,
            secret_key=secret_key,
            region=region,
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
    ready_agents = agent_rows_from_service_catalog(imported_by=imported_by)
    ready_rows = replace(ready_rows, agents=ready_agents)
    if imported_by is not None:
        ready_rows = replace(
            ready_rows,
            benchmarks=[replace(row, imported_by=imported_by) for row in ready_rows.benchmarks],
        )
    source_tasks_by_id = {row.id: row for row in source_rows.tasks}
    object_copy_rows = replace(
        ready_rows,
        tasks=[replace(row, source=source_tasks_by_id[row.id].source) for row in ready_rows.tasks],
    )

    target_rows = await target_catalog.load_rows()
    _assert_tb21_catalog_rows_absent_or_exact(
        existing=target_rows,
        incoming=ready_rows,
    )
    fail_closed_rows = _tb21_fail_closed_restore_rows(
        existing=target_rows,
        incoming=ready_rows,
    )
    if fail_closed_rows is not None:
        # Never inspect or replace target bytes while the existing physical
        # profile is runnable. A fresh target-local audit is required after
        # the complete object copy and final catalog upsert.
        await target_catalog.upsert_rows(fail_closed_rows)
    await target_objects.ensure_bucket(target_bucket)
    stats = await _copy_s3_bundle_objects(
        rows=object_copy_rows,
        source_objects=source_objects,
        target_objects=target_objects,
        target_bucket=target_bucket,
    )
    if stats.target_objects_missing == 0:
        await target_catalog.upsert_rows(ready_rows)
    return stats


def _ready_catalog_rows(rows: CatalogRows, *, target_bucket: str) -> CatalogRows:
    _require_republished_workspace_contracts(rows.tasks)
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
        if benchmark.id == TB21_PROFILE_ID:
            # Activation evidence is environment-local: copied object bytes
            # must receive a fresh target audit before any selector can run.
            benchmark = replace(
                benchmark,
                execution_state="pending",
                profile_provenance=_immutable_profile_provenance(
                    benchmark.profile_provenance,
                ),
            )
        ready_benchmarks.append(benchmark)
        ready_tasks.extend(
            _rewrite_s3_task_source(task, target_bucket=target_bucket)
            for task in tasks
            if _has_valid_task_config(task)
        )

    return CatalogRows(benchmarks=ready_benchmarks, tasks=ready_tasks)


def _requires_workspace_exec(benchmark_id: str | None) -> bool:
    if benchmark_id is None:
        return False
    return any(
        benchmark_id == stem
        or benchmark_id.startswith(f"{stem}-")
        or benchmark_id.startswith(f"{stem}@")
        for stem in _WORKSPACE_EXEC_BENCHMARK_STEMS
    )


def _require_republished_workspace_contracts(tasks: Iterable[TaskRow]) -> None:
    stale: list[str] = []
    for task in tasks:
        if not _requires_workspace_exec(task.benchmark_id):
            continue
        try:
            config = TaskConfig.model_validate(task.config)
        except ValidationError:
            continue
        if "workspace_exec" not in config.required_agent_capabilities:
            stale.append(task.id)
    if stale:
        raise ValueError(
            "republish workspace benchmark tasks with "
            "required_agent_capabilities=[\"workspace_exec\"] before "
            f"catalog provisioning; stale task examples: {sorted(stale)[:5]}",
        )


def agent_rows_from_service_catalog(
    *,
    imported_by: str | None = None,
    entries: Iterable[AgentEntry] | None = None,
) -> list[AgentRow]:
    """Materialize the service-mode agent catalog into DB rows.

    `loom_service.agent_catalog` remains the source of truth for supported
    agents. Staging/staging provisioning writes an auditable DB snapshot so
    restore drills can prove benchmarks, tasks, and agents were restored
    through the same official operator path.
    """
    out: list[AgentRow] = []
    for entry in entries if entries is not None else list_agents():
        spec = dict(entry.to_dict())
        spec["catalog_provenance"] = {
            "source": "loom_service.agent_catalog",
            "schema_version": AGENT_CATALOG_SPEC_SCHEMA_VERSION,
            "provisioned_by": imported_by,
        }
        out.append(
            AgentRow(
                name=entry.name,
                version=AGENT_CATALOG_VERSION,
                mode=entry.kind,
                spec=spec,
            )
        )
    return out


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
        ready_agents=len(rows.agents),
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
    rest = source[len("s3://") :]
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
