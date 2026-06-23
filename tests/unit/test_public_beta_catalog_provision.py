from __future__ import annotations

from dataclasses import replace

import pytest

from loom_cli.public_beta_catalog import (
    BenchmarkRow,
    CatalogRows,
    ObjectInfo,
    ProvisionStats,
    TaskRow,
    provision_ready_benchmark_catalog,
)


def _valid_task_config(task_id: str) -> dict[str, object]:
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": task_id},
        "environment": {"os": "linux", "docker_image": "alpine"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "steps": [{"name": "main"}],
    }


class FakeCatalog:
    def __init__(self, rows: CatalogRows | None = None) -> None:
        self.rows = rows or CatalogRows(benchmarks=[], tasks=[])
        self.upserts: list[CatalogRows] = []

    async def load_rows(self) -> CatalogRows:
        return self.rows

    async def upsert_rows(self, rows: CatalogRows) -> None:
        self.upserts.append(rows)
        benchmarks = {row.id: row for row in self.rows.benchmarks}
        tasks = {row.id: row for row in self.rows.tasks}
        benchmarks.update({row.id: row for row in rows.benchmarks})
        tasks.update({row.id: row for row in rows.tasks})
        self.rows = CatalogRows(
            benchmarks=list(benchmarks.values()),
            tasks=list(tasks.values()),
        )


class FakeObjects:
    def __init__(self, objects: dict[tuple[str, str], bytes] | None = None) -> None:
        self.objects = objects or {}
        self.buckets: set[str] = set()

    async def ensure_bucket(self, bucket: str) -> None:
        self.buckets.add(bucket)

    async def list_objects(self, *, bucket: str, prefix: str) -> list[ObjectInfo]:
        return [
            ObjectInfo(bucket=bucket, key=key, size=len(body), etag=str(hash(body)))
            for (obj_bucket, key), body in sorted(self.objects.items())
            if obj_bucket == bucket and key.startswith(prefix)
        ]

    async def head_object(self, *, bucket: str, key: str) -> ObjectInfo | None:
        body = self.objects.get((bucket, key))
        if body is None:
            return None
        return ObjectInfo(bucket=bucket, key=key, size=len(body), etag=str(hash(body)))

    async def get_object(self, *, bucket: str, key: str) -> bytes:
        return self.objects[(bucket, key)]

    async def put_object(self, *, bucket: str, key: str, body: bytes) -> None:
        self.objects[(bucket, key)] = body


@pytest.mark.asyncio
async def test_provision_ready_catalog_filters_blocked_rows_and_copies_missing_objects() -> None:
    source = FakeCatalog(
        CatalogRows(
            benchmarks=[
                BenchmarkRow(
                    id="humaneval",
                    display_name="HumanEval",
                    upstream_kind="huggingface",
                    upstream_locator="openai/openai_humaneval",
                    upstream_revision="main",
                    license_spdx="MIT",
                    license_url="https://example/humaneval",
                    splits=["test"],
                    series=None,
                    imported_by="dev",
                ),
                BenchmarkRow(
                    id="legacy",
                    display_name="Legacy",
                    upstream_kind="huggingface",
                    upstream_locator="example/legacy",
                    upstream_revision="main",
                    license_spdx="MIT",
                    license_url="https://example/legacy",
                    splits=["test"],
                    series=None,
                    imported_by="dev",
                ),
                BenchmarkRow(
                    id="metadata-only",
                    display_name="Metadata only",
                    upstream_kind="huggingface",
                    upstream_locator="example/metadata-only",
                    upstream_revision="main",
                    license_spdx="MIT",
                    license_url="https://example/metadata-only",
                    splits=["test"],
                    series=None,
                    imported_by="dev",
                ),
            ],
            tasks=[
                TaskRow(
                    id="humaneval/HumanEval/0",
                    checksum="a" * 64,
                    config=_valid_task_config("humaneval/HumanEval/0"),
                    source="s3://loom-benchmarks/humaneval/HumanEval/0/",
                    license="MIT",
                    benchmark_id="humaneval",
                    tags={"split": "test"},
                ),
                TaskRow(
                    id="legacy/0",
                    checksum="b" * 64,
                    config={},
                    source="s3://loom-benchmarks/legacy/0/",
                    license="MIT",
                    benchmark_id="legacy",
                    tags={},
                ),
            ],
        ),
    )
    target = FakeCatalog()
    source_objects = FakeObjects({
        ("loom-benchmarks", "humaneval/HumanEval/0/task.toml"): b"task",
        ("loom-benchmarks", "humaneval/HumanEval/0/solution/solve.sh"): b"solve",
        ("loom-benchmarks", "legacy/0/task.toml"): b"legacy",
    })
    target_objects = FakeObjects({
        ("loom-benchmarks", "humaneval/HumanEval/0/task.toml"): b"task",
    })

    stats = await provision_ready_benchmark_catalog(
        source_catalog=source,
        target_catalog=target,
        source_objects=source_objects,
        target_objects=target_objects,
        target_bucket="loom-benchmarks",
        imported_by="public-beta-provision",
    )

    assert stats == ProvisionStats(
        ready_benchmarks=1,
        ready_tasks=1,
        source_objects=2,
        target_objects_uploaded=1,
        target_objects_skipped=1,
        target_objects_missing=0,
        bytes_uploaded=5,
        bytes_skipped=4,
    )
    assert target_objects.buckets == {"loom-benchmarks"}
    assert target.rows.benchmarks == [
        replace(source.rows.benchmarks[0], imported_by="public-beta-provision"),
    ]
    assert [row.id for row in target.rows.tasks] == ["humaneval/HumanEval/0"]
    assert target.rows.tasks[0].source == "s3://loom-benchmarks/humaneval/HumanEval/0/"
    assert ("loom-benchmarks", "legacy/0/task.toml") not in target_objects.objects

    second = await provision_ready_benchmark_catalog(
        source_catalog=source,
        target_catalog=target,
        source_objects=source_objects,
        target_objects=target_objects,
        target_bucket="loom-benchmarks",
        imported_by="public-beta-provision",
    )

    assert second.target_objects_uploaded == 0
    assert second.target_objects_skipped == 2
    assert second.bytes_uploaded == 0
    assert second.bytes_skipped == 9


@pytest.mark.asyncio
async def test_provision_ready_catalog_reports_missing_source_bundles() -> None:
    source = FakeCatalog(
        CatalogRows(
            benchmarks=[
                BenchmarkRow(
                    id="humaneval",
                    display_name="HumanEval",
                    upstream_kind="huggingface",
                    upstream_locator="openai/openai_humaneval",
                    upstream_revision="main",
                    license_spdx="MIT",
                    license_url="https://example/humaneval",
                    splits=["test"],
                    series=None,
                    imported_by="dev",
                ),
            ],
            tasks=[
                TaskRow(
                    id="humaneval/HumanEval/0",
                    checksum="a" * 64,
                    config=_valid_task_config("humaneval/HumanEval/0"),
                    source="s3://loom-benchmarks/humaneval/HumanEval/0/",
                    license="MIT",
                    benchmark_id="humaneval",
                    tags={},
                ),
            ],
        ),
    )

    target = FakeCatalog()

    stats = await provision_ready_benchmark_catalog(
        source_catalog=source,
        target_catalog=target,
        source_objects=FakeObjects(),
        target_objects=FakeObjects(),
        target_bucket="loom-benchmarks",
    )

    assert stats.target_objects_missing == 1
    assert stats.source_objects == 0
    assert target.upserts == []
