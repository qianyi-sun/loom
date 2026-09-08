"""Production registration paths preserve/recreate image prerequisites."""

from __future__ import annotations

from copy import deepcopy

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.config.benchmarks import load_benchmarks_config
from loom.db.schema import Task, TaskImageMaterialization
from loom.trajectory.storage import FakeObjectStore
from loom_benchmark_tool.register_cmd import run_register
from loom_cli.benchmarks_sync import sync
from loom_cli.catalog_provision import CatalogRows, PostgresCatalogStore, TaskRow
from loom_cli.local_benchmark_publish import publish_local_benchmark
from tests.integration.test_benchmarks_sync_local import _write_bundle, _write_toml
from tests.integration.test_local_benchmark_publish import _write_environment_path_mismatch_layout
from tests.integration.test_tb21_publish_register_audit import _manifest


def _dockerfile_config(task_id):
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": task_id},
        "environment": {
            "os": "linux",
            "cpu_arch": "x86_64",
            "dockerfile": "environment/Dockerfile",
        },
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "steps": [{"name": "main"}],
    }


@pytest.mark.parametrize("writer", ["sync", "publish", "catalog", "register"])
async def test_registration_queues_and_reregistration_restores_retired_images(
    isolated_migration_postgres_url, tmp_path, writer
):
    database_url = isolated_migration_postgres_url
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    if writer == "sync":
        fixtures_root = tmp_path / "fixtures"
        bundle = _write_bundle(fixtures_root / "team-evals", "alpha")
        task_toml = bundle / "task.toml"
        task_toml.write_text(
            task_toml.read_text().replace(
                'docker_image = "python:3.11-alpine"', 'dockerfile = "environment/Dockerfile"'
            )
        )
        (bundle / "environment").mkdir()
        (bundle / "environment" / "Dockerfile").write_text("FROM scratch\n")
        config_path = tmp_path / "benchmarks.toml"
        _write_toml(config_path, "team-evals")
        config = load_benchmarks_config(config_path)

        async def register(*, dry_run=False):
            async with sessions() as session:
                return await sync(
                    config,
                    fixtures_root=fixtures_root,
                    session=session,
                    registry_names=set(),
                    dry_run=dry_run,
                )
    elif writer == "publish":
        root = tmp_path / "local"
        _write_environment_path_mismatch_layout(root)
        (root / "tasks" / "app-path-missing" / "environment" / "Dockerfile").write_text(
            "FROM scratch\n"
        )
        store = FakeObjectStore()

        async def register():
            return await publish_local_benchmark(
                root, db_url=database_url, object_store=store, bucket="test-bundles"
            )
    elif writer == "catalog":
        rows = CatalogRows(
            benchmarks=[],
            tasks=[
                TaskRow(
                    id="catalog-task",
                    checksum="a" * 64,
                    config=_dockerfile_config("catalog-task"),
                    source="s3://test-bundles/catalog-task",
                    license="MIT",
                    benchmark_id=None,
                    tags={},
                )
            ],
        )

        async def register():
            return await PostgresCatalogStore(database_url).upsert_rows(rows)
    else:
        manifest = deepcopy(_manifest())
        task = manifest["tasks"][0]
        task["task_config"]["environment"] = _dockerfile_config("unused")["environment"]
        task["source_provenance"]["image_provenance"].update(
            docker_image=None, dockerfile="environment/Dockerfile", cpu_arch="x86_64"
        )

        async def register():
            return await run_register(
                benchmark="terminal-bench-2",
                source="object-store",
                revision="test-revision",
                object_store=FakeObjectStore(),
                db_url=database_url,
                manifest=manifest,
            )

    try:
        await register()
        async with sessions() as session:
            task = (await session.scalars(select(Task))).one()
            images = list(await session.scalars(select(TaskImageMaterialization)))
            assert images, "registered Dockerfile task has no durable image prerequisites"
            assert all(row.state == "queued" and row.task_id == task.id for row in images)
            before = {
                row.id: (row.materialization_key, row.task_checksum, row.cpu_arch) for row in images
            }
            # Simulate previously retired prerequisites. This tests registration
            # recovery, not the separate pending retirement/reference transaction.
            for row in images:
                row.state = "retired"
                row.registry_images = {}
                row.ready_at = None
            await session.commit()
        if writer == "sync":
            await register(dry_run=True)
            async with sessions() as session:
                assert set(await session.scalars(select(TaskImageMaterialization.state))) == {
                    "retired"
                }
        result = await register()
        async with sessions() as session:
            images = list(await session.scalars(select(TaskImageMaterialization)))
            assert {
                row.id: (row.materialization_key, row.task_checksum, row.cpu_arch) for row in images
            } == before
            assert all(row.state == "queued" and row.registry_images == {} for row in images)
        if writer == "register":
            assert result["registered"] == 0 and result["skipped"] == 1
        elif writer == "publish":
            assert result.inserted == 0 and result.unchanged == 1
        elif writer == "sync":
            assert result.tasks["team-evals"].unchanged == 1
    finally:
        await engine.dispose()
