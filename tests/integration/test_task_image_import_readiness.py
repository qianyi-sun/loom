from __future__ import annotations

from pathlib import Path

import tomli_w
from httpx import ASGITransport, AsyncClient
from loom_benchmarks.base import BenchmarkInstance, ConvertedTask, UpstreamSource
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Task, TaskImageMaterialization
from loom.models.task_checksum import task_checksum
from loom.trajectory.storage import FakeObjectStore
from loom_benchmark_tool.import_cmd import run_import
from tests.integration.test_task_image_registration_readiness import _dockerfile_config
from tests.integration.test_taskset_materialization import (
    _MANIFEST_BUNDLE_UPLOAD,
    _bundle_tar_bytes,
    _run_materializer_once,
)
from tests.integration.test_taskset_materialization import (
    materialization_minio as materialization_minio,
)
from tests.integration.test_taskset_materialization import (
    materialization_setup as materialization_setup,
)


class _Adapter:
    name = "image-prerequisites"
    display_name = "Image prerequisites"
    upstream_source = UpstreamSource(kind="git", locator="https://example.test/repo")
    license_spdx = "MIT"
    license_url = "https://example.test/license"
    splits = ("test",)

    def list_instances(self, **_options):
        yield BenchmarkInstance(instance_id="task", split="test", raw={})

    def convert_instance(self, instance, *, out_dir: Path):
        identity = f"{self.name}/{instance.instance_id}"
        (out_dir / "task.toml").write_text(tomli_w.dumps(_dockerfile_config(identity)))
        (out_dir / "instruction.md").write_text("Do the task.\n")
        (out_dir / "environment").mkdir()
        (out_dir / "environment" / "Dockerfile").write_text("FROM scratch\n")
        return ConvertedTask(
            task_id=identity, checksum=task_checksum(out_dir), license_spdx="MIT", warnings=()
        )


async def test_adapter_import_queues_and_restores_image_prerequisite(
    isolated_migration_postgres_url, tmp_path, monkeypatch
):
    monkeypatch.setattr(
        "loom_benchmark_tool.import_cmd._resolve_adapter", lambda *_a, **_kw: _Adapter()
    )
    monkeypatch.setattr(
        "loom_benchmark_tool.import_cmd.fetch_upstream", lambda *_a, **_kw: tmp_path
    )
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        for iteration in range(2):
            stats = await run_import(
                benchmark=_Adapter.name,
                db_url=isolated_migration_postgres_url,
                object_store=FakeObjectStore(),
                bucket="bundles",
                cache_dir=tmp_path / "cache",
            )
            assert stats == {"converted": 1, "warnings": 0}
            async with sessions() as session:
                task = (await session.scalars(select(Task))).one()
                images = list(await session.scalars(select(TaskImageMaterialization)))
                assert len(images) == 1, "imported Dockerfile task has no image prerequisite"
                image = images[0]
                assert image.state == "queued" and image.task_id == task.id
                assert image.task_source == task.source and image.task_config == task.config
                if iteration == 0:
                    first_id = image.id
                    image.state = "retired"
                    await session.commit()
                else:
                    assert image.id == first_id
    finally:
        await engine.dispose()


async def test_taskset_bundle_publication_queues_image_prerequisites(materialization_setup):
    app, tokens, _teams = materialization_setup
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={
                "manifest": (
                    "manifest.yaml",
                    _MANIFEST_BUNDLE_UPLOAD.encode(),
                    "application/x-yaml",
                ),
                "bundle": ("bundle.tar.gz", _bundle_tar_bytes(), "application/gzip"),
            },
        )
        assert response.status_code == 202, response.text
        task_set_id = response.json()["task_set_id"]
        await _run_materializer_once(app)
        async with app.state.session_factory() as session:
            task = (
                await session.scalars(select(Task).where(Task.task_set_id == task_set_id))
            ).one()
            images = list(
                await session.scalars(
                    select(TaskImageMaterialization).where(
                        TaskImageMaterialization.task_id == task.id
                    )
                )
            )
            assert images, "published taskset Dockerfile task has no image prerequisite"
            assert all(
                row.state == "queued"
                and row.task_source == task.source
                and row.task_config == task.config
                for row in images
            )
