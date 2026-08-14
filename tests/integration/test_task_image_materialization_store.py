from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import Task, TaskImageMaterialization
from loom.task_image_materialization import ensure_task_image_materializations


def _task_values(*, task_id: str, checksum: str) -> dict[str, object]:
    return {
        "id": task_id,
        "checksum": checksum,
        "source": f"s3://loom-tasks/{task_id}/{checksum}.tar.zst",
        "config": {
            "schema_version": "1",
            "task": {"id": task_id, "name": task_id},
            "environment": {
                "os": "linux",
                "cpu_arch": "any",
                "dockerfile": "environment/Dockerfile",
            },
            "agent": {"name": "oracle"},
            "verifier": {"name": "pytest"},
            "steps": [{"name": "main"}],
        },
        "source_provenance": {"fixture": "task-image-materialization-test"},
    }


@pytest.fixture
async def materialization_session(
    postgres_url: str,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        async with factory() as session:
            await session.execute(delete(TaskImageMaterialization))
            await session.execute(
                delete(Task).where(
                    Task.source_provenance["fixture"].astext
                    == "task-image-materialization-test"
                )
            )
            await session.commit()
        await engine.dispose()


async def test_ensure_enqueues_idempotent_architecture_snapshots(
    materialization_session: async_sessionmaker[AsyncSession],
) -> None:
    task_id = f"materialization/{uuid4()}"
    checksum = "1" * 64
    async with materialization_session() as session:
        await session.execute(insert(Task).values(**_task_values(task_id=task_id, checksum=checksum)))
        await session.commit()
        task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one()

        first = await ensure_task_image_materializations(session, task_row=task)
        second = await ensure_task_image_materializations(session, task_row=task)
        await session.commit()

        assert [row.cpu_arch for row in first] == ["x86_64", "arm64"]
        assert [row.id for row in second] == [row.id for row in first]
        assert all(row.state == "queued" for row in first)
        assert all(row.task_config == task.config for row in first)
        assert all(row.task_source == task.source for row in first)
        assert all(row.task_source_provenance == task.source_provenance for row in first)
        count = await session.scalar(
            select(func.count(TaskImageMaterialization.id)).where(
                TaskImageMaterialization.task_id == task_id
            )
        )
        assert count == 2


async def test_new_task_checksum_creates_new_immutable_materializations(
    materialization_session: async_sessionmaker[AsyncSession],
) -> None:
    task_id = f"materialization/{uuid4()}"
    original_checksum = "2" * 64
    replacement_checksum = "3" * 64
    async with materialization_session() as session:
        await session.execute(
            insert(Task).values(**_task_values(task_id=task_id, checksum=original_checksum))
        )
        await session.commit()
        task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one()
        original = await ensure_task_image_materializations(session, task_row=task)
        await session.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                checksum=replacement_checksum,
                source=f"s3://loom-tasks/{task_id}/{replacement_checksum}.tar.zst",
            )
        )
        await session.commit()
        replacement_task = (
            await session.execute(select(Task).where(Task.id == task_id))
        ).scalar_one()

        replacement = await ensure_task_image_materializations(
            session,
            task_row=replacement_task,
        )
        await session.commit()

        assert {row.materialization_key for row in original}.isdisjoint(
            {row.materialization_key for row in replacement}
        )
        checksums = set(
            (
                await session.execute(
                    select(TaskImageMaterialization.task_checksum).where(
                        TaskImageMaterialization.task_id == task_id
                    )
                )
            ).scalars()
        )
        assert checksums == {original_checksum, replacement_checksum}
