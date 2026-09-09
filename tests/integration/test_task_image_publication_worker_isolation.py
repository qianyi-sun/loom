"""The owned worker completion transaction must support fixed-snapshot factories."""

from uuid import UUID

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker

from loom.db.schema import TaskImageMaterialization, TaskImagePublicationJob
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_publication_worker import _prepared, _worker, worker_module
from tests.unit.test_task_image_registry_reader import tls_registry as tls_registry
from tests.unit.test_task_image_registry_reader import token_key as token_key


@pytest.mark.parametrize("isolation", ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"])
async def test_worker_owns_completion_mode_without_changing_factory_default(
    registry_authority_session, tls_registry, token_key, monkeypatch, isolation,
):
    values = await _prepared(registry_authority_session, tls_registry, token_key)
    engine = registry_authority_session.kw["bind"].execution_options(isolation_level=isolation)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    module = worker_module()
    original = module.complete_publication_job
    observed = []

    async def complete(session, **options):
        observed.append(await session.scalar(text("SHOW transaction_isolation")))
        return await original(session, **options)

    monkeypatch.setattr(module, "complete_publication_job", complete)
    receipt = await _worker(factory, tls_registry, values).run(UUID(values[0].operation_id))
    assert receipt.component_count == 1
    assert observed == ["read committed"]
    async with factory() as session:
        assert await session.scalar(text("SHOW transaction_isolation")) == isolation.lower()
        job = (await session.scalars(select(TaskImagePublicationJob))).one()
        row = (await session.scalars(select(TaskImageMaterialization))).one()
        assert job.state == "completed"
        assert row.state == "ready" and row.ready_at is not None
