"""Credential-first cleanup evidence survives ordinary SQL and rollback."""

import pytest
from alembic import command
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError

from loom.db.schema import TaskImagePublicationCandidate, TaskImageRegistryCredentialGeneration
from tests.integration.test_task_image_candidate_v2 import _prepared, _record
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credential_migration import _config
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)


@pytest.mark.parametrize("candidate", [False, True], ids=["first-credential", "candidate"])
async def test_issued_audit_cannot_be_changed_or_removed(
    registry_authority_session, registry_issuer, candidate
):
    async with registry_authority_session() as session:
        authorization, _, request, _ = await _prepared(session, registry_issuer)
        if candidate:
            await _record(session, authorization, request)
        else:
            assert await session.scalar(select(TaskImagePublicationCandidate)) is None
        await session.commit()
        table = (
            TaskImagePublicationCandidate.__tablename__
            if candidate
            else TaskImageRegistryCredentialGeneration.__tablename__
        )
        before = (await session.execute(text(f"SELECT * FROM {table}"))).one()
        await session.rollback()
        for sql in (
            f"UPDATE {table} SET response_sha256 = repeat('e', 64)",
            f"UPDATE {table} SET response_sha256 = response_sha256",
            f"DELETE FROM {table}",
            f"TRUNCATE {table} CASCADE",
        ):
            with pytest.raises(DBAPIError, match="task-image registry audit is immutable"):
                await session.execute(text(sql))
            await session.rollback()
            assert (await session.execute(text(f"SELECT * FROM {table}"))).one() == before
            await session.rollback()


async def test_downgrade_cannot_remove_first_credential_protection(
    isolated_migration_postgres_url, registry_authority_session, registry_issuer
):
    async with registry_authority_session() as session:
        await _prepared(session, registry_issuer)
        assert await session.scalar(select(TaskImagePublicationCandidate)) is None
        await session.commit()
        before = (
            await session.execute(text("SELECT * FROM task_image_registry_credentials"))
        ).one()
        await session.rollback()
        with pytest.raises(DBAPIError, match="publication authority cannot be discarded"):
            command.downgrade(_config(isolated_migration_postgres_url), "0134")
        assert await session.scalar(text("SELECT version_num FROM alembic_version")) == "0135"
        assert (
            await session.execute(text("SELECT * FROM task_image_registry_credentials"))
        ).one() == before
        await session.rollback()
        with pytest.raises(DBAPIError, match="task-image registry audit is immutable"):
            await session.execute(text("DELETE FROM task_image_registry_credentials"))
        await session.rollback()
