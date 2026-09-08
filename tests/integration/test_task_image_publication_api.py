"""Real authenticated HTTP publication submit/poll over disposable PostgreSQL."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import TaskImageMaterialization, TaskImagePublicationJob
from loom_task_image_authority.api import create_app
from loom_task_image_authority.contracts import TaskImageMaterializationOperationRequestV1
from loom_task_image_authority.publication_receipts import (
    PublicationCandidateIdentity,
    candidate_set_sha256,
)
from loom_task_image_authority.publication_status import (
    canonical_status_bytes,
    decode_publication_status,
)
from loom_task_image_authority.registry_token import DistributionRegistryTokenIssuer
from tests.integration.test_task_image_authority_api import _HEADERS, _settings
from tests.integration.test_task_image_candidate_v2 import _prepared, _record
from tests.integration.test_task_image_registry_credentials import NOW
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)


@dataclass
class PublicationAPI:
    client: httpx.AsyncClient
    sessions: async_sessionmaker[AsyncSession]
    now: list[datetime]
    request: TaskImageMaterializationOperationRequestV1
    candidate_sha256: str

    def path(self, operation: str) -> str:
        return (
            f"/v1/projections/{self.request.grant_id}/materializations/"
            f"{self.request.materialization_id}/publication-{operation}"
        )


@pytest.fixture
async def publication_api(
    tmp_path: Path,
    isolated_migration_postgres_url: str,
    registry_issuer: DistributionRegistryTokenIssuer,
) -> AsyncIterator[PublicationAPI]:
    engine = create_async_engine(isolated_migration_postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            authorization, _, candidate, _ = await _prepared(session, registry_issuer)
            acknowledgement = await _record(session, authorization, candidate)
            await session.commit()
        request = TaskImageMaterializationOperationRequestV1(
            grant_id=candidate.grant_id,
            session_id=candidate.session_id,
            session_generation=candidate.session_generation,
            session_token=candidate.session_token,
            operation_id=uuid4(),
            materialization_id=candidate.materialization_id,
            attempt_id=candidate.attempt_id,
            lease_epoch=candidate.lease_epoch,
        )
        now = [NOW + timedelta(seconds=14)]
        app = create_app(
            _settings(tmp_path, isolated_migration_postgres_url),
            now_factory=lambda: now[0],
            registry_token_issuer=registry_issuer,
        )
        async with app.router.lifespan_context(app):
            assert app.state.ready
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="https://authority.example",
                headers=_HEADERS,
                trust_env=False,
            ) as client:
                yield PublicationAPI(
                    client,
                    factory,
                    now,
                    request,
                    candidate_set_sha256(
                        (
                            PublicationCandidateIdentity(
                                candidate_id=str(acknowledgement.candidate_id),
                                component=acknowledgement.component,
                            ),
                        )
                    ),
                )
    finally:
        await engine.dispose()


async def test_authenticated_submit_replay_and_poll_are_compact_without_readiness(
    publication_api: PublicationAPI,
) -> None:
    api = publication_api
    request = api.request.model_dump(mode="json")
    first = await api.client.post(api.path("submit"), json=request)
    assert first.status_code == 200, first.text
    replay = await api.client.post(api.path("submit"), json=request)
    poll = await api.client.post(api.path("poll"), json=request)
    assert replay.status_code == poll.status_code == 200
    assert first.content == replay.content == poll.content
    result = decode_publication_status(first.content)
    assert first.content == canonical_status_bytes(result)
    assert len(first.content) <= 4096
    assert result.state == "queued"
    assert result.operation_id == str(api.request.operation_id)
    assert result.grant_id == str(api.request.grant_id)
    assert result.materialization_id == str(api.request.materialization_id)
    assert result.attempt_id == str(api.request.attempt_id)
    assert result.lease_epoch == api.request.lease_epoch
    assert result.candidate_set_sha256 == api.candidate_sha256
    assert result.component_count == 1
    assert result.receipt is None
    for private in ("repository", "registry_origin", "worker_id", "deadline", "session_token"):
        assert private not in first.text
    assert api.request.session_token not in first.text
    async with api.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(TaskImagePublicationJob)) == 1
        row = await session.get(TaskImageMaterialization, api.request.materialization_id)
        assert row is not None
        assert row.registry_images == {}
        assert row.ready_at is None
    metrics = (await api.client.get("/metrics")).text
    assert 'route="publication_submit"' in metrics
    assert 'route="publication_poll"' in metrics
    assert api.request.session_token not in metrics


async def test_poll_unknown_operation_does_not_submit_work(publication_api: PublicationAPI) -> None:
    api = publication_api
    response = await api.client.post(api.path("poll"), json=api.request.model_dump(mode="json"))
    assert response.status_code == 409, response.text
    assert response.json() == {"detail": "task-image authority conflict"}
    async with api.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(TaskImagePublicationJob)) == 0
