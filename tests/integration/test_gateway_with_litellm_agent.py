"""E2E: real HttpLLMGatewayClient → real Gateway app → fake provider →
LiteLLMAgent emits llm_call events that round-trip into the trajectory."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import create_engine, delete, insert, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.agent.http_gateway_client import HttpLLMGatewayClient
from loom.agent.litellm import LiteLLMAgent
from loom.db.schema import (
    DataLifecycleAuthority,
    LlmCall,
    RateCard,
    Task,
    Team,
    Token,
    Trial,
)
from loom.driver.base import StartOptions
from loom.driver.fake import FakeDriver
from loom.models.trajectory import EventKind
from loom.models.types import ModelSpec
from loom.trajectory.reader import TrajectoryReader
from loom.trajectory.storage import FakeObjectStore
from loom.trajectory.writer import TrajectoryWriter
from loom_llm_gateway.app import create_app
from loom_llm_gateway.config import GatewaySettings
from loom_llm_gateway.rate_card import RateCardCache
from tests.integration.gateway_db import delete_all_teams_and_quotas

pytestmark = pytest.mark.docker


async def test_e2e_litellm_agent_via_gateway(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    tmp_path: Path,
):
    sync_engine = create_engine(postgres_url)
    sync_factory = sessionmaker(sync_engine)
    team_id = uuid4()
    task_id = f"e2e-task-{uuid4().hex}"
    trial_uuid = uuid4()
    submitted_at = datetime.now(UTC)
    raw_token = f"loom_team_{uuid4().hex}"
    try:
        with sync_factory() as s:
            s.execute(insert(Team).values(id=team_id, name=f"e2e-{team_id}"))
            s.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(raw_token.encode()).digest(),
                    type="team",
                    scopes=["submit", "llm:call"],
                    team_id=team_id,
                    issued_at=submitted_at,
                    expires_at=None,
                )
            )
            s.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="0" * 64,
                    config={},
                    source="integration",
                )
            )
            s.execute(
                insert(Trial).values(
                    id=trial_uuid,
                    team_id=team_id,
                    task_id=task_id,
                    config={},
                    requires_caps={},
                    state="running",
                    submitted_at=submitted_at,
                )
            )
            s.execute(
                insert(RateCard).values(
                    id="card-e2e",
                    captured_at=submitted_at,
                    table={
                        "id": "card-e2e",
                        "entries": [
                            {
                                "provider": "anthropic",
                                "model": "claude-opus-4-7",
                                "input_per_mtok": 1,
                                "output_per_mtok": 1,
                                "cache_read_per_mtok": 0,
                                "cache_write_per_mtok": 0,
                            }
                        ],
                    },
                )
            )
            s.commit()

        monkeypatch.setenv("LOOM_GW_DB_URL", postgres_url)
        monkeypatch.setenv("LOOM_GW_ANTHROPIC_API_KEY", "stub")
        settings = GatewaySettings(_env_file=None)
        app = create_app(settings)
        # ASGITransport doesn't run lifespan — populate state directly.
        async_engine = create_async_engine(str(settings.db_url))
        app.state.settings = settings
        app.state.session_factory = async_sessionmaker(
            async_engine,
            expire_on_commit=False,
        )
        app.state.rate_card_cache = RateCardCache(
            session_factory=app.state.session_factory,
            ttl_sec=settings.rate_card_cache_ttl_sec,
        )

        async def stub(**kwargs: Any) -> dict[str, Any]:
            return {
                "id": "stub",
                "model": kwargs.get("model"),
                "choices": [
                    {
                        "message": {"role": "assistant", "content": "done"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 2, "completion_tokens": 1},
            }

        monkeypatch.setattr("loom_llm_gateway.litellm_wrapper.acompletion", stub)

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://gw",
        ) as client:
            gateway_client = HttpLLMGatewayClient(
                base_url="http://gw",
                token=raw_token,
                _client=client,
            )

            store = FakeObjectStore()
            writer = TrajectoryWriter(
                local_path=tmp_path / "events.jsonl",
                store=store,
                bucket="trajectories",
                key=f"team/{trial_uuid}/events.jsonl",
                min_part_bytes=0,
            )
            driver = FakeDriver()
            await driver.start(options=StartOptions())

            agent = LiteLLMAgent(
                model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
                gateway=gateway_client,
                team_id=str(team_id),
                trial_id=trial_uuid,
            )

            async with writer:
                await agent.run(
                    instruction="hello",
                    env=driver,
                    trajectory=writer,
                    mcp=[],
                    skills_dir=None,
                    step_id="main",
                )

        reader = TrajectoryReader(writer.local_path)
        calls = list(reader.iter_kind(EventKind.LLM_CALL))
        assert len(calls) == 1
        assert calls[0].input_tokens == 2  # type: ignore[attr-defined]
        assert calls[0].cost_usd_snapshot > 0  # type: ignore[attr-defined]
        assert calls[0].rate_card_hash  # type: ignore[attr-defined]
        with sync_factory() as s:
            trial_authority_id = s.scalar(
                select(Trial.lifecycle_authority_id).where(Trial.id == trial_uuid),
            )
            call_authority_id = s.scalar(
                select(LlmCall.lifecycle_authority_id).where(
                    LlmCall.trial_id == trial_uuid,
                ),
            )
        assert trial_authority_id is not None
        assert call_authority_id is not None
        assert call_authority_id != trial_authority_id

        await async_engine.dispose()
    finally:
        with sync_factory() as s:
            s.execute(delete(LlmCall).where(LlmCall.trial_id == trial_uuid))
            s.execute(delete(Trial).where(Trial.id == trial_uuid))
            s.execute(
                delete(DataLifecycleAuthority).where(
                    DataLifecycleAuthority.owner_kind == "trial",
                    DataLifecycleAuthority.owner_id == str(trial_uuid),
                )
            )
            s.execute(delete(Task).where(Task.id == task_id))
            s.execute(delete(Token))
            delete_all_teams_and_quotas(s)
            s.execute(delete(RateCard))
            s.commit()
        sync_engine.dispose()
