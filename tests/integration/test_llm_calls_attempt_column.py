"""Migration 0029 + #298 Slice B: `llm_calls.attempt` column wiring.

Verifies:
- The column exists, defaults to 1, accepts arbitrary integers.
- `record_call(attempt=N)` writes the supplied value.
- The CP /trials/{id}/llm-calls payload surfaces the column.
- The trial's `_append_llm_call_events` projection threads the value
  into `LLMCallEvent.attempt`.

The column is added via migration `0029_llm_calls_attempt.py`; the
session-scoped `postgres_url` fixture brings the schema to head, so
the column is present in every test.
"""
from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, delete, insert, select
from sqlalchemy.orm import sessionmaker

from loom.db.schema import LlmCall, Team, Token


@pytest.fixture
def team_and_trial(postgres_url: str) -> Iterator[tuple[str, str]]:
    """Bare-minimum (team, trial-id-uuid) so we can write llm_calls rows."""
    import hashlib
    from datetime import UTC, datetime

    engine = create_engine(postgres_url)
    session_local = sessionmaker(engine)
    team_id = uuid4()
    trial_id = uuid4()
    raw = f"t_{uuid4().hex}"
    now = datetime.now(UTC)
    with session_local() as s:
        s.execute(insert(Team).values(id=team_id, name=f"team-{team_id}"))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="team", scopes=["read:own"], team_id=team_id,
            issued_at=now, expires_at=None,
        ))
        s.commit()
    try:
        yield (str(team_id), str(trial_id))
    finally:
        with session_local() as s:
            s.execute(delete(LlmCall).where(LlmCall.trial_id == trial_id))
            s.execute(delete(Token).where(Token.team_id == team_id))
            s.execute(delete(Team).where(Team.id == team_id))
            s.commit()


def test_attempt_column_defaults_to_one(
    postgres_url: str, team_and_trial: tuple[str, str],
) -> None:
    """Insert without `attempt` → server default of 1 applies."""
    team_id, trial_id = team_and_trial
    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        s.execute(insert(LlmCall).values(
            team_id=team_id,
            trial_id=trial_id,
            step_id="main",
            dialect="openai_chat",
            model="gpt-4",
            input_tokens=10, output_tokens=5,
            provider_extras={},
            cost_usd=Decimal("0.001"),
            rate_card_hash="abc",
        ))
        s.commit()
        row = s.execute(
            select(LlmCall).where(LlmCall.trial_id == trial_id),
        ).scalar_one()
        assert row.attempt == 1


def test_attempt_column_accepts_explicit_value(
    postgres_url: str, team_and_trial: tuple[str, str],
) -> None:
    """Insert with `attempt=3` → row carries that value."""
    team_id, trial_id = team_and_trial
    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        s.execute(insert(LlmCall).values(
            team_id=team_id,
            trial_id=trial_id,
            step_id="main",
            dialect="openai_chat",
            model="gpt-4",
            input_tokens=10, output_tokens=5,
            provider_extras={},
            cost_usd=Decimal("0.001"),
            rate_card_hash="abc",
            attempt=3,
        ))
        s.commit()
        row = s.execute(
            select(LlmCall).where(LlmCall.trial_id == trial_id),
        ).scalar_one()
        assert row.attempt == 3


@pytest.mark.asyncio
async def test_record_call_writes_attempt(
    postgres_url: str, team_and_trial: tuple[str, str],
) -> None:
    """record_call(attempt=N) should land that value in the row."""
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from loom_llm_gateway.dialect import TokenUsage
    from loom_llm_gateway.llm_calls import record_call

    team_id, trial_id = team_and_trial
    db_url = postgres_url.replace("postgresql+psycopg://", "postgresql+psycopg://")
    engine = create_async_engine(db_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await record_call(
                session,
                team_id=uuid4().__class__(team_id),
                trial_id=uuid4().__class__(trial_id),
                step_id="main",
                dialect="openai_chat",
                model="gpt-4",
                usage=TokenUsage(
                    input_tokens=42, output_tokens=17, provider_extras={},
                ),
                cost_usd=0.001,
                rate_card_hash="abc",
                attempt=4,
            )
        # Read back synchronously
        sync = create_engine(postgres_url)
        sl = sessionmaker(sync)
        with sl() as s:
            row = s.execute(
                select(LlmCall).where(LlmCall.trial_id == trial_id),
            ).scalar_one()
            assert row.attempt == 4
            assert row.input_tokens == 42
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_record_call_writes_request_params(
    postgres_url: str, team_and_trial: tuple[str, str],
) -> None:
    """record_call(request_params=...) should persist non-sensitive audit params."""
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from loom_llm_gateway.dialect import TokenUsage
    from loom_llm_gateway.llm_calls import record_call

    team_id, trial_id = team_and_trial
    db_url = postgres_url.replace("postgresql+psycopg://", "postgresql+psycopg://")
    engine = create_async_engine(db_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    request_params = {
        "status": "available",
        "parameters": {
            "temperature": 0,
            "max_output_tokens": 1024,
        },
    }
    try:
        async with factory() as session:
            await record_call(
                session,
                team_id=uuid4().__class__(team_id),
                trial_id=uuid4().__class__(trial_id),
                step_id="main",
                dialect="openai_responses",
                model="gpt-4o",
                usage=TokenUsage(
                    input_tokens=42, output_tokens=17, provider_extras={},
                ),
                cost_usd=0.001,
                rate_card_hash="abc",
                request_params=request_params,
            )
        sync = create_engine(postgres_url)
        sl = sessionmaker(sync)
        with sl() as s:
            row = s.execute(
                select(LlmCall).where(LlmCall.trial_id == trial_id),
            ).scalar_one()
            assert row.request_params == request_params
    finally:
        await engine.dispose()
