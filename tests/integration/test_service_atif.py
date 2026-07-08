"""ATIF download route."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import httpx
from fastapi import FastAPI
from sqlalchemy import create_engine, insert, select
from sqlalchemy.orm import sessionmaker

from loom.db.schema import LlmCall, Task, Trial
from tests.integration.test_service_trajectory import _seed_trial_events_postgres


async def test_atif_download_proxies_object_through_service(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    app, raw, _team_id, trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/atif",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200
    assert "location" not in r.headers
    assert r.json()["trial_id"] == "x"


async def test_atif_unknown_trial_404(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    app, raw, _team_id, _trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{uuid4()}/atif",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 404


async def test_atif_falls_back_to_postgres_events(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id, _trial_id = traj_setup
    postgres_trial = uuid4()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    now = datetime.now(UTC)
    with sl() as s:
        task_row_id = s.execute(select(Task.id).limit(1)).scalar_one()
        s.execute(insert(Trial).values(
            id=postgres_trial,
            task_id=task_row_id,
            team_id=team_id,
            state="succeeded",
            config={},
            requires_caps={},
            result={
                "aggregate_reward": 1.0,
                "task_id": task_row_id,
                "agent": {
                    "name": "opencode",
                    "version": "1.0",
                    "mode": "out-of-box",
                    "model": {
                        "provider": "openai",
                        "name": "glm-5.1-thinking",
                    },
                },
            },
            submitted_at=now,
            started_at=now,
            finished_at=now,
        ))
        s.commit()
    sync_engine.dispose()
    _seed_trial_events_postgres(postgres_url, postgres_trial, [
        {
            "seq": 0,
            "kind": "trial_start",
            "source": "worker",
            "schema_version": 1,
            "payload": {
                "emitted_at": now.isoformat(),
                "trial_id": str(postgres_trial),
                "step_id": "__trial__",
                "seq": 0,
                "kind": "trial_start",
                "task_id": task_row_id,
                "agent_name": "opencode",
                "agent_mode": "out-of-box",
            },
        },
        {
            "seq": 1,
            "kind": "step_start",
            "source": "worker",
            "schema_version": 1,
            "payload": {
                "emitted_at": now.isoformat(),
                "trial_id": str(postgres_trial),
                "step_id": "main",
                "seq": 1,
                "kind": "step_start",
                "instruction_excerpt": "solve the task",
            },
        },
        {
            "seq": 2,
            "kind": "llm_call",
            "source": "worker",
            "schema_version": 1,
            "payload": {
                "emitted_at": now.isoformat(),
                "trial_id": str(postgres_trial),
                "step_id": "main",
                "seq": 2,
                "kind": "llm_call",
                "model": {"provider": "openai", "name": "glm-5.1-thinking"},
                "rate_card_hash": "rate-card-test",
                "system_prompt": "be useful",
                "messages": [{"role": "user", "content": "hello"}],
                "tools": None,
                "tool_choice": None,
                "response": {"role": "assistant", "content": "done"},
                "finish_reason": "stop",
                "input_tokens": 11,
                "cached_input_tokens": 0,
                "cache_write_tokens": 0,
                "output_tokens": 7,
                "thinking_tokens": 3,
                "provider_extras": {},
                "cost_usd_snapshot": 0.01,
                "duration_sec": 0.5,
                "streamed": False,
                "time_to_first_token_sec": None,
                "gateway_request_id": "req-postgres-atif",
                "cache_keys": [],
            },
        },
        {
            "seq": 3,
            "kind": "trial_end",
            "source": "worker",
            "schema_version": 1,
            "payload": {
                "emitted_at": now.isoformat(),
                "trial_id": str(postgres_trial),
                "step_id": "__trial__",
                "seq": 3,
                "kind": "trial_end",
                "final_state": "succeeded",
                "reward": {"passed": 1.0},
                "failure_reason": None,
            },
        },
    ])

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{postgres_trial}/atif",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/json")
    body = r.json()
    assert body["schema_version"] == "1.7"
    assert body["metadata"]["task_id"] == task_row_id
    assert body["metadata"]["agent_name"] == "opencode"
    assert body["metadata"]["agent_version"] == "1.0"
    assert body["metadata"]["final_state"] == "succeeded"
    assert body["steps"][0]["step_id"] == "main"
    assert body["steps"][0]["llm_call_count"] == 1
    assert body["steps"][0]["metrics"]["input_tokens"] == 11
    assert body["steps"][0]["metrics"]["thinking_tokens"] == 3


async def test_atif_postgres_fallback_requires_projection_metadata(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id, _trial_id = traj_setup
    postgres_trial = uuid4()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    now = datetime.now(UTC)
    with sl() as s:
        task_row_id = s.execute(select(Task.id).limit(1)).scalar_one()
        s.execute(insert(Trial).values(
            id=postgres_trial,
            task_id=task_row_id,
            team_id=team_id,
            state="succeeded",
            config={},
            requires_caps={},
            result={"aggregate_reward": 1.0},
            submitted_at=now,
            started_at=now,
            finished_at=now,
        ))
        s.commit()
    sync_engine.dispose()
    _seed_trial_events_postgres(postgres_url, postgres_trial, [
        {
            "seq": 0,
            "kind": "trial_start",
            "source": "worker",
            "schema_version": 1,
            "payload": {
                "emitted_at": now.isoformat(),
                "trial_id": str(postgres_trial),
                "step_id": "__trial__",
                "seq": 0,
                "kind": "trial_start",
                "task_id": task_row_id,
                "agent_name": "opencode",
                "agent_mode": "out-of-box",
            },
        },
    ])

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{postgres_trial}/atif",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 409
    assert r.json() == {
        "detail": (
            "atif projection metadata unavailable; trajectory events "
            "are downloadable"
        ),
    }


async def test_atif_postgres_fallback_enriches_gateway_calls_and_terminal_state(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id, _trial_id = traj_setup
    postgres_trial = uuid4()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    now = datetime.now(UTC)
    with sl() as s:
        task_row_id = s.execute(select(Task.id).limit(1)).scalar_one()
        s.execute(insert(Trial).values(
            id=postgres_trial,
            task_id=task_row_id,
            team_id=team_id,
            state="succeeded",
            config={},
            requires_caps={},
            result={
                "aggregate_reward": 0.5,
                "task_id": task_row_id,
                "agent": {
                    "name": "terminus-2",
                    "version": "2.0",
                    "mode": "out-of-box",
                    "model": {
                        "provider": "openai",
                        "name": "glm5.1-thinking",
                    },
                },
            },
            submitted_at=now,
            started_at=now,
            finished_at=now,
        ))
        s.execute(insert(LlmCall).values(
            team_id=team_id,
            trial_id=postgres_trial,
            step_id="main",
            dialect="openai_facade",
            model="glm5.1-thinking",
            input_tokens=23,
            output_tokens=7,
            provider_extras={"reasoning_tokens": 5},
            request_params={
                "status": "available",
                "parameters": {"temperature": 0},
            },
            cost_usd=Decimal("0.010000"),
            rate_card_hash="facade:test-card",
            captured_at=now,
        ))
        s.commit()
    sync_engine.dispose()
    _seed_trial_events_postgres(postgres_url, postgres_trial, [
        {
            "seq": 0,
            "kind": "trial_start",
            "source": "worker",
            "schema_version": 1,
            "payload": {
                "emitted_at": now.isoformat(),
                "trial_id": str(postgres_trial),
                "step_id": "__trial__",
                "seq": 0,
                "kind": "trial_start",
                "task_id": task_row_id,
                "agent_name": "terminus-2",
                "agent_mode": "out-of-box",
            },
        },
    ])

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{postgres_trial}/atif",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["metadata"]["final_state"] == "succeeded"
    assert body["metadata"]["reward"] == {"aggregate_reward": 0.5}
    assert body["steps"][0]["step_id"] == "main"
    assert body["steps"][0]["llm_call_count"] == 1
    assert body["steps"][0]["metrics"] == {
        "input_tokens": 23,
        "output_tokens": 7,
        "cached_input_tokens": 0,
        "cache_write_tokens": 0,
        "thinking_tokens": 5,
        "cost_usd": 0.01,
    }
