"""Plan 28 PR-3: multi-(agent, model) combinations + backend +
subset semantics on POST /api/v1/batches.
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    Batch,
    RateCard,
    Task,
    Team,
    TeamQuota,
    Token,
    Trial,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


@pytest.fixture
async def setup(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, str]]:
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "x",
        "LOOM_SVC_MINIO_SECRET_KEY": "y",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(
        engine, expire_on_commit=False,
    )
    app.state.minio_client = boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key.get_secret_value(),
        aws_secret_access_key=settings.minio_secret_key.get_secret_value(),
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )
    app.state.http_client = httpx.AsyncClient(
        base_url=str(settings.control_plane_url),
    )
    app.state.gateway_client = httpx.AsyncClient(
        base_url=str(settings.gateway_url),
    )

    team_id = uuid4()
    raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="team", scopes=["submit", "read:own"], team_id=team_id,
            issued_at=datetime.now(UTC),
        ))
        # Seed 10 deterministic tasks under benchmark "humaneval".
        for i in range(10):
            s.execute(insert(Task).values(
                id=f"humaneval/HumanEval/{i}",
                checksum="x" * 64,
                config={},
                source="local",
                license="MIT",
                benchmark_id=None,
            ))
        s.commit()

    try:
        yield app, raw
    finally:
        await app.state.gateway_client.aclose()
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(Trial))
            s.execute(delete(Batch))
            s.execute(delete(Task))
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.execute(delete(RateCard))
            s.commit()
        sync_engine.dispose()


async def _post(app: FastAPI, raw: str, body: dict) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        return await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json=body,
        )


# ----------------------------------------------------------------
# Subset semantics
# ----------------------------------------------------------------


async def test_subset_first_n(setup: tuple[FastAPI, str]) -> None:
    app, raw = setup
    r = await _post(app, raw, {
        "name": "first-3",
        "backend": "docker",
        "task_filter": {
            "license": "MIT",
            "subset_kind": "first_n",
            "n": 3,
        },
        "trial_config": {
            "agent_name": "oracle", "agent_model": None,
        },
    })
    assert r.status_code == 201, r.text
    # First 3 by id-asc are HumanEval/0, /1, /2 (3 trials × 1 sample).
    assert r.json()["expected_trial_count"] == 3


async def test_subset_last_n(setup: tuple[FastAPI, str]) -> None:
    app, raw = setup
    r = await _post(app, raw, {
        "name": "last-2",
        "task_filter": {
            "license": "MIT",
            "subset_kind": "last_n",
            "n": 2,
        },
        "trial_config": {
            "agent_name": "oracle", "agent_model": None,
        },
    })
    assert r.status_code == 201
    assert r.json()["expected_trial_count"] == 2


async def test_subset_random_n_reproducible(
    setup: tuple[FastAPI, str],
) -> None:
    """Same seed → same task selection → same trial count.
    Different seeds with the same n still produce the same COUNT
    (sampling without replacement)."""
    app, raw = setup
    body = {
        "name": "rand-5-seed-42",
        "task_filter": {
            "license": "MIT", "subset_kind": "random_n", "n": 5, "seed": 42,
        },
        "trial_config": {
            "agent_name": "oracle", "agent_model": None,
        },
    }
    r1 = await _post(app, raw, body)
    assert r1.status_code == 201
    assert r1.json()["expected_trial_count"] == 5


async def test_subset_random_n_requires_seed(
    setup: tuple[FastAPI, str],
) -> None:
    app, raw = setup
    r = await _post(app, raw, {
        "name": "no-seed",
        "task_filter": {
            "license": "MIT", "subset_kind": "random_n", "n": 3,
        },
        "trial_config": {
            "agent_name": "oracle", "agent_model": None,
        },
    })
    assert r.status_code == 400
    assert "seed" in r.json()["detail"].lower()


async def test_subset_unknown_kind_rejected(
    setup: tuple[FastAPI, str],
) -> None:
    app, raw = setup
    r = await _post(app, raw, {
        "name": "phantom",
        "task_filter": {
            "license": "MIT", "subset_kind": "fancy_n", "n": 3,
        },
        "trial_config": {
            "agent_name": "oracle", "agent_model": None,
        },
    })
    assert r.status_code == 400
    assert "unknown subset_kind" in r.json()["detail"]


# ----------------------------------------------------------------
# Combinations
# ----------------------------------------------------------------


async def test_combinations_compute_expected_count(
    setup: tuple[FastAPI, str],
) -> None:
    """Multi-combination batch: 10 tasks × 2 combinations (n=3 each)
    = 60 trials expected."""
    app, raw = setup
    r = await _post(app, raw, {
        "name": "multi-combo",
        "task_filter": {"license": "MIT"},
        "trial_config": {},
        "combinations": [
            {
                "agent_name": "oracle",
                "agent_model": None,
                "n_per_task": 3,
                "label": "oracle-3",
            },
            {
                "agent_name": "oracle",
                "agent_model": None,
                "n_per_task": 3,
                "label": "oracle-3-bis",
            },
        ],
    })
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["expected_trial_count"] == 60
    assert len(body["combinations"]) == 2


async def test_combinations_reject_agent_in_trial_config(
    setup: tuple[FastAPI, str],
) -> None:
    app, raw = setup
    r = await _post(app, raw, {
        "name": "ambiguous",
        "task_filter": {"license": "MIT"},
        "trial_config": {
            "agent_name": "oracle", "agent_model": None,
        },
        "combinations": [{
            "agent_name": "oracle",
            "agent_model": None,
            "n_per_task": 1,
            "label": "x",
        }],
    })
    assert r.status_code == 400
    assert "trial_config.agent_name must be absent" in r.json()["detail"]


async def test_combinations_unique_labels(
    setup: tuple[FastAPI, str],
) -> None:
    app, raw = setup
    r = await _post(app, raw, {
        "name": "dupes",
        "task_filter": {"license": "MIT"},
        "trial_config": {},
        "combinations": [
            {
                "agent_name": "oracle", "agent_model": None,
                "n_per_task": 1, "label": "twin",
            },
            {
                "agent_name": "oracle", "agent_model": None,
                "n_per_task": 1, "label": "twin",
            },
        ],
    })
    assert r.status_code == 400
    assert "duplicated" in r.json()["detail"]


async def test_combinations_reject_unknown_agent(
    setup: tuple[FastAPI, str],
) -> None:
    app, raw = setup
    r = await _post(app, raw, {
        "name": "ghost",
        "task_filter": {"license": "MIT"},
        "trial_config": {},
        "combinations": [{
            "agent_name": "not-a-real-agent",
            "agent_model": None,
            "n_per_task": 1,
            "label": "x",
        }],
    })
    assert r.status_code == 400
    assert "agent catalog" in r.json()["detail"].lower()


# ----------------------------------------------------------------
# Backend
# ----------------------------------------------------------------


async def test_backend_stored_on_batch(setup: tuple[FastAPI, str]) -> None:
    app, raw = setup
    r = await _post(app, raw, {
        "name": "docker-batch",
        "backend": "docker",
        "task_filter": {"license": "MIT", "subset_kind": "first_n", "n": 1},
        "trial_config": {
            "agent_name": "oracle", "agent_model": None,
        },
    })
    assert r.status_code == 201
    assert r.json()["backend"] == "docker"
