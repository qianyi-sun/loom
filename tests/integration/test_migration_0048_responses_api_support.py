"""Migration 0048 — add `responses_api_supported` + `responses_api_probed_at`
columns to `provider_connections` so the LLM gateway can cache per-connection
Responses-API support and dispatch through the existing
`responses_chat_compat` translator for providers that lack it (yibuapi et al.).

Spec: docs/architecture/responses-api-support-probe.md
"""
from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

REPO_ROOT = Path(__file__).resolve().parents[2]


def _alembic(url: str, *args: str) -> None:
    env = os.environ.copy()
    env["LOOM_DB_URL"] = url
    subprocess.run(
        [sys.executable, "-m", "alembic", "-c", "migrations/alembic.ini", *args],
        cwd=REPO_ROOT, env=env, check=True,
    )


@pytest.fixture(scope="module")
def postgres_url_at_prev() -> Iterator[str]:
    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+psycopg://",
        )
        _alembic(url, "upgrade", "0047")
        yield url


def _seed_connection(url: str) -> str:
    conn_id = str(uuid4())
    team_id = str(uuid4())
    engine = create_engine(url)
    with engine.begin() as c:
        c.execute(text("insert into teams (id, name) values (:t, :n)"),
                  {"t": team_id, "n": f"t-{team_id}"})
        c.execute(text("""
            insert into provider_connections (
                id, team_id, provider_type, display_name, base_url,
                upstream_host, encrypted_api_key_ref, created_by
            ) values (:id, :team, 'openai-compatible', 'mock', 'http://mock/v1',
                     'mock', 'loom://test', 'test')
        """), {"id": conn_id, "team": team_id})
    engine.dispose()
    return conn_id


def test_upgrade_adds_columns_defaulting_null(postgres_url_at_prev: str) -> None:
    conn_id = _seed_connection(postgres_url_at_prev)
    _alembic(postgres_url_at_prev, "upgrade",
             "0048")
    engine = create_engine(postgres_url_at_prev)
    with engine.begin() as c:
        row = c.execute(text(
            "select responses_api_supported, responses_api_probed_at, "
            "responses_api_probe_error from provider_connections where id = :i"
        ), {"i": conn_id}).mappings().one()
    engine.dispose()
    assert row["responses_api_supported"] is None
    assert row["responses_api_probed_at"] is None
    assert row["responses_api_probe_error"] is None
    _alembic(postgres_url_at_prev, "downgrade", "0047")


def test_downgrade_drops_added_columns(postgres_url_at_prev: str) -> None:
    _alembic(postgres_url_at_prev, "upgrade",
             "0048")
    _alembic(postgres_url_at_prev, "downgrade", "0047")
    engine = create_engine(postgres_url_at_prev)
    with engine.begin() as c:
        cols = set(c.execute(text(
            "select column_name from information_schema.columns "
            "where table_name='provider_connections'"
        )).scalars().all())
    engine.dispose()
    for added in ("responses_api_supported", "responses_api_probed_at",
                  "responses_api_probe_error"):
        assert added not in cols


def test_upgrade_preserves_existing_rows(postgres_url_at_prev: str) -> None:
    conn_id = _seed_connection(postgres_url_at_prev)
    engine = create_engine(postgres_url_at_prev)
    with engine.begin() as c:
        before = c.execute(text(
            "select provider_type, display_name, base_url from "
            "provider_connections where id = :i"
        ), {"i": conn_id}).mappings().one()
    engine.dispose()
    _alembic(postgres_url_at_prev, "upgrade",
             "0048")
    engine = create_engine(postgres_url_at_prev)
    with engine.begin() as c:
        after = c.execute(text(
            "select provider_type, display_name, base_url from "
            "provider_connections where id = :i"
        ), {"i": conn_id}).mappings().one()
    engine.dispose()
    assert dict(after) == dict(before)
    _alembic(postgres_url_at_prev, "downgrade", "0047")
