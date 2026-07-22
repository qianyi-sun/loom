"""Shared service-integration fixtures.

`minio` (session-scoped) brings up a single MinIO container for the
suite. `traj_setup` is the common service-app + seeded trial + trajectory
fixture used by trajectory + ATIF tests in this package.

`pgbouncer_stack` brings up Postgres + pgbouncer (transaction mode) for
pgbouncer integration tests (#609).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from testcontainers.minio import MinioContainer
from testcontainers.postgres import PostgresContainer

from loom.db.schema import (
    DataLifecycleAuthority,
    LlmCall,
    Task,
    Team,
    TeamQuota,
    Token,
    Trial,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from tests.integration.taskset_fixtures import tasksets_minio, tasksets_setup  # noqa: F401


@pytest.fixture
def isolated_migration_postgres_url(postgres_url: str) -> Iterator[str]:
    """Provide a clean database for tests that traverse Alembic history.

    The integration suite normally shares one head-schema database. Historical
    migration tests must not downgrade that database: current lifecycle rows
    deliberately make migration 0066 fail closed, and a partial downgrade can
    poison every later test in the shard. Each caller instead gets a fresh
    database on the session Postgres server, upgraded to head before use.
    """
    source_url = make_url(postgres_url)
    database_name = f"loom_migration_{uuid4().hex}"
    admin_url = source_url.set(database="postgres")
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    quoted_database = admin_engine.dialect.identifier_preparer.quote(database_name)
    repo_root = Path(__file__).resolve().parents[2]

    try:
        with admin_engine.connect() as conn:
            conn.exec_driver_sql(
                f"CREATE DATABASE {quoted_database} TEMPLATE template0",
            )

        isolated_url = source_url.set(database=database_name).render_as_string(
            hide_password=False,
        )
        cfg = AlembicConfig(str(repo_root / "migrations" / "alembic.ini"))
        cfg.set_main_option("script_location", str(repo_root / "migrations"))
        cfg.set_main_option("sqlalchemy.url", isolated_url)
        command.upgrade(cfg, "head")
        yield isolated_url
    finally:
        try:
            with admin_engine.connect() as conn:
                conn.execute(
                    text(
                        "SELECT pg_terminate_backend(pid) "
                        "FROM pg_stat_activity "
                        "WHERE datname = :database_name AND pid <> pg_backend_pid()",
                    ),
                    {"database_name": database_name},
                )
                conn.exec_driver_sql(f"DROP DATABASE IF EXISTS {quoted_database}")
        finally:
            admin_engine.dispose()


@pytest.fixture(scope="module")
def shared_minio() -> Iterator[MinioContainer]:
    """Module-scoped MinIO so we don't pay container-start cost per
    test. Routes that exercise the boto3 path (trajectory, atif)
    share this."""
    with MinioContainer() as m:
        yield m


@pytest.fixture
async def traj_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    shared_minio: MinioContainer,
) -> AsyncIterator[tuple[FastAPI, str, UUID, UUID]]:
    cfg = shared_minio.get_config()
    endpoint = f"http://{cfg['endpoint']}"
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": endpoint,
        "LOOM_SVC_MINIO_ACCESS_KEY": cfg["access_key"],
        "LOOM_SVC_MINIO_SECRET_KEY": cfg["secret_key"],
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
    )
    app.state.minio_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=cfg["access_key"],
        aws_secret_access_key=cfg["secret_key"],
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )
    app.state.http_client = httpx.AsyncClient(
        base_url=str(settings.control_plane_url),
    )

    team_id = uuid4()
    raw = f"loom_team_{uuid4().hex}"
    task_id = f"local/task-{uuid4().hex[:8]}"
    trial_id = uuid4()
    now = datetime.now(UTC)

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    token_hash = hashlib.sha256(raw.encode()).digest()
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(
            insert(Token).values(
                token_hash=token_hash,
                type="team",
                scopes=["read:own"],
                team_id=team_id,
                issued_at=now,
                expires_at=None,
            )
        )
        s.execute(
            insert(Task).values(
                id=task_id,
                checksum="x" * 64,
                config={},
                source="local",
            )
        )
        s.execute(
            insert(Trial).values(
                id=trial_id,
                task_id=task_id,
                team_id=team_id,
                state="succeeded",
                config={},
                requires_caps={},
                submitted_at=now,
                # `trials_succeeded_has_result` CHECK (migration 0039 from
                # #416 Slice 4) requires result IS NOT NULL when state is
                # `succeeded`. Empty dict satisfies the constraint without
                # needing a real verifier projection in the fixture.
                result={},
            )
        )
        s.commit()

    # Seed events.jsonl + atif.json in the trajectories bucket.
    if not shared_minio.get_client().bucket_exists(
        settings.trajectories_bucket,
    ):
        shared_minio.get_client().make_bucket(settings.trajectories_bucket)
    events = [
        {"kind": "trial_start", "trial_id": str(trial_id), "seq": 0},
        {"kind": "step_start", "trial_id": str(trial_id), "step_id": "main", "seq": 1},
        {
            "kind": "llm_call",
            "trial_id": str(trial_id),
            "step_id": "main",
            "seq": 2,
            "input_tokens": 100,
            "output_tokens": 50,
        },
        {"kind": "step_end", "trial_id": str(trial_id), "step_id": "main", "seq": 3, "reward": 1.0},
        {"kind": "trial_end", "trial_id": str(trial_id), "seq": 4},
    ]
    body = ("\n".join(json.dumps(e) for e in events) + "\n").encode()
    prefix = f"{team_id}/{trial_id}"
    app.state.minio_client.put_object(
        Bucket=settings.trajectories_bucket,
        Key=f"{prefix}/events.jsonl",
        Body=body,
    )
    app.state.minio_client.put_object(
        Bucket=settings.trajectories_bucket,
        Key=f"{prefix}/atif.json",
        Body=b'{"version": "1.7", "trial_id": "x"}',
    )

    try:
        yield app, raw, team_id, trial_id
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(LlmCall).where(LlmCall.team_id == team_id))
            s.execute(delete(Trial).where(Trial.team_id == team_id))
            s.execute(
                delete(DataLifecycleAuthority).where(
                    DataLifecycleAuthority.team_id == team_id,
                    DataLifecycleAuthority.owner_kind == "trial",
                )
            )
            s.execute(delete(Token).where(Token.token_hash == token_hash))
            s.execute(delete(Task).where(Task.id == task_id))
            s.execute(delete(TeamQuota).where(TeamQuota.team_id == team_id))
            s.execute(delete(Team).where(Team.id == team_id))
            s.commit()
        sync_engine.dispose()


@pytest.fixture
def pgbouncer_stack() -> Iterator[dict[str, str]]:
    """Bring up Postgres + pgbouncer configured in transaction mode.

    Both containers share a private Docker network so pgbouncer can
    reach Postgres by the hostname alias ``postgres``.  pgbouncer's
    6432 port is also exposed to the host so test code can connect from
    outside.

    Uses ``edoburu/pgbouncer`` which is available on Docker Hub.
    Env vars follow the edoburu image convention: DB_HOST, DB_USER,
    DB_PASSWORD, DB_NAME, POOL_MODE, AUTH_TYPE, LISTEN_PORT.

    Yields a dict with:
      - ``direct_url``: DSN pointing at Postgres direct (psycopg driver)
      - ``pool_url``:   DSN pointing at pgbouncer in transaction mode
    """
    with Network() as network:
        with (
            PostgresContainer(
                "postgres:16-alpine",
                username="test",
                password="test",
                dbname="test",
                driver="psycopg",
            )
            .with_network(network)
            .with_network_aliases("postgres") as postgres
        ):
            pgbouncer = (
                DockerContainer("edoburu/pgbouncer:latest")
                .with_network(network)
                .with_env("DB_HOST", "postgres")
                .with_env("DB_PORT", "5432")
                .with_env("DB_USER", "test")
                .with_env("DB_PASSWORD", "test")
                .with_env("DB_NAME", "test")
                .with_env("POOL_MODE", "transaction")
                .with_env("DEFAULT_POOL_SIZE", "10")
                .with_env("MAX_CLIENT_CONN", "100")
                .with_env("AUTH_TYPE", "plain")
                .with_env("LISTEN_PORT", "6432")
                .with_exposed_ports(6432)
                # Wait until pgbouncer logs that it is listening.
                .waiting_for(LogMessageWaitStrategy("listening on 0.0.0.0:6432"))
            )
            with pgbouncer:
                direct_url = postgres.get_connection_url()
                pool_ip = pgbouncer.get_container_host_ip()
                pool_port = pgbouncer.get_exposed_port(6432)
                pool_url = f"postgresql+psycopg://test:test@{pool_ip}:{pool_port}/test"
                yield {"direct_url": direct_url, "pool_url": pool_url}
