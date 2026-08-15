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
import os
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
from sqlalchemy import create_engine, delete, insert, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
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
from tests.integration.pipeline_orchestrator_fixtures import orchestrator_seed  # noqa: F401
from tests.integration.taskset_fixtures import tasksets_minio, tasksets_setup  # noqa: F401


@pytest.fixture(scope="session")
def capacity_guard_template_database(postgres_url: str) -> Iterator[dict[str, object]]:
    """Create a clean protected-schema template and its cluster-scoped roles."""

    source_url = make_url(postgres_url)
    suffix = f"{os.getpid()}_{uuid4().hex[:8]}"
    database_name = f"loom_guard_test_{suffix}"
    owner_role = f"loom_guard_owner_test_{suffix}"
    migrator_role = f"loom_guard_migrator_test_{suffix}"
    agent_role = f"loom_guard_agent_test_{suffix}"
    executor_role = f"loom_guard_executor_test_{suffix}"
    observer_role = f"loom_guard_observer_test_{suffix}"
    # The literal percent becomes ``%25`` in the URL and exercises Alembic's
    # ConfigParser interpolation boundary on every protected migration test.
    migrator_password = f"guard-test-%-{uuid4().hex}"
    admin_url = source_url.set(database="postgres")
    environment_admin_url = source_url.set(database=database_name)
    migrator_url = source_url.set(
        database=database_name,
        username=migrator_role,
        password=migrator_password,
    )
    agent_password = f"guard-agent-test-{uuid4().hex}"
    executor_password = f"guard-executor-test-{uuid4().hex}"
    observer_password = f"guard-observer-test-{uuid4().hex}"
    agent_url = source_url.set(
        database=database_name,
        username=agent_role,
        password=agent_password,
    )
    executor_url = source_url.set(
        database=database_name,
        username=executor_role,
        password=executor_password,
    )
    observer_url = source_url.set(
        database=database_name,
        username=observer_role,
        password=observer_password,
    )
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    preparer = admin_engine.dialect.identifier_preparer
    quoted_database = preparer.quote(database_name)
    quoted_owner = preparer.quote(owner_role)
    quoted_migrator = preparer.quote(migrator_role)
    quoted_agent = preparer.quote(agent_role)
    quoted_executor = preparer.quote(executor_role)
    quoted_observer = preparer.quote(observer_role)
    repo_root = Path(__file__).resolve().parents[2]
    created_database = False
    created_owner = False
    created_migrator = False
    created_agent = False
    created_executor = False
    created_observer = False

    try:
        with admin_engine.connect() as connection:
            connection.exec_driver_sql(
                f"CREATE ROLE {quoted_owner} NOLOGIN NOSUPERUSER "
                "NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
            )
            created_owner = True
            connection.execution_options(no_parameters=True).exec_driver_sql(
                f"CREATE ROLE {quoted_migrator} LOGIN NOSUPERUSER "
                "NOCREATEDB NOCREATEROLE INHERIT NOREPLICATION NOBYPASSRLS "
                f"PASSWORD '{migrator_password}'"
            )
            created_migrator = True
            connection.exec_driver_sql(
                f"CREATE ROLE {quoted_agent} LOGIN NOSUPERUSER "
                "NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS "
                f"PASSWORD '{agent_password}'"
            )
            created_agent = True
            connection.exec_driver_sql(
                f"CREATE ROLE {quoted_executor} LOGIN NOSUPERUSER "
                "NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS "
                f"PASSWORD '{executor_password}'"
            )
            created_executor = True
            connection.exec_driver_sql(
                f"CREATE ROLE {quoted_observer} LOGIN NOSUPERUSER "
                "NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS "
                f"PASSWORD '{observer_password}'"
            )
            created_observer = True
            connection.exec_driver_sql(f"GRANT {quoted_owner} TO {quoted_migrator}")
            connection.exec_driver_sql(f"CREATE DATABASE {quoted_database} TEMPLATE template0")
            created_database = True
            connection.exec_driver_sql(
                f"GRANT CREATE ON DATABASE {quoted_database} TO {quoted_owner}"
            )

        application_cfg = AlembicConfig(str(repo_root / "migrations" / "alembic.ini"))
        application_cfg.set_main_option("script_location", str(repo_root / "migrations"))
        application_cfg.set_main_option(
            "sqlalchemy.url", environment_admin_url.render_as_string(hide_password=False)
        )
        command.upgrade(application_cfg, "head")

        environment_admin_engine = create_engine(environment_admin_url)
        try:
            with environment_admin_engine.begin() as connection:
                connection.exec_driver_sql(f"GRANT USAGE ON SCHEMA public TO {quoted_owner}")
                connection.exec_driver_sql(
                    f"GRANT REFERENCES (id) ON TABLE public.trials TO {quoted_owner}"
                )
                connection.exec_driver_sql(
                    "GRANT SELECT (id, state, requires_caps, cancellation_requested_at, "
                    "next_attempt_at, autoscaler_pool_name, worker_id, attempt_count, "
                    "submit_priority, submitted_at) "
                    f"ON TABLE public.trials TO {quoted_owner}"
                )
                connection.exec_driver_sql(
                    "GRANT SELECT (lifecycle_authority_id), "
                    "UPDATE (lifecycle_authority_id, state) ON TABLE public.trials "
                    f"TO {quoted_owner}"
                )
                connection.exec_driver_sql(
                    "GRANT INSERT (id, team_id, task_id, config, requires_caps, state, "
                    "submit_priority, batch_id, idempotency_key, sample_idx, combination_idx, "
                    "provider_connection_id, provider_model_id, submitted_by_user_id, "
                    "usage_attributed_user_id, usage_attributed_actor, family_key) "
                    f"ON TABLE public.trials TO {quoted_owner}"
                )
                connection.exec_driver_sql(
                    "GRANT SELECT (id) ON TABLE public.data_lifecycle_authorities "
                    f"TO {quoted_owner}"
                )
                connection.exec_driver_sql(
                    "GRANT INSERT (environment, namespace, team_id, data_class, owner_kind, "
                    "owner_id, created_at, expires_at, pinned, state) "
                    "ON TABLE public.data_lifecycle_authorities "
                    f"TO {quoted_owner}"
                )
                connection.exec_driver_sql(
                    "GRANT REFERENCES (id) ON TABLE public.data_lifecycle_authorities "
                    f"TO {quoted_owner}"
                )
                public_tables_before = frozenset(
                    inspect(connection).get_table_names(schema="public")
                )
        finally:
            environment_admin_engine.dispose()

        guard_cfg = AlembicConfig(str(repo_root / "capacity_guard_migrations" / "alembic.ini"))
        guard_cfg.set_main_option("script_location", str(repo_root / "capacity_guard_migrations"))
        previous_url = os.environ.get("LOOM_CAPACITY_GUARD_DB_URL")
        previous_owner = os.environ.get("LOOM_CAPACITY_GUARD_OWNER_ROLE")
        previous_agent = os.environ.get("LOOM_CAPACITY_GUARD_AGENT_ROLE")
        previous_executor = os.environ.get("LOOM_CAPACITY_GUARD_EXECUTOR_ROLE")
        previous_observer = os.environ.get("LOOM_CAPACITY_GUARD_OBSERVER_ROLE")
        os.environ["LOOM_CAPACITY_GUARD_DB_URL"] = migrator_url.render_as_string(
            hide_password=False
        )
        os.environ["LOOM_CAPACITY_GUARD_OWNER_ROLE"] = owner_role
        os.environ["LOOM_CAPACITY_GUARD_AGENT_ROLE"] = agent_role
        os.environ["LOOM_CAPACITY_GUARD_EXECUTOR_ROLE"] = executor_role
        os.environ["LOOM_CAPACITY_GUARD_OBSERVER_ROLE"] = observer_role
        try:
            command.upgrade(guard_cfg, "head")
        finally:
            if previous_url is None:
                os.environ.pop("LOOM_CAPACITY_GUARD_DB_URL", None)
            else:
                os.environ["LOOM_CAPACITY_GUARD_DB_URL"] = previous_url
            if previous_owner is None:
                os.environ.pop("LOOM_CAPACITY_GUARD_OWNER_ROLE", None)
            else:
                os.environ["LOOM_CAPACITY_GUARD_OWNER_ROLE"] = previous_owner
            if previous_agent is None:
                os.environ.pop("LOOM_CAPACITY_GUARD_AGENT_ROLE", None)
            else:
                os.environ["LOOM_CAPACITY_GUARD_AGENT_ROLE"] = previous_agent
            if previous_executor is None:
                os.environ.pop("LOOM_CAPACITY_GUARD_EXECUTOR_ROLE", None)
            else:
                os.environ["LOOM_CAPACITY_GUARD_EXECUTOR_ROLE"] = previous_executor
            if previous_observer is None:
                os.environ.pop("LOOM_CAPACITY_GUARD_OBSERVER_ROLE", None)
            else:
                os.environ["LOOM_CAPACITY_GUARD_OBSERVER_ROLE"] = previous_observer

        yield {
            "admin_url": environment_admin_url.render_as_string(hide_password=False),
            "agent_password": agent_password,
            "agent_role": agent_role,
            "agent_url": agent_url.render_as_string(hide_password=False),
            "executor_password": executor_password,
            "executor_role": executor_role,
            "executor_url": executor_url.render_as_string(hide_password=False),
            "observer_password": observer_password,
            "observer_role": observer_role,
            "observer_url": observer_url.render_as_string(hide_password=False),
            "cluster_admin_url": admin_url.render_as_string(hide_password=False),
            "database_name": database_name,
            "migrator_url": migrator_url.render_as_string(hide_password=False),
            "migrator_password": migrator_password,
            "migrator_role": migrator_role,
            "owner_role": owner_role,
            "public_tables_before": public_tables_before,
        }
    finally:
        if created_database:
            with admin_engine.connect() as connection:
                connection.execute(
                    text(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        "WHERE datname = :database_name AND pid <> pg_backend_pid()"
                    ),
                    {"database_name": database_name},
                )
                connection.exec_driver_sql(f"DROP DATABASE IF EXISTS {quoted_database}")
        with admin_engine.connect() as connection:
            if created_agent:
                connection.exec_driver_sql(f"DROP ROLE IF EXISTS {quoted_agent}")
            if created_executor:
                connection.exec_driver_sql(f"DROP ROLE IF EXISTS {quoted_executor}")
            if created_observer:
                connection.exec_driver_sql(f"DROP ROLE IF EXISTS {quoted_observer}")
            if created_migrator:
                connection.exec_driver_sql(f"DROP ROLE IF EXISTS {quoted_migrator}")
            if created_owner:
                connection.exec_driver_sql(f"DROP ROLE IF EXISTS {quoted_owner}")
        admin_engine.dispose()


@pytest.fixture
def capacity_guard_database(
    capacity_guard_template_database: dict[str, object],
) -> Iterator[dict[str, object]]:
    """Clone the clean guard template so append-only tests remain isolated."""

    def required_string(key: str) -> str:
        value = capacity_guard_template_database[key]
        assert isinstance(value, str)
        return value

    cluster_admin_url = make_url(required_string("cluster_admin_url"))
    template_name = required_string("database_name")
    database_name = f"loom_guard_case_{os.getpid()}_{uuid4().hex[:8]}"
    admin_url = make_url(required_string("admin_url")).set(database=database_name)
    migrator_url = make_url(required_string("migrator_url")).set(database=database_name)
    agent_url = make_url(required_string("agent_url")).set(database=database_name)
    executor_url = make_url(required_string("executor_url")).set(database=database_name)
    observer_url = make_url(required_string("observer_url")).set(database=database_name)
    engine = create_engine(cluster_admin_url, isolation_level="AUTOCOMMIT")
    preparer = engine.dialect.identifier_preparer
    quoted_database = preparer.quote(database_name)
    quoted_template = preparer.quote(template_name)
    quoted_owner = preparer.quote(required_string("owner_role"))
    try:
        with engine.connect() as connection:
            connection.exec_driver_sql(
                f"CREATE DATABASE {quoted_database} TEMPLATE {quoted_template}"
            )
            connection.exec_driver_sql(
                f"GRANT CREATE ON DATABASE {quoted_database} TO {quoted_owner}"
            )
        yield {
            **capacity_guard_template_database,
            "admin_url": admin_url.render_as_string(hide_password=False),
            "executor_url": executor_url.render_as_string(hide_password=False),
            "observer_url": observer_url.render_as_string(hide_password=False),
            "agent_url": agent_url.render_as_string(hide_password=False),
            "database_name": database_name,
            "migrator_url": migrator_url.render_as_string(hide_password=False),
        }
    finally:
        with engine.connect() as connection:
            connection.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = :database_name AND pid <> pg_backend_pid()"
                ),
                {"database_name": database_name},
            )
            connection.exec_driver_sql(f"DROP DATABASE IF EXISTS {quoted_database}")
        engine.dispose()


@pytest.fixture(scope="session")
def capacity_database_urls(postgres_url: str) -> Iterator[tuple[str, str]]:
    """Create management and empty databases distinct from the environment DB."""

    source_url = make_url(postgres_url)
    capacity_name = f"loom_capacity_test_{os.getpid()}"
    empty_name = f"loom_capacity_empty_{os.getpid()}"
    admin_url = source_url.set(database="postgres")
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    preparer = admin_engine.dialect.identifier_preparer
    quoted_capacity = preparer.quote(capacity_name)
    quoted_empty = preparer.quote(empty_name)
    repo_root = Path(__file__).resolve().parents[2]

    try:
        with admin_engine.connect() as connection:
            connection.exec_driver_sql(f"DROP DATABASE IF EXISTS {quoted_capacity}")
            connection.exec_driver_sql(f"DROP DATABASE IF EXISTS {quoted_empty}")
            connection.exec_driver_sql(f"CREATE DATABASE {quoted_capacity} TEMPLATE template0")
            connection.exec_driver_sql(f"CREATE DATABASE {quoted_empty} TEMPLATE template0")

        capacity_url = source_url.set(database=capacity_name).render_as_string(hide_password=False)
        empty_url = source_url.set(database=empty_name).render_as_string(hide_password=False)
        cfg = AlembicConfig(str(repo_root / "capacity_migrations" / "alembic.ini"))
        cfg.set_main_option("script_location", str(repo_root / "capacity_migrations"))
        previous = os.environ.get("LOOM_CAPACITY_DB_URL")
        os.environ["LOOM_CAPACITY_DB_URL"] = capacity_url
        try:
            command.upgrade(cfg, "head")
        finally:
            if previous is None:
                os.environ.pop("LOOM_CAPACITY_DB_URL", None)
            else:
                os.environ["LOOM_CAPACITY_DB_URL"] = previous
        yield capacity_url, empty_url
    finally:
        try:
            with admin_engine.connect() as connection:
                for name, quoted in (
                    (capacity_name, quoted_capacity),
                    (empty_name, quoted_empty),
                ):
                    connection.execute(
                        text(
                            "SELECT pg_terminate_backend(pid) "
                            "FROM pg_stat_activity "
                            "WHERE datname = :database_name "
                            "AND pid <> pg_backend_pid()"
                        ),
                        {"database_name": name},
                    )
                    connection.exec_driver_sql(f"DROP DATABASE IF EXISTS {quoted}")
        finally:
            admin_engine.dispose()


@pytest.fixture(scope="session")
def capacity_postgres_url(capacity_database_urls: tuple[str, str]) -> str:
    return capacity_database_urls[0]


@pytest.fixture(scope="session")
def empty_capacity_postgres_url(capacity_database_urls: tuple[str, str]) -> str:
    return capacity_database_urls[1]


@pytest.fixture(scope="session")
async def capacity_engine(capacity_postgres_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(capacity_postgres_url, isolation_level="SERIALIZABLE")
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture(scope="session")
async def empty_capacity_engine(
    empty_capacity_postgres_url: str,
) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(empty_capacity_postgres_url)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture(scope="session")
def capacity_session_factory(
    capacity_engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(capacity_engine, expire_on_commit=False)


@pytest.fixture
async def capacity_session(
    capacity_engine: AsyncEngine,
) -> AsyncIterator[AsyncSession]:
    async with capacity_engine.connect() as connection:
        outer = await connection.begin()
        session = AsyncSession(
            bind=connection,
            expire_on_commit=False,
            join_transaction_mode="create_savepoint",
        )
        try:
            yield session
        finally:
            await session.rollback()
            await session.close()
            await outer.rollback()


@pytest.fixture
def isolated_migration_postgres_url(postgres_url: str) -> Iterator[str]:
    """Provide a clean database for tests that traverse Alembic history.

    The integration suite normally shares one head-schema database. Historical
    migration tests must not downgrade that database: current lifecycle rows
    deliberately make migration 0069 fail closed, and a partial downgrade can
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


@pytest.fixture
def isolated_capacity_postgres_url(postgres_url: str) -> Iterator[str]:
    """Provide one fresh capacity-schema database for real concurrency tests."""

    source_url = make_url(postgres_url)
    database_name = f"loom_capacity_case_{os.getpid()}_{uuid4().hex[:8]}"
    admin_url = source_url.set(database="postgres")
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    quoted_database = admin_engine.dialect.identifier_preparer.quote(database_name)
    repo_root = Path(__file__).resolve().parents[2]
    try:
        with admin_engine.connect() as connection:
            connection.exec_driver_sql(f"CREATE DATABASE {quoted_database} TEMPLATE template0")
        isolated_url = source_url.set(database=database_name).render_as_string(
            hide_password=False
        )
        cfg = AlembicConfig(str(repo_root / "capacity_migrations" / "alembic.ini"))
        cfg.set_main_option("script_location", str(repo_root / "capacity_migrations"))
        previous = os.environ.get("LOOM_CAPACITY_DB_URL")
        os.environ["LOOM_CAPACITY_DB_URL"] = isolated_url
        try:
            command.upgrade(cfg, "head")
        finally:
            if previous is None:
                os.environ.pop("LOOM_CAPACITY_DB_URL", None)
            else:
                os.environ["LOOM_CAPACITY_DB_URL"] = previous
        yield isolated_url
    finally:
        try:
            with admin_engine.connect() as connection:
                connection.execute(
                    text(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        "WHERE datname = :database_name AND pid <> pg_backend_pid()"
                    ),
                    {"database_name": database_name},
                )
                connection.exec_driver_sql(f"DROP DATABASE IF EXISTS {quoted_database}")
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
