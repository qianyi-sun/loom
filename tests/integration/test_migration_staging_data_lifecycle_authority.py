"""Migration 0066 adds fail-closed staging lifecycle authority."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from testcontainers.postgres import PostgresContainer

from loom.data_lifecycle_gc import (
    AuthorityInventory,
    GcScope,
    RegisteredObject,
    build_gc_plan,
    execute_gc,
)
from loom.data_lifecycle_gc_sql import SqlAlchemyGcJournal
from loom.staging_mutation_epoch import (
    MutationEpochAdvance,
    ProtectedMutationClass,
    advance_mutation_epoch,
)
from loom.staging_mutation_epoch_sql import SqlAlchemyMutationEpochStore


def _cfg(postgres_url: str) -> Config:
    repo_root = Path(__file__).resolve().parents[2]
    cfg = Config(str(repo_root / "migrations" / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", postgres_url)
    return cfg


@pytest.fixture(scope="module")
def postgres_url_at_0065() -> Iterator[str]:
    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace(
            "postgresql+psycopg2://",
            "postgresql+psycopg://",
        )
        command.upgrade(_cfg(url), "0065")
        yield url


def test_upgrade_adds_authority_journal_and_nullable_execution_links(
    postgres_url_at_0065: str,
) -> None:
    cfg = _cfg(postgres_url_at_0065)
    engine = create_engine(postgres_url_at_0065)
    try:
        command.upgrade(cfg, "head")
        with engine.connect() as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema='public' AND table_name LIKE 'data_lifecycle_%'"
                    )
                )
            }
            linked = {
                row[0]
                for row in conn.execute(
                    text(
                        "SELECT table_name FROM information_schema.columns "
                        "WHERE table_schema='public' "
                        "AND column_name='lifecycle_authority_id'"
                    )
                )
            }
            mutation_tables = {
                row[0]
                for row in conn.execute(
                    text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema='public' AND table_name LIKE 'staging_mutation_%'"
                    )
                )
            }
    finally:
        engine.dispose()

    assert tables == {
        "data_lifecycle_authorities",
        "data_lifecycle_objects",
        "data_lifecycle_gc_runs",
        "data_lifecycle_gc_items",
    }
    assert linked == {"batches", "trials", "llm_calls", "trial_events", "artifacts"}
    assert mutation_tables == {
        "staging_mutation_epochs",
        "staging_mutation_epoch_events",
    }


def test_constraints_reject_unbounded_or_cross_environment_authority(
    postgres_url_at_0065: str,
) -> None:
    engine = create_engine(postgres_url_at_0065)
    try:
        with engine.begin() as conn:
            with pytest.raises(IntegrityError):
                conn.execute(
                    text(
                        "INSERT INTO data_lifecycle_authorities "
                        "(environment, namespace, data_class, owner_kind, owner_id, pinned) "
                        "VALUES ('staging','loom-staging','trial','trial','bad',false)"
                    )
                )
        with engine.begin() as conn:
            with pytest.raises(IntegrityError):
                conn.execute(
                    text(
                        "INSERT INTO staging_mutation_epochs "
                        "(environment, namespace, epoch, reason) "
                        "VALUES ('production','loom-prod',0,'bad')"
                    )
                )
    finally:
        engine.dispose()


def test_epoch_compare_and_swap_updates_authority_and_appends_exact_event(
    postgres_url_at_0065: str,
) -> None:
    engine = create_engine(postgres_url_at_0065)
    occurred_at = datetime(2026, 7, 20, tzinfo=UTC)
    advance = MutationEpochAdvance(
        environment="staging",
        namespace="loom-staging",
        expected_epoch=0,
        mutation_class=ProtectedMutationClass.LIFECYCLE_GC,
        request_id="req-gc0000000",
        evidence_sha256="a" * 64,
        occurred_at=occurred_at,
    )
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO staging_mutation_epochs "
                    "(environment, namespace, epoch, reason) "
                    "VALUES ('staging','loom-staging',0,'bootstrap') "
                    "ON CONFLICT (environment) DO NOTHING"
                )
            )
            state = advance_mutation_epoch(
                SqlAlchemyMutationEpochStore(conn),
                advance,
            )
        with engine.connect() as conn:
            epoch_row = conn.execute(
                text(
                    "SELECT epoch, reason, request_id, evidence_sha256 "
                    "FROM staging_mutation_epochs WHERE environment='staging'"
                )
            ).one()
            event_row = conn.execute(
                text(
                    "SELECT epoch, mutation_class, request_id, evidence_sha256 "
                    "FROM staging_mutation_epoch_events WHERE environment='staging'"
                )
            ).one()
        assert state.epoch == 1
        assert tuple(epoch_row) == (1, "lifecycle_gc", "req-gc0000000", "a" * 64)
        assert tuple(event_row) == (1, "lifecycle_gc", "req-gc0000000", "a" * 64)

        with engine.begin() as conn:
            with pytest.raises(RuntimeError, match="stale"):
                advance_mutation_epoch(SqlAlchemyMutationEpochStore(conn), advance)
    finally:
        engine.dispose()


def test_sql_gc_journal_commits_exact_phases_and_mutation_epoch(
    postgres_url_at_0065: str,
) -> None:
    engine = create_engine(postgres_url_at_0065)
    authority_id = uuid4()
    object_id = uuid4()
    now = datetime(2026, 7, 20, 1, tzinfo=UTC)
    with engine.connect() as connection:
        epoch = connection.execute(
            text("SELECT epoch FROM staging_mutation_epochs WHERE environment='staging'")
        ).scalar_one()
    plan = build_gc_plan(
        scope=GcScope(environment="staging", namespace="loom-staging"),
        mutation_epoch=epoch,
        now=now,
        authorities=[
            AuthorityInventory(
                id=authority_id,
                environment="staging",
                namespace="loom-staging",
                owner_kind="trial",
                owner_id="integration-trial",
                expires_at=now - timedelta(days=1),
                pinned=False,
                state="active",
            )
        ],
        objects=[
            RegisteredObject(
                id=object_id,
                authority_id=authority_id,
                environment="staging",
                namespace="loom-staging",
                bucket="loom-staging-artifacts",
                object_key="integration/trial.json",
                version_id="version-1",
                content_sha256="b" * 64,
                size_bytes=7,
                state="active",
            )
        ],
    )

    class NoBusinessRows:
        def delete_exact(self, connection, authority_ids) -> None:
            assert authority_ids == (authority_id,)

    class Deleter:
        deleted = False

        def delete_exact(self, item) -> None:
            assert item.id == object_id
            self.deleted = True

        def exact_absent(self, item) -> bool:
            return self.deleted and item.id == object_id

    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO data_lifecycle_authorities "
                    "(id, environment, namespace, data_class, owner_kind, owner_id, "
                    "created_at, expires_at, pinned, state) VALUES "
                    "(:id,'staging','loom-staging','trial','trial','integration-trial',"
                    ":created_at,:expires_at,false,'active')"
                ),
                {
                    "id": authority_id,
                    "created_at": now - timedelta(days=8),
                    "expires_at": now - timedelta(days=1),
                },
            )
            connection.execute(
                text(
                    "INSERT INTO data_lifecycle_objects "
                    "(id, authority_id, environment, namespace, bucket, object_key, "
                    "version_id, content_sha256, size_bytes, created_at, state) VALUES "
                    "(:id,:authority_id,'staging','loom-staging','loom-staging-artifacts',"
                    "'integration/trial.json','version-1',:sha,7,:created_at,'active')"
                ),
                {
                    "id": object_id,
                    "authority_id": authority_id,
                    "sha": "b" * 64,
                    "created_at": now - timedelta(days=8),
                },
            )
        result = execute_gc(
            plan=plan,
            requested_by="qianyi",
            journal=SqlAlchemyGcJournal(
                engine,
                metadata_purger=NoBusinessRows(),
            ),
            object_deleter=Deleter(),
            dry_run=False,
            request_id="req-gcinteg00",
            completed_at=now,
        )
        with engine.connect() as connection:
            run = connection.execute(
                text(
                    "SELECT state, mutation_epoch_before, mutation_epoch_after "
                    "FROM data_lifecycle_gc_runs WHERE id=:id"
                ),
                {"id": result.run_id},
            ).one()
            item_state = connection.execute(
                text(
                    "SELECT state FROM data_lifecycle_gc_items "
                    "WHERE gc_run_id=:id AND object_id=:object_id"
                ),
                {"id": result.run_id, "object_id": object_id},
            ).scalar_one()
            authority_count = connection.execute(
                text("SELECT count(*) FROM data_lifecycle_authorities WHERE id=:id"),
                {"id": authority_id},
            ).scalar_one()
        assert tuple(run) == ("completed", epoch, epoch + 1)
        assert item_state == "metadata_deleted"
        assert authority_count == 0
    finally:
        engine.dispose()


def test_downgrade_refuses_to_discard_lifecycle_data(postgres_url_at_0065: str) -> None:
    cfg = _cfg(postgres_url_at_0065)
    engine = create_engine(postgres_url_at_0065)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO staging_mutation_epochs "
                    "(environment, namespace, epoch, reason) "
                    "VALUES ('staging','loom-staging',0,'bootstrap') "
                    "ON CONFLICT (environment) DO NOTHING"
                )
            )
        with pytest.raises(Exception, match="deployment data remains"):
            command.downgrade(cfg, "0065")
    finally:
        engine.dispose()
