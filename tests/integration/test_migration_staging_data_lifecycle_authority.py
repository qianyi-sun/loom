"""Migration 0066 adds fail-closed staging lifecycle authority."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from testcontainers.postgres import PostgresContainer


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


def test_downgrade_refuses_to_discard_lifecycle_data(postgres_url_at_0065: str) -> None:
    cfg = _cfg(postgres_url_at_0065)
    engine = create_engine(postgres_url_at_0065)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO staging_mutation_epochs "
                    "(environment, namespace, epoch, reason) "
                    "VALUES ('staging','loom-staging',0,'bootstrap')"
                )
            )
        with pytest.raises(Exception, match="deployment data remains"):
            command.downgrade(cfg, "0065")
    finally:
        engine.dispose()
