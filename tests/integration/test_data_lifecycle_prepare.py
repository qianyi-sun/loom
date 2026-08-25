from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

from loom.data_lifecycle_gc import GcScope
from loom.data_lifecycle_prepare import (
    LifecyclePrepareError,
    LifecycleSourceIdentity,
    SqlAlchemyLifecyclePreparer,
)
from loom_cli.rollout.migration_readiness import inspect_migration_plan

_ROOT = Path(__file__).resolve().parents[2]


def _config(url: str) -> Config:
    config = Config(str(_ROOT / "migrations" / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    return config


@pytest.fixture(scope="module")
def postgres_at_0065() -> Iterator[str]:
    with PostgresContainer("postgres:16") as postgres:
        url = postgres.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+psycopg://"
        )
        command.upgrade(_config(url), "0065")
        yield url


def test_digest_approved_prepare_migrates_and_bootstraps_epoch_zero(
    postgres_at_0065: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    engine = create_engine(postgres_at_0065)
    try:
        migration = inspect_migration_plan(
            _ROOT / "migrations" / "alembic.ini",
            policy_path=_ROOT / "config" / "staging-migration-policy.json",
        )
        preparer = SqlAlchemyLifecyclePreparer(
            engine,
            alembic_config_path=_ROOT / "migrations" / "alembic.ini",
            source=LifecycleSourceIdentity(
                candidate_sha="1" * 40,
                candidate_tree="2" * 40,
                approved_base_sha="3" * 40,
            ),
            migration_policy_sha256=migration.policy_digest,
            migration_plan_sha256=migration.plan_digest,
            migration_target_revision=migration.head,
        )
        scope = GcScope(environment="staging", namespace="loom-staging")
        plan = preparer.inventory(scope=scope)
        assert plan.current_revision == "0065"
        assert plan.target_revision == "0110"
        assert plan.applicable
        assert plan.lifecycle_tables == ()
        assert plan.linked_execution_tables == ()

        with pytest.raises(LifecyclePrepareError, match="digest does not match"):
            preparer.apply(plan=plan, approved_inventory_digest="0" * 64)

        # A crash after either upstream-only migration cannot reuse the old
        # approval. Fresh inventory recognizes each exact pre-lifecycle state
        # without inventing lifecycle tables or bootstrap authority.
        command.upgrade(_config(postgres_at_0065), "0066")
        with pytest.raises(LifecyclePrepareError, match="inventory drifted"):
            preparer.apply(plan=plan, approved_inventory_digest=plan.inventory_digest)
        partial = preparer.inventory(scope=scope)
        assert partial.current_revision == "0066"
        assert partial.applicable
        assert partial.bootstrap is None
        assert partial.lifecycle_tables == ()
        assert partial.linked_execution_tables == ()

        command.upgrade(_config(postgres_at_0065), "0067")
        with pytest.raises(LifecyclePrepareError, match="inventory drifted"):
            preparer.apply(plan=partial, approved_inventory_digest=partial.inventory_digest)
        partial = preparer.inventory(scope=scope)
        assert partial.current_revision == "0067"
        assert partial.applicable
        assert partial.bootstrap is None
        assert partial.lifecycle_tables == ()
        assert partial.linked_execution_tables == ()

        # One concurrent preparer owns the fixed advisory lock; there is no
        # waiting race or second migration attempt.
        with engine.connect() as other:
            assert other.execute(
                text("SELECT pg_try_advisory_lock(:key)"),
                {"key": int.from_bytes(b"LOOMLIFE", "big")},
            ).scalar_one()
            with pytest.raises(LifecyclePrepareError, match="holds the advisory lock"):
                preparer.apply(
                    plan=partial,
                    approved_inventory_digest=partial.inventory_digest,
                )
            assert other.execute(
                text("SELECT pg_advisory_unlock(:key)"),
                {"key": int.from_bytes(b"LOOMLIFE", "big")},
            ).scalar_one()

        # Installed execution is intentionally independent of the caller CWD.
        monkeypatch.chdir(tmp_path)
        converged = preparer.apply(
            plan=partial,
            approved_inventory_digest=partial.inventory_digest,
        )
        assert converged.current_revision == "0110"
        assert converged.converged
        assert preparer.apply(
            plan=converged,
            approved_inventory_digest=converged.inventory_digest,
        ).converged
        with engine.connect() as connection:
            revision = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            epoch = connection.execute(
                text(
                    "SELECT environment,namespace,epoch,reason,request_id,evidence_sha256 "
                    "FROM staging_mutation_epochs"
                )
            ).one()
            events = connection.execute(
                text("SELECT count(*) FROM staging_mutation_epoch_events")
            ).scalar_one()
        assert revision == "0110"
        assert tuple(epoch) == ("staging", "loom-staging", 0, "bootstrap", None, None)
        assert events == 0
    finally:
        engine.dispose()
