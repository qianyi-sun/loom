"""Exercise the historical Nebius fork on real, disposable PostgreSQL."""

from __future__ import annotations

import json
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError

from migrations.nebius_lineage import convert_lineage, inspect_lineage


def _scripts() -> ScriptDirectory:
    return ScriptDirectory.from_config(Config("migrations/alembic.ini"))


def _historical(url: str, revision: str) -> None:
    config = Config("migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    command.downgrade(config, "0132")
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            # These are the integrated copies of Nebius 0133–0135 at
            # 7912ec076babc2fe51768fd2596b70c7de9e3d31. Shared history ends at 0132.
            with Operations.context(MigrationContext.configure(connection)):
                for number in range(144, int(revision) + 12):
                    script = _scripts().get_revision(f"{number:04}")
                    assert script is not None
                    script.module.upgrade()
            connection.execute(text("UPDATE alembic_version SET version_num=:rev"), {"rev": revision})
    finally:
        engine.dispose()


def _snapshot(connection: Connection) -> dict[str, object]:
    return {
        table: list(connection.exec_driver_sql(f"SELECT to_jsonb(t) FROM {table} t ORDER BY id").scalars())
        for table in ("trials", "task_image_materialization_attempts")
    }


@pytest.mark.parametrize("revision", ["0133", "0134", "0135"])
def test_conversion_preserves_history_and_reaches_dev(
    isolated_migration_postgres_url: str, revision: str,
) -> None:
    url = isolated_migration_postgres_url
    _historical(url, revision)
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            team, trial, materialization, attempt = (uuid4() for _ in range(4))
            connection.execute(text("INSERT INTO teams (id,name) VALUES (:id,:name)"),
                               {"id": team, "name": str(team)})
            connection.execute(text("INSERT INTO tasks (id,config,checksum) VALUES ('lineage','{}',:sha)"),
                               {"sha": "a" * 64})
            connection.execute(text("""
                INSERT INTO trials (id,team_id,task_id,config,requires_caps,state,result,trajectory_index)
                VALUES (:id,:team,'lineage','{}','{}','succeeded',:result,'{"artifacts":["retained"]}')
            """), {"id": trial, "team": team, "result": json.dumps({
                "schema_version": "loom.service-execution-trial-result.v1",
                "aggregate_reward": 1.0, "reward": {"passed": 1.0},
                "runtime_result": {"verifier_rewards": {"passed": 1.0}},
                "usage": {"total_tokens": 59},
            })})
            connection.execute(text("""
                INSERT INTO task_image_materializations
                    (id, materialization_key, task_id, task_checksum, cpu_arch, task_config)
                VALUES (:id,:key,'lineage',:sha,'amd64','{}')
            """), {"id": materialization, "key": "b" * 64, "sha": "a" * 64})
            connection.execute(text("""
                INSERT INTO task_image_materialization_attempts
                    (id,materialization_id,attempt_number,lease_epoch,builder_id,claimed_at)
                VALUES (:id,:materialization,1,1,'nebius:retained',now())
            """), {"id": attempt, "materialization": materialization})
            if revision == "0135":
                connection.exec_driver_sql("""UPDATE task_image_materialization_attempts
                    SET native_build='{"job_uid":"retained-native-job","cpu_millis":500}'""")
            before = _snapshot(connection)
            assert inspect_lineage(connection, revision) == revision
        with engine.begin() as connection:
            convert_lineage(connection, _scripts(), revision)
        with engine.connect() as connection:
            after = _snapshot(connection)
            if revision != "0135":
                for row in before["task_image_materialization_attempts"]:
                    row["native_build"] = None
            assert after == before
            assert connection.exec_driver_sql("SELECT version_num FROM alembic_version").scalar_one() == "0146"
            assert connection.exec_driver_sql("SELECT to_regclass('gateway_dispatch_receipts')").scalar_one()
            assert connection.exec_driver_sql("SELECT to_regclass('task_image_publication_keysets')").scalar_one()
            # A repeat or wrong-lineage invocation cannot reinterpret dev history.
            with pytest.raises(ValueError, match="revision"):
                inspect_lineage(connection, revision)
        config = Config("migrations/alembic.ini")
        config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(config, "head")
    finally:
        engine.dispose()


def test_conversion_rolls_back_all_ddl_on_midway_failure(isolated_migration_postgres_url: str) -> None:
    url = isolated_migration_postgres_url
    _historical(url, "0135")
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            # 0136 must reject unexpected provenance, after 0133–0135 have run.
            connection.execute(text("""
                INSERT INTO task_image_materializations
                    (id,materialization_key,task_id,task_checksum,cpu_arch,task_config,task_source_provenance)
                VALUES (:id,:key,'lineage-drift',:sha,'amd64','{}',
                        '{"bundle_content_manifest_sha256":"unexpected"}')
            """), {"id": uuid4(), "key": "c" * 64, "sha": "d" * 64})
        with pytest.raises(DBAPIError, match="unexpected content-manifest"):
            with engine.begin() as connection:
                convert_lineage(connection, _scripts(), "0135")
        with engine.connect() as connection:
            assert inspect_lineage(connection, "0135") == "0135"
            assert connection.exec_driver_sql("SELECT to_regclass('gateway_dispatch_receipts')").scalar_one() is None
            assert connection.exec_driver_sql("SELECT to_regclass('task_image_publication_keys')").scalar_one() is None
    finally:
        engine.dispose()


def test_busy_writer_fails_without_partial_conversion(isolated_migration_postgres_url: str) -> None:
    url = isolated_migration_postgres_url
    _historical(url, "0135")
    engine = create_engine(url)
    try:
        with engine.begin() as writer:
            writer.exec_driver_sql("LOCK TABLE trials IN ROW EXCLUSIVE MODE")
            with pytest.raises(DBAPIError, match="could not obtain lock"):
                with engine.begin() as connection:
                    convert_lineage(connection, _scripts(), "0135")
        with engine.connect() as connection:
            assert inspect_lineage(connection, "0135") == "0135"
    finally:
        engine.dispose()


@pytest.mark.parametrize("drift", ["native_type", "native_missing", "dev_marker", "quota", "multiple_heads"])
def test_conversion_rejects_source_drift(isolated_migration_postgres_url: str, drift: str) -> None:
    url = isolated_migration_postgres_url
    _historical(url, "0135")
    engine = create_engine(url)
    changes = {
        "native_type": "ALTER TABLE task_image_materialization_attempts ALTER COLUMN native_build TYPE text",
        "native_missing": "ALTER TABLE task_image_materialization_attempts DROP COLUMN native_build",
        "dev_marker": "CREATE TABLE gateway_dispatch_receipts (id uuid)",
        "quota": "ALTER TABLE execution_capacity_observations DROP CONSTRAINT execution_capacity_observations_quota_check",
        "multiple_heads": "INSERT INTO alembic_version VALUES ('0134')",
    }
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(changes[drift])
        with pytest.raises(ValueError):
            with engine.begin() as connection:
                convert_lineage(connection, _scripts(), "0135")
    finally:
        engine.dispose()
