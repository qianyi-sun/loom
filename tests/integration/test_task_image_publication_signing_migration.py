from __future__ import annotations

import hashlib
from datetime import timedelta
from uuid import UUID

import pytest
from alembic import command
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import DBAPIError

from loom.db import schema
from tests.integration.test_task_image_registry_credential_migration import (
    ATTEMPT_ID,
    CANDIDATE_ID,
    MATERIALIZATION_ID,
    NOW,
    _candidate_values,
    _config,
    _credential_values,
    _insert_attempt_prerequisites,
    _insert_candidate,
    _insert_credential,
    _insert_exchanged_projection,
)

TABLES = (
    "task_image_publication_state",
    "task_image_publication_keys",
    "task_image_publication_envelopes",
    "task_image_publication_jobs",
)


def test_0133_upgrade_downgrade_and_orm_parity(isolated_migration_postgres_url):
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0132")
    engine = create_engine(isolated_migration_postgres_url)
    try:
        assert not set(TABLES) & set(inspect(engine).get_table_names())
        command.upgrade(config, "head")
        assert set(TABLES) <= set(inspect(engine).get_table_names()), (
            "publication records are missing"
        )
        for model_name in (
            "TaskImagePublicationState",
            "TaskImagePublicationKey",
            "TaskImagePublicationEnvelope",
            "TaskImagePublicationJob",
        ):
            model = getattr(schema, model_name)
            actual = {column["name"] for column in inspect(engine).get_columns(model.__tablename__)}
            assert actual == set(model.__table__.columns.keys())
        with engine.connect() as connection:
            assert connection.execute(
                text(
                    "SELECT singleton_id, revocation_epoch, keyset_version FROM task_image_publication_state"
                )
            ).one() == (1, 0, 0)
            assert (
                connection.execute(
                    text("SELECT count(*) FROM task_image_publication_keys")
                ).scalar_one()
                == 0
            )
        foreign_keys = inspect(engine).get_foreign_keys(TABLES[2])
        assert {item["referred_table"] for item in foreign_keys} == {
            TABLES[1],
            "task_image_publication_candidates",
        }
        assert all(item["options"]["ondelete"] == "RESTRICT" for item in foreign_keys)
        command.downgrade(config, "0132")
        assert not set(TABLES) & set(inspect(engine).get_table_names())
        assert "task_image_publication_evidence" in inspect(engine).get_table_names()
        command.upgrade(config, "0133")
    finally:
        engine.dispose()


def _insert_key(connection, **changes):
    values = {
        "key_id": "publication-1",
        "public_key": bytes(range(32)),
        "activated_at": NOW,
        "status": "active",
        "retired_at": None,
        "revoked_at": None,
    }
    values.update(changes)
    connection.execute(
        text("""INSERT INTO task_image_publication_keys
        (key_id, public_key, activated_at, status, retired_at, revoked_at)
        VALUES (:key_id, :public_key, :activated_at, :status, :retired_at, :revoked_at)"""),
        values,
    )


def _reject(engine, sql, values=None):
    with pytest.raises(DBAPIError):
        with engine.begin() as connection:
            connection.execute(text(sql), values or {})


@pytest.mark.parametrize("used_authority", ["key", "keyset_version", "revocation_epoch"])
def test_0133_downgrade_refuses_used_publication_authority(
    isolated_migration_postgres_url,
    used_authority,
):
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as connection:
            if used_authority == "key":
                _insert_key(connection)
            else:
                connection.execute(
                    text(f"UPDATE task_image_publication_state SET {used_authority} = 1")
                )
            before = connection.execute(
                text(
                    "SELECT singleton_id, revocation_epoch, keyset_version "
                    "FROM task_image_publication_state"
                )
            ).one()
            key_count = connection.execute(
                text("SELECT count(*) FROM task_image_publication_keys")
            ).scalar_one()
        with pytest.raises(DBAPIError, match="publication authority cannot be discarded"):
            command.downgrade(_config(isolated_migration_postgres_url), "0132")
        assert set(TABLES) <= set(inspect(engine).get_table_names())
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT singleton_id, revocation_epoch, keyset_version "
                        "FROM task_image_publication_state"
                    )
                ).one()
                == before
            )
            assert (
                connection.execute(
                    text("SELECT count(*) FROM task_image_publication_keys")
                ).scalar_one()
                == key_count
            )
            assert (
                connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
                == "0133"
            )
    finally:
        engine.dispose()


def test_key_and_epoch_lifecycle_are_monotonic_and_share_singleton_lock(
    isolated_migration_postgres_url,
):
    engine = create_engine(isolated_migration_postgres_url)
    try:
        assert TABLES[1] in inspect(engine).get_table_names(), "publication key records are missing"
        for changes in (
            {"public_key": b"short"},
            {"status": "verify_only"},
            {"status": "revoked"},
            {"status": "active", "retired_at": NOW},
            {"key_id": "INVALID"},
            {"activated_at": NOW.replace(microsecond=1)},
            {"activated_at": "infinity"},
            {"status": "verify_only", "retired_at": NOW - timedelta(seconds=1)},
        ):
            with pytest.raises(DBAPIError):
                with engine.begin() as connection:
                    _insert_key(connection, **changes)
        with engine.begin() as connection:
            _insert_key(connection)
        for sql in (
            "UPDATE task_image_publication_keys SET public_key = decode(repeat('01',32),'hex')",
            "UPDATE task_image_publication_keys SET key_id = 'substitution'",
            "UPDATE task_image_publication_keys SET activated_at = activated_at + interval '1 second'",
            "DELETE FROM task_image_publication_keys",
            "DELETE FROM task_image_publication_state",
            "INSERT INTO task_image_publication_state (singleton_id) VALUES (2)",
            "UPDATE task_image_publication_state SET revocation_epoch = -1",
            "UPDATE task_image_publication_state SET keyset_version = 9007199254740992",
        ):
            _reject(engine, sql)
        with engine.begin() as first:
            first.execute(
                text(
                    "SELECT singleton_id FROM task_image_publication_state WHERE singleton_id = 1 FOR UPDATE"
                )
            )
            with pytest.raises(DBAPIError):
                with engine.begin() as second:
                    second.execute(text("SET LOCAL lock_timeout = '100ms'"))
                    second.execute(
                        text(
                            "UPDATE task_image_publication_keys SET status = 'verify_only', retired_at = :time"
                        ),
                        {"time": NOW + timedelta(seconds=1)},
                    )
        with engine.begin() as connection:
            connection.execute(text("UPDATE task_image_publication_state SET keyset_version = 3"))
            connection.execute(
                text(
                    "UPDATE task_image_publication_keys SET status = 'verify_only', retired_at = :time"
                ),
                {"time": NOW + timedelta(seconds=1)},
            )
        _reject(
            engine, "UPDATE task_image_publication_keys SET status = 'active', retired_at = NULL"
        )
        _reject(
            engine,
            "UPDATE task_image_publication_keys SET retired_at = retired_at + interval '1 second'",
        )
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE task_image_publication_keys SET status = 'revoked', revoked_at = :time"
                ),
                {"time": NOW + timedelta(seconds=2)},
            )
            assert (
                connection.execute(
                    text("SELECT revocation_epoch FROM task_image_publication_state")
                ).scalar_one()
                == 1
            )
        _reject(
            engine,
            "UPDATE task_image_publication_keys SET status = 'verify_only', revoked_at = NULL",
        )
        _reject(
            engine,
            "UPDATE task_image_publication_keys SET revoked_at = revoked_at + interval '1 second'",
        )
        _reject(engine, "UPDATE task_image_publication_state SET revocation_epoch = 0")
        _reject(engine, "UPDATE task_image_publication_state SET keyset_version = 2")
        with engine.begin() as connection:
            _insert_key(connection, key_id="publication-2")
            connection.execute(
                text(
                    "UPDATE task_image_publication_keys SET status = 'revoked', revoked_at = :time WHERE key_id = 'publication-2'"
                ),
                {"time": NOW + timedelta(seconds=2)},
            )
        _reject(
            engine,
            "UPDATE task_image_publication_keys SET retired_at = activated_at WHERE key_id = 'publication-2'",
        )
    finally:
        engine.dispose()


def test_envelopes_bind_candidate_attempt_component_key_and_cannot_rewrite_evidence(
    isolated_migration_postgres_url,
):
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0128")
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as connection:
            _insert_exchanged_projection(connection, authority_version=2)
        command.upgrade(config, "0130")
        with engine.begin() as connection:
            _insert_attempt_prerequisites(connection)
        command.upgrade(config, "head")
        assert TABLES[2] in inspect(engine).get_table_names(), "publication envelopes are missing"
        with engine.begin() as connection:
            _insert_credential(connection, _credential_values())
            _insert_candidate(connection, _candidate_values())
            _insert_key(connection)
        canonical = b'{"audit":"canonical bytes are validated at the application boundary"}'
        values = {
            "envelope_id": UUID("f2345678-1234-4123-8123-123456789abc"),
            "candidate_id": CANDIDATE_ID,
            "attempt_id": ATTEMPT_ID,
            "component": "task",
            "key_id": "publication-1",
            "canonical": canonical,
            "sha256": hashlib.sha256(canonical).hexdigest(),
            "signature": "A" * 86,
            "algorithm": "Ed25519",
            "issued_at": NOW,
            "recorded_at": NOW,
            "version": 3,
            "epoch": 0,
        }
        insert = """INSERT INTO task_image_publication_envelopes
            (envelope_id, candidate_id, materialization_attempt_id, component, key_id,
             canonical_statement, statement_sha256, signature, algorithm, issued_at,
             recorded_at, distributed_keyset_version, revocation_epoch)
            VALUES (:envelope_id, :candidate_id, :attempt_id, :component, :key_id,
             :canonical, :sha256, :signature, :algorithm, :issued_at, :recorded_at,
             :version, :epoch)"""
        for changes in (
            {"envelope_id": UUID(int=0)},
            {"component": "sidecar:redis"},
            {"attempt_id": UUID(int=1)},
            {"candidate_id": UUID(int=1)},
            {"key_id": "missing"},
            {"sha256": "f" * 64},
            {"canonical": b""},
            {"canonical": b"x" * 65537},
            {"signature": "bad"},
            {"algorithm": "RS256"},
            {"version": 0},
            {"epoch": -1},
            {"issued_at": NOW.replace(microsecond=1)},
        ):
            _reject(engine, insert, values | changes)
        _reject(engine, insert, values | {"issued_at": "infinity", "recorded_at": "infinity"})
        with engine.begin() as connection:
            before = connection.execute(
                text(
                    "SELECT state, ready_at, registry_images FROM task_image_materializations WHERE id = :id"
                ),
                {"id": MATERIALIZATION_ID},
            ).one()
            connection.execute(text(insert), values)
            after = connection.execute(
                text(
                    "SELECT state, ready_at, registry_images FROM task_image_materializations WHERE id = :id"
                ),
                {"id": MATERIALIZATION_ID},
            ).one()
            assert before == after
        _reject(engine, insert, values | {"envelope_id": UUID(int=5)})
        for column, replacement in (
            ("canonical_statement", "convert_to('{}','UTF8')"),
            ("signature", "repeat('B',86)"),
            ("key_id", "'different'"),
            ("recorded_at", "recorded_at + interval '1 second'"),
        ):
            _reject(engine, f"UPDATE task_image_publication_envelopes SET {column} = {replacement}")
        _reject(engine, "DELETE FROM task_image_publication_envelopes")
        _reject(engine, "DELETE FROM task_image_publication_candidates")
        _reject(engine, "DELETE FROM task_image_materialization_attempts")
    finally:
        engine.dispose()
