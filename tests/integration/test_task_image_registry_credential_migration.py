from __future__ import annotations

import json
from datetime import timedelta
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError

from loom.db.schema import (
    TaskImagePublicationCandidate,
    TaskImageRegistryCredentialGeneration,
)
from tests.integration.test_task_image_session_generation_migration import (
    ATTESTATION_SHA256,
    GRANT_ID,
    NOW,
    SESSION_ID,
    _insert_exchanged_projection,
)

MATERIALIZATION_ID = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
ATTEMPT_ID = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
CLAIM_ID = UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
HEARTBEAT_ID = UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
CREDENTIAL_ID = UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee")
REQUEST_ID = UUID("ffffffff-ffff-4fff-8fff-ffffffffffff")
CANDIDATE_ID = UUID("12345678-1234-4123-8123-123456789abc")
CANDIDATE_OPERATION_ID = UUID("22345678-1234-4123-8123-123456789abc")
BUILDER_ID = f"rootless:{SESSION_ID.hex}"
REPOSITORY = f"loom-task-image-attempts/arm64/{ATTEMPT_ID}/task"


def _config(postgres_url: str) -> Config:
    config = Config("migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url)
    return config


def _insert_attempt_prerequisites(connection: Connection) -> None:
    connection.execute(
        text(
            """
            INSERT INTO task_image_materializations (
                id, materialization_key, task_id, task_checksum,
                cpu_arch, task_config
            ) VALUES (
                :id, :key, 'phase2d1-migration', :checksum,
                'arm64', '{}'::jsonb
            )
            """
        ),
        {"id": MATERIALIZATION_ID, "key": "8" * 64, "checksum": "9" * 64},
    )
    connection.execute(
        text(
            """
            INSERT INTO task_image_materialization_attempts (
                id, materialization_id, attempt_number, lease_epoch,
                builder_id, claimed_at, grant_id, session_id,
                session_generation, claim_id,
                claim_deterministic_failure_count,
                claim_lease_expires_at, claim_plan_json,
                claim_plan_sha256
            ) VALUES (
                :attempt_id, :materialization_id, 1, 1,
                :builder_id, :now, :grant_id, :session_id, 1,
                :claim_id, 0, :lease_expires_at,
                '{"components":["task"]}'::jsonb, :plan_sha256
            )
            """
        ),
        {
            "attempt_id": ATTEMPT_ID,
            "materialization_id": MATERIALIZATION_ID,
            "builder_id": BUILDER_ID,
            "now": NOW,
            "grant_id": GRANT_ID,
            "session_id": SESSION_ID,
            "claim_id": CLAIM_ID,
            "lease_expires_at": NOW + timedelta(minutes=1),
            "plan_sha256": "f" * 64,
        },
    )
    connection.execute(
        text(
            """
            INSERT INTO task_image_materialization_operation_events (
                operation_id, operation_type,
                materialization_attempt_id, materialization_id,
                attempt_number, lease_epoch, builder_id, grant_id,
                session_id, session_generation, result_state,
                result_attempt_count, result_lease_expires_at, recorded_at
            ) VALUES (
                :operation_id, 'heartbeat', :attempt_id,
                :materialization_id, 1, 1, :builder_id, :grant_id,
                :session_id, 1, 'running', 1, :lease_expires_at,
                :recorded_at
            )
            """
        ),
        {
            "operation_id": HEARTBEAT_ID,
            "attempt_id": ATTEMPT_ID,
            "materialization_id": MATERIALIZATION_ID,
            "builder_id": BUILDER_ID,
            "grant_id": GRANT_ID,
            "session_id": SESSION_ID,
            "lease_expires_at": NOW + timedelta(minutes=1),
            "recorded_at": NOW + timedelta(seconds=20),
        },
    )


def _credential_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "credential_id": CREDENTIAL_ID,
        "request_id": REQUEST_ID,
        "attempt_id": ATTEMPT_ID,
        "materialization_id": MATERIALIZATION_ID,
        "attempt_number": 1,
        "lease_epoch": 1,
        "builder_id": BUILDER_ID,
        "grant_id": GRANT_ID,
        "session_id": SESSION_ID,
        "session_generation": 1,
        "attestation_generation": 1,
        "attestation_sha256": ATTESTATION_SHA256,
        "component": "task",
        "generation": 1,
        "predecessor_id": None,
        "heartbeat_id": None,
        "repository": REPOSITORY,
        "registry_origin": "https://registry.example",
        "registry_service": "registry.example",
        "registry_issuer": "loom-task-image-authority",
        "registry_key_id": "K" * 43,
        "request_sha256": "1" * 64,
        "response_json": json.dumps({"schema_version": 1}),
        "response_sha256": "2" * 64,
        "token_hash": b"t" * 32,
        "secret_ref": (
            "loom://task-image-registry-credential/32345678-1234-4123-8123-123456789abc"
        ),
        "issued_at": NOW + timedelta(seconds=7),
        "expires_at": NOW + timedelta(seconds=40),
        "recorded_at": NOW + timedelta(seconds=8),
    }
    values.update(overrides)
    return values


def _insert_credential(connection: Connection, values: dict[str, object]) -> None:
    connection.execute(
        text(
            """
            INSERT INTO task_image_registry_credentials (
                credential_id, request_id, materialization_attempt_id,
                materialization_id, attempt_number, lease_epoch, builder_id,
                grant_id, session_id, session_generation,
                attestation_generation, attestation_sha256, component,
                generation, predecessor_credential_id,
                lease_heartbeat_operation_id, repository, registry_origin,
                registry_service, registry_issuer, registry_key_id,
                request_sha256, response_public_json, response_sha256,
                token_hash, secret_response_ref, issued_at, expires_at,
                recorded_at
            ) VALUES (
                :credential_id, :request_id, :attempt_id,
                :materialization_id, :attempt_number, :lease_epoch,
                :builder_id, :grant_id, :session_id, :session_generation,
                :attestation_generation, :attestation_sha256, :component,
                :generation, :predecessor_id, :heartbeat_id, :repository,
                :registry_origin, :registry_service, :registry_issuer,
                :registry_key_id, :request_sha256,
                CAST(:response_json AS jsonb), :response_sha256, :token_hash,
                :secret_ref, :issued_at, :expires_at, :recorded_at
            )
            """
        ),
        values,
    )


def _candidate_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "candidate_id": CANDIDATE_ID,
        "operation_id": CANDIDATE_OPERATION_ID,
        "credential_id": CREDENTIAL_ID,
        "attempt_id": ATTEMPT_ID,
        "materialization_id": MATERIALIZATION_ID,
        "attempt_number": 1,
        "lease_epoch": 1,
        "builder_id": BUILDER_ID,
        "grant_id": GRANT_ID,
        "session_id": SESSION_ID,
        "session_generation": 1,
        "component": "task",
        "repository": REPOSITORY,
        "manifest_digest": "sha256:" + "3" * 64,
        "manifest_size": 512,
        "oci_file_sha256": "4" * 64,
        "oci_file_size": 4096,
        "platform": "linux/arm64",
        "response_json": json.dumps({"schema_version": 1}),
        "response_sha256": "5" * 64,
        "recorded_at": NOW + timedelta(seconds=9),
    }
    values.update(overrides)
    return values


def _insert_candidate(connection: Connection, values: dict[str, object]) -> None:
    connection.execute(
        text(
            """
            INSERT INTO task_image_publication_candidates (
                candidate_id, operation_id, credential_id,
                materialization_attempt_id, materialization_id,
                attempt_number, lease_epoch, builder_id, grant_id,
                session_id, session_generation, component, repository,
                manifest_digest, manifest_size, oci_file_sha256,
                oci_file_size, platform, response_json, response_sha256,
                recorded_at
            ) VALUES (
                :candidate_id, :operation_id, :credential_id, :attempt_id,
                :materialization_id, :attempt_number, :lease_epoch,
                :builder_id, :grant_id, :session_id, :session_generation,
                :component, :repository, :manifest_digest, :manifest_size,
                :oci_file_sha256, :oci_file_size, :platform,
                CAST(:response_json AS jsonb), :response_sha256, :recorded_at
            )
            """
        ),
        values,
    )


def test_0131_adds_fenced_registry_credentials_and_inert_candidates(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0130")
    engine = create_engine(isolated_migration_postgres_url)
    try:
        command.upgrade(config, "0131")
        inspector = inspect(engine)
        tables = set(inspector.get_table_names())
        assert TaskImageRegistryCredentialGeneration.__tablename__ in tables
        assert TaskImagePublicationCandidate.__tablename__ in tables

        credential_table = TaskImageRegistryCredentialGeneration.__tablename__
        credential_columns = {column["name"] for column in inspector.get_columns(credential_table)}
        assert credential_columns == {
            "credential_id",
            "request_id",
            "materialization_attempt_id",
            "materialization_id",
            "attempt_number",
            "lease_epoch",
            "builder_id",
            "grant_id",
            "session_id",
            "session_generation",
            "attestation_generation",
            "attestation_sha256",
            "component",
            "generation",
            "predecessor_credential_id",
            "lease_heartbeat_operation_id",
            "repository",
            "registry_origin",
            "registry_service",
            "registry_issuer",
            "registry_key_id",
            "request_sha256",
            "response_public_json",
            "response_sha256",
            "token_hash",
            "secret_response_ref",
            "issued_at",
            "expires_at",
            "recorded_at",
        }
        assert set(TaskImageRegistryCredentialGeneration.__table__.columns.keys()) == (
            credential_columns
        )
        assert inspector.get_pk_constraint(credential_table)["constrained_columns"] == [
            "credential_id"
        ]
        assert "bearer_token" not in credential_columns
        credential_uniques = {
            item["name"]: tuple(item["column_names"])
            for item in inspector.get_unique_constraints(credential_table)
        }
        assert credential_uniques == {
            "task_image_registry_credentials_component_generation_uidx": (
                "materialization_attempt_id",
                "component",
                "generation",
            ),
            "task_image_registry_credentials_candidate_binding_uidx": (
                "credential_id",
                "materialization_attempt_id",
                "component",
                "repository",
            ),
            "task_image_registry_credentials_request_uidx": ("request_id",),
        }
        credential_foreign_keys = {
            item["name"]: item for item in inspector.get_foreign_keys(credential_table)
        }
        assert set(credential_foreign_keys) == {
            "task_image_registry_credentials_attempt_fkey",
            "task_image_registry_credentials_attestation_fkey",
            "task_image_registry_credentials_heartbeat_fkey",
            "task_image_registry_credentials_predecessor_fkey",
            "task_image_registry_credentials_session_fkey",
        }
        assert all(
            item["options"].get("ondelete") == "RESTRICT"
            for item in credential_foreign_keys.values()
        )
        assert {
            name: (
                tuple(item["constrained_columns"]),
                item["referred_table"],
                tuple(item["referred_columns"]),
            )
            for name, item in credential_foreign_keys.items()
        } == {
            "task_image_registry_credentials_attempt_fkey": (
                (
                    "materialization_attempt_id",
                    "materialization_id",
                    "attempt_number",
                    "lease_epoch",
                    "builder_id",
                    "grant_id",
                ),
                "task_image_materialization_attempts",
                (
                    "id",
                    "materialization_id",
                    "attempt_number",
                    "lease_epoch",
                    "builder_id",
                    "grant_id",
                ),
            ),
            "task_image_registry_credentials_session_fkey": (
                ("grant_id", "session_generation", "session_id"),
                "task_image_build_session_generations",
                ("grant_id", "generation", "session_id"),
            ),
            "task_image_registry_credentials_attestation_fkey": (
                ("grant_id", "attestation_generation"),
                "task_image_build_containment_attestations",
                ("grant_id", "generation"),
            ),
            "task_image_registry_credentials_predecessor_fkey": (
                ("predecessor_credential_id",),
                "task_image_registry_credentials",
                ("credential_id",),
            ),
            "task_image_registry_credentials_heartbeat_fkey": (
                ("lease_heartbeat_operation_id",),
                "task_image_materialization_operation_events",
                ("operation_id",),
            ),
        }
        credential_checks = {
            item["name"] for item in inspector.get_check_constraints(credential_table)
        }
        assert credential_checks == {
            "task_image_registry_credentials_binding_check",
            "task_image_registry_credentials_chain_check",
            "task_image_registry_credentials_digest_check",
            "task_image_registry_credentials_registry_check",
            "task_image_registry_credentials_secret_check",
            "task_image_registry_credentials_time_check",
        }
        credential_indexes = {
            item["name"]: tuple(item["column_names"])
            for item in inspector.get_indexes(credential_table)
        }
        assert credential_indexes == credential_uniques | {
            "task_image_registry_credentials_expiry_idx": (
                "expires_at",
                "credential_id",
            ),
            "task_image_registry_credentials_renewal_idx": (
                "materialization_attempt_id",
                "component",
                "generation",
            ),
        }
        renewal_model_index = next(
            index
            for index in TaskImageRegistryCredentialGeneration.__table__.indexes
            if index.name == "task_image_registry_credentials_renewal_idx"
        )
        assert tuple(str(expression) for expression in renewal_model_index.expressions) == (
            "task_image_registry_credentials.materialization_attempt_id",
            "task_image_registry_credentials.component",
            "generation DESC",
        )
        with engine.connect() as connection:
            renewal_index_definition = connection.execute(
                text(
                    "SELECT pg_get_indexdef("
                    "'task_image_registry_credentials_renewal_idx'::regclass)"
                )
            ).scalar_one()
        assert renewal_index_definition.endswith(
            "(materialization_attempt_id, component, generation DESC)"
        )

        candidate_table = TaskImagePublicationCandidate.__tablename__
        candidate_columns = {column["name"] for column in inspector.get_columns(candidate_table)}
        assert candidate_columns == {
            "candidate_id",
            "operation_id",
            "credential_id",
            "materialization_attempt_id",
            "materialization_id",
            "attempt_number",
            "lease_epoch",
            "builder_id",
            "grant_id",
            "session_id",
            "session_generation",
            "component",
            "repository",
            "manifest_digest",
            "manifest_size",
            "oci_file_sha256",
            "oci_file_size",
            "platform",
            "response_json",
            "response_sha256",
            "recorded_at",
        }
        assert set(TaskImagePublicationCandidate.__table__.columns.keys()) == (candidate_columns)
        assert inspector.get_pk_constraint(candidate_table)["constrained_columns"] == [
            "candidate_id"
        ]
        candidate_uniques = {
            item["name"]: tuple(item["column_names"])
            for item in inspector.get_unique_constraints(candidate_table)
        }
        assert candidate_uniques == {
            "task_image_publication_candidates_attempt_component_uidx": (
                "materialization_attempt_id",
                "component",
            ),
            "task_image_publication_candidates_operation_uidx": ("operation_id",),
        }
        candidate_foreign_keys = {
            item["name"]: item for item in inspector.get_foreign_keys(candidate_table)
        }
        assert set(candidate_foreign_keys) == {
            "task_image_publication_candidates_attempt_fkey",
            "task_image_publication_candidates_credential_fkey",
            "task_image_publication_candidates_session_fkey",
        }
        assert all(
            item["options"].get("ondelete") == "RESTRICT"
            for item in candidate_foreign_keys.values()
        )
        assert {
            name: (
                tuple(item["constrained_columns"]),
                item["referred_table"],
                tuple(item["referred_columns"]),
            )
            for name, item in candidate_foreign_keys.items()
        } == {
            "task_image_publication_candidates_attempt_fkey": (
                (
                    "materialization_attempt_id",
                    "materialization_id",
                    "attempt_number",
                    "lease_epoch",
                    "builder_id",
                    "grant_id",
                ),
                "task_image_materialization_attempts",
                (
                    "id",
                    "materialization_id",
                    "attempt_number",
                    "lease_epoch",
                    "builder_id",
                    "grant_id",
                ),
            ),
            "task_image_publication_candidates_session_fkey": (
                ("grant_id", "session_generation", "session_id"),
                "task_image_build_session_generations",
                ("grant_id", "generation", "session_id"),
            ),
            "task_image_publication_candidates_credential_fkey": (
                (
                    "credential_id",
                    "materialization_attempt_id",
                    "component",
                    "repository",
                ),
                "task_image_registry_credentials",
                (
                    "credential_id",
                    "materialization_attempt_id",
                    "component",
                    "repository",
                ),
            ),
        }
        assert {item["name"] for item in inspector.get_check_constraints(candidate_table)} == {
            "task_image_publication_candidates_binding_check",
            "task_image_publication_candidates_digest_check",
            "task_image_publication_candidates_response_check",
        }
        candidate_indexes = {
            item["name"]: tuple(item["column_names"])
            for item in inspector.get_indexes(candidate_table)
        }
        assert candidate_indexes == candidate_uniques | {
            "task_image_publication_candidates_observed_idx": (
                "recorded_at",
                "candidate_id",
            )
        }

        command.downgrade(config, "0130")
        downgraded_tables = set(inspect(engine).get_table_names())
        assert credential_table not in downgraded_tables
        assert candidate_table not in downgraded_tables
        assert "task_image_build_session_generations" in downgraded_tables
    finally:
        engine.dispose()


def test_0131_rejects_invalid_authority_rows_and_keeps_candidates_inert(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0128")
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as connection:
            _insert_exchanged_projection(connection, authority_version=2)
        command.upgrade(config, "0130")
        with engine.begin() as connection:
            _insert_attempt_prerequisites(connection)
        command.upgrade(config, "0131")

        invalid_credentials = [
            _credential_values(credential_id=UUID(int=0)),
            _credential_values(request_id=UUID(int=0)),
            _credential_values(generation=0),
            _credential_values(generation=513),
            _credential_values(
                generation=2,
                credential_id=UUID("42345678-1234-4123-8123-123456789abc"),
                request_id=UUID("52345678-1234-4123-8123-123456789abc"),
            ),
            _credential_values(predecessor_id=UUID("62345678-1234-4123-8123-123456789abc")),
            _credential_values(attestation_sha256="0" * 64),
            _credential_values(request_sha256="not-a-sha256"),
            _credential_values(response_sha256="A" * 64),
            _credential_values(response_json=json.dumps([])),
            _credential_values(token_hash=b"short"),
            _credential_values(registry_key_id="too-short"),
            _credential_values(repository="loom-task-image-attempts/arm64/not-a-uuid/task"),
            _credential_values(repository=REPOSITORY.upper()),
            _credential_values(
                secret_ref=(
                    "loom://task-image-registry-credential/"
                    "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJjcmVkZW50aWFsIn0.signature"
                )
            ),
            _credential_values(
                expires_at=NOW + timedelta(seconds=7),
            ),
            _credential_values(
                expires_at=NOW + timedelta(seconds=54),
            ),
        ]
        for values in invalid_credentials:
            with pytest.raises(IntegrityError):
                with engine.begin() as connection:
                    _insert_credential(connection, values)

        with engine.begin() as connection:
            _insert_credential(connection, _credential_values())

        duplicate_generation = _credential_values(
            credential_id=UUID("72345678-1234-4123-8123-123456789abc"),
            request_id=UUID("82345678-1234-4123-8123-123456789abc"),
        )
        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                _insert_credential(connection, duplicate_generation)

        invalid_candidates = [
            _candidate_values(candidate_id=UUID(int=0)),
            _candidate_values(operation_id=UUID(int=0)),
            _candidate_values(credential_id=UUID("92345678-1234-4123-8123-123456789abc")),
            _candidate_values(component="sidecar:redis"),
            _candidate_values(repository="loom-task-image-attempts/arm64/not-a-uuid/task"),
            _candidate_values(manifest_digest="sha256:" + "0" * 64),
            _candidate_values(manifest_digest="not-a-digest"),
            _candidate_values(manifest_size=0),
            _candidate_values(oci_file_sha256="0" * 64),
            _candidate_values(oci_file_size=0),
            _candidate_values(platform="linux/s390x"),
            _candidate_values(response_json=json.dumps([])),
            _candidate_values(response_sha256="not-a-sha256"),
        ]
        for values in invalid_candidates:
            with pytest.raises(IntegrityError):
                with engine.begin() as connection:
                    _insert_candidate(connection, values)

        with engine.connect() as connection:
            before = (
                connection.execute(
                    text(
                        """
                    SELECT state, registry_images, registry_image_history,
                           ready_at, failure_reason, failure_message
                      FROM task_image_materializations
                     WHERE id = :id
                    """
                    ),
                    {"id": MATERIALIZATION_ID},
                )
                .mappings()
                .one()
            )
        with engine.begin() as connection:
            _insert_candidate(connection, _candidate_values())
        with engine.connect() as connection:
            after = (
                connection.execute(
                    text(
                        """
                    SELECT state, registry_images, registry_image_history,
                           ready_at, failure_reason, failure_message
                      FROM task_image_materializations
                     WHERE id = :id
                    """
                    ),
                    {"id": MATERIALIZATION_ID},
                )
                .mappings()
                .one()
            )
        assert dict(after) == dict(before)

        duplicate_candidate = _candidate_values(
            candidate_id=UUID("a2345678-1234-4123-8123-123456789abc"),
            operation_id=UUID("b2345678-1234-4123-8123-123456789abc"),
        )
        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                _insert_candidate(connection, duplicate_candidate)
    finally:
        engine.dispose()
