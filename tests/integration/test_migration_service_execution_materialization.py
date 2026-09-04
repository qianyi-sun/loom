from __future__ import annotations

from uuid import uuid4

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text


def _config(url: str) -> Config:
    config = Config("migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    return config


def test_service_execution_materialization_migration_round_trip(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0128")
    engine = create_engine(isolated_migration_postgres_url)
    try:
        before = {item["name"] for item in inspect(engine).get_columns("execution_leases")}
        assert "materialization_state" not in before

        command.upgrade(config, "0129")
        columns = {item["name"] for item in inspect(engine).get_columns("execution_leases")}
        assert {
            "materialization_state",
            "materialization_attempts",
            "materialization_next_attempt_at",
            "materialization_claim_id",
            "materialization_claim_expires_at",
            "materialization_started_at",
            "materialization_committed_at",
            "materialization_error_code",
            "materialization_error_message",
            "canonical_trajectory_sha256",
            "canonical_atif_sha256",
            "source_cleanup_state",
            "source_retain_until",
            "source_cleanup_attempts",
            "source_cleanup_claim_id",
            "source_cleanup_claim_expires_at",
            "source_cleanup_error_message",
        } <= columns
        indexes = {item["name"] for item in inspect(engine).get_indexes("execution_leases")}
        assert "execution_leases_materialization_queue_idx" in indexes
        assert "execution_leases_source_cleanup_queue_idx" in indexes
        with engine.connect() as connection:
            history_function = connection.scalar(
                text("SELECT pg_get_functiondef('append_execution_lease_history()'::regprocedure)")
            )
            mutation_function = connection.scalar(
                text(
                    "SELECT pg_get_functiondef('validate_execution_lease_mutation()'::regprocedure)"
                )
            )
        assert history_function is not None
        assert "'materialization_state', new.materialization_state" in history_function.lower()
        assert mutation_function is not None
        assert "deleted execution lease is immutable outside materialization" in mutation_function
        assert "source cleanup attempts cannot decrease" in mutation_function
        assert "complete source cleanup state is immutable" in mutation_function

        command.downgrade(config, "0128")
        after = {item["name"] for item in inspect(engine).get_columns("execution_leases")}
        assert "materialization_state" not in after
        command.upgrade(config, "head")
    finally:
        engine.dispose()


def test_service_execution_materialization_backfills_deleted_committed_lease(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0128")
    engine = create_engine(isolated_migration_postgres_url)
    team_id = uuid4()
    trial_id = uuid4()
    lease_id = uuid4()
    committed_at = None
    try:
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO teams (id, name) VALUES (:id, 'materialization-migration-team')"),
                {"id": team_id},
            )
            connection.execute(
                text(
                    "INSERT INTO tasks (id, checksum, config) "
                    "VALUES ('materialization-migration-task', :checksum, '{}'::jsonb)"
                ),
                {"checksum": "0" * 64},
            )
            connection.execute(
                text(
                    "INSERT INTO trials "
                    "(id, team_id, task_id, config, requires_caps, state, idempotency_key) "
                    "VALUES (:id, :team_id, 'materialization-migration-task', '{}'::jsonb, "
                    ' \'{"backend":"nebius","cpu_arch":"any"}\'::jsonb, '
                    " 'claimed', 'materialization-migration-trial')"
                ),
                {"id": trial_id, "team_id": team_id},
            )
            connection.execute(
                text(
                    "INSERT INTO execution_classes "
                    "(id, schema_version, spec_json, spec_sha256) VALUES "
                    "('materialization-migration-class', 'loom.execution-class.v1', "
                    " '{}'::jsonb, :digest)"
                ),
                {"digest": "sha256:" + "1" * 64},
            )
            connection.execute(
                text(
                    "INSERT INTO execution_targets "
                    "(id, logical_pool_id, execution_class_id, schema_version, spec_json, "
                    " spec_sha256, environment, provider, region, failure_domain, "
                    " data_residency) VALUES "
                    "('materialization-migration-target', 'nebius-cpu', "
                    " 'materialization-migration-class', 'loom.execution-target.v1', "
                    " '{}'::jsonb, :digest, 'development', 'nebius', 'eu-north1', "
                    " 'eu-north1-a', 'eu')"
                ),
                {"digest": "sha256:" + "2" * 64},
            )
            committed_at = connection.scalar(text("SELECT now() - interval '1 hour'"))
            connection.execute(
                text(
                    "INSERT INTO execution_leases "
                    "(id, request_id, trial_id, team_id, attempt, execution_role, generation, "
                    " resource_generation, execution_class_id, target_id, "
                    " workload_requirements_json, workload_requirements_sha256, "
                    " routing_generation, selected_pool_id, routing_reason, "
                    " routing_decision_sha256, desired_state, observed_state, cleanup_state, "
                    " cleanup_requested_at, cleanup_deadline_at, provider_scope_key, "
                    " namespace_name, job_name, execution_unit_key, deadline_at, deleted_at, "
                    " output_commit_state, output_upload_session_id, output_generation, "
                    " output_manifest_sha256, output_marker_sha256, output_committed_at) "
                    "VALUES "
                    "(:id, :request_id, :trial_id, :team_id, 1, 'attempt', 1, 1, "
                    " 'materialization-migration-class', 'materialization-migration-target', "
                    " '{}'::jsonb, :requirements_digest, 1, 'nebius-cpu', 'operator_pin', "
                    " :routing_digest, 'deleted', 'deleted', 'complete', "
                    " now() - interval '2 hours', now() - interval '90 minutes', "
                    " :provider_scope, 'materialization-migration', "
                    " 'materialization-migration-job', :execution_unit_key, "
                    " now() + interval '1 hour', now() - interval '30 minutes', "
                    " 'committed', :upload_session_id, 1, :manifest_digest, :marker_digest, "
                    " :committed_at)"
                ),
                {
                    "id": lease_id,
                    "request_id": uuid4(),
                    "trial_id": trial_id,
                    "team_id": team_id,
                    "requirements_digest": "sha256:" + "3" * 64,
                    "routing_digest": "sha256:" + "4" * 64,
                    "provider_scope": "sha256:" + "5" * 64,
                    "execution_unit_key": uuid4(),
                    "upload_session_id": uuid4(),
                    "manifest_digest": "sha256:" + "6" * 64,
                    "marker_digest": "sha256:" + "7" * 64,
                    "committed_at": committed_at,
                },
            )

        command.upgrade(config, "0129")

        with engine.connect() as connection:
            lease = connection.execute(
                text(
                    "SELECT materialization_state, materialization_next_attempt_at "
                    "FROM execution_leases WHERE id=:id"
                ),
                {"id": lease_id},
            ).one()

        assert lease.materialization_state == "pending"
        assert lease.materialization_next_attempt_at == committed_at
    finally:
        engine.dispose()
