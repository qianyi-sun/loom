from __future__ import annotations

from uuid import uuid4

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

from loom.execution_contract import ExecutionRoutingDecisionV1
from loom.pipeline.keys import canonical_digest


def _config(url: str) -> Config:
    config = Config("migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    return config


def test_execution_routing_decision_migration_backfills_and_round_trips(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0115")
    engine = create_engine(isolated_migration_postgres_url)
    team_id = uuid4()
    trial_id = uuid4()
    lease_id = uuid4()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO teams (id, name) VALUES (:id, 'route-migration-team')"),
                {"id": team_id},
            )
            connection.execute(
                text(
                    "INSERT INTO tasks (id, checksum, config) "
                    "VALUES ('route-migration-task', :checksum, '{}'::jsonb)"
                ),
                {"checksum": "0" * 64},
            )
            connection.execute(
                text(
                    "INSERT INTO trials "
                    "(id, team_id, task_id, config, requires_caps, state, idempotency_key, "
                    " autoscaler_pool_name, autoscaler_pool_assigned_at) "
                    "VALUES (:id, :team_id, 'route-migration-task', '{}'::jsonb, "
                    ' \'{"backend":"docker","cpu_arch":"any"}\'::jsonb, '
                    " 'claimed', 'route-migration-trial', 'oldlab', now())"
                ),
                {"id": trial_id, "team_id": team_id},
            )
            connection.execute(
                text(
                    "INSERT INTO execution_classes "
                    "(id, schema_version, spec_json, spec_sha256) VALUES "
                    "('route-migration-class', 'loom.execution-class.v1', '{}'::jsonb, :digest)"
                ),
                {"digest": "sha256:" + "1" * 64},
            )
            connection.execute(
                text(
                    "INSERT INTO execution_targets "
                    "(id, logical_pool_id, execution_class_id, schema_version, spec_json, "
                    " spec_sha256, environment, provider, region, failure_domain, "
                    " data_residency) VALUES "
                    "('route-migration-target', 'oldlab', 'route-migration-class', "
                    " 'loom.execution-target.v1', '{}'::jsonb, :digest, 'staging', "
                    " 'nebius', 'eu-test1', 'test-failure-domain', 'eu')"
                ),
                {"digest": "sha256:" + "2" * 64},
            )
            connection.execute(
                text(
                    "INSERT INTO execution_leases "
                    "(id, request_id, trial_id, team_id, attempt, execution_role, generation, "
                    " resource_generation, execution_class_id, target_id, "
                    " workload_requirements_json, workload_requirements_sha256, "
                    " desired_state, observed_state, cleanup_state, provider_scope_key, "
                    " namespace_name, job_name, execution_unit_key, deadline_at, "
                    " output_commit_state) VALUES "
                    "(:id, :request_id, :trial_id, :team_id, 1, 'attempt', 1, 1, "
                    " 'route-migration-class', 'route-migration-target', '{}'::jsonb, "
                    " :requirements_digest, 'create', 'reserved', 'not_requested', "
                    " :provider_scope, 'route-migration', 'route-migration-job', "
                    " :execution_unit_key, now() + interval '1 hour', 'not_started')"
                ),
                {
                    "id": lease_id,
                    "request_id": uuid4(),
                    "trial_id": trial_id,
                    "team_id": team_id,
                    "requirements_digest": "sha256:" + "3" * 64,
                    "provider_scope": "sha256:" + "4" * 64,
                    "execution_unit_key": uuid4(),
                },
            )
            connection.execute(
                text(
                    "INSERT INTO execution_commands "
                    "(id, lease_id, generation, sequence, command_type, idempotency_key, "
                    " payload_json, payload_sha256) VALUES "
                    "(:id, :lease_id, 1, 1, 'create', :idempotency_key, '{}'::jsonb, :digest)"
                ),
                {
                    "id": uuid4(),
                    "lease_id": lease_id,
                    "idempotency_key": "sha256:" + "5" * 64,
                    "digest": "sha256:" + "6" * 64,
                },
            )

        command.upgrade(config, "0116")
        columns = {item["name"] for item in inspect(engine).get_columns("trials")}
        assert {
            "execution_route_generation",
            "execution_route_pool_name",
            "execution_route_json",
            "execution_route_sha256",
        } <= columns
        lease_columns = {item["name"] for item in inspect(engine).get_columns("execution_leases")}
        assert {
            "routing_generation",
            "selected_pool_id",
            "routing_reason",
            "routing_decision_sha256",
        } <= lease_columns
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT execution_route_generation, execution_route_pool_name, "
                    "execution_route_json, execution_route_sha256 "
                    "FROM trials WHERE id=:id"
                ),
                {"id": trial_id},
            ).one()
            history_function = connection.scalar(
                text("SELECT pg_get_functiondef('append_execution_lease_history()'::regprocedure)")
            )
            mutation_function = connection.scalar(
                text(
                    "SELECT pg_get_functiondef('validate_execution_lease_mutation()'::regprocedure)"
                )
            )
            lease_row = connection.execute(
                text(
                    "SELECT routing_generation, selected_pool_id, routing_reason, "
                    "routing_decision_sha256 FROM execution_leases WHERE id=:id"
                ),
                {"id": lease_id},
            ).one()
            history_snapshot = connection.scalar(
                text(
                    "SELECT snapshot_json FROM execution_lease_history "
                    "WHERE lease_id=:lease_id ORDER BY transition_ordinal LIMIT 1"
                ),
                {"lease_id": lease_id},
            )
        assert row.execution_route_generation == 1
        assert row.execution_route_pool_name == "oldlab"
        assert row.execution_route_sha256.startswith("sha256:")
        decision = ExecutionRoutingDecisionV1.model_validate(row.execution_route_json)
        assert row.execution_route_sha256 == canonical_digest(decision.model_dump(mode="json"))
        assert decision.reason == "preexisting_assignment"
        assert decision.selected_pool_id == "oldlab"
        assert decision.selected_adapter_kind == "kubernetes_job"
        assert lease_row.routing_generation == 1
        assert lease_row.selected_pool_id == "oldlab"
        assert lease_row.routing_reason == "preexisting_assignment"
        assert lease_row.routing_decision_sha256 == row.execution_route_sha256
        assert history_snapshot["selected_pool_id"] == "oldlab"
        assert "routing_decision_sha256" in history_function
        assert "routing_decision_sha256" in mutation_function

        command.downgrade(config, "0115")
        assert "execution_route_json" not in {
            item["name"] for item in inspect(engine).get_columns("trials")
        }
        assert "routing_generation" not in {
            item["name"] for item in inspect(engine).get_columns("execution_leases")
        }
        command.upgrade(config, "head")
    finally:
        engine.dispose()
