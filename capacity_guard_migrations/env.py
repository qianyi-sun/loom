"""Alembic environment for the protected per-environment capacity schema."""

from __future__ import annotations

import os
import re
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool, text

PROTECTED_SCHEMA = "loom_capacity_guard"
VERSION_TABLE = "capacity_guard_alembic_version"
_ROLE_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,62}$")

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name, disable_existing_loggers=False)

db_url = os.environ.get("LOOM_CAPACITY_GUARD_DB_URL", "").strip()
owner_role = os.environ.get("LOOM_CAPACITY_GUARD_OWNER_ROLE", "").strip()
agent_role = os.environ.get("LOOM_CAPACITY_GUARD_AGENT_ROLE", "").strip()
executor_role = os.environ.get("LOOM_CAPACITY_GUARD_EXECUTOR_ROLE", "").strip()
observer_role = os.environ.get("LOOM_CAPACITY_GUARD_OBSERVER_ROLE", "").strip()
runtime_role = os.environ.get("LOOM_CAPACITY_GUARD_RUNTIME_ROLE", "").strip()
if not db_url:
    raise RuntimeError(
        "LOOM_CAPACITY_GUARD_DB_URL must be set to run protected capacity migrations"
    )
if _ROLE_PATTERN.fullmatch(owner_role) is None:
    raise RuntimeError("LOOM_CAPACITY_GUARD_OWNER_ROLE must name an explicit canonical SQL role")
if _ROLE_PATTERN.fullmatch(agent_role) is None or agent_role == owner_role:
    raise RuntimeError("LOOM_CAPACITY_GUARD_AGENT_ROLE must name a distinct canonical SQL role")
if _ROLE_PATTERN.fullmatch(executor_role) is None or executor_role in {owner_role, agent_role}:
    raise RuntimeError("LOOM_CAPACITY_GUARD_EXECUTOR_ROLE must name a distinct canonical SQL role")
if _ROLE_PATTERN.fullmatch(observer_role) is None or observer_role in {
    owner_role,
    agent_role,
    executor_role,
}:
    raise RuntimeError("LOOM_CAPACITY_GUARD_OBSERVER_ROLE must name a distinct canonical SQL role")
if _ROLE_PATTERN.fullmatch(runtime_role) is None or runtime_role in {
    owner_role,
    agent_role,
    executor_role,
    observer_role,
}:
    raise RuntimeError("LOOM_CAPACITY_GUARD_RUNTIME_ROLE must name a distinct canonical SQL role")
# Alembic stores options in ConfigParser, where URL-encoded percent signs in
# credentials are interpolation markers unless doubled at this one boundary.
config.set_main_option("sqlalchemy.url", db_url.replace("%", "%%"))
target_metadata = None


def run_migrations_offline() -> None:
    raise RuntimeError("protected capacity migrations require an online owner-role verification")


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    try:
        with connectable.connect() as connection:
            preparer = connection.dialect.identifier_preparer
            quoted_owner = preparer.quote(owner_role)
            quoted_schema = preparer.quote(PROTECTED_SCHEMA)
            login = (
                connection.execute(
                    text(
                        "SELECT rolname, rolcanlogin, rolsuper, rolcreatedb, "
                        "rolcreaterole, rolreplication, rolbypassrls "
                        "FROM pg_roles WHERE rolname = session_user"
                    )
                )
                .mappings()
                .one_or_none()
            )
            if (
                login is None
                or login["rolcanlogin"] is not True
                or any(
                    login[field] is True
                    for field in (
                        "rolsuper",
                        "rolcreatedb",
                        "rolcreaterole",
                        "rolreplication",
                        "rolbypassrls",
                    )
                )
            ):
                raise RuntimeError(
                    "protected capacity migration login must be a least-privileged role"
                )
            agent = (
                connection.execute(
                    text(
                        "SELECT rolname, rolcanlogin, rolinherit, rolsuper, rolcreatedb, "
                        "rolcreaterole, rolreplication, rolbypassrls, "
                        "pg_has_role(rolname, :owner_role, 'MEMBER') AS owner_member, "
                        "(SELECT count(*) FROM pg_auth_members AS m "
                        "WHERE m.member = pg_roles.oid) AS role_memberships "
                        "FROM pg_roles WHERE rolname = :agent_role"
                    ),
                    {"agent_role": agent_role, "owner_role": owner_role},
                )
                .mappings()
                .one_or_none()
            )
            if (
                agent is None
                or agent["rolcanlogin"] is not True
                or agent["rolinherit"] is not False
                or agent["owner_member"] is not False
                or agent["role_memberships"] != 0
                or agent["rolname"] == login["rolname"]
                or any(
                    agent[field] is True
                    for field in (
                        "rolsuper",
                        "rolcreatedb",
                        "rolcreaterole",
                        "rolreplication",
                        "rolbypassrls",
                    )
                )
            ):
                raise RuntimeError(
                    "protected capacity agent must be a distinct least-privileged "
                    "NOINHERIT login with no owner membership"
                )
            executor = (
                connection.execute(
                    text(
                        "SELECT rolname, rolcanlogin, rolinherit, rolsuper, rolcreatedb, "
                        "rolcreaterole, rolreplication, rolbypassrls, "
                        "pg_has_role(rolname, :owner_role, 'MEMBER') AS owner_member, "
                        "pg_has_role(rolname, :agent_role, 'MEMBER') AS agent_member, "
                        "(SELECT count(*) FROM pg_auth_members AS m "
                        "WHERE m.member = pg_roles.oid) AS role_memberships "
                        "FROM pg_roles WHERE rolname = :executor_role"
                    ),
                    {
                        "executor_role": executor_role,
                        "owner_role": owner_role,
                        "agent_role": agent_role,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if (
                executor is None
                or executor["rolinherit"] is not False
                or executor["owner_member"] is not False
                or executor["agent_member"] is not False
                or executor["role_memberships"] != 0
                or executor["rolname"] in {login["rolname"], agent_role, owner_role}
                or any(
                    executor[field] is True
                    for field in (
                        "rolsuper",
                        "rolcreatedb",
                        "rolcreaterole",
                        "rolreplication",
                        "rolbypassrls",
                    )
                )
            ):
                raise RuntimeError(
                    "protected capacity executor must be a distinct least-privileged "
                    "NOINHERIT role with no memberships"
                )
            observer = (
                connection.execute(
                    text(
                        "SELECT rolname, rolcanlogin, rolinherit, rolsuper, rolcreatedb, "
                        "rolcreaterole, rolreplication, rolbypassrls, "
                        "pg_has_role(rolname, :owner_role, 'MEMBER') AS owner_member, "
                        "pg_has_role(rolname, :agent_role, 'MEMBER') AS agent_member, "
                        "pg_has_role(rolname, :executor_role, 'MEMBER') AS executor_member, "
                        "(SELECT count(*) FROM pg_auth_members AS m "
                        "WHERE m.member = pg_roles.oid) AS role_memberships "
                        "FROM pg_roles WHERE rolname = :observer_role"
                    ),
                    {
                        "observer_role": observer_role,
                        "owner_role": owner_role,
                        "agent_role": agent_role,
                        "executor_role": executor_role,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if (
                observer is None
                or observer["rolcanlogin"] is not True
                or observer["rolinherit"] is not False
                or observer["owner_member"] is not False
                or observer["agent_member"] is not False
                or observer["executor_member"] is not False
                or observer["role_memberships"] != 0
                or observer["rolname"] in {login["rolname"], owner_role, agent_role, executor_role}
                or any(
                    observer[field] is True
                    for field in (
                        "rolsuper",
                        "rolcreatedb",
                        "rolcreaterole",
                        "rolreplication",
                        "rolbypassrls",
                    )
                )
            ):
                raise RuntimeError(
                    "protected capacity observer must be a distinct least-privileged "
                    "NOINHERIT login with no memberships"
                )
            runtime = (
                connection.execute(
                    text(
                        "SELECT rolname, rolcanlogin, rolinherit, rolsuper, rolcreatedb, "
                        "rolcreaterole, rolreplication, rolbypassrls, "
                        "pg_has_role(rolname, :owner_role, 'MEMBER') AS owner_member, "
                        "pg_has_role(rolname, :agent_role, 'MEMBER') AS agent_member, "
                        "pg_has_role(rolname, :executor_role, 'MEMBER') AS executor_member, "
                        "pg_has_role(rolname, :observer_role, 'MEMBER') AS observer_member, "
                        "(SELECT count(*) FROM pg_auth_members AS m "
                        "WHERE m.member = pg_roles.oid) AS role_memberships "
                        "FROM pg_roles WHERE rolname = :runtime_role"
                    ),
                    {
                        "runtime_role": runtime_role,
                        "owner_role": owner_role,
                        "agent_role": agent_role,
                        "executor_role": executor_role,
                        "observer_role": observer_role,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if (
                runtime is None
                or runtime["rolcanlogin"] is not True
                or runtime["rolinherit"] is not False
                or runtime["owner_member"] is not False
                or runtime["agent_member"] is not False
                or runtime["executor_member"] is not False
                or runtime["observer_member"] is not False
                or runtime["role_memberships"] != 0
                or runtime["rolname"]
                in {login["rolname"], owner_role, agent_role, executor_role, observer_role}
                or any(
                    runtime[field] is True
                    for field in (
                        "rolsuper",
                        "rolcreatedb",
                        "rolcreaterole",
                        "rolreplication",
                        "rolbypassrls",
                    )
                )
            ):
                raise RuntimeError(
                    "protected capacity runtime must be a distinct least-privileged "
                    "NOINHERIT login with no memberships"
                )
            config.attributes["capacity_guard_agent_role"] = agent_role
            config.attributes["capacity_guard_executor_role"] = executor_role
            config.attributes["capacity_guard_observer_role"] = observer_role
            config.attributes["capacity_guard_runtime_role"] = runtime_role
            connection.exec_driver_sql(f"SET ROLE {quoted_owner}")
            role = (
                connection.execute(
                    text(
                        "SELECT rolname, rolcanlogin, rolinherit, rolsuper, rolcreatedb, "
                        "rolcreaterole, rolreplication, rolbypassrls "
                        "FROM pg_roles WHERE rolname = current_role"
                    )
                )
                .mappings()
                .one_or_none()
            )
            if (
                role is None
                or role["rolname"] != owner_role
                or role["rolcanlogin"] is True
                or role["rolinherit"] is True
                or any(
                    role[field] is True
                    for field in (
                        "rolsuper",
                        "rolcreatedb",
                        "rolcreaterole",
                        "rolreplication",
                        "rolbypassrls",
                    )
                )
            ):
                raise RuntimeError(
                    "protected capacity migrations require the exact least-privileged "
                    "NOLOGIN NOINHERIT owner role"
                )
            required_trial_columns = (
                "id",
                "team_id",
                "task_id",
                "config",
                "state",
                "requires_caps",
                "submit_priority",
                "batch_id",
                "idempotency_key",
                "sample_idx",
                "combination_idx",
                "provider_connection_id",
                "provider_model_id",
                "submitted_by_user_id",
                "usage_attributed_user_id",
                "usage_attributed_actor",
                "family_key",
                "lifecycle_authority_id",
                "submitted_at",
                "cancellation_requested_at",
                "next_attempt_at",
                "autoscaler_pool_name",
                "worker_id",
                "attempt_count",
                "execution_route_json",
            )
            missing_trial_columns = tuple(
                column
                for column in required_trial_columns
                if not connection.execute(
                    text(
                        "SELECT has_column_privilege(current_user, "
                        "'public.trials', :column, 'SELECT')"
                    ),
                    {"column": column},
                ).scalar_one()
            )
            if missing_trial_columns:
                raise RuntimeError(
                    "protected capacity owner requires direct SELECT privileges on "
                    "the demand-source columns of public.trials: "
                    + ", ".join(missing_trial_columns)
                )
            required_trial_insert_columns = (
                "id",
                "team_id",
                "task_id",
                "config",
                "requires_caps",
                "state",
                "submit_priority",
                "batch_id",
                "idempotency_key",
                "sample_idx",
                "combination_idx",
                "provider_connection_id",
                "provider_model_id",
                "submitted_by_user_id",
                "usage_attributed_user_id",
                "usage_attributed_actor",
                "family_key",
            )
            missing_trial_insert_columns = tuple(
                column
                for column in required_trial_insert_columns
                if not connection.execute(
                    text(
                        "SELECT has_column_privilege(current_user, "
                        "'public.trials', :column, 'INSERT')"
                    ),
                    {"column": column},
                ).scalar_one()
            )
            if missing_trial_insert_columns:
                raise RuntimeError(
                    "protected capacity owner requires direct INSERT privileges on "
                    "the bounded projection columns of public.trials: "
                    + ", ".join(missing_trial_insert_columns)
                )
            required_trial_update_columns = (
                "lifecycle_authority_id",
                "state",
                "requires_caps",
                "worker_id",
                "claimed_at",
                "pre_start_heartbeat_at",
                "failure_reason",
                "failure_message",
                "attempt_count",
            )
            missing_trial_update_columns = tuple(
                column
                for column in required_trial_update_columns
                if not connection.execute(
                    text(
                        "SELECT has_column_privilege(current_user, "
                        "'public.trials', :column, 'UPDATE')"
                    ),
                    {"column": column},
                ).scalar_one()
            )
            if missing_trial_update_columns:
                raise RuntimeError(
                    "protected capacity owner requires direct UPDATE privileges on "
                    "public.trials: " + ", ".join(missing_trial_update_columns)
                )
            if not connection.execute(
                text(
                    "SELECT has_column_privilege(current_user, 'public.trials', 'id', 'REFERENCES')"
                )
            ).scalar_one():
                raise RuntimeError(
                    "protected capacity owner requires direct REFERENCES privilege on "
                    "public.trials.id"
                )
            if not connection.execute(
                text(
                    "SELECT has_column_privilege(current_user, "
                    "'public.data_lifecycle_authorities', 'id', 'SELECT')"
                )
            ).scalar_one():
                raise RuntimeError(
                    "protected capacity owner requires direct SELECT privilege on "
                    "public.data_lifecycle_authorities.id"
                )
            required_lifecycle_insert_columns = (
                "environment",
                "namespace",
                "team_id",
                "data_class",
                "owner_kind",
                "owner_id",
                "created_at",
                "expires_at",
                "pinned",
                "state",
            )
            missing_lifecycle_insert_columns = tuple(
                column
                for column in required_lifecycle_insert_columns
                if not connection.execute(
                    text(
                        "SELECT has_column_privilege(current_user, "
                        "'public.data_lifecycle_authorities', :column, 'INSERT')"
                    ),
                    {"column": column},
                ).scalar_one()
            )
            if missing_lifecycle_insert_columns:
                raise RuntimeError(
                    "protected capacity owner requires direct INSERT privileges on "
                    "public.data_lifecycle_authorities: "
                    + ", ".join(missing_lifecycle_insert_columns)
                )
            if not connection.execute(
                text(
                    "SELECT has_column_privilege(current_user, "
                    "'public.data_lifecycle_authorities', 'id', 'REFERENCES')"
                )
            ).scalar_one():
                raise RuntimeError(
                    "protected capacity owner requires direct REFERENCES privilege on "
                    "public.data_lifecycle_authorities.id"
                )

            prerequisite_select_columns = {
                "tasks": (
                    "id",
                    "checksum",
                    "config",
                    "source",
                    "source_provenance",
                ),
                "task_image_materializations": (
                    "id",
                    "materialization_key",
                    "task_id",
                    "task_checksum",
                    "cpu_arch",
                    "task_config",
                    "task_source",
                    "task_source_provenance",
                    "state",
                    "registry_images",
                ),
                "trial_task_image_materializations": (
                    "trial_id",
                    "materialization_id",
                ),
                "model_switch_plans": (
                    "id",
                    "trial_id",
                    "combination_idx",
                    "mix_mode",
                    "k1",
                    "k2",
                    "teacher_episodes",
                    "beta",
                    "seed",
                    "prng_version",
                    "student_model_snapshot",
                    "teacher_model_snapshot",
                    "provider_connection_id",
                    "pricing_snapshot",
                    "capability_snapshot",
                    "inherited_from_plan_id",
                    "created_at",
                ),
            }
            missing_prerequisite_columns = {
                table: missing
                for table, columns in prerequisite_select_columns.items()
                if (
                    missing := tuple(
                        column
                        for column in columns
                        if not connection.execute(
                            text(
                                "SELECT has_column_privilege(current_user, "
                                ":table, :column, 'SELECT')"
                            ),
                            {"table": f"public.{table}", "column": column},
                        ).scalar_one()
                    )
                )
            }
            if missing_prerequisite_columns:
                details = "; ".join(
                    f"{table}: {', '.join(columns)}"
                    for table, columns in missing_prerequisite_columns.items()
                )
                raise RuntimeError(
                    "protected capacity owner requires direct SELECT privileges on "
                    "the runtime submission prerequisite columns: " + details
                )
            if not connection.execute(
                text(
                    "SELECT has_column_privilege(current_user, "
                    "'public.task_image_materializations', 'state', 'UPDATE')"
                )
            ).scalar_one():
                raise RuntimeError(
                    "protected capacity owner requires direct UPDATE privilege on "
                    "public.task_image_materializations.state for protected claim row locking"
                )

            required_worker_select_columns = (
                "id",
                "hostname",
                "version",
                "capabilities",
                "supported_work_kinds",
                "capability_snapshot_digest",
                "capability_snapshot_json",
                "slurm_gpu_allocation_evidence_json",
                "slurm_gpu_allocation_evidence_digest",
                "max_concurrent",
                "pool_name",
                "auth_token_hash",
                "input_cache_capacity_bytes",
                "input_cache_reserved_bytes",
                "input_cache_ready_bytes",
                "status",
                "drain_state",
            )
            missing_worker_select_columns = tuple(
                column
                for column in required_worker_select_columns
                if not connection.execute(
                    text(
                        "SELECT has_column_privilege(current_user, "
                        "'public.workers', :column, 'SELECT')"
                    ),
                    {"column": column},
                ).scalar_one()
            )
            if missing_worker_select_columns:
                raise RuntimeError(
                    "protected capacity owner requires direct SELECT privileges on "
                    "the bounded projection columns of public.workers: "
                    + ", ".join(missing_worker_select_columns)
                )
            required_worker_insert_columns = (
                "id",
                "hostname",
                "version",
                "capabilities",
                "supported_work_kinds",
                "capability_snapshot_digest",
                "capability_snapshot_json",
                "slurm_gpu_allocation_evidence_json",
                "slurm_gpu_allocation_evidence_digest",
                "auth_token_hash",
                "max_concurrent",
                "pool_name",
                "input_cache_capacity_bytes",
                "input_cache_reserved_bytes",
                "input_cache_ready_bytes",
                "registered_at",
                "last_seen_at",
                "status",
            )
            missing_worker_insert_columns = tuple(
                column
                for column in required_worker_insert_columns
                if not connection.execute(
                    text(
                        "SELECT has_column_privilege(current_user, "
                        "'public.workers', :column, 'INSERT')"
                    ),
                    {"column": column},
                ).scalar_one()
            )
            if missing_worker_insert_columns:
                raise RuntimeError(
                    "protected capacity owner requires direct INSERT privileges on "
                    "the bounded projection columns of public.workers: "
                    + ", ".join(missing_worker_insert_columns)
                )
            if not connection.execute(
                text(
                    "SELECT has_column_privilege(current_user, "
                    "'public.workers', 'status', 'UPDATE')"
                )
            ).scalar_one():
                raise RuntimeError(
                    "protected capacity owner requires direct UPDATE privilege on "
                    "public.workers.status for protected claim row locking"
                )
            required_slurm_job_select_columns = (
                "id",
                "slurm_cluster_id",
                "environment",
                "pool_name",
                "nodelist",
                "requested_cpus",
                "requested_memory_mib",
                "requested_pids",
                "requested_gpu_tres",
                "requested_gpus",
                "requested_concurrency",
                "sandbox_identity",
                "candidate_sha",
                "compose_project",
                "job_id",
                "slurm_state",
                "state",
                "worker_id",
            )
            missing_slurm_job_select_columns = tuple(
                column
                for column in required_slurm_job_select_columns
                if not connection.execute(
                    text(
                        "SELECT has_column_privilege(current_user, "
                        "'public.slurm_worker_jobs', :column, 'SELECT')"
                    ),
                    {"column": column},
                ).scalar_one()
            )
            if missing_slurm_job_select_columns:
                raise RuntimeError(
                    "protected capacity owner requires direct SELECT privileges on "
                    "the protected linkage columns of public.slurm_worker_jobs: "
                    + ", ".join(missing_slurm_job_select_columns)
                )
            required_slurm_job_insert_columns = (
                "id",
                "slurm_cluster_id",
                "environment",
                "pool_name",
                "nodelist",
                "requested_cpus",
                "requested_memory_mib",
                "requested_gpu_tres",
                "requested_gpus",
                "requested_concurrency",
                "sandbox_identity",
                "candidate_sha",
                "compose_project",
                "job_id",
                "slurm_state",
                "state",
                "submitted_at",
                "started_at",
                "last_reconciled_at",
            )
            missing_slurm_job_insert_columns = tuple(
                column
                for column in required_slurm_job_insert_columns
                if not connection.execute(
                    text(
                        "SELECT has_column_privilege(current_user, "
                        "'public.slurm_worker_jobs', :column, 'INSERT')"
                    ),
                    {"column": column},
                ).scalar_one()
            )
            if missing_slurm_job_insert_columns:
                raise RuntimeError(
                    "protected capacity owner requires direct INSERT privileges on "
                    "the protected projection columns of public.slurm_worker_jobs: "
                    + ", ".join(missing_slurm_job_insert_columns)
                )
            if not connection.execute(
                text(
                    "SELECT has_column_privilege(current_user, "
                    "'public.slurm_worker_jobs', 'worker_id', 'UPDATE')"
                )
            ).scalar_one():
                raise RuntimeError(
                    "protected capacity owner requires direct UPDATE privilege on "
                    "public.slurm_worker_jobs.worker_id"
                )

            claim_select_columns = {
                "execution_attempts": ("worker_id", "state"),
                "worker_pool_autoscaler_policies": (
                    "id",
                    "pool_name",
                    "actuator",
                    "actuator_config",
                    "enabled",
                    "prod_pressure_state",
                    "updated_at",
                ),
                "pipeline_acceptance_preflight_prerequisites": (
                    "worker_id",
                    "fence_state",
                ),
                "team_quotas": (
                    "team_id",
                    "in_flight_count",
                    "fair_share_weight",
                    "max_attempts_ceiling",
                ),
                "batch_family_state": (
                    "batch_id",
                    "family_key",
                    "state",
                    "task_sequence",
                    "current_index",
                    "state_uri",
                ),
                "batches": ("id", "family_run_spec"),
                "execution_admission_policies": (
                    "scope_kind",
                    "scope_key",
                    "max_concurrent",
                    "active_count",
                    "enabled",
                ),
                "execution_admission_reservations": (
                    "id",
                    "trial_id",
                    "attempt",
                    "execution_role",
                ),
            }
            missing_claim_select_columns = {
                table: missing
                for table, columns in claim_select_columns.items()
                if (
                    missing := tuple(
                        column
                        for column in columns
                        if not connection.execute(
                            text(
                                "SELECT has_column_privilege(current_user, "
                                ":table, :column, 'SELECT')"
                            ),
                            {"table": f"public.{table}", "column": column},
                        ).scalar_one()
                    )
                )
            }
            if missing_claim_select_columns:
                details = "; ".join(
                    f"{table}: {', '.join(columns)}"
                    for table, columns in missing_claim_select_columns.items()
                )
                raise RuntimeError(
                    "protected capacity owner requires direct SELECT privileges on "
                    "the protected claim columns: " + details
                )
            claim_update_columns = {
                "batch_family_state": ("state", "updated_at"),
                "batches": ("id",),
                "execution_admission_policies": ("active_count", "counter_updated_at"),
                "model_switch_plans": ("id",),
                "team_quotas": ("in_flight_count",),
            }
            missing_claim_update_columns = {
                table: missing
                for table, columns in claim_update_columns.items()
                if (
                    missing := tuple(
                        column
                        for column in columns
                        if not connection.execute(
                            text(
                                "SELECT has_column_privilege(current_user, "
                                ":table, :column, 'UPDATE')"
                            ),
                            {"table": f"public.{table}", "column": column},
                        ).scalar_one()
                    )
                )
            }
            if missing_claim_update_columns:
                details = "; ".join(
                    f"{table}: {', '.join(columns)}"
                    for table, columns in missing_claim_update_columns.items()
                )
                raise RuntimeError(
                    "protected capacity owner requires direct UPDATE privileges on "
                    "the protected claim columns: " + details
                )
            required_reservation_insert_columns = (
                "trial_id",
                "attempt",
                "execution_role",
                "team_id",
                "batch_id",
                "environment",
                "region",
                "execution_class_id",
                "pool_id",
                "owner_kind",
                "owner_id",
                "acquired_at",
            )
            missing_reservation_insert_columns = tuple(
                column
                for column in required_reservation_insert_columns
                if not connection.execute(
                    text(
                        "SELECT has_column_privilege(current_user, "
                        "'public.execution_admission_reservations', :column, 'INSERT')"
                    ),
                    {"column": column},
                ).scalar_one()
            )
            if missing_reservation_insert_columns:
                raise RuntimeError(
                    "protected capacity owner requires direct INSERT privileges on "
                    "public.execution_admission_reservations: "
                    + ", ".join(missing_reservation_insert_columns)
                )
            connection.exec_driver_sql(
                f"CREATE SCHEMA IF NOT EXISTS {quoted_schema} AUTHORIZATION {quoted_owner}"
            )
            actual_owner = connection.execute(
                text("SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = :schema"),
                {"schema": PROTECTED_SCHEMA},
            ).scalar_one()
            if actual_owner != owner_role:
                raise RuntimeError("protected capacity schema has the wrong owner")
            connection.commit()

            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                include_schemas=True,
                version_table=VERSION_TABLE,
                version_table_schema=PROTECTED_SCHEMA,
            )
            with context.begin_transaction():
                context.run_migrations()
    finally:
        connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
