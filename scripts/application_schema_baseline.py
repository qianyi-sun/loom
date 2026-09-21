"""Disposable reference provisioning for 0134 / guard_0030 only.

The method below is preserved from personal_dev_capacity_runtime.py at protected
release 7f8687e4a4a38ccf54e8c6b74821021da60879b7. In particular it has no trial
writer retirement helper / TRIGGER grant and no ready-publication column grant.
Do not converge a historical reference using newer provisioning behavior.

This module is imported only by the isolated schema-reference builder. It is not
an installer, a downgrade path, or permission to provision a live database.
"""

from __future__ import annotations

import asyncio

import psycopg
from psycopg import sql
from sqlalchemy.engine import make_url

from loom.application_schema_provisioning import (
    CapacityDatabaseCredentials,
    PersonalDevCapacityInstallationError,
    ReferenceDatabase,
    ReferenceIdentity,
    _retarget_database_url,
    _role_names,
    fixture_database_url,
)


class BaselineReferenceDatabase(ReferenceDatabase):
    async def _converge_roles(
        self,
        identity: ReferenceIdentity,
        credentials: CapacityDatabaseCredentials,
    ) -> tuple[str, str, str, str, str, str, str, str]:
        owner, migrator, agent, executor, observer, runtime = _role_names(identity)
        migrator_url = _retarget_database_url(
            self._admin_url,
            database=identity.database,
            username=migrator,
            password=credentials.migrator_password,
        )
        agent_url = _retarget_database_url(
            self._admin_url,
            database=identity.database,
            username=agent,
            password=credentials.agent_password,
        )
        if self._transient_role_admin and make_url(self._admin_url).username != migrator:
            raise PersonalDevCapacityInstallationError(
                "protected capacity transient role authority is invalid"
            )
        try:
            if self._transient_role_admin:
                await self._verify_transient_role_envelope(
                    identity,
                    owner=owner,
                    migrator=migrator,
                    agent=agent,
                    executor=executor,
                    observer=observer,
                    runtime=runtime,
                )
            else:
                async with await psycopg.AsyncConnection.connect(
                    self._connect_url,
                    autocommit=True,
                ) as connection:
                    protected_roles = sql.SQL(", ").join(
                        sql.Identifier(role)
                        for role in (owner, migrator, agent, executor, observer, runtime)
                    )
                    for role in (owner, migrator, agent, executor, observer, runtime):
                        await connection.execute(
                            sql.SQL(
                                "DO $loom$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles "
                                "WHERE rolname = {}) THEN CREATE ROLE {}; END IF; END $loom$"
                            ).format(sql.Literal(role), sql.Identifier(role))
                        )
                        await connection.execute(
                            sql.SQL("ALTER ROLE {} RESET ALL").format(sql.Identifier(role))
                        )
                    restricted_nologin = (
                        "NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT "
                        "NOREPLICATION NOBYPASSRLS PASSWORD NULL"
                    )
                    restricted_login = (
                        "LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT "
                        "NOREPLICATION NOBYPASSRLS PASSWORD {}"
                    )
                    for role in (owner, executor):
                        await connection.execute(
                            sql.SQL("ALTER ROLE {} " + restricted_nologin).format(
                                sql.Identifier(role)
                            )
                        )
                    await connection.execute(
                        sql.SQL("ALTER ROLE {} " + restricted_login).format(
                            sql.Identifier(runtime),
                            sql.Literal(credentials.runtime_password),
                        )
                    )
                    credential_roles: tuple[tuple[str, str, str], ...] = (
                        (migrator, credentials.migrator_password, "INHERIT"),
                        (agent, credentials.agent_password, "NOINHERIT"),
                        (observer, credentials.observer_password, "NOINHERIT"),
                    )
                    for role, password, inherit in credential_roles:
                        credential_attributes = (
                            "LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                            f"{inherit} NOREPLICATION NOBYPASSRLS PASSWORD {{}}"
                        )
                        await connection.execute(
                            sql.SQL("ALTER ROLE {} " + credential_attributes).format(
                                sql.Identifier(role),
                                sql.Literal(password),
                            )
                        )
                    await connection.execute(
                        sql.SQL("GRANT {} TO {}").format(
                            sql.Identifier(owner),
                            sql.Identifier(migrator),
                        )
                    )
                    await connection.execute(
                        sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} FROM {}").format(
                            sql.Identifier(identity.database),
                            protected_roles,
                        )
                    )
                    await connection.execute(
                        sql.SQL("GRANT CONNECT ON DATABASE {} TO {}, {}, {}, {}").format(
                            sql.Identifier(identity.database),
                            sql.Identifier(migrator),
                            sql.Identifier(agent),
                            sql.Identifier(observer),
                            sql.Identifier(runtime),
                        )
                    )
                    await connection.execute(
                        sql.SQL("GRANT CREATE ON DATABASE {} TO {}").format(
                            sql.Identifier(identity.database),
                            sql.Identifier(owner),
                        )
                    )
                    memberships = await connection.execute(
                        "SELECT member.rolname AS member, granted.rolname AS granted "
                        "FROM pg_auth_members m JOIN pg_roles member ON member.oid = m.member "
                        "JOIN pg_roles granted ON granted.oid = m.roleid "
                        "WHERE member.rolname = ANY(%s) OR granted.rolname = ANY(%s) "
                        "ORDER BY member.rolname, granted.rolname",
                        (
                            [owner, migrator, agent, executor, observer, runtime],
                            [owner, migrator, agent, executor, observer, runtime],
                        ),
                    )
                    observed = {(row[0], row[1]) for row in await memberships.fetchall()}
                    expected_memberships = {(migrator, owner)}
                    if observed != expected_memberships:
                        for member, granted in sorted(observed - expected_memberships):
                            await connection.execute(
                                sql.SQL("REVOKE {} FROM {}").format(
                                    sql.Identifier(granted),
                                    sql.Identifier(member),
                                )
                            )
                        raise PersonalDevCapacityInstallationError(
                            "protected capacity roles have unexpected memberships"
                        )

            database_admin_url = fixture_database_url(self._admin_url, identity.database)
            async with await psycopg.AsyncConnection.connect(
                database_admin_url.replace("postgresql+psycopg://", "postgresql://", 1)
            ) as connection:
                async with connection.transaction():
                    if self._transient_role_admin:
                        await connection.execute(
                            sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(identity.db_role))
                        )
                    protected_roles = sql.SQL(", ").join(
                        sql.Identifier(role)
                        for role in (owner, migrator, agent, executor, observer, runtime)
                    )
                    for object_kind in (
                        "SCHEMA public",
                        "ALL TABLES IN SCHEMA public",
                        "ALL SEQUENCES IN SCHEMA public",
                        "ALL FUNCTIONS IN SCHEMA public",
                    ):
                        await connection.execute(
                            sql.SQL("REVOKE ALL PRIVILEGES ON {} FROM {}").format(
                                sql.SQL(object_kind),
                                protected_roles,
                            )
                        )
                    if self._transient_role_admin:
                        await connection.execute(
                            sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} FROM PUBLIC").format(
                                sql.Identifier(identity.database)
                            )
                        )
                        await connection.execute(
                            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}, {}, {}, {}").format(
                                sql.Identifier(identity.database),
                                sql.Identifier(migrator),
                                sql.Identifier(agent),
                                sql.Identifier(observer),
                                sql.Identifier(runtime),
                            )
                        )
                        await connection.execute(
                            sql.SQL("GRANT CREATE ON DATABASE {} TO {}").format(
                                sql.Identifier(identity.database),
                                sql.Identifier(owner),
                            )
                        )
                    application_role_result = await connection.execute(
                        "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %s)",
                        (identity.db_role,),
                    )
                    application_role_row = await application_role_result.fetchone()
                    if application_role_row is None:
                        raise PersonalDevCapacityInstallationError(
                            "application database role lookup failed"
                        )
                    application_role_exists = bool(application_role_row[0])
                    schemas_result = await connection.execute(
                        "SELECT namespace.nspname, EXISTS ("
                        "SELECT 1 FROM aclexplode(COALESCE("
                        "namespace.nspacl, acldefault('n', namespace.nspowner)"
                        ")) AS privilege WHERE privilege.grantee = 0 "
                        "AND privilege.privilege_type = 'USAGE'"
                        ") AS public_usage FROM pg_namespace AS namespace "
                        "WHERE nspname <> 'information_schema' "
                        "AND nspname NOT LIKE 'pg\\_%' ESCAPE '\\' "
                        "ORDER BY nspname"
                    )
                    for schema_name, public_usage in await schemas_result.fetchall():
                        if self._transient_role_admin and schema_name == "loom_capacity_guard":
                            continue
                        for object_kind in (
                            "SCHEMA {}",
                            "ALL TABLES IN SCHEMA {}",
                            "ALL SEQUENCES IN SCHEMA {}",
                            "ALL FUNCTIONS IN SCHEMA {}",
                        ):
                            await connection.execute(
                                sql.SQL(
                                    "REVOKE ALL PRIVILEGES ON " + object_kind + " FROM {}"
                                ).format(
                                    sql.Identifier(schema_name),
                                    sql.Identifier(executor),
                                )
                            )
                            await connection.execute(
                                sql.SQL(
                                    "REVOKE ALL PRIVILEGES ON " + object_kind + " FROM {}"
                                ).format(sql.Identifier(schema_name), sql.Identifier(observer))
                            )
                            await connection.execute(
                                sql.SQL(
                                    "REVOKE ALL PRIVILEGES ON " + object_kind + " FROM {}"
                                ).format(sql.Identifier(schema_name), sql.Identifier(runtime))
                            )
                        if public_usage:
                            await connection.execute(
                                sql.SQL("REVOKE USAGE ON SCHEMA {} FROM PUBLIC").format(
                                    sql.Identifier(schema_name)
                                )
                            )
                            if application_role_exists and schema_name != "loom_capacity_guard":
                                await connection.execute(
                                    sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                                        sql.Identifier(schema_name),
                                        sql.Identifier(identity.db_role),
                                    )
                                )
                    await connection.execute(
                        sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL("GRANT REFERENCES (id) ON TABLE public.trials TO {}").format(
                            sql.Identifier(owner)
                        )
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT SELECT (id, team_id, task_id, config, state, requires_caps, "
                            "submit_priority, batch_id, idempotency_key, sample_idx, "
                            "combination_idx, provider_connection_id, provider_model_id, "
                            "submitted_by_user_id, usage_attributed_user_id, "
                            "usage_attributed_actor, family_key, lifecycle_authority_id, "
                            "submitted_at, started_at, cancellation_requested_at, "
                            "cancellation_observed_at, finished_at, next_attempt_at, "
                            "autoscaler_pool_name, worker_id, attempt_count, "
                            "execution_route_json) "
                            "ON TABLE public.trials TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT UPDATE (lifecycle_authority_id, state, requires_caps, "
                            "worker_id, claimed_at, pre_start_heartbeat_at, failure_reason, "
                            "failure_message, attempt_count, next_attempt_at, "
                            "cancellation_requested_at, cancellation_observed_at, finished_at) "
                            "ON TABLE public.trials TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT INSERT (id, team_id, task_id, config, requires_caps, state, "
                            "submit_priority, batch_id, idempotency_key, sample_idx, "
                            "combination_idx, provider_connection_id, provider_model_id, "
                            "submitted_by_user_id, usage_attributed_user_id, "
                            "usage_attributed_actor, family_key) ON TABLE public.trials TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT SELECT (id) ON TABLE public.data_lifecycle_authorities TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT INSERT (environment, namespace, team_id, data_class, "
                            "owner_kind, owner_id, created_at, expires_at, pinned, state) "
                            "ON TABLE public.data_lifecycle_authorities TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT REFERENCES (id) ON TABLE public.data_lifecycle_authorities TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT SELECT (id, checksum, config, source, source_provenance) "
                            "ON TABLE public.tasks TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT SELECT (id, materialization_key, task_id, task_checksum, "
                            "cpu_arch, task_config, task_source, task_source_provenance, "
                            "state, registry_images) "
                            "ON TABLE public.task_image_materializations TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT SELECT (trial_id, materialization_id) "
                            "ON TABLE public.trial_task_image_materializations TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT UPDATE (state) ON TABLE public.task_image_materializations TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT SELECT (id, trial_id, combination_idx, mix_mode, k1, k2, "
                            "teacher_episodes, beta, seed, prng_version, "
                            "student_model_snapshot, teacher_model_snapshot, "
                            "provider_connection_id, pricing_snapshot, capability_snapshot, "
                            "inherited_from_plan_id, created_at) "
                            "ON TABLE public.model_switch_plans TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT SELECT (id, hostname, version, capabilities, "
                            "supported_work_kinds, capability_snapshot_digest, "
                            "capability_snapshot_json, slurm_gpu_allocation_evidence_json, "
                            "slurm_gpu_allocation_evidence_digest, auth_token_hash, "
                            "max_concurrent, pool_name, input_cache_capacity_bytes, "
                            "input_cache_reserved_bytes, input_cache_ready_bytes, status, "
                            "drain_state) "
                            "ON TABLE public.workers TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT INSERT (id, hostname, version, capabilities, "
                            "supported_work_kinds, capability_snapshot_digest, "
                            "capability_snapshot_json, slurm_gpu_allocation_evidence_json, "
                            "slurm_gpu_allocation_evidence_digest, auth_token_hash, "
                            "max_concurrent, pool_name, input_cache_capacity_bytes, "
                            "input_cache_reserved_bytes, input_cache_ready_bytes, "
                            "registered_at, last_seen_at, status) "
                            "ON TABLE public.workers TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL("GRANT UPDATE (status) ON TABLE public.workers TO {}").format(
                            sql.Identifier(owner)
                        )
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT SELECT (id, slurm_cluster_id, environment, pool_name, "
                            "nodelist, requested_cpus, requested_memory_mib, "
                            "requested_pids, requested_gpu_tres, requested_gpus, "
                            "requested_concurrency, "
                            "sandbox_identity, candidate_sha, compose_project, job_id, "
                            "slurm_state, state, worker_id) "
                            "ON TABLE public.slurm_worker_jobs TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT INSERT (id, slurm_cluster_id, environment, pool_name, "
                            "nodelist, requested_cpus, requested_memory_mib, "
                            "requested_gpu_tres, requested_gpus, requested_concurrency, "
                            "sandbox_identity, candidate_sha, compose_project, job_id, "
                            "slurm_state, state, submitted_at, started_at, "
                            "last_reconciled_at) ON TABLE public.slurm_worker_jobs TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT UPDATE (worker_id) ON TABLE public.slurm_worker_jobs TO {}"
                        ).format(sql.Identifier(owner))
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
                            "team_id",
                            "batch_id",
                            "environment",
                            "region",
                            "execution_class_id",
                            "pool_id",
                            "owner_kind",
                            "state",
                        ),
                    }
                    for table, columns in claim_select_columns.items():
                        await connection.execute(
                            sql.SQL("GRANT SELECT ({}) ON TABLE public.{} TO {}").format(
                                sql.SQL(", ").join(map(sql.Identifier, columns)),
                                sql.Identifier(table),
                                sql.Identifier(owner),
                            )
                        )
                    await connection.execute(
                        sql.SQL(
                            "GRANT UPDATE (state, updated_at) "
                            "ON TABLE public.batch_family_state TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT UPDATE (in_flight_count) ON TABLE public.team_quotas TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL("GRANT UPDATE (id) ON TABLE public.batches TO {}").format(
                            sql.Identifier(owner)
                        )
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT UPDATE (id) ON TABLE public.model_switch_plans TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT UPDATE (active_count, counter_updated_at) "
                            "ON TABLE public.execution_admission_policies TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT INSERT (trial_id, attempt, execution_role, team_id, "
                            "batch_id, environment, region, execution_class_id, pool_id, "
                            "owner_kind, owner_id, acquired_at) "
                            "ON TABLE public.execution_admission_reservations TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT UPDATE (state, released_at, release_reason) "
                            "ON TABLE public.execution_admission_reservations TO {}"
                        ).format(sql.Identifier(owner))
                    )
            if self._transient_role_admin:
                await self._verify_transient_login_credentials(
                    identity,
                    credentials,
                    migrator=migrator,
                    agent=agent,
                    observer=observer,
                    runtime=runtime,
                )
        except asyncio.CancelledError:
            await self._seal_migrator(identity, owner=owner, migrator=migrator)
            raise
        except PersonalDevCapacityInstallationError:
            await self._seal_migrator(identity, owner=owner, migrator=migrator)
            raise
        except Exception:
            await self._seal_migrator(identity, owner=owner, migrator=migrator)
            raise PersonalDevCapacityInstallationError(
                "protected capacity database role convergence failed"
            ) from None
        return owner, migrator, agent, executor, observer, runtime, migrator_url, agent_url
