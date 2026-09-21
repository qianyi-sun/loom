"""Project and authenticate protected workers in the public scheduler.

Revision ID: guard_0023
Revises: guard_0022
Create Date: 2026-09-02
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "guard_0023"
down_revision: str | Sequence[str] | None = "guard_0022"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"
REGISTER_FUNCTION = "register_staging_public_worker(text,jsonb)"
ASSERT_FUNCTION = "assert_staging_worker_session(uuid,text)"
CLAIM_TRIAL_FUNCTION = "claim_staging_assigned_trial(uuid,text,jsonb)"
CURRENT_REGISTRATION_FUNCTION = "current_protected_runtime_registration()"
SUBMIT_TRIAL_FUNCTION = (
    "submit_protected_runtime_trial_projection"
    "(uuid,jsonb,bytea,text,jsonb,bytea,text,bytea,text,jsonb,bytea,text)"
)
PUBLISH_TRIAL_READINESS_FUNCTION = (
    "publish_protected_runtime_trial_readiness(uuid,uuid,uuid)"
)
_SEALED_NETWORK_POLICIES = "WHERE policy NOT IN ('public', 'no-network', 'allowlist')"
_RUNTIME_NETWORK_POLICIES = (
    "WHERE policy NOT IN ('public', 'no-network', 'gateway-only', 'allowlist')"
)
_ATOMIC_REPLAY_UNAVAILABLE_CONFLICT = """RAISE EXCEPTION 'conflicting atomic trial submission replay'
                USING ERRCODE = '55000';"""
_ATOMIC_REPLAY_IDEMPOTENCY_CONFLICT = """RAISE EXCEPTION 'conflicting atomic trial submission replay'
                USING ERRCODE = '23505';"""
_UNFILTERED_LIFECYCLE_SOURCE = """WHERE head.lifecycle_state IN ('pending-unassigned', 'assigned')
            ORDER BY head.protected_attempt_id"""
_READINESS_FILTERED_LIFECYCLE_SOURCE = """WHERE head.lifecycle_state IN ('pending-unassigned', 'assigned')
              AND (
                NOT EXISTS (
                  SELECT 1
                    FROM loom_capacity_guard.protected_runtime_trial_submissions AS runtime
                   WHERE runtime.protected_attempt_id = head.protected_attempt_id
                )
                OR EXISTS (
                  SELECT 1
                    FROM loom_capacity_guard.protected_runtime_trial_readiness AS readiness
                   WHERE readiness.protected_attempt_id = head.protected_attempt_id
                )
              )
            ORDER BY head.protected_attempt_id"""
_LEGACY_ASSIGNMENT_PUBLIC_STATE = """AND ta.claim_state = 'queued'
                 AND t.state = 'queued'"""
_RUNTIME_AWARE_ASSIGNMENT_PUBLIC_STATE = f"""AND ta.claim_state = 'queued'
                 AND (
                   (
                     t.state = 'queued'
                     AND NOT EXISTS (
                       SELECT 1
                         FROM {SCHEMA}.protected_runtime_trial_submissions AS runtime
                        WHERE runtime.trial_id = t.id
                          AND runtime.protected_attempt_id = ta.protected_attempt_id
                     )
                   )
                   OR (
                     t.state = 'protected-pending'
                     AND EXISTS (
                       SELECT 1
                         FROM {SCHEMA}.protected_runtime_trial_submissions AS runtime
                         JOIN {SCHEMA}.protected_runtime_trial_readiness AS readiness
                           ON readiness.trial_id = runtime.trial_id
                          AND readiness.protected_attempt_id = runtime.protected_attempt_id
                         CROSS JOIN LATERAL (
                           SELECT {SCHEMA}.inspect_protected_runtime_trial_prerequisites(
                             p_agent_incarnation, runtime.trial_id,
                             runtime.protected_attempt_id, true
                           ) AS payload
                         ) AS current
                        WHERE runtime.trial_id = t.id
                          AND runtime.protected_attempt_id = ta.protected_attempt_id
                          AND readiness.public_requires_caps_digest =
                                current.payload->>'public_requires_caps_digest'
                          AND readiness.task_image_prerequisites_digest =
                                current.payload->>'task_image_prerequisites_digest'
                          AND readiness.model_switch_prerequisite_digest =
                                current.payload->>'model_switch_prerequisite_digest'
                     )
                   )
                 )"""
_LEGACY_CURRENT_ASSIGNMENT_PUBLIC_STATE = """AND attempt.claim_state = 'queued'
             AND trial.state = 'queued'"""
_RUNTIME_AWARE_CURRENT_ASSIGNMENT_PUBLIC_STATE = f"""AND attempt.claim_state = 'queued'
             AND (
               (
                 trial.state = 'queued'
                 AND NOT EXISTS (
                   SELECT 1
                     FROM {SCHEMA}.protected_runtime_trial_submissions AS runtime
                    WHERE runtime.trial_id = trial.id
                      AND runtime.protected_attempt_id = attempt.protected_attempt_id
                 )
               )
               OR (
                 trial.state = 'protected-pending'
                 AND EXISTS (
                   SELECT 1
                     FROM {SCHEMA}.protected_runtime_trial_submissions AS runtime
                     JOIN {SCHEMA}.protected_runtime_trial_readiness AS readiness
                       ON readiness.trial_id = runtime.trial_id
                      AND readiness.protected_attempt_id = runtime.protected_attempt_id
                     CROSS JOIN LATERAL (
                       SELECT {SCHEMA}.inspect_protected_runtime_trial_prerequisites(
                         p_agent_incarnation, runtime.trial_id,
                         runtime.protected_attempt_id, true
                       ) AS payload
                     ) AS current
                    WHERE runtime.trial_id = trial.id
                      AND runtime.protected_attempt_id = attempt.protected_attempt_id
                      AND readiness.public_requires_caps_digest =
                            current.payload->>'public_requires_caps_digest'
                      AND readiness.task_image_prerequisites_digest =
                            current.payload->>'task_image_prerequisites_digest'
                      AND readiness.model_switch_prerequisite_digest =
                            current.payload->>'model_switch_prerequisite_digest'
                 )
               )
             )"""


def _runtime_role() -> tuple[str, str]:
    role = op.get_context().config.attributes.get("capacity_guard_runtime_role")
    if not isinstance(role, str) or not role:
        raise RuntimeError("staging worker session migration is missing the runtime role")
    return role, op.get_bind().dialect.identifier_preparer.quote(role)


def _replace_function_clause(function: str, old: str, new: str) -> None:
    escaped_old = old.replace("'", "''")
    escaped_new = new.replace("'", "''")
    op.execute(
        f"""
        DO $$
        DECLARE
          v_definition text;
        BEGIN
          SELECT pg_get_functiondef(
            '{SCHEMA}.{function}'::regprocedure
          ) INTO v_definition;
          IF position('{escaped_old}' in v_definition) = 0 THEN
            RAISE EXCEPTION
              'protected runtime function clause was not found: {function}';
          END IF;
          IF length(v_definition) - length(replace(v_definition, '{escaped_old}', ''))
               <> length('{escaped_old}') THEN
            RAISE EXCEPTION
              'protected runtime function clause was ambiguous: {function}';
          END IF;
          EXECUTE replace(v_definition, '{escaped_old}', '{escaped_new}');
        END $$;
        """
    )


def _patch_runtime_assignment_admission() -> None:
    _replace_function_clause(
        "apply_inert_attempt_transition(uuid,jsonb,bytea,text)",
        _LEGACY_ASSIGNMENT_PUBLIC_STATE,
        _RUNTIME_AWARE_ASSIGNMENT_PUBLIC_STATE,
    )
    _replace_function_clause(
        "assert_current_inert_assignment(uuid,jsonb,bytea,text)",
        _LEGACY_CURRENT_ASSIGNMENT_PUBLIC_STATE,
        _RUNTIME_AWARE_CURRENT_ASSIGNMENT_PUBLIC_STATE,
    )


def _unpatch_runtime_assignment_admission() -> None:
    _replace_function_clause(
        "assert_current_inert_assignment(uuid,jsonb,bytea,text)",
        _RUNTIME_AWARE_CURRENT_ASSIGNMENT_PUBLIC_STATE,
        _LEGACY_CURRENT_ASSIGNMENT_PUBLIC_STATE,
    )
    _replace_function_clause(
        "apply_inert_attempt_transition(uuid,jsonb,bytea,text)",
        _RUNTIME_AWARE_ASSIGNMENT_PUBLIC_STATE,
        _LEGACY_ASSIGNMENT_PUBLIC_STATE,
    )


_AGENT_BINDING_DECLARATION = """v_registration loom_capacity_guard.agent_registrations%ROWTYPE;
          v_agent_role text;"""
_RUNTIME_BINDING_DECLARATION = """v_registration loom_capacity_guard.agent_registrations%ROWTYPE;
          v_agent_role text;
          v_runtime_role text;"""
_AGENT_BINDING_ROLE_CHECK = """SELECT agent_role_name INTO v_agent_role
            FROM loom_capacity_guard.agent_runtime_authority
           WHERE singleton_id = 1;
          IF v_agent_role IS NULL OR session_user::text <> v_agent_role THEN"""
_RUNTIME_BINDING_ROLE_CHECK = """SELECT agent.agent_role_name, runtime.runtime_role_name
            INTO v_agent_role, v_runtime_role
            FROM loom_capacity_guard.agent_runtime_authority AS agent
            CROSS JOIN loom_capacity_guard.staging_worker_runtime_authority AS runtime
           WHERE agent.singleton_id = 1
             AND runtime.singleton_id = 1;
          IF v_agent_role IS NULL
             OR v_runtime_role IS NULL
             OR session_user::text NOT IN (v_agent_role, v_runtime_role) THEN"""


def _install_atomic_lifecycle_capture(*, runtime_aware: bool) -> None:
    runtime_declarations = """
          v_runtime record;
          v_readiness loom_capacity_guard.protected_runtime_trial_readiness%ROWTYPE;
          v_snapshot jsonb;
          v_is_ready boolean;""" if runtime_aware else ""
    runtime_validation = f"""
          -- Runtime-origin demand remains inert until the guard has published
          -- readiness. Revalidate both pending and ready origins while holding
          -- the same writer mutex used by readiness publication; an append-only
          -- readiness receipt is therefore never treated as a stale capability.
          FOR v_runtime IN
            SELECT runtime.trial_id, runtime.protected_attempt_id,
                   (submission.payload->>'agent_incarnation')::uuid
                     AS agent_incarnation
              FROM {SCHEMA}.protected_runtime_trial_submissions AS runtime
              JOIN {SCHEMA}.atomic_trial_submissions AS submission
                ON submission.trial_id = runtime.trial_id
               AND submission.protected_attempt_id = runtime.protected_attempt_id
              JOIN {SCHEMA}.trial_attempts AS attempt
                ON attempt.trial_id = runtime.trial_id
               AND attempt.protected_attempt_id = runtime.protected_attempt_id
              JOIN {SCHEMA}.attempt_lifecycle_heads AS head
                ON head.protected_attempt_id = attempt.protected_attempt_id
             WHERE head.lifecycle_state IN ('pending-unassigned', 'assigned')
             ORDER BY runtime.protected_attempt_id
             FOR KEY SHARE OF runtime, submission, attempt, head
          LOOP
            IF v_runtime.agent_incarnation IS DISTINCT FROM p_agent_incarnation THEN
              RAISE EXCEPTION 'protected runtime submission agent binding drifted'
                USING ERRCODE = '55000';
            END IF;
            SELECT readiness.* INTO v_readiness
              FROM {SCHEMA}.protected_runtime_trial_readiness AS readiness
             WHERE readiness.trial_id = v_runtime.trial_id
               AND readiness.protected_attempt_id = v_runtime.protected_attempt_id
             FOR KEY SHARE;
            v_is_ready := FOUND;
            v_snapshot := {SCHEMA}.inspect_protected_runtime_trial_prerequisites(
              p_agent_incarnation, v_runtime.trial_id,
              v_runtime.protected_attempt_id, v_is_ready
            );
            IF v_is_ready
               AND (
                 v_readiness.public_requires_caps_digest IS DISTINCT FROM
                   v_snapshot->>'public_requires_caps_digest'
                 OR v_readiness.task_image_prerequisites_digest IS DISTINCT FROM
                      v_snapshot->>'task_image_prerequisites_digest'
                 OR v_readiness.model_switch_prerequisite_digest IS DISTINCT FROM
                      v_snapshot->>'model_switch_prerequisite_digest'
               ) THEN
              RAISE EXCEPTION 'protected runtime readiness prerequisites drifted'
                USING ERRCODE = '55000';
            END IF;
          END LOOP;
""" if runtime_aware else ""
    runtime_eligibility = f"""
             AND (
               NOT EXISTS (
                 SELECT 1
                   FROM {SCHEMA}.protected_runtime_trial_submissions AS runtime
                  WHERE runtime.protected_attempt_id = head.protected_attempt_id
               )
               OR EXISTS (
                 SELECT 1
                   FROM {SCHEMA}.protected_runtime_trial_readiness AS readiness
                  WHERE readiness.protected_attempt_id = head.protected_attempt_id
               )
             )""" if runtime_aware else ""
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.capture_lifecycle_demand_observation(
          p_agent_incarnation uuid,
          p_expected_high_water bigint,
          p_max_attempts integer
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_agent_role text;
          v_fenced_rows bigint;
          v_updated_rows bigint;
          v_payload jsonb;{runtime_declarations}
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION 'atomic lifecycle capture requires a SERIALIZABLE transaction'
              USING ERRCODE = '25000';
          END IF;
          SELECT agent_role_name INTO v_agent_role
            FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1;
          IF v_agent_role IS NULL OR session_user::text <> v_agent_role THEN
            RAISE EXCEPTION 'capacity demand caller is not the registered agent role'
              USING ERRCODE = '42501';
          END IF;
          IF pg_has_role(session_user, current_user, 'MEMBER') THEN
            RAISE EXCEPTION 'capacity demand agent unexpectedly holds owner membership'
              USING ERRCODE = '42501';
          END IF;

          -- Keep the state translation invisible to legacy claimers. The old
          -- capture owns the same writer mutex and sees queued only inside
          -- this SERIALIZABLE transaction; concurrent readers retain the
          -- committed protected-pending version.
          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;
{runtime_validation}
          IF EXISTS (
            SELECT 1
              FROM {SCHEMA}.atomic_trial_submissions AS submission
              JOIN public.trials AS trial ON trial.id = submission.trial_id
              JOIN {SCHEMA}.trial_attempts AS attempt
                ON attempt.trial_id = submission.trial_id
               AND attempt.protected_attempt_id = submission.protected_attempt_id
              JOIN {SCHEMA}.attempt_lifecycle_heads AS head
                ON head.protected_attempt_id = attempt.protected_attempt_id
             WHERE head.lifecycle_state IN ('pending-unassigned', 'assigned')
               AND (
                 head.executable IS DISTINCT FROM false
                 OR trial.state IS DISTINCT FROM 'protected-pending'
                 OR trial.cancellation_requested_at IS NOT NULL
                 OR trial.worker_id IS NOT NULL
                 OR trial.attempt_count IS DISTINCT FROM 0
                 OR trial.next_attempt_at IS NOT NULL
                 OR trial.autoscaler_pool_name IS NOT NULL
               )
          ) THEN
            RAISE EXCEPTION 'atomic shadow demand public fence drifted'
              USING ERRCODE = '55000';
          END IF;
          SELECT count(*) INTO v_fenced_rows
            FROM {SCHEMA}.atomic_trial_submissions AS submission
            JOIN public.trials AS trial ON trial.id = submission.trial_id
            JOIN {SCHEMA}.trial_attempts AS attempt
              ON attempt.trial_id = submission.trial_id
             AND attempt.protected_attempt_id = submission.protected_attempt_id
            JOIN {SCHEMA}.attempt_lifecycle_heads AS head
              ON head.protected_attempt_id = attempt.protected_attempt_id
           WHERE head.lifecycle_state IN ('pending-unassigned', 'assigned')
             AND head.executable = false
             AND trial.state = 'protected-pending'
             AND trial.cancellation_requested_at IS NULL
             AND trial.worker_id IS NULL
             AND trial.attempt_count = 0
             AND trial.next_attempt_at IS NULL
             AND trial.autoscaler_pool_name IS NULL{runtime_eligibility};

          UPDATE public.trials AS trial
             SET state = 'queued'
            FROM {SCHEMA}.atomic_trial_submissions AS submission,
                 {SCHEMA}.trial_attempts AS attempt,
                 {SCHEMA}.attempt_lifecycle_heads AS head
           WHERE trial.id = submission.trial_id
             AND attempt.trial_id = submission.trial_id
             AND attempt.protected_attempt_id = submission.protected_attempt_id
             AND head.protected_attempt_id = attempt.protected_attempt_id
             AND head.lifecycle_state IN ('pending-unassigned', 'assigned')
             AND head.executable = false
             AND trial.state = 'protected-pending'
             AND trial.cancellation_requested_at IS NULL
             AND trial.worker_id IS NULL
             AND trial.attempt_count = 0
             AND trial.next_attempt_at IS NULL
             AND trial.autoscaler_pool_name IS NULL{runtime_eligibility};
          GET DIAGNOSTICS v_updated_rows = ROW_COUNT;
          IF v_updated_rows IS DISTINCT FROM v_fenced_rows THEN
            RAISE EXCEPTION 'atomic shadow demand queue projection raced'
              USING ERRCODE = '40001';
          END IF;

          v_payload := {SCHEMA}.capture_lifecycle_demand_observation_v2_queued(
            p_agent_incarnation, p_expected_high_water, p_max_attempts
          );

          UPDATE public.trials AS trial
             SET state = 'protected-pending'
            FROM {SCHEMA}.atomic_trial_submissions AS submission,
                 {SCHEMA}.trial_attempts AS attempt,
                 {SCHEMA}.attempt_lifecycle_heads AS head
           WHERE trial.id = submission.trial_id
             AND attempt.trial_id = submission.trial_id
             AND attempt.protected_attempt_id = submission.protected_attempt_id
             AND head.protected_attempt_id = attempt.protected_attempt_id
             AND head.lifecycle_state IN ('pending-unassigned', 'assigned')
             AND head.executable = false
             AND trial.state = 'queued'
             AND trial.cancellation_requested_at IS NULL
             AND trial.worker_id IS NULL
             AND trial.attempt_count = 0
             AND trial.next_attempt_at IS NULL
             AND trial.autoscaler_pool_name IS NULL{runtime_eligibility};
          GET DIAGNOSTICS v_updated_rows = ROW_COUNT;
          IF v_updated_rows IS DISTINCT FROM v_fenced_rows THEN
            RAISE EXCEPTION 'atomic shadow demand public fence restoration raced'
              USING ERRCODE = '40001';
          END IF;
          RETURN v_payload;
        END
        $function$
        """
    )


def _install_runtime_submission_ledgers() -> None:
    op.create_unique_constraint(
        "guard_atomic_submission_trial_attempt_key",
        "atomic_trial_submissions",
        ["trial_id", "protected_attempt_id"],
        schema=SCHEMA,
    )
    op.create_table(
        "protected_runtime_trial_submissions",
        sa.Column("trial_id", sa.Uuid(), nullable=False),
        sa.Column("protected_attempt_id", sa.Uuid(), nullable=False),
        sa.Column("public_requires_caps", postgresql.JSONB(), nullable=False),
        sa.Column("public_requires_caps_canonical", sa.LargeBinary(), nullable=False),
        sa.Column("public_requires_caps_digest", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("statement_timestamp()"),
        ),
        sa.CheckConstraint(
            "trial_id <> protected_attempt_id",
            name="guard_runtime_submission_distinct_identity_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(public_requires_caps) = 'object' AND "
            "octet_length(public_requires_caps::text) <= 8388608 AND "
            "octet_length(public_requires_caps_canonical) <= 8388608 AND "
            "convert_from(public_requires_caps_canonical, 'UTF8')::jsonb "
            "= public_requires_caps",
            name="guard_runtime_submission_public_requirements_check",
        ),
        sa.CheckConstraint(
            "public_requires_caps_digest ~ '^[0-9a-f]{64}$' AND "
            "encode(sha256(public_requires_caps_canonical), 'hex') "
            "= public_requires_caps_digest",
            name="guard_runtime_submission_public_digest_check",
        ),
        sa.ForeignKeyConstraint(
            ["trial_id", "protected_attempt_id"],
            [
                f"{SCHEMA}.atomic_trial_submissions.trial_id",
                f"{SCHEMA}.atomic_trial_submissions.protected_attempt_id",
            ],
            ondelete="RESTRICT",
            name="guard_runtime_submission_atomic_binding_fk",
        ),
        sa.PrimaryKeyConstraint("trial_id"),
        sa.UniqueConstraint(
            "protected_attempt_id",
            name="guard_runtime_submission_attempt_key",
        ),
        sa.UniqueConstraint(
            "trial_id",
            "protected_attempt_id",
            name="guard_runtime_submission_trial_attempt_key",
        ),
        schema=SCHEMA,
    )
    op.create_table(
        "protected_runtime_trial_readiness",
        sa.Column("trial_id", sa.Uuid(), nullable=False),
        sa.Column("protected_attempt_id", sa.Uuid(), nullable=False),
        sa.Column("public_requires_caps_digest", sa.Text(), nullable=False),
        sa.Column("task_image_prerequisites_digest", sa.Text(), nullable=False),
        sa.Column("model_switch_prerequisite_digest", sa.Text(), nullable=False),
        sa.Column(
            "ready_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("statement_timestamp()"),
        ),
        sa.CheckConstraint(
            "trial_id <> protected_attempt_id",
            name="guard_runtime_readiness_distinct_identity_check",
        ),
        sa.CheckConstraint(
            "public_requires_caps_digest ~ '^[0-9a-f]{64}$' AND "
            "task_image_prerequisites_digest ~ '^[0-9a-f]{64}$' AND "
            "model_switch_prerequisite_digest ~ '^[0-9a-f]{64}$'",
            name="guard_runtime_readiness_digest_check",
        ),
        sa.ForeignKeyConstraint(
            ["trial_id", "protected_attempt_id"],
            [
                f"{SCHEMA}.protected_runtime_trial_submissions.trial_id",
                f"{SCHEMA}.protected_runtime_trial_submissions.protected_attempt_id",
            ],
            ondelete="RESTRICT",
            name="guard_runtime_readiness_submission_binding_fk",
        ),
        sa.PrimaryKeyConstraint("trial_id"),
        sa.UniqueConstraint(
            "protected_attempt_id",
            name="guard_runtime_readiness_attempt_key",
        ),
        schema=SCHEMA,
    )
    for table in (
        "protected_runtime_trial_submissions",
        "protected_runtime_trial_readiness",
    ):
        op.execute(
            f"CREATE TRIGGER {table}_append_only_row "
            f"BEFORE UPDATE OR DELETE ON {SCHEMA}.{table} "
            f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()"
        )
        op.execute(
            f"CREATE TRIGGER {table}_append_only_truncate "
            f"BEFORE TRUNCATE ON {SCHEMA}.{table} "
            f"FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()"
        )


def _install_runtime_submission_functions() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.current_protected_runtime_registration()
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_runtime_role text;
          v_registration {SCHEMA}.agent_registrations%ROWTYPE;
        BEGIN
          SELECT runtime_role_name INTO v_runtime_role
            FROM {SCHEMA}.staging_worker_runtime_authority
           WHERE singleton_id = 1
           FOR KEY SHARE;
          IF v_runtime_role IS NULL OR session_user::text <> v_runtime_role THEN
            RAISE EXCEPTION 'protected submission runtime caller is not bound'
              USING ERRCODE = '42501';
          END IF;
          IF pg_has_role(session_user, current_user, 'MEMBER') THEN
            RAISE EXCEPTION 'protected submission runtime unexpectedly holds owner membership'
              USING ERRCODE = '42501';
          END IF;

          SELECT registration.* INTO v_registration
            FROM {SCHEMA}.agent_registrations AS registration
            JOIN {SCHEMA}.authority_state AS authority
              ON authority.singleton_id = registration.singleton_id
             AND authority.environment_id = registration.environment_id
             AND authority.subject_id = registration.subject_id
             AND authority.subject_incarnation = registration.subject_incarnation
             AND authority.authority_incarnation = registration.authority_incarnation
             AND authority.reporter_incarnation = registration.reporter_incarnation
             AND authority.authority_mode = registration.authority_mode
             AND authority.allocation_epoch = registration.allocation_epoch
             AND authority.deployment_generation = registration.deployment_generation
             AND authority.configuration_generation = registration.configuration_generation
             AND authority.candidate_digest = registration.candidate_digest
           WHERE registration.singleton_id = 1
             AND registration.registration_state = 'registered'
           FOR KEY SHARE OF registration, authority;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'protected submission agent registration is unavailable'
              USING ERRCODE = '55000';
          END IF;

          RETURN jsonb_build_object(
            'schema_version', 1,
            'environment_id', v_registration.environment_id,
            'subject_id', v_registration.subject_id,
            'subject_incarnation', v_registration.subject_incarnation,
            'authority_incarnation', v_registration.authority_incarnation,
            'agent_incarnation', v_registration.agent_incarnation,
            'reporter_incarnation', v_registration.reporter_incarnation,
            'authority_mode', v_registration.authority_mode,
            'allocation_epoch', v_registration.allocation_epoch,
            'reporter_high_water', 0,
            'candidate_digest', v_registration.candidate_digest,
            'candidate_identity_algorithm', v_registration.candidate_identity_algorithm,
            'candidate_identity', v_registration.candidate_identity,
            'candidate_publication_sha256', v_registration.candidate_publication_sha256,
            'deployment_generation', v_registration.deployment_generation,
            'configuration_generation', v_registration.configuration_generation
          );
        END
        $function$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.submit_protected_runtime_trial_projection(
          p_agent_incarnation uuid,
          p_payload jsonb,
          p_canonical_payload bytea,
          p_payload_digest text,
          p_protected_payload jsonb,
          p_protected_canonical_payload bytea,
          p_protected_payload_digest text,
          p_requirements_payload bytea,
          p_requirements_digest text,
          p_public_requires_caps jsonb,
          p_public_requires_caps_canonical bytea,
          p_public_requires_caps_digest text
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_runtime_role text;
          v_receipt jsonb;
          v_stored {SCHEMA}.protected_runtime_trial_submissions%ROWTYPE;
          v_updated_rows bigint;
        BEGIN
          SELECT runtime_role_name INTO v_runtime_role
            FROM {SCHEMA}.staging_worker_runtime_authority
           WHERE singleton_id = 1
           FOR KEY SHARE;
          IF v_runtime_role IS NULL OR session_user::text <> v_runtime_role THEN
            RAISE EXCEPTION 'protected submission runtime caller is not bound'
              USING ERRCODE = '42501';
          END IF;
          IF pg_has_role(session_user, current_user, 'MEMBER') THEN
            RAISE EXCEPTION 'protected submission runtime unexpectedly holds owner membership'
              USING ERRCODE = '42501';
          END IF;
          IF jsonb_typeof(p_public_requires_caps) IS DISTINCT FROM 'object'
             OR octet_length(p_public_requires_caps::text) > 8388608
             OR octet_length(p_public_requires_caps_canonical) > 8388608
             OR convert_from(p_public_requires_caps_canonical, 'UTF8')::jsonb
                  IS DISTINCT FROM p_public_requires_caps
             OR p_public_requires_caps_canonical IS DISTINCT FROM
                  convert_to(p_public_requires_caps::text, 'UTF8')
             OR p_public_requires_caps_digest !~ '^[0-9a-f]{{64}}$'
             OR encode(sha256(p_public_requires_caps_canonical), 'hex')
                  IS DISTINCT FROM p_public_requires_caps_digest
             OR (SELECT count(*) FROM jsonb_object_keys(p_public_requires_caps))
                  NOT IN (6, 7)
             OR NOT p_public_requires_caps ?& ARRAY[
                  'backend', 'os', 'cpu_arch', 'gpu_vendor',
                  'network_policies', 'terminus2_model_switch'
                ]
             OR p_public_requires_caps - ARRAY[
                  'backend', 'os', 'cpu_arch', 'gpu_vendor',
                  'network_policies', 'terminus2_model_switch', 'worker_pool'
                ] <> '{{}}'::jsonb
             OR p_public_requires_caps->>'backend' IS DISTINCT FROM 'docker'
             OR p_public_requires_caps->>'os'
                  IS DISTINCT FROM p_protected_payload->'requirements'->>'os'
             OR p_public_requires_caps->>'cpu_arch'
                  IS DISTINCT FROM p_protected_payload->'requirements'->>'cpu_arch'
             OR p_public_requires_caps->>'gpu_vendor'
                  IS DISTINCT FROM p_protected_payload->'requirements'->>'gpu_vendor'
             OR p_public_requires_caps->'network_policies'
                  IS DISTINCT FROM p_protected_payload->'requirements'->'network_policies'
             OR jsonb_typeof(p_public_requires_caps->'terminus2_model_switch')
                  IS DISTINCT FROM 'boolean'
             OR (CASE
                  WHEN p_protected_payload->'requirements'->>'required_pool' = 'oldlab'
                  THEN COALESCE(
                    p_public_requires_caps->>'worker_pool' NOT IN (
                      'oldlab', 'behavior-cpu-data', 'behavior-gpu-oldlab',
                      'terminalgen-generate-gateway', 'terminalgen-package-none',
                      'terminalgen-plan-none', 'terminalgen-validate-none'
                    ), true
                  )
                  WHEN p_protected_payload->'requirements'->>'required_pool' = 'gb10'
                  THEN COALESCE(
                    p_public_requires_caps->>'worker_pool' NOT IN (
                      'gb10', 'behavior-gpu-gb10'
                    ), true
                  )
                  ELSE p_public_requires_caps ? 'worker_pool'
                END) THEN
            RAISE EXCEPTION 'protected public trial requirements are invalid'
              USING ERRCODE = '22023';
          END IF;

          v_receipt := {SCHEMA}.submit_inert_trial_projection(
            p_agent_incarnation,
            p_payload,
            p_canonical_payload,
            p_payload_digest,
            p_protected_payload,
            p_protected_canonical_payload,
            p_protected_payload_digest,
            p_requirements_payload,
            p_requirements_digest
          );

          SELECT runtime.* INTO v_stored
            FROM {SCHEMA}.protected_runtime_trial_submissions AS runtime
           WHERE runtime.trial_id = (v_receipt->>'trial_id')::uuid
             AND runtime.protected_attempt_id =
                   (v_receipt->>'protected_attempt_id')::uuid
           FOR KEY SHARE;
          IF FOUND THEN
            IF (v_receipt->>'replayed')::boolean IS DISTINCT FROM true
               OR v_stored.public_requires_caps IS DISTINCT FROM p_public_requires_caps
               OR v_stored.public_requires_caps_canonical IS DISTINCT FROM
                    p_public_requires_caps_canonical
               OR v_stored.public_requires_caps_digest IS DISTINCT FROM
                    p_public_requires_caps_digest
               OR NOT EXISTS (
                 SELECT 1 FROM public.trials AS trial
                  WHERE trial.id = v_stored.trial_id
                    AND trial.state = 'protected-pending'
                    AND trial.requires_caps = v_stored.public_requires_caps
                    AND trial.worker_id IS NULL
                    AND trial.attempt_count = 0
                    AND trial.cancellation_requested_at IS NULL
                    AND trial.next_attempt_at IS NULL
                    AND trial.autoscaler_pool_name IS NULL
               ) THEN
              RAISE EXCEPTION 'conflicting protected runtime submission replay'
                USING ERRCODE = '55000';
            END IF;
            RETURN v_receipt;
          END IF;
          IF (v_receipt->>'replayed')::boolean IS DISTINCT FROM false THEN
            RAISE EXCEPTION 'protected runtime submission lacks origin provenance'
              USING ERRCODE = '55000';
          END IF;

          UPDATE public.trials AS trial
             SET requires_caps = p_public_requires_caps
           WHERE trial.id = (v_receipt->>'trial_id')::uuid
             AND trial.state = 'protected-pending'
             AND trial.worker_id IS NULL
             AND trial.attempt_count = 0
             AND trial.cancellation_requested_at IS NULL
             AND trial.next_attempt_at IS NULL
             AND trial.autoscaler_pool_name IS NULL;
          GET DIAGNOSTICS v_updated_rows = ROW_COUNT;
          IF v_updated_rows IS DISTINCT FROM 1 THEN
            RAISE EXCEPTION 'protected runtime public capability projection drifted'
              USING ERRCODE = '40001';
          END IF;
          INSERT INTO {SCHEMA}.protected_runtime_trial_submissions
            (trial_id, protected_attempt_id, public_requires_caps,
             public_requires_caps_canonical, public_requires_caps_digest)
          VALUES
            ((v_receipt->>'trial_id')::uuid,
             (v_receipt->>'protected_attempt_id')::uuid,
             p_public_requires_caps, p_public_requires_caps_canonical,
             p_public_requires_caps_digest);
          RETURN v_receipt;
        END
        $function$
        """
    )


def _install_runtime_readiness_functions() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.inspect_protected_runtime_trial_prerequisites(
          p_agent_incarnation uuid,
          p_trial_id uuid,
          p_protected_attempt_id uuid,
          p_require_prerequisites boolean
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_origin record;
          v_trial record;
          v_task record;
          v_requires_task_images boolean;
          v_expected_arches text[];
          v_task_images jsonb;
          v_task_image_count bigint;
          v_task_image_digest text;
          v_model_switches jsonb;
          v_model_switch_count bigint;
          v_model_switch_required boolean;
          v_model_switch_digest text;
        BEGIN
          IF p_agent_incarnation IS NULL
             OR p_trial_id IS NULL
             OR p_protected_attempt_id IS NULL
             OR p_trial_id = p_protected_attempt_id
             OR p_require_prerequisites IS NULL THEN
            RAISE EXCEPTION 'protected runtime prerequisite identity is invalid'
              USING ERRCODE = '22023';
          END IF;
          SELECT runtime.public_requires_caps,
                 runtime.public_requires_caps_digest,
                 submission.payload,
                 submission.lifecycle_authority_id,
                 submission.submitted_at
            INTO v_origin
            FROM {SCHEMA}.protected_runtime_trial_submissions AS runtime
            JOIN {SCHEMA}.atomic_trial_submissions AS submission
              ON submission.trial_id = runtime.trial_id
             AND submission.protected_attempt_id = runtime.protected_attempt_id
            JOIN {SCHEMA}.trial_attempts AS attempt
              ON attempt.trial_id = runtime.trial_id
             AND attempt.protected_attempt_id = runtime.protected_attempt_id
            JOIN {SCHEMA}.attempt_lifecycle_heads AS head
              ON head.protected_attempt_id = attempt.protected_attempt_id
           WHERE runtime.trial_id = p_trial_id
             AND runtime.protected_attempt_id = p_protected_attempt_id
             AND (submission.payload->>'agent_incarnation')::uuid = p_agent_incarnation
             AND head.lifecycle_state IN ('pending-unassigned', 'assigned')
             AND head.executable = false
             AND EXISTS (
               SELECT 1
                 FROM {SCHEMA}.agent_registrations AS registration
                 JOIN {SCHEMA}.authority_state AS authority
                   ON authority.singleton_id = registration.singleton_id
                  AND authority.environment_id = registration.environment_id
                  AND authority.subject_id = registration.subject_id
                  AND authority.subject_incarnation = registration.subject_incarnation
                  AND authority.authority_incarnation = registration.authority_incarnation
                  AND authority.reporter_incarnation = registration.reporter_incarnation
                  AND authority.authority_mode = registration.authority_mode
                  AND authority.allocation_epoch = registration.allocation_epoch
                  AND authority.deployment_generation = registration.deployment_generation
                  AND authority.configuration_generation =
                        registration.configuration_generation
                  AND authority.candidate_digest = registration.candidate_digest
                WHERE registration.agent_incarnation = p_agent_incarnation
                  AND registration.registration_state = 'registered'
             )
           FOR KEY SHARE OF runtime, submission, attempt, head;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'protected runtime submission binding is unavailable'
              USING ERRCODE = '55000';
          END IF;

          SELECT trial.team_id, trial.task_id, trial.config, trial.requires_caps,
                 trial.state, trial.submit_priority, trial.batch_id,
                 trial.idempotency_key, trial.sample_idx, trial.combination_idx,
                 trial.provider_connection_id, trial.provider_model_id,
                 trial.submitted_by_user_id, trial.usage_attributed_user_id,
                 trial.usage_attributed_actor, trial.family_key,
                 trial.lifecycle_authority_id, trial.submitted_at,
                 trial.cancellation_requested_at, trial.next_attempt_at,
                 trial.autoscaler_pool_name, trial.worker_id, trial.attempt_count
            INTO v_trial
            FROM public.trials AS trial
           WHERE trial.id = p_trial_id
           FOR UPDATE;
          IF NOT FOUND
             OR v_trial.team_id IS DISTINCT FROM (v_origin.payload->>'team_id')::uuid
             OR v_trial.task_id IS DISTINCT FROM v_origin.payload->>'task_id'
             OR v_trial.config IS DISTINCT FROM v_origin.payload->'config'
             OR v_trial.requires_caps IS DISTINCT FROM v_origin.public_requires_caps
             OR v_trial.state IS DISTINCT FROM 'protected-pending'
             OR v_trial.submit_priority IS DISTINCT FROM
                  (v_origin.payload->>'submit_priority')::integer
             OR v_trial.batch_id IS DISTINCT FROM (v_origin.payload->>'batch_id')::uuid
             OR v_trial.idempotency_key IS DISTINCT FROM
                  v_origin.payload->>'idempotency_key'
             OR v_trial.sample_idx IS DISTINCT FROM
                  (v_origin.payload->>'sample_idx')::integer
             OR v_trial.combination_idx IS DISTINCT FROM
                  (v_origin.payload->>'combination_idx')::integer
             OR v_trial.provider_connection_id IS DISTINCT FROM
                  (v_origin.payload->>'provider_connection_id')::uuid
             OR v_trial.provider_model_id IS DISTINCT FROM
                  v_origin.payload->>'provider_model_id'
             OR v_trial.submitted_by_user_id IS DISTINCT FROM
                  (v_origin.payload->>'submitted_by_user_id')::uuid
             OR v_trial.usage_attributed_user_id IS DISTINCT FROM
                  (v_origin.payload->>'usage_attributed_user_id')::uuid
             OR v_trial.usage_attributed_actor IS DISTINCT FROM
                  v_origin.payload->>'usage_attributed_actor'
             OR v_trial.family_key IS DISTINCT FROM v_origin.payload->>'family_key'
             OR v_trial.lifecycle_authority_id IS DISTINCT FROM
                  v_origin.lifecycle_authority_id
             OR v_trial.submitted_at IS DISTINCT FROM v_origin.submitted_at
             OR v_trial.cancellation_requested_at IS NOT NULL
             OR v_trial.next_attempt_at IS NOT NULL
             OR v_trial.autoscaler_pool_name IS NOT NULL
             OR v_trial.worker_id IS NOT NULL
             OR v_trial.attempt_count IS DISTINCT FROM 0 THEN
            RAISE EXCEPTION 'protected runtime public trial binding drifted'
              USING ERRCODE = '55000';
          END IF;

          SELECT task.id, task.checksum, task.config, task.source,
                 task.source_provenance
            INTO v_task
            FROM public.tasks AS task
           WHERE task.id = v_trial.task_id;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'protected runtime task prerequisite is unavailable'
              USING ERRCODE = '55000';
          END IF;
          v_requires_task_images := (
            (
              v_task.config #> '{{environment,dockerfile}}' IS NOT NULL
              AND v_task.config #> '{{environment,dockerfile}}' <> 'null'::jsonb
            )
            OR EXISTS (
              SELECT 1
                FROM jsonb_array_elements(
                  CASE
                    WHEN jsonb_typeof(v_task.config #> '{{environment,sidecars}}') = 'array'
                    THEN v_task.config #> '{{environment,sidecars}}'
                    ELSE '[]'::jsonb
                  END
                ) AS sidecar
               WHERE sidecar->'dockerfile' IS NOT NULL
                 AND sidecar->'dockerfile' <> 'null'::jsonb
            )
          );
          IF v_requires_task_images THEN
            v_expected_arches := CASE COALESCE(
              v_task.config #>> '{{environment,cpu_arch}}', 'x86_64'
            )
              WHEN 'any' THEN ARRAY['arm64', 'x86_64']::text[]
              WHEN 'arm64' THEN ARRAY['arm64']::text[]
              WHEN 'x86_64' THEN ARRAY['x86_64']::text[]
              ELSE NULL
            END;
            IF v_expected_arches IS NULL THEN
              RAISE EXCEPTION 'protected runtime task architecture is invalid'
                USING ERRCODE = '55000';
            END IF;
          ELSE
            v_expected_arches := ARRAY[]::text[];
          END IF;

          SELECT COALESCE(
                   jsonb_agg(
                     jsonb_build_object(
                       'id', materialization.id,
                       'materialization_key', materialization.materialization_key,
                       'task_id', materialization.task_id,
                       'task_checksum', materialization.task_checksum,
                       'cpu_arch', materialization.cpu_arch,
                       'task_config', materialization.task_config,
                       'task_source', materialization.task_source,
                       'task_source_provenance',
                         materialization.task_source_provenance
                     ) ORDER BY materialization.cpu_arch, materialization.id
                   ),
                   '[]'::jsonb
                 ),
                 count(*)
            INTO v_task_images, v_task_image_count
            FROM public.trial_task_image_materializations AS link
            JOIN public.task_image_materializations AS materialization
              ON materialization.id = link.materialization_id
           WHERE link.trial_id = p_trial_id;
          IF p_require_prerequisites
             AND (
               v_task_image_count IS DISTINCT FROM cardinality(v_expected_arches)
               OR COALESCE(
                    (
                      SELECT array_agg(DISTINCT materialization.cpu_arch::text
                                       ORDER BY materialization.cpu_arch::text)
                        FROM public.trial_task_image_materializations AS link
                        JOIN public.task_image_materializations AS materialization
                          ON materialization.id = link.materialization_id
                       WHERE link.trial_id = p_trial_id
                    ),
                    ARRAY[]::text[]
                  ) IS DISTINCT FROM v_expected_arches
               OR EXISTS (
                 SELECT 1
                   FROM public.trial_task_image_materializations AS link
                   JOIN public.task_image_materializations AS materialization
                     ON materialization.id = link.materialization_id
                  WHERE link.trial_id = p_trial_id
                    AND (
                      materialization.task_id IS DISTINCT FROM v_task.id
                      OR materialization.task_checksum IS DISTINCT FROM
                           regexp_replace(v_task.checksum, '^sha256:', '')
                      OR materialization.task_config IS DISTINCT FROM v_task.config
                      OR materialization.task_source IS DISTINCT FROM v_task.source
                      OR materialization.task_source_provenance IS DISTINCT FROM
                           v_task.source_provenance
                    )
               )
             ) THEN
            RAISE EXCEPTION 'protected runtime task-image prerequisites are incomplete'
              USING ERRCODE = '55000';
          END IF;
          v_task_image_digest := encode(
            sha256(convert_to(v_task_images::text, 'UTF8')), 'hex'
          );

          SELECT COALESCE(
                   jsonb_agg(to_jsonb(plan) ORDER BY plan.id),
                   '[]'::jsonb
                 ),
                 count(*)
            INTO v_model_switches, v_model_switch_count
            FROM public.model_switch_plans AS plan
           WHERE plan.trial_id = p_trial_id;
          v_model_switch_required :=
            (v_origin.public_requires_caps->>'terminus2_model_switch')::boolean;
          IF p_require_prerequisites
             AND (
               (v_model_switch_required AND v_model_switch_count <> 1)
               OR (NOT v_model_switch_required AND v_model_switch_count <> 0)
               OR (
                 v_model_switch_required
                 AND EXISTS (
                   SELECT 1 FROM public.model_switch_plans AS plan
                    WHERE plan.trial_id = p_trial_id
                      AND (
                        v_trial.config #>> '{{multi_model,enabled}}'
                          IS DISTINCT FROM 'true'
                        OR plan.combination_idx IS DISTINCT FROM
                             v_trial.combination_idx
                        OR plan.mix_mode IS DISTINCT FROM COALESCE(
                             v_trial.config #>> '{{multi_model,policy}}',
                             'student_teacher_student'
                           )
                        OR plan.student_model_snapshot IS DISTINCT FROM
                             v_trial.config->'agent_model'
                        OR plan.teacher_model_snapshot IS DISTINCT FROM
                             v_trial.config #> '{{multi_model,secondary_model}}'
                        OR plan.provider_connection_id IS DISTINCT FROM
                             v_trial.provider_connection_id
                        OR plan.prng_version IS DISTINCT FROM 'model_switch_plan.v2'
                        OR (
                          v_trial.config #>> '{{multi_model,mix_seed}}' IS NOT NULL
                          AND plan.seed IS DISTINCT FROM
                                v_trial.config #>> '{{multi_model,mix_seed}}'
                        )
                        OR CASE plan.mix_mode
                             WHEN 'student_teacher_student' THEN
                               plan.k1 IS DISTINCT FROM
                                 (v_trial.config #>> '{{multi_model,switch_episode}}')::integer
                               OR plan.k2 IS DISTINCT FROM
                                 (v_trial.config #>>
                                   '{{multi_model,return_switch_episode}}')::integer
                               OR plan.teacher_episodes IS DISTINCT FROM
                                 (v_trial.config #>>
                                   '{{multi_model,teacher_episodes}}')::integer
                               OR plan.beta IS NOT NULL
                             WHEN 'beta_mixture' THEN
                               plan.k1 IS NOT NULL OR plan.k2 IS NOT NULL
                               OR plan.teacher_episodes IS NOT NULL
                               OR plan.beta IS DISTINCT FROM
                                 (v_trial.config #>> '{{multi_model,beta}}')::double precision
                             ELSE true
                           END
                      )
                 )
               )
             ) THEN
            RAISE EXCEPTION 'protected runtime model-switch prerequisite is incomplete'
              USING ERRCODE = '55000';
          END IF;
          v_model_switch_digest := encode(
            sha256(convert_to(v_model_switches::text, 'UTF8')), 'hex'
          );

          RETURN jsonb_build_object(
            'public_requires_caps_digest', v_origin.public_requires_caps_digest,
            'task_image_prerequisites_digest', v_task_image_digest,
            'model_switch_prerequisite_digest', v_model_switch_digest
          );
        END
        $function$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.publish_protected_runtime_trial_readiness(
          p_agent_incarnation uuid,
          p_trial_id uuid,
          p_protected_attempt_id uuid
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_runtime_role text;
          v_snapshot jsonb;
          v_stored {SCHEMA}.protected_runtime_trial_readiness%ROWTYPE;
          v_ready_at timestamptz;
          v_replayed boolean := false;
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION 'protected runtime readiness requires a SERIALIZABLE transaction'
              USING ERRCODE = '25000';
          END IF;
          SELECT runtime_role_name INTO v_runtime_role
            FROM {SCHEMA}.staging_worker_runtime_authority
           WHERE singleton_id = 1
           FOR KEY SHARE;
          IF v_runtime_role IS NULL OR session_user::text <> v_runtime_role THEN
            RAISE EXCEPTION 'protected runtime readiness caller is not bound'
              USING ERRCODE = '42501';
          END IF;
          IF pg_has_role(session_user, current_user, 'MEMBER') THEN
            RAISE EXCEPTION 'protected runtime readiness unexpectedly holds owner membership'
              USING ERRCODE = '42501';
          END IF;
          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;
          v_snapshot := {SCHEMA}.inspect_protected_runtime_trial_prerequisites(
            p_agent_incarnation, p_trial_id, p_protected_attempt_id, true
          );
          SELECT readiness.* INTO v_stored
            FROM {SCHEMA}.protected_runtime_trial_readiness AS readiness
           WHERE readiness.trial_id = p_trial_id
             AND readiness.protected_attempt_id = p_protected_attempt_id
           FOR KEY SHARE;
          IF FOUND THEN
            IF v_stored.public_requires_caps_digest IS DISTINCT FROM
                 v_snapshot->>'public_requires_caps_digest'
               OR v_stored.task_image_prerequisites_digest IS DISTINCT FROM
                    v_snapshot->>'task_image_prerequisites_digest'
               OR v_stored.model_switch_prerequisite_digest IS DISTINCT FROM
                    v_snapshot->>'model_switch_prerequisite_digest' THEN
              RAISE EXCEPTION 'protected runtime readiness prerequisites drifted'
                USING ERRCODE = '55000';
            END IF;
            v_ready_at := v_stored.ready_at;
            v_replayed := true;
          ELSE
            INSERT INTO {SCHEMA}.protected_runtime_trial_readiness
              (trial_id, protected_attempt_id, public_requires_caps_digest,
               task_image_prerequisites_digest, model_switch_prerequisite_digest)
            VALUES
              (p_trial_id, p_protected_attempt_id,
               v_snapshot->>'public_requires_caps_digest',
               v_snapshot->>'task_image_prerequisites_digest',
               v_snapshot->>'model_switch_prerequisite_digest')
            RETURNING ready_at INTO v_ready_at;
          END IF;
          RETURN jsonb_build_object(
            'schema_version', 1,
            'trial_id', p_trial_id,
            'protected_attempt_id', p_protected_attempt_id,
            'public_requires_caps_digest',
              v_snapshot->>'public_requires_caps_digest',
            'task_image_prerequisites_digest',
              v_snapshot->>'task_image_prerequisites_digest',
            'model_switch_prerequisite_digest',
              v_snapshot->>'model_switch_prerequisite_digest',
            'ready_at', v_ready_at,
            'replayed', v_replayed,
            'executable', false
          );
        END
        $function$
        """
    )


def _install_registration_function() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.register_staging_public_worker(
          p_worker_credential text,
          p_payload jsonb
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_runtime_role text;
          v_credential_sha256 text;
          v_credential_digest bytea;
          v_match_count bigint;
          v_worker record;
          v_physical {SCHEMA}.executable_admission_events%ROWTYPE;
          v_public_worker record;
          v_job record;
          v_predecessor {SCHEMA}.executable_admission_events%ROWTYPE;
          v_supported_work_kinds text[];
          v_environment_id text;
          v_runtime_identity text;
          v_requested_cpus integer;
          v_requested_memory_mib integer;
          v_requested_gpu_tres text;
          v_now timestamptz := statement_timestamp();
        BEGIN
          SELECT runtime_role_name INTO v_runtime_role
            FROM {SCHEMA}.staging_worker_runtime_authority
           WHERE singleton_id = 1
           FOR KEY SHARE;
          IF v_runtime_role IS NULL OR session_user::text <> v_runtime_role THEN
            RAISE EXCEPTION 'staging worker runtime caller is not bound'
              USING ERRCODE = '42501';
          END IF;
          IF pg_has_role(session_user, current_user, 'MEMBER') THEN
            RAISE EXCEPTION 'staging worker runtime unexpectedly holds owner membership'
              USING ERRCODE = '42501';
          END IF;
          IF p_worker_credential IS NULL
             OR octet_length(p_worker_credential) NOT BETWEEN 1 AND 4096
             OR p_worker_credential !~ '^[A-Za-z0-9._~-]+$' THEN
            RAISE EXCEPTION 'staging worker credential is malformed'
              USING ERRCODE = '42501';
          END IF;
          IF jsonb_typeof(p_payload) IS DISTINCT FROM 'object'
             OR (SELECT count(*) FROM jsonb_object_keys(p_payload)) <> 17
             OR NOT p_payload ?& ARRAY[
               'hostname', 'version', 'capabilities', 'supported_work_kinds',
               'capability_snapshot_digest', 'capability_snapshot_json',
               'slurm_gpu_allocation_evidence_json',
               'slurm_gpu_allocation_evidence_digest', 'max_concurrent',
               'pool_name', 'input_cache_capacity_bytes',
               'input_cache_reserved_bytes', 'input_cache_ready_bytes',
               'sandbox_identity', 'candidate_sha', 'slurm_job_id',
               'compose_project'
             ] THEN
            RAISE EXCEPTION 'staging worker registration projection is malformed'
              USING ERRCODE = '22023';
          END IF;
          IF jsonb_typeof(p_payload->'hostname') IS DISTINCT FROM 'string'
             OR length(p_payload->>'hostname') NOT BETWEEN 1 AND 255
             OR jsonb_typeof(p_payload->'version') IS DISTINCT FROM 'string'
             OR length(p_payload->>'version') NOT BETWEEN 1 AND 255
             OR jsonb_typeof(p_payload->'capabilities') IS DISTINCT FROM 'array'
             OR jsonb_array_length(p_payload->'capabilities') < 1
             OR octet_length((p_payload->'capabilities')::text) > 1048576
             OR jsonb_typeof(p_payload->'supported_work_kinds') IS DISTINCT FROM 'array'
             OR p_payload->'supported_work_kinds' NOT IN (
               '["trial"]'::jsonb,
               '["trial", "execution_attempt"]'::jsonb
             )
             OR (
               p_payload->'capability_snapshot_digest' <> 'null'::jsonb
               AND (
                 jsonb_typeof(p_payload->'capability_snapshot_digest')
                   IS DISTINCT FROM 'string'
                 OR p_payload->>'capability_snapshot_digest'
                   !~ '^sha256:[0-9a-f]{{64}}$'
               )
             )
             OR jsonb_typeof(p_payload->'capability_snapshot_json')
                  NOT IN ('object', 'null')
             OR jsonb_typeof(p_payload->'slurm_gpu_allocation_evidence_json')
                  NOT IN ('object', 'null')
             OR (
               p_payload->'slurm_gpu_allocation_evidence_digest' <> 'null'::jsonb
               AND (
                 jsonb_typeof(p_payload->'slurm_gpu_allocation_evidence_digest')
                   IS DISTINCT FROM 'string'
                 OR p_payload->>'slurm_gpu_allocation_evidence_digest'
                   !~ '^sha256:[0-9a-f]{{64}}$'
               )
             )
             OR (
               (p_payload->'slurm_gpu_allocation_evidence_json' = 'null'::jsonb)
               IS DISTINCT FROM
               (p_payload->'slurm_gpu_allocation_evidence_digest' = 'null'::jsonb)
             )
             OR jsonb_typeof(p_payload->'max_concurrent') IS DISTINCT FROM 'number'
             OR p_payload->>'max_concurrent' !~ '^[1-9][0-9]*$'
             OR (p_payload->>'max_concurrent')::numeric > 2147483647
             OR jsonb_typeof(p_payload->'pool_name') IS DISTINCT FROM 'string'
             OR p_payload->>'pool_name' NOT IN (
               'oldlab', 'gb10', 'behavior-cpu-data',
               'behavior-gpu-oldlab', 'behavior-gpu-gb10',
               'terminalgen-generate-gateway', 'terminalgen-package-none',
               'terminalgen-plan-none', 'terminalgen-validate-none'
             )
             OR jsonb_typeof(p_payload->'input_cache_capacity_bytes')
                  IS DISTINCT FROM 'number'
             OR p_payload->>'input_cache_capacity_bytes' !~ '^[0-9]+$'
             OR (p_payload->>'input_cache_capacity_bytes')::numeric > 9223372036854775807
             OR jsonb_typeof(p_payload->'input_cache_reserved_bytes')
                  IS DISTINCT FROM 'number'
             OR p_payload->>'input_cache_reserved_bytes' !~ '^[0-9]+$'
             OR (p_payload->>'input_cache_reserved_bytes')::numeric > 9223372036854775807
             OR jsonb_typeof(p_payload->'input_cache_ready_bytes')
                  IS DISTINCT FROM 'number'
             OR p_payload->>'input_cache_ready_bytes' !~ '^[0-9]+$'
             OR (p_payload->>'input_cache_ready_bytes')::numeric > 9223372036854775807
             OR (p_payload->>'input_cache_reserved_bytes')::bigint >
                  (p_payload->>'input_cache_capacity_bytes')::bigint
             OR (p_payload->>'input_cache_ready_bytes')::bigint >
                  (p_payload->>'input_cache_capacity_bytes')::bigint
             OR jsonb_typeof(p_payload->'sandbox_identity') IS DISTINCT FROM 'string'
             OR p_payload->>'sandbox_identity'
                  !~ '^[a-z0-9][a-z0-9_.-]{{0,127}}$'
             OR jsonb_typeof(p_payload->'candidate_sha') IS DISTINCT FROM 'string'
             OR p_payload->>'candidate_sha' !~ '^[0-9a-f]{{40}}$'
             OR jsonb_typeof(p_payload->'slurm_job_id') IS DISTINCT FROM 'string'
             OR p_payload->>'slurm_job_id' !~ '^[1-9][0-9]*(_[0-9]+)?$'
             OR jsonb_typeof(p_payload->'compose_project') IS DISTINCT FROM 'string'
             OR length(p_payload->>'compose_project') NOT BETWEEN 1 AND 255 THEN
            RAISE EXCEPTION 'staging worker registration projection is invalid'
              USING ERRCODE = '22023';
          END IF;

          v_supported_work_kinds := ARRAY(
            SELECT jsonb_array_elements_text(p_payload->'supported_work_kinds')
          );
          v_credential_digest := sha256(convert_to(p_worker_credential, 'UTF8'));
          v_credential_sha256 := encode(v_credential_digest, 'hex');

          -- Every protected register/drain/release operation locks this row for
          -- update.  Holding a key-share lock through this transaction keeps
          -- the public projection in the same lifecycle epoch.
          PERFORM 1 FROM {SCHEMA}.executable_admission_authority
           WHERE singleton_id = 1
           FOR KEY SHARE;

          SELECT count(*) INTO v_match_count
            FROM {SCHEMA}.executable_admission_events AS worker
            JOIN {SCHEMA}.agent_registrations AS registration
              ON registration.agent_incarnation = worker.agent_incarnation
             AND registration.subject_id = worker.subject_id
             AND registration.subject_incarnation = worker.subject_incarnation
             AND registration.registration_state = 'registered'
            JOIN {SCHEMA}.authority_state AS authority
              ON authority.singleton_id = registration.singleton_id
             AND authority.environment_id = registration.environment_id
             AND authority.subject_id = registration.subject_id
             AND authority.subject_incarnation = registration.subject_incarnation
             AND authority.authority_incarnation = registration.authority_incarnation
             AND authority.reporter_incarnation = registration.reporter_incarnation
             AND authority.authority_mode = registration.authority_mode
             AND authority.allocation_epoch = registration.allocation_epoch
             AND authority.deployment_generation = registration.deployment_generation
             AND authority.configuration_generation = registration.configuration_generation
             AND authority.candidate_digest = registration.candidate_digest
            JOIN {SCHEMA}.executable_claim_state AS claim_state
              ON claim_state.intent_id = worker.intent_id
             AND claim_state.subject_id = worker.subject_id
             AND claim_state.subject_incarnation = worker.subject_incarnation
             AND claim_state.binding = worker.binding
             AND claim_state.draining = false
           WHERE worker.event_kind = 'worker-registered'
             AND worker.worker_credential_revoked = false
             AND worker.worker_credential_sha256 = v_credential_sha256
             AND worker.binding->>'tier_id' = CASE
                   WHEN registration.environment_id LIKE 'dev-%' THEN 'development'
                   ELSE registration.environment_id
                 END
             AND worker.binding->'candidate'->>'algorithm' = 'git-sha1'
             AND registration.candidate_identity_algorithm = 'git-sha1'
             AND registration.candidate_identity = worker.binding->'candidate'->>'identity'
             AND registration.candidate_publication_sha256 =
                   worker.binding->'candidate'->>'publication_sha256'
             AND NOT EXISTS (
               SELECT 1
                 FROM {SCHEMA}.executable_admission_events AS successor
                WHERE successor.intent_id = worker.intent_id
                  AND (
                    successor.event_kind IN (
                      'draining', 'released', 'withdrawn', 'prepared-revoked'
                    )
                    OR (
                      successor.event_kind = 'worker-registered'
                      AND successor.protected_registration_epoch >
                            worker.protected_registration_epoch
                    )
                  )
             );
          IF v_match_count <> 1 THEN
            RAISE EXCEPTION 'staging worker credential is not current'
              USING ERRCODE = '42501';
          END IF;

          SELECT worker.*, registration.environment_id AS matched_environment_id
            INTO v_worker
            FROM {SCHEMA}.executable_admission_events AS worker
            JOIN {SCHEMA}.agent_registrations AS registration
              ON registration.agent_incarnation = worker.agent_incarnation
             AND registration.subject_id = worker.subject_id
             AND registration.subject_incarnation = worker.subject_incarnation
             AND registration.registration_state = 'registered'
            JOIN {SCHEMA}.authority_state AS authority
              ON authority.singleton_id = registration.singleton_id
             AND authority.environment_id = registration.environment_id
             AND authority.subject_id = registration.subject_id
             AND authority.subject_incarnation = registration.subject_incarnation
             AND authority.authority_incarnation = registration.authority_incarnation
             AND authority.reporter_incarnation = registration.reporter_incarnation
             AND authority.authority_mode = registration.authority_mode
             AND authority.allocation_epoch = registration.allocation_epoch
             AND authority.deployment_generation = registration.deployment_generation
             AND authority.configuration_generation = registration.configuration_generation
             AND authority.candidate_digest = registration.candidate_digest
            JOIN {SCHEMA}.executable_claim_state AS claim_state
              ON claim_state.intent_id = worker.intent_id
             AND claim_state.subject_id = worker.subject_id
             AND claim_state.subject_incarnation = worker.subject_incarnation
             AND claim_state.binding = worker.binding
             AND claim_state.draining = false
           WHERE worker.event_kind = 'worker-registered'
             AND worker.worker_credential_revoked = false
             AND worker.worker_credential_sha256 = v_credential_sha256
             AND worker.binding->>'tier_id' = CASE
                   WHEN registration.environment_id LIKE 'dev-%' THEN 'development'
                   ELSE registration.environment_id
                 END
             AND worker.binding->'candidate'->>'algorithm' = 'git-sha1'
             AND registration.candidate_identity_algorithm = 'git-sha1'
             AND registration.candidate_identity = worker.binding->'candidate'->>'identity'
             AND registration.candidate_publication_sha256 =
                   worker.binding->'candidate'->>'publication_sha256'
             AND NOT EXISTS (
               SELECT 1
                 FROM {SCHEMA}.executable_admission_events AS successor
                WHERE successor.intent_id = worker.intent_id
                  AND (
                    successor.event_kind IN (
                      'draining', 'released', 'withdrawn', 'prepared-revoked'
                    )
                    OR (
                      successor.event_kind = 'worker-registered'
                      AND successor.protected_registration_epoch >
                            worker.protected_registration_epoch
                    )
                  )
             )
           FOR KEY SHARE OF worker, registration, authority, claim_state;

          v_environment_id := v_worker.matched_environment_id;
          v_runtime_identity := CASE
            WHEN v_environment_id LIKE 'dev-%' THEN 'loom-' || v_environment_id
            ELSE v_environment_id
          END;
          v_requested_cpus := CASE
            WHEN (v_worker.binding->'resources'->>'cpu_millicores')::bigint > 0
             AND (v_worker.binding->'resources'->>'cpu_millicores')::bigint % 1000 = 0
            THEN (v_worker.binding->'resources'->>'cpu_millicores')::bigint / 1000
            ELSE NULL
          END;
          v_requested_memory_mib := CASE
            WHEN (v_worker.binding->'resources'->>'memory_bytes')::bigint > 0
             AND (v_worker.binding->'resources'->>'memory_bytes')::bigint % 1048576 = 0
            THEN (v_worker.binding->'resources'->>'memory_bytes')::bigint / 1048576
            ELSE NULL
          END;

          SELECT physical.* INTO v_physical
            FROM {SCHEMA}.executable_admission_events AS physical
           WHERE physical.intent_id = v_worker.intent_id
             AND physical.event_kind = 'physical-bound'
             AND physical.subject_id = v_worker.subject_id
             AND physical.subject_incarnation = v_worker.subject_incarnation
             AND physical.binding = v_worker.binding
             AND physical.bootstrap_registration_epoch =
                   v_worker.bootstrap_registration_epoch
             AND physical.physical_job_id = v_worker.physical_job_id
           FOR KEY SHARE;
          IF NOT FOUND
             OR jsonb_array_length(v_worker.binding->'node_ids') <> 1
             OR v_worker.binding->'node_ids'->>0 IS DISTINCT FROM p_payload->>'hostname'
             OR v_worker.binding->'candidate'->>'identity'
                  IS DISTINCT FROM p_payload->>'candidate_sha'
             OR (v_worker.binding->>'concurrency_slots')::bigint IS DISTINCT FROM
                  (p_payload->>'max_concurrent')::bigint
             OR v_worker.physical_job_id IS DISTINCT FROM p_payload->>'slurm_job_id'
             OR p_payload->>'sandbox_identity' IS DISTINCT FROM v_runtime_identity
             OR NOT (
               (
                 p_payload->'capability_snapshot_json' = 'null'::jsonb
                 AND p_payload->>'pool_name' = v_worker.binding->>'pool_id'
               )
               OR (
                 (v_worker.binding->'resources'->>'gpu_count')::integer = 0
                 AND v_worker.binding->>'pool_id' = 'oldlab'
                 AND p_payload->>'pool_name' IN (
                   'behavior-cpu-data', 'terminalgen-generate-gateway',
                   'terminalgen-package-none', 'terminalgen-plan-none',
                   'terminalgen-validate-none'
                 )
               )
               OR (
                 (v_worker.binding->'resources'->>'gpu_count')::integer > 0
                 AND p_payload->>'pool_name' =
                       'behavior-gpu-' || (v_worker.binding->>'pool_id')
               )
             )
             OR (
               ((v_worker.binding->'resources'->>'gpu_count')::integer = 0)
               IS DISTINCT FROM
               (p_payload->'slurm_gpu_allocation_evidence_json' = 'null'::jsonb)
             )
             OR (
               (v_worker.binding->'resources'->>'gpu_count')::integer > 0
               AND (
                 p_payload->'slurm_gpu_allocation_evidence_json'->>'slurm_cluster_id'
                   IS DISTINCT FROM v_worker.binding->>'pool_id'
                 OR p_payload->'slurm_gpu_allocation_evidence_json'->>'job_id'
                   IS DISTINCT FROM v_worker.physical_job_id
                 OR p_payload->'slurm_gpu_allocation_evidence_json'->>'node_name'
                   IS DISTINCT FROM v_worker.binding->'node_ids'->>0
                 OR p_payload->'slurm_gpu_allocation_evidence_json'->>'allocation_id'
                   IS DISTINCT FROM
                     v_worker.binding->>'pool_id' || ':' || v_worker.physical_job_id
                 OR jsonb_array_length(
                      p_payload->'slurm_gpu_allocation_evidence_json'->'allocated_device_ids'
                    ) IS DISTINCT FROM
                      (v_worker.binding->'resources'->>'gpu_count')::integer
                 OR jsonb_array_length(
                      p_payload->'slurm_gpu_allocation_evidence_json'->'device_uuids'
                    ) IS DISTINCT FROM
                      (v_worker.binding->'resources'->>'gpu_count')::integer
                 OR CASE v_worker.binding->>'pool_id'
                      WHEN 'oldlab' THEN
                        p_payload->'slurm_gpu_allocation_evidence_json'->>'partition'
                          IS DISTINCT FROM 'all'
                        OR p_payload->'slurm_gpu_allocation_evidence_json'->>'gpu_tres'
                          IS DISTINCT FROM 'gpu:rtx5080:2'
                        OR p_payload->'slurm_gpu_allocation_evidence_json'->>'variant_id'
                          IS DISTINCT FROM 'oldlab-rtx5080-2gpu'
                        OR (v_worker.binding->'resources'->>'gpu_count')::integer <> 2
                      WHEN 'gb10' THEN
                        p_payload->'slurm_gpu_allocation_evidence_json'->>'partition'
                          IS DISTINCT FROM 'gb10'
                        OR p_payload->'slurm_gpu_allocation_evidence_json'->>'gpu_tres'
                          IS DISTINCT FROM 'gpu:gb10:1'
                        OR p_payload->'slurm_gpu_allocation_evidence_json'->>'variant_id'
                          IS DISTINCT FROM 'gb10-shared-1gpu'
                        OR (v_worker.binding->'resources'->>'gpu_count')::integer <> 1
                      ELSE true
                    END
                 OR p_payload->>'slurm_gpu_allocation_evidence_digest'
                   IS DISTINCT FROM 'sha256:' || encode(
                     sha256(
                       convert_to(
                         {SCHEMA}.canonical_executable_publication_payload(
                           p_payload->'slurm_gpu_allocation_evidence_json'
                         ) || E'\n',
                         'UTF8'
                       )
                     ),
                     'hex'
                   )
               )
             ) THEN
            RAISE EXCEPTION 'staging worker projection differs from protected binding'
              USING ERRCODE = '55000';
          END IF;
          v_requested_gpu_tres := CASE
            WHEN (v_worker.binding->'resources'->>'gpu_count')::integer = 0 THEN NULL
            ELSE p_payload->'slurm_gpu_allocation_evidence_json'->>'gpu_tres'
          END;

          -- The protected physical binding is the sole provenance for this
          -- row.  A legacy autoscaler-created row may be adopted only when it
          -- is byte-for-byte equivalent under the exact lookup below.
          INSERT INTO public.slurm_worker_jobs (
            id, slurm_cluster_id, environment, pool_name, nodelist,
            requested_cpus, requested_memory_mib, requested_gpu_tres,
            requested_gpus, requested_concurrency, sandbox_identity,
            candidate_sha, compose_project, job_id, slurm_state, state,
            submitted_at, started_at, last_reconciled_at
          ) VALUES (
            v_worker.intent_id,
            v_worker.binding->>'pool_id',
            v_runtime_identity,
            p_payload->>'pool_name',
            v_worker.binding->'node_ids'->>0,
            v_requested_cpus,
            v_requested_memory_mib,
            v_requested_gpu_tres,
            (v_worker.binding->'resources'->>'gpu_count')::integer,
            (v_worker.binding->>'concurrency_slots')::integer,
            p_payload->>'sandbox_identity',
            v_worker.binding->'candidate'->>'identity',
            p_payload->>'compose_project',
            v_worker.physical_job_id,
            'RUNNING', 'running', v_now, v_now, v_now
          ) ON CONFLICT (slurm_cluster_id, job_id) WHERE job_id IS NOT NULL
            DO NOTHING;

          SELECT id, slurm_cluster_id, environment, pool_name, nodelist,
                 requested_cpus, requested_memory_mib, requested_pids,
                 requested_gpu_tres, requested_gpus, requested_concurrency,
                 sandbox_identity, candidate_sha, compose_project, job_id,
                 slurm_state, state, worker_id
            INTO v_job
            FROM public.slurm_worker_jobs
           WHERE slurm_cluster_id = v_worker.binding->>'pool_id'
             AND job_id = v_worker.physical_job_id
           FOR UPDATE;
          IF NOT FOUND
             OR v_job.environment IS DISTINCT FROM v_runtime_identity
             OR v_job.pool_name IS DISTINCT FROM p_payload->>'pool_name'
             OR v_job.nodelist IS DISTINCT FROM v_worker.binding->'node_ids'->>0
             OR v_job.requested_cpus IS DISTINCT FROM v_requested_cpus
             OR v_job.requested_memory_mib IS DISTINCT FROM v_requested_memory_mib
             OR v_job.requested_pids IS NOT NULL
             OR v_job.requested_gpu_tres IS DISTINCT FROM v_requested_gpu_tres
             OR v_job.requested_gpus IS DISTINCT FROM
                  (v_worker.binding->'resources'->>'gpu_count')::integer
             OR v_job.requested_concurrency IS DISTINCT FROM
                  (v_worker.binding->>'concurrency_slots')::integer
             OR v_job.sandbox_identity IS DISTINCT FROM v_runtime_identity
             OR v_job.candidate_sha IS DISTINCT FROM
                  v_worker.binding->'candidate'->>'identity'
             OR v_job.compose_project IS DISTINCT FROM p_payload->>'compose_project'
             OR v_job.slurm_state IS DISTINCT FROM 'RUNNING'
             OR v_job.state IS DISTINCT FROM 'running' THEN
            RAISE EXCEPTION 'staging worker has no exact active Slurm job'
              USING ERRCODE = '55000';
          END IF;
          IF v_job.worker_id IS NOT NULL
             AND v_job.worker_id IS DISTINCT FROM v_worker.worker_id THEN
            SELECT predecessor.* INTO v_predecessor
              FROM {SCHEMA}.executable_admission_events AS predecessor
             WHERE predecessor.intent_id = v_worker.intent_id
               AND predecessor.event_kind = 'worker-registered'
               AND predecessor.worker_id = v_job.worker_id
               AND predecessor.worker_incarnation =
                     v_worker.predecessor_worker_incarnation
               AND predecessor.protected_registration_epoch <
                     v_worker.protected_registration_epoch
             FOR KEY SHARE;
            IF NOT FOUND THEN
              RAISE EXCEPTION 'staging Slurm job is linked to another worker'
                USING ERRCODE = '55000';
            END IF;
          END IF;

          INSERT INTO public.workers (
            id, hostname, version, capabilities, supported_work_kinds,
            capability_snapshot_digest, capability_snapshot_json,
            slurm_gpu_allocation_evidence_json,
            slurm_gpu_allocation_evidence_digest, auth_token_hash,
            max_concurrent, pool_name, input_cache_capacity_bytes,
            input_cache_reserved_bytes, input_cache_ready_bytes,
            registered_at, last_seen_at, status
          ) VALUES (
            v_worker.worker_id,
            p_payload->>'hostname',
            p_payload->>'version',
            p_payload->'capabilities',
            v_supported_work_kinds,
            NULLIF(p_payload->>'capability_snapshot_digest', ''),
            CASE WHEN p_payload->'capability_snapshot_json' = 'null'::jsonb
                 THEN NULL ELSE p_payload->'capability_snapshot_json' END,
            CASE WHEN p_payload->'slurm_gpu_allocation_evidence_json' = 'null'::jsonb
                 THEN NULL ELSE p_payload->'slurm_gpu_allocation_evidence_json' END,
            NULLIF(p_payload->>'slurm_gpu_allocation_evidence_digest', ''),
            v_credential_digest,
            (p_payload->>'max_concurrent')::integer,
            p_payload->>'pool_name',
            (p_payload->>'input_cache_capacity_bytes')::bigint,
            (p_payload->>'input_cache_reserved_bytes')::bigint,
            (p_payload->>'input_cache_ready_bytes')::bigint,
            v_now, v_now, 'active'
          ) ON CONFLICT (id) DO NOTHING;

          SELECT id, hostname, version, capabilities, supported_work_kinds,
                 capability_snapshot_digest, capability_snapshot_json,
                 slurm_gpu_allocation_evidence_json,
                 slurm_gpu_allocation_evidence_digest, auth_token_hash,
                 max_concurrent, pool_name, input_cache_capacity_bytes,
                 input_cache_reserved_bytes, input_cache_ready_bytes
            INTO v_public_worker
            FROM public.workers
           WHERE id = v_worker.worker_id;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'staging worker projection disappeared concurrently'
              USING ERRCODE = '40001';
          END IF;
          IF v_public_worker.hostname IS DISTINCT FROM p_payload->>'hostname'
               OR v_public_worker.version IS DISTINCT FROM p_payload->>'version'
               OR v_public_worker.capabilities IS DISTINCT FROM p_payload->'capabilities'
               OR v_public_worker.supported_work_kinds
                    IS DISTINCT FROM v_supported_work_kinds
               OR v_public_worker.capability_snapshot_digest IS DISTINCT FROM
                    NULLIF(p_payload->>'capability_snapshot_digest', '')
               OR v_public_worker.capability_snapshot_json IS DISTINCT FROM
                    (CASE WHEN p_payload->'capability_snapshot_json' = 'null'::jsonb
                          THEN NULL ELSE p_payload->'capability_snapshot_json' END)
               OR v_public_worker.slurm_gpu_allocation_evidence_json IS DISTINCT FROM
                    (CASE
                      WHEN p_payload->'slurm_gpu_allocation_evidence_json' = 'null'::jsonb
                      THEN NULL
                      ELSE p_payload->'slurm_gpu_allocation_evidence_json'
                    END)
               OR v_public_worker.slurm_gpu_allocation_evidence_digest IS DISTINCT FROM
                    NULLIF(p_payload->>'slurm_gpu_allocation_evidence_digest', '')
               OR v_public_worker.auth_token_hash IS DISTINCT FROM v_credential_digest
               OR v_public_worker.max_concurrent IS DISTINCT FROM
                    (p_payload->>'max_concurrent')::integer
               OR v_public_worker.pool_name IS DISTINCT FROM p_payload->>'pool_name'
               OR v_public_worker.input_cache_capacity_bytes IS DISTINCT FROM
                    (p_payload->>'input_cache_capacity_bytes')::bigint
               OR v_public_worker.input_cache_reserved_bytes IS DISTINCT FROM
                    (p_payload->>'input_cache_reserved_bytes')::bigint
               OR v_public_worker.input_cache_ready_bytes IS DISTINCT FROM
                    (p_payload->>'input_cache_ready_bytes')::bigint THEN
            RAISE EXCEPTION 'conflicting staging worker projection replay'
              USING ERRCODE = '55000';
          END IF;

          UPDATE public.slurm_worker_jobs
             SET worker_id = v_worker.worker_id
           WHERE id = v_job.id
             AND (worker_id IS NULL OR worker_id = v_job.worker_id);
          IF NOT FOUND THEN
            RAISE EXCEPTION 'staging Slurm worker linkage changed concurrently'
              USING ERRCODE = '40001';
          END IF;

          RETURN jsonb_build_object(
            'worker_id', v_worker.worker_id,
            'worker_incarnation', v_worker.worker_incarnation,
            'intent_id', v_worker.intent_id,
            'capability_snapshot_digest',
              NULLIF(p_payload->>'capability_snapshot_digest', ''),
            'supported_work_kinds', p_payload->'supported_work_kinds',
            'input_cache_capacity_bytes',
              (p_payload->>'input_cache_capacity_bytes')::bigint,
            'input_cache_reserved_bytes',
              (p_payload->>'input_cache_reserved_bytes')::bigint,
            'input_cache_ready_bytes',
              (p_payload->>'input_cache_ready_bytes')::bigint,
            'slurm_gpu_allocation_evidence_digest',
              NULLIF(p_payload->>'slurm_gpu_allocation_evidence_digest', ''),
            'heartbeat_interval_sec', 5,
            'claim_poll_interval_sec', 1.0,
            'drain_timeout_sec', 600
          );
        END
        $function$
        """
    )


def _install_session_assertion_function() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.assert_staging_worker_session(
          p_worker_id uuid,
          p_worker_credential text
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_runtime_role text;
          v_credential_sha256 text;
          v_worker record;
          v_public_worker record;
          v_job record;
          v_environment_id text;
          v_runtime_identity text;
          v_requested_cpus integer;
          v_requested_memory_mib integer;
          v_requested_gpu_tres text;
        BEGIN
          SELECT runtime_role_name INTO v_runtime_role
            FROM {SCHEMA}.staging_worker_runtime_authority
           WHERE singleton_id = 1
           FOR KEY SHARE;
          IF v_runtime_role IS NULL OR session_user::text <> v_runtime_role THEN
            RAISE EXCEPTION 'staging worker runtime caller is not bound'
              USING ERRCODE = '42501';
          END IF;
          IF pg_has_role(session_user, current_user, 'MEMBER') THEN
            RAISE EXCEPTION 'staging worker runtime unexpectedly holds owner membership'
              USING ERRCODE = '42501';
          END IF;
          IF p_worker_id IS NULL
             OR p_worker_credential IS NULL
             OR octet_length(p_worker_credential) NOT BETWEEN 1 AND 4096
             OR p_worker_credential !~ '^[A-Za-z0-9._~-]+$' THEN
            RAISE EXCEPTION 'staging worker session identity is malformed'
              USING ERRCODE = '42501';
          END IF;
          v_credential_sha256 := encode(
            sha256(convert_to(p_worker_credential, 'UTF8')),
            'hex'
          );

          -- Keep drain/release mutually exclusive with the caller's public
          -- mutation for the lifetime of its transaction.
          PERFORM 1 FROM {SCHEMA}.executable_admission_authority
           WHERE singleton_id = 1
           FOR KEY SHARE;
          SELECT worker.*, registration.environment_id AS matched_environment_id
            INTO v_worker
            FROM {SCHEMA}.executable_admission_events AS worker
            JOIN {SCHEMA}.agent_registrations AS registration
              ON registration.agent_incarnation = worker.agent_incarnation
             AND registration.subject_id = worker.subject_id
             AND registration.subject_incarnation = worker.subject_incarnation
             AND registration.registration_state = 'registered'
            JOIN {SCHEMA}.authority_state AS authority
              ON authority.singleton_id = registration.singleton_id
             AND authority.environment_id = registration.environment_id
             AND authority.subject_id = registration.subject_id
             AND authority.subject_incarnation = registration.subject_incarnation
             AND authority.authority_incarnation = registration.authority_incarnation
             AND authority.reporter_incarnation = registration.reporter_incarnation
             AND authority.authority_mode = registration.authority_mode
             AND authority.allocation_epoch = registration.allocation_epoch
             AND authority.deployment_generation = registration.deployment_generation
             AND authority.configuration_generation = registration.configuration_generation
             AND authority.candidate_digest = registration.candidate_digest
            JOIN {SCHEMA}.executable_claim_state AS claim_state
              ON claim_state.intent_id = worker.intent_id
             AND claim_state.subject_id = worker.subject_id
             AND claim_state.subject_incarnation = worker.subject_incarnation
             AND claim_state.binding = worker.binding
             AND claim_state.draining = false
           WHERE worker.event_kind = 'worker-registered'
             AND worker.worker_id = p_worker_id
             AND worker.worker_credential_sha256 = v_credential_sha256
             AND worker.worker_credential_revoked = false
             AND worker.binding->>'tier_id' = CASE
                   WHEN registration.environment_id LIKE 'dev-%' THEN 'development'
                   ELSE registration.environment_id
                 END
             AND worker.binding->'candidate'->>'algorithm' = 'git-sha1'
             AND registration.candidate_identity_algorithm = 'git-sha1'
             AND registration.candidate_identity = worker.binding->'candidate'->>'identity'
             AND registration.candidate_publication_sha256 =
                   worker.binding->'candidate'->>'publication_sha256'
             AND NOT EXISTS (
               SELECT 1
                 FROM {SCHEMA}.executable_admission_events AS successor
                WHERE successor.intent_id = worker.intent_id
                  AND (
                    successor.event_kind IN (
                      'draining', 'released', 'withdrawn', 'prepared-revoked'
                    )
                    OR (
                      successor.event_kind = 'worker-registered'
                      AND successor.protected_registration_epoch >
                            worker.protected_registration_epoch
                    )
                  )
             )
           FOR KEY SHARE OF worker, registration, authority, claim_state;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'staging worker session is not current'
              USING ERRCODE = '42501';
          END IF;
          v_environment_id := v_worker.matched_environment_id;
          v_runtime_identity := CASE
            WHEN v_environment_id LIKE 'dev-%' THEN 'loom-' || v_environment_id
            ELSE v_environment_id
          END;
          v_requested_cpus := CASE
            WHEN (v_worker.binding->'resources'->>'cpu_millicores')::bigint > 0
             AND (v_worker.binding->'resources'->>'cpu_millicores')::bigint % 1000 = 0
            THEN (v_worker.binding->'resources'->>'cpu_millicores')::bigint / 1000
            ELSE NULL
          END;
          v_requested_memory_mib := CASE
            WHEN (v_worker.binding->'resources'->>'memory_bytes')::bigint > 0
             AND (v_worker.binding->'resources'->>'memory_bytes')::bigint % 1048576 = 0
            THEN (v_worker.binding->'resources'->>'memory_bytes')::bigint / 1048576
            ELSE NULL
          END;

          SELECT id, hostname, max_concurrent, pool_name, auth_token_hash,
                 capability_snapshot_json, slurm_gpu_allocation_evidence_json
            INTO v_public_worker
            FROM public.workers
           WHERE id = v_worker.worker_id
             AND hostname = v_worker.binding->'node_ids'->>0
             AND max_concurrent = (v_worker.binding->>'concurrency_slots')::bigint
             AND auth_token_hash = sha256(convert_to(p_worker_credential, 'UTF8'))
             AND (
               (
                 capability_snapshot_json IS NULL
                 AND pool_name = v_worker.binding->>'pool_id'
               )
               OR (
                 (v_worker.binding->'resources'->>'gpu_count')::integer = 0
                 AND v_worker.binding->>'pool_id' = 'oldlab'
                 AND slurm_gpu_allocation_evidence_json IS NULL
                 AND pool_name IN (
                   'behavior-cpu-data', 'terminalgen-generate-gateway',
                   'terminalgen-package-none', 'terminalgen-plan-none',
                   'terminalgen-validate-none'
                 )
               )
               OR (
                 (v_worker.binding->'resources'->>'gpu_count')::integer > 0
                 AND pool_name = 'behavior-gpu-' || (v_worker.binding->>'pool_id')
                 AND slurm_gpu_allocation_evidence_json->>'slurm_cluster_id' =
                       v_worker.binding->>'pool_id'
                 AND slurm_gpu_allocation_evidence_json->>'job_id' =
                       v_worker.physical_job_id
                 AND slurm_gpu_allocation_evidence_json->>'node_name' =
                       v_worker.binding->'node_ids'->>0
                 AND jsonb_array_length(
                       slurm_gpu_allocation_evidence_json->'allocated_device_ids'
                     ) = (v_worker.binding->'resources'->>'gpu_count')::integer
                 AND jsonb_array_length(
                       slurm_gpu_allocation_evidence_json->'device_uuids'
                     ) = (v_worker.binding->'resources'->>'gpu_count')::integer
               )
             );
          IF NOT FOUND THEN
            RAISE EXCEPTION 'staging worker public session differs from protected binding'
              USING ERRCODE = '55000';
          END IF;
          v_requested_gpu_tres := CASE
            WHEN (v_worker.binding->'resources'->>'gpu_count')::integer = 0 THEN NULL
            ELSE v_public_worker.slurm_gpu_allocation_evidence_json->>'gpu_tres'
          END;

          SELECT id, slurm_cluster_id, environment, pool_name, nodelist,
                 requested_cpus, requested_memory_mib, requested_pids,
                 requested_gpu_tres, requested_gpus, requested_concurrency,
                 sandbox_identity, candidate_sha, compose_project, job_id,
                 slurm_state, state, worker_id
            INTO v_job
            FROM public.slurm_worker_jobs
           WHERE slurm_cluster_id = v_worker.binding->>'pool_id'
             AND job_id = v_worker.physical_job_id
           FOR KEY SHARE;
          IF NOT FOUND
             OR v_job.environment IS DISTINCT FROM v_runtime_identity
             OR v_job.pool_name IS DISTINCT FROM v_public_worker.pool_name
             OR v_job.nodelist IS DISTINCT FROM v_worker.binding->'node_ids'->>0
             OR v_job.requested_cpus IS DISTINCT FROM v_requested_cpus
             OR v_job.requested_memory_mib IS DISTINCT FROM v_requested_memory_mib
             OR v_job.requested_pids IS NOT NULL
             OR v_job.requested_gpu_tres IS DISTINCT FROM v_requested_gpu_tres
             OR v_job.requested_gpus IS DISTINCT FROM
                  (v_worker.binding->'resources'->>'gpu_count')::integer
             OR v_job.requested_concurrency IS DISTINCT FROM
                  (v_worker.binding->>'concurrency_slots')::integer
             OR v_job.sandbox_identity IS DISTINCT FROM v_runtime_identity
             OR v_job.candidate_sha IS DISTINCT FROM
                  v_worker.binding->'candidate'->>'identity'
             OR v_job.slurm_state IS DISTINCT FROM 'RUNNING'
             OR v_job.state IS DISTINCT FROM 'running'
             OR v_job.worker_id IS DISTINCT FROM v_worker.worker_id THEN
            RAISE EXCEPTION 'staging worker has no exact active public Slurm linkage'
              USING ERRCODE = '55000';
          END IF;

          RETURN jsonb_build_object(
            'worker_id', v_worker.worker_id,
            'worker_incarnation', v_worker.worker_incarnation,
            'intent_id', v_worker.intent_id,
            'pool_name', v_public_worker.pool_name,
            'hostname', v_worker.binding->'node_ids'->>0,
            'candidate_sha', v_worker.binding->'candidate'->>'identity',
            'slurm_job_id', v_worker.physical_job_id,
            'credential_sha256', v_credential_sha256
          );
        END
        $function$
        """
    )


def _install_assigned_trial_claim_function() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.claim_staging_assigned_trial(
          p_worker_id uuid,
          p_worker_credential text,
          p_claim_request jsonb
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_session jsonb;
          v_worker record;
          v_candidate record;
          v_snapshot jsonb;
          v_assignment_payload jsonb;
          v_claim_state {SCHEMA}.executable_claim_state%ROWTYPE;
          v_operation_id uuid := gen_random_uuid();
          v_claim_payload jsonb;
          v_claim_canonical text;
          v_claim_digest text;
          v_claim_receipt jsonb;
          v_reservation_id uuid;
          v_admission_policy record;
          v_family_state_uri text;
          v_family_run_spec jsonb;
          v_task_image jsonb;
          v_model_switch jsonb;
          v_claimed record;
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION 'protected staging claim requires a SERIALIZABLE transaction'
              USING ERRCODE = '25000';
          END IF;
          IF jsonb_typeof(p_claim_request) IS DISTINCT FROM 'object'
             OR octet_length(p_claim_request::text) > 1048576
             OR jsonb_typeof(p_claim_request->'schema_version') IS DISTINCT FROM 'number'
             OR (p_claim_request->>'schema_version')::integer IS DISTINCT FROM 1
             OR jsonb_typeof(p_claim_request->'protocol') IS DISTINCT FROM 'string'
             OR p_claim_request->>'protocol' NOT IN ('trial', 'work')
             OR jsonb_typeof(p_claim_request->'worker_id') IS DISTINCT FROM 'string'
             OR (p_claim_request->>'worker_id' ~
                  '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
                  IS DISTINCT FROM true
             OR (p_claim_request->>'worker_id')::uuid IS DISTINCT FROM p_worker_id
             OR (
               p_claim_request->>'protocol' = 'trial'
               AND (
                 p_claim_request - ARRAY[
                   'schema_version', 'protocol', 'worker_id', 'capabilities'
                 ] <> '{{}}'::jsonb
                 OR (SELECT count(*) FROM jsonb_object_keys(p_claim_request)) <> 4
                 OR jsonb_typeof(p_claim_request->'capabilities') IS DISTINCT FROM 'array'
                 OR jsonb_array_length(p_claim_request->'capabilities') < 1
               )
             )
             OR (
               p_claim_request->>'protocol' = 'work'
               AND (
                 p_claim_request - ARRAY[
                   'schema_version', 'protocol', 'worker_id',
                   'capability_snapshot_digest', 'supported_work_kinds', 'free_slots'
                 ] <> '{{}}'::jsonb
                 OR (SELECT count(*) FROM jsonb_object_keys(p_claim_request)) <> 6
                 OR jsonb_typeof(p_claim_request->'capability_snapshot_digest')
                      IS DISTINCT FROM 'string'
                 OR p_claim_request->>'capability_snapshot_digest'
                      !~ '^sha256:[0-9a-f]{{64}}$'
                 OR p_claim_request->'supported_work_kinds'
                      IS DISTINCT FROM '["trial", "execution_attempt"]'::jsonb
                 OR jsonb_typeof(p_claim_request->'free_slots') IS DISTINCT FROM 'number'
                 OR p_claim_request->>'free_slots' !~ '^[1-9][0-9]*$'
                 OR (p_claim_request->>'free_slots')::numeric > 2147483647
               )
             ) THEN
            RAISE EXCEPTION 'protected staging claim request is malformed'
              USING ERRCODE = '22023';
          END IF;

          -- This assertion authenticates the raw worker credential and pins
          -- drain/release plus the exact protected-to-public worker binding
          -- for the lifetime of this transaction.
          v_session := {SCHEMA}.assert_staging_worker_session(
            p_worker_id, p_worker_credential
          );
          SELECT event.subject_id, event.subject_incarnation,
                 event.agent_incarnation, event.intent_id,
                 event.worker_incarnation, event.binding,
                 worker.capabilities, worker.supported_work_kinds,
                 worker.capability_snapshot_digest, worker.capability_snapshot_json,
                 worker.max_concurrent,
                 worker.pool_name, worker.status, worker.drain_state,
                 job.environment AS admission_environment,
                 policy.actuator_config->>'routing_region' AS admission_region,
                 (
                   (SELECT count(*) FROM public.trials AS active_trial
                     WHERE active_trial.worker_id = worker.id
                       AND active_trial.state IN ('claimed', 'running'))
                   +
                   (SELECT count(*) FROM public.execution_attempts AS active_attempt
                     WHERE active_attempt.worker_id = worker.id
                       AND active_attempt.state IN ('claimed', 'running'))
                 ) AS active_count
            INTO v_worker
            FROM {SCHEMA}.executable_admission_events AS event
            JOIN public.workers AS worker ON worker.id = event.worker_id
            JOIN public.slurm_worker_jobs AS job
              ON job.worker_id = worker.id
             AND job.slurm_cluster_id = event.binding->>'pool_id'
             AND job.job_id = event.physical_job_id
             AND job.state = 'running'
            LEFT JOIN LATERAL (
              SELECT candidate.actuator_config
                FROM public.worker_pool_autoscaler_policies AS candidate
               WHERE candidate.pool_name = worker.pool_name
               ORDER BY candidate.enabled DESC, candidate.updated_at DESC, candidate.id
               LIMIT 1
            ) AS policy ON true
           WHERE event.event_kind = 'worker-registered'
             AND event.worker_id = p_worker_id
             AND event.worker_incarnation = (v_session->>'worker_incarnation')::uuid
             AND event.intent_id = (v_session->>'intent_id')::uuid
           FOR UPDATE OF worker
           FOR KEY SHARE OF event, job;
          IF NOT FOUND
             OR v_worker.status IS DISTINCT FROM 'active'
             OR v_worker.drain_state IS DISTINCT FROM 'active'
             OR v_worker.active_count >= v_worker.max_concurrent
             OR NOT ('trial' = ANY(v_worker.supported_work_kinds))
             OR (
               p_claim_request->>'protocol' = 'trial'
               AND p_claim_request->'capabilities' IS DISTINCT FROM v_worker.capabilities
             )
             OR (
               p_claim_request->>'protocol' = 'work'
               AND (
                 v_worker.capability_snapshot_digest IS DISTINCT FROM
                   p_claim_request->>'capability_snapshot_digest'
                 OR to_jsonb(v_worker.supported_work_kinds) IS DISTINCT FROM
                      p_claim_request->'supported_work_kinds'
                 OR (p_claim_request->>'free_slots')::integer > v_worker.max_concurrent
               )
             )
             OR EXISTS (
               SELECT 1
                 FROM public.worker_pool_autoscaler_policies AS drain_policy
                WHERE drain_policy.pool_name = v_worker.pool_name
                  AND drain_policy.actuator = 'slurm'
                  AND drain_policy.prod_pressure_state->>'state' = 'draining'
             )
             OR EXISTS (
               SELECT 1
                 FROM public.pipeline_acceptance_preflight_prerequisites AS fence
                WHERE fence.worker_id = p_worker_id
                  AND fence.fence_state = 'active'
             )
             OR COALESCE(
                  v_worker.capability_snapshot_json->'container_runtime_features',
                  '[]'::jsonb
                ) ? 'loom-stage1-smoke-worker-v1' THEN
            RETURN NULL;
          END IF;

          -- Serialize readiness publication, manager lifecycle mutation, and
          -- the executable claim against the same protected writer mutex.
          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;

          SELECT trial.id, trial.team_id, trial.task_id, trial.config,
                 trial.requires_caps, trial.attempt_count,
                 trial.provider_connection_id, trial.family_key, trial.batch_id,
                 trial.execution_route_json, trial.submit_priority,
                 trial.submitted_at, quota.in_flight_count,
                 quota.fair_share_weight, attempt.protected_attempt_id,
                 attempt.execution_generation, attempt.requirements_digest,
                 assignment.payload AS assignment_payload,
                 assignment.payload_digest AS assignment_payload_digest,
                 readiness.public_requires_caps_digest,
                 readiness.task_image_prerequisites_digest,
                 readiness.model_switch_prerequisite_digest,
                 task_definition.config AS task_config
            INTO v_candidate
            FROM {SCHEMA}.protected_runtime_trial_submissions AS runtime
            JOIN {SCHEMA}.protected_runtime_trial_readiness AS readiness
              ON readiness.trial_id = runtime.trial_id
             AND readiness.protected_attempt_id = runtime.protected_attempt_id
            JOIN {SCHEMA}.trial_attempts AS attempt
              ON attempt.trial_id = runtime.trial_id
             AND attempt.protected_attempt_id = runtime.protected_attempt_id
            JOIN {SCHEMA}.attempt_lifecycle_heads AS head
              ON head.protected_attempt_id = attempt.protected_attempt_id
            JOIN {SCHEMA}.attempt_lifecycle_events AS assignment
              ON assignment.transition_id = head.transition_id
             AND assignment.protected_attempt_id = head.protected_attempt_id
            JOIN public.trials AS trial ON trial.id = runtime.trial_id
            JOIN public.tasks AS task_definition ON task_definition.id = trial.task_id
            JOIN public.team_quotas AS quota ON quota.team_id = trial.team_id
           WHERE head.lifecycle_state = 'assigned'
             AND head.executable = false
             AND assignment.operation = 'assign'
             AND assignment.lifecycle_state = 'assigned'
             AND assignment.executable = false
             AND assignment.submission_intent_id = v_worker.intent_id
             AND assignment.pool_id = v_worker.binding->>'pool_id'
             AND assignment.shape_instance_id = v_worker.binding->>'shape_instance_id'
             AND attempt.claim_state = 'queued'
             AND trial.state = 'protected-pending'
             AND trial.cancellation_requested_at IS NULL
             AND trial.worker_id IS NULL
             AND trial.attempt_count < quota.max_attempts_ceiling
             AND trial.next_attempt_at IS NULL
             AND trial.autoscaler_pool_name IS NULL
             AND (
               NULLIF(trial.requires_caps->>'worker_pool', '') IS NULL
               OR trial.requires_caps->>'worker_pool' IN (
                    v_worker.pool_name, v_worker.binding->>'pool_id'
                  )
             )
             AND EXISTS (
               SELECT 1
                 FROM jsonb_array_elements(v_worker.capabilities) AS capability
                WHERE capability->>'os' = trial.requires_caps->>'os'
                  AND (
                    COALESCE(trial.requires_caps->>'cpu_arch', 'x86_64') = 'any'
                    OR COALESCE(capability->>'cpu_arch', 'x86_64') =
                         COALESCE(trial.requires_caps->>'cpu_arch', 'x86_64')
                  )
                  AND capability->>'gpu_vendor' = trial.requires_caps->>'gpu_vendor'
                  AND COALESCE(capability->>'backend', 'docker') =
                        COALESCE(trial.requires_caps->>'backend', 'docker')
                  AND (trial.requires_caps->'network_policies') <@
                        (capability->'network_policies')
                  AND (
                    COALESCE(
                      (trial.requires_caps->>'terminus2_model_switch')::boolean,
                      false
                    ) IS NOT TRUE
                    OR COALESCE(
                         (capability->>'terminus2_model_switch')::boolean,
                         false
                       ) IS TRUE
                  )
             )
             AND (
               EXISTS (
                 SELECT 1
                   FROM public.trial_task_image_materializations AS image_link
                   JOIN public.task_image_materializations AS materialization
                     ON materialization.id = image_link.materialization_id
                  WHERE image_link.trial_id = trial.id
                    AND materialization.cpu_arch IN (
                      SELECT COALESCE(capability->>'cpu_arch', 'x86_64')
                        FROM jsonb_array_elements(v_worker.capabilities) AS capability
                    )
                    AND materialization.state = 'ready'
                    AND jsonb_typeof(materialization.registry_images) = 'object'
                    AND materialization.registry_images <> '{{}}'::jsonb
               )
               OR (
                 NOT EXISTS (
                   SELECT 1
                     FROM public.trial_task_image_materializations AS image_link
                    WHERE image_link.trial_id = trial.id
                 )
                 AND task_definition.config #>> '{{environment,dockerfile}}' IS NULL
                 AND NOT EXISTS (
                   SELECT 1
                     FROM jsonb_array_elements(
                       CASE
                         WHEN jsonb_typeof(
                           task_definition.config #> '{{environment,sidecars}}'
                         ) = 'array'
                         THEN task_definition.config #> '{{environment,sidecars}}'
                         ELSE '[]'::jsonb
                       END
                     ) AS sidecar
                    WHERE sidecar->>'dockerfile' IS NOT NULL
                 )
               )
             )
             AND (
               trial.family_key IS NULL
               OR EXISTS (
                 SELECT 1 FROM public.batch_family_state AS family
                  WHERE family.batch_id = trial.batch_id
                    AND family.family_key = trial.family_key
                    AND family.state = 'pending'
                    AND family.task_sequence[family.current_index + 1] = trial.task_id
               )
             )
             AND NOT EXISTS (
               SELECT 1
                 FROM public.execution_admission_policies AS admission_policy
                WHERE admission_policy.enabled
                  AND CASE admission_policy.scope_kind
                        WHEN 'global' THEN admission_policy.scope_key = '*'
                        WHEN 'environment' THEN
                          v_worker.admission_environment = admission_policy.scope_key
                        WHEN 'region' THEN
                          v_worker.admission_region = admission_policy.scope_key
                        WHEN 'team' THEN
                          trial.team_id::text = admission_policy.scope_key
                        WHEN 'batch' THEN
                          trial.batch_id::text = admission_policy.scope_key
                        WHEN 'execution_class' THEN
                          trial.execution_route_json->>'selected_execution_class_id' =
                            admission_policy.scope_key
                        WHEN 'pool' THEN
                          v_worker.pool_name = admission_policy.scope_key
                        ELSE false
                      END
                  AND admission_policy.active_count >=
                        admission_policy.max_concurrent
             )
           ORDER BY
             (quota.in_flight_count * 1.0) /
               NULLIF(quota.fair_share_weight, 0) ASC,
             trial.submit_priority DESC,
             trial.submitted_at ASC,
             trial.id ASC
           LIMIT 1
           FOR UPDATE OF trial SKIP LOCKED;
          IF NOT FOUND THEN
            RETURN NULL;
          END IF;

          v_snapshot := {SCHEMA}.inspect_protected_runtime_trial_prerequisites(
            v_worker.agent_incarnation, v_candidate.id,
            v_candidate.protected_attempt_id, true
          );
          IF v_candidate.public_requires_caps_digest IS DISTINCT FROM
               v_snapshot->>'public_requires_caps_digest'
             OR v_candidate.task_image_prerequisites_digest IS DISTINCT FROM
                  v_snapshot->>'task_image_prerequisites_digest'
             OR v_candidate.model_switch_prerequisite_digest IS DISTINCT FROM
                  v_snapshot->>'model_switch_prerequisite_digest' THEN
            RAISE EXCEPTION 'protected staging claim readiness drifted'
              USING ERRCODE = '55000';
          END IF;

          v_assignment_payload := {SCHEMA}.assert_current_inert_assignment(
            v_worker.agent_incarnation,
            v_candidate.assignment_payload,
            convert_to(
              {SCHEMA}.canonical_executable_publication_payload(
                v_candidate.assignment_payload
              ),
              'UTF8'
            ),
            v_candidate.assignment_payload_digest
          );

          SELECT jsonb_build_object(
                   'schema_version', 'loom.task-image-execution-grant.v1',
                   'materialization_id', materialization.id,
                   'materialization_key', materialization.materialization_key,
                   'cpu_arch', materialization.cpu_arch,
                   'task_checksum', materialization.task_checksum,
                   'task_config', materialization.task_config,
                   'task_source', materialization.task_source,
                   'task_source_provenance', materialization.task_source_provenance,
                   'registry_images', materialization.registry_images
                 )
            INTO v_task_image
            FROM public.trial_task_image_materializations AS image_link
            JOIN public.task_image_materializations AS materialization
              ON materialization.id = image_link.materialization_id
           WHERE image_link.trial_id = v_candidate.id
             AND materialization.cpu_arch IN (
               SELECT COALESCE(capability->>'cpu_arch', 'x86_64')
                 FROM jsonb_array_elements(v_worker.capabilities) AS capability
             )
             AND materialization.state = 'ready'
             AND jsonb_typeof(materialization.registry_images) = 'object'
             AND materialization.registry_images <> '{{}}'::jsonb
           ORDER BY materialization.cpu_arch, materialization.id
           LIMIT 1
           FOR UPDATE OF materialization;
          IF v_task_image IS NULL AND EXISTS (
            SELECT 1 FROM public.trial_task_image_materializations AS image_link
             WHERE image_link.trial_id = v_candidate.id
          ) THEN
            RAISE EXCEPTION 'protected staging claim task image raced'
              USING ERRCODE = '40001';
          END IF;

          SELECT jsonb_build_object(
                   'id', plan.id,
                   'trial_id', plan.trial_id,
                   'combination_idx', plan.combination_idx,
                   'mix_mode', plan.mix_mode,
                   'k1', plan.k1,
                   'k2', plan.k2,
                   'teacher_episodes', plan.teacher_episodes,
                   'beta', plan.beta,
                   'seed', plan.seed,
                   'prng_version', plan.prng_version,
                   'student_model', plan.student_model_snapshot,
                   'teacher_model', plan.teacher_model_snapshot,
                   'provider_connection_id', plan.provider_connection_id,
                   'pricing_snapshot', plan.pricing_snapshot,
                   'capability_snapshot', plan.capability_snapshot,
                   'inherited_from_plan_id', plan.inherited_from_plan_id
                 )
            INTO v_model_switch
            FROM public.model_switch_plans AS plan
           WHERE plan.trial_id = v_candidate.id
           FOR KEY SHARE;

          SELECT state.* INTO v_claim_state
            FROM {SCHEMA}.executable_claim_state AS state
           WHERE state.intent_id = v_worker.intent_id
           FOR UPDATE;
          IF NOT FOUND
             OR v_claim_state.subject_id IS DISTINCT FROM v_worker.subject_id
             OR v_claim_state.subject_incarnation IS DISTINCT FROM
                  v_worker.subject_incarnation
             OR v_claim_state.binding IS DISTINCT FROM v_worker.binding
             OR v_claim_state.draining THEN
            RAISE EXCEPTION 'protected staging claim worker state drifted'
              USING ERRCODE = '55000';
          END IF;

          PERFORM pg_advisory_xact_lock_shared(
            hashtextextended('execution-admission-policy-mutation', 1552)
          );
          FOR v_admission_policy IN
            SELECT admission_policy.scope_kind, admission_policy.scope_key,
                   admission_policy.max_concurrent, admission_policy.active_count
              FROM public.execution_admission_policies AS admission_policy
             WHERE admission_policy.enabled
               AND CASE admission_policy.scope_kind
                     WHEN 'global' THEN admission_policy.scope_key = '*'
                     WHEN 'environment' THEN
                       v_worker.admission_environment = admission_policy.scope_key
                     WHEN 'region' THEN
                       v_worker.admission_region = admission_policy.scope_key
                     WHEN 'team' THEN
                       v_candidate.team_id::text = admission_policy.scope_key
                     WHEN 'batch' THEN
                       v_candidate.batch_id::text = admission_policy.scope_key
                     WHEN 'execution_class' THEN
                       v_candidate.execution_route_json->>
                         'selected_execution_class_id' = admission_policy.scope_key
                     WHEN 'pool' THEN v_worker.pool_name = admission_policy.scope_key
                     ELSE false
                   END
             ORDER BY CASE admission_policy.scope_kind
                        WHEN 'global' THEN 1
                        WHEN 'environment' THEN 2
                        WHEN 'region' THEN 3
                        WHEN 'team' THEN 4
                        WHEN 'batch' THEN 5
                        WHEN 'execution_class' THEN 6
                        WHEN 'pool' THEN 7
                        ELSE 8
                      END,
                      admission_policy.scope_key
             FOR UPDATE
          LOOP
            IF v_admission_policy.active_count >=
                 v_admission_policy.max_concurrent THEN
              RETURN NULL;
            END IF;
          END LOOP;
          INSERT INTO public.execution_admission_reservations (
            trial_id, attempt, execution_role, team_id, batch_id,
            environment, region, execution_class_id, pool_id,
            owner_kind, owner_id, acquired_at
          ) VALUES (
            v_candidate.id, v_candidate.attempt_count + 1, 'attempt',
            v_candidate.team_id, v_candidate.batch_id,
            v_worker.admission_environment, v_worker.admission_region,
            v_candidate.execution_route_json->>'selected_execution_class_id',
            v_worker.pool_name, 'protected_worker_claim', p_worker_id,
            statement_timestamp()
          )
          ON CONFLICT (trial_id, attempt, execution_role) DO NOTHING
          RETURNING id INTO v_reservation_id;
          IF v_reservation_id IS NULL THEN
            RETURN NULL;
          END IF;
          UPDATE public.execution_admission_policies AS admission_policy
             SET active_count = admission_policy.active_count + 1,
                 counter_updated_at = statement_timestamp()
           WHERE admission_policy.enabled
             AND CASE admission_policy.scope_kind
                   WHEN 'global' THEN admission_policy.scope_key = '*'
                   WHEN 'environment' THEN
                     v_worker.admission_environment = admission_policy.scope_key
                   WHEN 'region' THEN
                     v_worker.admission_region = admission_policy.scope_key
                   WHEN 'team' THEN
                     v_candidate.team_id::text = admission_policy.scope_key
                   WHEN 'batch' THEN
                     v_candidate.batch_id::text = admission_policy.scope_key
                   WHEN 'execution_class' THEN
                     v_candidate.execution_route_json->>'selected_execution_class_id' =
                       admission_policy.scope_key
                   WHEN 'pool' THEN v_worker.pool_name = admission_policy.scope_key
                   ELSE false
                 END;

          IF v_candidate.family_key IS NOT NULL THEN
            UPDATE public.batch_family_state AS family
               SET state = 'running', updated_at = statement_timestamp()
             WHERE family.batch_id = v_candidate.batch_id
               AND family.family_key = v_candidate.family_key
               AND family.state = 'pending'
               AND family.task_sequence[family.current_index + 1] = v_candidate.task_id
            RETURNING family.state_uri INTO v_family_state_uri;
            IF NOT FOUND THEN
              RAISE EXCEPTION 'protected staging family claim raced'
                USING ERRCODE = '40001';
            END IF;
            SELECT batch.family_run_spec INTO v_family_run_spec
              FROM public.batches AS batch
             WHERE batch.id = v_candidate.batch_id
             FOR KEY SHARE;
          END IF;

          v_claim_payload := jsonb_build_object(
            'schema_version', 2,
            'operation_id', v_operation_id,
            'protected_attempt_id', v_candidate.protected_attempt_id,
            'execution_generation', v_candidate.execution_generation,
            'requirements_digest', v_candidate.requirements_digest,
            'worker_id', p_worker_id,
            'worker_incarnation', v_worker.worker_incarnation,
            'expected_claim_high_water', v_claim_state.claim_high_water,
            'executable', true
          );
          v_claim_canonical := {SCHEMA}.canonical_executable_publication_payload(
            v_claim_payload
          );
          v_claim_digest := encode(
            sha256(convert_to(v_claim_canonical, 'UTF8')),
            'hex'
          );
          v_claim_receipt := jsonb_build_object(
            'schema_version', 2,
            'operation_id', v_operation_id,
            'subject_id', v_worker.subject_id,
            'subject_incarnation', v_worker.subject_incarnation,
            'protected_attempt_id', v_candidate.protected_attempt_id,
            'execution_generation', v_candidate.execution_generation,
            'requirements_digest', v_candidate.requirements_digest,
            'intent_id', v_worker.intent_id,
            'worker_id', p_worker_id,
            'worker_incarnation', v_worker.worker_incarnation,
            'claim_high_water', v_claim_state.claim_high_water + 1,
            'request_digest', v_claim_digest,
            'lease_state', 'live',
            'admitted', true,
            'executable', true
          );
          UPDATE {SCHEMA}.executable_claim_state
             SET claim_high_water = claim_high_water + 1
           WHERE intent_id = v_worker.intent_id
             AND claim_high_water = v_claim_state.claim_high_water
             AND draining = false;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'protected staging claim high-water raced'
              USING ERRCODE = '40001';
          END IF;
          INSERT INTO {SCHEMA}.executable_claim_leases
            (operation_id, intent_id, subject_id, subject_incarnation,
             protected_attempt_id, execution_generation, requirements_digest,
             worker_id, worker_incarnation, claim_high_water, lease_state,
             request_payload, request_digest, receipt, executable)
          VALUES
            (v_operation_id, v_worker.intent_id, v_worker.subject_id,
             v_worker.subject_incarnation, v_candidate.protected_attempt_id,
             v_candidate.execution_generation, v_candidate.requirements_digest,
             p_worker_id, v_worker.worker_incarnation,
             v_claim_state.claim_high_water + 1, 'live', v_claim_payload,
             v_claim_digest, v_claim_receipt, true);

          UPDATE public.trials AS trial
             SET state = 'claimed',
                 worker_id = p_worker_id,
                 claimed_at = statement_timestamp(),
                 pre_start_heartbeat_at = NULL,
                 failure_reason = NULL,
                 failure_message = NULL,
                 attempt_count = trial.attempt_count + 1
           WHERE trial.id = v_candidate.id
             AND trial.state = 'protected-pending'
             AND trial.worker_id IS NULL
             AND trial.attempt_count = v_candidate.attempt_count
             AND trial.cancellation_requested_at IS NULL
             AND trial.next_attempt_at IS NULL
             AND trial.autoscaler_pool_name IS NULL
          RETURNING trial.id, trial.team_id, trial.task_id, trial.config,
                    trial.requires_caps, trial.attempt_count,
                    trial.provider_connection_id, trial.family_key,
                    trial.batch_id
               INTO v_claimed;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'protected staging public claim raced'
              USING ERRCODE = '40001';
          END IF;

          RETURN jsonb_build_object(
            'trial_id', v_claimed.id,
            'team_id', v_claimed.team_id,
            'task_id', v_claimed.task_id,
            'config', v_claimed.config,
            'requires_caps', v_claimed.requires_caps,
            'attempt_count', v_claimed.attempt_count,
            'provider_connection_id', v_claimed.provider_connection_id,
            'family_key', v_claimed.family_key,
            'family_state_uri', v_family_state_uri,
            'family_run_spec', v_family_run_spec,
            'task_image_materialization', v_task_image,
            'model_switch_plan', v_model_switch,
            'state', 'claimed'
          );
        END
        $function$
        """
    )


def upgrade() -> None:
    runtime_role, quoted_runtime = _runtime_role()
    op.execute(
        f"""
        CREATE TABLE {SCHEMA}.staging_worker_runtime_authority (
          singleton_id smallint PRIMARY KEY CHECK (singleton_id = 1),
          runtime_role_name text NOT NULL CHECK (
            runtime_role_name ~ '^[a-z][a-z0-9_]{{0,62}}$'
          ),
          created_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.get_bind().execute(
        sa.text(
            f"INSERT INTO {SCHEMA}.staging_worker_runtime_authority "
            "(singleton_id, runtime_role_name) VALUES (1, :runtime_role)"
        ),
        {"runtime_role": runtime_role},
    )
    op.execute(
        f"CREATE TRIGGER staging_worker_runtime_authority_append_only_row "
        f"BEFORE UPDATE OR DELETE ON {SCHEMA}.staging_worker_runtime_authority "
        f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()"
    )
    op.execute(
        f"CREATE TRIGGER staging_worker_runtime_authority_append_only_truncate "
        f"BEFORE TRUNCATE ON {SCHEMA}.staging_worker_runtime_authority "
        f"FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()"
    )
    _replace_function_clause(
        "assert_inert_agent_binding(uuid,jsonb,bytea,text)",
        _AGENT_BINDING_DECLARATION,
        _RUNTIME_BINDING_DECLARATION,
    )
    _replace_function_clause(
        "assert_inert_agent_binding(uuid,jsonb,bytea,text)",
        _AGENT_BINDING_ROLE_CHECK,
        _RUNTIME_BINDING_ROLE_CHECK,
    )
    _install_runtime_submission_ledgers()
    _replace_function_clause(
        "register_inert_trial_submission(uuid,jsonb,bytea,text,bytea,text)",
        _SEALED_NETWORK_POLICIES,
        _RUNTIME_NETWORK_POLICIES,
    )
    _replace_function_clause(
        "submit_inert_trial_projection(uuid,jsonb,bytea,text,jsonb,bytea,text,bytea,text)",
        _SEALED_NETWORK_POLICIES,
        _RUNTIME_NETWORK_POLICIES,
    )
    _replace_function_clause(
        "submit_inert_trial_projection(uuid,jsonb,bytea,text,jsonb,bytea,text,bytea,text)",
        _ATOMIC_REPLAY_UNAVAILABLE_CONFLICT,
        _ATOMIC_REPLAY_IDEMPOTENCY_CONFLICT,
    )
    _replace_function_clause(
        "capture_lifecycle_demand_observation_v2_queued(uuid,bigint,integer)",
        _UNFILTERED_LIFECYCLE_SOURCE,
        _READINESS_FILTERED_LIFECYCLE_SOURCE,
    )
    _install_runtime_submission_functions()
    _install_runtime_readiness_functions()
    _patch_runtime_assignment_admission()
    _install_atomic_lifecycle_capture(runtime_aware=True)
    _install_registration_function()
    _install_session_assertion_function()
    _install_assigned_trial_claim_function()
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON SCHEMA {SCHEMA} FROM {quoted_runtime}")
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {SCHEMA} FROM {quoted_runtime}")
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {SCHEMA} FROM {quoted_runtime}")
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {SCHEMA} FROM {quoted_runtime}")
    op.execute(f"GRANT USAGE ON SCHEMA {SCHEMA} TO {quoted_runtime}")
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{CURRENT_REGISTRATION_FUNCTION} "
        f"TO {quoted_runtime}"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{SUBMIT_TRIAL_FUNCTION} TO {quoted_runtime}"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{PUBLISH_TRIAL_READINESS_FUNCTION} "
        f"TO {quoted_runtime}"
    )
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{REGISTER_FUNCTION} TO {quoted_runtime}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{ASSERT_FUNCTION} TO {quoted_runtime}")
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{CLAIM_TRIAL_FUNCTION} TO {quoted_runtime}"
    )


def downgrade() -> None:
    bind = op.get_bind()
    persisted_runtime_role = bind.execute(
        sa.text(
            f"SELECT runtime_role_name FROM {SCHEMA}.staging_worker_runtime_authority "
            "WHERE singleton_id = 1"
        )
    ).scalar_one()
    quoted_runtime = bind.dialect.identifier_preparer.quote(persisted_runtime_role)
    op.execute(
        f"LOCK TABLE {SCHEMA}.protected_runtime_trial_readiness, "
        f"{SCHEMA}.protected_runtime_trial_submissions IN SHARE MODE"
    )
    if bind.execute(
        sa.text(
            f"SELECT EXISTS ("
            f"SELECT 1 FROM {SCHEMA}.protected_runtime_trial_submissions "
            f"UNION ALL "
            f"SELECT 1 FROM {SCHEMA}.protected_runtime_trial_readiness"
            f")"
        )
    ).scalar_one():
        raise RuntimeError(
            "cannot downgrade guard_0023 while protected runtime submissions exist"
        )
    if bind.execute(
        sa.text(
            f"SELECT EXISTS ("
            f"SELECT 1 FROM {SCHEMA}.executable_admission_events AS event "
            "JOIN public.workers AS worker ON worker.id = event.worker_id "
            "WHERE event.event_kind = 'worker-registered' "
            "AND worker.auth_token_hash = decode(event.worker_credential_sha256, 'hex')"
            ")"
        )
    ).scalar_one():
        raise RuntimeError(
            "cannot downgrade guard_0023 while protected staging worker projections exist"
        )
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{CLAIM_TRIAL_FUNCTION} FROM {quoted_runtime}"
    )
    op.execute(f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{ASSERT_FUNCTION} FROM {quoted_runtime}")
    op.execute(f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{REGISTER_FUNCTION} FROM {quoted_runtime}")
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{PUBLISH_TRIAL_READINESS_FUNCTION} "
        f"FROM {quoted_runtime}"
    )
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{SUBMIT_TRIAL_FUNCTION} FROM {quoted_runtime}"
    )
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{CURRENT_REGISTRATION_FUNCTION} "
        f"FROM {quoted_runtime}"
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.{CLAIM_TRIAL_FUNCTION}")
    op.execute(f"DROP FUNCTION {SCHEMA}.{ASSERT_FUNCTION}")
    op.execute(f"DROP FUNCTION {SCHEMA}.{REGISTER_FUNCTION}")
    _install_atomic_lifecycle_capture(runtime_aware=False)
    _unpatch_runtime_assignment_admission()
    _replace_function_clause(
        "capture_lifecycle_demand_observation_v2_queued(uuid,bigint,integer)",
        _READINESS_FILTERED_LIFECYCLE_SOURCE,
        _UNFILTERED_LIFECYCLE_SOURCE,
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.{PUBLISH_TRIAL_READINESS_FUNCTION}")
    op.execute(
        f"DROP FUNCTION {SCHEMA}.inspect_protected_runtime_trial_prerequisites"
        "(uuid,uuid,uuid,boolean)"
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.{SUBMIT_TRIAL_FUNCTION}")
    op.execute(f"DROP FUNCTION {SCHEMA}.{CURRENT_REGISTRATION_FUNCTION}")
    _replace_function_clause(
        "submit_inert_trial_projection(uuid,jsonb,bytea,text,jsonb,bytea,text,bytea,text)",
        _ATOMIC_REPLAY_IDEMPOTENCY_CONFLICT,
        _ATOMIC_REPLAY_UNAVAILABLE_CONFLICT,
    )
    _replace_function_clause(
        "submit_inert_trial_projection(uuid,jsonb,bytea,text,jsonb,bytea,text,bytea,text)",
        _RUNTIME_NETWORK_POLICIES,
        _SEALED_NETWORK_POLICIES,
    )
    _replace_function_clause(
        "register_inert_trial_submission(uuid,jsonb,bytea,text,bytea,text)",
        _RUNTIME_NETWORK_POLICIES,
        _SEALED_NETWORK_POLICIES,
    )
    for table in (
        "protected_runtime_trial_readiness",
        "protected_runtime_trial_submissions",
    ):
        op.execute(
            f"DROP TRIGGER {table}_append_only_truncate ON {SCHEMA}.{table}"
        )
        op.execute(f"DROP TRIGGER {table}_append_only_row ON {SCHEMA}.{table}")
        op.drop_table(table, schema=SCHEMA)
    op.drop_constraint(
        "guard_atomic_submission_trial_attempt_key",
        "atomic_trial_submissions",
        schema=SCHEMA,
        type_="unique",
    )
    _replace_function_clause(
        "assert_inert_agent_binding(uuid,jsonb,bytea,text)",
        _RUNTIME_BINDING_ROLE_CHECK,
        _AGENT_BINDING_ROLE_CHECK,
    )
    _replace_function_clause(
        "assert_inert_agent_binding(uuid,jsonb,bytea,text)",
        _RUNTIME_BINDING_DECLARATION,
        _AGENT_BINDING_DECLARATION,
    )
    op.execute(
        f"DROP TRIGGER staging_worker_runtime_authority_append_only_truncate "
        f"ON {SCHEMA}.staging_worker_runtime_authority"
    )
    op.execute(
        f"DROP TRIGGER staging_worker_runtime_authority_append_only_row "
        f"ON {SCHEMA}.staging_worker_runtime_authority"
    )
    op.execute(f"DROP TABLE {SCHEMA}.staging_worker_runtime_authority")
    op.execute(f"REVOKE USAGE ON SCHEMA {SCHEMA} FROM {quoted_runtime}")
