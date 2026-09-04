"""Create fresh protected attempts for pre-start trial retries.

Revision ID: guard_0025
Revises: guard_0024
Create Date: 2026-09-02
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "guard_0025"
down_revision: str | Sequence[str] | None = "guard_0024"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"
RETRY_FUNCTION = "retry_staging_claimed_trial(uuid,text,jsonb)"


def _runtime_role() -> tuple[str, str]:
    role = op.get_context().config.attributes.get("capacity_guard_runtime_role")
    if not isinstance(role, str) or not role:
        raise RuntimeError("protected retry migration is missing the runtime role")
    return role, op.get_bind().dialect.identifier_preparer.quote(role)


def _replace_function_clause(
    function: str,
    old: str,
    new: str,
    *,
    expected_count: int = 1,
) -> None:
    escaped_old = old.replace("'", "''")
    escaped_new = new.replace("'", "''")
    op.execute(
        f"""
        DO $$
        DECLARE
          v_definition text;
          v_match_count integer;
        BEGIN
          SELECT pg_get_functiondef(
            '{SCHEMA}.{function}'::regprocedure
          ) INTO v_definition;
          v_match_count := (
            length(v_definition) - length(replace(v_definition, '{escaped_old}', ''))
          ) / length('{escaped_old}');
          IF v_match_count IS DISTINCT FROM {expected_count} THEN
            RAISE EXCEPTION
              'protected retry function clause count differed for {function}: %',
              v_match_count;
          END IF;
          EXECUTE replace(v_definition, '{escaped_old}', '{escaped_new}');
        END $$;
        """
    )


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
_MULTI_ATTEMPT_ASSIGNMENT_PUBLIC_STATE = _RUNTIME_AWARE_ASSIGNMENT_PUBLIC_STATE.replace(
    "AND readiness.public_requires_caps_digest =\n",
    "AND runtime.attempt_sequence = ta.attempt_sequence\n"
    "                          AND runtime.public_attempt_count = t.attempt_count\n"
    "                          AND runtime.not_before IS NOT DISTINCT FROM "
    "t.next_attempt_at\n"
    "                          AND readiness.public_requires_caps_digest =\n",
)

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
_MULTI_ATTEMPT_CURRENT_ASSIGNMENT_PUBLIC_STATE = (
    _RUNTIME_AWARE_CURRENT_ASSIGNMENT_PUBLIC_STATE.replace(
        "AND readiness.public_requires_caps_digest =\n",
        "AND runtime.attempt_sequence = attempt.attempt_sequence\n"
        "                      AND runtime.public_attempt_count = trial.attempt_count\n"
        "                      AND runtime.not_before IS NOT DISTINCT FROM "
        "trial.next_attempt_at\n"
        "                      AND readiness.public_requires_caps_digest =\n",
    )
)

_CAPTURE_RUNTIME_ORIGIN_BINDING = f"""JOIN {SCHEMA}.atomic_trial_submissions AS submission
                ON submission.trial_id = runtime.trial_id
               AND submission.protected_attempt_id = runtime.protected_attempt_id"""
_CAPTURE_MULTI_RUNTIME_ORIGIN_BINDING = f"""JOIN {SCHEMA}.atomic_trial_submissions AS submission
                ON submission.trial_id = runtime.trial_id"""
_CAPTURE_ATTEMPT_ORIGIN_BINDING = (
    "AND attempt.protected_attempt_id = submission.protected_attempt_id"
)
_CAPTURE_MULTI_ATTEMPT_ORIGIN_BINDING = f"""AND (
                 attempt.protected_attempt_id = submission.protected_attempt_id
                 OR EXISTS (
                   SELECT 1
                     FROM {SCHEMA}.protected_runtime_trial_submissions AS current_runtime
                    WHERE current_runtime.trial_id = attempt.trial_id
                      AND current_runtime.protected_attempt_id =
                            attempt.protected_attempt_id
                      AND current_runtime.attempt_sequence = attempt.attempt_sequence
                 )
               )"""
_CAPTURE_RUNTIME_ATTEMPT_BINDING = "AND attempt.protected_attempt_id = runtime.protected_attempt_id"
_CAPTURE_MULTI_RUNTIME_ATTEMPT_BINDING = (
    "AND attempt.protected_attempt_id = runtime.protected_attempt_id\n"
    "               AND attempt.attempt_sequence = runtime.attempt_sequence"
)
_CAPTURE_INITIAL_DRIFT = """OR trial.attempt_count IS DISTINCT FROM 0
                 OR trial.next_attempt_at IS NOT NULL"""
_CAPTURE_MULTI_DRIFT = f"""OR trial.attempt_count IS DISTINCT FROM COALESCE(
                      (
                        SELECT current_runtime.public_attempt_count
                          FROM {SCHEMA}.protected_runtime_trial_submissions
                               AS current_runtime
                         WHERE current_runtime.trial_id = attempt.trial_id
                           AND current_runtime.protected_attempt_id =
                                 attempt.protected_attempt_id
                      ),
                      0
                    )
                 OR trial.next_attempt_at IS DISTINCT FROM (
                      SELECT current_runtime.not_before
                        FROM {SCHEMA}.protected_runtime_trial_submissions
                             AS current_runtime
                       WHERE current_runtime.trial_id = attempt.trial_id
                         AND current_runtime.protected_attempt_id =
                               attempt.protected_attempt_id
                    )"""
_CAPTURE_INITIAL_READY = """AND trial.attempt_count = 0
             AND trial.next_attempt_at IS NULL"""
_CAPTURE_MULTI_READY = f"""AND trial.attempt_count = COALESCE(
                  (
                    SELECT current_runtime.public_attempt_count
                      FROM {SCHEMA}.protected_runtime_trial_submissions
                           AS current_runtime
                     WHERE current_runtime.trial_id = attempt.trial_id
                       AND current_runtime.protected_attempt_id =
                             attempt.protected_attempt_id
                  ),
                  0
                )
             AND trial.next_attempt_at IS NOT DISTINCT FROM (
                  SELECT current_runtime.not_before
                    FROM {SCHEMA}.protected_runtime_trial_submissions
                         AS current_runtime
                   WHERE current_runtime.trial_id = attempt.trial_id
                     AND current_runtime.protected_attempt_id =
                           attempt.protected_attempt_id
                )"""

_INSPECT_ORIGIN_BINDING = f"""JOIN {SCHEMA}.atomic_trial_submissions AS submission
              ON submission.trial_id = runtime.trial_id
             AND submission.protected_attempt_id = runtime.protected_attempt_id
            JOIN {SCHEMA}.trial_attempts AS attempt
              ON attempt.trial_id = runtime.trial_id
             AND attempt.protected_attempt_id = runtime.protected_attempt_id"""
_INSPECT_MULTI_ORIGIN_BINDING = f"""JOIN {SCHEMA}.atomic_trial_submissions AS submission
              ON submission.trial_id = runtime.trial_id
            JOIN {SCHEMA}.trial_attempts AS attempt
              ON attempt.trial_id = runtime.trial_id
             AND attempt.protected_attempt_id = runtime.protected_attempt_id
             AND attempt.attempt_sequence = runtime.attempt_sequence"""
_INSPECT_INITIAL_PUBLIC_BINDING = """OR v_trial.cancellation_requested_at IS NOT NULL
             OR v_trial.next_attempt_at IS NOT NULL
             OR v_trial.autoscaler_pool_name IS NOT NULL
             OR v_trial.worker_id IS NOT NULL
             OR v_trial.attempt_count IS DISTINCT FROM 0"""
_INSPECT_MULTI_PUBLIC_BINDING = """OR v_trial.cancellation_requested_at IS NOT NULL
             OR v_trial.next_attempt_at IS DISTINCT FROM v_origin.not_before
             OR v_trial.autoscaler_pool_name IS NOT NULL
             OR v_trial.worker_id IS NOT NULL
             OR v_trial.attempt_count IS DISTINCT FROM v_origin.public_attempt_count"""
_INSPECT_ORIGIN_FIELDS = """runtime.public_requires_caps,
                 runtime.public_requires_caps_digest,
                 submission.payload,"""
_INSPECT_MULTI_ORIGIN_FIELDS = """runtime.public_requires_caps,
                 runtime.public_requires_caps_digest,
                 runtime.public_attempt_count,
                 runtime.not_before,
                 submission.payload,"""

_CLAIM_INITIAL_CANDIDATE = """AND attempt.claim_state = 'queued'
             AND trial.state = 'protected-pending'
             AND trial.cancellation_requested_at IS NULL
             AND trial.worker_id IS NULL
             AND trial.attempt_count < quota.max_attempts_ceiling
             AND trial.next_attempt_at IS NULL
             AND trial.autoscaler_pool_name IS NULL"""
_CLAIM_MULTI_CANDIDATE = """AND attempt.claim_state = 'queued'
             AND attempt.attempt_sequence = runtime.attempt_sequence
             AND trial.state = 'protected-pending'
             AND trial.cancellation_requested_at IS NULL
             AND trial.worker_id IS NULL
             AND trial.attempt_count = runtime.public_attempt_count
             AND trial.attempt_count < quota.max_attempts_ceiling
             AND trial.next_attempt_at IS NOT DISTINCT FROM runtime.not_before
             AND (runtime.not_before IS NULL
                  OR runtime.not_before <= statement_timestamp())
             AND trial.autoscaler_pool_name IS NULL"""
_CLAIM_INITIAL_UPDATE = """AND trial.attempt_count = v_candidate.attempt_count
             AND trial.cancellation_requested_at IS NULL
             AND trial.next_attempt_at IS NULL
             AND trial.autoscaler_pool_name IS NULL"""
_CLAIM_MULTI_UPDATE = """AND trial.attempt_count = v_candidate.attempt_count
             AND trial.cancellation_requested_at IS NULL
             AND (trial.next_attempt_at IS NULL
                  OR trial.next_attempt_at <= statement_timestamp())
             AND trial.autoscaler_pool_name IS NULL"""
_CLAIM_INITIAL_ATTEMPT_FIELDS = """quota.fair_share_weight, attempt.protected_attempt_id,
                 attempt.execution_generation, attempt.requirements_digest,"""
_CLAIM_MULTI_ATTEMPT_FIELDS = """quota.fair_share_weight, attempt.protected_attempt_id,
                 attempt.execution_generation, attempt.requirements_digest,
                 attempt.attempt_sequence,"""
_CLAIM_INITIAL_RESERVATION_ATTEMPT = "v_candidate.id, v_candidate.attempt_count + 1, 'attempt',"
_CLAIM_MULTI_RESERVATION_ATTEMPT = "v_candidate.id, v_candidate.attempt_sequence + 1, 'attempt',"

_TERMINAL_INITIAL_BINDING = """AND trial.attempt_count = p_attempt_count
             AND attempt.claim_state = 'queued'"""
_TERMINAL_MULTI_BINDING = """AND trial.attempt_count = p_attempt_count
             AND runtime.attempt_sequence = attempt.attempt_sequence
             AND runtime.public_attempt_count + 1 = p_attempt_count
             AND runtime.not_before IS NOT DISTINCT FROM trial.next_attempt_at
             AND attempt.claim_state = 'queued'"""

_EXACT_ASSIGNMENT_INITIAL_EPOCH = """AND lifecycle.manager_allocation_epoch =
                   (worker.binding->'execution'->>'allocation_epoch')::bigint"""
_EXACT_ASSIGNMENT_RETRY_EPOCH = f"""AND (
                 lifecycle.manager_allocation_epoch =
                   (worker.binding->'execution'->>'allocation_epoch')::bigint
                 OR (
                   lifecycle.manager_allocation_epoch >
                     (worker.binding->'execution'->>'allocation_epoch')::bigint
                   AND EXISTS (
                     SELECT 1
                       FROM {SCHEMA}.protected_runtime_trial_submissions AS runtime
                       JOIN {SCHEMA}.trial_attempts AS retry_attempt
                         ON retry_attempt.trial_id = runtime.trial_id
                        AND retry_attempt.protected_attempt_id =
                              runtime.protected_attempt_id
                        AND retry_attempt.attempt_sequence = runtime.attempt_sequence
                       JOIN public.trials AS retry_trial
                         ON retry_trial.id = runtime.trial_id
                      WHERE runtime.protected_attempt_id = NEW.protected_attempt_id
                        AND runtime.attempt_sequence > 0
                        AND retry_attempt.execution_generation =
                              NEW.execution_generation
                        AND retry_attempt.requirements_digest =
                              NEW.requirements_digest
                        AND retry_attempt.claim_state = 'queued'
                        AND retry_trial.state = 'protected-pending'
                        AND retry_trial.cancellation_requested_at IS NULL
                        AND retry_trial.worker_id IS NULL
                        AND retry_trial.attempt_count = runtime.public_attempt_count
                        AND retry_trial.next_attempt_at IS NOT DISTINCT FROM
                              runtime.not_before
                        AND (runtime.not_before IS NULL
                             OR runtime.not_before <= statement_timestamp())
                   )
                 )
               )"""


def _upgrade_runtime_ledgers() -> None:
    op.add_column(
        "protected_runtime_trial_submissions",
        sa.Column(
            "attempt_sequence",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "protected_runtime_trial_submissions",
        sa.Column(
            "public_attempt_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "protected_runtime_trial_submissions",
        sa.Column("not_before", postgresql.TIMESTAMP(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "guard_runtime_submission_retry_identity_check",
        "protected_runtime_trial_submissions",
        "(attempt_sequence = 0 AND public_attempt_count = 0 "
        " AND not_before IS NULL) OR "
        "(attempt_sequence > 0 AND public_attempt_count >= 0 "
        " AND public_attempt_count <= attempt_sequence AND not_before IS NOT NULL)",
        schema=SCHEMA,
    )
    op.drop_constraint(
        "guard_runtime_submission_atomic_binding_fk",
        "protected_runtime_trial_submissions",
        schema=SCHEMA,
        type_="foreignkey",
    )
    op.drop_constraint(
        "protected_runtime_trial_submissions_pkey",
        "protected_runtime_trial_submissions",
        schema=SCHEMA,
        type_="primary",
    )
    op.create_primary_key(
        "protected_runtime_trial_submissions_pkey",
        "protected_runtime_trial_submissions",
        ["trial_id", "protected_attempt_id"],
        schema=SCHEMA,
    )
    op.create_foreign_key(
        "guard_runtime_submission_atomic_origin_fk",
        "protected_runtime_trial_submissions",
        "atomic_trial_submissions",
        ["trial_id"],
        ["trial_id"],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "guard_runtime_submission_attempt_binding_fk",
        "protected_runtime_trial_submissions",
        "trial_attempts",
        ["protected_attempt_id", "trial_id"],
        ["protected_attempt_id", "trial_id"],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
        ondelete="RESTRICT",
    )
    op.drop_constraint(
        "protected_runtime_trial_readiness_pkey",
        "protected_runtime_trial_readiness",
        schema=SCHEMA,
        type_="primary",
    )
    op.create_primary_key(
        "protected_runtime_trial_readiness_pkey",
        "protected_runtime_trial_readiness",
        ["trial_id", "protected_attempt_id"],
        schema=SCHEMA,
    )


def _downgrade_runtime_ledgers() -> None:
    op.drop_constraint(
        "protected_runtime_trial_readiness_pkey",
        "protected_runtime_trial_readiness",
        schema=SCHEMA,
        type_="primary",
    )
    op.create_primary_key(
        "protected_runtime_trial_readiness_pkey",
        "protected_runtime_trial_readiness",
        ["trial_id"],
        schema=SCHEMA,
    )
    op.drop_constraint(
        "guard_runtime_submission_attempt_binding_fk",
        "protected_runtime_trial_submissions",
        schema=SCHEMA,
        type_="foreignkey",
    )
    op.drop_constraint(
        "guard_runtime_submission_atomic_origin_fk",
        "protected_runtime_trial_submissions",
        schema=SCHEMA,
        type_="foreignkey",
    )
    op.drop_constraint(
        "protected_runtime_trial_submissions_pkey",
        "protected_runtime_trial_submissions",
        schema=SCHEMA,
        type_="primary",
    )
    op.create_primary_key(
        "protected_runtime_trial_submissions_pkey",
        "protected_runtime_trial_submissions",
        ["trial_id"],
        schema=SCHEMA,
    )
    op.create_foreign_key(
        "guard_runtime_submission_atomic_binding_fk",
        "protected_runtime_trial_submissions",
        "atomic_trial_submissions",
        ["trial_id", "protected_attempt_id"],
        ["trial_id", "protected_attempt_id"],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
        ondelete="RESTRICT",
    )
    op.drop_constraint(
        "guard_runtime_submission_retry_identity_check",
        "protected_runtime_trial_submissions",
        schema=SCHEMA,
        type_="check",
    )
    op.drop_column("protected_runtime_trial_submissions", "not_before", schema=SCHEMA)
    op.drop_column("protected_runtime_trial_submissions", "public_attempt_count", schema=SCHEMA)
    op.drop_column("protected_runtime_trial_submissions", "attempt_sequence", schema=SCHEMA)


def _patch_multi_attempt_functions(*, upgrade: bool) -> None:
    pairs = (
        (
            "apply_inert_attempt_transition(uuid,jsonb,bytea,text)",
            _RUNTIME_AWARE_ASSIGNMENT_PUBLIC_STATE,
            _MULTI_ATTEMPT_ASSIGNMENT_PUBLIC_STATE,
            1,
        ),
        (
            "assert_current_inert_assignment(uuid,jsonb,bytea,text)",
            _RUNTIME_AWARE_CURRENT_ASSIGNMENT_PUBLIC_STATE,
            _MULTI_ATTEMPT_CURRENT_ASSIGNMENT_PUBLIC_STATE,
            1,
        ),
        (
            "capture_lifecycle_demand_observation(uuid,bigint,integer)",
            _CAPTURE_RUNTIME_ORIGIN_BINDING,
            _CAPTURE_MULTI_RUNTIME_ORIGIN_BINDING,
            1,
        ),
        (
            "capture_lifecycle_demand_observation(uuid,bigint,integer)",
            _CAPTURE_ATTEMPT_ORIGIN_BINDING,
            _CAPTURE_MULTI_ATTEMPT_ORIGIN_BINDING,
            4,
        ),
        (
            "capture_lifecycle_demand_observation(uuid,bigint,integer)",
            _CAPTURE_RUNTIME_ATTEMPT_BINDING,
            _CAPTURE_MULTI_RUNTIME_ATTEMPT_BINDING,
            1,
        ),
        (
            "capture_lifecycle_demand_observation(uuid,bigint,integer)",
            _CAPTURE_INITIAL_DRIFT,
            _CAPTURE_MULTI_DRIFT,
            1,
        ),
        (
            "capture_lifecycle_demand_observation(uuid,bigint,integer)",
            _CAPTURE_INITIAL_READY,
            _CAPTURE_MULTI_READY,
            3,
        ),
        (
            "inspect_protected_runtime_trial_prerequisites(uuid,uuid,uuid,boolean)",
            _INSPECT_ORIGIN_FIELDS,
            _INSPECT_MULTI_ORIGIN_FIELDS,
            1,
        ),
        (
            "inspect_protected_runtime_trial_prerequisites(uuid,uuid,uuid,boolean)",
            _INSPECT_ORIGIN_BINDING,
            _INSPECT_MULTI_ORIGIN_BINDING,
            1,
        ),
        (
            "inspect_protected_runtime_trial_prerequisites(uuid,uuid,uuid,boolean)",
            _INSPECT_INITIAL_PUBLIC_BINDING,
            _INSPECT_MULTI_PUBLIC_BINDING,
            1,
        ),
        (
            "claim_staging_assigned_trial(uuid,text,jsonb)",
            _CLAIM_INITIAL_CANDIDATE,
            _CLAIM_MULTI_CANDIDATE,
            1,
        ),
        (
            "claim_staging_assigned_trial(uuid,text,jsonb)",
            _CLAIM_INITIAL_UPDATE,
            _CLAIM_MULTI_UPDATE,
            1,
        ),
        (
            "claim_staging_assigned_trial(uuid,text,jsonb)",
            _CLAIM_INITIAL_ATTEMPT_FIELDS,
            _CLAIM_MULTI_ATTEMPT_FIELDS,
            1,
        ),
        (
            "claim_staging_assigned_trial(uuid,text,jsonb)",
            _CLAIM_INITIAL_RESERVATION_ATTEMPT,
            _CLAIM_MULTI_RESERVATION_ATTEMPT,
            1,
        ),
        (
            "close_protected_runtime_trial_claim(uuid,text,text,uuid,integer)",
            _TERMINAL_INITIAL_BINDING,
            _TERMINAL_MULTI_BINDING,
            1,
        ),
        (
            "enforce_executable_claim_assignment()",
            _EXACT_ASSIGNMENT_INITIAL_EPOCH,
            _EXACT_ASSIGNMENT_RETRY_EPOCH,
            1,
        ),
    )
    if not upgrade:
        pairs = tuple((function, new, old, count) for function, old, new, count in pairs)
    for function, old, new, count in pairs:
        _replace_function_clause(function, old, new, expected_count=count)


def _install_retry_function() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.retry_staging_claimed_trial(
          p_worker_id uuid,
          p_worker_credential text,
          p_retry_request jsonb
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_session jsonb;
          v_current record;
          v_transition_id uuid := gen_random_uuid();
          v_transition_payload jsonb;
          v_transition_digest text;
          v_next_attempt_id uuid := gen_random_uuid();
          v_next_attempt_count integer;
          v_next_attempt_at timestamptz;
          v_readiness jsonb;
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION 'protected staging retry requires a SERIALIZABLE transaction'
              USING ERRCODE = '25000';
          END IF;
          IF jsonb_typeof(p_retry_request) IS DISTINCT FROM 'object'
             OR octet_length(p_retry_request::text) > 65536
             OR p_retry_request - ARRAY[
                  'schema_version', 'trial_id', 'worker_id', 'failure_reason',
                  'failure_message', 'retry_after_sec'
                ] <> '{{}}'::jsonb
             OR (SELECT count(*) FROM jsonb_object_keys(p_retry_request)) <> 6
             OR jsonb_typeof(p_retry_request->'schema_version') IS DISTINCT FROM 'number'
             OR (p_retry_request->>'schema_version')::integer IS DISTINCT FROM 1
             OR jsonb_typeof(p_retry_request->'trial_id') IS DISTINCT FROM 'string'
             OR (p_retry_request->>'trial_id' ~
                  '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
                  IS DISTINCT FROM true
             OR jsonb_typeof(p_retry_request->'worker_id') IS DISTINCT FROM 'string'
             OR (p_retry_request->>'worker_id' ~
                  '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
                  IS DISTINCT FROM true
             OR (p_retry_request->>'worker_id')::uuid IS DISTINCT FROM p_worker_id
             OR jsonb_typeof(p_retry_request->'failure_reason') IS DISTINCT FROM 'string'
             OR p_retry_request->>'failure_reason' NOT IN (
                  'agent_error', 'agent_timeout', 'env_start_failure',
                  'env_healthcheck_failed', 'verifier_error', 'verifier_timeout',
                  'artifact_upload_failed', 'missing_required_artifacts',
                  'trajectory_flush_failed', 'task_image_build_timeout',
                  'node_setup_health', 'task_compatibility', 'exhausted_retries',
                  'worker_lost_claim', 'internal_error', 'provider_error',
                  'gateway_error', 'provider_transport_disconnect'
                )
             OR jsonb_typeof(p_retry_request->'failure_message') NOT IN (
                  'string', 'null'
                )
             OR jsonb_typeof(p_retry_request->'retry_after_sec') IS DISTINCT FROM 'number'
             OR (p_retry_request->>'retry_after_sec')::double precision < 0 THEN
            RAISE EXCEPTION 'protected staging retry request is malformed'
              USING ERRCODE = '22023';
          END IF;

          v_session := {SCHEMA}.assert_staging_worker_session(
            p_worker_id, p_worker_credential
          );
          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;

          SELECT runtime.trial_id, runtime.protected_attempt_id,
                 runtime.attempt_sequence, runtime.public_requires_caps,
                 runtime.public_requires_caps_canonical,
                 runtime.public_requires_caps_digest,
                 attempt.execution_generation, attempt.requirements_digest,
                 head.transition_sequence,
                 assignment.allowance_id, assignment.plan_id,
                 assignment.admission_incarnation,
                 assignment.manager_allocation_epoch, assignment.pool_id,
                 assignment.shape_instance_id, assignment.submission_intent_id,
                 assignment.payload AS assignment_payload,
                 (submission.payload->>'agent_incarnation')::uuid
                   AS agent_incarnation,
                 trial.attempt_count
            INTO v_current
            FROM {SCHEMA}.protected_runtime_trial_submissions AS runtime
            JOIN {SCHEMA}.protected_runtime_trial_readiness AS readiness
              ON readiness.trial_id = runtime.trial_id
             AND readiness.protected_attempt_id = runtime.protected_attempt_id
            JOIN {SCHEMA}.atomic_trial_submissions AS submission
              ON submission.trial_id = runtime.trial_id
            JOIN {SCHEMA}.trial_attempts AS attempt
              ON attempt.trial_id = runtime.trial_id
             AND attempt.protected_attempt_id = runtime.protected_attempt_id
             AND attempt.attempt_sequence = runtime.attempt_sequence
            JOIN {SCHEMA}.attempt_lifecycle_heads AS head
              ON head.protected_attempt_id = attempt.protected_attempt_id
            JOIN {SCHEMA}.attempt_lifecycle_events AS assignment
              ON assignment.transition_id = head.transition_id
             AND assignment.protected_attempt_id = head.protected_attempt_id
            JOIN {SCHEMA}.executable_claim_leases AS claim
              ON claim.protected_attempt_id = attempt.protected_attempt_id
             AND claim.execution_generation = attempt.execution_generation
             AND claim.requirements_digest = attempt.requirements_digest
            JOIN {SCHEMA}.executable_claim_state AS claim_state
              ON claim_state.intent_id = claim.intent_id
            JOIN public.trials AS trial ON trial.id = runtime.trial_id
            JOIN public.team_quotas AS quota ON quota.team_id = trial.team_id
           WHERE runtime.trial_id = (p_retry_request->>'trial_id')::uuid
             AND runtime.public_attempt_count + 1 = trial.attempt_count
             AND runtime.not_before IS NOT DISTINCT FROM trial.next_attempt_at
             AND trial.state = 'claimed'
             AND trial.worker_id = p_worker_id
             AND trial.started_at IS NULL
             AND trial.cancellation_requested_at IS NULL
             AND (
               p_retry_request->>'failure_reason' = 'node_setup_health'
               OR trial.attempt_count < quota.max_attempts_ceiling
             )
             AND attempt.claim_state = 'queued'
             AND head.lifecycle_state = 'assigned'
             AND head.executable = false
             AND assignment.operation = 'assign'
             AND assignment.previous_state = 'pending-unassigned'
             AND assignment.lifecycle_state = 'assigned'
             AND assignment.transition_sequence = head.transition_sequence
             AND assignment.execution_generation = attempt.execution_generation
             AND assignment.requirements_digest = attempt.requirements_digest
             AND assignment.executable = false
             AND assignment.submission_intent_id =
                   (v_session->>'intent_id')::uuid
             AND assignment.submission_intent_id = claim.intent_id
             AND claim.worker_id = p_worker_id
             AND claim.worker_incarnation =
                   (v_session->>'worker_incarnation')::uuid
             AND claim.lease_state = 'live'
             AND claim.executable = true
             AND claim_state.subject_id = claim.subject_id
             AND claim_state.subject_incarnation = claim.subject_incarnation
             AND claim_state.claim_high_water >= claim.claim_high_water
             AND claim_state.terminal_high_water < claim_state.claim_high_water
             AND NOT EXISTS (
               SELECT 1
                 FROM {SCHEMA}.executable_claim_terminal_events AS terminal
                WHERE terminal.admitted_operation_id = claim.operation_id
                   OR terminal.protected_attempt_id = claim.protected_attempt_id
             )
           FOR UPDATE OF trial, head, claim_state
           FOR KEY SHARE OF runtime, readiness, submission, attempt,
                            assignment, claim, quota;
          IF NOT FOUND THEN
            RETURN NULL;
          END IF;

          v_transition_payload := v_current.assignment_payload || jsonb_build_object(
            'transition_id', v_transition_id,
            'expected_transition_sequence', v_current.transition_sequence,
            'operation', 'cancel',
            'expected_state', 'assigned',
            'target_state', 'cancelled-terminal',
            'transition_reason', 'claimed-attempt-retry'
          );
          v_transition_digest := encode(
            sha256(
              convert_to(
                {SCHEMA}.canonical_executable_publication_payload(
                  v_transition_payload
                ),
                'UTF8'
              )
            ),
            'hex'
          );
          INSERT INTO {SCHEMA}.attempt_lifecycle_events
            (transition_id, protected_attempt_id, execution_generation,
             requirements_digest, transition_sequence, operation,
             previous_state, lifecycle_state, allowance_id, plan_id,
             admission_incarnation, manager_allocation_epoch, pool_id,
             shape_instance_id, submission_intent_id, executable,
             payload, payload_digest)
          VALUES
            (v_transition_id, v_current.protected_attempt_id,
             v_current.execution_generation, v_current.requirements_digest,
             v_current.transition_sequence + 1, 'cancel', 'assigned',
             'cancelled-terminal', v_current.allowance_id, v_current.plan_id,
             v_current.admission_incarnation,
             v_current.manager_allocation_epoch, v_current.pool_id,
             v_current.shape_instance_id, v_current.submission_intent_id,
             false, v_transition_payload, v_transition_digest);

          v_next_attempt_at := statement_timestamp()
            + (p_retry_request->>'retry_after_sec')::double precision
              * interval '1 second';
          UPDATE public.trials AS trial
             SET state = 'protected-pending',
                 worker_id = NULL,
                 failure_reason = p_retry_request->>'failure_reason',
                 failure_message = NULLIF(
                   p_retry_request->>'failure_message', ''
                 ),
                 next_attempt_at = v_next_attempt_at
           WHERE trial.id = v_current.trial_id
             AND trial.state = 'claimed'
             AND trial.worker_id = p_worker_id
             AND trial.started_at IS NULL
             AND trial.attempt_count = v_current.attempt_count;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'protected staging retry public transition raced'
              USING ERRCODE = '40001';
          END IF;

          v_next_attempt_count := v_current.attempt_count;
          IF p_retry_request->>'failure_reason' = 'node_setup_health' THEN
            UPDATE public.trials AS trial
               SET attempt_count = trial.attempt_count - 1
             WHERE trial.id = v_current.trial_id
               AND trial.state = 'protected-pending'
               AND trial.worker_id IS NULL
               AND trial.attempt_count = v_current.attempt_count
            RETURNING trial.attempt_count INTO v_next_attempt_count;
            IF NOT FOUND THEN
              RAISE EXCEPTION 'protected staging retry attempt refund raced'
                USING ERRCODE = '40001';
            END IF;
          END IF;

          INSERT INTO {SCHEMA}.trial_attempts
            (protected_attempt_id, trial_id, execution_generation,
             attempt_sequence, requirements_digest, claim_state,
             assigned_pool, assignment_epoch, worker_id, claim_epoch)
          VALUES
            (v_next_attempt_id, v_current.trial_id,
             v_current.execution_generation + 1,
             v_current.attempt_sequence + 1, v_current.requirements_digest,
             'queued', NULL, NULL, NULL, NULL);
          INSERT INTO {SCHEMA}.protected_runtime_trial_submissions
            (trial_id, protected_attempt_id, attempt_sequence,
             public_attempt_count, not_before, public_requires_caps,
             public_requires_caps_canonical, public_requires_caps_digest)
          VALUES
            (v_current.trial_id, v_next_attempt_id,
             v_current.attempt_sequence + 1, v_next_attempt_count,
             v_next_attempt_at, v_current.public_requires_caps,
             v_current.public_requires_caps_canonical,
             v_current.public_requires_caps_digest);

          v_readiness := {SCHEMA}.publish_protected_runtime_trial_readiness(
            v_current.agent_incarnation, v_current.trial_id, v_next_attempt_id
          );
          RETURN jsonb_build_object(
            'schema_version', 1,
            'trial_id', v_current.trial_id,
            'state', 'protected-pending',
            'protected_attempt_id', v_next_attempt_id,
            'attempt_sequence', v_current.attempt_sequence + 1,
            'execution_generation', v_current.execution_generation + 1,
            'public_attempt_count', v_next_attempt_count,
            'not_before', v_next_attempt_at,
            'readiness', v_readiness,
            'executable', false
          );
        END
        $function$
        """
    )


def upgrade() -> None:
    _, quoted_runtime = _runtime_role()
    op.execute(
        f"LOCK TABLE {SCHEMA}.protected_runtime_trial_readiness, "
        f"{SCHEMA}.protected_runtime_trial_submissions, "
        f"{SCHEMA}.trial_attempts IN ACCESS EXCLUSIVE MODE"
    )
    _upgrade_runtime_ledgers()
    _patch_multi_attempt_functions(upgrade=True)
    _install_retry_function()
    op.execute(f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.{RETRY_FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{RETRY_FUNCTION} TO {quoted_runtime}")


def downgrade() -> None:
    bind = op.get_bind()
    _, quoted_runtime = _runtime_role()
    op.execute(
        f"LOCK TABLE {SCHEMA}.protected_runtime_trial_readiness, "
        f"{SCHEMA}.protected_runtime_trial_submissions, "
        f"{SCHEMA}.trial_attempts IN ACCESS EXCLUSIVE MODE"
    )
    if bind.execute(
        sa.text(
            f"SELECT EXISTS (SELECT 1 FROM {SCHEMA}.trial_attempts WHERE attempt_sequence <> 0)"
        )
    ).scalar_one():
        raise RuntimeError("cannot downgrade guard_0025 while retry attempts exist")
    op.execute(f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{RETRY_FUNCTION} FROM {quoted_runtime}")
    op.execute(f"DROP FUNCTION {SCHEMA}.{RETRY_FUNCTION}")
    _patch_multi_attempt_functions(upgrade=False)
    _downgrade_runtime_ledgers()
