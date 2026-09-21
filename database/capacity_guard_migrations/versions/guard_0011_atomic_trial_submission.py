"""Atomic public/protected trial submission.

Revision ID: guard_0011
Revises: guard_0010
Create Date: 2026-08-13
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "guard_0011"
down_revision: str | Sequence[str] | None = "guard_0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"


def _agent_role() -> str:
    config = op.get_context().config
    if config is None:
        raise RuntimeError("atomic trial submission migration is missing Alembic config")
    role = config.attributes.get("capacity_guard_agent_role")
    if not isinstance(role, str) or not role:
        raise RuntimeError("atomic trial submission migration is missing the validated agent role")
    return op.get_bind().dialect.identifier_preparer.quote(role)


def _install_atomic_lifecycle_capture() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.capture_lifecycle_demand_observation(
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
          v_payload jsonb;
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
             AND trial.autoscaler_pool_name IS NULL;

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
             AND trial.autoscaler_pool_name IS NULL;
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
             AND trial.autoscaler_pool_name IS NULL;
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


def upgrade() -> None:
    quoted_agent = _agent_role()
    op.add_column(
        "authority_state",
        sa.Column(
            "lifecycle_environment",
            sa.Text(),
            sa.Computed("environment_id", persisted=True),
            nullable=False,
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "authority_state",
        sa.Column(
            "lifecycle_namespace",
            sa.Text(),
            sa.Computed(
                "CASE environment_id "
                "WHEN 'development' THEN 'loom-dev' "
                "WHEN 'staging' THEN 'loom-staging' "
                "WHEN 'production' THEN 'loom-prod' "
                "ELSE 'loom-' || environment_id END",
                persisted=True,
            ),
            nullable=False,
        ),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "guard_authority_lifecycle_scope_check",
        "authority_state",
        "environment_id IN ('development', 'staging', 'production') OR "
        "environment_id ~ '^dev-[a-z]([-a-z0-9]{0,18}[a-z0-9])?$'",
        schema=SCHEMA,
    )
    op.create_table(
        "atomic_trial_submissions",
        sa.Column("trial_id", sa.Uuid(), nullable=False),
        sa.Column("protected_attempt_id", sa.Uuid(), nullable=False),
        sa.Column("lifecycle_authority_id", sa.Uuid(), nullable=False),
        sa.Column("idempotency_key", sa.Text(), nullable=True),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("canonical_payload", sa.LargeBinary(), nullable=False),
        sa.Column("payload_digest", sa.Text(), nullable=False),
        sa.Column("protected_payload", postgresql.JSONB(), nullable=False),
        sa.Column("protected_canonical_payload", sa.LargeBinary(), nullable=False),
        sa.Column("protected_payload_digest", sa.Text(), nullable=False),
        sa.Column("requirements_digest", sa.Text(), nullable=False),
        sa.Column("lifecycle_environment", sa.Text(), nullable=False),
        sa.Column("lifecycle_namespace", sa.Text(), nullable=False),
        sa.Column("submitted_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.CheckConstraint(
            "octet_length(payload::text) <= 8388608 AND "
            "octet_length(canonical_payload) <= 8388608 AND "
            "octet_length(protected_payload::text) <= 8388608 AND "
            "octet_length(protected_canonical_payload) <= 8388608",
            name="guard_atomic_submission_payload_bounds_check",
        ),
        sa.CheckConstraint(
            "payload_digest ~ '^[0-9a-f]{64}$' AND "
            "protected_payload_digest ~ '^[0-9a-f]{64}$' AND "
            "requirements_digest ~ '^[0-9a-f]{64}$'",
            name="guard_atomic_submission_digest_check",
        ),
        sa.CheckConstraint(
            "idempotency_key IS NULL OR char_length(idempotency_key) BETWEEN 1 AND 1024",
            name="guard_atomic_submission_idempotency_check",
        ),
        sa.CheckConstraint(
            "trial_id <> protected_attempt_id AND "
            "(payload->>'trial_id')::uuid = trial_id AND "
            "(payload->>'protected_attempt_id')::uuid = protected_attempt_id AND "
            "(protected_payload->>'trial_id')::uuid = trial_id AND "
            "(protected_payload->>'protected_attempt_id')::uuid = protected_attempt_id AND "
            "payload->>'idempotency_key' IS NOT DISTINCT FROM idempotency_key AND "
            "payload->>'requirements_digest' = requirements_digest AND "
            "protected_payload->>'requirements_digest' = requirements_digest",
            name="guard_atomic_submission_identity_check",
        ),
        sa.ForeignKeyConstraint(
            ["trial_id"],
            ["public.trials.id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["protected_attempt_id", "trial_id"],
            [
                f"{SCHEMA}.trial_attempts.protected_attempt_id",
                f"{SCHEMA}.trial_attempts.trial_id",
            ],
            ondelete="RESTRICT",
            name="guard_atomic_submission_attempt_binding_fk",
        ),
        sa.ForeignKeyConstraint(
            ["lifecycle_authority_id"],
            ["public.data_lifecycle_authorities.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("trial_id"),
        sa.UniqueConstraint("protected_attempt_id", name="guard_atomic_submission_attempt_key"),
        sa.UniqueConstraint("lifecycle_authority_id", name="guard_atomic_submission_lifecycle_key"),
        sa.UniqueConstraint("idempotency_key", name="guard_atomic_submission_idempotency_key"),
        schema=SCHEMA,
    )
    op.execute(
        f"""
        CREATE TRIGGER atomic_trial_submissions_append_only_row
        BEFORE UPDATE OR DELETE ON {SCHEMA}.atomic_trial_submissions
        FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER atomic_trial_submissions_append_only_truncate
        BEFORE TRUNCATE ON {SCHEMA}.atomic_trial_submissions
        FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.submit_inert_trial_projection(
          p_agent_incarnation uuid,
          p_payload jsonb,
          p_canonical_payload bytea,
          p_payload_digest text,
          p_protected_payload jsonb,
          p_protected_canonical_payload bytea,
          p_protected_payload_digest text,
          p_requirements_payload bytea,
          p_requirements_digest text
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_requested_trial_id uuid;
          v_trial_id uuid;
          v_protected_attempt_id uuid;
          v_lifecycle_authority_id uuid;
          v_submitted_at timestamptz;
          v_idempotency_key text;
          v_requirements jsonb;
          v_public_requirements jsonb;
          v_canonical_network_policies text;
          v_expected_requirements_payload text;
          v_expected_protected_payload text;
          v_scope record;
          v_stored_submission record;
          v_protected_attempt record;
          v_updated_rows bigint;
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION 'atomic trial submission requires a SERIALIZABLE transaction'
              USING ERRCODE = '25000';
          END IF;
          PERFORM {SCHEMA}.assert_inert_agent_binding(
            p_agent_incarnation, p_payload, p_canonical_payload, p_payload_digest
          );
          PERFORM {SCHEMA}.assert_inert_agent_binding(
            p_agent_incarnation, p_protected_payload,
            p_protected_canonical_payload, p_protected_payload_digest
          );

          IF octet_length(p_requirements_payload) > 8388608
             OR p_requirements_digest !~ '^[0-9a-f]{{64}}$'
             OR encode(sha256(p_requirements_payload), 'hex')
                  IS DISTINCT FROM p_requirements_digest THEN
            RAISE EXCEPTION 'atomic trial submission requirements payload is invalid'
              USING ERRCODE = '22023';
          END IF;
          v_requirements := convert_from(p_requirements_payload, 'UTF8')::jsonb;
          IF jsonb_typeof(v_requirements) IS DISTINCT FROM 'object'
             OR (SELECT count(*) FROM jsonb_object_keys(v_requirements)) <> 6
             OR EXISTS (
               SELECT 1 FROM jsonb_object_keys(v_requirements) AS key
                WHERE key NOT IN (
                  'schema_version', 'os', 'cpu_arch', 'gpu_vendor',
                  'network_policies', 'required_pool'
                )
             )
             OR (v_requirements->>'schema_version')::integer IS DISTINCT FROM 1
             OR v_requirements->>'os' NOT IN ('linux', 'windows')
             OR v_requirements->>'cpu_arch' NOT IN ('x86_64', 'arm64', 'any')
             OR v_requirements->>'gpu_vendor' NOT IN ('none', 'nvidia')
             OR jsonb_typeof(v_requirements->'network_policies') IS DISTINCT FROM 'array'
             OR jsonb_array_length(v_requirements->'network_policies') > 3
             OR EXISTS (
               SELECT 1
                 FROM jsonb_array_elements_text(v_requirements->'network_policies') AS policy
                WHERE policy NOT IN ('public', 'no-network', 'allowlist')
             )
             OR (
               v_requirements->>'required_pool' IS NOT NULL
               AND v_requirements->>'required_pool' NOT IN ('oldlab', 'gb10')
             )
             OR (
               SELECT count(*) FROM jsonb_array_elements_text(
                 v_requirements->'network_policies'
               )
             ) IS DISTINCT FROM (
               SELECT count(DISTINCT policy) FROM jsonb_array_elements_text(
                 v_requirements->'network_policies'
               ) AS policy
             )
             OR v_requirements->'network_policies' IS DISTINCT FROM (
               SELECT COALESCE(jsonb_agg(policy ORDER BY policy), '[]'::jsonb)
                 FROM jsonb_array_elements_text(
                   v_requirements->'network_policies'
                 ) AS policy
             ) THEN
            RAISE EXCEPTION 'atomic trial submission requirements payload is invalid'
              USING ERRCODE = '22023';
          END IF;
          SELECT '[' || COALESCE(
                   string_agg(to_jsonb(policy)::text, ',' ORDER BY policy), ''
                 ) || ']'
            INTO v_canonical_network_policies
            FROM jsonb_array_elements_text(
              v_requirements->'network_policies'
            ) AS policy;
          v_expected_requirements_payload :=
            '{{"cpu_arch":' || to_jsonb(v_requirements->>'cpu_arch')::text ||
            ',"gpu_vendor":' || to_jsonb(v_requirements->>'gpu_vendor')::text ||
            ',"network_policies":' || v_canonical_network_policies ||
            ',"os":' || to_jsonb(v_requirements->>'os')::text ||
            ',"required_pool":' || (v_requirements->'required_pool')::text ||
            ',"schema_version":' || (v_requirements->>'schema_version') || '}}';
          IF p_requirements_payload IS DISTINCT FROM
               convert_to(v_expected_requirements_payload, 'UTF8') THEN
            RAISE EXCEPTION 'atomic trial submission requirements are not canonical'
              USING ERRCODE = '22023';
          END IF;

          IF (p_protected_payload->>'trial_id')::uuid IS NOT DISTINCT FROM
               (p_protected_payload->>'protected_attempt_id')::uuid THEN
            RAISE EXCEPTION 'atomic trial submission protected identities must be distinct'
              USING ERRCODE = '22023';
          END IF;
          IF jsonb_typeof(p_protected_payload) IS DISTINCT FROM 'object'
             OR (SELECT count(*) FROM jsonb_object_keys(p_protected_payload)) <> 20
             OR EXISTS (
               SELECT 1 FROM jsonb_object_keys(p_protected_payload) AS key
                WHERE key NOT IN (
                  'schema_version', 'environment_id', 'subject_id',
                  'subject_incarnation', 'authority_incarnation',
                  'agent_incarnation', 'reporter_incarnation', 'authority_mode',
                  'allocation_epoch', 'reporter_high_water', 'candidate_digest',
                  'deployment_generation', 'configuration_generation', 'trial_id',
                  'protected_attempt_id', 'attempt_sequence', 'execution_generation',
                  'requirements', 'requirements_digest', 'executable'
                )
             )
             OR (p_protected_payload->>'attempt_sequence')::bigint IS DISTINCT FROM 0
             OR (p_protected_payload->>'execution_generation')::bigint IS DISTINCT FROM
                  (p_protected_payload->>'deployment_generation')::bigint
             OR (p_protected_payload->>'executable')::boolean IS DISTINCT FROM false
             OR p_protected_payload->'requirements' IS DISTINCT FROM v_requirements
             OR p_protected_payload->>'requirements_digest'
                  IS DISTINCT FROM p_requirements_digest THEN
            RAISE EXCEPTION 'atomic trial submission protected payload is invalid'
              USING ERRCODE = '22023';
          END IF;
          v_expected_protected_payload :=
            '{{"agent_incarnation":' ||
              to_jsonb(p_protected_payload->>'agent_incarnation')::text ||
            ',"allocation_epoch":' || (p_protected_payload->>'allocation_epoch') ||
            ',"attempt_sequence":' || (p_protected_payload->>'attempt_sequence') ||
            ',"authority_incarnation":' ||
              to_jsonb(p_protected_payload->>'authority_incarnation')::text ||
            ',"authority_mode":"disabled"' ||
            ',"candidate_digest":' ||
              to_jsonb(p_protected_payload->>'candidate_digest')::text ||
            ',"configuration_generation":' ||
              (p_protected_payload->>'configuration_generation') ||
            ',"deployment_generation":' ||
              (p_protected_payload->>'deployment_generation') ||
            ',"environment_id":' ||
              to_jsonb(p_protected_payload->>'environment_id')::text ||
            ',"executable":' || (p_protected_payload->'executable')::text ||
            ',"execution_generation":' ||
              (p_protected_payload->>'execution_generation') ||
            ',"protected_attempt_id":' ||
              to_jsonb(p_protected_payload->>'protected_attempt_id')::text ||
            ',"reporter_high_water":' ||
              (p_protected_payload->>'reporter_high_water') ||
            ',"reporter_incarnation":' ||
              to_jsonb(p_protected_payload->>'reporter_incarnation')::text ||
            ',"requirements":' || v_expected_requirements_payload ||
            ',"requirements_digest":' ||
              to_jsonb(p_protected_payload->>'requirements_digest')::text ||
            ',"schema_version":' || (p_protected_payload->>'schema_version') ||
            ',"subject_id":' || to_jsonb(p_protected_payload->>'subject_id')::text ||
            ',"subject_incarnation":' ||
              to_jsonb(p_protected_payload->>'subject_incarnation')::text ||
            ',"trial_id":' || to_jsonb(p_protected_payload->>'trial_id')::text || '}}';
          IF p_protected_canonical_payload IS DISTINCT FROM
               convert_to(v_expected_protected_payload, 'UTF8') THEN
            RAISE EXCEPTION 'atomic trial submission protected payload is not canonical'
              USING ERRCODE = '22023';
          END IF;

          -- Validate the lexical integer contract in a separate statement so
          -- no cast below can surface an implementation-level database error.
          IF jsonb_typeof(p_payload) IS DISTINCT FROM 'object'
             OR jsonb_typeof(p_payload->'submit_priority') IS DISTINCT FROM 'number'
             OR COALESCE(
               p_payload->>'submit_priority' !~ '^(0|[1-9][0-9]*)$', true
             )
             OR jsonb_typeof(p_payload->'sample_idx') IS DISTINCT FROM 'number'
             OR COALESCE(p_payload->>'sample_idx' !~ '^(0|[1-9][0-9]*)$', true)
             OR jsonb_typeof(p_payload->'combination_idx') IS DISTINCT FROM 'number'
             OR COALESCE(
               p_payload->>'combination_idx' !~ '^(0|[1-9][0-9]*)$', true
             ) THEN
            RAISE EXCEPTION 'atomic trial submission payload is invalid'
              USING ERRCODE = '22023';
          END IF;

          IF jsonb_typeof(p_payload) IS DISTINCT FROM 'object'
             OR (SELECT count(*) FROM jsonb_object_keys(p_payload)) <> 34
             OR EXISTS (
               SELECT 1 FROM jsonb_object_keys(p_payload) AS key
                WHERE key NOT IN (
                  'schema_version', 'environment_id', 'subject_id',
                  'subject_incarnation', 'authority_incarnation',
                  'agent_incarnation', 'reporter_incarnation', 'authority_mode',
                  'allocation_epoch', 'reporter_high_water', 'candidate_digest',
                  'deployment_generation', 'configuration_generation', 'trial_id',
                  'protected_attempt_id', 'attempt_sequence', 'execution_generation',
                  'requirements', 'requirements_digest', 'executable', 'team_id',
                  'task_id', 'config', 'submit_priority', 'batch_id',
                  'idempotency_key', 'sample_idx', 'combination_idx',
                  'provider_connection_id', 'provider_model_id',
                  'submitted_by_user_id', 'usage_attributed_user_id',
                  'usage_attributed_actor', 'family_key'
                )
             )
             OR jsonb_typeof(p_payload->'team_id') IS DISTINCT FROM 'string'
             OR jsonb_typeof(p_payload->'task_id') IS DISTINCT FROM 'string'
             OR char_length(p_payload->>'task_id') NOT BETWEEN 1 AND 1024
             OR jsonb_typeof(p_payload->'config') IS DISTINCT FROM 'object'
             OR jsonb_typeof(p_payload->'submit_priority') IS DISTINCT FROM 'number'
             OR jsonb_typeof(p_payload->'batch_id') NOT IN ('string', 'null')
             OR jsonb_typeof(p_payload->'idempotency_key') NOT IN ('string', 'null')
             OR (
               p_payload->>'idempotency_key' IS NOT NULL
               AND char_length(p_payload->>'idempotency_key') NOT BETWEEN 1 AND 1024
             )
             OR jsonb_typeof(p_payload->'sample_idx') IS DISTINCT FROM 'number'
             OR jsonb_typeof(p_payload->'combination_idx') IS DISTINCT FROM 'number'
             OR jsonb_typeof(p_payload->'provider_connection_id') NOT IN ('string', 'null')
             OR jsonb_typeof(p_payload->'provider_model_id') NOT IN ('string', 'null')
             OR (
               p_payload->>'provider_model_id' IS NOT NULL
               AND char_length(p_payload->>'provider_model_id') NOT BETWEEN 1 AND 1024
             )
             OR jsonb_typeof(p_payload->'submitted_by_user_id') NOT IN ('string', 'null')
             OR jsonb_typeof(p_payload->'usage_attributed_user_id')
                  NOT IN ('string', 'null')
             OR jsonb_typeof(p_payload->'usage_attributed_actor') NOT IN ('string', 'null')
             OR (
               p_payload->>'usage_attributed_actor' IS NOT NULL
               AND char_length(p_payload->>'usage_attributed_actor') NOT BETWEEN 1 AND 1024
             )
             OR jsonb_typeof(p_payload->'family_key') NOT IN ('string', 'null')
             OR (
               p_payload->>'family_key' IS NOT NULL
               AND char_length(p_payload->>'family_key') NOT BETWEEN 1 AND 1024
             )
             OR (p_payload->>'trial_id')::uuid IS DISTINCT FROM
                  (p_protected_payload->>'trial_id')::uuid
             OR (p_payload->>'protected_attempt_id')::uuid IS DISTINCT FROM
                  (p_protected_payload->>'protected_attempt_id')::uuid
             OR (p_payload->>'trial_id')::uuid IS NOT DISTINCT FROM
                  (p_payload->>'protected_attempt_id')::uuid
             OR (p_payload->>'execution_generation')::bigint IS DISTINCT FROM
                  (p_protected_payload->>'execution_generation')::bigint
             OR p_payload->'requirements' IS DISTINCT FROM v_requirements
             OR p_payload->'requirements' IS DISTINCT FROM
                  p_protected_payload->'requirements'
             OR p_payload->>'requirements_digest' IS DISTINCT FROM
                  p_protected_payload->>'requirements_digest'
             OR p_payload->>'requirements_digest' IS DISTINCT FROM p_requirements_digest
             OR (p_payload->>'attempt_sequence')::bigint IS DISTINCT FROM 0
             OR (p_payload->>'executable')::boolean IS DISTINCT FROM false
             OR (p_payload->>'submit_priority')::numeric NOT BETWEEN 0 AND 2147483647
             OR (p_payload->>'sample_idx')::numeric NOT BETWEEN 0 AND 2147483647
             OR (p_payload->>'combination_idx')::numeric NOT BETWEEN 0 AND 2147483647 THEN
            RAISE EXCEPTION 'atomic trial submission payload is invalid'
              USING ERRCODE = '22023';
          END IF;
          IF p_canonical_payload IS DISTINCT FROM convert_to(p_payload::text, 'UTF8') THEN
            RAISE EXCEPTION 'atomic trial submission payload is not canonical'
              USING ERRCODE = '22023';
          END IF;

          IF NOT EXISTS (
            SELECT 1 FROM {SCHEMA}.claim_guard_activation
             WHERE singleton_id = 1
               AND activation_state = 'disabled'
               AND authority_mode = 'disabled'
               AND activation_epoch = 0
               AND executable_new_capacity_ceiling = 0
               AND live_claim_entry_enabled = false
          ) THEN
            RAISE EXCEPTION 'claim guard is not in its immutable disabled state'
              USING ERRCODE = '55000';
          END IF;

          SELECT lifecycle_environment, lifecycle_namespace
            INTO v_scope
            FROM {SCHEMA}.authority_state
           WHERE singleton_id = 1
           FOR KEY SHARE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'atomic trial submission lifecycle scope is unavailable'
              USING ERRCODE = '55000';
          END IF;
          IF v_scope.lifecycle_environment = 'staging' THEN
            RAISE EXCEPTION
              'atomic trial submission protected retention is unavailable for staging'
              USING ERRCODE = '55000';
          END IF;

          v_requested_trial_id := (p_payload->>'trial_id')::uuid;
          v_idempotency_key := p_payload->>'idempotency_key';
          v_public_requirements := jsonb_build_object(
            'os', v_requirements->'os',
            'cpu_arch', v_requirements->'cpu_arch',
            'gpu_vendor', v_requirements->'gpu_vendor',
            'network_policies', v_requirements->'network_policies'
          );
          IF v_requirements->>'required_pool' IS NOT NULL THEN
            v_public_requirements := v_public_requirements || jsonb_build_object(
              'worker_pool', v_requirements->'required_pool'
            );
          END IF;

          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR SHARE;
          IF v_idempotency_key IS NULL THEN
            PERFORM pg_advisory_xact_lock(
              1129270867, hashtext(v_requested_trial_id::text)
            );
            SELECT s.*
              INTO v_stored_submission
              FROM {SCHEMA}.atomic_trial_submissions AS s
             WHERE s.trial_id = v_requested_trial_id
             FOR KEY SHARE;
          ELSE
            PERFORM pg_advisory_xact_lock(
              1129270868, hashtext(v_idempotency_key)
            );
            SELECT s.*
              INTO v_stored_submission
              FROM {SCHEMA}.atomic_trial_submissions AS s
             WHERE s.idempotency_key = v_idempotency_key
             FOR KEY SHARE;
          END IF;

          IF FOUND THEN
            IF v_stored_submission.lifecycle_environment IS DISTINCT FROM
                 v_scope.lifecycle_environment
               OR v_stored_submission.lifecycle_namespace IS DISTINCT FROM
                    v_scope.lifecycle_namespace
               OR v_stored_submission.requirements_digest IS DISTINCT FROM
                    p_requirements_digest
               OR (
                 v_idempotency_key IS NULL
                 AND (
                   v_stored_submission.trial_id IS DISTINCT FROM v_requested_trial_id
                   OR v_stored_submission.protected_attempt_id IS DISTINCT FROM
                        (p_payload->>'protected_attempt_id')::uuid
                   OR v_stored_submission.canonical_payload IS DISTINCT FROM
                        p_canonical_payload
                   OR v_stored_submission.protected_canonical_payload IS DISTINCT FROM
                        p_protected_canonical_payload
                 )
               )
               OR (
                 v_idempotency_key IS NOT NULL
                 AND (
                   (v_stored_submission.payload - 'trial_id' - 'protected_attempt_id')
                     IS DISTINCT FROM (p_payload - 'trial_id' - 'protected_attempt_id')
                   OR (
                     v_stored_submission.protected_payload
                       - 'trial_id' - 'protected_attempt_id'
                   ) IS DISTINCT FROM (
                     p_protected_payload - 'trial_id' - 'protected_attempt_id'
                   )
                 )
               ) THEN
              RAISE EXCEPTION 'conflicting atomic trial submission replay'
                USING ERRCODE = '55000';
            END IF;
            RETURN jsonb_build_object(
              'schema_version', 1,
              'trial_id', v_stored_submission.trial_id,
              'protected_attempt_id', v_stored_submission.protected_attempt_id,
              'lifecycle_authority_id', v_stored_submission.lifecycle_authority_id,
              'requirements_digest', v_stored_submission.requirements_digest,
              'submitted_at', v_stored_submission.submitted_at,
              'replayed', true,
              'executable', false
            );
          END IF;

          INSERT INTO public.trials
            (id, team_id, task_id, config, requires_caps, state, submit_priority,
             batch_id, idempotency_key, sample_idx, combination_idx,
             provider_connection_id, provider_model_id, submitted_by_user_id,
             usage_attributed_user_id, usage_attributed_actor, family_key)
          VALUES
            (v_requested_trial_id, (p_payload->>'team_id')::uuid, p_payload->>'task_id',
             p_payload->'config', v_public_requirements, 'queued',
             (p_payload->>'submit_priority')::integer,
             (p_payload->>'batch_id')::uuid, v_idempotency_key,
             (p_payload->>'sample_idx')::integer,
             (p_payload->>'combination_idx')::integer,
             (p_payload->>'provider_connection_id')::uuid,
             p_payload->>'provider_model_id',
             (p_payload->>'submitted_by_user_id')::uuid,
             (p_payload->>'usage_attributed_user_id')::uuid,
             p_payload->>'usage_attributed_actor', p_payload->>'family_key')
          ON CONFLICT DO NOTHING
          RETURNING id, submitted_at INTO v_trial_id, v_submitted_at;
          IF v_trial_id IS NULL THEN
            RAISE EXCEPTION 'public trial collision lacks atomic submission provenance'
              USING ERRCODE = '55000';
          END IF;

          INSERT INTO public.data_lifecycle_authorities
            (environment, namespace, team_id, data_class, owner_kind, owner_id,
             created_at, expires_at, pinned, state)
          VALUES
            (v_scope.lifecycle_environment, v_scope.lifecycle_namespace,
             (p_payload->>'team_id')::uuid, 'trial', 'trial', v_trial_id::text,
             v_submitted_at,
             CASE WHEN v_scope.lifecycle_environment = 'staging'
                  THEN v_submitted_at + interval '7 days' ELSE NULL END,
             v_scope.lifecycle_environment <> 'staging', 'active')
          RETURNING id INTO v_lifecycle_authority_id;

          UPDATE public.trials AS t
             SET lifecycle_authority_id = v_lifecycle_authority_id
           WHERE t.id = v_trial_id
             AND t.lifecycle_authority_id IS NULL;
          GET DIAGNOSTICS v_updated_rows = ROW_COUNT;
          IF v_updated_rows IS DISTINCT FROM 1 THEN
            RAISE EXCEPTION 'trial lifecycle authority binding raced'
              USING ERRCODE = '40001';
          END IF;

          PERFORM {SCHEMA}.register_inert_trial_submission(
            p_agent_incarnation, p_protected_payload,
            p_protected_canonical_payload, p_protected_payload_digest,
            p_requirements_payload, p_requirements_digest
          );

          -- The existing registration function validates the legacy queued
          -- projection. Keep that state transaction-local, then fence the
          -- public row before commit so no legacy worker can claim it.
          UPDATE public.trials AS t
             SET state = 'protected-pending'
           WHERE t.id = v_trial_id
             AND t.state = 'queued'
             AND t.worker_id IS NULL
             AND t.attempt_count = 0
             AND t.cancellation_requested_at IS NULL
             AND t.next_attempt_at IS NULL
             AND t.autoscaler_pool_name IS NULL;
          GET DIAGNOSTICS v_updated_rows = ROW_COUNT;
          IF v_updated_rows IS DISTINCT FROM 1 THEN
            RAISE EXCEPTION 'atomic trial submission public fence drifted'
              USING ERRCODE = '40001';
          END IF;

          SELECT a.protected_attempt_id, a.execution_generation,
                 a.requirements_digest, a.claim_state, a.assigned_pool,
                 a.assignment_epoch, a.worker_id, a.claim_epoch,
                 h.transition_sequence, h.lifecycle_state, h.executable
            INTO v_protected_attempt
            FROM {SCHEMA}.trial_attempts AS a
            JOIN {SCHEMA}.attempt_lifecycle_heads AS h
              ON h.protected_attempt_id = a.protected_attempt_id
           WHERE a.trial_id = v_trial_id
             AND a.attempt_sequence = 0;
          IF NOT FOUND
             OR v_protected_attempt.protected_attempt_id IS DISTINCT FROM
                  (p_payload->>'protected_attempt_id')::uuid
             OR v_protected_attempt.execution_generation IS DISTINCT FROM
                  (p_payload->>'execution_generation')::bigint
             OR v_protected_attempt.requirements_digest IS DISTINCT FROM
                  p_requirements_digest
             OR v_protected_attempt.claim_state IS DISTINCT FROM 'queued'
             OR v_protected_attempt.assigned_pool IS NOT NULL
             OR v_protected_attempt.assignment_epoch IS NOT NULL
             OR v_protected_attempt.worker_id IS NOT NULL
             OR v_protected_attempt.claim_epoch IS NOT NULL
             OR v_protected_attempt.transition_sequence IS DISTINCT FROM 0
             OR v_protected_attempt.lifecycle_state IS DISTINCT FROM
                  'pending-unassigned'
             OR v_protected_attempt.executable IS DISTINCT FROM false THEN
            RAISE EXCEPTION 'atomic trial submission initialization drifted'
              USING ERRCODE = '55000';
          END IF;
          v_protected_attempt_id := v_protected_attempt.protected_attempt_id;

          INSERT INTO {SCHEMA}.atomic_trial_submissions
            (trial_id, protected_attempt_id, lifecycle_authority_id,
             idempotency_key, payload, canonical_payload, payload_digest,
             protected_payload, protected_canonical_payload,
             protected_payload_digest, requirements_digest,
             lifecycle_environment, lifecycle_namespace, submitted_at)
          VALUES
            (v_trial_id, v_protected_attempt_id, v_lifecycle_authority_id,
             v_idempotency_key, p_payload, p_canonical_payload, p_payload_digest,
             p_protected_payload, p_protected_canonical_payload,
             p_protected_payload_digest, p_requirements_digest,
             v_scope.lifecycle_environment, v_scope.lifecycle_namespace, v_submitted_at);

          RETURN jsonb_build_object(
            'schema_version', 1,
            'trial_id', v_trial_id,
            'protected_attempt_id', v_protected_attempt_id,
            'lifecycle_authority_id', v_lifecycle_authority_id,
            'requirements_digest', p_requirements_digest,
            'submitted_at', v_submitted_at,
            'replayed', false,
            'executable', false
          );
        END
        $function$
        """
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.submit_inert_trial_projection"
        "(uuid, jsonb, bytea, text, jsonb, bytea, text, bytea, text) FROM PUBLIC"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.submit_inert_trial_projection"
        f"(uuid, jsonb, bytea, text, jsonb, bytea, text, bytea, text) TO {quoted_agent}"
    )
    op.execute(
        f"ALTER FUNCTION {SCHEMA}.capture_lifecycle_demand_observation"
        "(uuid, bigint, integer) RENAME TO capture_lifecycle_demand_observation_v2_queued"
    )
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.capture_lifecycle_demand_observation_v2_queued"
        f"(uuid, bigint, integer) FROM {quoted_agent}"
    )
    _install_atomic_lifecycle_capture()
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.capture_lifecycle_demand_observation"
        "(uuid, bigint, integer) FROM PUBLIC"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION "
        f"{SCHEMA}.capture_lifecycle_demand_observation_v2_queued"
        "(uuid, bigint, integer) FROM PUBLIC"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.capture_lifecycle_demand_observation"
        f"(uuid, bigint, integer) TO {quoted_agent}"
    )


def downgrade() -> None:
    op.execute(f"LOCK TABLE {SCHEMA}.atomic_trial_submissions IN SHARE MODE")
    op.execute(
        f"""
        DO $block$
        BEGIN
          IF EXISTS (SELECT 1 FROM {SCHEMA}.atomic_trial_submissions) THEN
            RAISE EXCEPTION
              'cannot downgrade guard_0011 while atomic trial submissions exist'
              USING ERRCODE = '55000';
          END IF;
        END
        $block$
        """
    )
    quoted_agent = _agent_role()
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.capture_lifecycle_demand_observation"
        f"(uuid, bigint, integer) FROM {quoted_agent}"
    )
    op.execute(
        f"DROP FUNCTION {SCHEMA}.capture_lifecycle_demand_observation(uuid, bigint, integer)"
    )
    op.execute(
        f"ALTER FUNCTION {SCHEMA}.capture_lifecycle_demand_observation_v2_queued"
        "(uuid, bigint, integer) RENAME TO capture_lifecycle_demand_observation"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.capture_lifecycle_demand_observation"
        f"(uuid, bigint, integer) TO {quoted_agent}"
    )
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.submit_inert_trial_projection"
        f"(uuid, jsonb, bytea, text, jsonb, bytea, text, bytea, text) FROM {quoted_agent}"
    )
    op.execute(
        f"DROP FUNCTION {SCHEMA}.submit_inert_trial_projection"
        "(uuid, jsonb, bytea, text, jsonb, bytea, text, bytea, text)"
    )
    op.drop_table("atomic_trial_submissions", schema=SCHEMA)
    op.drop_constraint(
        "guard_authority_lifecycle_scope_check",
        "authority_state",
        schema=SCHEMA,
        type_="check",
    )
    op.drop_column("authority_state", "lifecycle_namespace", schema=SCHEMA)
    op.drop_column("authority_state", "lifecycle_environment", schema=SCHEMA)
