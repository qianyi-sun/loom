"""Inert trusted trial-submission registration.

Revision ID: guard_0007
Revises: guard_0006
Create Date: 2026-08-10
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "guard_0007"
down_revision: str | Sequence[str] | None = "guard_0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"


def _agent_role() -> str:
    role = op.get_context().config.attributes.get("capacity_guard_agent_role")
    if not isinstance(role, str) or not role:
        raise RuntimeError("trial submission migration is missing the validated agent role")
    return op.get_bind().dialect.identifier_preparer.quote(role)


def _install_submission_function() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.register_inert_trial_submission(
          p_agent_incarnation uuid,
          p_payload jsonb,
          p_canonical_payload bytea,
          p_payload_digest text,
          p_requirements_payload bytea,
          p_requirements_digest text
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_trial_id uuid;
          v_attempt_id uuid;
          v_execution_generation bigint;
          v_public_state text;
          v_cancellation_requested_at timestamptz;
          v_public_worker_id uuid;
          v_public_attempt_count integer;
          v_next_attempt_at timestamptz;
          v_autoscaler_pool_name text;
          v_public_requirements jsonb;
          v_requirements jsonb;
          v_normalized_network_policies jsonb;
          v_public_network_count integer;
          v_public_network_distinct_count integer;
          v_canonical_network_policies text;
          v_expected_requirements_payload text;
          v_expected_submission_payload text;
          v_inserted_attempt uuid;
          v_stored_attempt record;
          v_audit_payload jsonb;
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION 'trial submission registration requires a SERIALIZABLE transaction'
              USING ERRCODE = '25000';
          END IF;
          PERFORM {SCHEMA}.assert_inert_agent_binding(
            p_agent_incarnation, p_payload, p_canonical_payload, p_payload_digest
          );
          IF octet_length(p_requirements_payload) > 8388608
             OR convert_from(p_requirements_payload, 'UTF8')::jsonb
                  IS DISTINCT FROM p_payload->'requirements'
             OR p_requirements_digest !~ '^[0-9a-f]{{64}}$'
             OR encode(sha256(p_requirements_payload), 'hex')
                  IS DISTINCT FROM p_requirements_digest
             OR p_payload->>'requirements_digest'
                  IS DISTINCT FROM p_requirements_digest THEN
            RAISE EXCEPTION 'trial submission requirements payload is invalid or oversized'
              USING ERRCODE = '22023';
          END IF;

          IF (SELECT count(*) FROM jsonb_object_keys(p_payload)) <> 20
             OR EXISTS (
               SELECT 1 FROM jsonb_object_keys(p_payload) AS key
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
             OR (p_payload->>'attempt_sequence')::bigint IS DISTINCT FROM 0
             OR (p_payload->>'execution_generation')::bigint IS DISTINCT FROM
                  (p_payload->>'deployment_generation')::bigint
             OR (p_payload->>'executable')::boolean IS DISTINCT FROM false THEN
            RAISE EXCEPTION 'trial submission contract is not an inert initial attempt'
              USING ERRCODE = '22023';
          END IF;

          v_trial_id := (p_payload->>'trial_id')::uuid;
          v_attempt_id := (p_payload->>'protected_attempt_id')::uuid;
          v_execution_generation := (p_payload->>'execution_generation')::bigint;
          IF v_trial_id = v_attempt_id THEN
            RAISE EXCEPTION 'trial and protected attempt identities must be distinct'
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
             OR (v_requirements->>'required_pool' IS NOT NULL
                 AND v_requirements->>'required_pool' NOT IN ('oldlab', 'gb10')) THEN
            RAISE EXCEPTION 'trial submission sealed requirements are invalid'
              USING ERRCODE = '22023';
          END IF;
          IF (
            SELECT count(*) FROM jsonb_array_elements_text(
              v_requirements->'network_policies'
            )
          ) IS DISTINCT FROM (
            SELECT count(DISTINCT policy) FROM jsonb_array_elements_text(
              v_requirements->'network_policies'
            ) AS policy
          ) OR v_requirements->'network_policies' IS DISTINCT FROM (
            SELECT COALESCE(jsonb_agg(policy ORDER BY policy), '[]'::jsonb)
              FROM jsonb_array_elements_text(v_requirements->'network_policies') AS policy
          ) THEN
            RAISE EXCEPTION 'trial submission sealed requirements are not canonical'
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
          v_expected_submission_payload :=
            '{{"agent_incarnation":' ||
              to_jsonb(p_payload->>'agent_incarnation')::text ||
            ',"allocation_epoch":' || (p_payload->>'allocation_epoch') ||
            ',"attempt_sequence":' || (p_payload->>'attempt_sequence') ||
            ',"authority_incarnation":' ||
              to_jsonb(p_payload->>'authority_incarnation')::text ||
            ',"authority_mode":"disabled"' ||
            ',"candidate_digest":' ||
              to_jsonb(p_payload->>'candidate_digest')::text ||
            ',"configuration_generation":' ||
              (p_payload->>'configuration_generation') ||
            ',"deployment_generation":' ||
              (p_payload->>'deployment_generation') ||
            ',"environment_id":' || to_jsonb(p_payload->>'environment_id')::text ||
            ',"executable":' || (p_payload->'executable')::text ||
            ',"execution_generation":' || (p_payload->>'execution_generation') ||
            ',"protected_attempt_id":' ||
              to_jsonb(p_payload->>'protected_attempt_id')::text ||
            ',"reporter_high_water":' || (p_payload->>'reporter_high_water') ||
            ',"reporter_incarnation":' ||
              to_jsonb(p_payload->>'reporter_incarnation')::text ||
            ',"requirements":' || v_expected_requirements_payload ||
            ',"requirements_digest":' ||
              to_jsonb(p_payload->>'requirements_digest')::text ||
            ',"schema_version":' || (p_payload->>'schema_version') ||
            ',"subject_id":' || to_jsonb(p_payload->>'subject_id')::text ||
            ',"subject_incarnation":' ||
              to_jsonb(p_payload->>'subject_incarnation')::text ||
            ',"trial_id":' || to_jsonb(p_payload->>'trial_id')::text || '}}';
          IF p_requirements_payload IS DISTINCT FROM
               convert_to(v_expected_requirements_payload, 'UTF8')
             OR p_canonical_payload IS DISTINCT FROM
               convert_to(v_expected_submission_payload, 'UTF8') THEN
            RAISE EXCEPTION 'trial submission payload is not canonically encoded'
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

          -- Multiple submissions may register concurrently, but the demand
          -- capture and lifecycle writer must observe either side of every
          -- completed insert. A shared row lock gives that boundary without
          -- serializing unrelated trial registrations with one another.
          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR SHARE;
          PERFORM pg_advisory_xact_lock(1129270867, hashtext(v_trial_id::text));

          SELECT trial.state, trial.cancellation_requested_at, trial.worker_id,
                 trial.attempt_count, trial.next_attempt_at,
                 trial.autoscaler_pool_name, trial.requires_caps
            INTO v_public_state, v_cancellation_requested_at, v_public_worker_id,
                 v_public_attempt_count, v_next_attempt_at,
                 v_autoscaler_pool_name, v_public_requirements
            FROM public.trials AS trial
           WHERE trial.id = v_trial_id;
          IF NOT FOUND
             OR v_public_state IS DISTINCT FROM 'queued'
             OR v_cancellation_requested_at IS NOT NULL
             OR v_public_worker_id IS NOT NULL
             OR v_public_attempt_count IS DISTINCT FROM 0
             OR v_next_attempt_at IS NOT NULL
             OR v_autoscaler_pool_name IS NOT NULL THEN
            RAISE EXCEPTION 'public trial is not an initial runnable submission'
              USING ERRCODE = '55000';
          END IF;
          IF jsonb_typeof(v_public_requirements) IS DISTINCT FROM 'object'
             OR EXISTS (
               SELECT 1 FROM jsonb_object_keys(v_public_requirements) AS key
                WHERE key NOT IN (
                  'os', 'cpu_arch', 'gpu_vendor', 'network_policies', 'worker_pool'
                )
             )
             OR jsonb_typeof(v_public_requirements->'network_policies')
                  IS DISTINCT FROM 'array' THEN
            RAISE EXCEPTION 'public trial requirements are not normalizable'
              USING ERRCODE = '55000';
          END IF;
          SELECT count(*), count(DISTINCT policy),
                 COALESCE(jsonb_agg(policy ORDER BY policy), '[]'::jsonb)
            INTO v_public_network_count, v_public_network_distinct_count,
                 v_normalized_network_policies
            FROM jsonb_array_elements_text(
              v_public_requirements->'network_policies'
            ) AS policy;
          IF v_public_network_count IS DISTINCT FROM v_public_network_distinct_count
             OR v_requirements IS DISTINCT FROM jsonb_build_object(
               'schema_version', 1,
               'os', v_public_requirements->>'os',
               'cpu_arch', v_public_requirements->>'cpu_arch',
               'gpu_vendor', v_public_requirements->>'gpu_vendor',
               'network_policies', v_normalized_network_policies,
               'required_pool', v_public_requirements->'worker_pool'
             ) THEN
            RAISE EXCEPTION 'public requirements differ from the sealed submission'
              USING ERRCODE = '55000';
          END IF;

          INSERT INTO {SCHEMA}.trial_requirements
            (trial_id, schema_version, requirements_digest, requirements)
          VALUES (v_trial_id, 1, p_requirements_digest, v_requirements)
          ON CONFLICT (trial_id) DO NOTHING;
          IF NOT EXISTS (
            SELECT 1 FROM {SCHEMA}.trial_requirements
             WHERE trial_id = v_trial_id
               AND schema_version = 1
               AND requirements_digest = p_requirements_digest
               AND requirements = v_requirements
          ) THEN
            RAISE EXCEPTION 'conflicting inert submission requirements replay'
              USING ERRCODE = '55000';
          END IF;

          INSERT INTO {SCHEMA}.trial_attempts
            (protected_attempt_id, trial_id, execution_generation,
             attempt_sequence, requirements_digest, claim_state,
             assigned_pool, assignment_epoch, worker_id, claim_epoch)
          VALUES
            (v_attempt_id, v_trial_id, v_execution_generation, 0,
             p_requirements_digest, 'queued', NULL, NULL, NULL, NULL)
          ON CONFLICT DO NOTHING
          RETURNING protected_attempt_id INTO v_inserted_attempt;

          SELECT a.protected_attempt_id, a.trial_id, a.execution_generation,
                 a.attempt_sequence, a.requirements_digest, a.claim_state,
                 a.assigned_pool, a.assignment_epoch, a.worker_id, a.claim_epoch
            INTO v_stored_attempt
            FROM {SCHEMA}.trial_attempts AS a
           WHERE a.trial_id = v_trial_id AND a.attempt_sequence = 0
           FOR KEY SHARE;
          IF NOT FOUND
             OR v_stored_attempt.protected_attempt_id IS DISTINCT FROM v_attempt_id
             OR v_stored_attempt.execution_generation IS DISTINCT FROM
                  v_execution_generation
             OR v_stored_attempt.requirements_digest IS DISTINCT FROM
                  p_requirements_digest
             OR v_stored_attempt.claim_state IS DISTINCT FROM 'queued'
             OR v_stored_attempt.assigned_pool IS NOT NULL
             OR v_stored_attempt.assignment_epoch IS NOT NULL
             OR v_stored_attempt.worker_id IS NOT NULL
             OR v_stored_attempt.claim_epoch IS NOT NULL THEN
            RAISE EXCEPTION 'conflicting inert submission replay'
              USING ERRCODE = '55000';
          END IF;

          IF v_inserted_attempt IS NOT NULL THEN
            v_audit_payload := jsonb_build_object(
              'schema_version', 1,
              'trial_id', v_trial_id,
              'protected_attempt_id', v_attempt_id,
              'attempt_sequence', 0,
              'execution_generation', v_execution_generation,
              'requirements_digest', p_requirements_digest,
              'executable', false
            );
            INSERT INTO {SCHEMA}.audit_events
              (event_type, trial_id, protected_attempt_id, payload, payload_digest)
            VALUES
              ('trial_submission_registered.v1', v_trial_id, v_attempt_id,
               v_audit_payload, p_payload_digest);
          ELSIF (
            SELECT count(*) FROM {SCHEMA}.audit_events
             WHERE event_type = 'trial_submission_registered.v1'
               AND trial_id = v_trial_id
               AND protected_attempt_id = v_attempt_id
               AND payload_digest = p_payload_digest
          ) IS DISTINCT FROM 1 THEN
            RAISE EXCEPTION 'conflicting inert submission replay'
              USING ERRCODE = '55000';
          END IF;
          RETURN p_payload;
        END
        $function$
        """
    )


def upgrade() -> None:
    quoted_agent = _agent_role()
    op.add_column(
        "trial_attempts",
        sa.Column(
            "attempt_sequence",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "guard_attempt_sequence_check",
        "trial_attempts",
        "attempt_sequence >= 0",
        schema=SCHEMA,
    )
    op.drop_constraint(
        "guard_attempt_trial_generation_key",
        "trial_attempts",
        schema=SCHEMA,
        type_="unique",
    )
    op.create_unique_constraint(
        "guard_attempt_trial_sequence_key",
        "trial_attempts",
        ["trial_id", "attempt_sequence"],
        schema=SCHEMA,
    )
    op.create_unique_constraint(
        "guard_attempt_trial_execution_identity_key",
        "trial_attempts",
        ["trial_id", "execution_generation", "protected_attempt_id"],
        schema=SCHEMA,
    )
    _install_submission_function()
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.register_inert_trial_submission"
        "(uuid, jsonb, bytea, text, bytea, text) FROM PUBLIC"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.register_inert_trial_submission"
        f"(uuid, jsonb, bytea, text, bytea, text) TO {quoted_agent}"
    )


def downgrade() -> None:
    quoted_agent = _agent_role()
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.register_inert_trial_submission"
        f"(uuid, jsonb, bytea, text, bytea, text) FROM {quoted_agent}"
    )
    op.execute(
        f"DROP FUNCTION {SCHEMA}.register_inert_trial_submission"
        "(uuid, jsonb, bytea, text, bytea, text)"
    )
    op.execute(
        f"""
        DO $block$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM {SCHEMA}.trial_attempts WHERE attempt_sequence <> 0
          ) THEN
            RAISE EXCEPTION
              'cannot downgrade inert submission identity with retry attempts present';
          END IF;
        END
        $block$
        """
    )
    op.drop_constraint(
        "guard_attempt_trial_execution_identity_key",
        "trial_attempts",
        schema=SCHEMA,
        type_="unique",
    )
    op.drop_constraint(
        "guard_attempt_trial_sequence_key",
        "trial_attempts",
        schema=SCHEMA,
        type_="unique",
    )
    op.create_unique_constraint(
        "guard_attempt_trial_generation_key",
        "trial_attempts",
        ["trial_id", "execution_generation"],
        schema=SCHEMA,
    )
    op.drop_constraint(
        "guard_attempt_sequence_check",
        "trial_attempts",
        schema=SCHEMA,
        type_="check",
    )
    op.drop_column("trial_attempts", "attempt_sequence", schema=SCHEMA)
