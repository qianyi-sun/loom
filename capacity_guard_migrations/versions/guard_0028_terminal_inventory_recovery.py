"""Import manager-authenticated physical terminal evidence for safe recovery.

Revision ID: guard_0028
Revises: guard_0027
Create Date: 2026-09-05
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "guard_0028"
down_revision: str | Sequence[str] | None = "guard_0027"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"
IMPORT_FUNCTION = (
    "import_executable_terminal_inventory_evidence"
    "(uuid,uuid,jsonb,bytea,text)"
)
REQUEUE_FUNCTION = (
    "transform_protected_runtime_trial_requeue"
    "(uuid,text,uuid,integer,uuid,integer,text,text,timestamp with time zone)"
)
REQUEUE_GUARD_0026_FUNCTION = (
    "transform_protected_runtime_trial_requeue_guard_0026"
    "(uuid,text,uuid,integer,uuid,integer,text,text,timestamp with time zone)"
)
TRIGGER_FUNCTION = "public.loom_transform_protected_runtime_trial_requeue()"


def _agent_role() -> str:
    role = op.get_context().config.attributes.get("capacity_guard_agent_role")
    if not isinstance(role, str) or not role:
        raise RuntimeError("terminal inventory recovery migration is missing the agent role")
    return op.get_bind().dialect.identifier_preparer.quote(role)


def _trigger_function_owner() -> str:
    bind = op.get_bind()
    owner = bind.execute(
        sa.text(
            "SELECT pg_catalog.pg_get_userbyid(routine.proowner) "
            "FROM pg_catalog.pg_proc AS routine "
            "WHERE routine.oid = pg_catalog.to_regprocedure(:signature)"
        ),
        {"signature": TRIGGER_FUNCTION},
    ).scalar_one_or_none()
    if not isinstance(owner, str) or not owner:
        raise RuntimeError(
            "terminal inventory recovery requires the application requeue trigger"
        )
    return bind.dialect.identifier_preparer.quote(owner)


def _install_evidence_gated_requeue(quoted_trigger_owner: str) -> None:
    op.execute(
        f"ALTER FUNCTION {SCHEMA}.{REQUEUE_FUNCTION} "
        "RENAME TO transform_protected_runtime_trial_requeue_guard_0026"
    )
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{REQUEUE_GUARD_0026_FUNCTION} "
        f"FROM {quoted_trigger_owner}"
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.transform_protected_runtime_trial_requeue(
          p_trial_id uuid,
          p_previous_state text,
          p_previous_worker_id uuid,
          p_previous_attempt_count integer,
          p_next_worker_id uuid,
          p_next_attempt_count integer,
          p_failure_reason text,
          p_failure_message text,
          p_next_attempt_at timestamptz
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_current record;
        BEGIN
          -- Serialize the physical-terminal observation with evidence import,
          -- lifecycle mutation, and executable claim admission.
          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'protected requeue authority is unavailable'
              USING ERRCODE = '55000';
          END IF;

          SELECT runtime.protected_attempt_id, claim.intent_id,
                 claim.operation_id AS claim_operation_id,
                 claim.subject_id, claim.subject_incarnation,
                 claim.worker_id, claim.worker_incarnation,
                 claim_state.draining,
                 EXISTS (
                   SELECT 1
                     FROM {SCHEMA}.executable_terminal_inventory_evidence AS evidence
                    WHERE evidence.intent_id = claim.intent_id
                      AND evidence.protected_attempt_id = claim.protected_attempt_id
                      AND evidence.claim_operation_id = claim.operation_id
                      AND evidence.subject_id = claim.subject_id
                      AND evidence.subject_incarnation = claim.subject_incarnation
                      AND evidence.worker_id = claim.worker_id
                      AND evidence.worker_incarnation = claim.worker_incarnation
                 ) AS terminal_evidence_present
            INTO v_current
            FROM {SCHEMA}.protected_runtime_trial_submissions AS runtime
            JOIN {SCHEMA}.trial_attempts AS attempt
              ON attempt.trial_id = runtime.trial_id
             AND attempt.protected_attempt_id = runtime.protected_attempt_id
             AND attempt.attempt_sequence = runtime.attempt_sequence
            JOIN {SCHEMA}.attempt_lifecycle_heads AS head
              ON head.protected_attempt_id = attempt.protected_attempt_id
            JOIN {SCHEMA}.executable_claim_leases AS claim
              ON claim.protected_attempt_id = attempt.protected_attempt_id
             AND claim.execution_generation = attempt.execution_generation
             AND claim.requirements_digest = attempt.requirements_digest
            JOIN {SCHEMA}.executable_claim_state AS claim_state
              ON claim_state.intent_id = claim.intent_id
             AND claim_state.subject_id = claim.subject_id
             AND claim_state.subject_incarnation = claim.subject_incarnation
            JOIN public.trials AS trial ON trial.id = runtime.trial_id
           WHERE runtime.trial_id = p_trial_id
             AND runtime.public_attempt_count + 1 = trial.attempt_count
             AND runtime.not_before IS NOT DISTINCT FROM trial.next_attempt_at
             AND trial.state = p_previous_state
             AND trial.worker_id = p_previous_worker_id
             AND trial.attempt_count = p_previous_attempt_count
             AND trial.cancellation_requested_at IS NULL
             AND attempt.claim_state = 'queued'
             AND head.lifecycle_state = 'assigned'
             AND head.executable = false
             AND claim.worker_id = p_previous_worker_id
             AND claim.lease_state = 'live'
             AND claim.executable = true
             AND claim_state.claim_high_water >= claim.claim_high_water
             AND claim_state.terminal_high_water < claim_state.claim_high_water
             AND NOT EXISTS (
               SELECT 1
                 FROM {SCHEMA}.executable_claim_terminal_events AS terminal
                WHERE terminal.admitted_operation_id = claim.operation_id
                   OR terminal.protected_attempt_id = claim.protected_attempt_id
             )
           FOR UPDATE OF head, claim_state
           FOR KEY SHARE OF runtime, attempt, claim, trial;

          IF FOUND
             AND (
               v_current.draining IS DISTINCT FROM true
               OR v_current.terminal_evidence_present IS DISTINCT FROM true
             ) THEN
            RETURN pg_catalog.jsonb_build_object(
              'state', 'retained',
              'protected_attempt_id', v_current.protected_attempt_id,
              'intent_id', v_current.intent_id,
              'worker_id', v_current.worker_id,
              'worker_incarnation', v_current.worker_incarnation,
              'executable', false
            );
          END IF;

          -- The guard_0026 implementation performs the complete exactness
          -- check and is callable only by this SECURITY DEFINER wrapper.
          RETURN {SCHEMA}.transform_protected_runtime_trial_requeue_guard_0026(
            p_trial_id, p_previous_state, p_previous_worker_id,
            p_previous_attempt_count, p_next_worker_id, p_next_attempt_count,
            p_failure_reason, p_failure_message, p_next_attempt_at
          );
        END
        $function$
        """
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.{REQUEUE_FUNCTION} FROM PUBLIC"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{REQUEUE_FUNCTION} "
        f"TO {quoted_trigger_owner}"
    )


def _uninstall_evidence_gated_requeue(quoted_trigger_owner: str) -> None:
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{REQUEUE_FUNCTION} "
        f"FROM {quoted_trigger_owner}"
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.{REQUEUE_FUNCTION}")
    op.execute(
        f"ALTER FUNCTION {SCHEMA}.{REQUEUE_GUARD_0026_FUNCTION} "
        "RENAME TO transform_protected_runtime_trial_requeue"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{REQUEUE_FUNCTION} "
        f"TO {quoted_trigger_owner}"
    )


def upgrade() -> None:
    quoted_agent = _agent_role()
    quoted_trigger_owner = _trigger_function_owner()
    op.create_table(
        "executable_terminal_inventory_evidence",
        sa.Column("evidence_id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("agent_incarnation", sa.Uuid(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("subject_incarnation", sa.Uuid(), nullable=False),
        sa.Column("intent_id", sa.Uuid(), nullable=False),
        sa.Column("protected_attempt_id", sa.Uuid(), nullable=False),
        sa.Column("claim_operation_id", sa.Uuid(), nullable=False),
        sa.Column("worker_id", sa.Uuid(), nullable=False),
        sa.Column("worker_incarnation", sa.Uuid(), nullable=False),
        sa.Column("physical_job_id", sa.Text(), nullable=False),
        sa.Column("execution_epoch", sa.BigInteger(), nullable=False),
        sa.Column("execution_manifest_sha256", sa.Text(), nullable=False),
        sa.Column("pool_id", sa.Text(), nullable=False),
        sa.Column("pool_generation", sa.BigInteger(), nullable=False),
        sa.Column("executor_id", sa.Text(), nullable=False),
        sa.Column("executor_incarnation", sa.Uuid(), nullable=False),
        sa.Column("inventory_sequence", sa.BigInteger(), nullable=False),
        sa.Column("inventory_digest", sa.Text(), nullable=False),
        sa.Column("journal_sequence", sa.BigInteger(), nullable=False),
        sa.Column("journal_digest", sa.Text(), nullable=False),
        sa.Column("controller_authority_sha256", sa.Text(), nullable=False),
        sa.Column("controller_evidence_sha256", sa.Text(), nullable=False),
        sa.Column("terminal_evidence_sha256", sa.Text(), nullable=False),
        sa.Column("evidence_digest", sa.Text(), nullable=False),
        sa.Column("evidence_payload", postgresql.JSONB(), nullable=False),
        sa.Column("receipt", postgresql.JSONB(), nullable=False),
        sa.Column("observed_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.CheckConstraint(
            "execution_epoch > 0 AND pool_generation > 0 "
            "AND inventory_sequence > 0 AND journal_sequence >= 0",
            name="guard_terminal_inventory_quantity_check",
        ),
        sa.CheckConstraint(
            "pool_id IN ('oldlab','gb10') "
            "AND physical_job_id ~ '^[a-z0-9][a-z0-9_.-]{0,127}$'",
            name="guard_terminal_inventory_physical_check",
        ),
        sa.CheckConstraint(
            "execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND inventory_digest ~ '^[0-9a-f]{64}$' "
            "AND journal_digest ~ '^[0-9a-f]{64}$' "
            "AND controller_authority_sha256 ~ '^[0-9a-f]{64}$' "
            "AND controller_evidence_sha256 ~ '^[0-9a-f]{64}$' "
            "AND terminal_evidence_sha256 ~ '^[0-9a-f]{64}$' "
            "AND evidence_digest ~ '^[0-9a-f]{64}$'",
            name="guard_terminal_inventory_digest_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(evidence_payload) = 'object' "
            "AND jsonb_typeof(receipt) = 'object' "
            "AND octet_length(evidence_payload::text) <= 8388608 "
            "AND octet_length(receipt::text) <= 65536",
            name="guard_terminal_inventory_payload_check",
        ),
        sa.ForeignKeyConstraint(
            ["agent_incarnation"],
            [f"{SCHEMA}.agent_registrations.agent_incarnation"],
            ondelete="RESTRICT",
            name="guard_terminal_inventory_agent_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["intent_id"],
            [f"{SCHEMA}.executable_claim_state.intent_id"],
            ondelete="RESTRICT",
            name="guard_terminal_inventory_intent_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["claim_operation_id"],
            [f"{SCHEMA}.executable_claim_leases.operation_id"],
            ondelete="RESTRICT",
            name="guard_terminal_inventory_claim_fkey",
        ),
        sa.PrimaryKeyConstraint("evidence_id"),
        sa.UniqueConstraint("intent_id", name="guard_terminal_inventory_intent_key"),
        sa.UniqueConstraint(
            "protected_attempt_id",
            name="guard_terminal_inventory_attempt_key",
        ),
        sa.UniqueConstraint(
            "claim_operation_id",
            name="guard_terminal_inventory_claim_key",
        ),
        sa.UniqueConstraint(
            "evidence_digest",
            name="guard_terminal_inventory_digest_key",
        ),
        sa.UniqueConstraint(
            "executor_incarnation",
            "physical_job_id",
            name="guard_terminal_inventory_physical_key",
        ),
        schema=SCHEMA,
    )
    for suffix, operation in (
        ("row", "UPDATE OR DELETE"),
        ("truncate", "TRUNCATE"),
    ):
        level = "ROW" if suffix == "row" else "STATEMENT"
        op.execute(
            f"""
            CREATE TRIGGER executable_terminal_inventory_evidence_append_only_{suffix}
            BEFORE {operation} ON {SCHEMA}.executable_terminal_inventory_evidence
            FOR EACH {level} EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
            """
        )

    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.import_executable_terminal_inventory_evidence(
          p_agent_incarnation uuid,
          p_protected_attempt_id uuid,
          p_payload jsonb,
          p_canonical_payload bytea,
          p_evidence_digest text
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_agent_role text;
          v_registration {SCHEMA}.agent_registrations%ROWTYPE;
          v_binding jsonb := p_payload->'binding';
          v_execution jsonb := p_payload->'binding'->'execution';
          v_inventory_execution jsonb := p_payload->'inventory_execution';
          v_record jsonb := p_payload->'record';
          v_proof jsonb := p_payload->'record'->'ownership_proof';
          v_metadata jsonb := p_payload->'record'->'ownership_proof'->'metadata';
          v_intent_id uuid;
          v_claim {SCHEMA}.executable_claim_leases%ROWTYPE;
          v_claim_state {SCHEMA}.executable_claim_state%ROWTYPE;
          v_worker {SCHEMA}.executable_admission_events%ROWTYPE;
          v_physical {SCHEMA}.executable_admission_events%ROWTYPE;
          v_existing {SCHEMA}.executable_terminal_inventory_evidence%ROWTYPE;
          v_receipt jsonb;
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION 'terminal inventory evidence import requires a SERIALIZABLE transaction'
              USING ERRCODE = '25000';
          END IF;
          SELECT agent_role_name INTO v_agent_role
            FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1;
          IF v_agent_role IS NULL OR session_user::text <> v_agent_role THEN
            RAISE EXCEPTION 'terminal inventory evidence caller is not the registered agent role'
              USING ERRCODE = '42501';
          END IF;
          IF pg_catalog.pg_has_role(session_user, current_user, 'MEMBER') THEN
            RAISE EXCEPTION 'terminal inventory evidence agent unexpectedly holds owner membership'
              USING ERRCODE = '42501';
          END IF;
          IF p_agent_incarnation IS NULL
             OR p_protected_attempt_id IS NULL
             OR pg_catalog.jsonb_typeof(p_payload) IS DISTINCT FROM 'object'
             OR pg_catalog.octet_length(p_payload::text) > 8388608
             OR pg_catalog.octet_length(p_canonical_payload) > 8388608
             OR pg_catalog.convert_from(p_canonical_payload, 'UTF8')
                  IS DISTINCT FROM
                    {SCHEMA}.canonical_executable_publication_payload(p_payload)
             OR p_evidence_digest !~ '^[0-9a-f]{{64}}$'
             OR pg_catalog.encode(pg_catalog.sha256(p_canonical_payload), 'hex')
                  IS DISTINCT FROM p_evidence_digest
             OR NOT p_payload ?& ARRAY[
                  'schema_version', 'binding', 'inventory_execution',
                  'inventory_sequence', 'inventory_digest', 'journal_sequence',
                  'journal_digest', 'record', 'observed_at', 'executable'
                ]
             OR p_payload - ARRAY[
                  'schema_version', 'binding', 'inventory_execution',
                  'inventory_sequence', 'inventory_digest', 'journal_sequence',
                  'journal_digest', 'record', 'observed_at', 'executable'
                ] <> '{{}}'::jsonb
             OR p_payload->'schema_version' IS DISTINCT FROM '2'::jsonb
             OR p_payload->'executable' IS DISTINCT FROM 'true'::jsonb
             OR pg_catalog.jsonb_typeof(v_binding) IS DISTINCT FROM 'object'
             OR pg_catalog.jsonb_typeof(v_inventory_execution) IS DISTINCT FROM 'object'
             OR pg_catalog.jsonb_typeof(v_record) IS DISTINCT FROM 'object'
             OR pg_catalog.jsonb_typeof(v_proof) IS DISTINCT FROM 'object'
             OR pg_catalog.jsonb_typeof(v_metadata) IS DISTINCT FROM 'object'
             OR (p_payload->>'inventory_sequence' ~ '^[1-9][0-9]*$')
                  IS DISTINCT FROM true
             OR (p_payload->>'inventory_sequence')::numeric > 9223372036854775807
             OR p_payload->>'inventory_digest' !~ '^[0-9a-f]{{64}}$'
             OR (p_payload->>'journal_sequence' ~ '^[0-9]+$')
                  IS DISTINCT FROM true
             OR (p_payload->>'journal_sequence')::numeric > 9223372036854775807
             OR p_payload->>'journal_digest' !~ '^[0-9a-f]{{64}}$'
             OR (((p_payload->>'journal_sequence')::bigint = 0)
                  IS DISTINCT FROM (p_payload->>'journal_digest' = repeat('0', 64)))
             OR (p_payload->>'observed_at')::timestamptz IS NULL
             OR v_inventory_execution IS DISTINCT FROM
                  (v_execution - ARRAY['allocation_epoch', 'executable'])
             OR NOT v_record ?& ARRAY[
                  'schema_version', 'physical_identity', 'physical_kind',
                  'authority_scope', 'state', 'resources', 'node_ids',
                  'controller_evidence_sha256', 'ownership_proof',
                  'terminal_evidence_sha256'
                ]
             OR v_record - ARRAY[
                  'schema_version', 'physical_identity', 'physical_kind',
                  'authority_scope', 'state', 'resources', 'node_ids',
                  'controller_evidence_sha256', 'ownership_proof',
                  'terminal_evidence_sha256'
                ] <> '{{}}'::jsonb
             OR v_record->'schema_version' IS DISTINCT FROM '2'::jsonb
             OR v_record->>'physical_kind' IS DISTINCT FROM 'slurm-job'
             OR v_record->>'physical_identity' !~ '^[a-z0-9][a-z0-9_.-]{{0,127}}$'
             OR v_record->>'authority_scope'
                  IS DISTINCT FROM 'dedicated-loom-association'
             OR v_record->>'state' IS DISTINCT FROM 'terminal'
             OR v_record->'resources' IS DISTINCT FROM v_binding->'resources'
             OR v_record->'node_ids' IS DISTINCT FROM v_binding->'node_ids'
             OR v_record->>'controller_evidence_sha256' !~ '^[0-9a-f]{{64}}$'
             OR v_record->>'terminal_evidence_sha256' !~ '^[0-9a-f]{{64}}$'
             OR v_proof->'schema_version' IS DISTINCT FROM '2'::jsonb
             OR v_proof->>'signing_key_id' !~ '^[a-z0-9][a-z0-9_.-]{{0,127}}$'
             OR pg_catalog.length(v_proof->>'signature_base64') <> 88
             OR (v_proof->>'signature_base64' ~ '^[A-Za-z0-9+/]{{86}}==$')
                  IS DISTINCT FROM true
             OR pg_catalog.octet_length(
                  pg_catalog.decode(v_proof->>'signature_base64', 'base64')
                ) <> 64
             OR v_metadata->'schema_version' IS DISTINCT FROM '2'::jsonb
             OR v_metadata->'binding' IS DISTINCT FROM v_binding
             OR v_metadata->>'controller_authority_sha256' !~ '^[0-9a-f]{{64}}$'
             OR v_metadata->>'trusted_launcher_sha256'
                  IS DISTINCT FROM v_execution->>'trusted_fleet_release_sha256'
             OR v_metadata->>'submitter_identity' IS DISTINCT FROM 'loom'
             OR v_metadata->>'slurm_cluster' !~ '^[a-z0-9][a-z0-9_.-]{{0,127}}$'
             OR v_metadata->>'association' !~ '^[a-z0-9][a-z0-9_.-]{{0,127}}$'
             OR (v_metadata->>'submitted_at')::timestamptz IS NULL THEN
            RAISE EXCEPTION 'terminal inventory evidence is invalid or noncanonical'
              USING ERRCODE = '22023';
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
             AND authority.deployment_generation = registration.deployment_generation
             AND authority.configuration_generation = registration.configuration_generation
             AND authority.candidate_digest = registration.candidate_digest
           WHERE registration.agent_incarnation = p_agent_incarnation
             AND registration.subject_id = (v_binding->>'subject_id')::uuid
             AND registration.subject_incarnation =
                   (v_binding->>'subject_incarnation')::uuid
             AND registration.deployment_generation =
                   (v_binding->>'deployment_generation')::bigint
             AND registration.candidate_identity_algorithm =
                   v_binding->'candidate'->>'algorithm'
             AND registration.candidate_identity = v_binding->'candidate'->>'identity'
             AND registration.candidate_publication_sha256 =
                   v_binding->'candidate'->>'publication_sha256'
             AND registration.registration_state = 'registered'
           FOR KEY SHARE OF registration, authority;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'terminal inventory evidence subject registration changed'
              USING ERRCODE = '55000';
          END IF;

          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;
          v_intent_id := (v_binding->>'intent_id')::uuid;
          SELECT * INTO v_existing
            FROM {SCHEMA}.executable_terminal_inventory_evidence
           WHERE intent_id = v_intent_id
              OR protected_attempt_id = p_protected_attempt_id
              OR evidence_digest = p_evidence_digest
              OR (
                executor_incarnation = (v_binding->>'executor_incarnation')::uuid
                AND physical_job_id = v_record->>'physical_identity'
              )
           ORDER BY evidence_id
           LIMIT 1 FOR KEY SHARE;
          IF FOUND THEN
            IF v_existing.agent_incarnation IS DISTINCT FROM p_agent_incarnation
               OR v_existing.intent_id IS DISTINCT FROM v_intent_id
               OR v_existing.protected_attempt_id IS DISTINCT FROM p_protected_attempt_id
               OR v_existing.evidence_digest IS DISTINCT FROM p_evidence_digest
               OR v_existing.evidence_payload IS DISTINCT FROM p_payload THEN
              RAISE EXCEPTION 'conflicting terminal inventory evidence replay'
                USING ERRCODE = '55000';
            END IF;
            RETURN v_existing.receipt;
          END IF;

          SELECT * INTO v_claim
            FROM {SCHEMA}.executable_claim_leases
           WHERE protected_attempt_id = p_protected_attempt_id
             AND intent_id = v_intent_id
             AND subject_id = v_registration.subject_id
             AND subject_incarnation = v_registration.subject_incarnation
             AND lease_state = 'live'
             AND executable = true
           FOR KEY SHARE;
          SELECT * INTO v_claim_state
            FROM {SCHEMA}.executable_claim_state
           WHERE intent_id = v_intent_id
             AND subject_id = v_registration.subject_id
             AND subject_incarnation = v_registration.subject_incarnation
           FOR UPDATE;
          SELECT * INTO v_worker
            FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = v_intent_id
             AND subject_id = v_registration.subject_id
             AND subject_incarnation = v_registration.subject_incarnation
             AND event_kind = 'worker-registered'
           ORDER BY protected_registration_epoch DESC, event_id DESC
           LIMIT 1 FOR KEY SHARE;
          SELECT * INTO v_physical
            FROM {SCHEMA}.executable_admission_events
           WHERE intent_id = v_intent_id
             AND subject_id = v_registration.subject_id
             AND subject_incarnation = v_registration.subject_incarnation
             AND event_kind = 'physical-bound'
           FOR KEY SHARE;
          IF v_claim.operation_id IS NULL
             OR v_claim_state.intent_id IS NULL
             OR v_worker.operation_id IS NULL
             OR v_physical.operation_id IS NULL
             OR v_claim_state.binding IS DISTINCT FROM v_binding
             OR v_claim_state.draining IS DISTINCT FROM false
             OR v_claim_state.claim_high_water < v_claim.claim_high_water
             OR v_claim_state.terminal_high_water >= v_claim.claim_high_water
             OR v_worker.binding IS DISTINCT FROM v_binding
             OR v_physical.binding IS DISTINCT FROM v_binding
             OR v_worker.worker_id IS DISTINCT FROM v_claim.worker_id
             OR v_worker.worker_incarnation IS DISTINCT FROM v_claim.worker_incarnation
             OR v_worker.physical_job_id IS DISTINCT FROM v_physical.physical_job_id
             OR v_physical.physical_job_id IS DISTINCT FROM v_record->>'physical_identity'
             OR EXISTS (
                  SELECT 1 FROM {SCHEMA}.executable_admission_events AS successor
                   WHERE successor.intent_id = v_intent_id
                     AND successor.event_kind = 'worker-registered'
                     AND successor.protected_registration_epoch >
                           v_worker.protected_registration_epoch
                )
             OR EXISTS (
                  SELECT 1 FROM {SCHEMA}.executable_admission_events AS terminal
                   WHERE terminal.intent_id = v_intent_id
                     AND terminal.event_kind IN (
                           'draining', 'released', 'withdrawn', 'prepared-revoked'
                         )
                )
             OR EXISTS (
                  SELECT 1 FROM {SCHEMA}.executable_claim_terminal_events AS terminal
                   WHERE terminal.admitted_operation_id = v_claim.operation_id
                      OR terminal.protected_attempt_id = p_protected_attempt_id
                )
             OR NOT EXISTS (
                  SELECT 1
                    FROM {SCHEMA}.trial_attempts AS attempt
                    JOIN {SCHEMA}.attempt_lifecycle_heads AS head
                      ON head.protected_attempt_id = attempt.protected_attempt_id
                    JOIN public.trials AS trial ON trial.id = attempt.trial_id
                   WHERE attempt.protected_attempt_id = p_protected_attempt_id
                     AND attempt.execution_generation = v_claim.execution_generation
                     AND attempt.requirements_digest = v_claim.requirements_digest
                     AND attempt.claim_state = 'queued'
                     AND head.lifecycle_state = 'assigned'
                     AND head.executable = false
                     AND trial.state IN ('claimed', 'running')
                     AND trial.worker_id = v_claim.worker_id
                ) THEN
            RAISE EXCEPTION 'terminal inventory evidence differs from the live protected claim'
              USING ERRCODE = '55000';
          END IF;

          v_receipt := pg_catalog.jsonb_build_object(
            'schema_version', 2,
            'intent_id', v_intent_id,
            'protected_attempt_id', p_protected_attempt_id,
            'worker_id', v_claim.worker_id,
            'worker_incarnation', v_claim.worker_incarnation,
            'physical_job_id', v_physical.physical_job_id,
            'inventory_sequence', (p_payload->>'inventory_sequence')::bigint,
            'terminal_evidence_sha256', v_record->>'terminal_evidence_sha256',
            'evidence_digest', p_evidence_digest,
            'import_state', 'imported',
            'executable', false
          );
          INSERT INTO {SCHEMA}.executable_terminal_inventory_evidence
            (agent_incarnation, subject_id, subject_incarnation, intent_id,
             protected_attempt_id, claim_operation_id, worker_id,
             worker_incarnation, physical_job_id, execution_epoch,
             execution_manifest_sha256, pool_id, pool_generation, executor_id,
             executor_incarnation, inventory_sequence, inventory_digest,
             journal_sequence, journal_digest, controller_authority_sha256,
             controller_evidence_sha256, terminal_evidence_sha256,
             evidence_digest, evidence_payload, receipt, observed_at)
          VALUES
            (p_agent_incarnation, v_registration.subject_id,
             v_registration.subject_incarnation, v_intent_id,
             p_protected_attempt_id, v_claim.operation_id, v_claim.worker_id,
             v_claim.worker_incarnation, v_physical.physical_job_id,
             (v_execution->>'execution_epoch')::bigint,
             v_execution->>'execution_manifest_sha256', v_binding->>'pool_id',
             (v_binding->>'pool_generation')::bigint,
             v_binding->>'executor_id', (v_binding->>'executor_incarnation')::uuid,
             (p_payload->>'inventory_sequence')::bigint,
             p_payload->>'inventory_digest',
             (p_payload->>'journal_sequence')::bigint,
             p_payload->>'journal_digest',
             v_metadata->>'controller_authority_sha256',
             v_record->>'controller_evidence_sha256',
             v_record->>'terminal_evidence_sha256', p_evidence_digest,
             p_payload, v_receipt, (p_payload->>'observed_at')::timestamptz);
          UPDATE {SCHEMA}.executable_claim_state
             SET draining = true
           WHERE intent_id = v_intent_id
             AND draining = false;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'terminal inventory evidence drain transition raced'
              USING ERRCODE = '40001';
          END IF;
          RETURN v_receipt;
        END
        $function$
        """
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.{IMPORT_FUNCTION} FROM PUBLIC"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{IMPORT_FUNCTION} TO {quoted_agent}"
    )
    _install_evidence_gated_requeue(quoted_trigger_owner)


def downgrade() -> None:
    op.execute(
        f"LOCK TABLE {SCHEMA}.executable_terminal_inventory_evidence "
        "IN ACCESS EXCLUSIVE MODE"
    )
    if op.get_bind().execute(
        sa.text(
            f"SELECT EXISTS (SELECT 1 FROM "
            f"{SCHEMA}.executable_terminal_inventory_evidence)"
        )
    ).scalar_one():
        raise RuntimeError(
            "cannot downgrade guard_0028 while terminal inventory evidence exists"
        )
    quoted_agent = _agent_role()
    quoted_trigger_owner = _trigger_function_owner()
    _uninstall_evidence_gated_requeue(quoted_trigger_owner)
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.{IMPORT_FUNCTION} FROM {quoted_agent}"
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.{IMPORT_FUNCTION}")
    op.execute(
        f"DROP TRIGGER executable_terminal_inventory_evidence_append_only_truncate "
        f"ON {SCHEMA}.executable_terminal_inventory_evidence"
    )
    op.execute(
        f"DROP TRIGGER executable_terminal_inventory_evidence_append_only_row "
        f"ON {SCHEMA}.executable_terminal_inventory_evidence"
    )
    op.drop_table("executable_terminal_inventory_evidence", schema=SCHEMA)
