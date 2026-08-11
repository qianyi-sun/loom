"""Inert legacy-authority preparation and monotonic freeze evidence.

Revision ID: guard_0005
Revises: guard_0004
Create Date: 2026-08-10
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "guard_0005"
down_revision: str | Sequence[str] | None = "guard_0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"
INVENTORY_DIGEST = "81b48ba31d00111a532b2317248357f8af05a40b53e4b2b8bf7cd00c3ad59616"
MAX_WRITER_CURSORS = 64
MUTATION_PATH_IDS = (
    "batch-hard-budget-cancel",
    "batch-user-cancel",
    "dead-worker-reclaim",
    "dev-environment-destroy",
    "family-finalize-cascade",
    "legacy-compatibility-writer",
    "neutral-pool-assignment",
    "pre-start-heartbeat",
    "pre-start-retry-requeue",
    "queued-to-claimed",
    "single-trial-cancel",
    "slurm-job-launch-registry-release",
    "stale-running-failure",
    "trial-requirement-and-lifecycle-binding",
    "trial-submission",
    "worker-drain-and-release",
    "worker-heartbeat-status",
    "worker-registration",
    "worker-result-state",
    "worker-token-issuance",
)
CURSOR_KEYS = (
    "authority_digest",
    "freeze_supported",
    "high_water",
    "mutation_path_id",
    "observation_state",
    "schema_version",
    "writer_domain",
    "writer_epoch",
    "writer_incarnation",
)
FREEZE_CURSOR_KEYS = (
    "authority_digest",
    "freeze_acknowledgement_digest",
    "freeze_state",
    "high_water",
    "mutation_path_id",
    "schema_version",
    "writer_domain",
    "writer_epoch",
    "writer_incarnation",
)
PREPARATION_KEYS = (
    "activation_epoch",
    "agent_incarnation",
    "allocation_epoch",
    "authority_incarnation",
    "authority_mode",
    "candidate_digest",
    "compatibility_incarnation",
    "compatibility_not_after",
    "compatibility_state",
    "configuration_generation",
    "cross_pool_placement_authority",
    "deployment_generation",
    "environment_id",
    "executable",
    "fleet_migration_epoch",
    "global_allowance_authority",
    "mutation_inventory_digest",
    "new_claim_authority",
    "new_submission_authority",
    "new_worker_authority",
    "preparation_id",
    "proposed_authority_mode",
    "reporter_high_water",
    "reporter_incarnation",
    "scale_up_authority",
    "schema_version",
    "subject_id",
    "subject_incarnation",
    "writer_cursors",
)
FREEZE_KEYS = (
    "activation_epoch",
    "agent_incarnation",
    "allocation_epoch",
    "authority_incarnation",
    "authority_mode",
    "candidate_digest",
    "compatibility_incarnation",
    "configuration_generation",
    "cross_pool_placement_authority",
    "deployment_generation",
    "environment_id",
    "executable",
    "fleet_migration_epoch",
    "freeze_id",
    "freeze_state",
    "global_allowance_authority",
    "mutation_inventory_digest",
    "new_claim_authority",
    "new_submission_authority",
    "new_worker_authority",
    "preparation_digest",
    "preparation_id",
    "reporter_high_water",
    "reporter_incarnation",
    "scale_up_authority",
    "schema_version",
    "subject_id",
    "subject_incarnation",
    "writer_cursors",
)
APPEND_ONLY_TABLES = (
    "legacy_compatibility_preparations",
    "legacy_writer_cursors",
    "legacy_compatibility_freezes",
)


def _agent_role() -> str:
    role = op.get_context().config.attributes.get("capacity_guard_agent_role")
    if not isinstance(role, str) or not role:
        raise RuntimeError("protected capacity migration is missing the validated agent role")
    return op.get_bind().dialect.identifier_preparer.quote(role)


def _quoted_literals(values: tuple[str, ...]) -> str:
    return ", ".join("'" + value.replace("'", "''") + "'" for value in values)


def _install_append_only_guards() -> None:
    for table in APPEND_ONLY_TABLES:
        op.execute(
            f"""
            CREATE TRIGGER {table}_append_only_row
            BEFORE UPDATE OR DELETE ON {SCHEMA}.{table}
            FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
            """
        )
        op.execute(
            f"""
            CREATE TRIGGER {table}_append_only_truncate
            BEFORE TRUNCATE ON {SCHEMA}.{table}
            FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
            """
        )


def _install_cross_mode_guard() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.reject_global_preparation_with_legacy()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $function$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM {SCHEMA}.legacy_compatibility_preparations
             WHERE singleton_id = 1
          ) THEN
            RAISE EXCEPTION 'legacy compatibility preparation already exists'
              USING ERRCODE = '55000';
          END IF;
          RETURN NEW;
        END
        $function$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER prepared_admission_plans_reject_legacy
        BEFORE INSERT ON {SCHEMA}.prepared_admission_plans
        FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.reject_global_preparation_with_legacy()
        """
    )


def _install_prepare_function() -> None:
    path_array = _quoted_literals(MUTATION_PATH_IDS)
    payload_key_array = _quoted_literals(tuple(sorted(PREPARATION_KEYS)))
    cursor_key_array = _quoted_literals(tuple(sorted(CURSOR_KEYS)))
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.prepare_inert_legacy_compatibility(
          p_agent_incarnation uuid,
          p_payload jsonb,
          p_canonical_payload bytea,
          p_payload_digest text
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_existing {SCHEMA}.legacy_compatibility_preparations%ROWTYPE;
          v_preparation_id uuid;
          v_compatibility_incarnation uuid;
          v_path_ids text[];
          v_cursor_keys text[];
          v_sorted_cursor_keys text[];
          v_cursor_count bigint;
          v_distinct_cursor_count bigint;
          v_payload_keys text[];
          v_audit_payload jsonb;
        BEGIN
          PERFORM {SCHEMA}.assert_inert_agent_binding(
            p_agent_incarnation, p_payload, p_canonical_payload, p_payload_digest
          );
          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;

          IF NOT EXISTS (
            SELECT 1 FROM {SCHEMA}.authority_state
             WHERE singleton_id = 1
               AND authority_mode = 'disabled'
               AND allocation_epoch = 0
          ) OR NOT EXISTS (
            SELECT 1 FROM {SCHEMA}.claim_guard_activation
             WHERE singleton_id = 1
               AND activation_state = 'disabled'
               AND authority_mode = 'disabled'
               AND activation_epoch = 0
               AND executable_new_capacity_ceiling = 0
               AND live_claim_entry_enabled = false
          ) THEN
            RAISE EXCEPTION 'protected authority is not immutably disabled'
              USING ERRCODE = '55000';
          END IF;
          IF EXISTS (SELECT 1 FROM {SCHEMA}.prepared_admission_plans) THEN
            RAISE EXCEPTION 'global admission preparation already exists'
              USING ERRCODE = '55000';
          END IF;

          v_preparation_id := (p_payload->>'preparation_id')::uuid;
          v_compatibility_incarnation :=
            (p_payload->>'compatibility_incarnation')::uuid;
          SELECT * INTO v_existing
            FROM {SCHEMA}.legacy_compatibility_preparations
           WHERE singleton_id = 1 FOR KEY SHARE;
          IF FOUND THEN
            IF v_existing.preparation_id IS DISTINCT FROM v_preparation_id
               OR v_existing.payload IS DISTINCT FROM p_payload
               OR v_existing.payload_digest IS DISTINCT FROM p_payload_digest THEN
              RAISE EXCEPTION 'conflicting legacy compatibility preparation'
                USING ERRCODE = '55000';
            END IF;
            RETURN v_existing.payload;
          END IF;

          SELECT array_agg(path_id ORDER BY path_id)
            INTO v_path_ids
            FROM (
              SELECT DISTINCT cursor.value->>'mutation_path_id' AS path_id
                FROM jsonb_array_elements(p_payload->'writer_cursors') AS cursor(value)
            ) AS paths;
          SELECT
              array_agg(
                (cursor.value->>'mutation_path_id') || ':' ||
                (cursor.value->>'writer_domain')
                ORDER BY cursor.ordinal
              ),
              array_agg(
                (cursor.value->>'mutation_path_id') || ':' ||
                (cursor.value->>'writer_domain')
                ORDER BY (cursor.value->>'mutation_path_id'),
                         (cursor.value->>'writer_domain')
              ),
              count(*),
              count(DISTINCT (
                cursor.value->>'mutation_path_id',
                cursor.value->>'writer_domain'
              ))
            INTO v_cursor_keys, v_sorted_cursor_keys,
                 v_cursor_count, v_distinct_cursor_count
            FROM jsonb_array_elements(p_payload->'writer_cursors')
                 WITH ORDINALITY AS cursor(value, ordinal);
          SELECT array_agg(payload_key.key ORDER BY payload_key.key)
            INTO v_payload_keys
            FROM jsonb_object_keys(p_payload) AS payload_key(key);
          IF p_payload->>'mutation_inventory_digest' IS DISTINCT FROM '{INVENTORY_DIGEST}'
             OR p_payload->>'proposed_authority_mode' IS DISTINCT FROM
                'legacy-compatibility'
             OR p_payload->>'compatibility_state' IS DISTINCT FROM 'prepared'
             OR (p_payload->>'fleet_migration_epoch')::bigint < 1
             OR (p_payload->>'activation_epoch')::bigint IS DISTINCT FROM 0
             OR (p_payload->>'new_submission_authority')::boolean IS DISTINCT FROM false
             OR (p_payload->>'new_claim_authority')::boolean IS DISTINCT FROM false
             OR (p_payload->>'scale_up_authority')::boolean IS DISTINCT FROM false
             OR (p_payload->>'cross_pool_placement_authority')::boolean
                IS DISTINCT FROM false
             OR (p_payload->>'global_allowance_authority')::boolean IS DISTINCT FROM false
             OR (p_payload->>'new_worker_authority')::boolean IS DISTINCT FROM false
             OR (p_payload->>'executable')::boolean IS DISTINCT FROM false
             OR v_payload_keys IS DISTINCT FROM ARRAY[{payload_key_array}]::text[]
             OR v_path_ids IS DISTINCT FROM ARRAY[{path_array}]::text[]
             OR v_cursor_count < {len(MUTATION_PATH_IDS)}
             OR v_cursor_count > {MAX_WRITER_CURSORS}
             OR v_distinct_cursor_count IS DISTINCT FROM v_cursor_count
             OR v_cursor_keys IS DISTINCT FROM v_sorted_cursor_keys THEN
            RAISE EXCEPTION 'legacy compatibility preparation is invalid or incomplete'
              USING ERRCODE = '22023';
          END IF;
          IF (p_payload->>'compatibility_not_after')::timestamptz
                <= statement_timestamp() THEN
            RAISE EXCEPTION 'legacy compatibility preparation has expired'
              USING ERRCODE = '55000';
          END IF;
          IF (
            SELECT count(DISTINCT identity)
              FROM unnest(ARRAY[
                (p_payload->>'subject_id')::uuid,
                (p_payload->>'subject_incarnation')::uuid,
                (p_payload->>'authority_incarnation')::uuid,
                p_agent_incarnation,
                (p_payload->>'reporter_incarnation')::uuid,
                v_preparation_id,
                v_compatibility_incarnation
              ]) AS identity
          ) <> 7 THEN
            RAISE EXCEPTION 'legacy compatibility identities are not distinct'
              USING ERRCODE = '22023';
          END IF;
          IF EXISTS (
            SELECT 1
              FROM jsonb_array_elements(p_payload->'writer_cursors') AS cursor(value)
             WHERE (cursor.value->>'schema_version')::integer IS DISTINCT FROM 1
                OR cursor.value->>'mutation_path_id' NOT IN ({path_array})
                OR cursor.value->>'writer_domain'
                   !~ '^[a-z0-9][a-z0-9_.-]{{0,127}}$'
                OR (cursor.value->>'writer_incarnation') IS NULL
                OR (cursor.value->>'writer_epoch')::bigint < 1
                OR (cursor.value->>'high_water')::bigint < 0
                OR cursor.value->>'authority_digest' !~ '^[0-9a-f]{{64}}$'
                OR cursor.value->>'observation_state' IS DISTINCT FROM 'observed'
                OR (cursor.value->>'freeze_supported')::boolean IS DISTINCT FROM true
                OR (
                  SELECT array_agg(cursor_key.key ORDER BY cursor_key.key)
                    FROM jsonb_object_keys(cursor.value) AS cursor_key(key)
                ) IS DISTINCT FROM ARRAY[{cursor_key_array}]::text[]
          ) THEN
            RAISE EXCEPTION 'legacy writer cursor is invalid'
              USING ERRCODE = '22023';
          END IF;

          INSERT INTO {SCHEMA}.legacy_compatibility_preparations
            (singleton_id, preparation_id, agent_incarnation,
             compatibility_incarnation, fleet_migration_epoch,
             compatibility_not_after,
             mutation_inventory_digest, proposed_authority_mode,
             compatibility_state, activation_epoch, new_submission_authority,
             new_claim_authority, scale_up_authority,
             cross_pool_placement_authority, global_allowance_authority,
             new_worker_authority, executable, payload, payload_digest)
          VALUES
            (1, v_preparation_id, p_agent_incarnation,
             v_compatibility_incarnation,
             (p_payload->>'fleet_migration_epoch')::bigint,
             (p_payload->>'compatibility_not_after')::timestamptz,
             p_payload->>'mutation_inventory_digest',
             p_payload->>'proposed_authority_mode',
             p_payload->>'compatibility_state',
             (p_payload->>'activation_epoch')::bigint,
             (p_payload->>'new_submission_authority')::boolean,
             (p_payload->>'new_claim_authority')::boolean,
             (p_payload->>'scale_up_authority')::boolean,
             (p_payload->>'cross_pool_placement_authority')::boolean,
             (p_payload->>'global_allowance_authority')::boolean,
             (p_payload->>'new_worker_authority')::boolean,
             (p_payload->>'executable')::boolean,
             p_payload, p_payload_digest);

          INSERT INTO {SCHEMA}.legacy_writer_cursors
            (preparation_id, cursor_index, mutation_path_id, writer_incarnation,
             writer_domain, writer_epoch, high_water, authority_digest, observation_state,
             freeze_supported)
          SELECT v_preparation_id, cursor.ordinal - 1,
                 cursor.value->>'mutation_path_id',
                 (cursor.value->>'writer_incarnation')::uuid,
                 cursor.value->>'writer_domain',
                 (cursor.value->>'writer_epoch')::bigint,
                 (cursor.value->>'high_water')::bigint,
                 cursor.value->>'authority_digest',
                 cursor.value->>'observation_state',
                 (cursor.value->>'freeze_supported')::boolean
            FROM jsonb_array_elements(p_payload->'writer_cursors')
                 WITH ORDINALITY AS cursor(value, ordinal);
          v_audit_payload := jsonb_build_object(
            'schema_version', 1,
            'preparation_id', v_preparation_id,
            'compatibility_incarnation', v_compatibility_incarnation,
            'fleet_migration_epoch', (p_payload->>'fleet_migration_epoch')::bigint,
            'mutation_inventory_digest', p_payload->>'mutation_inventory_digest',
            'writer_cursor_count', v_cursor_count,
            'prepared_payload_digest', p_payload_digest,
            'executable', false
          );
          INSERT INTO {SCHEMA}.audit_events
            (event_type, payload, payload_digest)
          VALUES
            ('legacy_compatibility_prepared.v1', v_audit_payload,
             encode(sha256(convert_to(v_audit_payload::text, 'UTF8')), 'hex'));
          RETURN p_payload;
        END
        $function$
        """
    )


def _install_freeze_function() -> None:
    payload_key_array = _quoted_literals(tuple(sorted(FREEZE_KEYS)))
    path_array = _quoted_literals(MUTATION_PATH_IDS)
    cursor_key_array = _quoted_literals(tuple(sorted(FREEZE_CURSOR_KEYS)))
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.freeze_inert_legacy_compatibility(
          p_agent_incarnation uuid,
          p_payload jsonb,
          p_canonical_payload bytea,
          p_payload_digest text
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_prepared {SCHEMA}.legacy_compatibility_preparations%ROWTYPE;
          v_existing {SCHEMA}.legacy_compatibility_freezes%ROWTYPE;
          v_freeze_id uuid;
          v_payload_keys text[];
          v_path_ids text[];
          v_cursor_keys text[];
          v_sorted_cursor_keys text[];
          v_cursor_count bigint;
          v_distinct_cursor_count bigint;
          v_audit_payload jsonb;
        BEGIN
          PERFORM {SCHEMA}.assert_inert_agent_binding(
            p_agent_incarnation, p_payload, p_canonical_payload, p_payload_digest
          );
          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;

          IF NOT EXISTS (
            SELECT 1 FROM {SCHEMA}.authority_state
             WHERE singleton_id = 1
               AND authority_mode = 'disabled'
               AND allocation_epoch = 0
          ) OR NOT EXISTS (
            SELECT 1 FROM {SCHEMA}.claim_guard_activation
             WHERE singleton_id = 1
               AND activation_state = 'disabled'
               AND authority_mode = 'disabled'
               AND activation_epoch = 0
               AND executable_new_capacity_ceiling = 0
               AND live_claim_entry_enabled = false
          ) THEN
            RAISE EXCEPTION 'protected authority is not immutably disabled'
              USING ERRCODE = '55000';
          END IF;

          SELECT * INTO v_prepared
            FROM {SCHEMA}.legacy_compatibility_preparations
           WHERE singleton_id = 1 FOR KEY SHARE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'prepared legacy compatibility is missing'
              USING ERRCODE = '55000';
          END IF;
          SELECT array_agg(payload_key.key ORDER BY payload_key.key)
            INTO v_payload_keys
            FROM jsonb_object_keys(p_payload) AS payload_key(key);
          SELECT array_agg(path_id ORDER BY path_id)
            INTO v_path_ids
            FROM (
              SELECT DISTINCT cursor.value->>'mutation_path_id' AS path_id
                FROM jsonb_array_elements(p_payload->'writer_cursors') AS cursor(value)
            ) AS paths;
          SELECT
              array_agg(
                (cursor.value->>'mutation_path_id') || ':' ||
                (cursor.value->>'writer_domain')
                ORDER BY cursor.ordinal
              ),
              array_agg(
                (cursor.value->>'mutation_path_id') || ':' ||
                (cursor.value->>'writer_domain')
                ORDER BY (cursor.value->>'mutation_path_id'),
                         (cursor.value->>'writer_domain')
              ),
              count(*),
              count(DISTINCT (
                cursor.value->>'mutation_path_id',
                cursor.value->>'writer_domain'
              ))
            INTO v_cursor_keys, v_sorted_cursor_keys,
                 v_cursor_count, v_distinct_cursor_count
            FROM jsonb_array_elements(p_payload->'writer_cursors')
                 WITH ORDINALITY AS cursor(value, ordinal);
          IF (p_payload->>'preparation_id')::uuid
                IS DISTINCT FROM v_prepared.preparation_id
             OR (p_payload->>'compatibility_incarnation')::uuid
                IS DISTINCT FROM v_prepared.compatibility_incarnation
             OR (p_payload->>'fleet_migration_epoch')::bigint
                IS DISTINCT FROM v_prepared.fleet_migration_epoch
             OR p_payload->>'mutation_inventory_digest'
                IS DISTINCT FROM v_prepared.mutation_inventory_digest
             OR p_payload->>'preparation_digest'
                IS DISTINCT FROM v_prepared.payload_digest
             OR v_payload_keys IS DISTINCT FROM ARRAY[{payload_key_array}]::text[]
             OR v_path_ids IS DISTINCT FROM ARRAY[{path_array}]::text[]
             OR v_cursor_count < {len(MUTATION_PATH_IDS)}
             OR v_cursor_count > {MAX_WRITER_CURSORS}
             OR v_distinct_cursor_count IS DISTINCT FROM v_cursor_count
             OR v_cursor_keys IS DISTINCT FROM v_sorted_cursor_keys
             OR v_cursor_count IS DISTINCT FROM (
               SELECT count(*) FROM {SCHEMA}.legacy_writer_cursors
                WHERE preparation_id = v_prepared.preparation_id
             ) THEN
            RAISE EXCEPTION 'freeze differs from prepared legacy compatibility'
              USING ERRCODE = '55000';
          END IF;
          IF EXISTS (
            SELECT 1
              FROM jsonb_array_elements(p_payload->'writer_cursors') AS cursor(value)
              LEFT JOIN {SCHEMA}.legacy_writer_cursors AS stored
                ON stored.preparation_id = v_prepared.preparation_id
               AND stored.mutation_path_id = cursor.value->>'mutation_path_id'
               AND stored.writer_domain = cursor.value->>'writer_domain'
             WHERE (
                  SELECT array_agg(cursor_key.key ORDER BY cursor_key.key)
                    FROM jsonb_object_keys(cursor.value) AS cursor_key(key)
                   ) IS DISTINCT FROM ARRAY[{cursor_key_array}]::text[]
                OR (cursor.value->>'schema_version')::integer IS DISTINCT FROM 1
                OR cursor.value->>'mutation_path_id' NOT IN ({path_array})
                OR cursor.value->>'writer_domain'
                   !~ '^[a-z0-9][a-z0-9_.-]{{0,127}}$'
                OR (cursor.value->>'writer_epoch')::bigint < 1
                OR (cursor.value->>'high_water')::bigint < 0
                OR cursor.value->>'authority_digest' !~ '^[0-9a-f]{{64}}$'
                OR cursor.value->>'freeze_acknowledgement_digest'
                   !~ '^[0-9a-f]{{64}}$'
                OR cursor.value->>'freeze_state' IS DISTINCT FROM 'frozen'
                OR stored.mutation_path_id IS NULL
                OR stored.writer_incarnation IS DISTINCT FROM
                   (cursor.value->>'writer_incarnation')::uuid
                OR stored.writer_epoch IS DISTINCT FROM
                   (cursor.value->>'writer_epoch')::bigint
                OR stored.high_water IS DISTINCT FROM
                   (cursor.value->>'high_water')::bigint
                OR stored.authority_digest IS DISTINCT FROM
                   cursor.value->>'authority_digest'
          ) THEN
            RAISE EXCEPTION 'freeze cursor differs from prepared legacy compatibility'
              USING ERRCODE = '55000';
          END IF;
          IF p_payload->>'freeze_state' IS DISTINCT FROM 'frozen'
             OR (p_payload->>'activation_epoch')::bigint IS DISTINCT FROM 0
             OR (p_payload->>'new_submission_authority')::boolean IS DISTINCT FROM false
             OR (p_payload->>'new_claim_authority')::boolean IS DISTINCT FROM false
             OR (p_payload->>'scale_up_authority')::boolean IS DISTINCT FROM false
             OR (p_payload->>'cross_pool_placement_authority')::boolean
                IS DISTINCT FROM false
             OR (p_payload->>'global_allowance_authority')::boolean IS DISTINCT FROM false
             OR (p_payload->>'new_worker_authority')::boolean IS DISTINCT FROM false
             OR (p_payload->>'executable')::boolean IS DISTINCT FROM false THEN
            RAISE EXCEPTION 'legacy compatibility freeze is invalid'
              USING ERRCODE = '22023';
          END IF;
          v_freeze_id := (p_payload->>'freeze_id')::uuid;
          IF (
            SELECT count(DISTINCT identity)
              FROM unnest(ARRAY[
                (p_payload->>'subject_id')::uuid,
                (p_payload->>'subject_incarnation')::uuid,
                (p_payload->>'authority_incarnation')::uuid,
                p_agent_incarnation,
                (p_payload->>'reporter_incarnation')::uuid,
                v_freeze_id,
                v_prepared.preparation_id,
                v_prepared.compatibility_incarnation
              ]) AS identity
          ) <> 8 THEN
            RAISE EXCEPTION 'legacy freeze identities are not distinct'
              USING ERRCODE = '22023';
          END IF;

          SELECT * INTO v_existing
            FROM {SCHEMA}.legacy_compatibility_freezes
           WHERE singleton_id = 1 FOR KEY SHARE;
          IF FOUND THEN
            IF v_existing.freeze_id IS DISTINCT FROM v_freeze_id
               OR v_existing.payload IS DISTINCT FROM p_payload
               OR v_existing.payload_digest IS DISTINCT FROM p_payload_digest THEN
              RAISE EXCEPTION 'conflicting legacy compatibility freeze'
                USING ERRCODE = '55000';
            END IF;
            RETURN v_existing.payload;
          END IF;

          INSERT INTO {SCHEMA}.legacy_compatibility_freezes
            (singleton_id, freeze_id, preparation_id, agent_incarnation,
             compatibility_incarnation, fleet_migration_epoch,
             mutation_inventory_digest, preparation_digest, freeze_state,
             activation_epoch, new_submission_authority, new_claim_authority,
             scale_up_authority, cross_pool_placement_authority,
             global_allowance_authority, new_worker_authority, executable,
             payload, payload_digest)
          VALUES
            (1, v_freeze_id, v_prepared.preparation_id, p_agent_incarnation,
             v_prepared.compatibility_incarnation, v_prepared.fleet_migration_epoch,
             v_prepared.mutation_inventory_digest, v_prepared.payload_digest,
             p_payload->>'freeze_state',
             (p_payload->>'activation_epoch')::bigint,
             (p_payload->>'new_submission_authority')::boolean,
             (p_payload->>'new_claim_authority')::boolean,
             (p_payload->>'scale_up_authority')::boolean,
             (p_payload->>'cross_pool_placement_authority')::boolean,
             (p_payload->>'global_allowance_authority')::boolean,
             (p_payload->>'new_worker_authority')::boolean,
             (p_payload->>'executable')::boolean,
             p_payload, p_payload_digest);
          v_audit_payload := jsonb_build_object(
            'schema_version', 1,
            'freeze_id', v_freeze_id,
            'preparation_id', v_prepared.preparation_id,
            'compatibility_incarnation', v_prepared.compatibility_incarnation,
            'fleet_migration_epoch', v_prepared.fleet_migration_epoch,
            'mutation_inventory_digest', v_prepared.mutation_inventory_digest,
            'frozen_preparation_digest', v_prepared.payload_digest,
            'writer_cursor_count', v_cursor_count,
            'executable', false
          );
          INSERT INTO {SCHEMA}.audit_events
            (event_type, payload, payload_digest)
          VALUES
            ('legacy_compatibility_frozen.v1', v_audit_payload,
             encode(sha256(convert_to(v_audit_payload::text, 'UTF8')), 'hex'));
          RETURN p_payload;
        END
        $function$
        """
    )


def upgrade() -> None:
    quoted_agent = _agent_role()
    path_literals = _quoted_literals(MUTATION_PATH_IDS)
    op.create_table(
        "legacy_compatibility_preparations",
        sa.Column("singleton_id", sa.SmallInteger(), nullable=False),
        sa.Column("preparation_id", sa.Uuid(), nullable=False),
        sa.Column("agent_incarnation", sa.Uuid(), nullable=False),
        sa.Column("compatibility_incarnation", sa.Uuid(), nullable=False),
        sa.Column("fleet_migration_epoch", sa.BigInteger(), nullable=False),
        sa.Column(
            "compatibility_not_after",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
        ),
        sa.Column("mutation_inventory_digest", sa.Text(), nullable=False),
        sa.Column("proposed_authority_mode", sa.Text(), nullable=False),
        sa.Column("compatibility_state", sa.Text(), nullable=False),
        sa.Column("activation_epoch", sa.BigInteger(), nullable=False),
        sa.Column("new_submission_authority", sa.Boolean(), nullable=False),
        sa.Column("new_claim_authority", sa.Boolean(), nullable=False),
        sa.Column("scale_up_authority", sa.Boolean(), nullable=False),
        sa.Column("cross_pool_placement_authority", sa.Boolean(), nullable=False),
        sa.Column("global_allowance_authority", sa.Boolean(), nullable=False),
        sa.Column("new_worker_authority", sa.Boolean(), nullable=False),
        sa.Column("executable", sa.Boolean(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("payload_digest", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("singleton_id = 1", name="guard_legacy_preparation_singleton_check"),
        sa.CheckConstraint(
            "fleet_migration_epoch > 0", name="guard_legacy_preparation_epoch_check"
        ),
        sa.CheckConstraint(
            "compatibility_not_after > created_at",
            name="guard_legacy_preparation_expiry_check",
        ),
        sa.CheckConstraint(
            f"mutation_inventory_digest = '{INVENTORY_DIGEST}'",
            name="guard_legacy_preparation_inventory_check",
        ),
        sa.CheckConstraint(
            "proposed_authority_mode = 'legacy-compatibility' AND compatibility_state = 'prepared'",
            name="guard_legacy_preparation_state_check",
        ),
        sa.CheckConstraint(
            "activation_epoch = 0 AND NOT new_submission_authority "
            "AND NOT new_claim_authority AND NOT scale_up_authority "
            "AND NOT cross_pool_placement_authority "
            "AND NOT global_allowance_authority AND NOT new_worker_authority "
            "AND NOT executable",
            name="guard_legacy_preparation_disabled_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object' AND octet_length(payload::text) <= 65536",
            name="guard_legacy_preparation_payload_check",
        ),
        sa.CheckConstraint(
            "payload_digest ~ '^[0-9a-f]{64}$'",
            name="guard_legacy_preparation_digest_check",
        ),
        sa.ForeignKeyConstraint(
            ["agent_incarnation"],
            [f"{SCHEMA}.agent_registrations.agent_incarnation"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("preparation_id"),
        sa.UniqueConstraint("singleton_id", name="guard_legacy_single_preparation_key"),
        sa.UniqueConstraint(
            "compatibility_incarnation", name="guard_legacy_compatibility_incarnation_key"
        ),
        sa.UniqueConstraint(
            "preparation_id",
            "compatibility_incarnation",
            "fleet_migration_epoch",
            "mutation_inventory_digest",
            "payload_digest",
            name="guard_legacy_preparation_exact_binding_key",
        ),
        schema=SCHEMA,
    )
    op.create_table(
        "legacy_writer_cursors",
        sa.Column("preparation_id", sa.Uuid(), nullable=False),
        sa.Column("cursor_index", sa.Integer(), nullable=False),
        sa.Column("mutation_path_id", sa.Text(), nullable=False),
        sa.Column("writer_domain", sa.Text(), nullable=False),
        sa.Column("writer_incarnation", sa.Uuid(), nullable=False),
        sa.Column("writer_epoch", sa.BigInteger(), nullable=False),
        sa.Column("high_water", sa.BigInteger(), nullable=False),
        sa.Column("authority_digest", sa.Text(), nullable=False),
        sa.Column("observation_state", sa.Text(), nullable=False),
        sa.Column("freeze_supported", sa.Boolean(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            f"mutation_path_id IN ({path_literals})",
            name="guard_legacy_cursor_path_check",
        ),
        sa.CheckConstraint(
            f"cursor_index >= 0 AND cursor_index < {MAX_WRITER_CURSORS}",
            name="guard_legacy_cursor_index_check",
        ),
        sa.CheckConstraint(
            "writer_domain ~ '^[a-z0-9][a-z0-9_.-]{0,127}$'",
            name="guard_legacy_cursor_domain_check",
        ),
        sa.CheckConstraint(
            "writer_epoch > 0 AND high_water >= 0",
            name="guard_legacy_cursor_sequence_check",
        ),
        sa.CheckConstraint(
            "authority_digest ~ '^[0-9a-f]{64}$'",
            name="guard_legacy_cursor_digest_check",
        ),
        sa.CheckConstraint(
            "observation_state = 'observed' AND freeze_supported",
            name="guard_legacy_cursor_state_check",
        ),
        sa.ForeignKeyConstraint(
            ["preparation_id"],
            [f"{SCHEMA}.legacy_compatibility_preparations.preparation_id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("preparation_id", "mutation_path_id", "writer_domain"),
        sa.UniqueConstraint("preparation_id", "cursor_index", name="guard_legacy_cursor_order_key"),
        schema=SCHEMA,
    )
    op.create_table(
        "legacy_compatibility_freezes",
        sa.Column("singleton_id", sa.SmallInteger(), nullable=False),
        sa.Column("freeze_id", sa.Uuid(), nullable=False),
        sa.Column("preparation_id", sa.Uuid(), nullable=False),
        sa.Column("agent_incarnation", sa.Uuid(), nullable=False),
        sa.Column("compatibility_incarnation", sa.Uuid(), nullable=False),
        sa.Column("fleet_migration_epoch", sa.BigInteger(), nullable=False),
        sa.Column("mutation_inventory_digest", sa.Text(), nullable=False),
        sa.Column("preparation_digest", sa.Text(), nullable=False),
        sa.Column("freeze_state", sa.Text(), nullable=False),
        sa.Column("activation_epoch", sa.BigInteger(), nullable=False),
        sa.Column("new_submission_authority", sa.Boolean(), nullable=False),
        sa.Column("new_claim_authority", sa.Boolean(), nullable=False),
        sa.Column("scale_up_authority", sa.Boolean(), nullable=False),
        sa.Column("cross_pool_placement_authority", sa.Boolean(), nullable=False),
        sa.Column("global_allowance_authority", sa.Boolean(), nullable=False),
        sa.Column("new_worker_authority", sa.Boolean(), nullable=False),
        sa.Column("executable", sa.Boolean(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("payload_digest", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("singleton_id = 1", name="guard_legacy_freeze_singleton_check"),
        sa.CheckConstraint("fleet_migration_epoch > 0", name="guard_legacy_freeze_epoch_check"),
        sa.CheckConstraint(
            f"mutation_inventory_digest = '{INVENTORY_DIGEST}'",
            name="guard_legacy_freeze_inventory_check",
        ),
        sa.CheckConstraint(
            "preparation_digest ~ '^[0-9a-f]{64}$' AND payload_digest ~ '^[0-9a-f]{64}$'",
            name="guard_legacy_freeze_digest_check",
        ),
        sa.CheckConstraint(
            "freeze_state = 'frozen' AND activation_epoch = 0 "
            "AND NOT new_submission_authority AND NOT new_claim_authority "
            "AND NOT scale_up_authority AND NOT cross_pool_placement_authority "
            "AND NOT global_allowance_authority AND NOT new_worker_authority "
            "AND NOT executable",
            name="guard_legacy_freeze_disabled_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object' AND octet_length(payload::text) <= 65536",
            name="guard_legacy_freeze_payload_check",
        ),
        sa.ForeignKeyConstraint(
            [
                "preparation_id",
                "compatibility_incarnation",
                "fleet_migration_epoch",
                "mutation_inventory_digest",
                "preparation_digest",
            ],
            [
                f"{SCHEMA}.legacy_compatibility_preparations.preparation_id",
                f"{SCHEMA}.legacy_compatibility_preparations.compatibility_incarnation",
                f"{SCHEMA}.legacy_compatibility_preparations.fleet_migration_epoch",
                f"{SCHEMA}.legacy_compatibility_preparations.mutation_inventory_digest",
                f"{SCHEMA}.legacy_compatibility_preparations.payload_digest",
            ],
            ondelete="RESTRICT",
            name="guard_legacy_freeze_exact_preparation_fk",
        ),
        sa.ForeignKeyConstraint(
            ["agent_incarnation"],
            [f"{SCHEMA}.agent_registrations.agent_incarnation"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("freeze_id"),
        sa.UniqueConstraint("singleton_id", name="guard_legacy_single_freeze_key"),
        sa.UniqueConstraint("preparation_id", name="guard_legacy_freeze_preparation_key"),
        schema=SCHEMA,
    )
    _install_append_only_guards()
    _install_cross_mode_guard()
    _install_prepare_function()
    _install_freeze_function()
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.prepare_inert_legacy_compatibility"
        f"(uuid, jsonb, bytea, text) TO {quoted_agent}"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.freeze_inert_legacy_compatibility"
        f"(uuid, jsonb, bytea, text) TO {quoted_agent}"
    )


def downgrade() -> None:
    quoted_agent = _agent_role()
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.freeze_inert_legacy_compatibility"
        f"(uuid, jsonb, bytea, text) FROM {quoted_agent}"
    )
    op.execute(
        f"DROP FUNCTION {SCHEMA}.freeze_inert_legacy_compatibility(uuid, jsonb, bytea, text)"
    )
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.prepare_inert_legacy_compatibility"
        f"(uuid, jsonb, bytea, text) FROM {quoted_agent}"
    )
    op.execute(
        f"DROP FUNCTION {SCHEMA}.prepare_inert_legacy_compatibility(uuid, jsonb, bytea, text)"
    )
    op.execute(
        f"DROP TRIGGER prepared_admission_plans_reject_legacy ON {SCHEMA}.prepared_admission_plans"
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.reject_global_preparation_with_legacy()")
    op.drop_table("legacy_compatibility_freezes", schema=SCHEMA)
    op.drop_table("legacy_writer_cursors", schema=SCHEMA)
    op.drop_table("legacy_compatibility_preparations", schema=SCHEMA)
