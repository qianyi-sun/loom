"""Persist Daytona backend policy and scheduler decisions.

Revision ID: 0111
Revises: 0110
"""

from alembic import op

revision = "0111"
down_revision = "0110"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.get_bind().exec_driver_sql(
        """
        ALTER TABLE batches
          ADD COLUMN backend_policy_snapshot JSONB,
          ADD COLUMN backend_policy_digest TEXT;

        UPDATE batches
           SET backend_policy_snapshot = jsonb_build_object(
                 'schema_version', 'loom.daytona-backend-policy.v1',
                 'mode', CASE WHEN backend = 'daytona' THEN 'explicit' ELSE 'local_only' END,
                 'allowed_backends', jsonb_build_array(backend),
                 'spillover_after_queue_seconds', 0,
                 'daytona_resources', NULL,
                 'daytona_price_snapshot', NULL,
                 'max_cloud_cost_usd', NULL,
                 'max_runtime_seconds', NULL,
                 'max_attempts', 1,
                 'expected_trial_count', GREATEST(expected_trial_count, 1),
                 'worst_case_cloud_cost_usd', NULL,
                 'authority', jsonb_build_object('kind', 'legacy_backfill'),
                 'accepted_at', to_jsonb(created_at)
               );
        UPDATE batches
           SET backend_policy_digest =
                 'sha256:0000000000000000000000000000000000000000000000000000000000000000';
        ALTER TABLE batches
          ALTER COLUMN backend_policy_snapshot SET NOT NULL,
          ALTER COLUMN backend_policy_digest SET NOT NULL,
          ALTER COLUMN backend_policy_snapshot SET DEFAULT
            '{"schema_version":"loom.daytona-backend-policy.v1","mode":"local_only","allowed_backends":["docker"],"spillover_after_queue_seconds":0,"max_attempts":1,"expected_trial_count":1,"authority":{"kind":"legacy_insert"},"accepted_at":"1970-01-01T00:00:00Z"}'::jsonb,
          ALTER COLUMN backend_policy_digest SET DEFAULT
            'sha256:0000000000000000000000000000000000000000000000000000000000000000',
          ADD CONSTRAINT batches_backend_policy_object_check
            CHECK (jsonb_typeof(backend_policy_snapshot) = 'object'),
          ADD CONSTRAINT batches_backend_policy_digest_check
            CHECK (backend_policy_digest ~ '^sha256:[0-9a-f]{64}$');

        ALTER TABLE trials
          ADD COLUMN backend_policy_snapshot JSONB,
          ADD COLUMN backend_policy_digest TEXT,
          ADD COLUMN selected_backend TEXT,
          ADD COLUMN backend_selection_reason TEXT,
          ADD COLUMN backend_selected_at TIMESTAMPTZ,
          ADD COLUMN backend_incompatibility_reasons JSONB NOT NULL DEFAULT '[]'::jsonb;

        UPDATE trials t
           SET backend_policy_snapshot = COALESCE(
                 b.backend_policy_snapshot,
                 jsonb_build_object(
                   'schema_version', 'loom.daytona-backend-policy.v1',
                   'mode', CASE
                     WHEN COALESCE(t.requires_caps->>'backend', 'docker') = 'daytona'
                     THEN 'explicit' ELSE 'local_only' END,
                   'allowed_backends', jsonb_build_array(
                     COALESCE(t.requires_caps->>'backend', 'docker')
                   ),
                   'spillover_after_queue_seconds', 0,
                   'daytona_resources', NULL,
                   'daytona_price_snapshot', NULL,
                   'max_cloud_cost_usd', NULL,
                   'max_runtime_seconds', NULL,
                   'max_attempts', 1,
                   'expected_trial_count', 1,
                   'worst_case_cloud_cost_usd', NULL,
                   'authority', jsonb_build_object('kind', 'legacy_backfill'),
                   'accepted_at', to_jsonb(t.submitted_at)
                 )
               ),
               backend_policy_digest = b.backend_policy_digest,
               selected_backend = COALESCE(t.requires_caps->>'backend', 'docker'),
               backend_selection_reason = 'legacy_backend',
               backend_selected_at = t.submitted_at
          FROM batches b
         WHERE b.id = t.batch_id;

        UPDATE trials
           SET backend_policy_snapshot = jsonb_build_object(
                 'schema_version', 'loom.daytona-backend-policy.v1',
                 'mode', CASE
                   WHEN COALESCE(requires_caps->>'backend', 'docker') = 'daytona'
                   THEN 'explicit' ELSE 'local_only' END,
                 'allowed_backends', jsonb_build_array(
                   COALESCE(requires_caps->>'backend', 'docker')
                 ),
                 'spillover_after_queue_seconds', 0,
                 'daytona_resources', NULL,
                 'daytona_price_snapshot', NULL,
                 'max_cloud_cost_usd', NULL,
                 'max_runtime_seconds', NULL,
                 'max_attempts', 1,
                 'expected_trial_count', 1,
                 'worst_case_cloud_cost_usd', NULL,
                 'authority', jsonb_build_object('kind', 'legacy_backfill'),
                 'accepted_at', to_jsonb(submitted_at)
               ),
               selected_backend = COALESCE(requires_caps->>'backend', 'docker'),
               backend_selection_reason = 'legacy_backend',
               backend_selected_at = submitted_at
         WHERE backend_policy_snapshot IS NULL;
        UPDATE trials
           SET backend_policy_digest =
                 'sha256:0000000000000000000000000000000000000000000000000000000000000000'
         WHERE backend_policy_digest IS NULL;
        -- Legacy Daytona rows predate resource, price, runtime, and budget
        -- authority. Fail them closed instead of manufacturing cloud spend
        -- permission or silently rewriting them to Docker.
        UPDATE trials
           SET backend_policy_digest =
                 'sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff',
               selected_backend = NULL,
               backend_selection_reason = NULL,
               backend_selected_at = NULL,
               backend_incompatibility_reasons = jsonb_build_array(
                 jsonb_build_object(
                   'code', 'legacy_daytona_policy_missing',
                   'detail', 'operator must resubmit with a complete Daytona backend policy'
                 )
               )
         WHERE COALESCE(requires_caps->>'backend', 'docker') = 'daytona';

        ALTER TABLE trials
          ALTER COLUMN backend_policy_snapshot SET NOT NULL,
          ALTER COLUMN backend_policy_digest SET NOT NULL,
          ALTER COLUMN backend_policy_snapshot SET DEFAULT
            '{"schema_version":"loom.daytona-backend-policy.v1","mode":"local_only","allowed_backends":["docker"],"spillover_after_queue_seconds":0,"max_attempts":1,"expected_trial_count":1,"authority":{"kind":"legacy_insert"},"accepted_at":"1970-01-01T00:00:00Z"}'::jsonb,
          ALTER COLUMN backend_policy_digest SET DEFAULT
            'sha256:0000000000000000000000000000000000000000000000000000000000000000',
          ADD CONSTRAINT trials_backend_policy_object_check
            CHECK (jsonb_typeof(backend_policy_snapshot) = 'object'),
          ADD CONSTRAINT trials_backend_policy_digest_check
            CHECK (backend_policy_digest ~ '^sha256:[0-9a-f]{64}$'),
          ADD CONSTRAINT trials_selected_backend_check
            CHECK (selected_backend IS NULL OR selected_backend IN ('docker','daytona')),
          ADD CONSTRAINT trials_backend_selection_group_check CHECK (
            (selected_backend IS NULL AND backend_selection_reason IS NULL
             AND backend_selected_at IS NULL)
            OR
            (selected_backend IS NOT NULL AND backend_selection_reason IS NOT NULL
             AND backend_selected_at IS NOT NULL)
          ),
          ADD CONSTRAINT trials_backend_incompatibility_array_check
            CHECK (jsonb_typeof(backend_incompatibility_reasons) = 'array');
        CREATE INDEX trials_backend_overflow_queue_idx
          ON trials (submitted_at, id)
          WHERE state = 'queued' AND selected_backend IS NULL;
        """
    )


def downgrade() -> None:
    op.get_bind().exec_driver_sql(
        """
        DROP INDEX trials_backend_overflow_queue_idx;
        ALTER TABLE trials
          DROP CONSTRAINT trials_backend_incompatibility_array_check,
          DROP CONSTRAINT trials_backend_selection_group_check,
          DROP CONSTRAINT trials_selected_backend_check,
          DROP CONSTRAINT trials_backend_policy_digest_check,
          DROP CONSTRAINT trials_backend_policy_object_check,
          DROP COLUMN backend_incompatibility_reasons,
          DROP COLUMN backend_selected_at,
          DROP COLUMN backend_selection_reason,
          DROP COLUMN selected_backend,
          DROP COLUMN backend_policy_digest,
          DROP COLUMN backend_policy_snapshot;
        ALTER TABLE batches
          DROP CONSTRAINT batches_backend_policy_digest_check,
          DROP CONSTRAINT batches_backend_policy_object_check,
          DROP COLUMN backend_policy_digest,
          DROP COLUMN backend_policy_snapshot;
        """
    )
