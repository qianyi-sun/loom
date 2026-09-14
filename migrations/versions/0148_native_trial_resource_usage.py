"""Retain native resource samples under execution lease and Pod identity.

Revision ID: 0148
Revises: 0147
"""
from alembic import op

revision = "0148"
down_revision = "0147"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("LOCK TABLE public.trial_resource_usage, public.execution_leases IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute("""
      ALTER TABLE trial_resource_usage
        ALTER COLUMN worker_id DROP NOT NULL,
        ADD COLUMN execution_lease_id uuid REFERENCES execution_leases(id) ON DELETE RESTRICT,
        ADD COLUMN resource_generation integer,
        ADD COLUMN target_id text,
        ADD COLUMN pod_uid text,
        ADD COLUMN cpu_sampled_max_nanocores bigint CHECK (cpu_sampled_max_nanocores >= 0),
        ADD COLUMN memory_sampled_max_bytes bigint CHECK (memory_sampled_max_bytes >= 0),
        ADD COLUMN filesystem_sampled_max_bytes bigint CHECK (filesystem_sampled_max_bytes >= 0),
        ADD COLUMN ephemeral_storage_sampled_max_bytes bigint CHECK (ephemeral_storage_sampled_max_bytes >= 0),
        DROP CONSTRAINT trial_resource_usage_role_check,
        DROP CONSTRAINT trial_resource_usage_source_check,
        ADD CONSTRAINT trial_resource_usage_role_check CHECK (
          container_role IN ('agent','verifier','sidecar','controller','task','pod')),
        ADD CONSTRAINT trial_resource_usage_source_check CHECK (
          source IN ('docker_stats','provider','unsupported','kubelet_summary')),
        ADD CONSTRAINT trial_resource_usage_authority_check CHECK (
          (worker_id IS NOT NULL AND execution_lease_id IS NULL AND resource_generation IS NULL
           AND target_id IS NULL AND pod_uid IS NULL) OR
          (worker_id IS NULL AND execution_lease_id IS NOT NULL AND resource_generation IS NOT NULL AND resource_generation > 0
           AND target_id IS NOT NULL AND pod_uid IS NOT NULL));
      CREATE INDEX trial_resource_usage_native_lease_idx ON trial_resource_usage(execution_lease_id)
        WHERE execution_lease_id IS NOT NULL;
    """)


def downgrade() -> None:
    op.execute("LOCK TABLE public.trial_resource_usage IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute("""
      DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM trial_resource_usage WHERE execution_lease_id IS NOT NULL) THEN
          RAISE EXCEPTION 'native usage must be retained; refusing destructive downgrade';
        END IF;
      END $$;
      DROP INDEX trial_resource_usage_native_lease_idx;
      ALTER TABLE trial_resource_usage
        DROP CONSTRAINT trial_resource_usage_authority_check,
        DROP CONSTRAINT trial_resource_usage_role_check,
        DROP CONSTRAINT trial_resource_usage_source_check,
        ADD CONSTRAINT trial_resource_usage_role_check CHECK (container_role IN ('agent','verifier','sidecar')),
        ADD CONSTRAINT trial_resource_usage_source_check CHECK (source IN ('docker_stats','provider','unsupported')),
        ALTER COLUMN worker_id SET NOT NULL,
        DROP COLUMN execution_lease_id, DROP COLUMN resource_generation,
        DROP COLUMN target_id, DROP COLUMN pod_uid,
        DROP COLUMN cpu_sampled_max_nanocores, DROP COLUMN memory_sampled_max_bytes,
        DROP COLUMN filesystem_sampled_max_bytes, DROP COLUMN ephemeral_storage_sampled_max_bytes;
    """)
