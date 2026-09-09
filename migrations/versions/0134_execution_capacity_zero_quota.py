"""Accept exhausted/removed provider quotas as observations.

Revision ID: 0134
Revises: 0133
"""

from alembic import op

revision = "0134"
down_revision = "0133"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE execution_capacity_observations
          DROP CONSTRAINT execution_capacity_observations_quota_check,
          ADD CONSTRAINT execution_capacity_observations_quota_check CHECK (
            provider_quota_nodes >= 0 AND provider_quota_vcpu_millis >= 0
            AND provider_quota_memory_mib >= 0 AND provider_quota_storage_mib >= 0
            AND provider_used_nodes >= 0 AND provider_used_vcpu_millis >= 0
            AND provider_used_memory_mib >= 0 AND provider_used_storage_mib >= 0
          )
    """)


def downgrade() -> None:
    # Never delete or rewrite immutable zero-quota evidence to force a rollback.
    op.execute("""
        ALTER TABLE execution_capacity_observations
          DROP CONSTRAINT execution_capacity_observations_quota_check,
          ADD CONSTRAINT execution_capacity_observations_quota_check CHECK (
            provider_quota_nodes > 0 AND provider_quota_vcpu_millis > 0
            AND provider_quota_memory_mib > 0 AND provider_quota_storage_mib > 0
            AND provider_used_nodes >= 0 AND provider_used_vcpu_millis >= 0
            AND provider_used_memory_mib >= 0 AND provider_used_storage_mib >= 0
          )
    """)
