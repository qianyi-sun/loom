"""Allow pre-activation abandonment and require exact capacity identities.

Revision ID: 0122
Revises: 0121
"""

from alembic import op

revision = "0122"
down_revision = "0121"
branch_labels = None
depends_on = None

_CONSTRAINT = "dev_lifecycle_operations_capacity_completion_check"
_IDENTITY_CONSTRAINT = "dev_instances_personal_capacity_identity_check"
_OLD_RULE = "state <> 'succeeded' OR kind = 'noop' OR capacity_configuration_epoch IS NOT NULL"
_NEW_RULE = (
    "(checkpoint <> 'pre_activation_abandoned' OR ("
    "kind = 'destroy' AND state = 'succeeded' "
    "AND readiness_evidence_sha256 IS NULL "
    "AND activation_acknowledgement_sha256 IS NULL "
    "AND local_activation_sha256 IS NULL "
    "AND capacity_expected_configuration_epoch IS NULL "
    "AND capacity_projection_request_sha256 IS NULL "
    "AND capacity_configuration_epoch IS NULL "
    "AND capacity_configuration_sha256 IS NULL "
    "AND capacity_reporter_incarnation IS NULL "
    "AND capacity_reporter_token_sha256 IS NULL "
    "AND protected_admission_sha256 IS NULL "
    "AND capacity_agent_installation_sha256 IS NULL "
    "AND capacity_supported_pool_ids IS NULL "
    "AND capacity_supported_architectures IS NULL)) AND ("
    "state <> 'succeeded' OR kind = 'noop' "
    "OR checkpoint = 'pre_activation_abandoned' "
    "OR capacity_configuration_epoch IS NOT NULL)"
)
_OLD_IDENTITY_RULE = (
    "(candidate_id IS NULL AND capacity_namespace IS NULL AND capacity_database IS NULL) "
    "OR (candidate_id IS NOT NULL AND capacity_namespace = 'loom-dev-' || name "
    "AND capacity_database = 'loom_dev_' || replace(name, '-', '_'))"
)
_NEW_IDENTITY_RULE = (
    "(candidate_id IS NULL AND capacity_namespace IS NULL AND capacity_database IS NULL) "
    "OR (candidate_id IS NOT NULL AND capacity_namespace IS NOT NULL "
    "AND capacity_database IS NOT NULL "
    "AND capacity_namespace = 'loom-dev-' || name "
    "AND capacity_database = 'loom_dev_' || replace(name, '-', '_'))"
)


def upgrade() -> None:
    op.drop_constraint(_CONSTRAINT, "dev_lifecycle_operations", type_="check")
    op.create_check_constraint(_CONSTRAINT, "dev_lifecycle_operations", _NEW_RULE)
    op.execute(
        "UPDATE dev_instances SET capacity_namespace = 'loom-dev-' || name, "
        "capacity_database = 'loom_dev_' || replace(name, '-', '_') "
        "WHERE candidate_id IS NOT NULL AND ("
        "capacity_namespace IS DISTINCT FROM 'loom-dev-' || name OR "
        "capacity_database IS DISTINCT FROM 'loom_dev_' || replace(name, '-', '_'))"
    )
    op.drop_constraint(_IDENTITY_CONSTRAINT, "dev_instances", type_="check")
    op.create_check_constraint(
        _IDENTITY_CONSTRAINT,
        "dev_instances",
        _NEW_IDENTITY_RULE,
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM dev_lifecycle_operations
                WHERE checkpoint = 'pre_activation_abandoned'
            ) THEN
                RAISE EXCEPTION 'cannot downgrade 0122 with pre-activation abandonment records';
            END IF;
        END $$;
        """
    )
    op.drop_constraint(_CONSTRAINT, "dev_lifecycle_operations", type_="check")
    op.create_check_constraint(_CONSTRAINT, "dev_lifecycle_operations", _OLD_RULE)
    op.drop_constraint(_IDENTITY_CONSTRAINT, "dev_instances", type_="check")
    op.create_check_constraint(
        _IDENTITY_CONSTRAINT,
        "dev_instances",
        _OLD_IDENTITY_RULE,
    )
