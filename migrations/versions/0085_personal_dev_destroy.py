"""Add durable personal-development teardown operations.

Revision ID: 0085
Revises: 0084
Create Date: 2026-08-11
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0085"
down_revision = "0084"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Package 4 is not active yet. Refuse to guess capability evidence for a
    # lifecycle that was run ahead of the guarded rollout stack.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM dev_lifecycle_operations) THEN
                RAISE EXCEPTION
                    'cannot upgrade 0085 with pre-destroy personal-dev lifecycle rows';
            END IF;
        END
        $$
        """
    )
    for table in ("dev_instances", "dev_lifecycle_operations"):
        op.add_column(
            table,
            sa.Column("capacity_supported_pool_ids", postgresql.JSONB(), nullable=True),
        )
        op.add_column(
            table,
            sa.Column(
                "capacity_supported_architectures",
                postgresql.JSONB(),
                nullable=True,
            ),
        )
    op.add_column(
        "dev_lifecycle_operations",
        sa.Column(
            "keep_data",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.drop_constraint(
        "dev_instances_capacity_projection_check",
        "dev_instances",
        type_="check",
    )
    op.create_check_constraint(
        "dev_instances_capacity_projection_check",
        "dev_instances",
        "(capacity_configuration_epoch IS NULL "
        "AND capacity_configuration_sha256 IS NULL "
        "AND capacity_reporter_incarnation IS NULL "
        "AND capacity_reporter_token_sha256 IS NULL "
        "AND local_activation_sha256 IS NULL "
        "AND protected_admission_sha256 IS NULL "
        "AND capacity_agent_installation_sha256 IS NULL "
        "AND capacity_supported_pool_ids IS NULL "
        "AND capacity_supported_architectures IS NULL) OR ("
        "capacity_configuration_epoch > 0 "
        "AND capacity_configuration_sha256 ~ '^[0-9a-f]{64}$' "
        "AND capacity_reporter_incarnation IS NOT NULL "
        "AND capacity_reporter_token_sha256 ~ '^[0-9a-f]{64}$' "
        "AND local_activation_sha256 ~ '^[0-9a-f]{64}$' "
        "AND protected_admission_sha256 ~ '^[0-9a-f]{64}$' "
        "AND capacity_agent_installation_sha256 ~ '^[0-9a-f]{64}$' "
        "AND jsonb_typeof(capacity_supported_pool_ids) = 'array' "
        "AND jsonb_array_length(capacity_supported_pool_ids) > 0 "
        "AND jsonb_typeof(capacity_supported_architectures) = 'array' "
        "AND jsonb_array_length(capacity_supported_architectures) > 0)",
    )
    op.drop_constraint(
        "dev_lifecycle_operations_capacity_projection_check",
        "dev_lifecycle_operations",
        type_="check",
    )
    op.create_check_constraint(
        "dev_lifecycle_operations_capacity_projection_check",
        "dev_lifecycle_operations",
        "(capacity_expected_configuration_epoch IS NULL "
        "AND capacity_projection_request_sha256 IS NULL "
        "AND capacity_configuration_epoch IS NULL "
        "AND capacity_configuration_sha256 IS NULL "
        "AND capacity_reporter_incarnation IS NULL "
        "AND capacity_reporter_token_sha256 IS NULL "
        "AND protected_admission_sha256 IS NULL "
        "AND capacity_agent_installation_sha256 IS NULL "
        "AND capacity_supported_pool_ids IS NULL "
        "AND capacity_supported_architectures IS NULL) OR (("
        "kind = 'destroy' "
        "AND capacity_expected_configuration_epoch IS NULL "
        "AND capacity_projection_request_sha256 IS NULL "
        "AND capacity_configuration_epoch IS NULL "
        "AND capacity_configuration_sha256 IS NULL "
        "AND local_activation_sha256 IS NOT NULL "
        "AND capacity_reporter_incarnation IS NOT NULL "
        "AND capacity_reporter_token_sha256 IS NOT NULL "
        "AND protected_admission_sha256 IS NOT NULL "
        "AND capacity_agent_installation_sha256 IS NOT NULL "
        "AND jsonb_typeof(capacity_supported_pool_ids) = 'array' "
        "AND jsonb_array_length(capacity_supported_pool_ids) > 0 "
        "AND jsonb_typeof(capacity_supported_architectures) = 'array' "
        "AND jsonb_array_length(capacity_supported_architectures) > 0) OR ("
        "capacity_expected_configuration_epoch > 0 "
        "AND local_activation_sha256 IS NOT NULL "
        "AND capacity_projection_request_sha256 IS NOT NULL "
        "AND capacity_reporter_incarnation IS NOT NULL "
        "AND capacity_reporter_token_sha256 IS NOT NULL "
        "AND protected_admission_sha256 IS NOT NULL "
        "AND capacity_agent_installation_sha256 IS NOT NULL "
        "AND jsonb_typeof(capacity_supported_pool_ids) = 'array' "
        "AND jsonb_array_length(capacity_supported_pool_ids) > 0 "
        "AND jsonb_typeof(capacity_supported_architectures) = 'array' "
        "AND jsonb_array_length(capacity_supported_architectures) > 0 "
        "AND ((capacity_configuration_epoch IS NULL "
        "AND capacity_configuration_sha256 IS NULL) OR ("
        "capacity_configuration_epoch = capacity_expected_configuration_epoch + 1 "
        "AND capacity_configuration_sha256 IS NOT NULL))))",
    )
    op.drop_constraint(
        "dev_lifecycle_operations_kind_check",
        "dev_lifecycle_operations",
        type_="check",
    )
    op.create_check_constraint(
        "dev_lifecycle_operations_kind_check",
        "dev_lifecycle_operations",
        "kind IN ('create', 'update', 'capacity', 'destroy', 'noop')",
    )
    op.drop_constraint(
        "dev_lifecycle_operations_activation_evidence_check",
        "dev_lifecycle_operations",
        type_="check",
    )
    op.create_check_constraint(
        "dev_lifecycle_operations_activation_evidence_check",
        "dev_lifecycle_operations",
        "(kind IN ('capacity', 'destroy', 'noop') "
        "AND readiness_evidence_sha256 IS NULL "
        "AND activation_acknowledgement_sha256 IS NULL) OR "
        "(kind IN ('create', 'update') AND ("
        "(state IN ('requested', 'running', 'failed', 'cancelling', 'cancelled') "
        "AND readiness_evidence_sha256 IS NULL "
        "AND activation_acknowledgement_sha256 IS NULL) OR "
        "(state = 'activating' AND readiness_evidence_sha256 IS NOT NULL) OR "
        "(state = 'succeeded' AND readiness_evidence_sha256 IS NOT NULL "
        "AND activation_acknowledgement_sha256 IS NOT NULL)))",
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM dev_lifecycle_operations WHERE kind = 'destroy'
            ) THEN
                RAISE EXCEPTION
                    'cannot downgrade 0085 after personal-dev destroy authority exists';
            END IF;
        END
        $$
        """
    )
    op.drop_constraint(
        "dev_lifecycle_operations_capacity_projection_check",
        "dev_lifecycle_operations",
        type_="check",
    )
    op.create_check_constraint(
        "dev_lifecycle_operations_capacity_projection_check",
        "dev_lifecycle_operations",
        "(capacity_expected_configuration_epoch IS NULL "
        "AND capacity_projection_request_sha256 IS NULL "
        "AND capacity_configuration_epoch IS NULL "
        "AND capacity_configuration_sha256 IS NULL "
        "AND capacity_reporter_incarnation IS NULL "
        "AND capacity_reporter_token_sha256 IS NULL "
        "AND protected_admission_sha256 IS NULL "
        "AND capacity_agent_installation_sha256 IS NULL) OR ("
        "capacity_expected_configuration_epoch > 0 "
        "AND local_activation_sha256 IS NOT NULL "
        "AND capacity_projection_request_sha256 IS NOT NULL "
        "AND capacity_reporter_incarnation IS NOT NULL "
        "AND capacity_reporter_token_sha256 IS NOT NULL "
        "AND protected_admission_sha256 IS NOT NULL "
        "AND capacity_agent_installation_sha256 IS NOT NULL "
        "AND ((capacity_configuration_epoch IS NULL "
        "AND capacity_configuration_sha256 IS NULL) OR ("
        "capacity_configuration_epoch = capacity_expected_configuration_epoch + 1 "
        "AND capacity_configuration_sha256 IS NOT NULL)))",
    )
    op.drop_constraint(
        "dev_instances_capacity_projection_check",
        "dev_instances",
        type_="check",
    )
    op.create_check_constraint(
        "dev_instances_capacity_projection_check",
        "dev_instances",
        "(capacity_configuration_epoch IS NULL "
        "AND capacity_configuration_sha256 IS NULL "
        "AND capacity_reporter_incarnation IS NULL "
        "AND capacity_reporter_token_sha256 IS NULL "
        "AND local_activation_sha256 IS NULL "
        "AND protected_admission_sha256 IS NULL "
        "AND capacity_agent_installation_sha256 IS NULL) OR ("
        "capacity_configuration_epoch > 0 "
        "AND capacity_configuration_sha256 ~ '^[0-9a-f]{64}$' "
        "AND capacity_reporter_incarnation IS NOT NULL "
        "AND capacity_reporter_token_sha256 ~ '^[0-9a-f]{64}$' "
        "AND local_activation_sha256 ~ '^[0-9a-f]{64}$' "
        "AND protected_admission_sha256 ~ '^[0-9a-f]{64}$' "
        "AND capacity_agent_installation_sha256 ~ '^[0-9a-f]{64}$')",
    )
    op.drop_constraint(
        "dev_lifecycle_operations_activation_evidence_check",
        "dev_lifecycle_operations",
        type_="check",
    )
    op.create_check_constraint(
        "dev_lifecycle_operations_activation_evidence_check",
        "dev_lifecycle_operations",
        "(kind IN ('capacity', 'noop') "
        "AND readiness_evidence_sha256 IS NULL "
        "AND activation_acknowledgement_sha256 IS NULL) OR "
        "(kind IN ('create', 'update') AND ("
        "(state IN ('requested', 'running', 'failed', 'cancelling', 'cancelled') "
        "AND readiness_evidence_sha256 IS NULL "
        "AND activation_acknowledgement_sha256 IS NULL) OR "
        "(state = 'activating' AND readiness_evidence_sha256 IS NOT NULL) OR "
        "(state = 'succeeded' AND readiness_evidence_sha256 IS NOT NULL "
        "AND activation_acknowledgement_sha256 IS NOT NULL)))",
    )
    op.drop_constraint(
        "dev_lifecycle_operations_kind_check",
        "dev_lifecycle_operations",
        type_="check",
    )
    op.create_check_constraint(
        "dev_lifecycle_operations_kind_check",
        "dev_lifecycle_operations",
        "kind IN ('create', 'update', 'capacity', 'noop')",
    )
    op.drop_column("dev_lifecycle_operations", "keep_data")
    for table in ("dev_lifecycle_operations", "dev_instances"):
        op.drop_column(table, "capacity_supported_architectures")
        op.drop_column(table, "capacity_supported_pool_ids")
