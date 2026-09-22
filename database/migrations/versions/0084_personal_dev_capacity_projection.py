"""Gate personal-dev readiness on global capacity projection.

Revision ID: 0084
Revises: 0083
Create Date: 2026-08-11
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0084"
down_revision = "0083"
branch_labels = None
depends_on = None

_DIGEST_COLUMNS = (
    "capacity_configuration_sha256",
    "capacity_reporter_token_sha256",
    "local_activation_sha256",
    "protected_admission_sha256",
    "capacity_agent_installation_sha256",
)


def _add_current_projection_columns() -> None:
    op.add_column(
        "dev_instances",
        sa.Column("capacity_configuration_epoch", sa.BigInteger(), nullable=True),
    )
    for name in _DIGEST_COLUMNS:
        op.add_column(
            "dev_instances",
            sa.Column(name, sa.String(length=64), nullable=True),
        )
    op.add_column(
        "dev_instances",
        sa.Column(
            "capacity_reporter_incarnation",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
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
    op.create_check_constraint(
        "dev_instances_personal_readiness_capacity_check",
        "dev_instances",
        "status <> 'ready' OR candidate_id IS NULL "
        "OR capacity_configuration_epoch IS NOT NULL",
    )


def _add_operation_projection_columns() -> None:
    columns = (
        sa.Column("local_activation_sha256", sa.String(length=64), nullable=True),
        sa.Column(
            "capacity_expected_configuration_epoch",
            sa.BigInteger(),
            nullable=True,
        ),
        sa.Column(
            "capacity_projection_request_sha256",
            sa.String(length=64),
            nullable=True,
        ),
        sa.Column("capacity_configuration_epoch", sa.BigInteger(), nullable=True),
        sa.Column(
            "capacity_configuration_sha256",
            sa.String(length=64),
            nullable=True,
        ),
        sa.Column(
            "capacity_reporter_incarnation",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column(
            "capacity_reporter_token_sha256",
            sa.String(length=64),
            nullable=True,
        ),
        sa.Column(
            "protected_admission_sha256",
            sa.String(length=64),
            nullable=True,
        ),
        sa.Column(
            "capacity_agent_installation_sha256",
            sa.String(length=64),
            nullable=True,
        ),
    )
    for column in columns:
        op.add_column("dev_lifecycle_operations", column)
    op.drop_constraint(
        "dev_lifecycle_operations_digests_check",
        "dev_lifecycle_operations",
        type_="check",
    )
    op.create_check_constraint(
        "dev_lifecycle_operations_digests_check",
        "dev_lifecycle_operations",
        "request_sha256 ~ '^[0-9a-f]{64}$' "
        "AND candidate_sha ~ '^[0-9a-f]{64}$' "
        "AND (readiness_evidence_sha256 IS NULL OR "
        "readiness_evidence_sha256 ~ '^[0-9a-f]{64}$') "
        "AND (activation_acknowledgement_sha256 IS NULL OR "
        "activation_acknowledgement_sha256 ~ '^[0-9a-f]{64}$') "
        "AND (local_activation_sha256 IS NULL OR "
        "local_activation_sha256 ~ '^[0-9a-f]{64}$') "
        "AND (capacity_projection_request_sha256 IS NULL OR "
        "capacity_projection_request_sha256 ~ '^[0-9a-f]{64}$') "
        "AND (capacity_configuration_sha256 IS NULL OR "
        "capacity_configuration_sha256 ~ '^[0-9a-f]{64}$') "
        "AND (capacity_reporter_token_sha256 IS NULL OR "
        "capacity_reporter_token_sha256 ~ '^[0-9a-f]{64}$') "
        "AND (protected_admission_sha256 IS NULL OR "
        "protected_admission_sha256 ~ '^[0-9a-f]{64}$') "
        "AND (capacity_agent_installation_sha256 IS NULL OR "
        "capacity_agent_installation_sha256 ~ '^[0-9a-f]{64}$')",
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
    op.create_check_constraint(
        "dev_lifecycle_operations_capacity_completion_check",
        "dev_lifecycle_operations",
        "state <> 'succeeded' OR kind = 'noop' "
        "OR capacity_configuration_epoch IS NOT NULL",
    )


def upgrade() -> None:
    # The preceding package was activation-blocked. Refuse to reinterpret a
    # lifecycle that an operator ran ahead of that gate: those rows have no
    # protected manager projection and cannot be made trustworthy by inference.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM dev_lifecycle_operations) THEN
                RAISE EXCEPTION
                    'cannot upgrade 0084 with pre-projection personal-dev lifecycle rows';
            END IF;
        END
        $$
        """
    )
    _add_current_projection_columns()
    _add_operation_projection_columns()


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM dev_instances
                 WHERE capacity_configuration_epoch IS NOT NULL
                    OR capacity_configuration_sha256 IS NOT NULL
                    OR capacity_reporter_incarnation IS NOT NULL
                    OR capacity_reporter_token_sha256 IS NOT NULL
                    OR local_activation_sha256 IS NOT NULL
                    OR protected_admission_sha256 IS NOT NULL
                    OR capacity_agent_installation_sha256 IS NOT NULL
            ) OR EXISTS (
                SELECT 1 FROM dev_lifecycle_operations
                 WHERE local_activation_sha256 IS NOT NULL
                    OR capacity_expected_configuration_epoch IS NOT NULL
                    OR capacity_projection_request_sha256 IS NOT NULL
                    OR capacity_configuration_epoch IS NOT NULL
                    OR capacity_configuration_sha256 IS NOT NULL
                    OR capacity_reporter_incarnation IS NOT NULL
                    OR capacity_reporter_token_sha256 IS NOT NULL
                    OR protected_admission_sha256 IS NOT NULL
                    OR capacity_agent_installation_sha256 IS NOT NULL
            ) THEN
                RAISE EXCEPTION
                    'cannot downgrade 0084 after personal-dev capacity evidence exists';
            END IF;
        END
        $$
        """
    )
    op.drop_constraint(
        "dev_lifecycle_operations_capacity_completion_check",
        "dev_lifecycle_operations",
        type_="check",
    )
    op.drop_constraint(
        "dev_lifecycle_operations_capacity_projection_check",
        "dev_lifecycle_operations",
        type_="check",
    )
    op.drop_constraint(
        "dev_lifecycle_operations_digests_check",
        "dev_lifecycle_operations",
        type_="check",
    )
    op.create_check_constraint(
        "dev_lifecycle_operations_digests_check",
        "dev_lifecycle_operations",
        "request_sha256 ~ '^[0-9a-f]{64}$' "
        "AND candidate_sha ~ '^[0-9a-f]{64}$' "
        "AND (readiness_evidence_sha256 IS NULL OR "
        "readiness_evidence_sha256 ~ '^[0-9a-f]{64}$') "
        "AND (activation_acknowledgement_sha256 IS NULL OR "
        "activation_acknowledgement_sha256 ~ '^[0-9a-f]{64}$')",
    )
    for name in (
        "capacity_agent_installation_sha256",
        "protected_admission_sha256",
        "capacity_reporter_token_sha256",
        "capacity_reporter_incarnation",
        "capacity_configuration_sha256",
        "capacity_configuration_epoch",
        "capacity_projection_request_sha256",
        "capacity_expected_configuration_epoch",
        "local_activation_sha256",
    ):
        op.drop_column("dev_lifecycle_operations", name)
    op.drop_constraint(
        "dev_instances_capacity_projection_check",
        "dev_instances",
        type_="check",
    )
    op.drop_constraint(
        "dev_instances_personal_readiness_capacity_check",
        "dev_instances",
        type_="check",
    )
    for name in (
        "capacity_reporter_incarnation",
        "capacity_agent_installation_sha256",
        "protected_admission_sha256",
        "local_activation_sha256",
        "capacity_reporter_token_sha256",
        "capacity_configuration_sha256",
        "capacity_configuration_epoch",
    ):
        op.drop_column("dev_instances", name)
