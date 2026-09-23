"""Persist explicit personal-dev capacity membership lifecycle state.

Revision ID: 0139
Revises: 0138
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0139"
down_revision = "0138"
branch_labels = None
depends_on = None

_ENV_PROJECTION = (
    "(capacity_configuration_epoch IS NULL AND capacity_configuration_sha256 IS NULL "
    "AND capacity_reporter_incarnation IS NULL AND capacity_reporter_token_sha256 IS NULL "
    "AND local_activation_sha256 IS NULL AND protected_admission_sha256 IS NULL "
    "AND capacity_agent_installation_sha256 IS NULL AND capacity_supported_pool_ids IS NULL "
    "AND capacity_supported_architectures IS NULL) OR ("
    "capacity_reporter_incarnation IS NOT NULL "
    "AND capacity_reporter_token_sha256 ~ '^[0-9a-f]{64}$' "
    "AND local_activation_sha256 ~ '^[0-9a-f]{64}$' "
    "AND protected_admission_sha256 ~ '^[0-9a-f]{64}$' "
    "AND capacity_agent_installation_sha256 ~ '^[0-9a-f]{64}$' "
    "AND jsonb_typeof(capacity_supported_pool_ids) = 'array' "
    "AND jsonb_array_length(capacity_supported_pool_ids) > 0 "
    "AND jsonb_typeof(capacity_supported_architectures) = 'array' "
    "AND jsonb_array_length(capacity_supported_architectures) > 0 "
    "AND ((accepted_capacity_mode = 'shadow-v1' "
    "AND capacity_configuration_epoch > 0 "
    "AND capacity_configuration_sha256 ~ '^[0-9a-f]{64}$') OR ("
    "accepted_capacity_mode = 'membership-v1' "
    "AND capacity_configuration_epoch IS NULL "
    "AND capacity_configuration_sha256 IS NULL)))"
)
_OLD_ENV_PROJECTION = (
    "(capacity_configuration_epoch IS NULL AND capacity_configuration_sha256 IS NULL "
    "AND capacity_reporter_incarnation IS NULL AND capacity_reporter_token_sha256 IS NULL "
    "AND local_activation_sha256 IS NULL AND protected_admission_sha256 IS NULL "
    "AND capacity_agent_installation_sha256 IS NULL AND capacity_supported_pool_ids IS NULL "
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
    "AND jsonb_array_length(capacity_supported_architectures) > 0)"
)
_OLD_OP_PROJECTION = (
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
    "AND capacity_configuration_sha256 IS NOT NULL))))"
)
_OP_PROJECTION = "capacity_mode = 'membership-v1' OR (" + _OLD_OP_PROJECTION + ")"
_PRE_ABANDONED = (
    "checkpoint <> 'pre_activation_abandoned' OR (kind = 'destroy' AND state = 'succeeded' "
    "AND readiness_evidence_sha256 IS NULL AND activation_acknowledgement_sha256 IS NULL "
    "AND local_activation_sha256 IS NULL "
    "AND capacity_expected_configuration_epoch IS NULL "
    "AND capacity_projection_request_sha256 IS NULL "
    "AND capacity_configuration_epoch IS NULL AND capacity_configuration_sha256 IS NULL "
    "AND capacity_reporter_incarnation IS NULL "
    "AND capacity_reporter_token_sha256 IS NULL AND protected_admission_sha256 IS NULL "
    "AND capacity_agent_installation_sha256 IS NULL "
    "AND capacity_supported_pool_ids IS NULL AND capacity_supported_architectures IS NULL)"
)
_COMPLETION = (
    f"({_PRE_ABANDONED}) AND (state <> 'succeeded' OR kind = 'noop' "
    "OR checkpoint = 'pre_activation_abandoned' OR (capacity_mode = 'shadow-v1' "
    "AND capacity_configuration_epoch IS NOT NULL) OR (capacity_mode = 'membership-v1' "
    "AND ((capacity_membership_envelope -> 'result' IS NOT NULL "
    "AND jsonb_typeof(capacity_membership_envelope -> 'result') = 'object') OR ("
    "kind = 'destroy' AND checkpoint = 'complete' "
    "AND capacity_membership_envelope -> 'historical_outcome' ->> 'outcome' = 'committed' "
    "AND jsonb_typeof(capacity_membership_envelope "
    "-> 'historical_outcome' -> 'receipt') = 'object' "
    "AND jsonb_typeof(capacity_membership_envelope -> 'release') = 'object'))))"
)
_OLD_COMPLETION = (
    f"({_PRE_ABANDONED}) AND (state <> 'succeeded' OR kind = 'noop' "
    "OR checkpoint = 'pre_activation_abandoned' OR capacity_configuration_epoch IS NOT NULL)"
)


def upgrade() -> None:
    op.add_column(
        "dev_instances",
        sa.Column(
            "accepted_capacity_mode",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'shadow-v1'"),
        ),
    )
    op.add_column(
        "dev_instances",
        sa.Column(
            "accepted_capacity_membership_checkpoint",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.drop_constraint(
        "dev_instances_capacity_projection_check", "dev_instances", type_="check"
    )
    op.drop_constraint(
        "dev_instances_personal_readiness_capacity_check", "dev_instances", type_="check"
    )
    op.create_check_constraint(
        "dev_instances_accepted_capacity_mode_check",
        "dev_instances",
        "accepted_capacity_mode IN ('shadow-v1', 'membership-v1') AND (("
        "accepted_capacity_mode = 'shadow-v1' "
        "AND accepted_capacity_membership_checkpoint IS NULL) OR (("
        "accepted_capacity_mode = 'membership-v1' "
        "AND accepted_capacity_membership_checkpoint IS NOT NULL "
        "AND jsonb_typeof(accepted_capacity_membership_checkpoint) = 'object' "
        "AND accepted_capacity_membership_checkpoint->>'schema_version' = '1' "
        "AND jsonb_typeof(accepted_capacity_membership_checkpoint->'execution') = 'object' "
        "AND accepted_capacity_membership_checkpoint->>'namespace_id' IS NOT NULL "
        "AND jsonb_typeof(accepted_capacity_membership_checkpoint->'revision') = 'number' "
        "AND accepted_capacity_membership_checkpoint->>'head_sha256' ~ '^[0-9a-f]{64}$' "
        "AND capacity_reporter_incarnation IS NOT NULL "
        "AND capacity_reporter_token_sha256 IS NOT NULL "
        "AND local_activation_sha256 IS NOT NULL "
        "AND protected_admission_sha256 IS NOT NULL "
        "AND capacity_agent_installation_sha256 IS NOT NULL "
        "AND capacity_supported_pool_ids IS NOT NULL "
        "AND capacity_supported_architectures IS NOT NULL) IS TRUE))",
    )
    op.create_check_constraint(
        "dev_instances_capacity_projection_check", "dev_instances", _ENV_PROJECTION
    )
    op.create_check_constraint(
        "dev_instances_personal_readiness_capacity_check",
        "dev_instances",
        "status <> 'ready' OR candidate_id IS NULL OR (("
        "accepted_capacity_mode = 'shadow-v1' "
        "AND capacity_configuration_epoch IS NOT NULL) OR ("
        "accepted_capacity_mode = 'membership-v1' "
        "AND accepted_capacity_membership_checkpoint IS NOT NULL))",
    )

    op.add_column(
        "dev_lifecycle_operations",
        sa.Column(
            "capacity_mode",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'shadow-v1'"),
        ),
    )
    op.add_column(
        "dev_lifecycle_operations",
        sa.Column(
            "capacity_membership_envelope",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
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
        "dev_lifecycle_operations_request_uidx",
        "dev_lifecycle_operations",
        type_="unique",
    )
    op.create_check_constraint(
        "dev_lifecycle_operations_capacity_mode_check",
        "dev_lifecycle_operations",
        "capacity_mode IN ('shadow-v1', 'membership-v1') AND (("
        "capacity_mode = 'shadow-v1' AND capacity_membership_envelope IS NULL) OR ("
        "capacity_mode = 'membership-v1' "
        "AND capacity_expected_configuration_epoch IS NULL "
        "AND capacity_projection_request_sha256 IS NULL "
        "AND capacity_configuration_epoch IS NULL "
        "AND capacity_configuration_sha256 IS NULL "
        "AND (capacity_membership_envelope IS NULL OR "
        "(jsonb_typeof(capacity_membership_envelope) = 'object' "
        "AND capacity_membership_envelope->>'schema_version' = '1' "
        "AND capacity_membership_envelope->>'mode' = 'membership-v1' "
        "AND jsonb_typeof(capacity_membership_envelope->'request') = 'object' "
        "AND jsonb_typeof(capacity_membership_envelope->'observation') = 'object' "
        "AND jsonb_typeof(capacity_membership_envelope->'expected_checkpoint') = 'object' "
        "AND capacity_membership_envelope->>'request_sha256' ~ '^[0-9a-f]{64}$' "
        "AND capacity_membership_envelope->>'idempotency_key' IS NOT NULL "
        "AND jsonb_typeof(capacity_membership_envelope->'result') "
        "IN ('null', 'object') "
        "AND jsonb_typeof(capacity_membership_envelope->'historical_outcome') "
        "IN ('null', 'object') "
        "AND (jsonb_typeof(capacity_membership_envelope->'release') = 'null' OR ("
        "jsonb_typeof(capacity_membership_envelope->'release') = 'object' "
        "AND capacity_membership_envelope->'release'->>'schema_version' = '1' "
        "AND capacity_membership_envelope->'release'->>'outcome' = 'verified' "
        "AND capacity_membership_envelope->'release'->>'query_sha256' "
        "~ '^[0-9a-f]{64}$' "
        "AND jsonb_typeof(capacity_membership_envelope->'release' "
        "->'membership_receipt') = 'object' "
        "AND jsonb_typeof(capacity_membership_envelope->'release'->'current') = 'object' "
        "AND capacity_membership_envelope->'release'->>'historical' = 'true' "
        "AND capacity_membership_envelope->'release'->>'worker_available' = 'false' "
        "AND jsonb_typeof(capacity_membership_envelope->'release' "
        "->'incarnation_work') = 'object' "
        "AND capacity_membership_envelope->'release'->>'release_set_sha256' "
        "~ '^[0-9a-f]{64}$')) IS TRUE) IS TRUE)) AND (kind = 'noop' OR checkpoint NOT IN ("
        "'capacity_projection_pending', 'capacity_projected', 'cleanup_pending', "
        "'membership_outcome_resolved', 'release_verified', "
        "'local_authority_sealed', 'namespace_deleted', 'database_deleted', "
        "'buckets_deleted', 'tenant_deleted', 'complete') OR ("
        "capacity_membership_envelope IS NOT NULL "
        "AND capacity_reporter_incarnation IS NOT NULL "
        "AND capacity_reporter_token_sha256 IS NOT NULL "
        "AND local_activation_sha256 IS NOT NULL "
        "AND protected_admission_sha256 IS NOT NULL "
        "AND capacity_agent_installation_sha256 IS NOT NULL "
        "AND capacity_supported_pool_ids IS NOT NULL "
        "AND jsonb_typeof(capacity_supported_pool_ids) = 'array' "
        "AND jsonb_array_length(capacity_supported_pool_ids) > 0 "
        "AND capacity_supported_architectures IS NOT NULL "
        "AND jsonb_typeof(capacity_supported_architectures) = 'array' "
        "AND jsonb_array_length(capacity_supported_architectures) > 0)))",
    )
    op.create_check_constraint(
        "dev_lifecycle_operations_capacity_completion_check",
        "dev_lifecycle_operations",
        _COMPLETION,
    )
    op.create_check_constraint(
        "dev_lifecycle_operations_capacity_projection_check",
        "dev_lifecycle_operations",
        _OP_PROJECTION,
    )
    op.create_check_constraint(
        "dev_lifecycle_operations_membership_completion_check",
        "dev_lifecycle_operations",
        "(capacity_mode = 'shadow-v1' OR kind = 'noop' "
        "OR checkpoint = 'pre_activation_abandoned' "
        "OR (checkpoint NOT IN ('capacity_projection_pending', 'capacity_projected', "
        "'cleanup_pending', 'membership_outcome_resolved', 'release_verified', "
        "'local_authority_sealed', 'namespace_deleted', 'database_deleted', "
        "'buckets_deleted', 'tenant_deleted', 'complete') AND ("
        "capacity_membership_envelope IS NULL OR "
        "jsonb_typeof(capacity_membership_envelope -> 'release') = 'null')) OR ("
        "checkpoint = 'capacity_projection_pending' "
        "AND jsonb_typeof(capacity_membership_envelope -> 'result') = 'null' "
        "AND jsonb_typeof(capacity_membership_envelope -> 'historical_outcome') = 'null' "
        "AND jsonb_typeof(capacity_membership_envelope -> 'release') = 'null') "
        "OR (checkpoint = 'membership_outcome_resolved' "
        "AND jsonb_typeof(capacity_membership_envelope -> 'result') = 'null' "
        "AND jsonb_typeof(capacity_membership_envelope -> 'historical_outcome') = 'object' "
        "AND jsonb_typeof(capacity_membership_envelope -> 'release') = 'null') "
        "OR (checkpoint = 'capacity_projected' AND "
        "(capacity_membership_envelope -> 'result' IS NOT NULL AND "
        "jsonb_typeof(capacity_membership_envelope -> 'result') = 'object') AND "
        "jsonb_typeof(capacity_membership_envelope -> 'historical_outcome') = 'null' "
        "AND jsonb_typeof(capacity_membership_envelope -> 'release') = 'null') "
        "OR (checkpoint = 'cleanup_pending' AND kind = 'destroy' "
        "AND jsonb_typeof(capacity_membership_envelope -> 'result') = 'object' "
        "AND jsonb_typeof(capacity_membership_envelope -> 'historical_outcome') = 'null' "
        "AND jsonb_typeof(capacity_membership_envelope -> 'release') = 'null') "
        "OR (kind = 'destroy' AND checkpoint IN ('release_verified', "
        "'local_authority_sealed', 'namespace_deleted', 'database_deleted', "
        "'buckets_deleted', 'tenant_deleted', 'complete') "
        "AND jsonb_typeof(capacity_membership_envelope -> 'release') = 'object' AND (("
        "jsonb_typeof(capacity_membership_envelope -> 'result') = 'object' "
        "AND jsonb_typeof(capacity_membership_envelope -> 'historical_outcome') = 'null') "
        "OR (jsonb_typeof(capacity_membership_envelope -> 'result') = 'null' "
        "AND capacity_membership_envelope -> 'historical_outcome' ->> 'outcome' "
        "= 'committed' AND jsonb_typeof(capacity_membership_envelope "
        "-> 'historical_outcome' -> 'receipt') = 'object'))) "
        "OR (checkpoint = 'complete' AND kind <> 'destroy' "
        "AND jsonb_typeof(capacity_membership_envelope -> 'result') = 'object' "
        "AND jsonb_typeof(capacity_membership_envelope -> 'historical_outcome') = 'null' "
        "AND jsonb_typeof(capacity_membership_envelope -> 'release') = 'null')) "
        "IS TRUE",
    )
    op.create_unique_constraint(
        "dev_lifecycle_operations_request_uidx",
        "dev_lifecycle_operations",
        [
            "subject_id",
            "subject_incarnation",
            "expected_operation_epoch",
            "request_sha256",
            "capacity_mode",
        ],
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM dev_lifecycle_operations
                       WHERE capacity_mode <> 'shadow-v1')
               OR EXISTS (SELECT 1 FROM dev_instances
                          WHERE accepted_capacity_mode <> 'shadow-v1') THEN
                RAISE EXCEPTION 'cannot downgrade 0139 with membership lifecycle records';
            END IF;
        END $$;
        """
    )
    op.drop_constraint(
        "dev_lifecycle_operations_request_uidx",
        "dev_lifecycle_operations",
        type_="unique",
    )
    op.drop_constraint(
        "dev_lifecycle_operations_membership_completion_check",
        "dev_lifecycle_operations",
        type_="check",
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
        "dev_lifecycle_operations_capacity_mode_check",
        "dev_lifecycle_operations",
        type_="check",
    )
    op.create_check_constraint(
        "dev_lifecycle_operations_capacity_completion_check",
        "dev_lifecycle_operations",
        _OLD_COMPLETION,
    )
    op.create_check_constraint(
        "dev_lifecycle_operations_capacity_projection_check",
        "dev_lifecycle_operations",
        _OLD_OP_PROJECTION,
    )
    op.create_unique_constraint(
        "dev_lifecycle_operations_request_uidx",
        "dev_lifecycle_operations",
        ["subject_id", "subject_incarnation", "expected_operation_epoch", "request_sha256"],
    )
    op.drop_column("dev_lifecycle_operations", "capacity_membership_envelope")
    op.drop_column("dev_lifecycle_operations", "capacity_mode")

    op.drop_constraint(
        "dev_instances_personal_readiness_capacity_check", "dev_instances", type_="check"
    )
    op.drop_constraint(
        "dev_instances_capacity_projection_check", "dev_instances", type_="check"
    )
    op.drop_constraint(
        "dev_instances_accepted_capacity_mode_check", "dev_instances", type_="check"
    )
    op.create_check_constraint(
        "dev_instances_capacity_projection_check", "dev_instances", _OLD_ENV_PROJECTION
    )
    op.create_check_constraint(
        "dev_instances_personal_readiness_capacity_check",
        "dev_instances",
        "status <> 'ready' OR candidate_id IS NULL OR capacity_configuration_epoch IS NOT NULL",
    )
    op.drop_column("dev_instances", "accepted_capacity_membership_checkpoint")
    op.drop_column("dev_instances", "accepted_capacity_mode")
