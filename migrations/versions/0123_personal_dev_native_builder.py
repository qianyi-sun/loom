"""Persist fenced native personal-dev builder agents and grants.

Revision ID: 0123
Revises: 0122
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0123"
down_revision = "0122"
branch_labels = None
depends_on = None

_AGENT_IDENTITY_CHECK = (
    "key_id ~ '^[a-z][a-z0-9._-]{0,63}$' "
    "AND provider = 'gb10-gvisor-docker-v1' "
    "AND platform = 'linux/arm64' "
    "AND protocol_version = 1 "
    "AND host_architecture = 'aarch64' "
    "AND host_name <> '' AND host_name = btrim(host_name) "
    "AND octet_length(agent_image) BETWEEN 73 AND 584 "
    "AND agent_image ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64}$' "
    "AND octet_length(builder_image) BETWEEN 73 AND 584 "
    "AND builder_image ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64}$' "
    "AND runtime_profile_sha256 ~ '^[0-9a-f]{64}$' "
    "AND readiness_evidence_sha256 ~ '^[0-9a-f]{64}$' "
    "AND status_sha256 ~ '^[0-9a-f]{64}$' "
    "AND max_concurrency = 2"
)
_AGENT_INVENTORY_CHECK = (
    "jsonb_typeof(managed_grant_ids_json) = 'array' "
    "AND jsonb_array_length(managed_grant_ids_json) <= 64 "
    "AND jsonb_typeof(active_grant_ids_json) = 'array' "
    "AND jsonb_array_length(active_grant_ids_json) <= 2"
)
_AGENT_STATUS_CHECK = (
    "available = (unavailable_reason IS NULL) "
    "AND (unavailable_reason IS NULL OR unavailable_reason ~ '^[a-z][a-z0-9_]{0,127}$') "
    "AND jsonb_typeof(status_json) = 'object' "
    "AND ((status_json->>'agent_instance_id' = instance_id::text "
    "AND status_json->>'agent_key_id' = key_id "
    "AND status_json->>'provider' = provider "
    "AND status_json->>'platform' = platform "
    "AND (status_json->>'protocol_version')::integer = protocol_version "
    "AND status_json->>'host_name' = host_name "
    "AND status_json->>'host_architecture' = host_architecture "
    "AND status_json->>'host_boot_id' = host_boot_id::text "
    "AND status_json->>'agent_image' = agent_image "
    "AND status_json->>'builder_image' = builder_image "
    "AND status_json->>'runtime_profile_sha256' = runtime_profile_sha256 "
    "AND (status_json->>'max_concurrency')::integer = max_concurrency "
    "AND status_json->'managed_grant_ids' = managed_grant_ids_json "
    "AND status_json->'active_grant_ids' = active_grant_ids_json "
    "AND (status_json->>'available')::boolean = available "
    "AND status_json->>'unavailable_reason' IS NOT DISTINCT FROM unavailable_reason "
    "AND status_json->>'readiness_evidence_sha256' = readiness_evidence_sha256) IS TRUE) "
    "AND first_seen_at <= last_seen_at AND last_seen_at <= updated_at"
)
_GRANT_IDENTITY_CHECK = (
    "attempt_lease_epoch > 0 "
    "AND platform = 'linux/arm64' "
    "AND provider = 'gb10-gvisor-docker-v1' "
    "AND required_agent_key_id ~ '^[a-z][a-z0-9._-]{0,63}$' "
    "AND (running_agent_instance_id IS NULL "
    "OR running_agent_instance_id = required_agent_instance_id) "
    "AND octet_length(agent_image) BETWEEN 73 AND 584 "
    "AND agent_image ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64}$' "
    "AND octet_length(builder_image) BETWEEN 73 AND 584 "
    "AND builder_image ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64}$' "
    "AND runtime_profile_sha256 ~ '^[0-9a-f]{64}$' "
    "AND contract_sha256 ~ '^[0-9a-f]{64}$' "
    "AND octet_length(contract_json) BETWEEN 2 AND 65536 "
    "AND artifact_max_bytes BETWEEN 1 AND 17179869184 "
    "AND active_deadline_seconds BETWEEN 300 AND 7200"
)
_GRANT_OBJECT_BINDING_CHECK = (
    "source_bucket <> '' AND source_bucket = btrim(source_bucket) "
    "AND position('/' in source_bucket) = 0 "
    "AND artifact_bucket = source_bucket "
    "AND source_object_key <> '' AND artifact_object_key <> '' "
    "AND octet_length(source_object_key) <= 2048 "
    "AND octet_length(artifact_object_key) <= 2048 "
    "AND source_object_key !~ '[[:cntrl:]]' "
    "AND artifact_object_key !~ '[[:cntrl:]]' "
    "AND artifact_object_key LIKE 'personal-dev/builds/%/artifacts.tar'"
)
_GRANT_STATE_CHECK = "state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')"
_GRANT_TERMINAL_CHECK = (
    "((last_request_at IS NULL AND last_request_nonce IS NULL) OR "
    "(last_request_at IS NOT NULL AND last_request_nonce IS NOT NULL)) AND ("
    "(state = 'queued' AND running_agent_instance_id IS NULL "
    "AND started_at IS NULL AND heartbeat_at IS NULL AND finished_at IS NULL "
    "AND failure_reason IS NULL AND completion_json IS NULL "
    "AND completion_sha256 IS NULL AND runtime_evidence_json IS NULL "
    "AND runtime_evidence_sha256 IS NULL AND artifact_head_json IS NULL "
    "AND artifact_head_sha256 IS NULL) OR ("
    "state = 'running' AND running_agent_instance_id IS NOT NULL "
    "AND started_at IS NOT NULL AND heartbeat_at IS NOT NULL AND finished_at IS NULL "
    "AND failure_reason IS NULL AND completion_json IS NULL "
    "AND completion_sha256 IS NULL AND runtime_evidence_json IS NULL "
    "AND runtime_evidence_sha256 IS NULL AND artifact_head_json IS NULL "
    "AND artifact_head_sha256 IS NULL) OR ("
    "state = 'succeeded' AND running_agent_instance_id IS NOT NULL "
    "AND started_at IS NOT NULL AND heartbeat_at IS NOT NULL AND finished_at IS NOT NULL "
    "AND failure_reason IS NULL AND jsonb_typeof(completion_json) = 'object' "
    "AND completion_sha256 ~ '^[0-9a-f]{64}$' "
    "AND jsonb_typeof(runtime_evidence_json) = 'object' "
    "AND runtime_evidence_sha256 ~ '^[0-9a-f]{64}$' "
    "AND jsonb_typeof(artifact_head_json) = 'object' "
    "AND artifact_head_sha256 ~ '^[0-9a-f]{64}$') OR ("
    "state = 'failed' AND running_agent_instance_id IS NOT NULL "
    "AND started_at IS NOT NULL AND heartbeat_at IS NOT NULL AND finished_at IS NOT NULL "
    "AND failure_reason ~ '^[a-z][a-z0-9_]{0,127}$' "
    "AND jsonb_typeof(completion_json) = 'object' "
    "AND completion_sha256 ~ '^[0-9a-f]{64}$' "
    "AND runtime_evidence_json IS NULL AND runtime_evidence_sha256 IS NULL "
    "AND artifact_head_json IS NULL AND artifact_head_sha256 IS NULL) OR ("
    "state = 'cancelled' AND finished_at IS NOT NULL "
    "AND failure_reason ~ '^[a-z][a-z0-9_]{0,127}$' "
    "AND ((running_agent_instance_id IS NULL AND started_at IS NULL AND heartbeat_at IS NULL) "
    "OR (running_agent_instance_id IS NOT NULL AND started_at IS NOT NULL "
    "AND heartbeat_at IS NOT NULL)) "
    "AND completion_json IS NULL AND completion_sha256 IS NULL "
    "AND runtime_evidence_json IS NULL AND runtime_evidence_sha256 IS NULL "
    "AND artifact_head_json IS NULL AND artifact_head_sha256 IS NULL)) "
    "AND queued_at <= updated_at "
    "AND (started_at IS NULL OR queued_at <= started_at) "
    "AND (heartbeat_at IS NULL OR started_at <= heartbeat_at) "
    "AND (finished_at IS NULL OR COALESCE(heartbeat_at, queued_at) <= finished_at)"
)


def upgrade() -> None:
    op.create_table(
        "personal_dev_native_builder_agents",
        sa.Column("instance_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("key_id", sa.String(64), nullable=False),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("platform", sa.String(32), nullable=False),
        sa.Column("protocol_version", sa.Integer(), nullable=False),
        sa.Column("host_name", sa.String(253), nullable=False),
        sa.Column("host_architecture", sa.String(32), nullable=False),
        sa.Column("host_boot_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("agent_image", sa.Text(), nullable=False),
        sa.Column("builder_image", sa.Text(), nullable=False),
        sa.Column("runtime_profile_sha256", sa.String(64), nullable=False),
        sa.Column("max_concurrency", sa.Integer(), nullable=False),
        sa.Column("managed_grant_ids_json", postgresql.JSONB(), nullable=False),
        sa.Column("active_grant_ids_json", postgresql.JSONB(), nullable=False),
        sa.Column("available", sa.Boolean(), nullable=False),
        sa.Column("unavailable_reason", sa.String(128), nullable=True),
        sa.Column("readiness_evidence_sha256", sa.String(64), nullable=False),
        sa.Column("status_json", postgresql.JSONB(), nullable=False),
        sa.Column("status_sha256", sa.String(64), nullable=False),
        sa.Column("last_poll_requested_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("last_poll_nonce", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("first_seen_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("last_seen_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("updated_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.CheckConstraint(
            _AGENT_IDENTITY_CHECK,
            name="personal_dev_native_builder_agents_identity_check",
        ),
        sa.CheckConstraint(
            _AGENT_INVENTORY_CHECK,
            name="personal_dev_native_builder_agents_inventory_check",
        ),
        sa.CheckConstraint(
            _AGENT_STATUS_CHECK,
            name="personal_dev_native_builder_agents_status_check",
        ),
        sa.UniqueConstraint(
            "key_id",
            name="personal_dev_native_builder_agents_key_uidx",
        ),
    )
    op.create_index(
        "personal_dev_native_builder_agents_freshness_idx",
        "personal_dev_native_builder_agents",
        ["available", "last_seen_at", "instance_id"],
    )

    op.create_table(
        "personal_dev_native_build_grants",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("candidate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("attempt_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("attempt_lease_epoch", sa.BigInteger(), nullable=False),
        sa.Column("platform", sa.String(32), nullable=False),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("required_agent_instance_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("required_agent_key_id", sa.String(64), nullable=False),
        sa.Column("agent_image", sa.Text(), nullable=False),
        sa.Column("builder_image", sa.Text(), nullable=False),
        sa.Column("runtime_profile_sha256", sa.String(64), nullable=False),
        sa.Column("contract_json", sa.Text(), nullable=False),
        sa.Column("contract_sha256", sa.String(64), nullable=False),
        sa.Column("source_bucket", sa.Text(), nullable=False),
        sa.Column("source_object_key", sa.Text(), nullable=False),
        sa.Column("artifact_bucket", sa.Text(), nullable=False),
        sa.Column("artifact_object_key", sa.Text(), nullable=False),
        sa.Column("artifact_max_bytes", sa.BigInteger(), nullable=False),
        sa.Column("active_deadline_seconds", sa.Integer(), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("running_agent_instance_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("last_request_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("last_request_nonce", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("failure_reason", sa.String(128), nullable=True),
        sa.Column("completion_json", postgresql.JSONB(), nullable=True),
        sa.Column("completion_sha256", sa.String(64), nullable=True),
        sa.Column("runtime_evidence_json", postgresql.JSONB(), nullable=True),
        sa.Column("runtime_evidence_sha256", sa.String(64), nullable=True),
        sa.Column("artifact_head_json", postgresql.JSONB(), nullable=True),
        sa.Column("artifact_head_sha256", sa.String(64), nullable=True),
        sa.Column("queued_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("started_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("heartbeat_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("finished_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("updated_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["candidate_id"],
            ["personal_dev_candidates.id"],
            name="personal_dev_native_build_grants_candidate_fkey",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["attempt_id"],
            ["personal_dev_candidate_build_attempts.id"],
            name="personal_dev_native_build_grants_attempt_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["required_agent_instance_id"],
            ["personal_dev_native_builder_agents.instance_id"],
            name="personal_dev_native_build_grants_required_agent_fkey",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["running_agent_instance_id"],
            ["personal_dev_native_builder_agents.instance_id"],
            name="personal_dev_native_build_grants_running_agent_fkey",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "attempt_id",
            "attempt_lease_epoch",
            "platform",
            name="personal_dev_native_build_grants_attempt_platform_uidx",
        ),
        sa.CheckConstraint(
            _GRANT_IDENTITY_CHECK,
            name="personal_dev_native_build_grants_identity_check",
        ),
        sa.CheckConstraint(
            _GRANT_OBJECT_BINDING_CHECK,
            name="personal_dev_native_build_grants_object_binding_check",
        ),
        sa.CheckConstraint(
            _GRANT_STATE_CHECK,
            name="personal_dev_native_build_grants_state_check",
        ),
        sa.CheckConstraint(
            _GRANT_TERMINAL_CHECK,
            name="personal_dev_native_build_grants_terminal_check",
        ),
    )
    op.create_index(
        "personal_dev_native_build_grants_picker_idx",
        "personal_dev_native_build_grants",
        ["state", "queued_at", "id"],
    )
    op.create_index(
        "personal_dev_native_build_grants_agent_state_idx",
        "personal_dev_native_build_grants",
        ["required_agent_instance_id", "state", "updated_at", "id"],
    )
    op.create_index(
        "personal_dev_native_build_grants_attempt_idx",
        "personal_dev_native_build_grants",
        ["attempt_id", "attempt_lease_epoch", "id"],
    )


def downgrade() -> None:
    op.drop_table("personal_dev_native_build_grants")
    op.drop_table("personal_dev_native_builder_agents")
