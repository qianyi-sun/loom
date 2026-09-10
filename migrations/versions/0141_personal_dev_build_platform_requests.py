"""Retain owner/installation/lease-bound native build demand, without admission.

Revision ID: 0141
Revises: 0140
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0141"
down_revision = "0140"
branch_labels = None
depends_on = None
_TABLE = "personal_dev_build_platform_requests"


def upgrade() -> None:
    op.create_table(_TABLE,
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("owner_user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("candidate_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("personal_dev_candidates.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("attempt_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("personal_dev_candidate_build_attempts.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("attempt_lease_epoch", sa.BigInteger(), nullable=False),
        sa.Column("platform", sa.Text(), nullable=False),
        sa.Column("subject_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("subject_incarnation", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("deployment_generation", sa.BigInteger(), nullable=False),
        sa.Column("bucket_id", sa.Text(), nullable=False),
        sa.Column("source_binding_sha256", sa.Text(), nullable=False),
        sa.Column("runtime_installation_sha256", sa.Text(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("cancelled_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.CheckConstraint("platform IN ('linux/amd64', 'linux/arm64') AND attempt_lease_epoch > 0 AND deployment_generation > 0", name="personal_build_request_identity_check"),
        sa.CheckConstraint("bucket_id ~ '^build-[0-9a-f]{64}$' AND source_binding_sha256 ~ '^[0-9a-f]{64}$' AND source_binding_sha256 <> repeat('0',64) "
            "AND runtime_installation_sha256 ~ '^[0-9a-f]{64}$' AND runtime_installation_sha256 <> repeat('0',64)", name="personal_build_request_digest_check"),
        sa.CheckConstraint("cancelled_at IS NULL OR cancelled_at >= created_at", name="personal_build_request_time_check"),
        sa.UniqueConstraint("attempt_id", "attempt_lease_epoch", "platform", name="personal_build_request_attempt_uidx"),
    )
    op.create_index("personal_build_request_owner_pending_idx", _TABLE,
        ["owner_user_id", "subject_id", "subject_incarnation", "cancelled_at"])
    op.execute("""
        CREATE FUNCTION public.guard_personal_build_platform_request() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $function$
        BEGIN
          IF TG_OP <> 'UPDATE' THEN
            RAISE EXCEPTION 'personal build platform requests cannot be removed';
          END IF;
          IF (to_jsonb(NEW) - 'cancelled_at') IS DISTINCT FROM (to_jsonb(OLD) - 'cancelled_at')
             OR OLD.cancelled_at IS NOT NULL OR NEW.cancelled_at IS NULL THEN
            RAISE EXCEPTION 'personal build platform request identity is immutable';
          END IF;
          RETURN NEW;
        END $function$;
        CREATE TRIGGER personal_build_platform_request_immutable
          BEFORE UPDATE OR DELETE ON public.personal_dev_build_platform_requests
          FOR EACH ROW EXECUTE FUNCTION public.guard_personal_build_platform_request();
        CREATE TRIGGER personal_build_platform_request_no_truncate
          BEFORE TRUNCATE ON public.personal_dev_build_platform_requests
          FOR EACH STATEMENT EXECUTE FUNCTION public.guard_personal_build_platform_request();
    """)


def downgrade() -> None:
    op.execute("LOCK TABLE public.personal_dev_build_platform_requests IN ACCESS EXCLUSIVE MODE")
    op.execute("""DO $block$ BEGIN
        IF EXISTS (SELECT 1 FROM public.personal_dev_build_platform_requests) THEN
          RAISE EXCEPTION 'cannot downgrade 0141 with retained personal build platform requests';
        END IF;
        END $block$""")
    op.drop_table(_TABLE)
    op.execute("DROP FUNCTION public.guard_personal_build_platform_request()")
