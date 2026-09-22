"""Retain private installation, plan and assignment evidence without admission.

Revision ID: build_guard_0001
Revises: None

No runtime mutation procedures are installed by this revision. Agent access is
limited to schema usage until protected prepare/publication/release is complete.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql as pg

revision = "build_guard_0001"
down_revision = None
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"


def _payload():
    return (
        sa.Column("payload", pg.JSONB(), nullable=False),
        sa.Column("wire_payload", sa.LargeBinary(), nullable=False),
        sa.Column("payload_sha256", sa.Text(), nullable=False),
        sa.CheckConstraint("octet_length(wire_payload) BETWEEN 2 AND 1048576 "
            "AND jsonb_typeof(payload) = 'object' "
            "AND convert_from(wire_payload, 'UTF8')::jsonb = payload "
            "AND encode(sha256(wire_payload), 'hex') = payload_sha256"),
    )


def upgrade():
    op.create_table("installations",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("owner_user_id", sa.Uuid(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("subject_incarnation", sa.Uuid(), nullable=False),
        sa.Column("deployment_generation", sa.BigInteger(), nullable=False),
        sa.Column("reporter_incarnation", sa.Uuid(), nullable=False),
        sa.CheckConstraint("deployment_generation > 0"),
        sa.UniqueConstraint("subject_id", "subject_incarnation", "deployment_generation"),
        *_payload(), schema=SCHEMA)
    op.create_table("plans",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("installation_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.installations.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("expires_at", pg.TIMESTAMP(timezone=True), nullable=False),
        *_payload(), schema=SCHEMA)
    op.create_table("assignments",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("plan_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.plans.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("request_id", sa.Uuid(), sa.ForeignKey("public.personal_dev_build_platform_requests.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("submission_intent_id", sa.Uuid(), nullable=False),
        sa.Column("shape_instance_id", sa.Text(), nullable=False),
        sa.Column("shape_slot_index", sa.Integer(), nullable=False),
        sa.CheckConstraint("shape_slot_index = 0"),
        sa.UniqueConstraint("plan_id", "request_id"),
        sa.UniqueConstraint("request_id", "id"),
        sa.UniqueConstraint("submission_intent_id", "shape_slot_index"),
        *_payload(), schema=SCHEMA)
    # Current ownership is separate from immutable history. Only the eventual
    # protected release procedure may remove a hold; logical cancellation cannot.
    op.create_table("request_holds",
        sa.Column("request_id", sa.Uuid(), primary_key=True),
        sa.Column("assignment_id", sa.Uuid(), nullable=False, unique=True),
        sa.ForeignKeyConstraint(["request_id", "assignment_id"],
            [f"{SCHEMA}.assignments.request_id", f"{SCHEMA}.assignments.id"], ondelete="RESTRICT"),
        schema=SCHEMA)
    op.create_table("dispositions",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("plan_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.plans.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.CheckConstraint("kind IN ('publication', 'closure', 'release')"),
        *_payload(), schema=SCHEMA)
    op.execute(f"""CREATE FUNCTION {SCHEMA}.reject_evidence_mutation() RETURNS trigger
        LANGUAGE plpgsql SET search_path=pg_catalog AS $$
        BEGIN RAISE EXCEPTION 'build guard evidence is append-only'; END $$""")
    for table in ("installations", "plans", "assignments", "dispositions"):
        op.execute(f"CREATE TRIGGER immutable BEFORE UPDATE OR DELETE ON {SCHEMA}.{table} "
            f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.reject_evidence_mutation()")
        op.execute(f"CREATE TRIGGER no_truncate BEFORE TRUNCATE ON {SCHEMA}.{table} "
            f"FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_evidence_mutation()")
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quoted = op.get_bind().dialect.identifier_preparer.quote(agent)
    for grantee in ("PUBLIC", quoted):
        op.execute(f"REVOKE ALL ON ALL TABLES IN SCHEMA {SCHEMA} FROM {grantee}")
        op.execute(f"REVOKE ALL ON ALL FUNCTIONS IN SCHEMA {SCHEMA} FROM {grantee}")
    op.execute("ALTER DEFAULT PRIVILEGES REVOKE ALL ON TABLES FROM PUBLIC")
    op.execute("ALTER DEFAULT PRIVILEGES REVOKE ALL ON FUNCTIONS FROM PUBLIC")
    op.execute(f"GRANT USAGE ON SCHEMA {SCHEMA} TO {quoted}")


def downgrade():
    tables = ("request_holds", "dispositions", "assignments", "plans", "installations")
    op.execute("LOCK TABLE " + ", ".join(f"{SCHEMA}.{table}" for table in tables) + " IN ACCESS EXCLUSIVE MODE")
    for table in tables:
        op.execute(f"""DO $$ BEGIN IF EXISTS (SELECT 1 FROM {SCHEMA}.{table}) THEN
            RAISE EXCEPTION 'cannot downgrade build guard with retained evidence'; END IF; END $$""")
    for table in tables:
        op.drop_table(table, schema=SCHEMA)
    op.execute(f"DROP FUNCTION {SCHEMA}.reject_evidence_mutation()")
