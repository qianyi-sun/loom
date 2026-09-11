"""Verify service scope against committed installer-owned runtime evidence.

Revision ID: build_guard_0026
Revises: build_guard_0025
"""

from alembic import op

revision = "build_guard_0026"
down_revision = "build_guard_0025"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = "assert_management_installation(uuid,bytea)"


def upgrade():
    op.execute(f"ALTER TABLE {SCHEMA}.installations ADD COLUMN retention_xid xid8 NOT NULL DEFAULT pg_current_xact_id()")
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.assert_management_installation(p_installation uuid,p_wire bytea)
        RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        DECLARE installation {SCHEMA}.installations%ROWTYPE;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build management installation requires serializable transaction'; END IF;
            SELECT * INTO installation FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build management installation absent'; END IF;
            IF installation.retention_xid=pg_current_xact_id() THEN
                RAISE EXCEPTION 'build management installation requires committed retention'; END IF;
            IF p_wire IS NULL OR octet_length(p_wire) NOT BETWEEN 2 AND 1048576
                OR p_wire IS DISTINCT FROM installation.wire_payload
                OR encode(sha256(p_wire),'hex') IS DISTINCT FROM installation.payload_sha256
                OR p_wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(installation.payload),'UTF8')
                OR installation.payload->>'id' IS DISTINCT FROM installation.id::text
                OR installation.payload->>'owner_user_id' IS DISTINCT FROM installation.owner_user_id::text
                OR installation.payload->>'subject_id' IS DISTINCT FROM installation.subject_id::text
                OR installation.payload->>'subject_incarnation' IS DISTINCT FROM installation.subject_incarnation::text
                OR installation.payload->>'deployment_generation' IS DISTINCT FROM installation.deployment_generation::text
                OR installation.payload->>'reporter_incarnation' IS DISTINCT FROM installation.reporter_incarnation::text THEN
                RAISE EXCEPTION 'build management installation binding changed'; END IF;
            RETURN true;
        END $$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quote(agent)}")


def downgrade():
    op.execute(f"DROP FUNCTION {SCHEMA}.{FUNCTION}")
    op.execute(f"ALTER TABLE {SCHEMA}.installations DROP COLUMN retention_xid")
