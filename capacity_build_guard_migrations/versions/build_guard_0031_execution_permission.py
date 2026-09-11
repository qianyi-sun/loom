"""Issue short-lived native permission without creating another capacity grant.

Revision ID: build_guard_0031
Revises: build_guard_0030
"""

from alembic import op

revision = "build_guard_0031"
down_revision = "build_guard_0030"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = "authorize_execution(uuid,jsonb,bytea,text,text)"


def upgrade():
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.authorize_execution(
            p_installation uuid,p jsonb,wire bytea,digest text,credential_hash text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE access jsonb; claim_wire bytea; issued timestamptz; deadline timestamptz;
        BEGIN
            IF jsonb_typeof(p) IS DISTINCT FROM 'object'
                OR NOT p ?& ARRAY['schema_version','claim','challenge','source_binding_sha256']
                OR (SELECT count(*) FROM jsonb_object_keys(p)) <> 4
                OR p->'schema_version' IS DISTINCT FROM '1'::jsonb
                OR p->>'schema_version' IS DISTINCT FROM '1'
                OR jsonb_typeof(p->'claim') IS DISTINCT FROM 'object'
                OR jsonb_typeof(p->'challenge') IS DISTINCT FROM 'string'
                OR (p->>'challenge')::uuid::text IS DISTINCT FROM p->>'challenge'
                OR jsonb_typeof(p->'source_binding_sha256') IS DISTINCT FROM 'string'
                OR p->>'source_binding_sha256' !~ '^[0-9a-f]{{64}}$'
                OR wire IS NULL OR octet_length(wire) NOT BETWEEN 2 AND 1048576
                OR wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR digest IS DISTINCT FROM encode(sha256(wire),'hex') THEN
                RAISE EXCEPTION 'native execution request canonical binding changed'; END IF;
            claim_wire := convert_to({SCHEMA}.canonical_plan_json(p->'claim'),'UTF8');
            -- This is a fresh locked fence, never historical claim replay. It
            -- checks current installation, credential, assignment, live parent
            -- source/lease and every drain/outcome/terminal/release exclusion.
            access := {SCHEMA}.authorize_source(p_installation,p->'claim',claim_wire,
                encode(sha256(claim_wire),'hex'),credential_hash)::jsonb;
            IF access->>'source_binding_sha256' IS DISTINCT FROM p->>'source_binding_sha256' THEN
                RAISE EXCEPTION 'native execution source binding changed'; END IF;
            issued := clock_timestamp();
            deadline := (access->>'lease_not_after')::timestamptz;
            IF deadline IS NULL OR deadline <= issued THEN
                RAISE EXCEPTION 'native execution source lease expired'; END IF;
            deadline := LEAST(deadline,issued + interval '10 seconds');
            RETURN {SCHEMA}.canonical_plan_json(jsonb_build_object('schema_version',1,
                'request',p,'request_digest',digest,'executable',true,
                'issued_at',to_char(issued AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS')
                    || CASE WHEN to_char(issued,'US')='000000' THEN '' ELSE '.' || to_char(issued,'US') END || 'Z',
                'not_after',to_char(deadline AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS')
                    || CASE WHEN to_char(deadline,'US')='000000' THEN '' ELSE '.' || to_char(deadline,'US') END || 'Z'));
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quote(agent)}")


def downgrade():
    op.execute(f"DROP FUNCTION {SCHEMA}.{FUNCTION}")
