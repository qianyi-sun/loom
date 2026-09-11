"""Resolve registered native worker allocation before delegating claim authority.

Revision ID: build_guard_0029
Revises: build_guard_0028
"""

from alembic import op

revision = "build_guard_0029"
down_revision = "build_guard_0028"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = "claim_assigned_platform(uuid,jsonb,bytea,text,text)"


def upgrade():
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.claim_assigned_platform(
            p_installation uuid,p jsonb,wire bytea,digest text,credential_hash text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE registration {SCHEMA}.worker_registrations%ROWTYPE;
            assignment {SCHEMA}.assignments%ROWTYPE; claim jsonb; claim_wire bytea;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'allocated native claim requires serializable transaction'; END IF;
            IF wire IS NULL OR octet_length(wire) NOT BETWEEN 2 AND 1048576
                OR wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR digest IS DISTINCT FROM encode(sha256(wire),'hex')
                OR jsonb_typeof(p) IS DISTINCT FROM 'object' OR p->'schema_version' IS DISTINCT FROM '1'::jsonb
                OR NOT p ?& ARRAY['schema_version','binding','operation_id','worker_id','worker_incarnation']
                OR p - ARRAY['schema_version','binding','operation_id','worker_id','worker_incarnation'] <> '{{}}'::jsonb THEN
                RAISE EXCEPTION 'allocated native claim request changed'; END IF;
            -- Read only immutable routing facts here. claim_platform acquires
            -- installation/bootstrap/physical/worker locks in its existing order
            -- and rechecks all fields before any claim mutation or replay.
            SELECT * INTO registration FROM {SCHEMA}.worker_registrations
                WHERE intent_id=(p->'binding'->>'intent_id')::uuid;
            IF NOT FOUND OR registration.retention_xid=pg_current_xact_id()
                OR registration.installation_id IS DISTINCT FROM p_installation
                OR registration.payload->'binding' IS DISTINCT FROM p->'binding'
                OR registration.worker_id::text IS DISTINCT FROM p->>'worker_id'
                OR registration.worker_incarnation::text IS DISTINCT FROM p->>'worker_incarnation'
                OR credential_hash IS NULL OR registration.credential_sha256 IS DISTINCT FROM credential_hash THEN
                RAISE EXCEPTION 'allocated native claim worker credential changed'; END IF;
            SELECT * INTO assignment FROM {SCHEMA}.assignments WHERE id=registration.assignment_id;
            IF NOT FOUND OR assignment.submission_intent_id IS DISTINCT FROM registration.intent_id THEN
                RAISE EXCEPTION 'allocated native claim assignment changed'; END IF;
            claim := p || jsonb_build_object('request_id',assignment.request_id);
            claim_wire := convert_to({SCHEMA}.canonical_plan_json(claim),'UTF8');
            RETURN {SCHEMA}.claim_platform(p_installation,claim,claim_wire,
                encode(sha256(claim_wire),'hex'),credential_hash);
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quote(agent)}")


def downgrade():
    op.execute(f"DROP FUNCTION {SCHEMA}.{FUNCTION}")
