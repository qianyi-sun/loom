"""Authorize bounded native source IO independently of historical claim replay.

Revision ID: build_guard_0027
Revises: build_guard_0026
"""

from alembic import op

revision = "build_guard_0027"
down_revision = "build_guard_0026"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = "authorize_source(uuid,jsonb,bytea,text,text)"


def upgrade():
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.authorize_source(
            p_installation uuid,p jsonb,wire bytea,digest text,credential_hash text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE claim {SCHEMA}.platform_claims%ROWTYPE;
            registration {SCHEMA}.worker_registrations%ROWTYPE;
            assignment {SCHEMA}.assignments%ROWTYPE;
            source_wire bytea; source jsonb; deadline timestamptz;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'native source requires serializable transaction'; END IF;
            IF wire IS NULL OR octet_length(wire) NOT BETWEEN 2 AND 1048576
                OR wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR digest IS DISTINCT FROM encode(sha256(wire),'hex') THEN
                RAISE EXCEPTION 'native source claim canonical bytes changed'; END IF;
            PERFORM id FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'native source installation absent'; END IF;
            SELECT * INTO claim FROM {SCHEMA}.platform_claims WHERE id=(p->>'operation_id')::uuid;
            IF NOT FOUND OR claim.retention_xid=pg_current_xact_id()
                OR claim.installation_id IS DISTINCT FROM p_installation
                OR claim.payload IS DISTINCT FROM p OR claim.wire_payload IS DISTINCT FROM wire
                OR claim.payload_sha256 IS DISTINCT FROM digest THEN
                RAISE EXCEPTION 'native source requires exact committed claim'; END IF;
            SELECT * INTO registration FROM {SCHEMA}.worker_registrations WHERE id=claim.registration_id FOR UPDATE;
            IF NOT FOUND OR registration.retention_xid=pg_current_xact_id()
                OR registration.installation_id IS DISTINCT FROM p_installation
                OR registration.payload->'binding' IS DISTINCT FROM p->'binding'
                OR registration.worker_id::text IS DISTINCT FROM p->>'worker_id'
                OR registration.worker_incarnation::text IS DISTINCT FROM p->>'worker_incarnation'
                OR credential_hash IS NULL OR credential_hash IS DISTINCT FROM registration.credential_sha256 THEN
                RAISE EXCEPTION 'native source worker credential changed'; END IF;
            SELECT * INTO assignment FROM {SCHEMA}.assignments WHERE id=claim.assignment_id;
            IF NOT FOUND OR assignment.id IS DISTINCT FROM registration.assignment_id
                OR assignment.request_id IS DISTINCT FROM claim.request_id
                OR assignment.request_id::text IS DISTINCT FROM p->>'request_id'
                OR assignment.submission_intent_id IS DISTINCT FROM registration.intent_id
                OR assignment.wire_payload IS NULL
                OR assignment.payload IS DISTINCT FROM convert_from(assignment.wire_payload,'UTF8')::jsonb
                OR assignment.payload_sha256 IS DISTINCT FROM encode(sha256(assignment.wire_payload),'hex') THEN
                RAISE EXCEPTION 'native source assignment changed'; END IF;
            PERFORM {SCHEMA}.assert_bootstrap_not_revoked(registration.intent_id);
            IF EXISTS (SELECT 1 FROM {SCHEMA}.worker_drains WHERE registration_id=registration.id)
                OR EXISTS (SELECT 1 FROM {SCHEMA}.platform_outcomes WHERE claim_id=claim.id)
                OR EXISTS (SELECT 1 FROM {SCHEMA}.worker_releases WHERE registration_id=registration.id)
                OR EXISTS (SELECT 1 FROM {SCHEMA}.terminal_inventory WHERE intent_id=registration.intent_id)
                OR EXISTS (SELECT 1 FROM {SCHEMA}.dispositions WHERE plan_id=assignment.plan_id AND kind<>'publication')
                OR NOT EXISTS (SELECT 1 FROM {SCHEMA}.request_holds WHERE assignment_id=assignment.id AND request_id=claim.request_id) THEN
                RAISE EXCEPTION 'native source execution is closed'; END IF;
            source_wire := convert_to(assignment.payload->>'source_canonical_json','UTF8');
            source := convert_from(source_wire,'UTF8')::jsonb;
            -- Running work follows the current whole-attempt heartbeat, not the
            -- expired initial plan/bootstrap admission window. This grants no
            -- new claim and does not extend either lease.
            deadline := {SCHEMA}.assert_current_source(p_installation,claim.request_id,
                source,source_wire,assignment.payload->>'source_binding_sha256');
            RETURN {SCHEMA}.canonical_plan_json(jsonb_build_object('schema_version',1,
                'claim',p,'claim_digest',digest,'object_bucket',source->'object_bucket',
                'object_key',source->'object_key','archive_sha256',source->'archive_sha256',
                'archive_size_bytes',source->'archive_size_bytes',
                'source_binding_sha256',assignment.payload->'source_binding_sha256',
                'lease_not_after',to_char(deadline AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS')
                    || CASE WHEN to_char(deadline,'US')='000000' THEN '' ELSE '.' || to_char(deadline,'US') END || 'Z'));
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quote(agent)}")


def downgrade():
    op.execute(f"DROP FUNCTION {SCHEMA}.{FUNCTION}")
