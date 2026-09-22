"""Observe current platform results without confusing failure with pending work.

Revision ID: build_guard_0030
Revises: build_guard_0029
"""

from alembic import op

revision = "build_guard_0030"
down_revision = "build_guard_0029"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = "read_platform_outcome(uuid,uuid,jsonb,bytea,text)"


def upgrade():
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.read_platform_outcome(
            p_installation uuid,p_request uuid,p_source jsonb,p_wire bytea,p_digest text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE assignment {SCHEMA}.assignments%ROWTYPE; claim {SCHEMA}.platform_claims%ROWTYPE;
            registration {SCHEMA}.worker_registrations%ROWTYPE;
        BEGIN
            -- Authentication/fence errors are never a pending observation.
            PERFORM {SCHEMA}.assert_live_source(p_installation,p_request,p_source,p_wire,p_digest);
            SELECT a.* INTO assignment FROM {SCHEMA}.assignments a
                JOIN {SCHEMA}.plans p ON p.id=a.plan_id
                WHERE a.request_id=p_request AND p.installation_id=p_installation
                ORDER BY (a.payload->>'request_sequence')::bigint DESC LIMIT 1;
            IF NOT FOUND THEN RETURN NULL; END IF;
            IF EXISTS (SELECT 1 FROM {SCHEMA}.plans WHERE id=assignment.plan_id
                AND preparation_xid=pg_current_xact_id()) THEN
                RAISE EXCEPTION 'native platform observation requires committed preparation'; END IF;
            IF assignment.payload->>'source_canonical_json' IS DISTINCT FROM convert_from(p_wire,'UTF8')
                OR assignment.payload->>'source_binding_sha256' IS DISTINCT FROM p_digest THEN
                RAISE EXCEPTION 'native platform observation source binding changed'; END IF;
            SELECT * INTO claim FROM {SCHEMA}.platform_claims WHERE assignment_id=assignment.id;
            IF NOT FOUND THEN RETURN NULL; END IF;
            SELECT * INTO registration FROM {SCHEMA}.worker_registrations WHERE id=claim.registration_id;
            IF NOT FOUND OR registration.retention_xid=pg_current_xact_id()
                OR registration.installation_id IS DISTINCT FROM p_installation
                OR registration.assignment_id IS DISTINCT FROM assignment.id
                OR registration.intent_id IS DISTINCT FROM assignment.submission_intent_id
                OR claim.installation_id IS DISTINCT FROM p_installation
                OR claim.request_id IS DISTINCT FROM p_request
                OR claim.payload->>'request_id' IS DISTINCT FROM p_request::text
                OR claim.payload->>'operation_id' IS DISTINCT FROM claim.id::text
                OR claim.payload->>'worker_id' IS DISTINCT FROM registration.worker_id::text
                OR claim.payload->>'worker_incarnation' IS DISTINCT FROM registration.worker_incarnation::text
                OR claim.payload->'binding' IS DISTINCT FROM registration.payload->'binding' THEN
                RAISE EXCEPTION 'native platform observation requires exact committed worker'; END IF;
            -- Reuse the existing exact-claim/canonical/committed-outcome reader.
            -- A newer unclaimed assignment must not return an older failed result.
            RETURN {SCHEMA}.read_outcome(p_installation,claim.payload,claim.wire_payload,claim.payload_sha256);
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quote(agent)}")


def downgrade():
    op.execute(f"DROP FUNCTION {SCHEMA}.{FUNCTION}")
