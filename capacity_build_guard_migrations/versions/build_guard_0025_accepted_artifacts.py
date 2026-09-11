"""Read accepted native archives without reopening finished admission requests.

Revision ID: build_guard_0025
Revises: build_guard_0024
"""

import sqlalchemy as sa
from alembic import op

revision = "build_guard_0025"
down_revision = "build_guard_0024"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
HELPER = "assert_live_source(uuid,uuid,jsonb,bytea,text)"
FUNCTION = "read_accepted_artifact(uuid,uuid,jsonb,bytea,text)"
FINISHED = f" OR {SCHEMA}.native_request_finished(p_request)"


def _definition(signature):
    return op.get_bind().scalar(sa.text("SELECT pg_get_functiondef(CAST(:signature AS regprocedure))"),
        {"signature": f"{SCHEMA}.{signature}"})


def upgrade():
    # Preserve every locked source/lease check in a shared private helper. Only
    # new admission rejects finished requests; exporting their result needs the
    # same live parent/source authority without authorizing another execution.
    original = _definition("assert_current_source(uuid,uuid,jsonb,bytea,text)")
    name = f"FUNCTION {SCHEMA}.assert_current_source("
    if original.count(name) != 1 or original.count(FINISHED) != 1:
        raise RuntimeError("accepted artifact source helper definition changed")
    op.execute(original.replace(name, f"FUNCTION {SCHEMA}.assert_live_source(").replace(FINISHED, ""))
    op.execute(f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.assert_current_source(
            p_installation uuid,p_request uuid,p_source jsonb,p_wire bytea,p_digest text)
        RETURNS timestamptz LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
        DECLARE deadline timestamptz;
        BEGIN
            deadline := {SCHEMA}.assert_live_source(p_installation,p_request,p_source,p_wire,p_digest);
            IF {SCHEMA}.native_request_finished(p_request) THEN
                RAISE EXCEPTION 'build source request installation or live lease changed'; END IF;
            RETURN deadline;
        END $$;

        CREATE FUNCTION {SCHEMA}.read_accepted_artifact(
            p_installation uuid,p_request uuid,p_source jsonb,p_wire bytea,p_digest text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE outcome {SCHEMA}.platform_outcomes%ROWTYPE; claim {SCHEMA}.platform_claims%ROWTYPE;
            registration {SCHEMA}.worker_registrations%ROWTYPE; assignment {SCHEMA}.assignments%ROWTYPE;
        BEGIN
            PERFORM {SCHEMA}.assert_live_source(p_installation,p_request,p_source,p_wire,p_digest);
            IF (SELECT count(*) FROM {SCHEMA}.platform_outcomes o
                JOIN {SCHEMA}.platform_claims c ON c.id=o.claim_id
                WHERE c.request_id=p_request AND c.installation_id=p_installation
                    AND o.payload->>'result'='artifact-ready') <> 1 THEN
                RAISE EXCEPTION 'accepted native artifact is absent or ambiguous'; END IF;
            SELECT o.* INTO outcome FROM {SCHEMA}.platform_outcomes o
                JOIN {SCHEMA}.platform_claims c ON c.id=o.claim_id
                WHERE c.request_id=p_request AND c.installation_id=p_installation
                    AND o.payload->>'result'='artifact-ready';
            SELECT * INTO claim FROM {SCHEMA}.platform_claims WHERE id=outcome.claim_id;
            SELECT * INTO registration FROM {SCHEMA}.worker_registrations WHERE id=claim.registration_id;
            SELECT * INTO assignment FROM {SCHEMA}.assignments WHERE id=claim.assignment_id;
            IF outcome.retention_xid=pg_current_xact_id() OR claim.retention_xid=pg_current_xact_id()
                OR registration.retention_xid=pg_current_xact_id() THEN
                RAISE EXCEPTION 'accepted native artifact requires committed outcome history'; END IF;
            IF outcome.installation_id IS DISTINCT FROM p_installation
                OR registration.installation_id IS DISTINCT FROM p_installation
                OR assignment.request_id IS DISTINCT FROM p_request
                OR registration.assignment_id IS DISTINCT FROM assignment.id
                OR assignment.payload->>'source_canonical_json' IS DISTINCT FROM convert_from(p_wire,'UTF8')
                OR assignment.payload->>'source_binding_sha256' IS DISTINCT FROM p_digest
                OR claim.payload->>'request_id' IS DISTINCT FROM p_request::text
                OR claim.payload->>'operation_id' IS DISTINCT FROM claim.id::text
                OR claim.payload->>'worker_id' IS DISTINCT FROM registration.worker_id::text
                OR claim.payload->>'worker_incarnation' IS DISTINCT FROM registration.worker_incarnation::text
                OR claim.payload->'binding' IS DISTINCT FROM registration.payload->'binding'
                OR outcome.payload->'claim' IS DISTINCT FROM claim.payload
                OR outcome.wire_payload IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(outcome.payload),'UTF8')
                OR outcome.payload_sha256 IS DISTINCT FROM encode(sha256(outcome.wire_payload),'hex')
                OR claim.wire_payload IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(claim.payload),'UTF8')
                OR claim.payload_sha256 IS DISTINCT FROM encode(sha256(claim.wire_payload),'hex') THEN
                RAISE EXCEPTION 'accepted native artifact binding changed'; END IF;
            RETURN {SCHEMA}.native_outcome_receipt(outcome);
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{HELPER} FROM PUBLIC, {quote(agent)}")
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quote(agent)}")


def downgrade():
    definition = _definition(HELPER)
    name = f"FUNCTION {SCHEMA}.assert_live_source("
    clause = "OR request.cancelled_at IS NOT NULL"
    if definition.count(name) != 1 or definition.count(clause) != 1:
        raise RuntimeError("accepted artifact source helper definition changed")
    op.execute(definition.replace(name, f"FUNCTION {SCHEMA}.assert_current_source(").replace(clause, clause + FINISHED))
    op.execute(f"DROP FUNCTION {SCHEMA}.{FUNCTION}")
    op.execute(f"DROP FUNCTION {SCHEMA}.{HELPER}")
