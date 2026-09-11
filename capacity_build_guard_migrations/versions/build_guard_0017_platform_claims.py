"""Claim allocated native platform work without granting source access.

Revision ID: build_guard_0017
Revises: build_guard_0016
"""

import sqlalchemy as sa
from alembic import op

from capacity_build_guard_migrations.versions.build_guard_0001_assignments import _payload
from capacity_build_guard_migrations.versions.build_guard_0011_revocation import _replace_clause

revision = "build_guard_0017"
down_revision = "build_guard_0016"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = "claim_platform(uuid,jsonb,bytea,text,text)"


def _consumers(*, install):
    for signature, old, new in (
        ("observe_registered_worker(uuid,jsonb)", "'claim_high_water',0,",
            f"'claim_high_water',{SCHEMA}.native_claim_high_water(p_intent),"),
        ("capture_demand(uuid,bigint,jsonb)", "'fixed_claims','[]'::jsonb",
            f"'fixed_claims',{SCHEMA}.fixed_native_claims(p_installation)"),
    ):
        _replace_clause(signature, old if install else new, new if install else old)


def upgrade():
    op.create_table("platform_claims",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("installation_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.installations.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("registration_id", sa.BigInteger(), sa.ForeignKey(f"{SCHEMA}.worker_registrations.id", ondelete="RESTRICT"), nullable=False, unique=True),
        sa.Column("assignment_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.assignments.id", ondelete="RESTRICT"), nullable=False, unique=True),
        sa.Column("request_id", sa.Uuid(), sa.ForeignKey("public.personal_dev_build_platform_requests.id", ondelete="RESTRICT"), nullable=False),
        *_payload(), schema=SCHEMA)
    op.execute(f"ALTER TABLE {SCHEMA}.platform_claims ADD COLUMN retention_xid xid8 NOT NULL DEFAULT pg_current_xact_id()")
    op.execute(f"CREATE TRIGGER build_claim_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON {SCHEMA}.platform_claims FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_evidence_mutation()")
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.native_claim_receipt(e {SCHEMA}.platform_claims)
        RETURNS text LANGUAGE sql SECURITY INVOKER SET search_path=pg_catalog AS $$
            SELECT {SCHEMA}.canonical_plan_json(jsonb_build_object('schema_version',1,
                'request',e.payload,'request_digest',e.payload_sha256,'claim_high_water',1))
        $$;

        CREATE FUNCTION {SCHEMA}.native_claim_high_water(p_intent uuid)
        RETURNS bigint LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
        DECLARE e {SCHEMA}.platform_claims%ROWTYPE;
        BEGIN
            SELECT c.* INTO e FROM {SCHEMA}.platform_claims c
                JOIN {SCHEMA}.worker_registrations r ON r.id=c.registration_id WHERE r.intent_id=p_intent;
            IF NOT FOUND THEN RETURN 0; END IF;
            IF e.retention_xid=pg_current_xact_id() THEN
                RAISE EXCEPTION 'native observation requires committed claim'; END IF;
            RETURN 1;
        END $$;

        CREATE FUNCTION {SCHEMA}.fixed_native_claims(p_installation uuid)
        RETURNS jsonb LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
        DECLARE e record; binding jsonb; claims jsonb := '[]'::jsonb; state text;
        BEGIN
            FOR e IN SELECT c.*, r.worker_incarnation, r.intent_id, r.payload AS registration,
                    q.cancelled_at, a.state AS attempt_state, a.lease_expires_at,
                    EXISTS (SELECT 1 FROM {SCHEMA}.terminal_inventory t WHERE t.intent_id=r.intent_id) AS terminal
                FROM {SCHEMA}.platform_claims c
                JOIN {SCHEMA}.worker_registrations r ON r.id=c.registration_id
                JOIN {SCHEMA}.request_holds h ON h.request_id=c.request_id AND h.assignment_id=c.assignment_id
                JOIN public.personal_dev_build_platform_requests q ON q.id=c.request_id
                JOIN public.personal_dev_candidate_build_attempts a ON a.id=q.attempt_id
                WHERE c.installation_id=p_installation ORDER BY c.id LIMIT 10001 LOOP
                IF e.retention_xid=pg_current_xact_id()
                    OR e.payload->'binding' IS DISTINCT FROM e.registration->'binding'
                    OR e.payload->>'request_id' IS DISTINCT FROM e.request_id::text
                    OR e.payload->>'worker_incarnation' IS DISTINCT FROM e.worker_incarnation::text THEN
                    RAISE EXCEPTION 'native demand requires exact committed claim'; END IF;
                binding := e.payload->'binding';
                state := CASE WHEN e.cancelled_at IS NOT NULL THEN 'cancel-pending'
                    WHEN e.terminal OR e.attempt_state <> 'running' OR e.lease_expires_at IS NULL
                        OR e.lease_expires_at <= clock_timestamp() THEN 'unknown' ELSE 'live' END;
                claims := claims || jsonb_build_array(jsonb_build_object('schema_version',1,
                    'claim_id',e.id,'attempt_id',e.request_id,'worker_identity',e.worker_incarnation,
                    'pool_id',binding->'pool_id','pool_generation',binding->'pool_generation',
                    'profile_id',binding->'profile_id','profile_generation',binding->'profile_generation',
                    'profile_digest',binding->'profile_digest','shape_id',binding->'shape_id',
                    'deployment_generation',binding->'deployment_generation',
                    'concurrency_slots',binding->'concurrency_slots','resources',binding->'resources','state',state));
            END LOOP;
            IF jsonb_array_length(claims)>10000 THEN RAISE EXCEPTION 'native fixed claims exceed bound'; END IF;
            RETURN claims;
        END $$;

        CREATE FUNCTION {SCHEMA}.claim_platform(p_installation uuid,p jsonb,wire bytea,digest text,credential_hash text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE installation {SCHEMA}.installations%ROWTYPE; bootstrap {SCHEMA}.bootstraps%ROWTYPE;
            physical {SCHEMA}.execution_events%ROWTYPE; registration {SCHEMA}.worker_registrations%ROWTYPE;
            assignment {SCHEMA}.assignments%ROWTYPE; e {SCHEMA}.platform_claims%ROWTYPE; field text;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'native claim requires serializable transaction'; END IF;
            IF wire IS NULL OR octet_length(wire) NOT BETWEEN 2 AND 1048576
                OR wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR digest IS DISTINCT FROM encode(sha256(wire),'hex') THEN
                RAISE EXCEPTION 'native claim canonical request changed'; END IF;
            IF jsonb_typeof(p) IS DISTINCT FROM 'object' OR p->'schema_version' IS DISTINCT FROM '1'::jsonb
                OR NOT p ?& ARRAY['schema_version','binding','operation_id','request_id','worker_id','worker_incarnation']
                OR p - ARRAY['schema_version','binding','operation_id','request_id','worker_id','worker_incarnation'] <> '{{}}'::jsonb THEN
                RAISE EXCEPTION 'native claim schema changed'; END IF;
            FOREACH field IN ARRAY ARRAY['operation_id','request_id','worker_id','worker_incarnation'] LOOP
                IF jsonb_typeof(p->field) IS DISTINCT FROM 'string'
                    OR (p->>field) !~ '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$' THEN
                    RAISE EXCEPTION 'native claim UUID field is invalid'; END IF;
            END LOOP;
            SELECT * INTO installation FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'native claim installation is absent'; END IF;
            SELECT * INTO bootstrap FROM {SCHEMA}.bootstraps WHERE intent_id=(p->'binding'->>'intent_id')::uuid FOR UPDATE;
            IF NOT FOUND OR bootstrap.installation_id IS DISTINCT FROM p_installation
                OR bootstrap.payload->'proposal'->'binding' IS DISTINCT FROM p->'binding' THEN
                RAISE EXCEPTION 'native claim bootstrap binding changed'; END IF;
            SELECT * INTO physical FROM {SCHEMA}.execution_events WHERE intent_id=bootstrap.intent_id AND kind='bound' FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'native claim physical binding absent'; END IF;
            SELECT * INTO registration FROM {SCHEMA}.worker_registrations WHERE intent_id=bootstrap.intent_id FOR UPDATE;
            IF NOT FOUND OR registration.retention_xid=pg_current_xact_id()
                OR registration.installation_id IS DISTINCT FROM p_installation
                OR registration.physical_event_id IS DISTINCT FROM physical.id
                OR registration.payload->'binding' IS DISTINCT FROM p->'binding'
                OR registration.worker_id::text IS DISTINCT FROM p->>'worker_id'
                OR registration.worker_incarnation::text IS DISTINCT FROM p->>'worker_incarnation'
                OR credential_hash IS NULL OR registration.credential_sha256 IS DISTINCT FROM credential_hash THEN
                RAISE EXCEPTION 'native claim requires exact committed worker credential'; END IF;
            SELECT * INTO assignment FROM {SCHEMA}.assignments WHERE id=registration.assignment_id;
            IF NOT FOUND OR assignment.request_id::text IS DISTINCT FROM p->>'request_id'
                OR assignment.submission_intent_id IS DISTINCT FROM bootstrap.intent_id THEN
                RAISE EXCEPTION 'native claim assigned request changed'; END IF;
            SELECT * INTO e FROM {SCHEMA}.platform_claims WHERE registration_id=registration.id;
            IF FOUND THEN
                IF e.payload IS DISTINCT FROM p OR e.wire_payload IS DISTINCT FROM wire OR e.payload_sha256 IS DISTINCT FROM digest THEN
                    RAISE EXCEPTION 'native claim exact replay changed'; END IF;
                RETURN {SCHEMA}.native_claim_receipt(e);
            END IF;
            PERFORM {SCHEMA}.assert_bootstrap_not_revoked(bootstrap.intent_id);
            IF EXISTS (SELECT 1 FROM {SCHEMA}.terminal_inventory WHERE intent_id=bootstrap.intent_id) THEN
                RAISE EXCEPTION 'native claim physical job is already terminal'; END IF;
            PERFORM {SCHEMA}.authorize_publication(p_installation,assignment.plan_id);
            INSERT INTO {SCHEMA}.platform_claims(id,installation_id,registration_id,assignment_id,request_id,payload,wire_payload,payload_sha256)
                VALUES((p->>'operation_id')::uuid,p_installation,registration.id,assignment.id,assignment.request_id,p,wire,digest)
                RETURNING * INTO e;
            -- A source lock or unique-index wait cannot extend the source/plan lease.
            PERFORM {SCHEMA}.authorize_publication(p_installation,assignment.plan_id);
            RETURN {SCHEMA}.native_claim_receipt(e);
        END $function$;
    """)
    _consumers(install=True)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    for signature in (f"native_claim_receipt({SCHEMA}.platform_claims)", "native_claim_high_water(uuid)", "fixed_native_claims(uuid)"):
        op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{signature} FROM PUBLIC, {quote(agent)}")
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quote(agent)}")


def downgrade():
    op.execute(f"LOCK TABLE {SCHEMA}.platform_claims IN ACCESS EXCLUSIVE MODE")
    op.execute(f"""DO $$ BEGIN IF EXISTS (SELECT 1 FROM {SCHEMA}.platform_claims) THEN
        RAISE EXCEPTION 'cannot remove native claims with retained evidence'; END IF; END $$""")
    _consumers(install=False)
    for signature in (FUNCTION, f"native_claim_receipt({SCHEMA}.platform_claims)", "native_claim_high_water(uuid)", "fixed_native_claims(uuid)"):
        op.execute(f"DROP FUNCTION {SCHEMA}.{signature}")
    op.drop_table("platform_claims", schema=SCHEMA)
