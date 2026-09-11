"""Retain native results without conferring publication or physical release.

Revision ID: build_guard_0019
Revises: build_guard_0018
"""

import sqlalchemy as sa
from alembic import op

from capacity_build_guard_migrations.versions.build_guard_0001_assignments import _payload
from capacity_build_guard_migrations.versions.build_guard_0011_revocation import _replace_clause

revision = "build_guard_0019"
down_revision = "build_guard_0018"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
CALLABLES = ("record_outcome(uuid,jsonb,bytea,text,text)", "read_outcome(uuid,jsonb,bytea,text)")
HELPERS = (f"native_outcome_receipt({SCHEMA}.platform_outcomes)", "native_live_claim_count(uuid)", "native_request_finished(uuid)")


def _consumers(*, install):
    pending = f"AND NOT EXISTS (SELECT 1 FROM {SCHEMA}.request_holds h WHERE h.request_id=r.id)"
    for signature, old, new in (
        ("native_drain_receipt(loom_capacity_build_guard.worker_drains)",
            "'live_claim_count',e.payload->'expected_claim_high_water'",
            "'live_claim_count',coalesce(e.snapshot_live_claim_count,(e.payload->>'expected_claim_high_water')::bigint)"),
        ("begin_drain(uuid,jsonb,bytea,text)",
            "operation_id,payload,wire_payload,payload_sha256)\n                VALUES(p_installation,registration.id,(p->>'operation_id')::uuid,p,wire,digest)",
            "operation_id,payload,wire_payload,payload_sha256,snapshot_live_claim_count)\n"
            f"                VALUES(p_installation,registration.id,(p->>'operation_id')::uuid,p,wire,digest,{SCHEMA}.native_live_claim_count(bootstrap.intent_id))"),
        ("fixed_native_claims(uuid)", "WHERE c.installation_id=p_installation ORDER BY",
            f"WHERE c.installation_id=p_installation AND {SCHEMA}.native_live_claim_count(r.intent_id)>0 ORDER BY"),
        ("assert_current_source(uuid,uuid,jsonb,bytea,text)", "OR request.cancelled_at IS NOT NULL",
            f"OR request.cancelled_at IS NOT NULL OR {SCHEMA}.native_request_finished(p_request)"),
        ("read_pending_sources(uuid)", pending, pending + f" AND NOT {SCHEMA}.native_request_finished(r.id)"),
    ):
        _replace_clause(signature, old if install else new, new if install else old)
    # Two copies deliberately exist: discovery and locked-source recheck.
    signature = f"{SCHEMA}.capture_demand(uuid,bigint,jsonb)"
    old = pending if install else pending + f" AND NOT {SCHEMA}.native_request_finished(r.id)"
    new = pending + f" AND NOT {SCHEMA}.native_request_finished(r.id)" if install else pending
    definition = op.get_bind().scalar(sa.text("SELECT pg_get_functiondef(CAST(:signature AS regprocedure))"), {"signature": signature})
    if definition.count(old) != 2:
        raise RuntimeError("native outcome demand migration clause changed")
    op.execute(definition.replace(old, new))


def upgrade():
    op.create_table("platform_outcomes",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("installation_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.installations.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("claim_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.platform_claims.id", ondelete="RESTRICT"), nullable=False, unique=True),
        *_payload(), schema=SCHEMA)
    op.create_index("native_claim_request_lookup", "platform_claims", ["request_id"], schema=SCHEMA)
    op.execute(f"ALTER TABLE {SCHEMA}.platform_outcomes ADD COLUMN retention_xid xid8 NOT NULL DEFAULT pg_current_xact_id()")
    op.execute(f"CREATE TRIGGER build_outcome_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON {SCHEMA}.platform_outcomes FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_evidence_mutation()")
    # NULL preserves every pre-outcome drain receipt without rewriting history.
    op.add_column("worker_drains", sa.Column("snapshot_live_claim_count", sa.BigInteger(), nullable=True), schema=SCHEMA)
    op.create_check_constraint("native_drain_live_count", "worker_drains",
        "snapshot_live_claim_count BETWEEN 0 AND (payload->>'expected_claim_high_water')::bigint", schema=SCHEMA)
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.native_outcome_receipt(e {SCHEMA}.platform_outcomes)
        RETURNS text LANGUAGE sql SECURITY INVOKER SET search_path=pg_catalog AS $$
            SELECT {SCHEMA}.canonical_plan_json(jsonb_build_object('schema_version',1,
                'request',e.payload,'request_digest',e.payload_sha256,
                'claim_high_water',1,'live_claim_count',0,'executable',false))
        $$;

        CREATE FUNCTION {SCHEMA}.native_live_claim_count(p_intent uuid)
        RETURNS bigint LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
        DECLARE outcome {SCHEMA}.platform_outcomes%ROWTYPE;
        BEGIN
            IF {SCHEMA}.native_claim_high_water(p_intent)=0 THEN RETURN 0; END IF;
            SELECT o.* INTO outcome FROM {SCHEMA}.platform_outcomes o
                JOIN {SCHEMA}.platform_claims c ON c.id=o.claim_id
                JOIN {SCHEMA}.worker_registrations r ON r.id=c.registration_id WHERE r.intent_id=p_intent;
            IF NOT FOUND THEN RETURN 1; END IF;
            IF outcome.retention_xid=pg_current_xact_id() THEN
                RAISE EXCEPTION 'native observation requires committed outcome'; END IF;
            RETURN 0;
        END $$;

        CREATE FUNCTION {SCHEMA}.native_request_finished(p_request uuid)
        RETURNS boolean LANGUAGE sql SECURITY INVOKER SET search_path=pg_catalog AS $$
            SELECT EXISTS (SELECT 1 FROM {SCHEMA}.platform_outcomes o
                JOIN {SCHEMA}.platform_claims c ON c.id=o.claim_id
                WHERE c.request_id=p_request AND o.payload->>'result' IN ('artifact-ready','cancelled'))
        $$;

        CREATE FUNCTION {SCHEMA}.record_outcome(p_installation uuid,p jsonb,wire bytea,digest text,credential_hash text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE installation {SCHEMA}.installations%ROWTYPE; bootstrap {SCHEMA}.bootstraps%ROWTYPE;
            physical {SCHEMA}.execution_events%ROWTYPE; registration {SCHEMA}.worker_registrations%ROWTYPE;
            claim {SCHEMA}.platform_claims%ROWTYPE; outcome {SCHEMA}.platform_outcomes%ROWTYPE;
            binding jsonb; artifact jsonb;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'native outcome requires serializable transaction'; END IF;
            IF wire IS NULL OR octet_length(wire) NOT BETWEEN 2 AND 1048576
                OR wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR digest IS DISTINCT FROM encode(sha256(wire),'hex') THEN
                RAISE EXCEPTION 'native outcome canonical request changed'; END IF;
            IF jsonb_typeof(p) IS DISTINCT FROM 'object' OR p->'schema_version' IS DISTINCT FROM '1'::jsonb
                OR NOT p ?& ARRAY['schema_version','claim','operation_id','result','artifact']
                OR p - ARRAY['schema_version','claim','operation_id','result','artifact'] <> '{{}}'::jsonb
                OR jsonb_typeof(p->'operation_id') IS DISTINCT FROM 'string'
                OR (p->>'operation_id') !~ '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
                OR jsonb_typeof(p->'result') IS DISTINCT FROM 'string'
                OR p->>'result' NOT IN ('artifact-ready','failed','cancelled') THEN
                RAISE EXCEPTION 'native outcome schema changed'; END IF;
            artifact := p->'artifact';
            IF p->>'result'='artifact-ready' THEN
                IF jsonb_typeof(artifact) IS DISTINCT FROM 'object'
                    OR artifact->'schema_version' IS DISTINCT FROM '1'::jsonb
                    OR NOT artifact ?& ARRAY['schema_version','archive_sha256','archive_size_bytes']
                    OR artifact - ARRAY['schema_version','archive_sha256','archive_size_bytes'] <> '{{}}'::jsonb
                    OR jsonb_typeof(artifact->'archive_sha256') IS DISTINCT FROM 'string'
                    OR (artifact->>'archive_sha256') !~ '^[0-9a-f]{{64}}$'
                    OR jsonb_typeof(artifact->'archive_size_bytes') IS DISTINCT FROM 'number'
                    OR (artifact->>'archive_size_bytes') !~ '^[1-9][0-9]*$'
                    OR (artifact->>'archive_size_bytes')::numeric > 9223372036854775807 THEN
                    RAISE EXCEPTION 'native outcome archive evidence changed'; END IF;
            ELSIF artifact IS DISTINCT FROM 'null'::jsonb THEN
                RAISE EXCEPTION 'native failure outcome cannot assert an artifact'; END IF;
            binding := p->'claim'->'binding';
            SELECT * INTO installation FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'native outcome installation is absent'; END IF;
            SELECT * INTO bootstrap FROM {SCHEMA}.bootstraps WHERE intent_id=(binding->>'intent_id')::uuid FOR UPDATE;
            IF NOT FOUND OR bootstrap.installation_id IS DISTINCT FROM p_installation
                OR bootstrap.payload->'proposal'->'binding' IS DISTINCT FROM binding THEN
                RAISE EXCEPTION 'native outcome bootstrap binding changed'; END IF;
            SELECT * INTO physical FROM {SCHEMA}.execution_events WHERE intent_id=bootstrap.intent_id AND kind='bound' FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'native outcome physical binding absent'; END IF;
            SELECT * INTO registration FROM {SCHEMA}.worker_registrations WHERE intent_id=bootstrap.intent_id FOR UPDATE;
            IF NOT FOUND OR registration.retention_xid=pg_current_xact_id()
                OR registration.installation_id IS DISTINCT FROM p_installation
                OR registration.physical_event_id IS DISTINCT FROM physical.id
                OR credential_hash IS NULL OR registration.credential_sha256 IS DISTINCT FROM credential_hash THEN
                RAISE EXCEPTION 'native outcome requires exact committed worker credential'; END IF;
            SELECT * INTO claim FROM {SCHEMA}.platform_claims WHERE registration_id=registration.id FOR UPDATE;
            IF NOT FOUND OR claim.retention_xid=pg_current_xact_id()
                OR claim.installation_id IS DISTINCT FROM p_installation
                OR claim.payload IS DISTINCT FROM p->'claim' THEN
                RAISE EXCEPTION 'native outcome requires exact committed claim'; END IF;
            SELECT * INTO outcome FROM {SCHEMA}.platform_outcomes WHERE claim_id=claim.id;
            IF FOUND THEN
                IF outcome.payload IS DISTINCT FROM p OR outcome.wire_payload IS DISTINCT FROM wire OR outcome.payload_sha256 IS DISTINCT FROM digest THEN
                    RAISE EXCEPTION 'native outcome exact replay changed'; END IF;
                RETURN {SCHEMA}.native_outcome_receipt(outcome);
            END IF;
            -- Historical completion is valid after source cancellation/expiry.
            -- This never writes public candidate status or releases a request hold.
            INSERT INTO {SCHEMA}.platform_outcomes(id,installation_id,claim_id,payload,wire_payload,payload_sha256)
                VALUES((p->>'operation_id')::uuid,p_installation,claim.id,p,wire,digest) RETURNING * INTO outcome;
            RETURN {SCHEMA}.native_outcome_receipt(outcome);
        END $function$;

        CREATE FUNCTION {SCHEMA}.read_outcome(p_installation uuid,p jsonb,wire bytea,digest text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        DECLARE claim {SCHEMA}.platform_claims%ROWTYPE; outcome {SCHEMA}.platform_outcomes%ROWTYPE;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'native outcome observation requires serializable transaction'; END IF;
            IF wire IS NULL OR octet_length(wire) NOT BETWEEN 2 AND 1048576
                OR wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR digest IS DISTINCT FROM encode(sha256(wire),'hex') THEN
                RAISE EXCEPTION 'native outcome observation canonical request changed'; END IF;
            PERFORM 1 FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'native outcome observation installation absent'; END IF;
            SELECT * INTO claim FROM {SCHEMA}.platform_claims WHERE id=(p->>'operation_id')::uuid;
            IF NOT FOUND OR claim.installation_id IS DISTINCT FROM p_installation
                OR claim.retention_xid=pg_current_xact_id() OR claim.payload IS DISTINCT FROM p THEN
                RAISE EXCEPTION 'native outcome observation requires exact committed claim'; END IF;
            SELECT * INTO outcome FROM {SCHEMA}.platform_outcomes WHERE claim_id=claim.id;
            IF NOT FOUND THEN RETURN NULL; END IF;
            IF outcome.retention_xid=pg_current_xact_id() THEN
                RAISE EXCEPTION 'native observation requires committed outcome'; END IF;
            RETURN {SCHEMA}.native_outcome_receipt(outcome);
        END $$;
    """)
    _consumers(install=True)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    for signature in HELPERS:
        op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{signature} FROM PUBLIC, {quote(agent)}")
    for signature in CALLABLES:
        op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{signature} FROM PUBLIC")
        op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{signature} TO {quote(agent)}")


def downgrade():
    op.execute(f"LOCK TABLE {SCHEMA}.platform_outcomes IN ACCESS EXCLUSIVE MODE")
    op.execute(f"""DO $$ BEGIN IF EXISTS (SELECT 1 FROM {SCHEMA}.platform_outcomes) THEN
        RAISE EXCEPTION 'cannot remove native outcomes with retained evidence'; END IF; END $$""")
    _consumers(install=False)
    for signature in (*CALLABLES, *HELPERS):
        op.execute(f"DROP FUNCTION {SCHEMA}.{signature}")
    op.drop_constraint("native_drain_live_count", "worker_drains", schema=SCHEMA)
    op.drop_column("worker_drains", "snapshot_live_claim_count", schema=SCHEMA)
    op.drop_index("native_claim_request_lookup", table_name="platform_claims", schema=SCHEMA)
    op.drop_table("platform_outcomes", schema=SCHEMA)
