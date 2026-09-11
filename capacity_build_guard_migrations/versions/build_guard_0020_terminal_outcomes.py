"""Settle lost native results using exact committed manager terminal evidence.

Revision ID: build_guard_0020
Revises: build_guard_0019
"""

from alembic import op

revision = "build_guard_0020"
down_revision = "build_guard_0019"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = "settle_interrupted_claim(uuid,jsonb,bytea,text)"


def upgrade():
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.settle_interrupted_claim(p_installation uuid,p jsonb,wire bytea,digest text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE bootstrap {SCHEMA}.bootstraps%ROWTYPE; physical {SCHEMA}.execution_events%ROWTYPE;
            registration {SCHEMA}.worker_registrations%ROWTYPE; claim {SCHEMA}.platform_claims%ROWTYPE;
            terminal {SCHEMA}.terminal_inventory%ROWTYPE; outcome {SCHEMA}.platform_outcomes%ROWTYPE;
            binding jsonb;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'native interruption requires serializable transaction'; END IF;
            IF wire IS NULL OR octet_length(wire) NOT BETWEEN 2 AND 1048576
                OR wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR digest IS DISTINCT FROM encode(sha256(wire),'hex') THEN
                RAISE EXCEPTION 'native interruption canonical request changed'; END IF;
            IF jsonb_typeof(p) IS DISTINCT FROM 'object' OR p->'schema_version' IS DISTINCT FROM '1'::jsonb
                OR NOT p ?& ARRAY['schema_version','claim','operation_id','result','terminal_inventory_sha256']
                OR p - ARRAY['schema_version','claim','operation_id','result','terminal_inventory_sha256'] <> '{{}}'::jsonb
                OR p->>'result' IS DISTINCT FROM 'interrupted'
                OR jsonb_typeof(p->'operation_id') IS DISTINCT FROM 'string'
                OR (p->>'operation_id') !~ '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
                OR jsonb_typeof(p->'terminal_inventory_sha256') IS DISTINCT FROM 'string'
                OR (p->>'terminal_inventory_sha256') !~ '^[0-9a-f]{{64}}$' THEN
                RAISE EXCEPTION 'native interruption schema changed'; END IF;
            binding := p->'claim'->'binding';
            PERFORM 1 FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'native interruption installation is absent'; END IF;
            SELECT * INTO bootstrap FROM {SCHEMA}.bootstraps WHERE intent_id=(binding->>'intent_id')::uuid FOR UPDATE;
            IF NOT FOUND OR bootstrap.installation_id IS DISTINCT FROM p_installation
                OR bootstrap.retention_xid=pg_current_xact_id()
                OR bootstrap.payload->'proposal'->'binding' IS DISTINCT FROM binding THEN
                RAISE EXCEPTION 'native interruption bootstrap binding changed'; END IF;
            SELECT * INTO physical FROM {SCHEMA}.execution_events WHERE intent_id=bootstrap.intent_id AND kind='bound' FOR UPDATE;
            IF NOT FOUND OR physical.retention_xid=pg_current_xact_id() THEN
                RAISE EXCEPTION 'native interruption physical binding absent'; END IF;
            SELECT * INTO registration FROM {SCHEMA}.worker_registrations WHERE intent_id=bootstrap.intent_id FOR UPDATE;
            IF NOT FOUND OR registration.retention_xid=pg_current_xact_id()
                OR registration.installation_id IS DISTINCT FROM p_installation
                OR registration.physical_event_id IS DISTINCT FROM physical.id
                OR registration.payload->'binding' IS DISTINCT FROM binding THEN
                RAISE EXCEPTION 'native interruption requires exact committed worker'; END IF;
            SELECT * INTO claim FROM {SCHEMA}.platform_claims WHERE registration_id=registration.id FOR UPDATE;
            IF NOT FOUND OR claim.retention_xid=pg_current_xact_id()
                OR claim.installation_id IS DISTINCT FROM p_installation
                OR claim.assignment_id IS DISTINCT FROM physical.assignment_id
                OR claim.payload IS DISTINCT FROM p->'claim' THEN
                RAISE EXCEPTION 'native interruption requires exact committed claim'; END IF;
            SELECT * INTO terminal FROM {SCHEMA}.terminal_inventory WHERE intent_id=bootstrap.intent_id;
            IF NOT FOUND OR terminal.retention_xid=pg_current_xact_id()
                OR terminal.installation_id IS DISTINCT FROM p_installation
                OR terminal.assignment_id IS DISTINCT FROM claim.assignment_id
                OR terminal.payload->'binding' IS DISTINCT FROM binding
                OR terminal.payload->'record'->>'physical_identity' IS DISTINCT FROM physical.slurm_job_id
                OR terminal.payload_sha256 IS DISTINCT FROM p->>'terminal_inventory_sha256' THEN
                RAISE EXCEPTION 'native interruption requires exact committed terminal inventory'; END IF;
            SELECT * INTO outcome FROM {SCHEMA}.platform_outcomes WHERE claim_id=claim.id;
            IF FOUND THEN
                IF outcome.retention_xid=pg_current_xact_id() THEN
                    RAISE EXCEPTION 'native interruption requires committed outcome'; END IF;
                IF outcome.payload->>'result'='interrupted' AND
                    (outcome.payload IS DISTINCT FROM p OR outcome.wire_payload IS DISTINCT FROM wire OR outcome.payload_sha256 IS DISTINCT FROM digest) THEN
                    RAISE EXCEPTION 'native interruption exact replay changed'; END IF;
                -- A committed wrapper result wins unchanged. Never replace its
                -- archive evidence with a guess based on physical termination.
                RETURN {SCHEMA}.native_outcome_receipt(outcome);
            END IF;
            INSERT INTO {SCHEMA}.platform_outcomes(id,installation_id,claim_id,payload,wire_payload,payload_sha256)
                VALUES((p->>'operation_id')::uuid,p_installation,claim.id,p,wire,digest) RETURNING * INTO outcome;
            RETURN {SCHEMA}.native_outcome_receipt(outcome);
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quote(agent)}")


def downgrade():
    op.execute(f"LOCK TABLE {SCHEMA}.platform_outcomes IN ACCESS EXCLUSIVE MODE")
    op.execute(f"""DO $$ BEGIN IF EXISTS (SELECT 1 FROM {SCHEMA}.platform_outcomes WHERE payload->>'result'='interrupted') THEN
        RAISE EXCEPTION 'cannot remove native interruption with retained evidence'; END IF; END $$""")
    op.execute(f"DROP FUNCTION {SCHEMA}.{FUNCTION}")
