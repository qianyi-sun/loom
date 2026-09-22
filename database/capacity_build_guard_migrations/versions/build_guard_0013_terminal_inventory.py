"""Retain native terminal inventory without publishing release or deleting holds.

Revision ID: build_guard_0013
Revises: build_guard_0012
"""

import sqlalchemy as sa
from alembic import op

from capacity_build_guard_migrations.versions.build_guard_0001_assignments import _payload

revision = "build_guard_0013"
down_revision = "build_guard_0012"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = "import_terminal_inventory(uuid,jsonb,bytea,text)"


def upgrade():
    op.create_table("terminal_inventory",
        sa.Column("id", sa.BigInteger(), primary_key=True,
            server_default=sa.text(f"nextval(pg_get_serial_sequence('{SCHEMA}.execution_events','id')::regclass)")),
        sa.Column("intent_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.bootstraps.intent_id"), nullable=False, unique=True),
        sa.Column("installation_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.installations.id"), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.assignments.id"), nullable=False),
        *_payload(), schema=SCHEMA)
    op.execute(f"ALTER TABLE {SCHEMA}.terminal_inventory ADD COLUMN retention_xid xid8 NOT NULL DEFAULT pg_current_xact_id()")
    op.execute(f"CREATE TRIGGER build_terminal_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON {SCHEMA}.terminal_inventory FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_evidence_mutation()")
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.terminal_inventory_receipt(e {SCHEMA}.terminal_inventory)
        RETURNS text LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $function$
        BEGIN
            RETURN {SCHEMA}.canonical_plan_json(jsonb_build_object(
                'schema_version',1,'installation_id',e.installation_id,'assignment_id',e.assignment_id,
                'binding',e.payload->'binding','physical_job_id',e.payload->'record'->'physical_identity',
                'inventory_sequence',e.payload->'inventory_sequence',
                'terminal_evidence_sha256',e.payload->'record'->'terminal_evidence_sha256',
                'evidence_digest',e.payload_sha256,'protected_high_water',e.id,
                'import_state','imported','executable',false));
        END $function$;

        CREATE FUNCTION {SCHEMA}.import_terminal_inventory(p_installation uuid,p jsonb,wire bytea,digest text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE installation {SCHEMA}.installations%ROWTYPE; bootstrap {SCHEMA}.bootstraps%ROWTYPE;
            physical {SCHEMA}.execution_events%ROWTYPE; assignment {SCHEMA}.assignments%ROWTYPE;
            e {SCHEMA}.terminal_inventory%ROWTYPE; r jsonb; proof jsonb; metadata jsonb; authority jsonb;
            native_pool jsonb; observed timestamptz;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build terminal import requires serializable transaction';
            END IF;
            IF wire IS NULL OR octet_length(wire) NOT BETWEEN 2 AND 1048576
                OR wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR encode(sha256(wire),'hex') IS DISTINCT FROM digest THEN
                RAISE EXCEPTION 'build terminal canonical evidence changed';
            END IF;
            IF p->'schema_version' IS DISTINCT FROM '3'::jsonb OR p->'executable' IS DISTINCT FROM 'true'::jsonb THEN
                RAISE EXCEPTION 'build terminal requires V3 evidence';
            END IF;
            PERFORM {SCHEMA}.assert_plan_fields(p || jsonb_build_object('schema_version',2),
                ARRAY['schema_version','binding','inventory_execution','inventory_sequence','inventory_digest',
                    'journal_sequence','journal_digest','record','observed_at','executable'],
                ARRAY[]::text[],ARRAY['inventory_sequence'],ARRAY[]::text[],ARRAY['inventory_digest','journal_digest']);
            IF jsonb_typeof(p->'journal_sequence') IS DISTINCT FROM 'number'
                OR p->>'journal_sequence' !~ '^(0|[1-9][0-9]*)$'
                OR ((p->>'journal_sequence')::bigint=0) IS DISTINCT FROM (p->>'journal_digest'=repeat('0',64))
                OR jsonb_typeof(p->'observed_at') IS DISTINCT FROM 'string' THEN
                RAISE EXCEPTION 'build terminal inventory journal or timestamp changed';
            END IF;
            observed := (p->>'observed_at')::timestamptz;
            IF p->>'observed_at' IS DISTINCT FROM {SCHEMA}.demand_timestamp(observed) THEN
                RAISE EXCEPTION 'build terminal timestamp must be canonical UTC';
            END IF;
            r := p->'record'; proof := r->'ownership_proof'; metadata := proof->'metadata';
            authority := metadata->'subject_authority';
            PERFORM {SCHEMA}.assert_plan_fields(r || jsonb_build_object('schema_version',2),
                ARRAY['schema_version','physical_identity','physical_kind','authority_scope','state','resources',
                    'node_ids','controller_evidence_sha256','ownership_proof','terminal_evidence_sha256'],
                ARRAY[]::text[],ARRAY[]::text[],ARRAY['physical_identity'],ARRAY['controller_evidence_sha256','terminal_evidence_sha256']);
            IF r->'schema_version' IS DISTINCT FROM '3'::jsonb OR r->>'state' IS DISTINCT FROM 'terminal'
                OR r->>'physical_kind' IS DISTINCT FROM 'slurm-job'
                OR r->>'authority_scope' IS DISTINCT FROM 'dedicated-loom-association'
                OR proof->'schema_version' IS DISTINCT FROM '3'::jsonb OR metadata->'schema_version' IS DISTINCT FROM '3'::jsonb
                OR authority->'schema_version' IS DISTINCT FROM '3'::jsonb
                OR authority->>'purpose' IS DISTINCT FROM 'personal-build-worker'
                OR authority->>'source' IS DISTINCT FROM 'personal-membership'
                OR metadata->'binding' IS DISTINCT FROM p->'binding'
                OR r->'resources' IS DISTINCT FROM p->'binding'->'resources'
                OR r->'node_ids' IS DISTINCT FROM p->'binding'->'node_ids'
                OR p->'inventory_execution' IS DISTINCT FROM ((p->'binding'->'execution') - 'allocation_epoch' - 'executable') THEN
                RAISE EXCEPTION 'build terminal native record binding changed';
            END IF;
            SELECT * INTO installation FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build terminal installation is absent'; END IF;
            SELECT * INTO bootstrap FROM {SCHEMA}.bootstraps WHERE intent_id=(p->'binding'->>'intent_id')::uuid FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build terminal bootstrap is absent'; END IF;
            IF bootstrap.installation_id IS DISTINCT FROM p_installation
                OR bootstrap.retention_xid=pg_current_xact_id()
                OR bootstrap.payload->'proposal'->'binding' IS DISTINCT FROM p->'binding' THEN
                RAISE EXCEPTION 'build terminal requires exact committed installation and bootstrap';
            END IF;
            SELECT * INTO physical FROM {SCHEMA}.execution_events
                WHERE intent_id=bootstrap.intent_id AND kind='bound' FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build terminal physical binding is absent'; END IF;
            IF physical.retention_xid=pg_current_xact_id() THEN
                RAISE EXCEPTION 'build terminal requires committed physical binding';
            END IF;
            IF physical.payload->'binding' IS DISTINCT FROM p->'binding'
                OR physical.slurm_job_id IS DISTINCT FROM r->>'physical_identity'
                OR physical.payload->>'ownership_evidence_sha256' IS DISTINCT FROM
                    encode(sha256(convert_to({SCHEMA}.canonical_plan_json(proof),'UTF8')),'hex') THEN
                RAISE EXCEPTION 'build terminal physical ownership changed';
            END IF;
            SELECT * INTO assignment FROM {SCHEMA}.assignments WHERE id=physical.assignment_id;
            IF NOT FOUND THEN RAISE EXCEPTION 'build terminal assignment is absent'; END IF;
            IF assignment.submission_intent_id IS DISTINCT FROM bootstrap.intent_id OR NOT EXISTS (
                SELECT 1 FROM {SCHEMA}.plans WHERE id=assignment.plan_id AND installation_id=p_installation) THEN
                RAISE EXCEPTION 'build terminal assignment installation changed';
            END IF;
            SELECT value INTO native_pool FROM jsonb_array_elements(installation.payload->'runtime'->'pools')
                WHERE value->>'pool_id'=p->'binding'->>'pool_id';
            IF NOT FOUND THEN RAISE EXCEPTION 'build terminal installation pool is absent'; END IF;
            IF authority->'membership'->>'owner_id' IS DISTINCT FROM installation.owner_user_id::text
                OR authority->'membership'->'execution_manifest_sha256' IS DISTINCT FROM installation.payload->'runtime'->'execution_manifest_sha256'
                OR authority->'configuration'->>'subject_id' IS DISTINCT FROM installation.subject_id::text
                OR authority->'configuration'->>'subject_incarnation' IS DISTINCT FROM installation.subject_incarnation::text
                OR metadata->'launch_profile_sha256' IS DISTINCT FROM native_pool->'launch_profile_sha256'
                OR metadata->'controller_authority_sha256' IS DISTINCT FROM native_pool->'controller_authority_sha256'
                OR metadata->'trusted_launcher_sha256' IS DISTINCT FROM installation.payload->'runtime'->'trusted_fleet_release_sha256' THEN
                RAISE EXCEPTION 'build terminal installation ownership pins changed';
            END IF;
            SELECT * INTO e FROM {SCHEMA}.terminal_inventory WHERE intent_id=bootstrap.intent_id;
            IF FOUND THEN
                IF e.payload IS DISTINCT FROM p OR e.wire_payload IS DISTINCT FROM wire OR e.payload_sha256 IS DISTINCT FROM digest THEN
                    RAISE EXCEPTION 'build terminal exact replay changed';
                END IF;
                RETURN {SCHEMA}.terminal_inventory_receipt(e);
            END IF;
            -- Only the manager-authenticated reporter may supply this witness.
            -- This local join is not signature verification, worker withdrawal,
            -- scheduler cancellation, protected release publication or hold deletion.
            INSERT INTO {SCHEMA}.terminal_inventory(intent_id,installation_id,assignment_id,payload,wire_payload,payload_sha256)
                VALUES(bootstrap.intent_id,p_installation,assignment.id,p,wire,digest) RETURNING * INTO e;
            RETURN {SCHEMA}.terminal_inventory_receipt(e);
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.terminal_inventory_receipt({SCHEMA}.terminal_inventory) FROM PUBLIC, {quote(agent)}")
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quote(agent)}")


def downgrade():
    op.execute(f"LOCK TABLE {SCHEMA}.terminal_inventory IN ACCESS EXCLUSIVE MODE")
    op.execute(f"""DO $$ BEGIN IF EXISTS (SELECT 1 FROM {SCHEMA}.terminal_inventory) THEN
        RAISE EXCEPTION 'cannot remove build terminal inventory with retained evidence'; END IF; END $$""")
    op.execute(f"DROP FUNCTION {SCHEMA}.{FUNCTION}")
    op.execute(f"DROP FUNCTION {SCHEMA}.terminal_inventory_receipt({SCHEMA}.terminal_inventory)")
    op.drop_table("terminal_inventory", schema=SCHEMA)
