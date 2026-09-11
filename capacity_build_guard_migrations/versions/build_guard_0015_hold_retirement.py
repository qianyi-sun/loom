"""Retire exact unregistered holds using retained manager release authority.

Revision ID: build_guard_0015
Revises: build_guard_0014
"""

import sqlalchemy as sa
from alembic import op

from capacity_build_guard_migrations.versions.build_guard_0001_assignments import _payload

revision = "build_guard_0015"
down_revision = "build_guard_0014"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = "retire_request_hold(uuid,jsonb,bytea,text)"


def upgrade():
    op.create_table("hold_retirements",
        sa.Column("id", sa.BigInteger(), primary_key=True,
            server_default=sa.text(f"nextval(pg_get_serial_sequence('{SCHEMA}.execution_events','id')::regclass)")),
        sa.Column("installation_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.installations.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("intent_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.bootstraps.intent_id", ondelete="RESTRICT"), nullable=False, unique=True),
        sa.Column("request_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), nullable=False, unique=True),
        sa.Column("publication_event_id", sa.BigInteger(), sa.ForeignKey(f"{SCHEMA}.release_publication_receipts.event_id", ondelete="RESTRICT"), nullable=False),
        sa.ForeignKeyConstraint(["request_id", "assignment_id"], [f"{SCHEMA}.assignments.request_id", f"{SCHEMA}.assignments.id"], ondelete="RESTRICT"),
        *_payload(), schema=SCHEMA)
    op.execute(f"ALTER TABLE {SCHEMA}.hold_retirements ADD COLUMN retention_xid xid8 NOT NULL DEFAULT pg_current_xact_id()")
    op.execute(f"CREATE TRIGGER build_hold_retirement_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON {SCHEMA}.hold_retirements FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_evidence_mutation()")
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.hold_retirement_receipt(e {SCHEMA}.hold_retirements)
        RETURNS text LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $function$
        BEGIN
            RETURN {SCHEMA}.canonical_plan_json(jsonb_build_object('schema_version',1,
                'installation_id',e.installation_id,'request_id',e.request_id,'assignment_id',e.assignment_id,
                'binding',e.payload->'release'->'binding','witness_sha256',e.payload_sha256,
                'protected_high_water',e.id,'retirement_state','retired','executable',false));
        END $function$;

        CREATE FUNCTION {SCHEMA}.retire_request_hold(p_installation uuid,p jsonb,wire bytea,digest text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE bootstrap {SCHEMA}.bootstraps%ROWTYPE; assignment {SCHEMA}.assignments%ROWTYPE;
            physical {SCHEMA}.execution_events%ROWTYPE; terminal {SCHEMA}.terminal_inventory%ROWTYPE;
            acknowledged {SCHEMA}.release_publication_receipts%ROWTYPE;
            retained {SCHEMA}.hold_retirements%ROWTYPE; event record; publication jsonb;
            released jsonb; binding jsonb; released_at timestamptz;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build hold retirement requires serializable transaction';
            END IF;
            IF wire IS NULL OR octet_length(wire) NOT BETWEEN 2 AND 1048576
                OR wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR digest IS DISTINCT FROM encode(sha256(wire),'hex') THEN
                RAISE EXCEPTION 'build hold retirement canonical witness changed';
            END IF;
            PERFORM {SCHEMA}.assert_plan_fields(p,
                ARRAY['schema_version','release','protected_release','protected_acknowledgement_sha256',
                    'command_sequence','command_request_sha256','released_at'],ARRAY[]::text[],
                ARRAY['command_sequence'],ARRAY[]::text[],ARRAY['protected_acknowledgement_sha256','command_request_sha256']);
            released := p->'release'; binding := released->'binding';
            PERFORM {SCHEMA}.assert_plan_fields(released,
                ARRAY['schema_version','binding','inventory_sequence','terminal_kind','terminal_identity',
                    'terminal_evidence_sha256','protected_registration_epoch','bootstrap_revoked','protected_release_sha256'],
                ARRAY[]::text[],ARRAY['inventory_sequence','protected_registration_epoch'],
                ARRAY['terminal_kind','terminal_identity'],ARRAY['terminal_evidence_sha256','protected_release_sha256']);
            IF p->'schema_version' IS DISTINCT FROM '2'::jsonb OR released->'schema_version' IS DISTINCT FROM '2'::jsonb
                OR released->'bootstrap_revoked' IS DISTINCT FROM 'true'::jsonb
                OR jsonb_typeof(p->'released_at') IS DISTINCT FROM 'string' THEN
                RAISE EXCEPTION 'build hold retirement witness schema changed';
            END IF;
            released_at := (p->>'released_at')::timestamptz;
            IF p->>'released_at' IS DISTINCT FROM {SCHEMA}.demand_timestamp(released_at) THEN
                RAISE EXCEPTION 'build hold retirement requires canonical release time';
            END IF;
            PERFORM 1 FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build hold retirement installation is absent'; END IF;
            SELECT * INTO bootstrap FROM {SCHEMA}.bootstraps WHERE intent_id=(binding->>'intent_id')::uuid FOR UPDATE;
            IF NOT FOUND OR bootstrap.installation_id IS DISTINCT FROM p_installation
                OR bootstrap.retention_xid=pg_current_xact_id()
                OR bootstrap.payload->'proposal'->'binding' IS DISTINCT FROM binding THEN
                RAISE EXCEPTION 'build hold retirement requires exact committed bootstrap';
            END IF;
            SELECT * INTO assignment FROM {SCHEMA}.assignments WHERE submission_intent_id=bootstrap.intent_id;
            IF NOT FOUND OR NOT EXISTS (SELECT 1 FROM {SCHEMA}.plans
                WHERE id=assignment.plan_id AND installation_id=p_installation) THEN
                RAISE EXCEPTION 'build hold retirement assignment is absent';
            END IF;
            SELECT * INTO retained FROM {SCHEMA}.hold_retirements WHERE intent_id=bootstrap.intent_id;
            IF FOUND THEN
                IF retained.installation_id IS DISTINCT FROM p_installation OR retained.assignment_id IS DISTINCT FROM assignment.id
                    OR retained.payload IS DISTINCT FROM p OR retained.wire_payload IS DISTINCT FROM wire
                    OR retained.payload_sha256 IS DISTINCT FROM digest THEN
                    RAISE EXCEPTION 'build hold retirement exact replay changed';
                END IF;
                -- Never examine or delete a successor assignment's hold on replay.
                RETURN {SCHEMA}.hold_retirement_receipt(retained);
            END IF;
            SELECT e.* INTO event FROM (
                SELECT id,intent_id,retention_xid,'prepared-revoked' AS kind FROM {SCHEMA}.bootstrap_revocations
                UNION ALL SELECT id,intent_id,retention_xid,'withdrawn' AS kind FROM {SCHEMA}.worker_withdrawals
            ) e WHERE e.intent_id=bootstrap.intent_id;
            IF NOT FOUND OR event.retention_xid=pg_current_xact_id() THEN
                RAISE EXCEPTION 'build hold retirement requires committed revocation';
            END IF;
            publication := {SCHEMA}.protected_release_publication(p_installation,event.id)::jsonb;
            SELECT * INTO acknowledged FROM {SCHEMA}.release_publication_receipts WHERE event_id=event.id;
            IF NOT FOUND OR acknowledged.installation_id IS DISTINCT FROM p_installation
                OR acknowledged.retention_xid=pg_current_xact_id()
                OR acknowledged.payload->'manager_acknowledgement_digest' IS DISTINCT FROM p->'protected_acknowledgement_sha256'
                OR publication->'publication_digest' IS DISTINCT FROM p->'protected_acknowledgement_sha256'
                OR publication->'release' IS DISTINCT FROM p->'protected_release'
                OR released->'protected_registration_epoch' IS DISTINCT FROM p->'protected_release'->'protected_registration_epoch'
                OR released->'protected_release_sha256' IS DISTINCT FROM p->'protected_release'->'protected_release_sha256' THEN
                RAISE EXCEPTION 'build hold retirement requires exact committed manager acknowledgement';
            END IF;
            SELECT * INTO physical FROM {SCHEMA}.execution_events WHERE intent_id=bootstrap.intent_id AND kind='bound' FOR UPDATE;
            IF event.kind='prepared-revoked' THEN
                IF FOUND OR released->>'terminal_kind' IS DISTINCT FROM 'unused'
                    OR released->>'terminal_identity' IS DISTINCT FROM assignment.shape_instance_id THEN
                    RAISE EXCEPTION 'build hold retirement unused physical binding changed';
                END IF;
            ELSE
                IF NOT FOUND OR physical.retention_xid=pg_current_xact_id()
                    OR physical.assignment_id IS DISTINCT FROM assignment.id
                    OR physical.payload->'binding' IS DISTINCT FROM binding
                    OR released->>'terminal_kind' IS DISTINCT FROM 'slurm-job'
                    OR released->>'terminal_identity' IS DISTINCT FROM physical.slurm_job_id THEN
                    RAISE EXCEPTION 'build hold retirement physical binding changed';
                END IF;
                SELECT * INTO terminal FROM {SCHEMA}.terminal_inventory WHERE intent_id=bootstrap.intent_id;
                IF NOT FOUND OR terminal.retention_xid=pg_current_xact_id()
                    OR terminal.assignment_id IS DISTINCT FROM assignment.id
                    OR terminal.installation_id IS DISTINCT FROM p_installation
                    OR terminal.payload->'binding' IS DISTINCT FROM binding
                    OR terminal.payload->'inventory_sequence' IS DISTINCT FROM released->'inventory_sequence'
                    OR terminal.payload->'record'->'physical_identity' IS DISTINCT FROM released->'terminal_identity'
                    OR terminal.payload->'record'->'terminal_evidence_sha256' IS DISTINCT FROM released->'terminal_evidence_sha256' THEN
                    RAISE EXCEPTION 'build hold retirement requires exact committed native terminal evidence';
                END IF;
            END IF;
            PERFORM 1 FROM {SCHEMA}.request_holds WHERE request_id=assignment.request_id AND assignment_id=assignment.id FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build hold retirement current hold changed'; END IF;
            INSERT INTO {SCHEMA}.hold_retirements(installation_id,intent_id,request_id,assignment_id,publication_event_id,payload,wire_payload,payload_sha256)
                VALUES(p_installation,bootstrap.intent_id,assignment.request_id,assignment.id,event.id,p,wire,digest) RETURNING * INTO retained;
            DELETE FROM {SCHEMA}.request_holds WHERE request_id=assignment.request_id AND assignment_id=assignment.id;
            RETURN {SCHEMA}.hold_retirement_receipt(retained);
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.hold_retirement_receipt({SCHEMA}.hold_retirements) FROM PUBLIC, {quote(agent)}")
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quote(agent)}")


def downgrade():
    op.execute(f"LOCK TABLE {SCHEMA}.hold_retirements IN ACCESS EXCLUSIVE MODE")
    op.execute(f"""DO $$ BEGIN IF EXISTS (SELECT 1 FROM {SCHEMA}.hold_retirements) THEN
        RAISE EXCEPTION 'cannot remove build hold retirement with retained evidence'; END IF; END $$""")
    op.execute(f"DROP FUNCTION {SCHEMA}.{FUNCTION}")
    op.execute(f"DROP FUNCTION {SCHEMA}.hold_retirement_receipt({SCHEMA}.hold_retirements)")
    op.drop_table("hold_retirements", schema=SCHEMA)
