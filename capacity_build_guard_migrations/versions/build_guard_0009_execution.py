"""Retain admitted build preparation and physical ownership without credentials.

Revision ID: build_guard_0009
Revises: build_guard_0008
"""

import sqlalchemy as sa
from alembic import op

from capacity_build_guard_migrations.versions.build_guard_0001_assignments import _payload

revision = "build_guard_0009"
down_revision = "build_guard_0008"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTIONS = ("prepare_worker(uuid,jsonb,bytea,text,text)", "bind_slurm_job(uuid,jsonb,bytea,text)")


def upgrade():
    op.create_table("execution_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("intent_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.bootstraps.intent_id"), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.assignments.id"), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("operation_id", sa.Uuid(), nullable=False, unique=True),
        sa.Column("bootstrap_sha256", sa.Text(), nullable=False),
        sa.Column("pool_id", sa.Text(), nullable=False),
        sa.Column("slurm_job_id", sa.Text()),
        sa.CheckConstraint("kind IN ('prepared','bound') AND (kind='bound')=(slurm_job_id IS NOT NULL)"),
        sa.UniqueConstraint("intent_id", "kind"), sa.UniqueConstraint("pool_id", "slurm_job_id"),
        *_payload(), schema=SCHEMA)
    op.execute(f"ALTER TABLE {SCHEMA}.execution_events ADD COLUMN retention_xid xid8 NOT NULL DEFAULT pg_current_xact_id()")
    op.execute(f"CREATE TRIGGER build_execution_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON {SCHEMA}.execution_events FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_evidence_mutation()")
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.execution_receipt(e {SCHEMA}.execution_events)
        RETURNS text LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $function$
        DECLARE receipt jsonb; binding jsonb := e.payload->'binding';
        BEGIN
            receipt := jsonb_build_object('schema_version',2,'subject_id',binding->'subject_id',
                'subject_incarnation',binding->'subject_incarnation','intent_id',e.intent_id,
                'bootstrap_registration_epoch',1,'request_digest',e.payload_sha256,
                'protected_high_water',e.id,'executable',true);
            IF e.kind='prepared' THEN
                receipt := receipt || jsonb_build_object('bootstrap_sha256',e.bootstrap_sha256,
                    'admission_digest',e.payload_sha256,'admission_state','prepared');
            ELSE
                receipt := receipt || jsonb_build_object('slurm_job_id',e.slurm_job_id,
                    'ownership_evidence_sha256',e.payload->'ownership_evidence_sha256',
                    'binding_digest',e.payload_sha256,'binding_state','bound');
            END IF;
            RETURN {SCHEMA}.canonical_plan_json(receipt);
        END $function$;

        CREATE FUNCTION {SCHEMA}.prepare_worker(p_installation uuid, p jsonb, wire bytea, digest text, bootstrap_hash text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE installation {SCHEMA}.installations%ROWTYPE; bootstrap {SCHEMA}.bootstraps%ROWTYPE;
            event {SCHEMA}.execution_events%ROWTYPE; assignment {SCHEMA}.assignments%ROWTYPE;
            plan {SCHEMA}.plans%ROWTYPE; proposal jsonb; publication {SCHEMA}.dispositions%ROWTYPE;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build worker preparation requires serializable transaction';
            END IF;
            IF wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR encode(sha256(wire),'hex') IS DISTINCT FROM digest OR octet_length(wire) NOT BETWEEN 2 AND 1048576 THEN
                RAISE EXCEPTION 'build worker preparation canonical request changed';
            END IF;
            PERFORM {SCHEMA}.assert_plan_fields(p,
                ARRAY['schema_version','binding','command_sequence','bootstrap_registration_epoch','bootstrap_evidence_sha256','executable'],
                ARRAY[]::text[],ARRAY['command_sequence','bootstrap_registration_epoch'],ARRAY[]::text[],ARRAY['bootstrap_evidence_sha256']);
            SELECT * INTO installation FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build worker installation is absent'; END IF;
            SELECT * INTO bootstrap FROM {SCHEMA}.bootstraps WHERE intent_id=(p->'binding'->>'intent_id')::uuid FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build worker bootstrap is absent'; END IF;
            proposal := bootstrap.payload->'proposal';
            IF bootstrap.installation_id IS DISTINCT FROM p_installation
                OR bootstrap.retention_xid=pg_current_xact_id()
                OR p->'binding' IS DISTINCT FROM proposal->'binding'
                OR p->'command_sequence' IS DISTINCT FROM proposal->'command_sequence'
                OR p->>'bootstrap_evidence_sha256' IS DISTINCT FROM bootstrap.payload_sha256
                OR p->'bootstrap_registration_epoch' IS DISTINCT FROM '1'::jsonb
                OR p->'executable' IS DISTINCT FROM 'true'::jsonb
                OR bootstrap_hash IS DISTINCT FROM proposal->>'bootstrap_sha256' THEN
                RAISE EXCEPTION 'build worker committed bootstrap binding changed';
            END IF;
            SELECT * INTO event FROM {SCHEMA}.execution_events WHERE intent_id=bootstrap.intent_id AND kind='prepared' FOR UPDATE;
            IF FOUND THEN
                IF event.payload IS DISTINCT FROM p OR event.wire_payload IS DISTINCT FROM wire
                    OR event.payload_sha256 IS DISTINCT FROM digest OR event.bootstrap_sha256 IS DISTINCT FROM bootstrap_hash THEN
                    RAISE EXCEPTION 'build worker preparation replay changed';
                END IF;
                -- Replay is evidence recovery, not a fresh permit. It must survive
                -- cancellation so an executor can advance its journal to cleanup.
                RETURN {SCHEMA}.execution_receipt(event);
            END IF;
            SELECT * INTO assignment FROM {SCHEMA}.assignments WHERE submission_intent_id=bootstrap.intent_id;
            IF NOT FOUND THEN RAISE EXCEPTION 'build worker assignment is absent'; END IF;
            SELECT * INTO plan FROM {SCHEMA}.plans WHERE id=assignment.plan_id FOR UPDATE;
            IF plan.installation_id IS DISTINCT FROM p_installation OR NOT EXISTS (
                SELECT 1 FROM jsonb_array_elements(plan.payload->'shapes') s
                WHERE s->'binding'=p->'binding' AND s->'bootstrap_registration_epoch'='1'::jsonb) THEN
                RAISE EXCEPTION 'build worker admitted shape binding changed';
            END IF;
            SELECT * INTO publication FROM {SCHEMA}.dispositions WHERE plan_id=plan.id AND kind='publication';
            IF NOT FOUND THEN RAISE EXCEPTION 'build worker committed publication is absent'; END IF;
            IF publication.retention_xid=pg_current_xact_id() THEN
                RAISE EXCEPTION 'build worker requires committed publication';
            END IF;
            PERFORM {SCHEMA}.assert_bootstrap(proposal,convert_to({SCHEMA}.canonical_plan_json(proposal),'UTF8'),
                bootstrap.proposal_sha256,installation.payload);
            -- Reuse the complete sorted-lock source/assignment/lease checks, but
            -- only after proving publication already exists and committed.
            PERFORM {SCHEMA}.authorize_publication(p_installation,plan.id);
            INSERT INTO {SCHEMA}.execution_events(intent_id,assignment_id,kind,operation_id,bootstrap_sha256,pool_id,
                payload,wire_payload,payload_sha256)
                VALUES(bootstrap.intent_id,assignment.id,'prepared',bootstrap.intent_id,bootstrap_hash,p->'binding'->>'pool_id',p,wire,digest)
                RETURNING * INTO event;
            RETURN {SCHEMA}.execution_receipt(event);
        END $function$;

        CREATE FUNCTION {SCHEMA}.bind_slurm_job(p_installation uuid, p jsonb, wire bytea, digest text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE bootstrap {SCHEMA}.bootstraps%ROWTYPE; prepared {SCHEMA}.execution_events%ROWTYPE;
            event {SCHEMA}.execution_events%ROWTYPE;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build physical binding requires serializable transaction';
            END IF;
            IF wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR encode(sha256(wire),'hex') IS DISTINCT FROM digest OR octet_length(wire) NOT BETWEEN 2 AND 1048576 THEN
                RAISE EXCEPTION 'build physical binding canonical request changed';
            END IF;
            PERFORM {SCHEMA}.assert_plan_fields(p,
                ARRAY['schema_version','operation_id','binding','bootstrap_registration_epoch','slurm_job_id','ownership_evidence_sha256','executable'],
                ARRAY['operation_id'],ARRAY['bootstrap_registration_epoch'],ARRAY['slurm_job_id'],ARRAY['ownership_evidence_sha256']);
            PERFORM 1 FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build physical binding installation is absent'; END IF;
            SELECT * INTO bootstrap FROM {SCHEMA}.bootstraps WHERE intent_id=(p->'binding'->>'intent_id')::uuid FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build physical binding bootstrap is absent'; END IF;
            IF bootstrap.installation_id IS DISTINCT FROM p_installation OR p->'executable' IS DISTINCT FROM 'true'::jsonb
                OR p->'bootstrap_registration_epoch' IS DISTINCT FROM '1'::jsonb
                OR p->'binding' IS DISTINCT FROM bootstrap.payload->'proposal'->'binding' THEN
                RAISE EXCEPTION 'build physical binding bootstrap changed';
            END IF;
            SELECT * INTO prepared FROM {SCHEMA}.execution_events WHERE intent_id=bootstrap.intent_id AND kind='prepared' FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build physical binding preparation is absent'; END IF;
            IF prepared.retention_xid=pg_current_xact_id() THEN
                RAISE EXCEPTION 'build physical binding requires committed preparation';
            END IF;
            SELECT * INTO event FROM {SCHEMA}.execution_events WHERE intent_id=bootstrap.intent_id AND kind='bound' FOR UPDATE;
            IF FOUND THEN
                IF event.payload IS DISTINCT FROM p OR event.wire_payload IS DISTINCT FROM wire
                    OR event.payload_sha256 IS DISTINCT FROM digest THEN
                    RAISE EXCEPTION 'build physical binding replay changed';
                END IF;
                RETURN {SCHEMA}.execution_receipt(event);
            END IF;
            -- A scheduler job may exist even when source/plan authority expired.
            -- Record ownership for cleanup; this issues no execution credential.
            INSERT INTO {SCHEMA}.execution_events(intent_id,assignment_id,kind,operation_id,bootstrap_sha256,pool_id,slurm_job_id,
                payload,wire_payload,payload_sha256)
                VALUES(bootstrap.intent_id,prepared.assignment_id,'bound',(p->>'operation_id')::uuid,prepared.bootstrap_sha256,
                    p->'binding'->>'pool_id',p->>'slurm_job_id',p,wire,digest) RETURNING * INTO event;
            RETURN {SCHEMA}.execution_receipt(event);
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.execution_receipt({SCHEMA}.execution_events) FROM PUBLIC, {quote(agent)}")
    for signature in FUNCTIONS:
        op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{signature} FROM PUBLIC")
        op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{signature} TO {quote(agent)}")


def downgrade():
    op.execute(f"LOCK TABLE {SCHEMA}.execution_events IN ACCESS EXCLUSIVE MODE")
    op.execute(f"""DO $$ BEGIN IF EXISTS (SELECT 1 FROM {SCHEMA}.execution_events) THEN
        RAISE EXCEPTION 'cannot remove build execution with retained evidence'; END IF; END $$""")
    for signature in FUNCTIONS:
        op.execute(f"DROP FUNCTION {SCHEMA}.{signature}")
    op.execute(f"DROP FUNCTION {SCHEMA}.execution_receipt({SCHEMA}.execution_events)")
    op.drop_table("execution_events",schema=SCHEMA)
