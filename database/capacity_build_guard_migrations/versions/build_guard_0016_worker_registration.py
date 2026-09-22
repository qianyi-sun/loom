"""Register one private native worker after exact physical and source admission.

Revision ID: build_guard_0016
Revises: build_guard_0015
"""

import sqlalchemy as sa
from alembic import op

from capacity_build_guard_migrations.versions.build_guard_0001_assignments import _payload
from capacity_build_guard_migrations.versions.build_guard_0011_revocation import _replace_clause

revision = "build_guard_0016"
down_revision = "build_guard_0015"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = "register_worker(uuid,jsonb,bytea,text,text)"


def _fences(*, install):
    observation = f"SELECT * INTO prepared FROM {SCHEMA}.execution_events\n                WHERE intent_id=bootstrap.intent_id AND kind='prepared';"
    registered = (f"IF EXISTS (SELECT 1 FROM {SCHEMA}.worker_registrations WHERE intent_id=bootstrap.intent_id) THEN\n"
        f"                RETURN {SCHEMA}.observe_registered_worker(bootstrap.intent_id,p);\n"
        "            END IF;\n            " + observation)
    _replace_clause("observe_intent(uuid,jsonb,bytea,text)", observation if install else registered, registered if install else observation)
    withdrawal = f"PERFORM {SCHEMA}.assert_bootstrap_not_revoked(bootstrap.intent_id);"
    fenced = (f"IF EXISTS (SELECT 1 FROM {SCHEMA}.worker_registrations WHERE intent_id=bootstrap.intent_id) THEN\n"
        "                RAISE EXCEPTION 'build withdrawal cannot revoke a registered worker';\n"
        "            END IF;\n            " + withdrawal)
    _replace_clause("withdraw_unregistered_worker(uuid,jsonb,bytea,text)", withdrawal if install else fenced, fenced if install else withdrawal)


def upgrade():
    op.create_table("worker_registrations",
        sa.Column("id", sa.BigInteger(), primary_key=True,
            server_default=sa.text(f"nextval(pg_get_serial_sequence('{SCHEMA}.execution_events','id')::regclass)")),
        sa.Column("installation_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.installations.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("intent_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.bootstraps.intent_id", ondelete="RESTRICT"), nullable=False, unique=True),
        sa.Column("assignment_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.assignments.id", ondelete="RESTRICT"), nullable=False, unique=True),
        sa.Column("physical_event_id", sa.BigInteger(), sa.ForeignKey(f"{SCHEMA}.execution_events.id", ondelete="RESTRICT"), nullable=False, unique=True),
        sa.Column("operation_id", sa.Uuid(), nullable=False, unique=True),
        sa.Column("worker_id", sa.Uuid(), nullable=False, unique=True),
        sa.Column("worker_incarnation", sa.Uuid(), nullable=False, unique=True),
        sa.Column("credential_sha256", sa.Text(), nullable=False, unique=True),
        *_payload(), schema=SCHEMA)
    op.execute(f"ALTER TABLE {SCHEMA}.worker_registrations ADD COLUMN retention_xid xid8 NOT NULL DEFAULT pg_current_xact_id()")
    op.execute(f"CREATE TRIGGER build_worker_registration_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON {SCHEMA}.worker_registrations FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_evidence_mutation()")
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.worker_registration_receipt(e {SCHEMA}.worker_registrations)
        RETURNS text LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $function$
        BEGIN
            RETURN {SCHEMA}.canonical_plan_json(jsonb_build_object('schema_version',2,
                'subject_id',e.payload->'binding'->'subject_id','subject_incarnation',e.payload->'binding'->'subject_incarnation',
                'intent_id',e.intent_id,'worker_id',e.worker_id,'worker_incarnation',e.worker_incarnation,
                'predecessor_worker_incarnation',NULL,'protected_registration_epoch',2,
                'request_digest',e.payload_sha256,'registration_digest',e.payload_sha256,
                'protected_high_water',e.id,'registration_state','registered','executable',true));
        END $function$;

        CREATE FUNCTION {SCHEMA}.observe_registered_worker(p_intent uuid,binding jsonb)
        RETURNS text LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $function$
        DECLARE e {SCHEMA}.worker_registrations%ROWTYPE;
        BEGIN
            SELECT * INTO STRICT e FROM {SCHEMA}.worker_registrations WHERE intent_id=p_intent;
            IF e.retention_xid=pg_current_xact_id() OR e.payload->'binding' IS DISTINCT FROM binding THEN
                RAISE EXCEPTION 'build observation requires exact committed registration';
            END IF;
            RETURN {SCHEMA}.canonical_plan_json(jsonb_build_object('schema_version',2,'binding',binding,
                'bootstrap_registration_epoch',1,'worker_id',e.worker_id,'worker_incarnation',e.worker_incarnation,
                'protected_registration_epoch',2,'claim_high_water',0,'drain',NULL,'release',NULL,
                'withdrawal',NULL,'prepared_revocation',NULL,'executable',true));
        END $function$;

        CREATE FUNCTION {SCHEMA}.register_worker(p_installation uuid,p jsonb,wire bytea,digest text,bootstrap_hash text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE installation {SCHEMA}.installations%ROWTYPE; bootstrap {SCHEMA}.bootstraps%ROWTYPE;
            physical {SCHEMA}.execution_events%ROWTYPE; assignment {SCHEMA}.assignments%ROWTYPE;
            e {SCHEMA}.worker_registrations%ROWTYPE; identities uuid[]; proposal jsonb;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build registration requires serializable transaction';
            END IF;
            IF wire IS NULL OR octet_length(wire) NOT BETWEEN 2 AND 1048576
                OR wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR digest IS DISTINCT FROM encode(sha256(wire),'hex') THEN
                RAISE EXCEPTION 'build registration canonical request changed';
            END IF;
            PERFORM {SCHEMA}.assert_plan_fields(p,
                ARRAY['schema_version','operation_id','binding','bootstrap_registration_epoch','protected_registration_epoch',
                    'slurm_job_id','worker_id','worker_incarnation','worker_credential_sha256','predecessor_worker_incarnation','executable'],
                ARRAY['operation_id','worker_id','worker_incarnation'],ARRAY['bootstrap_registration_epoch','protected_registration_epoch'],
                ARRAY['slurm_job_id'],ARRAY['worker_credential_sha256']);
            IF p->'schema_version' IS DISTINCT FROM '2'::jsonb OR p->'executable' IS DISTINCT FROM 'true'::jsonb
                OR p->'bootstrap_registration_epoch' IS DISTINCT FROM '1'::jsonb
                OR p->'protected_registration_epoch' IS DISTINCT FROM '2'::jsonb
                OR p->'predecessor_worker_incarnation' IS DISTINCT FROM 'null'::jsonb THEN
                RAISE EXCEPTION 'build registration requires initial native epochs';
            END IF;
            identities := ARRAY[(p->>'operation_id')::uuid,(p->'binding'->>'intent_id')::uuid,
                (p->'binding'->>'tranche_id')::uuid,(p->>'worker_id')::uuid,(p->>'worker_incarnation')::uuid];
            IF (SELECT count(DISTINCT id) FROM unnest(identities) id) <> 5 THEN
                RAISE EXCEPTION 'build registration identities must be distinct';
            END IF;
            SELECT * INTO installation FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build registration installation is absent'; END IF;
            SELECT * INTO bootstrap FROM {SCHEMA}.bootstraps WHERE intent_id=(p->'binding'->>'intent_id')::uuid FOR UPDATE;
            IF NOT FOUND OR bootstrap.installation_id IS DISTINCT FROM p_installation
                OR bootstrap.retention_xid=pg_current_xact_id()
                OR bootstrap.payload->'proposal'->'binding' IS DISTINCT FROM p->'binding'
                OR bootstrap.payload->'proposal'->>'bootstrap_sha256' IS DISTINCT FROM bootstrap_hash
                OR bootstrap_hash IS NULL OR bootstrap_hash=p->>'worker_credential_sha256' THEN
                RAISE EXCEPTION 'build registration committed bootstrap or capability changed';
            END IF;
            SELECT * INTO physical FROM {SCHEMA}.execution_events WHERE intent_id=bootstrap.intent_id AND kind='bound' FOR UPDATE;
            IF NOT FOUND OR physical.retention_xid=pg_current_xact_id()
                OR physical.payload->'binding' IS DISTINCT FROM p->'binding'
                OR physical.slurm_job_id IS DISTINCT FROM p->>'slurm_job_id' THEN
                RAISE EXCEPTION 'build registration requires exact committed physical binding';
            END IF;
            SELECT * INTO e FROM {SCHEMA}.worker_registrations WHERE intent_id=bootstrap.intent_id;
            IF FOUND THEN
                IF e.installation_id IS DISTINCT FROM p_installation OR e.physical_event_id IS DISTINCT FROM physical.id
                    OR e.payload IS DISTINCT FROM p OR e.wire_payload IS DISTINCT FROM wire OR e.payload_sha256 IS DISTINCT FROM digest THEN
                    RAISE EXCEPTION 'build registration exact replay changed';
                END IF;
                RETURN {SCHEMA}.worker_registration_receipt(e);
            END IF;
            PERFORM {SCHEMA}.assert_bootstrap_not_revoked(bootstrap.intent_id);
            IF EXISTS (SELECT 1 FROM {SCHEMA}.terminal_inventory WHERE intent_id=bootstrap.intent_id) THEN
                RAISE EXCEPTION 'build registration physical job is already terminal';
            END IF;
            SELECT * INTO assignment FROM {SCHEMA}.assignments WHERE id=physical.assignment_id;
            IF NOT FOUND OR assignment.submission_intent_id IS DISTINCT FROM bootstrap.intent_id THEN
                RAISE EXCEPTION 'build registration assignment changed';
            END IF;
            proposal := bootstrap.payload->'proposal';
            PERFORM {SCHEMA}.assert_bootstrap(proposal,convert_to({SCHEMA}.canonical_plan_json(proposal),'UTF8'),
                bootstrap.proposal_sha256,installation.payload);
            PERFORM {SCHEMA}.authorize_publication(p_installation,assignment.plan_id);
            INSERT INTO {SCHEMA}.worker_registrations(installation_id,intent_id,assignment_id,physical_event_id,
                operation_id,worker_id,worker_incarnation,credential_sha256,payload,wire_payload,payload_sha256)
                VALUES(p_installation,bootstrap.intent_id,assignment.id,physical.id,(p->>'operation_id')::uuid,
                    (p->>'worker_id')::uuid,(p->>'worker_incarnation')::uuid,p->>'worker_credential_sha256',p,wire,digest)
                RETURNING * INTO e;
            -- Source locks and unique credential/worker indexes may have waited.
            -- Revalidate time-dependent authority after every blocking operation;
            -- failure rolls back the new credential, not the retained source hold.
            PERFORM {SCHEMA}.assert_bootstrap(proposal,convert_to({SCHEMA}.canonical_plan_json(proposal),'UTF8'),
                bootstrap.proposal_sha256,installation.payload);
            PERFORM {SCHEMA}.authorize_publication(p_installation,assignment.plan_id);
            RETURN {SCHEMA}.worker_registration_receipt(e);
        END $function$;
    """)
    _fences(install=True)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    for signature in (f"worker_registration_receipt({SCHEMA}.worker_registrations)", "observe_registered_worker(uuid,jsonb)"):
        op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{signature} FROM PUBLIC, {quote(agent)}")
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quote(agent)}")


def downgrade():
    op.execute(f"LOCK TABLE {SCHEMA}.worker_registrations IN ACCESS EXCLUSIVE MODE")
    op.execute(f"""DO $$ BEGIN IF EXISTS (SELECT 1 FROM {SCHEMA}.worker_registrations) THEN
        RAISE EXCEPTION 'cannot remove native registration with retained evidence'; END IF; END $$""")
    _fences(install=False)
    for signature in (FUNCTION, f"worker_registration_receipt({SCHEMA}.worker_registrations)", "observe_registered_worker(uuid,jsonb)"):
        op.execute(f"DROP FUNCTION {SCHEMA}.{signature}")
    op.drop_table("worker_registrations", schema=SCHEMA)
