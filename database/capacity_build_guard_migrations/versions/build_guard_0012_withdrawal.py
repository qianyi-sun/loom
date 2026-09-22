"""Revoke physically bound native bootstraps without declaring jobs terminal.

Revision ID: build_guard_0012
Revises: build_guard_0011
"""

import sqlalchemy as sa
from alembic import op

from capacity_build_guard_migrations.versions.build_guard_0001_assignments import _payload
from capacity_build_guard_migrations.versions.build_guard_0011_revocation import _replace_clause

revision = "build_guard_0012"
down_revision = "build_guard_0011"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = "withdraw_unregistered_worker(uuid,jsonb,bytea,text)"


def _fences(*, install):
    old = f"IF EXISTS (SELECT 1 FROM {SCHEMA}.bootstrap_revocations WHERE intent_id=p_intent) THEN"
    new = old.replace(" THEN", f" OR EXISTS (SELECT 1 FROM {SCHEMA}.worker_withdrawals WHERE intent_id=p_intent) THEN")
    _replace_clause("assert_bootstrap_not_revoked(uuid)", old if install else new, new if install else old)
    old = f"IF EXISTS (SELECT 1 FROM {SCHEMA}.bootstrap_revocations WHERE intent_id=bootstrap.intent_id) THEN"
    new = (f"IF EXISTS (SELECT 1 FROM {SCHEMA}.worker_withdrawals WHERE intent_id=bootstrap.intent_id) THEN\n"
        f"                RETURN {SCHEMA}.observe_worker_withdrawal(bootstrap.intent_id,p);\n"
        "            END IF;\n            " + old)
    _replace_clause("observe_intent(uuid,jsonb,bytea,text)", old if install else new, new if install else old)


def upgrade():
    op.create_table("worker_withdrawals",
        sa.Column("id",sa.BigInteger(),primary_key=True,
            server_default=sa.text(f"nextval(pg_get_serial_sequence('{SCHEMA}.execution_events','id')::regclass)")),
        sa.Column("intent_id",sa.Uuid(),sa.ForeignKey(f"{SCHEMA}.bootstraps.intent_id"),nullable=False,unique=True),
        sa.Column("operation_id",sa.Uuid(),nullable=False,unique=True),
        *_payload(),schema=SCHEMA)
    op.execute(f"ALTER TABLE {SCHEMA}.worker_withdrawals ADD COLUMN retention_xid xid8 NOT NULL DEFAULT pg_current_xact_id()")
    op.execute(f"CREATE TRIGGER build_withdrawal_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON {SCHEMA}.worker_withdrawals FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_evidence_mutation()")
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.worker_withdrawal_receipt(e {SCHEMA}.worker_withdrawals)
        RETURNS text LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $function$
        BEGIN
            RETURN {SCHEMA}.canonical_plan_json(jsonb_build_object(
                'schema_version',2,'subject_id',e.payload->'binding'->'subject_id',
                'subject_incarnation',e.payload->'binding'->'subject_incarnation','intent_id',e.intent_id,
                'bootstrap_registration_epoch',1,'protected_registration_epoch',e.payload->'protected_registration_epoch',
                'slurm_job_id',e.payload->'slurm_job_id','ownership_evidence_sha256',e.payload->'ownership_evidence_sha256',
                'claim_high_water',0,'live_claim_count',0,'bootstrap_revoked',true,
                'request_digest',e.payload_sha256,'withdrawal_digest',e.payload_sha256,
                'protected_high_water',e.id,'withdrawal_state','withdrawn','executable',true));
        END $function$;

        CREATE FUNCTION {SCHEMA}.observe_worker_withdrawal(p_intent uuid, binding jsonb)
        RETURNS text LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $function$
        DECLARE e {SCHEMA}.worker_withdrawals%ROWTYPE;
        BEGIN
            SELECT * INTO STRICT e FROM {SCHEMA}.worker_withdrawals WHERE intent_id=p_intent;
            IF e.retention_xid=pg_current_xact_id() OR e.payload->'binding' IS DISTINCT FROM binding THEN
                RAISE EXCEPTION 'build observation requires exact committed withdrawal';
            END IF;
            RETURN {SCHEMA}.canonical_plan_json(jsonb_build_object(
                'schema_version',2,'binding',binding,'bootstrap_registration_epoch',1,
                'worker_id',NULL,'worker_incarnation',NULL,'protected_registration_epoch',0,
                'claim_high_water',0,'drain',NULL,'release',NULL,
                'withdrawal',{SCHEMA}.worker_withdrawal_receipt(e)::jsonb,
                'prepared_revocation',NULL,'executable',true));
        END $function$;

        CREATE FUNCTION {SCHEMA}.withdraw_unregistered_worker(p_installation uuid, p jsonb, wire bytea, digest text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE bootstrap {SCHEMA}.bootstraps%ROWTYPE; physical {SCHEMA}.execution_events%ROWTYPE;
            e {SCHEMA}.worker_withdrawals%ROWTYPE;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build withdrawal requires serializable transaction';
            END IF;
            IF wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR encode(sha256(wire),'hex') IS DISTINCT FROM digest OR octet_length(wire) NOT BETWEEN 2 AND 1048576 THEN
                RAISE EXCEPTION 'build withdrawal canonical request changed';
            END IF;
            PERFORM {SCHEMA}.assert_plan_fields(p,
                ARRAY['schema_version','operation_id','binding','bootstrap_registration_epoch','protected_registration_epoch',
                    'slurm_job_id','ownership_evidence_sha256','expected_claim_high_water','executable'],
                ARRAY['operation_id'],ARRAY['bootstrap_registration_epoch','protected_registration_epoch'],
                ARRAY['slurm_job_id'],ARRAY['ownership_evidence_sha256']);
            PERFORM 1 FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build withdrawal installation is absent'; END IF;
            SELECT * INTO bootstrap FROM {SCHEMA}.bootstraps WHERE intent_id=(p->'binding'->>'intent_id')::uuid FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build withdrawal bootstrap is absent'; END IF;
            IF bootstrap.installation_id IS DISTINCT FROM p_installation
                OR bootstrap.retention_xid=pg_current_xact_id()
                OR p->'binding' IS DISTINCT FROM bootstrap.payload->'proposal'->'binding'
                OR p->'bootstrap_registration_epoch' IS DISTINCT FROM '1'::jsonb
                OR (p->>'protected_registration_epoch')::bigint <= 1
                OR p->'expected_claim_high_water' IS DISTINCT FROM '0'::jsonb
                OR p->'executable' IS DISTINCT FROM 'true'::jsonb THEN
                RAISE EXCEPTION 'build withdrawal committed bootstrap binding changed';
            END IF;
            SELECT * INTO physical FROM {SCHEMA}.execution_events
                WHERE intent_id=bootstrap.intent_id AND kind='bound' FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build withdrawal physical binding is absent'; END IF;
            IF physical.retention_xid=pg_current_xact_id() THEN
                RAISE EXCEPTION 'build withdrawal requires committed physical binding';
            END IF;
            IF physical.payload->'binding' IS DISTINCT FROM p->'binding'
                OR physical.slurm_job_id IS DISTINCT FROM p->>'slurm_job_id'
                OR physical.payload->'ownership_evidence_sha256' IS DISTINCT FROM p->'ownership_evidence_sha256' THEN
                RAISE EXCEPTION 'build withdrawal physical identity changed';
            END IF;
            SELECT * INTO e FROM {SCHEMA}.worker_withdrawals WHERE intent_id=bootstrap.intent_id;
            IF FOUND THEN
                IF e.payload IS DISTINCT FROM p OR e.wire_payload IS DISTINCT FROM wire OR e.payload_sha256 IS DISTINCT FROM digest THEN
                    RAISE EXCEPTION 'build withdrawal exact replay changed';
                END IF;
                RETURN {SCHEMA}.worker_withdrawal_receipt(e);
            END IF;
            PERFORM {SCHEMA}.assert_bootstrap_not_revoked(bootstrap.intent_id);
            -- No native worker exchange or claim exists in this schema revision.
            -- Its future installation must fence registration under these locks.
            -- This event revokes bootstrap only: no terminal proof or hold release.
            INSERT INTO {SCHEMA}.worker_withdrawals(intent_id,operation_id,payload,wire_payload,payload_sha256)
                VALUES(bootstrap.intent_id,(p->>'operation_id')::uuid,p,wire,digest) RETURNING * INTO e;
            RETURN {SCHEMA}.worker_withdrawal_receipt(e);
        END $function$;
    """)
    _fences(install=True)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    for helper in (f"worker_withdrawal_receipt({SCHEMA}.worker_withdrawals)", "observe_worker_withdrawal(uuid,jsonb)"):
        op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{helper} FROM PUBLIC, {quote(agent)}")
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quote(agent)}")


def downgrade():
    op.execute(f"LOCK TABLE {SCHEMA}.worker_withdrawals IN ACCESS EXCLUSIVE MODE")
    op.execute(f"""DO $$ BEGIN IF EXISTS (SELECT 1 FROM {SCHEMA}.worker_withdrawals) THEN
        RAISE EXCEPTION 'cannot remove build withdrawal with retained evidence'; END IF; END $$""")
    _fences(install=False)
    for signature in (FUNCTION, "observe_worker_withdrawal(uuid,jsonb)",
        f"worker_withdrawal_receipt({SCHEMA}.worker_withdrawals)"):
        op.execute(f"DROP FUNCTION {SCHEMA}.{signature}")
    op.drop_table("worker_withdrawals",schema=SCHEMA)
