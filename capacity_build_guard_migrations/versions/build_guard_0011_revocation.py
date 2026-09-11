"""Fence unbound native bootstraps even when preparation never succeeded.

Revision ID: build_guard_0011
Revises: build_guard_0010
"""

import sqlalchemy as sa
from alembic import op

from capacity_build_guard_migrations.versions.build_guard_0001_assignments import _payload

revision = "build_guard_0011"
down_revision = "build_guard_0010"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = "revoke_prepared_bootstrap(uuid,jsonb,bytea,text)"


def _replace_clause(signature, old, new):
    definition = op.get_bind().scalar(sa.text("SELECT pg_get_functiondef(CAST(:signature AS regprocedure))"),
        {"signature":f"{SCHEMA}.{signature}"})
    if definition.count(old) != 1:
        raise RuntimeError("native bootstrap revocation migration clause changed")
    op.execute(definition.replace(old, new))


def _fences(*, install):
    clauses = (
        ("prepare_worker(uuid,jsonb,bytea,text,text)",
            f"SELECT * INTO assignment FROM {SCHEMA}.assignments WHERE submission_intent_id=bootstrap.intent_id;",
            f"PERFORM {SCHEMA}.assert_bootstrap_not_revoked(bootstrap.intent_id);\n            "),
        ("bind_slurm_job(uuid,jsonb,bytea,text)",
            f"SELECT * INTO prepared FROM {SCHEMA}.execution_events WHERE intent_id=bootstrap.intent_id AND kind='prepared' FOR UPDATE;",
            f"PERFORM {SCHEMA}.assert_bootstrap_not_revoked(bootstrap.intent_id);\n            "),
        ("authorize_bootstrap_publication(uuid,uuid)",
            f"PERFORM {SCHEMA}.assert_bootstrap(proposal,convert_to({SCHEMA}.canonical_plan_json(proposal),'UTF8'),",
            f"PERFORM {SCHEMA}.assert_bootstrap_not_revoked(p_intent);\n            "),
        ("observe_intent(uuid,jsonb,bytea,text)",
            f"SELECT * INTO prepared FROM {SCHEMA}.execution_events\n                WHERE intent_id=bootstrap.intent_id AND kind='prepared';",
            f"IF EXISTS (SELECT 1 FROM {SCHEMA}.bootstrap_revocations WHERE intent_id=bootstrap.intent_id) THEN\n"
            f"                RETURN {SCHEMA}.observe_bootstrap_revocation(bootstrap.intent_id,p);\n"
            "            END IF;\n            "),
    )
    for signature, old, prefix in clauses:
        new = prefix + old
        _replace_clause(signature, old if install else new, new if install else old)


def upgrade():
    op.create_table("bootstrap_revocations",
        # Share the protected event high-water, without requiring an assignment
        # which may never have existed for an abandoned bootstrap.
        sa.Column("id",sa.BigInteger(),primary_key=True,
            server_default=sa.text(f"nextval(pg_get_serial_sequence('{SCHEMA}.execution_events','id')::regclass)")),
        sa.Column("intent_id",sa.Uuid(),sa.ForeignKey(f"{SCHEMA}.bootstraps.intent_id"),nullable=False,unique=True),
        sa.Column("operation_id",sa.Uuid(),nullable=False,unique=True),
        *_payload(),schema=SCHEMA)
    op.execute(f"ALTER TABLE {SCHEMA}.bootstrap_revocations ADD COLUMN retention_xid xid8 NOT NULL DEFAULT pg_current_xact_id()")
    op.execute(f"CREATE TRIGGER build_revocation_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON {SCHEMA}.bootstrap_revocations FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_evidence_mutation()")
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.assert_bootstrap_not_revoked(p_intent uuid)
        RETURNS void LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $function$
        BEGIN
            IF EXISTS (SELECT 1 FROM {SCHEMA}.bootstrap_revocations WHERE intent_id=p_intent) THEN
                RAISE EXCEPTION 'native build bootstrap is revoked';
            END IF;
        END $function$;

        CREATE FUNCTION {SCHEMA}.bootstrap_revocation_receipt(e {SCHEMA}.bootstrap_revocations)
        RETURNS text LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $function$
        DECLARE reporter uuid;
        BEGIN
            SELECT i.reporter_incarnation INTO STRICT reporter FROM {SCHEMA}.installations i
                JOIN {SCHEMA}.bootstraps b ON b.installation_id=i.id WHERE b.intent_id=e.intent_id;
            RETURN {SCHEMA}.canonical_plan_json(jsonb_build_object(
                'schema_version',2,'binding',e.payload->'binding','reporter_incarnation',reporter,
                'bootstrap_registration_epoch',1,'protected_registration_epoch',e.payload->'protected_registration_epoch',
                'claim_high_water',0,'live_claim_count',0,'bootstrap_revoked',true,
                'request_digest',e.payload_sha256,'protected_release_sha256',e.payload_sha256,
                'protected_high_water',e.id,'revocation_state','revoked','executable',true));
        END $function$;

        CREATE FUNCTION {SCHEMA}.observe_bootstrap_revocation(p_intent uuid, binding jsonb)
        RETURNS text LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $function$
        DECLARE e {SCHEMA}.bootstrap_revocations%ROWTYPE;
        BEGIN
            SELECT * INTO STRICT e FROM {SCHEMA}.bootstrap_revocations WHERE intent_id=p_intent;
            IF e.retention_xid=pg_current_xact_id() OR e.payload->'binding' IS DISTINCT FROM binding THEN
                RAISE EXCEPTION 'build observation requires exact committed revocation';
            END IF;
            RETURN {SCHEMA}.canonical_plan_json(jsonb_build_object(
                'schema_version',2,'binding',binding,'bootstrap_registration_epoch',1,
                'worker_id',NULL,'worker_incarnation',NULL,'protected_registration_epoch',0,
                'claim_high_water',0,'drain',NULL,'release',NULL,'withdrawal',NULL,
                'prepared_revocation',{SCHEMA}.bootstrap_revocation_receipt(e)::jsonb,'executable',true));
        END $function$;

        CREATE FUNCTION {SCHEMA}.revoke_prepared_bootstrap(p_installation uuid, p jsonb, wire bytea, digest text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE bootstrap {SCHEMA}.bootstraps%ROWTYPE; e {SCHEMA}.bootstrap_revocations%ROWTYPE;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build bootstrap revocation requires serializable transaction';
            END IF;
            IF wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR encode(sha256(wire),'hex') IS DISTINCT FROM digest OR octet_length(wire) NOT BETWEEN 2 AND 1048576 THEN
                RAISE EXCEPTION 'build revocation canonical request changed';
            END IF;
            PERFORM {SCHEMA}.assert_plan_fields(p,
                ARRAY['schema_version','operation_id','binding','bootstrap_registration_epoch','protected_registration_epoch','expected_claim_high_water','executable'],
                ARRAY['operation_id'],ARRAY['bootstrap_registration_epoch','protected_registration_epoch'],ARRAY[]::text[],ARRAY[]::text[]);
            PERFORM 1 FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build revocation installation is absent'; END IF;
            SELECT * INTO bootstrap FROM {SCHEMA}.bootstraps WHERE intent_id=(p->'binding'->>'intent_id')::uuid FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build revocation bootstrap is absent'; END IF;
            IF bootstrap.installation_id IS DISTINCT FROM p_installation
                OR bootstrap.retention_xid=pg_current_xact_id()
                OR p->'binding' IS DISTINCT FROM bootstrap.payload->'proposal'->'binding'
                OR p->'bootstrap_registration_epoch' IS DISTINCT FROM '1'::jsonb
                OR (p->>'protected_registration_epoch')::bigint <= 1
                OR p->'expected_claim_high_water' IS DISTINCT FROM '0'::jsonb
                OR p->'executable' IS DISTINCT FROM 'true'::jsonb THEN
                RAISE EXCEPTION 'build revocation committed bootstrap binding changed';
            END IF;
            SELECT * INTO e FROM {SCHEMA}.bootstrap_revocations WHERE intent_id=bootstrap.intent_id;
            IF FOUND THEN
                IF e.payload IS DISTINCT FROM p OR e.wire_payload IS DISTINCT FROM wire OR e.payload_sha256 IS DISTINCT FROM digest THEN
                    RAISE EXCEPTION 'build revocation exact replay changed';
                END IF;
                RETURN {SCHEMA}.bootstrap_revocation_receipt(e);
            END IF;
            IF EXISTS (SELECT 1 FROM {SCHEMA}.execution_events WHERE intent_id=bootstrap.intent_id AND kind='bound') THEN
                RAISE EXCEPTION 'build bootstrap revocation requires unbound evidence';
            END IF;
            INSERT INTO {SCHEMA}.bootstrap_revocations(intent_id,operation_id,payload,wire_payload,payload_sha256)
                VALUES(bootstrap.intent_id,(p->>'operation_id')::uuid,p,wire,digest) RETURNING * INTO e;
            RETURN {SCHEMA}.bootstrap_revocation_receipt(e);
        END $function$;
    """)
    _fences(install=True)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    for helper in ("assert_bootstrap_not_revoked(uuid)", f"bootstrap_revocation_receipt({SCHEMA}.bootstrap_revocations)",
        "observe_bootstrap_revocation(uuid,jsonb)"):
        op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{helper} FROM PUBLIC, {quote(agent)}")
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quote(agent)}")


def downgrade():
    op.execute(f"LOCK TABLE {SCHEMA}.bootstrap_revocations IN ACCESS EXCLUSIVE MODE")
    op.execute(f"""DO $$ BEGIN IF EXISTS (SELECT 1 FROM {SCHEMA}.bootstrap_revocations) THEN
        RAISE EXCEPTION 'cannot remove build revocation with retained evidence'; END IF; END $$""")
    _fences(install=False)
    for signature in (FUNCTION, "observe_bootstrap_revocation(uuid,jsonb)",
        f"bootstrap_revocation_receipt({SCHEMA}.bootstrap_revocations)", "assert_bootstrap_not_revoked(uuid)"):
        op.execute(f"DROP FUNCTION {SCHEMA}.{signature}")
    op.drop_table("bootstrap_revocations",schema=SCHEMA)
