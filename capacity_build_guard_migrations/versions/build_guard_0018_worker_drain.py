"""Fence new native claims while retaining registered work and capacity charges.

Revision ID: build_guard_0018
Revises: build_guard_0017
"""

import sqlalchemy as sa
from alembic import op

from capacity_build_guard_migrations.versions.build_guard_0001_assignments import _payload
from capacity_build_guard_migrations.versions.build_guard_0011_revocation import _replace_clause

revision = "build_guard_0018"
down_revision = "build_guard_0017"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = "begin_drain(uuid,jsonb,bytea,text)"


def _consumers(*, install):
    for signature, old, new in (
        ("observe_registered_worker(uuid,jsonb)", "'drain',NULL,",
            f"'drain',{SCHEMA}.native_worker_drain(p_intent),"),
        ("fixed_native_claims(uuid)", "WHEN e.cancelled_at IS NOT NULL THEN 'cancel-pending'",
            f"WHEN e.cancelled_at IS NOT NULL OR EXISTS (SELECT 1 FROM {SCHEMA}.worker_drains d WHERE d.registration_id=e.registration_id) THEN 'cancel-pending'"),
        ("claim_platform(uuid,jsonb,bytea,text,text)",
            f"PERFORM {SCHEMA}.assert_bootstrap_not_revoked(bootstrap.intent_id);",
            f"IF EXISTS (SELECT 1 FROM {SCHEMA}.worker_drains WHERE registration_id=registration.id) THEN\n"
            "                RAISE EXCEPTION 'native worker is draining'; END IF;\n            "
            f"PERFORM {SCHEMA}.assert_bootstrap_not_revoked(bootstrap.intent_id);"),
    ):
        _replace_clause(signature, old if install else new, new if install else old)


def upgrade():
    op.create_table("worker_drains",
        sa.Column("id", sa.BigInteger(), primary_key=True,
            server_default=sa.text(f"nextval(pg_get_serial_sequence('{SCHEMA}.execution_events','id')::regclass)")),
        sa.Column("installation_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.installations.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("registration_id", sa.BigInteger(), sa.ForeignKey(f"{SCHEMA}.worker_registrations.id", ondelete="RESTRICT"), nullable=False, unique=True),
        sa.Column("operation_id", sa.Uuid(), nullable=False, unique=True),
        *_payload(), schema=SCHEMA)
    op.execute(f"ALTER TABLE {SCHEMA}.worker_drains ADD COLUMN retention_xid xid8 NOT NULL DEFAULT pg_current_xact_id()")
    op.execute(f"CREATE TRIGGER build_drain_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON {SCHEMA}.worker_drains FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_evidence_mutation()")
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.native_drain_receipt(e {SCHEMA}.worker_drains)
        RETURNS text LANGUAGE sql SECURITY INVOKER SET search_path=pg_catalog AS $$
            SELECT {SCHEMA}.canonical_plan_json(jsonb_build_object('schema_version',2,
                'subject_id',e.payload->'binding'->'subject_id','subject_incarnation',e.payload->'binding'->'subject_incarnation',
                'intent_id',e.payload->'binding'->'intent_id','worker_id',e.payload->'worker_id',
                'worker_incarnation',e.payload->'worker_incarnation','claim_high_water',e.payload->'expected_claim_high_water',
                'live_claim_count',e.payload->'expected_claim_high_water','drain_epoch',3,
                'request_digest',e.payload_sha256,'drain_digest',e.payload_sha256,
                'protected_high_water',e.id,'worker_state','draining','executable',true))
        $$;

        CREATE FUNCTION {SCHEMA}.native_worker_drain(p_intent uuid)
        RETURNS jsonb LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
        DECLARE e {SCHEMA}.worker_drains%ROWTYPE;
        BEGIN
            SELECT d.* INTO e FROM {SCHEMA}.worker_drains d
                JOIN {SCHEMA}.worker_registrations r ON r.id=d.registration_id WHERE r.intent_id=p_intent;
            IF NOT FOUND THEN RETURN NULL; END IF;
            IF e.retention_xid=pg_current_xact_id() THEN
                RAISE EXCEPTION 'native observation requires committed drain'; END IF;
            RETURN {SCHEMA}.native_drain_receipt(e)::jsonb;
        END $$;

        CREATE FUNCTION {SCHEMA}.begin_drain(p_installation uuid,p jsonb,wire bytea,digest text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE installation {SCHEMA}.installations%ROWTYPE; bootstrap {SCHEMA}.bootstraps%ROWTYPE;
            physical {SCHEMA}.execution_events%ROWTYPE; registration {SCHEMA}.worker_registrations%ROWTYPE;
            e {SCHEMA}.worker_drains%ROWTYPE; high_water bigint;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'native drain requires serializable transaction'; END IF;
            IF wire IS NULL OR octet_length(wire) NOT BETWEEN 2 AND 1048576
                OR wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR digest IS DISTINCT FROM encode(sha256(wire),'hex') THEN
                RAISE EXCEPTION 'native drain canonical request changed'; END IF;
            PERFORM {SCHEMA}.assert_plan_fields(p,
                ARRAY['schema_version','binding','operation_id','worker_id','worker_incarnation','expected_claim_high_water','drain_epoch','executable'],
                ARRAY['operation_id','worker_id','worker_incarnation'],ARRAY['drain_epoch'],ARRAY[]::text[],ARRAY[]::text[]);
            IF p->'executable' IS DISTINCT FROM 'true'::jsonb OR p->'drain_epoch' IS DISTINCT FROM '3'::jsonb
                OR jsonb_typeof(p->'expected_claim_high_water') IS DISTINCT FROM 'number'
                OR (p->>'expected_claim_high_water') !~ '^(0|1)$' THEN
                RAISE EXCEPTION 'native drain epoch or claim high-water changed'; END IF;
            SELECT * INTO installation FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'native drain installation is absent'; END IF;
            SELECT * INTO bootstrap FROM {SCHEMA}.bootstraps WHERE intent_id=(p->'binding'->>'intent_id')::uuid FOR UPDATE;
            IF NOT FOUND OR bootstrap.installation_id IS DISTINCT FROM p_installation
                OR bootstrap.payload->'proposal'->'binding' IS DISTINCT FROM p->'binding' THEN
                RAISE EXCEPTION 'native drain bootstrap binding changed'; END IF;
            SELECT * INTO physical FROM {SCHEMA}.execution_events WHERE intent_id=bootstrap.intent_id AND kind='bound' FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'native drain physical binding absent'; END IF;
            SELECT * INTO registration FROM {SCHEMA}.worker_registrations WHERE intent_id=bootstrap.intent_id FOR UPDATE;
            IF NOT FOUND OR registration.retention_xid=pg_current_xact_id()
                OR registration.installation_id IS DISTINCT FROM p_installation
                OR registration.physical_event_id IS DISTINCT FROM physical.id
                OR registration.payload->'binding' IS DISTINCT FROM p->'binding'
                OR registration.worker_id::text IS DISTINCT FROM p->>'worker_id'
                OR registration.worker_incarnation::text IS DISTINCT FROM p->>'worker_incarnation' THEN
                RAISE EXCEPTION 'native drain requires exact committed worker'; END IF;
            SELECT * INTO e FROM {SCHEMA}.worker_drains WHERE registration_id=registration.id;
            IF FOUND THEN
                IF e.payload IS DISTINCT FROM p OR e.wire_payload IS DISTINCT FROM wire OR e.payload_sha256 IS DISTINCT FROM digest THEN
                    RAISE EXCEPTION 'native drain exact replay changed'; END IF;
                RETURN {SCHEMA}.native_drain_receipt(e);
            END IF;
            high_water := {SCHEMA}.native_claim_high_water(bootstrap.intent_id);
            IF p->'expected_claim_high_water' IS DISTINCT FROM to_jsonb(high_water) THEN
                RAISE EXCEPTION 'native drain observed claim high-water changed'; END IF;
            INSERT INTO {SCHEMA}.worker_drains(installation_id,registration_id,operation_id,payload,wire_payload,payload_sha256)
                VALUES(p_installation,registration.id,(p->>'operation_id')::uuid,p,wire,digest) RETURNING * INTO e;
            RETURN {SCHEMA}.native_drain_receipt(e);
        END $function$;
    """)
    _consumers(install=True)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    for signature in (f"native_drain_receipt({SCHEMA}.worker_drains)", "native_worker_drain(uuid)"):
        op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{signature} FROM PUBLIC, {quote(agent)}")
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quote(agent)}")


def downgrade():
    op.execute(f"LOCK TABLE {SCHEMA}.worker_drains IN ACCESS EXCLUSIVE MODE")
    op.execute(f"""DO $$ BEGIN IF EXISTS (SELECT 1 FROM {SCHEMA}.worker_drains) THEN
        RAISE EXCEPTION 'cannot remove native drains with retained evidence'; END IF; END $$""")
    _consumers(install=False)
    for signature in (FUNCTION, f"native_drain_receipt({SCHEMA}.worker_drains)", "native_worker_drain(uuid)"):
        op.execute(f"DROP FUNCTION {SCHEMA}.{signature}")
    op.drop_table("worker_drains", schema=SCHEMA)
