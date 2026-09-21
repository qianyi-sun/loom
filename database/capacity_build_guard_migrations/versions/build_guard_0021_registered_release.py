"""Release registered native workers and retire exact physically released holds.

Revision ID: build_guard_0021
Revises: build_guard_0020
"""

import sqlalchemy as sa
from alembic import op

from capacity_build_guard_migrations.versions.build_guard_0001_assignments import _payload
from capacity_build_guard_migrations.versions.build_guard_0011_revocation import _replace_clause

revision = "build_guard_0021"
down_revision = "build_guard_0020"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
CALLABLES = ("acknowledge_release(uuid,jsonb,bytea,text,text)", "release_terminal_worker(uuid,jsonb,bytea,text,text)")
HELPERS = (f"native_release_receipt({SCHEMA}.worker_releases)", "native_worker_release(uuid)", "native_release(uuid,jsonb,bytea,text,text,text)")


def _consumers(*, install):
    for signature, columns in (
        ("protected_release_publication(uuid,bigint)", "id,intent_id,payload,payload_sha256,retention_xid,'withdrawn' AS kind"),
        ("read_next_protected_release(uuid)", "id,intent_id"),
        ("retire_request_hold(uuid,jsonb,bytea,text)", "id,intent_id,retention_xid,'withdrawn' AS kind"),
        ("read_pending_retirements(uuid,bigint,bigint,integer)", "id,intent_id"),
    ):
        old = f"UNION ALL SELECT {columns} FROM {SCHEMA}.worker_withdrawals"
        released_columns = columns.replace("'withdrawn'", "'released'")
        new = old + f"\n                UNION ALL SELECT {released_columns} FROM {SCHEMA}.worker_releases"
        _replace_clause(signature, old if install else new, new if install else old)
    for signature, old, new in (
        ("observe_registered_worker(uuid,jsonb)", "'release',NULL,", f"'release',{SCHEMA}.native_worker_release(p_intent),"),
        ("assert_bootstrap_not_revoked(uuid)", f"OR EXISTS (SELECT 1 FROM {SCHEMA}.worker_withdrawals WHERE intent_id=p_intent) THEN",
            f"OR EXISTS (SELECT 1 FROM {SCHEMA}.worker_withdrawals WHERE intent_id=p_intent) OR EXISTS (SELECT 1 FROM {SCHEMA}.worker_releases WHERE intent_id=p_intent) THEN"),
    ):
        _replace_clause(signature, old if install else new, new if install else old)


def upgrade():
    op.create_table("worker_releases",
        sa.Column("id", sa.BigInteger(), primary_key=True,
            server_default=sa.text(f"nextval(pg_get_serial_sequence('{SCHEMA}.execution_events','id')::regclass)")),
        sa.Column("installation_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.installations.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("intent_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.bootstraps.intent_id", ondelete="RESTRICT"), nullable=False, unique=True),
        sa.Column("registration_id", sa.BigInteger(), sa.ForeignKey(f"{SCHEMA}.worker_registrations.id", ondelete="RESTRICT"), nullable=False, unique=True),
        sa.Column("drain_id", sa.BigInteger(), sa.ForeignKey(f"{SCHEMA}.worker_drains.id", ondelete="RESTRICT"), nullable=False, unique=True),
        sa.Column("terminal_id", sa.BigInteger(), sa.ForeignKey(f"{SCHEMA}.terminal_inventory.id", ondelete="RESTRICT"), nullable=True),
        sa.Column("operation_id", sa.Uuid(), nullable=False, unique=True),
        *_payload(), schema=SCHEMA)
    op.execute(f"ALTER TABLE {SCHEMA}.worker_releases ADD COLUMN retention_xid xid8 NOT NULL DEFAULT pg_current_xact_id()")
    op.execute(f"CREATE TRIGGER native_release_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON {SCHEMA}.worker_releases FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_evidence_mutation()")
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.native_release_receipt(e {SCHEMA}.worker_releases)
        RETURNS text LANGUAGE sql SECURITY INVOKER SET search_path=pg_catalog AS $$
            SELECT {SCHEMA}.canonical_plan_json(jsonb_build_object('schema_version',2,
                'binding',e.payload->'binding','reporter_incarnation',e.payload->'reporter_incarnation',
                'bootstrap_registration_epoch',1,'protected_registration_epoch',2,
                'claim_high_water',e.payload->'expected_claim_high_water','live_claim_count',0,'release_epoch',4,
                'bootstrap_revoked',true,'worker_credentials_revoked',true,'request_digest',e.payload_sha256,
                'protected_release_sha256',e.payload_sha256,'protected_high_water',e.id,'release_state','acknowledged','executable',true))
        $$;

        CREATE FUNCTION {SCHEMA}.native_worker_release(p_intent uuid)
        RETURNS jsonb LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
        DECLARE e {SCHEMA}.worker_releases%ROWTYPE;
        BEGIN
            SELECT * INTO e FROM {SCHEMA}.worker_releases WHERE intent_id=p_intent;
            IF NOT FOUND THEN RETURN NULL; END IF;
            IF e.retention_xid=pg_current_xact_id() THEN RAISE EXCEPTION 'native observation requires committed release'; END IF;
            RETURN {SCHEMA}.native_release_receipt(e)::jsonb;
        END $$;

        CREATE FUNCTION {SCHEMA}.native_release(p_installation uuid,p jsonb,wire bytea,digest text,credential_hash text,terminal_digest text)
        RETURNS text LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $function$
        DECLARE installation {SCHEMA}.installations%ROWTYPE; bootstrap {SCHEMA}.bootstraps%ROWTYPE;
            physical {SCHEMA}.execution_events%ROWTYPE; registration {SCHEMA}.worker_registrations%ROWTYPE;
            drain {SCHEMA}.worker_drains%ROWTYPE; terminal {SCHEMA}.terminal_inventory%ROWTYPE;
            e {SCHEMA}.worker_releases%ROWTYPE; high_water bigint;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'native release requires serializable transaction'; END IF;
            IF wire IS NULL OR octet_length(wire) NOT BETWEEN 2 AND 1048576
                OR wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR digest IS DISTINCT FROM encode(sha256(wire),'hex') THEN
                RAISE EXCEPTION 'native release canonical request changed'; END IF;
            PERFORM {SCHEMA}.assert_plan_fields(p,
                ARRAY['schema_version','operation_id','binding','reporter_incarnation','bootstrap_registration_epoch',
                    'expected_claim_high_water','protected_registration_epoch','release_epoch','executable'],
                ARRAY['operation_id','reporter_incarnation'],ARRAY['bootstrap_registration_epoch','protected_registration_epoch','release_epoch'],
                ARRAY[]::text[],ARRAY[]::text[]);
            IF p->'executable' IS DISTINCT FROM 'true'::jsonb OR p->'bootstrap_registration_epoch' IS DISTINCT FROM '1'::jsonb
                OR p->'protected_registration_epoch' IS DISTINCT FROM '2'::jsonb OR p->'release_epoch' IS DISTINCT FROM '4'::jsonb
                OR jsonb_typeof(p->'expected_claim_high_water') IS DISTINCT FROM 'number'
                OR (p->>'expected_claim_high_water') !~ '^(0|1)$'
                OR (credential_hash IS NULL)=(terminal_digest IS NULL) THEN
                RAISE EXCEPTION 'native release epochs or authentication changed'; END IF;
            SELECT * INTO installation FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND OR installation.reporter_incarnation::text IS DISTINCT FROM p->>'reporter_incarnation' THEN
                RAISE EXCEPTION 'native release installation reporter changed'; END IF;
            SELECT * INTO bootstrap FROM {SCHEMA}.bootstraps WHERE intent_id=(p->'binding'->>'intent_id')::uuid FOR UPDATE;
            IF NOT FOUND OR bootstrap.installation_id IS DISTINCT FROM p_installation OR bootstrap.retention_xid=pg_current_xact_id()
                OR bootstrap.payload->'proposal'->'binding' IS DISTINCT FROM p->'binding' THEN
                RAISE EXCEPTION 'native release bootstrap binding changed'; END IF;
            SELECT * INTO physical FROM {SCHEMA}.execution_events WHERE intent_id=bootstrap.intent_id AND kind='bound' FOR UPDATE;
            IF NOT FOUND OR physical.retention_xid=pg_current_xact_id() THEN
                RAISE EXCEPTION 'native release physical binding absent'; END IF;
            SELECT * INTO registration FROM {SCHEMA}.worker_registrations WHERE intent_id=bootstrap.intent_id FOR UPDATE;
            IF NOT FOUND OR registration.retention_xid=pg_current_xact_id()
                OR registration.installation_id IS DISTINCT FROM p_installation OR registration.physical_event_id IS DISTINCT FROM physical.id
                OR registration.payload->'binding' IS DISTINCT FROM p->'binding' THEN
                RAISE EXCEPTION 'native release requires exact committed worker'; END IF;
            IF credential_hash IS NOT NULL THEN
                IF registration.credential_sha256 IS DISTINCT FROM credential_hash THEN
                    RAISE EXCEPTION 'native release worker credential changed'; END IF;
            ELSE
                SELECT * INTO terminal FROM {SCHEMA}.terminal_inventory WHERE intent_id=bootstrap.intent_id;
                IF NOT FOUND OR terminal.retention_xid=pg_current_xact_id() OR terminal.installation_id IS DISTINCT FROM p_installation
                    OR terminal.assignment_id IS DISTINCT FROM registration.assignment_id
                    OR terminal.payload->'binding' IS DISTINCT FROM p->'binding'
                    OR terminal.payload->'record'->>'physical_identity' IS DISTINCT FROM physical.slurm_job_id
                    OR terminal.payload_sha256 IS DISTINCT FROM terminal_digest THEN
                    RAISE EXCEPTION 'native release requires exact committed terminal inventory'; END IF;
            END IF;
            SELECT * INTO e FROM {SCHEMA}.worker_releases WHERE registration_id=registration.id;
            IF FOUND THEN
                IF e.payload IS DISTINCT FROM p OR e.wire_payload IS DISTINCT FROM wire OR e.payload_sha256 IS DISTINCT FROM digest THEN
                    RAISE EXCEPTION 'native release exact replay changed'; END IF;
                RETURN {SCHEMA}.native_release_receipt(e);
            END IF;
            SELECT * INTO drain FROM {SCHEMA}.worker_drains WHERE registration_id=registration.id FOR UPDATE;
            IF NOT FOUND OR drain.retention_xid=pg_current_xact_id() OR drain.installation_id IS DISTINCT FROM p_installation
                OR drain.payload->'binding' IS DISTINCT FROM p->'binding' THEN
                RAISE EXCEPTION 'native release requires exact committed drain'; END IF;
            high_water := {SCHEMA}.native_claim_high_water(bootstrap.intent_id);
            IF to_jsonb(high_water) IS DISTINCT FROM p->'expected_claim_high_water'
                OR to_jsonb(high_water) IS DISTINCT FROM drain.payload->'expected_claim_high_water'
                OR {SCHEMA}.native_live_claim_count(bootstrap.intent_id) <> 0 THEN
                RAISE EXCEPTION 'native release requires zero live claims and exact high-water'; END IF;
            INSERT INTO {SCHEMA}.worker_releases(installation_id,intent_id,registration_id,drain_id,terminal_id,operation_id,payload,wire_payload,payload_sha256)
                VALUES(p_installation,bootstrap.intent_id,registration.id,drain.id,terminal.id,(p->>'operation_id')::uuid,p,wire,digest) RETURNING * INTO e;
            RETURN {SCHEMA}.native_release_receipt(e);
        END $function$;

        CREATE FUNCTION {SCHEMA}.acknowledge_release(p_installation uuid,p jsonb,wire bytea,digest text,credential_hash text)
        RETURNS text LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $$
            SELECT {SCHEMA}.native_release(p_installation,p,wire,digest,credential_hash,NULL)
        $$;
        CREATE FUNCTION {SCHEMA}.release_terminal_worker(p_installation uuid,p jsonb,wire bytea,digest text,terminal_digest text)
        RETURNS text LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $$
            SELECT {SCHEMA}.native_release(p_installation,p,wire,digest,NULL,terminal_digest)
        $$;
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
    op.execute(f"LOCK TABLE {SCHEMA}.worker_releases IN ACCESS EXCLUSIVE MODE")
    op.execute(f"""DO $$ BEGIN IF EXISTS (SELECT 1 FROM {SCHEMA}.worker_releases) THEN
        RAISE EXCEPTION 'cannot remove native release with retained evidence'; END IF; END $$""")
    _consumers(install=False)
    for signature in (*CALLABLES, *HELPERS):
        op.execute(f"DROP FUNCTION {SCHEMA}.{signature}")
    op.drop_table("worker_releases", schema=SCHEMA)
