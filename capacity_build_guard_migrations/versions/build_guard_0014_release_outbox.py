"""Publish committed native revocations without deleting capacity holds.

Revision ID: build_guard_0014
Revises: build_guard_0013
"""

import sqlalchemy as sa
from alembic import op

from capacity_build_guard_migrations.versions.build_guard_0001_assignments import _payload

revision = "build_guard_0014"
down_revision = "build_guard_0013"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTIONS = ("read_next_protected_release(uuid)", "acknowledge_protected_release(uuid,jsonb,bytea,text,text)")


def upgrade():
    op.create_table("release_publication_receipts",
        sa.Column("event_id", sa.BigInteger(), primary_key=True),
        sa.Column("installation_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.installations.id"), nullable=False),
        *_payload(), schema=SCHEMA)
    op.execute(f"ALTER TABLE {SCHEMA}.release_publication_receipts ADD COLUMN retention_xid xid8 NOT NULL DEFAULT pg_current_xact_id()")
    op.execute(f"CREATE TRIGGER build_release_publication_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON {SCHEMA}.release_publication_receipts FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_evidence_mutation()")
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.protected_release_publication(p_installation uuid,p_event bigint)
        RETURNS text LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $function$
        DECLARE installation {SCHEMA}.installations%ROWTYPE; event record; release jsonb; wire bytea;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build release outbox requires serializable transaction';
            END IF;
            SELECT * INTO installation FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build release installation is absent'; END IF;
            SELECT e.* INTO event FROM (
                SELECT id,intent_id,payload,payload_sha256,retention_xid,'prepared-revoked' AS kind FROM {SCHEMA}.bootstrap_revocations
                UNION ALL SELECT id,intent_id,payload,payload_sha256,retention_xid,'withdrawn' AS kind FROM {SCHEMA}.worker_withdrawals
            ) e JOIN {SCHEMA}.bootstraps b ON b.intent_id=e.intent_id
                WHERE e.id=p_event AND b.installation_id=p_installation;
            IF NOT FOUND THEN RAISE EXCEPTION 'build release event is absent from installation'; END IF;
            IF event.retention_xid=pg_current_xact_id() THEN
                RAISE EXCEPTION 'build release requires committed revocation';
            END IF;
            release := jsonb_build_object('schema_version',2,'binding',event.payload->'binding',
                'reporter_incarnation',installation.reporter_incarnation,'bootstrap_registration_epoch',1,
                'protected_registration_epoch',event.payload->'protected_registration_epoch',
                'bootstrap_revoked',true,'protected_release_sha256',event.payload_sha256,'executable',true);
            wire := convert_to({SCHEMA}.canonical_plan_json(release),'UTF8');
            RETURN {SCHEMA}.canonical_plan_json(jsonb_build_object('schema_version',2,
                'event_id',event.id,'event_kind',event.kind,'release',release,'publication_digest',encode(sha256(wire),'hex')));
        END $function$;

        CREATE FUNCTION {SCHEMA}.read_next_protected_release(p_installation uuid)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE next_event bigint;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build release outbox requires serializable transaction';
            END IF;
            PERFORM 1 FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build release installation is absent'; END IF;
            IF EXISTS (SELECT 1 FROM {SCHEMA}.release_publication_receipts
                WHERE installation_id=p_installation AND retention_xid=pg_current_xact_id()) THEN
                RAISE EXCEPTION 'build release cursor requires committed acknowledgement';
            END IF;
            SELECT min(e.id) INTO next_event FROM (
                SELECT id,intent_id FROM {SCHEMA}.bootstrap_revocations
                UNION ALL SELECT id,intent_id FROM {SCHEMA}.worker_withdrawals
            ) e JOIN {SCHEMA}.bootstraps b ON b.intent_id=e.intent_id
                WHERE b.installation_id=p_installation AND NOT EXISTS (
                    SELECT 1 FROM {SCHEMA}.release_publication_receipts a WHERE a.event_id=e.id);
            IF next_event IS NULL THEN RETURN NULL; END IF;
            RETURN {SCHEMA}.protected_release_publication(p_installation,next_event);
        END $function$;

        CREATE FUNCTION {SCHEMA}.acknowledge_protected_release(p_installation uuid,p jsonb,wire bytea,digest text,manager_digest text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE publication text; receipt jsonb; receipt_wire bytea; retained {SCHEMA}.release_publication_receipts%ROWTYPE;
        BEGIN
            IF wire IS NULL OR octet_length(wire) NOT BETWEEN 2 AND 1048576
                OR wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR digest IS DISTINCT FROM encode(sha256(wire),'hex') THEN
                RAISE EXCEPTION 'build release canonical publication changed';
            END IF;
            PERFORM {SCHEMA}.assert_plan_fields(p,
                ARRAY['schema_version','event_id','event_kind','release','publication_digest'],ARRAY[]::text[],
                ARRAY['event_id'],ARRAY['event_kind'],ARRAY['publication_digest']);
            publication := {SCHEMA}.protected_release_publication(p_installation,(p->>'event_id')::bigint);
            IF publication IS DISTINCT FROM convert_from(wire,'UTF8')
                OR manager_digest IS DISTINCT FROM p->>'publication_digest' THEN
                RAISE EXCEPTION 'build release exact publication or manager receipt changed';
            END IF;
            receipt := jsonb_build_object('schema_version',2,'event_id',p->'event_id','event_kind',p->'event_kind',
                'publication_digest',p->'publication_digest','manager_acknowledgement_digest',manager_digest);
            receipt_wire := convert_to({SCHEMA}.canonical_plan_json(receipt),'UTF8');
            SELECT * INTO retained FROM {SCHEMA}.release_publication_receipts WHERE event_id=(p->>'event_id')::bigint;
            IF FOUND THEN
                IF retained.installation_id IS DISTINCT FROM p_installation OR retained.payload IS DISTINCT FROM receipt
                    OR retained.wire_payload IS DISTINCT FROM receipt_wire
                    OR retained.payload_sha256 IS DISTINCT FROM encode(sha256(receipt_wire),'hex') THEN
                    RAISE EXCEPTION 'build release acknowledgement replay changed';
                END IF;
                RETURN convert_from(retained.wire_payload,'UTF8');
            END IF;
            IF {SCHEMA}.read_next_protected_release(p_installation) IS DISTINCT FROM publication THEN
                RAISE EXCEPTION 'build release acknowledgement must cover exact next event';
            END IF;
            INSERT INTO {SCHEMA}.release_publication_receipts(event_id,installation_id,payload,wire_payload,payload_sha256)
                VALUES((p->>'event_id')::bigint,p_installation,receipt,receipt_wire,encode(sha256(receipt_wire),'hex'));
            -- Manager acknowledgement proves durable revocation reporting only.
            -- Physical terminal/manager release authority is still required; no hold is removed.
            RETURN convert_from(receipt_wire,'UTF8');
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.protected_release_publication(uuid,bigint) FROM PUBLIC, {quote(agent)}")
    for signature in FUNCTIONS:
        op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{signature} FROM PUBLIC")
        op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{signature} TO {quote(agent)}")


def downgrade():
    op.execute(f"LOCK TABLE {SCHEMA}.release_publication_receipts IN ACCESS EXCLUSIVE MODE")
    op.execute(f"""DO $$ BEGIN IF EXISTS (SELECT 1 FROM {SCHEMA}.release_publication_receipts) THEN
        RAISE EXCEPTION 'cannot remove build release publication with retained evidence'; END IF; END $$""")
    for signature in FUNCTIONS:
        op.execute(f"DROP FUNCTION {SCHEMA}.{signature}")
    op.execute(f"DROP FUNCTION {SCHEMA}.protected_release_publication(uuid,bigint)")
    op.drop_table("release_publication_receipts", schema=SCHEMA)
