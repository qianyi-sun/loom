"""Observe committed preparation without inferring worker or release authority.

Revision ID: build_guard_0010
Revises: build_guard_0009
"""

from alembic import op

revision = "build_guard_0010"
down_revision = "build_guard_0009"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = "observe_intent(uuid,jsonb,bytea,text)"


def upgrade():
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.observe_intent(p_installation uuid, p jsonb, wire bytea, digest text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE bootstrap {SCHEMA}.bootstraps%ROWTYPE;
            prepared {SCHEMA}.execution_events%ROWTYPE;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build observation requires serializable transaction';
            END IF;
            IF wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR encode(sha256(wire),'hex') IS DISTINCT FROM digest
                OR octet_length(wire) NOT BETWEEN 2 AND 1048576 THEN
                RAISE EXCEPTION 'build observation canonical binding changed';
            END IF;
            SELECT * INTO bootstrap FROM {SCHEMA}.bootstraps
                WHERE intent_id=(p->>'intent_id')::uuid AND installation_id=p_installation;
            IF NOT FOUND THEN RAISE EXCEPTION 'build observation bootstrap is absent'; END IF;
            IF p IS DISTINCT FROM bootstrap.payload->'proposal'->'binding' THEN
                RAISE EXCEPTION 'build observation exact binding changed';
            END IF;
            SELECT * INTO prepared FROM {SCHEMA}.execution_events
                WHERE intent_id=bootstrap.intent_id AND kind='prepared';
            IF NOT FOUND THEN RAISE EXCEPTION 'build observation preparation is absent'; END IF;
            IF prepared.retention_xid=pg_current_xact_id()
                OR bootstrap.retention_xid=pg_current_xact_id() THEN
                RAISE EXCEPTION 'build observation requires committed preparation';
            END IF;
            -- Recovery must survive logical closure, cancellation and expiry.
            -- This revision has no worker exchange/claim/terminal procedures.
            -- Extend this projection atomically when those consumers are added;
            -- absence of terminal evidence can never authorize physical release.
            RETURN {SCHEMA}.canonical_plan_json(jsonb_build_object(
                'schema_version',2,'binding',p,'bootstrap_registration_epoch',1,
                'worker_id',NULL,'worker_incarnation',NULL,'protected_registration_epoch',0,
                'claim_high_water',0,'drain',NULL,'release',NULL,'withdrawal',NULL,
                'prepared_revocation',NULL,'executable',true));
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quote(agent)}")


def downgrade():
    # Read-only surface has no new evidence to discard. Earlier revisions retain
    # their own nonempty-ledger downgrade fences.
    op.execute(f"DROP FUNCTION {SCHEMA}.{FUNCTION}")
