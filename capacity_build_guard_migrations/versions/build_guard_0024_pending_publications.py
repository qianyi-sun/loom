"""Rediscover committed plans after an admission acknowledgement reply is lost.

Revision ID: build_guard_0024
Revises: build_guard_0023
"""

from alembic import op

revision = "build_guard_0024"
down_revision = "build_guard_0023"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = "read_pending_publications(uuid,uuid,uuid,integer)"


def upgrade():
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.read_pending_publications(p_installation uuid,p_after uuid,p_through uuid,p_limit integer)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE upper_id uuid; pending jsonb;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build publication discovery requires serializable transaction'; END IF;
            IF p_after IS NULL OR (p_through IS NOT NULL AND p_through < p_after)
                OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 16 THEN
                RAISE EXCEPTION 'build publication discovery pagination bounds changed'; END IF;
            PERFORM 1 FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build publication discovery installation absent'; END IF;
            IF EXISTS (SELECT 1 FROM {SCHEMA}.plans WHERE installation_id=p_installation
                AND preparation_xid=pg_current_xact_id()) THEN
                RAISE EXCEPTION 'build publication discovery requires committed preparation'; END IF;
            IF EXISTS (SELECT 1 FROM {SCHEMA}.hold_retirements WHERE installation_id=p_installation
                AND retention_xid=pg_current_xact_id()) THEN
                RAISE EXCEPTION 'build publication discovery requires committed retirement'; END IF;
            SELECT COALESCE(p_through,GREATEST(p_after,(SELECT id FROM {SCHEMA}.plans
                WHERE installation_id=p_installation ORDER BY id DESC LIMIT 1))) INTO upper_id;
            SELECT COALESCE(jsonb_agg(jsonb_build_object('schema_version',1,
                'plan_id',p.id,'proposal_digest',p.payload_sha256) ORDER BY p.id),'[]'::jsonb)
                INTO pending FROM (SELECT id,payload_sha256 FROM {SCHEMA}.plans p
                    WHERE installation_id=p_installation AND id>p_after AND id<=upper_id
                        AND NOT EXISTS (SELECT 1 FROM {SCHEMA}.dispositions d WHERE d.plan_id=p.id)
                        AND NOT (
                            EXISTS (SELECT 1 FROM {SCHEMA}.assignments a WHERE a.plan_id=p.id)
                            AND NOT EXISTS (SELECT 1 FROM {SCHEMA}.assignments a
                                WHERE a.plan_id=p.id AND NOT EXISTS (
                                    SELECT 1 FROM {SCHEMA}.hold_retirements r
                                    WHERE r.assignment_id=a.id AND r.request_id=a.request_id
                                        AND r.installation_id=p.installation_id)))
                    ORDER BY id LIMIT p_limit) p;
            -- Discovery is not authorization: publication rechecks the exact
            -- retained plan, current source, leases and holds under locks.
            RETURN {SCHEMA}.canonical_plan_json(jsonb_build_object('schema_version',1,
                'installation_id',p_installation,'after_plan_id',p_after,'through_plan_id',upper_id,
                'plans',pending,'executable',false));
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quote(agent)}")


def downgrade():
    op.execute(f"DROP FUNCTION {SCHEMA}.{FUNCTION}")
