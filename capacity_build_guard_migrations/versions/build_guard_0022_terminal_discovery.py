"""Discover held registered workers independently of final release.

Revision ID: build_guard_0022
Revises: build_guard_0021
"""

from alembic import op

revision = "build_guard_0022"
down_revision = "build_guard_0021"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = "read_pending_native_workers(uuid,bigint,bigint,integer)"


def upgrade():
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.read_pending_native_workers(p_installation uuid,p_after bigint,p_through bigint,p_limit integer)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE upper_id bigint; workers jsonb;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'native terminal discovery requires serializable transaction'; END IF;
            IF p_after IS NULL OR p_after < 0 OR (p_through IS NOT NULL AND p_through < p_after)
                OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 64 THEN
                RAISE EXCEPTION 'native terminal discovery pagination bounds changed'; END IF;
            PERFORM 1 FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'native terminal discovery installation absent'; END IF;
            IF EXISTS (SELECT 1 FROM {SCHEMA}.worker_registrations WHERE installation_id=p_installation AND retention_xid=pg_current_xact_id())
                OR EXISTS (SELECT 1 FROM {SCHEMA}.platform_claims WHERE installation_id=p_installation AND retention_xid=pg_current_xact_id())
                OR EXISTS (SELECT 1 FROM {SCHEMA}.worker_releases WHERE installation_id=p_installation AND retention_xid=pg_current_xact_id()) THEN
                RAISE EXCEPTION 'native terminal discovery requires committed worker history'; END IF;
            SELECT COALESCE(p_through,GREATEST(p_after,COALESCE(max(id),0))) INTO upper_id
                FROM {SCHEMA}.worker_registrations WHERE installation_id=p_installation;
            SELECT COALESCE(jsonb_agg(jsonb_build_object('schema_version',1,
                'event_id',pending.id,'binding',pending.payload->'binding','operation_id',pending.operation_id,
                'worker_id',pending.worker_id,'worker_incarnation',pending.worker_incarnation,'claim',pending.claim)
                ORDER BY pending.id),'[]'::jsonb) INTO workers FROM (
                SELECT r.id,r.payload,r.operation_id,r.worker_id,r.worker_incarnation,c.payload AS claim
                FROM {SCHEMA}.worker_registrations r
                JOIN {SCHEMA}.assignments a ON a.id=r.assignment_id AND a.submission_intent_id=r.intent_id
                JOIN {SCHEMA}.plans p ON p.id=a.plan_id AND p.installation_id=r.installation_id
                JOIN {SCHEMA}.request_holds h ON h.request_id=a.request_id AND h.assignment_id=a.id
                LEFT JOIN {SCHEMA}.platform_claims c ON c.registration_id=r.id AND c.installation_id=r.installation_id
                WHERE r.installation_id=p_installation AND r.id>p_after AND r.id<=upper_id
                    AND NOT EXISTS (SELECT 1 FROM {SCHEMA}.worker_releases e WHERE e.registration_id=r.id)
                ORDER BY r.id LIMIT p_limit
            ) pending;
            RETURN {SCHEMA}.canonical_plan_json(jsonb_build_object('schema_version',1,
                'installation_id',p_installation,'after_event_id',p_after,'through_event_id',upper_id,
                'workers',workers,'executable',false));
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quote(agent)}")


def downgrade():
    op.execute(f"DROP FUNCTION {SCHEMA}.{FUNCTION}")
