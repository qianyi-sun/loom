"""Read retained terminal recovery independently of live worker credentials.

Revision ID: build_guard_0033
Revises: build_guard_0032
"""

from alembic import op

revision = "build_guard_0033"
down_revision = "build_guard_0032"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = "read_terminal_native_recovery(uuid,bytea,uuid)"
PAGE_FUNCTION = "discover_terminal_native_recovery(uuid,bytea,bigint,bigint,integer)"


def upgrade():
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.read_terminal_native_recovery(p_installation uuid,p_installation_wire bytea,p_claim uuid)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE result jsonb; retained xid8[];
        BEGIN
            PERFORM {SCHEMA}.assert_management_installation(p_installation,p_installation_wire);
            IF p_claim IS NULL THEN RAISE EXCEPTION 'terminal recovery claim selector absent'; END IF;
            SELECT jsonb_build_object('schema_version',1,'executable',false,
                'preparation',jsonb_build_object('schema_version',1,'request',p.payload,'request_digest',p.payload_sha256),
                'finalization',CASE WHEN f.claim_id IS NULL THEN NULL ELSE jsonb_build_object(
                    'schema_version',1,'request',f.payload,'request_digest',f.payload_sha256) END,
                'profile',profile.payload,'host',host.payload,
                'terminal',CAST({SCHEMA}.terminal_inventory_receipt(t) AS jsonb),
                'release',CAST({SCHEMA}.native_release_receipt(w) AS jsonb)),
                ARRAY[c.retention_xid,r.retention_xid,p.retention_xid,f.retention_xid,
                    profile.retention_xid,host.retention_xid,t.retention_xid,w.retention_xid,e.retention_xid,b.retention_xid]
                INTO result,retained
            FROM {SCHEMA}.platform_claims c
            JOIN {SCHEMA}.worker_registrations r ON r.id=c.registration_id AND r.installation_id=c.installation_id
            JOIN {SCHEMA}.native_recovery_records p ON p.claim_id=c.id AND p.installation_id=c.installation_id AND p.phase='preparation'
            LEFT JOIN {SCHEMA}.native_recovery_records f ON f.claim_id=c.id AND f.installation_id=c.installation_id AND f.phase='finalization'
            JOIN {SCHEMA}.native_recovery_profiles profile ON profile.installation_id=c.installation_id
                AND profile.pool_id=c.payload->'binding'->>'pool_id'
            JOIN {SCHEMA}.native_recovery_hosts host ON host.installation_id=profile.installation_id
                AND host.pool_id=profile.pool_id AND host.payload_sha256=p.payload->'record'->>'node_configuration_sha256'
            JOIN {SCHEMA}.bootstraps b ON b.intent_id=r.intent_id AND b.installation_id=c.installation_id
            JOIN {SCHEMA}.execution_events e ON e.intent_id=b.intent_id AND e.kind='bound'
            JOIN {SCHEMA}.terminal_inventory t ON t.intent_id=r.intent_id AND t.installation_id=c.installation_id
                AND t.assignment_id=r.assignment_id
            JOIN {SCHEMA}.worker_releases w ON w.registration_id=r.id AND w.installation_id=c.installation_id
                AND w.intent_id=r.intent_id
            WHERE c.installation_id=p_installation AND c.id=p_claim
                AND convert_to({SCHEMA}.canonical_plan_json(p.payload->'claim'),'UTF8')=c.wire_payload
                AND convert_to({SCHEMA}.canonical_plan_json(p.payload->'record'->'locator'->'physical'),'UTF8')=e.wire_payload
                AND t.payload->'binding'=c.payload->'binding' AND w.payload->'binding'=c.payload->'binding'
                AND t.payload->'record'->>'physical_identity'=e.payload->>'slurm_job_id'
                AND (w.terminal_id IS NULL OR w.terminal_id=t.id);
            IF NOT FOUND THEN RETURN NULL; END IF;
            IF pg_current_xact_id()=ANY(retained) THEN
                RAISE EXCEPTION 'terminal recovery requires committed historical evidence'; END IF;
            RETURN {SCHEMA}.canonical_plan_json(result);
        END $function$;

        CREATE FUNCTION {SCHEMA}.discover_terminal_native_recovery(
            p_installation uuid,p_installation_wire bytea,p_after bigint,p_through bigint,p_limit integer)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE upper_id bigint; candidate record; attempts jsonb := '[]'::jsonb;
        BEGIN
            PERFORM {SCHEMA}.assert_management_installation(p_installation,p_installation_wire);
            IF p_after IS NULL OR p_after<0 OR (p_through IS NOT NULL AND p_through<p_after)
                OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 64 THEN
                RAISE EXCEPTION 'terminal recovery discovery bounds changed'; END IF;
            SELECT COALESCE(p_through,GREATEST(p_after,COALESCE(max(id),0))) INTO upper_id
                FROM {SCHEMA}.worker_registrations WHERE installation_id=p_installation;
            FOR candidate IN
                SELECT r.id AS event_id,c.id AS claim_id,
                    ARRAY[r.retention_xid,c.retention_xid,p.retention_xid,t.retention_xid,w.retention_xid] AS retained
                FROM {SCHEMA}.worker_registrations r
                JOIN {SCHEMA}.platform_claims c ON c.registration_id=r.id AND c.installation_id=r.installation_id
                JOIN {SCHEMA}.native_recovery_records p ON p.claim_id=c.id AND p.installation_id=c.installation_id AND p.phase='preparation'
                JOIN {SCHEMA}.terminal_inventory t ON t.intent_id=r.intent_id AND t.installation_id=r.installation_id
                    AND t.assignment_id=r.assignment_id
                JOIN {SCHEMA}.worker_releases w ON w.registration_id=r.id AND w.installation_id=r.installation_id
                    AND w.intent_id=r.intent_id
                WHERE r.installation_id=p_installation AND r.id>p_after AND r.id<=upper_id
                ORDER BY r.id LIMIT p_limit
            LOOP
                IF pg_current_xact_id()=ANY(candidate.retained) THEN
                    RAISE EXCEPTION 'terminal recovery discovery requires committed historical evidence'; END IF;
                attempts := attempts || jsonb_build_array(jsonb_build_object(
                    'schema_version',1,'event_id',candidate.event_id,'claim_id',candidate.claim_id));
            END LOOP;
            RETURN {SCHEMA}.canonical_plan_json(jsonb_build_object('schema_version',1,
                'installation_id',p_installation,'after_event_id',p_after,'through_event_id',upper_id,
                'attempts',attempts,'executable',false));
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    for signature in (FUNCTION, PAGE_FUNCTION):
        op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{signature} FROM PUBLIC")
        op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{signature} TO {quote(agent)}")


def downgrade():
    for signature in (PAGE_FUNCTION, FUNCTION):
        op.execute(f"DROP FUNCTION {SCHEMA}.{signature}")
