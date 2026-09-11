"""Capture complete pending/held demand before native claim admission is installed.

Revision ID: build_guard_0006
Revises: build_guard_0005
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql as pg

from capacity_build_guard_migrations.versions.build_guard_0001_assignments import _payload

revision = "build_guard_0006"
down_revision = "build_guard_0005"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTIONS = ("capture_demand(uuid,bigint,jsonb)", "read_demand(uuid)")


def upgrade():
    op.create_index("build_guard_plans_installation_idx","plans",["installation_id"],schema=SCHEMA)
    op.create_table("reporter_state",
        sa.Column("installation_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.installations.id"), primary_key=True),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("configuration_generation", sa.BigInteger(), nullable=False),
        sa.Column("source_observed_at", pg.TIMESTAMP(timezone=True), nullable=False),
        sa.CheckConstraint("sequence > 0 AND configuration_generation > 0"),
        *_payload(), schema=SCHEMA)
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.demand_timestamp(value timestamptz) RETURNS text
        LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
          SELECT to_char(value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS') ||
            CASE WHEN extract(microseconds FROM value)::bigint % 1000000=0 THEN ''
              ELSE '.' || to_char(value AT TIME ZONE 'UTC','US') END || 'Z'
        $$;

        CREATE FUNCTION {SCHEMA}.read_demand(p_installation uuid) RETURNS text
        LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE installation {SCHEMA}.installations%ROWTYPE; state {SCHEMA}.reporter_state%ROWTYPE;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build demand requires serializable transaction';
            END IF;
            SELECT * INTO installation FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build demand installation is absent'; END IF;
            SELECT * INTO state FROM {SCHEMA}.reporter_state WHERE installation_id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RETURN NULL; END IF;
            IF state.payload IS DISTINCT FROM convert_from(state.wire_payload,'UTF8')::jsonb
                OR state.payload_sha256 IS DISTINCT FROM encode(sha256(state.wire_payload),'hex')
                OR convert_from(state.wire_payload,'UTF8') IS DISTINCT FROM {SCHEMA}.canonical_plan_json(state.payload)
                OR state.payload->>'subject_id' IS DISTINCT FROM installation.subject_id::text
                OR state.payload->>'subject_incarnation' IS DISTINCT FROM installation.subject_incarnation::text
                OR state.payload->>'reporter_incarnation' IS DISTINCT FROM installation.reporter_incarnation::text
                OR state.payload->'deployment_generation' IS DISTINCT FROM to_jsonb(installation.deployment_generation)
                OR state.payload->'sequence' IS DISTINCT FROM to_jsonb(state.sequence)
                OR state.payload->'configuration_generation' IS DISTINCT FROM to_jsonb(state.configuration_generation)
                OR state.payload->>'source_observed_at' IS DISTINCT FROM {SCHEMA}.demand_timestamp(state.source_observed_at) THEN
                RAISE EXCEPTION 'build demand retained report binding changed';
            END IF;
            RETURN convert_from(state.wire_payload,'UTF8');
        END $function$;

        CREATE FUNCTION {SCHEMA}.capture_demand(p_installation uuid, p_configuration bigint, p_sources jsonb)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE
            installation {SCHEMA}.installations%ROWTYPE;
            request public.personal_dev_build_platform_requests%ROWTYPE;
            held record;
            binding jsonb;
            source_wire bytea;
            pending jsonb := '[]'::jsonb;
            assigned jsonb := '[]'::jsonb;
            report jsonb;
            wire bytea;
            observed timestamptz;
            next_sequence bigint;
            pending_ids uuid[];
            lock_request_ids uuid[];
            expected_sources text[] := ARRAY[]::text[];
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build demand requires serializable transaction';
            END IF;
            IF p_configuration IS NULL OR p_configuration <= 0 OR jsonb_typeof(p_sources) IS DISTINCT FROM 'object'
                OR octet_length(p_sources::text)>8388608 THEN
                RAISE EXCEPTION 'build demand configuration or complete sources are invalid';
            END IF;
            SELECT * INTO installation FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build demand installation is absent'; END IF;
            observed := clock_timestamp();
            SELECT coalesce(array_agg(id),ARRAY[]::uuid[]) INTO pending_ids FROM (
                SELECT r.id FROM public.personal_dev_build_platform_requests r
                JOIN public.personal_dev_candidate_build_attempts a ON a.id=r.attempt_id
                JOIN public.personal_dev_candidates c ON c.id=r.candidate_id
                WHERE r.owner_user_id=installation.owner_user_id
                    AND r.subject_id=installation.subject_id AND r.subject_incarnation=installation.subject_incarnation
                    AND r.deployment_generation=installation.deployment_generation AND r.cancelled_at IS NULL
                    AND a.state='running' AND a.finished_at IS NULL AND a.lease_epoch=r.attempt_lease_epoch
                    AND a.lease_expires_at>observed AND c.status='building' AND c.artifact_state='retained'
                    AND NOT EXISTS (SELECT 1 FROM {SCHEMA}.request_holds h WHERE h.request_id=r.id)
                LIMIT 10001) active_requests;
            IF cardinality(pending_ids)>10000 THEN RAISE EXCEPTION 'build demand pending requests exceed bound'; END IF;
            SELECT coalesce(array_agg(id),ARRAY[]::uuid[]) INTO lock_request_ids FROM (
                SELECT unnest(pending_ids) AS id UNION
                SELECT h.request_id FROM {SCHEMA}.request_holds h
                    JOIN {SCHEMA}.assignments a ON a.id=h.assignment_id
                    JOIN {SCHEMA}.plans p ON p.id=a.plan_id WHERE p.installation_id=p_installation
                LIMIT 20001) current_requests;
            IF cardinality(lock_request_ids)>20000 THEN RAISE EXCEPTION 'build demand current work exceeds bound'; END IF;
            PERFORM a.id FROM public.personal_dev_candidate_build_attempts a WHERE a.id IN (
                SELECT r.attempt_id FROM public.personal_dev_build_platform_requests r
                WHERE r.id=ANY(pending_ids))
                ORDER BY a.id FOR UPDATE;
            PERFORM c.id FROM public.personal_dev_candidates c WHERE c.id IN (
                SELECT r.candidate_id FROM public.personal_dev_build_platform_requests r
                WHERE r.id=ANY(pending_ids))
                ORDER BY c.id FOR UPDATE;
            PERFORM r.id FROM public.personal_dev_build_platform_requests r
                WHERE r.id=ANY(lock_request_ids) ORDER BY r.id FOR UPDATE;
            observed := clock_timestamp();
            FOR request IN SELECT r.* FROM public.personal_dev_build_platform_requests r
                JOIN public.personal_dev_candidate_build_attempts a ON a.id=r.attempt_id
                JOIN public.personal_dev_candidates c ON c.id=r.candidate_id
                WHERE r.id=ANY(pending_ids) AND r.owner_user_id=installation.owner_user_id
                    AND r.subject_id=installation.subject_id AND r.subject_incarnation=installation.subject_incarnation
                    AND r.deployment_generation=installation.deployment_generation AND r.cancelled_at IS NULL
                    AND a.state='running' AND a.finished_at IS NULL AND a.lease_epoch=r.attempt_lease_epoch
                    AND a.lease_expires_at>observed AND c.status='building' AND c.artifact_state='retained'
                    AND NOT EXISTS (SELECT 1 FROM {SCHEMA}.request_holds h WHERE h.request_id=r.id)
                ORDER BY r.bucket_id LOOP
                expected_sources := array_append(expected_sources,request.id::text);
                IF cardinality(expected_sources)>10000 OR NOT p_sources ? request.id::text THEN
                    RAISE EXCEPTION 'build demand complete source set changed or exceeds bound';
                END IF;
                source_wire := convert_to(p_sources->>request.id::text,'UTF8');
                PERFORM {SCHEMA}.assert_current_source(p_installation,request.id,
                    convert_from(source_wire,'UTF8')::jsonb,source_wire,request.source_binding_sha256);
                IF request.created_at>observed THEN RAISE EXCEPTION 'build demand submission is in the future'; END IF;
                pending := pending || jsonb_build_array(jsonb_build_object('schema_version',1,
                    'bucket_id',request.bucket_id,'requested_slots',1,'local_priority',0,
                    'oldest_submitted_at',{SCHEMA}.demand_timestamp(request.created_at),
                    'eligible_pool_ids',jsonb_build_array(CASE request.platform WHEN 'linux/arm64' THEN 'gb10' ELSE 'oldlab' END),
                    'required_capabilities',jsonb_build_array(CASE request.platform WHEN 'linux/arm64' THEN 'cpu_arch.arm64' ELSE 'cpu_arch.x86_64' END,
                        'personal-build-worker'),'attempt_ids',jsonb_build_array(request.id)));
            END LOOP;
            IF cardinality(expected_sources) <> (SELECT count(*) FROM jsonb_object_keys(p_sources)) THEN
                RAISE EXCEPTION 'build demand complete source set changed';
            END IF;
            -- Held requests are immutable accounting facts, even when source
            -- expired/cancelled. No native claims exist in this revision; do not
            -- fabricate a worker identity or physical-release evidence.
            FOR held IN SELECT a.*,p.payload AS proposal,p.wire_payload AS proposal_wire,r.created_at
                FROM {SCHEMA}.request_holds h JOIN {SCHEMA}.assignments a ON a.id=h.assignment_id
                JOIN {SCHEMA}.plans p ON p.id=a.plan_id
                JOIN public.personal_dev_build_platform_requests r ON r.id=h.request_id
                WHERE p.installation_id=p_installation ORDER BY a.request_id LOOP
                PERFORM {SCHEMA}.assert_plan_contract(held.proposal,held.proposal_wire);
                PERFORM {SCHEMA}.assert_native_closure_plan(held.proposal,installation.payload);
                SELECT value->'binding' INTO binding FROM jsonb_array_elements(held.proposal->'shapes')
                    WHERE value->'binding'->>'shape_instance_id'=held.shape_instance_id;
                IF NOT FOUND OR held.payload->>'request_id' IS DISTINCT FROM held.request_id::text
                    OR held.payload->>'id' IS DISTINCT FROM held.id::text
                    OR held.payload->>'plan_id' IS DISTINCT FROM held.plan_id::text
                    OR held.payload->>'submission_intent_id' IS DISTINCT FROM binding->>'intent_id'
                    OR held.created_at>observed THEN
                    RAISE EXCEPTION 'build demand held assignment binding changed';
                END IF;
                assigned := assigned || jsonb_build_array(jsonb_build_object('schema_version',1,
                    'attempt_id',held.request_id,'pool_id',binding->'pool_id','pool_generation',binding->'pool_generation',
                    'profile_id',binding->'profile_id','profile_generation',binding->'profile_generation','profile_digest',binding->'profile_digest',
                    'shape_id',binding->'shape_id','allowance_epoch',binding->'execution'->'allocation_epoch',
                    'local_priority',0,'submitted_at',{SCHEMA}.demand_timestamp(held.created_at)));
                IF jsonb_array_length(assigned)>10000 THEN RAISE EXCEPTION 'build demand held assignments exceed bound'; END IF;
            END LOOP;
            PERFORM {SCHEMA}.read_demand(p_installation);
            IF EXISTS (SELECT 1 FROM {SCHEMA}.reporter_state WHERE installation_id=p_installation
                AND (configuration_generation>p_configuration OR source_observed_at>observed)) THEN
                RAISE EXCEPTION 'build demand configuration or observation time regressed';
            END IF;
            SELECT sequence+1 INTO next_sequence FROM {SCHEMA}.reporter_state WHERE installation_id=p_installation;
            next_sequence := coalesce(next_sequence,1);
            report := jsonb_build_object('schema_version',1,'subject_id',installation.subject_id,
                'subject_incarnation',installation.subject_incarnation,'configuration_generation',p_configuration,
                'deployment_generation',installation.deployment_generation,'reporter_incarnation',installation.reporter_incarnation,
                'sequence',next_sequence,'source_observed_at',{SCHEMA}.demand_timestamp(observed),
                'pending_unassigned',pending,'current_assignments',assigned,'fixed_claims','[]'::jsonb);
            wire := convert_to({SCHEMA}.canonical_plan_json(report),'UTF8');
            INSERT INTO {SCHEMA}.reporter_state(installation_id,sequence,configuration_generation,source_observed_at,payload,wire_payload,payload_sha256)
                VALUES(p_installation,next_sequence,p_configuration,observed,report,wire,encode(sha256(wire),'hex'))
                ON CONFLICT(installation_id) DO UPDATE SET sequence=EXCLUDED.sequence,
                    configuration_generation=EXCLUDED.configuration_generation,source_observed_at=EXCLUDED.source_observed_at,
                    payload=EXCLUDED.payload,wire_payload=EXCLUDED.wire_payload,payload_sha256=EXCLUDED.payload_sha256;
            RETURN convert_from(wire,'UTF8');
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON TABLE {SCHEMA}.reporter_state FROM PUBLIC, {quote(agent)}")
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.demand_timestamp(timestamptz) FROM PUBLIC, {quote(agent)}")
    for signature in FUNCTIONS:
        op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{signature} FROM PUBLIC")
        op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{signature} TO {quote(agent)}")


def downgrade():
    op.execute(f"LOCK TABLE {SCHEMA}.reporter_state IN ACCESS EXCLUSIVE MODE")
    op.execute(f"""DO $$ BEGIN IF EXISTS (SELECT 1 FROM {SCHEMA}.reporter_state) THEN
        RAISE EXCEPTION 'cannot remove build demand with retained reporter state'; END IF; END $$""")
    for signature in FUNCTIONS:
        op.execute(f"DROP FUNCTION {SCHEMA}.{signature}")
    op.execute(f"DROP FUNCTION {SCHEMA}.demand_timestamp(timestamptz)")
    op.drop_table("reporter_state",schema=SCHEMA)
    op.drop_index("build_guard_plans_installation_idx",table_name="plans",schema=SCHEMA)
