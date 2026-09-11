"""Persist one exact manager plan and source-bound holds, without publication.

Revision ID: build_guard_0003
Revises: build_guard_0002
"""

from alembic import op

from capacity_build_guard_migrations import prepare_v1_sql

revision = "build_guard_0003"
down_revision = "build_guard_0002"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = f"{SCHEMA}.prepare_plan(uuid,jsonb,bytea,text,jsonb)"


def upgrade():
    prepare_v1_sql.install()
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.prepare_plan(
            p_installation uuid, p_proposal jsonb, p_wire bytea, p_digest text, p_sources jsonb
        ) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        <<guard>>
        DECLARE
            installation {SCHEMA}.installations%ROWTYPE;
            existing_plan {SCHEMA}.plans%ROWTYPE;
            existing_assignment {SCHEMA}.assignments%ROWTYPE;
            allowance jsonb;
            shape jsonb;
            binding jsonb;
            anchor jsonb;
            profile jsonb;
            native_pool jsonb;
            request_id uuid;
            assignment_id uuid;
            plan_id uuid;
            source_wire bytea;
            source_digest text;
            lease_until timestamptz;
            expires_at timestamptz;
            source_epoch bigint;
            next_sequence bigint;
            record jsonb;
            record_wire bytea;
            result jsonb := '[]'::jsonb;
            item_count integer;
            replay boolean;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build plan preparation requires serializable transaction';
            END IF;
            IF p_wire IS NULL OR octet_length(p_wire) NOT BETWEEN 2 AND 1048576
                OR jsonb_typeof(p_proposal) IS DISTINCT FROM 'object'
                OR convert_from(p_wire, 'UTF8')::jsonb IS DISTINCT FROM p_proposal
                OR encode(sha256(p_wire), 'hex') IS DISTINCT FROM p_digest
                OR p_proposal->'schema_version' IS DISTINCT FROM '2'::jsonb
                OR p_proposal->'executable' IS DISTINCT FROM 'true'::jsonb
                OR jsonb_typeof(p_sources) IS DISTINCT FROM 'object'
                OR octet_length(p_sources::text) > 8388608
                OR jsonb_typeof(p_proposal->'shapes') IS DISTINCT FROM 'array'
                OR jsonb_typeof(p_proposal->'allowances') IS DISTINCT FROM 'array' THEN
                RAISE EXCEPTION 'build plan wire or complete work set is invalid';
            END IF;
            PERFORM {SCHEMA}.assert_plan_contract(p_proposal, p_wire);
            item_count := jsonb_array_length(p_proposal->'allowances');
            IF item_count NOT BETWEEN 1 AND 10000
                OR item_count <> jsonb_array_length(p_proposal->'shapes')
                OR item_count <> (SELECT count(*) FROM jsonb_object_keys(p_sources))
                OR item_count <> (SELECT count(DISTINCT value->>'allowance_id') FROM jsonb_array_elements(p_proposal->'allowances'))
                OR item_count <> (SELECT count(DISTINCT value->>'protected_attempt_id') FROM jsonb_array_elements(p_proposal->'allowances'))
                OR item_count <> (SELECT count(DISTINCT value->>'shape_instance_id') FROM jsonb_array_elements(p_proposal->'allowances')) THEN
                RAISE EXCEPTION 'build plan assignments must cover each cold shape exactly once';
            END IF;
            plan_id := (p_proposal->>'plan_id')::uuid;
            expires_at := (p_proposal->>'lease_not_after')::timestamptz;
            IF plan_id IS NULL OR expires_at IS NULL THEN RAISE EXCEPTION 'build plan identity or expiry is absent'; END IF;
            SELECT * INTO installation FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build plan installation is absent'; END IF;
            IF p_proposal->>'reporter_incarnation' IS DISTINCT FROM installation.reporter_incarnation::text
                OR p_proposal->>'protected_admission_sha256' IS DISTINCT FROM installation.payload->>'protected_admission_sha256' THEN
                RAISE EXCEPTION 'build plan reporter installation changed';
            END IF;
            SELECT * INTO existing_plan FROM {SCHEMA}.plans p WHERE p.id=plan_id FOR UPDATE;
            replay := FOUND;
            IF replay AND (existing_plan.installation_id IS DISTINCT FROM p_installation
                OR existing_plan.wire_payload IS DISTINCT FROM p_wire OR existing_plan.payload IS DISTINCT FROM p_proposal
                OR existing_plan.expires_at IS DISTINCT FROM expires_at
                OR existing_plan.payload_sha256 IS DISTINCT FROM p_digest) THEN
                RAISE EXCEPTION 'build plan replay binding changed';
            END IF;
            IF EXISTS (SELECT 1 FROM {SCHEMA}.dispositions d WHERE d.plan_id=guard.plan_id) THEN
                RAISE EXCEPTION 'build plan already has a disposition';
            END IF;
            -- Lock each class globally before the single-request helper, which
            -- repeats these locks without changing their established ordering.
            PERFORM a.id FROM public.personal_dev_candidate_build_attempts a
              WHERE a.id IN (SELECT r.attempt_id FROM public.personal_dev_build_platform_requests r
                WHERE r.id::text IN (SELECT jsonb_object_keys(p_sources))) ORDER BY a.id FOR UPDATE;
            PERFORM c.id FROM public.personal_dev_candidates c
              WHERE c.id IN (SELECT r.candidate_id FROM public.personal_dev_build_platform_requests r
                WHERE r.id::text IN (SELECT jsonb_object_keys(p_sources))) ORDER BY c.id FOR UPDATE;
            PERFORM r.id FROM public.personal_dev_build_platform_requests r
              WHERE r.id::text IN (SELECT jsonb_object_keys(p_sources)) ORDER BY r.id FOR UPDATE;
            IF expires_at <= clock_timestamp() THEN RAISE EXCEPTION 'build plan lease expired'; END IF;
            IF NOT replay THEN
                INSERT INTO {SCHEMA}.plans(id, installation_id, expires_at, payload, wire_payload, payload_sha256)
                  VALUES(plan_id, p_installation, expires_at, p_proposal, p_wire, p_digest);
            END IF;
            anchor := p_proposal->'shapes'->0->'binding';
            FOR allowance IN SELECT value FROM jsonb_array_elements(p_proposal->'allowances') ORDER BY value->>'protected_attempt_id' LOOP
                request_id := (allowance->>'protected_attempt_id')::uuid;
                IF NOT p_sources ? request_id::text OR allowance->'shape_slot_index' IS DISTINCT FROM '0'::jsonb THEN
                    RAISE EXCEPTION 'build plan source or native slot is absent';
                END IF;
                IF (SELECT count(*) FROM jsonb_array_elements(p_proposal->'shapes') s
                    WHERE s->'binding'->>'shape_instance_id'=allowance->>'shape_instance_id') <> 1 THEN
                    RAISE EXCEPTION 'build plan assignment shape is not exact';
                END IF;
                SELECT value INTO shape FROM jsonb_array_elements(p_proposal->'shapes')
                    WHERE value->'binding'->>'shape_instance_id'=allowance->>'shape_instance_id';
                binding := shape->'binding';
                SELECT value INTO profile FROM jsonb_array_elements(installation.payload->'runtime'->'profiles')
                    WHERE value->>'pool_id'=binding->>'pool_id';
                IF NOT FOUND THEN RAISE EXCEPTION 'build plan native profile is absent'; END IF;
                SELECT value INTO native_pool FROM jsonb_array_elements(installation.payload->'runtime'->'pools')
                    WHERE value->>'pool_id'=binding->>'pool_id';
                IF NOT FOUND THEN RAISE EXCEPTION 'build plan native pool is absent'; END IF;
                IF binding->>'subject_id' IS DISTINCT FROM installation.subject_id::text
                    OR binding->>'subject_incarnation' IS DISTINCT FROM installation.subject_incarnation::text
                    OR binding->'deployment_generation' IS DISTINCT FROM to_jsonb(installation.deployment_generation)
                    OR binding->'candidate_generation' IS DISTINCT FROM installation.payload->'candidate_generation'
                    OR binding->>'account_id' IS DISTINCT FROM 'dev-owner-' || replace(installation.owner_user_id::text,'-','')
                    OR binding->>'tier_id' IS DISTINCT FROM 'development'
                    OR binding->'candidate' IS DISTINCT FROM installation.payload->'runtime'->'candidate'
                    OR binding->'execution'->>'execution_state' IS DISTINCT FROM 'active'
                    OR binding->'execution'->>'execution_manifest_sha256' IS DISTINCT FROM installation.payload->'runtime'->>'execution_manifest_sha256'
                    OR binding->'execution'->>'trusted_fleet_release_sha256' IS DISTINCT FROM installation.payload->'runtime'->>'trusted_fleet_release_sha256'
                    OR binding->'execution' IS DISTINCT FROM anchor->'execution'
                    OR binding->'tranche_id' IS DISTINCT FROM anchor->'tranche_id'
                    OR binding->'pool_id' IS DISTINCT FROM anchor->'pool_id'
                    OR binding->'pool_generation' IS DISTINCT FROM profile->'pool_generation'
                    OR binding->'profile_id' IS DISTINCT FROM native_pool->'profile_id'
                    OR binding->'profile_generation' IS DISTINCT FROM profile->'profile_generation'
                    OR binding->'profile_digest' IS DISTINCT FROM profile->'profile_digest'
                    OR binding->'executor_id' IS DISTINCT FROM native_pool->'executor_id'
                    OR binding->'executor_incarnation' IS DISTINCT FROM native_pool->'executor_incarnation'
                    OR binding->'concurrency_slots' IS DISTINCT FROM '1'::jsonb
                    OR binding->'rollout_surge_slots' IS DISTINCT FROM '0'::jsonb
                    OR binding->>'old_shape_backing_id' IS NOT NULL
                    OR jsonb_typeof(binding->'node_ids') IS DISTINCT FROM 'array'
                    OR jsonb_array_length(binding->'node_ids') <> 1
                    OR NOT ((native_pool->'node_ids') @> (binding->'node_ids'))
                    OR binding->'shape_id' IS DISTINCT FROM profile->'worker_shapes'->0->'shape_id'
                    OR binding->'resources' IS DISTINCT FROM profile->'worker_shapes'->0->'total_resources'
                    OR shape->'worker_shape' IS DISTINCT FROM profile->'worker_shapes'->0
                    OR shape->'protocol_generation' IS DISTINCT FROM profile->'protocol_generation'
                    OR shape->'protocol_digest' IS DISTINCT FROM profile->'protocol_digest'
                    OR binding->'intent_id' IS DISTINCT FROM allowance->'submission_intent_id' THEN
                    RAISE EXCEPTION 'build plan source, service or native shape binding changed';
                END IF;
                source_wire := convert_to(p_sources->>request_id::text, 'UTF8');
                source_digest := encode(sha256(source_wire),'hex');
                lease_until := LEAST(expires_at, {SCHEMA}.assert_current_source(
                    p_installation, request_id, convert_from(source_wire,'UTF8')::jsonb, source_wire, source_digest));
                IF (SELECT r.platform FROM public.personal_dev_build_platform_requests r WHERE r.id=request_id)
                    IS DISTINCT FROM native_pool->>'platform' THEN
                    RAISE EXCEPTION 'build plan source platform differs from native pool';
                END IF;
                SELECT * INTO existing_assignment FROM {SCHEMA}.assignments a WHERE a.plan_id=guard.plan_id AND a.request_id=guard.request_id FOR UPDATE;
                IF FOUND THEN
                    IF NOT replay OR existing_assignment.submission_intent_id IS DISTINCT FROM (allowance->>'submission_intent_id')::uuid
                        OR existing_assignment.shape_instance_id IS DISTINCT FROM allowance->>'shape_instance_id'
                        OR existing_assignment.shape_slot_index <> 0
                        OR existing_assignment.payload->>'id' IS DISTINCT FROM existing_assignment.id::text
                        OR existing_assignment.payload->>'plan_id' IS DISTINCT FROM guard.plan_id::text
                        OR existing_assignment.payload->>'request_id' IS DISTINCT FROM guard.request_id::text
                        OR existing_assignment.payload->'allowance_id' IS DISTINCT FROM allowance->'allowance_id'
                        OR existing_assignment.payload->'submission_intent_id' IS DISTINCT FROM allowance->'submission_intent_id'
                        OR existing_assignment.payload->'shape_instance_id' IS DISTINCT FROM allowance->'shape_instance_id'
                        OR existing_assignment.payload->'shape_slot_index' IS DISTINCT FROM '0'::jsonb
                        OR existing_assignment.payload->>'source_canonical_json' IS DISTINCT FROM convert_from(source_wire,'UTF8')
                        OR existing_assignment.payload->>'source_binding_sha256' IS DISTINCT FROM source_digest
                        OR existing_assignment.payload->'runtime_installation_sha256' IS DISTINCT FROM installation.payload->'runtime_installation_sha256'
                        OR existing_assignment.payload->'execution_generation' IS DISTINCT FROM convert_from(source_wire,'UTF8')::jsonb->'lease_epoch'
                        OR jsonb_typeof(existing_assignment.payload->'request_sequence') IS DISTINCT FROM 'number'
                        OR (existing_assignment.payload->>'request_sequence')::bigint <= 0
                        OR jsonb_typeof(existing_assignment.payload->'lease_not_after_epoch_microseconds') IS DISTINCT FROM 'number'
                        OR (existing_assignment.payload->>'lease_not_after_epoch_microseconds')::bigint
                            > (extract(epoch FROM lease_until)*1000000)::bigint
                        OR (existing_assignment.payload->>'lease_not_after_epoch_microseconds')::bigint
                            <= (extract(epoch FROM clock_timestamp())*1000000)::bigint
                        OR NOT EXISTS (SELECT 1 FROM {SCHEMA}.request_holds h WHERE h.request_id=guard.request_id AND h.assignment_id=existing_assignment.id) THEN
                        RAISE EXCEPTION 'build plan assignment replay or hold changed';
                    END IF;
                    record := existing_assignment.payload;
                ELSE
                    IF replay OR EXISTS (SELECT 1 FROM {SCHEMA}.request_holds h WHERE h.request_id=guard.request_id) THEN
                        RAISE EXCEPTION 'build request already held or assignment missing';
                    END IF;
                    SELECT count(*)+1 INTO next_sequence FROM {SCHEMA}.assignments a WHERE a.request_id=guard.request_id;
                    SELECT r.attempt_lease_epoch INTO source_epoch FROM public.personal_dev_build_platform_requests r WHERE r.id=request_id;
                    assignment_id := gen_random_uuid();
                    record := jsonb_build_object('id', assignment_id, 'request_id', request_id, 'plan_id', plan_id,
                        'request_sequence', next_sequence, 'execution_generation', source_epoch,
                        'allowance_id', allowance->>'allowance_id', 'submission_intent_id', allowance->>'submission_intent_id',
                        'shape_instance_id', allowance->>'shape_instance_id', 'shape_slot_index', 0,
                        'source_binding_sha256', source_digest, 'source_canonical_json', convert_from(source_wire,'UTF8'),
                        'runtime_installation_sha256', installation.payload->>'runtime_installation_sha256',
                        'lease_not_after_epoch_microseconds', (extract(epoch FROM lease_until)*1000000)::bigint);
                    record_wire := convert_to(record::text,'UTF8');
                    INSERT INTO {SCHEMA}.assignments(id, plan_id, request_id, submission_intent_id,
                        shape_instance_id, shape_slot_index, payload, wire_payload, payload_sha256)
                      VALUES(assignment_id, plan_id, request_id, (allowance->>'submission_intent_id')::uuid,
                        allowance->>'shape_instance_id', 0, record, record_wire, encode(sha256(record_wire),'hex'));
                    INSERT INTO {SCHEMA}.request_holds(request_id, assignment_id) VALUES(request_id, assignment_id);
                END IF;
                result := result || jsonb_build_array(record);
            END LOOP;
            IF (SELECT count(*) FROM {SCHEMA}.assignments a WHERE a.plan_id=guard.plan_id) <> item_count THEN
                RAISE EXCEPTION 'build plan assignment set changed';
            END IF;
            IF expires_at <= clock_timestamp() OR EXISTS (SELECT 1 FROM jsonb_array_elements(result) r
                WHERE (r->>'lease_not_after_epoch_microseconds')::bigint <= (extract(epoch FROM clock_timestamp())*1000000)::bigint) THEN
                RAISE EXCEPTION 'build plan or assignment lease expired during preparation';
            END IF;
            RETURN jsonb_build_object('proposal_digest', p_digest, 'assignments', result);
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON FUNCTION {FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {FUNCTION} TO {quote(agent)}")


def downgrade():
    op.execute(f"LOCK TABLE {SCHEMA}.plans IN ACCESS EXCLUSIVE MODE")
    op.execute(f"""DO $$ BEGIN IF EXISTS (SELECT 1 FROM {SCHEMA}.plans) THEN
        RAISE EXCEPTION 'cannot remove build preparation with retained plans'; END IF; END $$""")
    op.execute(f"DROP FUNCTION {FUNCTION}")
    prepare_v1_sql.uninstall()
