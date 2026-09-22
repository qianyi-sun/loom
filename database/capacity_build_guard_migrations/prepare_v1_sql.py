"""Frozen SQL contract checks for build_guard_0003's native-plan surface."""

from alembic import op


def install():
    op.execute(r"""
        CREATE FUNCTION loom_capacity_build_guard.canonical_plan_json(p jsonb) RETURNS text
        LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
        DECLARE result text;
        BEGIN
          CASE jsonb_typeof(p)
            WHEN 'object' THEN
              SELECT '{' || coalesce(string_agg(to_json(key)::text || ':' ||
                loom_capacity_build_guard.canonical_plan_json(value), ',' ORDER BY key COLLATE "C"),'') || '}'
                INTO result FROM jsonb_each(p);
            WHEN 'array' THEN
              SELECT '[' || coalesce(string_agg(loom_capacity_build_guard.canonical_plan_json(value), ',' ORDER BY ord),'') || ']'
                INTO result FROM jsonb_array_elements(p) WITH ORDINALITY AS a(value,ord);
            WHEN 'number' THEN
              result := p::text;
              IF result !~ '^(0|[1-9][0-9]*)$' OR result::numeric > 9223372036854775807 THEN
                RAISE EXCEPTION 'build plan contract number is not a bounded integer';
              END IF;
            ELSE result := p::text;
          END CASE;
          IF result IS NULL OR octet_length(result) <> length(result) THEN
            RAISE EXCEPTION 'build plan contract must contain only ASCII protocol fields';
          END IF;
          RETURN result;
        END $$;

        CREATE FUNCTION loom_capacity_build_guard.assert_plan_fields(
          p jsonb, fields text[], uuids text[], positives text[], identifiers text[], digests text[]
        ) RETURNS void LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
        DECLARE field text;
        BEGIN
          IF jsonb_typeof(p) IS DISTINCT FROM 'object' OR NOT p ?& fields
             OR p - fields <> '{}'::jsonb OR p->'schema_version' IS DISTINCT FROM '2'::jsonb THEN
            RAISE EXCEPTION 'build plan contract fields differ';
          END IF;
          FOREACH field IN ARRAY uuids LOOP
            IF jsonb_typeof(p->field) IS DISTINCT FROM 'string'
              OR (p->>field) !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
              RAISE EXCEPTION 'build plan contract UUID field is invalid';
            END IF;
          END LOOP;
          FOREACH field IN ARRAY positives LOOP
            IF jsonb_typeof(p->field) IS DISTINCT FROM 'number'
              OR (p->>field) !~ '^[1-9][0-9]*$' OR (p->>field)::numeric > 9223372036854775807 THEN
              RAISE EXCEPTION 'build plan contract positive field is invalid';
            END IF;
          END LOOP;
          FOREACH field IN ARRAY identifiers LOOP
            IF jsonb_typeof(p->field) IS DISTINCT FROM 'string'
              OR (p->>field) !~ '^[a-z0-9][a-z0-9_.-]{0,127}$' THEN
              RAISE EXCEPTION 'build plan contract identifier field is invalid';
            END IF;
          END LOOP;
          FOREACH field IN ARRAY digests LOOP
            IF jsonb_typeof(p->field) IS DISTINCT FROM 'string' OR (p->>field) !~ '^[0-9a-f]{64}$' THEN
              RAISE EXCEPTION 'build plan contract digest field is invalid';
            END IF;
          END LOOP;
        END $$;

        CREATE FUNCTION loom_capacity_build_guard.assert_plan_contract(p jsonb, wire bytea) RETURNS void
        LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
        DECLARE shape jsonb; binding jsonb; execution jsonb; allowance jsonb; anchor jsonb;
          expiry timestamptz; expiry_text text;
        BEGIN
          IF convert_to(loom_capacity_build_guard.canonical_plan_json(p),'UTF8') IS DISTINCT FROM wire THEN
            RAISE EXCEPTION 'build plan canonical wire changed';
          END IF;
          PERFORM loom_capacity_build_guard.assert_plan_fields(p,
            ARRAY['schema_version','proposal_id','plan_id','admission_incarnation','reporter_incarnation',
              'protected_admission_sha256','manager_input_digest','manager_allocation_digest','lease_not_after','shapes','allowances','executable'],
            ARRAY['proposal_id','plan_id','admission_incarnation','reporter_incarnation'], ARRAY[]::text[], ARRAY[]::text[],
            ARRAY['protected_admission_sha256','manager_input_digest','manager_allocation_digest']);
          IF jsonb_typeof(p->'lease_not_after') IS DISTINCT FROM 'string' THEN
            RAISE EXCEPTION 'build plan contract lease field is invalid';
          END IF;
          expiry := (p->>'lease_not_after')::timestamptz;
          expiry_text := to_char(expiry AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') ||
            CASE WHEN extract(microseconds FROM expiry)::bigint % 1000000=0 THEN ''
              ELSE '.' || to_char(expiry AT TIME ZONE 'UTC','US') END || 'Z';
          IF p->>'lease_not_after' IS DISTINCT FROM expiry_text THEN
            RAISE EXCEPTION 'build plan canonical lease field changed';
          END IF;
          IF p->'shapes' IS DISTINCT FROM (SELECT jsonb_agg(value ORDER BY (value->'binding'->>'shape_instance_id') COLLATE "C") FROM jsonb_array_elements(p->'shapes'))
            OR p->'allowances' IS DISTINCT FROM (SELECT coalesce(jsonb_agg(value ORDER BY (value->>'allowance_id') COLLATE "C"),'[]'::jsonb) FROM jsonb_array_elements(p->'allowances')) THEN
            RAISE EXCEPTION 'build plan canonical array order changed';
          END IF;
          anchor := p->'shapes'->0->'binding';
          IF (SELECT count(DISTINCT value) FROM unnest(ARRAY[p->>'proposal_id', p->>'plan_id', p->>'admission_incarnation',
              p->>'reporter_incarnation', anchor->'execution'->>'authority_incarnation', anchor->>'tranche_id',
              anchor->>'subject_id', anchor->>'subject_incarnation', anchor->>'executor_incarnation']) value) <> 9 THEN
            RAISE EXCEPTION 'build plan contract authority identities collide';
          END IF;
          FOR shape IN SELECT value FROM jsonb_array_elements(p->'shapes') LOOP
            PERFORM loom_capacity_build_guard.assert_plan_fields(shape,
              ARRAY['schema_version','binding','protocol_generation','protocol_digest','worker_shape','worker_shape_digest','bootstrap_registration_epoch'],
              ARRAY[]::text[], ARRAY['protocol_generation','bootstrap_registration_epoch'], ARRAY[]::text[], ARRAY['protocol_digest','worker_shape_digest']);
            IF shape->>'worker_shape_digest' IS DISTINCT FROM encode(sha256(convert_to(
                loom_capacity_build_guard.canonical_plan_json(shape->'worker_shape'),'UTF8')),'hex') THEN
              RAISE EXCEPTION 'build plan contract worker shape digest changed';
            END IF;
            binding := shape->'binding';
            PERFORM loom_capacity_build_guard.assert_plan_fields(binding,
              ARRAY['schema_version','execution','tranche_id','intent_id','shape_instance_id','subject_id','subject_incarnation',
                'account_id','tier_id','candidate','candidate_generation','deployment_generation','pool_id','pool_generation','executor_id',
                'executor_incarnation','shape_id','profile_id','profile_generation','profile_digest','concurrency_slots','resources','node_ids',
                'rollout_surge_slots','old_shape_backing_id'],
              ARRAY['tranche_id','intent_id','subject_id','subject_incarnation','executor_incarnation'],
              ARRAY['candidate_generation','deployment_generation','pool_generation','profile_generation','concurrency_slots'],
              ARRAY['shape_instance_id','account_id','pool_id','executor_id','shape_id','profile_id'], ARRAY['profile_digest']);
            execution := binding->'execution';
            PERFORM loom_capacity_build_guard.assert_plan_fields(execution,
              ARRAY['schema_version','authority_incarnation','writer_epoch','configuration_epoch','execution_epoch','execution_manifest_sha256',
                'execution_state','executable_new_capacity_ceiling','executable_new_capacity_rate_per_minute','trusted_fleet_release_sha256',
                'executable','allocation_epoch'], ARRAY['authority_incarnation'],
              ARRAY['writer_epoch','configuration_epoch','execution_epoch','executable_new_capacity_ceiling',
                'executable_new_capacity_rate_per_minute','allocation_epoch'], ARRAY[]::text[],
              ARRAY['execution_manifest_sha256','trusted_fleet_release_sha256']);
            IF execution->'executable' IS DISTINCT FROM 'true'::jsonb THEN
              RAISE EXCEPTION 'build plan contract execution flag changed';
            END IF;
          END LOOP;
          FOR allowance IN SELECT value FROM jsonb_array_elements(p->'allowances') LOOP
            PERFORM loom_capacity_build_guard.assert_plan_fields(allowance,
              ARRAY['schema_version','allowance_id','protected_attempt_id','shape_instance_id','shape_slot_index','submission_intent_id'],
              ARRAY['allowance_id','protected_attempt_id','submission_intent_id'], ARRAY[]::text[], ARRAY['shape_instance_id'], ARRAY[]::text[]);
          END LOOP;
        END $$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    for signature in ("canonical_plan_json(jsonb)", "assert_plan_fields(jsonb,text[],text[],text[],text[],text[])", "assert_plan_contract(jsonb,bytea)"):
        op.execute(f"REVOKE ALL ON FUNCTION loom_capacity_build_guard.{signature} FROM PUBLIC, {quote(agent)}")


def uninstall():
    for signature in ("assert_plan_contract(jsonb,bytea)", "assert_plan_fields(jsonb,text[],text[],text[],text[],text[])", "canonical_plan_json(jsonb)"):
        op.execute(f"DROP FUNCTION loom_capacity_build_guard.{signature}")
