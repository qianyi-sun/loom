"""Retain exact manager closure independently of expired source or physical release.

Revision ID: build_guard_0005
Revises: build_guard_0004
"""

from alembic import op

revision = "build_guard_0005"
down_revision = "build_guard_0004"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTIONS = ("close_plan(uuid,jsonb,bytea,text)", "authorize_closure_publication(uuid,uuid)")


def upgrade():
    op.execute(f"ALTER TABLE {SCHEMA}.dispositions ADD COLUMN retention_xid xid8 NOT NULL DEFAULT pg_current_xact_id()")
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.assert_native_closure_plan(p jsonb, installation jsonb)
        RETURNS void LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $function$
        DECLARE
            item_count integer;
            anchor jsonb;
            shape jsonb;
            binding jsonb;
            profile jsonb;
            native_pool jsonb;
            allowance jsonb;
        BEGIN
            item_count := jsonb_array_length(p->'shapes');
            IF p->'executable' IS DISTINCT FROM 'true'::jsonb OR item_count NOT BETWEEN 1 AND 10000
                OR jsonb_array_length(p->'allowances') <> item_count
                OR item_count <> (SELECT count(DISTINCT value->'binding'->>'shape_instance_id') FROM jsonb_array_elements(p->'shapes'))
                OR item_count <> (SELECT count(DISTINCT value->'binding'->>'intent_id') FROM jsonb_array_elements(p->'shapes'))
                OR item_count <> (SELECT count(DISTINCT value->>'allowance_id') FROM jsonb_array_elements(p->'allowances'))
                OR item_count <> (SELECT count(DISTINCT value->>'protected_attempt_id') FROM jsonb_array_elements(p->'allowances'))
                OR item_count <> (SELECT count(DISTINCT value->>'shape_instance_id') FROM jsonb_array_elements(p->'allowances')) THEN
                RAISE EXCEPTION 'build closure native proposal contract changed';
            END IF;
            anchor := p->'shapes'->0->'binding';
            FOR shape IN SELECT value FROM jsonb_array_elements(p->'shapes') LOOP
                binding := shape->'binding';
                SELECT value INTO profile FROM jsonb_array_elements(installation->'runtime'->'profiles')
                    WHERE value->>'pool_id'=binding->>'pool_id';
                IF NOT FOUND THEN RAISE EXCEPTION 'build closure native profile binding is absent'; END IF;
                SELECT value INTO native_pool FROM jsonb_array_elements(installation->'runtime'->'pools')
                    WHERE value->>'pool_id'=binding->>'pool_id';
                IF NOT FOUND THEN RAISE EXCEPTION 'build closure native pool binding is absent'; END IF;
                IF binding->'subject_id' IS DISTINCT FROM installation->'subject_id'
                    OR binding->'subject_incarnation' IS DISTINCT FROM installation->'subject_incarnation'
                    OR binding->'deployment_generation' IS DISTINCT FROM installation->'deployment_generation'
                    OR binding->'candidate_generation' IS DISTINCT FROM installation->'candidate_generation'
                    OR binding->>'account_id' IS DISTINCT FROM 'dev-owner-' || replace(installation->>'owner_user_id','-','')
                    OR binding->>'tier_id' IS DISTINCT FROM 'development'
                    OR binding->'candidate' IS DISTINCT FROM installation->'runtime'->'candidate'
                    OR binding->'execution'->>'execution_state' IS DISTINCT FROM 'active'
                    OR binding->'execution'->'execution_manifest_sha256' IS DISTINCT FROM installation->'runtime'->'execution_manifest_sha256'
                    OR binding->'execution'->'trusted_fleet_release_sha256' IS DISTINCT FROM installation->'runtime'->'trusted_fleet_release_sha256'
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
                    OR binding->'old_shape_backing_id' IS DISTINCT FROM 'null'::jsonb
                    OR jsonb_typeof(binding->'node_ids') IS DISTINCT FROM 'array'
                    OR jsonb_array_length(binding->'node_ids') <> 1
                    OR NOT ((native_pool->'node_ids') @> (binding->'node_ids'))
                    OR binding->'shape_id' IS DISTINCT FROM profile->'worker_shapes'->0->'shape_id'
                    OR binding->'resources' IS DISTINCT FROM profile->'worker_shapes'->0->'total_resources'
                    OR shape->'worker_shape' IS DISTINCT FROM profile->'worker_shapes'->0
                    OR shape->'protocol_generation' IS DISTINCT FROM profile->'protocol_generation'
                    OR shape->'protocol_digest' IS DISTINCT FROM profile->'protocol_digest' THEN
                    RAISE EXCEPTION 'build closure native installation binding changed';
                END IF;
            END LOOP;
            FOR allowance IN SELECT value FROM jsonb_array_elements(p->'allowances') LOOP
                SELECT value INTO shape FROM jsonb_array_elements(p->'shapes')
                    WHERE value->'binding'->'shape_instance_id'=allowance->'shape_instance_id';
                IF NOT FOUND OR allowance->'shape_slot_index' IS DISTINCT FROM '0'::jsonb
                    OR allowance->'submission_intent_id' IS DISTINCT FROM shape->'binding'->'intent_id' THEN
                    RAISE EXCEPTION 'build closure allowance binding changed';
                END IF;
            END LOOP;
        END $function$;

        CREATE FUNCTION {SCHEMA}.close_plan(p_installation uuid, p_closure jsonb, p_wire bytea, p_digest text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        <<guard>>
        DECLARE
            installation {SCHEMA}.installations%ROWTYPE;
            plan {SCHEMA}.plans%ROWTYPE;
            disposition {SCHEMA}.dispositions%ROWTYPE;
            proposal jsonb;
            proposal_wire bytea;
            anchor jsonb;
            plan_id uuid;
            ids jsonb;
            record jsonb;
            wire bytea;
            digest text;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build closure requires serializable transaction';
            END IF;
            IF p_wire IS NULL OR octet_length(p_wire) NOT BETWEEN 2 AND 1048576
                OR p_closure IS DISTINCT FROM convert_from(p_wire,'UTF8')::jsonb
                OR p_digest IS DISTINCT FROM encode(sha256(p_wire),'hex')
                OR convert_from(p_wire,'UTF8') IS DISTINCT FROM {SCHEMA}.canonical_plan_json(p_closure) THEN
                RAISE EXCEPTION 'build closure canonical wire changed';
            END IF;
            PERFORM {SCHEMA}.assert_plan_fields(p_closure,
                ARRAY['schema_version','closure_id','proposal','close_reason','executable'],
                ARRAY['closure_id'],ARRAY[]::text[],ARRAY[]::text[],ARRAY[]::text[]);
            IF p_closure->'executable' IS DISTINCT FROM 'false'::jsonb
                OR coalesce(p_closure->>'close_reason','') NOT IN ('expired','allocation-superseded','manager-closed') THEN
                RAISE EXCEPTION 'build closure contract is invalid';
            END IF;
            proposal := p_closure->'proposal';
            proposal_wire := convert_to({SCHEMA}.canonical_plan_json(proposal),'UTF8');
            PERFORM {SCHEMA}.assert_plan_contract(proposal,proposal_wire);
            anchor := proposal->'shapes'->0->'binding';
            plan_id := (proposal->>'plan_id')::uuid;
            IF p_closure->>'closure_id' = ANY(ARRAY[proposal->>'proposal_id',proposal->>'plan_id',
                proposal->>'admission_incarnation',anchor->>'tranche_id',anchor->>'subject_id',
                anchor->>'subject_incarnation',proposal->>'reporter_incarnation']) THEN
                RAISE EXCEPTION 'build closure identity is not distinct';
            END IF;
            SELECT * INTO installation FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build closure installation is absent'; END IF;
            PERFORM {SCHEMA}.assert_native_closure_plan(proposal,installation.payload);
            IF proposal->>'reporter_incarnation' IS DISTINCT FROM installation.reporter_incarnation::text
                OR proposal->'protected_admission_sha256' IS DISTINCT FROM installation.payload->'protected_admission_sha256'
                OR anchor->>'subject_id' IS DISTINCT FROM installation.subject_id::text
                OR anchor->>'subject_incarnation' IS DISTINCT FROM installation.subject_incarnation::text
                OR anchor->>'deployment_generation' IS DISTINCT FROM installation.deployment_generation::text
                OR anchor->'candidate' IS DISTINCT FROM installation.payload->'runtime'->'candidate' THEN
                RAISE EXCEPTION 'build closure installation binding changed';
            END IF;
            SELECT * INTO plan FROM {SCHEMA}.plans p WHERE p.id=plan_id FOR UPDATE;
            IF FOUND THEN
                IF plan.installation_id IS DISTINCT FROM p_installation OR plan.payload IS DISTINCT FROM proposal
                    OR plan.wire_payload IS DISTINCT FROM proposal_wire
                    OR plan.payload_sha256 IS DISTINCT FROM encode(sha256(proposal_wire),'hex') THEN
                    RAISE EXCEPTION 'build closure plan replay binding changed';
                END IF;
            ELSE
                -- A manager can close a plan that never reached this agent.
                -- Retain that terminal identity without assigning any request.
                INSERT INTO {SCHEMA}.plans(id,installation_id,expires_at,payload,wire_payload,payload_sha256)
                    VALUES(plan_id,p_installation,(proposal->>'lease_not_after')::timestamptz,
                        proposal,proposal_wire,encode(sha256(proposal_wire),'hex'));
            END IF;
            SELECT coalesce(jsonb_agg(id ORDER BY id),'[]'::jsonb) INTO ids
                FROM {SCHEMA}.assignments a WHERE a.plan_id=guard.plan_id;
            record := jsonb_build_object('schema_version',1,'installation_id',p_installation,
                'closure',p_closure,'disposition_kind',CASE WHEN jsonb_array_length(ids)=0 THEN 'never-converged' ELSE 'abandoned' END,
                'assignment_ids',ids);
            wire := convert_to({SCHEMA}.canonical_plan_json(record),'UTF8');
            digest := encode(sha256(wire),'hex');
            IF (SELECT count(*) FROM {SCHEMA}.dispositions d WHERE d.plan_id=guard.plan_id AND kind='closure') > 1 THEN
                RAISE EXCEPTION 'build closure disposition set changed';
            END IF;
            SELECT * INTO disposition FROM {SCHEMA}.dispositions d WHERE d.plan_id=guard.plan_id AND kind='closure';
            IF FOUND THEN
                IF disposition.id IS DISTINCT FROM (p_closure->>'closure_id')::uuid
                    OR disposition.payload IS DISTINCT FROM record OR disposition.wire_payload IS DISTINCT FROM wire
                    OR disposition.payload_sha256 IS DISTINCT FROM digest THEN
                    RAISE EXCEPTION 'build closure replay binding changed';
                END IF;
            ELSE
                IF EXISTS (SELECT 1 FROM {SCHEMA}.dispositions d WHERE d.plan_id=guard.plan_id AND kind='release') THEN
                    RAISE EXCEPTION 'build closure cannot change a released plan';
                END IF;
                INSERT INTO {SCHEMA}.dispositions(id,plan_id,kind,payload,wire_payload,payload_sha256)
                    VALUES((p_closure->>'closure_id')::uuid,plan_id,'closure',record,wire,digest);
            END IF;
            RETURN convert_from(wire,'UTF8');
        END $function$;

        CREATE FUNCTION {SCHEMA}.authorize_closure_publication(p_installation uuid, p_plan uuid)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE
            plan {SCHEMA}.plans%ROWTYPE;
            disposition {SCHEMA}.dispositions%ROWTYPE;
            closure jsonb;
            anchor jsonb;
            ack jsonb;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build closure publication requires serializable transaction';
            END IF;
            PERFORM id FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build closure installation is absent'; END IF;
            SELECT * INTO plan FROM {SCHEMA}.plans WHERE id=p_plan FOR UPDATE;
            IF NOT FOUND THEN
                RAISE EXCEPTION USING ERRCODE='P0002', MESSAGE='build closure plan is absent';
            END IF;
            IF plan.installation_id IS DISTINCT FROM p_installation THEN
                RAISE EXCEPTION 'build closure plan installation changed';
            END IF;
            SELECT * INTO disposition FROM {SCHEMA}.dispositions WHERE plan_id=p_plan AND kind='closure';
            IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0002', MESSAGE='build closure is absent'; END IF;
            IF disposition.retention_xid=pg_current_xact_id() THEN
                RAISE EXCEPTION 'build closure publication requires committed closure';
            END IF;
            closure := disposition.payload->'closure';
            IF closure->'proposal' IS DISTINCT FROM plan.payload
                OR disposition.payload->>'installation_id' IS DISTINCT FROM p_installation::text
                OR closure->>'closure_id' IS DISTINCT FROM disposition.id::text
                OR disposition.wire_payload IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(disposition.payload),'UTF8')
                OR disposition.payload_sha256 IS DISTINCT FROM encode(sha256(disposition.wire_payload),'hex') THEN
                RAISE EXCEPTION 'build closure publication retained evidence changed';
            END IF;
            anchor := plan.payload->'shapes'->0->'binding';
            ack := jsonb_build_object('schema_version',2,'executable',false,
                'closure_id',disposition.id,'proposal_id',plan.payload->'proposal_id',
                'proposal_digest',plan.payload_sha256,'plan_id',p_plan,
                'admission_incarnation',plan.payload->'admission_incarnation',
                'subject_id',anchor->'subject_id','subject_incarnation',anchor->'subject_incarnation',
                'reporter_incarnation',plan.payload->'reporter_incarnation',
                'protected_admission_sha256',plan.payload->'protected_admission_sha256',
                'close_reason',closure->'close_reason','disposition_kind',disposition.payload->'disposition_kind',
                'disposition_digest',disposition.payload_sha256);
            RETURN {SCHEMA}.canonical_plan_json(ack);
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.assert_native_closure_plan(jsonb,jsonb) FROM PUBLIC, {quote(agent)}")
    for signature in FUNCTIONS:
        op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{signature} FROM PUBLIC")
        op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{signature} TO {quote(agent)}")


def downgrade():
    op.execute(f"LOCK TABLE {SCHEMA}.dispositions IN ACCESS EXCLUSIVE MODE")
    op.execute(f"""DO $$ BEGIN IF EXISTS (SELECT 1 FROM {SCHEMA}.dispositions WHERE kind='closure') THEN
        RAISE EXCEPTION 'cannot remove build closure with retained evidence'; END IF; END $$""")
    for signature in reversed(FUNCTIONS):
        op.execute(f"DROP FUNCTION {SCHEMA}.{signature}")
    op.execute(f"DROP FUNCTION {SCHEMA}.assert_native_closure_plan(jsonb,jsonb)")
    op.execute(f"ALTER TABLE {SCHEMA}.dispositions DROP COLUMN retention_xid")
