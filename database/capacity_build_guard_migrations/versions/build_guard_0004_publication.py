"""Authorize exact publication while retaining current source and assignment locks.

Revision ID: build_guard_0004
Revises: build_guard_0003
"""

from alembic import op

revision = "build_guard_0004"
down_revision = "build_guard_0003"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = f"{SCHEMA}.authorize_publication(uuid,uuid)"


def upgrade():
    # Full top-level xid (not xmin, which can be a subtransaction ID). Existing
    # plans receive this migration's xid and become publishable after it commits.
    op.execute(f"ALTER TABLE {SCHEMA}.plans ADD COLUMN preparation_xid xid8 NOT NULL DEFAULT pg_current_xact_id()")
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.authorize_publication(p_installation uuid, p_plan uuid)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE
            installation {SCHEMA}.installations%ROWTYPE;
            plan {SCHEMA}.plans%ROWTYPE;
            assignment {SCHEMA}.assignments%ROWTYPE;
            publication {SCHEMA}.dispositions%ROWTYPE;
            allowance jsonb;
            record jsonb;
            source_wire bytea;
            current_lease timestamptz;
            retained_lease bigint;
            assignments jsonb := '[]'::jsonb;
            ack_assignments jsonb := '[]'::jsonb;
            anchor jsonb;
            prepared jsonb;
            ack jsonb;
            wire bytea;
            digest text;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build publication requires serializable transaction';
            END IF;
            SELECT * INTO installation FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build publication installation is absent'; END IF;
            SELECT * INTO plan FROM {SCHEMA}.plans WHERE id=p_plan FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build publication plan is absent'; END IF;
            IF plan.preparation_xid = pg_current_xact_id() THEN
                RAISE EXCEPTION 'build publication requires committed preparation';
            END IF;
            IF plan.installation_id IS DISTINCT FROM p_installation
                OR plan.payload->>'plan_id' IS DISTINCT FROM p_plan::text
                OR plan.payload->>'reporter_incarnation' IS DISTINCT FROM installation.reporter_incarnation::text
                OR plan.payload->'protected_admission_sha256' IS DISTINCT FROM installation.payload->'protected_admission_sha256'
                OR plan.payload IS DISTINCT FROM convert_from(plan.wire_payload,'UTF8')::jsonb
                OR plan.payload_sha256 IS DISTINCT FROM encode(sha256(plan.wire_payload),'hex') THEN
                RAISE EXCEPTION 'build publication plan installation binding changed';
            END IF;
            PERFORM {SCHEMA}.assert_plan_contract(plan.payload, plan.wire_payload);
            IF EXISTS (SELECT 1 FROM {SCHEMA}.dispositions WHERE plan_id=p_plan AND kind <> 'publication') THEN
                RAISE EXCEPTION 'build publication plan has a terminal disposition';
            END IF;
            IF (SELECT count(*) FROM {SCHEMA}.assignments WHERE plan_id=p_plan)
                    <> jsonb_array_length(plan.payload->'allowances') THEN
                RAISE EXCEPTION 'build publication assignment set changed';
            END IF;
            -- Match preparation and management lock ordering, including when
            -- multiple requests refer to the same parent or candidate.
            PERFORM a.id FROM public.personal_dev_candidate_build_attempts a
                WHERE a.id IN (SELECT r.attempt_id FROM public.personal_dev_build_platform_requests r
                    JOIN {SCHEMA}.assignments s ON s.request_id=r.id WHERE s.plan_id=p_plan)
                ORDER BY a.id FOR UPDATE;
            PERFORM c.id FROM public.personal_dev_candidates c
                WHERE c.id IN (SELECT r.candidate_id FROM public.personal_dev_build_platform_requests r
                    JOIN {SCHEMA}.assignments s ON s.request_id=r.id WHERE s.plan_id=p_plan)
                ORDER BY c.id FOR UPDATE;
            PERFORM r.id FROM public.personal_dev_build_platform_requests r
                WHERE r.id IN (SELECT request_id FROM {SCHEMA}.assignments WHERE plan_id=p_plan)
                ORDER BY r.id FOR UPDATE;
            FOR allowance IN SELECT value FROM jsonb_array_elements(plan.payload->'allowances')
                ORDER BY value->>'allowance_id' LOOP
                SELECT * INTO assignment FROM {SCHEMA}.assignments
                    WHERE plan_id=p_plan AND request_id=(allowance->>'protected_attempt_id')::uuid FOR UPDATE;
                IF NOT FOUND THEN RAISE EXCEPTION 'build publication assignment is absent'; END IF;
                record := assignment.payload;
                source_wire := convert_to(record->>'source_canonical_json','UTF8');
                current_lease := {SCHEMA}.assert_current_source(p_installation, assignment.request_id,
                    convert_from(source_wire,'UTF8')::jsonb, source_wire, record->>'source_binding_sha256');
                retained_lease := (record->>'lease_not_after_epoch_microseconds')::bigint;
                IF assignment.wire_payload IS NULL
                    OR convert_from(assignment.wire_payload,'UTF8')::jsonb IS DISTINCT FROM record
                    OR encode(sha256(assignment.wire_payload),'hex') IS DISTINCT FROM assignment.payload_sha256
                    OR record->>'id' IS DISTINCT FROM assignment.id::text
                    OR record->>'plan_id' IS DISTINCT FROM p_plan::text
                    OR record->>'request_id' IS DISTINCT FROM assignment.request_id::text
                    OR record->'allowance_id' IS DISTINCT FROM allowance->'allowance_id'
                    OR record->'submission_intent_id' IS DISTINCT FROM allowance->'submission_intent_id'
                    OR record->>'submission_intent_id' IS DISTINCT FROM assignment.submission_intent_id::text
                    OR record->'shape_instance_id' IS DISTINCT FROM allowance->'shape_instance_id'
                    OR record->>'shape_instance_id' IS DISTINCT FROM assignment.shape_instance_id
                    OR record->'shape_slot_index' IS DISTINCT FROM '0'::jsonb OR assignment.shape_slot_index <> 0
                    OR record->'execution_generation' IS DISTINCT FROM convert_from(source_wire,'UTF8')::jsonb->'lease_epoch'
                    OR record->'runtime_installation_sha256' IS DISTINCT FROM installation.payload->'runtime_installation_sha256'
                    OR jsonb_typeof(record->'request_sequence') IS DISTINCT FROM 'number'
                    OR (record->>'request_sequence')::bigint <= 0
                    OR jsonb_typeof(record->'lease_not_after_epoch_microseconds') IS DISTINCT FROM 'number'
                    OR retained_lease <= (extract(epoch FROM clock_timestamp())*1000000)::bigint
                    OR retained_lease > (extract(epoch FROM LEAST(current_lease,plan.expires_at))*1000000)::bigint
                    OR NOT EXISTS (SELECT 1 FROM {SCHEMA}.request_holds
                        WHERE request_id=assignment.request_id AND assignment_id=assignment.id) THEN
                    RAISE EXCEPTION 'build publication assignment source, lease or hold changed';
                END IF;
                assignments := assignments || jsonb_build_array(record);
                ack_assignments := ack_assignments || jsonb_build_array(jsonb_build_object(
                    'schema_version',2,'transition_id',assignment.id,'allowance_id',allowance->'allowance_id',
                    'protected_attempt_id',assignment.request_id,'execution_generation',record->'execution_generation',
                    'requirements_digest',record->'source_binding_sha256','shape_instance_id',assignment.shape_instance_id,
                    'shape_slot_index',0,'submission_intent_id',assignment.submission_intent_id,
                    'lifecycle_sequence',record->'request_sequence'));
            END LOOP;
            prepared := jsonb_build_object('schema_version',1,'installation_id',p_installation,
                'proposal',plan.payload,'assignments',assignments);
            anchor := plan.payload->'shapes'->0->'binding';
            ack := jsonb_build_object('schema_version',2,'executable',true,
                'execution',anchor->'execution','tranche_id',anchor->'tranche_id',
                'proposal_id',plan.payload->'proposal_id','plan_id',p_plan,
                'admission_incarnation',plan.payload->'admission_incarnation',
                'subject_id',anchor->'subject_id','subject_incarnation',anchor->'subject_incarnation',
                'pool_id',anchor->'pool_id','reporter_incarnation',plan.payload->'reporter_incarnation',
                'protected_admission_sha256',plan.payload->'protected_admission_sha256',
                'proposal_digest',plan.payload_sha256,
                'prepared_plan_digest',encode(sha256(convert_to({SCHEMA}.canonical_plan_json(prepared),'UTF8')),'hex'),
                'assignment_count',jsonb_array_length(assignments),'assignments',ack_assignments);
            wire := convert_to({SCHEMA}.canonical_plan_json(ack),'UTF8');
            digest := encode(sha256(wire),'hex');
            IF (SELECT count(*) FROM {SCHEMA}.dispositions WHERE plan_id=p_plan AND kind='publication') > 1 THEN
                RAISE EXCEPTION 'build publication disposition set changed';
            END IF;
            SELECT * INTO publication FROM {SCHEMA}.dispositions WHERE plan_id=p_plan AND kind='publication';
            IF FOUND THEN
                IF publication.payload IS DISTINCT FROM ack OR publication.wire_payload IS DISTINCT FROM wire
                    OR publication.payload_sha256 IS DISTINCT FROM digest THEN
                    RAISE EXCEPTION 'build publication replay binding changed';
                END IF;
            ELSE
                INSERT INTO {SCHEMA}.dispositions(id,plan_id,kind,payload,wire_payload,payload_sha256)
                    VALUES(gen_random_uuid(),p_plan,'publication',ack,wire,digest);
            END IF;
            IF plan.expires_at <= clock_timestamp() OR EXISTS (SELECT 1 FROM jsonb_array_elements(assignments) r
                WHERE (r->>'lease_not_after_epoch_microseconds')::bigint <= (extract(epoch FROM clock_timestamp())*1000000)::bigint) THEN
                RAISE EXCEPTION 'build publication lease expired during authorization';
            END IF;
            RETURN convert_from(wire,'UTF8');
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON FUNCTION {FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {FUNCTION} TO {quote(agent)}")


def downgrade():
    op.execute(f"LOCK TABLE {SCHEMA}.dispositions IN ACCESS EXCLUSIVE MODE")
    op.execute(f"""DO $$ BEGIN IF EXISTS (SELECT 1 FROM {SCHEMA}.dispositions) THEN
        RAISE EXCEPTION 'cannot remove build publication with retained evidence'; END IF; END $$""")
    op.execute(f"DROP FUNCTION {FUNCTION}")
    op.execute(f"ALTER TABLE {SCHEMA}.plans DROP COLUMN preparation_xid")
