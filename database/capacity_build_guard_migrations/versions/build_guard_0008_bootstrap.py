"""Protect epoch-one native bootstrap hashes before manager plan creation.

Revision ID: build_guard_0008
Revises: build_guard_0007
"""

import sqlalchemy as sa
from alembic import op

from capacity_build_guard_migrations.versions.build_guard_0001_assignments import _payload

revision = "build_guard_0008"
down_revision = "build_guard_0007"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTIONS = ("register_bootstrap(uuid,jsonb,bytea,text)", "authorize_bootstrap_publication(uuid,uuid)")


def upgrade():
    op.create_table("bootstraps",
        sa.Column("intent_id", sa.Uuid(), primary_key=True),
        sa.Column("installation_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.installations.id"), nullable=False),
        sa.Column("proposal_sha256", sa.Text(), nullable=False),
        *_payload(), schema=SCHEMA)
    op.execute(f"ALTER TABLE {SCHEMA}.bootstraps ADD COLUMN retention_xid xid8 NOT NULL DEFAULT pg_current_xact_id()")
    op.execute(f"CREATE TRIGGER build_bootstraps_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON {SCHEMA}.bootstraps FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_evidence_mutation()")
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.assert_bootstrap(p jsonb, wire bytea, digest text, installation jsonb)
        RETURNS void LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $function$
        DECLARE binding jsonb; execution jsonb; profile jsonb; native_pool jsonb; expiry timestamptz;
        BEGIN
            IF octet_length(wire) NOT BETWEEN 2 AND 1048576
                OR wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR encode(sha256(wire),'hex') IS DISTINCT FROM digest THEN
                RAISE EXCEPTION 'build bootstrap canonical proposal changed';
            END IF;
            PERFORM {SCHEMA}.assert_plan_fields(p,
                ARRAY['schema_version','binding','command_sequence','proposal_epoch','bootstrap_sha256','expires_at','executable'],
                ARRAY[]::text[],ARRAY['command_sequence','proposal_epoch'],ARRAY[]::text[],ARRAY['bootstrap_sha256']);
            IF p->'executable' IS DISTINCT FROM 'true'::jsonb OR p->'proposal_epoch' IS DISTINCT FROM '1'::jsonb
                OR p->>'bootstrap_sha256'=repeat('0',64) OR jsonb_typeof(p->'expires_at') IS DISTINCT FROM 'string' THEN
                RAISE EXCEPTION 'build bootstrap requires epoch one hash-only proposal';
            END IF;
            expiry := (p->>'expires_at')::timestamptz;
            IF p->>'expires_at' IS DISTINCT FROM {SCHEMA}.demand_timestamp(expiry)
                OR expiry <= clock_timestamp() OR expiry > clock_timestamp()+interval '10 minutes' THEN
                RAISE EXCEPTION 'build bootstrap proposal expiry changed';
            END IF;
            binding := p->'binding'; execution := binding->'execution';
            PERFORM {SCHEMA}.assert_plan_fields(binding,
                ARRAY['schema_version','execution','tranche_id','intent_id','shape_instance_id','subject_id','subject_incarnation',
                    'account_id','tier_id','candidate','candidate_generation','deployment_generation','pool_id','pool_generation','executor_id',
                    'executor_incarnation','shape_id','profile_id','profile_generation','profile_digest','concurrency_slots','resources','node_ids',
                    'rollout_surge_slots','old_shape_backing_id'],
                ARRAY['tranche_id','intent_id','subject_id','subject_incarnation','executor_incarnation'],
                ARRAY['candidate_generation','deployment_generation','pool_generation','profile_generation','concurrency_slots'],
                ARRAY['shape_instance_id','account_id','pool_id','executor_id','shape_id','profile_id'],ARRAY['profile_digest']);
            PERFORM {SCHEMA}.assert_plan_fields(execution,
                ARRAY['schema_version','authority_incarnation','writer_epoch','configuration_epoch','execution_epoch','execution_manifest_sha256',
                    'execution_state','executable_new_capacity_ceiling','executable_new_capacity_rate_per_minute','trusted_fleet_release_sha256',
                    'executable','allocation_epoch'],ARRAY['authority_incarnation'],
                ARRAY['writer_epoch','configuration_epoch','execution_epoch','executable_new_capacity_ceiling',
                    'executable_new_capacity_rate_per_minute','allocation_epoch'],ARRAY[]::text[],
                ARRAY['execution_manifest_sha256','trusted_fleet_release_sha256']);
            SELECT value INTO profile FROM jsonb_array_elements(installation->'runtime'->'profiles')
                WHERE value->>'pool_id'=binding->>'pool_id';
            IF NOT FOUND THEN RAISE EXCEPTION 'build bootstrap native profile is absent'; END IF;
            SELECT value INTO native_pool FROM jsonb_array_elements(installation->'runtime'->'pools')
                WHERE value->>'pool_id'=binding->>'pool_id';
            IF NOT FOUND THEN RAISE EXCEPTION 'build bootstrap native pool is absent'; END IF;
            IF binding->'subject_id' IS DISTINCT FROM installation->'subject_id'
                OR binding->'subject_incarnation' IS DISTINCT FROM installation->'subject_incarnation'
                OR binding->'deployment_generation' IS DISTINCT FROM installation->'deployment_generation'
                OR binding->'candidate_generation' IS DISTINCT FROM installation->'candidate_generation'
                OR binding->>'account_id' IS DISTINCT FROM 'dev-owner-' || replace(installation->>'owner_user_id','-','')
                OR binding->>'tier_id' IS DISTINCT FROM 'development'
                OR binding->'candidate' IS DISTINCT FROM installation->'runtime'->'candidate'
                OR execution->>'execution_state' IS DISTINCT FROM 'active' OR execution->'executable' IS DISTINCT FROM 'true'::jsonb
                OR execution->'execution_manifest_sha256' IS DISTINCT FROM installation->'runtime'->'execution_manifest_sha256'
                OR execution->'trusted_fleet_release_sha256' IS DISTINCT FROM installation->'runtime'->'trusted_fleet_release_sha256'
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
                OR binding->'resources' IS DISTINCT FROM profile->'worker_shapes'->0->'total_resources' THEN
                RAISE EXCEPTION 'build bootstrap native installation binding changed';
            END IF;
        END $function$;

        CREATE FUNCTION {SCHEMA}.register_bootstrap(p_installation uuid, p jsonb, wire bytea, digest text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE installation {SCHEMA}.installations%ROWTYPE; existing {SCHEMA}.bootstraps%ROWTYPE;
            receipt jsonb; receipt_wire bytea; intent uuid;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build bootstrap requires serializable transaction';
            END IF;
            SELECT * INTO installation FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build bootstrap installation is absent'; END IF;
            PERFORM {SCHEMA}.assert_bootstrap(p,wire,digest,installation.payload);
            intent := (p->'binding'->>'intent_id')::uuid;
            receipt := jsonb_build_object('schema_version',1,'installation_id',p_installation,
                'proposal',p,'bootstrap_registration_epoch',1,'executable',false);
            receipt_wire := convert_to({SCHEMA}.canonical_plan_json(receipt),'UTF8');
            SELECT * INTO existing FROM {SCHEMA}.bootstraps WHERE intent_id=intent FOR UPDATE;
            IF FOUND THEN
                IF existing.installation_id IS DISTINCT FROM p_installation OR existing.proposal_sha256 IS DISTINCT FROM digest
                    OR existing.payload IS DISTINCT FROM receipt OR existing.wire_payload IS DISTINCT FROM receipt_wire
                    OR existing.payload_sha256 IS DISTINCT FROM encode(sha256(receipt_wire),'hex') THEN
                    RAISE EXCEPTION 'build bootstrap exact replay changed; same-intent rotation is forbidden';
                END IF;
            ELSE
                INSERT INTO {SCHEMA}.bootstraps(intent_id,installation_id,proposal_sha256,payload,wire_payload,payload_sha256)
                    VALUES(intent,p_installation,digest,receipt,receipt_wire,encode(sha256(receipt_wire),'hex'));
            END IF;
            RETURN convert_from(receipt_wire,'UTF8');
        END $function$;

        CREATE FUNCTION {SCHEMA}.authorize_bootstrap_publication(p_installation uuid, p_intent uuid)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE installation {SCHEMA}.installations%ROWTYPE; retained {SCHEMA}.bootstraps%ROWTYPE;
            proposal jsonb; ack jsonb;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build bootstrap publication requires serializable transaction';
            END IF;
            SELECT * INTO installation FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build bootstrap installation is absent'; END IF;
            SELECT * INTO retained FROM {SCHEMA}.bootstraps WHERE intent_id=p_intent FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build bootstrap registration is absent' USING ERRCODE='P0002'; END IF;
            IF retained.retention_xid=pg_current_xact_id() THEN
                RAISE EXCEPTION 'build bootstrap publication requires committed registration';
            END IF;
            proposal := retained.payload->'proposal';
            IF retained.installation_id IS DISTINCT FROM p_installation
                OR retained.payload->>'installation_id' IS DISTINCT FROM p_installation::text
                OR proposal->'binding'->>'intent_id' IS DISTINCT FROM p_intent::text
                OR retained.wire_payload IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(retained.payload),'UTF8')
                OR retained.payload_sha256 IS DISTINCT FROM encode(sha256(retained.wire_payload),'hex') THEN
                RAISE EXCEPTION 'build bootstrap retained registration binding changed';
            END IF;
            PERFORM {SCHEMA}.assert_bootstrap(proposal,convert_to({SCHEMA}.canonical_plan_json(proposal),'UTF8'),
                retained.proposal_sha256,installation.payload);
            ack := jsonb_build_object('schema_version',2,'binding',proposal->'binding','proposal_epoch',1,
                'proposal_digest',retained.proposal_sha256,'reporter_incarnation',installation.reporter_incarnation,
                'bootstrap_registration_epoch',1,'bootstrap_evidence_sha256',retained.payload_sha256,
                'protected_admission_sha256',installation.payload->'protected_admission_sha256','executable',true);
            RETURN {SCHEMA}.canonical_plan_json(ack);
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.assert_bootstrap(jsonb,bytea,text,jsonb) FROM PUBLIC, {quote(agent)}")
    for signature in FUNCTIONS:
        op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{signature} FROM PUBLIC")
        op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{signature} TO {quote(agent)}")


def downgrade():
    op.execute(f"LOCK TABLE {SCHEMA}.bootstraps IN ACCESS EXCLUSIVE MODE")
    op.execute(f"""DO $$ BEGIN IF EXISTS (SELECT 1 FROM {SCHEMA}.bootstraps) THEN
        RAISE EXCEPTION 'cannot remove build bootstrap with retained evidence'; END IF; END $$""")
    for signature in FUNCTIONS:
        op.execute(f"DROP FUNCTION {SCHEMA}.{signature}")
    op.execute(f"DROP FUNCTION {SCHEMA}.assert_bootstrap(jsonb,bytea,text,jsonb)")
    op.drop_table("bootstraps",schema=SCHEMA)
