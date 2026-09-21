"""Check current source under locks without granting runtime admission.

Revision ID: build_guard_0002
Revises: build_guard_0001
"""

from alembic import op

revision = "build_guard_0002"
down_revision = "build_guard_0001"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = f"{SCHEMA}.assert_current_source(uuid,uuid,jsonb,bytea,text)"


def upgrade():
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.assert_current_source(
            p_installation uuid, p_request uuid, p_source jsonb, p_wire bytea, p_digest text
        ) RETURNS timestamptz LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $function$
        DECLARE
            installation {SCHEMA}.installations%ROWTYPE;
            request public.personal_dev_build_platform_requests%ROWTYPE;
            attempt public.personal_dev_candidate_build_attempts%ROWTYPE;
            candidate public.personal_dev_candidates%ROWTYPE;
            expected_source jsonb;
            observed_at timestamptz;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build source check requires serializable transaction';
            END IF;
            IF p_wire IS NULL OR octet_length(p_wire) NOT BETWEEN 2 AND 1048576
                OR jsonb_typeof(p_source) IS DISTINCT FROM 'object'
                OR convert_from(p_wire, 'UTF8')::jsonb IS DISTINCT FROM p_source
                OR encode(sha256(p_wire), 'hex') IS DISTINCT FROM p_digest THEN
                RAISE EXCEPTION 'build source canonical wire or digest changed';
            END IF;
            SELECT * INTO installation FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build source installation is absent'; END IF;
            -- Request identity cannot change; read it to locate the source rows,
            -- then lock parent -> candidate -> request, matching management.
            SELECT * INTO request FROM public.personal_dev_build_platform_requests WHERE id=p_request;
            IF NOT FOUND THEN RAISE EXCEPTION 'build source request is absent'; END IF;
            SELECT * INTO attempt FROM public.personal_dev_candidate_build_attempts WHERE id=request.attempt_id FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build source parent is absent'; END IF;
            SELECT * INTO candidate FROM public.personal_dev_candidates WHERE id=request.candidate_id FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build source candidate is absent'; END IF;
            SELECT * INTO request FROM public.personal_dev_build_platform_requests WHERE id=p_request FOR UPDATE;
            observed_at := clock_timestamp();
            IF (request.owner_user_id, request.subject_id, request.subject_incarnation, request.deployment_generation)
                IS DISTINCT FROM (installation.owner_user_id, installation.subject_id,
                    installation.subject_incarnation, installation.deployment_generation)
                OR request.runtime_installation_sha256 IS DISTINCT FROM installation.payload->>'runtime_installation_sha256'
                OR request.source_binding_sha256 IS DISTINCT FROM p_digest
                OR request.cancelled_at IS NOT NULL
                OR attempt.candidate_id IS DISTINCT FROM candidate.id
                OR candidate.owner_user_id IS DISTINCT FROM request.owner_user_id
                OR attempt.lease_epoch IS DISTINCT FROM request.attempt_lease_epoch
                OR attempt.state IS DISTINCT FROM 'running' OR attempt.finished_at IS NOT NULL
                OR attempt.claimed_by IS NULL OR btrim(attempt.claimed_by)=''
                OR attempt.lease_expires_at IS NULL OR attempt.lease_expires_at <= observed_at
                OR attempt.created_at > observed_at
                OR candidate.status IS DISTINCT FROM 'building'
                OR candidate.artifact_state IS DISTINCT FROM 'retained' THEN
                RAISE EXCEPTION 'build source request installation or live lease changed';
            END IF;
            expected_source := jsonb_build_object(
                'owner_user_id', candidate.owner_user_id, 'owner_team_id', candidate.owner_team_id,
                'candidate_id', candidate.id, 'source_generation_id', candidate.source_generation_id,
                'candidate_sha256', candidate.candidate_sha, 'source_sha256', candidate.source_sha256,
                'archive_sha256', candidate.archive_sha256, 'build_contract_sha256', candidate.build_contract_sha256,
                'object_bucket', candidate.object_bucket, 'object_key', candidate.object_key,
                'archive_size_bytes', candidate.archive_size_bytes,
                'attempt_id', attempt.id, 'lease_epoch', attempt.lease_epoch, 'claimed_by', attempt.claimed_by,
                'subject_id', attempt.subject_id, 'subject_incarnation', attempt.subject_incarnation,
                'operation_id', attempt.operation_id, 'operation_epoch', attempt.operation_epoch);
            IF expected_source IS DISTINCT FROM p_source THEN
                RAISE EXCEPTION 'build source snapshot differs from current candidate or attempt';
            END IF;
            RETURN attempt.lease_expires_at;
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON FUNCTION {FUNCTION} FROM PUBLIC, {quote(agent)}")


def downgrade():
    op.execute(f"DROP FUNCTION {FUNCTION}")
