"""Read current source preimages through the private guard, without table grants.

Revision ID: build_guard_0007
Revises: build_guard_0006
"""

from alembic import op

revision = "build_guard_0007"
down_revision = "build_guard_0006"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = f"{SCHEMA}.read_pending_sources(uuid)"


def upgrade():
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.read_pending_sources(p_installation uuid) RETURNS jsonb
        LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE installation {SCHEMA}.installations%ROWTYPE;
            observed timestamptz; sources jsonb;
        BEGIN
            IF current_setting('transaction_isolation') <> 'serializable' THEN
                RAISE EXCEPTION 'build source observation requires serializable transaction';
            END IF;
            SELECT * INTO installation FROM {SCHEMA}.installations WHERE id=p_installation FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'build source observation installation is absent'; END IF;
            observed := clock_timestamp();
            SELECT coalesce(jsonb_object_agg(id, source),'{{}}'::jsonb) INTO sources FROM (
                SELECT r.id, jsonb_build_object(
                    'owner_user_id', c.owner_user_id, 'owner_team_id', c.owner_team_id,
                    'candidate_id', c.id, 'source_generation_id', c.source_generation_id,
                    'candidate_sha256', c.candidate_sha, 'source_sha256', c.source_sha256,
                    'archive_sha256', c.archive_sha256, 'build_contract_sha256', c.build_contract_sha256,
                    'object_bucket', c.object_bucket, 'object_key', c.object_key,
                    'archive_size_bytes', c.archive_size_bytes,
                    'attempt_id', a.id, 'lease_epoch', a.lease_epoch, 'claimed_by', a.claimed_by,
                    'subject_id', a.subject_id, 'subject_incarnation', a.subject_incarnation,
                    'operation_id', a.operation_id, 'operation_epoch', a.operation_epoch) AS source
                FROM public.personal_dev_build_platform_requests r
                JOIN public.personal_dev_candidate_build_attempts a ON a.id=r.attempt_id
                JOIN public.personal_dev_candidates c ON c.id=r.candidate_id
                WHERE r.owner_user_id=installation.owner_user_id
                    AND r.subject_id=installation.subject_id AND r.subject_incarnation=installation.subject_incarnation
                    AND r.deployment_generation=installation.deployment_generation AND r.cancelled_at IS NULL
                    AND a.state='running' AND a.finished_at IS NULL AND a.lease_epoch=r.attempt_lease_epoch
                    AND a.lease_expires_at>observed AND c.status='building' AND c.artifact_state='retained'
                    AND NOT EXISTS (SELECT 1 FROM {SCHEMA}.request_holds h WHERE h.request_id=r.id)
                ORDER BY r.id LIMIT 2049
            ) pending;
            IF (SELECT count(*) FROM jsonb_object_keys(sources))>2048 OR octet_length(sources::text)>8388608 THEN
                RAISE EXCEPTION 'build source observation exceeds complete bound';
            END IF;
            -- Observation grants nothing. capture_demand rechecks complete current
            -- work, takes sorted source locks and compares the original staged
            -- source hash. Native preparation independently repeats those checks.
            RETURN sources;
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON FUNCTION {FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {FUNCTION} TO {quote(agent)}")


def downgrade():
    op.execute(f"DROP FUNCTION {FUNCTION}")
