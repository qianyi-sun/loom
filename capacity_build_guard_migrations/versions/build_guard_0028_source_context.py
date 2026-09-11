"""Expose compact build context only under current committed claim authority.

Revision ID: build_guard_0028
Revises: build_guard_0027
"""

from alembic import op

revision = "build_guard_0028"
down_revision = "build_guard_0027"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTION = "read_source_context(uuid,jsonb,bytea,text,text)"


def upgrade():
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.read_source_context(
            p_installation uuid,p jsonb,wire bytea,digest text,credential_hash text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE access jsonb;
            request public.personal_dev_build_platform_requests%ROWTYPE;
            candidate public.personal_dev_candidates%ROWTYPE;
            attempt public.personal_dev_candidate_build_attempts%ROWTYPE;
        BEGIN
            -- Reuse the live fence, not historical claim replay. It holds the
            -- parent, candidate and request locks until this transaction ends.
            access := {SCHEMA}.authorize_source(p_installation,p,wire,digest,credential_hash)::jsonb;
            SELECT * INTO STRICT request FROM public.personal_dev_build_platform_requests
                WHERE id=(p->>'request_id')::uuid;
            SELECT * INTO STRICT candidate FROM public.personal_dev_candidates WHERE id=request.candidate_id;
            SELECT * INTO STRICT attempt FROM public.personal_dev_candidate_build_attempts WHERE id=request.attempt_id;
            RETURN {SCHEMA}.canonical_plan_json(jsonb_build_object('schema_version',1,
                'claim_digest',digest,'request_id',request.id,
                'source_binding_sha256',access->'source_binding_sha256','platform',request.platform,
                'candidate_id',candidate.id,'candidate_sha',candidate.candidate_sha,
                'source_sha256',candidate.source_sha256,'archive_sha256',candidate.archive_sha256,
                'archive_size_bytes',candidate.archive_size_bytes,'build_contract_sha256',candidate.build_contract_sha256,
                'source_commit',candidate.source_commit,'dirty',candidate.dirty,
                'attempt_id',attempt.id,'attempt_sequence',attempt.attempt_sequence,'lease_epoch',attempt.lease_epoch,
                'subject_id',attempt.subject_id,'subject_incarnation',attempt.subject_incarnation,
                'operation_id',attempt.operation_id,'operation_epoch',attempt.operation_epoch,
                'lease_not_after',access->'lease_not_after'));
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{FUNCTION} TO {quote(agent)}")


def downgrade():
    op.execute(f"DROP FUNCTION {SCHEMA}.{FUNCTION}")
