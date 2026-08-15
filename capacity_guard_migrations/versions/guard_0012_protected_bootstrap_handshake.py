"""Protected subject registration for executable bootstrap proposals.

Revision ID: guard_0012
Revises: guard_0011
Create Date: 2026-08-15
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "guard_0012"
down_revision: str | Sequence[str] | None = "guard_0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"


def _agent_role() -> str:
    role = op.get_context().config.attributes.get("capacity_guard_agent_role")
    if not isinstance(role, str) or not role:
        raise RuntimeError("protected bootstrap migration is missing the validated agent role")
    return op.get_bind().dialect.identifier_preparer.quote(role)


def upgrade() -> None:
    quoted_agent = _agent_role()
    op.create_table(
        "protected_executable_bootstrap_registrations",
        sa.Column("registration_id", sa.BigInteger(), nullable=False),
        sa.Column("agent_incarnation", sa.Uuid(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("subject_incarnation", sa.Uuid(), nullable=False),
        sa.Column("intent_id", sa.Uuid(), nullable=False),
        sa.Column("proposal_epoch", sa.BigInteger(), nullable=False),
        sa.Column("proposal_digest", sa.Text(), nullable=False),
        sa.Column("bootstrap_registration_epoch", sa.BigInteger(), nullable=False),
        sa.Column("bootstrap_sha256", sa.Text(), nullable=False),
        sa.Column("protected_admission_sha256", sa.Text(), nullable=False),
        sa.Column("binding", postgresql.JSONB(), nullable=False),
        sa.Column("proposal_payload", postgresql.JSONB(), nullable=False),
        sa.Column("receipt", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.CheckConstraint(
            "registration_id > 0 AND proposal_epoch > 0 "
            "AND bootstrap_registration_epoch > 0",
            name="guard_protected_bootstrap_quantity_check",
        ),
        sa.CheckConstraint(
            "proposal_digest ~ '^[0-9a-f]{64}$' "
            "AND bootstrap_sha256 ~ '^[0-9a-f]{64}$' "
            "AND protected_admission_sha256 ~ '^[0-9a-f]{64}$'",
            name="guard_protected_bootstrap_digest_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(binding) = 'object' "
            "AND jsonb_typeof(proposal_payload) = 'object' "
            "AND jsonb_typeof(receipt) = 'object' "
            "AND octet_length(proposal_payload::text) <= 8388608 "
            "AND octet_length(receipt::text) <= 8388608",
            name="guard_protected_bootstrap_payload_check",
        ),
        sa.ForeignKeyConstraint(
            ["agent_incarnation"],
            [f"{SCHEMA}.agent_registrations.agent_incarnation"],
            ondelete="RESTRICT",
            name="guard_protected_bootstrap_agent_fkey",
        ),
        sa.PrimaryKeyConstraint("registration_id"),
        sa.UniqueConstraint(
            "intent_id",
            "proposal_epoch",
            name="guard_protected_bootstrap_proposal_epoch_key",
        ),
        sa.UniqueConstraint(
            "intent_id",
            "bootstrap_registration_epoch",
            name="guard_protected_bootstrap_registration_epoch_key",
        ),
        schema=SCHEMA,
    )
    op.execute(
        f"""
        CREATE TRIGGER protected_executable_bootstrap_registrations_append_only_row
        BEFORE UPDATE OR DELETE
        ON {SCHEMA}.protected_executable_bootstrap_registrations
        FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER protected_executable_bootstrap_registrations_append_only_truncate
        BEFORE TRUNCATE
        ON {SCHEMA}.protected_executable_bootstrap_registrations
        FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_append_only_mutation()
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.protect_executable_bootstrap(
          p_agent_incarnation uuid,
          p_payload jsonb,
          p_canonical_payload bytea,
          p_proposal_digest text,
          p_protected_admission_sha256 text
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_agent_role text;
          v_registration {SCHEMA}.agent_registrations%ROWTYPE;
          v_binding jsonb := p_payload->'binding';
          v_intent_id uuid;
          v_proposal_epoch bigint;
          v_existing {SCHEMA}.protected_executable_bootstrap_registrations%ROWTYPE;
          v_latest_proposal_epoch bigint;
          v_latest_registration_epoch bigint;
          v_high_water bigint;
          v_receipt jsonb;
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION 'protected bootstrap requires a SERIALIZABLE transaction'
              USING ERRCODE = '25000';
          END IF;
          SELECT agent_role_name INTO v_agent_role
            FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1;
          IF v_agent_role IS NULL OR session_user::text <> v_agent_role THEN
            RAISE EXCEPTION 'protected bootstrap caller is not the registered agent role'
              USING ERRCODE = '42501';
          END IF;
          IF pg_has_role(session_user, current_user, 'MEMBER') THEN
            RAISE EXCEPTION 'protected bootstrap agent unexpectedly holds owner membership'
              USING ERRCODE = '42501';
          END IF;
          IF pg_catalog.jsonb_typeof(p_payload) IS DISTINCT FROM 'object'
             OR octet_length(p_payload::text) > 8388608
             OR octet_length(p_canonical_payload) > 8388608
             OR convert_from(p_canonical_payload, 'UTF8')::jsonb IS DISTINCT FROM p_payload
             OR p_proposal_digest !~ '^[0-9a-f]{{64}}$'
             OR encode(sha256(p_canonical_payload), 'hex')
                  IS DISTINCT FROM p_proposal_digest
             OR p_protected_admission_sha256 !~ '^[0-9a-f]{{64}}$'
             OR NOT p_payload ?& ARRAY[
                  'schema_version', 'binding', 'command_sequence', 'proposal_epoch',
                  'bootstrap_sha256', 'expires_at', 'executable'
                ]
             OR p_payload - ARRAY[
                  'schema_version', 'binding', 'command_sequence', 'proposal_epoch',
                  'bootstrap_sha256', 'expires_at', 'executable'
                ] <> '{{}}'::jsonb
             OR (p_payload->>'schema_version')::bigint IS DISTINCT FROM 2
             OR (p_payload->>'command_sequence')::bigint <= 0
             OR (p_payload->>'proposal_epoch')::bigint <= 0
             OR p_payload->>'bootstrap_sha256' !~ '^[0-9a-f]{{64}}$'
             OR (p_payload->>'expires_at')::timestamptz <= pg_catalog.clock_timestamp()
             OR (p_payload->>'expires_at')::timestamptz
                  > pg_catalog.clock_timestamp() + interval '10 minutes'
             OR p_payload->'executable' IS DISTINCT FROM 'true'::jsonb
             OR pg_catalog.jsonb_typeof(v_binding) IS DISTINCT FROM 'object'
             OR (v_binding->>'subject_id')::uuid IS NULL
             OR (v_binding->>'subject_incarnation')::uuid IS NULL
             OR (v_binding->>'intent_id')::uuid IS NULL
             OR (v_binding->>'deployment_generation')::bigint <= 0
             OR v_binding->'execution'->>'execution_state' IS DISTINCT FROM 'active'
             OR (v_binding->'execution'->>'allocation_epoch')::bigint <= 0
             OR (v_binding->'execution'->>'executable_new_capacity_ceiling')::bigint <= 0
             OR (v_binding->'execution'->>'executable_new_capacity_rate_per_minute')::bigint
                  <= 0 THEN
            RAISE EXCEPTION 'protected bootstrap proposal is invalid or oversized'
              USING ERRCODE = '22023';
          END IF;

          SELECT registration.* INTO v_registration
            FROM {SCHEMA}.agent_registrations AS registration
            JOIN {SCHEMA}.authority_state AS authority
              ON authority.singleton_id = registration.singleton_id
             AND authority.environment_id = registration.environment_id
             AND authority.subject_id = registration.subject_id
             AND authority.subject_incarnation = registration.subject_incarnation
             AND authority.authority_incarnation = registration.authority_incarnation
             AND authority.reporter_incarnation = registration.reporter_incarnation
             AND authority.deployment_generation = registration.deployment_generation
             AND authority.configuration_generation
                  = registration.configuration_generation
             AND authority.candidate_digest = registration.candidate_digest
           WHERE registration.agent_incarnation = p_agent_incarnation
             AND registration.subject_id = (v_binding->>'subject_id')::uuid
             AND registration.subject_incarnation
                  = (v_binding->>'subject_incarnation')::uuid
             AND registration.deployment_generation
                  = (v_binding->>'deployment_generation')::bigint
             AND registration.registration_state = 'registered'
           FOR KEY SHARE OF registration, authority;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'protected bootstrap subject registration changed'
              USING ERRCODE = '55000';
          END IF;

          PERFORM 1 FROM {SCHEMA}.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE;
          v_intent_id := (v_binding->>'intent_id')::uuid;
          v_proposal_epoch := (p_payload->>'proposal_epoch')::bigint;
          SELECT * INTO v_existing
            FROM {SCHEMA}.protected_executable_bootstrap_registrations
           WHERE intent_id = v_intent_id AND proposal_epoch = v_proposal_epoch
           FOR KEY SHARE;
          IF FOUND THEN
            IF v_existing.agent_incarnation IS DISTINCT FROM p_agent_incarnation
               OR v_existing.subject_id IS DISTINCT FROM v_registration.subject_id
               OR v_existing.subject_incarnation
                    IS DISTINCT FROM v_registration.subject_incarnation
               OR v_existing.proposal_digest IS DISTINCT FROM p_proposal_digest
               OR v_existing.bootstrap_sha256
                    IS DISTINCT FROM p_payload->>'bootstrap_sha256'
               OR v_existing.protected_admission_sha256
                    IS DISTINCT FROM p_protected_admission_sha256
               OR v_existing.proposal_payload IS DISTINCT FROM p_payload THEN
              RAISE EXCEPTION 'conflicting protected bootstrap replay'
                USING ERRCODE = '55000';
            END IF;
            RETURN v_existing.receipt;
          END IF;

          SELECT max(proposal_epoch), max(bootstrap_registration_epoch)
            INTO v_latest_proposal_epoch, v_latest_registration_epoch
            FROM {SCHEMA}.protected_executable_bootstrap_registrations
           WHERE intent_id = v_intent_id;
          IF v_proposal_epoch IS DISTINCT FROM
               COALESCE(v_latest_proposal_epoch, 0) + 1 THEN
            RAISE EXCEPTION 'protected bootstrap proposal epoch changed'
              USING ERRCODE = '55000';
          END IF;
          SELECT count(*) + 1 INTO v_high_water
            FROM {SCHEMA}.protected_executable_bootstrap_registrations;
          v_receipt := jsonb_build_object(
            'schema_version', 2,
            'subject_id', v_registration.subject_id,
            'subject_incarnation', v_registration.subject_incarnation,
            'intent_id', v_intent_id,
            'proposal_epoch', v_proposal_epoch,
            'proposal_digest', p_proposal_digest,
            'bootstrap_registration_epoch',
              COALESCE(v_latest_registration_epoch, 0) + 1,
            'bootstrap_sha256', p_payload->>'bootstrap_sha256',
            'protected_admission_sha256', p_protected_admission_sha256,
            'protected_high_water', v_high_water,
            'registration_state', 'registered',
            'executable', false
          );
          INSERT INTO {SCHEMA}.protected_executable_bootstrap_registrations
            (registration_id, agent_incarnation, subject_id, subject_incarnation,
             intent_id, proposal_epoch, proposal_digest,
             bootstrap_registration_epoch, bootstrap_sha256,
             protected_admission_sha256, binding, proposal_payload, receipt)
          VALUES
            (v_high_water, p_agent_incarnation, v_registration.subject_id,
             v_registration.subject_incarnation, v_intent_id, v_proposal_epoch,
             p_proposal_digest, COALESCE(v_latest_registration_epoch, 0) + 1,
             p_payload->>'bootstrap_sha256', p_protected_admission_sha256,
             v_binding, p_payload, v_receipt);
          RETURN v_receipt;
        END
        $function$
        """
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON FUNCTION {SCHEMA}.protect_executable_bootstrap"
        "(uuid,jsonb,bytea,text,text) FROM PUBLIC"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.protect_executable_bootstrap"
        f"(uuid,jsonb,bytea,text,text) TO {quoted_agent}"
    )


def downgrade() -> None:
    op.execute(
        f"LOCK TABLE {SCHEMA}.protected_executable_bootstrap_registrations "
        "IN ACCESS EXCLUSIVE MODE"
    )
    if op.get_bind().execute(
        sa.text(
            f"SELECT EXISTS (SELECT 1 FROM "
            f"{SCHEMA}.protected_executable_bootstrap_registrations)"
        )
    ).scalar_one():
        raise RuntimeError(
            "cannot downgrade guard_0012 while protected bootstrap evidence exists"
        )
    quoted_agent = _agent_role()
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SCHEMA}.protect_executable_bootstrap"
        f"(uuid,jsonb,bytea,text,text) FROM {quoted_agent}"
    )
    op.execute(
        f"DROP FUNCTION {SCHEMA}.protect_executable_bootstrap"
        "(uuid,jsonb,bytea,text,text)"
    )
    op.execute(
        f"DROP TRIGGER protected_executable_bootstrap_registrations_append_only_truncate "
        f"ON {SCHEMA}.protected_executable_bootstrap_registrations"
    )
    op.execute(
        f"DROP TRIGGER protected_executable_bootstrap_registrations_append_only_row "
        f"ON {SCHEMA}.protected_executable_bootstrap_registrations"
    )
    op.drop_table("protected_executable_bootstrap_registrations", schema=SCHEMA)
