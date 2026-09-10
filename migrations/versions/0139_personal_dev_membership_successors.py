"""Retain unique, immutable, lease-fenced personal membership successor lineage.

Revision ID: 0139
Revises: 0138
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0139"
down_revision = "0138"
branch_labels = None
depends_on = None

_OPERATIONS = "dev_lifecycle_operations"
_ATTEMPTS = "dev_lifecycle_operation_attempts"
_OLD_CHECKS = {
    (_OPERATIONS, "state"): (
        "state IN ('requested', 'running', 'activating', 'succeeded', "
        "'failed', 'cancelling', 'cancelled')"
    ),
    (_OPERATIONS, "terminal_fields"): (
        "(state IN ('requested', 'running', 'activating', 'cancelling') "
        "AND finished_at IS NULL AND failure_reason IS NULL) OR "
        "(state = 'succeeded' AND finished_at IS NOT NULL AND failure_reason IS NULL) OR "
        "(state IN ('failed', 'cancelled') AND finished_at IS NOT NULL)"
    ),
    (_OPERATIONS, "activation_evidence"): (
        "(kind IN ('capacity', 'destroy', 'noop') AND readiness_evidence_sha256 IS NULL "
        "AND activation_acknowledgement_sha256 IS NULL) OR "
        "(kind IN ('create', 'update') AND ((state IN ('requested', 'running', 'failed', "
        "'cancelling', 'cancelled') AND readiness_evidence_sha256 IS NULL "
        "AND activation_acknowledgement_sha256 IS NULL) OR "
        "(state = 'activating' AND readiness_evidence_sha256 IS NOT NULL) OR "
        "(state = 'succeeded' AND readiness_evidence_sha256 IS NOT NULL "
        "AND activation_acknowledgement_sha256 IS NOT NULL)))"
    ),
    (_ATTEMPTS, "state"): "state IN ('running', 'activating', 'succeeded', 'failed', 'cancelled')",
    (_ATTEMPTS, "terminal_fields"): (
        "(state IN ('running', 'activating') AND finished_at IS NULL "
        "AND failure_reason IS NULL) OR (state = 'succeeded' AND finished_at IS NOT NULL "
        "AND failure_reason IS NULL) OR "
        "(state IN ('failed', 'cancelled') AND finished_at IS NOT NULL)"
    ),
}
_SUPPLEMENTS = {
    "state": "state = 'superseded'",
    "terminal_fields": (
        "state = 'superseded' AND finished_at IS NOT NULL AND failure_reason IS NULL"
    ),
    "activation_evidence": (
        "state = 'superseded' AND kind IN ('create', 'update') "
        "AND readiness_evidence_sha256 IS NOT NULL "
        "AND activation_acknowledgement_sha256 IS NOT NULL"
    ),
}
_FIELDS = (
    "(membership_predecessor_operation_id IS NULL "
    "AND membership_accepted_operation_id IS NULL "
    "AND membership_predecessor_envelope_sha256 IS NULL "
    "AND membership_successor_binding IS NULL "
    "AND membership_successor_binding_sha256 IS NULL "
    "AND membership_continuation_kind IS NULL) OR (("
    "membership_predecessor_operation_id IS NOT NULL "
    "AND membership_predecessor_operation_id <> id "
    "AND capacity_mode = 'membership-v1' AND kind IN ('create', 'update', 'destroy') "
    "AND membership_predecessor_envelope_sha256 ~ '^[0-9a-f]{64}$' "
    "AND membership_predecessor_envelope_sha256 <> repeat('0', 64) "
    "AND membership_successor_binding_sha256 ~ '^[0-9a-f]{64}$' "
    "AND membership_successor_binding_sha256 <> repeat('0', 64) "
    "AND membership_continuation_kind IN ('create', 'update', 'capacity', 'destroy') "
    "AND jsonb_typeof(membership_successor_binding) = 'object' "
    "AND membership_successor_binding->>'schema_version' = '1' "
    "AND membership_successor_binding->>'predecessor_operation_id' "
    "= membership_predecessor_operation_id::text "
    "AND membership_successor_binding->>'predecessor_envelope_sha256' "
    "= membership_predecessor_envelope_sha256 "
    "AND (membership_successor_binding->>'accepted_operation_id') "
    "IS NOT DISTINCT FROM membership_accepted_operation_id::text "
    "AND membership_successor_binding->>'owner_team_id' = owner_team_id::text) IS TRUE)"
)
_SUPERSEDED = (
    "(state = 'superseded') = (checkpoint = 'membership_successor_created') "
    "AND (state <> 'superseded' OR ((capacity_mode = 'membership-v1' "
    "AND jsonb_typeof(capacity_membership_envelope->'result') = 'null' "
    "AND jsonb_typeof(capacity_membership_envelope->'historical_outcome') = 'object' "
    "AND jsonb_typeof(capacity_membership_envelope->'release') = 'null' "
    "AND capacity_membership_envelope->'historical_outcome'->>'outcome' "
    "IN ('committed', 'terminal-not-committed') "
    "AND NOT (kind = 'destroy' AND capacity_membership_envelope "
    "->'historical_outcome'->>'outcome' = 'committed')) IS TRUE))"
)


def upgrade() -> None:
    for column in (
        sa.Column("membership_predecessor_operation_id", postgresql.UUID(as_uuid=True)),
        sa.Column("membership_accepted_operation_id", postgresql.UUID(as_uuid=True)),
        sa.Column("membership_predecessor_envelope_sha256", sa.String(64)),
        sa.Column("membership_successor_binding", postgresql.JSONB()),
        sa.Column("membership_successor_binding_sha256", sa.String(64)),
        sa.Column("membership_continuation_kind", sa.String(16)),
    ):
        op.add_column(_OPERATIONS, column)
    op.create_foreign_key(
        "dev_lifecycle_operations_membership_predecessor_fkey", _OPERATIONS, _OPERATIONS,
        ["membership_predecessor_operation_id"], ["id"], ondelete="RESTRICT",
    )
    op.create_unique_constraint(
        "dev_lifecycle_operations_successor_predecessor_uidx", _OPERATIONS,
        ["membership_predecessor_operation_id"],
    )
    op.create_foreign_key(
        "dev_lifecycle_operations_membership_accepted_fkey", _OPERATIONS, _OPERATIONS,
        ["membership_accepted_operation_id"], ["id"], ondelete="RESTRICT",
    )
    for (table, suffix), old in _OLD_CHECKS.items():
        name = f"{table}_{suffix}_check"
        op.drop_constraint(name, table, type_="check")
        op.create_check_constraint(name, table, f"({old}) OR ({_SUPPLEMENTS[suffix]})")
    op.create_check_constraint("dev_lifecycle_operations_successor_fields_check", _OPERATIONS, _FIELDS)
    op.create_check_constraint("dev_lifecycle_operations_superseded_check", _OPERATIONS, _SUPERSEDED)
    op.execute("""
        CREATE FUNCTION loom_guard_dev_membership_lineage()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE parent dev_lifecycle_operations%ROWTYPE;
                accepted dev_lifecycle_operations%ROWTYPE;
        BEGIN
            IF TG_OP IN ('UPDATE', 'DELETE') AND EXISTS (
                SELECT 1 FROM dev_lifecycle_operations
                WHERE membership_accepted_operation_id = OLD.id
            ) THEN
                IF TG_OP = 'DELETE' OR NEW IS DISTINCT FROM OLD THEN
                    RAISE EXCEPTION 'accepted membership source history is immutable';
                END IF;
            END IF;
            IF TG_OP = 'DELETE' THEN
                IF OLD.state = 'superseded' OR OLD.membership_predecessor_operation_id IS NOT NULL THEN
                    RAISE EXCEPTION 'membership successor history cannot be deleted';
                END IF;
                RETURN OLD;
            END IF;
            IF TG_OP = 'UPDATE' THEN
                IF OLD.state = 'superseded' AND NEW IS DISTINCT FROM OLD THEN
                    RAISE EXCEPTION 'superseded membership history is immutable';
                END IF;
                IF NEW.state = 'superseded' AND OLD.state <> 'superseded' THEN
                    IF OLD.state NOT IN ('running', 'activating')
                       OR OLD.checkpoint <> 'membership_outcome_resolved'
                       OR (to_jsonb(NEW) - ARRAY['state','checkpoint','updated_at','finished_at'])
                          IS DISTINCT FROM
                          (to_jsonb(OLD) - ARRAY['state','checkpoint','updated_at','finished_at']) THEN
                        RAISE EXCEPTION 'membership successor cannot rewrite predecessor';
                    END IF;
                END IF;
                IF ROW(NEW.membership_predecessor_operation_id,
                       NEW.membership_accepted_operation_id,
                       NEW.membership_predecessor_envelope_sha256, NEW.membership_successor_binding,
                       NEW.membership_successor_binding_sha256, NEW.membership_continuation_kind)
                   IS DISTINCT FROM ROW(OLD.membership_predecessor_operation_id,
                       OLD.membership_accepted_operation_id,
                       OLD.membership_predecessor_envelope_sha256, OLD.membership_successor_binding,
                       OLD.membership_successor_binding_sha256, OLD.membership_continuation_kind) THEN
                    RAISE EXCEPTION 'membership successor lineage is immutable';
                END IF;
                IF OLD.membership_predecessor_operation_id IS NOT NULL AND
                   ROW(NEW.id, NEW.idempotency_key, NEW.environment_name, NEW.subject_id,
                       NEW.subject_incarnation, NEW.owner_user_id, NEW.owner_team_id,
                       NEW.operation_epoch, NEW.expected_operation_epoch, NEW.kind,
                       NEW.request_sha256, NEW.candidate_id, NEW.candidate_sha,
                       NEW.min_slots, NEW.max_slots, NEW.deployment_generation, NEW.keep_data,
                       NEW.capacity_mode)
                   IS DISTINCT FROM
                   ROW(OLD.id, OLD.idempotency_key, OLD.environment_name, OLD.subject_id,
                       OLD.subject_incarnation, OLD.owner_user_id, OLD.owner_team_id,
                       OLD.operation_epoch, OLD.expected_operation_epoch, OLD.kind,
                       OLD.request_sha256, OLD.candidate_id, OLD.candidate_sha,
                       OLD.min_slots, OLD.max_slots, OLD.deployment_generation, OLD.keep_data,
                       OLD.capacity_mode) THEN
                    RAISE EXCEPTION 'membership successor owner intent is immutable';
                END IF;
                IF OLD.membership_predecessor_operation_id IS NOT NULL AND OLD.kind = 'destroy'
                   AND ROW(NEW.capacity_reporter_incarnation, NEW.capacity_reporter_token_sha256,
                       NEW.local_activation_sha256, NEW.protected_admission_sha256,
                       NEW.capacity_agent_installation_sha256, NEW.capacity_supported_pool_ids,
                       NEW.capacity_supported_architectures)
                   IS DISTINCT FROM ROW(OLD.capacity_reporter_incarnation, OLD.capacity_reporter_token_sha256,
                       OLD.local_activation_sha256, OLD.protected_admission_sha256,
                       OLD.capacity_agent_installation_sha256, OLD.capacity_supported_pool_ids,
                       OLD.capacity_supported_architectures) THEN
                    RAISE EXCEPTION 'destroy successor retained evidence is immutable';
                END IF;
                RETURN NEW;
            END IF;
            IF NEW.state = 'superseded' THEN
                RAISE EXCEPTION 'membership predecessor must transition under a lease';
            END IF;
            IF NEW.membership_predecessor_operation_id IS NULL THEN RETURN NEW; END IF;
            SELECT * INTO parent FROM dev_lifecycle_operations
              WHERE id = NEW.membership_predecessor_operation_id FOR KEY SHARE;
            IF NOT FOUND OR parent.state <> 'superseded'
               OR parent.capacity_mode <> 'membership-v1'
               OR ROW(NEW.environment_name, NEW.subject_id, NEW.subject_incarnation,
                   NEW.owner_user_id, NEW.owner_team_id, NEW.candidate_id, NEW.candidate_sha,
                   NEW.min_slots, NEW.max_slots, NEW.keep_data)
                  IS DISTINCT FROM
                  ROW(parent.environment_name, parent.subject_id, parent.subject_incarnation,
                   parent.owner_user_id, parent.owner_team_id, parent.candidate_id, parent.candidate_sha,
                   parent.min_slots, parent.max_slots, parent.keep_data)
               OR NEW.operation_epoch <> parent.operation_epoch + 1
               OR NEW.expected_operation_epoch <> parent.operation_epoch
               OR NEW.id = parent.id OR NEW.idempotency_key = parent.idempotency_key
               OR NEW.attempt_id = parent.attempt_id
               OR NEW.membership_successor_binding->>'request_sha256' IS DISTINCT FROM parent.request_sha256
               OR NEW.membership_continuation_kind IS DISTINCT FROM
                  COALESCE(parent.membership_continuation_kind, parent.kind)
               OR NEW.kind IS DISTINCT FROM (CASE
                   WHEN parent.kind = 'destroy' THEN 'destroy'
                   WHEN NEW.membership_successor_binding->'adopted_member' = 'null'::jsonb THEN 'create'
                   ELSE 'update' END)
               OR NEW.state <> 'running' OR NEW.attempt_sequence <> 0
               OR NEW.checkpoint IS DISTINCT FROM (CASE WHEN NEW.kind = 'destroy'
                   THEN 'capacity_retirement_requested' ELSE 'candidate_build' END)
               OR (NEW.kind <> 'destroy' AND NEW.deployment_generation <= parent.deployment_generation)
               OR (NEW.kind = 'destroy' AND NEW.deployment_generation IS DISTINCT FROM
                   (NEW.membership_successor_binding->'adopted_member'->'configuration'
                    ->>'deployment_generation')::bigint)
               OR NEW.capacity_membership_envelope IS NOT NULL THEN
                RAISE EXCEPTION 'membership successor differs from predecessor intent';
            END IF;
            IF NEW.kind = 'destroy' AND ROW(
                NEW.capacity_reporter_incarnation, NEW.capacity_reporter_token_sha256,
                NEW.local_activation_sha256, NEW.protected_admission_sha256,
                NEW.capacity_agent_installation_sha256, NEW.capacity_supported_pool_ids,
                NEW.capacity_supported_architectures
            ) IS DISTINCT FROM ROW(
                parent.capacity_reporter_incarnation, parent.capacity_reporter_token_sha256,
                parent.local_activation_sha256, parent.protected_admission_sha256,
                parent.capacity_agent_installation_sha256, parent.capacity_supported_pool_ids,
                parent.capacity_supported_architectures
            ) THEN
                RAISE EXCEPTION 'destroy successor must retain predecessor evidence';
            END IF;
            IF NEW.membership_accepted_operation_id IS NOT NULL THEN
                SELECT * INTO accepted FROM dev_lifecycle_operations
                    WHERE id = NEW.membership_accepted_operation_id FOR UPDATE;
                IF NOT FOUND OR accepted.state <> 'succeeded' OR accepted.checkpoint <> 'complete'
                   OR ROW(accepted.environment_name, accepted.subject_id, accepted.subject_incarnation,
                       accepted.owner_user_id, accepted.owner_team_id)
                   IS DISTINCT FROM ROW(NEW.environment_name, NEW.subject_id, NEW.subject_incarnation,
                       NEW.owner_user_id, NEW.owner_team_id) THEN
                    RAISE EXCEPTION 'membership successor accepted source is invalid';
                END IF;
            END IF;
            RETURN NEW;
        END $$
    """)
    op.execute("""
        CREATE TRIGGER dev_lifecycle_membership_lineage_guard
        BEFORE INSERT OR UPDATE OR DELETE ON dev_lifecycle_operations
        FOR EACH ROW EXECUTE FUNCTION loom_guard_dev_membership_lineage()
    """)
    op.execute("""
        CREATE FUNCTION loom_guard_dev_membership_successor_attempt()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF EXISTS (SELECT 1 FROM dev_lifecycle_operations
                       WHERE membership_accepted_operation_id = OLD.operation_id)
               AND (TG_OP = 'DELETE' OR NEW IS DISTINCT FROM OLD) THEN
                RAISE EXCEPTION 'accepted membership source attempt is immutable';
            END IF;
            IF OLD.state = 'superseded' THEN
                IF TG_OP = 'DELETE' OR NEW IS DISTINCT FROM OLD THEN
                    RAISE EXCEPTION 'superseded membership attempt is immutable';
                END IF;
            END IF;
            IF TG_OP = 'DELETE' THEN
                IF EXISTS (SELECT 1 FROM dev_lifecycle_operations
                           WHERE id = OLD.operation_id AND membership_predecessor_operation_id IS NOT NULL) THEN
                    RAISE EXCEPTION 'membership successor attempt cannot be deleted';
                END IF;
                RETURN OLD;
            END IF;
            IF NEW.state = 'superseded' AND OLD.state <> 'superseded' AND (
                OLD.state NOT IN ('running', 'activating')
                OR OLD.checkpoint <> 'membership_outcome_resolved'
                OR (to_jsonb(NEW) - ARRAY['state','checkpoint','updated_at','finished_at','claimed_by','lease_expires_at'])
                   IS DISTINCT FROM
                   (to_jsonb(OLD) - ARRAY['state','checkpoint','updated_at','finished_at','claimed_by','lease_expires_at'])
                OR NOT EXISTS (SELECT 1 FROM dev_lifecycle_operations
                               WHERE id = OLD.operation_id AND state = 'superseded'
                                 AND attempt_id = OLD.id)
            ) THEN
                RAISE EXCEPTION 'membership attempt cannot rewrite predecessor';
            END IF;
            RETURN NEW;
        END $$
    """)
    op.execute("""
        CREATE TRIGGER dev_lifecycle_membership_successor_attempt_guard
        BEFORE UPDATE OR DELETE ON dev_lifecycle_operation_attempts
        FOR EACH ROW EXECUTE FUNCTION loom_guard_dev_membership_successor_attempt()
    """)
    op.execute("""
        CREATE FUNCTION loom_check_dev_membership_successor_complete()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF OLD.state <> 'superseded' AND NEW.state = 'superseded' AND NOT EXISTS (
                SELECT 1 FROM dev_lifecycle_operations child
                JOIN dev_instances env ON env.name = child.environment_name
                JOIN dev_lifecycle_operation_attempts attempt ON attempt.id = NEW.attempt_id
                JOIN dev_lifecycle_operation_attempts child_attempt ON child_attempt.id = child.attempt_id
                WHERE child.membership_predecessor_operation_id = NEW.id
                  AND env.operation_id = child.id AND env.operation_epoch = child.operation_epoch
                  AND attempt.state = 'superseded'
                  AND attempt.checkpoint = 'membership_successor_created'
                  AND attempt.claimed_by IS NULL AND attempt.lease_expires_at IS NULL
                  AND child.state = 'running' AND child.attempt_sequence = 0
                  AND child.checkpoint = CASE WHEN child.kind = 'destroy'
                      THEN 'capacity_retirement_requested' ELSE 'candidate_build' END
                  AND child_attempt.operation_id = child.id
                  AND child_attempt.operation_epoch = child.operation_epoch
                  AND child_attempt.attempt_sequence = 0 AND child_attempt.state = 'running'
                  AND child_attempt.checkpoint = child.checkpoint
                  AND child_attempt.claimed_by IS NULL AND child_attempt.lease_expires_at IS NULL
                  AND child_attempt.lease_epoch = 0
                  AND env.operation_step = child.checkpoint
                  AND env.status = CASE child.kind WHEN 'destroy' THEN 'deleting'
                      WHEN 'create' THEN 'provisioning' ELSE 'updating' END
            ) THEN
                RAISE EXCEPTION 'membership successor transition is incomplete';
            END IF;
            RETURN NULL;
        END $$
    """)
    op.execute("""
        CREATE CONSTRAINT TRIGGER dev_lifecycle_membership_successor_complete
        AFTER UPDATE OF state ON dev_lifecycle_operations DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION loom_check_dev_membership_successor_complete()
    """)


def downgrade() -> None:
    op.execute("""
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM dev_lifecycle_operations
                       WHERE membership_predecessor_operation_id IS NOT NULL OR state = 'superseded')
               OR EXISTS (SELECT 1 FROM dev_lifecycle_operation_attempts WHERE state = 'superseded') THEN
                RAISE EXCEPTION 'cannot downgrade 0139 with membership successor history';
            END IF;
        END $$
    """)
    op.execute("DROP TRIGGER dev_lifecycle_membership_successor_complete ON dev_lifecycle_operations")
    op.execute("DROP FUNCTION loom_check_dev_membership_successor_complete()")
    op.execute("DROP TRIGGER dev_lifecycle_membership_successor_attempt_guard ON dev_lifecycle_operation_attempts")
    op.execute("DROP FUNCTION loom_guard_dev_membership_successor_attempt()")
    op.execute("DROP TRIGGER dev_lifecycle_membership_lineage_guard ON dev_lifecycle_operations")
    op.execute("DROP FUNCTION loom_guard_dev_membership_lineage()")
    for (table, suffix), old in _OLD_CHECKS.items():
        name = f"{table}_{suffix}_check"
        op.drop_constraint(name, table, type_="check")
        op.create_check_constraint(name, table, old)
    for suffix in ("successor_fields", "superseded"):
        op.drop_constraint(f"dev_lifecycle_operations_{suffix}_check", _OPERATIONS, type_="check")
    op.drop_constraint("dev_lifecycle_operations_successor_predecessor_uidx", _OPERATIONS, type_="unique")
    op.drop_constraint("dev_lifecycle_operations_membership_predecessor_fkey", _OPERATIONS, type_="foreignkey")
    op.drop_constraint("dev_lifecycle_operations_membership_accepted_fkey", _OPERATIONS, type_="foreignkey")
    for column in (
        "membership_continuation_kind", "membership_successor_binding_sha256",
        "membership_successor_binding", "membership_predecessor_envelope_sha256",
        "membership_predecessor_operation_id",
        "membership_accepted_operation_id",
    ):
        op.drop_column(_OPERATIONS, column)
