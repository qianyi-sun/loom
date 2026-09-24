"""Allow one audited archival retry for the legacy verifier projection defect.

Revision ID: 0157
Revises: 0156
"""
import sqlalchemy as sa
from alembic import op

revision = "0157"
down_revision = "0156"
branch_labels = None
depends_on = None

_COLUMN = "materialization_recovery_requested_at"
_TERMINAL = "IF OLD.materialization_state IN ('committed','unavailable')"
_AUTHORIZATION = """(
            OLD.materialization_state = 'unavailable'
            AND OLD.materialization_error_code = 'verifier_reward_drift'
            AND OLD.materialization_recovery_requested_at IS NULL
            AND NEW.materialization_recovery_requested_at IS NOT NULL
            AND NEW.materialization_state = 'pending'
            AND NEW.materialization_next_attempt_at = NEW.materialization_recovery_requested_at
            AND OLD.output_commit_state = 'committed'
            AND OLD.output_generation = OLD.resource_generation
            AND OLD.execution_role = 'attempt'
            AND OLD.finalized_at IS NOT NULL
            AND OLD.desired_state = 'deleted' AND OLD.observed_state = 'deleted'
            AND OLD.deleted_at IS NOT NULL AND OLD.cleanup_state = 'complete'
            AND OLD.source_cleanup_state = 'not_ready'
            AND (to_jsonb(NEW) - ARRAY['materialization_state','materialization_next_attempt_at',
                 'materialization_recovery_requested_at','updated_at']::text[])
                = (to_jsonb(OLD) - ARRAY['materialization_state','materialization_next_attempt_at',
                 'materialization_recovery_requested_at','updated_at']::text[])
            AND EXISTS (SELECT 1 FROM trials t WHERE t.id = OLD.trial_id AND t.team_id = OLD.team_id
                AND t.attempt_count = OLD.attempt AND t.state = 'failed'
                AND t.failure_reason = 'output_unavailable'
                AND t.result->'runtime_result'->>'status' = 'verifier_error'
                AND t.result->'runtime_result'->'partial_evidence' = 'true'::jsonb
                AND COALESCE(t.result->'runtime_result'->'verifier_rewards', 'null'::jsonb) = 'null'::jsonb
                AND jsonb_path_query_first(t.result,
                    '$.runtime_result.outputs[*] ? (@.kind == "verifier" && @.state == "captured")')
                    ->>'relative_path' = 'diagnostics/verifier-exception.json')
          )"""
_GUARD = """IF NEW.materialization_recovery_requested_at IS DISTINCT FROM OLD.materialization_recovery_requested_at
             AND NOT COALESCE(""" + _AUTHORIZATION + """, false) THEN
            RAISE EXCEPTION 'archival recovery requires one diagnosed deleted verifier attempt';
          END IF;
          """
_HISTORY = "'materialization_state', NEW.materialization_state,"
_HISTORY_NEW = _HISTORY + "\n            'materialization_recovery_requested_at', NEW.materialization_recovery_requested_at,"


def _replace_function(name: str, replacements: tuple[tuple[str, str, int], ...]) -> None:
    definition = op.get_bind().scalar(sa.text(
        "SELECT pg_get_functiondef(to_regprocedure(:name))"
    ), {"name": name + "()"})
    if not isinstance(definition, str):
        raise RuntimeError("missing execution lease mutation/history function")
    for before, after, count in replacements:
        if definition.count(before) != count:
            raise RuntimeError("unexpected execution lease mutation/history function")
        definition = definition.replace(before, after)
    op.execute(sa.text(definition))


def upgrade() -> None:
    op.add_column("execution_leases", sa.Column(_COLUMN, sa.TIMESTAMP(timezone=True), nullable=True))
    _replace_function("validate_execution_lease_mutation", (
        (_TERMINAL, _GUARD + _TERMINAL + " AND NOT COALESCE(" + _AUTHORIZATION + ", false)", 1),
        ("'materialization_state','materialization_attempts',",
         "'materialization_state','materialization_attempts','materialization_recovery_requested_at',", 2),
    ))
    _replace_function("append_execution_lease_history", ((_HISTORY, _HISTORY_NEW, 1),))


def downgrade() -> None:
    if op.get_bind().scalar(sa.text(
        "SELECT EXISTS (SELECT 1 FROM execution_leases WHERE materialization_recovery_requested_at IS NOT NULL)"
    )):
        raise RuntimeError("cannot remove retained archival recovery evidence")
    _replace_function("validate_execution_lease_mutation", (
        (_GUARD + _TERMINAL + " AND NOT COALESCE(" + _AUTHORIZATION + ", false)", _TERMINAL, 1),
        ("'materialization_state','materialization_attempts','materialization_recovery_requested_at',",
         "'materialization_state','materialization_attempts',", 2),
    ))
    _replace_function("append_execution_lease_history", ((_HISTORY_NEW, _HISTORY, 1),))
    op.drop_column("execution_leases", _COLUMN)
