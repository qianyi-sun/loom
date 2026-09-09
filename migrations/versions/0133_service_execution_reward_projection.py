"""Backfill missing scalar rewards from service-execution verifier results.

Revision ID: 0133
Revises: 0132
"""

from __future__ import annotations

import json
import math

import sqlalchemy as sa
from alembic import op

revision = "0133"
down_revision = "0132"
branch_labels = None
depends_on = None


def _historical_scalar(rewards: object) -> float | None:
    # Freeze the existing shared scalar semantics in migration history; later
    # application changes must not change the meaning of an already-run revision.
    if not isinstance(rewards, dict) or not rewards:
        return None
    if any(
        not isinstance(key, str)
        or not key
        or len(key.encode("utf-8")) > 256
        or isinstance(value, bool)
        or not isinstance(value, (int, float))
        for key, value in rewards.items()
    ):
        return None
    try:
        values = [float(value) for value in rewards.values()]
    except OverflowError:
        return None
    if not all(math.isfinite(value) for value in values):
        return None
    scalar = values[0] if len(values) == 1 else sum(values) / len(values)
    return scalar if math.isfinite(scalar) else None


def upgrade() -> None:
    connection = op.get_bind()
    rows = connection.execute(
        sa.text("""
        SELECT id, result->'reward' AS rewards
        FROM public.trials
        WHERE result->>'schema_version' = 'loom.service-execution-trial-result.v1'
          AND (result->'aggregate_reward' IS NULL OR result->'aggregate_reward' = 'null'::jsonb)
          AND jsonb_typeof(result->'reward') = 'object'
          AND result->'reward' = result#>'{runtime_result,verifier_rewards}'
        FOR UPDATE
    """)
    )
    for row in rows.mappings():
        scalar = _historical_scalar(row["rewards"])
        if scalar is not None:
            connection.execute(
                sa.text("""
                UPDATE public.trials
                SET result = jsonb_set(result, '{aggregate_reward}', CAST(:scalar AS jsonb), true)
                WHERE id = :id
            """),
                {"id": row["id"], "scalar": json.dumps(scalar)},
            )


def downgrade() -> None:
    # A downgrade must not remove a valid derived score from a completed Trial.
    pass
