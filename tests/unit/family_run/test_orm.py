"""ORM: Batch.family_run_spec + Trial.family_key + BatchFamilyState."""

from __future__ import annotations

from loom.db.schema import Batch, BatchFamilyState, Trial


def test_batch_has_family_run_spec_column():
    assert "family_run_spec" in Batch.__table__.columns


def test_trial_has_family_key_column():
    assert "family_key" in Trial.__table__.columns


def test_batch_family_state_has_expected_columns():
    cols = set(BatchFamilyState.__table__.columns.keys())
    assert cols >= {
        "batch_id", "family_key", "task_sequence", "current_index",
        "state", "state_uri", "attempt_count", "next_attempt_at",
        "last_error", "updated_at",
    }


def test_batch_family_state_composite_pk():
    pk = {c.name for c in BatchFamilyState.__table__.primary_key.columns}
    assert pk == {"batch_id", "family_key"}
