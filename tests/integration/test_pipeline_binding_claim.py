from __future__ import annotations

from loom_control_plane.scheduler.claim import _WORK_CLAIM_SQL


def test_claim_sql_fences_control_snapshot_provider_assets_and_budget_before_pick() -> None:
    sql = str(_WORK_CLAIM_SQL)
    for authority in (
        "pipeline_run_control_bindings",
        "judge_execution_profiles",
        "recipe_provider_bindings",
        "provider_connections",
        "execution_attempt_provider_budgets",
        "provider_asset_locks",
        "snapshot_bytes",
        "snapshot_sha256",
        "allowed_team_ids",
    ):
        assert authority in sql
    assert sql.index("pipeline_run_control_bindings") < sql.index("), picked AS")
    assert "connection.status = 'valid'" in sql
    assert "connection.deleted_at IS NULL" in sql
