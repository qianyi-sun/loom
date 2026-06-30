from __future__ import annotations

from loom_service.routes.monitor import _resource_trials_stmt


def test_resource_trials_stmt_limits_rows_to_active_states() -> None:
    compiled = str(_resource_trials_stmt().compile(compile_kwargs={"literal_binds": True}))

    assert "trials.state IN ('queued', 'claimed', 'running')" in compiled
