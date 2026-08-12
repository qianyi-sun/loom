from loom_control_plane.routes.trials import _CANCEL_SQL


def test_controller_owned_trial_cancel_records_observation() -> None:
    sql = str(_CANCEL_SQL)
    assert "cancellation_observed_at" in sql
    assert "state = 'queued'" in sql
    assert "ELSE cancellation_observed_at" in sql
