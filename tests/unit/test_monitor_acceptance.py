from uuid import uuid4

from loom_service.routes.monitor import _public_execution_capacity


def test_unavailable_forecast_is_not_reported_as_zero_capacity():
    target = {"target_id": "primary", "desired_state": "active"}
    profile = {
        "target_id": "primary",
        "forecast_is_fresh": False,
        "immediate_executable_slots": 0,
        "configured_total_fit_slots": 0,
        "configured_scale_headroom_slots": 0,
        "blockers": ["resource_profile_binding_unavailable", "resource_calibration_unavailable"],
    }
    result = _public_execution_capacity({"targets": [target]}, {"targets": [profile]}, admin=False)
    assert result[0]["resource_profile"]["immediate_executable_slots"] is None
    profile.update(forecast_is_fresh=True, blockers=[])
    result = _public_execution_capacity({"targets": [target]}, {"targets": [profile]}, admin=False)
    assert result[0]["resource_profile"]["immediate_executable_slots"] == 0
    # A calibrated, fresh observation with an exhausted quota is a real zero.
    profile.update(
        forecast_is_fresh=False,
        binding={"enabled": True},
        calibration={"eligible": True},
        blockers=["resource_forecast_quota_nodes_exhausted"],
    )
    target["observation"] = {"is_fresh": True}
    result = _public_execution_capacity({"targets": [target]}, {"targets": [profile]}, admin=False)
    assert result[0]["resource_profile"]["configured_total_fit_slots"] == 0


def test_artifact_projection_merges_metadata_without_inventing_empty_files():
    from fastapi import FastAPI
    from starlette.requests import Request

    from loom_service.routes.trials import _merge_projected_artifacts, _projected_artifacts

    app = FastAPI()
    app.add_api_route(
        "/trials/{trial_id}/artifacts/download", lambda: None, name="download_artifact"
    )
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [],
            "app": app,
            "scheme": "https",
            "server": ("loom.test", 443),
            "router": app.router,
        }
    )
    projected = _projected_artifacts(
        request,
        trial_id=uuid4(),
        trajectory_index={
            "artifacts": [
                {"key": "result.json"},
                {"key": "empty.log", "size": 0},
                {"key": "unknown.log"},
            ]
        },
    )
    merged = _merge_projected_artifacts(
        projected,
        [
            {
                "key": "result.json",
                "size": 4096,
                "share_status": "blocked",
                "blocked_reason": "policy",
            },
        ],
    )
    assert len(merged) == 3
    assert merged[0]["size"] == 4096
    assert merged[0]["share_status"] == "blocked"
    assert merged[1]["size"] == 0
    assert merged[2]["size"] is None
