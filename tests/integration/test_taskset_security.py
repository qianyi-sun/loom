"""Security and cross-team isolation tests for TaskSets (#242 sub-plan 8)."""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from tests.integration.test_service_tasksets import _manifest_bytes

pytest_plugins = ["tests.integration.test_service_tasksets"]


@pytest.mark.parametrize(
    ("method", "path_suffix"),
    [
        ("GET", ""),
        ("POST", "/rebuild"),
        ("DELETE", ""),
    ],
)
@pytest.mark.asyncio
async def test_cross_team_endpoint_returns_404(
    tasksets_setup,
    method: str,
    path_suffix: str,
) -> None:
    """Team B cannot read, rebuild, or delete team A's TaskSet."""
    app, tokens, _teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        post = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")},
        )
        assert post.status_code == 202
        task_set_id = post.json()["task_set_id"]
        path = f"/api/v1/tasksets/{task_set_id}{path_suffix}"
        headers = {"Authorization": f"Bearer {tokens['team_b']}"}
        if method == "GET":
            resp = await client.get(path, headers=headers)
        elif method == "POST":
            resp = await client.post(path, headers=headers)
        else:
            resp = await client.delete(path, headers=headers)
    assert resp.status_code == 404
