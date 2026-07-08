"""Integration tests for ``/api/v1/tasksets`` (#242 sub-plan 2)."""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine, text

from tests.integration.taskset_fixtures import _manifest_bytes


@pytest.mark.asyncio
async def test_post_taskset_happy_path(tasksets_setup) -> None:
    app, tokens, teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")},
        )
    assert resp.status_code == 202, resp.text
    body = resp.json()
    task_set_id = f"ts/{teams['team_a']}/sample-tasks"
    assert body["task_set_id"] == task_set_id
    assert body["status"] == "materializing"
    assert body["capabilities"] == ["trajectory-only"]
    assert body["evaluation_ready"] is False

    sync_engine = create_engine(str(app.state.settings.db_url))
    with sync_engine.begin() as conn:
        job_state = conn.execute(
            text(
                "SELECT state FROM task_set_materialization_jobs "
                "WHERE task_set_id = :id",
            ),
            {"id": task_set_id},
        ).scalar_one()
    sync_engine.dispose()
    assert job_state == "queued"


@pytest.mark.asyncio
async def test_verifier_infers_evaluation(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    manifest = _manifest_bytes(
        verifier="verifier:\n  type: pytest\n  file: verifier/test.py\n",
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={
                "manifest": ("manifest.yaml", manifest, "application/x-yaml"),
                "verifier": ("verifier/test.py", b"def test_x(): pass", "text/x-python"),
            },
        )
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert "evaluation" in body["inferred_intents"]
    assert body["capabilities"] == ["both"]
    assert any(w["code"] == "evaluation_inferred_from_verifier" for w in body["warnings"])


@pytest.mark.asyncio
async def test_cross_team_get_returns_404(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        post = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")},
        )
        task_set_id = post.json()["task_set_id"]
        get_resp = await client.get(
            f"/api/v1/tasksets/{task_set_id}",
            headers={"Authorization": f"Bearer {tokens['team_b']}"},
        )
    assert post.status_code == 202
    assert get_resp.status_code == 404


@pytest.mark.asyncio
async def test_evaluation_without_verifier_returns_400(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    manifest = _manifest_bytes(intents="  - evaluation\n")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={"manifest": ("manifest.yaml", manifest, "application/x-yaml")},
        )
    assert resp.status_code == 400
    assert resp.json()["detail"] == "verifier_required_for_evaluation"


@pytest.mark.asyncio
async def test_duplicate_slug_returns_409(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {"Authorization": f"Bearer {tokens['team_a']}"}
        files = {"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")}
        first = await client.post("/api/v1/tasksets", headers=headers, files=files)
        second = await client.post("/api/v1/tasksets", headers=headers, files=files)
    assert first.status_code == 202
    assert second.status_code == 409


@pytest.mark.asyncio
async def test_legacy_team_token_cannot_submit(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['legacy_a']}"},
            files={"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")},
        )
    assert resp.status_code == 403
    assert "legacy team token" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_duplicate_slug_does_not_overwrite_stored_manifest(tasksets_setup) -> None:
    app, tokens, teams = tasksets_setup
    settings = app.state.settings
    manifest_key = f"tasksets/user/{teams['team_a']}/sample-tasks/manifest.yaml"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {"Authorization": f"Bearer {tokens['team_a']}"}
        first = await client.post(
            "/api/v1/tasksets",
            headers=headers,
            files={
                "manifest": (
                    "manifest.yaml",
                    _manifest_bytes(display_name="Original Tasks"),
                    "application/x-yaml",
                ),
            },
        )
        assert first.status_code == 202, first.text
        stored = app.state.minio_client.get_object(
            Bucket=settings.artifacts_bucket,
            Key=manifest_key,
        )["Body"].read()

        second = await client.post(
            "/api/v1/tasksets",
            headers=headers,
            files={
                "manifest": (
                    "manifest.yaml",
                    _manifest_bytes(display_name="Replacement Tasks"),
                    "application/x-yaml",
                ),
            },
        )
        assert second.status_code == 409

        after = app.state.minio_client.get_object(
            Bucket=settings.artifacts_bucket,
            Key=manifest_key,
        )["Body"].read()
    assert after == stored
    assert b"Original Tasks" in stored
    assert b"Replacement Tasks" not in stored


@pytest.mark.asyncio
async def test_get_delete_rebuild(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {"Authorization": f"Bearer {tokens['team_a']}"}
        post = await client.post(
            "/api/v1/tasksets",
            headers=headers,
            files={"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")},
        )
        task_set_id = post.json()["task_set_id"]
        get_resp = await client.get(f"/api/v1/tasksets/{task_set_id}", headers=headers)
        assert get_resp.status_code == 200
        assert get_resp.json()["materialization_job_state"] == "queued"
        assert get_resp.json()["error_summary"] == []

        rebuild = await client.post(
            f"/api/v1/tasksets/{task_set_id}/rebuild",
            headers=headers,
        )
        assert rebuild.status_code == 409

        delete_resp = await client.delete(
            f"/api/v1/tasksets/{task_set_id}",
            headers=headers,
        )
        assert delete_resp.status_code == 204

        gone = await client.get(f"/api/v1/tasksets/{task_set_id}", headers=headers)
        assert gone.status_code == 404


@pytest.mark.asyncio
async def test_list_tasksets_returns_own_team_rows(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {"Authorization": f"Bearer {tokens['team_a']}"}
        post = await client.post(
            "/api/v1/tasksets",
            headers=headers,
            files={"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")},
        )
        assert post.status_code == 202
        task_set_id = post.json()["task_set_id"]
        list_resp = await client.get("/api/v1/tasksets", headers=headers)
    assert list_resp.status_code == 200
    items = list_resp.json()["items"]
    assert len(items) == 1
    row = items[0]
    assert row["task_set_id"] == task_set_id
    assert row["display_name"] == "Sample Tasks"
    assert row["status"] == "materializing"
    assert row["evaluation_ready"] is False
    assert row["task_count"] == 0
    assert "created_at" in row


@pytest.mark.asyncio
async def test_list_tasksets_excludes_other_team(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")},
        )
        list_b = await client.get(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_b']}"},
        )
    assert list_b.status_code == 200
    assert list_b.json()["items"] == []


@pytest.mark.asyncio
async def test_list_tasksets_excludes_soft_deleted(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {"Authorization": f"Bearer {tokens['team_a']}"}
        post = await client.post(
            "/api/v1/tasksets",
            headers=headers,
            files={"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")},
        )
        task_set_id = post.json()["task_set_id"]
        delete_resp = await client.delete(
            f"/api/v1/tasksets/{task_set_id}",
            headers=headers,
        )
        assert delete_resp.status_code == 204
        list_resp = await client.get("/api/v1/tasksets", headers=headers)
    assert list_resp.status_code == 200
    assert list_resp.json()["items"] == []
