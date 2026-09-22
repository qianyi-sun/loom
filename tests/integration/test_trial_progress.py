"""Preparation is visible before an execution lease exists."""
from uuid import uuid4

import httpx
from sqlalchemy import delete

from loom.db.schema import TaskImageMaterialization, Trial, TrialTaskImageMaterialization
from tests.integration.test_service_trials_read import trials_setup  # noqa: F401


async def test_shared_image_progress_and_filter_before_execution(trials_setup):  # noqa: F811
    app, token, _, ids = trials_setup
    image_id = uuid4()
    seed_id = ids[0]
    ids = [uuid4(), uuid4()]
    async with app.state.session_factory() as session, session.begin():
        seed = await session.get(Trial, seed_id)
        trial = Trial(id=ids[0], task_id=seed.task_id, team_id=seed.team_id,
                      state="queued", config=seed.config, requires_caps={"backend": "nebius"})
        session.add(trial)
        await session.flush()
        session.add(TaskImageMaterialization(
            id=image_id, materialization_key=uuid4().hex * 2, task_id=trial.task_id,
            task_checksum="a" * 64, cpu_arch="x86_64", task_config={}, state="queued",
        ))
        await session.flush()
        other = Trial(id=ids[1], task_id=seed.task_id, team_id=seed.team_id,
                      state="queued", config=seed.config, requires_caps={"backend": "nebius"})
        session.add(other)
        await session.flush()
        session.add_all([TrialTaskImageMaterialization(trial_id=trial_id, materialization_id=image_id)
                         for trial_id in ids])
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://svc",
            headers={"Authorization": f"Bearer {token}"},
        ) as client:
            detail = await client.get(f"/api/v1/trials/{ids[0]}")
            assert detail.status_code == 200, detail.text
            assert detail.json()["progress"]["stage"] == "image_preparation"
            listed = await client.get("/api/v1/trials", params={"state": "stage:image_preparation"})
            assert {item["id"] for item in listed.json()["items"]} == {str(i) for i in ids}
            assert listed.json()["items"][0]["progress"]["stage"] == "image_preparation"
            summary = await client.get("/api/v1/monitor/summary", params={"view": "trials"})
            assert summary.status_code == 200, summary.text
            assert summary.json()["progress"]["stages"]["image_preparation"] == 2
            assert summary.json()["progress"]["images"]["states"]["queued"] == 1
            assert summary.json()["queue"]["status"] != "blocked"
            async with app.state.session_factory() as session, session.begin():
                trial = await session.get(Trial, ids[0])
                trial.state = "cancelled"
            # A shared cache that remains queued cannot override terminal history.
            detail = await client.get(f"/api/v1/trials/{ids[0]}")
            assert detail.json()["progress"]["stage"] == "cancelled"
            async with app.state.session_factory() as session, session.begin():
                image = await session.get(TaskImageMaterialization, image_id)
                image.state = "ready"
            reused = await client.get(f"/api/v1/trials/{ids[1]}")
            assert reused.json()["progress"]["stage"] == "execution_wait"
            cancelled = await client.get(f"/api/v1/trials/{ids[0]}")
            assert cancelled.json()["progress"]["stage"] == "cancelled"
    finally:
        async with app.state.session_factory() as session, session.begin():
            await session.execute(delete(TrialTaskImageMaterialization).where(
                TrialTaskImageMaterialization.materialization_id == image_id))
            await session.execute(delete(TaskImageMaterialization).where(
                TaskImageMaterialization.id == image_id))
            await session.execute(delete(Trial).where(Trial.id.in_(ids)))
