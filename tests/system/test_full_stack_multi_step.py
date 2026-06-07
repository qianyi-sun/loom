"""Full-stack: submit multi-step-3 → all 3 steps complete → aggregate
reward = mean of step rewards."""

from __future__ import annotations

import time

import httpx

from tests.system.docker_compose import run_seed


def test_multi_step_aggregates_mean(compose_stack: dict[str, str]) -> None:
    cp = compose_stack["control_plane"]
    # Seed an additional fixture (the session stack is already running and
    # already seeded hello-world by stack_up). multi-step-3 needs its own
    # task row + team token.
    raw_token = run_seed(task_id="multi-step-3", which="team")

    r = httpx.post(
        f"{cp}/trials",
        headers={"Authorization": f"Bearer {raw_token}"},
        json={
            "task_id": "multi-step-3",
            "config": {"step_aggregation": "mean"},
        },
        timeout=10,
    )
    assert r.status_code == 201, r.text
    trial_id = r.json()["trial_id"]

    body: dict = {}
    for _ in range(600):
        r = httpx.get(
            f"{cp}/trials/{trial_id}",
            headers={"Authorization": f"Bearer {raw_token}"},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        if body["state"] in ("succeeded", "failed", "cancelled"):
            break
        time.sleep(1.0)
    assert body["state"] == "succeeded", body
    steps = body.get("steps") or []
    assert len(steps) == 3, f"expected 3 steps, got {len(steps)}"
