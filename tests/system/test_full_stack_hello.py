"""Full-stack: docker-compose up → seed task → submit hello-world → wait
for terminal state → assert succeeded."""

from __future__ import annotations

import time

import httpx

from tests.system.docker_compose import run_seed


def test_hello_world_succeeds(compose_stack: dict[str, str]) -> None:
    cp = compose_stack["control_plane"]
    raw_token = run_seed(task_id="hello-world", which="team")

    r = httpx.post(
        f"{cp}/trials",
        headers={"Authorization": f"Bearer {raw_token}"},
        json={"task_id": "hello-world", "config": {}},
        timeout=10,
    )
    assert r.status_code == 201, r.text
    trial_id = r.json()["trial_id"]

    final_state: str | None = None
    for _ in range(300):
        r = httpx.get(
            f"{cp}/trials/{trial_id}",
            headers={"Authorization": f"Bearer {raw_token}"},
        )
        assert r.status_code == 200, r.text
        final_state = r.json()["state"]
        if final_state in ("succeeded", "failed", "cancelled"):
            break
        time.sleep(1.0)
    assert final_state == "succeeded", (
        f"trial ended in state={final_state!r}; last body={r.text}"
    )
