from __future__ import annotations

import hashlib
import json
from typing import Any

import pytest

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.steps.s13_smoke import SmokeStep


def test_smoke_posts_current_trial_config_contract(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(13, "smoke")
    captured_payloads: list[dict[str, Any]] = []

    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-token")
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s13_smoke._ingress_base",
        lambda _ctx: "https://loom.test",
    )

    def fake_get(url: str, *, token: str | None = None) -> tuple[int, bytes]:
        assert token == "admin-token"
        if url.endswith("/api/v1/health"):
            return 200, b'{"status":"ok"}'
        if url.endswith("/api/v1/benchmarks"):
            return 200, b'{"items":[{"id":"sample-tasks"}]}'
        if url.endswith("/api/v1/trials/trial-1"):
            return 200, b'{"id":"trial-1","state":"succeeded","aggregate_reward":1.0}'
        if url.endswith("/api/v1/usage"):
            return 200, b'{"items":[]}'
        raise AssertionError(f"unexpected GET {url}")

    def fake_post(
        url: str,
        payload: dict[str, object],
        *,
        token: str | None = None,
    ) -> tuple[int, bytes]:
        assert url.endswith("/api/v1/trials")
        assert token == "admin-token"
        captured_payloads.append(dict(payload))
        return 201, b'{"trial_id":"trial-1"}'

    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_get", fake_get)
    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_post", fake_post)

    result = SmokeStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert captured_payloads == [
        {
            "task_id": "hello/hello-world",
            "config": {"agent_name": "oracle", "agent_model": None},
            "idempotency_key": "smoke-" + hashlib.sha256(
                f"{ctx.image_tag}|{ctx.resolved_sha}".encode(),
            ).hexdigest()[:16],
        }
    ]
    assert json.loads(step_dir.artifact_path("03-submit.json").read_text()) == {
        "trial_id": "trial-1",
    }
