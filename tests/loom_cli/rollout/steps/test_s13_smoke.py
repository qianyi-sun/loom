from __future__ import annotations

import hashlib
import json
from typing import Any

import pytest

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.steps.s13_smoke import SmokeStep


def test_smoke_posts_current_trial_config_contract_with_user_owned_token(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(13, "smoke")
    captured_payloads: list[dict[str, Any]] = []

    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-token")
    monkeypatch.setenv("LOOM_SMOKE_API_TOKEN", "smoke-user-token")
    monkeypatch.setenv("LOOM_SMOKE_TASK_ID", "terminal-bench-2/hello-world")
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s13_smoke._ingress_base",
        lambda _ctx: "https://loom.test",
    )

    def fake_get(url: str, *, token: str | None = None) -> tuple[int, bytes]:
        assert token == "smoke-user-token"
        if url.endswith("/api/v1/health"):
            return 200, b'{"status":"ok"}'
        if url.endswith("/api/v1/auth/whoami"):
            return (
                200,
                b'{"credential_type":"user_owned_api_token",'
                b'"username":"Qianyi","team_name":"admin",'
                b'"scopes":["read:own","submit"]}',
            )
        if url.endswith("/api/v1/benchmarks"):
            return 200, b'{"items":[{"id":"sample-tasks"}]}'
        if url.endswith("/api/v1/tasks/terminal-bench-2/hello-world"):
            return (
                200,
                b'{"id":"terminal-bench-2/hello-world",'
                b'"benchmark_id":"terminal-bench-2"}',
            )
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
        assert token == "smoke-user-token"
        captured_payloads.append(dict(payload))
        return 201, b'{"trial_id":"trial-1"}'

    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_get", fake_get)
    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_post", fake_post)

    result = SmokeStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert captured_payloads == [
        {
            "task_id": "terminal-bench-2/hello-world",
            "config": {"agent_name": "oracle", "agent_model": None},
            "idempotency_key": "smoke-" + hashlib.sha256(
                f"{ctx.image_tag}|{ctx.resolved_sha}".encode(),
            ).hexdigest()[:16],
        }
    ]
    assert json.loads(step_dir.artifact_path("05-submit.json").read_text()) == {
        "trial_id": "trial-1",
    }


def test_current_gb10_smoke_defaults_to_gb10_compatible_task_and_pool(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, scope="current-gb10", exclude_oldlab=True)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(13, "smoke")
    captured_payloads: list[dict[str, Any]] = []

    monkeypatch.setenv("LOOM_SMOKE_API_TOKEN", "smoke-user-token")
    monkeypatch.delenv("LOOM_SMOKE_TASK_ID", raising=False)
    monkeypatch.delenv("LOOM_SMOKE_REQUIRED_WORKER_POOL", raising=False)
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s13_smoke._ingress_base",
        lambda _ctx: "https://loom.test",
    )

    def fake_get(url: str, *, token: str | None = None) -> tuple[int, bytes]:
        assert token == "smoke-user-token"
        if url.endswith("/api/v1/health"):
            return 200, b'{"status":"ok"}'
        if url.endswith("/api/v1/auth/whoami"):
            return (
                200,
                b'{"credential_type":"user_owned_api_token",'
                b'"scopes":["read:own","submit"]}',
            )
        if url.endswith("/api/v1/benchmarks"):
            return 200, b'{"items":[{"id":"skilllearnbench"}]}'
        if url.endswith(
            "/api/v1/tasks/skilllearnbench/anthropic-poster-design/"
            "anthropic-poster-design-1",
        ):
            return (
                200,
                b'{"id":"skilllearnbench/anthropic-poster-design/'
                b'anthropic-poster-design-1",'
                b'"benchmark_id":"skilllearnbench"}',
            )
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
        assert token == "smoke-user-token"
        captured_payloads.append(dict(payload))
        return 201, b'{"trial_id":"trial-1"}'

    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_get", fake_get)
    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_post", fake_post)

    result = SmokeStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert captured_payloads == [
        {
            "task_id": (
                "skilllearnbench/anthropic-poster-design/"
                "anthropic-poster-design-1"
            ),
            "config": {"agent_name": "oracle", "agent_model": None},
            "idempotency_key": "smoke-" + hashlib.sha256(
                f"{ctx.image_tag}|{ctx.resolved_sha}".encode(),
            ).hexdigest()[:16],
            "required_worker_pool": "gb10-arm64",
        }
    ]


def test_full_cluster_smoke_keeps_terminal_bench_default_without_pool(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, scope="full-cluster")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(13, "smoke")
    captured_payloads: list[dict[str, Any]] = []

    monkeypatch.setenv("LOOM_SMOKE_API_TOKEN", "smoke-user-token")
    monkeypatch.delenv("LOOM_SMOKE_TASK_ID", raising=False)
    monkeypatch.delenv("LOOM_SMOKE_REQUIRED_WORKER_POOL", raising=False)
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s13_smoke._ingress_base",
        lambda _ctx: "https://loom.test",
    )

    def fake_get(url: str, *, token: str | None = None) -> tuple[int, bytes]:
        assert token == "smoke-user-token"
        if url.endswith("/api/v1/health"):
            return 200, b'{"status":"ok"}'
        if url.endswith("/api/v1/auth/whoami"):
            return (
                200,
                b'{"credential_type":"user_owned_api_token",'
                b'"scopes":["read:own","submit"]}',
            )
        if url.endswith("/api/v1/benchmarks"):
            return 200, b'{"items":[{"id":"terminal-bench-2"}]}'
        if url.endswith("/api/v1/tasks/terminal-bench-2/hello-world"):
            return (
                200,
                b'{"id":"terminal-bench-2/hello-world",'
                b'"benchmark_id":"terminal-bench-2"}',
            )
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
        assert token == "smoke-user-token"
        captured_payloads.append(dict(payload))
        return 201, b'{"trial_id":"trial-1"}'

    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_get", fake_get)
    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_post", fake_post)

    result = SmokeStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert captured_payloads == [
        {
            "task_id": "terminal-bench-2/hello-world",
            "config": {"agent_name": "oracle", "agent_model": None},
            "idempotency_key": "smoke-" + hashlib.sha256(
                f"{ctx.image_tag}|{ctx.resolved_sha}".encode(),
            ).hexdigest()[:16],
        }
    ]


def test_smoke_rejects_non_user_owned_smoke_token_before_submit(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(13, "smoke")

    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-token")
    monkeypatch.setenv("LOOM_SMOKE_API_TOKEN", "legacy-token")
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s13_smoke._ingress_base",
        lambda _ctx: "https://loom.test",
    )

    def fake_get(url: str, *, token: str | None = None) -> tuple[int, bytes]:
        assert token == "legacy-token"
        if url.endswith("/api/v1/health"):
            return 200, b'{"status":"ok"}'
        if url.endswith("/api/v1/auth/whoami"):
            return 200, b'{"credential_type":"legacy_team_token"}'
        raise AssertionError(f"unexpected GET {url}")

    def fake_post(
        url: str,
        payload: dict[str, object],
        *,
        token: str | None = None,
    ) -> tuple[int, bytes]:
        raise AssertionError("smoke submitted before validating whoami")

    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_get", fake_get)
    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_post", fake_post)

    result = SmokeStep().run(ctx, step_dir)

    assert result.exit_code == 1
    assert "user-owned API token" in str(result.error)


def test_smoke_rejects_missing_smoke_task_before_submit(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(13, "smoke")

    monkeypatch.setenv("LOOM_SMOKE_API_TOKEN", "smoke-user-token")
    monkeypatch.setenv("LOOM_SMOKE_TASK_ID", "missing/task")
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s13_smoke._ingress_base",
        lambda _ctx: "https://loom.test",
    )

    def fake_get(url: str, *, token: str | None = None) -> tuple[int, bytes]:
        assert token == "smoke-user-token"
        if url.endswith("/api/v1/health"):
            return 200, b'{"status":"ok"}'
        if url.endswith("/api/v1/auth/whoami"):
            return (
                200,
                b'{"credential_type":"user_owned_api_token",'
                b'"scopes":["read:own","submit"]}',
            )
        if url.endswith("/api/v1/benchmarks"):
            return 200, b'{"items":[{"id":"sample-tasks"}]}'
        if url.endswith("/api/v1/tasks/missing/task"):
            return 404, b'{"detail":"task not found"}'
        raise AssertionError(f"unexpected GET {url}")

    def fake_post(
        url: str,
        payload: dict[str, object],
        *,
        token: str | None = None,
    ) -> tuple[int, bytes]:
        raise AssertionError("smoke submitted before validating task existence")

    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_get", fake_get)
    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_post", fake_post)

    result = SmokeStep().run(ctx, step_dir)

    assert result.exit_code == 1
    assert "missing/task" in str(result.error)
    assert "not found" in str(result.error)
