from __future__ import annotations

import hashlib
import json
import urllib.error
from typing import Any

import pytest

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.steps.s13_smoke import (
    SmokeStep,
    _admin_on_behalf_config,
    _ingress_base,
    _smoke_task_id,
    _trajectory_head_request,
    run_admin_on_behalf_smoke,
)


def test_smoke_api_base_uses_frontend_api_base_path(tmp_path) -> None:
    ctx = make_ctx(tmp_path)
    ctx.cluster_config_path.write_text(
        'ingress_host = "yylx.world"\n'
        'frontend_route_path = "/dev"\n'
        'frontend_api_base_path = "/dev"\n',
        encoding="utf-8",
    )

    assert _ingress_base(ctx) == "https://yylx.world/dev"


def test_smoke_posts_current_trial_config_contract_with_user_owned_token(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(15, "smoke")
    captured_payloads: list[dict[str, Any]] = []

    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-token")
    monkeypatch.setenv("LOOM_SMOKE_API_TOKEN", "smoke-user-token")
    monkeypatch.setenv(
        "LOOM_SMOKE_TASK_ID",
        "terminal-bench-2@tb2.1-r6/chess-best-move",
    )
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
            return 200, b'{"items":[{"id":"terminal-bench-2"}]}'
        if url.endswith(
            "/api/v1/tasks/terminal-bench-2%40tb2.1-r6/chess-best-move",
        ):
            return (
                200,
                b'{"id":"terminal-bench-2@tb2.1-r6/chess-best-move",'
                b'"benchmark_id":"terminal-bench-2@tb2.1-r6"}',
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
            "task_id": "terminal-bench-2@tb2.1-r6/chess-best-move",
            "config": {"agent_name": "oracle", "agent_model": None},
            "idempotency_key": "smoke-"
            + hashlib.sha256(
                f"{ctx.image_tag}|{ctx.resolved_sha}".encode(),
            ).hexdigest()[:16],
        }
    ]
    assert json.loads(step_dir.artifact_path("05-submit.json").read_text()) == {
        "trial_id": "trial-1",
    }


def test_smoke_resolves_user_token_from_context_secret_source(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token_file = tmp_path / "smoke-token"
    token_file.write_text("smoke-user-token\n", encoding="utf-8")
    ctx = make_ctx(
        tmp_path,
        smoke_api_token_source=f"file:{token_file}",
        smoke_task_id="terminal-bench-2@tb2.1-r6/chess-best-move",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(15, "smoke")

    monkeypatch.delenv("LOOM_SMOKE_API_TOKEN", raising=False)
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
                b'{"credential_type":"user_owned_api_token","scopes":["read:own","submit"]}',
            )
        if url.endswith("/api/v1/benchmarks"):
            return 200, b'{"items":[{"id":"terminal-bench-2"}]}'
        if url.endswith(
            "/api/v1/tasks/terminal-bench-2%40tb2.1-r6/chess-best-move",
        ):
            return (
                200,
                b'{"id":"terminal-bench-2@tb2.1-r6/chess-best-move",'
                b'"benchmark_id":"terminal-bench-2@tb2.1-r6"}',
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
        return 201, b'{"trial_id":"trial-1"}'

    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_get", fake_get)
    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_post", fake_post)

    result = SmokeStep().run(ctx, step_dir)

    assert result.exit_code == 0
    fingerprint = SmokeStep()._inputs_fingerprint(ctx)
    assert fingerprint["smoke_api_token_source"] == f"file:{token_file}"
    assert "smoke-user-token" not in json.dumps(fingerprint, sort_keys=True)
    assert "smoke-user-token" not in step_dir.stdout_path().read_text()


def test_smoke_uses_token_when_heading_platform_trajectory_download_url(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(15, "smoke")

    monkeypatch.setenv("LOOM_SMOKE_API_TOKEN", "smoke-user-token")
    monkeypatch.setenv(
        "LOOM_SMOKE_TASK_ID",
        "terminal-bench-2@tb2.1-r6/chess-best-move",
    )
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
                b'{"credential_type":"user_owned_api_token","scopes":["read:own","submit"]}',
            )
        if url.endswith("/api/v1/benchmarks"):
            return 200, b'{"items":[{"id":"terminal-bench-2"}]}'
        if url.endswith(
            "/api/v1/tasks/terminal-bench-2%40tb2.1-r6/chess-best-move",
        ):
            return (
                200,
                b'{"id":"terminal-bench-2@tb2.1-r6/chess-best-move",'
                b'"benchmark_id":"terminal-bench-2@tb2.1-r6"}',
            )
        if url.endswith("/api/v1/trials/trial-1"):
            return (
                200,
                b'{"id":"trial-1","state":"succeeded","aggregate_reward":1.0,'
                b'"trajectory_url":"http://loom.test/api/v1/trials/trial-1/'
                b'trajectory/download"}',
            )
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
        return 201, b'{"trial_id":"trial-1"}'

    class _HeadResponse:
        status = 200

        def __init__(self) -> None:
            self.headers = {"Content-Length": "123"}

        def __enter__(self) -> _HeadResponse:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    def fake_urlopen(request: Any, timeout: float) -> _HeadResponse:
        assert timeout == 15
        assert request.get_method() == "HEAD"
        assert request.full_url == ("https://loom.test/api/v1/trials/trial-1/trajectory/download")
        if request.get_header("Authorization") != "Bearer smoke-user-token":
            raise urllib.error.HTTPError(
                request.full_url,
                401,
                "Unauthorized",
                hdrs=None,
                fp=None,
            )
        return _HeadResponse()

    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_get", fake_get)
    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_post", fake_post)
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s13_smoke.urllib.request.urlopen",
        fake_urlopen,
    )

    result = SmokeStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert step_dir.artifact_path("07-trajectory-head.txt").read_text() == (
        "status=200\ncontent-length=123\n"
    )


def test_trajectory_head_request_keeps_external_signed_url_anonymous() -> None:
    req = _trajectory_head_request(
        "https://minio.internal/traj?X-Amz-Signature=abc",
        ingress_base="https://loom.test",
        token="smoke-user-token",
    )

    assert req.full_url == "https://minio.internal/traj?X-Amz-Signature=abc"
    assert req.get_method() == "HEAD"
    assert req.get_header("Authorization") is None


def test_trajectory_head_request_authenticates_prefixed_platform_url() -> None:
    req = _trajectory_head_request(
        "https://yylx.world/dev/api/v1/trials/trial-1/trajectory/download",
        ingress_base="https://yylx.world/dev",
        token="smoke-user-token",
    )

    assert req.full_url == "https://yylx.world/dev/api/v1/trials/trial-1/trajectory/download"
    assert req.get_method() == "HEAD"
    assert req.get_header("Authorization") == "Bearer smoke-user-token"


def test_trajectory_head_request_normalizes_root_api_url_to_prefixed_api_base() -> None:
    req = _trajectory_head_request(
        "https://yylx.world/api/v1/trials/trial-1/trajectory/download",
        ingress_base="https://yylx.world/dev",
        token="smoke-user-token",
    )

    assert req.full_url == "https://yylx.world/dev/api/v1/trials/trial-1/trajectory/download"
    assert req.get_method() == "HEAD"
    assert req.get_header("Authorization") == "Bearer smoke-user-token"


def test_smoke_get_probes_platform_trajectory_when_head_not_allowed(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(15, "smoke")

    monkeypatch.setenv("LOOM_SMOKE_API_TOKEN", "smoke-user-token")
    monkeypatch.setenv(
        "LOOM_SMOKE_TASK_ID",
        "terminal-bench-2@tb2.1-r6/chess-best-move",
    )
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
                b'{"credential_type":"user_owned_api_token","scopes":["read:own","submit"]}',
            )
        if url.endswith("/api/v1/benchmarks"):
            return 200, b'{"items":[{"id":"terminal-bench-2"}]}'
        if url.endswith(
            "/api/v1/tasks/terminal-bench-2%40tb2.1-r6/chess-best-move",
        ):
            return (
                200,
                b'{"id":"terminal-bench-2@tb2.1-r6/chess-best-move",'
                b'"benchmark_id":"terminal-bench-2@tb2.1-r6"}',
            )
        if url.endswith("/api/v1/trials/trial-1"):
            return (
                200,
                b'{"id":"trial-1","state":"succeeded","aggregate_reward":1.0,'
                b'"trajectory_url":"http://loom.test/api/v1/trials/trial-1/'
                b'trajectory/download"}',
            )
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
        return 201, b'{"trial_id":"trial-1"}'

    class _ProbeResponse:
        status = 200

        def __init__(self) -> None:
            self.headers = {"Content-Length": "123"}

        def read(self, size: int = -1) -> bytes:
            assert size == 1
            return b"{"

        def __enter__(self) -> _ProbeResponse:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    calls: list[tuple[str, str, str | None, str | None]] = []

    def fake_urlopen(request: Any, timeout: float) -> _ProbeResponse:
        assert timeout == 15
        calls.append(
            (
                request.get_method(),
                request.full_url,
                request.get_header("Authorization"),
                request.get_header("Range"),
            ),
        )
        if request.get_method() == "HEAD":
            raise urllib.error.HTTPError(
                request.full_url,
                405,
                "Method Not Allowed",
                hdrs=None,
                fp=None,
            )
        assert request.get_method() == "GET"
        return _ProbeResponse()

    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_get", fake_get)
    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_post", fake_post)
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s13_smoke.urllib.request.urlopen",
        fake_urlopen,
    )

    result = SmokeStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert calls == [
        (
            "HEAD",
            "https://loom.test/api/v1/trials/trial-1/trajectory/download",
            "Bearer smoke-user-token",
            None,
        ),
        (
            "GET",
            "https://loom.test/api/v1/trials/trial-1/trajectory/download",
            "Bearer smoke-user-token",
            "bytes=0-0",
        ),
    ]
    assert step_dir.artifact_path("07-trajectory-head.txt").read_text() == (
        "status=200\ncontent-length=123\nmethod=GET\nbytes-read=1\n"
    )


def test_current_gb10_smoke_defaults_to_gb10_compatible_task_and_pool(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, scope="current-gb10", exclude_oldlab=True)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(15, "smoke")
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
                b'{"credential_type":"user_owned_api_token","scopes":["read:own","submit"]}',
            )
        if url.endswith("/api/v1/benchmarks"):
            return 200, b'{"items":[{"id":"loom-smoke"}]}'
        if url.endswith("/api/v1/tasks/loom-smoke/gb10-oracle-hello-world"):
            return (
                200,
                b'{"id":"loom-smoke/gb10-oracle-hello-world","benchmark_id":"loom-smoke"}',
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
            "task_id": "loom-smoke/gb10-oracle-hello-world",
            "config": {"agent_name": "oracle", "agent_model": None},
            "idempotency_key": "smoke-"
            + hashlib.sha256(
                f"{ctx.image_tag}|{ctx.resolved_sha}".encode(),
            ).hexdigest()[:16],
            "required_worker_pool": "gb10",
        }
    ]


def test_full_cluster_smoke_requires_explicit_audited_task_id(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, scope="full-cluster")
    monkeypatch.delenv("LOOM_SMOKE_TASK_ID", raising=False)

    with pytest.raises(
        RuntimeError,
        match=r"--smoke-task-id.*audited current profile",
    ):
        _smoke_task_id(ctx)


def test_full_cluster_smoke_submits_explicit_tb21_task_without_gb10_pool(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_id = "terminal-bench-2@tb2.1-r6/chess-best-move"
    ctx = make_ctx(tmp_path, scope="full-cluster", smoke_task_id=task_id)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(15, "smoke")
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
        if url.endswith(
            "/api/v1/tasks/terminal-bench-2%40tb2.1-r6/chess-best-move",
        ):
            return (
                200,
                b'{"id":"terminal-bench-2@tb2.1-r6/chess-best-move",'
                b'"benchmark_id":"terminal-bench-2@tb2.1-r6"}',
            )
        if url.endswith("/api/v1/trials/trial-1"):
            return 200, b'{"id":"trial-1","state":"succeeded","aggregate_reward":0}'
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
            "task_id": task_id,
            "config": {"agent_name": "oracle", "agent_model": None},
            "idempotency_key": "smoke-"
            + hashlib.sha256(
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
    step_dir = ev.step_dir(15, "smoke")

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


def test_admin_on_behalf_smoke_submits_batch_with_admin_source_ref(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admin_token = "admin-token-from-env"
    fingerprint = (
        f"sha256:{hashlib.sha256(admin_token.encode()).hexdigest()[:12]} len={len(admin_token)}"
    )
    ctx = make_ctx(
        tmp_path,
        expect_admin_token_fingerprint=fingerprint,
        scope="current-gb10",
        exclude_oldlab=True,
        smoke_submit_mode="admin-on-behalf",
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="11111111-1111-4111-8111-111111111111",
        smoke_admin_actor="qianyi",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(15, "smoke")
    captured_payloads: list[dict[str, Any]] = []
    captured_headers: list[dict[str, str]] = []

    monkeypatch.setenv("LOOM_CP_ADMIN_TOKEN", admin_token)
    monkeypatch.delenv("LOOM_SMOKE_SUBMIT_MODE", raising=False)
    monkeypatch.delenv("LOOM_SMOKE_ON_BEHALF_USERNAME", raising=False)
    monkeypatch.delenv("LOOM_SMOKE_ON_BEHALF_TEAM_ID", raising=False)
    monkeypatch.delenv("LOOM_SMOKE_ADMIN_ACTOR", raising=False)
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s13_smoke._ingress_base",
        lambda _ctx: "https://loom.test",
    )

    def fake_get(url: str, *, token: str | None = None) -> tuple[int, bytes]:
        assert token == admin_token
        if url.endswith("/api/v1/health"):
            return 200, b'{"status":"ok"}'
        if url.endswith("/api/v1/auth/whoami"):
            return (
                200,
                b'{"credential_type":"admin_bearer_token",'
                b'"principal_type":"admin","scopes":["admin:worker_pools"]}',
            )
        if url.endswith("/api/v1/benchmarks"):
            return 200, b'{"items":[{"id":"loom-smoke"}]}'
        if url.endswith("/api/v1/tasks/loom-smoke/gb10-oracle-hello-world"):
            return (
                200,
                b'{"id":"loom-smoke/gb10-oracle-hello-world","benchmark_id":"loom-smoke"}',
            )
        if url.startswith("https://loom.test/api/v1/batches?"):
            return 200, b'{"items":[]}'
        if url.endswith("/api/v1/batches/batch-1"):
            return (
                200,
                b'{"id":"batch-1","state":"finished","result_status":"succeeded",'
                b'"expected_trial_count":1,'
                b'"trial_summary":{"succeeded":1,"failed":0,"cancelled":0},'
                b'"submitted_by_user":{"username":"Devansh",'
                b'"team_id":"11111111-1111-4111-8111-111111111111"}}',
            )
        raise AssertionError(f"unexpected GET {url}")

    def fake_post(
        url: str,
        payload: dict[str, object],
        *,
        token: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, bytes]:
        assert url.endswith("/api/v1/admin/batches/on-behalf")
        assert token == admin_token
        captured_payloads.append(dict(payload))
        captured_headers.append(dict(headers or {}))
        return 201, b'{"batch_id":"batch-1","expected_trial_count":1}'

    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_get", fake_get)
    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_post", fake_post)

    result = SmokeStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert result.artifacts == {"batch_id": "batch-1"}
    assert captured_headers == [{"X-Loom-Admin-Actor": "qianyi"}]
    assert captured_payloads == [
        {
            "name": "rollout-smoke-"
            + hashlib.sha256(
                f"{ctx.image_tag}|{ctx.resolved_sha}".encode(),
            ).hexdigest()[:16],
            "represented_username": "devansh",
            "team_id": "11111111-1111-4111-8111-111111111111",
            "task_filter": {"task_ids": ["loom-smoke/gb10-oracle-hello-world"]},
            "trial_config": {"agent_name": "oracle", "agent_model": None},
            "n_per_task": 1,
            "required_worker_pools": ["gb10"],
        }
    ]
    assert "admin-token-from-env" not in step_dir.stdout_path().read_text()
    assert (
        "admin-token-from-env"
        not in step_dir.artifact_path(
            "05-submit.json",
        ).read_text()
    )


def test_admin_on_behalf_smoke_accepts_candidate_bound_canary_overrides(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(
        tmp_path,
        scope="current-gb10",
        smoke_submit_mode="admin-on-behalf",
        smoke_task_id="skilllearnbench/fix-security-bug/fix-security-bug-1",
        smoke_required_worker_pool="gb10",
        smoke_agent="oracle",
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="11111111-1111-4111-8111-111111111111",
        smoke_admin_actor="qianyi",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(14, "release-gate")
    captured: list[dict[str, object]] = []
    monkeypatch.setenv("LOOM_CP_ADMIN_TOKEN", "admin-token")
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s13_smoke._ingress_base",
        lambda _ctx: "https://loom.test",
    )

    def fake_get(url: str, *, token: str | None = None) -> tuple[int, bytes]:
        assert token == "admin-token"
        if url.endswith("/api/v1/health"):
            return 200, b'{"status":"ok"}'
        if url.endswith("/api/v1/auth/whoami"):
            return 200, b'{"credential_type":"admin_bearer_token"}'
        if url.endswith("/api/v1/benchmarks"):
            return 200, b'{"items":[{"id":"skilllearnbench"}]}'
        if url.endswith("/api/v1/tasks/skilllearnbench/fix-security-bug/fix-security-bug-1"):
            return 200, b'{"id":"skilllearnbench/fix-security-bug/fix-security-bug-1"}'
        if url.startswith("https://loom.test/api/v1/batches?"):
            return 200, b'{"items":[]}'
        if url.endswith("/api/v1/batches/batch-current"):
            return (
                200,
                b'{"state":"finished","result_status":"succeeded",'
                b'"expected_trial_count":1,"trial_summary":{"succeeded":1},'
                b'"submitted_by_user":{"username":"devansh",'
                b'"team_id":"11111111-1111-4111-8111-111111111111"}}',
            )
        raise AssertionError(f"unexpected GET {url}")

    def fake_post(
        _url: str,
        payload: dict[str, object],
        *,
        token: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, bytes]:
        assert token == "admin-token"
        assert headers == {"X-Loom-Admin-Actor": "qianyi"}
        captured.append(dict(payload))
        return 201, b'{"batch_id":"batch-current"}'

    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_get", fake_get)
    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_post", fake_post)

    result = run_admin_on_behalf_smoke(
        ctx,
        step_dir,
        batch_name="rollout-hf-boundary-staging-abc123-registration",
        n_per_task=1,
        artifact_prefix="hf-canary-",
        terminal_timeout_sec=60.0,
    )

    assert result.exit_code == 0
    assert result.artifacts == {"batch_id": "batch-current"}
    assert captured[0]["name"] == "rollout-hf-boundary-staging-abc123-registration"
    assert captured[0]["task_filter"] == {
        "task_ids": ["skilllearnbench/fix-security-bug/fix-security-bug-1"]
    }
    assert captured[0]["required_worker_pools"] == ["gb10"]
    assert step_dir.artifact_path("hf-canary-05-submit.json").is_file()


def test_admin_on_behalf_config_resolves_team_id_source(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(
        tmp_path,
        smoke_submit_mode="admin-on-behalf",
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="env:LOOM_SMOKE_ON_BEHALF_TEAM_ID",
        smoke_admin_actor="qianyi",
    )
    monkeypatch.setenv(
        "LOOM_SMOKE_ON_BEHALF_TEAM_ID",
        "11111111-1111-4111-8111-111111111111",
    )

    config, error = _admin_on_behalf_config(ctx)

    assert error is None
    assert config is not None
    assert config.team_id == "11111111-1111-4111-8111-111111111111"


def test_admin_on_behalf_smoke_fails_fast_on_fanout_submit_failure(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admin_token = "admin-token-from-env"
    ctx = make_ctx(tmp_path, scope="current-gb10", exclude_oldlab=True)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(15, "smoke")

    monkeypatch.setenv("LOOM_CP_ADMIN_TOKEN", admin_token)
    monkeypatch.setenv("LOOM_SMOKE_SUBMIT_MODE", "admin-on-behalf")
    monkeypatch.setenv("LOOM_SMOKE_ON_BEHALF_USERNAME", "devansh")
    monkeypatch.setenv(
        "LOOM_SMOKE_ON_BEHALF_TEAM_ID",
        "11111111-1111-4111-8111-111111111111",
    )
    monkeypatch.setenv("LOOM_SMOKE_ADMIN_ACTOR", "qianyi")
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s13_smoke._ingress_base",
        lambda _ctx: "https://loom.test",
    )

    def fake_get(url: str, *, token: str | None = None) -> tuple[int, bytes]:
        assert token == admin_token
        if url.endswith("/api/v1/health"):
            return 200, b'{"status":"ok"}'
        if url.endswith("/api/v1/auth/whoami"):
            return (
                200,
                b'{"credential_type":"admin_bearer_token",'
                b'"principal_type":"admin","scopes":["admin:worker_pools"]}',
            )
        if url.endswith("/api/v1/benchmarks"):
            return 200, b'{"items":[{"id":"loom-smoke"}]}'
        if url.endswith("/api/v1/tasks/loom-smoke/gb10-oracle-hello-world"):
            return (
                200,
                b'{"id":"loom-smoke/gb10-oracle-hello-world","benchmark_id":"loom-smoke"}',
            )
        if url.startswith("https://loom.test/api/v1/batches?"):
            return 200, b'{"items":[]}'
        if url.endswith("/api/v1/batches/batch-1"):
            return (
                200,
                b'{"id":"batch-1","state":"running",'
                b'"result_status":"partial_failed",'
                b'"failure_reason":"fanout_submit_failed",'
                b'"failure_message":"required_worker_pool gb10 is incompatible",'
                b'"fanout_errors":[{"reason":"required_worker_pool_incompatible",'
                b'"required_worker_pool":"gb10",'
                b'"detail":"Authorization: Bearer loom_admin_fake_secret",'
                b'"pool_cpu_arches":["arm64"],'
                b'"task_cpu_arches":{"x86_64":["loom-smoke/gb10-oracle-hello-world"]}}]}',
            )
        raise AssertionError(f"unexpected GET {url}")

    def fake_post(
        url: str,
        payload: dict[str, object],
        *,
        token: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, bytes]:
        assert url.endswith("/api/v1/admin/batches/on-behalf")
        assert token == admin_token
        return 201, b'{"batch_id":"batch-1","expected_trial_count":1}'

    times = iter([100.0, 101.0, 500.0])
    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke.time.time", lambda: next(times))
    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke.time.sleep", lambda _seconds: None)
    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_get", fake_get)
    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_post", fake_post)

    result = SmokeStep().run(ctx, step_dir)

    assert result.exit_code == 1
    assert "fanout_submit_failed" in str(result.error)
    assert "required_worker_pool_incompatible" in str(result.error)
    assert "gb10" in str(result.error)
    assert "admin-token-from-env" not in str(result.error)
    assert "loom_admin_fake_secret" not in str(result.error)
    assert "Bearer [REDACTED:bearer]" in str(result.error)


def test_admin_on_behalf_smoke_reuses_existing_deterministic_batch(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admin_token = "admin-token-from-env"
    ctx = make_ctx(tmp_path, scope="current-gb10", exclude_oldlab=True)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(15, "smoke")
    batch_name = (
        "rollout-smoke-"
        + hashlib.sha256(
            f"{ctx.image_tag}|{ctx.resolved_sha}".encode(),
        ).hexdigest()[:16]
    )

    monkeypatch.setenv("LOOM_CP_ADMIN_TOKEN", admin_token)
    monkeypatch.setenv("LOOM_SMOKE_SUBMIT_MODE", "admin-on-behalf")
    monkeypatch.setenv("LOOM_SMOKE_ON_BEHALF_USERNAME", "devansh")
    monkeypatch.setenv(
        "LOOM_SMOKE_ON_BEHALF_TEAM_ID",
        "11111111-1111-4111-8111-111111111111",
    )
    monkeypatch.setenv("LOOM_SMOKE_ADMIN_ACTOR", "qianyi")
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s13_smoke._ingress_base",
        lambda _ctx: "https://loom.test",
    )

    def fake_get(url: str, *, token: str | None = None) -> tuple[int, bytes]:
        assert token == admin_token
        if url.endswith("/api/v1/health"):
            return 200, b'{"status":"ok"}'
        if url.endswith("/api/v1/auth/whoami"):
            return (
                200,
                b'{"credential_type":"admin_bearer_token",'
                b'"principal_type":"admin","scopes":["admin:worker_pools"]}',
            )
        if url.endswith("/api/v1/benchmarks"):
            return 200, b'{"items":[{"id":"loom-smoke"}]}'
        if url.endswith("/api/v1/tasks/loom-smoke/gb10-oracle-hello-world"):
            return (
                200,
                b'{"id":"loom-smoke/gb10-oracle-hello-world","benchmark_id":"loom-smoke"}',
            )
        if url.startswith("https://loom.test/api/v1/batches?"):
            return (
                200,
                json.dumps(
                    {
                        "items": [
                            {
                                "id": "batch-existing",
                                "name": batch_name,
                                "team_id": ("11111111-1111-4111-8111-111111111111"),
                                "submitted_by_user": {
                                    "username": "Devansh",
                                    "team_id": ("11111111-1111-4111-8111-111111111111"),
                                },
                                "task_filter": {
                                    "task_ids": ["loom-smoke/gb10-oracle-hello-world"],
                                },
                            }
                        ]
                    },
                ).encode(),
            )
        if url.endswith("/api/v1/batches/batch-existing"):
            return (
                200,
                b'{"id":"batch-existing","state":"finished",'
                b'"result_status":"succeeded","expected_trial_count":1,'
                b'"trial_summary":{"succeeded":1},'
                b'"submitted_by_user":{"username":"Devansh",'
                b'"team_id":"11111111-1111-4111-8111-111111111111"}}',
            )
        raise AssertionError(f"unexpected GET {url}")

    def fake_post(
        url: str,
        payload: dict[str, object],
        *,
        token: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, bytes]:
        raise AssertionError("rerun submitted a duplicate admin smoke batch")

    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_get", fake_get)
    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_post", fake_post)

    result = SmokeStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert result.artifacts == {"batch_id": "batch-existing"}
    assert json.loads(step_dir.artifact_path("05-submit.json").read_text()) == {
        "batch_id": "batch-existing",
        "recovered": True,
    }


def test_admin_on_behalf_smoke_rejects_non_admin_identity_before_submit(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(15, "smoke")

    monkeypatch.setenv("LOOM_CP_ADMIN_TOKEN", "team-token")
    monkeypatch.setenv("LOOM_SMOKE_SUBMIT_MODE", "admin-on-behalf")
    monkeypatch.setenv("LOOM_SMOKE_ON_BEHALF_USERNAME", "devansh")
    monkeypatch.setenv(
        "LOOM_SMOKE_ON_BEHALF_TEAM_ID",
        "11111111-1111-4111-8111-111111111111",
    )
    monkeypatch.setenv("LOOM_SMOKE_ADMIN_ACTOR", "qianyi")
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s13_smoke._ingress_base",
        lambda _ctx: "https://loom.test",
    )

    def fake_get(url: str, *, token: str | None = None) -> tuple[int, bytes]:
        assert token == "team-token"
        if url.endswith("/api/v1/health"):
            return 200, b'{"status":"ok"}'
        if url.endswith("/api/v1/auth/whoami"):
            return (
                200,
                b'{"credential_type":"legacy_team_token",'
                b'"principal_type":"team","scopes":["read:own","submit"]}',
            )
        if url.endswith("/api/v1/benchmarks"):
            raise AssertionError("benchmarks should not run for non-admin identity")
        raise AssertionError(f"unexpected GET {url}")

    def fake_post(
        url: str,
        payload: dict[str, object],
        *,
        token: str | None = None,
    ) -> tuple[int, bytes]:
        raise AssertionError("admin on-behalf submitted with non-admin identity")

    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_get", fake_get)
    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_post", fake_post)

    result = SmokeStep().run(ctx, step_dir)

    assert result.exit_code == 1
    assert "admin-on-behalf smoke requires an admin-capable token" in str(
        result.error,
    )


def test_admin_on_behalf_smoke_requires_represented_identity_before_http(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(15, "smoke")

    monkeypatch.setenv("LOOM_CP_ADMIN_TOKEN", "admin-token")
    monkeypatch.setenv("LOOM_SMOKE_SUBMIT_MODE", "admin-on-behalf")
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s13_smoke._ingress_base",
        lambda _ctx: "https://loom.test",
    )

    def fake_get(url: str, *, token: str | None = None) -> tuple[int, bytes]:
        raise AssertionError(f"HTTP should not be called before validation: {url}")

    def fake_post(
        url: str,
        payload: dict[str, object],
        *,
        token: str | None = None,
    ) -> tuple[int, bytes]:
        raise AssertionError(f"HTTP should not be called before validation: {url}")

    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_get", fake_get)
    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_post", fake_post)

    result = SmokeStep().run(ctx, step_dir)

    assert result.exit_code == 2
    assert "LOOM_SMOKE_ON_BEHALF_USERNAME" in str(result.error)
    assert "LOOM_SMOKE_ON_BEHALF_TEAM_ID" in str(result.error)
    assert "LOOM_SMOKE_ADMIN_ACTOR" in str(result.error)


def test_admin_on_behalf_smoke_fails_before_http_when_admin_fingerprint_drifts(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(
        tmp_path,
        expect_admin_token_fingerprint="sha256:stale0000000 len=18",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(15, "smoke")

    monkeypatch.setenv("LOOM_CP_ADMIN_TOKEN", "admin-token-from-env")
    monkeypatch.setenv("LOOM_SMOKE_SUBMIT_MODE", "admin-on-behalf")
    monkeypatch.setenv("LOOM_SMOKE_ON_BEHALF_USERNAME", "devansh")
    monkeypatch.setenv(
        "LOOM_SMOKE_ON_BEHALF_TEAM_ID",
        "11111111-1111-4111-8111-111111111111",
    )
    monkeypatch.setenv("LOOM_SMOKE_ADMIN_ACTOR", "qianyi")
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s13_smoke._ingress_base",
        lambda _ctx: "https://loom.test",
    )

    def fake_get(url: str, *, token: str | None = None) -> tuple[int, bytes]:
        raise AssertionError(f"HTTP should not be called before fingerprint: {url}")

    def fake_post(
        url: str,
        payload: dict[str, object],
        *,
        token: str | None = None,
    ) -> tuple[int, bytes]:
        raise AssertionError(f"HTTP should not be called before fingerprint: {url}")

    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_get", fake_get)
    monkeypatch.setattr("loom_cli.rollout.steps.s13_smoke._http_post", fake_post)

    result = SmokeStep().run(ctx, step_dir)

    assert result.exit_code == 1
    assert "admin_token_fingerprint drift" in str(result.error)
    assert "sha256:stale0000000 len=18" in str(result.error)
    assert "admin-token-from-env" not in str(result.error)


def test_user_token_inputs_hash_ignores_admin_only_agent_env(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    monkeypatch.setenv("LOOM_SMOKE_API_TOKEN", "smoke-user-token")
    before = SmokeStep().inputs_hash(ctx)

    monkeypatch.setenv("LOOM_SMOKE_AGENT", "codex")

    assert SmokeStep().inputs_hash(ctx) == before


def test_smoke_rejects_missing_smoke_task_before_submit(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(15, "smoke")

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
                b'{"credential_type":"user_owned_api_token","scopes":["read:own","submit"]}',
            )
        if url.endswith("/api/v1/benchmarks"):
            return 200, b'{"items":[{"id":"terminal-bench-2"}]}'
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
