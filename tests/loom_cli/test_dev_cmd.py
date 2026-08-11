from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import httpx
import pytest

from loom_cli.__main__ import main


def _instance(name: str = "alice", status: str = "ready") -> dict[str, Any]:
    return {
        "name": name,
        "owner_user_id": "00000000-0000-0000-0000-000000000001",
        "owner_team_id": "00000000-0000-0000-0000-000000000002",
        "status": status,
        "min_slots": 0,
        "max_slots": 2,
        "deployment_generation": 1,
        "candidate_sha": "a" * 40,
        "operation_epoch": 1,
        "keep_data": False,
        "failure_reason": None,
        "created_at": "2026-08-06T12:00:00Z",
        "updated_at": "2026-08-06T12:00:00Z",
        "ready_at": "2026-08-06T12:00:00Z" if status == "ready" else None,
        "deleted_at": "2026-08-06T12:00:00Z" if status == "deleted" else None,
        "identity": {
            "environment": f"dev-{name}",
            "namespace": f"loom-dev-{name}",
            "database": f"loom_dev_{name}",
            "task_bucket": f"loom-dev-{name}-tasks",
            "trajectories_bucket": f"loom-dev-{name}-trajectories",
            "artifacts_bucket": f"loom-dev-{name}-artifacts",
            "route_host": f"{name}.dev.yylx.world",
            "worker_control_plane_host": f"cp-{name}.dev.yylx.world",
            "worker_gateway_host": f"gw-{name}.dev.yylx.world",
            "route_path": f"/dev-{name}",
            "worker_pool": f"dev-{name}",
        },
    }


@pytest.fixture(autouse=True)
def _logged_in(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    monkeypatch.setenv("LOOM_DEV_TEST_TOKEN", "loom_test_token")
    assert (
        main(
            [
                "auth",
                "login",
                "--server",
                "https://loom.test",
                "--token",
                "env:LOOM_DEV_TEST_TOKEN",
            ]
        )
        == 0
    )


class _Server:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.replies: list[httpx.Response] = []


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch) -> _Server:
    server = _Server()

    def handler(request: httpx.Request) -> httpx.Response:
        server.requests.append(request)
        if server.replies:
            return server.replies.pop(0)
        return httpx.Response(500, json={"detail": "missing canned response"})

    transport = httpx.MockTransport(handler)

    def client(cfg: Any, *, timeout: float = 30.0) -> httpx.Client:
        return httpx.Client(
            base_url=cfg.server_url,
            headers={"Authorization": f"Bearer {cfg.auth_token}"},
            transport=transport,
            timeout=timeout,
        )

    monkeypatch.setattr("loom_cli.dev_cmd.authed_client", client)
    return server


def test_create_posts_guarded_shape_and_waits(
    server: _Server,
    capsys: pytest.CaptureFixture[str],
) -> None:
    server.replies.extend(
        [
            httpx.Response(202, json=_instance(status="provisioning")),
            httpx.Response(200, json=_instance(status="ready")),
        ]
    )
    assert (
        main(
            [
                "dev",
                "create",
                "alice",
                "--max-slots",
                "2",
                "--timeout",
                "1",
                "--poll-interval",
                "0.001",
                "--format",
                "json",
            ]
        )
        == 0
    )
    assert [request.method for request in server.requests] == ["POST", "GET"]
    assert server.requests[0].url.path == "/api/v1/dev-instances"
    assert json.loads(server.requests[0].content) == {
        "name": "alice",
        "min_slots": 0,
        "max_slots": 2,
    }
    assert json.loads(capsys.readouterr().out)["status"] == "ready"


@pytest.mark.parametrize("name", ["Alice!", "staging"])
def test_create_rejects_invalid_name_before_http(
    name: str,
    server: _Server,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as error:
        main(["dev", "create", name])

    assert error.value.code == 2
    assert server.requests == []
    assert "dev-instance name" in capsys.readouterr().err


def test_list_sends_filters_and_prints_rows(
    server: _Server,
    capsys: pytest.CaptureFixture[str],
) -> None:
    server.replies.append(httpx.Response(200, json={"items": [_instance()]}))
    assert main(["dev", "list", "--mine", "--include-deleted"]) == 0
    assert server.requests[0].url.params == httpx.QueryParams(
        {
            "mine": "true",
            "include_deleted": "true",
        }
    )
    output = capsys.readouterr().out
    assert "alice" in output
    assert "dev-alice" in output


def test_destroy_preserves_data_only_when_explicit(
    server: _Server,
) -> None:
    key = "00000000-0000-0000-0000-000000000099"
    server.replies.extend(
        (
            httpx.Response(200, json=_instance()),
            httpx.Response(202, json=_instance(status="deleted")),
        )
    )
    assert (
        main(
            [
                "dev",
                "destroy",
                "alice",
                "--keep-data",
                "--idempotency-key",
                key,
                "--no-wait",
            ]
        )
        == 0
    )
    assert [request.method for request in server.requests] == ["GET", "DELETE"]
    params = server.requests[1].url.params
    assert params["keep_data"] == "true"
    assert params["expected_operation_epoch"] == "1"
    assert params["idempotency_key"] == key


def test_destroy_retry_reuses_the_original_compare_and_set_epoch(
    server: _Server,
) -> None:
    key = "00000000-0000-0000-0000-000000000099"
    current = _instance(status="deleting")
    current["operation_epoch"] = 2
    server.replies.extend(
        (
            httpx.Response(200, json=current),
            httpx.Response(202, json=current),
        )
    )

    assert (
        main(
            [
                "dev",
                "destroy",
                "alice",
                "--idempotency-key",
                key,
                "--no-wait",
            ]
        )
        == 0
    )

    params = server.requests[1].url.params
    assert params["expected_operation_epoch"] == "1"
    assert params["idempotency_key"] == key


def test_destroy_uses_a_reproducible_default_idempotency_key(server: _Server) -> None:
    server.replies.extend(
        (
            httpx.Response(200, json=_instance()),
            httpx.Response(202, json=_instance(status="deleting")),
        )
    )

    assert main(["dev", "destroy", "alice", "--no-wait"]) == 0

    assert server.requests[1].url.params["idempotency_key"] == str(
        uuid5(
            NAMESPACE_URL,
            "loom-personal-dev-destroy-v1\0alice\0" "1\0false",
        )
    )


def test_server_failure_is_redacted_and_nonzero(
    server: _Server,
    capsys: pytest.CaptureFixture[str],
) -> None:
    server.replies.append(
        httpx.Response(
            503,
            json={"detail": "postgresql://secret@fixture provision failed"},
        )
    )
    assert main(["dev", "create", "alice", "--no-wait"]) == 1
    error = capsys.readouterr().err
    assert "postgresql://" not in error
    assert "secret" not in error
