"""Owner provisioning CLI sends explicit remote requests and never falls back."""

from __future__ import annotations

import json
from uuid import UUID

import httpx
import pytest

from loom_cli.__main__ import main
from loom_cli.config import LoomConfig, save_config

CANDIDATE = "10000000-0000-4000-8000-000000000001"
ENVIRONMENT = "20000000-0000-4000-8000-000000000001"
OPERATION = "30000000-0000-4000-8000-000000000001"


@pytest.fixture
def management_http(monkeypatch, tmp_path):
    from loom_cli import environment_client
    from loom_cli.server_client import authed_client

    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    save_config(LoomConfig(server_url="https://manage.example.com", auth_token="test-management-token"))
    responses = {}
    requests = []

    def handle(request):
        requests.append(request)
        value = responses.get((request.method, request.url.path))
        if value is None:
            return httpx.Response(404, json={"detail": "not found"})
        return value

    monkeypatch.setattr(environment_client, "authed_client", lambda cfg, **kwargs: authed_client(
        cfg, transport=httpx.MockTransport(handle), **kwargs,
    ))
    return requests, responses


def operation(phase="pending"):
    return {"operation_id": OPERATION, "environment_id": ENVIRONMENT,
            "deployment_generation": 1, "action": "create", "phase": phase,
            "error_code": None, "execution_enabled": False}


def test_create_uses_selected_candidate_and_replay_key(management_http, capsys):
    requests, responses = management_http
    responses["POST", "/api/v1/environments"] = httpx.Response(202, json=operation())
    assert main(["dev", "create", "alice", "--candidate", CANDIDATE, "--idempotency-key", "same-request"]) == 0
    assert len(requests) == 1
    assert json.loads(requests[0].content) == {"slug": "alice", "candidate_id": CANDIDATE}
    assert requests[0].headers["Idempotency-Key"] == "same-request"
    assert json.loads(capsys.readouterr().out)["operation_id"] == OPERATION


def test_create_prints_replay_key_before_failed_network_response(management_http, capsys):
    _, responses = management_http
    responses["POST", "/api/v1/environments"] = httpx.Response(503, json={"detail": {"code": "unavailable"}})
    assert main(["dev", "create", "alice", "--candidate", CANDIDATE]) == 1
    output = capsys.readouterr()
    key = output.err.split("Idempotency-Key: ", 1)[1].splitlines()[0]
    assert UUID(key)
    assert "not logged" not in output.err


def test_wait_timeout_does_not_cancel_operation(management_http, capsys):
    requests, responses = management_http
    responses["GET", f"/api/v1/environment-operations/{OPERATION}"] = httpx.Response(200, json=operation())
    assert main(["dev", "wait", OPERATION, "--timeout", "0"]) == 2
    assert all(request.method == "GET" for request in requests)
    assert OPERATION in capsys.readouterr().err


def test_wait_completed_operation_reports_execution_still_disabled(management_http, capsys):
    _, responses = management_http
    responses["GET", f"/api/v1/environment-operations/{OPERATION}"] = httpx.Response(200, json=operation("completed"))
    assert main(["dev", "wait", OPERATION]) == 0
    assert json.loads(capsys.readouterr().out)["execution_enabled"] is False


def test_service_up_explicit_personal_environment_dispatches_remote(management_http):
    requests, responses = management_http
    responses["POST", "/api/v1/environments"] = httpx.Response(503, json={"detail": "not configured"})
    assert main(["service", "up", "--environment", "dev-alice", "--candidate", CANDIDATE]) == 1
    assert len(requests) == 1
    assert json.loads(requests[0].content)["slug"] == "alice"


@pytest.mark.parametrize("flags", [
    ["--environment", "dev-alice"], ["--candidate", CANDIDATE],
    ["--environment", "prod", "--candidate", CANDIDATE],
    ["--environment", "dev-alice", "--candidate", CANDIDATE, "--compose-file", "local.yml"],
    ["--environment", "dev-alice", "--candidate", CANDIDATE, "--db-url", "postgresql://local/db"],
])
def test_service_up_rejects_target_source_conflicts_without_requests(management_http, flags):
    requests, _ = management_http
    assert main(["service", "up", *flags]) == 1
    assert requests == []
