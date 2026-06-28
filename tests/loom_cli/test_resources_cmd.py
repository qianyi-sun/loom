"""`loom resources status` exposes the Monitor slot summary for scripts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from loom_cli.__main__ import main

_SUMMARY = {
    "resources": {
        "aggregate": {
            "desired_slots": 18,
            "pending_slots": 6,
            "current_active_slots": 12,
            "max_slots": 52,
            "ceiling_slots": 52,
            "active_workers": 2,
            "draining_workers": 1,
            "total_slots": 12,
            "draining_slots": 2,
            "occupied_slots": 2,
            "free_slots": 10,
            "running_tasks": 1,
            "starting_tasks": 1,
            "queued_tasks": 1,
        },
        "pools": [
            {
                "pool_name": "gb10-arm64",
                "backend": "docker",
                "cpu_arch": "arm64",
                "autoscaler_environment": "production",
                "autoscaler_actuator": "slurm",
                "autoscaler_enabled": True,
                "autoscaler_idle_since_at": "2026-06-27T12:00:00+00:00",
                "autoscaler_idle_seconds": 601,
                "desired_slots": 12,
                "pending_slots": 0,
                "current_active_slots": 10,
                "max_slots": 40,
                "ceiling_slots": 40,
                "active_workers": 1,
                "draining_workers": 1,
                "total_slots": 10,
                "draining_slots": 2,
                "occupied_slots": 0,
                "free_slots": 10,
                "running_tasks": 0,
                "starting_tasks": 0,
                "queued_tasks": 1,
                "last_autoscaler_decision": "request_drain",
                "last_autoscaler_reason": "idle_excess_capacity",
                "decision_reason": "idle_excess_capacity",
                "last_autoscaler_blocked_reason": None,
                "blocked_reason": None,
                "last_autoscaler_error": None,
            },
            {
                "pool_name": "public-beta-x86",
                "backend": "docker",
                "cpu_arch": "x86_64",
                "autoscaler_environment": "production",
                "autoscaler_actuator": "slurm",
                "autoscaler_enabled": True,
                "autoscaler_idle_since_at": None,
                "autoscaler_idle_seconds": None,
                "desired_slots": 6,
                "pending_slots": 6,
                "current_active_slots": 2,
                "max_slots": 12,
                "ceiling_slots": 12,
                "active_workers": 1,
                "draining_workers": 0,
                "total_slots": 2,
                "draining_slots": 0,
                "occupied_slots": 2,
                "free_slots": 0,
                "running_tasks": 1,
                "starting_tasks": 1,
                "queued_tasks": 1,
                "last_autoscaler_decision": "scale_up",
                "last_autoscaler_reason": "queued_deficit",
                "decision_reason": "queued_deficit",
                "last_autoscaler_blocked_reason": "pending_cap",
                "blocked_reason": "pending_cap",
                "last_autoscaler_error": None,
            },
        ],
    },
    "queue": {
        "queued": 1,
        "claimed": 1,
        "running": 1,
        "waiting": 2,
        "active_workers": 2,
        "available_backends": ["docker"],
        "has_default_backend": True,
        "status": "waiting",
    },
}


class _MockServer:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []


@pytest.fixture
def _logged_in(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    monkeypatch.setenv("LOOM_TEST_TOKEN", "loom_api_resources")
    main([
        "auth",
        "login",
        "--server",
        "https://loom.test",
        "--token",
        "env:LOOM_TEST_TOKEN",
    ])


@pytest.fixture
def mock_resources_server(
    monkeypatch: pytest.MonkeyPatch,
    _logged_in: None,
) -> _MockServer:
    import loom_cli.resources_cmd as resources_cmd

    server = _MockServer()

    def _handler(request: httpx.Request) -> httpx.Response:
        server.requests.append(request)
        if request.method == "GET" and request.url.path == "/api/v1/monitor/summary":
            return httpx.Response(200, json=_SUMMARY)
        return httpx.Response(404, json={"detail": "not found"})

    transport = httpx.MockTransport(_handler)

    def _patched(cfg: Any, *, timeout: float = 30.0) -> httpx.Client:
        return httpx.Client(
            base_url=cfg.server_url,
            headers={"Authorization": f"Bearer {cfg.auth_token}"},
            transport=transport,
            timeout=timeout,
        )

    monkeypatch.setattr(resources_cmd, "authed_client", _patched)
    return server


def test_resources_status_json_matches_monitor_resource_summary(
    mock_resources_server: _MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["resources", "status", "--json"])

    assert rc == 0
    assert json.loads(capsys.readouterr().out) == _SUMMARY["resources"]
    req = mock_resources_server.requests[0]
    assert req.url.path == "/api/v1/monitor/summary"
    assert req.url.params["view"] == "trials"


def test_resources_status_text_shows_slots_and_pool_breakdown(
    mock_resources_server: _MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["resources", "status"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "Concurrent tasks: 2 / 12" in out
    assert "Running: 1" in out
    assert "Starting: 1" in out
    assert "Queued: 1" in out
    assert "active 12" in out
    assert "pending 6" in out
    assert "desired 18" in out
    assert "max 52" in out
    assert "draining 2 slots / 1 workers" in out
    assert "idle=601s" in out
    assert "gb10-arm64" in out
    assert "slurm" in out
    assert "0/10" in out
    assert "40" in out
    assert "request_drain" in out
    assert "idle_excess_capacity" in out
    assert "public-beta-x86" in out
    assert "2/2" in out
    assert "scale_up" in out
    assert "pending_cap" in out
