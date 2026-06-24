from __future__ import annotations

import importlib.util
import io
import sys
from pathlib import Path
from urllib.error import HTTPError

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "benchmark_reward_gate.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("benchmark_reward_gate", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_readiness_gate_fails_on_blocked_displayed_benchmark() -> None:
    gate = _load_module()

    results = gate.check_benchmark_readiness(
        [
            {
                "id": "gaia",
                "readiness_state": "blocked",
                "readiness_label": "Needs publish",
                "readiness_message": "Publish/register tasks before selecting this benchmark.",
                "task_count": 0,
            },
            {
                "id": "mbpp",
                "readiness_state": "runnable",
                "readiness_label": "Ready",
                "task_count": 257,
            },
        ],
    )

    assert [r.status for r in results] == ["fail"]
    assert results[0].check_id == "benchmarks.all_displayed_runnable"
    assert "gaia" in results[0].detail
    assert "Needs publish" in results[0].detail


def test_readiness_gate_passes_when_all_displayed_benchmarks_are_runnable() -> None:
    gate = _load_module()

    results = gate.check_benchmark_readiness(
        [
            {
                "id": "mbpp",
                "readiness_state": "runnable",
                "readiness_label": "Ready",
                "task_count": 257,
            },
            {
                "id": "gpqa",
                "readiness_state": "runnable",
                "readiness_label": "Ready",
                "task_count": 546,
            },
        ],
    )

    assert [r.status for r in results] == ["pass"]
    assert "2 runnable benchmarks" in results[0].detail


def test_batch_reward_gate_requires_numeric_reward_for_every_trial() -> None:
    gate = _load_module()

    results = gate.check_batch_rewards(
        batch={
            "id": "batch-1",
            "state": "succeeded",
            "expected_trial_count": 2,
        },
        trials=[
            {
                "id": "trial-1",
                "task_id": "mbpp/1",
                "state": "succeeded",
                "failure_reason": None,
                "aggregate_reward": 1.0,
            },
            {
                "id": "trial-2",
                "task_id": "mbpp/2",
                "state": "succeeded",
                "failure_reason": None,
                "aggregate_reward": None,
            },
        ],
    )

    assert [r.status for r in results] == ["fail"]
    assert "trial-2" in results[0].detail
    assert "missing numeric reward" in results[0].detail


def test_batch_reward_gate_accepts_finished_service_batches() -> None:
    gate = _load_module()

    results = gate.check_batch_rewards(
        batch={
            "id": "batch-1",
            "state": "finished",
            "result_status": "succeeded",
            "expected_trial_count": 1,
        },
        trials=[
            {
                "id": "trial-1",
                "task_id": "swe-bench-multimodal/1",
                "state": "succeeded",
                "failure_reason": None,
                "aggregate_reward": 0.0,
            },
        ],
    )

    assert [r.status for r in results] == ["pass"]
    assert "1 trials have numeric rewards" in results[0].detail


def test_batch_reward_gate_flags_benchmark_side_failure_reasons() -> None:
    gate = _load_module()

    results = gate.check_batch_rewards(
        batch={
            "id": "batch-1",
            "state": "failed",
            "expected_trial_count": 1,
        },
        trials=[
            {
                "id": "trial-1",
                "task_id": "hendrycks-math/1",
                "state": "failed",
                "failure_reason": "verifier_error",
                "aggregate_reward": None,
            },
        ],
    )

    assert [r.status for r in results] == ["fail"]
    assert "verifier_error" in results[0].detail
    assert "benchmark-side" in results[0].detail


def test_collect_paginated_trials_follows_next_cursor() -> None:
    gate = _load_module()

    class FakeClient:
        def __init__(self) -> None:
            self.paths: list[str] = []

        def get_json(self, path: str):
            self.paths.append(path)
            if "cursor=" in path:
                return {"items": [{"id": "trial-2"}], "next_cursor": None}
            return {"items": [{"id": "trial-1"}], "next_cursor": "cursor-1"}

    client = FakeClient()

    trials = gate.collect_batch_trials(client, "batch-1", page_limit=1)

    assert trials == [{"id": "trial-1"}, {"id": "trial-2"}]
    assert client.paths == [
        "/api/v1/trials?batch_id=batch-1&limit=1",
        "/api/v1/trials?batch_id=batch-1&limit=1&cursor=cursor-1",
    ]


def test_cli_reports_api_errors_without_traceback(monkeypatch, capsys) -> None:
    gate = _load_module()

    def raise_unauthorized(_req, *, timeout):
        assert timeout == 30
        raise HTTPError(
            url="https://loom.example/api/v1/benchmarks",
            code=401,
            msg="Unauthorized",
            hdrs=None,
            fp=io.BytesIO(b'{"detail":"invalid token"}'),
        )

    monkeypatch.setattr(gate, "urlopen", raise_unauthorized)

    exit_code = gate.main(
        [
            "readiness",
            "--server-url",
            "https://loom.example",
            "--token",
            "loom_api_test",
        ],
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "api request failed" in captured.err
    assert "HTTP 401" in captured.err
    assert "invalid token" in captured.err
    assert "Traceback" not in captured.err


def test_readiness_cli_checks_full_user_visible_catalog(monkeypatch) -> None:
    gate = _load_module()
    paths: list[str] = []

    class FakeApiClient:
        def __init__(self, server_url: str, token: str) -> None:
            assert server_url == "https://loom.example"
            assert token == "loom_api_test"

        def get_json(self, path: str):
            paths.append(path)
            return {
                "items": [
                    {
                        "id": "mbpp",
                        "readiness_state": "runnable",
                        "task_count": 257,
                    },
                ],
            }

    monkeypatch.setattr(gate, "ApiClient", FakeApiClient)

    exit_code = gate.main(
        [
            "readiness",
            "--server-url",
            "https://loom.example",
            "--token",
            "loom_api_test",
        ],
    )

    assert exit_code == 0
    assert paths == ["/api/v1/benchmarks?limit=200&include_empty=true"]
