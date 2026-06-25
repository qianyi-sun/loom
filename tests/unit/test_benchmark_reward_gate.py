from __future__ import annotations

import importlib.util
import io
import sys
from pathlib import Path
from urllib.error import HTTPError

import pytest

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


def test_readiness_gate_skips_explicitly_unsupported_benchmarks() -> None:
    gate = _load_module()

    results = gate.check_benchmark_readiness(
        [
            {
                "id": "osworld",
                "readiness_state": "blocked",
                "readiness_label": "Not supported yet",
                "blocker_reason": "unsupported_runtime",
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

    assert [r.status for r in results] == ["pass"]
    assert "1 runnable benchmarks" in results[0].detail
    assert "1 unsupported benchmarks skipped" in results[0].detail


def test_readiness_gate_skips_explicitly_deferred_benchmarks() -> None:
    gate = _load_module()

    results = gate.check_benchmark_readiness(
        [
            {
                "id": "gaia",
                "readiness_state": "blocked",
                "readiness_label": "Deferred",
                "blocker_reason": "deferred_support",
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

    assert [r.status for r in results] == ["pass"]
    assert "1 runnable benchmarks" in results[0].detail
    assert "1 unsupported benchmarks skipped" in results[0].detail


def test_readiness_gate_skips_non_v1_supported_benchmarks() -> None:
    gate = _load_module()

    results = gate.check_benchmark_readiness(
        [
            {
                "id": "browsecomp",
                "readiness_state": "blocked",
                "readiness_label": "Not in v1.0",
                "blocker_reason": "not_v1_supported",
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

    assert [r.status for r in results] == ["pass"]
    assert "1 runnable benchmarks" in results[0].detail
    assert "1 unsupported benchmarks skipped" in results[0].detail


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


def test_sweep_gate_requires_distinct_task_coverage_per_benchmark() -> None:
    gate = _load_module()

    results = gate.check_reward_sweep(
        batches=[
            {
                "id": "batch-1",
                "state": "succeeded",
                "expected_trial_count": 3,
            },
        ],
        trials_by_batch={
            "batch-1": [
                {
                    "id": "trial-1",
                    "task_id": "mbpp/1",
                    "state": "succeeded",
                    "failure_reason": None,
                    "aggregate_reward": 1.0,
                },
                {
                    "id": "trial-2",
                    "task_id": "mbpp/1",
                    "state": "succeeded",
                    "failure_reason": None,
                    "aggregate_reward": 0.0,
                },
                {
                    "id": "trial-3",
                    "task_id": "gpqa/1",
                    "state": "succeeded",
                    "failure_reason": None,
                    "aggregate_reward": 1.0,
                },
            ],
        },
        expected_benchmark_ids=["mbpp", "gpqa"],
        expected_task_counts={"mbpp": 2, "gpqa": 1},
    )

    assert [r.status for r in results] == ["fail"]
    assert results[0].check_id == "benchmarks.v1_reward_sweep_complete"
    assert "mbpp task coverage 1/2" in results[0].detail
    assert "gpqa" not in results[0].detail


def test_reward_sweep_defaults_to_math500_not_full_hendrycks_math() -> None:
    gate = _load_module()

    assert "math-500" in gate.V1_SUPPORTED_BENCHMARK_IDS
    assert "hendrycks-math" not in gate.V1_SUPPORTED_BENCHMARK_IDS


def test_sweep_gate_passes_with_full_numeric_task_coverage() -> None:
    gate = _load_module()

    results = gate.check_reward_sweep(
        batches=[
            {
                "id": "batch-1",
                "state": "succeeded",
                "expected_trial_count": 3,
            },
        ],
        trials_by_batch={
            "batch-1": [
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
                    "aggregate_reward": 0.0,
                },
                {
                    "id": "trial-3",
                    "task_id": "gpqa/1",
                    "state": "succeeded",
                    "failure_reason": None,
                    "aggregate_reward": 0.0,
                },
            ],
        },
        expected_benchmark_ids=["mbpp", "gpqa"],
        expected_task_counts={"mbpp": 2, "gpqa": 1},
    )

    assert [r.status for r in results] == ["pass"]
    assert "2 benchmarks covered" in results[0].detail
    assert "3 distinct tasks" in results[0].detail


def test_sweep_gate_allows_rerun_to_cover_provider_failed_attempt() -> None:
    gate = _load_module()

    results = gate.check_reward_sweep(
        batches=[
            {
                "id": "full-batch",
                "state": "finished",
                "result_status": "partial_failed",
                "expected_trial_count": 2,
            },
            {
                "id": "rerun-batch",
                "state": "finished",
                "result_status": "succeeded",
                "expected_trial_count": 1,
            },
        ],
        trials_by_batch={
            "full-batch": [
                {
                    "id": "trial-1",
                    "task_id": "math-500/test/00001",
                    "state": "succeeded",
                    "failure_reason": None,
                    "aggregate_reward": 1.0,
                },
                {
                    "id": "trial-2",
                    "task_id": "math-500/test/00340",
                    "state": "failed",
                    "failure_reason": "gateway_error",
                    "aggregate_reward": None,
                },
            ],
            "rerun-batch": [
                {
                    "id": "trial-3",
                    "task_id": "math-500/test/00340",
                    "state": "succeeded",
                    "failure_reason": None,
                    "aggregate_reward": 0.0,
                },
            ],
        },
        expected_benchmark_ids=["math-500"],
        expected_task_counts={"math-500": 2},
    )

    assert [r.status for r in results] == ["pass"]
    assert "2 distinct tasks" in results[0].detail


def test_sweep_gate_allows_rerun_to_cover_platform_internal_error() -> None:
    gate = _load_module()

    results = gate.check_reward_sweep(
        batches=[
            {
                "id": "polluted-batch",
                "state": "finished",
                "result_status": "partial_failed",
                "expected_trial_count": 2,
            },
            {
                "id": "gap-rerun",
                "state": "finished",
                "result_status": "succeeded",
                "expected_trial_count": 1,
            },
        ],
        trials_by_batch={
            "polluted-batch": [
                {
                    "id": "trial-1",
                    "task_id": "skillflow/DMAIC-Quality-Analysis/ok-task",
                    "state": "succeeded",
                    "failure_reason": None,
                    "aggregate_reward": 1.0,
                },
                {
                    "id": "trial-2",
                    "task_id": "skillflow/DMAIC-Quality-Analysis/upload-path-with-spaces",
                    "state": "failed",
                    "failure_reason": "internal_error",
                    "aggregate_reward": None,
                },
            ],
            "gap-rerun": [
                {
                    "id": "trial-3",
                    "task_id": "skillflow/DMAIC-Quality-Analysis/upload-path-with-spaces",
                    "state": "succeeded",
                    "failure_reason": None,
                    "aggregate_reward": 0.0,
                },
            ],
        },
        expected_benchmark_ids=["skillflow"],
        expected_task_counts={"skillflow": 2},
    )

    assert [r.status for r in results] == ["pass"]
    assert "2 distinct tasks" in results[0].detail


def test_sweep_gate_keeps_verifier_errors_fatal_even_after_rerun() -> None:
    gate = _load_module()

    results = gate.check_reward_sweep(
        batches=[
            {
                "id": "polluted-batch",
                "state": "finished",
                "result_status": "partial_failed",
                "expected_trial_count": 1,
            },
            {
                "id": "gap-rerun",
                "state": "finished",
                "result_status": "succeeded",
                "expected_trial_count": 1,
            },
        ],
        trials_by_batch={
            "polluted-batch": [
                {
                    "id": "trial-1",
                    "task_id": "skillflow/DMAIC-Quality-Analysis/verifier-broken",
                    "state": "failed",
                    "failure_reason": "verifier_error",
                    "aggregate_reward": None,
                },
            ],
            "gap-rerun": [
                {
                    "id": "trial-2",
                    "task_id": "skillflow/DMAIC-Quality-Analysis/verifier-broken",
                    "state": "succeeded",
                    "failure_reason": None,
                    "aggregate_reward": 0.0,
                },
            ],
        },
        expected_benchmark_ids=["skillflow"],
        expected_task_counts={"skillflow": 1},
    )

    assert [r.status for r in results] == ["fail"]
    assert "trial-1 has benchmark-side failure reason verifier_error" in results[0].detail


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
        assert timeout == 120
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


def test_cli_reports_timeout_errors_without_traceback(monkeypatch, capsys) -> None:
    gate = _load_module()

    def raise_timeout(_req, *, timeout):
        assert timeout == 120
        raise TimeoutError("read timed out")

    monkeypatch.setattr(gate, "urlopen", raise_timeout)

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
    assert "network timeout" in captured.err
    assert "read timed out" in captured.err
    assert "Traceback" not in captured.err


def test_cli_accepts_custom_request_timeout(monkeypatch) -> None:
    gate = _load_module()
    seen_timeouts: list[float] = []

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return b'{"items":[]}'

    def capture_timeout(_req, *, timeout):
        seen_timeouts.append(timeout)
        return FakeResponse()

    monkeypatch.setattr(gate, "urlopen", capture_timeout)

    exit_code = gate.main(
        [
            "readiness",
            "--server-url",
            "https://loom.example",
            "--token",
            "loom_api_test",
            "--request-timeout",
            "90",
        ],
    )

    assert exit_code == 1
    assert seen_timeouts == [90.0]


def test_readiness_cli_checks_full_user_visible_catalog(monkeypatch) -> None:
    gate = _load_module()
    paths: list[str] = []

    class FakeApiClient:
        def __init__(
            self,
            server_url: str,
            token: str,
            *,
            timeout: float,
        ) -> None:
            assert server_url == "https://loom.example"
            assert token == "loom_api_test"
            assert timeout == 120.0

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


def test_sweep_cli_fetches_batches_trials_and_task_counts(monkeypatch) -> None:
    gate = _load_module()
    get_paths: list[str] = []
    post_calls: list[tuple[str, dict]] = []

    class FakeApiClient:
        def __init__(
            self,
            server_url: str,
            token: str,
            *,
            timeout: float,
        ) -> None:
            assert server_url == "https://loom.example"
            assert token == "loom_api_test"
            assert timeout == 120.0

        def get_json(self, path: str):
            get_paths.append(path)
            if path == "/api/v1/batches/batch-1":
                return {
                    "id": "batch-1",
                    "state": "succeeded",
                    "expected_trial_count": 2,
                }
            if path == "/api/v1/trials?batch_id=batch-1&limit=200":
                return {
                    "items": [
                        {
                            "id": "trial-1",
                            "task_id": "mbpp/1",
                            "state": "succeeded",
                            "failure_reason": None,
                            "aggregate_reward": 1.0,
                        },
                        {
                            "id": "trial-2",
                            "task_id": "gpqa/1",
                            "state": "succeeded",
                            "failure_reason": None,
                            "aggregate_reward": 0.0,
                        },
                    ],
                    "next_cursor": None,
                }
            raise AssertionError(f"unexpected GET {path}")

        def post_json(self, path: str, payload: dict):
            post_calls.append((path, payload))
            benchmark_id = payload["task_filter"]["benchmark_id"]
            return {"count": {"mbpp": 1, "gpqa": 1}[benchmark_id]}

    monkeypatch.setattr(gate, "ApiClient", FakeApiClient)

    exit_code = gate.main(
        [
            "sweep",
            "--server-url",
            "https://loom.example",
            "--token",
            "loom_api_test",
            "--batch-id",
            "batch-1",
            "--expected-benchmark",
            "mbpp",
            "--expected-benchmark",
            "gpqa",
        ],
    )

    assert exit_code == 0
    assert get_paths == [
        "/api/v1/batches/batch-1",
        "/api/v1/trials?batch_id=batch-1&limit=200",
    ]
    assert post_calls == [
        ("/api/v1/tasks/count", {"task_filter": {"benchmark_id": "mbpp"}}),
        ("/api/v1/tasks/count", {"task_filter": {"benchmark_id": "gpqa"}}),
    ]


def test_sweep_cli_rejects_page_limit_above_public_api_max(monkeypatch, capsys) -> None:
    gate = _load_module()

    class FakeApiClient:
        def __init__(self, server_url: str, token: str) -> None:
            raise AssertionError("client should not be constructed for invalid limit")

    monkeypatch.setattr(gate, "ApiClient", FakeApiClient)

    with pytest.raises(SystemExit) as exc:
        gate.main(
            [
                "sweep",
                "--server-url",
                "https://loom.example",
                "--token",
                "loom_api_test",
                "--limit",
                "201",
                "--batch-id",
                "batch-1",
            ],
        )

    assert exc.value.code == 2
    assert "between 1 and 200" in capsys.readouterr().err
