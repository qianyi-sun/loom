from __future__ import annotations

import argparse
from typing import Any

import pytest
from scripts.ops import ci_runner_capacity_metrics as metrics
from scripts.ops.github_actions_metrics import MetricsError


def _job(
    name: str,
    *,
    created: str = "2026-08-04T21:00:00Z",
    started: str = "2026-08-04T21:00:10Z",
    completed: str = "2026-08-04T21:01:10Z",
    conclusion: str = "success",
    runner_name: str = "oldlab5-kvm-normal-00-abcd",
    labels: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "created_at": created,
        "started_at": started,
        "completed_at": completed,
        "conclusion": conclusion,
        "runner_name": runner_name,
        "labels": labels
        if labels is not None
        else ["self-hosted", "oldlab-5", "loom-ci-normal"],
    }


def _run(workflow: str, run_id: int, jobs: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "workflow": workflow,
        "run_id": run_id,
        "attempt": 1,
        "event": "pull_request",
        "head_sha": "a" * 40,
        "jobs": jobs,
    }


def test_parse_run_spec_requires_supported_exact_identity() -> None:
    assert metrics.parse_run_spec("CI:123:2") == metrics.RunSpec("CI", 123, 2)
    assert metrics.parse_run_spec("images:456") == metrics.RunSpec("images", 456, 1)

    with pytest.raises(argparse.ArgumentTypeError, match="WORKFLOW:RUN_ID"):
        metrics.parse_run_spec("unknown:123")
    with pytest.raises(argparse.ArgumentTypeError, match="WORKFLOW:RUN_ID"):
        metrics.parse_run_spec("CI:0")


def test_summary_separates_workflow_work_class_and_runner_class() -> None:
    summary = metrics.summarize_runs(
        [
            _run("CI", 1, [_job("tests-root (1-of-2)")]),
            _run(
                "images",
                2,
                [
                    _job(
                        "worker (multi-arch)",
                        runner_name="GitHub Actions 1000000000",
                        labels=["ubuntu-latest"],
                    ),
                ],
            ),
            _run(
                "cluster-smoke",
                3,
                [_job("cluster contract (render live k3s topology)")],
            ),
        ],
    )

    assert summary["by_work_class"]["normal"]["jobs"] == 1
    assert summary["by_work_class"]["image"]["jobs"] == 1
    assert summary["by_work_class"]["smoke"]["jobs"] == 1
    assert summary["by_work_class_and_runner"]["normal/oldlab5"]["jobs"] == 1
    assert summary["by_work_class_and_runner"]["image/github_hosted"]["jobs"] == 1
    assert summary["jobs"][0]["queue_seconds"] == 10
    assert summary["jobs"][0]["execution_seconds"] == 60
    assert summary["by_architecture"]["emulated_multi_arch"]["jobs"] == 1


def test_summary_separates_native_image_architectures_and_runners() -> None:
    summary = metrics.summarize_runs(
        [
            _run(
                "images",
                2,
                [
                    _job("worker (linux/amd64)"),
                    _job(
                        "worker (linux/arm64)",
                        runner_name="GitHub Actions 1000000000",
                        labels=["ubuntu-24.04-arm"],
                    ),
                ],
            ),
        ],
    )

    assert summary["schema_version"] == 2
    assert summary["by_architecture"]["amd64"]["jobs"] == 1
    assert summary["by_architecture"]["arm64"]["jobs"] == 1
    assert summary["by_architecture_and_runner"]["amd64/oldlab5"]["jobs"] == 1
    assert summary["by_architecture_and_runner"]["arm64/github_hosted"]["jobs"] == 1
    assert metrics.evaluate_native_architectures(summary)["status"] == "pass"


def test_native_architecture_acceptance_rejects_emulated_or_missing_architecture() -> None:
    summary = metrics.summarize_runs(
        [_run("images", 2, [_job("worker (multi-arch)")])],
    )

    acceptance = metrics.evaluate_native_architectures(summary)

    assert acceptance["status"] == "fail"
    assert acceptance["emulated_multi_arch_jobs"] == 1
    assert acceptance["criteria"]["amd64"]["jobs"] is None
    assert acceptance["criteria"]["arm64"]["jobs"] is None


def test_summary_excludes_control_and_skipped_jobs() -> None:
    summary = metrics.summarize_runs(
        [
            _run(
                "CI",
                1,
                [
                    _job("workflow-plan"),
                    _job("repository-checks"),
                    _job("tests-packages", conclusion="skipped"),
                    _job("runtime-payload"),
                ],
            ),
        ],
    )

    assert [job["name"] for job in summary["jobs"]] == ["runtime-payload"]


def test_bounded_wait_fails_on_queue_boundary_or_job_failure() -> None:
    summary = metrics.summarize_runs(
        [
            _run(
                "CI",
                1,
                [
                    _job(
                        "runtime-payload",
                        started="2026-08-04T21:06:00Z",
                        completed="2026-08-04T21:07:00Z",
                    ),
                ],
            ),
            _run(
                "images",
                2,
                [_job("worker (multi-arch)", conclusion="failure")],
            ),
            _run(
                "staging-smoke",
                3,
                [_job("manifest-owned system smoke")],
            ),
        ],
    )

    acceptance = metrics.evaluate_bounded_wait(summary)

    assert acceptance["status"] == "fail"
    assert acceptance["criteria"]["normal"]["passed"] is False
    assert acceptance["criteria"]["image"]["passed"] is False
    assert acceptance["criteria"]["smoke"]["passed"] is True


def test_bounded_wait_rejects_single_max_breach_hidden_by_p95() -> None:
    normal_jobs = [
        _job(
            f"integration ({index}-of-9)",
            started="2026-08-04T21:00:10Z",
            completed="2026-08-04T21:01:10Z",
        )
        for index in range(1, 9)
    ]
    normal_jobs.append(
        _job(
            "lint-and-static",
            started="2026-08-04T21:05:01Z",
            completed="2026-08-04T21:06:01Z",
        ),
    )
    summary = metrics.summarize_runs(
        [
            _run("CI", 1, normal_jobs),
            _run("images", 2, [_job("worker (multi-arch)")]),
            _run(
                "cluster-smoke",
                3,
                [_job("cluster contract (render live k3s topology)")],
            ),
            _run(
                "staging-smoke",
                4,
                [_job("manifest-owned system smoke")],
            ),
        ],
    )

    acceptance = metrics.evaluate_bounded_wait(summary)

    assert summary["by_work_class"]["normal"]["queue_seconds"]["p95"] == 10
    assert acceptance["status"] == "fail"
    assert acceptance["criteria"]["normal"] == {
        "jobs": 9,
        "queue_p95_seconds": 10,
        "queue_max_seconds": 301,
        "queue_boundary_breaches": 1,
        "required_max_seconds": 300,
        "failures": 0,
        "passed": False,
    }


def test_bounded_wait_requires_an_observed_job_in_every_class() -> None:
    summary = metrics.summarize_runs(
        [
            _run("CI", 1, [_job("runtime-payload")]),
            _run("images", 2, [_job("worker (multi-arch)")]),
        ],
    )

    acceptance = metrics.evaluate_bounded_wait(summary)

    assert acceptance["status"] == "fail"
    assert acceptance["criteria"]["smoke"]["jobs"] == 0


def test_bounded_wait_requires_all_four_workflows_on_one_head() -> None:
    runs = [
        _run("CI", 1, [_job("runtime-payload")]),
        _run("images", 2, [_job("worker (multi-arch)")]),
        _run(
            "cluster-smoke",
            3,
            [_job("cluster contract (render live k3s topology)")],
        ),
        _run("staging-smoke", 4, [_job("manifest-owned system smoke")]),
    ]
    summary = metrics.summarize_runs(runs)

    acceptance = metrics.evaluate_bounded_wait(summary)

    assert acceptance["status"] == "pass"
    assert acceptance["workflow_coverage"]["passed"] is True

    with pytest.raises(MetricsError, match="same head"):
        metrics.summarize_runs(
            [runs[0], {**runs[1], "head_sha": "b" * 40}],
        )


def test_non_terminal_or_negative_job_timing_fails_closed() -> None:
    with pytest.raises(MetricsError, match="not terminal"):
        metrics.summarize_runs(
            [
                _run(
                    "CI",
                    1,
                    [{**_job("runtime-payload"), "completed_at": None}],
                ),
            ],
        )

    with pytest.raises(MetricsError, match="queue duration is negative"):
        metrics.summarize_runs(
            [
                _run(
                    "CI",
                    1,
                    [
                        _job(
                            "runtime-payload",
                            created="2026-08-04T21:01:00Z",
                            started="2026-08-04T21:00:00Z",
                        ),
                    ],
                ),
            ],
        )


def test_non_pr_or_invalid_head_evidence_fails_closed() -> None:
    run = _run("CI", 1, [_job("runtime-payload")])
    with pytest.raises(MetricsError, match="pull_request"):
        metrics.summarize_runs([{**run, "event": "push"}])
    with pytest.raises(MetricsError, match="head SHA"):
        metrics.summarize_runs([{**run, "head_sha": "not-a-sha"}])
