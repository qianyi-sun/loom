from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from threading import Barrier, get_ident
from typing import Any
from urllib.error import HTTPError

import pytest
from scripts.ops import authoritative_gate
from scripts.ops.authoritative_gate import GitHubAPIError, GitHubClient
from scripts.ops.authoritative_gate_metrics import (
    GitHubMetricsClient,
    MetricsError,
    evaluate_track2_acceptance,
    extract_job_metrics,
    parse_run_name,
    summarize_runs,
)


def run_name(
    *,
    trigger: str = "workflow_run",
    workflow: int = 302898379,
    run: int = 1001,
    attempt: int = 1,
    delivery: str = "completed",
    pull: int = 0,
) -> str:
    return (
        "publisher-metrics-v1 "
        f"trigger={trigger} source_workflow={workflow} source_run={run} "
        f"source_attempt={attempt} delivery={delivery} pull={pull}"
    )


def test_parse_run_name_preserves_exact_source_attempt() -> None:
    identity = parse_run_name(run_name(run=42, attempt=3, delivery="in_progress"))

    assert identity is not None
    assert identity.source_attempt_key == (302898379, 42, 3)
    assert identity.delivery == "in_progress"


@pytest.mark.parametrize(
    "value",
    [
        "publisher-metrics-v1 trigger=workflow_run",
        run_name(run=0),
        run_name(trigger="push"),
        run_name() + " pull=4",
    ],
)
def test_parse_run_name_rejects_malformed_instrumented_names(value: str) -> None:
    with pytest.raises(MetricsError, match="malformed publisher run name"):
        parse_run_name(value)


def test_parse_run_name_marks_legacy_runs_uncovered() -> None:
    assert parse_run_name("authoritative-gates") is None


def test_extract_job_metrics_handles_actions_timestamp_prefix() -> None:
    log = "\n".join(
        (
            "2026-08-01T01:02:03Z starting publisher",
            '2026-08-01T01:02:04Z {"api_calls": 7, "contexts": ["repository-checks"], '
            '"outcome": "success"}',
        )
    )

    assert extract_job_metrics(log) == {
        "api_calls": 7,
        "contexts": ["repository-checks"],
        "outcome": "success",
    }


def test_track2_acceptance_uses_terminal_attempts_and_ignores_expected_red_results() -> None:
    acceptance = evaluate_track2_acceptance(
        {
            "failures": {"by_class": {"authoritative_result": 7}},
            "source_attempt_coverage": {
                "terminal": 86,
                "terminal_without_complete_publish_metrics": 0,
                "terminal_without_invalidation": 0,
            },
            "terminal_per_source_attempt": {
                "api_calls": 20.256,
                "executed_publish_jobs": 1.849,
                "publisher_runs": 4.395,
            },
        },
        baseline_api_calls_per_attempt=64.8,
        baseline_executed_publish_jobs_per_attempt=4.767,
        baseline_publisher_runs_per_attempt=4.767,
    )

    assert acceptance["status"] == "pass"
    assert acceptance["criteria"]["minimum_terminal_source_attempts"] == {
        "actual": 86,
        "passed": True,
        "required": 30,
    }
    assert acceptance["criteria"]["executed_publish_job_reduction"] == {
        "actual_percent": 61.213,
        "passed": True,
        "required_percent": 40.0,
    }
    assert acceptance["criteria"]["api_call_reduction"] == {
        "actual_percent": 68.741,
        "passed": True,
        "required_percent": 40.0,
    }
    assert acceptance["publisher_run_record_reduction_percent"] == 7.804


def test_track2_acceptance_fails_closed_on_transport_or_coverage_gap() -> None:
    acceptance = evaluate_track2_acceptance(
        {
            "failures": {
                "by_class": {
                    "publisher_cancelled": 1,
                    "publisher_transport_failure": 2,
                }
            },
            "source_attempt_coverage": {
                "terminal": 12,
                "terminal_without_complete_publish_metrics": 1,
                "terminal_without_invalidation": 1,
            },
            "terminal_per_source_attempt": {
                "api_calls": None,
                "executed_publish_jobs": 4.2,
                "publisher_runs": 4.2,
            },
        },
        baseline_api_calls_per_attempt=64.8,
        baseline_executed_publish_jobs_per_attempt=4.767,
    )

    assert acceptance["status"] == "fail"
    assert not acceptance["criteria"]["minimum_terminal_source_attempts"]["passed"]
    assert not acceptance["criteria"]["complete_terminal_delivery_coverage"]["passed"]
    assert not acceptance["criteria"]["publisher_transport_integrity"]["passed"]
    assert not acceptance["criteria"]["api_call_reduction"]["passed"]


class FakeResponse:
    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return b'{"number": 1130}'


def test_publisher_client_counts_successful_api_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(authoritative_gate, "urlopen", lambda *_args, **_kwargs: FakeResponse())
    client = GitHubClient(token="token", repository="qianyi-sun/loom")

    assert client.get_pull_request(1130) == {"number": 1130}
    assert client.request_count == 1


def test_publisher_client_counts_failed_api_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*_args: object, **_kwargs: object) -> FakeResponse:
        raise HTTPError("https://api.github.com", 502, "bad gateway", {}, None)

    monkeypatch.setattr(authoritative_gate, "urlopen", fail)
    client = GitHubClient(token="token", repository="qianyi-sun/loom")

    with pytest.raises(GitHubAPIError):
        client.get_pull_request(1130)
    assert client.request_count == 1


def test_summarize_runs_exposes_amplification_and_coverage_gaps() -> None:
    runs = [
        {
            "display_title": run_name(delivery="requested"),
            "conclusion": "success",
            "jobs": [
                {
                    "name": "publish authoritative gate (repository-checks)",
                    "log": '2026-08-01T00:00:00Z {"api_calls": 4, "outcome": "in_progress"}',
                }
            ],
        },
        {
            "display_title": run_name(delivery="in_progress"),
            "conclusion": "success",
            "jobs": [
                {
                    "name": "publish authoritative gate (repository-checks)",
                    "log": '2026-08-01T00:01:00Z {"api_calls": 6, "outcome": "in_progress"}',
                }
            ],
        },
        {
            "display_title": run_name(delivery="completed"),
            "conclusion": "failure",
            "jobs": [
                {
                    "name": "publish authoritative gate (repository-checks)",
                    "log": "log download unavailable",
                }
            ],
        },
        {
            "display_title": run_name(
                trigger="pull_request_target",
                workflow=0,
                run=0,
                attempt=0,
                delivery="synchronize",
                pull=1131,
            ),
            "conclusion": "success",
            "jobs": [
                {
                    "name": "publish authoritative gate (images-gate)",
                    "log": '2026-08-01T00:02:00Z {"api_calls": 2, "outcome": "in_progress"}',
                }
            ],
        },
        {"display_title": "authoritative-gates", "conclusion": "success", "jobs": []},
    ]

    summary = summarize_runs(runs)

    assert summary["totals"] == {
        "api_calls": 12,
        "api_calls_complete": False,
        "distinct_source_attempts": 1,
        "publish_jobs": 4,
        "publisher_runs": 5,
        "trigger_counts": {"pull_request_target": 1, "workflow_run": 3},
    }
    assert summary["per_source_attempt"] == {
        "api_calls": None,
        "executed_publish_jobs": 3.0,
        "publish_jobs": 3.0,
        "publisher_runs": 3.0,
    }
    assert summary["pull_request_target"] == {
        "api_calls": 2,
        "api_calls_complete": True,
        "executed_publish_jobs": 1,
        "per_run": {
            "api_calls": 2.0,
            "executed_publish_jobs": 1.0,
            "publish_jobs": 1.0,
        },
        "publish_jobs": 1,
        "publisher_runs": 1,
    }
    assert summary["coverage"] == {
        "instrumented_runs": 4,
        "publish_job_log_error_examples": [],
        "publish_job_log_errors": 0,
        "publish_job_logs_skipped": 0,
        "publish_job_logs_without_metrics": 1,
        "legacy_runs": 1,
        "malformed_runs": 0,
        "publish_jobs_skipped": 0,
        "publish_jobs_with_metrics": 3,
        "publish_jobs_without_metrics": 1,
    }
    assert summary["failures"] == {
        "by_class": {"publisher_transport_failure": 1},
        "examples": [
            {
                "class": "publisher_transport_failure",
                "conclusion": "failure",
                "delivery": "completed",
                "run_id": None,
                "source_attempt": 1,
                "source_run": 1001,
                "source_workflow": 302898379,
            }
        ],
        "publisher_runs": 1,
    }
    assert summary["first_attempt_in_progress"] == {
        "api_calls": 6,
        "api_calls_complete": True,
        "executed_publish_jobs": 1,
        "publish_jobs": 1,
        "publisher_runs": 1,
    }
    assert summary["lifecycle_deliveries"] == {
        "completed": 1,
        "in_progress": 1,
        "requested": 1,
        "synchronize": 1,
    }
    assert summary["publisher_runs_per_source_attempt_distribution"] == {"3": 1}
    assert summary["source_workflows"] == {
        "302898379": {
            "api_calls": 10,
            "api_calls_complete": False,
            "attempts": 1,
            "deliveries": {"completed": 1, "in_progress": 1, "requested": 1},
            "name": "CI",
            "per_attempt": {
                "api_calls": None,
                "executed_publish_jobs": 3.0,
                "publish_jobs": 3.0,
                "publisher_runs": 3.0,
            },
            "executed_publish_jobs": 3,
            "publish_jobs": 3,
            "publisher_runs": 3,
        }
    }
    assert summary["source_attempt_coverage"] == {
        "active_or_incomplete": 0,
        "incomplete_terminal_examples": [
            {
                "source_attempt": 1,
                "source_run": 1001,
                "source_workflow": 302898379,
            }
        ],
        "observed": 1,
        "terminal": 1,
        "terminal_with_complete_publish_metrics": 0,
        "terminal_with_invalidation": 1,
        "terminal_without_complete_publish_metrics": 1,
        "terminal_without_invalidation": 0,
    }
    assert summary["terminal_per_source_attempt"] == {
        "api_calls": None,
        "executed_publish_jobs": 3.0,
        "publish_jobs": 3.0,
        "publisher_runs": 3.0,
    }


def test_summarize_runs_distinguishes_authoritative_failure_from_transport_failure() -> None:
    summary = summarize_runs(
        [
            {
                "conclusion": "failure",
                "display_title": run_name(delivery="completed"),
                "id": 9001,
                "jobs": [
                    {
                        "conclusion": "failure",
                        "log": (
                            '2026-08-04T20:00:00Z {"api_calls": 9, '
                            '"outcome": "failure"}'
                        ),
                        "name": "publish authoritative gate (repository-checks)",
                    }
                ],
            }
        ]
    )

    assert summary["failures"] == {
        "by_class": {"authoritative_result": 1},
        "examples": [
            {
                "class": "authoritative_result",
                "conclusion": "failure",
                "delivery": "completed",
                "run_id": 9001,
                "source_attempt": 1,
                "source_run": 1001,
                "source_workflow": 302898379,
            }
        ],
        "publisher_runs": 1,
    }
    assert summary["source_attempt_coverage"] == {
        "active_or_incomplete": 0,
        "incomplete_terminal_examples": [
            {
                "source_attempt": 1,
                "source_run": 1001,
                "source_workflow": 302898379,
            }
        ],
        "observed": 1,
        "terminal": 1,
        "terminal_with_complete_publish_metrics": 1,
        "terminal_with_invalidation": 0,
        "terminal_without_complete_publish_metrics": 0,
        "terminal_without_invalidation": 1,
    }


def test_summarize_runs_keeps_cancelled_publishers_out_of_authoritative_results() -> None:
    summary = summarize_runs(
        [
            {
                "conclusion": "cancelled",
                "display_title": run_name(delivery="requested"),
                "id": 9002,
                "jobs": [
                    {
                        "conclusion": "success",
                        "log": (
                            '2026-08-04T20:00:00Z {"api_calls": 5, '
                            '"outcome": "in_progress"}'
                        ),
                        "name": "publish authoritative gate (repository-checks)",
                    }
                ],
            }
        ]
    )

    assert summary["failures"]["by_class"] == {"publisher_cancelled": 1}


def test_collect_runs_fetches_instrumented_jobs_concurrently_and_preserves_order() -> None:
    barrier = Barrier(2)
    worker_threads: set[int] = set()

    class ParallelClient(GitHubMetricsClient):
        def __init__(self) -> None:
            super().__init__(
                token="token",
                repository="qianyi-sun/loom",
                api_url="https://api.github.test",
            )

        def _json(
            self,
            path: str,
            *,
            query: Mapping[str, str | int] | None = None,
        ) -> Any:
            assert path.endswith("/runs")
            return {
                "workflow_runs": [
                    {
                        "id": run_id,
                        "run_attempt": 1,
                        "created_at": f"2026-08-0{run_id}T00:00:00Z",
                        "display_title": run_name(run=run_id),
                    }
                    for run_id in (1, 2)
                ]
            }

        def _collect_jobs(
            self,
            run: Mapping[str, Any],
            *,
            include_logs: bool,
        ) -> list[Mapping[str, Any]]:
            assert include_logs is True
            worker_threads.add(get_ident())
            barrier.wait(timeout=2)
            return [{"name": f"job-{run['id']}"}]

    client = ParallelClient()
    runs = client.collect_runs(
        workflow_id=318631340,
        since=datetime(2026, 8, 1, tzinfo=UTC),
        until=datetime(2026, 8, 3, tzinfo=UTC),
        max_runs=2,
        workers=2,
        include_logs=True,
    )

    assert [run["id"] for run in runs] == [1, 2]
    assert [run["jobs"] for run in runs] == [[{"name": "job-1"}], [{"name": "job-2"}]]
    assert len(worker_threads) == 2


def test_summarize_runs_treats_workflow_skips_as_complete_zero_cost() -> None:
    summary = summarize_runs(
        [
            {
                "display_title": run_name(
                    trigger="pull_request_target",
                    workflow=0,
                    run=0,
                    attempt=0,
                    delivery="opened",
                    pull=1144,
                ),
                "conclusion": "success",
                "jobs": [
                    {
                        "conclusion": "skipped",
                        "name": "publish authoritative gate (${{ matrix.context }})",
                    }
                ],
            }
        ]
    )

    assert summary["totals"]["api_calls_complete"] is True
    assert summary["first_attempt_in_progress"] == {
        "api_calls": 0,
        "api_calls_complete": True,
        "executed_publish_jobs": 0,
        "publish_jobs": 0,
        "publisher_runs": 0,
    }
    assert summary["coverage"]["publish_jobs_skipped"] == 1
    assert summary["coverage"]["publish_jobs_without_metrics"] == 0
    assert summary["pull_request_target"] == {
        "api_calls": 0,
        "api_calls_complete": True,
        "executed_publish_jobs": 0,
        "per_run": {
            "api_calls": 0.0,
            "executed_publish_jobs": 0.0,
            "publish_jobs": 1.0,
        },
        "publish_jobs": 1,
        "publisher_runs": 1,
    }


def test_summarize_runs_treats_source_workflow_skips_as_complete_zero_cost() -> None:
    summary = summarize_runs(
        [
            {
                "display_title": run_name(delivery="in_progress"),
                "conclusion": "success",
                "jobs": [
                    {
                        "conclusion": "skipped",
                        "name": "publish authoritative gate (repository-checks)",
                    }
                ],
            }
        ]
    )

    assert summary["totals"]["api_calls_complete"] is True
    assert summary["first_attempt_in_progress"] == {
        "api_calls": 0,
        "api_calls_complete": True,
        "executed_publish_jobs": 0,
        "publish_jobs": 1,
        "publisher_runs": 1,
    }
    assert summary["per_source_attempt"] == {
        "api_calls": 0.0,
        "executed_publish_jobs": 0.0,
        "publish_jobs": 1.0,
        "publisher_runs": 1.0,
    }
    assert summary["source_workflows"]["302898379"] == {
        "api_calls": 0,
        "api_calls_complete": True,
        "attempts": 1,
        "deliveries": {"in_progress": 1},
        "executed_publish_jobs": 0,
        "name": "CI",
        "per_attempt": {
            "api_calls": 0.0,
            "executed_publish_jobs": 0.0,
            "publish_jobs": 1.0,
            "publisher_runs": 1.0,
        },
        "publish_jobs": 1,
        "publisher_runs": 1,
    }


def test_collect_jobs_can_skip_log_downloads(monkeypatch: pytest.MonkeyPatch) -> None:
    client = GitHubMetricsClient(
        token="token",
        repository="qianyi-sun/loom",
        api_url="https://api.github.test",
    )
    monkeypatch.setattr(
        client,
        "_json",
        lambda *_args, **_kwargs: {
            "jobs": [{"id": 44, "name": "publish authoritative gate (repository-checks)"}]
        },
    )
    monkeypatch.setattr(
        client,
        "_request",
        lambda *_args, **_kwargs: pytest.fail("job logs must not be downloaded"),
    )

    jobs = client._collect_jobs({"id": 11, "run_attempt": 1}, include_logs=False)

    assert jobs == [
        {
            "id": 44,
            "name": "publish authoritative gate (repository-checks)",
            "log_skipped": True,
        }
    ]


def test_collect_run_attempt_binds_metadata_and_exact_attempt_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = GitHubMetricsClient(
        token="token",
        repository="qianyi-sun/loom",
        api_url="https://api.github.test",
    )
    monkeypatch.setattr(
        client,
        "_json",
        lambda *_args, **_kwargs: {
            "id": 11,
            "run_attempt": 3,
            "event": "pull_request",
            "head_sha": "a" * 40,
        },
    )
    observed: list[tuple[int, int]] = []

    def collect_jobs(*, run_id: int, attempt: int) -> list[Mapping[str, Any]]:
        observed.append((run_id, attempt))
        return [{"id": 44}]

    monkeypatch.setattr(client, "collect_run_jobs", collect_jobs)

    run = client.collect_run_attempt(run_id=11, attempt=2)

    assert observed == [(11, 2)]
    assert run["requested_attempt"] == 2
    assert run["jobs"] == [{"id": 44}]

    with pytest.raises(MetricsError, match="attempt is invalid"):
        client.collect_run_attempt(run_id=11, attempt=4)


def test_collect_jobs_does_not_request_logs_for_workflow_skips(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = GitHubMetricsClient(
        token="token",
        repository="qianyi-sun/loom",
        api_url="https://api.github.test",
    )
    monkeypatch.setattr(
        client,
        "_json",
        lambda *_args, **_kwargs: {
            "jobs": [
                {
                    "conclusion": "skipped",
                    "id": 44,
                    "name": "publish authoritative gate (${{ matrix.context }})",
                }
            ]
        },
    )
    monkeypatch.setattr(
        client,
        "_request",
        lambda *_args, **_kwargs: pytest.fail("skipped jobs do not have logs"),
    )

    jobs = client._collect_jobs({"id": 11, "run_attempt": 1}, include_logs=True)

    assert jobs == [
        {
            "conclusion": "skipped",
            "id": 44,
            "log_unavailable_reason": "skipped_job",
            "name": "publish authoritative gate (${{ matrix.context }})",
        }
    ]
