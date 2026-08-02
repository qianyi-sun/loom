from __future__ import annotations

from urllib.error import HTTPError

import pytest
from scripts.ops import authoritative_gate
from scripts.ops.authoritative_gate import GitHubAPIError, GitHubClient
from scripts.ops.authoritative_gate_metrics import (
    MetricsError,
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
        "distinct_source_attempts": 1,
        "publish_jobs": 4,
        "publisher_runs": 5,
        "trigger_counts": {"pull_request_target": 1, "workflow_run": 3},
    }
    assert summary["per_source_attempt"] == {
        "api_calls": 10.0,
        "publish_jobs": 3.0,
        "publisher_runs": 3.0,
    }
    assert summary["coverage"] == {
        "instrumented_runs": 4,
        "legacy_runs": 1,
        "malformed_runs": 0,
        "publish_jobs_with_metrics": 3,
        "publish_jobs_without_metrics": 1,
    }
    assert summary["failures"] == {"publisher_runs": 1}
    assert summary["lifecycle_deliveries"] == {
        "completed": 1,
        "in_progress": 1,
        "requested": 1,
        "synchronize": 1,
    }
