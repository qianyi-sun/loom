from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from scripts.ops.ci_retry_governance import RetryError, RetryRequest, execute_retry

REPO_ROOT = Path(__file__).resolve().parents[2]


class FakeClient:
    def __init__(self, run: dict[str, Any], *, fail_request: bool = False) -> None:
        self.run = run
        self.fail_request = fail_request
        self.calls: list[tuple[int, str]] = []

    def get_run(self, run_id: int) -> dict[str, Any]:
        assert run_id == self.run["id"]
        return self.run

    def request_retry(self, run_id: int, mode: str) -> None:
        if self.fail_request:
            raise RetryError("request unavailable")
        self.calls.append((run_id, mode))


def _run(**overrides: Any) -> dict[str, Any]:
    run = {
        "id": 123,
        "workflow_id": 302898379,
        "run_attempt": 2,
        "event": "pull_request",
        "status": "completed",
        "conclusion": "failure",
        "head_sha": "a" * 40,
        "repository": {"full_name": "qianyi-sun/loom"},
    }
    run.update(overrides)
    return run


def _request(**overrides: Any) -> RetryRequest:
    values = {
        "repository": "qianyi-sun/loom",
        "run_id": 123,
        "failed_attempt": 2,
        "reason": "platform_transient",
        "evidence_url": "https://github.com/qianyi-sun/loom/issues/1130",
        "mode": "failed_jobs",
        "actor": "qianyi-sun",
    }
    values.update(overrides)
    return RetryRequest(**values)


def test_retryable_failure_records_reason_and_requests_failed_jobs() -> None:
    client = FakeClient(_run())

    record = execute_retry(_request(), client)

    assert record["decision"] == "retry_requested"
    assert record["reason"] == "platform_transient"
    assert record["source_workflow"] == "CI"
    assert client.calls == [(123, "failed_jobs")]


def test_code_failure_is_classified_but_never_retried() -> None:
    client = FakeClient(_run())

    record = execute_retry(_request(reason="code_failure"), client)

    assert record["decision"] == "denied_code_change_required"
    assert client.calls == []


def test_all_jobs_is_reserved_for_capacity_queue() -> None:
    with pytest.raises(RetryError, match="reserved"):
        execute_retry(_request(mode="all_jobs"), FakeClient(_run()))

    client = FakeClient(_run())
    record = execute_retry(_request(mode="all_jobs", reason="capacity_queue"), client)
    assert record["decision"] == "retry_requested"
    assert client.calls == [(123, "all_jobs")]


def test_request_failure_retains_the_classification_record() -> None:
    record = execute_retry(_request(), FakeClient(_run(), fail_request=True))

    assert record["decision"] == "retry_request_failed"
    assert record["request_error"] == "request unavailable"


@pytest.mark.parametrize(
    ("run_change", "request_change", "message"),
    [
        ({"run_attempt": 3}, {}, "stale"),
        ({"conclusion": "success"}, {}, "terminal non-successful"),
        ({"status": "in_progress", "conclusion": None}, {}, "terminal non-successful"),
        ({"workflow_id": 1}, {}, "four required"),
        ({"event": "push"}, {}, "pull_request"),
        ({"repository": {"full_name": "other/repo"}}, {}, "pull_request"),
        ({}, {"evidence_url": "https://example.com/incident"}, "this GitHub repository"),
    ],
)
def test_untrusted_or_stale_target_fails_closed(
    run_change: dict[str, Any],
    request_change: dict[str, Any],
    message: str,
) -> None:
    with pytest.raises(RetryError, match=message):
        execute_retry(_request(**request_change), FakeClient(_run(**run_change)))


def test_bot_actor_is_accepted() -> None:
    record = execute_retry(_request(actor="github-actions[bot]"), FakeClient(_run()))
    assert record["actor"] == "github-actions[bot]"


def test_retry_workflow_is_dispatch_only_and_least_privilege() -> None:
    workflow = yaml.safe_load(
        (REPO_ROOT / ".github/workflows/ci-retry.yml").read_text(encoding="utf-8"),
    )

    assert list(workflow[True]) == ["workflow_dispatch"]
    assert workflow["permissions"] == {"contents": "read"}
    job = workflow["jobs"]["retry"]
    assert job["permissions"] == {"actions": "write", "contents": "read"}
    checkout = job["steps"][0]
    assert checkout["with"]["ref"] == "${{ github.workflow_sha }}"
    assert checkout["with"]["persist-credentials"] is False
