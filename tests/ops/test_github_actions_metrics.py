from __future__ import annotations

import argparse
from collections.abc import Mapping
from typing import Any

import pytest
from scripts.ops.github_actions_metrics import (
    GitHubActionsMetricsClient,
    MetricsError,
    parse_timestamp,
)


def test_parse_timestamp_requires_iso_timezone() -> None:
    assert parse_timestamp("2026-08-20T12:00:00Z").isoformat() == "2026-08-20T12:00:00+00:00"
    with pytest.raises(argparse.ArgumentTypeError, match="ISO-8601"):
        parse_timestamp("not-a-timestamp")
    with pytest.raises(argparse.ArgumentTypeError, match="timezone"):
        parse_timestamp("2026-08-20T12:00:00")


def test_collect_run_attempt_binds_metadata_and_exact_attempt_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = GitHubActionsMetricsClient(
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


def test_collect_run_jobs_rejects_malformed_api_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = GitHubActionsMetricsClient(
        token="token",
        repository="qianyi-sun/loom",
        api_url="https://api.github.test",
    )
    monkeypatch.setattr(client, "_json", lambda *_args, **_kwargs: {"jobs": ["invalid"]})

    with pytest.raises(MetricsError, match="jobs response is invalid"):
        client.collect_run_jobs(run_id=11, attempt=1)
