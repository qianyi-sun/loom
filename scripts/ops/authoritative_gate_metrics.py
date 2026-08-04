#!/usr/bin/env python3
"""Measure authoritative-gate publisher amplification from GitHub Actions runs."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import local
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

DEFAULT_PUBLISHER_WORKFLOW_ID = 318631340
DEFAULT_WORKERS = 8
MAX_WORKERS = 32
RUN_NAME_VERSION = "publisher-metrics-v1"
PUBLISH_JOB_PREFIX = "publish authoritative gate ("
SOURCE_WORKFLOW_NAMES = {
    302898379: "CI",
    302898384: "images",
    302898381: "cluster-smoke",
    302898388: "staging-smoke",
}
RUN_NAME_FIELDS = (
    "trigger",
    "source_workflow",
    "source_run",
    "source_attempt",
    "delivery",
    "pull",
)
METRICS_JSON = re.compile(r"(\{.*\})\s*$")


class MetricsError(RuntimeError):
    """Raised when GitHub metrics evidence cannot be fetched or parsed."""


class SafeRedirectHandler(HTTPRedirectHandler):
    """Prevent the GitHub token from following job-log redirects off origin."""

    def redirect_request(
        self,
        req: Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> Request | None:
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is not None and urlsplit(req.full_url).netloc != urlsplit(newurl).netloc:
            redirected.remove_header("Authorization")
        return redirected


@dataclass(frozen=True)
class PublisherIdentity:
    trigger: str
    source_workflow: int
    source_run: int
    source_attempt: int
    delivery: str
    pull: int

    @property
    def source_attempt_key(self) -> tuple[int, int, int] | None:
        if self.trigger != "workflow_run":
            return None
        return (self.source_workflow, self.source_run, self.source_attempt)


def parse_run_name(value: str) -> PublisherIdentity | None:
    """Parse a versioned publisher run name, returning None for uncovered runs."""

    parts = value.split()
    if not parts or parts[0] != RUN_NAME_VERSION:
        return None
    values: dict[str, str] = {}
    for part in parts[1:]:
        key, separator, item = part.partition("=")
        if not separator or key in values:
            raise MetricsError(f"malformed publisher run name: {value}")
        values[key] = item
    if set(values) != set(RUN_NAME_FIELDS):
        raise MetricsError(f"malformed publisher run name: {value}")
    try:
        numeric = {
            key: int(values[key])
            for key in ("source_workflow", "source_run", "source_attempt", "pull")
        }
    except ValueError as exc:
        raise MetricsError(f"malformed publisher run name: {value}") from exc
    if any(item < 0 for item in numeric.values()):
        raise MetricsError(f"malformed publisher run name: {value}")
    trigger = values["trigger"]
    if trigger not in {"pull_request_target", "workflow_run"}:
        raise MetricsError(f"malformed publisher run name: {value}")
    if trigger == "workflow_run" and any(
        numeric[key] == 0 for key in ("source_workflow", "source_run", "source_attempt")
    ):
        raise MetricsError(f"malformed publisher run name: {value}")
    return PublisherIdentity(
        trigger=trigger,
        source_workflow=numeric["source_workflow"],
        source_run=numeric["source_run"],
        source_attempt=numeric["source_attempt"],
        delivery=values["delivery"],
        pull=numeric["pull"],
    )


def extract_job_metrics(log: str) -> Mapping[str, Any] | None:
    """Return the publisher's terminal JSON record from a timestamped job log."""

    for line in reversed(log.splitlines()):
        match = METRICS_JSON.search(line)
        if match is None:
            continue
        try:
            record = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        if not isinstance(record, Mapping):
            continue
        api_calls = record.get("api_calls")
        outcome = record.get("outcome")
        if isinstance(api_calls, int) and api_calls >= 0 and isinstance(outcome, str):
            return record
    return None


def _ratio(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 3) if denominator else None


def _percent_reduction(baseline: float, current: float | None) -> float | None:
    if baseline <= 0 or current is None:
        return None
    return round((baseline - current) / baseline * 100, 3)


def evaluate_track2_acceptance(
    summary: Mapping[str, Any],
    *,
    baseline_api_calls_per_attempt: float,
    baseline_executed_publish_jobs_per_attempt: float,
    baseline_publisher_runs_per_attempt: float | None = None,
    minimum_reduction_percent: float = 40.0,
    minimum_terminal_source_attempts: int = 30,
) -> dict[str, Any]:
    """Evaluate the #1130 Track 2 terminal-attempt acceptance boundary."""

    coverage = summary["source_attempt_coverage"]
    terminal_metrics = summary["terminal_per_source_attempt"]
    failures = summary["failures"]["by_class"]
    terminal_attempts = int(coverage["terminal"])
    current_api_calls = terminal_metrics["api_calls"]
    current_executed_jobs = terminal_metrics["executed_publish_jobs"]
    current_publisher_runs = terminal_metrics["publisher_runs"]
    api_reduction = _percent_reduction(
        baseline_api_calls_per_attempt,
        current_api_calls,
    )
    executed_job_reduction = _percent_reduction(
        baseline_executed_publish_jobs_per_attempt,
        current_executed_jobs,
    )
    publisher_run_reduction = (
        _percent_reduction(baseline_publisher_runs_per_attempt, current_publisher_runs)
        if baseline_publisher_runs_per_attempt is not None
        else None
    )
    criteria = {
        "api_call_reduction": {
            "actual_percent": api_reduction,
            "passed": api_reduction is not None
            and api_reduction >= minimum_reduction_percent,
            "required_percent": minimum_reduction_percent,
        },
        "complete_terminal_delivery_coverage": {
            "passed": coverage["terminal_without_invalidation"] == 0
            and coverage["terminal_without_complete_publish_metrics"] == 0,
            "terminal_without_complete_publish_metrics": coverage[
                "terminal_without_complete_publish_metrics"
            ],
            "terminal_without_invalidation": coverage["terminal_without_invalidation"],
        },
        "executed_publish_job_reduction": {
            "actual_percent": executed_job_reduction,
            "passed": executed_job_reduction is not None
            and executed_job_reduction >= minimum_reduction_percent,
            "required_percent": minimum_reduction_percent,
        },
        "minimum_terminal_source_attempts": {
            "actual": terminal_attempts,
            "passed": terminal_attempts >= minimum_terminal_source_attempts,
            "required": minimum_terminal_source_attempts,
        },
        "publisher_transport_integrity": {
            "cancelled_runs": int(failures.get("publisher_cancelled", 0)),
            "passed": failures.get("publisher_cancelled", 0) == 0
            and failures.get("publisher_transport_failure", 0) == 0,
            "transport_failure_runs": int(failures.get("publisher_transport_failure", 0)),
        },
    }
    return {
        "baseline": {
            "api_calls_per_source_attempt": baseline_api_calls_per_attempt,
            "executed_publish_jobs_per_source_attempt": (
                baseline_executed_publish_jobs_per_attempt
            ),
            "publisher_runs_per_source_attempt": baseline_publisher_runs_per_attempt,
        },
        "criteria": criteria,
        "current_terminal_attempts": {
            "api_calls_per_source_attempt": current_api_calls,
            "executed_publish_jobs_per_source_attempt": current_executed_jobs,
            "publisher_runs_per_source_attempt": current_publisher_runs,
        },
        "publisher_run_record_reduction_percent": publisher_run_reduction,
        "status": "pass" if all(item["passed"] for item in criteria.values()) else "fail",
    }


def summarize_runs(runs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Summarize publisher runs plus attached publish-job logs."""

    trigger_counts: Counter[str] = Counter()
    lifecycle_counts: Counter[str] = Counter()
    source_attempts: set[tuple[int, int, int]] = set()
    instrumented_runs = 0
    malformed_runs = 0
    legacy_runs = 0
    failed_runs = 0
    failure_classes: Counter[str] = Counter()
    failure_examples: list[dict[str, Any]] = []
    publish_jobs = 0
    publish_jobs_skipped = 0
    workflow_publish_jobs = 0
    workflow_publish_jobs_skipped = 0
    jobs_with_metrics = 0
    workflow_jobs_with_metrics = 0
    pull_request_jobs_with_metrics = 0
    job_log_errors = 0
    job_logs_skipped = 0
    job_logs_without_metrics = 0
    api_calls = 0
    workflow_api_calls = 0
    pull_request_api_calls = 0
    pull_request_publish_jobs = 0
    pull_request_publish_jobs_skipped = 0
    first_attempt_in_progress_runs = 0
    first_attempt_in_progress_publish_jobs = 0
    first_attempt_in_progress_publish_jobs_skipped = 0
    first_attempt_in_progress_jobs_with_metrics = 0
    first_attempt_in_progress_api_calls = 0
    source_workflows: dict[int, dict[str, Any]] = {}
    attempt_run_counts: Counter[tuple[int, int, int]] = Counter()
    source_attempt_stats: dict[tuple[int, int, int], dict[str, Any]] = {}
    log_error_examples: list[dict[str, Any]] = []

    for run in runs:
        title = run.get("display_title")
        if not isinstance(title, str):
            legacy_runs += 1
            continue
        try:
            identity = parse_run_name(title)
        except MetricsError:
            malformed_runs += 1
            continue
        if identity is None:
            legacy_runs += 1
            continue
        instrumented_runs += 1
        trigger_counts[identity.trigger] += 1
        lifecycle_counts[identity.delivery] += 1
        is_first_attempt_in_progress = (
            identity.trigger == "workflow_run"
            and identity.delivery == "in_progress"
            and identity.source_attempt == 1
        )
        if is_first_attempt_in_progress:
            first_attempt_in_progress_runs += 1
        source_key = identity.source_attempt_key
        if source_key is not None:
            source_attempts.add(source_key)
            attempt_run_counts[source_key] += 1
            attempt_stats = source_attempt_stats.setdefault(
                source_key,
                {
                    "api_calls": 0,
                    "deliveries": Counter(),
                    "publish_jobs": 0,
                    "publish_jobs_skipped": 0,
                    "publish_jobs_with_metrics": 0,
                },
            )
            attempt_stats["deliveries"][identity.delivery] += 1
            workflow_stats = source_workflows.setdefault(
                identity.source_workflow,
                {
                    "api_calls": 0,
                    "attempts": set(),
                    "deliveries": Counter(),
                    "publish_jobs": 0,
                    "publish_jobs_skipped": 0,
                    "publish_jobs_with_metrics": 0,
                    "publisher_runs": 0,
                },
            )
            workflow_stats["attempts"].add(source_key)
            workflow_stats["deliveries"][identity.delivery] += 1
            workflow_stats["publisher_runs"] += 1
        run_conclusion = run.get("conclusion")
        run_failed = run_conclusion not in {None, "", "success", "skipped", "neutral"}
        if run_failed:
            failed_runs += 1
        jobs = run.get("jobs", [])
        if not isinstance(jobs, Sequence) or isinstance(jobs, (str, bytes)):
            continue
        run_publish_jobs = 0
        run_publish_jobs_skipped = 0
        run_publish_jobs_with_metrics = 0
        for job in jobs:
            if not isinstance(job, Mapping) or not str(job.get("name", "")).startswith(
                PUBLISH_JOB_PREFIX
            ):
                continue
            publish_jobs += 1
            run_publish_jobs += 1
            job_was_skipped = job.get("conclusion") == "skipped"
            if job_was_skipped:
                publish_jobs_skipped += 1
                run_publish_jobs_skipped += 1
            if is_first_attempt_in_progress:
                first_attempt_in_progress_publish_jobs += 1
                if job_was_skipped:
                    first_attempt_in_progress_publish_jobs_skipped += 1
            if identity.trigger == "workflow_run":
                workflow_publish_jobs += 1
                source_workflows[identity.source_workflow]["publish_jobs"] += 1
                source_attempt_stats[source_key]["publish_jobs"] += 1
                if job_was_skipped:
                    workflow_publish_jobs_skipped += 1
                    source_workflows[identity.source_workflow]["publish_jobs_skipped"] += 1
                    source_attempt_stats[source_key]["publish_jobs_skipped"] += 1
            else:
                pull_request_publish_jobs += 1
                if job_was_skipped:
                    pull_request_publish_jobs_skipped += 1
            log = job.get("log")
            metrics = extract_job_metrics(log) if isinstance(log, str) else None
            if metrics is not None:
                run_publish_jobs_with_metrics += 1
                jobs_with_metrics += 1
                job_api_calls = int(metrics["api_calls"])
                api_calls += job_api_calls
                if is_first_attempt_in_progress:
                    first_attempt_in_progress_jobs_with_metrics += 1
                    first_attempt_in_progress_api_calls += job_api_calls
                if identity.trigger == "workflow_run":
                    workflow_jobs_with_metrics += 1
                    workflow_api_calls += job_api_calls
                    source_workflows[identity.source_workflow]["api_calls"] += job_api_calls
                    source_workflows[identity.source_workflow]["publish_jobs_with_metrics"] += 1
                    source_attempt_stats[source_key]["api_calls"] += job_api_calls
                    source_attempt_stats[source_key]["publish_jobs_with_metrics"] += 1
                else:
                    pull_request_jobs_with_metrics += 1
                    pull_request_api_calls += job_api_calls
            elif job_was_skipped:
                pass
            elif job.get("log_skipped") is True:
                job_logs_skipped += 1
            elif "log_error" in job:
                job_log_errors += 1
                if len(log_error_examples) < 5:
                    log_error_examples.append(
                        {
                            "error": str(job["log_error"]),
                            "job_id": job.get("id"),
                            "run_id": run.get("id"),
                        }
                    )
            else:
                job_logs_without_metrics += 1

        if run_failed:
            if run_conclusion == "cancelled":
                failure_class = "publisher_cancelled"
            elif (
                run_publish_jobs > 0
                and run_publish_jobs_with_metrics
                == run_publish_jobs - run_publish_jobs_skipped
            ):
                failure_class = "authoritative_result"
            else:
                failure_class = "publisher_transport_failure"
            failure_classes[failure_class] += 1
            if len(failure_examples) < 10:
                failure_examples.append(
                    {
                        "class": failure_class,
                        "conclusion": run_conclusion,
                        "delivery": identity.delivery,
                        "run_id": run.get("id"),
                        "source_attempt": identity.source_attempt,
                        "source_run": identity.source_run,
                        "source_workflow": identity.source_workflow,
                    }
                )

    workflow_run_count = trigger_counts["workflow_run"]
    distinct_attempts = len(source_attempts)
    terminal_attempt_keys = {
        key
        for key, stats in source_attempt_stats.items()
        if stats["deliveries"]["completed"] > 0
    }
    terminal_publish_jobs = sum(
        source_attempt_stats[key]["publish_jobs"] for key in terminal_attempt_keys
    )
    terminal_publish_jobs_skipped = sum(
        source_attempt_stats[key]["publish_jobs_skipped"] for key in terminal_attempt_keys
    )
    terminal_publish_jobs_with_metrics = sum(
        source_attempt_stats[key]["publish_jobs_with_metrics"] for key in terminal_attempt_keys
    )
    terminal_api_calls = sum(
        source_attempt_stats[key]["api_calls"] for key in terminal_attempt_keys
    )
    terminal_publisher_runs = sum(attempt_run_counts[key] for key in terminal_attempt_keys)
    terminal_with_invalidation = {
        key
        for key in terminal_attempt_keys
        if source_attempt_stats[key]["deliveries"]["requested"] > 0
        or source_attempt_stats[key]["deliveries"]["in_progress"] > 0
    }
    terminal_with_complete_metrics = {
        key
        for key in terminal_attempt_keys
        if source_attempt_stats[key]["publish_jobs_with_metrics"]
        + source_attempt_stats[key]["publish_jobs_skipped"]
        == source_attempt_stats[key]["publish_jobs"]
    }
    incomplete_terminal_keys = sorted(
        (terminal_attempt_keys - terminal_with_invalidation)
        | (terminal_attempt_keys - terminal_with_complete_metrics)
    )
    terminal_attempt_count = len(terminal_attempt_keys)
    source_workflow_summary: dict[str, Any] = {}
    for workflow_id, stats in sorted(source_workflows.items()):
        attempts = len(stats["attempts"])
        executed_publish_jobs = stats["publish_jobs"] - stats["publish_jobs_skipped"]
        api_calls_complete = stats["publish_jobs_with_metrics"] == executed_publish_jobs
        source_workflow_summary[str(workflow_id)] = {
            "api_calls": stats["api_calls"],
            "api_calls_complete": api_calls_complete,
            "attempts": attempts,
            "deliveries": dict(sorted(stats["deliveries"].items())),
            "name": SOURCE_WORKFLOW_NAMES.get(workflow_id, "unknown"),
            "per_attempt": {
                "api_calls": _ratio(stats["api_calls"], attempts)
                if api_calls_complete
                else None,
                "executed_publish_jobs": _ratio(executed_publish_jobs, attempts),
                "publish_jobs": _ratio(stats["publish_jobs"], attempts),
                "publisher_runs": _ratio(stats["publisher_runs"], attempts),
            },
            "executed_publish_jobs": executed_publish_jobs,
            "publish_jobs": stats["publish_jobs"],
            "publisher_runs": stats["publisher_runs"],
        }
    run_count_distribution = Counter(attempt_run_counts.values())
    return {
        "coverage": {
            "instrumented_runs": instrumented_runs,
            "publish_job_log_error_examples": log_error_examples,
            "publish_job_log_errors": job_log_errors,
            "publish_job_logs_skipped": job_logs_skipped,
            "publish_job_logs_without_metrics": job_logs_without_metrics,
            "legacy_runs": legacy_runs,
            "malformed_runs": malformed_runs,
            "publish_jobs_with_metrics": jobs_with_metrics,
            "publish_jobs_skipped": publish_jobs_skipped,
            "publish_jobs_without_metrics": (
                publish_jobs - jobs_with_metrics - publish_jobs_skipped
            ),
        },
        "failures": {
            "by_class": dict(sorted(failure_classes.items())),
            "examples": failure_examples,
            "publisher_runs": failed_runs,
        },
        "first_attempt_in_progress": {
            "api_calls": first_attempt_in_progress_api_calls,
            "api_calls_complete": (
                first_attempt_in_progress_jobs_with_metrics
                == first_attempt_in_progress_publish_jobs
                - first_attempt_in_progress_publish_jobs_skipped
            ),
            "executed_publish_jobs": (
                first_attempt_in_progress_publish_jobs
                - first_attempt_in_progress_publish_jobs_skipped
            ),
            "publish_jobs": first_attempt_in_progress_publish_jobs,
            "publisher_runs": first_attempt_in_progress_runs,
        },
        "lifecycle_deliveries": dict(sorted(lifecycle_counts.items())),
        "publisher_runs_per_source_attempt_distribution": {
            str(count): occurrences for count, occurrences in sorted(run_count_distribution.items())
        },
        "source_workflows": source_workflow_summary,
        "source_attempt_coverage": {
            "active_or_incomplete": distinct_attempts - terminal_attempt_count,
            "incomplete_terminal_examples": [
                {
                    "source_attempt": key[2],
                    "source_run": key[1],
                    "source_workflow": key[0],
                }
                for key in incomplete_terminal_keys[:10]
            ],
            "observed": distinct_attempts,
            "terminal": terminal_attempt_count,
            "terminal_with_complete_publish_metrics": len(terminal_with_complete_metrics),
            "terminal_with_invalidation": len(terminal_with_invalidation),
            "terminal_without_complete_publish_metrics": len(
                terminal_attempt_keys - terminal_with_complete_metrics
            ),
            "terminal_without_invalidation": len(
                terminal_attempt_keys - terminal_with_invalidation
            ),
        },
        "terminal_per_source_attempt": {
            "api_calls": _ratio(terminal_api_calls, terminal_attempt_count)
            if terminal_publish_jobs_with_metrics
            == terminal_publish_jobs - terminal_publish_jobs_skipped
            else None,
            "executed_publish_jobs": _ratio(
                terminal_publish_jobs - terminal_publish_jobs_skipped,
                terminal_attempt_count,
            ),
            "publish_jobs": _ratio(terminal_publish_jobs, terminal_attempt_count),
            "publisher_runs": _ratio(terminal_publisher_runs, terminal_attempt_count),
        },
        "totals": {
            "api_calls": api_calls,
            "api_calls_complete": jobs_with_metrics + publish_jobs_skipped == publish_jobs,
            "distinct_source_attempts": distinct_attempts,
            "publish_jobs": publish_jobs,
            "publisher_runs": len(runs),
            "trigger_counts": dict(sorted(trigger_counts.items())),
        },
        "per_source_attempt": {
            "api_calls": _ratio(workflow_api_calls, distinct_attempts)
            if workflow_jobs_with_metrics
            == workflow_publish_jobs - workflow_publish_jobs_skipped
            else None,
            "executed_publish_jobs": _ratio(
                workflow_publish_jobs - workflow_publish_jobs_skipped,
                distinct_attempts,
            ),
            "publish_jobs": _ratio(workflow_publish_jobs, distinct_attempts),
            "publisher_runs": _ratio(workflow_run_count, distinct_attempts),
        },
        "pull_request_target": {
            "api_calls": pull_request_api_calls,
            "api_calls_complete": (
                pull_request_jobs_with_metrics + pull_request_publish_jobs_skipped
                == pull_request_publish_jobs
            ),
            "executed_publish_jobs": (
                pull_request_publish_jobs - pull_request_publish_jobs_skipped
            ),
            "per_run": {
                "api_calls": _ratio(pull_request_api_calls, trigger_counts["pull_request_target"])
                if (
                    pull_request_jobs_with_metrics + pull_request_publish_jobs_skipped
                    == pull_request_publish_jobs
                )
                else None,
                "executed_publish_jobs": _ratio(
                    pull_request_publish_jobs - pull_request_publish_jobs_skipped,
                    trigger_counts["pull_request_target"],
                ),
                "publish_jobs": _ratio(
                    pull_request_publish_jobs,
                    trigger_counts["pull_request_target"],
                ),
            },
            "publish_jobs": pull_request_publish_jobs,
            "publisher_runs": trigger_counts["pull_request_target"],
        },
    }


def parse_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("timestamp must include a timezone")
    return parsed.astimezone(UTC)


class GitHubMetricsClient:
    def __init__(self, *, token: str, repository: str, api_url: str) -> None:
        if not token:
            raise MetricsError("GITHUB_TOKEN is required")
        owner_repo = repository.split("/")
        if len(owner_repo) != 2 or not all(owner_repo):
            raise MetricsError("repository must be owner/name")
        self._token = token
        self._repo_path = "/".join(quote(part, safe="") for part in owner_repo)
        self._api_url = api_url.rstrip("/")
        self._thread_state = local()

    def _opener(self) -> Any:
        opener = getattr(self._thread_state, "opener", None)
        if opener is None:
            opener = build_opener(SafeRedirectHandler())
            self._thread_state.opener = opener
        return opener

    def _request(self, path: str, *, query: Mapping[str, str | int] | None = None) -> bytes:
        url = f"{self._api_url}{path}"
        if query:
            url = f"{url}?{urlencode(query)}"
        request = Request(
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self._token}",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "loom-authoritative-gate-metrics",
            },
        )
        try:
            with self._opener().open(request, timeout=30) as response:
                return response.read()
        except HTTPError as exc:
            raise MetricsError(f"GitHub API GET {path} returned HTTP {exc.code}") from exc
        except (URLError, TimeoutError) as exc:
            raise MetricsError(f"GitHub API GET {path} failed") from exc

    def _json(self, path: str, *, query: Mapping[str, str | int] | None = None) -> Any:
        raw = self._request(path, query=query)
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise MetricsError(f"GitHub API GET {path} returned invalid JSON") from exc

    def collect_runs(
        self,
        *,
        workflow_id: int,
        since: datetime,
        until: datetime,
        max_runs: int,
        workers: int,
        include_logs: bool,
    ) -> list[Mapping[str, Any]]:
        collected: list[dict[str, Any]] = []
        page = 1
        while len(collected) < max_runs:
            response = self._json(
                f"/repos/{self._repo_path}/actions/workflows/{workflow_id}/runs",
                query={"per_page": 100, "page": page, "created": f">={since.isoformat()}"},
            )
            batch = response.get("workflow_runs", []) if isinstance(response, Mapping) else []
            if not isinstance(batch, list):
                raise MetricsError("GitHub workflow-runs response is invalid")
            for run in batch:
                if not isinstance(run, Mapping):
                    raise MetricsError("GitHub workflow-runs response is invalid")
                created_at = run.get("created_at")
                if not isinstance(created_at, str):
                    continue
                created = parse_timestamp(created_at)
                if not since <= created <= until:
                    continue
                record = dict(run)
                collected.append(record)
                if len(collected) >= max_runs:
                    break
            if len(batch) < 100:
                break
            page += 1
        instrumented = [
            (index, run)
            for index, run in enumerate(collected)
            if isinstance(run.get("display_title"), str)
            and str(run["display_title"]).startswith(f"{RUN_NAME_VERSION} ")
        ]
        if workers == 1:
            for index, run in instrumented:
                collected[index]["jobs"] = self._collect_jobs(run, include_logs=include_logs)
        else:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(self._collect_jobs, run, include_logs=include_logs): index
                    for index, run in instrumented
                }
                for future in as_completed(futures):
                    collected[futures[future]]["jobs"] = future.result()
        for run in collected:
            run.setdefault("jobs", [])
        return collected

    def collect_run_jobs(
        self,
        *,
        run_id: int,
        attempt: int,
    ) -> list[Mapping[str, Any]]:
        """Collect one exact workflow attempt without fetching job logs."""

        return self._collect_jobs(
            {"id": run_id, "run_attempt": attempt},
            include_logs=False,
        )

    def collect_run_attempt(
        self,
        *,
        run_id: int,
        attempt: int,
    ) -> Mapping[str, Any]:
        """Collect metadata and jobs for one exact workflow attempt."""

        response = self._json(f"/repos/{self._repo_path}/actions/runs/{run_id}")
        if not isinstance(response, Mapping) or response.get("id") != run_id:
            raise MetricsError("GitHub workflow run response is invalid")
        latest_attempt = response.get("run_attempt")
        if type(latest_attempt) is not int or not 1 <= attempt <= latest_attempt:
            raise MetricsError("GitHub workflow run attempt is invalid")
        record = dict(response)
        record["requested_attempt"] = attempt
        record["jobs"] = self.collect_run_jobs(run_id=run_id, attempt=attempt)
        return record

    def _collect_jobs(
        self,
        run: Mapping[str, Any],
        *,
        include_logs: bool,
    ) -> list[Mapping[str, Any]]:
        run_id = run.get("id")
        attempt = run.get("run_attempt")
        if not isinstance(run_id, int) or not isinstance(attempt, int):
            raise MetricsError("GitHub workflow run identity is invalid")
        response = self._json(
            f"/repos/{self._repo_path}/actions/runs/{run_id}/attempts/{attempt}/jobs",
            query={"per_page": 100},
        )
        jobs = response.get("jobs", []) if isinstance(response, Mapping) else []
        if not isinstance(jobs, list):
            raise MetricsError("GitHub jobs response is invalid")
        collected: list[Mapping[str, Any]] = []
        for job in jobs:
            if not isinstance(job, Mapping):
                raise MetricsError("GitHub jobs response is invalid")
            record = dict(job)
            if str(job.get("name", "")).startswith(PUBLISH_JOB_PREFIX):
                if job.get("conclusion") == "skipped":
                    record["log_unavailable_reason"] = "skipped_job"
                    collected.append(record)
                    continue
                if not include_logs:
                    record["log_skipped"] = True
                    collected.append(record)
                    continue
                job_id = job.get("id")
                if not isinstance(job_id, int):
                    raise MetricsError("GitHub job identity is invalid")
                try:
                    record["log"] = self._request(
                        f"/repos/{self._repo_path}/actions/jobs/{job_id}/logs"
                    ).decode("utf-8", errors="replace")
                except MetricsError as exc:
                    record["log_error"] = str(exc)
            collected.append(record)
        return collected


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--since", required=True, type=parse_timestamp)
    parser.add_argument("--until", type=parse_timestamp, default=datetime.now(UTC))
    parser.add_argument("--workflow-id", type=int, default=DEFAULT_PUBLISHER_WORKFLOW_ID)
    parser.add_argument("--max-runs", type=int, default=200)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--baseline-api-calls-per-attempt", type=float)
    parser.add_argument("--baseline-executed-publish-jobs-per-attempt", type=float)
    parser.add_argument("--baseline-publisher-runs-per-attempt", type=float)
    parser.add_argument("--minimum-reduction-percent", type=float, default=40.0)
    parser.add_argument("--minimum-terminal-source-attempts", type=int, default=30)
    parser.add_argument("--require-acceptance", action="store_true")
    parser.add_argument(
        "--skip-logs",
        action="store_true",
        help="collect run/job amplification without downloading API-call records from job logs",
    )
    parser.add_argument("--api-url", default=os.environ.get("GITHUB_API_URL", "https://api.github.com"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.max_runs < 1:
        print("authoritative gate metrics failed: --max-runs must be positive", file=sys.stderr)
        return 2
    if not 1 <= args.workers <= MAX_WORKERS:
        print(
            f"authoritative gate metrics failed: --workers must be between 1 and {MAX_WORKERS}",
            file=sys.stderr,
        )
        return 2
    if args.until < args.since:
        print("authoritative gate metrics failed: --until precedes --since", file=sys.stderr)
        return 2
    acceptance_baselines = (
        args.baseline_api_calls_per_attempt,
        args.baseline_executed_publish_jobs_per_attempt,
    )
    acceptance_requested = any(value is not None for value in acceptance_baselines) or (
        args.baseline_publisher_runs_per_attempt is not None
    )
    if acceptance_requested and not all(
        value is not None and value > 0 for value in acceptance_baselines
    ):
        print(
            "authoritative gate metrics failed: both positive acceptance baselines are required",
            file=sys.stderr,
        )
        return 2
    if args.baseline_publisher_runs_per_attempt is not None and (
        args.baseline_publisher_runs_per_attempt <= 0
    ):
        print(
            "authoritative gate metrics failed: publisher-runs baseline must be positive",
            file=sys.stderr,
        )
        return 2
    if not 0 <= args.minimum_reduction_percent <= 100:
        print(
            "authoritative gate metrics failed: minimum reduction must be between 0 and 100",
            file=sys.stderr,
        )
        return 2
    if args.minimum_terminal_source_attempts < 1:
        print(
            "authoritative gate metrics failed: minimum terminal source attempts must be positive",
            file=sys.stderr,
        )
        return 2
    if args.require_acceptance and not all(value is not None for value in acceptance_baselines):
        print(
            "authoritative gate metrics failed: --require-acceptance needs both baselines",
            file=sys.stderr,
        )
        return 2
    try:
        client = GitHubMetricsClient(
            token=os.environ.get("GITHUB_TOKEN", ""),
            repository=args.repository,
            api_url=args.api_url,
        )
        runs = client.collect_runs(
            workflow_id=args.workflow_id,
            since=args.since,
            until=args.until,
            max_runs=args.max_runs,
            workers=args.workers,
            include_logs=not args.skip_logs,
        )
    except MetricsError as exc:
        print(f"authoritative gate metrics failed: {exc}", file=sys.stderr)
        return 1
    summary = summarize_runs(runs)
    observed = [
        created_at
        for run in runs
        if isinstance((created_at := run.get("created_at")), str)
    ]
    summary["sample"] = {
        "max_runs": args.max_runs,
        "observed_created_at_max": max(observed) if observed else None,
        "observed_created_at_min": min(observed) if observed else None,
        "runs_collected": len(runs),
        "since": args.since.isoformat().replace("+00:00", "Z"),
        "truncated": len(runs) == args.max_runs,
        "until": args.until.isoformat().replace("+00:00", "Z"),
        "workers": args.workers,
    }
    if acceptance_requested:
        summary["track2_acceptance"] = evaluate_track2_acceptance(
            summary,
            baseline_api_calls_per_attempt=args.baseline_api_calls_per_attempt,
            baseline_executed_publish_jobs_per_attempt=(
                args.baseline_executed_publish_jobs_per_attempt
            ),
            baseline_publisher_runs_per_attempt=args.baseline_publisher_runs_per_attempt,
            minimum_reduction_percent=args.minimum_reduction_percent,
            minimum_terminal_source_attempts=args.minimum_terminal_source_attempts,
        )
    print(json.dumps(summary, indent=2, sort_keys=True))
    if args.require_acceptance and summary["track2_acceptance"]["status"] != "pass":
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
