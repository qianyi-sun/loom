#!/usr/bin/env python3
"""Summarize governed retries, flakes, queueing, and terminal causes."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from scripts.ops.github_actions_metrics import MetricsError, parse_timestamp

WORKFLOWS = {"CI", "images", "cluster-smoke", "staging-smoke"}
CAUSES = {"platform_transient", "external_dependency", "code_failure", "capacity_queue"}
RETRYABLE = CAUSES - {"code_failure"}


def _ratio(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 4) if denominator else None


def _runner_class(job: Mapping[str, Any]) -> str:
    name = job.get("runner_name")
    labels = job.get("labels")
    if isinstance(name, str) and name.startswith("oldlab5-kvm-"):
        return "oldlab5"
    if isinstance(labels, list) and "self-hosted" in labels:
        return "other_self_hosted"
    if isinstance(name, str) and name:
        return "github_hosted"
    return "unassigned"


def _queue_seconds(job: Mapping[str, Any]) -> int | None:
    created = job.get("created_at")
    started = job.get("started_at")
    if not isinstance(created, str) or not isinstance(started, str):
        return None
    seconds = round((parse_timestamp(started) - parse_timestamp(created)).total_seconds())
    if seconds < 0:
        raise MetricsError("job queue duration is negative")
    return seconds


def _attempt_conclusion(jobs: Sequence[Mapping[str, Any]]) -> str:
    conclusions = {job.get("conclusion") for job in jobs if job.get("conclusion") != "skipped"}
    if not conclusions:
        return "cancelled"
    for conclusion in ("failure", "timed_out", "cancelled", "action_required", "stale"):
        if conclusion in conclusions:
            return conclusion
    return "success" if conclusions == {"success"} else "failure"


def _validate_evidence(repository: str, value: Any) -> bool:
    if not isinstance(value, str):
        return False
    parsed = urlsplit(value)
    prefix = f"/{repository}/"
    return (
        parsed.scheme == "https"
        and parsed.netloc == "github.com"
        and parsed.path.startswith(prefix)
        and re.fullmatch(
            r"(?:actions/runs|issues|pull)/[1-9][0-9]*",
            parsed.path[len(prefix) :],
        )
        is not None
    )


def _group_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    queue = [int(row["queue_seconds"]) for row in rows if row["queue_seconds"] is not None]
    causes = Counter(str(row["terminal_cause"]) for row in rows)
    return {
        "jobs": len(rows),
        "queue_seconds": {
            "average": round(sum(queue) / len(queue), 2) if queue else None,
            "max": max(queue, default=None),
        },
        "terminal_causes": dict(sorted(causes.items())),
    }


def summarize_reliability(
    payload: Mapping[str, Any],
    *,
    require_governance: bool = False,
    minimum_runs: int = 1,
) -> dict[str, Any]:
    repository = payload.get("repository")
    attempts = payload.get("attempts")
    classifications = payload.get("classifications")
    if not isinstance(repository, str) or repository.count("/") != 1:
        raise MetricsError("repository must use owner/name form")
    if not isinstance(attempts, list) or not attempts:
        raise MetricsError("attempts must be a non-empty array")
    if not isinstance(classifications, list):
        raise MetricsError("classifications must be an array")

    classified: dict[tuple[int, int], str] = {}
    for item in classifications:
        if not isinstance(item, Mapping):
            raise MetricsError("classification record is invalid")
        key = (item.get("run_id"), item.get("failed_attempt"))
        reason = item.get("reason")
        if (
            type(key[0]) is not int
            or type(key[1]) is not int
            or reason not in CAUSES
            or not _validate_evidence(repository, item.get("evidence_url"))
            or key in classified
        ):
            raise MetricsError("classification must be unique, governed, and evidenced")
        classified[(int(key[0]), int(key[1]))] = str(reason)

    grouped_attempts: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    workflow_by_run: dict[int, str] = {}
    attempt_rows: list[dict[str, Any]] = []
    job_rows: list[dict[str, Any]] = []
    for item in attempts:
        if not isinstance(item, Mapping):
            raise MetricsError("attempt record is invalid")
        workflow = item.get("workflow")
        run_id = item.get("run_id")
        attempt = item.get("attempt")
        jobs = item.get("jobs")
        if (
            workflow not in WORKFLOWS
            or type(run_id) is not int
            or type(attempt) is not int
            or not isinstance(jobs, list)
        ):
            raise MetricsError("attempt identity is invalid")
        if run_id in workflow_by_run and workflow_by_run[run_id] != workflow:
            raise MetricsError("one run ID cannot belong to multiple workflows")
        workflow_by_run[run_id] = str(workflow)
        grouped_attempts[run_id].append(item)

    policy_violations: list[str] = []
    flakes = 0
    retries = 0
    retried_runs = 0
    for run_id, run_attempts in sorted(grouped_attempts.items()):
        ordered = sorted(run_attempts, key=lambda item: int(item["attempt"]))
        numbers = [int(item["attempt"]) for item in ordered]
        if numbers != list(range(1, max(numbers) + 1)):
            raise MetricsError(f"run {run_id} attempts must be contiguous from 1")
        retries += len(ordered) - 1
        retried_runs += int(len(ordered) > 1)
        had_retryable_failure = False
        for index, item in enumerate(ordered):
            jobs = item["jobs"]
            if any(not isinstance(job, Mapping) for job in jobs):
                raise MetricsError("job record is invalid")
            conclusion = _attempt_conclusion(jobs)
            key = (run_id, int(item["attempt"]))
            cause = "success" if conclusion == "success" else classified.get(key)
            if cause is None:
                raise MetricsError(f"run {run_id} attempt {key[1]} lacks terminal classification")
            if index < len(ordered) - 1:
                if conclusion == "success":
                    policy_violations.append(f"run {run_id} retried a successful attempt")
                elif cause == "code_failure":
                    policy_violations.append(f"run {run_id} retried deterministic code failure")
                elif cause in RETRYABLE:
                    had_retryable_failure = True
            attempt_rows.append(
                {
                    "workflow": item["workflow"],
                    "run_id": run_id,
                    "attempt": key[1],
                    "conclusion": conclusion,
                    "terminal_cause": cause,
                },
            )
            for job in jobs:
                if job.get("conclusion") == "skipped":
                    continue
                name = job.get("name")
                if not isinstance(name, str) or not name:
                    raise MetricsError("job name is invalid")
                job_rows.append(
                    {
                        "workflow": item["workflow"],
                        "job": name,
                        "runner_class": _runner_class(job),
                        "queue_seconds": _queue_seconds(job),
                        "terminal_cause": "success" if job.get("conclusion") == "success" else cause,
                    },
                )
        if had_retryable_failure and attempt_rows[-1]["conclusion"] == "success":
            flakes += 1

    by_workflow: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    by_job: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    by_runner: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in job_rows:
        by_workflow[str(row["workflow"])].append(row)
        by_job[f"{row['workflow']}/{row['job']}"].append(row)
        by_runner[str(row["runner_class"])].append(row)
    run_count = len(grouped_attempts)
    if run_count < minimum_runs:
        policy_violations.append(f"sample has {run_count} runs; minimum is {minimum_runs}")
    if require_governance and retries == 0:
        policy_violations.append("acceptance sample contains no governed retry")
    return {
        "schema_version": 1,
        "repository": repository,
        "runs": run_count,
        "attempts": len(attempt_rows),
        "retries": retries,
        "retried_runs": retried_runs,
        "retry_rate": _ratio(retried_runs, run_count),
        "retry_attempt_rate": _ratio(retries, len(attempt_rows)),
        "flakes": flakes,
        "flake_rate": _ratio(flakes, run_count),
        "terminal_causes": dict(
            sorted(Counter(row["terminal_cause"] for row in attempt_rows).items()),
        ),
        "by_workflow": {key: _group_summary(rows) for key, rows in sorted(by_workflow.items())},
        "by_job": {key: _group_summary(rows) for key, rows in sorted(by_job.items())},
        "by_runner_class": {
            key: _group_summary(rows) for key, rows in sorted(by_runner.items())
        },
        "policy_violations": policy_violations,
        "status": "pass" if not policy_violations else "fail",
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--minimum-runs", type=int, default=1)
    parser.add_argument("--require-governance", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        payload = json.loads(args.input.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise MetricsError("input root must be an object")
        summary = summarize_reliability(
            payload,
            require_governance=args.require_governance,
            minimum_runs=args.minimum_runs,
        )
    except (OSError, json.JSONDecodeError, MetricsError) as exc:
        print(f"CI reliability metrics failed: {exc}", file=sys.stderr)
        return 2
    summary["generated_at"] = datetime.now(UTC).isoformat()
    print(json.dumps(summary, sort_keys=True, indent=2))
    return 0 if summary["status"] == "pass" else 3


if __name__ == "__main__":
    raise SystemExit(main())
