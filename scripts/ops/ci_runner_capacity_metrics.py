#!/usr/bin/env python3
"""Measure #1130 Track 3 queue and execution data by isolated work class."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.ops.authoritative_gate_metrics import (  # noqa: E402
    SOURCE_WORKFLOW_NAMES,
    GitHubMetricsClient,
    MetricsError,
    parse_timestamp,
)

OLDLAB_RUNNER_PREFIX = "oldlab5-kvm-"
WORKFLOW_IDS = {name: workflow_id for workflow_id, name in SOURCE_WORKFLOW_NAMES.items()}
WORKFLOW_SPECS = {
    "CI": ("normal", 300, WORKFLOW_IDS["CI"]),
    "images": ("image", 900, WORKFLOW_IDS["images"]),
    "cluster-smoke": ("smoke", 300, WORKFLOW_IDS["cluster-smoke"]),
    "staging-smoke": ("smoke", 300, WORKFLOW_IDS["staging-smoke"]),
}
WORK_CLASS_THRESHOLDS = {"normal": 300, "image": 900, "smoke": 300}
RUN_SPEC_RE = re.compile(
    r"^(CI|images|cluster-smoke|staging-smoke):([1-9][0-9]*)(?::([1-9][0-9]*))?$",
)


@dataclass(frozen=True, slots=True)
class RunSpec:
    workflow: str
    run_id: int
    attempt: int = 1

    @property
    def work_class(self) -> str:
        return WORKFLOW_SPECS[self.workflow][0]


def parse_run_spec(value: str) -> RunSpec:
    match = RUN_SPEC_RE.fullmatch(value)
    if match is None:
        raise argparse.ArgumentTypeError(
            "run must be WORKFLOW:RUN_ID[:ATTEMPT] for a Track 3 workflow",
        )
    return RunSpec(
        workflow=match.group(1),
        run_id=int(match.group(2)),
        attempt=int(match.group(3) or "1"),
    )


def _job_is_in_scope(workflow: str, name: str) -> bool:
    if workflow == "CI":
        return (
            name in {
                "lint-and-static",
                "tests-packages",
                "runtime-payload",
                "go-checks",
                "web-checks",
                "integration-docker",
            }
            or name.startswith("tests-root (")
            or name.startswith("integration (")
        )
    if workflow == "images":
        return name.endswith("(multi-arch)") and not name.startswith("publish ")
    if workflow == "cluster-smoke":
        return name.startswith("cluster contract (")
    if workflow == "staging-smoke":
        return name == "manifest-owned system smoke"
    raise MetricsError(f"unsupported workflow: {workflow}")


def _label_names(job: Mapping[str, Any]) -> set[str]:
    raw = job.get("labels")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return set()
    return {item for item in raw if isinstance(item, str)}


def _runner_class(job: Mapping[str, Any]) -> str:
    runner_name = job.get("runner_name")
    labels = _label_names(job)
    if isinstance(runner_name, str) and runner_name.startswith(OLDLAB_RUNNER_PREFIX):
        return "oldlab5"
    if "self-hosted" in labels:
        return "other_self_hosted"
    if isinstance(runner_name, str) and runner_name:
        return "github_hosted"
    return "unassigned"


def _duration_seconds(start: str, end: str, *, field: str) -> int:
    start_at = parse_timestamp(start)
    end_at = parse_timestamp(end)
    seconds = round((end_at - start_at).total_seconds())
    if seconds < 0:
        raise MetricsError(f"{field} duration is negative")
    return seconds


def _percentile(values: Sequence[int], percentile: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, int((len(ordered) - 1) * percentile))
    return ordered[index]


def _metric_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    queue = [int(row["queue_seconds"]) for row in rows]
    execution = [int(row["execution_seconds"]) for row in rows]
    return {
        "jobs": len(rows),
        "failures": sum(row["conclusion"] != "success" for row in rows),
        "queue_seconds": {
            "p50": _percentile(queue, 0.5),
            "p95": _percentile(queue, 0.95),
            "max": max(queue, default=None),
        },
        "execution_seconds": {
            "p50": _percentile(execution, 0.5),
            "p95": _percentile(execution, 0.95),
            "max": max(execution, default=None),
            "total_runner_active": sum(execution),
        },
    }


def summarize_runs(runs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    observed_runs: list[dict[str, Any]] = []
    for run in runs:
        workflow = run.get("workflow")
        run_id = run.get("run_id")
        attempt = run.get("attempt")
        event = run.get("event")
        head_sha = run.get("head_sha")
        jobs = run.get("jobs")
        if workflow not in WORKFLOW_SPECS:
            raise MetricsError("run workflow is outside the Track 3 contract")
        if type(run_id) is not int or type(attempt) is not int:
            raise MetricsError("run identity is invalid")
        if event != "pull_request":
            raise MetricsError("Track 3 evidence requires a pull_request run")
        if not isinstance(head_sha, str) or re.fullmatch(r"[0-9a-f]{40}", head_sha) is None:
            raise MetricsError("run head SHA is invalid")
        if not isinstance(jobs, Sequence) or isinstance(jobs, (str, bytes)):
            raise MetricsError("run jobs are invalid")
        observed_runs.append(
            {
                "workflow": workflow,
                "run_id": run_id,
                "attempt": attempt,
                "event": event,
                "head_sha": head_sha,
            },
        )
        work_class = WORKFLOW_SPECS[workflow][0]
        for job in jobs:
            if not isinstance(job, Mapping):
                raise MetricsError("job record is invalid")
            name = job.get("name")
            if not isinstance(name, str) or not _job_is_in_scope(workflow, name):
                continue
            if job.get("conclusion") == "skipped":
                continue
            conclusion = job.get("conclusion")
            created_at = job.get("created_at")
            started_at = job.get("started_at")
            completed_at = job.get("completed_at")
            if not all(
                isinstance(value, str) and value
                for value in (conclusion, created_at, started_at, completed_at)
            ):
                raise MetricsError(f"job is not terminal: {workflow}/{name}")
            rows.append(
                {
                    "workflow": workflow,
                    "work_class": work_class,
                    "runner_class": _runner_class(job),
                    "run_id": run_id,
                    "attempt": attempt,
                    "name": name,
                    "conclusion": conclusion,
                    "queue_seconds": _duration_seconds(
                        created_at,
                        started_at,
                        field="queue",
                    ),
                    "execution_seconds": _duration_seconds(
                        started_at,
                        completed_at,
                        field="execution",
                    ),
                },
            )

    head_shas = {str(run["head_sha"]) for run in observed_runs}
    if len(head_shas) > 1:
        raise MetricsError("all runs in one Track 3 report must use the same head")

    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    by_work_class: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["work_class"]), str(row["runner_class"]))].append(row)
        by_work_class[str(row["work_class"])].append(row)

    class_summary: dict[str, Any] = {}
    for work_class, threshold in WORK_CLASS_THRESHOLDS.items():
        class_rows = by_work_class.get(work_class, [])
        metrics = _metric_summary(class_rows)
        metrics["hosted_overflow_after_seconds"] = threshold
        metrics["queue_boundary_breaches"] = sum(
            int(row["queue_seconds"]) > threshold for row in class_rows
        )
        class_summary[work_class] = metrics

    return {
        "schema_version": 1,
        "head_sha": next(iter(head_shas), None),
        "observed_runs": observed_runs,
        "jobs": rows,
        "by_work_class": class_summary,
        "by_work_class_and_runner": {
            f"{work_class}/{runner_class}": _metric_summary(group_rows)
            for (work_class, runner_class), group_rows in sorted(grouped.items())
        },
    }


def evaluate_bounded_wait(summary: Mapping[str, Any]) -> dict[str, Any]:
    classes = summary.get("by_work_class")
    if not isinstance(classes, Mapping):
        raise MetricsError("summary is missing work-class metrics")
    criteria: dict[str, Any] = {}
    for work_class in ("normal", "image", "smoke"):
        metrics = classes.get(work_class)
        if not isinstance(metrics, Mapping):
            raise MetricsError(f"summary is missing {work_class} metrics")
        threshold = metrics.get("hosted_overflow_after_seconds")
        queue_metrics = metrics.get("queue_seconds")
        if not isinstance(queue_metrics, Mapping):
            raise MetricsError(f"summary is missing {work_class} queue metrics")
        p95 = queue_metrics.get("p95")
        maximum = queue_metrics.get("max")
        boundary_breaches = metrics.get("queue_boundary_breaches")
        jobs = metrics.get("jobs")
        failures = metrics.get("failures")
        criteria[work_class] = {
            "jobs": jobs,
            "queue_p95_seconds": p95,
            "queue_max_seconds": maximum,
            "queue_boundary_breaches": boundary_breaches,
            "required_max_seconds": threshold,
            "failures": failures,
            "passed": isinstance(jobs, int)
            and jobs > 0
            and isinstance(p95, int)
            and isinstance(maximum, int)
            and isinstance(boundary_breaches, int)
            and isinstance(threshold, int)
            and p95 <= threshold
            and maximum <= threshold
            and boundary_breaches == 0
            and failures == 0,
        }
    observed_runs = summary.get("observed_runs")
    observed_workflows = (
        {
            run.get("workflow")
            for run in observed_runs
            if isinstance(run, Mapping)
        }
        if isinstance(observed_runs, Sequence)
        else set()
    )
    workflow_coverage = {
        "actual": sorted(str(item) for item in observed_workflows),
        "required": sorted(WORKFLOW_SPECS),
        "passed": observed_workflows == set(WORKFLOW_SPECS),
    }
    return {
        "status": (
            "pass"
            if all(item["passed"] for item in criteria.values())
            and workflow_coverage["passed"]
            else "fail"
        ),
        "criteria": criteria,
        "workflow_coverage": workflow_coverage,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--run", dest="runs", action="append", type=parse_run_spec)
    parser.add_argument("--require-bounded-wait", action="store_true")
    parser.add_argument(
        "--api-url",
        default=os.environ.get("GITHUB_API_URL", "https://api.github.com"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if not args.runs:
        print("error: at least one --run is required", file=sys.stderr)
        return 2
    try:
        client = GitHubMetricsClient(
            token=os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN", ""),
            repository=args.repository,
            api_url=args.api_url,
        )
        records = []
        for spec in args.runs:
            run = client.collect_run_attempt(
                run_id=spec.run_id,
                attempt=spec.attempt,
            )
            expected_workflow_id = WORKFLOW_SPECS[spec.workflow][2]
            if run.get("workflow_id") != expected_workflow_id:
                raise MetricsError(
                    f"run {spec.run_id} is not workflow {spec.workflow}",
                )
            records.append(
                {
                    "workflow": spec.workflow,
                    "run_id": spec.run_id,
                    "attempt": spec.attempt,
                    "event": run.get("event"),
                    "head_sha": run.get("head_sha"),
                    "jobs": run.get("jobs"),
                },
            )
        summary = summarize_runs(records)
        acceptance = evaluate_bounded_wait(summary)
    except (MetricsError, argparse.ArgumentTypeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    summary["bounded_wait_acceptance"] = acceptance
    summary["generated_at"] = datetime.now(UTC).isoformat()
    print(json.dumps(summary, sort_keys=True, indent=2))
    return 3 if args.require_bounded_wait and acceptance["status"] != "pass" else 0


if __name__ == "__main__":
    raise SystemExit(main())
