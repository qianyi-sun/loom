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
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

DEFAULT_PUBLISHER_WORKFLOW_ID = 318631340
RUN_NAME_VERSION = "publisher-metrics-v1"
PUBLISH_JOB_PREFIX = "publish authoritative gate ("
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


def summarize_runs(runs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Summarize publisher runs plus attached publish-job logs."""

    trigger_counts: Counter[str] = Counter()
    lifecycle_counts: Counter[str] = Counter()
    source_attempts: set[tuple[int, int, int]] = set()
    instrumented_runs = 0
    malformed_runs = 0
    legacy_runs = 0
    failed_runs = 0
    publish_jobs = 0
    workflow_publish_jobs = 0
    jobs_with_metrics = 0
    api_calls = 0
    workflow_api_calls = 0

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
        source_key = identity.source_attempt_key
        if source_key is not None:
            source_attempts.add(source_key)
        if run.get("conclusion") not in {None, "", "success", "skipped", "neutral"}:
            failed_runs += 1
        jobs = run.get("jobs", [])
        if not isinstance(jobs, Sequence) or isinstance(jobs, (str, bytes)):
            continue
        for job in jobs:
            if not isinstance(job, Mapping) or not str(job.get("name", "")).startswith(
                PUBLISH_JOB_PREFIX
            ):
                continue
            publish_jobs += 1
            if identity.trigger == "workflow_run":
                workflow_publish_jobs += 1
            log = job.get("log")
            metrics = extract_job_metrics(log) if isinstance(log, str) else None
            if metrics is not None:
                jobs_with_metrics += 1
                job_api_calls = int(metrics["api_calls"])
                api_calls += job_api_calls
                if identity.trigger == "workflow_run":
                    workflow_api_calls += job_api_calls

    workflow_run_count = trigger_counts["workflow_run"]
    distinct_attempts = len(source_attempts)
    return {
        "coverage": {
            "instrumented_runs": instrumented_runs,
            "legacy_runs": legacy_runs,
            "malformed_runs": malformed_runs,
            "publish_jobs_with_metrics": jobs_with_metrics,
            "publish_jobs_without_metrics": publish_jobs - jobs_with_metrics,
        },
        "failures": {"publisher_runs": failed_runs},
        "lifecycle_deliveries": dict(sorted(lifecycle_counts.items())),
        "totals": {
            "api_calls": api_calls,
            "distinct_source_attempts": distinct_attempts,
            "publish_jobs": publish_jobs,
            "publisher_runs": len(runs),
            "trigger_counts": dict(sorted(trigger_counts.items())),
        },
        "per_source_attempt": {
            "api_calls": _ratio(workflow_api_calls, distinct_attempts),
            "publish_jobs": _ratio(workflow_publish_jobs, distinct_attempts),
            "publisher_runs": _ratio(workflow_run_count, distinct_attempts),
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
        self._opener = build_opener(SafeRedirectHandler())

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
            with self._opener.open(request, timeout=30) as response:
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
    ) -> list[Mapping[str, Any]]:
        collected: list[Mapping[str, Any]] = []
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
                title = run.get("display_title")
                record["jobs"] = (
                    self._collect_jobs(run)
                    if isinstance(title, str) and title.startswith(f"{RUN_NAME_VERSION} ")
                    else []
                )
                collected.append(record)
                if len(collected) >= max_runs:
                    break
            if len(batch) < 100:
                break
            page += 1
        return collected

    def _collect_jobs(self, run: Mapping[str, Any]) -> list[Mapping[str, Any]]:
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
    parser.add_argument("--api-url", default=os.environ.get("GITHUB_API_URL", "https://api.github.com"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.max_runs < 1:
        print("authoritative gate metrics failed: --max-runs must be positive", file=sys.stderr)
        return 2
    if args.until < args.since:
        print("authoritative gate metrics failed: --until precedes --since", file=sys.stderr)
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
        )
    except MetricsError as exc:
        print(f"authoritative gate metrics failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(summarize_runs(runs), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
