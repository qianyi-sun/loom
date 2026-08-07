#!/usr/bin/env python3
"""Validate, record, and request a classified GitHub Actions retry."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen

SOURCE_WORKFLOWS = {
    302898379: "CI",
    302898384: "images",
    302898381: "cluster-smoke",
    302898388: "staging-smoke",
}
RETRYABLE_REASONS = {"platform_transient", "external_dependency", "capacity_queue"}
ALL_REASONS = RETRYABLE_REASONS | {"code_failure"}
FAILED_CONCLUSIONS = {
    "action_required",
    "cancelled",
    "failure",
    "stale",
    "startup_failure",
    "timed_out",
}


class RetryError(RuntimeError):
    """Raised when a retry is not safely classifiable."""


class RetryClient(Protocol):
    def get_run(self, run_id: int) -> Mapping[str, Any]: ...

    def request_retry(self, run_id: int, mode: str) -> None: ...


class GitHubRetryClient:
    def __init__(self, *, repository: str, token: str, api_url: str) -> None:
        parts = repository.split("/")
        if len(parts) != 2 or not all(parts):
            raise RetryError("repository must use owner/name form")
        if not token:
            raise RetryError("GITHUB_TOKEN is required")
        self._repo = "/".join(quote(part, safe="") for part in parts)
        self._token = token
        self._api_url = api_url.rstrip("/")

    def _request(self, path: str, *, method: str = "GET") -> bytes:
        request = Request(
            f"{self._api_url}{path}",
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self._token}",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "loom-ci-retry-governance",
            },
        )
        try:
            with urlopen(request, timeout=30) as response:
                return response.read()
        except HTTPError as exc:
            raise RetryError(f"GitHub API {method} {path} returned HTTP {exc.code}") from exc
        except (URLError, TimeoutError) as exc:
            raise RetryError(f"GitHub API {method} {path} failed") from exc

    def get_run(self, run_id: int) -> Mapping[str, Any]:
        try:
            payload = json.loads(self._request(f"/repos/{self._repo}/actions/runs/{run_id}"))
        except json.JSONDecodeError as exc:
            raise RetryError("GitHub returned invalid run JSON") from exc
        if not isinstance(payload, Mapping):
            raise RetryError("GitHub returned an invalid run record")
        return payload

    def request_retry(self, run_id: int, mode: str) -> None:
        suffix = "rerun-failed-jobs" if mode == "failed_jobs" else "rerun"
        self._request(f"/repos/{self._repo}/actions/runs/{run_id}/{suffix}", method="POST")


@dataclass(frozen=True)
class RetryRequest:
    repository: str
    run_id: int
    failed_attempt: int
    reason: str
    evidence_url: str
    mode: str
    actor: str


def _validate_evidence_url(repository: str, value: str) -> None:
    parsed = urlsplit(value)
    prefix = f"/{repository}/"
    if parsed.scheme != "https" or parsed.netloc != "github.com" or not parsed.path.startswith(prefix):
        raise RetryError("evidence URL must point to this GitHub repository")
    relative = parsed.path[len(prefix) :]
    if re.fullmatch(r"(?:actions/runs|issues|pull)/[1-9][0-9]*", relative) is None:
        raise RetryError("evidence URL must identify a run, issue, or pull request")


def execute_retry(request: RetryRequest, client: RetryClient) -> dict[str, Any]:
    if request.reason not in ALL_REASONS:
        raise RetryError("retry reason is not in the governed taxonomy")
    if request.mode not in {"failed_jobs", "all_jobs"}:
        raise RetryError("retry mode must be failed_jobs or all_jobs")
    if (
        re.fullmatch(
            r"(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})|[A-Za-z0-9][A-Za-z0-9-]{0,94}\[bot\])",
            request.actor,
        )
        is None
    ):
        raise RetryError("actor is invalid")
    _validate_evidence_url(request.repository, request.evidence_url)
    run = client.get_run(request.run_id)
    workflow_id = run.get("workflow_id")
    attempt = run.get("run_attempt")
    conclusion = run.get("conclusion")
    repository = run.get("repository")
    repository_name = repository.get("full_name") if isinstance(repository, Mapping) else None
    if workflow_id not in SOURCE_WORKFLOWS:
        raise RetryError("run is not one of the four required source workflows")
    if run.get("event") != "pull_request" or repository_name != request.repository:
        raise RetryError("retry target is not a pull_request run in this repository")
    if run.get("status") != "completed" or conclusion not in FAILED_CONCLUSIONS:
        raise RetryError("retry target must be a terminal non-successful run")
    if attempt != request.failed_attempt:
        raise RetryError("failed attempt is stale or does not match the current run attempt")
    record = {
        "schema": "loom-ci-retry-v1",
        "repository": request.repository,
        "source_workflow": SOURCE_WORKFLOWS[int(workflow_id)],
        "source_run_id": request.run_id,
        "failed_attempt": request.failed_attempt,
        "failed_conclusion": conclusion,
        "head_sha": run.get("head_sha"),
        "reason": request.reason,
        "evidence_url": request.evidence_url,
        "mode": request.mode,
        "actor": request.actor,
        "decision": "denied_code_change_required",
    }
    if request.reason == "code_failure":
        return record
    if request.mode == "all_jobs" and request.reason != "capacity_queue":
        raise RetryError("all_jobs retry is reserved for classified capacity/queue loss")
    try:
        client.request_retry(request.run_id, request.mode)
    except RetryError as exc:
        record["decision"] = "retry_request_failed"
        record["request_error"] = str(exc)
        return record
    record["decision"] = "retry_requested"
    return record


def _write_record(record: Mapping[str, Any]) -> None:
    rendered = json.dumps(record, sort_keys=True)
    print(rendered)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a", encoding="utf-8") as stream:
            stream.write(f"### Classified CI retry\n\n```json\n{rendered}\n```\n")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--source-run-id", required=True, type=int)
    parser.add_argument("--failed-attempt", required=True, type=int)
    parser.add_argument("--reason", required=True, choices=sorted(ALL_REASONS))
    parser.add_argument("--evidence-url", required=True)
    parser.add_argument("--mode", required=True, choices=("failed_jobs", "all_jobs"))
    parser.add_argument("--actor", required=True)
    parser.add_argument("--api-url", default=os.environ.get("GITHUB_API_URL", "https://api.github.com"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    request = RetryRequest(
        repository=args.repository,
        run_id=args.source_run_id,
        failed_attempt=args.failed_attempt,
        reason=args.reason,
        evidence_url=args.evidence_url,
        mode=args.mode,
        actor=args.actor,
    )
    try:
        record = execute_retry(
            request,
            GitHubRetryClient(
                repository=args.repository,
                token=os.environ.get("GITHUB_TOKEN", ""),
                api_url=args.api_url,
            ),
        )
    except RetryError as exc:
        print(f"ci retry rejected: {exc}", file=sys.stderr)
        return 2
    _write_record(record)
    return 3 if record["decision"] != "retry_requested" else 0


if __name__ == "__main__":
    raise SystemExit(main())
