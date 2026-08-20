#!/usr/bin/env python3
"""Shared, read-only GitHub Actions metrics primitives."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from threading import local
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


class MetricsError(RuntimeError):
    """Raised when GitHub Actions metrics evidence cannot be fetched or parsed."""


class SafeRedirectHandler(HTTPRedirectHandler):
    """Prevent the GitHub token from following redirects off origin."""

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


def parse_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("timestamp must include a timezone")
    return parsed.astimezone(UTC)


class GitHubActionsMetricsClient:
    """Fetch exact workflow-attempt metadata and jobs from GitHub Actions."""

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
                "User-Agent": "loom-github-actions-metrics",
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

    def collect_run_jobs(self, *, run_id: int, attempt: int) -> list[Mapping[str, Any]]:
        response = self._json(
            f"/repos/{self._repo_path}/actions/runs/{run_id}/attempts/{attempt}/jobs",
            query={"per_page": 100},
        )
        jobs = response.get("jobs", []) if isinstance(response, Mapping) else []
        if not isinstance(jobs, list) or not all(isinstance(job, Mapping) for job in jobs):
            raise MetricsError("GitHub jobs response is invalid")
        return [dict(job) for job in jobs]

    def collect_run_attempt(self, *, run_id: int, attempt: int) -> Mapping[str, Any]:
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
