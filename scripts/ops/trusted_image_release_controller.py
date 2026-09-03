#!/usr/bin/env python3
"""Reconcile trusted image publication for the protected development head."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

IMAGES_WORKFLOW_ID = 302898384
TRUSTED_DISPATCH_PREFIX = "gate=trusted-publish /"
_SHA_RE = re.compile(r"[0-9a-f]{40}")
_ACTIVE_STATUSES = {"pending", "queued", "requested", "waiting", "in_progress"}


class ReconcileError(RuntimeError):
    """Raised when release reconciliation cannot prove a safe action."""


class ReleaseClient(Protocol):
    def get_branch_head(self, branch: str) -> str: ...

    def list_image_runs(self, branch: str) -> Sequence[Mapping[str, Any]]: ...

    def has_trusted_release_artifact(self, run: Mapping[str, Any]) -> bool: ...

    def dispatch_images(self, *, branch: str, base_sha: str) -> None: ...


class GitHistory(Protocol):
    def is_ancestor(self, ancestor: str, descendant: str) -> bool: ...

    def distance(self, ancestor: str, descendant: str) -> int: ...


class LocalGitHistory:
    def _run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", *args],
            check=False,
            capture_output=True,
            text=True,
        )

    def is_ancestor(self, ancestor: str, descendant: str) -> bool:
        result = self._run("merge-base", "--is-ancestor", ancestor, descendant)
        if result.returncode not in {0, 1}:
            raise ReconcileError("git ancestry verification failed")
        return result.returncode == 0

    def distance(self, ancestor: str, descendant: str) -> int:
        result = self._run("rev-list", "--count", f"{ancestor}..{descendant}")
        if result.returncode != 0 or not result.stdout.strip().isdigit():
            raise ReconcileError("git release distance verification failed")
        return int(result.stdout.strip())


class GitHubReleaseClient:
    def __init__(self, *, repository: str, token: str, api_url: str) -> None:
        parts = repository.split("/")
        if len(parts) != 2 or not all(parts):
            raise ReconcileError("repository must use owner/name form")
        if not token:
            raise ReconcileError("GITHUB_TOKEN is required")
        self._repo = "/".join(quote(part, safe="") for part in parts)
        self._token = token
        self._api_url = api_url.rstrip("/")

    def _request(
        self,
        path: str,
        *,
        method: str = "GET",
        payload: Mapping[str, Any] | None = None,
    ) -> bytes:
        data = None
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self._token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "loom-trusted-image-release-controller",
        }
        if payload is not None:
            data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = Request(
            f"{self._api_url}{path}",
            method=method,
            headers=headers,
            data=data,
        )
        try:
            with urlopen(request, timeout=30) as response:
                return bytes(response.read())
        except HTTPError as exc:
            raise ReconcileError(f"GitHub API {method} {path} returned HTTP {exc.code}") from exc
        except (URLError, TimeoutError) as exc:
            raise ReconcileError(f"GitHub API {method} {path} failed") from exc

    def _json(self, path: str) -> Mapping[str, Any]:
        try:
            payload = json.loads(self._request(path))
        except json.JSONDecodeError as exc:
            raise ReconcileError("GitHub returned invalid JSON") from exc
        if not isinstance(payload, Mapping):
            raise ReconcileError("GitHub returned an invalid object")
        return payload

    def get_branch_head(self, branch: str) -> str:
        payload = self._json(f"/repos/{self._repo}/git/ref/heads/{quote(branch, safe='')}")
        target = payload.get("object")
        sha = target.get("sha") if isinstance(target, Mapping) else None
        return _require_sha(sha, "branch head")

    def list_image_runs(self, branch: str) -> Sequence[Mapping[str, Any]]:
        collected: list[Mapping[str, Any]] = []
        expected_total: int | None = None
        run_ids: set[int] = set()
        page = 1
        while True:
            query = urlencode({"per_page": "100", "page": str(page)})
            payload = self._json(
                f"/repos/{self._repo}/actions/workflows/{IMAGES_WORKFLOW_ID}/runs?{query}"
            )
            total_count = payload.get("total_count")
            runs = payload.get("workflow_runs")
            if (
                not isinstance(total_count, int)
                or isinstance(total_count, bool)
                or total_count < 0
                or not isinstance(runs, list)
                or not all(isinstance(run, Mapping) for run in runs)
            ):
                raise ReconcileError("GitHub returned an invalid workflow run list")
            if expected_total is None:
                expected_total = total_count
            elif total_count != expected_total:
                raise ReconcileError("GitHub workflow run list changed during pagination")

            for run in runs:
                run_id = _require_positive_int(run.get("id"), "workflow run id")
                if run_id in run_ids:
                    raise ReconcileError("GitHub workflow run pagination contained duplicates")
                run_ids.add(run_id)
                if run.get("head_branch") == branch and run.get("event") in {
                    "push",
                    "workflow_dispatch",
                }:
                    collected.append(run)

            if len(run_ids) == expected_total:
                break
            if not runs or len(run_ids) > expected_total:
                raise ReconcileError("GitHub returned a truncated workflow run list")
            page += 1
        return collected

    def has_trusted_release_artifact(self, run: Mapping[str, Any]) -> bool:
        run_id = _require_positive_int(run.get("id"), "workflow run id")
        run_attempt = _require_positive_int(run.get("run_attempt"), "workflow run attempt")
        head_sha = _require_sha(run.get("head_sha"), "workflow run head")
        artifact_name = (
            f"personal-dev-trusted-release-run-{run_id}-attempt-{run_attempt}"
        )
        query = urlencode({"name": artifact_name, "per_page": "100"})
        payload = self._json(
            f"/repos/{self._repo}/actions/runs/{run_id}/artifacts?{query}"
        )
        total_count = payload.get("total_count")
        artifacts = payload.get("artifacts")
        if (
            not isinstance(total_count, int)
            or isinstance(total_count, bool)
            or total_count < 0
            or not isinstance(artifacts, list)
            or not all(isinstance(artifact, Mapping) for artifact in artifacts)
        ):
            raise ReconcileError("GitHub returned an invalid trusted release artifact list")
        if total_count == 0 and not artifacts:
            return False
        if total_count != 1 or len(artifacts) != 1:
            raise ReconcileError("trusted release artifact lookup was ambiguous")

        artifact = artifacts[0]
        workflow_run = artifact.get("workflow_run")
        size_in_bytes = artifact.get("size_in_bytes")
        if (
            artifact.get("name") != artifact_name
            or artifact.get("expired") is not False
            or not isinstance(size_in_bytes, int)
            or isinstance(size_in_bytes, bool)
            or size_in_bytes <= 0
            or not isinstance(workflow_run, Mapping)
            or type(workflow_run.get("id")) is not int
            or workflow_run.get("id") != run_id
            or workflow_run.get("head_sha") != head_sha
        ):
            raise ReconcileError("trusted release artifact is invalid or unbound")
        return True

    def dispatch_images(self, *, branch: str, base_sha: str) -> None:
        self._request(
            f"/repos/{self._repo}/actions/workflows/{IMAGES_WORKFLOW_ID}/dispatches",
            method="POST",
            payload={
                "ref": branch,
                "inputs": {
                    "trusted_publish": "true",
                    "trusted_base_sha": base_sha,
                },
            },
        )


@dataclass(frozen=True)
class ReconcileRequest:
    repository: str
    branch: str
    checkout_head: str


def _require_sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA_RE.fullmatch(value) is None:
        raise ReconcileError(f"{label} is not a lowercase commit SHA")
    return value


def _require_positive_int(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ReconcileError(f"{label} is not a positive integer")
    return value


def _is_trusted_run(run: Mapping[str, Any]) -> bool:
    event = run.get("event")
    if event == "push":
        return True
    actor = run.get("actor")
    actor_login = actor.get("login") if isinstance(actor, Mapping) else None
    return (
        event == "workflow_dispatch"
        and actor_login == "github-actions[bot]"
        and str(run.get("display_title", "")).startswith(TRUSTED_DISPATCH_PREFIX)
    )


def reconcile_release(
    request: ReconcileRequest,
    client: ReleaseClient,
    history: GitHistory,
) -> dict[str, Any]:
    if request.branch not in {"dev", "main"}:
        raise ReconcileError("release branch must be dev or main")
    checkout_head = _require_sha(request.checkout_head, "checkout head")
    branch_head = client.get_branch_head(request.branch)
    if branch_head != checkout_head:
        raise ReconcileError("checked out source is not the current protected branch head")

    trusted_runs = [run for run in client.list_image_runs(request.branch) if _is_trusted_run(run)]
    for run in trusted_runs:
        if run.get("head_sha") != branch_head:
            continue
        status = run.get("status")
        conclusion = run.get("conclusion")
        if status in _ACTIVE_STATUSES:
            return {
                "schema": "loom-trusted-image-release-reconcile-v1",
                "decision": "already_active",
                "branch": request.branch,
                "head_sha": branch_head,
                "run_id": run.get("id"),
            }
        if (
            status == "completed"
            and conclusion == "success"
            and client.has_trusted_release_artifact(run)
        ):
            return {
                "schema": "loom-trusted-image-release-reconcile-v1",
                "decision": "already_published",
                "branch": request.branch,
                "head_sha": branch_head,
                "run_id": run.get("id"),
            }
        if status == "completed" and conclusion != "success":
            return {
                "schema": "loom-trusted-image-release-reconcile-v1",
                "decision": "blocked_failed_release",
                "branch": request.branch,
                "head_sha": branch_head,
                "run_id": run.get("id"),
                "conclusion": conclusion,
            }

    candidates: list[tuple[int, str, Mapping[str, Any]]] = []
    for run in trusted_runs:
        if run.get("status") != "completed" or run.get("conclusion") != "success":
            continue
        candidate = run.get("head_sha")
        if not isinstance(candidate, str) or _SHA_RE.fullmatch(candidate) is None:
            continue
        if candidate == branch_head or not history.is_ancestor(candidate, branch_head):
            continue
        candidates.append((history.distance(candidate, branch_head), candidate, run))

    baseline: tuple[int, str, Mapping[str, Any]] | None = None
    for candidate in sorted(candidates, key=lambda item: (item[0], item[1])):
        if client.has_trusted_release_artifact(candidate[2]):
            baseline = candidate
            break
    if baseline is None:
        raise ReconcileError("no successful trusted ancestor image release exists")

    distance, base_sha, base_run = baseline
    if distance < 1:
        raise ReconcileError("trusted release range must contain at least one commit")
    client.dispatch_images(branch=request.branch, base_sha=base_sha)
    return {
        "schema": "loom-trusted-image-release-reconcile-v1",
        "decision": "dispatch_requested",
        "branch": request.branch,
        "head_sha": branch_head,
        "base_sha": base_sha,
        "base_run_id": base_run.get("id"),
        "commit_distance": distance,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--branch", required=True, choices=("dev", "main"))
    parser.add_argument("--checkout-head", required=True)
    parser.add_argument(
        "--api-url", default=os.environ.get("GITHUB_API_URL", "https://api.github.com")
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        record = reconcile_release(
            ReconcileRequest(
                repository=args.repository,
                branch=args.branch,
                checkout_head=args.checkout_head,
            ),
            GitHubReleaseClient(
                repository=args.repository,
                token=os.environ.get("GITHUB_TOKEN", ""),
                api_url=args.api_url,
            ),
            LocalGitHistory(),
        )
    except ReconcileError as exc:
        print(f"trusted image release reconciliation rejected: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(record, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
