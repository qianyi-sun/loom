"""Trusted GitHub transport for oldlab-first CI placement leases."""

from __future__ import annotations

import argparse
import io
import json
import re
import sqlite3
import stat
import sys
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http.client import HTTPMessage
from pathlib import Path
from typing import Any, Protocol, cast

import jwt
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from loom_control_plane.ci_runner_lease_broker import (
    WORKFLOW_CLASS_CONTRACTS,
    AssignmentState,
    CiRunnerLeaseBroker,
    LeaseBrokerConfig,
    LeaseBrokerError,
    PlacementAssignment,
    RouteDecision,
    RouteDecisionState,
    RouteRequest,
    TrustedWorkflowGeneration,
)

EXPECTED_REPOSITORY = "qianyi-sun/loom"
ARTIFACT_PREFIX = "loom-ci-route-request-v1-"
ROUTE_REQUEST_FILENAME = "loom-ci-route-request.json"
ROUTE_CHECK_PREFIX = "loom-ci-route-v1"
GITHUB_ACTIONS_APP_ID = 15368
MAX_ARTIFACT_BYTES = 64 * 1024
MAX_JSON_BYTES = 4 * 1024 * 1024
GITHUB_REQUEST_TIMEOUT_SECONDS = 20
MAX_ACTIVE_RUNS_PER_WORKFLOW = 100
ACTIVE_WORKFLOW_INVENTORY_ATTEMPTS = 3
PUBLISHER_RETRY_SECONDS = 15
ROUTE_DECISION_RETENTION_DAYS = 7
OLDLAB_REQUEST_MAX_AGE_SECONDS = 30
TRUSTED_BRANCH = "dev"
ALLOWED_EVENTS = {"pull_request", "merge_group", "workflow_dispatch", "push"}
WORKFLOW_PATHS = {
    "CI": ".github/workflows/ci.yml",
    "images": ".github/workflows/images.yml",
    "cluster-smoke": ".github/workflows/cluster-smoke.yml",
    "staging-smoke": ".github/workflows/staging-smoke.yml",
}
REQUIRED_SOURCE_CHECKS = {
    "repository-checks": "CI",
    "images-gate": "images",
    "cluster-smoke-gate": "cluster-smoke",
    "staging-smoke-gate": "staging-smoke",
}
JOB_NAMES = {
    "CI": {
        "lint-and-static": "lint-and-static",
        "tests-root-1-of-2": "tests-root (1-of-2)",
        "tests-root-2-of-2": "tests-root (2-of-2)",
        "tests-packages": "tests-packages",
        "runtime-payload": "runtime-payload",
        "go-checks": "go-checks",
        "web-checks": "web-checks",
        "integration-1-of-2": "integration (1-of-2)",
        "integration-2-of-2": "integration (2-of-2)",
        "integration-docker": "integration-docker",
    },
    "images": {key: f"{key} (linux/amd64)" for key in WORKFLOW_CLASS_CONTRACTS["images"][2]},
    "cluster-smoke": {"cluster-contract": "cluster contract (render live k3s topology)"},
    "staging-smoke": {"system-smoke": "manifest-owned system smoke"},
}
ROUTE_JOB_NAMES = {
    "CI": "resolve oldlab-first routes",
    "images": "resolve oldlab-first image routes",
    "cluster-smoke": "resolve oldlab-first cluster route",
    "staging-smoke": "resolve oldlab-first staging route",
}
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SOURCE_JOB_URL_RE = re.compile(
    r"^https://github\.com/qianyi-sun/loom/actions/runs/[1-9][0-9]*/job/[1-9][0-9]*$"
)
MAX_APP_PRIVATE_KEY_BYTES = 64 * 1024
MAX_INSTALLATION_TOKEN_BYTES = 4096
APP_INSTALLATION_PERMISSIONS = {
    "actions": "read",
    "checks": "write",
    "contents": "read",
    "pull_requests": "read",
}


class RouteControllerError(RuntimeError):
    """A bounded, secret-free route-controller failure."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        request: urllib.request.Request,
        file_pointer: Any,
        code: int,
        message: str,
        headers: HTTPMessage,
        new_url: str,
    ) -> None:
        return None


class RouteGitHubAPI(Protocol):
    def branch_head(self, branch: str) -> str: ...

    def commit(self, ref: str) -> Mapping[str, object]: ...

    def compare_commits(self, base: str, head: str) -> Mapping[str, object]: ...

    def associated_pull_requests(self, commit_sha: str) -> Sequence[Mapping[str, object]]: ...

    def active_workflow_runs(self, workflow_id: int) -> Sequence[Mapping[str, object]]: ...

    def route_artifact(
        self, *, workflow_id: int, workflow_run_id: int, run_attempt: int
    ) -> Mapping[str, object] | None: ...

    def download_artifact(self, artifact_id: int) -> bytes: ...

    def workflow_run(self, run_id: int) -> Mapping[str, object]: ...

    def content_blob_sha(self, path: str, ref: str) -> str: ...

    def check_runs(self, head_sha: str, name: str) -> Sequence[Mapping[str, object]]: ...

    def workflow_jobs(self, run_id: int, attempt: int) -> Sequence[Mapping[str, object]]: ...


class RouteCheckPublisher(Protocol):
    app_id: int

    def publish(self, payload: Mapping[str, object]) -> Mapping[str, object]: ...


class GitHubRouteAPI:
    """Minimal outbound-only GitHub API used by the root-owned controller."""

    def __init__(
        self,
        *,
        repository: str,
        token: str | None = None,
        token_provider: Callable[[], str] | None = None,
    ) -> None:
        if repository != EXPECTED_REPOSITORY or (token is None) == (token_provider is None):
            raise RouteControllerError("GitHub route API configuration is invalid")
        if token is not None and not token:
            raise RouteControllerError("GitHub route API configuration is invalid")
        self.repository = repository
        self._token_provider = token_provider or (lambda: cast(str, token))

    def _token(self) -> str:
        token = self._token_provider()
        if not token or len(token.encode()) > MAX_INSTALLATION_TOKEN_BYTES:
            raise RouteControllerError("GitHub route API token is invalid")
        return token

    def _request(
        self,
        method: str,
        path: str,
        *,
        payload: Mapping[str, object] | None = None,
    ) -> Any:
        token = self._token()
        body = json.dumps(payload, separators=(",", ":")).encode() if payload is not None else None
        request = urllib.request.Request(
            f"https://api.github.com/repos/{self.repository}{path}",
            data=body,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "User-Agent": "loom-ci-route-controller/1",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(
                request, timeout=GITHUB_REQUEST_TIMEOUT_SECONDS
            ) as response:
                raw = cast(bytes, response.read(MAX_JSON_BYTES + 1))
        except urllib.error.HTTPError as exc:
            raise RouteControllerError(
                f"GitHub API {method} {path} failed with HTTP {exc.code}"
            ) from None
        except (OSError, TimeoutError) as exc:
            raise RouteControllerError(f"GitHub API {method} {path} failed") from exc
        if len(raw) > MAX_JSON_BYTES:
            raise RouteControllerError("GitHub API response exceeds the size limit")
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RouteControllerError("GitHub API returned invalid JSON") from exc

    def commit(self, ref: str) -> Mapping[str, object]:
        encoded_ref = urllib.parse.quote(ref, safe="")
        payload = self._request("GET", f"/commits/{encoded_ref}")
        if not isinstance(payload, dict):
            raise RouteControllerError("GitHub commit identity is malformed")
        return payload

    def branch_head(self, branch: str) -> str:
        if branch != TRUSTED_BRANCH:
            raise RouteControllerError("trusted branch is outside the route contract")
        payload = self.commit(branch)
        sha = payload.get("sha")
        if not isinstance(sha, str) or _SHA_RE.fullmatch(sha) is None:
            raise RouteControllerError("GitHub trusted branch head is malformed")
        return sha

    def compare_commits(self, base: str, head: str) -> Mapping[str, object]:
        if _SHA_RE.fullmatch(base) is None or _SHA_RE.fullmatch(head) is None:
            raise RouteControllerError("GitHub compare identity is malformed")
        payload = self._request("GET", f"/compare/{base}...{head}?per_page=1&page=1")
        if not isinstance(payload, dict):
            raise RouteControllerError("GitHub compare response is malformed")
        return payload

    def associated_pull_requests(self, commit_sha: str) -> Sequence[Mapping[str, object]]:
        if _SHA_RE.fullmatch(commit_sha) is None:
            raise RouteControllerError("GitHub commit pull identity is malformed")
        payload = self._request("GET", f"/commits/{commit_sha}/pulls?per_page=100")
        if not isinstance(payload, list) or any(not isinstance(item, dict) for item in payload):
            raise RouteControllerError("GitHub associated pull inventory is malformed")
        return cast(list[dict[str, object]], payload)

    def active_workflow_runs(self, workflow_id: int) -> Sequence[Mapping[str, object]]:
        path = (
            f"/actions/workflows/{workflow_id}/runs?status=in_progress&per_page="
            f"{MAX_ACTIVE_RUNS_PER_WORKFLOW}"
        )
        for _attempt in range(ACTIVE_WORKFLOW_INVENTORY_ATTEMPTS):
            payload = self._request("GET", path)
            runs = payload.get("workflow_runs") if isinstance(payload, dict) else None
            total_count = payload.get("total_count") if isinstance(payload, dict) else None
            if (
                not isinstance(runs, list)
                or isinstance(total_count, bool)
                or not isinstance(total_count, int)
                or total_count < 0
                or any(not isinstance(item, dict) for item in runs)
            ):
                continue
            if total_count > MAX_ACTIVE_RUNS_PER_WORKFLOW:
                raise RouteControllerError("active workflow inventory exceeds the scan bound")
            if total_count == len(runs):
                return cast(list[dict[str, object]], runs)
        raise RouteControllerError(
            "GitHub active workflow inventory remained malformed after bounded retries"
        )

    def route_artifact(
        self, *, workflow_id: int, workflow_run_id: int, run_attempt: int
    ) -> Mapping[str, object] | None:
        name = f"{ARTIFACT_PREFIX}{workflow_id}-{workflow_run_id}-{run_attempt}"
        encoded_name = urllib.parse.quote(name, safe="")
        payload = self._request("GET", f"/actions/artifacts?name={encoded_name}&per_page=100")
        artifacts = payload.get("artifacts") if isinstance(payload, dict) else None
        total_count = payload.get("total_count") if isinstance(payload, dict) else None
        if (
            not isinstance(artifacts, list)
            or isinstance(total_count, bool)
            or not isinstance(total_count, int)
            or total_count < 0
            or total_count != len(artifacts)
            or any(not isinstance(item, dict) for item in artifacts)
        ):
            raise RouteControllerError("GitHub route artifact inventory is malformed")
        exact = [
            item
            for item in artifacts
            if isinstance(item, dict) and item.get("name") == name and item.get("expired") is False
        ]
        if len(exact) > 1:
            raise RouteControllerError("GitHub route artifact identity is ambiguous")
        return exact[0] if exact else None

    def download_artifact(self, artifact_id: int) -> bytes:
        initial = urllib.request.Request(
            (f"https://api.github.com/repos/{self.repository}/actions/artifacts/{artifact_id}/zip"),
            method="GET",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self._token()}",
                "User-Agent": "loom-ci-route-controller/1",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        opener = urllib.request.build_opener(_NoRedirect())
        try:
            opener.open(initial, timeout=GITHUB_REQUEST_TIMEOUT_SECONDS)
        except urllib.error.HTTPError as exc:
            if exc.code not in {301, 302, 303, 307, 308}:
                raise RouteControllerError(
                    f"GitHub artifact download failed with HTTP {exc.code}"
                ) from None
            location = exc.headers.get("Location")
        except (OSError, TimeoutError) as exc:
            raise RouteControllerError("GitHub artifact download failed") from exc
        else:
            raise RouteControllerError("GitHub artifact download did not redirect")
        if not isinstance(location, str):
            raise RouteControllerError("GitHub artifact redirect is missing")
        parsed = urllib.parse.urlsplit(location)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
        ):
            raise RouteControllerError("GitHub artifact redirect is unsafe")
        download = urllib.request.Request(
            location,
            method="GET",
            headers={"User-Agent": "loom-ci-route-controller/1"},
        )
        try:
            with urllib.request.urlopen(
                download, timeout=GITHUB_REQUEST_TIMEOUT_SECONDS
            ) as response:
                raw = cast(bytes, response.read(MAX_ARTIFACT_BYTES + 1))
        except urllib.error.HTTPError as exc:
            raise RouteControllerError(
                f"GitHub artifact payload failed with HTTP {exc.code}"
            ) from None
        except (OSError, TimeoutError) as exc:
            raise RouteControllerError("GitHub artifact payload failed") from exc
        if len(raw) > MAX_ARTIFACT_BYTES:
            raise RouteControllerError("route artifact exceeds the size limit")
        return raw

    def workflow_run(self, run_id: int) -> Mapping[str, object]:
        payload = self._request("GET", f"/actions/runs/{run_id}")
        if not isinstance(payload, dict):
            raise RouteControllerError("GitHub workflow run is malformed")
        return payload

    def content_blob_sha(self, path: str, ref: str) -> str:
        encoded_path = urllib.parse.quote(path, safe="/")
        encoded_ref = urllib.parse.quote(ref, safe="")
        payload = self._request("GET", f"/contents/{encoded_path}?ref={encoded_ref}")
        sha = payload.get("sha") if isinstance(payload, dict) else None
        if not isinstance(sha, str) or _SHA_RE.fullmatch(sha) is None:
            raise RouteControllerError("GitHub workflow blob identity is malformed")
        return sha

    def check_runs(self, head_sha: str, name: str) -> Sequence[Mapping[str, object]]:
        encoded_name = urllib.parse.quote(name, safe="")
        payload = self._request(
            "GET",
            f"/commits/{head_sha}/check-runs?check_name={encoded_name}&filter=latest&per_page=100",
        )
        checks = payload.get("check_runs") if isinstance(payload, dict) else None
        if not isinstance(checks, list):
            raise RouteControllerError("GitHub check-run inventory is malformed")
        return [item for item in checks if isinstance(item, dict)]

    def workflow_jobs(self, run_id: int, attempt: int) -> Sequence[Mapping[str, object]]:
        payload = self._request(
            "GET", f"/actions/runs/{run_id}/attempts/{attempt}/jobs?filter=all&per_page=100"
        )
        jobs = payload.get("jobs") if isinstance(payload, dict) else None
        if not isinstance(jobs, list):
            raise RouteControllerError("GitHub workflow jobs are malformed")
        return [item for item in jobs if isinstance(item, dict)]


class GitHubAppRouteCheckPublisher:
    """Create route CheckRuns directly with one least-privilege GitHub App."""

    def __init__(
        self,
        *,
        repository: str,
        app_id: int,
        installation_id: int,
        private_key_pem: bytes,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if repository != EXPECTED_REPOSITORY:
            raise RouteControllerError("route publisher repository is invalid")
        if (
            isinstance(app_id, bool)
            or not isinstance(app_id, int)
            or app_id < 1
            or isinstance(installation_id, bool)
            or not isinstance(installation_id, int)
            or installation_id < 1
        ):
            raise RouteControllerError("route publisher app identity is invalid")
        try:
            key = serialization.load_pem_private_key(private_key_pem, password=None)
        except (TypeError, ValueError, UnsupportedAlgorithm) as exc:
            raise RouteControllerError("route publisher app private key is invalid") from exc
        if not isinstance(key, rsa.RSAPrivateKey) or key.key_size < 2048:
            raise RouteControllerError("route publisher app private key is invalid")
        self.repository = repository
        self.app_id = app_id
        self.installation_id = installation_id
        self._private_key_pem = private_key_pem
        self.now = now or (lambda: datetime.now(UTC))
        self._cached_token: str | None = None
        self._cached_token_expires_at: datetime | None = None

    def _request(
        self,
        method: str,
        url: str,
        *,
        token: str,
        payload: Mapping[str, object],
    ) -> Mapping[str, object]:
        request = urllib.request.Request(
            url,
            data=json.dumps(payload, separators=(",", ":")).encode(),
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "User-Agent": "loom-ci-route-app-publisher/1",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(
                request, timeout=GITHUB_REQUEST_TIMEOUT_SECONDS
            ) as response:
                raw = cast(bytes, response.read(MAX_JSON_BYTES + 1))
        except urllib.error.HTTPError as exc:
            raise RouteControllerError(
                f"GitHub route publisher {method} failed with HTTP {exc.code}"
            ) from None
        except (OSError, TimeoutError) as exc:
            raise RouteControllerError(f"GitHub route publisher {method} failed") from exc
        if len(raw) > MAX_JSON_BYTES:
            raise RouteControllerError("GitHub route publisher response is too large")
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RouteControllerError("GitHub route publisher returned invalid JSON") from exc
        if not isinstance(value, dict):
            raise RouteControllerError("GitHub route publisher response is malformed")
        return value

    def installation_token(self) -> str:
        observed_at = self.now()
        if observed_at.tzinfo is None:
            raise RouteControllerError("route publisher clock is invalid")
        observed_at = observed_at.astimezone(UTC)
        if (
            self._cached_token is not None
            and self._cached_token_expires_at is not None
            and self._cached_token_expires_at > observed_at + timedelta(minutes=1)
        ):
            return self._cached_token
        issued_at = int(observed_at.timestamp())
        try:
            app_jwt = jwt.encode(
                {
                    "iat": issued_at - 60,
                    "exp": issued_at + 540,
                    "iss": str(self.app_id),
                },
                self._private_key_pem,
                algorithm="RS256",
            )
        except jwt.PyJWTError as exc:
            raise RouteControllerError("route publisher JWT creation failed") from exc
        response = self._request(
            "POST",
            f"https://api.github.com/app/installations/{self.installation_id}/access_tokens",
            token=app_jwt,
            payload={
                "repositories": ["loom"],
                "permissions": APP_INSTALLATION_PERMISSIONS,
            },
        )
        token = response.get("token")
        permissions = response.get("permissions")
        expires_at = response.get("expires_at")
        parsed_expiry = (
            _github_timestamp(expires_at, "installation_token.expires_at")
            if isinstance(expires_at, str)
            else None
        )
        if (
            not isinstance(token, str)
            or not token
            or len(token.encode()) > MAX_INSTALLATION_TOKEN_BYTES
            or any(character.isspace() for character in token)
            or not isinstance(permissions, dict)
            or any(
                permissions.get(name) != level
                for name, level in APP_INSTALLATION_PERMISSIONS.items()
            )
            or parsed_expiry is None
            or parsed_expiry <= observed_at + timedelta(minutes=1)
        ):
            raise RouteControllerError("GitHub route publisher token is malformed")
        self._cached_token = token
        self._cached_token_expires_at = parsed_expiry
        return token

    def publish(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        return self._request(
            "POST",
            f"https://api.github.com/repos/{self.repository}/check-runs",
            token=self.installation_token(),
            payload=payload,
        )


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    requests_seen: int
    routes_published: int
    routes_replayed: int
    routes_pending: int
    routes_abandoned: int
    assignments_released: int
    decisions_pruned: int
    runtime_sha: str
    trusted_workflow_sha: str
    trusted_workflow_digest: str
    observed_dev_sha: str | None
    generation_lag_commits: int | None
    workflow_blob_drift: Mapping[str, bool] | None
    generation_promoted: bool
    generation_blocker: str | None

    def public_dict(self) -> dict[str, object]:
        return {
            "requests_seen": self.requests_seen,
            "routes_published": self.routes_published,
            "routes_replayed": self.routes_replayed,
            "routes_pending": self.routes_pending,
            "routes_abandoned": self.routes_abandoned,
            "assignments_released": self.assignments_released,
            "decisions_pruned": self.decisions_pruned,
            "runtime_sha": self.runtime_sha,
            "trusted_workflow_sha": self.trusted_workflow_sha,
            "trusted_workflow_digest": self.trusted_workflow_digest,
            "observed_dev_sha": self.observed_dev_sha,
            "generation_lag_commits": self.generation_lag_commits,
            "workflow_blob_drift": (
                dict(self.workflow_blob_drift) if self.workflow_blob_drift is not None else None
            ),
            "generation_promoted": self.generation_promoted,
            "generation_blocker": self.generation_blocker,
        }


def _exact_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise RouteControllerError(f"{field} is invalid")
    return value


def _exact_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise RouteControllerError(f"{field} is invalid")
    return value


def _route_request_from_zip(raw: bytes) -> RouteRequest:
    if not raw or len(raw) > MAX_ARTIFACT_BYTES:
        raise RouteControllerError("route artifact size is invalid")
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            members = archive.infolist()
            if len(members) != 1 or members[0].filename != ROUTE_REQUEST_FILENAME:
                raise RouteControllerError(
                    f"route artifact must contain only {ROUTE_REQUEST_FILENAME}"
                )
            if members[0].file_size > MAX_ARTIFACT_BYTES:
                raise RouteControllerError("route request exceeds the size limit")
            request_raw = archive.read(members[0])
    except RouteControllerError:
        raise
    except (OSError, zipfile.BadZipFile, RuntimeError) as exc:
        raise RouteControllerError("route artifact is not a valid zip archive") from exc
    try:
        value = json.loads(request_raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RouteControllerError("route request is not valid JSON") from exc
    if not isinstance(value, dict):
        raise RouteControllerError("route request must be one JSON object")
    try:
        return RouteRequest.from_mapping(value)
    except LeaseBrokerError as exc:
        raise RouteControllerError(str(exc)) from exc


def _read_app_private_key(path: Path) -> bytes:
    try:
        metadata = path.lstat()
        raw = path.read_bytes()
    except OSError as exc:
        raise RouteControllerError("could not read route publisher app private key") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise RouteControllerError("route publisher app key must be a regular file")
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise RouteControllerError("route publisher app key grants group or other access")
    if not raw or len(raw) > MAX_APP_PRIVATE_KEY_BYTES or b"\x00" in raw:
        raise RouteControllerError("route publisher app key has an invalid size")
    return raw


def _workflow_name_for_id(workflow_id: int) -> str:
    matches = [
        name for name, contract in WORKFLOW_CLASS_CONTRACTS.items() if contract[0] == workflow_id
    ]
    if len(matches) != 1:
        raise RouteControllerError("workflow id is outside the route contract")
    return matches[0]


def _github_timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise RouteControllerError(f"{field} is missing or invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RouteControllerError(f"{field} is missing or invalid") from exc
    if parsed.tzinfo is None:
        raise RouteControllerError(f"{field} is missing or invalid")
    return parsed.astimezone(UTC)


class CiRunnerRouteController:
    def __init__(
        self,
        *,
        api: RouteGitHubAPI,
        broker: CiRunnerLeaseBroker,
        runtime_sha: str,
        publisher: RouteCheckPublisher,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if _SHA_RE.fullmatch(runtime_sha) is None:
            raise RouteControllerError("runtime SHA must be a full lowercase commit SHA")
        self.api = api
        self.broker = broker
        self.runtime_sha = runtime_sha
        if publisher.app_id < 1:
            raise RouteControllerError("route publisher app identity is invalid")
        self.publisher = publisher
        self.now = now or (lambda: datetime.now(UTC))

    def _workflow_blobs(self, ref: str) -> dict[str, str]:
        return {
            workflow_name: self.api.content_blob_sha(path, ref)
            for workflow_name, path in sorted(WORKFLOW_PATHS.items())
        }

    @staticmethod
    def _commit_tree(commit: Mapping[str, object], expected_sha: str) -> str:
        if commit.get("sha") != expected_sha:
            raise RouteControllerError("GitHub commit SHA does not match trusted candidate")
        commit_value = commit.get("commit")
        tree = commit_value.get("tree") if isinstance(commit_value, dict) else None
        tree_sha = tree.get("sha") if isinstance(tree, dict) else None
        if not isinstance(tree_sha, str) or _SHA_RE.fullmatch(tree_sha) is None:
            raise RouteControllerError("GitHub commit tree identity is malformed")
        return tree_sha

    def _bootstrap_trusted_workflow_generation(self) -> TrustedWorkflowGeneration:
        commit = self.api.commit(self.runtime_sha)
        candidate_tree = self._commit_tree(commit, self.runtime_sha)
        return self.broker.record_trusted_workflow_generation(
            candidate_sha=self.runtime_sha,
            candidate_tree=candidate_tree,
            workflow_blobs=self._workflow_blobs(self.runtime_sha),
            evidence={
                "kind": "installed_runtime",
                "runtime_sha": self.runtime_sha,
            },
            predecessor_generation_id=None,
            now=self.now(),
        )

    @staticmethod
    def _authoritative_protected_source_check(
        *,
        inventory: Sequence[Mapping[str, object]],
        check_name: str,
        head_sha: str,
    ) -> tuple[Mapping[str, object], int]:
        error = f"protected source check {check_name} is missing or ambiguous"
        if not inventory:
            raise RouteControllerError(error)
        candidates: list[tuple[datetime, int, Mapping[str, object]]] = []
        try:
            for check in inventory:
                app = check.get("app")
                details_url = check.get("details_url")
                if (
                    check.get("name") != check_name
                    or check.get("head_sha") != head_sha
                    or not isinstance(app, dict)
                    or app.get("id") != GITHUB_ACTIONS_APP_ID
                    or not isinstance(details_url, str)
                    or _SOURCE_JOB_URL_RE.fullmatch(details_url) is None
                ):
                    raise RouteControllerError(error)
                check_id = _exact_int(check.get("id"), f"{check_name}.id")
                started_at = _github_timestamp(check.get("started_at"), f"{check_name}.started_at")
                candidates.append((started_at, check_id, check))
        except RouteControllerError as exc:
            raise RouteControllerError(error) from exc

        # A newer in-progress retry must not be hidden by an older run that
        # completed successfully, so authority follows start time rather than
        # completion time.
        latest_started_at = max(item[0] for item in candidates)
        latest = [item for item in candidates if item[0] == latest_started_at]
        if len(latest) != 1:
            raise RouteControllerError(error)
        started_at, check_id, check = latest[0]
        try:
            completed_at = _github_timestamp(
                check.get("completed_at"), f"{check_name}.completed_at"
            )
        except RouteControllerError as exc:
            raise RouteControllerError(error) from exc
        if (
            check.get("status") != "completed"
            or check.get("conclusion") != "success"
            or completed_at < started_at
        ):
            raise RouteControllerError(error)
        return check, check_id

    def _advance_trusted_workflow_generation(
        self, current: TrustedWorkflowGeneration, dev_head: str
    ) -> tuple[TrustedWorkflowGeneration, bool]:
        if dev_head == current.candidate_sha:
            return current, False
        compare = self.api.compare_commits(current.candidate_sha, dev_head)
        commits = compare.get("commits")
        ahead_by = compare.get("ahead_by")
        behind_by = compare.get("behind_by")
        total_commits = compare.get("total_commits")
        if (
            compare.get("status") != "ahead"
            or isinstance(ahead_by, bool)
            or not isinstance(ahead_by, int)
            or isinstance(behind_by, bool)
            or not isinstance(behind_by, int)
            or isinstance(total_commits, bool)
            or not isinstance(total_commits, int)
            or ahead_by < 1
            or behind_by != 0
            or total_commits < 1
            or not isinstance(commits, list)
            or len(commits) != 1
            or any(not isinstance(item, dict) for item in commits)
        ):
            raise RouteControllerError("trusted dev history is incomplete or non-monotonic")
        next_summary = cast(dict[str, object], commits[0])
        next_sha = next_summary.get("sha")
        parents = next_summary.get("parents")
        if (
            not isinstance(next_sha, str)
            or _SHA_RE.fullmatch(next_sha) is None
            or not isinstance(parents, list)
            or len(parents) != 1
            or not isinstance(parents[0], dict)
            or parents[0].get("sha") != current.candidate_sha
        ):
            raise RouteControllerError("next trusted dev commit is not a linear successor")
        commit = self.api.commit(next_sha)
        candidate_tree = self._commit_tree(commit, next_sha)
        pulls = self.api.associated_pull_requests(next_sha)
        matching_pulls = [
            pull
            for pull in pulls
            if pull.get("merge_commit_sha") == next_sha
            and pull.get("state") == "closed"
            and isinstance(pull.get("merged_at"), str)
        ]
        if len(matching_pulls) != 1:
            raise RouteControllerError("trusted dev commit has ambiguous merge ownership")
        pull = matching_pulls[0]
        head = pull.get("head")
        base = pull.get("base")
        head_repo = head.get("repo") if isinstance(head, dict) else None
        base_repo = base.get("repo") if isinstance(base, dict) else None
        pull_number = pull.get("number")
        head_sha = head.get("sha") if isinstance(head, dict) else None
        if (
            isinstance(pull_number, bool)
            or not isinstance(pull_number, int)
            or pull_number < 1
            or not isinstance(head_sha, str)
            or _SHA_RE.fullmatch(head_sha) is None
            or not isinstance(head_repo, dict)
            or head_repo.get("full_name") != EXPECTED_REPOSITORY
            or not isinstance(base, dict)
            or base.get("ref") != TRUSTED_BRANCH
            or not isinstance(base_repo, dict)
            or base_repo.get("full_name") != EXPECTED_REPOSITORY
        ):
            raise RouteControllerError("trusted dev merge pull identity is invalid")
        merged_at = _github_timestamp(pull.get("merged_at"), "pull_request.merged_at")
        check_evidence: dict[str, object] = {}
        for check_name, workflow_name in REQUIRED_SOURCE_CHECKS.items():
            inventory = list(self.api.check_runs(head_sha, check_name))
            check, check_id = self._authoritative_protected_source_check(
                inventory=inventory,
                check_name=check_name,
                head_sha=head_sha,
            )
            check_evidence[check_name] = {
                "id": check_id,
                "workflow": workflow_name,
                "details_url": cast(str, check["details_url"]),
            }
        generation = self.broker.record_trusted_workflow_generation(
            candidate_sha=next_sha,
            candidate_tree=candidate_tree,
            workflow_blobs=self._workflow_blobs(next_sha),
            evidence={
                "kind": "protected_merge",
                "merge_commit_sha": next_sha,
                "pull_request_number": pull_number,
                "pull_request_head_sha": head_sha,
                "merged_at": merged_at.isoformat().replace("+00:00", "Z"),
                "observed_dev_head": dev_head,
                "checks": check_evidence,
            },
            predecessor_generation_id=current.generation_id,
            now=self.now(),
        )
        return generation, True

    def _generation_observation(
        self,
        generation: TrustedWorkflowGeneration,
        observed_dev_sha: str,
    ) -> tuple[int, dict[str, bool]]:
        if observed_dev_sha == generation.candidate_sha:
            return 0, {workflow_name: False for workflow_name in sorted(WORKFLOW_PATHS)}
        compare = self.api.compare_commits(generation.candidate_sha, observed_dev_sha)
        ahead_by = compare.get("ahead_by")
        behind_by = compare.get("behind_by")
        if (
            compare.get("status") != "ahead"
            or isinstance(ahead_by, bool)
            or not isinstance(ahead_by, int)
            or ahead_by < 1
            or isinstance(behind_by, bool)
            or not isinstance(behind_by, int)
            or behind_by != 0
        ):
            raise RouteControllerError("trusted workflow generation lag is non-monotonic")
        trusted_blobs = generation.workflow_blobs()
        observed_blobs = self._workflow_blobs(observed_dev_sha)
        return ahead_by, {
            workflow_name: observed_blobs[workflow_name] != trusted_blobs[workflow_name]
            for workflow_name in sorted(WORKFLOW_PATHS)
        }

    def reconcile(self) -> ReconcileResult:
        generation = self.broker.current_trusted_workflow_generation()
        if generation is None:
            generation = self._bootstrap_trusted_workflow_generation()
        observed_dev_sha: str | None = None
        generation_promoted = False
        generation_blocker: str | None = None
        try:
            observed_dev_sha = self.api.branch_head(TRUSTED_BRANCH)
            generation, generation_promoted = self._advance_trusted_workflow_generation(
                generation, observed_dev_sha
            )
        except (LeaseBrokerError, RouteControllerError) as exc:
            generation_blocker = str(exc)
        generation_lag_commits: int | None = None
        workflow_blob_drift: dict[str, bool] | None = None
        if observed_dev_sha is not None:
            try:
                generation_lag_commits, workflow_blob_drift = self._generation_observation(
                    generation, observed_dev_sha
                )
            except (LeaseBrokerError, RouteControllerError) as exc:
                if generation_blocker is None:
                    generation_blocker = str(exc)
        promotion_result = (
            "blocked"
            if generation_blocker is not None
            else "promoted"
            if generation_promoted
            else "current"
        )
        self.broker.record_trusted_workflow_observation(
            runtime_sha=self.runtime_sha,
            publisher_app_id=self.publisher.app_id,
            trust_generation_id=generation.generation_id,
            observed_dev_sha=observed_dev_sha,
            generation_lag_commits=generation_lag_commits,
            workflow_blob_drift=workflow_blob_drift,
            promotion_result=promotion_result,
            promotion_blocker=generation_blocker,
            now=self.now(),
        )
        seen = 0
        outcomes = {"published": 0, "replayed": 0, "pending": 0, "abandoned": 0}
        processed: set[str] = set()
        errors: list[str] = []
        abandoned_releases = 0

        for decision in self.broker.route_decisions(states=(RouteDecisionState.PENDING,)):
            try:
                outcome, decision_releases = self._reconcile_decision(decision)
                outcomes[outcome] += 1
                abandoned_releases += decision_releases
                processed.add(decision.request_sha256)
            except (LeaseBrokerError, RouteControllerError) as exc:
                errors.append(f"decision {decision.decision_id}: {exc}")

        for artifact, run in self._active_route_artifacts():
            seen += 1
            try:
                artifact_id = _exact_int(artifact.get("id"), "artifact.id")
                request = _route_request_from_zip(self.api.download_artifact(artifact_id))
                self._validate_artifact(artifact, request)
                self._validate_run(run, request)
                workflow_path = WORKFLOW_PATHS[request.workflow_name]
                head_blob = self.api.content_blob_sha(workflow_path, request.head_sha)
                trusted_blob = generation.workflow_blobs()[request.workflow_name]
                created_at = _github_timestamp(artifact.get("created_at"), "artifact.created_at")
                observed_at = self.now()
                if observed_at.tzinfo is None:
                    raise RouteControllerError("controller observation time is invalid")
                age_seconds = (observed_at.astimezone(UTC) - created_at).total_seconds()
                if head_blob != trusted_blob:
                    eligibility_reason = "workflow_blob_drift"
                elif age_seconds < 0:
                    eligibility_reason = "future_request"
                elif age_seconds > OLDLAB_REQUEST_MAX_AGE_SECONDS:
                    eligibility_reason = "stale_request"
                else:
                    eligibility_reason = "trusted_workflow_match"
                allow_oldlab = eligibility_reason == "trusted_workflow_match"
                decision = self.broker.decide_route(
                    request,
                    allow_oldlab=allow_oldlab,
                    trust_generation_id=generation.generation_id,
                    eligibility_reason=eligibility_reason,
                    publisher_app_id=self.publisher.app_id,
                    now=observed_at,
                )
                if decision.request_sha256 in processed:
                    continue
                outcome, decision_releases = self._reconcile_decision(decision)
                outcomes[outcome] += 1
                abandoned_releases += decision_releases
                processed.add(decision.request_sha256)
            except (LeaseBrokerError, RouteControllerError) as exc:
                errors.append(f"artifact {artifact.get('id', 'unknown')}: {exc}")

        released = abandoned_releases + self._reconcile_releases()
        pruned = self.broker.prune_route_decisions(
            before=self.now() - timedelta(days=ROUTE_DECISION_RETENTION_DAYS)
        )
        if errors:
            raise RouteControllerError("; ".join(errors[:20]))
        return ReconcileResult(
            requests_seen=seen,
            routes_published=outcomes["published"],
            routes_replayed=outcomes["replayed"],
            routes_pending=outcomes["pending"],
            routes_abandoned=outcomes["abandoned"],
            assignments_released=released,
            decisions_pruned=pruned,
            runtime_sha=self.runtime_sha,
            trusted_workflow_sha=generation.candidate_sha,
            trusted_workflow_digest=generation.generation_digest,
            observed_dev_sha=observed_dev_sha,
            generation_lag_commits=generation_lag_commits,
            workflow_blob_drift=workflow_blob_drift,
            generation_promoted=generation_promoted,
            generation_blocker=generation_blocker,
        )

    def _active_route_artifacts(
        self,
    ) -> list[tuple[Mapping[str, object], Mapping[str, object]]]:
        discovered: list[tuple[Mapping[str, object], Mapping[str, object]]] = []
        for workflow_name, contract in sorted(WORKFLOW_CLASS_CONTRACTS.items()):
            workflow_id = contract[0]
            for run in self.api.active_workflow_runs(workflow_id):
                run_id = _exact_int(run.get("id"), "workflow_run.id")
                attempt = _exact_int(run.get("run_attempt"), "workflow_run.run_attempt")
                if run.get("workflow_id") != workflow_id:
                    raise RouteControllerError(
                        f"active {workflow_name} run has the wrong workflow identity"
                    )
                repository = run.get("repository")
                if (
                    not isinstance(repository, dict)
                    or repository.get("full_name") != EXPECTED_REPOSITORY
                    or run.get("status") != "in_progress"
                ):
                    raise RouteControllerError(
                        f"active {workflow_name} run is outside the route contract"
                    )
                artifact = self.api.route_artifact(
                    workflow_id=workflow_id,
                    workflow_run_id=run_id,
                    run_attempt=attempt,
                )
                if artifact is not None:
                    discovered.append((artifact, run))
        return sorted(discovered, key=lambda item: _exact_int(item[0].get("id"), "artifact.id"))

    @staticmethod
    def _validate_artifact(artifact: Mapping[str, object], request: RouteRequest) -> None:
        expected_name = (
            f"{ARTIFACT_PREFIX}{request.workflow_id}-{request.workflow_run_id}-"
            f"{request.run_attempt}"
        )
        if artifact.get("name") != expected_name:
            raise RouteControllerError("route artifact name does not match its request")
        workflow_run = artifact.get("workflow_run")
        if not isinstance(workflow_run, dict):
            raise RouteControllerError("route artifact workflow identity is missing")
        if workflow_run.get("id") != request.workflow_run_id:
            raise RouteControllerError("route artifact workflow run does not match")
        if workflow_run.get("head_sha") != request.head_sha:
            raise RouteControllerError("route artifact head does not match")

    @staticmethod
    def _validate_run(run: Mapping[str, object], request: RouteRequest) -> None:
        repository = run.get("repository")
        if not isinstance(repository, dict) or repository.get("full_name") != request.repository:
            raise RouteControllerError("workflow run repository does not match")
        expected = {
            "id": request.workflow_run_id,
            "run_attempt": request.run_attempt,
            "workflow_id": request.workflow_id,
            "head_sha": request.head_sha,
        }
        if any(run.get(key) != value for key, value in expected.items()):
            raise RouteControllerError("workflow run identity does not match route request")
        if run.get("event") not in ALLOWED_EVENTS:
            raise RouteControllerError("workflow run event is not eligible")
        if run.get("status") not in {"requested", "queued", "in_progress", "completed"}:
            raise RouteControllerError("workflow run status is invalid")

    def _reconcile_decision(self, decision: RouteDecision) -> tuple[str, int]:
        payload = self._route_payload(decision)
        existing = list(self.api.check_runs(decision.head_sha, cast(str, payload["name"])))
        if existing:
            self._validate_published_check(
                existing,
                payload,
                expected_app_id=decision.publisher_app_id,
            )
            self.broker.mark_route_published(
                decision.request_sha256,
                now=self.now(),
            )
            return "replayed", 0
        if decision.state is RouteDecisionState.PUBLISHED:
            raise RouteControllerError("published route CheckRun is missing")
        if decision.state is RouteDecisionState.ABANDONED:
            return "abandoned", 0

        request_value = json.loads(decision.request_json)
        if not isinstance(request_value, dict):
            raise RouteControllerError("stored route request is malformed")
        try:
            request = RouteRequest.from_mapping(request_value)
        except LeaseBrokerError as exc:
            raise RouteControllerError(str(exc)) from exc
        run = self.api.workflow_run(decision.workflow_run_id)
        self._validate_run(run, request)
        if self._route_delivery_is_terminal(run, request):
            abandoned = self.broker.abandon_route(
                decision.request_sha256,
                now=self.now(),
            )
            released = self._release_abandoned_decision(abandoned)
            return "abandoned", released
        return self._dispatch_pending_route(decision, payload), 0

    def _route_payload(self, decision: RouteDecision) -> dict[str, object]:
        document = decision.document()
        name = (
            f"{ROUTE_CHECK_PREFIX}/{decision.workflow_name}/"
            f"{decision.workflow_run_id}/{decision.run_attempt}"
        )
        external_id = (
            f"{ROUTE_CHECK_PREFIX}:{decision.workflow_id}:"
            f"{decision.workflow_run_id}:{decision.run_attempt}:"
            f"{decision.request_sha256}"
        )
        if document.public_dict() != decision.response_dict():
            raise RouteControllerError("stored route response is not frozen exactly")
        return {
            "name": name,
            "head_sha": decision.head_sha,
            "external_id": external_id,
            "status": "completed",
            "conclusion": "success",
            "output": {
                "title": "oldlab-first route assignment",
                "summary": decision.response_json,
            },
        }

    def _dispatch_pending_route(
        self,
        decision: RouteDecision,
        payload: Mapping[str, object],
    ) -> str:
        observed_at = self.now()
        if observed_at.tzinfo is None:
            raise RouteControllerError("controller observation time is invalid")
        if decision.dispatch_attempted_at is not None:
            attempted_at = _github_timestamp(
                decision.dispatch_attempted_at,
                "route_decision.dispatch_attempted_at",
            )
            elapsed = (observed_at.astimezone(UTC) - attempted_at).total_seconds()
            if elapsed < PUBLISHER_RETRY_SECONDS:
                return "pending"
        decision = self.broker.record_route_dispatch(
            decision.request_sha256,
            now=observed_at,
        )
        if decision.publisher_app_id != self.publisher.app_id:
            raise RouteControllerError("frozen route publisher app is unavailable in this runtime")
        published = self.publisher.publish(payload)
        self._validate_published_check(
            (published,),
            payload,
            expected_app_id=decision.publisher_app_id,
        )
        self.broker.mark_route_published(
            decision.request_sha256,
            now=self.now(),
        )
        return "published"

    def _route_delivery_is_terminal(
        self,
        run: Mapping[str, object],
        request: RouteRequest,
    ) -> bool:
        if run.get("status") == "completed":
            return True
        jobs = self.api.workflow_jobs(request.workflow_run_id, request.run_attempt)
        route_name = ROUTE_JOB_NAMES[request.workflow_name]
        matches = [job for job in jobs if job.get("name") == route_name]
        if len(matches) > 1:
            raise RouteControllerError("route resolver job identity is ambiguous")
        return bool(matches and matches[0].get("status") == "completed")

    def _release_abandoned_decision(self, decision: RouteDecision) -> int:
        active = {
            assignment.assignment_id: assignment for assignment in self.broker.active_assignments()
        }
        released = 0
        for frozen in decision.document().assignments:
            assignment = active.get(frozen.assignment_id)
            if assignment is None:
                continue
            self.broker.release(
                assignment_id=assignment.assignment_id,
                lease_epoch=assignment.lease_epoch,
                reason="superseded",
                terminal_observed=True,
                now=self.now(),
            )
            released += 1
        return released

    def _validate_published_check(
        self,
        checks: Sequence[Mapping[str, object]],
        payload: Mapping[str, object],
        *,
        expected_app_id: int | None,
    ) -> None:
        if len(checks) != 1:
            raise RouteControllerError("route check identity is ambiguous")
        check = checks[0]
        output = check.get("output")
        expected_output = payload["output"]
        app = check.get("app")
        if (
            check.get("name") != payload["name"]
            or check.get("head_sha") != payload["head_sha"]
            or check.get("external_id") != payload["external_id"]
            or check.get("status") != payload["status"]
            or check.get("conclusion") != payload["conclusion"]
            or not isinstance(output, dict)
            or not isinstance(expected_output, dict)
            or output.get("title") != expected_output["title"]
            or output.get("summary") != expected_output["summary"]
            or not isinstance(app, dict)
            or app.get("id") != expected_app_id
        ):
            raise RouteControllerError("existing route check does not match assignment")

    def _reconcile_releases(self) -> int:
        grouped: dict[tuple[int, int], list[PlacementAssignment]] = defaultdict(list)
        for assignment in self.broker.active_assignments():
            if assignment.state is AssignmentState.ASSIGNED:
                grouped[(assignment.workflow_run_id, assignment.run_attempt)].append(assignment)
        released = 0
        for (run_id, attempt), assignments in sorted(grouped.items()):
            run = self.api.workflow_run(run_id)
            workflow_id = _exact_int(run.get("workflow_id"), "workflow_run.workflow_id")
            workflow_name = _workflow_name_for_id(workflow_id)
            observed_attempt = _exact_int(run.get("run_attempt"), "workflow_run.run_attempt")
            if observed_attempt < attempt:
                raise RouteControllerError("active assignment run attempt does not match")
            if observed_attempt > attempt:
                for assignment in assignments:
                    self.broker.release(
                        assignment_id=assignment.assignment_id,
                        lease_epoch=assignment.lease_epoch,
                        reason="superseded",
                        terminal_observed=True,
                    )
                    released += 1
                continue
            jobs = self.api.workflow_jobs(run_id, attempt)
            jobs_by_name: dict[str, list[Mapping[str, object]]] = defaultdict(list)
            for job in jobs:
                name = job.get("name")
                if isinstance(name, str):
                    jobs_by_name[name].append(job)
            run_terminal = run.get("status") == "completed"
            for assignment in assignments:
                expected_name = JOB_NAMES[workflow_name].get(assignment.job_key)
                if expected_name is None:
                    raise RouteControllerError("active assignment job key is outside contract")
                matches = jobs_by_name.get(expected_name, [])
                if len(matches) > 1:
                    raise RouteControllerError("workflow job identity is ambiguous")
                if not matches:
                    if not run_terminal:
                        continue
                    reason = "superseded"
                else:
                    job = matches[0]
                    if job.get("status") != "completed":
                        continue
                    conclusion = _exact_text(job.get("conclusion"), "job.conclusion")
                    reason = conclusion if conclusion in {"cancelled", "skipped"} else "completed"
                self.broker.release(
                    assignment_id=assignment.assignment_id,
                    lease_epoch=assignment.lease_epoch,
                    reason=reason,
                    terminal_observed=True,
                )
                released += 1
        return released


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--state-db",
        type=Path,
        default=Path("/var/lib/loom-ci-runner-pool/leases.sqlite3"),
    )
    parser.add_argument(
        "--profile",
        type=Path,
        default=Path("/etc/loom-ci-runner-pool/profile.toml"),
    )
    parser.add_argument("--repository", default=EXPECTED_REPOSITORY)
    parser.add_argument("--runtime-sha", required=True)
    parser.add_argument("--publisher-app-id", type=int, required=True)
    parser.add_argument("--publisher-installation-id", type=int, required=True)
    parser.add_argument("--publisher-app-private-key-file", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        publisher_private_key = _read_app_private_key(args.publisher_app_private_key_file)
        config = LeaseBrokerConfig.from_profile(args.profile)
        broker = CiRunnerLeaseBroker(args.state_db, config)
        publisher = GitHubAppRouteCheckPublisher(
            repository=args.repository,
            app_id=args.publisher_app_id,
            installation_id=args.publisher_installation_id,
            private_key_pem=publisher_private_key,
        )
        api = GitHubRouteAPI(
            repository=args.repository,
            token_provider=publisher.installation_token,
        )
        controller = CiRunnerRouteController(
            api=api,
            broker=broker,
            runtime_sha=args.runtime_sha,
            publisher=publisher,
        )
        result: object = controller.reconcile().public_dict()
    except (LeaseBrokerError, RouteControllerError, OSError, sqlite3.Error) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
