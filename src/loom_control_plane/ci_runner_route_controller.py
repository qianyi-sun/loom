"""Trusted GitHub transport for oldlab-first CI placement leases."""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import io
import json
import os
import re
import sqlite3
import stat
import sys
import time
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
)

EXPECTED_REPOSITORY = "qianyi-sun/loom"
ARTIFACT_PREFIX = "loom-ci-route-request-v1-"
ROUTE_REQUEST_FILENAME = "loom-ci-route-request.json"
ROUTE_CHECK_PREFIX = "loom-ci-route-v1"
GITHUB_ACTIONS_APP_ID = 15368
MAX_ARTIFACT_BYTES = 64 * 1024
MAX_JSON_BYTES = 4 * 1024 * 1024
MAX_ACTIVE_RUNS_PER_WORKFLOW = 100
MAX_PUBLISHER_PAYLOAD_BYTES = 40 * 1024
PUBLISHER_POLL_SECONDS = 2.0
PUBLISHER_POLL_ATTEMPTS = 60
PUBLISHER_RETRY_SECONDS = 300
ROUTE_DECISION_RETENTION_DAYS = 7
OLDLAB_REQUEST_MAX_AGE_SECONDS = 30
PUBLISHER_WORKFLOW = "ci-runner-route-publisher.yml"
ALLOWED_EVENTS = {"pull_request", "merge_group", "workflow_dispatch", "push"}
WORKFLOW_PATHS = {
    "CI": ".github/workflows/ci.yml",
    "images": ".github/workflows/images.yml",
    "cluster-smoke": ".github/workflows/cluster-smoke.yml",
    "staging-smoke": ".github/workflows/staging-smoke.yml",
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
MAX_TOKEN_BYTES = 4096
MAX_PUBLISHER_KEY_BYTES = 4096


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
    def active_workflow_runs(self, workflow_id: int) -> Sequence[Mapping[str, object]]: ...

    def route_artifact(
        self, *, workflow_id: int, workflow_run_id: int, run_attempt: int
    ) -> Mapping[str, object] | None: ...

    def download_artifact(self, artifact_id: int) -> bytes: ...

    def workflow_run(self, run_id: int) -> Mapping[str, object]: ...

    def content_blob_sha(self, path: str, ref: str) -> str: ...

    def check_runs(self, head_sha: str, name: str) -> Sequence[Mapping[str, object]]: ...

    def dispatch_route_publisher(
        self, *, candidate_sha: str, payload_b64: str, signature: str
    ) -> None: ...

    def workflow_jobs(self, run_id: int, attempt: int) -> Sequence[Mapping[str, object]]: ...


class GitHubRouteAPI:
    """Minimal outbound-only GitHub API used by the root-owned controller."""

    def __init__(self, *, repository: str, token: str) -> None:
        if repository != EXPECTED_REPOSITORY or not token:
            raise RouteControllerError("GitHub route API configuration is invalid")
        self.repository = repository
        self._token = token

    def _request(
        self,
        method: str,
        path: str,
        *,
        payload: Mapping[str, object] | None = None,
    ) -> Any:
        body = json.dumps(payload, separators=(",", ":")).encode() if payload is not None else None
        request = urllib.request.Request(
            f"https://api.github.com/repos/{self.repository}{path}",
            data=body,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
                "User-Agent": "loom-ci-route-controller/1",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
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

    def active_workflow_runs(self, workflow_id: int) -> Sequence[Mapping[str, object]]:
        payload = self._request(
            "GET",
            f"/actions/workflows/{workflow_id}/runs?status=in_progress&per_page="
            f"{MAX_ACTIVE_RUNS_PER_WORKFLOW}",
        )
        runs = payload.get("workflow_runs") if isinstance(payload, dict) else None
        total_count = payload.get("total_count") if isinstance(payload, dict) else None
        if (
            not isinstance(runs, list)
            or isinstance(total_count, bool)
            or not isinstance(total_count, int)
            or total_count < 0
            or any(not isinstance(item, dict) for item in runs)
        ):
            raise RouteControllerError("GitHub active workflow inventory is malformed")
        if total_count > MAX_ACTIVE_RUNS_PER_WORKFLOW:
            raise RouteControllerError("active workflow inventory exceeds the scan bound")
        if total_count != len(runs):
            raise RouteControllerError("GitHub active workflow inventory is malformed")
        return cast(list[dict[str, object]], runs)

    def route_artifact(
        self, *, workflow_id: int, workflow_run_id: int, run_attempt: int
    ) -> Mapping[str, object] | None:
        name = f"{ARTIFACT_PREFIX}{workflow_id}-{workflow_run_id}-{run_attempt}"
        encoded_name = urllib.parse.quote(name, safe="")
        payload = self._request(
            "GET", f"/actions/artifacts?name={encoded_name}&per_page=100"
        )
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
            if isinstance(item, dict)
            and item.get("name") == name
            and item.get("expired") is False
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
                "Authorization": f"Bearer {self._token}",
                "User-Agent": "loom-ci-route-controller/1",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        opener = urllib.request.build_opener(_NoRedirect())
        try:
            opener.open(initial, timeout=20)
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
            with urllib.request.urlopen(download, timeout=20) as response:
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
            f"/commits/{head_sha}/check-runs?check_name={encoded_name}&filter=all&per_page=100",
        )
        checks = payload.get("check_runs") if isinstance(payload, dict) else None
        if not isinstance(checks, list):
            raise RouteControllerError("GitHub check-run inventory is malformed")
        return [item for item in checks if isinstance(item, dict)]

    def dispatch_route_publisher(
        self, *, candidate_sha: str, payload_b64: str, signature: str
    ) -> None:
        result = self._request(
            "POST",
            f"/actions/workflows/{PUBLISHER_WORKFLOW}/dispatches",
            payload={
                "ref": "dev",
                "inputs": {
                    "candidate_sha": candidate_sha,
                    "payload_b64": payload_b64,
                    "signature": signature,
                },
            },
        )
        if result is not None:
            raise RouteControllerError("GitHub route publisher dispatch returned a body")

    def workflow_jobs(self, run_id: int, attempt: int) -> Sequence[Mapping[str, object]]:
        payload = self._request(
            "GET", f"/actions/runs/{run_id}/attempts/{attempt}/jobs?filter=all&per_page=100"
        )
        jobs = payload.get("jobs") if isinstance(payload, dict) else None
        if not isinstance(jobs, list):
            raise RouteControllerError("GitHub workflow jobs are malformed")
        return [item for item in jobs if isinstance(item, dict)]


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    requests_seen: int
    routes_published: int
    routes_replayed: int
    routes_pending: int
    routes_abandoned: int
    assignments_released: int
    decisions_pruned: int

    def public_dict(self) -> dict[str, int]:
        return {
            "requests_seen": self.requests_seen,
            "routes_published": self.routes_published,
            "routes_replayed": self.routes_replayed,
            "routes_pending": self.routes_pending,
            "routes_abandoned": self.routes_abandoned,
            "assignments_released": self.assignments_released,
            "decisions_pruned": self.decisions_pruned,
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


def _read_token(path: Path) -> str:
    try:
        metadata = path.lstat()
        raw = path.read_bytes()
    except OSError as exc:
        raise RouteControllerError("could not read GitHub token file") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise RouteControllerError("GitHub token source must be a regular file")
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise RouteControllerError("GitHub token file grants group or other access")
    if not raw or len(raw) > MAX_TOKEN_BYTES or b"\x00" in raw:
        raise RouteControllerError("GitHub token file has an invalid size or encoding")
    try:
        token = raw.decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise RouteControllerError("GitHub token file is not UTF-8") from exc
    if not token or any(character.isspace() for character in token):
        raise RouteControllerError("GitHub token file must contain one opaque token")
    return token


def _read_publisher_key(path: Path) -> bytes:
    try:
        metadata = path.lstat()
        raw = path.read_bytes()
    except OSError as exc:
        raise RouteControllerError("could not read route publisher key file") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise RouteControllerError("route publisher key source must be a regular file")
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise RouteControllerError("route publisher key file grants group or other access")
    if not raw or len(raw) > MAX_PUBLISHER_KEY_BYTES or b"\x00" in raw:
        raise RouteControllerError("route publisher key file has an invalid size or encoding")
    try:
        key = raw.decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise RouteControllerError("route publisher key file is not UTF-8") from exc
    if len(key) < 32 or any(character.isspace() for character in key):
        raise RouteControllerError("route publisher key must contain one strong opaque value")
    return key.encode()


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
        candidate_sha: str,
        publisher_key: bytes,
        publisher_poll_attempts: int = PUBLISHER_POLL_ATTEMPTS,
        publisher_poll_seconds: float = PUBLISHER_POLL_SECONDS,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if _SHA_RE.fullmatch(candidate_sha) is None:
            raise RouteControllerError("candidate SHA must be a full lowercase commit SHA")
        self.api = api
        self.broker = broker
        self.candidate_sha = candidate_sha
        if len(publisher_key) < 32:
            raise RouteControllerError("route publisher key is too short")
        if publisher_poll_attempts < 1 or publisher_poll_seconds < 0:
            raise RouteControllerError("route publisher polling configuration is invalid")
        self.publisher_key = publisher_key
        self.publisher_poll_attempts = publisher_poll_attempts
        self.publisher_poll_seconds = publisher_poll_seconds
        self.sleep = sleep
        self.now = now or (lambda: datetime.now(UTC))

    def reconcile(self) -> ReconcileResult:
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
                trusted_blob = self.api.content_blob_sha(workflow_path, self.candidate_sha)
                created_at = _github_timestamp(
                    artifact.get("created_at"), "artifact.created_at"
                )
                observed_at = self.now()
                if observed_at.tzinfo is None:
                    raise RouteControllerError("controller observation time is invalid")
                age_seconds = (observed_at.astimezone(UTC) - created_at).total_seconds()
                allow_oldlab = (
                    head_blob == trusted_blob
                    and 0 <= age_seconds <= OLDLAB_REQUEST_MAX_AGE_SECONDS
                )
                decision = self.broker.decide_route(
                    request,
                    allow_oldlab=allow_oldlab,
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
        existing = list(
            self.api.check_runs(decision.head_sha, cast(str, payload["name"]))
        )
        if existing:
            self._validate_published_check(existing, payload)
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
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        if len(canonical) > MAX_PUBLISHER_PAYLOAD_BYTES:
            raise RouteControllerError("route publisher payload exceeds the size limit")
        payload_b64 = base64.b64encode(canonical).decode("ascii")
        signature = hmac.new(self.publisher_key, canonical, hashlib.sha256).hexdigest()
        self.api.dispatch_route_publisher(
            candidate_sha=self.candidate_sha,
            payload_b64=payload_b64,
            signature=signature,
        )
        for attempt in range(self.publisher_poll_attempts):
            published = list(
                self.api.check_runs(decision.head_sha, cast(str, payload["name"]))
            )
            if published:
                self._validate_published_check(published, payload)
                self.broker.mark_route_published(
                    decision.request_sha256,
                    now=self.now(),
                )
                return "published"
            if attempt + 1 < self.publisher_poll_attempts:
                self.sleep(self.publisher_poll_seconds)
        return "pending"

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
            assignment.assignment_id: assignment
            for assignment in self.broker.active_assignments()
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

    @staticmethod
    def _validate_published_check(
        checks: Sequence[Mapping[str, object]], payload: Mapping[str, object]
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
            or app.get("id") != GITHUB_ACTIONS_APP_ID
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
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--token-env", default="GH_TOKEN")
    parser.add_argument("--publisher-secret-file", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        token = (
            _read_token(args.token_file)
            if args.token_file is not None
            else os.environ.get(args.token_env, "")
        )
        if not token:
            raise RouteControllerError("GitHub token environment variable is absent")
        publisher_key = _read_publisher_key(args.publisher_secret_file)
        config = LeaseBrokerConfig.from_profile(args.profile)
        broker = CiRunnerLeaseBroker(args.state_db, config)
        api = GitHubRouteAPI(repository=args.repository, token=token)
        controller = CiRunnerRouteController(
            api=api,
            broker=broker,
            candidate_sha=args.candidate_sha,
            publisher_key=publisher_key,
        )
        result: object = controller.reconcile().public_dict()
    except (LeaseBrokerError, RouteControllerError, OSError, sqlite3.Error) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
