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
from datetime import UTC, datetime
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
    RouteAssignmentDocument,
    RouteRequest,
)

EXPECTED_REPOSITORY = "qianyi-sun/loom"
ARTIFACT_PREFIX = "loom-ci-route-request-v1-"
ROUTE_REQUEST_FILENAME = "loom-ci-route-request.json"
ROUTE_CHECK_PREFIX = "loom-ci-route-v1"
GITHUB_ACTIONS_APP_ID = 15368
MAX_ARTIFACT_BYTES = 64 * 1024
MAX_JSON_BYTES = 4 * 1024 * 1024
MAX_ROUTE_ARTIFACTS = 100
MAX_ARTIFACT_PAGES = 5
MAX_PUBLISHER_PAYLOAD_BYTES = 40 * 1024
PUBLISHER_POLL_SECONDS = 2.0
PUBLISHER_POLL_ATTEMPTS = 60
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
    def latest_artifact_id(self) -> int: ...

    def list_route_artifacts(self, after_id: int) -> tuple[int, Sequence[Mapping[str, object]]]: ...

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

    def _artifact_page(self, page: int) -> list[Mapping[str, object]]:
        payload = self._request("GET", f"/actions/artifacts?per_page=100&page={page}")
        artifacts = payload.get("artifacts") if isinstance(payload, dict) else None
        if not isinstance(artifacts, list):
            raise RouteControllerError("GitHub artifact inventory is malformed")
        return [item for item in artifacts if isinstance(item, dict)]

    def latest_artifact_id(self) -> int:
        artifacts = self._artifact_page(1)
        if not artifacts:
            return 0
        return max(_exact_int(item.get("id"), "artifact.id") for item in artifacts)

    def list_route_artifacts(self, after_id: int) -> tuple[int, Sequence[Mapping[str, object]]]:
        matches: list[Mapping[str, object]] = []
        highwater = after_id
        for page in range(1, MAX_ARTIFACT_PAGES + 1):
            artifacts = self._artifact_page(page)
            artifact_ids = [_exact_int(item.get("id"), "artifact.id") for item in artifacts]
            if artifact_ids:
                highwater = max(highwater, max(artifact_ids))
            matches.extend(
                item
                for item, artifact_id in zip(artifacts, artifact_ids, strict=True)
                if artifact_id > after_id
                and isinstance(item.get("name"), str)
                and str(item["name"]).startswith(ARTIFACT_PREFIX)
            )
            if len(artifacts) < 100 or any(artifact_id <= after_id for artifact_id in artifact_ids):
                return highwater, matches
        raise RouteControllerError("new GitHub artifact burst exceeds the scan bound")

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
    artifacts_seen: int
    routes_published: int
    routes_replayed: int
    assignments_released: int

    def public_dict(self) -> dict[str, int]:
        return {
            "artifacts_seen": self.artifacts_seen,
            "routes_published": self.routes_published,
            "routes_replayed": self.routes_replayed,
            "assignments_released": self.assignments_released,
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


def _read_artifact_cursor(path: Path) -> int:
    if not path.exists():
        raise RouteControllerError("artifact cursor is absent; initialize before routing")
    if path.is_symlink():
        raise RouteControllerError("artifact cursor must not be a symlink")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RouteControllerError("artifact cursor is unreadable or invalid") from exc
    if (
        not isinstance(value, dict)
        or set(value) != {"schema_version", "last_artifact_id"}
        or value.get("schema_version") != 1
    ):
        raise RouteControllerError("artifact cursor schema is invalid")
    artifact_id = value.get("last_artifact_id")
    if isinstance(artifact_id, bool) or not isinstance(artifact_id, int) or artifact_id < 0:
        raise RouteControllerError("artifact cursor identity is invalid")
    return artifact_id


def _write_artifact_cursor(path: Path, artifact_id: int) -> None:
    if path.is_symlink():
        raise RouteControllerError("artifact cursor must not be a symlink")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    payload = json.dumps(
        {"schema_version": 1, "last_artifact_id": artifact_id},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    descriptor = -1
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        path.chmod(0o600)
    except OSError as exc:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise RouteControllerError("artifact cursor could not be updated") from exc


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
        cursor_file: Path,
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
        self.cursor_file = cursor_file
        if len(publisher_key) < 32:
            raise RouteControllerError("route publisher key is too short")
        if publisher_poll_attempts < 1 or publisher_poll_seconds < 0:
            raise RouteControllerError("route publisher polling configuration is invalid")
        self.publisher_key = publisher_key
        self.publisher_poll_attempts = publisher_poll_attempts
        self.publisher_poll_seconds = publisher_poll_seconds
        self.sleep = sleep
        self.now = now or (lambda: datetime.now(UTC))

    def initialize_cursor(self) -> int:
        if self.cursor_file.exists() or self.cursor_file.is_symlink():
            raise RouteControllerError("artifact cursor already exists")
        artifact_id = self.api.latest_artifact_id()
        _write_artifact_cursor(self.cursor_file, artifact_id)
        return artifact_id

    def reconcile(self) -> ReconcileResult:
        seen, published, replayed = self._reconcile_requests()
        released = self._reconcile_releases()
        return ReconcileResult(seen, published, replayed, released)

    def _reconcile_requests(self) -> tuple[int, int, int]:
        cursor = _read_artifact_cursor(self.cursor_file)
        highwater, inventory = self.api.list_route_artifacts(cursor)
        artifacts = [
            item
            for item in inventory
            if isinstance(item.get("name"), str)
            and str(item["name"]).startswith(ARTIFACT_PREFIX)
            and item.get("expired") is False
            and _exact_int(item.get("id"), "artifact.id") > cursor
        ]
        if len(artifacts) > MAX_ROUTE_ARTIFACTS:
            raise RouteControllerError("too many live route artifacts")
        published = 0
        replayed = 0
        for artifact in sorted(
            artifacts, key=lambda item: _exact_int(item.get("id"), "artifact.id")
        ):
            artifact_id = _exact_int(artifact.get("id"), "artifact.id")
            request = _route_request_from_zip(self.api.download_artifact(artifact_id))
            self._validate_artifact(artifact, request)
            run = self.api.workflow_run(request.workflow_run_id)
            self._validate_run(run, request)
            if run.get("status") == "completed" and not self._route_check_exists(request):
                _write_artifact_cursor(self.cursor_file, artifact_id)
                continue
            workflow_path = WORKFLOW_PATHS[request.workflow_name]
            head_blob = self.api.content_blob_sha(workflow_path, request.head_sha)
            trusted_blob = self.api.content_blob_sha(workflow_path, self.candidate_sha)
            created_at = _github_timestamp(artifact.get("created_at"), "artifact.created_at")
            observed_at = self.now()
            if observed_at.tzinfo is None:
                raise RouteControllerError("controller observation time is invalid")
            age_seconds = (observed_at.astimezone(UTC) - created_at).total_seconds()
            allow_oldlab = (
                head_blob == trusted_blob and 0 <= age_seconds <= OLDLAB_REQUEST_MAX_AGE_SECONDS
            )
            document = self.broker.allocate_route(request, allow_oldlab=allow_oldlab)
            if self._publish_route(document, allow_oldlab=allow_oldlab):
                published += 1
            else:
                replayed += 1
            _write_artifact_cursor(self.cursor_file, artifact_id)
        if highwater > cursor:
            _write_artifact_cursor(self.cursor_file, highwater)
        return len(artifacts), published, replayed

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

    def _route_check_exists(self, request: RouteRequest) -> bool:
        name = (
            f"{ROUTE_CHECK_PREFIX}/{request.workflow_name}/"
            f"{request.workflow_run_id}/{request.run_attempt}"
        )
        request_sha = RouteAssignmentDocument.create(request, ()).request_sha256
        external_id = (
            f"{ROUTE_CHECK_PREFIX}:{request.workflow_id}:"
            f"{request.workflow_run_id}:{request.run_attempt}:{request_sha}"
        )
        return any(
            check.get("external_id") == external_id
            for check in self.api.check_runs(request.head_sha, name)
        )

    def _publish_route(self, document: RouteAssignmentDocument, *, allow_oldlab: bool) -> bool:
        name = (
            f"{ROUTE_CHECK_PREFIX}/{document.workflow_name}/"
            f"{document.workflow_run_id}/{document.run_attempt}"
        )
        external_id = (
            f"{ROUTE_CHECK_PREFIX}:{document.workflow_id}:"
            f"{document.workflow_run_id}:{document.run_attempt}:"
            f"{document.request_sha256}"
        )
        result = document.public_dict()
        result["oldlab_eligible"] = allow_oldlab
        summary = json.dumps(result, sort_keys=True, separators=(",", ":"))
        payload: dict[str, object] = {
            "name": name,
            "head_sha": document.head_sha,
            "external_id": external_id,
            "status": "completed",
            "conclusion": "success",
            "output": {"title": "oldlab-first route assignment", "summary": summary},
        }
        existing = list(self.api.check_runs(document.head_sha, name))
        if existing:
            self._validate_published_check(existing, payload)
            return False
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
            published = list(self.api.check_runs(document.head_sha, name))
            if published:
                self._validate_published_check(published, payload)
                return True
            if attempt + 1 < self.publisher_poll_attempts:
                self.sleep(self.publisher_poll_seconds)
        raise RouteControllerError("route publisher did not create the exact CheckRun in time")

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
            if run.get("run_attempt") != attempt:
                raise RouteControllerError("active assignment run attempt does not match")
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
    parser.add_argument(
        "--cursor-file",
        type=Path,
        default=Path("/var/lib/loom-ci-runner-pool/route-controller-cursor.json"),
    )
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--token-env", default="GH_TOKEN")
    parser.add_argument("--publisher-secret-file", type=Path, required=True)
    parser.add_argument("--initialize-cursor", action="store_true")
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
            cursor_file=args.cursor_file,
            publisher_key=publisher_key,
        )
        result: object = (
            {"initialized_cursor": controller.initialize_cursor()}
            if args.initialize_cursor
            else controller.reconcile().public_dict()
        )
    except (LeaseBrokerError, RouteControllerError, OSError, sqlite3.Error) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
