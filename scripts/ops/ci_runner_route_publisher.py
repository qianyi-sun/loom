#!/usr/bin/env python3
"""Publish one HMAC-authorized oldlab route CheckRun with a workflow token."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from typing import Any, Protocol, cast

EXPECTED_REPOSITORY = "qianyi-sun/loom"
ROUTE_CHECK_PREFIX = "loom-ci-route-v1"
ROUTE_CHECK_TITLE = "oldlab-first route assignment"
GITHUB_ACTIONS_APP_ID = 15368
MAX_PAYLOAD_BYTES = 40 * 1024
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
WORKFLOW_IDS = {
    "CI": 302898379,
    "images": 302898384,
    "cluster-smoke": 302898381,
    "staging-smoke": 302898388,
}
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")


class RoutePublisherError(RuntimeError):
    """A bounded, secret-free publisher failure."""


class RoutePublisherAPI(Protocol):
    def check_runs(self, head_sha: str, name: str) -> Sequence[Mapping[str, object]]: ...

    def create_check_run(self, payload: Mapping[str, object]) -> Mapping[str, object]: ...


class GitHubRoutePublisherAPI:
    def __init__(self, *, repository: str, token: str) -> None:
        if repository != EXPECTED_REPOSITORY or not token:
            raise RoutePublisherError("GitHub publisher configuration is invalid")
        self.repository = repository
        self._token = token

    def _request(
        self, method: str, path: str, *, payload: Mapping[str, object] | None = None
    ) -> Any:
        body = json.dumps(payload, separators=(",", ":")).encode() if payload else None
        request = urllib.request.Request(
            f"https://api.github.com/repos/{self.repository}{path}",
            data=body,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
                "User-Agent": "loom-ci-route-publisher/1",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                raw = cast(bytes, response.read(MAX_RESPONSE_BYTES + 1))
        except urllib.error.HTTPError as exc:
            raise RoutePublisherError(
                f"GitHub API {method} {path} failed with HTTP {exc.code}"
            ) from None
        except (OSError, TimeoutError) as exc:
            raise RoutePublisherError(f"GitHub API {method} {path} failed") from exc
        if len(raw) > MAX_RESPONSE_BYTES:
            raise RoutePublisherError("GitHub API response exceeds the size limit")
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RoutePublisherError("GitHub API returned invalid JSON") from exc
        if not isinstance(value, dict):
            raise RoutePublisherError("GitHub API response is not an object")
        return value

    def check_runs(self, head_sha: str, name: str) -> Sequence[Mapping[str, object]]:
        encoded_name = urllib.parse.quote(name, safe="")
        response = self._request(
            "GET",
            f"/commits/{head_sha}/check-runs?check_name={encoded_name}&filter=all&per_page=100",
        )
        checks = response.get("check_runs")
        if not isinstance(checks, list):
            raise RoutePublisherError("GitHub check-run inventory is malformed")
        return [item for item in checks if isinstance(item, dict)]

    def create_check_run(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        return cast(Mapping[str, object], self._request("POST", "/check-runs", payload=payload))


def _canonical_json(value: Mapping[str, object]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _load_signed_payload(environment: Mapping[str, str]) -> dict[str, object]:
    encoded = environment.get("ROUTE_PUBLISH_PAYLOAD_B64", "")
    signature = environment.get("ROUTE_PUBLISH_SIGNATURE", "")
    key_text = environment.get("LOOM_CI_ROUTE_PUBLISH_HMAC_KEY", "").strip()
    if not encoded or len(encoded) > ((MAX_PAYLOAD_BYTES + 2) // 3) * 4:
        raise RoutePublisherError("route publisher payload size is invalid")
    if _DIGEST_RE.fullmatch(signature) is None:
        raise RoutePublisherError("route publisher signature is invalid")
    if len(key_text) < 32 or any(character.isspace() for character in key_text):
        raise RoutePublisherError("route publisher key is absent or invalid")
    try:
        raw = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise RoutePublisherError("route publisher payload is not valid base64") from exc
    if not raw or len(raw) > MAX_PAYLOAD_BYTES:
        raise RoutePublisherError("route publisher payload size is invalid")
    expected = hmac.new(key_text.encode(), raw, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        raise RoutePublisherError("route publisher signature does not match")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RoutePublisherError("route publisher payload is not valid JSON") from exc
    if not isinstance(value, dict) or _canonical_json(value) != raw:
        raise RoutePublisherError("route publisher payload is not canonical JSON")
    return value


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise RoutePublisherError(f"{field} is invalid")
    return value


def _validate_payload(payload: Mapping[str, object]) -> None:
    if set(payload) != {
        "name",
        "head_sha",
        "external_id",
        "status",
        "conclusion",
        "output",
    }:
        raise RoutePublisherError("route publisher CheckRun fields do not match the contract")
    head_sha = payload.get("head_sha")
    if not isinstance(head_sha, str) or _SHA_RE.fullmatch(head_sha) is None:
        raise RoutePublisherError("route publisher head SHA is invalid")
    if payload.get("status") != "completed" or payload.get("conclusion") != "success":
        raise RoutePublisherError("route publisher terminal state is invalid")
    output = payload.get("output")
    if (
        not isinstance(output, dict)
        or set(output) != {"title", "summary"}
        or output.get("title") != ROUTE_CHECK_TITLE
        or not isinstance(output.get("summary"), str)
    ):
        raise RoutePublisherError("route publisher output is invalid")
    try:
        summary = json.loads(cast(str, output["summary"]))
    except json.JSONDecodeError as exc:
        raise RoutePublisherError("route publisher summary is not valid JSON") from exc
    if not isinstance(summary, dict) or cast(str, output["summary"]).encode() != _canonical_json(
        summary
    ):
        raise RoutePublisherError("route publisher summary is not canonical JSON")
    if set(summary) != {
        "schema_version",
        "repository",
        "workflow_name",
        "workflow_id",
        "workflow_run_id",
        "run_attempt",
        "head_sha",
        "request_sha256",
        "assignments",
        "oldlab_eligible",
    }:
        raise RoutePublisherError("route publisher summary fields do not match schema 1")
    workflow_name = summary.get("workflow_name")
    workflow_id = _positive_int(summary.get("workflow_id"), "workflow id")
    run_id = _positive_int(summary.get("workflow_run_id"), "workflow run id")
    attempt = _positive_int(summary.get("run_attempt"), "run attempt")
    request_sha = summary.get("request_sha256")
    if (
        summary.get("schema_version") != 1
        or summary.get("repository") != EXPECTED_REPOSITORY
        or not isinstance(workflow_name, str)
        or WORKFLOW_IDS.get(workflow_name) != workflow_id
        or summary.get("head_sha") != head_sha
        or not isinstance(request_sha, str)
        or _DIGEST_RE.fullmatch(request_sha) is None
        or not isinstance(summary.get("oldlab_eligible"), bool)
        or not isinstance(summary.get("assignments"), list)
        or not summary["assignments"]
        or any(not isinstance(item, dict) for item in summary["assignments"])
    ):
        raise RoutePublisherError("route publisher summary identity is invalid")
    expected_name = f"{ROUTE_CHECK_PREFIX}/{workflow_name}/{run_id}/{attempt}"
    expected_external_id = f"{ROUTE_CHECK_PREFIX}:{workflow_id}:{run_id}:{attempt}:{request_sha}"
    if payload.get("name") != expected_name or payload.get("external_id") != expected_external_id:
        raise RoutePublisherError("route publisher CheckRun identity does not match its summary")


def _check_matches(check: Mapping[str, object], payload: Mapping[str, object]) -> bool:
    output = check.get("output")
    expected_output = payload["output"]
    app = check.get("app")
    return (
        check.get("name") == payload["name"]
        and check.get("head_sha") == payload["head_sha"]
        and check.get("external_id") == payload["external_id"]
        and check.get("status") == "completed"
        and check.get("conclusion") == "success"
        and isinstance(output, dict)
        and isinstance(expected_output, dict)
        and output.get("title") == expected_output["title"]
        and output.get("summary") == expected_output["summary"]
        and isinstance(app, dict)
        and app.get("id") == GITHUB_ACTIONS_APP_ID
    )


def publish(api: RoutePublisherAPI, payload: Mapping[str, object]) -> str:
    _validate_payload(payload)
    checks = list(api.check_runs(cast(str, payload["head_sha"]), cast(str, payload["name"])))
    if len(checks) > 1:
        raise RoutePublisherError("route publisher CheckRun identity is ambiguous")
    if checks:
        if not _check_matches(checks[0], payload):
            raise RoutePublisherError("existing route publisher CheckRun does not match")
        return "replayed"
    created = api.create_check_run(payload)
    if not _check_matches(created, payload):
        raise RoutePublisherError("GitHub did not return the exact route publisher CheckRun")
    return "created"


def main() -> int:
    try:
        payload = _load_signed_payload(os.environ)
        token = os.environ.get("GITHUB_TOKEN", "")
        if not token:
            raise RoutePublisherError("GitHub workflow token is absent")
        api = GitHubRoutePublisherAPI(repository=EXPECTED_REPOSITORY, token=token)
        result = publish(api, payload)
    except RoutePublisherError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps({"route_check": result}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
