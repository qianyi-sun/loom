#!/usr/bin/env python3
"""Prepare and consume the immutable oldlab-first route protocol."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

SCHEMA_VERSION = 1
EXPECTED_REPOSITORY = "qianyi-sun/loom"
ACTIVE_MODE = "oldlab-preferred-v1"
DISABLED_MODE = "disabled"
CHECK_PREFIX = "loom-ci-route-v1"
MAX_JSON_BYTES = 4 * 1024 * 1024
POLL_SECONDS = 2
POLL_TIMEOUT_SECONDS = 240
SHARED_OLDLAB_LABELS = (
    "self-hosted",
    "linux",
    "x64",
    "loom-ci",
    "oldlab-5",
    "ephemeral-kvm",
)
CONTRACTS = {
    "CI": {
        "workflow_id": 302898379,
        "work_class": "normal",
        "capacity": 5,
        "hosted": ("ubuntu-latest",),
        "jobs": (
            "lint-and-static",
            "tests-root-1-of-2",
            "tests-root-2-of-2",
            "tests-packages",
            "runtime-payload",
            "go-checks",
            "web-checks",
            "integration-1-of-2",
            "integration-2-of-2",
            "integration-docker",
        ),
    },
    "images": {
        "workflow_id": 302898384,
        "work_class": "image",
        "capacity": 4,
        "hosted": ("ubuntu-24.04",),
        "jobs": (
            "agent-sandbox",
            "control-plane",
            "egress-xds",
            "family-orchestrator",
            "llm-gateway",
            "llm-gateway-sandbox",
            "service",
            "web",
            "staging-admin-browser-smoke",
            "rehearsal-postgres",
            "worker",
        ),
    },
    "cluster-smoke": {
        "workflow_id": 302898381,
        "work_class": "smoke",
        "capacity": 2,
        "hosted": ("ubuntu-latest",),
        "jobs": ("cluster-contract",),
    },
    "staging-smoke": {
        "workflow_id": 302898388,
        "work_class": "smoke",
        "capacity": 2,
        "hosted": ("ubuntu-latest",),
        "jobs": ("system-smoke",),
    },
}
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


class RouteActionError(RuntimeError):
    """A bounded, secret-free route action failure."""


def _exact_positive_int(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise RouteActionError(f"{field} must be a positive integer")
    try:
        parsed = int(cast(Any, value))
    except (TypeError, ValueError) as exc:
        raise RouteActionError(f"{field} must be a positive integer") from exc
    if parsed < 1 or str(parsed) != str(value):
        raise RouteActionError(f"{field} must be a positive integer")
    return parsed


def _job_keys(value: str, allowed: Sequence[str]) -> tuple[str, ...]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise RouteActionError("job keys are not valid JSON") from exc
    if (
        not isinstance(parsed, list)
        or not parsed
        or any(not isinstance(item, str) for item in parsed)
    ):
        raise RouteActionError("job keys must be a non-empty string array")
    keys = tuple(parsed)
    if len(keys) != len(set(keys)):
        raise RouteActionError("job keys must be unique")
    if not set(keys) <= set(allowed):
        raise RouteActionError("job key is outside the workflow contract")
    return keys


def build_request(environment: Mapping[str, str]) -> tuple[dict[str, object], str]:
    repository = environment.get("ROUTE_REPOSITORY", "")
    workflow_name = environment.get("ROUTE_WORKFLOW_NAME", "")
    head_sha = environment.get("ROUTE_HEAD_SHA", "")
    if repository != EXPECTED_REPOSITORY:
        raise RouteActionError("repository is outside the route contract")
    contract = CONTRACTS.get(workflow_name)
    if contract is None:
        raise RouteActionError("workflow is outside the route contract")
    workflow_id = _exact_positive_int(environment.get("ROUTE_WORKFLOW_ID"), "workflow id")
    if workflow_id != contract["workflow_id"]:
        raise RouteActionError("workflow id does not match its name")
    if _SHA_RE.fullmatch(head_sha) is None:
        raise RouteActionError("head SHA must be a full lowercase commit SHA")
    keys = _job_keys(environment.get("ROUTE_JOB_KEYS_JSON", ""), contract["jobs"])
    request = {
        "schema_version": SCHEMA_VERSION,
        "repository": repository,
        "workflow_name": workflow_name,
        "workflow_id": workflow_id,
        "workflow_run_id": _exact_positive_int(
            environment.get("ROUTE_WORKFLOW_RUN_ID"), "workflow run id"
        ),
        "run_attempt": _exact_positive_int(environment.get("ROUTE_RUN_ATTEMPT"), "run attempt"),
        "head_sha": head_sha,
        "job_keys": list(keys),
    }
    mode = environment.get("ROUTE_MODE", "")
    if mode not in {ACTIVE_MODE, DISABLED_MODE}:
        raise RouteActionError("route mode is invalid")
    return request, mode


def _canonical_sha(value: Mapping[str, object]) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


def _write_output(path: Path, name: str, value: object) -> None:
    serialized = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))
    if "\n" in serialized or "\r" in serialized:
        raise RouteActionError("workflow output contains a newline")
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{name}={serialized}\n")


def prepare(environment: Mapping[str, str], output_path: Path) -> dict[str, object]:
    request, mode = build_request(environment)
    request_path = Path(environment.get("ROUTE_REQUEST_PATH", ""))
    if not request_path.is_absolute():
        raise RouteActionError("route request path must be absolute")
    if mode == DISABLED_MODE:
        contract = CONTRACTS[cast(str, request["workflow_name"])]
        routes = {key: list(contract["hosted"]) for key in request["job_keys"]}
        _write_output(output_path, "active", "false")
        _write_output(output_path, "routes", routes)
        return routes
    request_path.write_text(
        json.dumps(request, sort_keys=True, separators=(",", ":")), encoding="utf-8"
    )
    request_path.chmod(0o600)
    artifact_name = (
        f"loom-ci-route-request-v1-{request['workflow_id']}-"
        f"{request['workflow_run_id']}-{request['run_attempt']}"
    )
    _write_output(output_path, "active", "true")
    _write_output(output_path, "artifact-name", artifact_name)
    return request


def validate_assignment(
    request: Mapping[str, object], response: Mapping[str, object]
) -> dict[str, list[str]]:
    expected_response_keys = {
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
    }
    if set(response) != expected_response_keys:
        raise RouteActionError("route response fields do not match schema 1")
    for field in (
        "repository",
        "workflow_name",
        "workflow_id",
        "workflow_run_id",
        "run_attempt",
        "head_sha",
    ):
        if response.get(field) != request.get(field):
            raise RouteActionError(f"route response {field} does not match request")
    if response.get("schema_version") != SCHEMA_VERSION:
        raise RouteActionError("route response schema version is invalid")
    request_sha = _canonical_sha(request)
    if response.get("request_sha256") != request_sha:
        raise RouteActionError("route response request digest does not match")
    oldlab_eligible = response.get("oldlab_eligible")
    if not isinstance(oldlab_eligible, bool):
        raise RouteActionError("route response eligibility is invalid")
    assignments = response.get("assignments")
    if not isinstance(assignments, list) or len(assignments) != len(request["job_keys"]):
        raise RouteActionError("route response assignment count does not match")
    workflow_name = cast(str, request["workflow_name"])
    contract = CONTRACTS[workflow_name]
    expected_oldlab = (*SHARED_OLDLAB_LABELS, f"loom-ci-{contract['work_class']}")
    expected_hosted = cast(tuple[str, ...], contract["hosted"])
    expected_keys = cast(list[str], request["job_keys"])
    routes: dict[str, list[str]] = {}
    oldlab_slots: set[int] = set()
    assignment_keys = {
        "assignment_id",
        "repository",
        "workflow_run_id",
        "run_attempt",
        "job_key",
        "head_sha",
        "work_class",
        "target",
        "slot",
        "lease_epoch",
        "state",
        "runs_on",
        "created_at",
        "lease_expires_at",
        "released_at",
        "release_reason",
    }
    for expected_key, raw in zip(expected_keys, assignments, strict=True):
        if not isinstance(raw, dict):
            raise RouteActionError("route assignment is not an object")
        if set(raw) != assignment_keys:
            raise RouteActionError("route assignment fields do not match schema 1")
        if (
            raw.get("repository") != request["repository"]
            or raw.get("workflow_run_id") != request["workflow_run_id"]
            or raw.get("run_attempt") != request["run_attempt"]
            or raw.get("head_sha") != request["head_sha"]
            or raw.get("job_key") != expected_key
            or raw.get("work_class") != contract["work_class"]
            or raw.get("state") != "assigned"
        ):
            raise RouteActionError("route assignment identity does not match")
        target = raw.get("target")
        runs_on = raw.get("runs_on")
        slot = raw.get("slot")
        if (
            isinstance(raw.get("assignment_id"), bool)
            or not isinstance(raw.get("assignment_id"), int)
            or cast(int, raw["assignment_id"]) < 1
            or isinstance(raw.get("lease_epoch"), bool)
            or not isinstance(raw.get("lease_epoch"), int)
            or cast(int, raw["lease_epoch"]) < 1
            or not isinstance(raw.get("created_at"), str)
            or raw.get("released_at") is not None
            or raw.get("release_reason") is not None
            or not isinstance(runs_on, list)
            or any(not isinstance(label, str) for label in runs_on)
        ):
            raise RouteActionError("route assignment metadata is invalid")
        if target == "oldlab":
            if not oldlab_eligible or runs_on != list(expected_oldlab):
                raise RouteActionError("oldlab assignment is not eligible or labeled exactly")
            if not isinstance(raw.get("lease_expires_at"), str):
                raise RouteActionError("oldlab assignment lease deadline is invalid")
            if isinstance(slot, bool) or not isinstance(slot, int):
                raise RouteActionError("oldlab assignment slot is invalid")
            if not 0 <= slot < cast(int, contract["capacity"]) or slot in oldlab_slots:
                raise RouteActionError("oldlab assignment slot is duplicated or out of range")
            oldlab_slots.add(slot)
        elif target == "github_hosted":
            if (
                slot is not None
                or raw.get("lease_expires_at") is not None
                or runs_on != list(expected_hosted)
            ):
                raise RouteActionError("hosted assignment is not labeled exactly")
        else:
            raise RouteActionError("route assignment target is invalid")
        routes[expected_key] = cast(list[str], runs_on)
    return routes


def _get_json(url: str, token: str) -> Mapping[str, object]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "loom-ci-route-action/1",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            raw = cast(bytes, response.read(MAX_JSON_BYTES + 1))
    except urllib.error.HTTPError as exc:
        if 500 <= exc.code < 600:
            raise TimeoutError from exc
        raise RouteActionError(f"GitHub check query failed with HTTP {exc.code}") from None
    except (OSError, TimeoutError) as exc:
        raise TimeoutError from exc
    if len(raw) > MAX_JSON_BYTES:
        raise RouteActionError("GitHub check response exceeds the size limit")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RouteActionError("GitHub check response is not valid JSON") from exc
    if not isinstance(value, dict):
        raise RouteActionError("GitHub check response is not an object")
    return value


def poll(environment: Mapping[str, str], output_path: Path) -> dict[str, list[str]]:
    token = environment.get("GITHUB_TOKEN", "")
    if not token or any(character.isspace() for character in token):
        raise RouteActionError("GitHub token is absent or malformed")
    request_path = Path(environment.get("ROUTE_REQUEST_PATH", ""))
    try:
        request = json.loads(request_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RouteActionError("route request is unreadable") from exc
    if not isinstance(request, dict):
        raise RouteActionError("route request is not an object")
    validated, mode = build_request(
        {
            "ROUTE_MODE": ACTIVE_MODE,
            "ROUTE_REPOSITORY": str(request.get("repository", "")),
            "ROUTE_WORKFLOW_NAME": str(request.get("workflow_name", "")),
            "ROUTE_WORKFLOW_ID": str(request.get("workflow_id", "")),
            "ROUTE_WORKFLOW_RUN_ID": str(request.get("workflow_run_id", "")),
            "ROUTE_RUN_ATTEMPT": str(request.get("run_attempt", "")),
            "ROUTE_HEAD_SHA": str(request.get("head_sha", "")),
            "ROUTE_JOB_KEYS_JSON": json.dumps(request.get("job_keys")),
        }
    )
    if mode != ACTIVE_MODE or validated != request:
        raise RouteActionError("stored route request is not canonical")
    name = (
        f"{CHECK_PREFIX}/{request['workflow_name']}/{request['workflow_run_id']}/"
        f"{request['run_attempt']}"
    )
    external_id = (
        f"{CHECK_PREFIX}:{request['workflow_id']}:{request['workflow_run_id']}:"
        f"{request['run_attempt']}:{_canonical_sha(request)}"
    )
    encoded_name = urllib.parse.quote(name, safe="")
    url = (
        f"https://api.github.com/repos/{request['repository']}/commits/"
        f"{request['head_sha']}/check-runs?check_name={encoded_name}&per_page=100"
    )
    deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        try:
            payload = _get_json(url, token)
        except TimeoutError:
            time.sleep(POLL_SECONDS)
            continue
        checks = payload.get("check_runs")
        if not isinstance(checks, list):
            raise RouteActionError("GitHub check inventory is malformed")
        matches = [
            check
            for check in checks
            if isinstance(check, dict) and check.get("external_id") == external_id
        ]
        if len(matches) > 1:
            raise RouteActionError("trusted route check identity is ambiguous")
        if matches:
            check = matches[0]
            output = check.get("output")
            if check.get("status") != "completed" or check.get("conclusion") != "success":
                raise RouteActionError("trusted route check is not successful")
            summary = output.get("summary") if isinstance(output, dict) else None
            try:
                response = json.loads(summary) if isinstance(summary, str) else None
            except json.JSONDecodeError as exc:
                raise RouteActionError("trusted route summary is invalid JSON") from exc
            if not isinstance(response, dict):
                raise RouteActionError("trusted route summary is not an object")
            routes = validate_assignment(request, response)
            _write_output(output_path, "routes", routes)
            return routes
        time.sleep(POLL_SECONDS)
    raise RouteActionError("timed out waiting for trusted route assignment")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("request", "poll"))
    parser.add_argument("--github-output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.operation == "request":
            prepare(os.environ, args.github_output)
        else:
            poll(os.environ, args.github_output)
    except RouteActionError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
