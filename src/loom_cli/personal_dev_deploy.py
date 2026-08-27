"""Authenticated client for one source-fresh personal-development deployment."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx

from loom.personal_dev_expected_denial import (
    EXPECTED_HIDDEN_DENIAL_PHASE_HEADER,
    expected_hidden_denial_phase,
)
from loom.personal_dev_source import PersonalDevSourceSnapshotV1
from loom_cli.server_client import assert_2xx


class PersonalDevDeployError(RuntimeError):
    """The remote personal-development apply did not satisfy its bindings."""


def _object(value: object, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise PersonalDevDeployError(f"{label} response is invalid")
    return value


def _candidate(value: object, *, expected_sha: str | None = None) -> dict[str, Any]:
    candidate = _object(value, label="personal-dev candidate")
    try:
        UUID(str(candidate["id"]))
        digest = candidate["candidate_sha"]
        status = candidate["status"]
    except (KeyError, TypeError, ValueError):
        raise PersonalDevDeployError("personal-dev candidate response is invalid") from None
    if (
        not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
        or status not in {"uploaded", "queued", "building", "ready", "failed"}
        or candidate.get("attestation_scope") != "personal-dev-only"
        or candidate.get("promotable") is not False
        or (expected_sha is not None and digest != expected_sha)
    ):
        raise PersonalDevDeployError("personal-dev candidate response binding is invalid")
    return candidate


def _environment(
    value: object,
    *,
    expected_name: str,
    expected_candidate_sha: str | None = None,
    expected_min_slots: int | None = None,
    expected_max_slots: int | None = None,
    expected_operation_epoch: int | None = None,
) -> dict[str, Any]:
    environment = _object(value, label="personal-dev environment")
    try:
        name = environment["name"]
        status = environment["status"]
        operation_epoch = environment["operation_epoch"]
    except KeyError:
        raise PersonalDevDeployError("personal-dev environment response is invalid") from None
    if (
        name != expected_name
        or status
        not in {
            "provisioning",
            "ready",
            "updating",
            "activating",
            "deleting",
            "draining",
            "failed",
            "deleted",
        }
        or type(operation_epoch) is not int
        or operation_epoch < 0
        or (
            expected_candidate_sha is not None
            and environment.get("candidate_sha") != expected_candidate_sha
        )
        or (
            expected_min_slots is not None
            and environment.get("min_slots") != expected_min_slots
        )
        or (
            expected_max_slots is not None
            and environment.get("max_slots") != expected_max_slots
        )
        or (
            expected_operation_epoch is not None
            and operation_epoch != expected_operation_epoch
        )
    ):
        raise PersonalDevDeployError("personal-dev environment response binding is invalid")
    return environment


def _operation(
    value: object,
    *,
    expected_id: str | None = None,
    expected_name: str,
    expected_candidate_sha: str,
    expected_min_slots: int,
    expected_max_slots: int,
    expected_operation_epoch: int,
) -> dict[str, Any]:
    operation = _object(value, label="personal-dev operation")
    try:
        operation_id = str(UUID(str(operation["id"])))
        state = operation["state"]
    except (KeyError, TypeError, ValueError):
        raise PersonalDevDeployError("personal-dev operation response is invalid") from None
    if (
        (expected_id is not None and operation_id != expected_id)
        or operation.get("environment_name") != expected_name
        or operation.get("candidate_sha") != expected_candidate_sha
        or operation.get("min_slots") != expected_min_slots
        or operation.get("max_slots") != expected_max_slots
        or operation.get("operation_epoch") != expected_operation_epoch
        or state
        not in {
            "requested",
            "running",
            "activating",
            "succeeded",
            "failed",
            "cancelling",
            "cancelled",
        }
    ):
        raise PersonalDevDeployError("personal-dev operation response binding is invalid")
    return operation


class PersonalDevDeployClient:
    """Bind source, immutable candidate, capacity, and environment epoch."""

    def __init__(self, client: httpx.Client) -> None:
        if not isinstance(client, httpx.Client):
            raise TypeError("personal-dev deploy client requires a synchronous HTTP client")
        self._client = client

    def upload_snapshot(
        self,
        archive_path: Path,
        snapshot: PersonalDevSourceSnapshotV1,
    ) -> dict[str, Any]:
        with archive_path.open("rb") as archive:
            response = self._client.post(
                "/api/v1/personal-dev-candidates",
                data={
                    "source_sha256": snapshot.source_digest,
                    "archive_sha256": snapshot.archive_sha256,
                },
                files={"source": ("personal-dev-source.tar", archive, "application/x-tar")},
            )
        body = assert_2xx(response, action="upload the sealed personal-dev source")
        candidate = _candidate(body)
        if (
            candidate.get("source_sha256") != snapshot.source_digest
            or candidate.get("archive_sha256") != snapshot.archive_sha256
            or candidate.get("attestation_scope") != "personal-dev-only"
            or candidate.get("promotable") is not False
        ):
            raise PersonalDevDeployError("personal-dev candidate source binding is invalid")
        return candidate

    def resolve_ready_candidate(self, candidate_sha: str) -> dict[str, Any]:
        response = self._client.get(
            "/api/v1/personal-dev-candidates",
            params={"mine": "true", "limit": "500"},
        )
        body = assert_2xx(response, action="resolve the owned personal-dev candidate")
        items = body.get("items")
        if not isinstance(items, list):
            raise PersonalDevDeployError("personal-dev candidate listing is invalid")
        matches = [
            _candidate(item, expected_sha=candidate_sha)
            for item in items
            if isinstance(item, dict) and item.get("candidate_sha") == candidate_sha
        ]
        ready = [item for item in matches if item["status"] == "ready"]
        if len(ready) != 1:
            raise PersonalDevDeployError(
                "--candidate must identify exactly one owned, retained, ready "
                "personal-dev-only candidate"
            )
        return ready[0]

    def expected_operation_epoch(self, name: str) -> int:
        response = self._client.get(f"/api/v1/dev-instances/{name}")
        if response.status_code == 404:
            return 0
        body = assert_2xx(response, action=f"resolve development environment {name!r}")
        environment = _environment(body, expected_name=name)
        if environment["status"] != "ready":
            raise PersonalDevDeployError(
                f"development environment {name!r} is {environment['status']}; "
                "wait for or resolve its current lifecycle operation before applying another"
            )
        return int(environment["operation_epoch"])

    def _request_apply(
        self,
        *,
        name: str,
        candidate: Mapping[str, Any],
        min_slots: int,
        max_slots: int,
        expected_operation_epoch: int,
        idempotency_key: UUID | None = None,
    ) -> httpx.Response:
        return self._client.put(
            f"/api/v1/dev-instances/{name}",
            json={
                "candidate_id": str(candidate["id"]),
                "candidate_sha": candidate["candidate_sha"],
                "min_slots": min_slots,
                "max_slots": max_slots,
                "expected_operation_epoch": expected_operation_epoch,
                "idempotency_key": str(idempotency_key or uuid4()),
            },
        )

    def apply_expected_hidden_denial(
        self,
        *,
        name: str,
        candidate: Mapping[str, Any],
        min_slots: int,
        max_slots: int,
        expected_operation_epoch: int,
        idempotency_key: UUID | None = None,
    ) -> bool:
        """Return whether the exact target PUT was hidden with HTTP 404.

        The response body is intentionally never parsed or copied into output.
        """

        response = self._request_apply(
            name=name,
            candidate=candidate,
            min_slots=min_slots,
            max_slots=max_slots,
            expected_operation_epoch=expected_operation_epoch,
            idempotency_key=idempotency_key,
        )
        return (
            response.status_code == 404
            and response.headers.get(EXPECTED_HIDDEN_DENIAL_PHASE_HEADER)
            == expected_hidden_denial_phase("update")
        )

    def apply(
        self,
        *,
        name: str,
        candidate: Mapping[str, Any],
        min_slots: int,
        max_slots: int,
        expected_operation_epoch: int,
        idempotency_key: UUID | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        response = self._request_apply(
            name=name,
            candidate=candidate,
            min_slots=min_slots,
            max_slots=max_slots,
            expected_operation_epoch=expected_operation_epoch,
            idempotency_key=idempotency_key,
        )
        body = assert_2xx(response, action=f"apply development environment {name!r}")
        environment = _environment(
            body.get("environment"),
            expected_name=name,
            expected_candidate_sha=str(candidate["candidate_sha"]),
            expected_min_slots=min_slots,
            expected_max_slots=max_slots,
        )
        operation = _operation(
            body.get("operation"),
            expected_name=name,
            expected_candidate_sha=str(candidate["candidate_sha"]),
            expected_min_slots=min_slots,
            expected_max_slots=max_slots,
            expected_operation_epoch=int(environment["operation_epoch"]),
        )
        if (
            operation.get("expected_operation_epoch") != expected_operation_epoch
            or operation.get("operation_epoch") != environment["operation_epoch"]
        ):
            raise PersonalDevDeployError("personal-dev apply response binding is invalid")
        return environment, operation

    def wait_ready(
        self,
        name: str,
        *,
        operation_id: str,
        candidate_sha: str,
        min_slots: int,
        max_slots: int,
        operation_epoch: int,
        timeout: float,
        poll_interval: float,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> dict[str, Any]:
        deadline = monotonic() + timeout
        while True:
            operation_body = _operation(
                assert_2xx(
                    self._client.get(
                        f"/api/v1/dev-instances/{name}/operations/{operation_id}"
                    ),
                    action=f"check development environment {name!r} operation",
                ),
                expected_id=operation_id,
                expected_name=name,
                expected_candidate_sha=candidate_sha,
                expected_min_slots=min_slots,
                expected_max_slots=max_slots,
                expected_operation_epoch=operation_epoch,
            )
            state = operation_body.get("state")
            if state == "succeeded":
                environment_body = assert_2xx(
                    self._client.get(f"/api/v1/dev-instances/{name}"),
                    action=f"fetch development environment {name!r}",
                )
                environment = _environment(
                    environment_body,
                    expected_name=name,
                    expected_candidate_sha=candidate_sha,
                    expected_min_slots=min_slots,
                    expected_max_slots=max_slots,
                    expected_operation_epoch=operation_epoch,
                )
                if environment["status"] != "ready":
                    raise PersonalDevDeployError(
                        "personal-dev operation succeeded without a ready environment projection"
                    )
                return environment
            if state in {"failed", "cancelled"}:
                reason = operation_body.get("failure_reason") or state
                raise PersonalDevDeployError(
                    f"development environment {name!r} operation {state}: {reason}"
                )
            if state not in {"requested", "running", "activating", "cancelling"}:
                raise PersonalDevDeployError("personal-dev operation state is invalid")
            if monotonic() >= deadline:
                raise PersonalDevDeployError(
                    f"timed out after {timeout:g}s waiting for development "
                    f"environment {name!r}"
                )
            sleep(min(poll_interval, max(0.0, deadline - monotonic())))


__all__ = [
    "PersonalDevDeployClient",
    "PersonalDevDeployError",
]
