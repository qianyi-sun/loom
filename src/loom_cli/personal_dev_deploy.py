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


def _receipt_identity(operation: Mapping[str, Any]) -> tuple[str, ...]:
    try:
        values = tuple(str(UUID(str(operation[key]))) for key in (
            "id", "idempotency_key", "attempt_id", "subject_id", "subject_incarnation", "candidate_id",
        ))
        if any(UUID(value).int == 0 for value in values):
            raise ValueError
        if type(operation["deployment_generation"]) is not int or operation["deployment_generation"] < 1:
            raise ValueError
        return values
    except (KeyError, TypeError, ValueError):
        raise PersonalDevDeployError("personal-dev operation identity binding is invalid") from None


def _verify_ready_projection(environment: Mapping[str, Any], operation: Mapping[str, Any]) -> None:
    _receipt_identity(operation)
    if (
        operation.get("state") != "succeeded" or operation.get("checkpoint") != "complete"
        or operation.get("kind") not in {"create", "update", "capacity", "noop"}
        or environment.get("status") != "ready" or environment.get("operation_step") != "complete"
        or (operation.get("kind") != "noop" and environment.get("operation_id") != operation.get("id"))
        or any(environment.get(key) != operation.get(key) for key in (
            "operation_epoch", "subject_id", "subject_incarnation", "candidate_id", "candidate_sha",
            "min_slots", "max_slots", "deployment_generation",
        ))
    ):
        raise PersonalDevDeployError("personal-dev environment readiness binding is invalid")


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
            "superseded",
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
        if environment["status"] not in {"ready", "failed"}:
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

        if (
            type(expected_operation_epoch) is not int
            or expected_operation_epoch <= 0
        ):
            raise PersonalDevDeployError(
                "expected hidden-denial operation epoch must be positive",
            )

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
        request_key = idempotency_key or uuid4()
        response = self._request_apply(
            name=name,
            candidate=candidate,
            min_slots=min_slots,
            max_slots=max_slots,
            expected_operation_epoch=expected_operation_epoch,
            idempotency_key=request_key,
        )
        body = assert_2xx(response, action=f"apply development environment {name!r}")
        environment = _environment(body.get("environment"), expected_name=name)
        operation_body = _object(body.get("operation"), label="personal-dev operation")
        operation = _operation(
            operation_body,
            expected_name=name,
            expected_candidate_sha=str(candidate["candidate_sha"]),
            expected_min_slots=min_slots,
            expected_max_slots=max_slots,
            expected_operation_epoch=expected_operation_epoch + (operation_body.get("kind") != "noop"),
        )
        _receipt_identity(operation)
        if (
            operation.get("expected_operation_epoch") != expected_operation_epoch
            or operation.get("idempotency_key") != str(request_key)
            or operation.get("candidate_id") != str(candidate["id"])
            or operation.get("kind") not in {"create", "update", "capacity", "noop"}
            or any(environment.get(key) != operation.get(key) for key in ("subject_id", "subject_incarnation"))
        ):
            raise PersonalDevDeployError("personal-dev apply response binding is invalid")
        if operation["state"] == "superseded":
            if environment["operation_epoch"] <= operation["operation_epoch"]:
                raise PersonalDevDeployError("personal-dev superseded apply epoch is invalid")
        elif operation["state"] == "succeeded":
            _verify_ready_projection(environment, operation)
        elif (
            operation["state"] in {"failed", "cancelled"}
            or environment["operation_epoch"] != operation["operation_epoch"]
            or environment.get("operation_id") != operation["id"]
            or environment["status"] not in {"provisioning", "updating", "activating"}
        ):
            raise PersonalDevDeployError("personal-dev apply is terminal or no longer current")
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
        operation_receipt: Mapping[str, Any] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> dict[str, Any]:
        deadline = monotonic() + timeout
        predecessor_id: str | None = None
        retained_identity: tuple[str, ...] | None = None
        continuation_kind: str | None = None
        visited = {operation_id}
        anchor = _receipt_identity(operation_receipt) if operation_receipt is not None else None
        previous_identity: tuple[str, ...] | None = None
        previous_generation: int | None = None
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
            identity = _receipt_identity(operation_body)
            if predecessor_id is None and operation_receipt is not None and (
                identity != anchor or any(operation_body.get(key) != operation_receipt.get(key) for key in (
                    "deployment_generation", "kind", "expected_operation_epoch", "membership_continuation_kind",
                ))
            ):
                raise PersonalDevDeployError("personal-dev operation differs from the apply receipt")
            if predecessor_id is not None and (
                operation_body.get("membership_predecessor_operation_id") != predecessor_id
                or operation_body.get("expected_operation_epoch") != operation_epoch - 1
                or operation_body.get("membership_continuation_kind") != continuation_kind
                or operation_body.get("kind") not in {"create", "update"}
                or tuple(str(operation_body.get(key)) for key in (
                    "subject_id", "subject_incarnation", "candidate_id",
                )) != retained_identity
                or previous_identity is None or any(identity[index] == previous_identity[index] for index in range(3))
                or previous_generation is None or operation_body["deployment_generation"] <= previous_generation
            ):
                raise PersonalDevDeployError("personal-dev successor changed the original owner intent")
            if state == "superseded":
                try:
                    successor_id = str(UUID(str(operation_body["membership_successor_operation_id"])))
                    identity = tuple(str(UUID(str(operation_body[key]))) for key in (
                        "subject_id", "subject_incarnation", "candidate_id",
                    ))
                except (KeyError, TypeError, ValueError):
                    raise PersonalDevDeployError("personal-dev successor identity is invalid") from None
                intent = operation_body.get("membership_continuation_kind") or operation_body.get("kind")
                if (
                    successor_id in visited or UUID(successor_id).int == 0
                    or any(UUID(value).int == 0 for value in identity)
                    or intent not in {"create", "update", "capacity"}
                    or operation_body.get("checkpoint") != "membership_successor_created"
                    or operation_body.get("expected_operation_epoch") != operation_epoch - 1
                ):
                    raise PersonalDevDeployError("personal-dev successor lineage is invalid")
                if monotonic() >= deadline:
                    raise PersonalDevDeployError("timed out following personal-dev successor recovery")
                predecessor_id = operation_id
                previous_identity = _receipt_identity(operation_body)
                previous_generation = operation_body["deployment_generation"]
                retained_identity = identity
                continuation_kind = str(intent)
                operation_id = successor_id
                operation_epoch += 1
                visited.add(successor_id)
                continue
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
                _verify_ready_projection(environment, operation_body)
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
