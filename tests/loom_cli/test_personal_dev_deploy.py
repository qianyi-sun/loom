from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID

import httpx
import pytest

from loom.personal_dev_source import (
    PersonalDevSourceManifestV1,
    PersonalDevSourceSnapshotV1,
)
from loom_cli.personal_dev_deploy import (
    PersonalDevDeployClient,
    PersonalDevDeployError,
)

_CANDIDATE_ID = "00000000-0000-0000-0000-000000000001"
_OPERATION_ID = "00000000-0000-0000-0000-000000000002"
_CANDIDATE_SHA = "c" * 64
_SOURCE_SHA = "a" * 64
_ARCHIVE_SHA = "b" * 64


def _candidate(*, status: str = "ready") -> dict[str, object]:
    return {
        "id": _CANDIDATE_ID,
        "candidate_sha": _CANDIDATE_SHA,
        "source_sha256": _SOURCE_SHA,
        "archive_sha256": _ARCHIVE_SHA,
        "attestation_scope": "personal-dev-only",
        "promotable": False,
        "status": status,
    }


def _environment(*, status: str, epoch: int) -> dict[str, object]:
    return {
        "name": "alice",
        "status": status,
        "operation_epoch": epoch,
        "candidate_sha": _CANDIDATE_SHA,
        "min_slots": 0,
        "max_slots": 2,
        "identity": {"route_host": "alice.dev.example"},
    }


def _operation(*, state: str) -> dict[str, object]:
    return {
        "id": _OPERATION_ID,
        "environment_name": "alice",
        "candidate_sha": _CANDIDATE_SHA,
        "min_slots": 0,
        "max_slots": 2,
        "expected_operation_epoch": 0,
        "operation_epoch": 1,
        "state": state,
        "failure_reason": None,
    }


def _snapshot() -> PersonalDevSourceSnapshotV1:
    return PersonalDevSourceSnapshotV1(
        manifest=PersonalDevSourceManifestV1(
            schema_version=1,
            attestation_scope="personal-dev-only",
            source_commit="1" * 40,
            dirty=False,
            worktree_state_sha256="d" * 64,
            contexts=(".",),
            files=(),
            deleted_tracked_paths=(),
            excluded_sensitive_paths=(),
            file_count=0,
            total_bytes=0,
        ),
        source_digest=_SOURCE_SHA,
        archive_sha256=_ARCHIVE_SHA,
    )


def test_personal_deploy_uploads_binds_epoch_and_waits_for_exact_operation(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "source.tar"
    archive.write_bytes(b"sealed")
    requests: list[httpx.Request] = []
    operation_reads = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal operation_reads
        requests.append(request)
        path = request.url.path
        if path == "/api/v1/personal-dev-candidates":
            return httpx.Response(201, json=_candidate(status="uploaded"))
        if path == "/api/v1/dev-instances/alice" and request.method == "GET":
            if operation_reads:
                return httpx.Response(200, json=_environment(status="ready", epoch=1))
            return httpx.Response(404, json={"detail": "not found"})
        if path == "/api/v1/dev-instances/alice" and request.method == "PUT":
            payload = json.loads(request.content)
            assert payload["candidate_id"] == _CANDIDATE_ID
            assert payload["candidate_sha"] == _CANDIDATE_SHA
            assert payload["expected_operation_epoch"] == 0
            UUID(payload["idempotency_key"])
            return httpx.Response(
                202,
                json={
                    "environment": _environment(status="provisioning", epoch=1),
                    "operation": _operation(state="running"),
                },
            )
        if path.endswith(f"/operations/{_OPERATION_ID}"):
            operation_reads += 1
            return httpx.Response(200, json=_operation(state="succeeded"))
        raise AssertionError(f"unexpected request: {request.method} {path}")

    with httpx.Client(
        base_url="https://loom.example",
        transport=httpx.MockTransport(handler),
    ) as http_client:
        deploy = PersonalDevDeployClient(http_client)
        candidate = deploy.upload_snapshot(archive, _snapshot())
        epoch = deploy.expected_operation_epoch("alice")
        environment, operation = deploy.apply(
            name="alice",
            candidate=candidate,
            min_slots=0,
            max_slots=2,
            expected_operation_epoch=epoch,
        )
        ready = deploy.wait_ready(
            "alice",
            operation_id=str(operation["id"]),
            candidate_sha=_CANDIDATE_SHA,
            min_slots=0,
            max_slots=2,
            operation_epoch=1,
            timeout=1.0,
            poll_interval=0.01,
            sleep=lambda _seconds: None,
        )

    assert environment["status"] == "provisioning"
    assert ready["status"] == "ready"
    assert requests[0].headers["content-type"].startswith("multipart/form-data;")


def test_candidate_reuse_requires_exactly_one_owned_ready_result() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"items": [_candidate(status="building")]})

    with httpx.Client(
        base_url="https://loom.example",
        transport=httpx.MockTransport(handler),
    ) as http_client:
        with pytest.raises(PersonalDevDeployError, match="owned, retained, ready"):
            PersonalDevDeployClient(http_client).resolve_ready_candidate(_CANDIDATE_SHA)


@pytest.mark.parametrize(
    ("field", "value"),
    [("attestation_scope", "release"), ("promotable", True)],
)
def test_candidate_reuse_rejects_a_non_personal_candidate_binding(
    field: str,
    value: object,
) -> None:
    body = _candidate()
    body[field] = value

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"items": [body]})

    with httpx.Client(
        base_url="https://loom.example",
        transport=httpx.MockTransport(handler),
    ) as http_client:
        with pytest.raises(PersonalDevDeployError, match="binding is invalid"):
            PersonalDevDeployClient(http_client).resolve_ready_candidate(_CANDIDATE_SHA)


def test_apply_rejects_an_invalid_operation_identity() -> None:
    operation = _operation(state="running")
    operation["id"] = "not-a-uuid"

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            202,
            json={
                "environment": _environment(status="provisioning", epoch=1),
                "operation": operation,
            },
        )

    with httpx.Client(
        base_url="https://loom.example",
        transport=httpx.MockTransport(handler),
    ) as http_client:
        with pytest.raises(PersonalDevDeployError, match=r"operation.*invalid"):
            PersonalDevDeployClient(http_client).apply(
                name="alice",
                candidate=_candidate(),
                min_slots=0,
                max_slots=2,
                expected_operation_epoch=0,
            )


@pytest.mark.parametrize("epoch", [0, -1, False])
def test_expected_hidden_denial_rejects_nonpositive_epoch_before_http(epoch: int) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    with httpx.Client(
        base_url="https://loom.example",
        transport=httpx.MockTransport(handler),
    ) as http_client:
        with pytest.raises(PersonalDevDeployError, match="positive"):
            PersonalDevDeployClient(http_client).apply_expected_hidden_denial(
                name="alice",
                candidate=_candidate(),
                min_slots=0,
                max_slots=2,
                expected_operation_epoch=epoch,
            )


def test_wait_rejects_a_ready_projection_that_drifted_from_the_apply() -> None:
    drifted = _environment(status="ready", epoch=1)
    drifted["candidate_sha"] = "d" * 64

    def handler(request: httpx.Request) -> httpx.Response:
        if "/operations/" in request.url.path:
            return httpx.Response(200, json=_operation(state="succeeded"))
        return httpx.Response(200, json=drifted)

    with httpx.Client(
        base_url="https://loom.example",
        transport=httpx.MockTransport(handler),
    ) as http_client:
        with pytest.raises(
            PersonalDevDeployError,
            match=r"environment.*binding is invalid",
        ):
            PersonalDevDeployClient(http_client).wait_ready(
                "alice",
                operation_id=_OPERATION_ID,
                candidate_sha=_CANDIDATE_SHA,
                min_slots=0,
                max_slots=2,
                operation_epoch=1,
                timeout=1.0,
                poll_interval=0.01,
                sleep=lambda _seconds: None,
            )


def test_failed_environment_reuses_its_current_operation_epoch() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_environment(status="failed", epoch=3))

    with httpx.Client(
        base_url="https://loom.example",
        transport=httpx.MockTransport(handler),
    ) as http_client:
        assert PersonalDevDeployClient(http_client).expected_operation_epoch("alice") == 3


@pytest.mark.parametrize("status", ["provisioning", "activating", "updating", "deleting"])
def test_existing_lifecycle_environment_blocks_a_new_apply(status: str) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_environment(status=status, epoch=3))

    with httpx.Client(
        base_url="https://loom.example",
        transport=httpx.MockTransport(handler),
    ) as http_client:
        with pytest.raises(PersonalDevDeployError, match=status):
            PersonalDevDeployClient(http_client).expected_operation_epoch("alice")
