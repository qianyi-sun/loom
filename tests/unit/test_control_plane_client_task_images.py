from __future__ import annotations

import json
from uuid import UUID

import httpx

from loom_worker.control_plane_client import HttpControlPlaneClient, TaskImageBuildClaim


def _claim_payload() -> dict[str, object]:
    return {
        "id": "00000000-0000-0000-0000-000000000123",
        "materialization_key": "a" * 64,
        "task_id": "benchmark/task",
        "task_checksum": "b" * 64,
        "cpu_arch": "arm64",
        "task_config": {"schema_version": "1"},
        "task_source": "s3://loom-tasks/task",
        "task_source_provenance": {"revision": "abc"},
        "state": "claimed",
        "attempt_count": 1,
        "max_attempts": 3,
        "lease_epoch": 7,
        "lease_expires_at": "2026-08-14T12:00:00+00:00",
        "next_attempt_at": None,
        "registry_images": {
            "task": "registry.example/task@sha256:" + "c" * 64,
        },
        "registry_image_history": [],
        "failure_reason": None,
        "failure_message": None,
    }


async def test_claim_task_image_materialization_is_typed_and_architecture_specific() -> None:
    observed: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        observed.append(request)
        return httpx.Response(200, json=_claim_payload())

    http = httpx.AsyncClient(
        base_url="https://control.invalid",
        transport=httpx.MockTransport(handler),
    )
    client = HttpControlPlaneClient(
        base_url="https://control.invalid",
        token="builder-token",
        _client=http,
    )
    try:
        claim = await client.claim_task_image_materialization(
            builder_id="builder-arm",
            cpu_arch="arm64",
        )
    finally:
        await http.aclose()

    assert claim == TaskImageBuildClaim(
        id=UUID("00000000-0000-0000-0000-000000000123"),
        materialization_key="a" * 64,
        task_id="benchmark/task",
        task_checksum="b" * 64,
        cpu_arch="arm64",
        task_config={"schema_version": "1"},
        task_source="s3://loom-tasks/task",
        task_source_provenance={"revision": "abc"},
        attempt_count=1,
        max_attempts=3,
        lease_epoch=7,
        lease_expires_at="2026-08-14T12:00:00+00:00",
        registry_images={
            "task": "registry.example/task@sha256:" + "c" * 64,
        },
    )
    assert observed[0].url.path.endswith("/task-image-materializations/claim")
    assert json.loads(observed[0].content) == {
        "builder_id": "builder-arm",
        "cpu_arch": "arm64",
    }
    assert observed[0].headers["authorization"] == "Bearer builder-token"


async def test_claim_task_image_materialization_returns_none_for_empty_queue() -> None:
    http = httpx.AsyncClient(
        base_url="https://control.invalid",
        transport=httpx.MockTransport(lambda _request: httpx.Response(204)),
    )
    client = HttpControlPlaneClient(
        base_url="https://control.invalid",
        token="builder-token",
        _client=http,
    )
    try:
        assert (
            await client.claim_task_image_materialization(
                builder_id="builder-x86",
                cpu_arch="x86_64",
            )
            is None
        )
    finally:
        await http.aclose()


async def test_task_image_mutations_serialize_fence_and_surface_conflict() -> None:
    observed: list[tuple[str, dict[str, object]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        observed.append((request.url.path, body))
        if request.url.path.endswith("/heartbeat"):
            return httpx.Response(409, json={"detail": "stale lease"})
        return httpx.Response(200, json={"state": "ok"})

    http = httpx.AsyncClient(
        base_url="https://control.invalid",
        transport=httpx.MockTransport(handler),
    )
    client = HttpControlPlaneClient(
        base_url="https://control.invalid",
        token="builder-token",
        _client=http,
    )
    materialization_id = UUID("00000000-0000-0000-0000-000000000123")
    try:
        assert await client.start_task_image_materialization(
            materialization_id=materialization_id,
            builder_id="builder-a",
            lease_epoch=4,
        )
        assert not await client.heartbeat_task_image_materialization(
            materialization_id=materialization_id,
            builder_id="builder-a",
            lease_epoch=4,
        )
        assert await client.record_task_image_publication(
            materialization_id=materialization_id,
            builder_id="builder-a",
            lease_epoch=4,
            component="task",
            registry_image="registry.example/task@sha256:" + "c" * 64,
        )
        assert await client.complete_task_image_materialization(
            materialization_id=materialization_id,
            builder_id="builder-a",
            lease_epoch=4,
            registry_images={
                "task": "registry.example/task@sha256:" + "d" * 64,
            },
        )
        assert await client.fail_task_image_materialization(
            materialization_id=materialization_id,
            builder_id="builder-a",
            lease_epoch=4,
            retryable=True,
            failure_reason="registry_unavailable",
            failure_message="temporary outage",
            registry_images={
                "task": "registry.example/task@sha256:" + "e" * 64,
            },
        )
    finally:
        await http.aclose()

    common = {"builder_id": "builder-a", "lease_epoch": 4}
    assert observed == [
        (
            f"/api/v1/internal/task-image-materializations/{materialization_id}/start",
            common,
        ),
        (
            f"/api/v1/internal/task-image-materializations/{materialization_id}/heartbeat",
            common,
        ),
        (
            f"/api/v1/internal/task-image-materializations/{materialization_id}/publication",
            {
                **common,
                "component": "task",
                "registry_image": "registry.example/task@sha256:" + "c" * 64,
            },
        ),
        (
            f"/api/v1/internal/task-image-materializations/{materialization_id}/complete",
            {
                **common,
                "registry_images": {
                    "task": "registry.example/task@sha256:" + "d" * 64,
                },
            },
        ),
        (
            f"/api/v1/internal/task-image-materializations/{materialization_id}/fail",
            {
                **common,
                "retryable": True,
                "failure_reason": "registry_unavailable",
                "failure_message": "temporary outage",
                "registry_images": {
                    "task": "registry.example/task@sha256:" + "e" * 64,
                },
            },
        ),
    ]
