from __future__ import annotations

import asyncio
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from loom.personal_dev_candidate import PERSONAL_DEV_COMPONENTS
from loom.personal_dev_candidate_gc import (
    PersonalDevArtifactGcClaim,
    PersonalDevArtifactGcCoordinator,
    S3PersonalDevArtifactCollector,
    build_personal_dev_artifact_gc_manifest,
    validate_personal_dev_registry_prefix,
)
from tests.unit.test_personal_dev_builder import _attempt, _candidate

_NOW = datetime(2026, 8, 11, tzinfo=UTC)


def test_gc_manifest_is_exact_owner_scoped_and_repeatable() -> None:
    candidate = _candidate(
        status="failed",
        registry_prefix="registry.example/personal-dev",
    )
    attempts = [
        _attempt(id=UUID(int=10), lease_epoch=2, state="failed"),
        _attempt(id=UUID(int=11), lease_epoch=1, state="failed", attempt_sequence=1),
    ]

    first = build_personal_dev_artifact_gc_manifest(candidate, attempts)
    second = build_personal_dev_artifact_gc_manifest(candidate, list(reversed(attempts)))

    assert first == second
    assert first.source_object_key == candidate.object_key
    assert first.object_prefixes == tuple(
        sorted(
            prefix
            for attempt in attempts
            for prefix in (
                f"personal-dev/builds/{candidate.owner_team_id}/{candidate.owner_user_id}/"
                f"{candidate.candidate_sha}/{attempt.id}/",
                f"personal-dev/evidence/{candidate.candidate_sha}/{attempt.id}/",
            )
        )
    )
    assert len(first.registry_tags) == len(PERSONAL_DEV_COMPONENTS) * 3 * 3
    assert all(
        f"/{candidate.owner_team_id}/{candidate.owner_user_id}/" in reference
        for reference in first.registry_tags
    )
    assert len(first.manifest_sha256) == 64
    payload = first.payload()
    payload["unexpected"] = True
    with pytest.raises(ValueError, match="manifest is invalid"):
        type(first).from_json(payload, first.manifest_sha256)
    payload = first.payload()
    payload["candidate_sha"] = 1
    with pytest.raises(ValueError, match="manifest is invalid"):
        type(first).from_json(payload, first.manifest_sha256)
    with pytest.raises(ValueError, match="manifest is invalid"):
        replace(first, manifest_sha256="0" * 64).validate()


def test_gc_manifest_rejects_a_source_key_outside_exact_owner_scope() -> None:
    with pytest.raises(ValueError, match="source object key"):
        build_personal_dev_artifact_gc_manifest(
            _candidate(status="failed", object_key="personal-dev/sources/sibling/source.tar"),
            [],
        )


def test_gc_manifest_rejects_unbounded_attempt_and_registry_histories() -> None:
    candidate = _candidate(
        status="failed",
        registry_prefix="registry.example/personal-dev",
    )
    with pytest.raises(ValueError, match="attempt history"):
        build_personal_dev_artifact_gc_manifest(
            candidate,
            [
                _attempt(
                    id=UUID(int=index + 1),
                    state="failed",
                    attempt_sequence=index,
                )
                for index in range(1025)
            ],
        )
    with pytest.raises(ValueError, match="registry history"):
        build_personal_dev_artifact_gc_manifest(
            candidate,
            [_attempt(state="failed", lease_epoch=10_000)],
        )


def test_gc_manifest_fails_closed_without_registry_authority_for_a_prior_build() -> None:
    with pytest.raises(ValueError, match="registry authority"):
        build_personal_dev_artifact_gc_manifest(
            _candidate(status="failed", registry_prefix=None),
            [_attempt(state="failed", lease_epoch=1)],
        )


def test_registry_prefix_is_bounded_for_attempt_isolated_repository_paths() -> None:
    assert validate_personal_dev_registry_prefix("r" * 309) == "r" * 309
    with pytest.raises(ValueError, match="registry prefix"):
        validate_personal_dev_registry_prefix("r" * 310)


class _Paginator:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []
        self.calls_by_prefix = {}

    def paginate(self, **kwargs):
        self.calls.append(kwargs)
        prefix = kwargs["Prefix"]
        call_count = self.calls_by_prefix.get(prefix, 0)
        self.calls_by_prefix[prefix] = call_count + 1
        if call_count > 0:
            return []
        return [
            {
                field: [
                    {
                        key: (prefix if value == "{prefix}" else value)
                        for key, value in item.items()
                    }
                    for item in items
                ]
                for field, items in page.items()
            }
            for page in self.pages
        ]


class _S3:
    def __init__(self) -> None:
        self.paginators = {
            "list_multipart_uploads": _Paginator(
                [{"Uploads": [{"Key": "prefix/upload", "UploadId": "u1"}]}]
            ),
            "list_objects_v2": _Paginator([{"Contents": [{"Key": "{prefix}"}]}]),
            "list_object_versions": _Paginator(
                [
                    {
                        "Versions": [{"Key": "{prefix}", "VersionId": "v1"}],
                        "DeleteMarkers": [{"Key": "{prefix}", "VersionId": "d1"}],
                    }
                ]
            ),
        }
        self.aborted = []
        self.deleted = []

    def get_paginator(self, name):
        return self.paginators[name]

    def abort_multipart_upload(self, **kwargs):
        self.aborted.append(kwargs)

    def delete_objects(self, **kwargs):
        self.deleted.append(kwargs)
        return {"Deleted": list(kwargs["Delete"]["Objects"]), "Errors": []}

    def delete_object(self, **kwargs):
        self.deleted.append(kwargs)


async def test_s3_collector_removes_multipart_current_versions_and_exact_source() -> None:
    candidate = _candidate(
        status="failed",
        registry_prefix="registry.example/personal-dev",
    )
    manifest = build_personal_dev_artifact_gc_manifest(candidate, [_attempt(state="failed")])
    client = _S3()
    collector = S3PersonalDevArtifactCollector(client, expected_bucket="artifacts")

    await collector.collect(manifest)

    assert len(client.aborted) == len(manifest.object_prefixes)
    assert any(
        item.get("Key") == manifest.source_object_key
        for call in client.deleted
        for item in call.get("Delete", {}).get("Objects", [])
    )
    assert all(
        call["Bucket"] == "artifacts"
        for call in [*client.aborted, *client.deleted]
    )
    for paginator in client.paginators.values():
        assert [call["Prefix"] for call in paginator.calls] == [
            item
            for target in (*manifest.object_prefixes, manifest.source_object_key)
            for item in (target, target)
        ]


async def test_s3_collector_fails_closed_on_partial_batch_error() -> None:
    candidate = _candidate(
        status="failed",
        registry_prefix="registry.example/personal-dev",
    )
    manifest = build_personal_dev_artifact_gc_manifest(candidate, [_attempt(state="failed")])
    client = _S3()

    def failed_delete(**_kwargs):
        return {"Deleted": [], "Errors": [{"Code": "AccessDenied"}]}

    client.delete_objects = failed_delete  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="object cleanup"):
        await S3PersonalDevArtifactCollector(
            client,
            expected_bucket="artifacts",
        ).collect(manifest)


async def test_s3_collector_fails_closed_on_incomplete_multipart_identity() -> None:
    candidate = _candidate(status="failed")
    manifest = build_personal_dev_artifact_gc_manifest(candidate, [])

    class _InvalidMultipartPaginator:
        def paginate(self, **kwargs):
            if kwargs["Prefix"] == manifest.source_object_key:
                return [{"Uploads": [{"Key": manifest.source_object_key}]}]
            return []

    class _EmptyPaginator:
        def paginate(self, **_kwargs):
            return []

    class _InvalidMultipartClient:
        def get_paginator(self, name):
            if name == "list_multipart_uploads":
                return _InvalidMultipartPaginator()
            return _EmptyPaginator()

    with pytest.raises(RuntimeError, match="multipart cleanup identity"):
        await S3PersonalDevArtifactCollector(
            _InvalidMultipartClient(),
            expected_bucket="artifacts",
        ).collect(manifest)


async def test_s3_collector_requires_empty_post_delete_evidence() -> None:
    candidate = _candidate(status="failed")
    manifest = build_personal_dev_artifact_gc_manifest(candidate, [])

    class _PersistentPaginator:
        def paginate(self, **kwargs):
            return [{"Contents": [{"Key": kwargs["Prefix"]}]}]

    class _EmptyPaginator:
        def paginate(self, **_kwargs):
            return []

    class _PersistentClient:
        def get_paginator(self, name):
            if name == "list_objects_v2":
                return _PersistentPaginator()
            return _EmptyPaginator()

        def delete_objects(self, **kwargs):
            return {"Deleted": list(kwargs["Delete"]["Objects"]), "Errors": []}

    with pytest.raises(RuntimeError, match="verification failed"):
        await S3PersonalDevArtifactCollector(
            _PersistentClient(),
            expected_bucket="artifacts",
        ).collect(manifest)


async def test_s3_collector_cancellation_waits_for_destructive_thread() -> None:
    candidate = _candidate(status="failed")
    manifest = build_personal_dev_artifact_gc_manifest(candidate, [])
    started = threading.Event()
    release = threading.Event()

    class _EmptyPaginator:
        def paginate(self, **_kwargs):
            return []

    class _BlockingClient:
        blocked = False

        def get_paginator(self, _name):
            if not self.blocked:
                self.blocked = True
                started.set()
                release.wait(timeout=2)
            return _EmptyPaginator()

    task = asyncio.create_task(
        S3PersonalDevArtifactCollector(
            _BlockingClient(),
            expected_bucket="artifacts",
        ).collect(manifest)
    )
    assert await asyncio.to_thread(started.wait, 1)
    task.cancel()
    await asyncio.sleep(0.01)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task


class _Authority:
    def __init__(self, claim: PersonalDevArtifactGcClaim | None, *, marked: bool = False) -> None:
        self.claim = claim
        self.marked = marked
        self.finished = []
        self.heartbeats = []

    async def claim_next_artifact_gc(self, **_kwargs):
        return self.claim

    async def mark_next_artifact_gc(self, **_kwargs):
        return self.marked

    async def finish_artifact_gc(self, **kwargs):
        self.finished.append(kwargs)

    async def heartbeat_artifact_gc(self, **kwargs):
        self.heartbeats.append(kwargs)


class _Collector:
    def __init__(self) -> None:
        self.manifests = []

    async def collect(self, manifest):
        self.manifests.append(manifest)


async def test_gc_coordinator_collects_then_finishes_exact_lease() -> None:
    candidate = _candidate(status="failed", registry_prefix="registry.example/personal-dev")
    manifest = build_personal_dev_artifact_gc_manifest(candidate, [_attempt(state="failed")])
    claim = PersonalDevArtifactGcClaim(
        candidate_id=candidate.id,
        collector_id="collector-a",
        lease_epoch=4,
        lease_expires_at=_NOW + timedelta(minutes=5),
        manifest=manifest,
    )
    authority = _Authority(claim)
    collector = _Collector()
    coordinator = PersonalDevArtifactGcCoordinator(
        authority=authority,
        collector=collector,
        collector_id="collector-a",
        retention_seconds=60,
        lease_seconds=300,
    )

    assert await coordinator.collect_once(now=_NOW) is True
    assert collector.manifests == [manifest]
    assert len(authority.finished) == 1
    finished = authority.finished[0]
    assert finished["candidate_id"] == candidate.id
    assert finished["collector_id"] == "collector-a"
    assert finished["lease_epoch"] == 4
    assert finished["manifest_sha256"] == manifest.manifest_sha256
    assert finished["now"] >= _NOW


async def test_gc_coordinator_marks_before_retention_wait() -> None:
    authority = _Authority(None, marked=True)
    coordinator = PersonalDevArtifactGcCoordinator(
        authority=authority,
        collector=_Collector(),
        collector_id="collector-a",
        retention_seconds=60,
        lease_seconds=300,
    )

    assert await coordinator.collect_once(now=_NOW) is True


async def test_gc_coordinator_heartbeats_long_collection() -> None:
    candidate = _candidate(status="failed")
    manifest = build_personal_dev_artifact_gc_manifest(candidate, [])
    claim = PersonalDevArtifactGcClaim(
        candidate_id=candidate.id,
        collector_id="collector-a",
        lease_epoch=2,
        lease_expires_at=_NOW + timedelta(seconds=1),
        manifest=manifest,
    )
    authority = _Authority(claim)

    class _SlowCollector:
        async def collect(self, _manifest):
            await asyncio.sleep(0.035)

    coordinator = PersonalDevArtifactGcCoordinator(
        authority=authority,
        collector=_SlowCollector(),  # type: ignore[arg-type]
        collector_id="collector-a",
        retention_seconds=60,
        lease_seconds=1,
        heartbeat_interval_seconds=0.01,
    )

    assert await coordinator.collect_once(now=_NOW) is True
    assert len(authority.heartbeats) >= 2
    assert all(item["lease_epoch"] == 2 for item in authority.heartbeats)
