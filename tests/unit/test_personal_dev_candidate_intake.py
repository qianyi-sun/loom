"""Trusted intake contract for immutable personal-development candidates."""

from __future__ import annotations

import io
import os
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from fastapi import FastAPI, HTTPException, Response, UploadFile
from starlette.requests import Request

from loom.auth import AuthContext
from loom.personal_dev_candidate import (
    PERSONAL_DEV_BUILD_CONTRACT_SHA256,
    PERSONAL_DEV_COMPONENTS,
    PERSONAL_DEV_PLATFORMS,
    CandidateRegistration,
    PersonalDevArtifactCollectionInProgressError,
    PersonalDevCandidateQuotaError,
    PersonalDevCandidateRecord,
    personal_dev_image_set_manifest_digest,
    validate_personal_dev_candidate_publication,
)
from loom.personal_dev_source import create_personal_dev_source_snapshot
from loom_service.personal_dev_candidate_intake import (
    intake_personal_dev_candidate,
)
from loom_service.routes.personal_dev_candidates import (
    create_personal_dev_candidate,
    get_personal_dev_candidate,
    list_personal_dev_candidates,
)

_OWNER = UUID("00000000-0000-0000-0000-000000000011")
_TEAM = UUID("00000000-0000-0000-0000-000000000012")


def _git_repo(tmp_path: Path) -> Path:
    import subprocess

    repo = tmp_path / "repo"
    repo.mkdir(parents=True)
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "candidate@example.test"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.name", "Candidate Test"],
        check=True,
    )
    (repo / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", "app.py"], check=True)
    commit_env = os.environ.copy()
    commit_env.update(
        {
            "GIT_AUTHOR_DATE": "2026-01-01T00:00:00Z",
            "GIT_COMMITTER_DATE": "2026-01-01T00:00:00Z",
        }
    )
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-qm", "base"],
        check=True,
        env=commit_env,
    )
    return repo


class _Registry:
    def __init__(self) -> None:
        self.by_identity: dict[tuple[UUID, UUID, str, str, str], CandidateRegistration] = {}
        self.calls = 0

    async def register(self, requested: PersonalDevCandidateRecord) -> CandidateRegistration:
        self.calls += 1
        identity = (
            requested.owner_user_id,
            requested.owner_team_id,
            requested.source_sha256,
            requested.archive_sha256,
            requested.build_contract_sha256,
        )
        current = self.by_identity.get(identity)
        if current is not None:
            return CandidateRegistration(
                candidate=current.candidate,
                build_attempt=current.build_attempt,
                created=False,
            )
        registration = CandidateRegistration.from_candidate(requested)
        self.by_identity[identity] = registration
        return registration

    async def get(self, candidate_id: UUID) -> CandidateRegistration | None:
        return next(
            (
                registration
                for registration in self.by_identity.values()
                if registration.candidate.id == candidate_id
            ),
            None,
        )

    async def list_visible(
        self,
        *,
        owner_user_id: UUID | None,
        limit: int = 100,
    ) -> list[CandidateRegistration]:
        rows = list(self.by_identity.values())
        if owner_user_id is not None:
            rows = [row for row in rows if row.candidate.owner_user_id == owner_user_id]
        return rows[:limit]


class _ObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.metadata: dict[tuple[str, str], dict[str, str]] = {}
        self.version = 0
        self.deleted: list[dict[str, object]] = []

    def put_object(self, **kwargs: object) -> dict[str, str]:
        body = kwargs["Body"]
        assert hasattr(body, "read")
        payload = body.read()  # type: ignore[union-attr]
        key = (str(kwargs["Bucket"]), str(kwargs["Key"]))
        self.objects[key] = payload
        self.metadata[key] = dict(kwargs["Metadata"])  # type: ignore[arg-type]
        self.version += 1
        return {"VersionId": f"v{self.version}"}

    def delete_object(self, **kwargs: object) -> None:
        self.deleted.append(dict(kwargs))
        key = (str(kwargs["Bucket"]), str(kwargs["Key"]))
        self.objects.pop(key, None)
        self.metadata.pop(key, None)


async def _intake(
    tmp_path: Path,
    *,
    registry: _Registry | None = None,
    object_store: _ObjectStore | None = None,
    expected_archive_sha256: str | None = None,
    max_archive_bytes: int | None = None,
) -> tuple[CandidateRegistration, _Registry, _ObjectStore]:
    repo = _git_repo(tmp_path)
    archive = tmp_path / "source.tar"
    snapshot = create_personal_dev_source_snapshot(repo, archive)
    bound_registry = registry or _Registry()
    bound_store = object_store or _ObjectStore()
    options: dict[str, int] = {}
    if max_archive_bytes is not None:
        options["max_archive_bytes"] = max_archive_bytes
    with archive.open("rb") as source:
        result = await intake_personal_dev_candidate(
            registry=bound_registry,
            object_store=bound_store,
            bucket="artifacts",
            owner_user_id=_OWNER,
            owner_team_id=_TEAM,
            source_upload=UploadFile(filename="source.tar", file=source),
            expected_source_sha256=snapshot.source_digest,
            expected_archive_sha256=(expected_archive_sha256 or snapshot.archive_sha256),
            **options,
        )
    return result, bound_registry, bound_store


async def test_verified_upload_is_content_addressed_and_enqueues_one_build(
    tmp_path: Path,
) -> None:
    result, registry, object_store = await _intake(tmp_path)

    assert result.created is True
    assert result.candidate.status == "uploaded"
    assert result.candidate.owner_user_id == _OWNER
    assert result.candidate.owner_team_id == _TEAM
    assert result.candidate.build_contract_sha256 == PERSONAL_DEV_BUILD_CONTRACT_SHA256
    assert len(result.candidate.candidate_sha) == 64
    assert result.candidate.object_key.startswith(f"personal-dev/sources/{_TEAM}/{_OWNER}/")
    assert f"/{result.candidate.source_generation_id}/" in result.candidate.object_key
    assert result.build_attempt is None
    object_key = (result.candidate.object_bucket, result.candidate.object_key)
    assert len(object_store.objects[object_key]) == result.candidate.archive_size_bytes
    assert object_store.metadata[object_key] == {
        "archive-sha256": result.candidate.archive_sha256,
        "attestation-scope": "personal-dev-only",
        "build-contract-sha256": PERSONAL_DEV_BUILD_CONTRACT_SHA256,
        "candidate-sha256": result.candidate.candidate_sha,
        "source-sha256": result.candidate.source_sha256,
    }
    assert registry.calls == 1


async def test_exact_retry_is_idempotent_and_does_not_enqueue_another_build(
    tmp_path: Path,
) -> None:
    registry = _Registry()
    object_store = _ObjectStore()
    first, _, _ = await _intake(
        tmp_path / "first",
        registry=registry,
        object_store=object_store,
    )
    second, _, _ = await _intake(
        tmp_path / "second",
        registry=registry,
        object_store=object_store,
    )

    assert second.created is False
    assert second.candidate.id == first.candidate.id
    assert second.build_attempt is None
    assert len(registry.by_identity) == 1
    assert len(object_store.objects) == 1
    assert len(object_store.deleted) == 1
    assert object_store.deleted[0]["Key"] != first.candidate.object_key


async def test_collection_race_removes_rejected_reupload_and_returns_conflict(
    tmp_path: Path,
) -> None:
    class _CollectingRegistry(_Registry):
        async def register(
            self,
            requested: PersonalDevCandidateRecord,
        ) -> CandidateRegistration:
            self.calls += 1
            raise PersonalDevArtifactCollectionInProgressError(
                "personal-dev candidate artifacts are being collected"
            )

    object_store = _ObjectStore()
    with pytest.raises(HTTPException) as exc:
        await _intake(
            tmp_path,
            registry=_CollectingRegistry(),
            object_store=object_store,
        )

    assert exc.value.status_code == 409
    assert object_store.objects == {}
    assert object_store.deleted[0]["VersionId"] == "v1"


async def test_collection_race_deletes_only_its_unique_unversioned_generation(
    tmp_path: Path,
) -> None:
    class _CollectingRegistry(_Registry):
        async def register(
            self,
            requested: PersonalDevCandidateRecord,
        ) -> CandidateRegistration:
            raise PersonalDevArtifactCollectionInProgressError("collecting")

    class _UnversionedObjectStore(_ObjectStore):
        def put_object(self, **kwargs: object) -> None:
            super().put_object(**kwargs)

    object_store = _UnversionedObjectStore()
    with pytest.raises(HTTPException) as exc:
        await _intake(
            tmp_path,
            registry=_CollectingRegistry(),
            object_store=object_store,
        )

    assert exc.value.status_code == 409
    assert object_store.objects == {}
    assert len(object_store.deleted) == 1
    assert "VersionId" not in object_store.deleted[0]


async def test_unexpected_registration_failure_removes_only_its_published_generation(
    tmp_path: Path,
) -> None:
    class _UnexpectedRegistry(_Registry):
        requested: PersonalDevCandidateRecord | None = None

        async def register(self, requested: PersonalDevCandidateRecord) -> CandidateRegistration:
            self.requested = requested
            raise RuntimeError("registry failure must not reach the client")

    registry = _UnexpectedRegistry()
    object_store = _ObjectStore()
    with pytest.raises(HTTPException) as exc:
        await _intake(tmp_path, registry=registry, object_store=object_store)

    assert exc.value.status_code == 503
    assert exc.value.detail == "personal-dev candidate registration failed"
    assert object_store.objects == {}
    assert registry.requested is not None
    assert object_store.deleted == [
        {
            "Bucket": "artifacts",
            "Key": registry.requested.object_key,
            "VersionId": "v1",
        }
    ]


async def test_unexpected_registration_cleanup_failure_is_generic(tmp_path: Path) -> None:
    class _UnexpectedRegistry(_Registry):
        async def register(self, requested: PersonalDevCandidateRecord) -> CandidateRegistration:
            del requested
            raise RuntimeError("registry failure must not reach the client")

    class _FailingCleanupObjectStore(_ObjectStore):
        def delete_object(self, **kwargs: object) -> None:
            self.deleted.append(dict(kwargs))
            raise RuntimeError("cleanup failure must not reach the client")

    object_store = _FailingCleanupObjectStore()
    with pytest.raises(HTTPException) as exc:
        await _intake(tmp_path, registry=_UnexpectedRegistry(), object_store=object_store)

    assert exc.value.status_code == 503
    assert exc.value.detail == "personal-dev rejected source cleanup failed"
    assert "registry failure" not in exc.value.detail
    assert "cleanup failure" not in exc.value.detail
    assert object_store.deleted[0]["VersionId"] == "v1"


async def test_digest_mismatch_and_oversize_fail_before_publication(tmp_path: Path) -> None:
    registry = _Registry()
    object_store = _ObjectStore()
    with pytest.raises(HTTPException) as mismatch:
        await _intake(
            tmp_path / "mismatch",
            registry=registry,
            object_store=object_store,
            expected_archive_sha256="0" * 64,
        )
    assert mismatch.value.status_code == 400
    assert registry.calls == 0
    assert object_store.objects == {}

    with pytest.raises(HTTPException) as too_large:
        await _intake(
            tmp_path / "large",
            registry=registry,
            object_store=object_store,
            max_archive_bytes=512,
        )
    assert too_large.value.status_code == 413
    assert registry.calls == 0
    assert object_store.objects == {}


async def test_empty_or_unsafe_upload_filename_is_rejected() -> None:
    for filename in ("", "../source.tar", "/source.tar"):
        with pytest.raises(HTTPException) as exc:
            await intake_personal_dev_candidate(
                registry=_Registry(),
                object_store=_ObjectStore(),
                bucket="artifacts",
                owner_user_id=_OWNER,
                owner_team_id=_TEAM,
                source_upload=UploadFile(filename=filename, file=io.BytesIO(b"x")),
                expected_source_sha256="0" * 64,
                expected_archive_sha256="0" * 64,
            )
        assert exc.value.status_code == 400


def _context(user_id: UUID) -> AuthContext:
    return AuthContext(
        token_hash=b"x" * 32,
        type="user",
        scopes=["read:own", "submit"],
        team_id=_TEAM,
        expires_at=None,
        user_id=user_id,
        role="member",
    )


def _request(registry: _Registry, object_store: _ObjectStore) -> Request:
    app = FastAPI()
    app.state.settings = SimpleNamespace(
        artifacts_bucket="artifacts",
        dev_instances_enabled=True,
        personal_dev_source_max_archive_bytes=384 * 1024 * 1024,
    )
    app.state.minio_client = object_store
    app.state.personal_dev_candidate_store_factory = lambda _session: registry
    return Request({"type": "http", "method": "POST", "path": "/", "app": app})


async def test_candidate_routes_are_owner_scoped(tmp_path: Path) -> None:
    repo = _git_repo(tmp_path)
    archive = tmp_path / "source.tar"
    snapshot = create_personal_dev_source_snapshot(repo, archive)
    registry = _Registry()
    request = _request(registry, _ObjectStore())
    with archive.open("rb") as source:
        created = await create_personal_dev_candidate(
            request,
            Response(),
            (object(), _context(_OWNER)),  # type: ignore[arg-type]
            UploadFile(filename="source.tar", file=source),
            snapshot.source_digest,
            snapshot.archive_sha256,
        )
    assert created.status == "uploaded"
    assert created.attestation_scope == "personal-dev-only"
    assert created.promotable is False
    assert created.build_attempt is None

    listed = await list_personal_dev_candidates(
        request,
        (object(), _context(_OWNER)),  # type: ignore[arg-type]
        mine=False,
        limit=100,
    )
    assert [item.id for item in listed.items] == [created.id]
    with pytest.raises(HTTPException) as hidden:
        await get_personal_dev_candidate(
            created.id,
            request,
            (object(), _context(UUID(int=99))),  # type: ignore[arg-type]
        )
    assert hidden.value.status_code == 404


async def test_candidate_route_fails_closed_outside_management_dev(tmp_path: Path) -> None:
    repo = _git_repo(tmp_path)
    archive = tmp_path / "source.tar"
    snapshot = create_personal_dev_source_snapshot(repo, archive)
    registry = _Registry()
    request = _request(registry, _ObjectStore())
    request.app.state.settings.dev_instances_enabled = False
    with archive.open("rb") as source, pytest.raises(HTTPException) as disabled:
        await create_personal_dev_candidate(
            request,
            Response(),
            (object(), _context(_OWNER)),  # type: ignore[arg-type]
            UploadFile(filename="source.tar", file=source),
            snapshot.source_digest,
            snapshot.archive_sha256,
        )
    assert disabled.value.status_code == 503
    assert registry.calls == 0


async def test_candidate_route_returns_conflict_when_owner_retention_is_full(
    tmp_path: Path,
) -> None:
    class _QuotaRegistry(_Registry):
        async def register(self, requested: PersonalDevCandidateRecord) -> CandidateRegistration:
            del requested
            raise PersonalDevCandidateQuotaError("retained candidate count limit is exhausted")

    repo = _git_repo(tmp_path)
    archive = tmp_path / "source.tar"
    snapshot = create_personal_dev_source_snapshot(repo, archive)
    object_store = _ObjectStore()
    request = _request(_QuotaRegistry(), object_store)
    with archive.open("rb") as source, pytest.raises(HTTPException) as conflict:
        await create_personal_dev_candidate(
            request,
            Response(),
            (object(), _context(_OWNER)),  # type: ignore[arg-type]
            UploadFile(filename="source.tar", file=source),
            snapshot.source_digest,
            snapshot.archive_sha256,
        )
    assert conflict.value.status_code == 409
    assert object_store.objects == {}


async def test_candidate_publication_requires_complete_immutable_safety_binding(
    tmp_path: Path,
) -> None:
    registration, _, _ = await _intake(tmp_path)
    candidate = registration.candidate
    images: dict[str, object] = {
        component: {
            "index": f"registry.example/loom-{component}@sha256:" + "2" * 64,
            "platforms": {
                platform: "sha256:" + "3" * 64 for platform in PERSONAL_DEV_PLATFORMS
            },
        }
        for component in PERSONAL_DEV_COMPONENTS
    }
    publication: dict[str, object] = {
        "schema_version": 1,
        "attestation_scope": "personal-dev-only",
        "candidate_sha": candidate.candidate_sha,
        "source_sha256": candidate.source_sha256,
        "archive_sha256": candidate.archive_sha256,
        "build_contract_sha256": candidate.build_contract_sha256,
        "image_set_manifest_digest": personal_dev_image_set_manifest_digest(images),
        "images": images,
        "supported_pools": ["gb10", "oldlab"],
        "supported_architectures": list(PERSONAL_DEV_PLATFORMS),
        "protocol_versions": {"capacity-agent": "v1", "claim-guard": "v1"},
        "trusted_launcher_profile_sha256": "4" * 64,
        "safety_evidence": {
            "bucket": candidate.object_bucket,
            "content_type": "application/vnd.loom.personal-dev-safety-evidence.v1+json",
            "key": (
                f"personal-dev/evidence/{candidate.candidate_sha}/"
                "test/safety-evidence.json"
            ),
            "sha256": "5" * 64,
            "size_bytes": 1024,
        },
        "safety_evidence_sha256": "5" * 64,
        "publisher_identity": "system:serviceaccount:loom-dev:candidate-builder",
        "published_at": "2026-08-10T12:00:00Z",
    }

    normalized, digest, image_set = validate_personal_dev_candidate_publication(
        candidate,
        publication,
    )
    assert normalized == publication
    assert len(digest) == 64
    assert image_set == personal_dev_image_set_manifest_digest(images)

    platform_digests = publication["images"]
    assert isinstance(platform_digests, dict)
    service = platform_digests["service"]
    assert isinstance(service, dict)
    service_platforms = service["platforms"]
    assert isinstance(service_platforms, dict)
    service_platforms["linux/amd64"] = "sha256:" + "9" * 64
    with pytest.raises(ValueError, match="image-set digest"):
        validate_personal_dev_candidate_publication(candidate, publication)
    service_platforms["linux/amd64"] = "sha256:" + "3" * 64

    images = publication["images"]
    assert isinstance(images, dict)
    first = images[PERSONAL_DEV_COMPONENTS[0]]
    assert isinstance(first, dict)
    first["index"] = "registry.example/loom-agent-sandbox:mutable"
    with pytest.raises(ValueError, match="immutable"):
        validate_personal_dev_candidate_publication(candidate, publication)
