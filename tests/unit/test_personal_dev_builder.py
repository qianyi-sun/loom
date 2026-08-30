from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import Mapping
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from loom.personal_dev_builder import (
    PersonalDevBuildCoordinator,
    S3PersonalDevBuildSource,
)
from loom.personal_dev_candidate import (
    PERSONAL_DEV_BUILD_CONTRACT_SHA256,
    PERSONAL_DEV_COMPONENTS,
    PERSONAL_DEV_PLATFORMS,
    CandidateRegistration,
    PersonalDevCandidateBuildAttemptRecord,
    PersonalDevCandidateLimits,
    PersonalDevCandidateRecord,
    personal_dev_image_set_manifest_digest,
)
from loom.personal_dev_source import PersonalDevSourceError, create_personal_dev_source_snapshot

_NOW = datetime(2026, 8, 11, tzinfo=UTC)
_CANDIDATE_ID = UUID("00000000-0000-0000-0000-000000000001")
_ATTEMPT_ID = UUID("00000000-0000-0000-0000-000000000002")


def test_personal_dev_candidate_limits_are_finite_and_ordered() -> None:
    limits = PersonalDevCandidateLimits()
    assert limits.global_active_builds >= limits.per_owner_active_builds > 0
    with pytest.raises(ValueError, match="active build"):
        PersonalDevCandidateLimits(global_active_builds=1, per_owner_active_builds=2)
    with pytest.raises(ValueError, match="retained"):
        PersonalDevCandidateLimits(per_owner_retained_candidates=0)


def _candidate(**overrides: object) -> PersonalDevCandidateRecord:
    values: dict[str, object] = {
        "id": _CANDIDATE_ID,
        "owner_user_id": UUID("00000000-0000-0000-0000-000000000003"),
        "owner_team_id": UUID("00000000-0000-0000-0000-000000000004"),
        "candidate_sha": "a" * 64,
        "source_sha256": "b" * 64,
        "archive_sha256": "c" * 64,
        "build_contract_sha256": PERSONAL_DEV_BUILD_CONTRACT_SHA256,
        "source_commit": "d" * 40,
        "dirty": True,
        "manifest_json": {},
        "object_bucket": "artifacts",
        "object_key": (
            "personal-dev/sources/00000000-0000-0000-0000-000000000004/"
            "00000000-0000-0000-0000-000000000003/"
            f"{'a' * 64}/{_CANDIDATE_ID}/{'c' * 64}.tar"
        ),
        "source_generation_id": _CANDIDATE_ID,
        "archive_size_bytes": 10240,
        "status": "building",
        "created_at": _NOW,
        "updated_at": _NOW,
    }
    values.update(overrides)
    return PersonalDevCandidateRecord(**values)  # type: ignore[arg-type]


def _attempt(**overrides: object) -> PersonalDevCandidateBuildAttemptRecord:
    values: dict[str, object] = {
        "id": _ATTEMPT_ID,
        "candidate_id": _CANDIDATE_ID,
        "subject_id": UUID("00000000-0000-0000-0000-000000000005"),
        "subject_incarnation": UUID("00000000-0000-0000-0000-000000000006"),
        "operation_id": UUID("00000000-0000-0000-0000-000000000007"),
        "operation_epoch": 3,
        "attempt_sequence": 0,
        "state": "claimed",
        "lease_epoch": 9,
        "claimed_by": "builder-a",
        "lease_expires_at": _NOW + timedelta(seconds=60),
        "created_at": _NOW,
        "updated_at": _NOW,
    }
    values.update(overrides)
    return PersonalDevCandidateBuildAttemptRecord(**values)  # type: ignore[arg-type]


def _registration() -> CandidateRegistration:
    return CandidateRegistration(
        candidate=_candidate(),
        build_attempt=_attempt(),
        created=False,
    )


def _publication(candidate: PersonalDevCandidateRecord) -> dict[str, object]:
    images: dict[str, object] = {}
    for component_index, component in enumerate(PERSONAL_DEV_COMPONENTS, start=1):
        images[component] = {
            "index": f"registry.example/loom-{component}@sha256:{component_index:064x}",
            "platforms": {
                platform: f"sha256:{component_index + platform_index:064x}"
                for platform_index, platform in enumerate(PERSONAL_DEV_PLATFORMS, start=20)
            },
        }
    return {
        "schema_version": 1,
        "attestation_scope": "personal-dev-only",
        "candidate_sha": candidate.candidate_sha,
        "source_sha256": candidate.source_sha256,
        "archive_sha256": candidate.archive_sha256,
        "build_contract_sha256": candidate.build_contract_sha256,
        "image_set_manifest_digest": personal_dev_image_set_manifest_digest(images),
        "images": images,
        "supported_pools": ["gb10", "oldlab"],
        "supported_architectures": ["linux/amd64", "linux/arm64"],
        "protocol_versions": {
            "capacity-agent": "v1",
            "claim-guard": "v1",
            "control-plane-worker": "v1",
            "database-migrations": "expand-compatible-v1",
            "personal-dev-activation": "v1",
        },
        "trusted_launcher_profile_sha256": "f" * 64,
        "safety_evidence": {
            "bucket": candidate.object_bucket,
            "content_type": "application/vnd.loom.personal-dev-safety-evidence.v1+json",
            "key": (
                f"personal-dev/evidence/{candidate.candidate_sha}/"
                "test/safety-evidence.json"
            ),
            "sha256": "1" * 64,
            "size_bytes": 1024,
        },
        "safety_evidence_sha256": "1" * 64,
        "publisher_identity": "system:serviceaccount:loom-dev:candidate-exporter",
        "published_at": "2026-08-11T00:00:00Z",
    }


class _Authority:
    def __init__(self, registration: CandidateRegistration) -> None:
        self.registration = registration
        self.events: list[str] = []
        self.publication: dict[str, object] | None = None
        self.failure_reason: str | None = None

    async def claim_next_build(self, **_kwargs):
        self.events.append("claim")
        return self.registration

    async def start_build(self, **_kwargs):
        self.events.append("start")
        return _attempt(state="running", started_at=_NOW)

    async def heartbeat_build(self, **_kwargs):
        self.events.append("heartbeat")
        return _attempt(state="running", started_at=_NOW)

    async def finish_build(self, *, publication=None, failure_reason=None, **_kwargs):
        self.publication = publication
        self.failure_reason = failure_reason
        self.events.append("finish")
        return self.registration


class _Executor:
    def __init__(self, publication: dict[str, object], *, delay: float = 0) -> None:
        self.publication = publication
        self.delay = delay
        self.cleaned = False

    async def build(self, registration, *, source_archive):
        assert registration.build_attempt is not None
        assert source_archive.read_bytes() == b"sealed-source"
        if self.delay:
            await asyncio.sleep(self.delay)
        return self.publication

    async def cleanup(self, registration):
        del registration
        self.cleaned = True


class _FailingExecutor(_Executor):
    def __init__(self, publication: Mapping[str, object], *, failure_phase: str) -> None:
        super().__init__(dict(publication))
        self.failure_phase = failure_phase

    async def build(self, registration, *, source_archive):
        if self.failure_phase == "executor_build":
            raise RuntimeError(_DIAGNOSTIC_MARKER)
        return await super().build(registration, source_archive=source_archive)

    async def cleanup(self, registration):
        if self.failure_phase == "cleanup":
            raise RuntimeError(_DIAGNOSTIC_MARKER)
        await super().cleanup(registration)


class _InvalidPublication(Mapping[str, object]):
    def __getitem__(self, key: str) -> object:
        raise ValueError(_DIAGNOSTIC_MARKER)

    def __iter__(self):
        raise ValueError(_DIAGNOSTIC_MARKER)

    def __len__(self) -> int:
        return 0


_DIAGNOSTIC_MARKER = "never-log-personal-dev-build-exception-text"


@asynccontextmanager
async def _source(_candidate):
    path = Path("/tmp/loom-personal-dev-builder-test-source")
    path.write_bytes(b"sealed-source")
    try:
        yield path
    finally:
        path.unlink(missing_ok=True)


@asynccontextmanager
async def _failing_source(_candidate):
    raise PersonalDevSourceError(_DIAGNOSTIC_MARKER)
    yield Path("unused")  # pragma: no cover


async def test_builder_coordinator_heartbeats_cleans_then_publishes() -> None:
    registration = _registration()
    authority = _Authority(registration)
    executor = _Executor(_publication(registration.candidate), delay=0.12)

    progressed = await PersonalDevBuildCoordinator(
        authority=authority,  # type: ignore[arg-type]
        source=_source,
        executor=executor,  # type: ignore[arg-type]
        builder_id="builder-a",
        lease_seconds=1,
        heartbeat_interval_seconds=0.05,
    ).build_once(now=_NOW)

    assert progressed is True
    assert executor.cleaned is True
    assert authority.publication == _publication(registration.candidate)
    assert authority.failure_reason is None
    assert authority.events[0:2] == ["claim", "start"]
    assert "heartbeat" in authority.events
    assert authority.events[-1] == "finish"


async def test_builder_coordinator_rejects_untrusted_publication_before_success() -> None:
    registration = _registration()
    authority = _Authority(registration)
    publication = _publication(registration.candidate)
    publication["candidate_sha"] = "9" * 64
    executor = _Executor(publication)

    await PersonalDevBuildCoordinator(
        authority=authority,  # type: ignore[arg-type]
        source=_source,
        executor=executor,  # type: ignore[arg-type]
        builder_id="builder-a",
        lease_seconds=60,
    ).build_once(now=_NOW)

    assert executor.cleaned is True
    assert authority.publication is None
    assert authority.failure_reason == "builder_output_invalid"


@pytest.mark.parametrize(
    ("phase", "source", "executor", "failure_reason", "error_type"),
    [
        (
            "source_acquisition",
            _failing_source,
            _Executor({}),
            "source_verification_failed",
            "PersonalDevSourceError",
        ),
        (
            "executor_build",
            _source,
            _FailingExecutor({}, failure_phase="executor_build"),
            "builder_failed",
            "RuntimeError",
        ),
        (
            "output_validation",
            _source,
            _Executor(_InvalidPublication()),
            "builder_output_invalid",
            "ValueError",
        ),
        (
            "cleanup",
            _source,
            _FailingExecutor(_publication(_candidate()), failure_phase="cleanup"),
            "builder_failed",
            "RuntimeError",
        ),
    ],
)
async def test_builder_failure_logs_only_a_bounded_phase_and_error_type(
    phase: str,
    source,
    executor,
    failure_reason: str,
    error_type: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Catches logging an exception's potentially sensitive text on failure."""
    registration = _registration()
    authority = _Authority(registration)
    caplog.set_level(logging.ERROR, logger="loom.personal_dev_builder")

    await PersonalDevBuildCoordinator(
        authority=authority,  # type: ignore[arg-type]
        source=source,
        executor=executor,  # type: ignore[arg-type]
        builder_id="builder-a",
        lease_seconds=60,
    ).build_once(now=_NOW)

    events = [
        record
        for record in caplog.records
        if record.getMessage()
        == (
            "personal_dev_builder_execution_failed "
            f"phase={phase} error_type={error_type}"
        )
    ]
    assert len(events) == 1
    assert events[0].phase == phase
    assert events[0].error_type == error_type
    assert _DIAGNOSTIC_MARKER not in caplog.text
    assert all(_DIAGNOSTIC_MARKER not in str(record.__dict__) for record in caplog.records)
    assert _DIAGNOSTIC_MARKER not in (authority.failure_reason or "")
    assert authority.failure_reason == failure_reason


class _ObjectBody:
    def __init__(self, value: bytes) -> None:
        self.value = value
        self.offset = 0

    def read(self, size: int) -> bytes:
        chunk = self.value[self.offset : self.offset + size]
        self.offset += len(chunk)
        return chunk

    def close(self) -> None:
        pass


class _ObjectStore:
    def __init__(self, value: bytes, metadata: dict[str, str]) -> None:
        self.value = value
        self.metadata = metadata
        self.calls = 0

    def get_object(self, **_kwargs):
        self.calls += 1
        return {
            "Body": _ObjectBody(self.value),
            "ContentLength": len(self.value),
            "ContentType": "application/x-tar",
            "Metadata": self.metadata,
        }


async def test_s3_build_source_reverifies_exact_object_and_removes_temporary_file(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "pyproject.toml").write_text("[project]\nname='demo'\nversion='0.1'\n")
    import subprocess

    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "add", "pyproject.toml"], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-qm",
            "init",
        ],
        check=True,
    )
    archive = tmp_path / "source.tar"
    snapshot = create_personal_dev_source_snapshot(repo, archive)
    candidate = _candidate(
        candidate_sha=hashlib.sha256(b"candidate").hexdigest(),
        source_sha256=snapshot.source_digest,
        archive_sha256=snapshot.archive_sha256,
        source_commit=snapshot.manifest.source_commit,
        dirty=snapshot.manifest.dirty,
        manifest_json=asdict(snapshot.manifest),
        archive_size_bytes=archive.stat().st_size,
    )
    canonical_key = (
        f"personal-dev/sources/{candidate.owner_team_id}/{candidate.owner_user_id}/"
        f"{candidate.candidate_sha}/{candidate.source_generation_id}/"
        f"{candidate.archive_sha256}.tar"
    )
    candidate = replace(candidate, object_key=canonical_key)
    store = _ObjectStore(
        archive.read_bytes(),
        {
            "archive-sha256": candidate.archive_sha256,
            "attestation-scope": "personal-dev-only",
            "build-contract-sha256": candidate.build_contract_sha256,
            "candidate-sha256": candidate.candidate_sha,
            "source-sha256": candidate.source_sha256,
        },
    )
    source = S3PersonalDevBuildSource(
        object_store=store,
        expected_bucket="artifacts",
        max_archive_bytes=1024 * 1024,
    )

    async with source(candidate) as staged:
        assert staged.read_bytes() == archive.read_bytes()
        staged_path = staged
    assert not staged_path.exists()
    assert store.calls == 1

    with pytest.raises(PersonalDevSourceError, match="object key"):
        async with source(replace(candidate, object_key="personal-dev/other.tar")):
            pass
    assert store.calls == 1

    with pytest.raises(PersonalDevSourceError, match="manifest binding"):
        async with source(replace(candidate, manifest_json={})):
            pass
