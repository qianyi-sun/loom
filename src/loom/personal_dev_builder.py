"""Lease-fenced coordinator for isolated personal-candidate builds.

The coordinator is intentionally not a build engine. Candidate-controlled
source is handed only to a restricted backend whose trusted exporter returns a
complete publication. The management service independently revalidates the
sealed source and publication before committing candidate readiness.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import tempfile
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from loom.personal_dev_candidate import (
    CandidateRegistration,
    PersonalDevCandidateBuildAttemptRecord,
    PersonalDevCandidateRecord,
    validate_personal_dev_candidate_publication,
)
from loom.personal_dev_source import (
    PersonalDevSourceError,
    verify_personal_dev_source_snapshot,
)

_DOWNLOAD_CHUNK_BYTES = 1024 * 1024


class PersonalDevBuildAuthority(Protocol):
    async def claim_next_build(self, **kwargs: object) -> CandidateRegistration | None: ...

    async def start_build(self, **kwargs: object) -> PersonalDevCandidateBuildAttemptRecord: ...

    async def heartbeat_build(self, **kwargs: object) -> PersonalDevCandidateBuildAttemptRecord: ...

    async def finish_build(self, **kwargs: object) -> CandidateRegistration: ...


class PersonalDevBuildExecutor(Protocol):
    """Candidate-independent restricted backend plus trusted exporter."""

    async def build(
        self,
        registration: CandidateRegistration,
        *,
        source_archive: Path,
    ) -> Mapping[str, object]: ...

    async def cleanup(self, registration: CandidateRegistration) -> None: ...


PersonalDevBuildSource = Callable[
    [PersonalDevCandidateRecord],
    AbstractAsyncContextManager[Path],
]


class _BuilderOutputInvalidError(RuntimeError):
    pass


def _claimed_attempt(
    registration: CandidateRegistration,
    *,
    builder_id: str,
    now: datetime,
) -> PersonalDevCandidateBuildAttemptRecord:
    candidate = registration.candidate
    attempt = registration.build_attempt
    if (
        candidate.status != "building"
        or attempt is None
        or attempt.candidate_id != candidate.id
        or attempt.state != "claimed"
        or attempt.claimed_by != builder_id
        or attempt.lease_epoch <= 0
        or attempt.lease_expires_at is None
        or attempt.lease_expires_at <= now
    ):
        raise RuntimeError("personal-dev build claim is inconsistent")
    return attempt


@dataclass(slots=True)
class PersonalDevBuildCoordinator:
    """Advance at most one build while heartbeating its exact durable lease."""

    authority: PersonalDevBuildAuthority
    source: PersonalDevBuildSource
    executor: PersonalDevBuildExecutor
    builder_id: str
    lease_seconds: int
    heartbeat_interval_seconds: float | None = None

    def __post_init__(self) -> None:
        if (
            not self.builder_id
            or self.builder_id.strip() != self.builder_id
            or len(self.builder_id) > 128
        ):
            raise ValueError("builder_id must be a non-empty bounded identifier")
        if type(self.lease_seconds) is not int or self.lease_seconds <= 0:
            raise ValueError("lease_seconds must be a positive integer")
        if self.heartbeat_interval_seconds is not None and (
            self.heartbeat_interval_seconds <= 0
            or self.heartbeat_interval_seconds >= self.lease_seconds
        ):
            raise ValueError("builder heartbeat interval must be positive and below the lease")

    async def build_once(self, *, now: datetime) -> bool:
        if now.tzinfo is None:
            raise ValueError("personal-dev builder time must include a timezone")
        loop = asyncio.get_running_loop()
        started = loop.time()
        registration = await self.authority.claim_next_build(
            builder_id=self.builder_id,
            now=now,
            lease_seconds=self.lease_seconds,
        )
        if registration is None:
            return False
        attempt = _claimed_attempt(registration, builder_id=self.builder_id, now=now)
        lease = {
            "attempt_id": attempt.id,
            "builder_id": self.builder_id,
            "lease_epoch": attempt.lease_epoch,
        }
        running_attempt = await self.authority.start_build(**lease, now=now)
        if (
            running_attempt.id != attempt.id
            or running_attempt.candidate_id != registration.candidate.id
            or running_attempt.state != "running"
            or running_attempt.claimed_by != self.builder_id
            or running_attempt.lease_epoch != attempt.lease_epoch
        ):
            raise RuntimeError("personal-dev build start acknowledgement is inconsistent")
        running = replace(registration, build_attempt=running_attempt)

        async def execute() -> dict[str, object]:
            entered_executor = False
            try:
                async with self.source(running.candidate) as archive:
                    entered_executor = True
                    publication = await self.executor.build(
                        running,
                        source_archive=archive,
                    )
                    try:
                        normalized, _publication_sha256, _image_manifest = (
                            validate_personal_dev_candidate_publication(
                                running.candidate,
                                publication,
                            )
                        )
                    except ValueError as exc:
                        raise _BuilderOutputInvalidError from exc
                    return normalized
            finally:
                if entered_executor:
                    await self.executor.cleanup(running)

        task = asyncio.create_task(
            execute(),
            name=f"loom-personal-dev-build-{attempt.id}",
        )
        heartbeat_interval = self.heartbeat_interval_seconds or max(
            0.1,
            self.lease_seconds / 3,
        )
        failure_reason: str | None = None
        publication: dict[str, object] | None = None
        try:
            while True:
                done, _pending = await asyncio.wait({task}, timeout=heartbeat_interval)
                if done:
                    publication = await task
                    break
                heartbeat_now = now + timedelta(seconds=loop.time() - started)
                await self.authority.heartbeat_build(
                    **lease,
                    now=heartbeat_now,
                    lease_seconds=self.lease_seconds,
                )
        except asyncio.CancelledError:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            raise
        except _BuilderOutputInvalidError:
            failure_reason = "builder_output_invalid"
        except PersonalDevSourceError:
            failure_reason = "source_verification_failed"
        except Exception:
            failure_reason = "builder_failed"
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        callback_now = now + timedelta(seconds=loop.time() - started)
        if publication is not None:
            await self.authority.finish_build(
                **lease,
                now=callback_now,
                publication=publication,
            )
        else:
            await self.authority.finish_build(
                **lease,
                now=callback_now,
                failure_reason=failure_reason or "builder_failed",
            )
        return True


class SyncObjectBody(Protocol):
    def read(self, size: int) -> bytes: ...

    def close(self) -> None: ...


class SyncObjectStore(Protocol):
    def get_object(self, **kwargs: Any) -> Mapping[str, object]: ...


def _canonical_source_key(candidate: PersonalDevCandidateRecord) -> str:
    return (
        f"personal-dev/sources/{candidate.owner_team_id}/{candidate.owner_user_id}/"
        f"{candidate.candidate_sha}/{candidate.archive_sha256}.tar"
    )


@dataclass(slots=True)
class S3PersonalDevBuildSource:
    """Reacquire and independently verify one exact intake object."""

    object_store: SyncObjectStore
    expected_bucket: str
    max_archive_bytes: int

    def __post_init__(self) -> None:
        if (
            not self.expected_bucket
            or self.expected_bucket.strip() != self.expected_bucket
            or "/" in self.expected_bucket
        ):
            raise ValueError("personal-dev source bucket is invalid")
        if type(self.max_archive_bytes) is not int or self.max_archive_bytes <= 0:
            raise ValueError("personal-dev builder archive limit must be positive")

    def _download(self, candidate: PersonalDevCandidateRecord, destination: Path) -> None:
        if candidate.object_bucket != self.expected_bucket:
            raise PersonalDevSourceError(
                "personal-dev source object bucket is not authoritative"
            )
        if candidate.object_key != _canonical_source_key(candidate):
            raise PersonalDevSourceError("personal-dev source object key is not canonical")
        if not 0 < candidate.archive_size_bytes <= self.max_archive_bytes:
            raise PersonalDevSourceError(
                "personal-dev source object size is outside builder limits"
            )
        response = self.object_store.get_object(
            Bucket=candidate.object_bucket,
            Key=candidate.object_key,
        )
        body = response.get("Body")
        metadata = response.get("Metadata")
        if (
            not hasattr(body, "read")
            or not hasattr(body, "close")
            or response.get("ContentLength") != candidate.archive_size_bytes
            or response.get("ContentType") != "application/x-tar"
            or not isinstance(metadata, Mapping)
        ):
            if hasattr(body, "close"):
                body.close()
            raise PersonalDevSourceError("personal-dev source object metadata is invalid")
        expected_metadata = {
            "archive-sha256": candidate.archive_sha256,
            "attestation-scope": "personal-dev-only",
            "build-contract-sha256": candidate.build_contract_sha256,
            "candidate-sha256": candidate.candidate_sha,
            "source-sha256": candidate.source_sha256,
        }
        if any(metadata.get(key) != value for key, value in expected_metadata.items()):
            typed_body = body
            typed_body.close()
            raise PersonalDevSourceError("personal-dev source object binding is invalid")
        descriptor = os.open(
            destination,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
        )
        observed = 0
        digest = hashlib.sha256()
        typed_body = body  # narrowed structurally by the checks above
        try:
            while chunk := typed_body.read(_DOWNLOAD_CHUNK_BYTES):
                if not isinstance(chunk, bytes):
                    raise PersonalDevSourceError("personal-dev source object body is invalid")
                observed += len(chunk)
                if observed > candidate.archive_size_bytes:
                    raise PersonalDevSourceError("personal-dev source object exceeded its binding")
                digest.update(chunk)
                view = memoryview(chunk)
                while view:
                    written = os.write(descriptor, view)
                    view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
            typed_body.close()
        if (
            observed != candidate.archive_size_bytes
            or digest.hexdigest() != candidate.archive_sha256
        ):
            raise PersonalDevSourceError("personal-dev source object digest is invalid")
        manifest = verify_personal_dev_source_snapshot(
            destination,
            expected_source_digest=candidate.source_sha256,
            expected_archive_sha256=candidate.archive_sha256,
        )
        if (
            manifest.source_commit != candidate.source_commit
            or manifest.dirty is not candidate.dirty
            or manifest.file_count != len(manifest.files)
            or json.dumps(
                asdict(manifest),
                sort_keys=True,
                separators=(",", ":"),
            )
            != json.dumps(
                dict(candidate.manifest_json),
                sort_keys=True,
                separators=(",", ":"),
            )
        ):
            raise PersonalDevSourceError("personal-dev source manifest binding is invalid")

    @asynccontextmanager
    async def __call__(
        self,
        candidate: PersonalDevCandidateRecord,
    ) -> AsyncIterator[Path]:
        with tempfile.TemporaryDirectory(prefix="loom-personal-dev-build-source-") as directory:
            archive = Path(directory) / "source.tar"
            await asyncio.to_thread(self._download, candidate, archive)
            yield archive


__all__ = [
    "PersonalDevBuildAuthority",
    "PersonalDevBuildCoordinator",
    "PersonalDevBuildExecutor",
    "PersonalDevBuildSource",
    "S3PersonalDevBuildSource",
]
