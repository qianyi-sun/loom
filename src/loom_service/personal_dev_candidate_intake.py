"""Authenticated, content-addressed intake for personal-dev source candidates.

The browser/CLI supplies an archive and its two snapshot digests.  This module
does not trust either claim: it bounds the upload, verifies the canonical tar
without extraction, publishes only verified bytes, then asks the registry to
atomically create the immutable candidate and its first build attempt.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Protocol
from uuid import UUID, uuid4

from fastapi import HTTPException, UploadFile

from loom.personal_dev_candidate import (
    PERSONAL_DEV_BUILD_CONTRACT_SHA256,
    CandidateRegistration,
    CandidateRegistry,
    PersonalDevCandidateQuotaError,
    PersonalDevCandidateRecord,
)
from loom.personal_dev_source import (
    PersonalDevSourceError,
    verify_personal_dev_source_snapshot,
)

DEFAULT_MAX_PERSONAL_DEV_ARCHIVE_BYTES = 384 * 1024 * 1024
_UPLOAD_CHUNK_BYTES = 1024 * 1024


class SyncObjectStore(Protocol):
    def put_object(self, **kwargs: Any) -> object: ...

    def delete_object(self, **kwargs: Any) -> object: ...


def _validate_digest(value: str, *, label: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise HTTPException(status_code=400, detail=f"invalid {label} SHA-256")


def _validate_upload_filename(filename: str | None) -> None:
    if filename is None or not filename or filename.strip() != filename:
        raise HTTPException(status_code=400, detail="invalid source archive filename")
    path = PurePosixPath(filename)
    if path.is_absolute() or len(path.parts) != 1 or path.name != filename or path.suffix != ".tar":
        raise HTTPException(status_code=400, detail="invalid source archive filename")


def _candidate_sha(*, source_sha256: str, archive_sha256: str) -> str:
    digest = hashlib.sha256()
    digest.update(b"loom-personal-dev-candidate-v1\0")
    digest.update(source_sha256.encode("ascii"))
    digest.update(b"\0")
    digest.update(archive_sha256.encode("ascii"))
    digest.update(b"\0")
    digest.update(PERSONAL_DEV_BUILD_CONTRACT_SHA256.encode("ascii"))
    return digest.hexdigest()


async def _receive_archive(
    source_upload: UploadFile,
    destination: Path,
    *,
    max_archive_bytes: int,
) -> int:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW
    descriptor = os.open(destination, flags, 0o600)
    observed = 0
    try:
        while chunk := await source_upload.read(_UPLOAD_CHUNK_BYTES):
            observed += len(chunk)
            if observed > max_archive_bytes:
                raise HTTPException(
                    status_code=413,
                    detail="personal-dev source archive exceeds the upload byte limit",
                )
            view = memoryview(chunk)
            while view:
                written = os.write(descriptor, view)
                view = view[written:]
        if observed == 0:
            raise HTTPException(status_code=400, detail="empty personal-dev source archive")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return observed


def _publish_archive(
    object_store: SyncObjectStore,
    *,
    archive_path: Path,
    bucket: str,
    key: str,
    size_bytes: int,
    candidate_sha: str,
    source_sha256: str,
    archive_sha256: str,
) -> None:
    descriptor = os.open(archive_path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(descriptor)
        if metadata.st_nlink != 1 or metadata.st_size != size_bytes:
            raise RuntimeError("verified personal-dev source archive changed before publication")
        with os.fdopen(os.dup(descriptor), "rb") as body:
            object_store.put_object(
                Bucket=bucket,
                Key=key,
                Body=body,
                ContentLength=size_bytes,
                ContentType="application/x-tar",
                Metadata={
                    "archive-sha256": archive_sha256,
                    "attestation-scope": "personal-dev-only",
                    "build-contract-sha256": PERSONAL_DEV_BUILD_CONTRACT_SHA256,
                    "candidate-sha256": candidate_sha,
                    "source-sha256": source_sha256,
                },
            )
    finally:
        os.close(descriptor)


async def intake_personal_dev_candidate(
    *,
    registry: CandidateRegistry,
    object_store: SyncObjectStore,
    bucket: str,
    owner_user_id: UUID,
    owner_team_id: UUID,
    source_upload: UploadFile,
    expected_source_sha256: str,
    expected_archive_sha256: str,
    max_archive_bytes: int = DEFAULT_MAX_PERSONAL_DEV_ARCHIVE_BYTES,
) -> CandidateRegistration:
    """Verify, publish, and transactionally enqueue one immutable candidate."""

    _validate_upload_filename(source_upload.filename)
    _validate_digest(expected_source_sha256, label="source")
    _validate_digest(expected_archive_sha256, label="archive")
    if not bucket or bucket.strip() != bucket or "/" in bucket:
        raise RuntimeError("personal-dev source bucket is invalid")
    if type(max_archive_bytes) is not int or max_archive_bytes <= 0:
        raise ValueError("max_archive_bytes must be a positive integer")

    with tempfile.TemporaryDirectory(prefix="loom-personal-dev-intake-") as temporary:
        archive_path = Path(temporary) / "source.tar"
        size_bytes = await _receive_archive(
            source_upload,
            archive_path,
            max_archive_bytes=max_archive_bytes,
        )
        try:
            manifest = await asyncio.to_thread(
                verify_personal_dev_source_snapshot,
                archive_path,
                expected_source_digest=expected_source_sha256,
                expected_archive_sha256=expected_archive_sha256,
            )
        except PersonalDevSourceError as exc:
            raise HTTPException(
                status_code=400,
                detail=f"invalid personal-dev source archive: {exc}",
            ) from exc

        candidate_sha = _candidate_sha(
            source_sha256=expected_source_sha256,
            archive_sha256=expected_archive_sha256,
        )
        object_key = (
            f"personal-dev/sources/{owner_team_id}/{owner_user_id}/"
            f"{candidate_sha}/{expected_archive_sha256}.tar"
        )
        try:
            await asyncio.to_thread(
                _publish_archive,
                object_store,
                archive_path=archive_path,
                bucket=bucket,
                key=object_key,
                size_bytes=size_bytes,
                candidate_sha=candidate_sha,
                source_sha256=expected_source_sha256,
                archive_sha256=expected_archive_sha256,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail="personal-dev source publication failed",
            ) from exc

        now = datetime.now(UTC)
        requested = PersonalDevCandidateRecord(
            id=uuid4(),
            owner_user_id=owner_user_id,
            owner_team_id=owner_team_id,
            candidate_sha=candidate_sha,
            source_sha256=expected_source_sha256,
            archive_sha256=expected_archive_sha256,
            build_contract_sha256=PERSONAL_DEV_BUILD_CONTRACT_SHA256,
            source_commit=manifest.source_commit,
            dirty=manifest.dirty,
            manifest_json=asdict(manifest),
            object_bucket=bucket,
            object_key=object_key,
            archive_size_bytes=size_bytes,
            status="uploaded",
            created_at=now,
            updated_at=now,
        )
        try:
            return await registry.register(requested)
        except PersonalDevCandidateQuotaError:
            try:
                await asyncio.to_thread(
                    object_store.delete_object,
                    Bucket=bucket,
                    Key=object_key,
                )
            except Exception as exc:
                raise HTTPException(
                    status_code=503,
                    detail="personal-dev rejected source cleanup failed",
                ) from exc
            raise
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail="personal-dev candidate registration failed",
            ) from exc


__all__ = [
    "DEFAULT_MAX_PERSONAL_DEV_ARCHIVE_BYTES",
    "intake_personal_dev_candidate",
]
