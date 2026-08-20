"""Marker-verified, server-owned TerminalGen corpus publication runtime."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
from collections.abc import AsyncIterator
from typing import Any, Protocol, cast

import yaml  # type: ignore[import-untyped]

from loom.integrations.terminalgen.artifacts import (
    ArtifactRefV1,
    TerminalGenCorpusArtifactV1,
    TerminalGenFinalAuditArtifactV1,
    TerminalGenPublicationRequestV1,
    TerminalGenTaskSetSmokeV1,
    validate_artifact_document,
)
from loom.integrations.terminalgen.publication import (
    RuntimeTaskArchive,
    TaskSetSmokeArchive,
    TaskSetSmokeArchiveBuilder,
    TerminalGenPublicationError,
    TerminalGenPublicationMaterial,
    validate_publication_material,
    validate_task_archive,
)
from loom.models.taskset import UserTaskSetManifest
from loom.pipeline.artifact_commit import MAX_APPLICATION_BUFFER_BYTES
from loom.pipeline.keys import canonical_document, digest_bytes
from loom.trajectory.storage import ObjectStore
from loom_pipeline_orchestrator.repository import (
    RunLease,
    TerminalGenPublicationArtifactSource,
    TerminalGenPublicationCandidate,
)

_MAX_DOCUMENT_BYTES = 67_108_864
_MAX_TASK_ARCHIVE_BYTES = 268_435_456


class TerminalGenPublicationRepositoryV1(Protocol):
    async def terminalgen_publication_candidate(
        self,
        lease: RunLease,
    ) -> TerminalGenPublicationCandidate | None: ...

    async def publish_terminalgen_corpus(
        self,
        lease: RunLease,
        *,
        candidate: TerminalGenPublicationCandidate,
        request_sha256: str,
        material: TerminalGenPublicationMaterial,
        smoke: TerminalGenTaskSetSmokeV1,
        smoke_object_key: str,
        taskset_manifest_object_key: str,
        taskset_manifest_json: dict[str, Any],
    ) -> object: ...

    async def fail_terminalgen_publication(
        self,
        lease: RunLease,
        *,
        candidate: TerminalGenPublicationCandidate,
        request_sha256: str,
        reason_code: str,
    ) -> None: ...


class TerminalGenCorpusPublicationRuntime:
    def __init__(
        self,
        *,
        repository: TerminalGenPublicationRepositoryV1,
        store: ObjectStore,
        bucket: str,
    ) -> None:
        self._repository = repository
        self._store = store
        self._bucket = bucket

    async def reconcile(self, lease: RunLease) -> bool:
        candidate = await self._repository.terminalgen_publication_candidate(lease)
        if candidate is None:
            return False
        request_sha256 = candidate.request.content_sha256
        try:
            request_bytes = await self._read_semantic_document(candidate.request)
            request_sha256 = digest_bytes(request_bytes)
            request = cast(
                TerminalGenPublicationRequestV1,
                validate_artifact_document_from_bytes(
                    candidate.request.artifact_type,
                    request_bytes,
                ),
            )
            self._validate_request_authority(candidate, request)
            audit = cast(
                TerminalGenFinalAuditArtifactV1,
                validate_artifact_document_from_bytes(
                    candidate.final_audit.artifact_type,
                    await self._read_semantic_document(candidate.final_audit),
                ),
            )
            authoring = cast(
                TerminalGenCorpusArtifactV1,
                validate_artifact_document_from_bytes(
                    candidate.authoring_corpus.artifact_type,
                    await self._read_semantic_document(candidate.authoring_corpus),
                ),
            )
            runtime = cast(
                TerminalGenCorpusArtifactV1,
                validate_artifact_document_from_bytes(
                    candidate.runtime_corpus.artifact_type,
                    await self._read_semantic_document(candidate.runtime_corpus),
                ),
            )
            material = validate_publication_material(
                TerminalGenPublicationMaterial(
                    request=request,
                    final_audit=audit,
                    authoring_corpus=authoring,
                    runtime_corpus=runtime,
                )
            )
            self._validate_corpus_inventory(candidate.authoring_corpus, authoring)
            self._validate_corpus_inventory(candidate.runtime_corpus, runtime)
            await self._validate_all_task_archives(candidate.authoring_corpus, authoring)
            smoke_archive = await self._build_and_validate_runtime_tasks(
                candidate.runtime_corpus,
                runtime,
                request.taskset_smoke_count,
            )
            try:
                prefix = (
                    f"pipeline-corpora/{candidate.team_id}/{request.corpus_id}/"
                    f"{material.corpus_version_sha256.removeprefix('sha256:')}/"
                    f"{smoke_archive.sha256.removeprefix('sha256:')}/"
                )
                smoke_key = f"{prefix}taskset-smoke.tar"
                manifest_key = f"{prefix}taskset-smoke.manifest.yaml"
                await self._put_archive(smoke_key, smoke_archive)
                await self._put_bytes(manifest_key, smoke_archive.manifest_bytes)
                await self._verify_object(
                    key=smoke_key,
                    expected_size=smoke_archive.size_bytes,
                    expected_sha256=smoke_archive.sha256,
                )
                await self._verify_object(
                    key=manifest_key,
                    expected_size=len(smoke_archive.manifest_bytes),
                    expected_sha256=smoke_archive.manifest_sha256,
                )
                manifest_model = UserTaskSetManifest.model_validate(
                    yaml.safe_load(smoke_archive.manifest_bytes)
                )
                smoke = TerminalGenTaskSetSmokeV1(
                    schema_version="terminalgen.taskset-smoke.v1",
                    corpus_version_sha256=material.corpus_version_sha256,
                    task_count=len(smoke_archive.task_ids),
                    task_ids=list(smoke_archive.task_ids),
                    manifest_sha256=smoke_archive.manifest_sha256,
                    archive_sha256=smoke_archive.sha256,
                    archive_size_bytes=smoke_archive.size_bytes,
                )
                await self._repository.publish_terminalgen_corpus(
                    lease,
                    candidate=candidate,
                    request_sha256=request_sha256,
                    material=material,
                    smoke=smoke,
                    smoke_object_key=smoke_key,
                    taskset_manifest_object_key=manifest_key,
                    taskset_manifest_json=manifest_model.model_dump(
                        mode="json",
                        by_alias=True,
                        exclude_none=True,
                    ),
                )
            finally:
                smoke_archive.close()
        except TerminalGenPublicationError as exc:
            await self._repository.fail_terminalgen_publication(
                lease,
                candidate=candidate,
                request_sha256=request_sha256,
                reason_code=exc.reason_code,
            )
        except ValueError:
            await self._repository.fail_terminalgen_publication(
                lease,
                candidate=candidate,
                request_sha256=request_sha256,
                reason_code="publication_document_invalid",
            )
        return True

    @staticmethod
    def _validate_request_authority(
        candidate: TerminalGenPublicationCandidate,
        request: TerminalGenPublicationRequestV1,
    ) -> None:
        expected = (
            (
                request.final_audit_artifact,
                candidate.final_audit,
            ),
            (
                request.authoring_corpus_artifact,
                candidate.authoring_corpus,
            ),
            (
                request.runtime_corpus_artifact,
                candidate.runtime_corpus,
            ),
        )
        if (
            request.pipeline_run_id != candidate.pipeline_run_id
            or request.recipe_digest != candidate.recipe_digest
        ):
            raise TerminalGenPublicationError("publication_request_authority_drift")
        for reference, source in expected:
            observed = ArtifactRefV1(
                artifact_id=source.artifact_id,
                artifact_type=source.artifact_type,
                manifest_sha256=source.manifest_sha256,
            )
            if reference != observed:
                raise TerminalGenPublicationError("publication_request_reference_drift")

    @staticmethod
    def _validate_corpus_inventory(
        source: TerminalGenPublicationArtifactSource,
        corpus: TerminalGenCorpusArtifactV1,
    ) -> None:
        payloads = {item.relative_path: item for item in source.files if item.role != "semantic_document"}
        expected = {item.bundle_relative_path: item for item in corpus.tasks}
        if set(payloads) != set(expected):
            raise TerminalGenPublicationError("publication_corpus_inventory_drift")
        for path, task in expected.items():
            stored = payloads[path]
            if (
                stored.role != "payload_archive"
                or stored.archive_format != "tar"
                or stored.sha256 != task.bundle_sha256
                or stored.size_bytes != task.bundle_size_bytes
            ):
                raise TerminalGenPublicationError("publication_corpus_inventory_drift")

    async def _validate_all_task_archives(
        self,
        source: TerminalGenPublicationArtifactSource,
        corpus: TerminalGenCorpusArtifactV1,
    ) -> None:
        by_path = {item.relative_path: item for item in source.files}
        for task in corpus.tasks:
            stored = by_path[task.bundle_relative_path]
            body = await self._read_object_exact(
                key=stored.storage_key,
                expected_size=stored.size_bytes,
                expected_sha256=stored.sha256,
                max_bytes=_MAX_TASK_ARCHIVE_BYTES,
            )
            validate_task_archive(RuntimeTaskArchive(entry=task, body=body))

    async def _build_and_validate_runtime_tasks(
        self,
        source: TerminalGenPublicationArtifactSource,
        corpus: TerminalGenCorpusArtifactV1,
        smoke_count: int,
    ) -> TaskSetSmokeArchive:
        by_path = {item.relative_path: item for item in source.files}
        builder = TaskSetSmokeArchiveBuilder(
            corpus_id=corpus.corpus_id,
            corpus_version=corpus.corpus_version,
            expected_task_count=smoke_count,
        )
        try:
            for index, task in enumerate(corpus.tasks):
                stored = by_path[task.bundle_relative_path]
                body = await self._read_object_exact(
                    key=stored.storage_key,
                    expected_size=stored.size_bytes,
                    expected_sha256=stored.sha256,
                    max_bytes=_MAX_TASK_ARCHIVE_BYTES,
                )
                archive = RuntimeTaskArchive(entry=task, body=body)
                if index < smoke_count:
                    builder.add(archive)
                else:
                    validate_task_archive(archive)
            return builder.finish()
        except Exception:
            builder.close()
            raise

    async def _read_semantic_document(
        self,
        source: TerminalGenPublicationArtifactSource,
    ) -> bytes:
        await self._assert_exact_object(
            key=source.root_manifest_key,
            expected=source.root_manifest_bytes,
            expected_sha256=source.root_manifest_sha256,
        )
        await self._assert_exact_object(
            key=source.committed_marker_key,
            expected=source.committed_marker_bytes,
            expected_sha256=source.committed_marker_sha256,
        )
        semantic = next(item for item in source.files if item.role == "semantic_document")
        return await self._read_object_exact(
            key=semantic.storage_key,
            expected_size=semantic.size_bytes,
            expected_sha256=semantic.sha256,
            max_bytes=_MAX_DOCUMENT_BYTES,
        )

    async def _assert_exact_object(
        self,
        *,
        key: str,
        expected: bytes,
        expected_sha256: str,
    ) -> None:
        observed = await self._read_object_exact(
            key=key,
            expected_size=len(expected),
            expected_sha256=expected_sha256,
            max_bytes=_MAX_DOCUMENT_BYTES,
        )
        if not hmac.compare_digest(observed, expected):
            raise TerminalGenPublicationError("publication_root_marker_drift")

    async def _read_object_exact(
        self,
        *,
        key: str,
        expected_size: int,
        expected_sha256: str,
        max_bytes: int,
    ) -> bytes:
        if not 1 <= expected_size <= max_bytes:
            raise TerminalGenPublicationError("publication_object_size_invalid")
        payload = bytearray()
        digest = hashlib.sha256()
        async for chunk in self._store.stream_object(
            bucket=self._bucket,
            key=key,
            chunk_size=MAX_APPLICATION_BUFFER_BYTES,
        ):
            if not chunk or len(payload) + len(chunk) > expected_size:
                raise TerminalGenPublicationError("publication_object_size_drift")
            payload.extend(chunk)
            digest.update(chunk)
        observed = f"sha256:{digest.hexdigest()}"
        if len(payload) != expected_size or not hmac.compare_digest(observed, expected_sha256):
            raise TerminalGenPublicationError("publication_object_digest_drift")
        return bytes(payload)

    async def _put_archive(self, key: str, archive: TaskSetSmokeArchive) -> None:
        async def chunks() -> AsyncIterator[bytes]:
            archive.file.seek(0)
            while True:
                chunk = await asyncio.to_thread(
                    archive.file.read,
                    MAX_APPLICATION_BUFFER_BYTES,
                )
                if not chunk:
                    return
                yield chunk

        await self._store.put_object_stream(bucket=self._bucket, key=key, body=chunks())

    async def _put_bytes(self, key: str, body: bytes) -> None:
        await self._store.put_object(bucket=self._bucket, key=key, body=body)

    async def _verify_object(
        self,
        *,
        key: str,
        expected_size: int,
        expected_sha256: str,
    ) -> None:
        facts = await self._store.stat_object(bucket=self._bucket, key=key)
        if facts.content_length != expected_size:
            raise TerminalGenPublicationError("publication_object_readback_drift")
        if facts.checksum_sha256 is not None and hmac.compare_digest(
            facts.checksum_sha256,
            expected_sha256,
        ):
            return
        await self._read_object_exact(
            key=key,
            expected_size=expected_size,
            expected_sha256=expected_sha256,
            max_bytes=max(expected_size, 1),
        )


def validate_artifact_document_from_bytes(artifact_type: str, body: bytes) -> object:
    try:
        value = validate_artifact_document_from_json(body)
        result = validate_artifact_document(value)
    except ValueError as exc:
        raise TerminalGenPublicationError("publication_document_invalid") from exc
    if getattr(result, "schema_version", None) != artifact_type or canonical_document(result) != body:
        raise TerminalGenPublicationError("publication_document_noncanonical")
    return result


def validate_artifact_document_from_json(body: bytes) -> object:
    import json

    return json.loads(body)


__all__ = [
    "TerminalGenCorpusPublicationRuntime",
    "TerminalGenPublicationRepositoryV1",
    "validate_artifact_document_from_bytes",
]
