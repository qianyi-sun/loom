"""Claim-bound Artifact input materialization into a verified local CAS."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import shutil
import stat
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol
from uuid import UUID, uuid4

import httpx

from loom.pipeline.artifact_commit import ArtifactManifestV1, StoredFileV1
from loom.pipeline.keys import canonical_document, canonical_identity, digest_bytes
from loom.pipeline.spec import BindingItemV1, BindingSetV1
from loom.pipeline.work_protocol import (
    ExecutionAttemptClaimV1,
    PipelineInputMaterializationEvidenceRefV1,
    PipelineInputMaterializationEvidenceReportV1,
    StageRequestGrantV1,
)
from loom_worker.artifact_input_journal import (
    ArtifactInputJournal,
    ArtifactInputJournalError,
    CacheEntry,
)
from loom_worker.artifact_safe_extract import (
    MAX_EXPANSION_RATIO,
    MAX_EXTRACTED_FILES,
    MAX_UNPACKED_BYTES,
    SafeExtractionError,
    extract_archive,
)
from loom_worker.control_plane_client import (
    ExecutionAttemptClaimHeaders,
    HttpControlPlaneClient,
)

MAX_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_SIDECAR_BYTES = 64 * 1024 * 1024
RETRY_DELAYS = (0.0, 1.0, 4.0, 16.0)


class ArtifactInputError(RuntimeError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class CancellationSignal(Protocol):
    async def requested(self) -> bool: ...


class ReadOnlyViewMounter(Protocol):
    async def bind_read_only(self, *, source: Path, target: Path) -> None: ...

    async def unmount(self, *, target: Path) -> None: ...


class MountCommandRunner(Protocol):
    async def run(self, argv: Sequence[str]) -> None: ...


@dataclass
class SubprocessMountCommandRunner:
    async def run(self, argv: Sequence[str]) -> None:
        process = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _stdout, stderr = await process.communicate()
        if process.returncode != 0:
            raise ArtifactInputError(
                "input_view_mount_failed"
                if argv[0] == "mount"
                else "input_view_unmount_failed"
            ) from RuntimeError(stderr.decode("utf-8", errors="replace")[:500])


@dataclass
class LinuxReadOnlyViewMounter:
    """Recursive private bind mount with the exact v1 read-only flags."""

    runner: MountCommandRunner = field(default_factory=SubprocessMountCommandRunner)

    async def bind_read_only(self, *, source: Path, target: Path) -> None:
        source = source.resolve(strict=True)
        if source.is_symlink() or not source.is_dir():
            raise ArtifactInputError("input_view_source_invalid")
        target.mkdir(parents=True, mode=0o500, exist_ok=False)
        await self.runner.run(("mount", "--rbind", str(source), str(target)))
        try:
            await self.runner.run(("mount", "--make-rprivate", str(target)))
            await self.runner.run(
                (
                    "mount",
                    "-o",
                    "remount,bind,ro,nosuid,nodev,noexec",
                    str(target),
                )
            )
        except Exception:
            await self.runner.run(("umount", str(target)))
            raise

    async def unmount(self, *, target: Path) -> None:
        if target.exists():
            await self.runner.run(("umount", str(target)))


@dataclass
class DeterministicReadOnlyViewMounter:
    """Test seam recording the bind operations without copying CAS content."""

    mounts: dict[Path, Path] = field(default_factory=dict)

    async def bind_read_only(self, *, source: Path, target: Path) -> None:
        if target in self.mounts:
            if self.mounts[target] != source:
                raise ArtifactInputError("input_view_mount_conflict")
            return
        target.mkdir(parents=True, exist_ok=True)
        self.mounts[target] = source

    async def unmount(self, *, target: Path) -> None:
        self.mounts.pop(target, None)


@dataclass(frozen=True)
class MaterializationCounters:
    manifest_open_count: int = 0
    file_open_count: int = 0
    file_bytes: int = 0
    archive_extraction_count: int = 0
    cas_rename_count: int = 0

    def add(self, **changes: int) -> MaterializationCounters:
        values = {
            "manifest_open_count": self.manifest_open_count,
            "file_open_count": self.file_open_count,
            "file_bytes": self.file_bytes,
            "archive_extraction_count": self.archive_extraction_count,
            "cas_rename_count": self.cas_rename_count,
        }
        for key, value in changes.items():
            if value < 0 or key not in values:
                raise ValueError("materialization counter update is invalid")
            values[key] += value
        if any(value > 9_007_199_254_740_991 for value in values.values()):
            raise ArtifactInputError("materialization_counter_overflow")
        return MaterializationCounters(**values)


@dataclass(frozen=True)
class _ReadyArtifact:
    entry: CacheEntry
    manifest: ArtifactManifestV1


@dataclass
class MaterializedInputSet:
    materializer: ArtifactInputMaterializer
    claim: ExecutionAttemptClaimV1
    cancellation: CancellationSignal
    root: Path | None = None
    input_view_digest: str | None = None
    stage_request_path: Path | None = None
    counters: MaterializationCounters = field(default_factory=MaterializationCounters)
    _entered: bool = False
    _closed: bool = False
    _evidence_reported: bool = False

    async def __aenter__(self) -> MaterializedInputSet:
        if self._entered or self._closed:
            raise ArtifactInputError("materialized_input_set_reused")
        self._entered = True
        await self.materializer._enter(self)
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self.materializer._release(self)

    def acceptance_evidence_report(
        self, *, worker_id: UUID
    ) -> PipelineInputMaterializationEvidenceReportV1:
        if not self._entered or self._closed or self.input_view_digest is None:
            raise ArtifactInputError("materialization_evidence_phase")
        grant = self.claim.acceptance_preflight
        if grant is None:
            raise ArtifactInputError("materialization_evidence_not_applicable")
        manifests = [
            item.manifest_sha256
            for binding in self.claim.input_bindings
            for item in binding.items
        ]
        return PipelineInputMaterializationEvidenceReportV1(
            schema_version="loom.pipeline-input-materialization-evidence-report.v1",
            execution_attempt_id=self.claim.execution_attempt_id,
            worker_id=worker_id,
            lease_epoch=self.claim.lease_epoch,
            cache_expectation=grant.cache_expectation,
            ordered_manifest_sha256s=manifests,
            manifest_open_count=self.counters.manifest_open_count,
            file_open_count=self.counters.file_open_count,
            file_bytes=self.counters.file_bytes,
            archive_extraction_count=self.counters.archive_extraction_count,
            cas_rename_count=self.counters.cas_rename_count,
            input_view_sha256=self.input_view_digest,
        )

    async def report_acceptance_evidence(
        self,
        *,
        control_plane: HttpControlPlaneClient,
        worker_id: UUID,
        request_id: UUID,
    ) -> PipelineInputMaterializationEvidenceRefV1:
        if self._evidence_reported:
            raise ArtifactInputError("materialization_evidence_already_reported")
        report = self.acceptance_evidence_report(worker_id=worker_id)
        response = await control_plane.report_input_materialization_evidence(
            attempt_id=self.claim.execution_attempt_id,
            claim=ArtifactInputReadClient._headers(self.claim),
            request_id=request_id,
            payload=report.model_dump(mode="json"),
        )
        reference = PipelineInputMaterializationEvidenceRefV1.model_validate(response)
        if (
            reference.attempt_id != self.claim.execution_attempt_id
            or reference.worker_id != worker_id
            or reference.lease_epoch != self.claim.lease_epoch
        ):
            raise ArtifactInputError("materialization_evidence_response_drift")
        self._evidence_reported = True
        return reference


@dataclass
class ArtifactInputReadClient:
    """Private locator-free reader; every operation is claim and descriptor bound."""

    control_plane: HttpControlPlaneClient
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep

    @staticmethod
    def _headers(claim: ExecutionAttemptClaimV1) -> ExecutionAttemptClaimHeaders:
        return ExecutionAttemptClaimHeaders(
            claim_id=claim.claim_id,
            lease_epoch=claim.lease_epoch,
            lease_token=claim.lease_token,
        )

    async def read_manifest(
        self,
        *,
        claim: ExecutionAttemptClaimV1,
        binding_name: str,
        item: BindingItemV1,
        artifact_type: str,
        cancellation: CancellationSignal,
    ) -> ArtifactManifestV1:
        response: httpx.Response | None = None
        for attempt, delay in enumerate(RETRY_DELAYS):
            await self._wait(delay, cancellation)
            try:
                response = await self.control_plane.read_execution_attempt_input_manifest(
                    attempt_id=claim.execution_attempt_id,
                    binding_name=binding_name,
                    item_key=item.item_key,
                    manifest_sha256=item.manifest_sha256,
                    claim=self._headers(claim),
                )
                break
            except (httpx.ConnectError, httpx.ReadError, httpx.TimeoutException) as exc:
                if attempt == len(RETRY_DELAYS) - 1:
                    raise ArtifactInputError("object_store_transport") from exc
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 503 and attempt < len(RETRY_DELAYS) - 1:
                    continue
                raise self._http_error(exc) from exc
        if response is None:
            raise ArtifactInputError("object_store_transport")
        expected_etag = f'"{item.manifest_sha256}"'
        content_type = response.headers.get("content-type", "").split(";", 1)[0]
        if (
            response.status_code != 200
            or response.headers.get("etag") != expected_etag
            or content_type != "application/vnd.loom.artifact-manifest+json"
            or response.headers.get("content-length") != str(len(response.content))
            or len(response.content) > MAX_MANIFEST_BYTES
        ):
            raise ArtifactInputError("input_descriptor_drift")
        try:
            manifest = ArtifactManifestV1.model_validate_json(response.content)
        except Exception as exc:
            raise ArtifactInputError("input_descriptor_drift") from exc
        if canonical_document(manifest) != response.content:
            raise ArtifactInputError("input_descriptor_drift")
        if (
            digest_bytes(response.content) != item.manifest_sha256
            or manifest.artifact_id != item.artifact_id
            or manifest.artifact_type != artifact_type
            or manifest.content_sha256 != item.content_sha256
        ):
            raise ArtifactInputError("input_descriptor_drift")
        if (
            manifest.file_count != item.file_count
            or manifest.stored_size_bytes != item.stored_size_bytes
            or manifest.unpacked_size_bytes != item.unpacked_size_bytes
            or [stored.file_index for stored in manifest.stored_files]
            != list(range(len(manifest.stored_files)))
            or sum(stored.size_bytes for stored in manifest.stored_files)
            != manifest.stored_size_bytes
        ):
            raise ArtifactInputError("input_descriptor_drift")
        return manifest

    async def read_file(
        self,
        *,
        claim: ExecutionAttemptClaimV1,
        binding_name: str,
        item: BindingItemV1,
        stored: StoredFileV1,
        destination: Path,
        cancellation: CancellationSignal,
    ) -> tuple[int, int]:
        digest = hashlib.sha256()
        observed = 0
        if destination.exists():
            destination.unlink()
        destination.parent.mkdir(parents=True, exist_ok=True)
        opens = 0
        for attempt, delay in enumerate(RETRY_DELAYS):
            await self._wait(delay, cancellation)
            range_start = observed if observed else None
            if observed == 0 and destination.exists():
                destination.unlink()
            try:
                async with self.control_plane.stream_execution_attempt_input_file(
                    attempt_id=claim.execution_attempt_id,
                    binding_name=binding_name,
                    item_key=item.item_key,
                    file_index=stored.file_index,
                    file_sha256=stored.sha256,
                    claim=self._headers(claim),
                    range_start=range_start,
                ) as response:
                    opens += 1
                    expected_status = 206 if observed else 200
                    expected_length = stored.size_bytes - observed
                    if (
                        response.status_code != expected_status
                        or response.headers.get("etag") != f'"{stored.sha256}"'
                        or response.headers.get("content-type", "").split(";", 1)[0]
                        != stored.media_type
                        or response.headers.get("content-encoding", "identity") != "identity"
                        or response.headers.get("content-length") != str(expected_length)
                    ):
                        raise ArtifactInputError("input_integrity_mismatch")
                    if observed and response.headers.get("content-range") != (
                        f"bytes {observed}-{stored.size_bytes - 1}/{stored.size_bytes}"
                    ):
                        raise ArtifactInputError("input_integrity_mismatch")
                    mode = "ab" if observed else "xb"
                    with destination.open(mode) as output:
                        async for chunk in response.aiter_bytes(64 * 1024 * 1024):
                            if await cancellation.requested():
                                raise ArtifactInputError("input_materialization_cancelled")
                            if observed + len(chunk) > stored.size_bytes:
                                raise ArtifactInputError("input_integrity_mismatch")
                            output.write(chunk)
                            digest.update(chunk)
                            observed += len(chunk)
                        output.flush()
                        os.fsync(output.fileno())
                    if observed == stored.size_bytes:
                        break
            except (httpx.ConnectError, httpx.ReadError, httpx.TimeoutException) as exc:
                if attempt == len(RETRY_DELAYS) - 1:
                    raise ArtifactInputError("object_store_transport") from exc
                continue
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 503 and attempt < len(RETRY_DELAYS) - 1:
                    continue
                raise self._http_error(exc) from exc
            if observed < stored.size_bytes and attempt == len(RETRY_DELAYS) - 1:
                raise ArtifactInputError("object_store_transport")
        if observed != stored.size_bytes or not hmac.compare_digest(
            f"sha256:{digest.hexdigest()}", stored.sha256
        ):
            raise ArtifactInputError("input_integrity_mismatch")
        return observed, opens

    async def _wait(self, delay: float, cancellation: CancellationSignal) -> None:
        if await cancellation.requested():
            raise ArtifactInputError("input_materialization_cancelled")
        if delay:
            try:
                await asyncio.wait_for(self.sleep(delay), timeout=min(delay + 1, 5))
            except TimeoutError:
                pass
        if await cancellation.requested():
            raise ArtifactInputError("input_materialization_cancelled")

    @staticmethod
    def _http_error(exc: httpx.HTTPStatusError) -> ArtifactInputError:
        status = exc.response.status_code
        if status == 503:
            return ArtifactInputError("object_store_transport")
        if status in {404, 409, 412, 416} or 300 <= status < 400:
            return ArtifactInputError("input_descriptor_drift")
        return ArtifactInputError("input_integrity_mismatch")


@dataclass
class ArtifactInputMaterializer:
    read_client: ArtifactInputReadClient
    journal: ArtifactInputJournal
    attempt_input_root: Path
    mounter: ReadOnlyViewMounter
    _manifest_locks: dict[str, asyncio.Lock] = field(default_factory=dict, init=False)

    async def materialize_inputs(
        self, *, claim: ExecutionAttemptClaimV1, cancellation: CancellationSignal
    ) -> MaterializedInputSet:
        return MaterializedInputSet(
            materializer=self,
            claim=claim,
            cancellation=cancellation,
        )

    async def _enter(self, result: MaterializedInputSet) -> None:
        claim = result.claim
        root = self.attempt_input_root.resolve() / str(claim.execution_attempt_id)
        if root.exists():
            raise ArtifactInputError("attempt_input_view_exists")
        root.mkdir(parents=True, mode=0o700)
        result.root = root
        view_records: list[dict[str, str]] = []
        try:
            bindings = self._claim_bindings(claim)
            prefetched: dict[tuple[str, str], ArtifactManifestV1] = {}
            if claim.acceptance_preflight is not None:
                if [binding.binding_name for binding in bindings] != [
                    "task_set",
                    "task_instances",
                    "dataset",
                    "policy",
                    "mop_bank",
                ] or any(len(binding.items) != 1 for binding in bindings):
                    raise ArtifactInputError("input_descriptor_drift")
                acceptance_manifests = [
                    binding.items[0].manifest_sha256 for binding in bindings
                ]
                if len(set(acceptance_manifests)) != 5:
                    raise ArtifactInputError("input_descriptor_drift")
                for binding in bindings:
                    item = binding.items[0]
                    prefetched[(binding.binding_name, item.item_key)] = (
                        await self.read_client.read_manifest(
                            claim=claim,
                            binding_name=binding.binding_name,
                            item=item,
                            artifact_type=binding.artifact_type,
                            cancellation=result.cancellation,
                        )
                    )
                    result.counters = result.counters.add(manifest_open_count=1)
                expectation = claim.acceptance_preflight.cache_expectation
                entries = [
                    self.journal.get_entry(binding.items[0].manifest_sha256)
                    for binding in bindings
                ]
                if expectation == "cold_after_eviction" and any(
                    entry is not None for entry in entries
                ):
                    raise ArtifactInputError("cold_cache_not_absent")
                if expectation == "warm_reuse_only" and any(
                    entry is None or entry.state != "ready" for entry in entries
                ):
                    raise ArtifactInputError("warm_cache_not_ready")
            for binding in bindings:
                if binding.binding_name == "stage-request.json":
                    raise ArtifactInputError("reserved_input_binding_name")
                record, counters = await self._materialize_binding(
                    result=result,
                    binding=binding,
                    prefetched=prefetched,
                )
                view_records.append(record)
                result.counters = result.counters.add(
                    manifest_open_count=counters.manifest_open_count,
                    file_open_count=counters.file_open_count,
                    file_bytes=counters.file_bytes,
                    archive_extraction_count=counters.archive_extraction_count,
                    cas_rename_count=counters.cas_rename_count,
                )
            result.stage_request_path = self._stage_request(root, claim.stage_request)
            identity = [*view_records, {"stage_request_sha256": (
                claim.stage_request.stage_request_sha256 if claim.stage_request else None
            )}]
            result.input_view_digest = digest_bytes(canonical_identity(identity))
            os.chmod(root, 0o555, follow_symlinks=False)
        except Exception:
            await self._release(result)
            raise

    @staticmethod
    def _claim_bindings(claim: ExecutionAttemptClaimV1) -> list[BindingSetV1]:
        if any(binding.binding_name == "loom_checkpoint" for binding in claim.input_bindings):
            raise ArtifactInputError("reserved_input_binding_name")
        bindings = list(claim.input_bindings)
        checkpoint = claim.resume_checkpoint
        if checkpoint is not None:
            bindings.append(
                BindingSetV1(
                    binding_name="loom_checkpoint",
                    artifact_type=checkpoint.artifact_type,
                    cardinality="one",
                    items=[
                        BindingItemV1(
                            artifact_id=checkpoint.artifact_id,
                            content_sha256=checkpoint.content_sha256,
                            file_count=checkpoint.file_count,
                            item_key="singleton",
                            manifest_sha256=checkpoint.manifest_sha256,
                            stored_size_bytes=checkpoint.stored_size_bytes,
                            unpacked_size_bytes=checkpoint.unpacked_size_bytes,
                        )
                    ],
                )
            )
        return bindings

    async def _materialize_binding(
        self,
        *,
        result: MaterializedInputSet,
        binding: BindingSetV1,
        prefetched: dict[tuple[str, str], ArtifactManifestV1],
    ) -> tuple[dict[str, str], MaterializationCounters]:
        assert result.root is not None
        ready: list[_ReadyArtifact] = []
        counters = MaterializationCounters()
        for item in binding.items:
            manifest = prefetched.get((binding.binding_name, item.item_key))
            if manifest is None:
                manifest = await self.read_client.read_manifest(
                    claim=result.claim,
                    binding_name=binding.binding_name,
                    item=item,
                    artifact_type=binding.artifact_type,
                    cancellation=result.cancellation,
                )
                counters = counters.add(manifest_open_count=1)
            artifact, delta = await self._ready_artifact(
                claim=result.claim,
                binding=binding,
                item=item,
                manifest=manifest,
                cancellation=result.cancellation,
                warm_only=(
                    result.claim.acceptance_preflight is not None
                    and result.claim.acceptance_preflight.cache_expectation == "warm_reuse_only"
                ),
            )
            ready.append(artifact)
            counters = counters.add(
                file_open_count=delta.file_open_count,
                file_bytes=delta.file_bytes,
                archive_extraction_count=delta.archive_extraction_count,
                cas_rename_count=delta.cas_rename_count,
            )
            self.journal.acquire_lease(
                execution_attempt_id=result.claim.execution_attempt_id,
                binding_name=binding.binding_name,
                item_key=item.item_key,
                manifest_sha256=item.manifest_sha256,
            )
        target = result.root / binding.binding_name
        if binding.cardinality == "one":
            assert len(ready) == 1 and ready[0].entry.ready_path is not None
            source = ready[0].entry.ready_path / "payload"
            await self.mounter.bind_read_only(source=source, target=target)
            view_digest = digest_bytes(
                canonical_identity(
                    {
                        "binding_name": binding.binding_name,
                        "artifact_type": binding.artifact_type,
                        "cardinality": "one",
                        "manifest_sha256": binding.items[0].manifest_sha256,
                    }
                )
            )
        else:
            target.mkdir(mode=0o700)
            items_root = target / "items"
            items_root.mkdir(mode=0o700)
            manifest_items: list[dict[str, object]] = []
            for ordinal, (item, artifact) in enumerate(zip(binding.items, ready, strict=True)):
                assert artifact.entry.ready_path is not None
                item_target = items_root / f"{ordinal:012d}"
                await self.mounter.bind_read_only(
                    source=artifact.entry.ready_path / "payload", target=item_target
                )
                manifest_items.append(
                    {
                        "ordinal": ordinal,
                        "item_key": item.item_key,
                        "artifact_id": str(item.artifact_id),
                        "artifact_type": binding.artifact_type,
                        "manifest_sha256": item.manifest_sha256,
                    }
                )
            binding_manifest = canonical_document(
                {
                    "schema_version": "loom.binding-view.v1",
                    "binding_name": binding.binding_name,
                    "artifact_type": binding.artifact_type,
                    "cardinality": "many",
                    "items": manifest_items,
                }
            )
            _write_immutable(target / "binding_manifest.json", binding_manifest)
            view_digest = digest_bytes(binding_manifest)
            os.chmod(items_root, 0o555, follow_symlinks=False)
            os.chmod(target, 0o555, follow_symlinks=False)
        return {"binding_name": binding.binding_name, "view_digest": view_digest}, counters

    async def _ready_artifact(
        self,
        *,
        claim: ExecutionAttemptClaimV1,
        binding: BindingSetV1,
        item: BindingItemV1,
        manifest: ArtifactManifestV1,
        cancellation: CancellationSignal,
        warm_only: bool,
    ) -> tuple[_ReadyArtifact, MaterializationCounters]:
        lock = self._manifest_locks.setdefault(item.manifest_sha256, asyncio.Lock())
        async with lock:
            return await self._ready_artifact_locked(
                claim=claim,
                binding=binding,
                item=item,
                manifest=manifest,
                cancellation=cancellation,
                warm_only=warm_only,
            )

    async def _ready_artifact_locked(
        self,
        *,
        claim: ExecutionAttemptClaimV1,
        binding: BindingSetV1,
        item: BindingItemV1,
        manifest: ArtifactManifestV1,
        cancellation: CancellationSignal,
        warm_only: bool,
    ) -> tuple[_ReadyArtifact, MaterializationCounters]:
        if (
            item.file_count > MAX_EXTRACTED_FILES
            or item.unpacked_size_bytes > MAX_UNPACKED_BYTES
            or (
                item.stored_size_bytes == 0
                and item.unpacked_size_bytes != 0
            )
            or item.unpacked_size_bytes
            > item.stored_size_bytes * MAX_EXPANSION_RATIO
        ):
            raise ArtifactInputError("input_integrity_mismatch")
        try:
            hit = self.journal.reserve(
                manifest_sha256=item.manifest_sha256,
                unpacked_size_bytes=item.unpacked_size_bytes,
                file_count=item.file_count,
                owner_attempt_id=claim.execution_attempt_id,
            )
        except ArtifactInputJournalError as exc:
            if warm_only and exc.reason != "input_descriptor_drift":
                raise ArtifactInputError("warm_cache_not_ready") from exc
            raise ArtifactInputError(exc.reason) from exc
        if hit is not None:
            self._verify_ready(hit, manifest)
            return _ReadyArtifact(hit, manifest), MaterializationCounters()
        if warm_only:
            self.journal.abandon_materialization(
                manifest_sha256=item.manifest_sha256,
                owner_attempt_id=claim.execution_attempt_id,
            )
            raise ArtifactInputError("warm_cache_not_ready")
        partial = self.journal.cas_root / ".partial" / str(uuid4())
        downloads = partial / ".downloads"
        payload = partial / "payload"
        ready_path: Path | None = None
        counters = MaterializationCounters()
        try:
            downloads.mkdir(parents=True, mode=0o700)
            payload.mkdir(mode=0o700)
            downloaded: list[tuple[StoredFileV1, Path]] = []
            for stored in manifest.stored_files:
                if await cancellation.requested():
                    raise ArtifactInputError("input_materialization_cancelled")
                path = downloads / f"{stored.file_index:012d}"
                size, opens = await self.read_client.read_file(
                    claim=claim,
                    binding_name=binding.binding_name,
                    item=item,
                    stored=stored,
                    destination=path,
                    cancellation=cancellation,
                )
                downloaded.append((stored, path))
                counters = counters.add(file_open_count=opens, file_bytes=size)
            archive_files = [stored for stored, _ in downloaded if stored.archive_format != "none"]
            ordinary_files = [stored for stored, _ in downloaded if stored.archive_format == "none"]
            if len(archive_files) > 1:
                raise ArtifactInputError("input_descriptor_drift")
            expected_archive_files = manifest.file_count - len(ordinary_files)
            expected_archive_bytes = manifest.unpacked_size_bytes - sum(
                stored.size_bytes for stored in ordinary_files
            )
            for stored, path in downloaded:
                if stored.archive_format == "none":
                    if stored.relative_path == "READY.json" or stored.relative_path.startswith(".downloads"):
                        raise ArtifactInputError("input_descriptor_drift")
                    destination = payload / stored.relative_path
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    os.replace(path, destination)
                    os.chmod(destination, 0o444, follow_symlinks=False)
                    if stored.role == "semantic_document":
                        raw = destination.read_bytes()
                        if len(raw) > MAX_SIDECAR_BYTES:
                            raise ArtifactInputError("input_integrity_mismatch")
                        try:
                            decoded = json.loads(raw)
                        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                            raise ArtifactInputError("input_integrity_mismatch") from exc
                        if canonical_document(decoded) != raw:
                            raise ArtifactInputError("input_integrity_mismatch")
                else:
                    if stored.role != "payload_archive":
                        raise ArtifactInputError("input_descriptor_drift")
                    archive_destination = partial / ".extracted"
                    try:
                        extract_archive(
                            archive_path=path,
                            archive_format=stored.archive_format,
                            destination=archive_destination,
                            stored_size_bytes=stored.size_bytes,
                            expected_file_count=expected_archive_files,
                            expected_unpacked_size_bytes=expected_archive_bytes,
                            require_payload_root=(stored.relative_path == "payload.tar.zst"),
                        )
                    except SafeExtractionError as exc:
                        raise ArtifactInputError("input_integrity_mismatch") from exc
                    for child in archive_destination.iterdir():
                        if (payload / child.name).exists():
                            raise ArtifactInputError("input_integrity_mismatch")
                        os.replace(child, payload / child.name)
                    archive_destination.rmdir()
                    counters = counters.add(archive_extraction_count=1)
            shutil.rmtree(downloads)
            inventory = _inventory(payload)
            if (
                len(inventory) != manifest.file_count
                or sum(item[1] for item in inventory) != manifest.unpacked_size_bytes
            ):
                raise ArtifactInputError("input_integrity_mismatch")
            ready_document = canonical_document(
                {
                    "schema_version": "loom.artifact-input-ready.v1",
                    "manifest_sha256": item.manifest_sha256,
                    "artifact_id": str(item.artifact_id),
                    "artifact_type": binding.artifact_type,
                    "content_sha256": item.content_sha256,
                    "file_count": item.file_count,
                    "stored_size_bytes": item.stored_size_bytes,
                    "unpacked_size_bytes": item.unpacked_size_bytes,
                    "inventory": [
                        {"relative_path": path, "size_bytes": size, "sha256": digest}
                        for path, size, digest in inventory
                    ],
                }
            )
            _make_tree_immutable(payload)
            _write_immutable(partial / "READY.json", ready_document)
            ready_path = self.journal.ready_path(item.manifest_sha256)
            ready_path.parent.mkdir(parents=True, exist_ok=True)
            os.replace(partial, ready_path)
            os.chmod(ready_path, 0o555, follow_symlinks=False)
            _fsync_dir(ready_path.parent)
            entry = self.journal.mark_ready(
                manifest_sha256=item.manifest_sha256,
                ready_path=ready_path,
                ready_sha256=digest_bytes(ready_document),
            )
            counters = counters.add(cas_rename_count=1)
            return _ReadyArtifact(entry, manifest), counters
        except Exception as exc:
            if ready_path is not None and ready_path.exists():
                persisted = self.journal.get_entry(item.manifest_sha256)
                if persisted is None or persisted.state != "ready":
                    _make_tree_removable(ready_path)
                    shutil.rmtree(ready_path)
                    _fsync_dir(ready_path.parent)
            if partial.exists():
                if isinstance(exc, ArtifactInputError) and exc.reason in {
                    "input_integrity_mismatch",
                    "input_descriptor_drift",
                }:
                    quarantine = self.journal.cas_root / ".quarantine" / str(uuid4())
                    os.replace(partial, quarantine)
                    _fsync_dir(quarantine.parent)
                    self.journal.quarantine(manifest_sha256=item.manifest_sha256)
                else:
                    shutil.rmtree(partial, ignore_errors=True)
                    self.journal.abandon_materialization(
                        manifest_sha256=item.manifest_sha256,
                        owner_attempt_id=claim.execution_attempt_id,
                    )
            else:
                self.journal.abandon_materialization(
                    manifest_sha256=item.manifest_sha256,
                    owner_attempt_id=claim.execution_attempt_id,
                )
            raise

    def _verify_ready(self, entry: CacheEntry, manifest: ArtifactManifestV1) -> None:
        if entry.ready_path is None or entry.ready_sha256 is None:
            raise ArtifactInputError("input_cache_journal_drift")
        root = entry.ready_path
        ready_path = root / "READY.json"
        payload = root / "payload"
        if root.is_symlink() or not root.is_dir() or ready_path.is_symlink() or not payload.is_dir():
            raise ArtifactInputError("input_cache_integrity")
        raw = ready_path.read_bytes()
        if digest_bytes(raw) != entry.ready_sha256:
            raise ArtifactInputError("input_cache_integrity")
        try:
            document = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ArtifactInputError("input_cache_integrity") from exc
        if canonical_document(document) != raw or document.get("manifest_sha256") != entry.manifest_sha256:
            raise ArtifactInputError("input_cache_integrity")
        inventory = _inventory(payload)
        expected = [
            (item["relative_path"], item["size_bytes"], item["sha256"])
            for item in document.get("inventory", [])
        ]
        if inventory != expected or len(inventory) != manifest.file_count:
            raise ArtifactInputError("input_cache_integrity")
        for path, _size, _digest in inventory:
            mode = (payload / path).stat(follow_symlinks=False).st_mode
            if not stat.S_ISREG(mode) or stat.S_IMODE(mode) != 0o444:
                raise ArtifactInputError("input_cache_integrity")

    @staticmethod
    def _stage_request(root: Path, grant: StageRequestGrantV1 | None) -> Path | None:
        if grant is None:
            return None
        raw = grant.canonical_jcs_lf.encode("utf-8")
        path = root / "stage-request.json"
        _write_immutable(path, raw)
        return path

    async def _release(self, result: MaterializedInputSet) -> None:
        if result.root is not None and result.root.exists():
            for target in sorted(result.root.glob("*/items/*"), reverse=True):
                await self.mounter.unmount(target=target)
            for binding in self._claim_bindings(result.claim):
                await self.mounter.unmount(target=result.root / binding.binding_name)
            _make_tree_removable(result.root)
            shutil.rmtree(result.root, ignore_errors=True)
        self.journal.release_attempt(result.claim.execution_attempt_id)


def _write_immutable(path: Path, content: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags, 0o400)
    try:
        with os.fdopen(fd, "wb", closefd=False) as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.fchmod(fd, 0o444)
    finally:
        os.close(fd)
    _fsync_dir(path.parent)


def _inventory(root: Path) -> list[tuple[str, int, str]]:
    result: list[tuple[str, int, str]] = []
    for path in sorted(root.rglob("*"), key=lambda item: str(item.relative_to(root)).encode()):
        relative = path.relative_to(root).as_posix()
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
            raise ArtifactInputError("input_cache_integrity")
        if stat.S_ISREG(info.st_mode):
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                while chunk := stream.read(64 * 1024 * 1024):
                    digest.update(chunk)
            result.append((relative, info.st_size, f"sha256:{digest.hexdigest()}"))
    return result


def _make_tree_immutable(root: Path) -> None:
    for path in sorted(root.rglob("*"), reverse=True):
        if path.is_symlink():
            raise ArtifactInputError("input_cache_integrity")
        os.chmod(path, 0o555 if path.is_dir() else 0o444, follow_symlinks=False)
    os.chmod(root, 0o555, follow_symlinks=False)


def _make_tree_removable(root: Path) -> None:
    for path in root.rglob("*"):
        if path.is_dir() and not path.is_symlink():
            os.chmod(path, 0o700, follow_symlinks=False)
    os.chmod(root, 0o700, follow_symlinks=False)


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


__all__ = [
    "ArtifactInputError",
    "ArtifactInputMaterializer",
    "ArtifactInputReadClient",
    "CancellationSignal",
    "DeterministicReadOnlyViewMounter",
    "LinuxReadOnlyViewMounter",
    "MaterializationCounters",
    "MaterializedInputSet",
    "MountCommandRunner",
    "ReadOnlyViewMounter",
    "SubprocessMountCommandRunner",
]
