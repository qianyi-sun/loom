"""Marker-verified production expansion of immutable Pipeline fanout artifacts."""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Mapping
from typing import Literal, Protocol
from uuid import UUID

from loom.pipeline.artifact_commit import MAX_APPLICATION_BUFFER_BYTES
from loom.pipeline.keys import canonical_document
from loom.pipeline.spec import MAX_FANOUT_MANIFEST_BYTES, FanoutManifestV1
from loom.trajectory.storage import ObjectStore
from loom_pipeline_orchestrator.repository import (
    FanoutSourceCandidate,
    RunLease,
)


class FanoutParametersValidatorV1(Protocol):
    def validate(self, parameters: dict[str, object]) -> None: ...


class FanoutRepositoryV1(Protocol):
    async def fanout_source_candidates(
        self, lease: RunLease
    ) -> tuple[FanoutSourceCandidate, ...]: ...

    async def expand_fanout(
        self,
        lease: RunLease,
        *,
        node_key: str,
        source_kind: Literal["run_input", "stage_output"],
        source_artifact_id: UUID,
        source_manifest_digest: str,
        manifest: FanoutManifestV1,
        source_stage_run_id: UUID | None = None,
        run_input_parameters_validated: bool = False,
    ) -> int: ...


class FanoutRuntimeError(ValueError):
    """A committed source cannot be expanded without authority drift."""


class FanoutExpansionRuntime:
    def __init__(
        self,
        *,
        repository: FanoutRepositoryV1,
        store: ObjectStore,
        bucket: str,
        parameter_validators: Mapping[str, FanoutParametersValidatorV1] | None = None,
    ) -> None:
        self._repository = repository
        self._store = store
        self._bucket = bucket
        self._parameter_validators = dict(parameter_validators or {})

    async def reconcile(self, lease: RunLease) -> int:
        expanded = 0
        for candidate in await self._repository.fanout_source_candidates(lease):
            manifest = await self._read_manifest(candidate)
            parameters_validated = candidate.source_kind == "stage_output"
            if candidate.source_kind == "run_input":
                digest = candidate.parameters_contract_digest
                validator = None if digest is None else self._parameter_validators.get(digest)
                if validator is None:
                    raise FanoutRuntimeError("fanout parameters contract is unavailable")
                for item in manifest.items:
                    validator.validate(item.parameters)
                parameters_validated = True
            expanded += await self._repository.expand_fanout(
                lease,
                node_key=candidate.node_key,
                source_kind=candidate.source_kind,
                source_stage_run_id=candidate.source_stage_run_id,
                source_artifact_id=candidate.source_artifact_id,
                source_manifest_digest=candidate.source_manifest_digest,
                manifest=manifest,
                run_input_parameters_validated=parameters_validated,
            )
        return expanded

    async def _read_manifest(self, candidate: FanoutSourceCandidate) -> FanoutManifestV1:
        if not await self._object_matches(
            key=candidate.root_manifest_key,
            expected_digest=candidate.root_manifest_sha256,
            max_bytes=MAX_FANOUT_MANIFEST_BYTES,
        ) or not await self._object_matches(
            key=candidate.committed_marker_key,
            expected_digest=candidate.committed_marker_sha256,
            max_bytes=65_536,
        ):
            raise FanoutRuntimeError("fanout source root marker drift")
        if not 1 <= candidate.source_file_size <= MAX_FANOUT_MANIFEST_BYTES:
            raise FanoutRuntimeError("fanout source size is outside the closed bound")
        payload = bytearray()
        digest = hashlib.sha256()
        async for chunk in self._store.stream_object(
            bucket=self._bucket,
            key=candidate.source_file_key,
            chunk_size=MAX_APPLICATION_BUFFER_BYTES,
        ):
            if not chunk or len(payload) + len(chunk) > candidate.source_file_size:
                raise FanoutRuntimeError("fanout source object size drift")
            payload.extend(chunk)
            digest.update(chunk)
        observed = f"sha256:{digest.hexdigest()}"
        if len(payload) != candidate.source_file_size or not hmac.compare_digest(
            observed, candidate.source_file_sha256
        ) or not hmac.compare_digest(observed, candidate.source_manifest_digest):
            raise FanoutRuntimeError("fanout source object digest drift")
        try:
            manifest = FanoutManifestV1.model_validate_json(bytes(payload))
        except ValueError as exc:
            raise FanoutRuntimeError("fanout source document is invalid") from exc
        if canonical_document(manifest) != bytes(payload):
            raise FanoutRuntimeError("fanout source document is noncanonical")
        return manifest

    async def _object_matches(
        self, *, key: str, expected_digest: str, max_bytes: int
    ) -> bool:
        facts = await self._store.stat_object(bucket=self._bucket, key=key)
        if not 1 <= facts.content_length <= max_bytes:
            return False
        if facts.checksum_sha256 is not None:
            return hmac.compare_digest(facts.checksum_sha256, expected_digest)
        digest = hashlib.sha256()
        observed_size = 0
        async for chunk in self._store.stream_object(
            bucket=self._bucket,
            key=key,
            chunk_size=MAX_APPLICATION_BUFFER_BYTES,
        ):
            observed_size += len(chunk)
            digest.update(chunk)
        return observed_size == facts.content_length and hmac.compare_digest(
            f"sha256:{digest.hexdigest()}", expected_digest
        )


__all__ = [
    "FanoutExpansionRuntime",
    "FanoutParametersValidatorV1",
    "FanoutRepositoryV1",
    "FanoutRuntimeError",
]
