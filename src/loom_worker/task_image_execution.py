"""Signed source/full-set verification at the runner's online start boundary.

The caller supplies release-pinned trust and an independently authenticated claim.
This consumer does not issue grants or replace the server's durable one-use gate.
Its source directory must remain private and worker-owned through runtime use.
"""

from __future__ import annotations

import asyncio
import hashlib
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path

from pydantic import TypeAdapter

from loom.driver.task_image import TaskImageBuildError
from loom.models.task import TaskConfig
from loom.task_image_materialization import ImmutableRegistryImage
from loom_task_image_authority.contracts import BuildPurpose
from loom_task_image_authority.execution_delivery import TaskImageExecutionDelivery
from loom_task_image_authority.execution_grant import (
    MAX_EXECUTION_GRANT_BYTES,
    MAX_EXECUTION_GRANT_ENVELOPE_BYTES,
    ExecutionGrantEnvelope,
    LegacyExecutionClaim,
    ProtectedExecutionClaim,
    TaskImageExecutionGrantV2,
    VerifiedExecutionGrant,
    _decode,
    verify_execution_grant,
)
from loom_task_image_authority.execution_refresh import (
    REFRESH_MINIMUM_REMAINING_SECONDS,
    ExecutionRefreshRequest,
)
from loom_task_image_authority.execution_start import ExecutionStartReceipt as ExecutionStartReceipt
from loom_task_image_authority.execution_start import ExecutionStartRequest
from loom_task_image_authority.publication_contracts import CanonicalUUID
from loom_task_image_authority.publication_keyset import ExecutionGrantTrustRoot, _instant
from loom_worker.task_bundle_integrity import verified_task_image_cache_identity


def _clock() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class WorkerExecutionTrust:
    """Injected by trusted release composition, never populated from a claim."""

    root: ExecutionGrantTrustRoot
    purpose: BuildPurpose
    shadow_campaign_id: str | None
    clock: Callable[[], datetime] = _clock

    def __post_init__(self) -> None:
        if type(self.root) is not ExecutionGrantTrustRoot:
            raise ValueError("execution trust requires a pinned root")
        self.root.__post_init__()
        TypeAdapter(BuildPurpose).validate_python(self.purpose, strict=True)
        if self.shadow_campaign_id is not None:
            TypeAdapter(CanonicalUUID).validate_python(self.shadow_campaign_id, strict=True)
        if (self.purpose == "production") != (self.shadow_campaign_id is None):
            raise ValueError("execution purpose and campaign disagree")


@dataclass
class WorkerTaskImageExecution:
    wire: bytes
    plan_wire: bytes
    publication_wires: tuple[bytes, ...]
    keyset_wire: bytes
    trust_root: ExecutionGrantTrustRoot
    expected_claim: LegacyExecutionClaim | ProtectedExecutionClaim
    expected_purpose: BuildPurpose
    expected_shadow_campaign_id: str | None
    task_dir: Path
    task_config: TaskConfig
    task_checksum: str
    cpu_arch: str
    task_image: str
    consume: Callable[[ExecutionStartRequest], Awaitable[ExecutionStartReceipt]]
    clock: Callable[[], datetime] = _clock
    timeout_seconds: float = 5.0
    refresh: Callable[[ExecutionRefreshRequest], Awaitable[TaskImageExecutionDelivery]] | None = None
    _attempted: bool = field(default=False, init=False)
    _preparing: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        if (
            type(self.timeout_seconds) is not float
            or not math.isfinite(self.timeout_seconds)
            or not 0 < self.timeout_seconds <= 10
        ):
            raise ValueError("invalid execution start deadline")

    def verify_runtime(self) -> VerifiedExecutionGrant:
        """Check current signed inputs and private source, without starting work."""
        verified = verify_execution_grant(
            wire=self.wire, plan_wire=self.plan_wire, publication_wires=self.publication_wires,
            keyset_wire=self.keyset_wire, trust_root=self.trust_root,
            expected_claim=self.expected_claim, expected_purpose=self.expected_purpose,
            expected_shadow_campaign_id=self.expected_shadow_campaign_id, now=self.clock(),
        )
        grant = verified.grant
        raw_task, provenance = grant.snapshots()
        frozen_task = TaskConfig.model_validate(raw_task)
        expected_image = dict(verified.registry_images).get("task", frozen_task.environment.docker_image)
        if (
            self.task_config.model_dump(mode="json") != frozen_task.model_dump(mode="json")
            or self.task_checksum != grant.task_checksum
            or self.cpu_arch != grant.cpu_arch
            or self.task_image != expected_image
        ):
            raise ValueError("runtime differs from signed execution grant")
        # Prebuilt components are covered by the signed task snapshot rather
        # than publication envelopes. Their references must still be immutable.
        image_type = TypeAdapter(ImmutableRegistryImage)
        image_type.validate_python(self.task_image, strict=True)
        for sidecar in frozen_task.environment.sidecars:
            if sidecar.dockerfile is None:
                image_type.validate_python(sidecar.docker_image, strict=True)
        try:
            verified_task_image_cache_identity(
                self.task_dir, task_checksum=grant.task_checksum, source_provenance=provenance,
            )
        except TaskImageBuildError as exc:
            raise ValueError("execution source verification failed") from exc
        return verified

    async def prepare(self) -> None:
        """Refresh after bounded cold preparation, never after attempting a start."""
        if self._attempted:
            raise RuntimeError("execution start already attempted")
        if self._preparing:
            raise RuntimeError("execution preparation already in progress")
        self._preparing = True
        try:
            async with asyncio.timeout(self.timeout_seconds):
                await self._refresh_if_needed()
        finally:
            self._preparing = False

    async def _refresh_if_needed(self, *, current_authority: bool = False) -> None:
        # Decode the old identity only to request current authority. Expired
        # bytes are never verified at an invented earlier time or admitted.
        envelope = _decode(self.wire, ExecutionGrantEnvelope, MAX_EXECUTION_GRANT_ENVELOPE_BYTES)
        old = _decode(envelope.canonical_grant.encode(), TaskImageExecutionGrantV2, MAX_EXECUTION_GRANT_BYTES)
        if self.refresh is None or (not current_authority and (_instant(old.expires_at) - self.clock()).total_seconds() > REFRESH_MINIMUM_REMAINING_SECONDS):
            self.verify_runtime()
            return
        previous = ExecutionStartRequest.model_validate(dict(
            schema="loom.task-image-execution-start-request/v1", grant_id=old.grant_id, revision=old.revision,
            envelope_sha256=hashlib.sha256(self.wire).hexdigest(), claim=self.expected_claim,
            keyset_sha256=old.keyset_sha256, keyset_version=old.keyset_version, revocation_epoch=old.revocation_epoch,
        ))
        delivery = await self.refresh(ExecutionRefreshRequest.model_validate(dict(
            schema="loom.task-image-execution-refresh-request/v1", previous=previous,
        )))
        if type(delivery) is not TaskImageExecutionDelivery or delivery.claim != self.expected_claim:
            raise ValueError("execution refresh changed claim")
        candidate = replace(self, wire=delivery.grant_envelope.encode(), plan_wire=delivery.frozen_plan.encode(),
                            publication_wires=tuple(item.encode() for item in delivery.publications), keyset_wire=delivery.keyset.encode())
        fresh = candidate.verify_runtime().grant
        mutable = {"revision", "issued_at", "expires_at", "keyset_sha256", "keyset_version", "revocation_epoch"}
        if (
            fresh.model_dump(exclude=mutable) != old.model_dump(exclude=mutable)
            or fresh.revision < old.revision or fresh.keyset_version < old.keyset_version
            or fresh.revocation_epoch < old.revocation_epoch
            or (fresh.revision == old.revision and candidate.wire != self.wire)
            or candidate.plan_wire != self.plan_wire or candidate.publication_wires != self.publication_wires
            or (_instant(fresh.expires_at) - self.clock()).total_seconds() <= REFRESH_MINIMUM_REMAINING_SECONDS
        ):
            raise ValueError("execution refresh changed immutable inputs or lacks fresh lifetime")
        self.wire, self.keyset_wire = candidate.wire, candidate.keyset_wire

    async def authorize(self) -> bool:
        if self._attempted:
            raise RuntimeError("execution start already attempted")
        if self._preparing:
            raise RuntimeError("execution preparation already in progress")
        self._attempted = True
        began = time.monotonic()
        async with asyncio.timeout(self.timeout_seconds):
            # A keyset may have rolled over even while local evidence remains
            # valid. Refresh before consume, never in response to its failure.
            await self._refresh_if_needed(current_authority=True)
            verified = self.verify_runtime()
            grant = verified.grant
            request = ExecutionStartRequest.model_validate(dict(
                schema="loom.task-image-execution-start-request/v1",
                grant_id=grant.grant_id, revision=grant.revision,
                envelope_sha256=verified.envelope_sha256,
                claim=grant.claim.model_dump(mode="json", exclude_none=True),
                keyset_sha256=grant.keyset_sha256, keyset_version=grant.keyset_version,
                revocation_epoch=grant.revocation_epoch,
            ))
            if time.monotonic() - began >= self.timeout_seconds:
                raise TimeoutError("execution start verification exceeded deadline")
            # No automatic retry: the server may have committed before a lost
            # response. Its next claim, not this request, owns recovery.
            response = await self.consume(request)
            if type(response) is not ExecutionStartReceipt:
                raise ValueError("invalid online execution start response")
            receipt = ExecutionStartReceipt.model_validate(
                response.model_dump(mode="json", by_alias=True, exclude_none=True)
            )
            # Recheck the actual tree and original signed evidence after I/O.
            # A prior verification object is not authority after a long pull.
            current = self.verify_runtime()
            now = self.clock()
            if (
                receipt.request_sha256 != request.digest
                or current.envelope_sha256 != verified.envelope_sha256
                or not _instant(grant.issued_at) <= _instant(receipt.consumed_at)
                <= now < _instant(receipt.expires_at) <= _instant(grant.expires_at)
            ):
                raise ValueError("online execution start receipt binding or lifetime differs")
            # Synchronous bounded-source capture cannot be cancelled midway;
            # check elapsed wall time before admitting any runtime afterward.
            if time.monotonic() - began >= self.timeout_seconds:
                raise TimeoutError("execution start verification exceeded deadline")
            return True
