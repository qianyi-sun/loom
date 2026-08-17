"""HTTP client for the Control Plane.

Used by the worker to register, claim, heartbeat, PATCH state, and update the
trajectory index. All operations that need crash-safe ownership semantics
are fenced by `worker_id` in the Control Plane's UPDATE WHERE clause
(state PATCH, trajectory index) — a stale worker_id returns 409 and we
surface that as `False` from the corresponding methods so callers can log
+ stop reporting against a trial we no longer own.
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol
from urllib.parse import quote
from uuid import UUID

import httpx

from loom.pipeline.live_preview import LivePreviewRecordV1, validate_preview_jpeg


class StepTokenClient(Protocol):
    async def mint_step_token(
        self,
        *,
        team_id: UUID,
        trial_id: UUID,
        step_id: str,
        ttl_sec: int,
    ) -> str: ...


class ExecutionAttemptStepTokenClient(Protocol):
    async def mint_execution_attempt_step_token(
        self,
        *,
        team_id: UUID,
        execution_attempt_id: UUID,
        step_id: str,
        ttl_sec: int,
        claim: ExecutionAttemptClaimHeaders,
    ) -> str: ...


@dataclass(frozen=True)
class ExecutionAttemptClaimHeaders:
    """The bearer-independent fencing identity for one Attempt claim.

    The raw lease token is deliberately kept out of request bodies.  Mutation
    callers supply their own stable ``request_id`` so an HTTP retry reuses the
    server-side idempotency key rather than creating a second operation.
    """

    claim_id: UUID
    lease_epoch: int
    lease_token: str

    def __post_init__(self) -> None:
        if self.lease_epoch <= 0:
            raise ValueError("lease_epoch must be positive")
        if not self.lease_token:
            raise ValueError("lease_token must be non-empty")

    def as_headers(
        self,
        *,
        request_id: UUID | None = None,
        include_lease_token: bool = True,
    ) -> dict[str, str]:
        headers = {
            "X-Loom-Claim-Id": str(self.claim_id),
            "X-Loom-Lease-Epoch": str(self.lease_epoch),
        }
        if include_lease_token:
            headers["X-Loom-Lease-Token"] = self.lease_token
        if request_id is not None:
            headers["X-Loom-Request-Id"] = str(request_id)
        return headers


# Short compatibility name for callers that do not need the subject repeated.
AttemptClaimHeaders = ExecutionAttemptClaimHeaders


@dataclass(frozen=True)
class TaskImageBuildClaim:
    id: UUID
    materialization_key: str
    task_id: str
    task_checksum: str
    cpu_arch: Literal["x86_64", "arm64"]
    task_config: dict[str, Any]
    task_source: str | None
    task_source_provenance: dict[str, Any]
    attempt_count: int
    max_attempts: int
    lease_epoch: int
    lease_expires_at: str
    registry_images: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> TaskImageBuildClaim:
        cpu_arch = str(payload["cpu_arch"])
        if cpu_arch not in {"x86_64", "arm64"}:
            raise ValueError("task image claim cpu_arch must be native")
        lease_epoch = int(payload["lease_epoch"])
        if lease_epoch <= 0:
            raise ValueError("task image claim lease_epoch must be positive")
        return cls(
            id=UUID(str(payload["id"])),
            materialization_key=str(payload["materialization_key"]),
            task_id=str(payload["task_id"]),
            task_checksum=str(payload["task_checksum"]),
            cpu_arch=cpu_arch,  # type: ignore[arg-type]
            task_config=dict(payload["task_config"]),
            task_source=(
                str(payload["task_source"]) if payload.get("task_source") is not None else None
            ),
            task_source_provenance=dict(payload["task_source_provenance"]),
            attempt_count=int(payload["attempt_count"]),
            max_attempts=int(payload["max_attempts"]),
            lease_epoch=lease_epoch,
            lease_expires_at=str(payload["lease_expires_at"]),
            registry_images={
                str(component): str(image)
                for component, image in dict(payload.get("registry_images") or {}).items()
            },
        )


@dataclass
class HttpControlPlaneClient:
    """One-per-worker, long-lived. Inject `_client` in tests for ASGITransport."""

    base_url: str
    token: str
    timeout_sec: float = 30.0
    _client: httpx.AsyncClient | None = None

    def _http(self) -> tuple[httpx.AsyncClient, bool]:
        """Return (client, owned). owned=True means caller must close it."""
        if self._client is not None:
            return self._client, False
        return (
            httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self.timeout_sec,
            ),
            True,
        )

    @property
    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    async def register(
        self,
        *,
        hostname: str,
        version: str,
        capabilities: list[dict[str, Any]],
        max_concurrent: int = 1,
        pool_name: str = "default",
        supported_work_kinds: Sequence[str] | None = None,
        capability_snapshot_digest: str | None = None,
        input_cache_capacity_bytes: int | None = None,
        input_cache_reserved_bytes: int | None = None,
        input_cache_ready_bytes: int | None = None,
        capability_snapshot: Mapping[str, Any] | None = None,
        slurm_gpu_allocation_evidence: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "hostname": hostname,
            "version": version,
            "capabilities": capabilities,
            "max_concurrent": max_concurrent,
            "pool_name": pool_name,
        }
        # Omission is intentional rolling-upgrade compatibility: old workers
        # continue to register as Trial-only with the byte-compatible body.
        if supported_work_kinds is not None:
            payload["supported_work_kinds"] = list(supported_work_kinds)
        if capability_snapshot_digest is not None:
            payload["capability_snapshot_digest"] = capability_snapshot_digest
        if capability_snapshot is not None:
            payload["capability_snapshot"] = dict(capability_snapshot)
        if slurm_gpu_allocation_evidence is not None:
            payload["slurm_gpu_allocation_evidence"] = dict(slurm_gpu_allocation_evidence)
        cache_values = (
            input_cache_capacity_bytes,
            input_cache_reserved_bytes,
            input_cache_ready_bytes,
        )
        if any(value is not None for value in cache_values):
            if any(value is None for value in cache_values):
                raise ValueError("input cache registration fields must be supplied together")
            payload.update(
                input_cache_capacity_bytes=input_cache_capacity_bytes,
                input_cache_reserved_bytes=input_cache_reserved_bytes,
                input_cache_ready_bytes=input_cache_ready_bytes,
            )
        client, owned = self._http()
        try:
            r = await client.post(
                "/workers/register",
                headers=self._headers,
                json=payload,
            )
            r.raise_for_status()
            return r.json()  # type: ignore[no-any-return]
        finally:
            if owned:
                await client.aclose()

    async def claim(
        self,
        *,
        worker_id: UUID,
        caps: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        client, owned = self._http()
        try:
            r = await client.post(
                "/trials/claim",
                headers=self._headers,
                json={"worker_id": str(worker_id), "caps": caps},
            )
            if r.status_code == 204:
                return None
            r.raise_for_status()
            return r.json()  # type: ignore[no-any-return]
        finally:
            if owned:
                await client.aclose()

    async def claim_work(
        self,
        *,
        worker_id: UUID,
        capability_snapshot_digest: str,
        free_slots: int,
        supported_work_kinds: Sequence[str] = ("trial", "execution_attempt"),
    ) -> dict[str, Any] | None:
        """Claim one item from the unified Trial/ExecutionAttempt scheduler.

        ``free_slots`` is only the worker's advisory view.  The Control Plane
        remains authoritative and may return 204 or reject a stale capability
        snapshot even when this process believes that it has capacity.
        """

        if free_slots < 1:
            raise ValueError("free_slots must be positive")
        client, owned = self._http()
        try:
            r = await client.post(
                "/work/claim",
                headers=self._headers,
                json={
                    "capability_snapshot_digest": capability_snapshot_digest,
                    "free_slots": free_slots,
                    "schema_version": "loom.work-claim-request.v1",
                    "supported_work_kinds": list(supported_work_kinds),
                    "worker_id": str(worker_id),
                },
            )
            if r.status_code == 204:
                return None
            r.raise_for_status()
            return r.json()  # type: ignore[no-any-return]
        finally:
            if owned:
                await client.aclose()

    async def heartbeat_execution_attempt(
        self,
        *,
        attempt_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return await self._post_execution_attempt_report(
            attempt_id=attempt_id,
            operation="heartbeats",
            claim=claim,
            request_id=request_id,
            payload=payload,
        )

    async def get_execution_attempt_control(
        self,
        *,
        attempt_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        after_seq: int,
    ) -> dict[str, Any]:
        client, owned = self._http()
        try:
            r = await client.get(
                f"/execution-attempts/{attempt_id}/control",
                headers={**self._headers, **claim.as_headers()},
                params={"after_seq": after_seq},
            )
            r.raise_for_status()
            return r.json()  # type: ignore[no-any-return]
        finally:
            if owned:
                await client.aclose()

    async def append_execution_attempt_events(
        self,
        *,
        attempt_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        events: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        return await self._post_execution_attempt_report(
            attempt_id=attempt_id,
            operation="events",
            claim=claim,
            request_id=request_id,
            payload={"events": [dict(event) for event in events]},
        )

    async def report_input_materialization_evidence(
        self,
        *,
        attempt_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return await self._post_execution_attempt_report(
            attempt_id=attempt_id,
            operation="input-materialization-evidence",
            claim=claim,
            request_id=request_id,
            payload=payload,
        )

    async def report_execution_attempt_started(
        self,
        *,
        attempt_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return await self._post_execution_attempt_report(
            attempt_id=attempt_id,
            operation="started",
            claim=claim,
            request_id=request_id,
            payload=payload,
        )

    async def complete_execution_attempt(
        self,
        *,
        attempt_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return await self._post_execution_attempt_report(
            attempt_id=attempt_id,
            operation="complete",
            claim=claim,
            request_id=request_id,
            payload=payload,
        )

    async def fail_execution_attempt(
        self,
        *,
        attempt_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return await self._post_execution_attempt_report(
            attempt_id=attempt_id,
            operation="failed",
            claim=claim,
            request_id=request_id,
            payload=payload,
        )

    async def acknowledge_execution_attempt_cancel(
        self,
        *,
        attempt_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return await self._post_execution_attempt_report(
            attempt_id=attempt_id,
            operation="cancel-ack",
            claim=claim,
            request_id=request_id,
            payload=payload,
        )

    async def acknowledge_worker_lost_cleanup(
        self,
        *,
        attempt_id: UUID,
        claim_id: UUID,
        lease_epoch: int,
        request_id: UUID,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Report positive cleanup proof after the Attempt lease expired.

        This is the one report that intentionally omits the now-expired raw
        lease token; the worker/node-reaper bearer plus claim id and epoch are
        the server-side fencing inputs.
        """

        if lease_epoch <= 0:
            raise ValueError("lease_epoch must be positive")
        client, owned = self._http()
        try:
            r = await client.post(
                f"/execution-attempts/{attempt_id}/worker-lost-cleanup-ack",
                headers={
                    **self._headers,
                    "X-Loom-Claim-Id": str(claim_id),
                    "X-Loom-Lease-Epoch": str(lease_epoch),
                    "X-Loom-Request-Id": str(request_id),
                },
                json=dict(payload),
            )
            r.raise_for_status()
            return self._response_json(r)
        finally:
            if owned:
                await client.aclose()

    async def read_execution_attempt_input_manifest(
        self,
        *,
        attempt_id: UUID,
        binding_name: str,
        item_key: str,
        manifest_sha256: str,
        claim: ExecutionAttemptClaimHeaders,
    ) -> httpx.Response:
        path = self._execution_attempt_input_path(
            attempt_id=attempt_id,
            binding_name=binding_name,
            item_key=item_key,
        )
        return await self._get_claim_bound_input(
            path=f"{path}/manifest",
            claim=claim,
            if_match_sha256=manifest_sha256,
        )

    async def read_execution_attempt_input_file(
        self,
        *,
        attempt_id: UUID,
        binding_name: str,
        item_key: str,
        file_index: int,
        manifest_sha256: str,
        claim: ExecutionAttemptClaimHeaders,
        range_start: int | None = None,
    ) -> httpx.Response:
        if file_index < 0:
            raise ValueError("file_index must be non-negative")
        path = self._execution_attempt_input_path(
            attempt_id=attempt_id,
            binding_name=binding_name,
            item_key=item_key,
        )
        return await self._get_claim_bound_input(
            path=f"{path}/files/{file_index}",
            claim=claim,
            if_match_sha256=manifest_sha256,
            range_start=range_start,
        )

    @asynccontextmanager
    async def stream_execution_attempt_input_file(
        self,
        *,
        attempt_id: UUID,
        binding_name: str,
        item_key: str,
        file_index: int,
        file_sha256: str,
        claim: ExecutionAttemptClaimHeaders,
        range_start: int | None = None,
    ) -> AsyncIterator[httpx.Response]:
        """Stream one claim-bound file with the fixed input timeout contract."""

        if file_index < 0 or (range_start is not None and range_start < 0):
            raise ValueError("file index and range start must be non-negative")
        path = self._execution_attempt_input_path(
            attempt_id=attempt_id,
            binding_name=binding_name,
            item_key=item_key,
        )
        headers = {
            **self._headers,
            **claim.as_headers(),
            "If-Match": self._quoted_etag(file_sha256),
        }
        if range_start is not None:
            headers["Range"] = f"bytes={range_start}-"
        client, owned = self._http()
        timeout = httpx.Timeout(connect=10.0, read=60.0, write=60.0, pool=10.0)
        try:
            async with client.stream(
                "GET",
                f"{path}/files/{file_index}",
                headers=headers,
                timeout=timeout,
                follow_redirects=False,
            ) as response:
                response.raise_for_status()
                yield response
        finally:
            if owned:
                await client.aclose()

    async def get_acceptance_fault_arm(
        self,
        *,
        attempt_id: UUID,
        seam: str,
        claim: ExecutionAttemptClaimHeaders,
    ) -> dict[str, Any] | None:
        client, owned = self._http()
        try:
            r = await client.get(
                f"/api/v1/internal/pipeline-acceptance/fault-arms/by-attempt/{attempt_id}",
                headers={**self._headers, **claim.as_headers()},
                params={"seam": seam},
            )
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return self._response_json(r)
        finally:
            if owned:
                await client.aclose()

    async def fire_acceptance_fault_arm(
        self,
        *,
        arm_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=f"/api/v1/internal/pipeline-acceptance/fault-arms/{arm_id}/fire",
            claim=claim,
            payload=payload,
        )

    async def acknowledge_acceptance_fault_arm(
        self,
        *,
        arm_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=f"/api/v1/internal/pipeline-acceptance/fault-arms/{arm_id}/ack",
            claim=claim,
            payload=payload,
        )

    async def prepare_final_output(
        self,
        *,
        attempt_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=(f"/api/v1/internal/execution-attempts/{attempt_id}/final-output-sessions"),
            claim=claim,
            request_id=request_id,
            payload=payload,
        )

    async def prepare_checkpoint(
        self,
        *,
        attempt_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=f"/api/v1/internal/execution-attempts/{attempt_id}/checkpoint-sessions",
            claim=claim,
            request_id=request_id,
            payload=payload,
        )

    async def publish_live_preview_frame(
        self,
        *,
        attempt_id: UUID,
        sequence: int,
        step_idx: int,
        jpeg_sha256: str,
        jpeg: bytes,
        claim: ExecutionAttemptClaimHeaders,
    ) -> dict[str, Any]:
        """Publish one bounded frame over the existing Attempt-fenced channel."""

        record = LivePreviewRecordV1(
            schema_version="loom.behavior-stage1-live-preview.v1",
            sequence=sequence,
            step_idx=step_idx,
            jpeg_sha256=jpeg_sha256,
            jpeg_size_bytes=len(jpeg),
        )
        validate_preview_jpeg(jpeg)
        if record.jpeg_sha256 != "sha256:" + hashlib.sha256(jpeg).hexdigest():
            raise ValueError("preview digest does not match bytes")
        idempotency_key = f"live-preview:{attempt_id}:{sequence}"
        client, owned = self._http()
        try:
            response = await client.put(
                f"/api/v1/execution-attempts/{attempt_id}/live-preview/frames/{sequence}",
                headers={
                    **self._headers,
                    **claim.as_headers(),
                    "Idempotency-Key": idempotency_key,
                    "Content-Type": "image/jpeg",
                    "Content-Length": str(len(jpeg)),
                    "If-Match": self._quoted_etag(jpeg_sha256),
                    "X-Loom-Preview-Step": str(step_idx),
                },
                content=jpeg,
                follow_redirects=False,
            )
            response.raise_for_status()
            return self._response_json(response)
        finally:
            if owned:
                await client.aclose()

    async def commit_checkpoint_session(
        self,
        *,
        attempt_id: UUID,
        session_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        upload_token: str,
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=(
                f"/api/v1/internal/execution-attempts/{attempt_id}/"
                f"checkpoint-sessions/{session_id}/commit"
            ),
            claim=claim,
            request_id=request_id,
            payload={"schema_version": "loom.final-output-session-commit.v1"},
            extra_headers={"X-Loom-Upload-Token": upload_token},
        )

    async def renew_checkpoint_token(
        self,
        *,
        attempt_id: UUID,
        session_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=(
                f"/api/v1/internal/execution-attempts/{attempt_id}/"
                f"checkpoint-sessions/{session_id}/renew"
            ),
            claim=claim,
            payload={"schema_version": "loom.upload-token-renew.v1"},
        )

    async def upload_checkpoint_part(
        self,
        *,
        attempt_id: UUID,
        session_id: UUID,
        file_index: int,
        part_number: int,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        upload_token: str,
        content_sha256: str,
        content: bytes,
    ) -> dict[str, Any]:
        client, owned = self._http()
        try:
            response = await client.put(
                f"/api/v1/internal/execution-attempts/{attempt_id}/"
                f"checkpoint-sessions/{session_id}/files/{file_index}/parts/{part_number}",
                headers={
                    **self._headers,
                    **claim.as_headers(request_id=request_id),
                    "X-Loom-Upload-Token": upload_token,
                    "X-Loom-Content-Sha256": content_sha256,
                    "Content-Length": str(len(content)),
                },
                content=content,
            )
            response.raise_for_status()
            return self._response_json(response)
        finally:
            if owned:
                await client.aclose()

    async def complete_checkpoint_file(
        self,
        *,
        attempt_id: UUID,
        session_id: UUID,
        file_index: int,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        upload_token: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=(
                f"/api/v1/internal/execution-attempts/{attempt_id}/"
                f"checkpoint-sessions/{session_id}/files/{file_index}/complete"
            ),
            claim=claim,
            request_id=request_id,
            payload=payload,
            extra_headers={"X-Loom-Upload-Token": upload_token},
        )

    async def abort_checkpoint_session(
        self,
        *,
        attempt_id: UUID,
        session_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        reason: str,
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=(
                f"/api/v1/internal/execution-attempts/{attempt_id}/"
                f"checkpoint-sessions/{session_id}/abort"
            ),
            claim=claim,
            request_id=request_id,
            payload={"schema_version": "loom.final-output-abort.v1", "reason": reason},
        )

    async def renew_final_output_token(
        self,
        *,
        attempt_id: UUID,
        session_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=(
                f"/api/v1/internal/execution-attempts/{attempt_id}/"
                f"final-output-sessions/{session_id}/renew"
            ),
            claim=claim,
            payload={"schema_version": "loom.upload-token-renew.v1"},
        )

    async def upload_final_output_part(
        self,
        *,
        attempt_id: UUID,
        session_id: UUID,
        file_index: int,
        part_number: int,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        upload_token: str,
        content_sha256: str,
        content: bytes,
    ) -> dict[str, Any]:
        client, owned = self._http()
        try:
            r = await client.put(
                f"/api/v1/internal/execution-attempts/{attempt_id}/"
                f"final-output-sessions/{session_id}/files/{file_index}/parts/{part_number}",
                headers={
                    **self._headers,
                    **claim.as_headers(request_id=request_id),
                    "X-Loom-Upload-Token": upload_token,
                    "X-Loom-Content-Sha256": content_sha256,
                    "Content-Length": str(len(content)),
                },
                content=content,
            )
            r.raise_for_status()
            return self._response_json(r)
        finally:
            if owned:
                await client.aclose()

    async def complete_final_output_file(
        self,
        *,
        attempt_id: UUID,
        session_id: UUID,
        file_index: int,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        upload_token: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=(
                f"/api/v1/internal/execution-attempts/{attempt_id}/"
                f"final-output-sessions/{session_id}/files/{file_index}/complete"
            ),
            claim=claim,
            request_id=request_id,
            payload=payload,
            extra_headers={"X-Loom-Upload-Token": upload_token},
        )

    async def commit_final_output_session(
        self,
        *,
        attempt_id: UUID,
        session_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        upload_token: str,
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=(
                f"/api/v1/internal/execution-attempts/{attempt_id}/"
                f"final-output-sessions/{session_id}/commit"
            ),
            claim=claim,
            request_id=request_id,
            payload={"schema_version": "loom.final-output-session-commit.v1"},
            extra_headers={"X-Loom-Upload-Token": upload_token},
        )

    async def abort_final_output_session(
        self,
        *,
        attempt_id: UUID,
        session_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        reason: str,
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=(
                f"/api/v1/internal/execution-attempts/{attempt_id}/"
                f"final-output-sessions/{session_id}/abort"
            ),
            claim=claim,
            request_id=request_id,
            payload={"schema_version": "loom.final-output-abort.v1", "reason": reason},
        )

    async def _post_internal_claim_bound(
        self,
        *,
        path: str,
        claim: ExecutionAttemptClaimHeaders,
        payload: Mapping[str, Any],
        request_id: UUID | None = None,
        extra_headers: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        client, owned = self._http()
        try:
            r = await client.post(
                path,
                headers={
                    **self._headers,
                    **claim.as_headers(request_id=request_id),
                    **dict(extra_headers or {}),
                },
                json=dict(payload),
            )
            r.raise_for_status()
            return self._response_json(r)
        finally:
            if owned:
                await client.aclose()

    async def _post_execution_attempt_report(
        self,
        *,
        attempt_id: UUID,
        operation: str,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        client, owned = self._http()
        try:
            r = await client.post(
                f"/execution-attempts/{attempt_id}/{operation}",
                headers={
                    **self._headers,
                    **claim.as_headers(request_id=request_id),
                },
                json=dict(payload),
            )
            r.raise_for_status()
            return self._response_json(r)
        finally:
            if owned:
                await client.aclose()

    async def _get_claim_bound_input(
        self,
        *,
        path: str,
        claim: ExecutionAttemptClaimHeaders,
        if_match_sha256: str,
        range_start: int | None = None,
    ) -> httpx.Response:
        headers = {
            **self._headers,
            **claim.as_headers(),
            "If-Match": self._quoted_etag(if_match_sha256),
        }
        if range_start is not None:
            if range_start < 0:
                raise ValueError("range_start must be non-negative")
            headers["Range"] = f"bytes={range_start}-"
        client, owned = self._http()
        try:
            # AsyncClient.get buffers the body before returning.  The Response
            # therefore remains readable after an owned one-shot client closes.
            r = await client.get(
                path,
                headers=headers,
                timeout=httpx.Timeout(connect=10.0, read=60.0, write=60.0, pool=10.0),
                follow_redirects=False,
            )
            r.raise_for_status()
            return r
        finally:
            if owned:
                await client.aclose()

    @staticmethod
    def _execution_attempt_input_path(
        *,
        attempt_id: UUID,
        binding_name: str,
        item_key: str,
    ) -> str:
        binding = quote(binding_name, safe="")
        item = quote(item_key, safe="")
        return (
            f"/api/v1/internal/execution-attempts/{attempt_id}"
            f"/input-bindings/{binding}/items/{item}"
        )

    @staticmethod
    def _quoted_etag(value: str) -> str:
        if value.startswith('"') and value.endswith('"'):
            return value
        return f'"{value}"'

    @staticmethod
    def _response_json(response: httpx.Response) -> dict[str, Any]:
        if not response.content:
            return {}
        body = response.json()
        if not isinstance(body, dict):
            raise ValueError("Control Plane response must be a JSON object")
        return body

    async def heartbeat(self, worker_id: UUID, *, status: str | None = None) -> None:
        client, owned = self._http()
        try:
            kwargs: dict[str, Any] = {"headers": self._headers}
            if status is not None:
                kwargs["json"] = {"status": status}
            r = await client.post(
                f"/workers/{worker_id}/heartbeat",
                **kwargs,
            )
            r.raise_for_status()
        finally:
            if owned:
                await client.aclose()

    async def patch_state(
        self,
        *,
        trial_id: UUID,
        worker_id: UUID,
        state: str,
        failure_reason: str | None = None,
        failure_message: str | None = None,
    ) -> bool:
        """True on accepted, False if the trial is no longer ours (409)."""
        client, owned = self._http()
        try:
            payload: dict[str, Any] = {
                "worker_id": str(worker_id),
                "state": state,
            }
            if failure_reason is not None:
                payload["failure_reason"] = failure_reason
            if failure_message is not None:
                payload["failure_message"] = failure_message
            r = await client.patch(
                f"/trials/{trial_id}/state",
                headers=self._headers,
                json=payload,
            )
            if r.status_code == 409:
                return False
            r.raise_for_status()
            return True
        finally:
            if owned:
                await client.aclose()

    async def requeue_trial_retry(
        self,
        *,
        trial_id: UUID,
        worker_id: UUID,
        failure_reason: str,
        failure_message: str | None,
        retry_after_sec: float,
    ) -> bool:
        """True on accepted, False if the trial is no longer ours (409)."""
        client, owned = self._http()
        try:
            payload: dict[str, Any] = {
                "worker_id": str(worker_id),
                "failure_reason": failure_reason,
                "retry_after_sec": retry_after_sec,
            }
            if failure_message is not None:
                payload["failure_message"] = failure_message
            r = await client.post(
                f"/trials/{trial_id}/retry",
                headers=self._headers,
                json=payload,
            )
            if r.status_code == 409:
                return False
            r.raise_for_status()
            return True
        finally:
            if owned:
                await client.aclose()

    async def pre_start_heartbeat(
        self,
        *,
        trial_id: UUID,
        worker_id: UUID,
    ) -> bool:
        """True on accepted, False if the trial is no longer in pre-start setup."""
        client, owned = self._http()
        try:
            r = await client.post(
                f"/trials/{trial_id}/pre-start-heartbeat",
                headers=self._headers,
                json={"worker_id": str(worker_id)},
            )
            if r.status_code == 409:
                return False
            r.raise_for_status()
            return True
        finally:
            if owned:
                await client.aclose()

    async def get_trial_state(self, trial_id: UUID) -> str:
        """Fetch the current CP-side state for ``trial_id``. Used by the
        worker's cancellation watchdog (#360) to detect operator-driven
        cancels and cascade an ``asyncio.CancelledError`` into the running
        trial task."""
        client, owned = self._http()
        try:
            r = await client.get(
                f"/trials/{trial_id}",
                headers=self._headers,
            )
            r.raise_for_status()
            body = r.json()
            return str(body["state"])
        finally:
            if owned:
                await client.aclose()

    async def get_task_bundle(self, task_id: str) -> dict[str, Any]:
        """Fetch full TaskConfig + checksum + source by `task_id`."""
        client, owned = self._http()
        try:
            encoded_task_id = quote(task_id, safe="/")
            r = await client.get(
                f"/tasks/{encoded_task_id}/bundle",
                headers=self._headers,
            )
            r.raise_for_status()
            return r.json()  # type: ignore[no-any-return]
        finally:
            if owned:
                await client.aclose()

    async def mint_step_token(
        self,
        *,
        team_id: UUID,
        trial_id: UUID,
        step_id: str,
        ttl_sec: int,
    ) -> str:
        """Mint a step-scoped JWT the agent presents to the Gateway as its
        bearer token (Plan 11 + Plan 9 Task 4). Returns the raw
        `loom_step_<jwt>` string."""
        client, owned = self._http()
        try:
            r = await client.post(
                "/admin/step-tokens",
                headers=self._headers,
                json={
                    "team_id": str(team_id),
                    "trial_id": str(trial_id),
                    "step_id": step_id,
                    "ttl_sec": ttl_sec,
                },
            )
            r.raise_for_status()
            body = r.json()
            return str(body["token"])
        finally:
            if owned:
                await client.aclose()

    async def mint_execution_attempt_step_token(
        self,
        *,
        team_id: UUID,
        execution_attempt_id: UUID,
        step_id: str,
        ttl_sec: int,
        claim: ExecutionAttemptClaimHeaders,
    ) -> str:
        """Mint the rotating JWT for one fenced Pipeline Attempt.

        The worker bearer remains outside the JSON body, and the lease token is
        confined to the existing claim headers.  The Control Plane verifies
        the exact Attempt, selected gateway network profile, and TTL contract.
        """

        client, owned = self._http()
        try:
            response = await client.post(
                "/admin/step-tokens",
                headers={**self._headers, **claim.as_headers()},
                json={
                    "team_id": str(team_id),
                    "execution_attempt_id": str(execution_attempt_id),
                    "step_id": step_id,
                    "ttl_sec": ttl_sec,
                },
            )
            response.raise_for_status()
            body = response.json()
            return str(body["token"])
        finally:
            if owned:
                await client.aclose()

    async def reclaim_terminus_execution(
        self,
        *,
        trial_id: UUID,
        step_id: str,
        worker_id: UUID | None = None,
    ) -> dict[str, Any]:
        client, owned = self._http()
        try:
            body: dict[str, Any] = {"step_id": step_id}
            if worker_id is not None:
                body["worker_id"] = str(worker_id)
            r = await client.post(
                f"/trials/{trial_id}/terminus/reclaim",
                headers=self._headers,
                json=body,
            )
            r.raise_for_status()
            return r.json()  # type: ignore[no-any-return]
        finally:
            if owned:
                await client.aclose()

    async def post_episode_checkpoint(
        self,
        *,
        trial_id: UUID,
        execution_id: UUID,
        run_attempt_id: UUID,
        episode: int,
        active_role: str,
        last_call_ordinal: int,
        last_seq: int,
        tmux_session_id: str | None = None,
    ) -> dict[str, Any]:
        client, owned = self._http()
        try:
            r = await client.post(
                f"/trials/{trial_id}/terminus/episode-checkpoints",
                headers=self._headers,
                json={
                    "execution_id": str(execution_id),
                    "run_attempt_id": str(run_attempt_id),
                    "episode": episode,
                    "active_role": active_role,
                    "last_call_ordinal": last_call_ordinal,
                    "last_seq": last_seq,
                    "tmux_session_id": tmux_session_id,
                },
            )
            r.raise_for_status()
            return r.json()  # type: ignore[no-any-return]
        finally:
            if owned:
                await client.aclose()

    async def get_trial_llm_calls(self, trial_id: UUID) -> list[dict[str, Any]]:
        """Fetch every `llm_calls` row the Gateway recorded against this
        trial (Plan 11 amendment A11.1). Called by the worker at finalize
        to project LLMCallEvents into the trial's local trajectory JSONL
        before ATIF projection runs."""
        client, owned = self._http()
        try:
            r = await client.get(
                f"/trials/{trial_id}/llm-calls",
                headers=self._headers,
            )
            r.raise_for_status()
            body = r.json()
            items: list[dict[str, Any]] = body.get("items", [])
            return items
        finally:
            if owned:
                await client.aclose()

    async def patch_trajectory_index(
        self,
        *,
        trial_id: UUID,
        worker_id: UUID,
        **fields: Any,
    ) -> bool:
        return await self._patch_trajectory_index_payload(
            trial_id=trial_id,
            worker_id=worker_id,
            fields=fields,
        )

    async def _patch_trajectory_index_payload(
        self,
        *,
        trial_id: UUID,
        worker_id: UUID,
        fields: dict[str, Any],
    ) -> bool:
        client, owned = self._http()
        try:
            r = await client.patch(
                f"/trials/{trial_id}/trajectory_index",
                headers=self._headers,
                json={"worker_id": str(worker_id), **fields},
            )
            if r.status_code == 409:
                return False
            r.raise_for_status()
            return True
        finally:
            if owned:
                await client.aclose()

    async def patch_output_projection(
        self,
        *,
        trial_id: UUID,
        worker_id: UUID,
        result: dict[str, Any],
        trajectory_index: dict[str, Any],
    ) -> bool:
        return await self._patch_trajectory_index_payload(
            trial_id=trial_id,
            worker_id=worker_id,
            fields={"result": result, **trajectory_index},
        )

    async def append_events(
        self,
        *,
        trial_id: UUID,
        worker_id: UUID,
        events: list[dict[str, Any]],
    ) -> bool:
        """POST a batch of typed trajectory events to the CP
        `trial_events` table (#5 Slice 3a endpoint).

        Returns True on success, False on 409 (worker lost claim — the
        caller's writer should stop trying to flush events for this
        trial). Raises on other HTTP errors so the caller can decide
        whether to drop the batch (MinIO trajectory remains the
        authoritative copy in Slice 3b — Slice 3c flips the SSE
        reader to Postgres, at which point a persistent CP write
        failure starts mattering more).
        """
        if not events:
            return True
        client, owned = self._http()
        try:
            r = await client.post(
                f"/trials/{trial_id}/events",
                headers=self._headers,
                json={
                    "worker_id": str(worker_id),
                    "events": events,
                },
            )
            if r.status_code == 409:
                return False
            r.raise_for_status()
            return True
        finally:
            if owned:
                await client.aclose()

    # ─── #317 trial-cache build coordination ────────────────────────

    async def claim_trial_cache_slot(
        self,
        cache_key: str,
        worker_id: UUID,
        *,
        ttl_sec: float,
    ) -> bool:
        """Atomic claim. Returns True if THIS worker is now the builder."""
        client, owned = self._http()
        try:
            r = await client.post(
                "/api/v1/internal/trial-cache/claim",
                headers=self._headers,
                json={
                    "cache_key": cache_key,
                    "worker_id": str(worker_id),
                    "ttl_sec": ttl_sec,
                },
            )
            r.raise_for_status()
            return bool(r.json().get("i_am_builder"))
        finally:
            if owned:
                await client.aclose()

    async def trial_cache_slot_exists(self, cache_key: str) -> bool:
        """Cheap probe for the waiter loop. True iff a non-expired slot exists."""
        client, owned = self._http()
        try:
            r = await client.get(
                f"/api/v1/internal/trial-cache/{cache_key}",
                headers=self._headers,
            )
            r.raise_for_status()
            return bool(r.json().get("exists"))
        finally:
            if owned:
                await client.aclose()

    async def release_trial_cache_slot(
        self,
        cache_key: str,
        worker_id: UUID,
    ) -> None:
        """Release our slot. Idempotent: a stolen-by-TTL slot deletes nothing."""
        client, owned = self._http()
        try:
            r = await client.delete(
                f"/api/v1/internal/trial-cache/{cache_key}",
                headers=self._headers,
                params={"worker_id": str(worker_id)},
            )
            r.raise_for_status()
        finally:
            if owned:
                await client.aclose()

    async def refresh_trial_cache_slot(
        self,
        cache_key: str,
        worker_id: UUID,
        *,
        ttl_sec: float,
    ) -> bool:
        """Heartbeat: extend our slot TTL. False = we no longer own the slot."""
        client, owned = self._http()
        try:
            r = await client.post(
                f"/api/v1/internal/trial-cache/{cache_key}/refresh",
                headers=self._headers,
                json={
                    "worker_id": str(worker_id),
                    "ttl_sec": ttl_sec,
                },
            )
            r.raise_for_status()
            return bool(r.json().get("refreshed"))
        finally:
            if owned:
                await client.aclose()

    # ─── durable task-image builder protocol ────────────────────────

    async def claim_task_image_materialization(
        self,
        *,
        builder_id: str,
        cpu_arch: Literal["x86_64", "arm64"],
    ) -> TaskImageBuildClaim | None:
        client, owned = self._http()
        try:
            response = await client.post(
                "/api/v1/internal/task-image-materializations/claim",
                headers=self._headers,
                json={"builder_id": builder_id, "cpu_arch": cpu_arch},
            )
            if response.status_code == 204:
                return None
            response.raise_for_status()
            return TaskImageBuildClaim.from_payload(response.json())
        finally:
            if owned:
                await client.aclose()

    async def _mutate_task_image_materialization(
        self,
        *,
        materialization_id: UUID,
        operation: str,
        payload: dict[str, Any],
    ) -> bool:
        client, owned = self._http()
        try:
            response = await client.post(
                f"/api/v1/internal/task-image-materializations/{materialization_id}/{operation}",
                headers=self._headers,
                json=payload,
            )
            if response.status_code == 409:
                return False
            response.raise_for_status()
            return True
        finally:
            if owned:
                await client.aclose()

    async def start_task_image_materialization(
        self,
        *,
        materialization_id: UUID,
        builder_id: str,
        lease_epoch: int,
    ) -> bool:
        return await self._mutate_task_image_materialization(
            materialization_id=materialization_id,
            operation="start",
            payload={"builder_id": builder_id, "lease_epoch": lease_epoch},
        )

    async def heartbeat_task_image_materialization(
        self,
        *,
        materialization_id: UUID,
        builder_id: str,
        lease_epoch: int,
    ) -> bool:
        return await self._mutate_task_image_materialization(
            materialization_id=materialization_id,
            operation="heartbeat",
            payload={"builder_id": builder_id, "lease_epoch": lease_epoch},
        )

    async def record_task_image_publication(
        self,
        *,
        materialization_id: UUID,
        builder_id: str,
        lease_epoch: int,
        component: str,
        registry_image: str,
    ) -> bool:
        return await self._mutate_task_image_materialization(
            materialization_id=materialization_id,
            operation="publication",
            payload={
                "builder_id": builder_id,
                "lease_epoch": lease_epoch,
                "component": component,
                "registry_image": registry_image,
            },
        )

    async def complete_task_image_materialization(
        self,
        *,
        materialization_id: UUID,
        builder_id: str,
        lease_epoch: int,
        registry_images: Mapping[str, str],
    ) -> bool:
        return await self._mutate_task_image_materialization(
            materialization_id=materialization_id,
            operation="complete",
            payload={
                "builder_id": builder_id,
                "lease_epoch": lease_epoch,
                "registry_images": dict(registry_images),
            },
        )

    async def fail_task_image_materialization(
        self,
        *,
        materialization_id: UUID,
        builder_id: str,
        lease_epoch: int,
        retryable: bool,
        failure_reason: str,
        failure_message: str,
        registry_images: Mapping[str, str],
    ) -> bool:
        return await self._mutate_task_image_materialization(
            materialization_id=materialization_id,
            operation="fail",
            payload={
                "builder_id": builder_id,
                "lease_epoch": lease_epoch,
                "retryable": retryable,
                "failure_reason": failure_reason,
                "failure_message": failure_message,
                "registry_images": dict(registry_images),
            },
        )
