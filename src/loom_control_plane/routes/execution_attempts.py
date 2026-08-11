"""Claim-fenced worker reports and control for Pipeline ExecutionAttempts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Protocol
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Query, Request, Response
from sqlalchemy import func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext, verify_bearer_token
from loom.db.schema import (
    ExecutionAttempt,
    ExecutionAttemptControlCommand,
    ExecutionAttemptRequest,
    ExecutionAttemptWorkerEvent,
    PipelineBudgetLedger,
    PipelineStageRun,
)
from loom.pipeline.work_protocol import (
    ExecutionCancelAckV1,
    ExecutionCompleteV1,
    ExecutionControlCommandV1,
    ExecutionControlResponseV1,
    ExecutionEventsV1,
    ExecutionFailedV1,
    ExecutionHeartbeatV1,
    ExecutionStartedV1,
    FinalOutputAbortV1,
    FinalOutputFileCompleteV1,
    FinalOutputPrepareRequestV1,
    FinalOutputSessionCommitV1,
    PipelineInputMaterializationEvidenceReportV1,
    UploadTokenRenewV1,
    WorkerLostCleanupAckV1,
)
from loom.security.redaction import redact_text
from loom_control_plane.execution_attempt_fencing import (
    AttemptFenceError,
    idempotency_values,
    replay_or_conflict,
    verify_attempt_claim,
)

router = APIRouter()

ClaimIdHeader = Annotated[UUID, Header(alias="X-Loom-Claim-Id")]
LeaseEpochHeader = Annotated[int, Header(alias="X-Loom-Lease-Epoch", ge=1)]
LeaseTokenHeader = Annotated[str, Header(alias="X-Loom-Lease-Token", min_length=32)]
RequestIdHeader = Annotated[UUID, Header(alias="X-Loom-Request-Id")]


class ArtifactReadServiceV1(Protocol):
    async def read_manifest(
        self,
        *,
        attempt_id: UUID,
        binding_name: str,
        item_key: str,
        if_match: str,
    ) -> Response: ...

    async def read_file(
        self,
        *,
        attempt_id: UUID,
        binding_name: str,
        item_key: str,
        file_index: int,
        if_match: str,
        range_header: str | None,
    ) -> Response: ...


class FinalOutputServiceV1(Protocol):
    async def prepare(self, **kwargs: Any) -> dict[str, Any]: ...
    async def renew(self, **kwargs: Any) -> dict[str, Any]: ...
    async def put_part(self, **kwargs: Any) -> dict[str, Any]: ...
    async def complete_file(self, **kwargs: Any) -> dict[str, Any]: ...
    async def commit(self, **kwargs: Any) -> dict[str, Any]: ...
    async def abort(self, **kwargs: Any) -> dict[str, Any]: ...


async def _worker_auth(
    request: Request,
    authorization: str | None,
    *,
    scope: str,
) -> AuthContext:
    async with request.app.state.session_factory() as auth_session:
        ctx = await verify_bearer_token(auth_session, authorization)
    if ctx is None or scope not in ctx.scopes:
        raise HTTPException(status_code=401, detail="not authorized")
    return ctx


def _raise_fence(exc: AttemptFenceError) -> None:
    raise HTTPException(
        status_code=404 if exc.not_found else 409,
        detail="not_found" if exc.not_found else exc.reason,
    ) from exc


async def _begin_mutation(
    session: AsyncSession,
    *,
    attempt_id: UUID,
    ctx: AuthContext,
    claim_id: UUID,
    lease_epoch: int,
    lease_token: str,
    request_id: UUID,
    route: str,
    payload: Any,
) -> tuple[ExecutionAttempt, dict[str, Any] | None]:
    try:
        attempt = await verify_attempt_claim(
            session,
            attempt_id=attempt_id,
            auth=ctx,
            claim_id=claim_id,
            lease_epoch=lease_epoch,
            lease_token=lease_token,
            require_live_lease=False,
        )
        replay = await replay_or_conflict(
            session,
            attempt_id=attempt_id,
            route=route,
            request_id=request_id,
            payload=payload,
        )
        if replay is None and (
            attempt.state not in {"claimed", "running"}
            or attempt.lease_expires_at is None
            or attempt.lease_expires_at <= datetime.now(UTC)
        ):
            raise AttemptFenceError("claim_fenced")
    except AttemptFenceError as exc:
        _raise_fence(exc)
    return attempt, replay


async def _journal_response(
    session: AsyncSession,
    *,
    attempt_id: UUID,
    route: str,
    request_id: UUID,
    payload: Any,
    response: dict[str, Any],
) -> None:
    await session.execute(
        insert(ExecutionAttemptRequest).values(
            **idempotency_values(
                attempt_id=attempt_id,
                route=route,
                request_id=request_id,
                payload=payload,
                response=response,
            )
        )
    )


@router.post("/execution-attempts/{attempt_id}/heartbeats")
async def heartbeat_attempt(
    attempt_id: UUID,
    payload: ExecutionHeartbeatV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    ctx = await _worker_auth(request, authorization, scope="worker:report")
    async with request.app.state.session_factory() as session:
        attempt, replay = await _begin_mutation(
            session,
            attempt_id=attempt_id,
            ctx=ctx,
            claim_id=claim_id,
            lease_epoch=lease_epoch,
            lease_token=lease_token,
            request_id=request_id,
            route="heartbeats",
            payload=payload,
        )
        if replay is not None:
            return replay
        expires_at = datetime.now(UTC) + timedelta(seconds=60)
        attempt.heartbeat_phase = payload.phase
        attempt.heartbeat_runtime_seconds = payload.monotonic_runtime_seconds
        attempt.last_heartbeat_at = datetime.now(UTC)
        attempt.lease_expires_at = expires_at
        response = {"lease_expires_at": expires_at.isoformat(), "state": attempt.state}
        await _journal_response(
            session,
            attempt_id=attempt_id,
            route="heartbeats",
            request_id=request_id,
            payload=payload,
            response=response,
        )
        await session.commit()
        return response


@router.get("/execution-attempts/{attempt_id}/control")
async def get_attempt_control(
    attempt_id: UUID,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    after_seq: int = Query(default=0, ge=0),
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    ctx = await _worker_auth(request, authorization, scope="worker:report")
    async with request.app.state.session_factory() as session:
        try:
            await verify_attempt_claim(
                session,
                attempt_id=attempt_id,
                auth=ctx,
                claim_id=claim_id,
                lease_epoch=lease_epoch,
                lease_token=lease_token,
                require_live_lease=True,
                lock=False,
            )
        except AttemptFenceError as exc:
            _raise_fence(exc)
        rows = (
            (
                await session.execute(
                    select(ExecutionAttemptControlCommand)
                    .where(
                        ExecutionAttemptControlCommand.execution_attempt_id == attempt_id,
                        ExecutionAttemptControlCommand.seq > after_seq,
                    )
                    .order_by(ExecutionAttemptControlCommand.seq)
                )
            )
            .scalars()
            .all()
        )
        current_seq = (
            await session.execute(
                select(func.coalesce(func.max(ExecutionAttemptControlCommand.seq), 0)).where(
                    ExecutionAttemptControlCommand.execution_attempt_id == attempt_id
                )
            )
        ).scalar_one()
    response = ExecutionControlResponseV1(
        commands=[ExecutionControlCommandV1(seq=row.seq, command=row.command) for row in rows],
        current_seq=current_seq,
    )
    return response.model_dump(mode="json")


@router.post("/execution-attempts/{attempt_id}/events")
async def append_attempt_events(
    attempt_id: UUID,
    payload: ExecutionEventsV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    ctx = await _worker_auth(request, authorization, scope="worker:report")
    async with request.app.state.session_factory() as session:
        _, replay = await _begin_mutation(
            session,
            attempt_id=attempt_id,
            ctx=ctx,
            claim_id=claim_id,
            lease_epoch=lease_epoch,
            lease_token=lease_token,
            request_id=request_id,
            route="events",
            payload=payload,
        )
        if replay is not None:
            return replay
        accepted_through = -1
        current_bytes = (
            await session.execute(
                select(func.coalesce(func.sum(ExecutionAttemptWorkerEvent.message_bytes), 0)).where(
                    ExecutionAttemptWorkerEvent.execution_attempt_id == attempt_id
                )
            )
        ).scalar_one()
        for event in payload.events:
            message = redact_text(event.message)
            message_bytes = len(message.encode("utf-8"))
            if int(current_bytes) + message_bytes > 16 * 1024 * 1024:
                await session.execute(
                    pg_insert(ExecutionAttemptWorkerEvent)
                    .values(
                        execution_attempt_id=attempt_id,
                        local_seq=9_223_372_036_854_775_807,
                        occurred_at=datetime.now(UTC),
                        stream="worker",
                        level="warning",
                        message="log_truncated",
                        message_bytes=len(b"log_truncated"),
                    )
                    .on_conflict_do_nothing(index_elements=["execution_attempt_id", "local_seq"])
                )
                break
            inserted = (
                await session.execute(
                    pg_insert(ExecutionAttemptWorkerEvent)
                    .values(
                        execution_attempt_id=attempt_id,
                        local_seq=event.local_seq,
                        occurred_at=event.timestamp,
                        stream=event.stream,
                        level=event.level,
                        message=message,
                        message_bytes=message_bytes,
                    )
                    .on_conflict_do_nothing(index_elements=["execution_attempt_id", "local_seq"])
                    .returning(ExecutionAttemptWorkerEvent.id)
                )
            ).scalar_one_or_none()
            if inserted is not None:
                current_bytes = int(current_bytes) + message_bytes
            accepted_through = event.local_seq
        response = {"accepted_through_local_seq": accepted_through}
        await _journal_response(
            session,
            attempt_id=attempt_id,
            route="events",
            request_id=request_id,
            payload=payload,
            response=response,
        )
        await session.commit()
        return response


@router.post("/execution-attempts/{attempt_id}/started")
async def report_attempt_started(
    attempt_id: UUID,
    payload: ExecutionStartedV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    ctx = await _worker_auth(request, authorization, scope="worker:report")
    async with request.app.state.session_factory() as session:
        attempt, replay = await _begin_mutation(
            session,
            attempt_id=attempt_id,
            ctx=ctx,
            claim_id=claim_id,
            lease_epoch=lease_epoch,
            lease_token=lease_token,
            request_id=request_id,
            route="started",
            payload=payload,
        )
        if replay is not None:
            return replay
        if attempt.started_at is not None:
            raise HTTPException(status_code=409, detail="attempt_already_started")
        attempt.state = "running"
        attempt.started_at = datetime.now(UTC)
        attempt.container_id = payload.container_id
        attempt.runtime_started_at = payload.runtime_started_at
        attempt.input_view_digest = payload.input_view_digest
        attempt.step_jwt_id = payload.step_jwt_id
        await session.execute(
            update(PipelineStageRun)
            .where(PipelineStageRun.id == attempt.stage_run_id)
            .values(
                state="running", started_at=datetime.now(UTC), version=PipelineStageRun.version + 1
            )
        )
        response = {"execution_attempt_id": str(attempt_id), "state": "running"}
        await _journal_response(
            session,
            attempt_id=attempt_id,
            route="started",
            request_id=request_id,
            payload=payload,
            response=response,
        )
        await session.commit()
        return response


async def _terminal_report(
    *,
    attempt_id: UUID,
    payload: ExecutionFailedV1 | ExecutionCancelAckV1,
    request: Request,
    claim_id: UUID,
    lease_epoch: int,
    lease_token: str,
    request_id: UUID,
    route: str,
    state: str,
    authorization: str | None,
) -> dict[str, Any]:
    ctx = await _worker_auth(request, authorization, scope="worker:report")
    async with request.app.state.session_factory() as session:
        attempt, replay = await _begin_mutation(
            session,
            attempt_id=attempt_id,
            ctx=ctx,
            claim_id=claim_id,
            lease_epoch=lease_epoch,
            lease_token=lease_token,
            request_id=request_id,
            route=route,
            payload=payload,
        )
        if replay is not None:
            return replay
        attempt.state = state
        attempt.finished_at = datetime.now(UTC)
        if isinstance(payload, ExecutionFailedV1):
            attempt.exit_code = payload.exit_code
            attempt.retry_class = payload.retry_class.value
            attempt.reason_code = payload.reason_code
            if payload.stage_result is not None:
                attempt.result_manifest_json = payload.stage_result.model_dump(mode="json")
                attempt.result_manifest_digest = payload.stage_result_sha256
        else:
            attempt.retry_class = "cancelled"
            attempt.reason_code = "cancelled"
            attempt.cancellation_observed_at = payload.observed_at
            attempt.cancellation_outcome = payload.outcome
        response = {"execution_attempt_id": str(attempt_id), "state": state}
        await _journal_response(
            session,
            attempt_id=attempt_id,
            route=route,
            request_id=request_id,
            payload=payload,
            response=response,
        )
        await session.commit()
        return response


@router.post("/execution-attempts/{attempt_id}/failed")
async def report_attempt_failed(
    attempt_id: UUID,
    payload: ExecutionFailedV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    return await _terminal_report(
        attempt_id=attempt_id,
        payload=payload,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        request_id=request_id,
        route="failed",
        state="failed",
        authorization=authorization,
    )


@router.post("/execution-attempts/{attempt_id}/cancel-ack")
async def report_attempt_cancelled(
    attempt_id: UUID,
    payload: ExecutionCancelAckV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    return await _terminal_report(
        attempt_id=attempt_id,
        payload=payload,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        request_id=request_id,
        route="cancel-ack",
        state="cancelled",
        authorization=authorization,
    )


@router.post("/execution-attempts/{attempt_id}/complete")
async def report_attempt_complete(
    attempt_id: UUID,
    payload: ExecutionCompleteV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    ctx = await _worker_auth(request, authorization, scope="worker:report")
    async with request.app.state.session_factory() as session:
        attempt, replay = await _begin_mutation(
            session,
            attempt_id=attempt_id,
            ctx=ctx,
            claim_id=claim_id,
            lease_epoch=lease_epoch,
            lease_token=lease_token,
            request_id=request_id,
            route="complete",
            payload=payload,
        )
        if replay is not None:
            return replay
        service = getattr(request.app.state, "execution_attempt_completion_service", None)
        if service is None:
            raise HTTPException(status_code=503, detail="artifact_committer_unavailable")
        await service.complete(attempt=attempt, report=payload, session=session)
        attempt.state = "succeeded"
        attempt.exit_code = 0
        attempt.result_manifest_json = payload.stage_result.model_dump(mode="json")
        attempt.result_manifest_digest = payload.stage_result_sha256
        attempt.finished_at = datetime.now(UTC)
        response = {"execution_attempt_id": str(attempt_id), "state": "succeeded"}
        await _journal_response(
            session,
            attempt_id=attempt_id,
            route="complete",
            request_id=request_id,
            payload=payload,
            response=response,
        )
        await session.commit()
        return response


@router.post("/execution-attempts/{attempt_id}/input-materialization-evidence")
async def report_input_materialization_evidence(
    attempt_id: UUID,
    payload: PipelineInputMaterializationEvidenceReportV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    if payload.execution_attempt_id != attempt_id or payload.lease_epoch != lease_epoch:
        raise HTTPException(status_code=409, detail="claim_fenced")
    ctx = await _worker_auth(request, authorization, scope="worker:report")
    async with request.app.state.session_factory() as session:
        attempt, replay = await _begin_mutation(
            session,
            attempt_id=attempt_id,
            ctx=ctx,
            claim_id=claim_id,
            lease_epoch=lease_epoch,
            lease_token=lease_token,
            request_id=request_id,
            route="input-materialization-evidence",
            payload=payload,
        )
        if replay is not None:
            return replay
        if attempt.worker_id != payload.worker_id:
            raise HTTPException(status_code=409, detail="claim_fenced")
        service = getattr(request.app.state, "input_materialization_evidence_service", None)
        if service is None:
            raise HTTPException(status_code=503, detail="input_materializer_unavailable")
        evidence_sha256 = await service.persist(attempt=attempt, report=payload, session=session)
        response = {
            "attempt_id": str(attempt_id),
            "worker_id": str(payload.worker_id),
            "lease_epoch": lease_epoch,
            "evidence_sha256": evidence_sha256,
        }
        await _journal_response(
            session,
            attempt_id=attempt_id,
            route="input-materialization-evidence",
            request_id=request_id,
            payload=payload,
            response=response,
        )
        await session.commit()
        return response


@router.post("/execution-attempts/{attempt_id}/worker-lost-cleanup-ack")
async def report_worker_lost_cleanup(
    attempt_id: UUID,
    payload: WorkerLostCleanupAckV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    request_id: RequestIdHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    ctx = await _worker_auth(request, authorization, scope="worker:reap")
    async with request.app.state.session_factory() as session:
        try:
            attempt = await verify_attempt_claim(
                session,
                attempt_id=attempt_id,
                auth=ctx,
                claim_id=claim_id,
                lease_epoch=lease_epoch,
                lease_token=None,
                require_live_lease=False,
            )
            replay = await replay_or_conflict(
                session,
                attempt_id=attempt_id,
                route="worker-lost-cleanup-ack",
                request_id=request_id,
                payload=payload,
            )
        except AttemptFenceError as exc:
            _raise_fence(exc)
        if replay is not None:
            return replay
        if attempt.lease_expires_at is None or attempt.lease_expires_at > datetime.now(UTC):
            raise HTTPException(status_code=409, detail="claim_not_expired")
        terminal_cause = (
            await session.execute(
                select(PipelineBudgetLedger.terminal_cause)
                .join(
                    PipelineStageRun,
                    PipelineStageRun.pipeline_run_id == PipelineBudgetLedger.pipeline_run_id,
                )
                .where(PipelineStageRun.id == attempt.stage_run_id)
            )
        ).scalar_one_or_none()
        attempt.state = "cancelled" if terminal_cause is not None else "lost"
        attempt.retry_class = (
            "cancelled" if terminal_cause is not None else "infrastructure_transient"
        )
        attempt.reason_code = "worker_lost_cleanup" if terminal_cause is not None else "worker_lost"
        attempt.finished_at = datetime.now(UTC)
        response = {"execution_attempt_id": str(attempt_id), "state": attempt.state}
        await _journal_response(
            session,
            attempt_id=attempt_id,
            route="worker-lost-cleanup-ack",
            request_id=request_id,
            payload=payload,
            response=response,
        )
        await session.commit()
        return response


async def _read_input(
    *,
    attempt_id: UUID,
    binding_name: str,
    item_key: str,
    file_index: int | None,
    request: Request,
    claim_id: UUID,
    lease_epoch: int,
    lease_token: str,
    if_match: str,
    range_header: str | None,
    authorization: str | None,
) -> Response:
    ctx = await _worker_auth(request, authorization, scope="worker:read_inputs")
    async with request.app.state.session_factory() as session:
        try:
            await verify_attempt_claim(
                session,
                attempt_id=attempt_id,
                auth=ctx,
                claim_id=claim_id,
                lease_epoch=lease_epoch,
                lease_token=lease_token,
                require_live_lease=True,
                lock=False,
            )
        except AttemptFenceError as exc:
            _raise_fence(exc)
    service: ArtifactReadServiceV1 | None = getattr(
        request.app.state, "artifact_read_service", None
    )
    if service is None:
        raise HTTPException(status_code=503, detail="artifact_read_unavailable")
    if file_index is None:
        return await service.read_manifest(
            attempt_id=attempt_id,
            binding_name=binding_name,
            item_key=item_key,
            if_match=if_match,
        )
    return await service.read_file(
        attempt_id=attempt_id,
        binding_name=binding_name,
        item_key=item_key,
        file_index=file_index,
        if_match=if_match,
        range_header=range_header,
    )


@router.get(
    "/api/v1/internal/execution-attempts/{attempt_id}/input-bindings/"
    "{binding_name}/items/{item_key}/manifest"
)
async def read_input_manifest(
    attempt_id: UUID,
    binding_name: str,
    item_key: str,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    if_match: Annotated[str, Header(alias="If-Match")],
    authorization: str | None = Header(default=None),
) -> Response:
    return await _read_input(
        attempt_id=attempt_id,
        binding_name=binding_name,
        item_key=item_key,
        file_index=None,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        if_match=if_match,
        range_header=None,
        authorization=authorization,
    )


@router.get(
    "/api/v1/internal/execution-attempts/{attempt_id}/input-bindings/"
    "{binding_name}/items/{item_key}/files/{file_index}"
)
async def read_input_file(
    attempt_id: UUID,
    binding_name: str,
    item_key: str,
    file_index: int,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    if_match: Annotated[str, Header(alias="If-Match")],
    range_header: Annotated[str | None, Header(alias="Range")] = None,
    authorization: str | None = Header(default=None),
) -> Response:
    if file_index < 0:
        raise HTTPException(status_code=404, detail="not_found")
    return await _read_input(
        attempt_id=attempt_id,
        binding_name=binding_name,
        item_key=item_key,
        file_index=file_index,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        if_match=if_match,
        range_header=range_header,
        authorization=authorization,
    )


async def _output_context(
    *,
    attempt_id: UUID,
    request: Request,
    claim_id: UUID,
    lease_epoch: int,
    lease_token: str,
    authorization: str | None,
) -> tuple[ExecutionAttempt, FinalOutputServiceV1]:
    ctx = await _worker_auth(request, authorization, scope="worker:write_outputs")
    async with request.app.state.session_factory() as session:
        try:
            attempt = await verify_attempt_claim(
                session,
                attempt_id=attempt_id,
                auth=ctx,
                claim_id=claim_id,
                lease_epoch=lease_epoch,
                lease_token=lease_token,
                require_live_lease=True,
                lock=False,
            )
        except AttemptFenceError as exc:
            _raise_fence(exc)
    service: FinalOutputServiceV1 | None = getattr(request.app.state, "final_output_service", None)
    if service is None:
        raise HTTPException(status_code=503, detail="artifact_committer_unavailable")
    return attempt, service


@router.post("/api/v1/internal/execution-attempts/{attempt_id}/final-output-sessions")
async def prepare_final_output(
    attempt_id: UUID,
    payload: FinalOutputPrepareRequestV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    attempt, service = await _output_context(
        attempt_id=attempt_id,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        authorization=authorization,
    )
    return await service.prepare(attempt=attempt, request_id=request_id, request=payload)


@router.post(
    "/api/v1/internal/execution-attempts/{attempt_id}/final-output-sessions/{session_id}/renew"
)
async def renew_final_output(
    attempt_id: UUID,
    session_id: UUID,
    payload: UploadTokenRenewV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    attempt, service = await _output_context(
        attempt_id=attempt_id,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        authorization=authorization,
    )
    return await service.renew(attempt=attempt, session_id=session_id, request=payload)


@router.put(
    "/api/v1/internal/execution-attempts/{attempt_id}/final-output-sessions/"
    "{session_id}/files/{file_index}/parts/{part_number}"
)
async def put_final_output_part(
    attempt_id: UUID,
    session_id: UUID,
    file_index: int,
    part_number: int,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    upload_token: Annotated[str, Header(alias="X-Loom-Upload-Token")],
    content_sha256: Annotated[str, Header(alias="X-Loom-Content-Sha256")],
    content_length: Annotated[int, Header(alias="Content-Length", ge=0)],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    if file_index < 0 or part_number < 1:
        raise HTTPException(status_code=400, detail="invalid_part")
    attempt, service = await _output_context(
        attempt_id=attempt_id,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        authorization=authorization,
    )
    return await service.put_part(
        attempt=attempt,
        session_id=session_id,
        file_index=file_index,
        part_number=part_number,
        request_id=request_id,
        upload_token=upload_token,
        content_sha256=content_sha256,
        content_length=content_length,
        body=request.stream(),
    )


@router.post(
    "/api/v1/internal/execution-attempts/{attempt_id}/final-output-sessions/"
    "{session_id}/files/{file_index}/complete"
)
async def complete_final_output_file(
    attempt_id: UUID,
    session_id: UUID,
    file_index: int,
    payload: FinalOutputFileCompleteV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    upload_token: Annotated[str, Header(alias="X-Loom-Upload-Token")],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    attempt, service = await _output_context(
        attempt_id=attempt_id,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        authorization=authorization,
    )
    return await service.complete_file(
        attempt=attempt,
        session_id=session_id,
        file_index=file_index,
        request_id=request_id,
        upload_token=upload_token,
        request=payload,
    )


@router.post(
    "/api/v1/internal/execution-attempts/{attempt_id}/final-output-sessions/{session_id}/commit"
)
async def commit_final_output_session(
    attempt_id: UUID,
    session_id: UUID,
    payload: FinalOutputSessionCommitV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    upload_token: Annotated[str, Header(alias="X-Loom-Upload-Token")],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    attempt, service = await _output_context(
        attempt_id=attempt_id,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        authorization=authorization,
    )
    return await service.commit(
        attempt=attempt,
        session_id=session_id,
        request_id=request_id,
        upload_token=upload_token,
        request=payload,
    )


@router.post(
    "/api/v1/internal/execution-attempts/{attempt_id}/final-output-sessions/{session_id}/abort"
)
async def abort_final_output_session(
    attempt_id: UUID,
    session_id: UUID,
    payload: FinalOutputAbortV1,
    request: Request,
    claim_id: ClaimIdHeader,
    lease_epoch: LeaseEpochHeader,
    lease_token: LeaseTokenHeader,
    request_id: RequestIdHeader,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    attempt, service = await _output_context(
        attempt_id=attempt_id,
        request=request,
        claim_id=claim_id,
        lease_epoch=lease_epoch,
        lease_token=lease_token,
        authorization=authorization,
    )
    return await service.abort(
        attempt=attempt,
        session_id=session_id,
        request_id=request_id,
        request=payload,
    )
