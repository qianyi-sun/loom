"""One-use online start; disabled without a release-owned admission service."""

import asyncio
import json
from uuid import UUID

import rfc8785
from fastapi import APIRouter, Header, HTTPException, Request, Response
from sqlalchemy.exc import SQLAlchemyError

from loom_control_plane.task_image_execution import TaskImageExecutionService
from loom_task_image_authority.execution_refresh import (
    MAX_EXECUTION_DELIVERY_BYTES,
    ExecutionRefreshRequest,
)
from loom_task_image_authority.execution_start import ExecutionStartRequest
from loom_task_image_authority.publication_contracts import _reject_constant, _unique_object

router = APIRouter()


@router.post("/trials/{trial_id}/task-image/refresh")
async def refresh_task_image_execution(
    trial_id: UUID, request: Request, authorization: str | None = Header(default=None),
) -> Response:
    service = getattr(request.app.state, "task_image_execution", None)
    if not isinstance(service, TaskImageExecutionService):
        raise HTTPException(status_code=503, detail="execution_refresh_unavailable")
    try:
        async with asyncio.timeout(service.timeout_seconds):
            if request.headers.get("content-type") != "application/json" or request.headers.get("content-encoding", "identity") != "identity":
                raise HTTPException(status_code=422, detail="execution_refresh_invalid_metadata")
            body = bytearray()
            async for chunk in request.stream():
                if len(body) + len(chunk) > 8192:
                    raise HTTPException(status_code=413, detail="execution_refresh_request_too_large")
                body.extend(chunk)
            try:
                parsed = ExecutionRefreshRequest.model_validate(json.loads(bytes(body), object_pairs_hook=_unique_object, parse_constant=_reject_constant))
            except (ValueError, TypeError, RecursionError):
                raise HTTPException(status_code=422, detail="execution_refresh_invalid_request") from None
            if parsed.previous.claim.trial_id != str(trial_id):
                raise HTTPException(status_code=409, detail="execution_refresh_trial_mismatch")
            delivery = await service.refresh(request=parsed, authorization=authorization)
            wire = rfc8785.dumps(delivery.model_dump(mode="json", by_alias=True, exclude_none=True))
            if len(wire) > MAX_EXECUTION_DELIVERY_BYTES:
                raise ValueError("execution refresh delivery exceeds byte ceiling")
            return Response(content=wire, media_type="application/json", headers={"Cache-Control": "no-store"})
    except PermissionError:
        raise HTTPException(status_code=401, detail="execution_refresh_unauthorized") from None
    except ValueError:
        raise HTTPException(status_code=409, detail="execution_refresh_rejected") from None
    except (TimeoutError, SQLAlchemyError, ConnectionError):
        raise HTTPException(status_code=503, detail="execution_refresh_unavailable") from None


@router.post("/trials/{trial_id}/task-image/start", status_code=201)
async def start_task_image_execution(
    trial_id: UUID, request: Request, authorization: str | None = Header(default=None),
) -> Response:
    service = getattr(request.app.state, "task_image_execution", None)
    if not isinstance(service, TaskImageExecutionService):
        raise HTTPException(status_code=503, detail="execution_start_unavailable")
    try:
        async with asyncio.timeout(service.timeout_seconds):
            if (
                request.headers.get("content-type") != "application/json"
                or request.headers.get("content-encoding", "identity") != "identity"
            ):
                raise HTTPException(status_code=422, detail="execution_start_invalid_metadata")
            body = bytearray()
            async for chunk in request.stream():
                if len(body) + len(chunk) > 8192:
                    raise HTTPException(status_code=413, detail="execution_start_request_too_large")
                body.extend(chunk)
            try:
                parsed = ExecutionStartRequest.model_validate(json.loads(
                    bytes(body), object_pairs_hook=_unique_object, parse_constant=_reject_constant,
                ))
            except (ValueError, TypeError, RecursionError):
                raise HTTPException(status_code=422, detail="execution_start_invalid_request") from None
            if parsed.claim.trial_id != str(trial_id):
                raise HTTPException(status_code=409, detail="execution_start_trial_mismatch")
            receipt = await service.consume(request=parsed, authorization=authorization)
            return Response(
                content=rfc8785.dumps(receipt.model_dump(mode="json", by_alias=True, exclude_none=True)),
                status_code=201, media_type="application/json", headers={"Cache-Control": "no-store"},
            )
    except PermissionError:
        raise HTTPException(status_code=401, detail="execution_start_unauthorized") from None
    except ValueError:
        raise HTTPException(status_code=409, detail="execution_start_rejected") from None
    except (TimeoutError, SQLAlchemyError, ConnectionError):
        raise HTTPException(status_code=503, detail="execution_start_unavailable") from None
