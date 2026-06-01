from __future__ import annotations

import re
from typing import Callable
from uuid import uuid4

from fastapi import Depends, FastAPI, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from agentic_data_platform.harbor.task_uploads import HarborTaskArchiveError, validate_harbor_task_archive
from agentic_data_platform.persistence.repositories import AuditEventRepository
from agentic_data_platform.service.security import require_authenticated_user, require_project_role


def register_harbor_task_routes(app: FastAPI, session_dependency: Callable) -> None:
    @app.post("/harbor/task-uploads", tags=["harbor"], status_code=201, response_model=None)
    async def upload_harbor_task_archive(
        request: Request,
        project_id: str = Form(...),
        source_uri: str | None = Form(default=None),
        source_version: str | None = Form(default=None),
        archive: UploadFile = File(...),
        session: Session = Depends(session_dependency),
    ):
        auth = require_authenticated_user(request, session)
        require_project_role(session, auth, project_id, minimum_role="member")

        filename = archive.filename or "harbor-task.zip"
        payload = await archive.read()
        try:
            validation = validate_harbor_task_archive(payload, filename=filename)
        except HarborTaskArchiveError as exc:
            return _error_response(
                request=request,
                status_code=422,
                code="validation_error",
                message=str(exc),
            )

        task_upload_id = f"harbor_task_{uuid4().hex}"
        stored = request.app.state.artifact_store.put_bytes(
            _task_upload_storage_key(project_id=project_id, task_upload_id=task_upload_id, filename=filename),
            payload,
            media_type=archive.content_type or "application/zip",
            metadata={
                "content_type": "harbor_task_archive",
                "project_id": project_id,
                "task_upload_id": task_upload_id,
                "task_name": validation.task_name,
                "source_uri": source_uri or "",
                "source_version": source_version or "",
            },
        )
        resolved_source_version = source_version or f"sha256:{stored.sha256}"
        upload_payload = {
            "task_upload_id": task_upload_id,
            "project_id": project_id,
            "filename": filename,
            "task_name": validation.task_name,
            "source_uri": source_uri or "",
            "source_version": resolved_source_version,
            "storage_key": stored.key,
            "media_type": stored.media_type,
            "sha256": stored.sha256,
            "size_bytes": stored.size_bytes,
            "validation": validation.to_dict(),
            "environment": validation.environment,
            "resource_requirements": validation.resource_requirements,
            "launch_metadata": {
                "harbor_run": {
                    "task_upload_id": task_upload_id,
                    "task_archive_storage_key": stored.key,
                    "environment": "docker",
                    "source_version": resolved_source_version,
                }
            },
        }
        AuditEventRepository(session).record_event(
            event_type="harbor_task_upload.created",
            actor_user_id=auth.user.user_id,
            project_id=project_id,
            subject_type="harbor_task_upload",
            subject_id=task_upload_id,
            payload={
                "task_upload_id": task_upload_id,
                "task_name": validation.task_name,
                "storage_key": stored.key,
                "sha256": stored.sha256,
                "size_bytes": stored.size_bytes,
                "source_uri": source_uri or "",
                "source_version": resolved_source_version,
                "declared_artifacts": validation.declared_artifacts,
                "environment": validation.environment,
                "resource_requirements": validation.resource_requirements,
            },
            request_id=_request_id(request),
        )
        return {
            "task_upload": upload_payload,
            "request_id": _request_id(request),
        }


def _task_upload_storage_key(*, project_id: str, task_upload_id: str, filename: str) -> str:
    return "/".join(
        [
            "harbor-task-uploads",
            _safe_component(project_id),
            _safe_component(task_upload_id),
            _safe_filename(filename),
        ]
    )


def _safe_component(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip()).strip(".-")
    return safe or "item"


def _safe_filename(value: str) -> str:
    filename = value.rsplit("/", maxsplit=1)[-1].rsplit("\\", maxsplit=1)[-1]
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", filename.strip()).strip(".-")
    return safe or "harbor-task.zip"


def _error_response(*, request: Request, status_code: int, code: str, message: str) -> JSONResponse:
    request_id = _request_id(request)
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": code,
                "message": message,
                "request_id": request_id,
            }
        },
        headers={"X-Request-ID": request_id} if request_id else None,
    )


def _request_id(request: Request) -> str:
    return getattr(request.state, "request_id", "")
