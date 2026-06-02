from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.orm import Session

from agentic_data_platform.dashboard.projections import RunDashboardProjection
from agentic_data_platform.domain.artifact_metadata import ArtifactChunkKind, ArtifactChunkMetadata, ArtifactUploadStatus
from agentic_data_platform.domain.execution_events import RunEventType, event_type_value
from agentic_data_platform.domain.run_records import (
    BenchmarkTaskInstance,
    EvaluatorConfig,
    JudgeConfig,
    ModelConfig,
    RunnerConfig,
    RunRecord,
    RunStatus,
    TerminalTurn,
)
from agentic_data_platform.persistence import session_scope
from agentic_data_platform.persistence.repositories import (
    AuditEventRepository,
    IdentityRepository,
    ProjectRecord,
    RunRepository,
)
from agentic_data_platform.providers.config import redact_sensitive_metadata, validate_secret_ref
from agentic_data_platform.service.security import (
    accessible_project_ids,
    require_authenticated_user,
    require_project_role,
    require_same_actor,
)


class BenchmarkTaskInstanceRequest(BaseModel):
    benchmark_suite: str
    benchmark_version: str
    task_family: str
    instance_id: str
    source_uri: str
    input_artifact_refs: list[str]
    required_artifacts: list[str]
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("metadata")
    @classmethod
    def require_instruction(cls, value: dict[str, Any]) -> dict[str, Any]:
        instruction = value.get("instruction")
        if not isinstance(instruction, str) or not instruction.strip():
            raise ValueError("task.metadata.instruction must be a non-empty string")
        return value


class ModelConfigRequest(BaseModel):
    provider: str
    model_name: str
    mode: str
    prompt_template_version: str
    model_version: str | None = None
    provider_config_id: str | None = None
    secret_ref: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class RunnerConfigRequest(BaseModel):
    kind: str
    sandbox_backend: str
    image: str
    entrypoint: list[str]
    internet_access: bool
    resource_limits: dict[str, int | float]
    metadata: dict[str, Any] = Field(default_factory=dict)


class JudgeConfigRequest(BaseModel):
    provider: str
    model_name: str
    rubric_version: str
    model_version: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class EvaluatorConfigRequest(BaseModel):
    evaluator_id: str
    mode: str
    judge: JudgeConfigRequest | None = None
    provider_config_id: str | None = None
    secret_ref: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class RunCreateRequest(BaseModel):
    run_id: str | None = None
    project_id: str
    owner_team: str
    task: BenchmarkTaskInstanceRequest
    model: ModelConfigRequest
    runner: RunnerConfigRequest
    evaluators: list[EvaluatorConfigRequest]
    created_by_user_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class RunActionRequest(BaseModel):
    reason: str
    actor_user_id: str | None = None

    @field_validator("reason")
    @classmethod
    def require_non_blank_reason(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("reason must be a non-empty string")
        return value


def register_run_routes(app: FastAPI, session_dependency) -> None:
    @app.post("/runs", status_code=201, tags=["runs"], responses=_example_response(_RUN_CREATED_EXAMPLE, status_code=201))
    def create_run(
        payload: RunCreateRequest,
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        auth = require_authenticated_user(request, session)
        actor_user_id = require_same_actor(auth, payload.created_by_user_id)
        project = require_project_role(session, auth, payload.project_id, minimum_role="member")

        try:
            run = _run_from_create_request(
                payload,
                created_by_user_id=actor_user_id,
                owner_team=_project_owner_team_name(session, project),
            )
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        try:
            created = RunRepository(session).create_run(
                run,
                created_by_user_id=actor_user_id,
                request_id=_request_id(request),
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

        _audit_run_event(
            session,
            event_type=RunEventType.CREATED,
            run=created,
            actor_user_id=actor_user_id,
            request_id=_request_id(request),
            payload={"status": created.status.value},
        )
        return _run_detail_payload(request, session, created)

    @app.get("/runs", tags=["runs"], responses=_example_response(_RUNS_EXAMPLE))
    def list_runs(
        request: Request,
        project_id: str | None = None,
        status: str | None = None,
        benchmark_suite: str | None = None,
        task_family: str | None = None,
        task_instance_id: str | None = None,
        created_by_user_id: str | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        auth = require_authenticated_user(request, session)
        repository = RunRepository(session)
        if project_id is not None:
            require_project_role(session, auth, project_id, minimum_role="viewer")
            allowed_project_ids = {project_id}
        else:
            allowed_project_ids = accessible_project_ids(session, auth)

        projections = repository.list_run_dashboard_summaries(
            project_ids=allowed_project_ids,
            project_id=project_id,
            status=status,
            benchmark_suite=benchmark_suite,
            task_family=task_family,
            task_instance_id=task_instance_id,
            created_by_user_id=created_by_user_id,
            created_after=created_after,
            created_before=created_before,
        )
        return _with_request_id(request, {"runs": projections})

    @app.get("/runs/{run_id}", tags=["runs"], responses=_example_response(_RUN_EXAMPLE))
    def get_run(
        run_id: str,
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        auth = require_authenticated_user(request, session)
        run = _get_run_or_404(session, run_id)
        require_project_role(session, auth, run.project_id, minimum_role="viewer")
        return _run_detail_payload(request, session, run)

    @app.get("/runs/{run_id}/events", tags=["runs"], responses=_example_response(_RUN_EVENTS_EXAMPLE))
    def list_run_events(
        run_id: str,
        request: Request,
        after_seq: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=500),
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        auth = require_authenticated_user(request, session)
        run = _get_run_or_404(session, run_id)
        require_project_role(session, auth, run.project_id, minimum_role="viewer")
        events = [
            _status_event_payload(event)
            for event in RunRepository(session).list_status_events(
                run_id,
                after_seq=after_seq,
                limit=limit,
            )
        ]
        return _with_request_id(
            request,
            {
                "run_id": run_id,
                "events": events,
                "next_after_seq": _next_after_seq(events, after_seq),
            },
        )

    @app.get("/runs/{run_id}/stream", tags=["runs"])
    def stream_run_events(
        run_id: str,
        request: Request,
        after_seq: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=500),
        once: bool = False,
        poll_interval_seconds: float = Query(1.0, gt=0, le=10),
        timeout_seconds: float = Query(30.0, ge=0, le=300),
        session: Session = Depends(session_dependency),
    ) -> StreamingResponse:
        auth = require_authenticated_user(request, session)
        run = _get_run_or_404(session, run_id)
        require_project_role(session, auth, run.project_id, minimum_role="viewer")
        engine = getattr(request.app.state, "database_engine", None)
        if engine is None:
            raise HTTPException(status_code=503, detail="Database is not configured")
        effective_after_seq = max(after_seq, _last_event_id(request))

        return StreamingResponse(
            _run_event_stream(
                engine=engine,
                run_id=run_id,
                after_seq=effective_after_seq,
                limit=limit,
                once=once,
                poll_interval_seconds=poll_interval_seconds,
                timeout_seconds=timeout_seconds,
            ),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache"},
        )

    @app.post("/runs/{run_id}/cancel", tags=["runs"], responses=_example_response(_RUN_CANCELED_EXAMPLE))
    def cancel_run(
        run_id: str,
        payload: RunActionRequest,
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        auth = require_authenticated_user(request, session)
        actor_user_id = require_same_actor(auth, payload.actor_user_id)
        existing = _get_run_or_404(session, run_id)
        require_project_role(session, auth, existing.project_id, minimum_role="member")
        try:
            run = RunRepository(session).cancel_run(
                run_id,
                reason=payload.reason,
                actor_user_id=actor_user_id,
                request_id=_request_id(request),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Unknown run: {run_id}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        _audit_run_event(
            session,
            event_type=RunEventType.CANCELED,
            run=run,
            actor_user_id=actor_user_id,
            request_id=_request_id(request),
            payload={"status": run.status.value, "reason": payload.reason},
        )
        return _run_detail_payload(request, session, run)

    @app.post("/runs/{run_id}/retry", tags=["runs"], responses=_example_response(_RUN_RETRIED_EXAMPLE))
    def retry_run(
        run_id: str,
        payload: RunActionRequest,
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        auth = require_authenticated_user(request, session)
        actor_user_id = require_same_actor(auth, payload.actor_user_id)
        existing = _get_run_or_404(session, run_id)
        require_project_role(session, auth, existing.project_id, minimum_role="member")
        try:
            run = RunRepository(session).retry_run(
                run_id,
                reason=payload.reason,
                actor_user_id=actor_user_id,
                request_id=_request_id(request),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Unknown run: {run_id}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        _audit_run_event(
            session,
            event_type=RunEventType.RETRIED,
            run=run,
            actor_user_id=actor_user_id,
            request_id=_request_id(request),
            payload={"status": run.status.value, "reason": payload.reason},
        )
        return _run_detail_payload(request, session, run)

    @app.get("/runs/{run_id}/artifacts", tags=["runs"], responses=_example_response(_ARTIFACTS_EXAMPLE))
    def list_run_artifacts(
        run_id: str,
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        auth = require_authenticated_user(request, session)
        run = _get_run_or_404(session, run_id)
        require_project_role(session, auth, run.project_id, minimum_role="viewer")
        artifacts = RunDashboardProjection.from_run(run).to_dict()["artifacts"]
        return _with_request_id(request, {"run_id": run_id, "artifacts": artifacts})

    @app.get("/runs/{run_id}/artifact-chunks", tags=["runs"], responses=_example_response(_ARTIFACT_CHUNKS_EXAMPLE))
    def list_run_artifact_chunks(
        run_id: str,
        request: Request,
        attempt_id: str | None = None,
        artifact_id: str | None = None,
        chunk_kind: ArtifactChunkKind | None = None,
        after_sequence: int | None = Query(None, ge=0),
        limit: int = Query(100, ge=1, le=500),
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        auth = require_authenticated_user(request, session)
        run = _get_run_or_404(session, run_id)
        require_project_role(session, auth, run.project_id, minimum_role="viewer")
        chunks = RunRepository(session).list_artifact_chunks(
            run_id=run_id,
            attempt_id=attempt_id,
            artifact_id=artifact_id,
            chunk_kind=chunk_kind,
            after_sequence=after_sequence,
            limit=limit,
        )
        payload: dict[str, Any] = {
            "run_id": run_id,
            "chunks": [_artifact_chunk_payload(chunk) for chunk in chunks],
            "next_after_sequence": _next_after_chunk_sequence(chunks, after_sequence),
        }
        if attempt_id is not None:
            payload["attempt_id"] = attempt_id
        if artifact_id is not None:
            payload["artifact_id"] = artifact_id
        if chunk_kind is not None:
            payload["chunk_kind"] = chunk_kind.value
        return _with_request_id(request, payload)

    @app.get("/runs/{run_id}/artifact-chunks/content", tags=["runs"])
    def download_run_artifact_chunk(
        run_id: str,
        request: Request,
        artifact_id: str,
        chunk_kind: ArtifactChunkKind,
        chunk_sequence: int = Query(..., ge=0),
        session: Session = Depends(session_dependency),
    ) -> Response:
        auth = require_authenticated_user(request, session)
        run = _get_run_or_404(session, run_id)
        require_project_role(session, auth, run.project_id, minimum_role="viewer")
        try:
            chunk = RunRepository(session).get_artifact_chunk(
                run_id=run_id,
                artifact_id=artifact_id,
                chunk_kind=chunk_kind,
                chunk_sequence=chunk_sequence,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Unknown artifact chunk") from exc
        if chunk.upload_status != ArtifactUploadStatus.COMPLETED:
            raise HTTPException(
                status_code=409,
                detail=f"Chunk upload is not completed: {chunk.upload_status.value}",
            )
        object_store = getattr(request.app.state, "artifact_store", None)
        if object_store is None:
            raise HTTPException(status_code=503, detail="Artifact object store is not configured")
        try:
            payload = object_store.get_bytes(chunk.storage_key)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail="Chunk payload is unavailable in object storage") from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail="Artifact object store read failed") from exc
        return Response(
            content=payload,
            media_type=chunk.media_type,
            headers={"Content-Disposition": f'attachment; filename="{_chunk_download_filename(chunk)}"'},
        )

    @app.get("/runs/{run_id}/evaluation", tags=["runs"], responses=_example_response(_EVALUATION_EXAMPLE))
    def get_run_evaluation(
        run_id: str,
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        auth = require_authenticated_user(request, session)
        run = _get_run_or_404(session, run_id)
        require_project_role(session, auth, run.project_id, minimum_role="viewer")
        projection = RunDashboardProjection.from_run(run).to_dict()
        evaluation = projection.get("evaluator")
        if evaluation is None:
            raise HTTPException(status_code=404, detail=f"Run has no evaluation: {run_id}")
        response_payload = {"run_id": run_id, "evaluation": evaluation}
        evaluator_results = projection.get("evaluator_results", [])
        if len(evaluator_results) > 1:
            response_payload["evaluator_results"] = evaluator_results
        return _with_request_id(request, response_payload)


def _get_run_or_404(session: Session, run_id: str):
    try:
        return RunRepository(session).get_run(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown run: {run_id}") from exc


def _run_from_create_request(
    payload: RunCreateRequest,
    *,
    created_by_user_id: str | None = None,
    owner_team: str,
) -> RunRecord:
    return RunRecord.create(
        run_id=payload.run_id or f"run_{uuid4().hex}",
        project_id=payload.project_id,
        owner_team=owner_team,
        task=BenchmarkTaskInstance(**payload.task.model_dump()),
        model=_model_config(payload.model),
        runner=RunnerConfig(**payload.runner.model_dump()),
        evaluator_configs=[_evaluator_config(config) for config in payload.evaluators],
        created_by_user_id=created_by_user_id,
        metadata=redact_sensitive_metadata(payload.metadata),
    )


def _project_owner_team_name(session: Session, project: ProjectRecord) -> str:
    if project.owner_team_id is None:
        return project.name
    try:
        return IdentityRepository(session).get_team(project.owner_team_id).name
    except KeyError:
        return project.owner_team_id


def _model_config(payload: ModelConfigRequest) -> ModelConfig:
    return ModelConfig(
        provider=payload.provider,
        model_name=payload.model_name,
        mode=payload.mode,
        prompt_template_version=payload.prompt_template_version,
        model_version=payload.model_version,
        metadata=_provider_metadata(
            payload.metadata,
            provider_config_id=payload.provider_config_id,
            secret_ref=payload.secret_ref,
        ),
    )


def _evaluator_config(payload: EvaluatorConfigRequest) -> EvaluatorConfig:
    judge = _judge_config(payload.judge) if payload.judge is not None else None
    return EvaluatorConfig(
        evaluator_id=payload.evaluator_id,
        mode=payload.mode,
        judge=judge,
        metadata=_provider_metadata(
            payload.metadata,
            provider_config_id=payload.provider_config_id,
            secret_ref=payload.secret_ref,
        ),
    )


def _judge_config(payload: JudgeConfigRequest) -> JudgeConfig:
    return JudgeConfig(
        provider=payload.provider,
        model_name=payload.model_name,
        rubric_version=payload.rubric_version,
        model_version=payload.model_version,
        metadata=redact_sensitive_metadata(payload.metadata),
    )


def _provider_metadata(
    metadata: dict[str, Any],
    *,
    provider_config_id: str | None,
    secret_ref: str | None,
) -> dict[str, Any]:
    safe_metadata = redact_sensitive_metadata(metadata)
    if provider_config_id is not None:
        safe_metadata["provider_config_id"] = provider_config_id
    if secret_ref is not None:
        validate_secret_ref(secret_ref)
        safe_metadata["secret_ref"] = secret_ref
    return safe_metadata


def _audit_run_event(
    session: Session,
    *,
    event_type: RunEventType | str,
    run: RunRecord,
    actor_user_id: str,
    request_id: str | None,
    payload: dict[str, Any],
) -> None:
    AuditEventRepository(session).record_event(
        event_type=event_type_value(event_type),
        actor_user_id=actor_user_id,
        project_id=run.project_id,
        run_id=run.run_id,
        subject_type="run",
        subject_id=run.run_id,
        payload=payload,
        request_id=request_id,
    )


def _run_detail_payload(request: Request, session: Session, run: RunRecord) -> dict[str, Any]:
    repository = RunRepository(session)
    events = [_status_event_payload(event) for event in repository.list_status_events(run.run_id)]
    return _with_request_id(
        request,
        {
            "run": RunDashboardProjection.from_run(run).to_dict(),
            "trajectory": [_terminal_turn_payload(turn) for turn in run.trajectory],
            "lifecycle_events": events,
        },
    )


def _terminal_turn_payload(turn: TerminalTurn) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "turn_index": turn.turn_index,
        "command": turn.command,
        "cwd": turn.cwd,
        "started_at": _datetime(turn.started_at),
        "completed_at": _datetime(turn.completed_at),
        "exit_code": turn.exit_code,
        "stdout": turn.stdout,
        "stderr": turn.stderr,
        "changed_paths": list(turn.changed_paths),
        "metadata": dict(turn.metadata),
    }
    if turn.model_call_id is not None:
        payload["model_call_id"] = turn.model_call_id
    return payload


def _artifact_chunk_payload(chunk: ArtifactChunkMetadata) -> dict[str, Any]:
    return chunk.to_dict()


def _chunk_download_filename(chunk: ArtifactChunkMetadata) -> str:
    artifact_id = _filename_component(chunk.artifact_id)
    chunk_kind = chunk.chunk_kind.value if isinstance(chunk.chunk_kind, ArtifactChunkKind) else str(chunk.chunk_kind)
    return f"{artifact_id}-{_filename_component(chunk_kind)}-{chunk.chunk_sequence:06d}.txt"


def _filename_component(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return sanitized.strip("._-") or "chunk"


def _status_event_payload(event) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "seq": event.seq,
        "run_id": event.run_id,
        "attempt_id": event.attempt_id,
        "event_type": event.event_type,
        "from_status": event.from_status.value if isinstance(event.from_status, RunStatus) else event.from_status,
        "to_status": event.to_status.value,
        "reason": event.reason,
        "actor_user_id": event.actor_user_id,
        "request_id": event.request_id,
        "metadata": dict(event.metadata),
        "created_at": _datetime(event.created_at),
    }


def _next_after_seq(events: list[dict[str, Any]], fallback: int) -> int:
    if not events:
        return fallback
    return int(events[-1]["seq"])


def _next_after_chunk_sequence(chunks: list[ArtifactChunkMetadata], fallback: int | None) -> int | None:
    if not chunks:
        return fallback
    return int(chunks[-1].chunk_sequence)


def _run_event_stream(
    *,
    engine,
    run_id: str,
    after_seq: int,
    limit: int,
    once: bool,
    poll_interval_seconds: float,
    timeout_seconds: float,
):
    next_after_seq = after_seq
    deadline = time.monotonic() + timeout_seconds
    while True:
        with session_scope(engine) as session:
            events = RunRepository(session).list_status_events(
                run_id,
                after_seq=next_after_seq,
                limit=limit,
            )
        for event in events:
            payload = _status_event_payload(event)
            next_after_seq = int(payload["seq"])
            yield _sse_event(payload)
        if once:
            break
        if time.monotonic() >= deadline:
            yield ": stream timeout\n\n"
            break
        time.sleep(poll_interval_seconds)


def _sse_event(payload: dict[str, Any]) -> str:
    return (
        f"id: {payload['seq']}\n"
        f"event: {payload['event_type']}\n"
        f"data: {json.dumps(payload, sort_keys=True)}\n\n"
    )


def _last_event_id(request: Request) -> int:
    value = request.headers.get("last-event-id", "")
    try:
        parsed = int(value)
    except ValueError:
        return 0
    return max(parsed, 0)


def _with_request_id(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
    request_id = getattr(request.state, "request_id", None)
    if request_id:
        payload["request_id"] = request_id
    return payload


def _example_response(example: dict[str, Any], *, status_code: int = 200) -> dict[int, dict[str, Any]]:
    return {status_code: {"content": {"application/json": {"example": example}}}}


def _request_id(request: Request) -> str | None:
    return getattr(request.state, "request_id", None)


def _datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


_RUN_PAYLOAD_EXAMPLE: dict[str, Any] = {
    "run_id": "run_001",
    "project": {"project_id": "latent-skill-pilot", "owner_team": "pilot group"},
    "created_by_user_id": "[REDACTED_OWNER]",
    "status": "succeeded",
    "failure_reason": None,
    "progress": {
        "status": "succeeded",
        "is_terminal": True,
        "turn_count": 12,
        "artifact_count": 3,
        "updated_at": "2026-05-28T12:30:00Z",
    },
    "task": {
        "benchmark_suite": "SkillFlow",
        "benchmark_version": "hf:zhang-ziao/SkillFlow-Task@ecaadb0e25d5d5cfd87bd86d81e77b4abe3a00bc",
        "task_family": "OCR-Data-Extraction",
        "instance_id": "task_family_invoice_images",
        "source_uri": "https://huggingface.co/datasets/zhang-ziao/SkillFlow-Task",
    },
    "model": {
        "provider": "openai",
        "model_name": "gpt-5",
        "model_version": "2026-05-28",
        "prompt_template_version": "terminal-agent-v0",
    },
    "runner": {
        "kind": "original_benchmark",
        "sandbox_backend": "docker_terminal",
        "image": "python:3.12-slim",
        "internet_access": True,
    },
    "evaluators": [
        {
            "evaluator_id": "llm-judge-v0",
            "mode": "llm_judge",
            "metadata": {"evaluation_mode": "llm_judge"},
            "judge": {
                "provider": "openai",
                "model_name": "gpt-5",
                "model_version": None,
                "rubric_version": "latent-skill-benchmark-2026-05-28",
            },
        }
    ],
    "artifacts": [
        {
            "artifact_id": "run_001-trajectory",
            "kind": "trajectory",
            "media_type": "application/x-ndjson",
            "size_bytes": 512,
            "storage_key": "runs/run_001/tasks/task_family_invoice_images/trajectory/trajectory.jsonl",
            "uri": "minio://runs/run_001/tasks/task_family_invoice_images/trajectory/trajectory.jsonl",
        }
    ],
    "evaluator": {
        "evaluator_id": "llm-judge-v0",
        "mode": "llm_judge",
        "status": "completed",
        "score": 0.91,
        "metrics": {"task_success": True},
        "verbal_feedback_summary": "The generated workbook is correct.",
        "metadata": {},
        "judge": {
            "provider": "openai",
            "model_name": "gpt-5",
            "model_version": None,
            "rubric_version": "latent-skill-benchmark-2026-05-28",
            "metadata": {},
        },
        "artifact_refs": ["report.json"],
        "created_at": "2026-05-28T12:30:00Z",
    },
    "created_at": "2026-05-28T12:00:00Z",
    "updated_at": "2026-05-28T12:30:00Z",
}
_RUN_PAYLOAD_EXAMPLE["evaluator_results"] = [_RUN_PAYLOAD_EXAMPLE["evaluator"]]
_LIFECYCLE_EVENT_EXAMPLE = {
    "event_id": "evt_001",
    "seq": 1,
    "run_id": "run_001",
    "attempt_id": "run_001:attempt:1",
    "event_type": "run.created",
    "from_status": None,
    "to_status": "queued",
    "reason": None,
    "actor_user_id": "[REDACTED_OWNER]",
    "request_id": "req_123",
    "metadata": {},
    "created_at": "2026-05-28T12:00:00Z",
}
_TRAJECTORY_TURN_EXAMPLE = {
    "turn_index": 0,
    "command": "python solve.py",
    "cwd": "/workspace",
    "started_at": "2026-05-28T12:00:00Z",
    "completed_at": "2026-05-28T12:00:02Z",
    "exit_code": 0,
    "stdout": "created answer.xlsx\n",
    "stderr": "",
    "changed_paths": ["answer.xlsx"],
    "model_call_id": "call_001",
    "metadata": {"sandbox_run_id": "run_001", "timed_out": False},
}
_RUNS_EXAMPLE = {"runs": [_RUN_PAYLOAD_EXAMPLE], "request_id": "req_123"}
_RUN_EXAMPLE = {
    "run": _RUN_PAYLOAD_EXAMPLE,
    "trajectory": [_TRAJECTORY_TURN_EXAMPLE],
    "lifecycle_events": [_LIFECYCLE_EVENT_EXAMPLE],
    "request_id": "req_123",
}
_RUN_EVENTS_EXAMPLE = {
    "run_id": "run_001",
    "events": [_LIFECYCLE_EVENT_EXAMPLE],
    "next_after_seq": 1,
    "request_id": "req_123",
}
_RUN_CREATED_EXAMPLE = _RUN_EXAMPLE
_RUN_CANCELED_EXAMPLE = {
    "run": {**_RUN_PAYLOAD_EXAMPLE, "status": "canceled", "failure_reason": "user requested cancellation"},
    "lifecycle_events": [
        _LIFECYCLE_EVENT_EXAMPLE,
        {**_LIFECYCLE_EVENT_EXAMPLE, "event_id": "evt_002", "event_type": "run.canceled", "from_status": "queued", "to_status": "canceled"},
    ],
    "request_id": "req_123",
}
_RUN_RETRIED_EXAMPLE = {
    "run": {**_RUN_PAYLOAD_EXAMPLE, "status": "queued", "failure_reason": None},
    "lifecycle_events": [
        _LIFECYCLE_EVENT_EXAMPLE,
        {**_LIFECYCLE_EVENT_EXAMPLE, "event_id": "evt_003", "attempt_id": "run_001:attempt:2", "event_type": "run.retried", "from_status": "canceled", "to_status": "queued"},
    ],
    "request_id": "req_123",
}
_ARTIFACTS_EXAMPLE = {
    "run_id": "run_001",
    "artifacts": _RUN_PAYLOAD_EXAMPLE["artifacts"],
    "request_id": "req_123",
}
_ARTIFACT_CHUNKS_EXAMPLE = {
    "run_id": "run_001",
    "artifact_id": "run_001-trajectory",
    "chunk_kind": "stdout",
    "chunks": [
        {
            "run_id": "run_001",
            "attempt_id": "run_001:attempt:1",
            "artifact_id": "run_001-trajectory",
            "chunk_kind": "stdout",
            "chunk_sequence": 0,
            "storage_key": "runs/run_001/tasks/task_family_invoice_images/logs/stdout/000000.jsonl",
            "media_type": "application/x-ndjson",
            "size_bytes": 65536,
            "sha256": "0" * 64,
            "upload_status": "completed",
            "created_at": "2026-05-28T12:00:00Z",
            "metadata": {"preview_start_byte": 0, "preview_end_byte": 65536},
            "schema_version": "artifact-chunk-metadata-v1",
        }
    ],
    "next_after_sequence": 0,
    "request_id": "req_123",
}
_EVALUATION_EXAMPLE = {
    "run_id": "run_001",
    "evaluation": _RUN_PAYLOAD_EXAMPLE["evaluator"],
    "request_id": "req_123",
}
