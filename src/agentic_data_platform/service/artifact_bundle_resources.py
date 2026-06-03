from __future__ import annotations

import io
import json
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import Response
from sqlalchemy.orm import Session

from agentic_data_platform.artifacts.store import Artifacpilot groupjectStore, LocalArtifactStore, build_s3_artifact_store
from agentic_data_platform.dashboard.projections import RunDashboardProjection
from agentic_data_platform.domain.artifact_metadata import ArtifactChunkKind, ArtifactChunkMetadata, ArtifactUploadStatus
from agentic_data_platform.domain.run_records import ArtifactRef
from agentic_data_platform.domain.run_records import RunStatus, TerminalTurn
from agentic_data_platform.persistence.repositories import RunRepository
from agentic_data_platform.service.security import require_authenticated_user, require_project_role


def register_artifact_bundle_routes(app: FastAPI, session_dependency: Callable) -> None:
    @app.get("/runs/{run_id}/artifact-bundle", tags=["runs"])
    def download_artifact_bundle(
        run_id: str,
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> Response:
        auth = require_authenticated_user(request, session)
        repository = RunRepository(session)
        try:
            run = repository.get_run(run_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Unknown run: {run_id}") from exc
        require_project_role(session, auth, run.project_id, minimum_role="viewer")
        events = [_status_event_payload(event) for event in repository.list_status_events(run.run_id)]
        artifact_chunks = repository.list_artifact_chunks(run_id=run.run_id)
        payload = build_artifact_bundle(
            run,
            lifecycle_events=events,
            artifact_chunks=artifact_chunks,
            object_store=getattr(request.app.state, "artifact_store", None),
        )
        return Response(
            content=payload,
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="{run.run_id}-artifacts.zip"'},
        )


def build_service_artifact_store(settings) -> Artifacpilot groupjectStore:
    if (
        settings.object_storage_endpoint
        and settings.object_storage_bucket
        and settings.object_storage_access_key
        and settings.object_storage_secret_key
    ):
        return build_s3_artifact_store(
            endpoint_url=settings.object_storage_endpoint,
            bucket=settings.object_storage_bucket,
            access_key=settings.object_storage_access_key,
            secret_key=settings.object_storage_secret_key,
            region=settings.object_storage_region,
        )
    return LocalArtifactStore(Path(".runtime/artifacts"))


def build_artifact_bundle(
    run,
    *,
    lifecycle_events: list[dict[str, Any]],
    artifact_chunks: list[ArtifactChunkMetadata] | None = None,
    object_store: Artifacpilot groupjectStore | None = None,
) -> bytes:
    projection = RunDashboardProjection.from_run(run).to_dict()
    artifacts = projection.get("artifacts") or []
    evaluator = projection.get("evaluator") or {}
    chunk_metadata = list(artifact_chunks or [])
    artifact_contents, artifact_content_errors = _artifact_contents(run.artifacts, object_store)
    artifact_chunk_contents, artifact_chunk_content_errors = _artifact_chunk_contents(chunk_metadata, object_store)
    manifest = {
        "schema_version": "artifact-bundle-v0",
        "run_id": run.run_id,
        "project_id": run.project_id,
        "status": run.status.value,
        "artifact_count": len(artifacts),
        "artifact_payload_count": len(artifact_contents),
        "artifact_chunk_count": len(chunk_metadata),
        "artifact_chunk_payload_count": len(artifact_chunk_contents),
        "trajectory_turn_count": len(run.trajectory),
        "generated_at": _datetime(datetime.now(timezone.utc)),
        "contents": [
            "run.json",
            "trajectory.jsonl",
            "evaluation.json",
            "artifact-metadata.json",
            "artifact-chunks.json",
            "lifecycle-events.json",
            *[item["path"] for item in artifact_contents],
            *[item["path"] for item in artifact_chunk_contents],
        ],
        "artifact_contents": [
            {
                "artifact_id": item["artifact_id"],
                "kind": item["kind"],
                "path": item["path"],
                "media_type": item["media_type"],
                "size_bytes": len(item["payload"]),
            }
            for item in artifact_contents
        ],
        "artifact_content_errors": artifact_content_errors,
        "artifact_chunk_contents": [
            {
                "artifact_id": item["artifact_id"],
                "chunk_kind": item["chunk_kind"],
                "chunk_sequence": item["chunk_sequence"],
                "path": item["path"],
                "media_type": item["media_type"],
                "size_bytes": len(item["payload"]),
            }
            for item in artifact_chunk_contents
        ],
        "artifact_chunk_content_errors": artifact_chunk_content_errors,
    }

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("manifest.json", _json_bytes(manifest))
        archive.writestr("run.json", _json_bytes(projection))
        archive.writestr("trajectory.jsonl", _trajectory_jsonl(run.trajectory))
        archive.writestr("evaluation.json", _json_bytes(evaluator))
        archive.writestr("artifact-metadata.json", _json_bytes({"artifacts": artifacts}))
        archive.writestr(
            "artifact-chunks.json",
            _json_bytes({"chunks": [chunk.to_dict() for chunk in chunk_metadata]}),
        )
        archive.writestr("lifecycle-events.json", _json_bytes({"lifecycle_events": lifecycle_events}))
        for item in artifact_contents:
            archive.writestr(item["path"], item["payload"])
        for item in artifact_chunk_contents:
            archive.writestr(item["path"], item["payload"])
    return buffer.getvalue()


def _artifact_contents(
    artifacts: list[ArtifactRef],
    object_store: Artifacpilot groupjectStore | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    contents: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for artifact in artifacts:
        upload_state_error = _artifact_upload_state_error(artifact)
        if upload_state_error is not None:
            errors.append(upload_state_error)
            continue
        if object_store is None:
            continue
        storage_key = _safe_storage_key(artifact)
        if storage_key is None:
            errors.append(_artifact_content_error(artifact, "artifact storage key is missing or unsafe"))
            continue
        try:
            payload = object_store.get_bytes(storage_key)
        except Exception:
            errors.append(_artifact_content_error(artifact, "artifact payload is not available from configured store"))
            continue
        contents.append(
            {
                "artifact_id": artifact.artifact_id,
                "kind": artifact.kind.value,
                "path": _artifact_payload_path(artifact, storage_key),
                "media_type": artifact.media_type,
                "payload": payload,
            }
        )
    return contents, errors


def _artifact_chunk_contents(
    chunks: list[ArtifactChunkMetadata],
    object_store: Artifacpilot groupjectStore | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    contents: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for chunk in chunks:
        upload_state_error = _artifact_chunk_upload_state_error(chunk)
        if upload_state_error is not None:
            errors.append(upload_state_error)
            continue
        if object_store is None:
            continue
        storage_key = _safe_chunk_storage_key(chunk)
        if storage_key is None:
            errors.append(_artifact_chunk_content_error(chunk, "artifact chunk storage key is missing or unsafe"))
            continue
        try:
            payload = object_store.get_bytes(storage_key)
        except Exception:
            errors.append(
                _artifact_chunk_content_error(
                    chunk,
                    "artifact chunk payload is not available from configured store",
                )
            )
            continue
        contents.append(
            {
                "artifact_id": chunk.artifact_id,
                "chunk_kind": _chunk_kind_value(chunk),
                "chunk_sequence": chunk.chunk_sequence,
                "path": _artifact_chunk_payload_path(chunk, storage_key),
                "media_type": chunk.media_type,
                "payload": payload,
            }
        )
    return contents, errors


def _artifact_upload_state_error(artifact: ArtifactRef) -> dict[str, Any] | None:
    raw_status = artifact.metadata.get("upload_status")
    if not isinstance(raw_status, str) or not raw_status.strip():
        return None
    upload_status = raw_status.strip()
    if upload_status == ArtifactUploadStatus.COMPLETED.value:
        return None

    error = _artifact_content_error(artifact, f"artifact upload is {upload_status}; payload is unavailable")
    error["upload_status"] = upload_status
    raw_reason = artifact.metadata.get("upload_error_reason")
    if isinstance(raw_reason, str) and raw_reason.strip():
        error["upload_error_reason"] = raw_reason.strip()
    return error


def _artifact_chunk_upload_state_error(chunk: ArtifactChunkMetadata) -> dict[str, Any] | None:
    upload_status = (
        chunk.upload_status.value
        if isinstance(chunk.upload_status, ArtifactUploadStatus)
        else str(chunk.upload_status).strip()
    )
    if upload_status == ArtifactUploadStatus.COMPLETED.value:
        return None
    error = _artifact_chunk_content_error(chunk, f"artifact chunk upload is {upload_status}; payload is unavailable")
    error["upload_status"] = upload_status
    if chunk.upload_error_reason is not None:
        error["upload_error_reason"] = chunk.upload_error_reason
    return error


def _safe_storage_key(artifact: ArtifactRef) -> str | None:
    return _safe_storage_key_value(artifact.metadata.get("storage_key"))


def _safe_chunk_storage_key(chunk: ArtifactChunkMetadata) -> str | None:
    return _safe_storage_key_value(chunk.storage_key)


def _safe_storage_key_value(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    key = value.strip()
    if key.startswith("/") or "\\" in key or "?" in key or "#" in key:
        return None
    parts = key.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        return None
    return key


def _artifact_payload_path(artifact: ArtifactRef, storage_key: str) -> str:
    filename = f"{_safe_path_component(artifact.artifact_id)}{_artifact_extension(artifact, storage_key)}"
    return f"artifacts/{artifact.kind.value}/{filename}"


def _artifact_extension(artifact: ArtifactRef, storage_key: str) -> str:
    media_type = artifact.media_type.lower()
    if media_type == "application/x-ndjson":
        return ".jsonl"
    if media_type == "application/json":
        return ".json"
    if media_type.startswith("text/"):
        return ".txt"
    if storage_key.endswith(".tar.gz"):
        return ".tar.gz"
    suffix = Path(storage_key).suffix
    if suffix and re.fullmatch(r"\.[A-Za-z0-9]{1,12}", suffix):
        return suffix
    return ".bin"


def _artifact_chunk_payload_path(chunk: ArtifactChunkMetadata, storage_key: str) -> str:
    chunk_kind = _safe_path_component(_chunk_kind_value(chunk))
    filename = (
        f"{_safe_path_component(chunk.artifact_id)}-"
        f"{chunk_kind}-{chunk.chunk_sequence:06d}{_artifact_chunk_extension(chunk, storage_key)}"
    )
    return f"artifact-chunks/{chunk_kind}/{filename}"


def _artifact_chunk_extension(chunk: ArtifactChunkMetadata, storage_key: str) -> str:
    media_type = chunk.media_type.lower()
    if media_type == "application/x-ndjson":
        return ".jsonl"
    if media_type == "application/json":
        return ".json"
    if media_type.startswith("text/"):
        return ".txt"
    suffix = Path(storage_key).suffix
    if suffix and re.fullmatch(r"\.[A-Za-z0-9]{1,12}", suffix):
        return suffix
    return ".bin"


def _safe_path_component(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip()).strip(".-")
    return safe or "artifact"


def _artifact_content_error(artifact: ArtifactRef, message: str) -> dict[str, Any]:
    return {"artifact_id": artifact.artifact_id, "kind": artifact.kind.value, "message": message}


def _artifact_chunk_content_error(chunk: ArtifactChunkMetadata, message: str) -> dict[str, Any]:
    return {
        "artifact_id": chunk.artifact_id,
        "chunk_kind": _chunk_kind_value(chunk),
        "chunk_sequence": chunk.chunk_sequence,
        "message": message,
    }


def _chunk_kind_value(chunk: ArtifactChunkMetadata) -> str:
    return chunk.chunk_kind.value if isinstance(chunk.chunk_kind, ArtifactChunkKind) else str(chunk.chunk_kind)


def _trajectory_jsonl(turns: list[TerminalTurn]) -> str:
    payload = "\n".join(json.dumps(_terminal_turn_payload(turn), sort_keys=True) for turn in turns)
    return f"{payload}\n" if payload else ""


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


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
