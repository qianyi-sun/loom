from __future__ import annotations

from dataclasses import dataclass, field, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any


ARTIFACT_OBJECT_METADATA_SCHEMA_VERSION = "artifact-object-metadata-v1"
ARTIFACT_CHUNK_METADATA_SCHEMA_VERSION = "artifact-chunk-metadata-v1"


class ArtifactContentType(str, Enum):
    TRAJECTORY_JSONL = "trajectory_jsonl"
    WORKSPACE_SNAPSHOT_MANIFEST = "workspace_snapshot_manifest"
    EVALUATOR_REPORT = "evaluator_report"
    HARBOR_JOBS_ARCHIVE = "harbor_jobs_archive"
    HARBOR_RUNNER_REPORT = "harbor_runner_report"
    HARBOR_INGESTION_DIAGNOSTICS = "harbor_ingestion_diagnostics"
    LOCAL_FILE_ARTIFACT = "local_file_artifact"
    ORIGINAL_WRAPPER_RESULT = "original_wrapper_result"
    ORIGINAL_WRAPPER_ARTIFACT = "original_wrapper_artifact"
    HARBOR_TASK_ARCHIVE = "harbor_task_archive"


class ArtifactUploadStatus(str, Enum):
    PENDING = "pending"
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"


class ArtifactChunkKind(str, Enum):
    STDOUT = "stdout"
    STDERR = "stderr"
    TRAJECTORY = "trajectory"
    ARTIFACT = "artifact"


@dataclass(frozen=True)
class ArtifactChunkMetadata:
    run_id: str
    attempt_id: str
    artifact_id: str
    chunk_kind: ArtifactChunkKind | str
    chunk_sequence: int
    storage_key: str
    media_type: str
    size_bytes: int | None
    sha256: str | None
    upload_status: ArtifactUploadStatus | str
    created_at: datetime
    upload_error_reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    schema_version: str = ARTIFACT_CHUNK_METADATA_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_non_empty("run_id", self.run_id)
        _require_non_empty("attempt_id", self.attempt_id)
        _require_non_empty("artifact_id", self.artifact_id)
        _require_non_empty("storage_key", self.storage_key)
        _require_safe_storage_key(self.storage_key)
        _require_non_empty("media_type", self.media_type)
        if self.chunk_sequence < 0:
            raise ValueError("chunk_sequence must be non-negative")
        object.__setattr__(self, "chunk_kind", _coerce_enum(ArtifactChunkKind, self.chunk_kind, "chunk_kind"))
        object.__setattr__(
            self,
            "upload_status",
            _coerce_enum(ArtifactUploadStatus, self.upload_status, "upload_status"),
        )
        if self.size_bytes is not None and self.size_bytes < 0:
            raise ValueError("size_bytes must be non-negative")
        if self.sha256 is not None and len(self.sha256) != 64:
            raise ValueError("sha256 must be a 64-character hex digest")
        if self.upload_status is ArtifactUploadStatus.COMPLETED and (
            self.size_bytes is None or self.sha256 is None
        ):
            raise ValueError("completed chunk uploads require size_bytes and sha256")
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")

    def to_dict(self) -> dict[str, Any]:
        return _to_jsonable(self)


def artifact_content_type_value(content_type: ArtifactContentType | str) -> str:
    if isinstance(content_type, ArtifactContentType):
        return content_type.value
    if isinstance(content_type, str) and content_type.strip():
        return content_type
    raise ValueError("content_type must be a non-empty string")


def artifact_upload_status_value(status: ArtifactUploadStatus | str) -> str:
    if isinstance(status, ArtifactUploadStatus):
        return status.value
    if isinstance(status, str) and status.strip():
        return status
    raise ValueError("upload_status must be a non-empty string")


def finalize_stored_artifact_metadata(
    metadata: dict[str, Any] | None,
    *,
    storage_key: str,
    size_bytes: int,
    sha256: str,
    storage_bucket: str | None = None,
) -> dict[str, Any]:
    if size_bytes < 0:
        raise ValueError("size_bytes must be non-negative")
    if len(sha256) != 64:
        raise ValueError("sha256 must be a 64-character hex digest")

    finalized = {key: _metadata_value(value) for key, value in dict(metadata or {}).items() if value is not None}
    if "content_type" in finalized:
        finalized["content_type"] = artifact_content_type_value(finalized["content_type"])
    finalized["artifact_metadata_schema"] = ARTIFACT_OBJECT_METADATA_SCHEMA_VERSION
    finalized["upload_status"] = artifact_upload_status_value(
        finalized.get("upload_status", ArtifactUploadStatus.COMPLETED)
    )
    finalized["storage_key"] = storage_key
    finalized["object_size_bytes"] = size_bytes
    finalized["object_sha256"] = sha256
    if storage_bucket is not None:
        finalized["storage_bucket"] = storage_bucket
    return finalized


def _metadata_value(value: Any) -> Any:
    if isinstance(value, ArtifactContentType):
        return value.value
    if isinstance(value, ArtifactUploadStatus):
        return value.value
    if isinstance(value, ArtifactChunkKind):
        return value.value
    return value


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _require_safe_storage_key(value: str) -> None:
    parts = value.split("/")
    if value.startswith("/") or any(part in {"", ".", ".."} for part in parts):
        raise ValueError("storage_key must be a safe object key")


def _coerce_enum(enum_type: type[Enum], value: Enum | str, field_name: str) -> Any:
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(value)
    except ValueError as exc:
        allowed = ", ".join(item.value for item in enum_type)
        raise ValueError(f"{field_name} must be one of: {allowed}") from exc


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if is_dataclass(value):
        return {
            field_name: _to_jsonable(field_value)
            for field_name, field_value in value.__dict__.items()
            if field_value is not None
        }
    if isinstance(value, dict):
        return {key: _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]
    return value
