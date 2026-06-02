from __future__ import annotations

import hashlib
import io
import json
import re
import tarfile
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Protocol, runtime_checkable
from urllib.parse import quote

from agentic_data_platform.domain.artifact_metadata import (
    ArtifactChunkKind,
    ArtifactChunkMetadata,
    ArtifactContentType,
    ArtifactUploadStatus,
    finalize_stored_artifact_metadata,
)
from agentic_data_platform.domain.run_records import (
    ArtifactKind,
    ArtifactRef,
    EvaluatorResult,
    TerminalTurn,
)
from agentic_data_platform.sandbox.docker_terminal import WorkspaceSnapshot


@dataclass(frozen=True)
class StoredArtifact:
    key: str
    uri: str
    media_type: str
    size_bytes: int
    sha256: str
    metadata: dict[str, Any]


@runtime_checkable
class Artifacpilot groupjectStore(Protocol):
    def ensure_bucket(self) -> None:
        ...

    def put_bytes(
        self,
        key: str,
        payload: bytes,
        *,
        media_type: str,
        metadata: dict[str, Any] | None = None,
    ) -> StoredArtifact:
        ...

    def get_bytes(self, key: str) -> bytes:
        ...

    def presigned_get_url(self, key: str, *, expires_in_seconds: int = 3600) -> str:
        ...


class LocalArtifactStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def ensure_bucket(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)

    def put_bytes(
        self,
        key: str,
        payload: bytes,
        *,
        media_type: str,
        metadata: dict[str, Any] | None = None,
    ) -> StoredArtifact:
        safe_key = _validate_artifact_key(key)
        _require_non_empty("media_type", media_type)

        target = self.root / safe_key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)

        digest = hashlib.sha256(payload).hexdigest()
        artifact_metadata = finalize_stored_artifact_metadata(
            metadata,
            storage_key=safe_key,
            size_bytes=len(payload),
            sha256=digest,
        )

        return StoredArtifact(
            key=safe_key,
            uri=target.resolve().as_uri(),
            media_type=media_type,
            size_bytes=len(payload),
            sha256=digest,
            metadata=artifact_metadata,
        )

    def get_bytes(self, key: str) -> bytes:
        safe_key = _validate_artifact_key(key)
        return (self.root / safe_key).read_bytes()

    def presigned_get_url(self, key: str, *, expires_in_seconds: int = 3600) -> str:
        safe_key = _validate_artifact_key(key)
        _validate_expiry(expires_in_seconds)
        return (self.root / safe_key).resolve().as_uri()


class S3ArtifactStore:
    def __init__(self, *, bucket: str, client) -> None:
        _require_non_empty("bucket", bucket)
        self.bucket = bucket
        self.client = client

    def ensure_bucket(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except Exception as exc:
            if not _is_missing_bucket_error(exc):
                raise
            self.client.create_bucket(Bucket=self.bucket)

    def put_bytes(
        self,
        key: str,
        payload: bytes,
        *,
        media_type: str,
        metadata: dict[str, Any] | None = None,
    ) -> StoredArtifact:
        safe_key = _validate_artifact_key(key)
        _require_non_empty("media_type", media_type)

        digest = hashlib.sha256(payload).hexdigest()
        artifact_metadata = finalize_stored_artifact_metadata(
            metadata,
            storage_key=safe_key,
            size_bytes=len(payload),
            sha256=digest,
            storage_bucket=self.bucket,
        )

        self.client.put_object(
            Bucket=self.bucket,
            Key=safe_key,
            Body=payload,
            ContentType=media_type,
            Metadata={key: str(value) for key, value in artifact_metadata.items()},
        )

        return StoredArtifact(
            key=safe_key,
            uri=f"s3://{self.bucket}/{safe_key}",
            media_type=media_type,
            size_bytes=len(payload),
            sha256=digest,
            metadata=artifact_metadata,
        )

    def get_bytes(self, key: str) -> bytes:
        safe_key = _validate_artifact_key(key)
        response = self.client.get_object(Bucket=self.bucket, Key=safe_key)
        return response["Body"].read()

    def presigned_get_url(self, key: str, *, expires_in_seconds: int = 3600) -> str:
        safe_key = _validate_artifact_key(key)
        _validate_expiry(expires_in_seconds)
        return self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": safe_key},
            ExpiresIn=expires_in_seconds,
        )


def build_s3_artifact_store(
    *,
    endpoint_url: str,
    bucket: str,
    access_key: str,
    secret_key: str,
    region: str = "us-east-1",
) -> S3ArtifactStore:
    _require_non_empty("endpoint_url", endpoint_url)
    _require_non_empty("bucket", bucket)
    _require_non_empty("access_key", access_key)
    _require_non_empty("secret_key", secret_key)
    _require_non_empty("region", region)

    import boto3
    from botocore.config import Config

    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    return S3ArtifactStore(bucket=bucket, client=client)


class ArtifactKeyFactory:
    def trajectory_key(self, run_id: str, task_instance_id: str) -> str:
        return self._key(run_id, task_instance_id, "trajectory", "trajectory.jsonl")

    def workspace_snapshot_key(self, run_id: str, task_instance_id: str) -> str:
        return self._key(run_id, task_instance_id, "workspace", "snapshot.json")

    def evaluator_report_key(self, run_id: str, task_instance_id: str, evaluator_id: str) -> str:
        return self._key(run_id, task_instance_id, "evaluation", _safe_component(evaluator_id), "report.json")

    def harbor_jobs_archive_key(self, run_id: str, task_instance_id: str, job_name: str) -> str:
        return self._key(run_id, task_instance_id, "raw", "harbor-jobs", f"{_safe_component(job_name)}.tar.gz")

    def harbor_runner_report_key(self, run_id: str, task_instance_id: str) -> str:
        return self._key(run_id, task_instance_id, "logs", "harbor-runner.json")

    def harbor_ingestion_diagnostics_key(self, run_id: str, task_instance_id: str) -> str:
        return self._key(run_id, task_instance_id, "logs", "harbor-ingestion-diagnostics.json")

    def terminal_log_chunk_key(
        self,
        run_id: str,
        task_instance_id: str,
        attempt_id: str,
        stream: ArtifactChunkKind | str,
        chunk_sequence: int,
    ) -> str:
        if chunk_sequence < 0:
            raise ValueError("chunk_sequence must be non-negative")
        stream_name = stream.value if isinstance(stream, ArtifactChunkKind) else stream
        return self._key(
            run_id,
            task_instance_id,
            "attempts",
            _safe_component(attempt_id),
            "logs",
            _safe_component(stream_name),
            f"{chunk_sequence:06d}.txt",
        )

    def wrapper_artifact_key(self, run_id: str, task_instance_id: str, artifact_path: str) -> str:
        return self._key(run_id, task_instance_id, "wrapper", *_safe_relative_parts(artifact_path))

    def _key(self, run_id: str, task_instance_id: str, *parts: str) -> str:
        return "/".join(
            [
                "runs",
                _safe_component(run_id),
                "tasks",
                _safe_component(task_instance_id),
                *[_safe_component(part) for part in parts],
            ]
        )


class ArtifactPersistence:
    def __init__(
        self,
        store: Artifacpilot groupjectStore,
        *,
        key_factory: ArtifactKeyFactory | None = None,
    ) -> None:
        self.store = store
        self.key_factory = key_factory or ArtifactKeyFactory()

    def persist_trajectory(
        self,
        *,
        run_id: str,
        task_instance_id: str,
        turns: list[TerminalTurn],
    ) -> ArtifactRef:
        key = self.key_factory.trajectory_key(run_id, task_instance_id)
        payload = "\n".join(json.dumps(_to_jsonable(turn), sort_keys=True) for turn in turns)
        if payload:
            payload += "\n"

        stored = self.store.put_bytes(
            key,
            payload.encode("utf-8"),
            media_type="application/x-ndjson",
            metadata={
                "run_id": run_id,
                "task_instance_id": task_instance_id,
                "content_type": ArtifactContentType.TRAJECTORY_JSONL,
                "turn_count": len(turns),
            },
        )

        return _artifact_ref(
            stored,
            artifact_id=f"{_safe_component(run_id)}-trajectory",
            kind=ArtifactKind.TRAJECTORY,
        )

    def persist_workspace_snapshot(
        self,
        *,
        run_id: str,
        task_instance_id: str,
        snapshot: WorkspaceSnapshot,
    ) -> ArtifactRef:
        key = self.key_factory.workspace_snapshot_key(run_id, task_instance_id)
        payload = _json_bytes(snapshot)

        stored = self.store.put_bytes(
            key,
            payload,
            media_type="application/json",
            metadata={
                "run_id": run_id,
                "task_instance_id": task_instance_id,
                "content_type": ArtifactContentType.WORKSPACE_SNAPSHOT_MANIFEST,
                "file_count": len(snapshot.files),
            },
        )

        return _artifact_ref(
            stored,
            artifact_id=f"{_safe_component(run_id)}-workspace-snapshot",
            kind=ArtifactKind.WORKSPACE_SNAPSHOT,
        )

    def persist_evaluator_report(
        self,
        *,
        run_id: str,
        task_instance_id: str,
        result: EvaluatorResult,
    ) -> ArtifactRef:
        key = self.key_factory.evaluator_report_key(run_id, task_instance_id, result.evaluator_id)
        payload = _json_bytes(result)

        stored = self.store.put_bytes(
            key,
            payload,
            media_type="application/json",
            metadata={
                "run_id": run_id,
                "task_instance_id": task_instance_id,
                "content_type": ArtifactContentType.EVALUATOR_REPORT,
                "evaluator_id": result.evaluator_id,
                "evaluator_status": result.status,
            },
        )

        return _artifact_ref(
            stored,
            artifact_id=f"{_safe_component(run_id)}-{_safe_component(result.evaluator_id)}-report",
            kind=ArtifactKind.EVALUATOR_REPORT,
        )

    def persist_harbor_jobs_archive(
        self,
        *,
        run_id: str,
        task_instance_id: str,
        job_name: str,
        jobs_dir: Path,
    ) -> ArtifactRef:
        if not jobs_dir.is_dir():
            raise ValueError(f"Harbor jobs directory does not exist: {jobs_dir}")

        payload, file_count = _tar_gz_directory(jobs_dir)
        key = self.key_factory.harbor_jobs_archive_key(run_id, task_instance_id, job_name)
        stored = self.store.put_bytes(
            key,
            payload,
            media_type="application/gzip",
            metadata={
                "run_id": run_id,
                "task_instance_id": task_instance_id,
                "content_type": ArtifactContentType.HARBOR_JOBS_ARCHIVE,
                "job_name": job_name,
                "file_count": file_count,
            },
        )

        return _artifact_ref(
            stored,
            artifact_id=f"{_safe_component(run_id)}-{_safe_component(job_name)}-harbor-jobs",
            kind=ArtifactKind.LOG,
        )

    def persist_harbor_runner_report(
        self,
        *,
        run_id: str,
        task_instance_id: str,
        report: dict[str, Any],
    ) -> ArtifactRef:
        key = self.key_factory.harbor_runner_report_key(run_id, task_instance_id)
        stored = self.store.put_bytes(
            key,
            _json_bytes(report),
            media_type="application/json",
            metadata={
                "run_id": run_id,
                "task_instance_id": task_instance_id,
                "content_type": ArtifactContentType.HARBOR_RUNNER_REPORT,
                "exit_code": report.get("exit_code"),
                "timed_out": report.get("timed_out", False),
            },
        )

        return _artifact_ref(
            stored,
            artifact_id=f"{_safe_component(run_id)}-harbor-runner-report",
            kind=ArtifactKind.LOG,
        )

    def persist_harbor_ingestion_diagnostics(
        self,
        *,
        run_id: str,
        task_instance_id: str,
        diagnostics: dict[str, Any],
    ) -> ArtifactRef:
        key = self.key_factory.harbor_ingestion_diagnostics_key(run_id, task_instance_id)
        stored = self.store.put_bytes(
            key,
            _json_bytes(diagnostics),
            media_type="application/json",
            metadata={
                "run_id": run_id,
                "task_instance_id": task_instance_id,
                "content_type": ArtifactContentType.HARBOR_INGESTION_DIAGNOSTICS,
                "failure_category": diagnostics.get("category"),
                "failure_source": diagnostics.get("source"),
            },
        )

        return _artifact_ref(
            stored,
            artifact_id=f"{_safe_component(run_id)}-harbor-ingestion-diagnostics",
            kind=ArtifactKind.LOG,
        )

    def persist_wrapper_artifact(
        self,
        *,
        run_id: str,
        task_instance_id: str,
        local_path: Path,
        artifact_path: str,
        kind: ArtifactKind,
        media_type: str,
        metadata: dict[str, Any] | None = None,
    ) -> ArtifactRef:
        if not local_path.is_file():
            raise ValueError(f"artifact file does not exist: {local_path}")
        relative_parts = _safe_relative_parts(artifact_path)
        stored = self.store.put_bytes(
            self.key_factory.wrapper_artifact_key(run_id, task_instance_id, artifact_path),
            local_path.read_bytes(),
            media_type=media_type,
            metadata={
                "run_id": run_id,
                "task_instance_id": task_instance_id,
                "content_type": ArtifactContentType.LOCAL_FILE_ARTIFACT,
                "artifact_path": "/".join(relative_parts),
                **dict(metadata or {}),
            },
        )

        return _artifact_ref(
            stored,
            artifact_id=f"{_safe_component(run_id)}-{_safe_component('-'.join(relative_parts))}",
            kind=kind,
        )

    def persist_terminal_log_chunks(
        self,
        *,
        run_id: str,
        task_instance_id: str,
        attempt_id: str,
        trajectory_artifact_id: str,
        turns: list[TerminalTurn],
    ) -> list[ArtifactChunkMetadata]:
        chunks: list[ArtifactChunkMetadata] = []
        sequence_by_stream = {
            ArtifactChunkKind.STDOUT: 0,
            ArtifactChunkKind.STDERR: 0,
        }
        for turn in turns:
            for chunk_kind, text in (
                (ArtifactChunkKind.STDOUT, turn.stdout),
                (ArtifactChunkKind.STDERR, turn.stderr),
            ):
                if not text:
                    continue
                chunk_sequence = sequence_by_stream[chunk_kind]
                sequence_by_stream[chunk_kind] += 1
                stored = self.store.put_bytes(
                    self.key_factory.terminal_log_chunk_key(
                        run_id,
                        task_instance_id,
                        attempt_id,
                        chunk_kind,
                        chunk_sequence,
                    ),
                    text.encode("utf-8"),
                    media_type="text/plain; charset=utf-8",
                    metadata={
                        "run_id": run_id,
                        "task_instance_id": task_instance_id,
                        "attempt_id": attempt_id,
                        "artifact_id": trajectory_artifact_id,
                        "chunk_kind": chunk_kind.value,
                        "chunk_sequence": chunk_sequence,
                        "turn_index": turn.turn_index,
                        "stream": chunk_kind.value,
                    },
                )
                chunks.append(
                    ArtifactChunkMetadata(
                        run_id=run_id,
                        attempt_id=attempt_id,
                        artifact_id=trajectory_artifact_id,
                        chunk_kind=chunk_kind,
                        chunk_sequence=chunk_sequence,
                        storage_key=stored.key,
                        media_type=stored.media_type,
                        size_bytes=stored.size_bytes,
                        sha256=stored.sha256,
                        upload_status=ArtifactUploadStatus.COMPLETED,
                        created_at=datetime.now(timezone.utc),
                        metadata={
                            "turn_index": turn.turn_index,
                            "stream": chunk_kind.value,
                            "command": turn.command,
                            "model_call_id": turn.model_call_id,
                        },
                    )
                )
        return chunks


def _artifact_ref(stored: StoredArtifact, *, artifact_id: str, kind: ArtifactKind) -> ArtifactRef:
    return ArtifactRef(
        artifact_id=artifact_id,
        kind=kind,
        uri=stored.uri,
        media_type=stored.media_type,
        sha256=stored.sha256,
        size_bytes=stored.size_bytes,
        metadata=stored.metadata,
    )


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(_to_jsonable(value), indent=2, sort_keys=True) + "\n").encode("utf-8")


def _tar_gz_directory(source_dir: Path) -> tuple[bytes, int]:
    root = source_dir.resolve()
    file_count = 0
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for path in sorted(root.rglob("*")):
            if not path.is_file() or path.is_symlink():
                continue
            relative_path = path.relative_to(root.parent).as_posix()
            archive.add(path, arcname=relative_path, recursive=False)
            file_count += 1
    return buffer.getvalue(), file_count


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value

    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    if is_dataclass(value):
        return {
            item.name: _to_jsonable(getattr(value, item.name))
            for item in fields(value)
            if getattr(value, item.name) is not None
        }

    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]

    if isinstance(value, dict):
        return {key: _to_jsonable(item) for key, item in value.items()}

    return value


def _validate_artifact_key(key: str) -> str:
    _require_non_empty("key", key)

    if key.startswith("/"):
        raise ValueError("unsafe artifact key: absolute paths are not allowed")

    parts = key.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError("unsafe artifact key: empty, current, or parent path segments are not allowed")

    return key


def _safe_relative_parts(value: str) -> tuple[str, ...]:
    _require_non_empty("artifact_path", value)
    path = Path(value)
    if path.is_absolute():
        raise ValueError("artifact_path must be relative")
    parts = path.parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise ValueError("artifact_path must not contain empty, current, or parent segments")
    return tuple(parts)


def _validate_expiry(expires_in_seconds: int) -> None:
    if not isinstance(expires_in_seconds, int) or expires_in_seconds <= 0:
        raise ValueError("expires_in_seconds must be a positive integer")


def _is_missing_bucket_error(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    error = response.get("Error")
    if not isinstance(error, dict):
        return False
    return str(error.get("Code")) in {"404", "NoSuchBucket", "NotFound"}


def _safe_component(value: str) -> str:
    _require_non_empty("path component", value)
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip()).strip(".-")
    if not safe:
        raise ValueError("path component must contain at least one safe character")
    return quote(safe, safe="A-Za-z0-9_.-")


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
