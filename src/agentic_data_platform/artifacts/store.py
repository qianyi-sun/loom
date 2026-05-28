from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Protocol, runtime_checkable
from urllib.parse import quote

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
    def put_bytes(
        self,
        key: str,
        payload: bytes,
        *,
        media_type: str,
        metadata: dict[str, Any] | None = None,
    ) -> StoredArtifact:
        ...


class LocalArtifactStore:
    def __init__(self, root: Path) -> None:
        self.root = root
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
        artifact_metadata = dict(metadata or {})
        artifact_metadata["storage_key"] = safe_key

        return StoredArtifact(
            key=safe_key,
            uri=target.resolve().as_uri(),
            media_type=media_type,
            size_bytes=len(payload),
            sha256=digest,
            metadata=artifact_metadata,
        )


class ArtifactKeyFactory:
    def trajectory_key(self, run_id: str, task_instance_id: str) -> str:
        return self._key(run_id, task_instance_id, "trajectory", "trajectory.jsonl")

    def workspace_snapshot_key(self, run_id: str, task_instance_id: str) -> str:
        return self._key(run_id, task_instance_id, "workspace", "snapshot.json")

    def evaluator_report_key(self, run_id: str, task_instance_id: str, evaluator_id: str) -> str:
        return self._key(run_id, task_instance_id, "evaluation", _safe_component(evaluator_id), "report.json")

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
                "content_type": "trajectory_jsonl",
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
                "content_type": "workspace_snapshot_manifest",
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
                "content_type": "evaluator_report",
                "evaluator_id": result.evaluator_id,
                "evaluator_status": result.status,
            },
        )

        return _artifact_ref(
            stored,
            artifact_id=f"{_safe_component(run_id)}-{_safe_component(result.evaluator_id)}-report",
            kind=ArtifactKind.EVALUATOR_REPORT,
        )


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


def _safe_component(value: str) -> str:
    _require_non_empty("path component", value)
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip()).strip(".-")
    if not safe:
        raise ValueError("path component must contain at least one safe character")
    return quote(safe, safe="A-Za-z0-9_.-")


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
