"""raw-harbor-tb2-v2 delivery export framework (#745)."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, BinaryIO

from botocore.exceptions import ClientError
from pydantic import TypeAdapter, ValidationError

from loom.agent.terminus2.mapper import Terminus2TrajectoryMapper
from loom.agent.terminus2.provenance import HARBOR_COMPAT_SHA, LOOM_BRIDGE_REVISION
from loom.db.schema import LlmCall, Trial
from loom.models.trajectory import (
    EventKind,
    Terminus2ArtifactRefEvent,
    Terminus2RuntimeProvenanceEvent,
    Terminus2TerminalObservationEvent,
    Terminus2TurnEvent,
    TrajectoryEvent,
)

MAX_JSONL_BYTES = 50 * 1024 * 1024
MAX_JSONL_LINES = 100_000
LEGACY_BENCHMARK_SHA = "91e10457"
NATIVE_HARBOR_TRAJECTORY = "native/harbor_trajectory.json"
NATIVE_RECORDING_CAST = "native/recording.cast"
SECRET_PATTERNS = (
    re.compile(r"sk-[A-Za-z0-9]{8,}"),
    re.compile(r"(?i)bearer\s+[A-Za-z0-9._\-]{8,}"),
)

_event_adapter: TypeAdapter[TrajectoryEvent] = TypeAdapter(TrajectoryEvent)


class Tb2V2ExportError(Exception):
    """Fail-closed export errors for raw-harbor-tb2-v2."""

    status_code = 409

    def __init__(self, code: str, detail: dict[str, Any]) -> None:
        self.code = code
        super().__init__(code)
        self.detail = {"code": code, **detail}


@dataclass
class Tb2V2TrialBundle:
    execution_trajectory: dict[str, Any]
    model_input_trajectory: dict[str, Any]
    terminal_transcript: bytes
    export_provenance: dict[str, Any]
    native_artifacts: dict[str, bytes] = field(default_factory=dict)
    artifact_manifest_entries: list[dict[str, str]] = field(default_factory=list)


def _iter_jsonl_raw_lines(stream: BinaryIO) -> Iterator[bytes]:
    """Yield JSONL records as raw bytes.

    boto3 ``StreamingBody.__iter__`` yields fixed-size *chunks* (typically
    1 KiB), not newline-delimited records. Prefer ``iter_lines()`` when
    present so MinIO GetObject streams parse as true JSONL.
    """
    iter_lines = getattr(stream, "iter_lines", None)
    if callable(iter_lines):
        yield from iter_lines()
        return
    yield from stream


def parse_trajectory_events(stream: BinaryIO) -> list[TrajectoryEvent]:
    """Parse trajectory JSONL with bounded size/line caps."""
    events: list[TrajectoryEvent] = []
    total_bytes = 0
    for line_no, raw in enumerate(_iter_jsonl_raw_lines(stream), start=1):
        if line_no > MAX_JSONL_LINES:
            raise Tb2V2ExportError(
                "trajectory_parse_limit_exceeded",
                {
                    "message": "trajectory JSONL exceeds maximum line count",
                    "max_lines": MAX_JSONL_LINES,
                },
            )
        total_bytes += len(raw)
        if total_bytes > MAX_JSONL_BYTES:
            raise Tb2V2ExportError(
                "trajectory_parse_limit_exceeded",
                {
                    "message": "trajectory JSONL exceeds maximum byte size",
                    "max_bytes": MAX_JSONL_BYTES,
                },
            )
        line = raw.strip()
        if not line:
            continue
        try:
            events.append(_event_adapter.validate_json(line))
        except ValidationError as exc:
            raise Tb2V2ExportError(
                "trajectory_parse_failed",
                {
                    "message": "trajectory JSONL contains an invalid event",
                    "line": line_no,
                    "error": str(exc),
                },
            ) from exc
    return events


def _agent_name_for_trial(trial: Trial) -> str:
    config = trial.config if isinstance(trial.config, dict) else {}
    raw = config.get("agent_name")
    return str(raw) if raw else "unknown"


def _has_legacy_runtime_markers(events: list[TrajectoryEvent]) -> bool:
    for event in events:
        if event.kind == EventKind.AGENT_THOUGHT:
            return True
        if event.kind == EventKind.ENV_EXEC:
            return True
    return False


def _normalize_hash(value: str) -> str:
    normalized = value.strip().lower()
    if normalized.startswith("sha256:"):
        return normalized.removeprefix("sha256:")
    return normalized


def validate_v2_eligibility(events: list[TrajectoryEvent], trial: Trial) -> None:
    agent_name = _agent_name_for_trial(trial)
    if agent_name != "terminus-2":
        raise Tb2V2ExportError(
            "incompatible_agent",
            {
                "message": "raw-harbor-tb2-v2 requires terminus-2 trials",
                "trial_id": str(trial.id),
                "agent_name": agent_name,
            },
        )

    provenance_events = [
        event
        for event in events
        if event.kind == EventKind.TERMINUS2_RUNTIME_PROVENANCE
    ]
    if not provenance_events:
        raise Tb2V2ExportError(
            "missing_provenance",
            {
                "message": "trajectory is missing terminus2_runtime_provenance",
                "trial_id": str(trial.id),
            },
        )
    provenance = provenance_events[0]
    assert isinstance(provenance, Terminus2RuntimeProvenanceEvent)
    if provenance.harbor_compat_sha != HARBOR_COMPAT_SHA:
        raise Tb2V2ExportError(
            "mixed_provenance",
            {
                "message": "harbor_compat_sha does not match the pinned bridge revision",
                "trial_id": str(trial.id),
                "expected": HARBOR_COMPAT_SHA,
                "actual": provenance.harbor_compat_sha,
            },
        )

    benchmark = provenance.benchmark_provenance
    if isinstance(benchmark, dict):
        serialized = json.dumps(benchmark, sort_keys=True)
        if LEGACY_BENCHMARK_SHA in serialized:
            raise Tb2V2ExportError(
                "legacy_benchmark_provenance",
                {
                    "message": "trajectory contains legacy benchmark provenance",
                    "trial_id": str(trial.id),
                },
            )

    turn_events = [event for event in events if event.kind == EventKind.TERMINUS2_TURN]
    if not turn_events:
        if _has_legacy_runtime_markers(events):
            raise Tb2V2ExportError(
                "legacy_runtime_stream",
                {
                    "message": (
                        "trajectory contains legacy subprocess runtime markers "
                        "without typed terminus2 turns"
                    ),
                    "trial_id": str(trial.id),
                },
            )
        raise Tb2V2ExportError(
            "legacy_runtime_stream",
            {
                "message": "trajectory lacks typed terminus2 turn events",
                "trial_id": str(trial.id),
            },
        )


def validate_v2_joins(events: list[TrajectoryEvent]) -> list[str]:
    errors = Terminus2TrajectoryMapper.validate_turn_joins(events)
    turn_events = [
        event
        for event in events
        if isinstance(event, Terminus2TurnEvent)
    ]
    if not turn_events:
        return errors

    artifact_refs = [
        event
        for event in events
        if isinstance(event, Terminus2ArtifactRefEvent)
    ]
    has_trajectory_ref = any(
        ref.artifact_kind == "terminus_2.pane" for ref in artifact_refs
    )
    if not has_trajectory_ref:
        errors.append(
            "missing terminus2_artifact_ref for native harbor trajectory",
        )
    return errors


def build_model_input_trajectory(
    *,
    trial_id: str,
    task_id: str,
    calls: list[LlmCall],
    messages_from_raw_log: Any,
) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    for index, call in enumerate(calls, start=1):
        extras = call.provider_extras if isinstance(call.provider_extras, dict) else {}
        raw_log = extras.get("_loom_raw_provider_log")
        if not isinstance(raw_log, dict):
            continue
        messages = messages_from_raw_log(raw_log, normalize_tb2=False)
        if not messages:
            continue
        entries.append(
            {
                "index": index,
                "llm_call_id": str(call.id),
                "step_id": call.step_id,
                "model": call.model,
                "dialect": call.dialect,
                "messages": messages,
            }
        )
    return {
        "schema_version": "harbor-tb2-v2-model-input",
        "trial_id": trial_id,
        "task_id": task_id,
        "source_of_truth": "provider_logs",
        "calls": entries,
    }


def build_terminal_transcript(events: list[TrajectoryEvent]) -> bytes:
    rows: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, Terminus2TerminalObservationEvent):
            continue
        turn_index = next(
            (
                turn.turn_index
                for turn in events
                if isinstance(turn, Terminus2TurnEvent)
                and turn.turn_id == event.turn_id
            ),
            None,
        )
        rows.append(
            {
                "turn_id": event.turn_id,
                "turn_index": turn_index,
                "text": event.text,
                "content_hash": event.content_hash,
                "is_aggregate": event.is_aggregate,
                "completeness": event.completeness,
                "capture_source": event.capture_source,
            }
        )
    return b"".join(
        (json.dumps(row, ensure_ascii=False) + "\n").encode() for row in rows
    )


def build_execution_trajectory(
    events: list[TrajectoryEvent],
    *,
    task_id: str,
    agent_name: str,
    agent_version: str,
) -> dict[str, Any]:
    return Terminus2TrajectoryMapper.project_to_atif(
        events,
        task_id=task_id,
        agent_name=agent_name,
        agent_version=agent_version,
    )


def reasoning_by_gateway_from_calls(
    calls: list[LlmCall],
    *,
    messages_from_raw_log: Any,
) -> dict[str, str]:
    """Map gateway/llm call id → reasoning_content from provider logs."""
    out: dict[str, str] = {}
    for call in calls:
        extras = call.provider_extras if isinstance(call.provider_extras, dict) else {}
        raw_log = extras.get("_loom_raw_provider_log")
        if not isinstance(raw_log, dict):
            continue
        messages = messages_from_raw_log(raw_log, normalize_tb2=False)
        for message in reversed(messages or []):
            if not isinstance(message, dict) or message.get("role") != "assistant":
                continue
            reasoning = message.get("reasoning_content")
            if isinstance(reasoning, str) and reasoning:
                out[str(call.id)] = reasoning
            break
    return out


def enrich_execution_trajectory(
    trajectory: dict[str, Any],
    *,
    native_bytes: bytes | None,
    reasoning_by_gateway: dict[str, str] | None = None,
) -> dict[str, Any]:
    native: dict[str, Any] | None = None
    if native_bytes:
        try:
            parsed = json.loads(native_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            parsed = None
        if isinstance(parsed, dict):
            native = parsed
    return Terminus2TrajectoryMapper.enrich_from_native(
        trajectory,
        native,
        reasoning_by_gateway=reasoning_by_gateway,
    )


def build_export_provenance(
    events: list[TrajectoryEvent],
    *,
    trial_id: str,
    join_errors: list[str],
) -> dict[str, Any]:
    provenance = next(
        event
        for event in events
        if isinstance(event, Terminus2RuntimeProvenanceEvent)
    )
    return {
        "schema_version": "1",
        "trial_id": trial_id,
        "bridge_revision": LOOM_BRIDGE_REVISION,
        "harbor_compat_sha": provenance.harbor_compat_sha,
        "loom_runtime_revision": provenance.loom_runtime_revision,
        "parser_name": provenance.parser_name,
        "benchmark_provenance": provenance.benchmark_provenance,
        "join_validation": {
            "ok": not join_errors,
            "errors": join_errors,
        },
    }


def _index_artifacts(trial: Trial) -> list[dict[str, Any]]:
    trajectory_index = (
        trial.trajectory_index if isinstance(trial.trajectory_index, dict) else {}
    )
    raw = trajectory_index.get("artifacts")
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, dict)]


def _artifact_key_matches(key: str, *, suffix: str) -> bool:
    return key.endswith(suffix) or suffix in key


def _fetch_artifact_bytes(
    client: Any,
    *,
    bucket: str,
    key: str,
) -> bytes:
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        raise Tb2V2ExportError(
            "missing_native_artifact",
            {
                "message": "native Harbor artifact is missing from object storage",
                "bucket": bucket,
                "key": key,
                "error_code": code,
            },
        ) from exc
    body = obj["Body"]
    try:
        return bytes(body.read())
    finally:
        close = getattr(body, "close", None)
        if callable(close):
            close()


def resolve_native_artifacts(
    trial: Trial,
    events: list[TrajectoryEvent],
    *,
    client: Any,
    artifacts_bucket: str,
) -> dict[str, bytes]:
    refs = [
        event
        for event in events
        if isinstance(event, Terminus2ArtifactRefEvent)
    ]
    indexed = _index_artifacts(trial)
    resolved: dict[str, bytes] = {}

    def _resolve_ref(
        ref: Terminus2ArtifactRefEvent,
        *,
        archive_path: str,
        key_suffix: str,
    ) -> None:
        candidate_keys = [
            item["key"]
            for item in indexed
            if isinstance(item.get("key"), str)
            and _artifact_key_matches(item["key"], suffix=key_suffix)
        ]
        if not candidate_keys:
            raise Tb2V2ExportError(
                "missing_native_artifact",
                {
                    "message": (
                        "terminus2_artifact_ref is present but no matching "
                        "trajectory_index artifact was found"
                    ),
                    "trial_id": str(trial.id),
                    "artifact_kind": ref.artifact_kind,
                    "sandbox_path": ref.sandbox_path,
                },
            )
        data = _fetch_artifact_bytes(
            client,
            bucket=artifacts_bucket,
            key=candidate_keys[0],
        )
        actual_hash = hashlib.sha256(data).hexdigest()
        expected_hash = _normalize_hash(ref.content_hash)
        if actual_hash != expected_hash:
            raise Tb2V2ExportError(
                "missing_native_artifact",
                {
                    "message": "native Harbor artifact hash does not match artifact ref",
                    "trial_id": str(trial.id),
                    "artifact_kind": ref.artifact_kind,
                    "expected_hash": expected_hash,
                    "actual_hash": actual_hash,
                },
            )
        resolved[archive_path] = data

    for ref in refs:
        if ref.artifact_kind == "terminus_2.pane":
            _resolve_ref(
                ref,
                archive_path=NATIVE_HARBOR_TRAJECTORY,
                key_suffix=".loom/agent/trajectory.json",
            )
        elif ref.artifact_kind == "recording.cast":
            _resolve_ref(
                ref,
                archive_path=NATIVE_RECORDING_CAST,
                key_suffix="recording.cast",
            )

    return resolved


def scan_members_for_secrets(members: dict[str, bytes]) -> None:
    for path, data in members.items():
        text = data.decode("utf-8", errors="replace")
        for pattern in SECRET_PATTERNS:
            if pattern.search(text):
                raise Tb2V2ExportError(
                    "secret_scan_failed",
                    {
                        "message": "export payload matched a secret pattern",
                        "path": path,
                    },
                )


def build_per_trial_v2_bundle(
    *,
    trial: Trial,
    events: list[TrajectoryEvent],
    calls: list[LlmCall],
    client: Any,
    artifacts_bucket: str,
    messages_from_raw_log: Any,
    agent_version: str = "1.0",
) -> Tb2V2TrialBundle:
    validate_v2_eligibility(events, trial)
    join_errors = validate_v2_joins(events)
    if join_errors:
        raise Tb2V2ExportError(
            "join_validation_failed",
            {
                "message": "typed terminus2 turn joins failed validation",
                "trial_id": str(trial.id),
                "errors": join_errors,
            },
        )

    agent_name = _agent_name_for_trial(trial)
    execution_trajectory = build_execution_trajectory(
        events,
        task_id=trial.task_id,
        agent_name=agent_name,
        agent_version=agent_version,
    )
    model_input_trajectory = build_model_input_trajectory(
        trial_id=str(trial.id),
        task_id=trial.task_id,
        calls=calls,
        messages_from_raw_log=messages_from_raw_log,
    )
    terminal_transcript = build_terminal_transcript(events)
    export_provenance = build_export_provenance(
        events,
        trial_id=str(trial.id),
        join_errors=join_errors,
    )
    native_artifacts = resolve_native_artifacts(
        trial,
        events,
        client=client,
        artifacts_bucket=artifacts_bucket,
    )
    execution_trajectory = enrich_execution_trajectory(
        execution_trajectory,
        native_bytes=native_artifacts.get(NATIVE_HARBOR_TRAJECTORY),
        reasoning_by_gateway=reasoning_by_gateway_from_calls(
            calls,
            messages_from_raw_log=messages_from_raw_log,
        ),
    )

    artifact_manifest_entries = [
        {"kind": "execution_trajectory", "path": "trajectory.json"},
        {"kind": "model_input_trajectory", "path": "model_input_trajectory.json"},
        {"kind": "terminal_transcript", "path": "terminal_transcript.jsonl"},
        {"kind": "audit_spine", "path": "loom_trajectory.jsonl"},
        {"kind": "export_provenance", "path": "export_provenance.json"},
    ]
    if NATIVE_HARBOR_TRAJECTORY in native_artifacts:
        artifact_manifest_entries.append(
            {"kind": "native_harbor_trajectory", "path": NATIVE_HARBOR_TRAJECTORY},
        )
    if NATIVE_RECORDING_CAST in native_artifacts:
        artifact_manifest_entries.append(
            {"kind": "native_recording_cast", "path": NATIVE_RECORDING_CAST},
        )

    scan_payload = {
        "trajectory.json": json.dumps(execution_trajectory, ensure_ascii=False).encode(),
        "model_input_trajectory.json": json.dumps(
            model_input_trajectory,
            ensure_ascii=False,
        ).encode(),
        "terminal_transcript.jsonl": terminal_transcript,
        "export_provenance.json": json.dumps(export_provenance, ensure_ascii=False).encode(),
        **native_artifacts,
    }
    scan_members_for_secrets(scan_payload)

    return Tb2V2TrialBundle(
        execution_trajectory=execution_trajectory,
        model_input_trajectory=model_input_trajectory,
        terminal_transcript=terminal_transcript,
        export_provenance=export_provenance,
        native_artifacts=native_artifacts,
        artifact_manifest_entries=artifact_manifest_entries,
    )
