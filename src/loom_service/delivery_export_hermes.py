"""hermes-export delivery export framework."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from loom.agent.hermes.mapper import HermesTrajectoryMapper
from loom.db.schema import LlmCall, Trial
from loom.models.trajectory import (
    EventKind,
    HermesArtifactRefEvent,
    HermesRuntimeProvenanceEvent,
    TrajectoryEvent,
)
from loom_service.delivery_export_tb2_v2 import (
    _fetch_artifact_bytes,
    _index_artifacts,
    _normalize_hash,
    _resolve_indexed_artifact_bucket,
    build_model_input_trajectory,
    parse_trajectory_events,
    scan_members_for_secrets,
)

NATIVE_HERMES_SESSION = "native/hermes_session.json"
SANDBOX_HERMES_SESSION = ".loom/agent/hermes_session.json"
LOOM_BRIDGE_REVISION = "1.0"


class HermesExportError(Exception):
    """Fail-closed export errors for hermes-export."""

    status_code = 409

    def __init__(self, code: str, detail: dict[str, Any]) -> None:
        self.code = code
        super().__init__(code)
        self.detail = {"code": code, **detail}


@dataclass
class HermesTrialBundle:
    execution_trajectory: dict[str, Any]
    model_input_trajectory: dict[str, Any]
    export_provenance: dict[str, Any]
    native_artifacts: dict[str, bytes] = field(default_factory=dict)
    artifact_manifest_entries: list[dict[str, str]] = field(default_factory=list)


def _agent_name_for_trial(trial: Trial) -> str:
    config = trial.config if isinstance(trial.config, dict) else {}
    raw = config.get("agent_name")
    return str(raw) if raw else "unknown"


def validate_hermes_eligibility(
    events: list[TrajectoryEvent],
    trial: Trial,
) -> None:
    agent_name = _agent_name_for_trial(trial)
    if agent_name != "hermes":
        raise HermesExportError(
            "incompatible_agent",
            {
                "message": "hermes-export requires hermes trials",
                "trial_id": str(trial.id),
                "agent_name": agent_name,
            },
        )

    provenance_events = [
        event
        for event in events
        if event.kind == EventKind.HERMES_RUNTIME_PROVENANCE
    ]
    if not provenance_events:
        raise HermesExportError(
            "missing_provenance",
            {
                "message": "trajectory is missing hermes_runtime_provenance",
                "trial_id": str(trial.id),
            },
        )

    artifact_refs = [
        event
        for event in events
        if isinstance(event, HermesArtifactRefEvent)
        and event.artifact_kind == "hermes.session"
    ]
    if not artifact_refs:
        raise HermesExportError(
            "missing_native_artifact",
            {
                "message": "trajectory is missing hermes_artifact_ref for hermes.session",
                "trial_id": str(trial.id),
            },
        )


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
        if isinstance(event, HermesArtifactRefEvent)
        and event.artifact_kind == "hermes.session"
    ]
    indexed = _index_artifacts(trial)
    resolved: dict[str, bytes] = {}

    for ref in refs:
        candidates = [
            item
            for item in indexed
            if isinstance(item.get("key"), str)
            and str(item["key"]).endswith(SANDBOX_HERMES_SESSION)
        ]
        if not candidates:
            raise HermesExportError(
                "missing_native_artifact",
                {
                    "message": (
                        "hermes_artifact_ref is present but no matching "
                        "trajectory_index artifact was found"
                    ),
                    "trial_id": str(trial.id),
                    "artifact_kind": ref.artifact_kind,
                    "sandbox_path": ref.sandbox_path,
                },
            )
        indexed_item = candidates[0]
        data = _fetch_artifact_bytes(
            client,
            bucket=_resolve_indexed_artifact_bucket(
                indexed_item,
                default_bucket=artifacts_bucket,
            ),
            key=str(indexed_item["key"]),
            missing_message="native Hermes session artifact is missing from object storage",
        )
        actual_hash = hashlib.sha256(data).hexdigest()
        expected_hash = _normalize_hash(ref.content_hash)
        if actual_hash != expected_hash:
            raise HermesExportError(
                "missing_native_artifact",
                {
                    "message": "native Hermes session artifact hash does not match artifact ref",
                    "trial_id": str(trial.id),
                    "artifact_kind": ref.artifact_kind,
                    "expected_hash": expected_hash,
                    "actual_hash": actual_hash,
                },
            )
        resolved[NATIVE_HERMES_SESSION] = data

    if NATIVE_HERMES_SESSION not in resolved:
        raise HermesExportError(
            "missing_native_artifact",
            {
                "message": "no hermes.session artifact ref was resolved",
                "trial_id": str(trial.id),
            },
        )
    return resolved


def build_export_provenance(
    events: list[TrajectoryEvent],
    *,
    trial_id: str,
    native_hash: str,
    session: dict[str, Any] | None = None,
) -> dict[str, Any]:
    provenance = next(
        event
        for event in events
        if isinstance(event, HermesRuntimeProvenanceEvent)
    )
    payload: dict[str, Any] = {
        "schema_version": "1",
        "trial_id": trial_id,
        "bridge_revision": LOOM_BRIDGE_REVISION,
        "hermes_version": provenance.hermes_version,
        "hermes_agent_ref": provenance.hermes_agent_ref,
        "loom_bridge_revision": provenance.loom_bridge_revision,
        "enabled_toolsets": list(provenance.enabled_toolsets),
        "native_session_hash": native_hash,
    }
    if isinstance(session, dict):
        for key in (
            "session_id",
            "model",
            "provider",
            "turn_exit_reason",
            "final_response",
        ):
            if key in session:
                payload[key] = session[key]
    return payload


def build_per_trial_hermes_bundle(
    *,
    trial: Trial,
    events: list[TrajectoryEvent],
    calls: list[LlmCall],
    client: Any,
    artifacts_bucket: str,
    messages_from_raw_log: Any,
) -> HermesTrialBundle:
    validate_hermes_eligibility(events, trial)
    native_artifacts = resolve_native_artifacts(
        trial,
        events,
        client=client,
        artifacts_bucket=artifacts_bucket,
    )
    native_bytes = native_artifacts[NATIVE_HERMES_SESSION]
    native_hash = hashlib.sha256(native_bytes).hexdigest()
    execution_trajectory = HermesTrajectoryMapper.project_trajectory(native_bytes)
    model_input_trajectory = build_model_input_trajectory(
        trial_id=str(trial.id),
        task_id=trial.task_id,
        calls=calls,
        messages_from_raw_log=messages_from_raw_log,
    )
    session_header = execution_trajectory.get("session")
    export_provenance = build_export_provenance(
        events,
        trial_id=str(trial.id),
        native_hash=native_hash,
        session=session_header if isinstance(session_header, dict) else None,
    )
    artifact_manifest_entries = [
        {"kind": "execution_trajectory", "path": "trajectory.json"},
        {"kind": "model_input_trajectory", "path": "model_input_trajectory.json"},
        {"kind": "audit_spine", "path": "loom_trajectory.jsonl"},
        {"kind": "export_provenance", "path": "export_provenance.json"},
        {"kind": "native_hermes_session", "path": NATIVE_HERMES_SESSION},
    ]
    scan_payload = {
        "trajectory.json": json.dumps(execution_trajectory, ensure_ascii=False).encode(),
        "model_input_trajectory.json": json.dumps(
            model_input_trajectory,
            ensure_ascii=False,
        ).encode(),
        "export_provenance.json": json.dumps(export_provenance, ensure_ascii=False).encode(),
        **native_artifacts,
    }
    scan_members_for_secrets(scan_payload)
    return HermesTrialBundle(
        execution_trajectory=execution_trajectory,
        model_input_trajectory=model_input_trajectory,
        export_provenance=export_provenance,
        native_artifacts=native_artifacts,
        artifact_manifest_entries=artifact_manifest_entries,
    )


__all__ = [
    "NATIVE_HERMES_SESSION",
    "HermesExportError",
    "HermesTrialBundle",
    "build_per_trial_hermes_bundle",
    "parse_trajectory_events",
    "resolve_native_artifacts",
    "validate_hermes_eligibility",
]
