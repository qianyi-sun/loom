from __future__ import annotations

import json
from pathlib import Path

from loom_launcher.openhands_sdk_capture import (
    SANDBOX_OPENHANDS_SDK_EVENTS,
    build_artifact_ref_payload,
    build_runtime_provenance_payload,
    serialize_sdk_events,
    write_native_events_file,
)


class _FakeEvent:
    def __init__(self, message: str) -> None:
        self.message = message

    def model_dump(self, *, mode: str = "python") -> dict[str, str]:
        assert mode == "json"
        return {"message": self.message}


def test_serialize_sdk_events_adds_event_type() -> None:
    events = [_FakeEvent("hello")]
    serialized = serialize_sdk_events(events)
    assert serialized == [{"message": "hello", "event_type": "_FakeEvent"}]


def test_write_native_events_file_writes_under_loom_agent(tmp_path: Path) -> None:
    events = [_FakeEvent("capture me")]
    path, content_hash, size_bytes = write_native_events_file(tmp_path, events)
    assert path == tmp_path / ".loom" / "agent" / "openhands_sdk_events.json"
    assert path.exists()
    data = path.read_bytes()
    assert size_bytes == len(data)
    assert len(content_hash) == 64
    parsed = json.loads(data.decode())
    assert parsed[0]["event_type"] == "_FakeEvent"


def test_capture_envelope_shapes() -> None:
    seq = {"value": 0}

    def envelope(kind: str, **fields: object) -> dict[str, object]:
        seq["value"] += 1
        return {"kind": kind, "seq": seq["value"], **fields}

    provenance = build_runtime_provenance_payload(
        envelope=envelope,
        sdk_version="1.34.0",
        openhands_tools_version="1.34.0",
        loom_bridge_revision="1.0",
    )
    assert provenance["kind"] == "openhands_sdk_runtime_provenance"
    assert provenance["sdk_version"] == "1.34.0"

    artifact_ref = build_artifact_ref_payload(
        envelope=envelope,
        sandbox_path=SANDBOX_OPENHANDS_SDK_EVENTS,
        content_hash="abc123",
        size_bytes=42,
    )
    assert artifact_ref["kind"] == "openhands_sdk_artifact_ref"
    assert artifact_ref["artifact_kind"] == "openhands_sdk.events"
    assert artifact_ref["sandbox_path"] == SANDBOX_OPENHANDS_SDK_EVENTS
