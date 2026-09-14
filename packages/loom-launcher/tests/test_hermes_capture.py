from __future__ import annotations

import json
from pathlib import Path

from loom_launcher.hermes_capture import (
    SANDBOX_HERMES_SESSION,
    build_artifact_ref_payload,
    build_runtime_provenance_payload,
    serialize_session_payload,
    write_native_session_file,
)


def test_serialize_session_payload_keeps_messages() -> None:
    result = {
        "final_response": "done",
        "session_id": "sess-1",
        "messages": [{"role": "assistant", "content": "hi"}],
    }
    serialized = serialize_session_payload(result)
    assert serialized["final_response"] == "done"
    assert serialized["session_id"] == "sess-1"
    assert serialized["messages"] == [{"role": "assistant", "content": "hi"}]


def test_write_native_session_file_writes_under_loom_agent(tmp_path: Path) -> None:
    result = {
        "final_response": "ok",
        "messages": [
            {"role": "assistant", "content": "hello", "reasoning_content": "think"}
        ],
    }
    path, content_hash, size_bytes = write_native_session_file(tmp_path, result)
    assert path == tmp_path / ".loom" / "agent" / "hermes_session.json"
    assert path.exists()
    data = path.read_bytes()
    assert size_bytes == len(data)
    assert len(content_hash) == 64
    parsed = json.loads(data.decode())
    assert parsed["messages"][0]["reasoning_content"] == "think"


def test_capture_envelope_shapes() -> None:
    seq = {"value": 0}

    def envelope(kind: str, **fields: object) -> dict[str, object]:
        seq["value"] += 1
        return {"kind": kind, "seq": seq["value"], **fields}

    provenance = build_runtime_provenance_payload(
        envelope=envelope,
        hermes_version="0.21.1",
        hermes_agent_ref="abc123",
        loom_bridge_revision="1.0",
        enabled_toolsets=("terminal", "file"),
    )
    assert provenance["kind"] == "hermes_runtime_provenance"
    assert provenance["hermes_version"] == "0.21.1"
    assert provenance["enabled_toolsets"] == ["terminal", "file"]

    artifact_ref = build_artifact_ref_payload(
        envelope=envelope,
        sandbox_path=SANDBOX_HERMES_SESSION,
        content_hash="abc123",
        size_bytes=42,
    )
    assert artifact_ref["kind"] == "hermes_artifact_ref"
    assert artifact_ref["artifact_kind"] == "hermes.session"
    assert artifact_ref["sandbox_path"] == SANDBOX_HERMES_SESSION
