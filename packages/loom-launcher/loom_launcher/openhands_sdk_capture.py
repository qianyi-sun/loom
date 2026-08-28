"""Post-run OpenHands SDK event capture helpers (#1590)."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

SANDBOX_OPENHANDS_SDK_EVENTS = ".loom/agent/openhands_sdk_events.json"
NATIVE_OPENHANDS_SDK_EVENTS = "native/openhands_sdk_events.json"
LOOM_BRIDGE_REVISION = "1.0"


def serialize_sdk_events(events: Sequence[object]) -> list[dict[str, Any]]:
    """Dump SDK conversation events with stable ``event_type`` labels."""
    serialized: list[dict[str, Any]] = []
    for event in events:
        if hasattr(event, "model_dump"):
            payload = event.model_dump(mode="json")
        elif isinstance(event, dict):
            payload = dict(event)
        else:
            payload = {"repr": repr(event)}
        if not isinstance(payload, dict):
            payload = {"value": payload}
        payload = dict(payload)
        payload["event_type"] = type(event).__name__
        serialized.append(payload)
    return serialized


def write_native_events_file(
    workdir: Path,
    events: Sequence[object],
) -> tuple[Path, str, int]:
    """Write ``.loom/agent/openhands_sdk_events.json`` under *workdir*."""
    agent_dir = workdir / ".loom" / "agent"
    agent_dir.mkdir(parents=True, exist_ok=True)
    target = agent_dir / "openhands_sdk_events.json"
    data = json.dumps(serialize_sdk_events(events), ensure_ascii=False, indent=2).encode()
    target.write_bytes(data)
    content_hash = hashlib.sha256(data).hexdigest()
    return target, content_hash, len(data)


def resolve_package_version(module_name: str, *, fallback: str = "unknown") -> str:
    try:
        module = __import__(module_name)
    except ImportError:
        return fallback
    version = getattr(module, "__version__", None)
    if isinstance(version, str) and version.strip():
        return version.strip()
    return fallback


def build_runtime_provenance_payload(
    *,
    envelope: Callable[..., dict[str, object]],
    sdk_version: str,
    openhands_tools_version: str,
    loom_bridge_revision: str = LOOM_BRIDGE_REVISION,
    terminus_style: bool = False,
) -> dict[str, object]:
    return envelope(
        "openhands_sdk_runtime_provenance",
        sdk_version=sdk_version,
        openhands_tools_version=openhands_tools_version,
        loom_bridge_revision=loom_bridge_revision,
        terminus_style=terminus_style,
    )


def build_artifact_ref_payload(
    *,
    envelope: Callable[..., dict[str, object]],
    sandbox_path: str,
    content_hash: str,
    size_bytes: int,
    share_policy: str = "restricted",
) -> dict[str, object]:
    return envelope(
        "openhands_sdk_artifact_ref",
        artifact_kind="openhands_sdk.events",
        sandbox_path=sandbox_path,
        content_hash=content_hash,
        size_bytes=size_bytes,
        share_policy=share_policy,
    )
