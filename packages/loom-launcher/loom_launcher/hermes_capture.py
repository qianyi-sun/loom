"""Post-run Hermes session capture helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

SANDBOX_HERMES_SESSION = ".loom/agent/hermes_session.json"
NATIVE_HERMES_SESSION = "native/hermes_session.json"
LOOM_BRIDGE_REVISION = "1.0"


def serialize_session_payload(
    result: Mapping[str, Any] | None,
    *,
    messages: Sequence[object] | None = None,
) -> dict[str, Any]:
    """Build a JSON-serializable native session dump from ``run_conversation``."""
    payload: dict[str, Any] = {}
    if result is not None:
        for key, value in result.items():
            if key == "messages":
                continue
            payload[key] = value
    msg_list = list(messages) if messages is not None else list((result or {}).get("messages") or [])
    payload["messages"] = [_coerce_message(m) for m in msg_list]
    return payload


def _coerce_message(message: object) -> dict[str, Any]:
    if hasattr(message, "model_dump"):
        dumped = message.model_dump(mode="json")  # type: ignore[union-attr]
        if isinstance(dumped, dict):
            return dumped
    if isinstance(message, dict):
        return dict(message)
    return {"repr": repr(message)}


def write_native_session_file(
    workdir: Path,
    result: Mapping[str, Any] | None,
    *,
    messages: Sequence[object] | None = None,
) -> tuple[Path, str, int]:
    """Write ``.loom/agent/hermes_session.json`` under *workdir*."""
    agent_dir = workdir / ".loom" / "agent"
    agent_dir.mkdir(parents=True, exist_ok=True)
    target = agent_dir / "hermes_session.json"
    data = json.dumps(
        serialize_session_payload(result, messages=messages),
        ensure_ascii=False,
        indent=2,
        default=str,
    ).encode()
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
    try:
        from importlib.metadata import version as pkg_version

        return pkg_version("hermes-agent")
    except Exception:
        return fallback


def build_runtime_provenance_payload(
    *,
    envelope: Callable[..., dict[str, object]],
    hermes_version: str,
    hermes_agent_ref: str,
    loom_bridge_revision: str = LOOM_BRIDGE_REVISION,
    enabled_toolsets: Sequence[str] | None = None,
) -> dict[str, object]:
    return envelope(
        "hermes_runtime_provenance",
        hermes_version=hermes_version,
        hermes_agent_ref=hermes_agent_ref,
        loom_bridge_revision=loom_bridge_revision,
        enabled_toolsets=list(enabled_toolsets or ("terminal", "file")),
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
        "hermes_artifact_ref",
        artifact_kind="hermes.session",
        sandbox_path=sandbox_path,
        content_hash=content_hash,
        size_bytes=size_bytes,
        share_policy=share_policy,
    )
