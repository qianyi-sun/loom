"""One-shot Hermes AIAgent runner for the ``hermes`` launcher adapter.

Constructs ``AIAgent`` with ``provider=custom``, Gateway auth via
``OPENAI_API_KEY`` / ``OPENAI_BASE_URL``, and ``enabled_toolsets`` limited to
``terminal`` + ``file``. Emits Loom JSONL on stdout and writes a native
session artifact under ``.loom/agent/``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from loom_launcher.adapters._hermes_runtime import HERMES_AGENT_REF
from loom_launcher.hermes_capture import (
    LOOM_BRIDGE_REVISION,
    SANDBOX_HERMES_SESSION,
    build_artifact_ref_payload,
    build_runtime_provenance_payload,
    resolve_package_version,
    write_native_session_file,
)
from loom_launcher.hermes_events import HermesEventMapper

ENABLED_TOOLSETS = ("terminal", "file")
DEFAULT_MAX_ITERATIONS = 90


def _load_ai_agent() -> type[Any]:
    try:
        from run_agent import AIAgent
    except ImportError as exc:  # pragma: no cover - exercised via main()
        raise RuntimeError(
            "hermes-agent is required for the hermes adapter; "
            "install the agent sandbox runtime (thin uv pip, not install.sh)"
        ) from exc
    return AIAgent


def _json_default(value: object) -> object:
    if isinstance(value, Path):
        return str(value)
    return repr(value)


def _emit(payload: dict[str, object]) -> None:
    print(json.dumps(payload, default=_json_default), flush=True)


def _prepare_env(*, workdir: Path) -> Path:
    """Neutralize Hermes footguns: YOLO, local terminal, ephemeral home."""
    os.environ.setdefault("HERMES_YOLO_MODE", "1")
    os.environ.setdefault("TERMINAL_ENV", "local")
    # Do not load user plugins / discovery cache from a shared home.
    hermes_home = Path(
        os.environ.get("HERMES_HOME")
        or (workdir / ".loom" / "hermes_home")
    )
    hermes_home.mkdir(parents=True, exist_ok=True)
    os.environ["HERMES_HOME"] = str(hermes_home)
    # Quiet discovery / avoid writing into the user profile.
    os.environ.setdefault("HERMES_QUIET", "1")
    return hermes_home


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one task with Hermes AIAgent")
    parser.add_argument("--model", required=True)
    parser.add_argument("--workdir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--task", required=True)
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=DEFAULT_MAX_ITERATIONS,
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.output != "jsonl":
        print("only --output jsonl is supported", file=sys.stderr)
        return 2

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("OPENAI_API_KEY is required", file=sys.stderr)
        return 2

    base_url = os.environ.get("OPENAI_BASE_URL")
    if not base_url:
        print("OPENAI_BASE_URL is required", file=sys.stderr)
        return 2

    workdir = Path(args.workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    _prepare_env(workdir=workdir)

    # Run with cwd = task workdir so terminal/file tools land in the sandbox root.
    prev_cwd = Path.cwd()
    try:
        os.chdir(workdir)
    except OSError as exc:
        print(f"cannot chdir to workdir: {exc}", file=sys.stderr)
        return 2

    mapper = HermesEventMapper()
    _emit(mapper.map_status("hermes runner started"))

    try:
        ai_agent_type = _load_ai_agent()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        os.chdir(prev_cwd)
        return 2

    agent = ai_agent_type(
        provider="custom",
        base_url=base_url,
        api_key=api_key,
        model=args.model,
        enabled_toolsets=list(ENABLED_TOOLSETS),
        quiet_mode=True,
        skip_memory=True,
        skip_context_files=True,
        max_iterations=args.max_iterations,
    )

    result: dict[str, Any]
    try:
        raw = agent.run_conversation(args.task)
        result = dict(raw) if isinstance(raw, dict) else {"final_response": raw, "messages": []}
        ok = not bool(result.get("failed") or result.get("error"))
    except Exception as exc:  # noqa: BLE001 — surface as failed trajectory
        print(f"hermes run_conversation failed: {exc}", file=sys.stderr)
        result = {
            "failed": True,
            "error": str(exc),
            "messages": list(getattr(agent, "messages", None) or []),
        }
        ok = False
    finally:
        os.chdir(prev_cwd)

    messages = list(result.get("messages") or [])
    for payload in mapper.map_messages(messages):
        _emit(payload)

    hermes_version = resolve_package_version("run_agent")
    _emit(
        build_runtime_provenance_payload(
            envelope=mapper._envelope,
            hermes_version=hermes_version,
            hermes_agent_ref=HERMES_AGENT_REF,
            loom_bridge_revision=LOOM_BRIDGE_REVISION,
            enabled_toolsets=ENABLED_TOOLSETS,
        )
    )
    _write_path, content_hash, size_bytes = write_native_session_file(
        workdir,
        result,
        messages=messages,
    )
    _emit(
        build_artifact_ref_payload(
            envelope=mapper._envelope,
            sandbox_path=SANDBOX_HERMES_SESSION,
            content_hash=content_hash,
            size_bytes=size_bytes,
        )
    )
    _emit(mapper.map_result(ok=ok))
    return 0 if ok else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
