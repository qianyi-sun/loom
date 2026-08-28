"""One-shot OpenHands SDK runner for the `openhands-sdk` adapter.

The upstream `openhands-sdk` package exposes the import package
`openhands.sdk`; it does not ship a stable `python -m ...` runner. Loom owns
this tiny runner so the adapter has a durable sandbox contract.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
from pathlib import Path
from typing import Any

from loom_launcher.openhands_sdk_capture import (
    LOOM_BRIDGE_REVISION,
    SANDBOX_OPENHANDS_SDK_EVENTS,
    build_artifact_ref_payload,
    build_runtime_provenance_payload,
    resolve_package_version,
    write_native_events_file,
)
from loom_launcher.openhands_sdk_prompt import build_terminus_style_agent_kwargs
from loom_launcher.openhands_sdk_events import OpenHandsEventMapper


def _load_sdk_types() -> tuple[type[Any], type[Any], type[Any], type[Any]]:
    os.environ.setdefault("OPENHANDS_SUPPRESS_BANNER", "1")
    try:
        sdk = importlib.import_module("openhands.sdk")
    except ImportError as exc:  # pragma: no cover - exercised via main()
        raise RuntimeError(
            "openhands-sdk is required for the openhands-sdk adapter; "
            "install the agent sandbox runtime dependencies"
        ) from exc

    return sdk.LLM, sdk.Agent, sdk.Conversation, sdk.Tool


def _load_default_tools(tool_type: type[Any]) -> list[Any]:
    specs = (
        ("openhands.tools.terminal", "TerminalTool"),
        ("openhands.tools.file_editor", "FileEditorTool"),
        ("openhands.tools.task_tracker", "TaskTrackerTool"),
    )
    try:
        tool_classes = [
            getattr(importlib.import_module(module), attr)
            for module, attr in specs
        ]
    except ImportError as exc:  # pragma: no cover - exercised via main()
        raise RuntimeError(
            "openhands-tools is required for the openhands-sdk adapter; "
            "install openhands-tools at the same version as openhands-sdk"
        ) from exc

    return [tool_type(name=tool_class.name) for tool_class in tool_classes]


def _json_default(value: object) -> object:
    if isinstance(value, Path):
        return str(value)
    return repr(value)


def _emit(payload: dict[str, object]) -> None:
    print(json.dumps(payload, default=_json_default), flush=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one task with OpenHands SDK")
    parser.add_argument("--model", required=True)
    parser.add_argument("--workdir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--task", required=True)
    parser.add_argument("--max-iterations", type=int, default=500)
    parser.add_argument(
        "--terminus-style",
        action="store_true",
        help="Enable Terminus-style Analysis/Plan preamble and disable ThinkTool",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.output != "jsonl":
        print("only --output jsonl is supported", file=sys.stderr)
        return 2

    api_key = os.environ.get("LLM_API_KEY")
    if not api_key:
        print("LLM_API_KEY is required", file=sys.stderr)
        return 2

    base_url = os.environ.get("LLM_BASE_URL") or None
    try:
        llm_type, agent_type, conversation_type, tool_type = _load_sdk_types()
        tools = _load_default_tools(tool_type)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    mapper = OpenHandsEventMapper()

    def _on_event(event: object) -> None:
        for payload in mapper.map_event(event):
            _emit(payload)

    _emit(mapper.map_status("openhands-sdk runner started"))
    if args.terminus_style:
        _emit(mapper.map_status("terminus-style enabled"))

    llm = llm_type(model=args.model, api_key=api_key, base_url=base_url)
    agent_kwargs: dict[str, Any] = {}
    if args.terminus_style:
        agent_kwargs = build_terminus_style_agent_kwargs()
    agent = agent_type(llm=llm, tools=tools, **agent_kwargs)
    conversation = conversation_type(
        agent=agent,
        callbacks=[_on_event],
        workspace=Path(args.workdir),
        max_iteration_per_run=args.max_iterations,
        visualizer=None,
    )
    conversation.send_message(args.task)
    conversation.run()

    for payload in mapper.flush_pending():
        _emit(payload)

    sdk_version = resolve_package_version("openhands.sdk")
    tools_version = resolve_package_version("openhands.tools.terminal")
    _emit(
        build_runtime_provenance_payload(
            envelope=mapper._envelope,
            sdk_version=sdk_version,
            openhands_tools_version=tools_version,
            loom_bridge_revision=LOOM_BRIDGE_REVISION,
            terminus_style=args.terminus_style,
        )
    )
    native_events = getattr(getattr(conversation, "state", None), "events", None) or []
    _write_path, content_hash, size_bytes = write_native_events_file(
        Path(args.workdir),
        native_events,
    )
    _emit(
        build_artifact_ref_payload(
            envelope=mapper._envelope,
            sandbox_path=SANDBOX_OPENHANDS_SDK_EVENTS,
            content_hash=content_hash,
            size_bytes=size_bytes,
        )
    )

    _emit(mapper.map_result(ok=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
