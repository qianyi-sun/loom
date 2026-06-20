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


def _load_sdk_types() -> tuple[type[Any], type[Any], type[Any]]:
    os.environ.setdefault("OPENHANDS_SUPPRESS_BANNER", "1")
    try:
        sdk = importlib.import_module("openhands.sdk")
    except ImportError as exc:  # pragma: no cover - exercised via main()
        raise RuntimeError(
            "openhands-sdk is required for the openhands-sdk adapter; "
            "install the agent sandbox runtime dependencies"
        ) from exc

    return sdk.LLM, sdk.Agent, sdk.Conversation


def _json_default(value: object) -> object:
    if isinstance(value, Path):
        return str(value)
    return repr(value)


def _event_payload(event: object) -> dict[str, object]:
    if hasattr(event, "model_dump"):
        payload = event.model_dump(mode="json")
    else:
        payload = repr(event)
    return {
        "kind": "openhands_sdk_event",
        "event_type": type(event).__name__,
        "event": payload,
    }


def _emit(payload: dict[str, object]) -> None:
    print(json.dumps(payload, default=_json_default), flush=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one task with OpenHands SDK")
    parser.add_argument("--model", required=True)
    parser.add_argument("--workdir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--task", required=True)
    parser.add_argument("--max-iterations", type=int, default=500)
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
        llm_type, agent_type, conversation_type = _load_sdk_types()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    _emit({"kind": "status", "message": "openhands-sdk runner started"})

    llm = llm_type(model=args.model, api_key=api_key, base_url=base_url)
    agent = agent_type(llm=llm)
    conversation = conversation_type(
        agent=agent,
        callbacks=[lambda event: _emit(_event_payload(event))],
        workspace=Path(args.workdir),
        max_iteration_per_run=args.max_iterations,
        visualizer=None,
    )
    conversation.send_message(args.task)
    conversation.run()
    _emit({"kind": "result", "ok": True})
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
