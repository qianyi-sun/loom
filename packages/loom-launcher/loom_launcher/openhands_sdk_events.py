"""Map OpenHands SDK conversation events to Loom trajectory envelopes."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from collections.abc import Sequence
from typing import Any

MAX_OBSERVATION_CHARS = 65536


def _truncate(text: str) -> tuple[str, bool]:
    if len(text) <= MAX_OBSERVATION_CHARS:
        return text, False
    return text[:MAX_OBSERVATION_CHARS], True


def _text_from_content(raw: object) -> str:
    if raw is None:
        return ""
    if isinstance(raw, str):
        return raw
    if isinstance(raw, list):
        parts: list[str] = []
        for item in raw:
            text = getattr(item, "text", None)
            if isinstance(text, str) and text:
                parts.append(text)
            elif item is not None:
                parts.append(str(item))
        return "\n".join(parts)
    return str(raw)


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _text_from_thought(thought: Sequence[object]) -> str:
    parts: list[str] = []
    for item in thought:
        text = getattr(item, "text", None)
        if isinstance(text, str) and text:
            parts.append(text)
    return "\n".join(parts)


def _parse_tool_arguments(raw: object) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {"raw": raw}
        if isinstance(parsed, dict):
            return parsed
        return {"raw": raw}
    return {}


def _action_arguments(event: object) -> dict[str, Any]:
    tool_call = getattr(event, "tool_call", None)
    if tool_call is not None:
        function = getattr(tool_call, "function", None)
        if function is not None:
            raw_args = getattr(function, "arguments", None)
            if raw_args is not None:
                args = _parse_tool_arguments(raw_args)
                if args:
                    return args

    action = getattr(event, "action", None)
    if action is not None and hasattr(action, "model_dump"):
        action_dict = action.model_dump(mode="json")
        if isinstance(action_dict, dict):
            return {
                key: value
                for key, value in action_dict.items()
                if key != "kind" and value is not None
            }
    return {}


def _action_metadata(event: object) -> dict[str, str | None]:
    return {
        "reasoning_content": _optional_str(getattr(event, "reasoning_content", None)),
        "thought": _optional_str(_text_from_thought(getattr(event, "thought", []) or [])),
        "summary": _optional_str(getattr(event, "summary", None)),
    }


def _think_tool_reasoning(tool_name: str, args: dict[str, Any], reasoning_content: str | None) -> str | None:
    if reasoning_content:
        return reasoning_content
    if tool_name == "think":
        return _optional_str(args.get("thought"))
    return None


class OpenHandsEventMapper:
    """Convert SDK callback events into complete Loom JSONL envelopes."""

    def __init__(self) -> None:
        self._seq = 0
        self._trial_id = os.environ.get("LOOM_TRIAL_ID", "")
        self._step_id = os.environ.get("LOOM_STEP_ID", "main")
        self._pending: dict[str, dict[str, Any]] = {}

    def _envelope(self, kind: str, **fields: object) -> dict[str, object]:
        payload: dict[str, object] = {
            "kind": kind,
            "emitted_at": datetime.now(UTC).isoformat(),
            "trial_id": self._trial_id,
            "step_id": self._step_id,
            "seq": self._seq,
            **fields,
        }
        self._seq += 1
        return payload

    def map_status(self, message: str) -> dict[str, object]:
        return self._envelope("agent_thought", content=f"status: {message}", tokens=None)

    def map_result(self, *, ok: bool) -> dict[str, object]:
        return self._envelope(
            "agent_thought",
            content=f"result: {'ok' if ok else 'failed'}",
            tokens=None,
        )

    def map_event(self, event: object) -> list[dict[str, object]]:
        event_type = type(event).__name__

        if event_type == "MessageEvent":
            return self._map_message_event(event)
        if event_type == "ActionEvent":
            return self._map_action_event(event)
        if event_type == "ObservationEvent":
            return self._map_observation_event(event)

        if hasattr(event, "model_dump"):
            payload = event.model_dump(mode="json")
            content = json.dumps({"event_type": event_type, "event": payload}, sort_keys=True)
        else:
            content = json.dumps({"event_type": event_type, "event": repr(event)}, sort_keys=True)
        return [self._envelope("agent_thought", content=content, tokens=None)]

    def flush_pending(self) -> list[dict[str, object]]:
        out: list[dict[str, object]] = []
        for tool_call_id, pending in list(self._pending.items()):
            tool_name = str(pending["tool_name"])
            args = dict(pending["args"])
            reasoning_content = _think_tool_reasoning(
                tool_name,
                args,
                _optional_str(pending.get("reasoning_content")),
            )
            out.append(
                self._tool_use_envelope(
                    tool_name=tool_name,
                    args=args,
                    result=None,
                    tool_call_id=tool_call_id,
                    reasoning_content=reasoning_content,
                    thought=_optional_str(pending.get("thought")),
                    summary=_optional_str(pending.get("summary")),
                )
            )
        self._pending.clear()
        return out

    def _map_message_event(self, event: object) -> list[dict[str, object]]:
        source = getattr(event, "source", "")
        llm_message = getattr(event, "llm_message", None)
        content = _text_from_content(getattr(llm_message, "content", None) if llm_message else None)
        reasoning_content = _optional_str(
            getattr(llm_message, "reasoning_content", None) if llm_message else None,
        )
        out: list[dict[str, object]] = []
        if content.strip():
            if source == "user":
                content = f"user: {content}"
            out.append(self._envelope("agent_thought", content=content, tokens=None))
        if reasoning_content:
            out.append(
                self._envelope(
                    "agent_thought",
                    content=reasoning_content,
                    reasoning_content=reasoning_content,
                    sdk_event_type="MessageEvent",
                    tokens=None,
                )
            )
        return out

    def _map_action_event(self, event: object) -> list[dict[str, object]]:
        tool_call_id = str(getattr(event, "tool_call_id", "") or "")
        tool_name = str(getattr(event, "tool_name", "") or "unknown")
        args = _action_arguments(event)
        metadata = _action_metadata(event)
        reasoning_content = metadata["reasoning_content"]
        thought = metadata["thought"]
        summary = metadata["summary"]

        out: list[dict[str, object]] = []
        primary = reasoning_content or thought
        if primary:
            thought_fields: dict[str, object] = {
                "content": primary,
                "tokens": None,
                "sdk_event_type": "ActionEvent",
            }
            if reasoning_content:
                thought_fields["reasoning_content"] = reasoning_content
            if thought:
                thought_fields["thought"] = thought
            if summary:
                thought_fields["summary"] = summary
            if tool_call_id:
                thought_fields["tool_call_id"] = tool_call_id
            out.append(self._envelope("agent_thought", **thought_fields))

        pending = {"tool_name": tool_name, "args": args, **metadata}
        if tool_call_id:
            self._pending[tool_call_id] = pending
            return out
        return out + [
            self._tool_use_envelope(
                tool_name=tool_name,
                args=args,
                result=None,
                tool_call_id=None,
                reasoning_content=_think_tool_reasoning(tool_name, args, reasoning_content),
                thought=thought,
                summary=summary,
            )
        ]

    def _map_observation_event(self, event: object) -> list[dict[str, object]]:
        tool_call_id = str(getattr(event, "tool_call_id", "") or "")
        tool_name = str(getattr(event, "tool_name", "") or "unknown")
        observation = getattr(event, "observation", None)
        content = _text_from_content(getattr(observation, "content", None) if observation else None)
        if not content and observation is not None:
            content = str(observation)
        content, truncated = _truncate(content)

        pending = self._pending.pop(tool_call_id, None) if tool_call_id else None
        args = dict(pending["args"]) if pending else {}
        reasoning_content: str | None = None
        thought: str | None = None
        summary: str | None = None
        if pending:
            tool_name = str(pending["tool_name"])
            reasoning_content = _optional_str(pending.get("reasoning_content"))
            thought = _optional_str(pending.get("thought"))
            summary = _optional_str(pending.get("summary"))

        result: dict[str, Any] = {"content": content}
        if tool_call_id:
            result["tool_call_id"] = tool_call_id
        if truncated:
            result["truncated"] = True

        return [
            self._tool_use_envelope(
                tool_name=tool_name,
                args=args,
                result=result,
                tool_call_id=tool_call_id or None,
                reasoning_content=_think_tool_reasoning(tool_name, args, reasoning_content),
                thought=thought,
                summary=summary,
            )
        ]

    def _tool_use_envelope(
        self,
        *,
        tool_name: str,
        args: dict[str, Any],
        result: dict[str, Any] | None,
        tool_call_id: str | None,
        reasoning_content: str | None = None,
        thought: str | None = None,
        summary: str | None = None,
    ) -> dict[str, object]:
        if tool_call_id and result is not None and "tool_call_id" not in result:
            result = {**result, "tool_call_id": tool_call_id}
        fields: dict[str, object] = {
            "tool_name": tool_name,
            "args": args,
            "result": result,
            "error": None,
            "duration_sec": 0.0,
        }
        if reasoning_content:
            fields["reasoning_content"] = reasoning_content
        if thought:
            fields["thought"] = thought
        if summary:
            fields["summary"] = summary
        return self._envelope("tool_use", **fields)
