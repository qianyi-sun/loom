"""HermesTrajectoryMapper — export-time projection from native session (#hermes-export)."""

from __future__ import annotations

import json
from typing import Any


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _content_text(content: object) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                text = item.get("text")
                if text:
                    parts.append(str(text))
                else:
                    parts.append(json.dumps(item, sort_keys=True, default=str))
            else:
                parts.append(str(item))
        return "".join(parts)
    return str(content)


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


_SESSION_HEADER_KEYS = (
    "session_id",
    "model",
    "provider",
    "base_url",
    "final_response",
    "turn_exit_reason",
    "completed",
    "failed",
    "partial",
    "interrupted",
    "api_calls",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "reasoning_tokens",
    "prompt_tokens",
    "completion_tokens",
    "total_tokens",
    "last_prompt_tokens",
    "estimated_cost_usd",
    "cost_status",
    "cost_source",
    "last_reasoning",
)


class HermesTrajectoryMapper:
    """Project native Hermes session messages into delivery ``trajectory.json``."""

    @staticmethod
    def project_trajectory(native_bytes: bytes) -> dict[str, Any]:
        parsed = json.loads(native_bytes.decode("utf-8"))
        if not isinstance(parsed, dict):
            raise ValueError("native Hermes session must be a JSON object")

        messages = parsed.get("messages")
        if messages is None:
            messages = []
        if not isinstance(messages, list):
            raise ValueError("native Hermes session.messages must be a JSON array")

        session_header: dict[str, Any] = {}
        for key in _SESSION_HEADER_KEYS:
            if key in parsed:
                session_header[key] = parsed[key]

        projected_events: list[dict[str, Any]] = []
        index = 0
        for raw_message in messages:
            if not isinstance(raw_message, dict):
                continue
            role = str(raw_message.get("role") or "")
            timestamp = raw_message.get("timestamp")

            if role == "system":
                content = _content_text(raw_message.get("content"))
                if content.strip():
                    projected_events.append(
                        {
                            "index": index,
                            "kind": "system_prompt",
                            "content": content,
                            "timestamp": timestamp,
                        }
                    )
                    index += 1
                continue

            if role == "user":
                content = _content_text(raw_message.get("content"))
                if content.strip():
                    projected_events.append(
                        {
                            "index": index,
                            "kind": "user_prompt",
                            "content": content,
                            "timestamp": timestamp,
                        }
                    )
                    index += 1
                continue

            if role == "assistant":
                content = _content_text(raw_message.get("content"))
                reasoning = _optional_str(
                    raw_message.get("reasoning")
                    or raw_message.get("reasoning_content")
                )
                tool_calls = raw_message.get("tool_calls")
                tool_call_ids: list[str] = []
                if isinstance(tool_calls, list):
                    for call in tool_calls:
                        if not isinstance(call, dict):
                            continue
                        call_id = _optional_str(call.get("id") or call.get("call_id"))
                        if call_id:
                            tool_call_ids.append(call_id)

                if content.strip() or reasoning or tool_call_ids:
                    assistant_event: dict[str, Any] = {
                        "index": index,
                        "kind": "assistant",
                        "content": content or None,
                        "finish_reason": raw_message.get("finish_reason"),
                        "timestamp": timestamp,
                        "tool_call_ids": tool_call_ids or None,
                    }
                    if reasoning:
                        assistant_event["reasoning"] = reasoning
                    projected_events.append(assistant_event)
                    index += 1

                if reasoning:
                    projected_events.append(
                        {
                            "index": index,
                            "kind": "reasoning",
                            "content": reasoning,
                            "timestamp": timestamp,
                        }
                    )
                    index += 1

                if isinstance(tool_calls, list):
                    for call in tool_calls:
                        if not isinstance(call, dict):
                            continue
                        function = call.get("function")
                        function = function if isinstance(function, dict) else {}
                        tool_call_id = _optional_str(
                            call.get("id") or call.get("call_id")
                        )
                        tool_name = _optional_str(
                            function.get("name") or call.get("name")
                        )
                        projected: dict[str, Any] = {
                            "index": index,
                            "kind": "tool_call",
                            "tool_call_id": tool_call_id,
                            "tool_name": tool_name,
                            "arguments": _parse_tool_arguments(
                                function.get("arguments")
                            ),
                            "timestamp": timestamp,
                        }
                        call_id = _optional_str(call.get("call_id"))
                        if call_id and call_id != tool_call_id:
                            projected["call_id"] = call_id
                        response_item_id = _optional_str(call.get("response_item_id"))
                        if response_item_id:
                            projected["response_item_id"] = response_item_id
                        if reasoning:
                            projected["reasoning"] = reasoning
                        projected_events.append(projected)
                        index += 1
                continue

            if role == "tool":
                tool_call_id = _optional_str(
                    raw_message.get("tool_call_id") or raw_message.get("call_id")
                )
                tool_name = _optional_str(
                    raw_message.get("tool_name") or raw_message.get("name")
                )
                projected_events.append(
                    {
                        "index": index,
                        "kind": "observation",
                        "tool_call_id": tool_call_id,
                        "tool_name": tool_name,
                        "content": _content_text(raw_message.get("content")),
                        "timestamp": timestamp,
                    }
                )
                index += 1
                continue

            # Unknown roles — keep a compact debug event.
            content = _content_text(raw_message.get("content"))
            if content.strip() or role:
                projected_events.append(
                    {
                        "index": index,
                        "kind": "message",
                        "role": role or None,
                        "content": content or None,
                        "timestamp": timestamp,
                    }
                )
                index += 1

        return {
            "schema_version": "hermes-export-projection",
            "source_of_truth": "native/hermes_session.json",
            "session": session_header,
            "events": projected_events,
        }
