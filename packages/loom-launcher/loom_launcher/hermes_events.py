"""Map Hermes ``run_conversation`` messages into Loom JSONL envelopes."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
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


def _parse_tool_args(raw: object) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {"raw": raw}
        if isinstance(parsed, dict):
            return parsed
        return {"value": parsed}
    return {"value": raw}


class HermesEventMapper:
    """Convert Hermes chat messages into complete Loom JSONL envelopes."""

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

    def map_messages(self, messages: list[dict[str, Any]] | list[Any]) -> list[dict[str, object]]:
        out: list[dict[str, object]] = []
        for message in messages:
            out.extend(self.map_message(message))
        out.extend(self.flush_pending())
        return out

    def map_message(self, message: object) -> list[dict[str, object]]:
        if hasattr(message, "model_dump"):
            payload = message.model_dump(mode="json")  # type: ignore[union-attr]
            if not isinstance(payload, dict):
                payload = {"value": payload}
        elif isinstance(message, dict):
            payload = dict(message)
        else:
            return [
                self._envelope(
                    "agent_thought",
                    content=json.dumps({"message": repr(message)}, sort_keys=True),
                    tokens=None,
                )
            ]

        role = str(payload.get("role") or "")
        if role == "assistant":
            return self._map_assistant(payload)
        if role == "tool":
            return self._map_tool_result(payload)
        if role == "user":
            content = _content_text(payload.get("content"))
            if content.strip():
                return [
                    self._envelope(
                        "agent_thought",
                        content=f"user: {content}",
                        tokens=None,
                    )
                ]
            return []
        # system / unknown — keep a compact thought for debugging
        content = _content_text(payload.get("content"))
        if content.strip():
            return [
                self._envelope(
                    "agent_thought",
                    content=f"{role or 'message'}: {content}",
                    tokens=None,
                )
            ]
        return []

    def flush_pending(self) -> list[dict[str, object]]:
        out: list[dict[str, object]] = []
        for tool_call_id, pending in list(self._pending.items()):
            out.append(
                self._tool_use_envelope(
                    tool_name=str(pending["tool_name"]),
                    args=dict(pending["args"]),
                    result=None,
                    tool_call_id=tool_call_id,
                    reasoning_content=_optional_str(pending.get("reasoning_content")),
                )
            )
        self._pending.clear()
        return out

    def _map_assistant(self, payload: dict[str, Any]) -> list[dict[str, object]]:
        out: list[dict[str, object]] = []
        content = _content_text(payload.get("content"))
        reasoning = _optional_str(
            payload.get("reasoning_content") or payload.get("reasoning")
        )
        if content.strip():
            fields: dict[str, object] = {
                "content": content,
                "tokens": None,
                "sdk_event_type": "hermes.assistant",
            }
            if reasoning:
                fields["reasoning_content"] = reasoning
            out.append(self._envelope("agent_thought", **fields))
        elif reasoning:
            out.append(
                self._envelope(
                    "agent_thought",
                    content=reasoning,
                    reasoning_content=reasoning,
                    sdk_event_type="hermes.assistant",
                    tokens=None,
                )
            )

        tool_calls = payload.get("tool_calls") or []
        if isinstance(tool_calls, list):
            for call in tool_calls:
                if not isinstance(call, dict):
                    continue
                tool_call_id = str(call.get("id") or "")
                function = call.get("function") if isinstance(call.get("function"), dict) else {}
                tool_name = str(
                    (function or {}).get("name")
                    or call.get("name")
                    or "unknown"
                )
                args = _parse_tool_args(
                    (function or {}).get("arguments")
                    if function
                    else call.get("arguments")
                )
                pending = {
                    "tool_name": tool_name,
                    "args": args,
                    "reasoning_content": reasoning,
                }
                if tool_call_id:
                    self._pending[tool_call_id] = pending
                else:
                    out.append(
                        self._tool_use_envelope(
                            tool_name=tool_name,
                            args=args,
                            result=None,
                            tool_call_id=None,
                            reasoning_content=reasoning,
                        )
                    )
        return out

    def _map_tool_result(self, payload: dict[str, Any]) -> list[dict[str, object]]:
        tool_call_id = str(payload.get("tool_call_id") or "")
        content = _content_text(payload.get("content"))
        pending = self._pending.pop(tool_call_id, None) if tool_call_id else None
        tool_name = str(
            (pending or {}).get("tool_name")
            or payload.get("name")
            or "unknown"
        )
        args = dict((pending or {}).get("args") or {})
        reasoning = _optional_str((pending or {}).get("reasoning_content"))
        result: dict[str, Any] = {"content": content}
        if tool_call_id:
            result["tool_call_id"] = tool_call_id
        return [
            self._tool_use_envelope(
                tool_name=tool_name,
                args=args,
                result=result,
                tool_call_id=tool_call_id or None,
                reasoning_content=reasoning,
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
        return self._envelope("tool_use", **fields)
