"""OpenHandsSdkTrajectoryMapper — export-time projection from native SDK events (#1590)."""

from __future__ import annotations

import json
import re
from typing import Any


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _text_from_thought(raw: object) -> str | None:
    if not isinstance(raw, list):
        return None
    parts: list[str] = []
    for item in raw:
        if isinstance(item, dict):
            text = item.get("text")
        else:
            text = getattr(item, "text", None)
        if isinstance(text, str) and text:
            parts.append(text)
    joined = "\n".join(parts)
    return joined or None


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


def _parse_analysis_plan(text: str | None) -> tuple[str | None, str | None]:
    if not text:
        return None, None
    match = re.search(
        r"(?is)\banalysis\s*:\s*(.*?)(?=\bplan\s*:|$)",
        text,
    )
    analysis = match.group(1).strip() if match else None
    match = re.search(
        r"(?is)\bplan\s*:\s*(.*)$",
        text,
    )
    plan = match.group(1).strip() if match else None
    return analysis or None, plan or None


def _reasoning_from_action(event: dict[str, Any]) -> str | None:
    reasoning = _optional_str(event.get("reasoning_content"))
    if reasoning:
        return reasoning
    thought = _text_from_thought(event.get("thought"))
    if thought:
        return thought
    tool_name = _optional_str(event.get("tool_name"))
    if tool_name == "think":
        tool_call = event.get("tool_call")
        if isinstance(tool_call, dict):
            function = tool_call.get("function")
            if isinstance(function, dict):
                args = _parse_tool_arguments(function.get("arguments"))
                return _optional_str(args.get("thought"))
    return None


def _reasoning_from_message(event: dict[str, Any]) -> str | None:
    llm_message = event.get("llm_message")
    if not isinstance(llm_message, dict):
        return None
    return _optional_str(llm_message.get("reasoning_content"))


class OpenHandsSdkTrajectoryMapper:
    """Project native OpenHands SDK events into delivery ``trajectory.json``."""

    @staticmethod
    def project_trajectory(native_bytes: bytes) -> dict[str, Any]:
        parsed = json.loads(native_bytes.decode("utf-8"))
        if not isinstance(parsed, list):
            raise ValueError("native OpenHands SDK events must be a JSON array")

        projected_events: list[dict[str, Any]] = []
        for index, raw_event in enumerate(parsed):
            if not isinstance(raw_event, dict):
                continue
            event_type = str(raw_event.get("event_type") or raw_event.get("kind") or "UnknownEvent")
            projected: dict[str, Any] = {
                "index": index,
                "event_type": event_type,
            }

            if event_type == "ActionEvent":
                projected["tool_call_id"] = raw_event.get("tool_call_id")
                projected["tool_name"] = raw_event.get("tool_name")
                projected["reasoning_content"] = _reasoning_from_action(raw_event)
                projected["thought"] = _text_from_thought(raw_event.get("thought"))
                analysis, plan = _parse_analysis_plan(projected["thought"])
                if analysis is not None:
                    projected["analysis"] = analysis
                if plan is not None:
                    projected["plan"] = plan
                projected["summary"] = raw_event.get("summary")
                tool_call = raw_event.get("tool_call")
                if isinstance(tool_call, dict):
                    function = tool_call.get("function")
                    if isinstance(function, dict):
                        projected["tool_arguments"] = _parse_tool_arguments(
                            function.get("arguments"),
                        )
                action = raw_event.get("action")
                if isinstance(action, dict):
                    projected["action"] = action
            elif event_type == "ObservationEvent":
                projected["tool_call_id"] = raw_event.get("tool_call_id")
                projected["tool_name"] = raw_event.get("tool_name")
                projected["observation"] = raw_event.get("observation")
                if projected.get("tool_name") == "think":
                    pending = next(
                        (
                            item
                            for item in reversed(projected_events)
                            if item.get("event_type") == "ActionEvent"
                            and item.get("tool_call_id") == projected.get("tool_call_id")
                        ),
                        None,
                    )
                    if pending is not None:
                        projected["reasoning_content"] = pending.get("reasoning_content")
            elif event_type == "MessageEvent":
                projected["source"] = raw_event.get("source")
                llm_message = raw_event.get("llm_message")
                if isinstance(llm_message, dict):
                    projected["llm_message"] = llm_message
                    projected["reasoning_content"] = _reasoning_from_message(raw_event)
            else:
                projected["event"] = raw_event

            projected_events.append(projected)

        return {
            "schema_version": "openhands-export-projection",
            "source_of_truth": "native/openhands_sdk_events.json",
            "events": projected_events,
        }
