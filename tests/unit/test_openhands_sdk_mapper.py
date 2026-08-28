from __future__ import annotations

import json

from loom.agent.openhands_sdk.mapper import OpenHandsSdkTrajectoryMapper


def test_mapper_preserves_action_reasoning_and_think_tool() -> None:
    native = [
        {
            "event_type": "ActionEvent",
            "tool_call_id": "call-1",
            "tool_name": "think",
            "reasoning_content": None,
            "thought": [],
            "tool_call": {
                "function": {
                    "arguments": '{"thought": "explicit think reasoning"}',
                }
            },
        },
        {
            "event_type": "ObservationEvent",
            "tool_call_id": "call-1",
            "tool_name": "think",
            "observation": {"content": [{"text": "ok"}]},
        },
        {
            "event_type": "ActionEvent",
            "tool_call_id": "call-2",
            "tool_name": "terminal",
            "reasoning_content": "model-native reasoning",
            "thought": [{"text": "sdk prose thought"}],
            "tool_call": {
                "function": {
                    "arguments": '{"command": "pwd"}',
                }
            },
        },
        {
            "event_type": "MessageEvent",
            "source": "agent",
            "llm_message": {
                "content": [{"text": "visible reply"}],
                "reasoning_content": "hidden reasoning",
            },
        },
    ]
    trajectory = OpenHandsSdkTrajectoryMapper.project_trajectory(
        json.dumps(native).encode(),
    )
    assert trajectory["schema_version"] == "openhands-export-projection"
    events = trajectory["events"]
    assert events[0]["reasoning_content"] == "explicit think reasoning"
    assert events[1]["reasoning_content"] == "explicit think reasoning"
    assert events[2]["reasoning_content"] == "model-native reasoning"
    assert events[2]["thought"] == "sdk prose thought"
    assert events[3]["reasoning_content"] == "hidden reasoning"


def test_mapper_parses_analysis_and_plan_from_thought() -> None:
    native = [
        {
            "event_type": "ActionEvent",
            "tool_call_id": "call-1",
            "tool_name": "terminal",
            "thought": [{"text": "Analysis: saw broken renderer\nPlan: rewrite render()"}],
            "tool_call": {
                "function": {
                    "arguments": '{"command": "pwd"}',
                }
            },
        },
    ]
    trajectory = OpenHandsSdkTrajectoryMapper.project_trajectory(
        json.dumps(native).encode(),
    )
    event = trajectory["events"][0]
    assert event["analysis"] == "saw broken renderer"
    assert event["plan"] == "rewrite render()"
    assert event["thought"] == "Analysis: saw broken renderer\nPlan: rewrite render()"
