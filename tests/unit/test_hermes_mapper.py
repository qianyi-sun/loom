from __future__ import annotations

import json
from pathlib import Path

from loom.agent.hermes.mapper import HermesTrajectoryMapper

_FIXTURE = (
    Path(__file__).resolve().parents[1] / "fixtures" / "hermes" / "session_smoke7_trim.json"
)


def test_mapper_projects_prompts_tool_calls_observations_and_reasoning() -> None:
    native = _FIXTURE.read_bytes()
    trajectory = HermesTrajectoryMapper.project_trajectory(native)
    assert trajectory["schema_version"] == "hermes-export-projection"
    assert trajectory["source_of_truth"] == "native/hermes_session.json"
    assert trajectory["session"]["session_id"]
    assert trajectory["session"]["model"]

    kinds = [event["kind"] for event in trajectory["events"]]
    assert "user_prompt" in kinds
    assert "assistant" in kinds
    assert "tool_call" in kinds
    assert "observation" in kinds
    assert "reasoning" in kinds

    user = next(e for e in trajectory["events"] if e["kind"] == "user_prompt")
    assert "dedup-merge" in user["content"]

    tool_call = next(e for e in trajectory["events"] if e["kind"] == "tool_call")
    assert tool_call["tool_name"] in {"search_files", "terminal", "read_file"}
    assert isinstance(tool_call["arguments"], dict)
    assert tool_call["tool_call_id"]

    observation = next(e for e in trajectory["events"] if e["kind"] == "observation")
    assert observation["tool_call_id"]
    assert observation["content"]

    reasoning = next(e for e in trajectory["events"] if e["kind"] == "reasoning")
    assert "enough context" in reasoning["content"]


def test_mapper_emits_system_prompt_and_parses_tool_arguments() -> None:
    session = {
        "session_id": "s-test",
        "model": "glm-5.2",
        "messages": [
            {"role": "system", "content": "You are Hermes.", "timestamp": "t0"},
            {"role": "user", "content": "Do the task.", "timestamp": "t1"},
            {
                "role": "assistant",
                "content": "Running terminal.",
                "reasoning": "Need workspace listing.",
                "tool_calls": [
                    {
                        "id": "call-1",
                        "type": "function",
                        "function": {
                            "name": "terminal",
                            "arguments": '{"command": "pwd"}',
                        },
                    }
                ],
                "timestamp": "t2",
            },
            {
                "role": "tool",
                "tool_call_id": "call-1",
                "tool_name": "terminal",
                "content": '{"output": "/app", "exit_code": 0}',
                "timestamp": "t3",
            },
        ],
    }
    trajectory = HermesTrajectoryMapper.project_trajectory(
        json.dumps(session).encode(),
    )
    events = trajectory["events"]
    assert events[0]["kind"] == "system_prompt"
    assert events[0]["content"] == "You are Hermes."
    assert events[1]["kind"] == "user_prompt"
    assistant = next(e for e in events if e["kind"] == "assistant")
    assert assistant["tool_call_ids"] == ["call-1"]
    assert assistant["reasoning"] == "Need workspace listing."
    tool_call = next(e for e in events if e["kind"] == "tool_call")
    assert tool_call["arguments"] == {"command": "pwd"}
    observation = next(e for e in events if e["kind"] == "observation")
    assert observation["tool_call_id"] == "call-1"
    assert '"exit_code": 0' in observation["content"] or "exit_code" in observation["content"]
