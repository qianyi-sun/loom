from __future__ import annotations

import json
import os

from loom_launcher.hermes_events import HermesEventMapper


def _mapper() -> HermesEventMapper:
    os.environ["LOOM_TRIAL_ID"] = "00000000-0000-4000-8000-000000000001"
    os.environ["LOOM_STEP_ID"] = "main"
    return HermesEventMapper()


def test_assistant_and_reasoning_become_agent_thought() -> None:
    mapper = _mapper()
    payloads = mapper.map_message(
        {
            "role": "assistant",
            "content": "planning next step",
            "reasoning_content": "model-native chain of thought",
        }
    )
    assert len(payloads) == 1
    assert payloads[0]["kind"] == "agent_thought"
    assert payloads[0]["content"] == "planning next step"
    assert payloads[0]["reasoning_content"] == "model-native chain of thought"


def test_tool_call_and_tool_result_pair_into_tool_use() -> None:
    mapper = _mapper()
    assistant = {
        "role": "assistant",
        "content": "",
        "tool_calls": [
            {
                "id": "call-1",
                "type": "function",
                "function": {
                    "name": "terminal",
                    "arguments": '{"command": "ls"}',
                },
            }
        ],
    }
    tool = {
        "role": "tool",
        "tool_call_id": "call-1",
        "content": "file.txt\n",
    }
    assert mapper.map_message(assistant) == []
    payloads = mapper.map_message(tool)
    assert len(payloads) == 1
    payload = payloads[0]
    assert payload["kind"] == "tool_use"
    assert payload["tool_name"] == "terminal"
    assert payload["args"] == {"command": "ls"}
    assert payload["result"] == {
        "content": "file.txt\n",
        "tool_call_id": "call-1",
    }
    assert payload["trial_id"] == "00000000-0000-4000-8000-000000000001"
    assert payload["step_id"] == "main"


def test_flush_pending_emits_unpaired_tool_calls() -> None:
    mapper = _mapper()
    mapper.map_message(
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": "call-orphan",
                    "function": {
                        "name": "read_file",
                        "arguments": '{"path": "/tmp/x"}',
                    },
                }
            ],
        }
    )
    payloads = mapper.flush_pending()
    assert len(payloads) == 1
    assert payloads[0]["kind"] == "tool_use"
    assert payloads[0]["tool_name"] == "read_file"
    assert payloads[0]["result"] is None


def test_map_messages_emits_thoughts_and_tools() -> None:
    mapper = _mapper()
    payloads = mapper.map_messages(
        [
            {"role": "user", "content": "list files"},
            {
                "role": "assistant",
                "content": "I will list",
                "reasoning": "need ls",
                "tool_calls": [
                    {
                        "id": "c1",
                        "function": {"name": "terminal", "arguments": '{"command": "ls"}'},
                    }
                ],
            },
            {"role": "tool", "tool_call_id": "c1", "content": "a.txt"},
        ]
    )
    kinds = [p["kind"] for p in payloads]
    assert kinds == ["agent_thought", "agent_thought", "tool_use"]
    assert payloads[0]["content"].startswith("user:")
    assert payloads[1]["reasoning_content"] == "need ls"
    assert payloads[2]["tool_name"] == "terminal"
