from __future__ import annotations

import json
import os
from types import SimpleNamespace

from loom_launcher.openhands_sdk_events import OpenHandsEventMapper


def _mapper() -> OpenHandsEventMapper:
    os.environ["LOOM_TRIAL_ID"] = "00000000-0000-4000-8000-000000000001"
    os.environ["LOOM_STEP_ID"] = "main"
    return OpenHandsEventMapper()


def test_action_and_observation_pair_into_tool_use() -> None:
    mapper = _mapper()
    ActionEvent = type("ActionEvent", (), {})
    ObservationEvent = type("ObservationEvent", (), {})
    action = ActionEvent()
    action.tool_call_id = "call-1"
    action.tool_name = "terminal"
    action.tool_call = SimpleNamespace(
        function=SimpleNamespace(arguments='{"command": "ls"}'),
    )
    action.action = None
    observation = ObservationEvent()
    observation.tool_call_id = "call-1"
    observation.tool_name = "terminal"
    observation.observation = SimpleNamespace(content=[SimpleNamespace(text="file.txt\n")])

    assert mapper.map_event(action) == []
    payloads = mapper.map_event(observation)
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


def test_message_event_becomes_agent_thought() -> None:
    mapper = _mapper()
    MessageEvent = type("MessageEvent", (), {})
    event = MessageEvent()
    event.source = "agent"
    event.llm_message = SimpleNamespace(content=[SimpleNamespace(text="planning next step")])
    payloads = mapper.map_event(event)
    assert len(payloads) == 1
    assert payloads[0]["kind"] == "agent_thought"
    assert payloads[0]["content"] == "planning next step"


def test_unknown_event_falls_back_to_agent_thought_json() -> None:
    mapper = _mapper()

    class FakeEvent:
        def model_dump(self, *, mode: str = "python") -> dict[str, str]:
            return {"message": "working"}

    payloads = mapper.map_event(FakeEvent())
    assert len(payloads) == 1
    assert payloads[0]["kind"] == "agent_thought"
    content = json.loads(str(payloads[0]["content"]))
    assert content["event_type"] == "FakeEvent"
    assert content["event"] == {"message": "working"}


def test_flush_pending_emits_unpaired_actions() -> None:
    mapper = _mapper()
    ActionEvent = type("ActionEvent", (), {})
    action = ActionEvent()
    action.tool_call_id = "call-orphan"
    action.tool_name = "file_editor"
    action.tool_call = None
    action.action = SimpleNamespace(
        model_dump=lambda mode="json": {"command": "view", "path": "/tmp/x"},
    )
    assert mapper.map_event(action) == []
    payloads = mapper.flush_pending()
    assert len(payloads) == 1
    assert payloads[0]["kind"] == "tool_use"
    assert payloads[0]["tool_name"] == "file_editor"
    assert payloads[0]["result"] is None
