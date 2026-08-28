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
    ActionEvent = type("ActionEvent", (), {})  # noqa: N806
    ObservationEvent = type("ObservationEvent", (), {})  # noqa: N806
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
    MessageEvent = type("MessageEvent", (), {})  # noqa: N806
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
    ActionEvent = type("ActionEvent", (), {})  # noqa: N806
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


def test_action_event_reasoning_content_emits_agent_thought_and_tool_use() -> None:
    mapper = _mapper()
    ActionEvent = type("ActionEvent", (), {})  # noqa: N806
    ObservationEvent = type("ObservationEvent", (), {})  # noqa: N806
    action = ActionEvent()
    action.tool_call_id = "call-reason"
    action.tool_name = "terminal"
    action.reasoning_content = "model-native chain of thought"
    action.thought = []
    action.summary = None
    action.tool_call = SimpleNamespace(
        function=SimpleNamespace(arguments='{"command": "pwd"}'),
    )
    action.action = None

    thought_payloads = mapper.map_event(action)
    assert len(thought_payloads) == 1
    assert thought_payloads[0]["kind"] == "agent_thought"
    assert thought_payloads[0]["content"] == "model-native chain of thought"
    assert thought_payloads[0]["reasoning_content"] == "model-native chain of thought"
    assert thought_payloads[0]["sdk_event_type"] == "ActionEvent"

    observation = ObservationEvent()
    observation.tool_call_id = "call-reason"
    observation.tool_name = "terminal"
    observation.observation = SimpleNamespace(content=[SimpleNamespace(text="/workspace\n")])

    tool_payloads = mapper.map_event(observation)
    assert len(tool_payloads) == 1
    assert tool_payloads[0]["kind"] == "tool_use"
    assert tool_payloads[0]["reasoning_content"] == "model-native chain of thought"


def test_action_event_thought_only_emits_agent_thought() -> None:
    mapper = _mapper()
    ActionEvent = type("ActionEvent", (), {})  # noqa: N806
    action = ActionEvent()
    action.tool_call_id = "call-thought"
    action.tool_name = "terminal"
    action.reasoning_content = None
    action.thought = [SimpleNamespace(text="sdk prose thought")]
    action.summary = "summarize"
    action.tool_call = SimpleNamespace(
        function=SimpleNamespace(arguments='{"command": "ls"}'),
    )
    action.action = None

    payloads = mapper.map_event(action)
    assert len(payloads) == 1
    assert payloads[0]["kind"] == "agent_thought"
    assert payloads[0]["content"] == "sdk prose thought"
    assert payloads[0]["thought"] == "sdk prose thought"
    assert payloads[0]["summary"] == "summarize"


def test_think_tool_observation_sets_reasoning_content_from_args() -> None:
    mapper = _mapper()
    ActionEvent = type("ActionEvent", (), {})  # noqa: N806
    ObservationEvent = type("ObservationEvent", (), {})  # noqa: N806
    action = ActionEvent()
    action.tool_call_id = "call-think"
    action.tool_name = "think"
    action.reasoning_content = None
    action.thought = []
    action.summary = None
    action.tool_call = SimpleNamespace(
        function=SimpleNamespace(arguments='{"thought": "explicit think tool reasoning"}'),
    )
    action.action = None
    assert mapper.map_event(action) == []

    observation = ObservationEvent()
    observation.tool_call_id = "call-think"
    observation.tool_name = "think"
    observation.observation = SimpleNamespace(content=[SimpleNamespace(text="ok")])

    payloads = mapper.map_event(observation)
    assert len(payloads) == 1
    assert payloads[0]["kind"] == "tool_use"
    assert payloads[0]["tool_name"] == "think"
    assert payloads[0]["reasoning_content"] == "explicit think tool reasoning"


def test_message_event_reasoning_content_emits_separate_agent_thought() -> None:
    mapper = _mapper()
    MessageEvent = type("MessageEvent", (), {})  # noqa: N806
    event = MessageEvent()
    event.source = "agent"
    event.llm_message = SimpleNamespace(
        content=[SimpleNamespace(text="visible reply")],
        reasoning_content="hidden reasoning on text-only turn",
    )
    payloads = mapper.map_event(event)
    assert len(payloads) == 2
    assert payloads[0]["kind"] == "agent_thought"
    assert payloads[0]["content"] == "visible reply"
    assert payloads[1]["kind"] == "agent_thought"
    assert payloads[1]["content"] == "hidden reasoning on text-only turn"
    assert payloads[1]["reasoning_content"] == "hidden reasoning on text-only turn"
    assert payloads[1]["sdk_event_type"] == "MessageEvent"
