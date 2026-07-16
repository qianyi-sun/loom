"""Terminus2TrajectoryMapper tests (#745)."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from loom.agent.terminus2.mapper import Terminus2TrajectoryMapper
from loom.models.trajectory import (
    ChatMessage,
    EventKind,
    LLMCallEvent,
    Terminus2CommandEvent,
    Terminus2TerminalObservationEvent,
    Terminus2TurnEvent,
    Terminus2UserPromptEvent,
)
from loom.models.types import ModelSpec


def _base(**kwargs: object) -> dict[str, object]:
    base = {
        "emitted_at": datetime.now(UTC),
        "trial_id": uuid4(),
        "step_id": "agent",
        "seq": 0,
    }
    base.update(kwargs)
    return base


def test_project_to_atif_joins_turn_command_observation() -> None:
    gw_id = "gw-1"
    turn_id = "turn-1"
    events = [
        LLMCallEvent(
            **_base(seq=1),
            kind=EventKind.LLM_CALL,
            model=ModelSpec(provider="openai", name="gpt-4"),
            rate_card_hash="h",
            system_prompt=None,
            messages=[ChatMessage(role="user", content="hi")],
            tools=None,
            tool_choice=None,
            response=ChatMessage(role="assistant", content="{}"),
            finish_reason="stop",
            input_tokens=1,
            cached_input_tokens=0,
            cache_write_tokens=0,
            output_tokens=1,
            thinking_tokens=0,
            provider_extras={},
            cost_usd_snapshot=0.0,
            duration_sec=0.1,
            streamed=False,
            time_to_first_token_sec=None,
            gateway_request_id=gw_id,
        ),
        Terminus2TurnEvent(
            **_base(seq=2),
            kind=EventKind.TERMINUS2_TURN,
            turn_id=turn_id,
            turn_index=0,
            gateway_request_id=gw_id,
            parse_state="ok",
            completion_state="continue",
            analysis="inspect raw feeds",
            plan="list and cat source files",
        ),
        Terminus2CommandEvent(
            **_base(seq=3),
            kind=EventKind.TERMINUS2_COMMAND,
            turn_id=turn_id,
            command_batch_id="batch-1",
            command_id="cmd-1",
            index=0,
            keystrokes="ls\n",
            duration_sec=0.1,
        ),
        Terminus2TerminalObservationEvent(
            **_base(seq=4),
            kind=EventKind.TERMINUS2_TERMINAL_OBSERVATION,
            turn_id=turn_id,
            command_batch_id="batch-1",
            observation_id="obs-1",
            text="New Terminal Output:\nfile.txt\n",
            capture_source="incremental",
            byte_len=10,
            truncated=False,
            completeness="full",
            content_hash="abc",
            redaction_applied=False,
            is_aggregate=False,
        ),
    ]
    doc = Terminus2TrajectoryMapper.project_to_atif(
        events,
        task_id="task-1",
        agent_name="terminus-2",
        agent_version="1.0",
    )
    assert len(doc["steps"]) == 1
    assert doc["steps"][0]["source"] == "agent"
    assert doc["steps"][0]["analysis"] == "inspect raw feeds"
    assert doc["steps"][0]["plan"] == "list and cat source files"
    assert (
        doc["steps"][0]["message"]
        == "Analysis: inspect raw feeds\nPlan: list and cat source files"
    )
    assert doc["steps"][0]["gateway_request_id"] == gw_id
    assert doc["steps"][0]["observation"].startswith("New Terminal Output")


def test_project_to_atif_parses_reasoning_from_excerpt_fallback() -> None:
    gw_id = "gw-2"
    turn_id = "turn-2"
    events = [
        Terminus2TurnEvent(
            **_base(),
            kind=EventKind.TERMINUS2_TURN,
            turn_id=turn_id,
            turn_index=0,
            gateway_request_id=gw_id,
            parse_state="ok",
            completion_state="continue",
            raw_response_excerpt="Analysis: fresh shell\nPlan: inspect files",
        ),
        Terminus2TerminalObservationEvent(
            **_base(seq=1),
            kind=EventKind.TERMINUS2_TERMINAL_OBSERVATION,
            turn_id=turn_id,
            command_batch_id="b",
            observation_id="o",
            text="output\n",
            capture_source="incremental",
            byte_len=1,
            truncated=False,
            completeness="full",
            content_hash="h",
            redaction_applied=False,
            is_aggregate=False,
        ),
    ]
    doc = Terminus2TrajectoryMapper.project_to_atif(
        events,
        task_id="task-1",
        agent_name="terminus-2",
        agent_version="1.0",
    )
    assert doc["steps"][0]["analysis"] == "fresh shell"
    assert doc["steps"][0]["plan"] == "inspect files"


def test_project_to_atif_includes_user_prompt_and_reasoning() -> None:
    gw_id = "gw-3"
    turn_id = "turn-3"
    events = [
        Terminus2UserPromptEvent(
            **_base(),
            kind=EventKind.TERMINUS2_USER_PROMPT,
            prompt_id="prompt-1",
            harbor_step_id=1,
            message="You are an AI assistant...\nTask: do the thing",
            is_initial=True,
        ),
        Terminus2TurnEvent(
            **_base(seq=1),
            kind=EventKind.TERMINUS2_TURN,
            turn_id=turn_id,
            turn_index=0,
            gateway_request_id=gw_id,
            parse_state="ok",
            completion_state="continue",
            analysis="look",
            plan="ls",
            reasoning_content="hidden chain",
            harbor_step_id=2,
        ),
        Terminus2TerminalObservationEvent(
            **_base(seq=2),
            kind=EventKind.TERMINUS2_TERMINAL_OBSERVATION,
            turn_id=turn_id,
            command_batch_id="b",
            observation_id="o",
            text="output\n",
            capture_source="incremental",
            byte_len=1,
            truncated=False,
            completeness="full",
            content_hash="h",
            redaction_applied=False,
            is_aggregate=False,
        ),
    ]
    doc = Terminus2TrajectoryMapper.project_to_atif(
        events,
        task_id="task-1",
        agent_name="terminus-2",
        agent_version="1.0",
    )
    assert [s["source"] for s in doc["steps"]] == ["user", "agent"]
    assert doc["steps"][0]["message"].startswith("You are an AI assistant")
    assert doc["steps"][0]["is_initial_prompt"] is True
    assert doc["steps"][1]["reasoning_content"] == "hidden chain"
    assert doc["steps"][1]["step_id"] == "2"


def test_project_to_atif_emits_null_reasoning_content_when_missing() -> None:
    turn_id = "turn-4"
    doc = Terminus2TrajectoryMapper.project_to_atif(
        [
            Terminus2TurnEvent(
                **_base(),
                kind=EventKind.TERMINUS2_TURN,
                turn_id=turn_id,
                turn_index=0,
                gateway_request_id="gw-4",
                parse_state="ok",
                completion_state="continue",
                analysis="inspect state",
                plan="run ls",
            ),
            Terminus2TerminalObservationEvent(
                **_base(seq=1),
                kind=EventKind.TERMINUS2_TERMINAL_OBSERVATION,
                turn_id=turn_id,
                command_batch_id="b",
                observation_id="o",
                text="output\n",
                capture_source="incremental",
                byte_len=1,
                truncated=False,
                completeness="full",
                content_hash="h",
                redaction_applied=False,
                is_aggregate=False,
            ),
        ],
        task_id="task-1",
        agent_name="terminus-2",
        agent_version="1.0",
    )
    assert "reasoning_content" in doc["steps"][0]
    assert doc["steps"][0]["reasoning_content"] is None
    assert doc["steps"][0]["analysis"] == "inspect state"


def test_enrich_from_native_backfills_user_prompt() -> None:
    traj = {
        "schema_version": "harbor-tb2-v2-projection",
        "steps": [
            {
                "step_id": "1",
                "source": "agent",
                "message": "Analysis: x\nPlan: y",
                "gateway_request_id": "gw-1",
            }
        ],
    }
    native = {
        "steps": [
            {"step_id": 1, "source": "user", "message": "TASK PROMPT HERE"},
            {
                "step_id": 2,
                "source": "agent",
                "message": "Analysis: x\nPlan: y",
                "reasoning_content": "from native",
            },
        ]
    }
    enriched = Terminus2TrajectoryMapper.enrich_from_native(traj, native)
    assert enriched["steps"][0]["source"] == "user"
    assert enriched["steps"][0]["message"] == "TASK PROMPT HERE"
    assert enriched["steps"][1]["reasoning_content"] == "from native"


def test_validate_turn_joins_detects_missing_llm() -> None:
    turn_id = "turn-1"
    events = [
        Terminus2TurnEvent(
            **_base(),
            kind=EventKind.TERMINUS2_TURN,
            turn_id=turn_id,
            turn_index=0,
            gateway_request_id="missing",
            parse_state="ok",
            completion_state="continue",
        ),
        Terminus2TerminalObservationEvent(
            **_base(seq=1),
            kind=EventKind.TERMINUS2_TERMINAL_OBSERVATION,
            turn_id=turn_id,
            command_batch_id="b",
            observation_id="o",
            text="x",
            capture_source="incremental",
            byte_len=1,
            truncated=False,
            completeness="full",
            content_hash="h",
            redaction_applied=False,
            is_aggregate=False,
        ),
    ]
    errors = Terminus2TrajectoryMapper.validate_turn_joins(events)
    assert any("missing LLMCallEvent" in e for e in errors)
