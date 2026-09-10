from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from loom.models.trajectory import (
    Terminus2CommandEvent,
    Terminus2TerminalObservationEvent,
    Terminus2TurnEvent,
    Terminus2UserPromptEvent,
    TrajectoryEvent,
    TrialEndEvent,
    TrialStartEvent,
)
from loom.trajectory.llm_call_events import llm_call_row_to_event
from loom_control_plane.service_execution_materializer import (
    MaterializationIntegrityError,
    build_canonical_atif,
)


def _events(*, complete: bool = True, succeeded: bool = True) -> list[TrajectoryEvent]:
    trial_id, call_id = uuid4(), str(uuid4())
    base = {"trial_id": trial_id, "emitted_at": datetime.now(UTC), "step_id": "agent", "seq": 0}
    events: list[TrajectoryEvent] = [
        TrialStartEvent(**base, task_id="manifest", agent_name="terminus-2", agent_mode="in-box"),
        Terminus2UserPromptEvent(**base, prompt_id="prompt", harbor_step_id=1,
                                message="Inspect the archive and write a manifest.", is_initial=True),
        llm_call_row_to_event({
            "id": call_id, "model": "glm-5.2", "dialect": "openai_facade",
            "step_id": "agent", "input_tokens": 10, "output_tokens": 5,
            "cost_usd": 0.02, "rate_card_hash": "rate", "provider_extras": {},
        }, trial_id=trial_id, seq=0),
        Terminus2TurnEvent(**base, turn_id="turn", turn_index=0, gateway_request_id=call_id,
                          harbor_step_id=2, parse_state="ok", completion_state="complete",
                          analysis="Inspect archive layout", plan="List the entries"),
        Terminus2CommandEvent(**base, turn_id="turn", command_batch_id="commands", command_id="ls",
                             index=0, keystrokes="ls /app\n", duration_sec=0.1),
    ]
    if complete:
        events.append(Terminus2TerminalObservationEvent(
            **base, turn_id="turn", command_batch_id="commands", observation_id="obs",
            text="archive_src", capture_source="incremental", byte_len=11, truncated=False,
            completeness="full", content_hash="existing-hash", redaction_applied=False,
            is_aggregate=False,
        ))
    events.append(TrialEndEvent(**base, final_state="succeeded" if succeeded else "cancelled",
                                reward={"resolved": 0} if succeeded else None))
    return events


def test_canonical_terminus_atif_preserves_prompt_shell_and_observation() -> None:
    document = json.loads(build_canonical_atif(
        _events(), task_id="manifest", agent_name="terminus-2", agent_version="1.0",
    ))
    assert document["schema_version"] == "harbor-tb2-v2-projection"
    assert document["steps"][0]["source"] == "user"
    assert document["steps"][0]["message"] == "Inspect the archive and write a manifest."
    turn = document["steps"][1]
    assert turn["tool_calls"][0]["arguments"]["keystrokes"] == "ls /app\n"
    assert turn["observation"] == "archive_src"
    assert turn["metrics"]["input_tokens"] == 10
    assert document["metadata"]["reward"] == {"resolved": 0}
    assert document["session_id"]


def test_successful_terminus_requires_existing_mapper_turn_joins() -> None:
    with pytest.raises(MaterializationIntegrityError, match="terminus_turn_join_invalid"):
        build_canonical_atif(_events(complete=False), task_id="manifest",
                             agent_name="terminus-2", agent_version="1.0")


def test_cancelled_terminus_retains_partial_turn() -> None:
    document = json.loads(build_canonical_atif(
        _events(complete=False, succeeded=False), task_id="manifest",
        agent_name="terminus-2", agent_version="1.0",
    ))
    assert document["metadata"]["final_state"] == "cancelled"
    assert document["steps"][1]["observation"] is None
    assert document["steps"][1]["tool_calls"]
