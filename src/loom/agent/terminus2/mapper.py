"""Terminus2TrajectoryMapper — derive Harbor-compatible ATIF steps from typed events (#745)."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from loom.models.trajectory import (
    EventKind,
    LLMCallEvent,
    Terminus2CommandEvent,
    Terminus2TerminalObservationEvent,
    Terminus2TurnEvent,
    TrajectoryEvent,
)


class Terminus2TrajectoryMapper:
    """Project native terminus2 typed events into per-turn ATIF-like steps."""

    @staticmethod
    def project_to_atif(
        events: Iterable[TrajectoryEvent],
        *,
        task_id: str,
        agent_name: str,
        agent_version: str,
    ) -> dict[str, Any]:
        turns: dict[str, dict[str, Any]] = {}
        commands_by_turn: dict[str, list[Terminus2CommandEvent]] = {}
        observations_by_turn: dict[str, Terminus2TerminalObservationEvent] = {}
        llm_by_gateway: dict[str, LLMCallEvent] = {}

        for event in events:
            if event.kind == EventKind.LLM_CALL:
                llm_by_gateway[event.gateway_request_id] = event
            elif event.kind == EventKind.TERMINUS2_TURN:
                turns[event.turn_id] = {
                    "turn_id": event.turn_id,
                    "turn_index": event.turn_index,
                    "gateway_request_id": event.gateway_request_id,
                    "parse_state": event.parse_state,
                    "completion_state": event.completion_state,
                    "analysis": event.analysis,
                    "plan": event.plan,
                }
            elif event.kind == EventKind.TERMINUS2_COMMAND:
                commands_by_turn.setdefault(event.turn_id, []).append(event)
            elif event.kind == EventKind.TERMINUS2_TERMINAL_OBSERVATION:
                observations_by_turn[event.turn_id] = event

        steps: list[dict[str, Any]] = []
        for turn_id, turn in sorted(
            turns.items(),
            key=lambda item: item[1]["turn_index"],
        ):
            gw_id = turn["gateway_request_id"]
            llm = llm_by_gateway.get(gw_id)
            obs = observations_by_turn.get(turn_id)
            cmds = sorted(commands_by_turn.get(turn_id, []), key=lambda c: c.index)
            tool_calls = [
                {
                    "tool_call_id": cmd.command_id,
                    "function_name": "bash_command",
                    "arguments": {
                        "keystrokes": cmd.keystrokes,
                        "duration": cmd.duration_sec,
                    },
                }
                for cmd in cmds
            ]
            if turn["completion_state"] in ("pending_confirm", "complete"):
                tool_calls.append(
                    {
                        "tool_call_id": f"{turn_id}_task_complete",
                        "function_name": "mark_task_complete",
                        "arguments": {},
                    },
                )
            step: dict[str, Any] = {
                "step_id": str(turn["turn_index"] + 1),
                "turn_id": turn_id,
                "parse_state": turn["parse_state"],
                "completion_state": turn["completion_state"],
                "tool_calls": tool_calls or None,
                "observation": obs.text if obs else None,
                "observation_metadata": (
                    {
                        "capture_source": obs.capture_source,
                        "is_aggregate": obs.is_aggregate,
                        "completeness": obs.completeness,
                        "content_hash": obs.content_hash,
                    }
                    if obs
                    else None
                ),
            }
            if llm is not None:
                step["metrics"] = {
                    "input_tokens": llm.input_tokens,
                    "output_tokens": llm.output_tokens,
                    "cached_input_tokens": llm.cached_input_tokens,
                    "cache_write_tokens": llm.cache_write_tokens,
                    "thinking_tokens": llm.thinking_tokens,
                    "cost_usd": llm.cost_usd_snapshot,
                }
                step["gateway_request_id"] = gw_id
            steps.append(step)

        return {
            "schema_version": "harbor-tb2-v2-projection",
            "task_id": task_id,
            "agent_name": agent_name,
            "agent_version": agent_version,
            "steps": steps,
        }

    @staticmethod
    def validate_turn_joins(events: Iterable[TrajectoryEvent]) -> list[str]:
        """Return validation errors for #745/#746 join invariants."""
        errors: list[str] = []
        turns: list[Terminus2TurnEvent] = []
        llm_ids: set[str] = set()
        obs_turns: set[str] = set()

        for event in events:
            if event.kind == EventKind.LLM_CALL:
                llm_ids.add(event.gateway_request_id)
            elif event.kind == EventKind.TERMINUS2_TURN:
                turns.append(event)
            elif event.kind == EventKind.TERMINUS2_TERMINAL_OBSERVATION:
                obs_turns.add(event.turn_id)

        for turn in turns:
            if turn.parse_state != "ok":
                continue
            if turn.gateway_request_id not in llm_ids:
                errors.append(
                    f"turn {turn.turn_id}: missing LLMCallEvent for "
                    f"gateway_request_id={turn.gateway_request_id}",
                )
            if turn.turn_id not in obs_turns:
                errors.append(
                    f"turn {turn.turn_id}: missing terminus2_terminal_observation",
                )
        return errors
