"""Tests for Terminus-2 agent message helpers."""

from __future__ import annotations

from loom.agent.terminus2.agent_message import format_agent_message, parse_agent_message


def test_format_agent_message_joins_analysis_and_plan() -> None:
    assert (
        format_agent_message(analysis="look around", plan="run ls")
        == "Analysis: look around\nPlan: run ls"
    )


def test_parse_agent_message_round_trip() -> None:
    message = format_agent_message(
        analysis="fresh shell",
        plan="inspect files",
    )
    assert parse_agent_message(message) == ("fresh shell", "inspect files")


def test_parse_agent_message_handles_analysis_only() -> None:
    assert parse_agent_message("Analysis: only analysis") == ("only analysis", "")


def test_parse_agent_message_rejects_unstructured_text() -> None:
    assert parse_agent_message("done") == ("", "")
