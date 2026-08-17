"""Gateway correlation helpers for #1380."""

from __future__ import annotations

from uuid import uuid4

import pytest

from loom.agent.terminus2.gateway_ledger import GatewayCallLedger
from loom_llm_gateway.model_switch_correlation import extract_and_strip_loom_fields


def test_extract_and_strip_loom_fields_from_extra_body() -> None:
    payload = {
        "model": "glm-5.1",
        "messages": [],
        "extra_body": {
            "loom_client_call_id": "abc",
            "temperature": 0.2,
        },
        "loom_role": "student",
    }
    stripped, extras = extract_and_strip_loom_fields(payload)
    assert "loom_role" not in stripped
    assert stripped["extra_body"] == {"temperature": 0.2}
    assert extras["loom_client_call_id"] == "abc"
    assert extras["loom_role"] == "student"
    assert payload["extra_body"]["loom_client_call_id"] == "abc"


@pytest.mark.asyncio
async def test_gateway_ledger_prefers_correlated_episode() -> None:
    trial_id = uuid4()
    ledger = GatewayCallLedger(trial_id=trial_id, step_id="agent")

    class _Cp:
        async def get_trial_llm_calls(self, tid):  # type: ignore[no-untyped-def]
            assert tid == trial_id
            return [
                {
                    "id": "legacy",
                    "step_id": "agent",
                    "input_tokens": 10,
                    "output_tokens": 5,
                    "correlation_status": "legacy_uncorrelated",
                },
                {
                    "id": "corr",
                    "step_id": "agent",
                    "episode": 3,
                    "call_ordinal": 1,
                    "correlation_status": "correlated",
                    "input_tokens": 99,
                    "output_tokens": 99,
                },
            ]

    await ledger.refresh(_Cp())
    row = ledger.resolve_for_metrics(
        {"prompt_tokens": 10, "completion_tokens": 5},
        episode=3,
    )
    assert row["id"] == "corr"


def test_extract_empty_is_legacy() -> None:
    stripped, extras = extract_and_strip_loom_fields({"model": "x"})
    assert extras == {}
    assert stripped["model"] == "x"
