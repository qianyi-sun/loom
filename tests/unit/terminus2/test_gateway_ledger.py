"""Gateway ledger unit tests (#744 Gate 2)."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from loom.agent.terminus2.gateway_ledger import (
    CheckpointBridgeError,
    GatewayCallLedger,
    harbor_metrics_tokens,
)


def test_harbor_metrics_tokens_normalizes_prompt_and_completion() -> None:
    assert harbor_metrics_tokens(
        {"prompt_tokens": 10, "completion_tokens": 5},
    ) == (10, 5)
    assert harbor_metrics_tokens(
        {"input_tokens": 3, "output_tokens": 2},
    ) == (3, 2)


@pytest.mark.asyncio
async def test_gateway_ledger_resolves_unique_llm_call_row() -> None:
    trial_id = uuid4()
    ledger = GatewayCallLedger(trial_id=trial_id, step_id="agent")

    class _Cp:
        async def get_trial_llm_calls(self, tid: UUID) -> list[dict[str, object]]:
            assert tid == trial_id
            return [
                {
                    "id": "gw-real-1",
                    "step_id": "agent",
                    "input_tokens": 10,
                    "output_tokens": 5,
                    "dialect": "openai_chat",
                    "model": "gpt-4",
                    "cost_usd": 0.01,
                    "rate_card_hash": "abc",
                    "captured_at": "2026-07-10T00:00:00Z",
                },
            ]

    await ledger.refresh(_Cp())
    row = ledger.resolve_for_metrics({"prompt_tokens": 10, "completion_tokens": 5})
    assert row["id"] == "gw-real-1"


@pytest.mark.asyncio
async def test_gateway_ledger_fail_closed_on_missing_row() -> None:
    ledger = GatewayCallLedger(trial_id=uuid4(), step_id="agent")

    class _Cp:
        async def get_trial_llm_calls(self, trial_id: UUID) -> list[dict[str, object]]:
            return []

    await ledger.refresh(_Cp())
    with pytest.raises(CheckpointBridgeError, match="no llm_calls row"):
        ledger.resolve_for_metrics({"prompt_tokens": 1, "completion_tokens": 1})


@pytest.mark.asyncio
async def test_gateway_ledger_fail_closed_on_ambiguous_row() -> None:
    ledger = GatewayCallLedger(trial_id=uuid4(), step_id="agent")

    class _Cp:
        async def get_trial_llm_calls(self, trial_id: UUID) -> list[dict[str, object]]:
            return [
                {
                    "id": "a",
                    "step_id": "agent",
                    "input_tokens": 1,
                    "output_tokens": 1,
                },
                {
                    "id": "b",
                    "step_id": "agent",
                    "input_tokens": 1,
                    "output_tokens": 1,
                },
            ]

    await ledger.refresh(_Cp())
    with pytest.raises(CheckpointBridgeError, match="ambiguous"):
        ledger.resolve_for_metrics({"prompt_tokens": 1, "completion_tokens": 1})
