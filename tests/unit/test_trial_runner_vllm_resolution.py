"""LocalTrialRunner._resolve_gateway — picks LocalVLLMGatewayClient
when the trial's model targets a worker-spawned vLLM, otherwise falls
through to the HTTP gateway.

Unit-scope so it doesn't need a real CP, driver, or vLLM."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from loom.agent.gateway_client import FakeLLMGatewayClient
from loom.agent.local_vllm_client import LocalVLLMGatewayClient
from loom.errors import AgentError
from loom.models.types import ModelSpec


@dataclass
class _FakeHandle:
    base_url: str = "http://127.0.0.1:8234/v1"
    served_model_name: str = "foo/bar"
    pid: int = 12345


@dataclass
class _FakeRegistry:
    """Mirrors WorkerVLLMRegistry.get_or_launch without spawning."""

    calls: list[str]

    async def get_or_launch(self, model_id: str) -> _FakeHandle:
        self.calls.append(model_id)
        return _FakeHandle()


def _runner(
    model: ModelSpec | None,
    *,
    vllm_registry: object | None = None,
) -> object:
    """Mint a LocalTrialRunner-shaped object whose `_resolve_gateway`
    only consults `trial_config.agent_model` + `vllm_registry`. We
    duck-type around the heavy fields (task_config, driver, etc.) so
    the test stays unit-scope."""
    from loom_worker.trial_runner import LocalTrialRunner

    runner = LocalTrialRunner.__new__(LocalTrialRunner)
    runner.gateway_client = FakeLLMGatewayClient(scripted=[])  # type: ignore[attr-defined]
    runner.trial_config = SimpleNamespace(  # type: ignore[attr-defined]
        agent_model=model,
        agent_name="litellm" if model is not None else "oracle",
    )
    runner.vllm_registry = vllm_registry  # type: ignore[attr-defined]
    return runner


async def test_no_model_returns_default_gateway() -> None:
    runner = _runner(None)
    gw = await runner._resolve_gateway()  # type: ignore[attr-defined]
    assert isinstance(gw, FakeLLMGatewayClient)


async def test_api_source_returns_default_gateway() -> None:
    runner = _runner(
        ModelSpec(provider="anthropic", name="claude-opus-4-7"),
    )
    gw = await runner._resolve_gateway()  # type: ignore[attr-defined]
    assert isinstance(gw, FakeLLMGatewayClient)


async def test_hf_inference_api_returns_default_gateway() -> None:
    """HF Inference still routes through the gateway (LiteLLM handles
    the upstream call), so the trial runner shouldn't swap clients."""
    runner = _runner(
        ModelSpec(
            provider="hf", name="foo/bar",
            source="hf", hf_execution="inference-api",
        ),
    )
    gw = await runner._resolve_gateway()  # type: ignore[attr-defined]
    assert isinstance(gw, FakeLLMGatewayClient)


async def test_hf_local_vllm_returns_local_client_and_calls_registry() -> None:
    registry = _FakeRegistry(calls=[])
    runner = _runner(
        ModelSpec(
            provider="hf", name="meta-llama/Llama-3-8B-Instruct",
            source="hf", hf_execution="local-vllm",
        ),
        vllm_registry=registry,
    )
    gw = await runner._resolve_gateway()  # type: ignore[attr-defined]
    assert isinstance(gw, LocalVLLMGatewayClient)
    assert gw.base_url == "http://127.0.0.1:8234/v1"
    assert registry.calls == ["meta-llama/Llama-3-8B-Instruct"]


async def test_hf_local_vllm_without_registry_raises() -> None:
    runner = _runner(
        ModelSpec(
            provider="hf", name="foo/bar",
            source="hf", hf_execution="local-vllm",
        ),
        vllm_registry=None,
    )
    with pytest.raises(AgentError, match="vllm_registry"):
        await runner._resolve_gateway()  # type: ignore[attr-defined]
