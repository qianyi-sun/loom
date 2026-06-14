"""ModelSpec.to_gateway_model_string — wire-format contract between
the worker (HttpLLMGatewayClient) and the gateway's chat dispatcher.

If either side drifts these strings, requests silently route to the
wrong upstream or 400 with an opaque message; pin every source the
catalog exposes."""

from __future__ import annotations

import pytest

from loom.models.types import ModelSpec


def test_api_source_legacy_format() -> None:
    """Existing rows without `source` default to `api` and produce
    the unchanged `provider/name` string the gateway already parses."""
    m = ModelSpec(provider="anthropic", name="claude-opus-4-7")
    assert m.to_gateway_model_string() == "anthropic/claude-opus-4-7"


def test_local_server_format() -> None:
    m = ModelSpec(
        provider="local", name="llama3",
        source="local-server", local_server="ollama-dev",
    )
    assert m.to_gateway_model_string() == "local/ollama-dev/llama3"


def test_local_server_missing_name_raises() -> None:
    """Construction-time guard: a local-server spec without the
    `local_server` field would produce `local//<name>` which the
    gateway can't split."""
    m = ModelSpec(
        provider="local", name="llama3", source="local-server",
    )
    with pytest.raises(ValueError, match="local_server"):
        m.to_gateway_model_string()


def test_hf_inference_api_format() -> None:
    m = ModelSpec(
        provider="hf", name="meta-llama/Llama-3-8B-Instruct",
        source="hf", hf_execution="inference-api",
    )
    assert m.to_gateway_model_string() == (
        "huggingface/meta-llama/Llama-3-8B-Instruct"
    )


def test_hf_local_vllm_format() -> None:
    """Worker-spawned vLLM — the gateway returns 501 for this prefix
    (worker should handle the call directly), but the string still
    needs a deterministic shape so the dispatcher can detect it."""
    m = ModelSpec(
        provider="hf", name="meta-llama/Llama-3-8B-Instruct",
        source="hf", hf_execution="local-vllm",
    )
    assert m.to_gateway_model_string() == (
        "local-vllm/meta-llama/Llama-3-8B-Instruct"
    )


def test_hf_local_vllm_is_the_default_for_hf_source() -> None:
    """The picker sends `hf_execution="local-vllm"` as the default;
    pin that here so a future default change is loud."""
    m = ModelSpec(
        provider="hf", name="foo/bar", source="hf",
    )
    assert m.hf_execution == "local-vllm"
    assert m.to_gateway_model_string() == "local-vllm/foo/bar"
