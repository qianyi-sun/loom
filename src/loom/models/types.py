"""Primitive types and scalar enums shared across Loom models.

Spec §2.3 (Capabilities), §4.1 (Task), §4.2 (Supporting types), §4.5 (TrialState).
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

# Scalar fields that match Postgres claim-query semantics exactly.
OS = Literal["linux", "windows"]
GPUVendor = Literal["none", "nvidia"]

# Verifier semantics.
VerifierEnvMode = Literal["shared", "separate"]

# Multi-step aggregation strategy.
MultiStepRewardStrategy = Literal["mean", "min", "weighted", "final"]

# Resource enforcement (trimmed from Harbor's 5 to 3 per spec §2.3).
ResourceMode = Literal["auto", "limit", "guarantee"]

# Capability-axis tag for NetworkPolicy. Matches NetworkPolicy subclass `kind` field.
NetworkPolicyKind = Literal["public", "no-network", "allowlist"]

# Logging level (spec §7.2).
LogLevel = Literal["debug", "info", "warn", "error", "fatal"]


ModelSource = Literal["api", "local-server", "hf"]
HFExecution = Literal["local-vllm", "inference-api"]


class ModelSpec(BaseModel):
    """Identifies an LLM model the agent should call (spec §4.2).

    `source` discriminates execution path (default "api" preserves the
    existing catalog-backed-API path and keeps old rows valid):

    - **api**: provider's hosted API (Anthropic/OpenAI/Google/HF Inference).
      Routed through the LLM Gateway.
    - **local-server**: pre-configured OpenAI-compatible local server
      (ollama, lm-studio, llama.cpp, vLLM). `local_server` names the
      operator's entry; the worker reads its base_url from config.
    - **hf**: HuggingFace model. `hf_execution` picks how to run it —
      "local-vllm" (default) spawns vLLM on a GPU worker;
      "inference-api" calls HF Inference Endpoints (managed, metered).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider: str
    name: str
    source: ModelSource = "api"
    local_server: str | None = None
    hf_execution: HFExecution = "local-vllm"
    tier: str | None = None
    region: str | None = None
    max_input_tokens: int | None = None
    max_output_tokens: int | None = None
