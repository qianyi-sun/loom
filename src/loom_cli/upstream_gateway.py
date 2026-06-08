"""UpstreamDirectGatewayClient — implements LLMGatewayClient against
upstream provider SDKs directly. Used by the stateless `loom run` CLI
where a real Loom Gateway service isn't available.

Cost is computed locally from `~/.config/loom/rate-cards.toml`. The
returned `GatewayCallResponse` matches the same dataclass the
Worker/Trial expect, so Trial.run() doesn't know it's running outside
the service stack.

Currently implements:
  - anthropic dialect -> anthropic.AsyncAnthropic.messages.create
  - openai dialect    -> openai.AsyncOpenAI.chat.completions.create   (Task 7)
  - google dialect    -> google.generativeai.GenerativeModel.generate_content (Task 7)
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from typing import Any

from loom.agent.gateway_client import GatewayCallRequest, GatewayCallResponse
from loom.models.trajectory import ChatMessage
from loom_cli.rate_cards import (
    RateCardTable,
    compute_cost_usd,
    load_rate_cards,
    lookup_entry,
    seed_default_if_missing,
)


@dataclass
class UpstreamDirectGatewayClient:
    """Stateless LLM Gateway implementation. Instantiate once per `loom run`."""

    anthropic_client: Any
    openai_client: Any
    google_client: Any
    tokens: dict[str, str]
    _rate_cards: RateCardTable | None = None

    def _table(self) -> RateCardTable:
        if self._rate_cards is None:
            seed_default_if_missing()
            self._rate_cards = load_rate_cards()
        return self._rate_cards

    def _rate_card_hash(self) -> str:
        text = repr(tuple(
            (e.provider, e.model, e.input_per_mtok, e.output_per_mtok,
             e.cache_read_per_mtok, e.cache_write_per_mtok)
            for e in self._table().entries
        ))
        return hashlib.sha256(text.encode()).hexdigest()

    async def call(self, request: GatewayCallRequest) -> GatewayCallResponse:
        provider = request.model.provider
        if provider == "anthropic":
            return await self._call_anthropic(request)
        if provider == "openai":
            return await self._call_openai(request)
        if provider == "google":
            return await self._call_google(request)
        raise ValueError(f"unsupported provider for upstream-direct: {provider!r}")

    async def _call_anthropic(
        self, request: GatewayCallRequest,
    ) -> GatewayCallResponse:
        if not self.tokens.get("anthropic"):
            raise ValueError(
                "no anthropic API key — run `loom config set token.anthropic <key>` "
                "or export ANTHROPIC_API_KEY",
            )
        msgs = [{"role": m.role, "content": m.content} for m in request.messages]
        kwargs: dict[str, Any] = {
            "model": request.model.name,
            "messages": msgs,
            "max_tokens": 4096,
        }
        if request.system_prompt is not None:
            kwargs["system"] = request.system_prompt
        t0 = time.monotonic()
        raw = await self.anthropic_client.messages.create(**kwargs)
        dur = time.monotonic() - t0

        text_parts: list[str] = []
        for block in raw.content:
            if getattr(block, "type", None) == "text":
                text_parts.append(block.text)
        text = "".join(text_parts)

        usage = raw.usage
        in_tok = int(getattr(usage, "input_tokens", 0))
        out_tok = int(getattr(usage, "output_tokens", 0))
        cache_w = int(getattr(usage, "cache_creation_input_tokens", 0) or 0)
        cache_r = int(getattr(usage, "cache_read_input_tokens", 0) or 0)

        entry = lookup_entry(
            self._table(), provider="anthropic", model=request.model.name,
        )
        cost = compute_cost_usd(
            entry,
            input_tokens=in_tok, output_tokens=out_tok,
            cached_input_tokens=cache_r, cache_write_tokens=cache_w,
        )

        return GatewayCallResponse(
            response=ChatMessage(role="assistant", content=text),
            input_tokens=in_tok,
            cached_input_tokens=cache_r,
            cache_write_tokens=cache_w,
            output_tokens=out_tok,
            thinking_tokens=0,
            provider_extras={
                "cache_read_input_tokens": cache_r,
                "cache_creation_input_tokens": cache_w,
            },
            cost_usd=cost,
            finish_reason="stop" if str(getattr(raw, "stop_reason", "")) in {
                "end_turn", "stop_sequence",
            } else str(getattr(raw, "stop_reason", "unknown")),
            duration_sec=dur,
            streamed=False,
            time_to_first_token_sec=None,
            rate_card_hash=self._rate_card_hash(),
            gateway_request_id=f"upstream-direct-{int(t0 * 1000)}",
        )

    async def _call_openai(
        self, request: GatewayCallRequest,
    ) -> GatewayCallResponse:
        if not self.tokens.get("openai"):
            raise ValueError(
                "no openai API key — run `loom config set token.openai <key>` "
                "or export OPENAI_API_KEY",
            )
        msgs: list[dict[str, Any]] = []
        if request.system_prompt is not None:
            msgs.append({"role": "system", "content": request.system_prompt})
        for m in request.messages:
            msgs.append({"role": m.role, "content": m.content})
        kwargs: dict[str, Any] = {"model": request.model.name, "messages": msgs}
        t0 = time.monotonic()
        raw = await self.openai_client.chat.completions.create(**kwargs)
        dur = time.monotonic() - t0

        choice = raw.choices[0]
        text = choice.message.content or ""
        finish = str(choice.finish_reason or "stop")

        usage = raw.usage
        in_tok = int(getattr(usage, "prompt_tokens", 0))
        out_tok = int(getattr(usage, "completion_tokens", 0))
        details = getattr(usage, "prompt_tokens_details", None)
        cached = int(getattr(details, "cached_tokens", 0) or 0) if details else 0

        entry = lookup_entry(
            self._table(), provider="openai", model=request.model.name,
        )
        cost = compute_cost_usd(
            entry,
            input_tokens=in_tok, output_tokens=out_tok,
            cached_input_tokens=cached, cache_write_tokens=0,
        )

        return GatewayCallResponse(
            response=ChatMessage(role="assistant", content=text),
            input_tokens=in_tok,
            cached_input_tokens=cached,
            cache_write_tokens=0,
            output_tokens=out_tok,
            thinking_tokens=0,
            provider_extras={"cached_tokens": cached},
            cost_usd=cost,
            finish_reason=finish,
            duration_sec=dur,
            streamed=False,
            time_to_first_token_sec=None,
            rate_card_hash=self._rate_card_hash(),
            gateway_request_id=f"upstream-direct-{int(t0 * 1000)}",
        )

    async def _call_google(
        self, request: GatewayCallRequest,
    ) -> GatewayCallResponse:
        if not self.tokens.get("google"):
            raise ValueError(
                "no google API key — run `loom config set token.google <key>` "
                "or export GOOGLE_API_KEY",
            )
        contents = [
            {"role": "user" if m.role == "user" else "model",
             "parts": [{"text": m.content}]}
            for m in request.messages
        ]
        t0 = time.monotonic()
        raw = await self.google_client.generate_content_async(
            model=request.model.name,
            contents=contents,
            system_instruction=request.system_prompt,
        )
        dur = time.monotonic() - t0

        cand = raw.candidates[0]
        text = "".join(p.text for p in cand.content.parts if hasattr(p, "text"))
        finish_int = int(getattr(cand, "finish_reason", 1))
        finish = "stop" if finish_int == 1 else f"finish_reason_{finish_int}"

        usage = raw.usage_metadata
        in_tok = int(getattr(usage, "prompt_token_count", 0))
        out_tok = int(getattr(usage, "candidates_token_count", 0))
        cached = int(getattr(usage, "cached_content_token_count", 0) or 0)

        entry = lookup_entry(
            self._table(), provider="google", model=request.model.name,
        )
        cost = compute_cost_usd(
            entry,
            input_tokens=in_tok, output_tokens=out_tok,
            cached_input_tokens=cached, cache_write_tokens=0,
        )

        return GatewayCallResponse(
            response=ChatMessage(role="assistant", content=text),
            input_tokens=in_tok,
            cached_input_tokens=cached,
            cache_write_tokens=0,
            output_tokens=out_tok,
            thinking_tokens=0,
            provider_extras={"cachedContentTokenCount": cached},
            cost_usd=cost,
            finish_reason=finish,
            duration_sec=dur,
            streamed=False,
            time_to_first_token_sec=None,
            rate_card_hash=self._rate_card_hash(),
            gateway_request_id=f"upstream-direct-{int(t0 * 1000)}",
        )
