# LLM Gateway

Service-mode-only FastAPI process that proxies all upstream LLM calls
so every trajectory carries faithful token usage + cost attribution
the host doesn't have to reconstruct.

CLI mode skips the Gateway entirely — `UpstreamDirectGatewayClient`
in `loom_cli/upstream_gateway.py` re-implements the same dispatch
contract against provider SDKs in-process. See [`cli-mode.md`](cli-mode.md).

## Responsibilities

1. **Multi-dialect routing** — accept the *same* logical request and
   forward via the right native dialect (`/v1/messages` for
   Anthropic, `/v1/chat/completions` + `/v1/responses` for OpenAI,
   `:generateContent` for Gemini). Other adapters (incl. LiteLLM)
   for tail providers go through `loom_llm_gateway.litellm_wrapper`.
2. **Token-usage extraction** — every dialect puts cache + reasoning
   counters in a different shape; `DialectAdapter` normalises them
   into a `TokenUsage` dataclass with a verbatim `provider_extras`
   dict.
3. **Cost attribution** — looks up the rate card for `(provider,
   model)` at request time, computes `cost_usd` from the extracted
   `TokenUsage`, writes a row to the `llm_calls` table with
   `(team_id, trial_id, step_id)` keys so any aggregation slices
   cleanly.
4. **Request-parameter audit** — stores a redacted normalized
   `request_params` JSON object with non-sensitive generation
   controls such as `temperature`, `top_p`, `seed`, max output limits,
   reasoning effort, tool-choice mode, and provider decoding extras.
   Prompt bodies, messages, headers, API keys, and raw credentials are
   omitted.
   Service-mode `TrialConfig.request_params` is the user-facing input
   for those controls on the `litellm` worker path; the worker filters
   that object through the same allowlist before forwarding it to the
   Gateway.
5. **Bearer auth + per-team RPM** — every call carries a team
   token; the Gateway gates RPM per `(team, provider)` and rejects
   when over.
6. **License allowlists** — task-level allow/deny of model ids; the
   Gateway denies a request if its `model` field isn't in the team's
   allowlist.

## Endpoint shape (dialects)

| Route                    | Dialect             | Forwarded to                       |
|--------------------------|---------------------|------------------------------------|
| `POST /v1/messages`      | Anthropic native    | `anthropic.AsyncAnthropic.messages.create` |
| `POST /v1/chat/completions` | OpenAI Chat / BYO OpenAI-compatible chat | Native OpenAI path or direct httpx through `EgressClientPool` when `loom.provider_connection_id` resolves to `openai-compatible` / `custom` |
| `POST /v1/responses`     | OpenAI Responses / BYO OpenAI-compatible Responses | Native OpenAI path or direct httpx through `EgressClientPool` when the step JWT or `x-loom-provider-connection-id` resolves to `openai-compatible` / `custom`; if a BYO endpoint exposes only Chat Completions and returns the chat-style "messages required" 400 from `/responses`, the provider facade retries once through `/chat/completions` and synthesizes a Responses body/SSE for Codex |
| `POST /v1/models/{model}:generateContent` | Gemini  | `google.generativeai.GenerativeModel.generate_content_async` |
| `POST /v1/chat/completions` *(via litellm)* | Tail-provider shim | non-BYO and non-OpenAI-compatible fallback providers |
| `POST /openai/v1/chat/completions`, `/openai/v1/responses`, `/anthropic/v1/messages`, `/google/v1beta/...` | Sandbox provider facade | Direct httpx through `EgressClientPool` |

The Gateway is **not** a single unified shape — each dialect's
response is forwarded verbatim. Agents see exactly the native
upstream shape, so client-side parsing stays standard. The
normalisation (TokenUsage → cost) happens *next to* the forward, not
*after* it.

Streaming is supported on the OpenAI Responses routes and on both
Anthropic Messages routes. The Gateway accepts `stream=true` on
`/v1/messages`, `/v1/responses`, `/openai/v1/responses`, and
`/anthropic/v1/messages`, forwards the native SSE stream to the
selected provider, and records usage from the terminal usage-bearing
SSE event. For Anthropic streams that means tee-parsing the upstream
bytes as they pass through the route: `message_start` carries the
input + cache token counts, the final `message_delta` carries the
cumulative `output_tokens`, and `record_call` runs after the upstream
stream completes (or on client disconnect, with whatever usage was
accumulated). Cost attribution is preserved without buffering the
full response. Other native routes (`/v1/chat/completions`) still
reject `stream=true` because their cost path expects the full JSON
body.

For provider connections that are OpenAI-chat-compatible but not
Responses-compatible, the Responses facade has a narrow compatibility
fallback. It triggers only after an upstream `/responses` call returns
the chat-only missing-`messages` signature. The fallback converts the
Responses `instructions`/`input`/function-tool shape to a
non-streaming `/chat/completions` request, then converts the chat
message or tool calls back into Responses JSON or SSE. Unsupported
Responses-only item types are not treated as first-class ground truth;
operators should prefer a native `/responses` provider for production
score evidence when one is available.

## Dispatch contract

```
client                       Gateway                upstream
  │  POST /v1/messages          │
  │  Bearer <team_token>        │
  │  X-Loom-Trial: <uuid>       │
  │  X-Loom-Step:  <step>       │
  ├────────────────────────────►│
  │                             │  verify_bearer_token
  │                             │  rate-limit per (team, provider)
  │                             │  license-allowlist gate
  │                             │
  │                             │  messages.create(...)
  │                             ├──────────────────────►│
  │                             │◄──────────────────────│
  │                             │
  │                             │  DialectAdapter.extract(response) → TokenUsage
  │                             │  RateCardTable.lookup(provider, model)
  │                             │  compute_cost_usd(usage, rate) → cost_usd
  │                             │  llm_calls.insert(team_id, trial_id, step_id,
  │                             │                   input_tokens, output_tokens,
  │                             │                   provider_extras, request_params,
  │                             │                   cost_usd, ...)
  │                             │
  │                             │  forward response verbatim
  │◄────────────────────────────│
```

Trial + step IDs ride as request headers (`X-Loom-Trial`,
`X-Loom-Step`) because the wire response is the unmodified upstream
shape — there's nowhere to inject them into the body without
breaking client parsing. Workers populate the headers from
`bind_trial_context` (see [`cli-mode.md`](cli-mode.md) for the
contextvars helper).

For `LiteLLMAgent` service-mode trials, callers may include
`trial_config.request_params` with safe generation controls, for
example:

```json
{
  "agent_name": "litellm",
  "agent_model": {"provider": "openai", "name": "gpt-4o-mini"},
  "request_params": {
    "temperature": 0,
    "top_p": 0.5,
    "seed": 1234,
    "extra_body": {"top_k": 40}
  }
}
```

The worker drops prompt/message payloads, headers, credentials, and
unknown provider fields before the request is sent. The Gateway then
records the effective non-sensitive controls in `llm_calls.request_params`
and the trial/batch debug evidence surfaces.

Codex subprocess alignment runs use a separate adapter-specific path
because Codex constructs its own Responses request. The worker converts
sanitized per-trial `trial_config.request_params` into
`LOOM_CODEX_SETTINGS_JSON` for Codex subprocesses; deployment-level
`LOOM_CODEX_SETTINGS_JSON` remains only a fallback. The Codex launcher
sanitizes the JSON again and encodes it as
`model_providers.loom.query_params.loom_request_params`. The Responses
facade accepts that namespaced query param, sanitizes it again, merges
the safe controls into the upstream provider payload, and records the
same merged payload through the normal request-parameter audit.

## Why centralise

Could agents call providers directly and emit token counts to the
trajectory themselves? Harbor does. We rejected that for three
reasons:

1. **Honesty** — agents are subprocesses that crash, misreport, or
   silently swallow errors. A Gateway-side count is observably
   correct even if the agent process disappears mid-call.
2. **Attribution** — `(team, trial, step)` cost slicing needs a
   *single* writer to the `llm_calls` table. With per-agent writes,
   teams have to trust every adapter to spell the keys right.
3. **Policy** — rate limits and per-team budget accounting need a chokepoint.
   Gateway is that chokepoint.

The cost is one extra hop + the need for a service stack — exactly
the trade-off CLI mode declines, accepting weaker attribution to
ship the same `Trial.run()` on a laptop.

## What `litellm_wrapper` is for

LiteLLM provides a single `acompletion(model=..., ...)` call that
dispatches to ~100 providers. We use it as the **tail-provider
adapter** — anything not covered by Anthropic / OpenAI Chat /
Responses / Gemini routes through LiteLLM. BYO OpenAI-compatible
connections are also excluded from LiteLLM on the service-mode gateway
path for both Chat Completions and Responses: the route owns that wire
shape and forwards with the pooled httpx client from `EgressClientPool`,
so Envoy sees the
`x-loom-connection-id` CONNECT header for the selected
`provider_connection_id`. We don't use LiteLLM for the top-4 providers
or BYO OpenAI-compatible dispatch because:

- Native dialect responses are verbatim — LiteLLM's normalised
  response shape would obscure provider-specific fields.
- Token-usage extraction is per-dialect; we'd lose
  `cache_creation_input_tokens` etc. through LiteLLM's filter.
- LiteLLM does not provide a stable per-call hook for proxy CONNECT
  headers; the egress proxy's per-connection allowlist depends on that
  header.

## Failure modes + behavior

| Failure                                | Response                              |
|----------------------------------------|---------------------------------------|
| Upstream timeout (>120s)               | 504, `llm_calls` row inserted with `error_kind` |
| Upstream non-2xx                       | Forwarded or surfaced with a redacted excerpt; `llm_calls` row with `error_kind` where the dialect path records failed calls |
| LiteLLM adapter exception or malformed upstream response | 502 with sanitized diagnostic text; provider API keys and `Authorization: Bearer` values are redacted before logs or responses |
| BYO `/responses` endpoint is actually chat-only | One fallback POST to `/chat/completions`; success returns synthetic Responses JSON/SSE and records normal `openai_responses` usage from chat `usage` tokens |
| Rate-card missing                      | 422 `RateCardNotFoundError`; no row inserted |
| Bearer invalid / over RPM              | 401 / 429; no upstream call          |
| `stream=true`                          | Allowed for OpenAI Responses and Anthropic provider-facade calls; other v1 dialect paths reject it where final usage cannot be attributed |
| Model not in team allowlist            | 403; no upstream call                 |

A failed call still produces an `llm_calls` row when the upstream
was contacted — necessary so the team is debited for partial work
and so retries appear in attribution. Pre-upstream failures (auth,
rate-card, license) leave no row.

## Persistence schema

`llm_calls` columns of note:

| Column           | Notes                                                    |
|------------------|----------------------------------------------------------|
| `team_id`        | from bearer token                                        |
| `trial_id`       | from `X-Loom-Trial`                                      |
| `step_id`        | from `X-Loom-Step`                                       |
| `provider`       | matches rate-card key (`anthropic`, `local:vllm`, ...)   |
| `model`          | exact model id the request used                          |
| `input_tokens`   | from DialectAdapter                                      |
| `output_tokens`  | from DialectAdapter                                      |
| `provider_extras`| JSONB — cache + reasoning counters verbatim              |
| `request_params` | JSONB — redacted normalized generation controls. NULL means pre-#85 legacy/unavailable data |
| `cost_usd`       | computed at insert; cached for usage metrics/audits     |
| `rate_card_id`   | FK to the rate-card row used; re-pricing follows the row |
| `error_kind`     | non-null when upstream returned a non-2xx                |

Why both `cost_usd` (frozen) and `rate_card_id` (re-derivable)? The
column is a fast index for `/api/v1/usage` and Gateway cost metrics;
trial and batch detail responses expose token totals and call counts
instead. Re-derive from `provider_extras` + a *different* rate card to
model what-if pricing without touching history.

## See also

- [`cost-and-rate-cards.md`](cost-and-rate-cards.md) — the rate-card
  model + cost formula
- [`service-mode.md`](service-mode.md) — where the Gateway sits in
  the cluster
- [`cli-mode.md`](cli-mode.md) — the CLI's in-process replacement
