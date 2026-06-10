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
4. **Bearer auth + per-team RPM** — every call carries a team
   token; the Gateway gates RPM per `(team, provider)` and rejects
   when over.
5. **License allowlists** — task-level allow/deny of model ids; the
   Gateway denies a request if its `model` field isn't in the team's
   allowlist.

## Endpoint shape (dialects)

| Route                    | Dialect             | Forwarded to                       |
|--------------------------|---------------------|------------------------------------|
| `POST /v1/messages`      | Anthropic native    | `anthropic.AsyncAnthropic.messages.create` |
| `POST /v1/chat/completions` | OpenAI Chat       | `openai.AsyncOpenAI.chat.completions.create` |
| `POST /v1/responses`     | OpenAI Responses    | `openai.AsyncOpenAI.responses.create` |
| `POST /v1/models/{model}:generateContent` | Gemini  | `google.generativeai.GenerativeModel.generate_content_async` |
| `POST /v1/chat/completions` *(via litellm)* | OpenAI-compat shim | tail providers (Cohere, Mistral, etc.) |

The Gateway is **not** a single unified shape — each dialect's
response is forwarded verbatim. Agents see exactly the native
upstream shape, so client-side parsing stays standard. The
normalisation (TokenUsage → cost) happens *next to* the forward, not
*after* it.

Streaming is disabled at v1 (`stream=true` → 400). The fenced
attribution model requires the response be inspectable to extract
usage; streaming would require a tee + reassembly. Tracked as
follow-up.

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
  │                             │                   provider_extras, cost_usd, ...)
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
3. **Policy** — rate limits, license allowlists, and per-team budget
   accounting need a chokepoint. Gateway is that chokepoint.

The cost is one extra hop + the need for a service stack — exactly
the trade-off CLI mode declines, accepting weaker attribution to
ship the same `Trial.run()` on a laptop.

## What `litellm_wrapper` is for

LiteLLM provides a single `acompletion(model=..., ...)` call that
dispatches to ~100 providers. We use it as the **tail-provider
adapter** — anything not covered by Anthropic / OpenAI Chat /
Responses / Gemini routes through LiteLLM. We don't use it for the
top-4 providers because:

- Native dialect responses are verbatim — LiteLLM's normalised
  response shape would obscure provider-specific fields.
- Token-usage extraction is per-dialect; we'd lose
  `cache_creation_input_tokens` etc. through LiteLLM's filter.

## Failure modes + behavior

| Failure                                | Response                              |
|----------------------------------------|---------------------------------------|
| Upstream timeout (>120s)               | 504, `llm_calls` row inserted with `error_kind` |
| Upstream non-2xx                       | Forwarded verbatim, `llm_calls` row with `error_kind` |
| Rate-card missing                      | 422 `RateCardNotFoundError`; no row inserted |
| Bearer invalid / over RPM              | 401 / 429; no upstream call          |
| `stream=true`                          | 400 with explicit "not supported v1" |
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
| `cost_usd`       | computed at insert; cached for fast aggregation         |
| `rate_card_id`   | FK to the rate-card row used; re-pricing follows the row |
| `error_kind`     | non-null when upstream returned a non-2xx                |

Why both `cost_usd` (frozen) and `rate_card_id` (re-derivable)? The
column is a fast index for `/api/v1/usage`; re-derive from
`provider_extras` + a *different* rate card to model what-if pricing
without touching history.

## See also

- [`cost-and-rate-cards.md`](cost-and-rate-cards.md) — the rate-card
  model + cost formula
- [`service-mode.md`](service-mode.md) — where the Gateway sits in
  the cluster
- [`cli-mode.md`](cli-mode.md) — the CLI's in-process replacement
