# Responses → Chat Completions translation shim

Status: design (no implementation). Extends [`llm-gateway.md`](llm-gateway.md) with a compatibility layer for BYO OpenAI-compatible providers that only implement `POST /v1/chat/completions`.

## Goal

Make codex (and any future Responses-API-consuming agent) function against BYO OpenAI-compatible provider connections whose upstream never implements `POST /v1/responses` — yibuapi, openrouter, deepinfra, groq, and most Chinese aggregators. The gateway accepts codex's Responses-shaped requests as usual, detects when the resolved upstream lacks Responses support, and transparently forwards the call through `/v1/chat/completions` — translating both the request and the response so codex is unaware.

## Non-goals

- Fix upstream provider behavior. If yibuapi decides to implement Responses natively, this shim becomes redundant and can be short-circuited; it never patches upstream state.
- Handle non-OpenAI dialects. Anthropic and Google routes stay untouched. Only `POST /openai/v1/responses` is affected.
- Emulate features Chat Completions cannot express. Where Responses depends on capabilities Chat lacks (structured reasoning output events, fine-grained streaming semantics), the shim degrades gracefully with documented losses rather than fabricating fidelity.
- Change codex CLI's expectations. The client sees the Responses envelope it was built for.
- Retrofit codex adapter-side fallback. Codex 0.141+ upstream requires `wire_api = "responses"`; there is no client-side switch.

## Design principles

- **Client-transparent.** The Responses request and the Responses response the client sees must be indistinguishable from a native-Responses upstream on every field the client actually consults. Failure modes exposed to the client are Responses-shaped errors, not translated Chat errors.
- **Server-side decision.** Whether to translate is a property of the resolved provider connection, cached in the DB and refreshed on a schedule, not something codex or any other client sees or votes on.
- **Fail-closed.** If translation encounters an input shape the shim can't map, the gateway returns a 501 with a Responses-shaped error naming the field. Better a hard fail than a silent fidelity loss.
- **Cost attribution intact.** Every translated call still writes one `llm_calls` row with a real `rate_card_hash` (never `failed-upstream`) and real `input_tokens` / `output_tokens` from the upstream's usage block. The shim does not distort billing.
- **Streaming first.** Codex is a streaming-native client; a non-streaming-only shim delivers no useful codex support. Streaming and non-streaming both land together in v1 — no half-baked v1 that only works for exotic non-streaming callers.

## Data model

Extend `provider_connections` (single Alembic revision):

| Column | Type | Notes |
|---|---|---|
| existing columns | | unchanged |
| `responses_api_supported` | `boolean NULL` | NEW. `NULL` = never probed / unknown; `TRUE` = probe reached a real Responses handler; `FALSE` = probe confirmed absent. |
| `responses_api_probed_at` | `timestamptz NULL` | NEW. Last probe time. Used by the refresh scheduler; NULL means "probe as soon as a call needs it". |
| `responses_api_probe_error` | `text NULL` | NEW. Last transport-level probe failure detail (`connect_error`, `read_timeout`, `unexpected_status:503`). Empty on success. |

No new tables. No changes to `llm_calls`, `trials`, or any downstream schema.

**Freshness policy** (implemented in the probe worker, not the schema):

- New `openai-compatible` connections: probed at creation as a fire-and-forget task; the first call blocks up to `PROBE_INITIAL_BLOCK_SEC` (default 5s) waiting for the probe to complete, then proceeds with the probe's answer or a fail-closed `FALSE`.
- Existing connections whose `responses_api_probed_at` is older than `PROBE_TTL_HOURS` (default 24): a background job re-probes.
- Manual: `POST /api/v1/provider-connections/{id}/probe-responses` forces a re-probe and returns the fresh result. Operators use this after upstream config changes at the provider side.

**Cache-inversion guard.** The gateway consults `responses_api_supported` under a bounded staleness window (default 5 minutes) — beyond that the value is treated as unknown and the shim runs a synchronous probe within the request path (blocking up to 5s). This is what keeps a stale `TRUE` from silently reintroducing the original hang.

## Components

### 1. Probe worker

`src/loom_llm_gateway/probe/responses_api.py` (new). Async coroutine:

- Fetches connections whose `responses_api_probed_at IS NULL OR now() - responses_api_probed_at > $ttl`.
- For each, issues `POST {base_url}/v1/responses` with a minimal payload (`model: <cached-model-id>`, `input: "ok"`, `max_output_tokens: 1`) and the connection's decrypted API key, 5s timeout.
- Classifies the response: `200 / 400 / 401 → supported`; `404 / 501 → unsupported`; `5xx / transport error → unsupported (fail-closed)`.
- Updates `responses_api_supported`, `responses_api_probed_at`, `responses_api_probe_error` atomically.

Runs both as a scheduled loop (every `PROBE_TTL_HOURS`) and as an on-demand endpoint. Rate-limited per connection to avoid tightloop probes during outages.

### 2. Gateway request router

`src/loom_llm_gateway/routes/responses.py` (modify). At entry to the existing Responses handler:

- Resolve the provider connection from the step-JWT's `provider_connection_id` claim.
- If `provider_connection.responses_api_supported IS TRUE` (fresh) → existing native pass-through path.
- If `provider_connection.responses_api_supported IS FALSE` (fresh) → dispatch to the translator.
- Otherwise (unknown / stale) → synchronous probe (5s timeout) then dispatch based on result.

### 3. Request translator: Responses → Chat

`src/loom_llm_gateway/facade/responses_to_chat.py` (new). Pure function `translate_request(responses_body: ResponsesRequest) -> ChatCompletionsRequest`.

Field mapping (v1 scope):

| Responses field | Chat Completions field | Notes |
|---|---|---|
| `model` | `model` | Verbatim. |
| `instructions` (string) | prepended to `messages` as `{"role": "system", "content": ...}` | If no instructions, no system message injected. |
| `input` (list of `{role, content: [{type, text \| image_url \| ...}]}`) | `messages` (list of `{role, content: string}` or `{role, content: [multimodal parts]}`) | Each Responses input item becomes one Chat message. Content parts of `type: input_text` collapse to a string; `type: input_image_url` maps to Chat's multimodal `image_url` part. |
| `input` items of `type: function_call` (assistant tool call) | `messages[i].tool_calls` on a synthesized assistant message | Requires threading a Responses-side `call_id` into Chat's `tool_calls[].id`. |
| `input` items of `type: function_call_output` (tool result) | `messages[i]` with `role: "tool"` + `tool_call_id` | Preserves `call_id → tool_call_id` continuity. |
| `tools[] (type=function)` | `tools[]` with `{"type": "function", "function": {...}}` | Responses puts `name`/`description`/`parameters` at the top level; Chat nests under `function`. |
| `tool_choice` | `tool_choice` | `"auto"` / `"required"` / `"none"` verbatim; `{"type": "function", "name": "..."}` becomes `{"type": "function", "function": {"name": "..."}}`. |
| `max_output_tokens` | `max_tokens` | Renamed. |
| `temperature`, `top_p`, `presence_penalty`, `frequency_penalty`, `seed`, `stop` | same names | Passed through. |
| `reasoning: {effort}` | dropped (see Fidelity losses) | Chat providers that don't implement OpenAI-Responses semantics simply don't honor a reasoning field; passing it unmodified causes 400 on many providers. |
| `stream` | `stream` | Verbatim. |
| `response_format` (Responses supports structured output as `text.format`) | `response_format` at the top level of Chat | Verbatim schema; providers that lack support 400, which we surface. |

Any Responses field the translator does not know about causes a 501 with `detail: "unsupported field in Responses translation: <field>"`. Better to fail loud than to silently drop.

### 4. Response translator: Chat → Responses

`src/loom_llm_gateway/facade/chat_to_responses.py` (new). Two entry points:

**Non-streaming**: `translate_response(chat_body: ChatCompletionsResponse) -> ResponsesResponse`

| Chat field | Responses field | Notes |
|---|---|---|
| `id` | `id` (prefixed `resp_`) | Preserves upstream id for debugging; prefix marks the shim path. |
| `model` | `model` | Verbatim. |
| `choices[0].message.content` | `output[0]` = `{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": ...}]}` | Wraps into Responses' output-item envelope. |
| `choices[0].message.tool_calls[i]` | `output[N]` = `{"type": "function_call", "call_id": ..., "name": ..., "arguments": ...}` | One output item per tool call, appended in order. |
| `choices[0].finish_reason` | `output[?].status` + top-level `status` | Chat's `"stop"` → Responses' `"completed"`; `"length"` → `"incomplete"` + `incomplete_details: {reason: "max_output_tokens"}`; `"tool_calls"` → `"in_progress"` semantics. |
| `usage.prompt_tokens` | `usage.input_tokens` | Verbatim rename. |
| `usage.completion_tokens` | `usage.output_tokens` | Verbatim rename. |
| `usage.completion_tokens_details.reasoning_tokens` (if present) | `usage.output_tokens_details.reasoning_tokens` | Passed through when the upstream exposes it. |

**Streaming**: `translate_stream(chat_sse: AsyncIterator[bytes]) -> AsyncIterator[bytes]`

Chat SSE emits `chat.completion.chunk` events with `choices[0].delta.content` accumulating token by token. Responses SSE emits a strict event sequence: `response.created` → `response.output_item.added` (for the assistant message) → `response.output_text.delta` (per token) → `response.output_text.done` → `response.output_item.done` → `response.completed`, plus separate item lifecycles for each tool call.

The stream translator holds a small state machine per response:

1. On the first Chat chunk with `choices`, emit `response.created` + `response.in_progress`.
2. Emit `response.output_item.added` for a message item; then per Chat `delta.content` emit one `response.output_text.delta` event.
3. When Chat emits a `delta.tool_calls[i]` fragment, buffer per tool-call index. Emit `response.output_item.added` for the function_call item on first fragment; emit `response.function_call_arguments.delta` for each argument fragment.
4. On the final Chat chunk (`finish_reason` set), close open items in reverse order with `.done` events and emit `response.completed` carrying the mapped `status` and the aggregated `usage`.

Reasoning-content deltas: if a Chat provider emits an OpenAI-compatible `delta.reasoning_content` fragment, the translator emits `response.reasoning.delta` and closes with `response.reasoning.done`. Providers that emit reasoning in the plain `content` stream (glm-5.1-thinking style) are surfaced as regular `output_text.delta` — no attempt to reverse-engineer reasoning vs. answer.

### 5. Cost attribution

Unchanged path. The gateway's existing usage-accounting layer reads `usage` off the (translated) response. Because we rewrite Chat's `prompt_tokens` / `completion_tokens` into Responses' `input_tokens` / `output_tokens` at the response level, the downstream `llm_calls` row is populated by the same code that populates it for native Responses — no branch, no duplicate accounting.

### 6. Admin UI

Provider Connections admin page (`web/src/pages/ProviderConnections.tsx`, modify):

- Show a per-connection column: `Responses API` with values `native`, `translated`, `unknown`, `probe-failed(<error>)`.
- Add a "Re-probe" button that calls `POST /api/v1/provider-connections/{id}/probe-responses` and refreshes.

## Data flow

### Request

```
codex → POST /openai/v1/responses (via step-JWT auth)
gateway → resolve provider_connection from JWT
gateway → check responses_api_supported (fresh if within staleness window)
    IF NULL / stale → synchronous probe (5s)
    IF TRUE  → native pass-through (unchanged existing path)
    IF FALSE → shim path:
        translate_request(responses_body) → chat_body
        POST {upstream}/v1/chat/completions (stream=? unchanged)
        IF stream:
            translate_stream(chat_sse) → responses_sse → client
        ELSE:
            translate_response(chat_body) → responses_body → client
        write llm_calls row (same code path as native)
```

### Probe

```
scheduler tick every PROBE_TTL_HOURS/2
    for each openai-compatible connection whose probe is stale or missing:
        POST {base_url}/v1/responses (minimal payload, 5s timeout)
        classify status → supported / unsupported / unknown
        UPDATE provider_connections SET responses_api_supported=?, responses_api_probed_at=now(), responses_api_probe_error=?
```

## Error handling

| Condition | Behavior |
|---|---|
| Upstream returns 4xx on translated Chat request | Passed through as Responses-shaped error (same wrapping the native path does today). |
| Upstream returns 5xx after gateway's existing retry budget | Gateway returns 502 with Responses envelope: `{"error": {"code": "upstream_5xx", "message": "translated_call_failed", "upstream_status": N}}`. |
| Upstream returns 200 but body is missing `choices[0]` (broken provider) | 502 with `code: "upstream_shape_error"`. |
| Translator hits an unmapped Responses input field | 501 with `code: "unsupported_input_field", detail: "<field-path>"`. Loud failure by design (Non-goals #3). |
| Translator hits an unmapped Chat response field | Emit a `logger.warning` and pass through as best-effort. Not a client-facing error — the client only sees what the translator can express. |
| Probe timeout during synchronous fallback | Log + treat as unsupported for THIS call (dispatch to translator); do not blacklist the connection persistently on a single-request timeout. |
| Request has `stream: true` and translator crashes mid-stream | Emit a `response.failed` event + close the connection. Downstream `llm_calls` writes with a failure marker (existing gateway pattern for aborted streams). |
| Provider connection's cached model list doesn't include the requested model | Existing gateway rejection path — no shim involvement. |

## Migration

Single Alembic revision:

1. `ALTER TABLE provider_connections ADD COLUMN responses_api_supported boolean NULL`
2. `ALTER TABLE provider_connections ADD COLUMN responses_api_probed_at timestamptz NULL`
3. `ALTER TABLE provider_connections ADD COLUMN responses_api_probe_error text NULL`
4. Create index `ix_provider_connections_probe_freshness` on `(responses_api_probed_at)` for the scheduler's freshness query.
5. Backfill: leave all existing rows with `NULL` — the first request against each connection triggers a probe.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `LOOM_GW_RESPONSES_PROBE_TTL_HOURS` | 24 | Background re-probe cadence per connection. |
| `LOOM_GW_RESPONSES_PROBE_TIMEOUT_SEC` | 5 | Per-attempt timeout for the outbound probe. |
| `LOOM_GW_RESPONSES_STALENESS_MAX_MIN` | 5 | Beyond this age, cached `responses_api_supported` is treated as unknown and a synchronous probe runs inside the request. |
| `LOOM_GW_RESPONSES_TRANSLATION_ENABLED` | true | Global kill-switch. When false, the shim never dispatches and the gateway behaves exactly as today — useful during rollback. |

## Fidelity losses

Documented, not smuggled:

- **Reasoning traces from Chat-only providers.** glm-5.1-thinking (and similar) emit thinking in the `content` stream. The shim surfaces it as regular `output_text` — the client cannot distinguish reasoning from answer. This is the honest translation; anything cleverer would require the shim to run a language classifier over model output, which is out of scope.
- **`response_format` strictness.** Chat providers that don't implement OpenAI's `response_format` return 400. The shim does not fabricate structured output from unstructured content.
- **Streaming timing.** Responses events arrive on a slightly different cadence than Chat deltas because of the state-machine buffering (batch-per-tool-call). Latency measurements taken end-to-end will show ~10-50ms per response of shim overhead.

## Testing

### Unit

- `translate_request`: one test per row in the field-mapping table above, both positive (well-formed input → expected Chat body) and negative (bad shape → 501 error).
- `translate_response` non-streaming: usage rename, message-content wrapping, single tool-call wrapping, multiple tool-calls with order preserved, `finish_reason` status mapping (`stop` → `completed`, `length` → `incomplete`, `tool_calls` → `in_progress`).
- `translate_stream`: harness that feeds a canned Chat SSE tape (real captures from openai / groq / yibuapi via Chat) and asserts on the emitted Responses event sequence. Golden fixtures per provider so translations don't drift silently.
- Probe classifier: parametrized over `200/400/401/403/404/429/500/501/502/503/504`, plus `TimeoutException`, `ConnectError`.

### Integration (testcontainers Postgres + a mock upstream FastAPI serving `/v1/chat/completions` only)

- End-to-end codex-shaped request → mock Chat upstream → client receives Responses envelope.
- Probe worker: seeded stale connection → tick → row updated.
- Synchronous fallback: connection with `responses_api_probed_at = NULL` → first request runs the probe inside the handler; subsequent requests use the cached bool.
- Streaming round-trip: mock upstream that emits `chat.completion.chunk` SSE → gateway translates → assert client receives well-formed `response.output_text.delta` events with the expected accumulated text.

### Golden

- Fixture pairs `(responses_request.json, expected_chat_request.json)` for every documented shape variant. Regenerated only via a review-gated `scripts/regen_responses_fixtures.py`.

### Security

- Probe uses the connection's stored key, never a step-JWT — probe traffic must not leak trial context.
- Translator never logs message content in warning/error paths (existing gateway redaction rules apply).
- SSE stream failures don't leak partial upstream tokens if the client aborts mid-stream (existing gateway pattern).

### CI gates

- Existing gateway test suite passes untouched (the shim is opt-in via connection metadata, so native-Responses providers exercise unchanged code).
- New `translation` suite passes.
- Golden fixtures unchanged unless explicitly regenerated.

## Rollout

1. Ship migration + probe worker + connection metadata (no gateway routing change). Verify probing populates the columns correctly across all existing connections.
2. Ship the translators with `LOOM_GW_RESPONSES_TRANSLATION_ENABLED=false` (feature-flag off). Run the translator unit + integration suites in CI; no live traffic touched.
3. Enable on a single low-traffic connection first (`mz_tn_canada_qianyi`); run the sample-tasks smoke from #266 with `codex + glm-5.1-thinking`; validate real completions land in `llm_calls`.
4. Enable globally.
5. If any regression surfaces, flip `LOOM_GW_RESPONSES_TRANSLATION_ENABLED=false` and every connection reverts to native pass-through.

## Open questions for the plan stage

- Which Chat providers require a bespoke response translator vs. the generic one? Groq's finish-reason semantics differ slightly from OpenAI's; yibuapi's usage block occasionally lacks `completion_tokens_details`. First-class fixtures will surface these; per-provider translator variants may or may not be needed.
- Should the streaming state machine live in its own package for reuse (e.g. by a future Anthropic-to-Chat shim), or stay coupled to this route? Both defensible; punt to plan stage.
- Does the probe payload need model-specific tuning? Some providers 400 the trivial `input: "ok"` for reasoning models that require specific `reasoning` fields. Fallback: probe with two payload shapes, treat 400 on both as success (endpoint exists), 404/501 on both as absent.

## Future scope (informational)

- **Anthropic-to-Chat shim.** Same shape, different translator. If future Anthropic-dialect adapters need to work against Chat-only providers, this doc's architecture extends without change — just add `anthropic_api_supported` + a symmetric translator.
- **Structured output emulation.** For providers without `response_format`, a wrapper prompt + response regex could approximate it. Non-goal for now; documented so the gap is visible.
