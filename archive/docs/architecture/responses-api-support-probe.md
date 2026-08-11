# Proactive probe for Responses API support (extends the existing Chat-fallback shim)

> Archived implementation design. Current gateway behavior is documented in
> `docs/architecture/responses-api.md`.

Status: design (no implementation). Extends [`llm-gateway.md`](../../../docs/architecture/llm-gateway.md) and the existing `src/loom_llm_gateway/routes/responses_chat_compat.py` shim.

## Goal

Make codex (and any future Responses-API-consuming agent) work against BYO OpenAI-compatible providers whose upstream never implements `POST /v1/responses` and whose failure mode is *not* the specific 400-with-`missing messages` shape the existing fallback recognises. Yibuapi is the concrete driver — it returns 504 (or hangs) on `/v1/responses` rather than the 400 signature — so the existing shim never triggers and codex burns its full agent budget with zero completions.

The change is small: add a **proactive per-connection probe** so the gateway knows, before the first Responses call for a given upstream, whether that upstream implements Responses at all. When it doesn't, dispatch straight into the existing translation path.

## Prior art (already in the gateway)

The gateway ALREADY implements Responses ↔ Chat translation. See `src/loom_llm_gateway/routes/responses_chat_compat.py`:

- `responses_payload_to_chat_completion` — request translator (Responses body → Chat body)
- `chat_completion_to_responses` — non-streaming response translator
- `_append_response_item_sse_events` — streaming state machine (Chat SSE → Responses SSE)
- `_chat_usage_to_responses_usage` — usage rename
- `synthetic_responses_http_response` — full HTTP response synthesis around the translated body/stream

`src/loom_llm_gateway/routes/responses.py` invokes this fallback via `should_fallback_to_chat_completions(upstream_response, payload)`, which triggers only on a 400 with a very specific "missing `messages` parameter" signature from the upstream. Yibuapi returns 504 (upstream timeout) instead, so the current heuristic never fires. This spec closes that gap.

**What this spec does NOT redesign:**

- The request-side field mapping (Responses → Chat). Already implemented and covered by unit tests under `responses_chat_compat`. Any correctness gaps in the existing mapping are separate bugs, not this spec's scope.
- The response-side rename (Chat → Responses). Already implemented.
- The streaming state machine. Already implemented.
- The cost-attribution flow. Already reads `usage` off the (translated) response body.

If the existing translators have field-coverage gaps that surface once real yibuapi traffic hits them, they're logged as follow-ups against `responses_chat_compat.py`, not this spec.

## Non-goals

- Fix upstream provider behavior. If yibuapi decides to implement Responses natively, the next probe flips `responses_api_supported` to `TRUE` and the gateway resumes native pass-through.
- Rewrite the translator. The existing `responses_chat_compat` implementation is the source of truth; any correctness gaps land as separate PRs, not part of this design.
- Handle non-OpenAI dialects. Anthropic and Google routes stay untouched. Only `POST /openai/v1/responses` is affected.
- Emulate features Chat Completions cannot express. Where Responses depends on capabilities Chat lacks (structured reasoning output events, fine-grained streaming semantics), the existing translator degrades gracefully with documented losses.
- Retrofit codex adapter-side fallback. Codex 0.141+ upstream requires `wire_api = "responses"`; there is no client-side switch.

## Design principles

- **Client-transparent.** The Responses request and the Responses response the client sees must be indistinguishable from a native-Responses upstream on every field the client actually consults. Failure modes exposed to the client are Responses-shaped errors, not translated Chat errors.
- **Server-side decision.** Whether to translate is a property of the resolved provider connection, cached in the DB and refreshed on a schedule, not something codex or any other client sees or votes on.
- **Fail-closed.** If translation encounters an input shape the shim can't map, the gateway returns a 501 with a Responses-shaped error naming the field. Better a hard fail than a silent fidelity loss.
- **Cost attribution intact.** Every translated call still writes one `llm_calls` row with a real `rate_card_hash` (never `failed-upstream`) and real `input_tokens` / `output_tokens` from the upstream's usage block. The shim does not distort billing.
- **Reuse over rebuild.** Translation already works in `responses_chat_compat.py`. This spec's contribution is the probe + the dispatch trigger — not new translators. Anything that reads like "and we'll also translate ..." is a signal that the doc has drifted.

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

- New `openai-compatible` connections: probed at creation as a fire-and-forget task; the first call blocks up to `PROBE_INITIAL_BLOCK_SEC` (default 5s) on an `asyncio.Event` registered against the connection id in a process-local `dict[UUID, asyncio.Event]`. The probe coroutine `set()`s the event after it commits the DB update, so any awaiting call proceeds with a fresh cached bool. If the process is restarted mid-probe (event registry lost), a follow-up request simply re-enters the synchronous fallback path and re-probes — no correctness issue, just an extra 5s on that specific request.
- Existing connections whose `responses_api_probed_at` is older than `PROBE_TTL_HOURS` (default 24): a background job re-probes.
- Manual: `POST /api/v1/provider-connections/{id}/probe-responses` forces a re-probe and returns the fresh result. Operators use this after upstream config changes at the provider side.

**Cache-inversion guard.** The gateway consults `responses_api_supported` under a bounded staleness window (default 5 minutes) — beyond that the value is treated as unknown and the shim runs a synchronous probe within the request path (blocking up to 5s). This is what keeps a stale `TRUE` from silently reintroducing the original hang.

## Components

### 1. Probe worker

`src/loom_llm_gateway/probe/responses_api.py` (new). Async coroutine:

- Fetches connections whose `responses_api_probed_at IS NULL OR now() - responses_api_probed_at > $ttl`.
- For each, decrypts the API key via `SecretStore` (`src/loom/security/secret_store.py`), then issues `POST {base_url}/v1/responses` with an **empty JSON body (`{}`)** — no model reference required — under a 5s timeout.
- Classifies the response: `200 / 400 / 401 → supported` (the endpoint parsed the request enough to reject it on body or auth); `404 / 501 → unsupported`; `5xx / transport error → unsupported (fail-closed)`.
- Updates `responses_api_supported`, `responses_api_probed_at`, `responses_api_probe_error` atomically.

The empty-body probe payload is deliberate: it side-steps the chicken-and-egg of "which model do we name for a brand-new connection whose `provider_models_cache` hasn't been populated yet". A real Responses handler always returns 400 (validation error) for `{}`; a missing endpoint always returns 404 (route error). Providers that require auth before body validation return 401; that still proves the endpoint exists.

Runs both as a scheduled loop (every `PROBE_TTL_HOURS`) and as an on-demand endpoint. Rate-limited per connection to at most one probe every `PROBE_MIN_INTERVAL_SEC` (default 60s) so a synchronous fallback path during an outage cannot beat the DB with retries.

### 2. Gateway request router

`src/loom_llm_gateway/routes/responses.py` (modify). At entry to the existing Responses handler:

- Resolve the provider connection from the step-JWT's `provider_connection_id` claim. The claim is minted into the step-JWT by the Control Plane whenever the trial's `provider_connection_id` is set (`src/loom/auth.py::mint_step_jwt`, part of issue #72's JWT-scope binding). The gateway's existing `resolve_provider_connection_id()` at `src/loom_llm_gateway/routes/_facade_common.py:110` already performs this lookup for every facade call.
- If `provider_connection.responses_api_supported IS TRUE` (fresh) → existing native pass-through path.
- If `provider_connection.responses_api_supported IS FALSE` (fresh) → dispatch to the translator.
- Otherwise (unknown / stale) → synchronous probe (5s timeout) then dispatch based on result.

### 3. Reused translation (already present)

Not reimplemented by this spec. When the router (§2) decides to translate, it invokes the existing helpers:

- Request: `responses_payload_to_chat_completion(payload)` at `responses_chat_compat.py`
- Response (non-streaming): `chat_completion_to_responses(chat_body, ...)`
- Response (streaming): `synthetic_responses_http_response(...)` producing a Responses-shaped SSE stream via `_append_response_item_sse_events`
- Usage rename: `_chat_usage_to_responses_usage(usage)`

The existing coverage of tool calls, structured output, `finish_reason` mapping, and `usage_body` handling all applies unchanged.

If a specific yibuapi (or other provider) traffic pattern surfaces a field the existing translators don't handle, that's tracked as a defect against `responses_chat_compat.py`, not this spec.

### 4. Admin UI

Provider Connections admin page (`web/src/pages/ProviderConnections.tsx`, modify):

- Show a per-connection column: `Responses API` with values `native`, `translated`, `unknown`, `probe-failed(<error>)`.
- Add a "Re-probe" button that calls `POST /api/v1/provider-connections/{id}/probe-responses` and refreshes.

## Data flow

### Request

```
codex → POST /openai/v1/responses (via step-JWT auth)
gateway → resolve provider_connection from JWT (existing helper)
gateway → check responses_api_supported (fresh if within staleness window)
    IF NULL / stale → synchronous probe (5s)
    IF TRUE  → native pass-through (existing path, unchanged)
    IF FALSE → invoke existing shim helpers:
        responses_payload_to_chat_completion(payload) → chat_payload
        POST {upstream}/v1/chat/completions (stream flag preserved)
        synthetic_responses_http_response(...) → client
        (llm_calls row written by existing accounting layer)
```

The only new line in this flow is the `responses_api_supported` check and the branch it drives. Every other step is either unchanged or a direct call into `responses_chat_compat.py`.

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
| `LOOM_GW_RESPONSES_PROBE_MIN_INTERVAL_SEC` | 60 | Per-connection rate limit; the probe worker refuses to re-probe a connection more than once per this window even if requests keep asking. Prevents a synchronous-fallback storm during an upstream outage from beating the DB. |
| `LOOM_GW_RESPONSES_PROBE_INITIAL_BLOCK_SEC` | 5 | How long a request may wait on the per-connection `asyncio.Event` while a fresh probe is in flight. |

## Fidelity notes

The existing translator's fidelity envelope is unchanged by this spec — dispatching to it more often does not alter what it does. The known losses (documented here for transparency, not proposed as new behavior):

- **Reasoning traces from Chat-only providers.** glm-5.1-thinking (and similar) emit thinking in the `content` stream. The translator surfaces this as regular `output_text` — the client cannot distinguish reasoning from answer.
- **`response_format` strictness.** Providers that don't implement OpenAI's `response_format` return 400. The translator does not fabricate structured output from unstructured content.
- **Streaming cadence.** SSE events arrive slightly reshaped by `_append_response_item_sse_events`; end-to-end latency measurements will show a small buffering overhead.

If any of these losses turn out to matter for a specific downstream (e.g. glm-thinking becomes the flagship model and reasoning-vs-answer segmentation matters), the fix lands in `responses_chat_compat.py` — not here.

## Testing

The bulk of the translator test coverage already exists under `responses_chat_compat`'s own unit suite. This spec adds tests only for the genuinely new code paths.

### Unit

- Probe classifier: parametrized over `200/400/401/403/404/429/500/501/502/503/504`, plus `TimeoutException`, `ConnectError`.
- Cache staleness: cached-fresh, cached-stale, cache-miss branches of the router each dispatch correctly.
- `asyncio.Event` handshake: multiple concurrent requests for the same new connection all wake on one probe completion; the DB is written once.

### Integration (testcontainers Postgres + a mock upstream FastAPI serving `/v1/chat/completions` only, returning yibuapi-shaped 504 on `/v1/responses`)

- End-to-end: probe classifies the mock upstream as unsupported → subsequent codex-shaped request dispatches through the existing translator → client receives Responses envelope with correct content and usage.
- Probe worker: seeded stale connection → scheduler tick → row updated with fresh timestamp and correct `responses_api_supported`.
- Synchronous fallback: connection with `responses_api_probed_at = NULL` → first request runs the probe inside the handler; the next request within the staleness window uses the cached bool.
- On-demand endpoint: `POST /api/v1/provider-connections/{id}/probe-responses` re-probes and returns the fresh result.

### Regression against the existing translator

- `LOOM_GW_RESPONSES_TRANSLATION_ENABLED=false` case: gateway behaves exactly as today, including the existing 400-signature fallback. No metric or `llm_calls` divergence for currently-working providers.
- `LOOM_GW_RESPONSES_TRANSLATION_ENABLED=true` with a provider whose `responses_api_supported=NULL`: first request probes; if upstream implements Responses natively, next request goes through the existing native path — no unnecessary translation invoked.

### Security

- Probe uses the connection's stored key from `SecretStore`, never a step-JWT — probe traffic must not leak trial context.
- Existing gateway redaction rules cover the translator; nothing added here changes them.

### CI gates

- Existing gateway test suite passes untouched.
- New `probe` and `dispatch` suites pass.
- `responses_chat_compat` unit tests unchanged (regression coverage that translators still work).

## Rollout

1. **Schema + probe worker only.** Ship the migration and the probe worker; no gateway routing change yet. Verify probing populates the columns correctly across all existing connections. Native providers should show `TRUE`; yibuapi should show `FALSE`.
2. **Wire the dispatch behind the flag.** Ship the router changes with `LOOM_GW_RESPONSES_TRANSLATION_ENABLED=false`. Confirm no behaviour change on any live traffic. Run the new probe/dispatch integration suite in CI.
3. **Single-connection enable.** Flip the flag globally but scoped to `mz_tn_canada_qianyi` only via an operator override table (or a per-connection `override_translation_enabled bool`; TBD in plan stage). Run a currently supported v1.0 smoke benchmark such as MBPP or HumanEval with `codex + glm-5.1-thinking`; verify real completions land in `llm_calls` with non-zero token counts.
4. **Global enable.** Remove the per-connection scope; the flag governs everything.
5. **Rollback path.** `LOOM_GW_RESPONSES_TRANSLATION_ENABLED=false` reverts every connection to today's behaviour (native pass-through + the existing 400-signature fallback). No DB state to unwind.

## Open questions for the plan stage

- **Rollout-stage scoping.** Step 3 (`single-connection enable`) needs a mechanism to enable translation for one connection only without the global flag. Two candidates: (a) an operator-side allowlist env var like `LOOM_GW_RESPONSES_TRANSLATION_CONNECTIONS=<uuid,uuid,...>`, or (b) an explicit `override_translation` column on `provider_connections`. Both are simple; decide during plan writing.
- **Interaction with the existing 400-signature fallback.** When `responses_api_supported = TRUE` (fresh) but the native call happens to fail with the specific 400 signature `should_fallback_to_chat_completions` catches, do we still trigger the existing per-request fallback, OR does a `TRUE` probe result mean "always native, never fall back"? Current lean: keep the 400-signature fallback in place as a defence-in-depth for provider drift.
- **Probe rate-limit persistence.** `PROBE_MIN_INTERVAL_SEC` is enforced in-process. Across multiple gateway pods, the DB serves as a soft coordination point (probes update `responses_api_probed_at`); a stricter cross-pod rate limit would need a Postgres advisory lock. Probably unnecessary for v1; document as a knob for later.

## Future scope (informational)

- **Anthropic-to-Chat translation.** Would follow the same probe + dispatch shape (`anthropic_api_supported` column, probe worker against `POST {base}/anthropic/v1/messages`, dispatch into a symmetric translator that does not yet exist). Only worth building if a real trial uses an Anthropic-dialect agent against a Chat-only upstream.
- **Structured output emulation.** For providers without `response_format`, a wrapper prompt + response regex could approximate it. Non-goal for now; documented so the gap is visible.
