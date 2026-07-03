# Cost & rate cards

How Loom turns token counts into dollar amounts, why the user-facing
model is *usage frozen, cost derived*, and where the rate cards live in
each mode.

## The model: usage frozen, cost derived

Every LLM call freezes its raw token counts (input, output, cache
reads, cache writes, plus any dialect-specific extras) verbatim into
storage at the moment the response lands. Priced calls also freeze the
per-call `cost_usd` snapshot and `rate_card_hash` used at emit time.
Trial, batch, and usage read APIs project these rows into token totals,
estimated spend, and cost diagnostics; they do not hide token-only or
missing-rate-card calls behind a zero-dollar total.
If a provider omits all or part of its usage block, the Gateway records
that fact in `provider_extras` and the read APIs expose
`partial_usage_llm_calls_count`, `missing_usage_llm_calls_count`,
`failed_upstream_llm_calls_count`, `usage_reporting_status`, and
`usage_estimate_confidence` so callers can distinguish a complete estimate
from a lower-confidence token/cost projection.
If the Gateway attempted an upstream provider request and the upstream
returned an error or the provider transport failed, it records a
zero-token `llm_calls` audit row with `rate_card_hash=failed-upstream`
and `provider_extras._loom_call_status=failed`. Trial/batch debug
evidence surfaces these rows through call-status and failure-category
counts; usage/cost projections should treat them as audit evidence,
not billable provider usage.

```
provider response
     │
     ▼
TokenUsage ──► llm_calls row
               { input_tokens, output_tokens,
                 provider_extras, rate_card_hash,
                 cost_usd snapshot }
                     │
        ┌────────────┴────────────┐
        ▼                         ▼
 trial/batch usage projection     usage/rate-card cost view
 { total_prompt_tokens,           { derived spend totals,
   total_completion_tokens,         rate-card diagnostics,
   llm_calls_count,                 optional batch drilldown }
  estimated_cost_usd,
  cost_status,
  usage_estimate_confidence }
```

Why this shape:

- **Prices change**; historical trajectories should not become
  retroactively wrong. Re-pricing yesterday's run with today's
  table is a query, not a migration.
- **Provider SDKs evolve** — new dialect-specific token counters
  appear (`cache_creation_input_tokens`, `reasoning_tokens`,
  `thoughtsTokenCount`). The `provider_extras` JSONB column
  absorbs them without a schema change.
- **Trial/batch responses stay stable** — dashboards can distinguish
  "no calls were made", "self-deployed token-only model", and
  "rate-card lookup missed" using `llm_calls_count`, token totals,
  `cost_status`, and `pricing_modes`.
- **Failed attempts stay visible without becoming spend** — failed
  upstream attempts are counted as `llm_calls` for debug evidence, but
  carry zero tokens, zero cost, `rate_card_hash=failed-upstream`, and
  explicit failure metadata in `provider_extras`. Usage projections expose
  them as `pricing_modes=["failed-upstream"]` with
  `cost_status=failed_upstream`, not as priced provider usage.
- **Cost attribution stays auditable** — the Gateway records a
  per-call `cost_usd` snapshot and `rate_card_hash` for metrics and
  diagnostics. Consumers that need fleet-wide totals should query
  `/api/v1/usage`; trial and batch detail responses expose the same
  projection fields for local debugging.
- **Usage confidence is separate from pricing mode** — `cost_status`
  says whether Loom had a price source (`estimated`,
  `not_applicable`, `price_unknown`, `failed_upstream`, `mixed`);
  `usage_estimate_confidence` says whether the provider returned complete
  token usage (`high`, `partial`, `missing`, `none`).

(Harbor froze `cost_usd` at emit time. RFC0001 acknowledges this
goes wrong when prices move.)

## Rate card shape

Both CLI mode and service mode use the same Pydantic model:

```python
class RateCardEntry:
    provider: str          # e.g. "anthropic", "openai", "local:vllm"
    model: str             # e.g. "claude-opus-4-7"
    input_per_mtok: float
    output_per_mtok: float
    cache_read_per_mtok: float
    cache_write_per_mtok: float
    currency: str          # optional metadata, defaults to "USD"
    source_url: str | None
    pricing_version: str | None
    source_model: str | None
    pricing_unit: str | None
```

Cost formula (per call):

```
cost = (input_tokens  / 1_000_000) * input_per_mtok
     + (output_tokens / 1_000_000) * output_per_mtok
     + (cache_read    / 1_000_000) * cache_read_per_mtok
     + (cache_write   / 1_000_000) * cache_write_per_mtok
```

Cache tokens come from `provider_extras`; missing keys default to 0
so providers without cache counters compute correctly.

## Where the table lives

| Mode         | Source                                            | Override                              | Lookup                |
|--------------|---------------------------------------------------|---------------------------------------|-----------------------|
| CLI          | `~/.config/loom/rate-cards.toml`                  | hand-edit the file                    | `loom_cli.rate_cards` |
| Service      | `rate_cards` table in Postgres (JSONB payload)    | `loom_service` admin endpoint         | `loom_llm_gateway.rate_card` |
| Seed for CLI | `src/loom_cli/data/default-rate-cards.toml`       | copied on first run                   | —                     |

CLI: missing rate-card row → `KeyError` with a hint to add one.
Service: missing row → `RateCardNotFoundError` (HTTP 422 from the
Gateway), so a misconfigured provider fails fast instead of silently
recording $0.

Provider-connection facade routes use the same service table, but the
lookup key comes from the connection rather than the legacy
`provider/model` routing string. `provider_connections.rate_card_provider`
stores the provider namespace to use with the raw request model id. Safe
defaults are `anthropic`, `google`, and `openai` for
`openai-compatible`; `custom` has no default. When a facade connection is
set to `pricing_source='rate-card'` and no matching entry exists, the
gateway records tokens with `cost_usd=0`,
`rate_card_hash='facade:rate-card:missing'`, and provider extras marking
`_loom_cost_source=unpriced`,
`_loom_cost_confidence=unavailable`, and
`_loom_unpriced_reason=missing_rate_card_entry`. API projections expose
that as `estimated_cost_usd=null` and `cost_status=price_unknown` so an
unpriced model such as `glm-5.1-thinking` is never presented as a
zero-dollar run. Trial and batch responses still show the non-zero call
count and token totals in this case.

For hosted YibuAPI usage, sync the official pricing catalog into the
service rate-card table:

```bash
loom admin rate-cards sync-yibuapi
```

The service fetches `https://yibuapi.com/api/pricing`, converts token
quota models into USD-per-1M-token entries using YibuAPI's group ratio,
and stores the catalog with `source_url`, `pricing_version`,
`last_checked_at`, `currency`, `group`, `group_ratio`, entry count, and
skipped model count. Model lookup normalizes common prefixes such as
`yibuapi/<model>` and `models/<model>`. The synced card is auditable by
its stored JSON payload and `rate_card_hash`. Trial and batch detail
responses include `price_snapshots` for resolved historical hashes with
the rate-card id, source URL, pricing version, check time, currency,
group, and group ratio; unresolved non-facade hashes remain visible with
`resolved=false`.

## Usage API and CLI

`GET /api/v1/usage` is the admin/user API for totals. It supports:

- `team_id`, `user_id`, `provider_connection_id`, `model`,
  `benchmark_id`, `batch_id`, `status`, and `pricing_mode` filters.
  `pricing_mode` accepts `priced`, `tokens-only`, `price-unknown`, and
  `failed-upstream`.
- `breakdown_by=team|user|provider_connection|model|benchmark|batch|status|pricing_mode`.
- `include_batches=true` for per-batch drilldown when not using a
  breakdown dimension.

`loom eval usage` exposes the same filters and prints cost status plus
usage confidence. Use `--format json` for the full API payload,
including per-batch `partial_usage_llm_calls_count` and
`missing_usage_llm_calls_count`.

## Local / self-hosted rates

Provider key uses a `local:<server>` prefix to match the model spec
shape (`local/<server>/<model_id>` → provider `local:<server>`):

```toml
[[entries]]
provider = "local:vllm"
model = "meta-llama/Llama-3.1-8B-Instruct"
input_per_mtok = 0.10
output_per_mtok = 0.30
cache_read_per_mtok = 0.0
cache_write_per_mtok = 0.0
```

Local trials default to **$0** if no row matches — they don't
incur a real upstream cost. Add a row to attribute internal GPU
budget; leave it absent to ignore.

For BYO OpenAI-compatible services registered through
`loom providers create`, set `--rate-card-provider PROVIDER` when the
endpoint should use a hosted provider's rate-card namespace:

```bash
loom providers create \
  --name together-prod \
  --type openai-compatible \
  --base-url https://api.together.xyz/v1 \
  --api-key env:TOGETHER_API_KEY \
  --rate-card-provider together
```

The connection still defaults to `pricing_source='tokens-only'` for
OpenAI-compatible endpoints; switch it to `rate-card` only when the
service rate-card table has rows for that provider/model pair. For
user-managed or self-deployed APIs, keep `tokens-only`: Loom records
token totals and returns `estimated_cost_usd=null` with
`cost_status='not_applicable'` rather than inventing a dollar amount.
Operator-supplied/manual pricing is supported by setting
`pricing_source='operator-supplied'` with `pricing_data` keys
`input_usd_per_1m` and `output_usd_per_1m`; facade calls then carry
`cost_estimate_source='operator-supplied'` and
`cost_estimate_confidence='configured'`.
Rate-card metadata is optional for launch selection: BYO provider model
discovery and manual model ids are exposed through `/api/v1/models`
even when no matching rate-card entry exists. Missing facade pricing is
reported with `rate_card_hash='facade:rate-card:missing'` rather than
blocking evaluation.

The local CLI vLLM helper (`--model hf:<id>` / `--model /path/`)
registers as provider `local:_auto_vllm`. Rate-card rows for that
provider attribute internal cost across all hf:/path `loom run`
invocations. This is not hosted platform inference; service-mode teams
should register their own hosted or self-hosted endpoint through provider
connections. The inline `--local-server` flag registers as `local:_inline`.

## What `provider_extras` captures

Dialect-specific counters that don't fit `{input, output}` cleanly:

| Dialect          | Extras stored verbatim                                              |
|------------------|----------------------------------------------------------------------|
| Anthropic        | `cache_creation_input_tokens`, `cache_read_input_tokens`            |
| OpenAI Chat      | `prompt_tokens_details.cached_tokens`, `completion_tokens_details.reasoning_tokens` |
| OpenAI Responses | same as Chat, plus `output_tokens_details.reasoning_tokens`         |
| Gemini           | `cachedContentTokenCount`, `thoughtsTokenCount`                     |

The `cached_input_tokens` derived property on `TokenUsage` sums every
"this read from cache" counter across dialects so cost math doesn't
need a dialect switch.

## Re-pricing historical runs

Both modes support it:

- **CLI**: edit `~/.config/loom/rate-cards.toml`, re-run any
  trial-summary projection. ATIF v1.7 cost fields are projected from
  `events.jsonl` + the current rate card on read.
- **Service**: insert a new `rate_cards` row with `effective_at` set
  to today. The lookup is `MAX(effective_at) WHERE effective_at <=
  call_time`, so a new row prices calls made *after* its
  `effective_at` and leaves earlier calls priced against the prior
  row.

## What this is NOT

- **Not a billing system.** Cost is internal accounting against
  team quotas + the `/api/v1/usage` dashboard. Loom does not invoice.
- **Not a billing system budget.** Batch-level `budget_usd` is an
  operator safety guard. `hard` budgets reject over-budget pre-run
  estimates and cancel running batches after recorded provider usage
  exceeds the cap; `soft` budgets require explicit confirmation when
  the estimate is over the cap or unpriced. This is still internal
  spend control, not invoicing or per-team currency accounting.
- **Not currency-aware.** All amounts are USD by convention; no
  conversion or per-team currency.

## See also

- [`local-llm.md`](local-llm.md) — `local:<server>` provider naming
- [`llm-gateway.md`](llm-gateway.md) — where the lookup happens in
  service mode
- [`cli-mode.md`](cli-mode.md) — where the lookup happens in CLI mode
