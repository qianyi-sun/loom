# Cost & rate cards

How Loom turns token counts into dollar amounts, why the model is
*usage frozen, cost derived*, and where the rate cards live in each
mode.

## The model: usage frozen, cost derived

Every LLM call freezes its raw token counts (input, output, cache
reads, cache writes, plus any dialect-specific extras) verbatim into
storage at the moment the response lands. **Cost is not stored** —
it's derived at query time from a versioned rate card.

```
  provider response                           query time
       │                                          │
       ▼                                          ▼
  TokenUsage  ──►  llm_calls row          rate_card lookup
                   { input_tokens,        (provider, model,
                     output_tokens,    ×   effective_at)
                     provider_extras,         │
                     ... }                    ▼
                          \             { input_per_mtok,
                           \              output_per_mtok,
                            \             cache_read_per_mtok,
                             ▼            cache_write_per_mtok }
                       cost_usd  ◄────  compute_cost_usd(...)
```

Why this shape:

- **Prices change**; historical trajectories should not become
  retroactively wrong. Re-pricing yesterday's run with today's
  table is a query, not a migration.
- **Provider SDKs evolve** — new dialect-specific token counters
  appear (`cache_creation_input_tokens`, `reasoning_tokens`,
  `thoughtsTokenCount`). The `provider_extras` JSONB column
  absorbs them without a schema change.
- **Cost attribution stays auditable** — a per-call breakdown is
  reproducible because the inputs to the formula are all in the
  row.

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

The managed-vLLM launcher (`--model hf:<id>` / `--model /path/`)
registers as provider `local:_auto_vllm`. Rate-card rows for that
provider attribute internal cost across all hf:/path runs. The
inline `--local-server` flag registers as `local:_inline`.

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
- **Not a budget guard.** There's no enforcement that a team stays
  under a dollar cap. Rate limiting is per-(team, provider) RPM at
  the Gateway, not $/day. (Tracked as a follow-up if needed.)
- **Not currency-aware.** All amounts are USD by convention; no
  conversion or per-team currency.

## See also

- [`local-llm.md`](local-llm.md) — `local:<server>` provider naming
- [`llm-gateway.md`](llm-gateway.md) — where the lookup happens in
  service mode
- [`cli-mode.md`](cli-mode.md) — where the lookup happens in CLI mode
