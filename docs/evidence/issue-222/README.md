# #222 Layer 3 — Claude Haiku 4.5 × Terminal-Bench 2.0

Real Layer 3 alignment evidence captured against the deployed public-beta
Loom cluster (`https://yylx.world`).

## Run configuration

- **Batch id**: `81d3f790-a426-4c64-97aa-2ddf5a08a563`
- **Benchmark**: `terminal-bench-2` (all 86 tasks; `terminal-bench-core` v0.1.1, commit `91e10457`)
- **Agent**: `claude-code` (loom-launcher adapter wrapping the Anthropic-published `@anthropic-ai/claude-code` CLI)
- **Provider**: `yibuapi-anthropic-pb` (Anthropic-typed connection → `https://yibuapi.com`)
- **Model**: `claude-haiku-4-5`
- **Wall clock**: 2026-06-30 18:25 → 19:27 EDT (~62 minutes)

## Headline numbers

| Metric | Value |
|---|---|
| Total tasks submitted | 86 |
| `state=succeeded, reward=1.0` (passed verifier) | **35** |
| `state=succeeded, reward=0.0` (completed but verifier rejected) | 41 |
| `state=failed` (system error before verifier) | 12 |
| **Resolved rate (35 / 86)** | **40.70%** |
| Resolved rate over completed (35 / 74) | 47.30% |
| LLM calls | 1,924 |
| Tokens | 383,552 prompt / 278,184 completion |

## Comparison to upstream reference

Anthropic's published Claude Haiku 4.5 number on Terminal-Bench is **~40.2%** (non-thinking mode, Terminus-2 scaffold, 11-run average). Loom's single-run **40.70%** with the **claude-code** agent matches it almost exactly. That this is the same number across two very different agent scaffolds (Anthropic-published Terminus-2 vs Loom claude-code) suggests TB-2's verifier dominates the score for Haiku 4.5 — agent-runtime choice contributes much less variance than for SkillLearnBench / GPQA where the multi-pp Loom-vs-Harbor gap was attributed to single-shot vs tool-loop differences (see `docs/benchmark-score-alignment-layer3.md`).

Classification per the #32 Layer-3 rubric:

> classify as `layer3_validated` if |delta| <= 3pp, otherwise `layer3_delta_flagged`

**Delta = 0.50pp → `layer3_validated`.**

## Caveats + remaining work

1. **One-run vs 11-run reference**: Anthropic's number is an 11-run average. Loom's number is a single run; per-task stochasticity can shift the headline by a few pp. A re-run pair would tighten the confidence interval.
2. **Agent scaffold mismatch**: Anthropic used Terminus-2, Loom used claude-code. The new `terminus-2` adapter (PR #249, just merged) makes a proper apples-to-apples comparison runnable. Recommend re-running this batch with `--agent terminus-2 --model claude-haiku-4-5` once the worker rolls an image with the new install_script — that re-run is the canonical Layer-3 evidence for #222.
3. **System-failure rate is 14%** (12/86). Worth a separate look. The 12 failures are recorded in `per-task-results.json` with their `failure_reason`. None of the 12 reached verifier output, so the resolved rate denominator is debatable — see the 35/74 (47.30%) figure for the alternative.

## Files

- `per-task-results.json` — per-trial id, state, reward, llm_calls, failure_reason for all 86 tasks. Generated from `GET /api/v1/trials?batch_id=...` against the public-beta service.

## Related

- #248 / #249 ship the `terminus-2` adapter that closes the agent-scaffold caveat above.
- `docs/benchmark-score-alignment-layer3.md` is the home for the canonical Layer-3 writeup; this issue's results belong there once the terminus-2 re-run is in.
