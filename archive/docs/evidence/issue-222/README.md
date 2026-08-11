# #222 Layer 3 — Claude Haiku 4.5 × Terminal-Bench 2.0

> Archived dated benchmark report. It is not current release evidence.

Preliminary Layer 3 alignment evidence captured against the deployed public-beta
Loom cluster (`https://yylx.world`). This preserves the historical #222
claude-code run, but it is not canonical acceptance evidence because the checked-in
per-task JSON distinguishes reward-positive rows from clean platform-successful
trials and still contains system failures.

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
| `reward=1.0` rows (historical reward-positive headline) | **35** |
| Clean `state=succeeded, reward=1.0` rows | **33** |
| `state=succeeded, reward=0.0` (completed but verifier rejected) | 41 |
| Reward-positive rows later marked `state=failed` with `trajectory_flush_failed` | 2 |
| `state=failed` system failures | 12 |
| Historical reward-positive rate (35/86) | **40.70%** |
| Clean platform-successful reward-positive rate (33/86) | **38.37%** |
| LLM calls | 1,924 |
| Tokens | 383,552 prompt / 278,184 completion |

## Comparison to upstream reference

Anthropic's published Claude Haiku 4.5 number on Terminal-Bench is **~40.2%**
(non-thinking mode, Terminus-2 scaffold, 11-run average). Loom's single-run
historical reward-positive headline is **40.70%** with the **claude-code** agent,
or +0.50 pp against that reference. The cleaner platform-successful
`state=succeeded, reward=1.0` count is **33/86 = 38.37%** because two
reward-positive rows later failed with `trajectory_flush_failed`.

Classification per the #32 Layer-3 rubric is therefore
`preliminary_pending_terminus_2_rerun`, not final `layer3_validated`. The
historical 35/86 reward-positive headline is useful evidence, but the
trajectory flush failures, agent-scaffold mismatch, and single-run basis make it
preliminary/caveated rather than canonical #222 acceptance.

## Caveats + remaining work

1. **One-run vs 11-run reference**: Anthropic's number is an 11-run average.
   Loom's number is a single run; per-task stochasticity can shift the headline
   by a few pp. A re-run pair would tighten the confidence interval.
2. **Agent scaffold mismatch**: Anthropic used Terminus-2, Loom used
   claude-code. Harbor-embedded `terminus-2` makes a proper apples-to-apples
   comparison runnable once staging GB10 workers roll a worker image built from
   `deploy/Dockerfile.worker` with pinned Harbor `@527d50d`.
   Recommend re-running this batch with
   `--agent terminus-2 --model claude-haiku-4-5` after that rollout — that
   re-run is the canonical Layer-3 evidence for #222. See
   [`architecture/terminus2-runtime.md`](../../../../docs/architecture/terminus2-runtime.md).
3. **System-failure rate is 14%** (12/86). The 12 `state=failed` rows are
   recorded in `per-task-results.json` with their `failure_reason`; two of them
   are reward-positive `trajectory_flush_failed` rows. This is why the
   historical 35/86 reward-positive headline and the 33/86 clean
   platform-successful count must stay separate.

## Files

- [`per-task-results.json`](per-task-results.json) — per-trial id, state,
  reward, llm_calls, and failure reason for all 86 tasks. Generated from
  `GET /api/v1/trials?batch_id=...` against the public-beta service.

## Related

- The Harbor-embedded `terminus-2` builtin closes the agent-scaffold caveat
  above (superseding the earlier launcher adapter path).
- `docs/score-alignment/layer-3.md` records this result as
  preliminary/caveated evidence; the Terminus-2 re-run remains the canonical
  Layer-3 acceptance path for #222.
