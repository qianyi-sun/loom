# #217 G3 evidence — Terminal-Bench-2 hello-world succeeds end-to-end

> Archived rollout evidence summary.

Partial evidence for issue [#217](https://github.com/qianyi-sun/loom/issues/217)
**G3** (live cluster end-to-end). Captured against a local dev-compose
Loom deployment — not the public-beta cluster — but exercises the same
worker code path (`src/loom_worker/task_image.py:resolve_task_image` →
build → trial → `verifier_shim.sh` → `VerifierResult`).

| File | What |
|---|---|
| `tb2-hello-world-atif.json` | ATIF v1.7 metadata for trial `7e1659af-91de-47b7-876d-345e56189fc0`. `final_state: succeeded`, `reward: {"resolved": 1.0}`. |
| `tb2-hello-world-trajectory.jsonl` | Event log including the oracle agent's `env_exec` (rc=0) and the verifier's `step_end` emitting `{'resolved': 1.0}`. |

Trial fingerprint:

- Task: `terminal-bench-2/hello-world`
- Agent: `oracle` (out-of-box)
- Submitted: 2026-06-19 20:41:33 UTC
- Finished: 2026-06-19 20:41:38 UTC (~5.9 s wall-clock)
- Pinned upstream: `terminal-bench-core` v0.1.1, commit `91e10457`

## Public-beta gap

Closing #217 G3 requires the equivalent evidence on the deployed
public-beta cluster, plus a representative long task (e.g.
`simple-web-scraper`). See `docs/runbooks/operator-runbook.md` §
"Terminal-Bench 2.0 public-beta readiness" for the one-command-per-step
recipe.
