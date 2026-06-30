# #217 G3 + G4 + G5 evidence — Terminal-Bench-2 on public-beta cluster

Real end-to-end evidence captured against the deployed public-beta
Loom cluster (`https://yylx.world`, kind cluster `loom-public-beta`).
Each artifact pair is a full ATIF v1.7 metadata + trajectory event log
from a `loom eval batch create` submission via the public-beta service.

| Task | Trial id | State | Reward | Wall clock | Covers |
|---|---|---|---|---|---|
| `terminal-bench-2/hello-world` | `f582ddff-a79e-495a-94b2-cc4868834c4f` | succeeded | 1.0 | ~25 s | G3 easy |
| `terminal-bench-2/simple-web-scraper` | `18d65c23-7959-437b-b8db-8885298e1f9f` | succeeded | 1.0 | ~60 s | G3 hard + G4 sidecar |
| `terminal-bench-2/security-vulhub-minio` | `f72be417-39be-45fa-9275-eb9aae0bf83e` | succeeded | 1.0 | ~40 s | G4 sidecar |
| `terminal-bench-2/simple-sheets-put` | `d474765a-cd71-427d-a2c0-d08fddb1bf65` | succeeded | 1.0 | ~60 s | G4 sidecar |

All four trials exercise the full chain:
`task bundle → resolve_task_image → image build → workspace materialize
→ OracleAgent runs solve.sh → verifier_shim.sh → run-tests.sh →
VerifierResult emitted → ATIF + trajectory written to MinIO`.

## Adapter fix required for these to score 1.0 (#240)

Initial public-beta smokes against this cluster shipped with
`state=succeeded, reward=0.0`. The trajectory showed `solve.sh exited 0
with 69 bytes of stderr` — the bash invocation
`bash $task_root/solution/reference.sh` resolved to the broken path
`/app/solution/solution/reference.sh` because `task_root` came from
`$(pwd)` and `OracleAgent` invokes `solve.sh` with cwd=`solution/`.
`exit 0` masked the error so trials looked successful but the
reference solution never ran. PR #240 fixes the wrapper to anchor at
`BASH_SOURCE[0]` and chdir to the task root before invoking the
reference solution. The 4 trials above were captured **after** the fix
was applied (via re-publish + re-register against the same cluster).

## G5 — MinIO mirror + audit

```
$ loom datasets audit terminal-bench-2 --verify-bundles
BENCHMARK        READINESS   RAW VALID SCHEMES      BLOCKER
terminal-bench-2 runnable     86    86 s3           -      
bundle_presence s3_tasks=86 verified=86 missing=0
```

86 tasks registered, 86 valid, 86 bundles verified in the in-cluster
MinIO, 0 missing.

## G9 — resource budget profiling

Easy task (`hello-world`): ~25 s wall clock.
Hard task (`simple-web-scraper`): ~60 s wall clock.
Sidecar tasks: ~40-60 s each.

All four trials completed within the upstream-declared
`max_agent_timeout_sec` and `max_test_timeout_sec` budgets — no
overrides required for the public-beta sandbox class.

## Still open after this batch

- **G6 — provider × TB-2 matrix.** The public-beta service has only
  one provider configured (`mz_tn_canada_qianyi`, openai-compatible).
  No Anthropic provider key is provisioned, so the Claude SKU matrix
  cannot run end-to-end yet. Unblocking is a config action
  (`loom providers create --type anthropic`), not a code change.
- **#222 — Layer 3 real-model alignment.** Still pending budget
  sign-off for the 86-task real-provider run.
