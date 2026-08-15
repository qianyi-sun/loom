# Task 4 Report: End-to-End Release and Retirement Proof

## Summary

Implemented executable global-capacity protected release proof coverage for the two zero/partial-physical retirement paths:

- Prepared-but-unused intent: protected prepare + bootstrap handoff, drain before consumption, protected bootstrap revocation, guard outbox publication, reporter publication through the manager ASGI route, exact unused release, final inventory/heartbeat, and retirement to shadow with zero Slurm mutation.
- Unregistered-withdrawn intent: pending Slurm submission with protected physical bind, no worker registration/credential/claim, drain withdrawal, cancellation of only the exact pending fake-Slurm job, terminal accounting inventory, protected release publication, final inventory/heartbeat, and retirement to shadow.
- One-pool reporter delay: mixed oldlab/gb10 retirement remains blocked after only one owner/pool publishes, then retires only after both protected release publications converge, with no cross-owner binding.

Production changes were limited to RED-test-exposed bridge behavior:

- Executor runtime now accepts immutable retained prepared/active artifacts when current authority has advanced monotonically to drain-only.
- Routed admission clients expose prepared-bootstrap revocation.
- Executor inventory/release confirmation matching ignores the non-semantic executable marker and accepts idempotent duplicate same-sequence confirmations only when the canonical inventory payload is identical.
- Manager retirement safety now allows released unused intents to be absent from final physical inventory when the unused terminal evidence is exact and sequenced before the final inventory.

## TDD / regression evidence

Focused RED history was captured during development before the GREEN changes:

- Initial prepared-unused test failed on missing harness/reporter wiring.
- After harness wiring, prepared-unused failed because retained execution comparison treated `executable=True` as a semantic execution change.
- After executor matching repair, retirement still failed because manager final-inventory safety required unused terminal intents to appear as physical inventory rows.
- The one-pool delay scenario exposed the real global launch ordering constraint; the test was adjusted to a realistic mixed flow where the earlier gb10 rank progresses to pending submission before oldlab prepares unused.

## Determinism

Required focused command was run twice:

```bash
uv run --no-sync pytest -q tests/integration/test_executable_global_capacity_bridge.py -k 'prepared_unused or unregistered_withdrawn or protected_release_reporter' -s
```

Both runs passed with stable canonical allocation/inventory digests:

- `prepared_unused`: allocation `79d2184b6567e72bfc2ab6086c2025df8ef704a87f955a3332c9bd6d989ca941`, inventory `895811ed81b6908a4ab1f2b3fb68e661d7613b0349509f917cd6219a66300773`
- `unregistered_withdrawn`: allocation `79d2184b6567e72bfc2ab6086c2025df8ef704a87f955a3332c9bd6d989ca941`, inventory `fb4cac6b0e55fb691ea2511a0c5fa4a2ed232665c4baa1130c2d8b6a44866e56`

The printed evidence digests varied between runs because runtime/journal timestamps are intentionally part of the evidence payload.

## Fresh verification on 2026-08-15

Passed:

```bash
uv run --no-sync ruff check src/loom_capacity_agent src/loom_capacity_executor tests
# All checks passed!

uv run --no-sync mypy src/loom_capacity_agent src/loom_capacity_executor
# Success: no issues found in 35 source files

git diff --check

uv run --no-sync pytest -q tests/integration/test_executable_global_capacity_bridge.py -k 'prepared_unused or unregistered_withdrawn or protected_release_reporter' -s
# 3 passed, 5 deselected in 16.16s

uv run --no-sync pytest -q tests/unit/test_capacity_agent_*.py tests/unit/test_capacity_executor_*.py tests/integration/test_capacity_agent_*.py tests/integration/test_capacity_guard_migrations.py tests/integration/test_capacity_manager_execution_store.py tests/integration/test_capacity_manager_execution_epoch.py tests/integration/test_executable_global_capacity_bridge.py tests/ops/test_global_fleet_pool_executor_once.py
# 623 passed in 100.75s (0:01:40)

uv run --no-sync ruff format --check src/loom_capacity_executor/executable.py src/loom_capacity_executor/runtime.py src/loom_capacity_manager/execution_store.py tests/support/executable_capacity_harness.py tests/integration/test_executable_global_capacity_bridge.py
# 5 files already formatted

test ! -e docs/superpowers
```

Known gate issues:

```bash
uv run --no-sync ruff format --check src/loom_capacity_agent src/loom_capacity_executor tests
# exit 1: 419 unrelated files would be reformatted; task-touched files listed above are already formatted.

uv run --no-sync python scripts/ops/check_repo_hygiene.py
# exit 2: scripts/ops/check_repo_hygiene.py does not exist in this worktree.

rg --files | rg '(^|/)check_repo_hygiene\.py$|repo_hygiene'
# exit 1: no matching hygiene script found.
```

## Safety / boundary notes

- No live Kubernetes, Slurm, systemd, deployment, or production database mutation was used.
- The tests use real guard migrations/store functions, the real protected-release reporter runtime, manager ASGI route publication, real executor journal/handoff stores, and the fake Slurm subprocess boundary.
- Tests do not directly call manager protected-release acknowledgement helpers or mutate intent state in fixtures.
