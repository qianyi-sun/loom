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

## Fix round 1: complete outage/restart matrix and exact release digest

Added missing public-boundary coverage:

- Prepared-unused now restarts the executor after protected bootstrap revocation before reporter publication/release convergence.
- Unregistered-withdrawn now covers manager outage before reporter publication, response loss after manager acknowledgement, reporter restart before cursor acknowledgement, stable idempotency keys, replay acknowledgement, and executor restart after protected withdrawal.
- Added a paired fresh-run determinism test for prepared-unused and unregistered-withdrawn. It compares canonical allocation, inventory, and exact release digests derived from the actual `ExecutableProtectedReleaseV2` payload persisted by the manager.
- Tightened outage handling so the helper catches only the expected `DemandPublishError` transport failure, not arbitrary runtime failures.

Root-cause notes:

- The first new RED failed because the harness only exposed allocation/inventory plus a synthesized evidence digest, not the exact release digest crossing the public manager boundary.
- After adding exact release digests, the repeat proof exposed nondeterministic protected evidence. Prepared release varied through test database URL/root material in the activation/admission boundary; withdrawn release additionally varied through signed launch `submitted_at`. The harness now offers deterministic task DB suffixes for repeat-proof runs, uses fixed task-owned repeat roots, and fixes the executor clock at the harness boundary while still exercising the real executor, guard store, reporter runtime, manager ASGI route, journal, handoff, and fake Slurm boundary.

Fresh verification on 2026-08-15:

```bash
uv run --no-sync pytest -q tests/integration/test_executable_global_capacity_bridge.py -k 'prepared_unused_retirement or unregistered_withdrawn_retirement or canonical_digests_are_stable' -s --tb=short
# RED: 3 failed, 6 deselected; missing ProtectedReleaseReplayEvidence.release_digest

uv run --no-sync pytest -q tests/integration/test_executable_global_capacity_bridge.py -k 'prepared_unused_retirement or unregistered_withdrawn_retirement or canonical_digests_are_stable' -s --tb=short
# GREEN: 3 passed, 6 deselected in 30.70s

uv run --no-sync pytest -q tests/integration/test_executable_global_capacity_bridge.py -k 'prepared_unused or unregistered_withdrawn or protected_release' -s
# 4 passed, 5 deselected in 35.17s

uv run --no-sync pytest -q tests/integration/test_executable_global_capacity_bridge.py -k 'prepared_unused or unregistered_withdrawn or protected_release' -s
# 4 passed, 5 deselected in 27.20s

uv run --no-sync pytest -q tests/unit/test_capacity_agent_*.py tests/unit/test_capacity_executor_*.py tests/integration/test_capacity_agent_*.py tests/integration/test_capacity_guard_migrations.py tests/integration/test_capacity_manager_execution_store.py tests/integration/test_capacity_manager_execution_epoch.py tests/integration/test_executable_global_capacity_bridge.py tests/ops/test_global_fleet_pool_executor_once.py
# 624 passed in 131.60s (0:02:11)

uv run --no-sync ruff format --check src/loom_capacity_executor/executable.py src/loom_capacity_manager/execution_store.py tests/support/executable_capacity_harness.py tests/integration/test_executable_global_capacity_bridge.py
# 4 files already formatted

uv run --no-sync ruff check src/loom_capacity_executor/executable.py src/loom_capacity_manager/execution_store.py tests/support/executable_capacity_harness.py tests/integration/test_executable_global_capacity_bridge.py
# All checks passed!

uv run --no-sync mypy src/loom_capacity_executor/executable.py src/loom_capacity_manager/execution_store.py
# Success: no issues found in 2 source files

git diff --check
```

Deterministic paired-run digests asserted by the new test in the second focused run:

- `prepared_unused_repeat`: allocation `79d2184b6567e72bfc2ab6086c2025df8ef704a87f955a3332c9bd6d989ca941`, inventory `895811ed81b6908a4ab1f2b3fb68e661d7613b0349509f917cd6219a66300773`, release `3a4cad43250f32750312641dd18b7007ceb6181f33445d553bcabf08a04fbccb`
- `unregistered_withdrawn_repeat`: allocation `79d2184b6567e72bfc2ab6086c2025df8ef704a87f955a3332c9bd6d989ca941`, inventory `fb4cac6b0e55fb691ea2511a0c5fa4a2ed232665c4baa1130c2d8b6a44866e56`, release `925526a54760ddeea4c70c7fcf486726ff7e647eac038a71b039ee3656ba7181`

Note: the raw one-off prepared/withdrawn release digest lines outside the paired repeat test still include the current testcontainer endpoint through the real protected admission URL binding, so those raw single-test release digests are expected to differ between separate pytest processes. The paired repeat test keeps that boundary stable and asserts exact equality without normalizing the protected release contract.

## Fix round 2: delayed release sole-blocker proof and safe repeat namespace

Added missing public-boundary coverage:

- The reporter-delay flow now publishes Alice/oldlab's protected release through outage + response-loss replay, completes Alice's exact executor/manager release, and only then proves retirement remains blocked solely by Bob/gb10.
- The test records Bob/gb10 intent + pool snapshots before Alice publication, after Alice publication replay, and after Alice release; those snapshots must remain byte-for-byte equal while Bob is delayed.
- After Bob/gb10 publishes and releases, the flow verifies both manager commitments are gone, global retirement reaches `shadow`, and Alice/oldlab's released snapshot did not change during Bob convergence.
- The deterministic pair test now uses `tmp_path / "protected-release-repeat"` and database suffixes derived from the pytest-provided temporary directory hash. The two sequential executions in a pair still share identical root/suffix inputs for exact release-digest equality, while different pytest workers receive distinct roots and database names.

Focused RED evidence on 2026-08-15:

```bash
uv run --no-sync pytest -q tests/integration/test_executable_global_capacity_bridge.py -k 'reporter_delay or canonical_digests_are_stable' -s --tb=short
# FAILED tests/integration/test_executable_global_capacity_bridge.py::test_protected_release_reporter_delay_keeps_pools_isolated
# AssertionError: left commitments still included Alice/oldlab closing before release.
# FAILED tests/integration/test_executable_global_capacity_bridge.py::test_protected_release_canonical_digests_are_stable_across_fresh_runs
# AssertionError: PosixPath('/tmp/loom-task4-protected-release-repeat').is_relative_to(tmp_path) was false.
# 2 failed, 7 deselected in 19.12s
```

Focused GREEN evidence:

```bash
uv run --no-sync pytest -q tests/integration/test_executable_global_capacity_bridge.py -k 'reporter_delay or canonical_digests_are_stable' -s --tb=short
# CANONICAL_DIGEST prepared_unused_repeat allocation=79d2184b6567e72bfc2ab6086c2025df8ef704a87f955a3332c9bd6d989ca941 inventory=895811ed81b6908a4ab1f2b3fb68e661d7613b0349509f917cd6219a66300773 release=3d67f15ec49352830950ed5dddcba414485d58f1698577fc9c199fc5822246ab
# CANONICAL_DIGEST unregistered_withdrawn_repeat allocation=79d2184b6567e72bfc2ab6086c2025df8ef704a87f955a3332c9bd6d989ca941 inventory=fb4cac6b0e55fb691ea2511a0c5fa4a2ed232665c4baa1130c2d8b6a44866e56 release=c4c1d23b2ae1345b068c2d9c12182caeb4e64852a2e1bb212bb5fc485619b420
# 2 passed, 7 deselected in 18.87s
```

Fresh verification on 2026-08-15:

```bash
uv run --no-sync ruff format tests/support/executable_capacity_harness.py tests/integration/test_executable_global_capacity_bridge.py
# 2 files left unchanged

uv run --no-sync pytest -q tests/integration/test_executable_global_capacity_bridge.py -k 'prepared_unused or unregistered_withdrawn or protected_release' -s
# CANONICAL_DIGEST prepared_unused allocation=79d2184b6567e72bfc2ab6086c2025df8ef704a87f955a3332c9bd6d989ca941 inventory=895811ed81b6908a4ab1f2b3fb68e661d7613b0349509f917cd6219a66300773 release=3c397290f2de33f650b52181e6179582bfc59d9485a1b656231029073594e2cd
# CANONICAL_DIGEST unregistered_withdrawn allocation=79d2184b6567e72bfc2ab6086c2025df8ef704a87f955a3332c9bd6d989ca941 inventory=fb4cac6b0e55fb691ea2511a0c5fa4a2ed232665c4baa1130c2d8b6a44866e56 release=a39849bbd87dd9dd7672eaa9ef65dfc822edbfa9ad47db8495df6c197f29bd85
# CANONICAL_DIGEST prepared_unused_repeat allocation=79d2184b6567e72bfc2ab6086c2025df8ef704a87f955a3332c9bd6d989ca941 inventory=895811ed81b6908a4ab1f2b3fb68e661d7613b0349509f917cd6219a66300773 release=60c15e838ef798795ea18b77766c37de14a9b01984f0345834b350dd93d29b82
# CANONICAL_DIGEST unregistered_withdrawn_repeat allocation=79d2184b6567e72bfc2ab6086c2025df8ef704a87f955a3332c9bd6d989ca941 inventory=fb4cac6b0e55fb691ea2511a0c5fa4a2ed232665c4baa1130c2d8b6a44866e56 release=68d4b1173d05069113d7ab49c7b8e4ad762d100347cd20294ecc919e791c106d
# 4 passed, 5 deselected in 23.56s

uv run --no-sync pytest -q tests/integration/test_executable_global_capacity_bridge.py -k 'prepared_unused or unregistered_withdrawn or protected_release' -s
# CANONICAL_DIGEST prepared_unused allocation=79d2184b6567e72bfc2ab6086c2025df8ef704a87f955a3332c9bd6d989ca941 inventory=895811ed81b6908a4ab1f2b3fb68e661d7613b0349509f917cd6219a66300773 release=4e64bbdf8f329b42abcdac3303cbd2ff18db7c34f8b7c0c86621b57d6ea3a652
# CANONICAL_DIGEST unregistered_withdrawn allocation=79d2184b6567e72bfc2ab6086c2025df8ef704a87f955a3332c9bd6d989ca941 inventory=fb4cac6b0e55fb691ea2511a0c5fa4a2ed232665c4baa1130c2d8b6a44866e56 release=74e395e4a0a700c59a88898a12bf6d7d91a73faf685097b07fa8e6b6dea370fb
# CANONICAL_DIGEST prepared_unused_repeat allocation=79d2184b6567e72bfc2ab6086c2025df8ef704a87f955a3332c9bd6d989ca941 inventory=895811ed81b6908a4ab1f2b3fb68e661d7613b0349509f917cd6219a66300773 release=c8e3f0e0bf9960cafb0efae4d7be1c34928f9fed683df74f7e6fcd12e708540c
# CANONICAL_DIGEST unregistered_withdrawn_repeat allocation=79d2184b6567e72bfc2ab6086c2025df8ef704a87f955a3332c9bd6d989ca941 inventory=fb4cac6b0e55fb691ea2511a0c5fa4a2ed232665c4baa1130c2d8b6a44866e56 release=45a1d6c4795dc7613e59a490b014fb70b56c4e453a04e2baa71fc146289e640e
# 4 passed, 5 deselected in 21.69s

uv run --no-sync pytest -q tests/unit/test_capacity_agent_*.py tests/unit/test_capacity_executor_*.py tests/integration/test_capacity_agent_*.py tests/integration/test_capacity_guard_migrations.py tests/integration/test_capacity_manager_execution_store.py tests/integration/test_capacity_manager_execution_epoch.py tests/integration/test_executable_global_capacity_bridge.py tests/ops/test_global_fleet_pool_executor_once.py
# 624 passed in 110.96s (0:01:50)

uv run --no-sync ruff format --check src/loom_capacity_executor/executable.py src/loom_capacity_manager/execution_store.py tests/support/executable_capacity_harness.py tests/integration/test_executable_global_capacity_bridge.py
# 4 files already formatted

uv run --no-sync ruff check src/loom_capacity_executor/executable.py src/loom_capacity_manager/execution_store.py tests/support/executable_capacity_harness.py tests/integration/test_executable_global_capacity_bridge.py
# All checks passed!

uv run --no-sync mypy src/loom_capacity_executor/executable.py src/loom_capacity_manager/execution_store.py
# Success: no issues found in 2 source files

git diff --check
# no output
```
