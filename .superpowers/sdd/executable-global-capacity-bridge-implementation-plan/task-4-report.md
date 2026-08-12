# Task 4 Report: Executable Reservation Store and Pool Work Queue

## Status

Blocked on two Important follow-up review findings. Task 4 is implemented on the dedicated executable-v2 ledger. The
checked-in default remains inert (`executable_new_capacity_ceiling = 0`), the
v1 protocol remains permanently dry-run, no activation route was added, and
no external scheduler mutation is performed.

Implementation commit: `bb433da4a223a3e7e8bb774a4b3adfccc1d3cec4`

## Changed files

- `capacity_migrations/versions/capacity_0004_executable_bridge.py`
- `src/loom_capacity_manager/api.py`
- `src/loom_capacity_manager/auth.py`
- `src/loom_capacity_manager/executable_contracts.py`
- `src/loom_capacity_manager/execution_store.py`
- `src/loom_capacity_manager/models.py`
- `src/loom_capacity_manager/ownership.py`
- `tests/integration/test_capacity_management_migrations.py`
- `tests/integration/test_capacity_manager_api.py`
- `tests/integration/test_capacity_manager_execution_store.py`
- `tests/unit/test_capacity_auth.py`
- `tests/unit/test_capacity_manager_executable_contracts.py`

## Requirement mapping

- A separate executable-v2 store and schema own immutable executor bindings,
  mutable lease/checkpoint/inventory state, intents, command receipts, and
  isolated executable rate buckets. The v1 dry-run tables and behavior are not
  widened.
- All mutations use SERIALIZABLE transactions, database time, exact execution,
  allocation, executor, pool/generation, lease, command-sequence, and replay
  guards. Equivocation and journal divergence durably fence after the rejected
  transaction rolls back.
- The bounded pool queue emits at most one proposal, bootstrap binding, permit,
  close, or release; it preserves global launch-rank order and blocks later
  work behind unresolved earlier work.
- Proposal acceptance, bootstrap registration, permit issue/consumption,
  close, protected release, and physical release are durable and replay-safe.
  Permit consumption rechecks fresh inventory, physical headroom, slot-based
  activation ceiling, scoped pending limits, durable scoped rate tokens,
  subject/candidate/profile/trusted-release bindings, and central launch order.
- Drain-only transitions retain old active intent bindings only for monotonic
  close/release work. Freeze/zero ceiling removes unaccepted proposals and
  does not expose new increase work. Expired permits are deterministically
  reissued.
- Ownership evidence must verify with the registered Ed25519 key and exact
  controller, trusted launcher, association, submitter, authority scope,
  resources, and nodes before terminal/release evidence is accepted.
- Strict pool/generation-bound v2 heartbeat, checkpoint, work, inventory,
  acceptance, bootstrap, permit-consumption, close, release, and protected
  release routes are present. There is no activation route.
- Migration/ORM parity includes exact composite foreign keys and downgrade
  ordering. Downgrade refuses any retained execution evidence, including a
  prepared epoch without executable allocations.
- Health remains tied to the current writer fence and reports the live
  executable ceiling; a valid active ceiling no longer makes the manager
  falsely not-ready.

## TDD / RED evidence

- Initial required RED: `ModuleNotFoundError: No module named
  'loom_capacity_manager.execution_store'`.
- Subsequent focused REDs demonstrated missing v2 transitions/routes,
  generation binding, protected release, active-freeze close behavior,
  drain-only close rejection, missing durable rate consumption, missing
  pending enforcement, cross-executor acceptance, active health rejection,
  rolled-back equivocation fencing, and expired-permit deadlock.
- Each production correction was followed by its focused passing regression
  before the broader gates were rerun.

## Review

Independent review compared base
`6f395a14867dcbcb66633432a76cc1ddd59b925f` with the implementation. Its two
Critical findings (rolled-back executor fencing and row-count rather than
slot-count ceiling) and all Important findings were resolved. Resolutions also
cover observed-pending quota accounting, expired permits, cross-pool ordering,
fresh inventory/headroom, mixed-shape selection, generation-bound acceptance,
freeze cleanup, complete ownership checks, and downgrade evidence retention.

Final follow-up verdict: **BLOCKED**. No Critical findings remain. The reviewer
reported these unresolved Important findings verbatim:

1. `execution_store.py:1499-1518 excludes state='closing' from ceiling charge,
   although closing may still await protected/physical release; design lines
   241-243 requires charge until both release fences. This can authorize
   replacement capacity before old capacity is released.`
2. `execution_store.py:1464-1472,1532-1540 reduces fresh commitments to one
   pool-wide resource sum and compares against pool-wide totals, ignoring
   commitment/binding node_ids. Capacity concentrated on binding.node_ids can
   oversubscribe those exact nodes while aggregate pool headroom remains.`

Minor: `models.py:1787-1789/1924-1925 and migration lines 344-347/453-454 add
last_inventory_at and observed_state without DB consistency/domain checks.`

## Verification

- Required Task 4 gate:
  `uv run --no-sync pytest -q tests/integration/test_capacity_manager_execution_store.py tests/integration/test_capacity_manager_api.py tests/unit/test_capacity_executor_dry_run.py`
  — `43 passed`, one pre-existing Starlette/httpx deprecation warning.
- Proportional regression/migration gate:
  `uv run --no-sync pytest -q tests/unit/test_capacity_auth.py tests/unit/test_capacity_manager_executable_contracts.py tests/unit/test_capacity_manager_executable_allocator.py tests/integration/test_capacity_management_migrations.py tests/integration/test_capacity_manager_execution_epoch.py tests/integration/test_capacity_management_store.py`
  — `99 passed`.
- Ruff format check — 12 files already formatted.
- Ruff lint — all checks passed.
- Mypy over the six changed source modules — no issues.
- `git diff --check 6f395a14..HEAD` — clean.

## Concerns

- The test environment emits one pre-existing Starlette `TestClient` / httpx
  deprecation warning.
- Two Important review findings remain unresolved pending fix-loop direction.
- Live activation and physical scheduler mutation remain intentionally outside
  Task 4 and are not exposed by this commit.
