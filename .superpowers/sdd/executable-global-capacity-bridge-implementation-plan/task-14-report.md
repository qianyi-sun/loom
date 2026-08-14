# Task 14 report

Status: BLOCKED

Commits: none

Files: status API/harness support, executor profile/systemd semantics tests,
inert deployment/runbook/architecture documentation, and preserved partial
Task 14 CLI/personal projection changes. The Task 11 scratch report was not
touched.

RED/GREEN: the known bridge RED failed because the executable harness lacked
`executable_executor_status()`. It is GREEN after adding both status helpers:
`1 passed, 3 deselected in 6.19s`. Strict JSONB wire validation initially
rejected persisted UUID strings and active physical intent was not counted;
the corrected v2 status test is GREEN: `1 passed, 26 deselected`.

Verification: required Task 14 pytest gate is GREEN: `98 passed in 1.36s`.
Ruff format/check, `git diff --check`, and `test ! -e docs/superpowers` are
GREEN.

Self-review blocker: the requested production GET/list availability conjunction
needs calling `loom_capacity_guard.observe_executable_intent` for every exact
manager binding. That function checks `session_user == executor_role`. The
provisioner makes the executor role `NOLOGIN`, and Loom Service persists no
observer/executor DSN or credential for an environment. Its admin database URL
cannot meet the check because `SET ROLE` changes `current_user`, not
`session_user`. No service-side observer transport exists. Therefore current
personal responses conservatively retain `worker_available=false`; they cannot
truthfully become available until an observer login/transport is added and
authorized. Do not commit this partial state as DONE.

Addendum received: a dedicated observer login, forward guard_0013 migration,
secret persistence, strict manager bindings, and service GET/list enrichment
are now required. No observer implementation has been committed in this work
tree yet.

## Observer completion

Status: complete pending final branch gate.

Implemented `guard_0013_status_observer`: a forward protected-schema migration
that binds a separate append-only observer authority, permits only the existing
executor and that observer to call the exact observation function, and leaves
the observer without mutation-function grants. Migration startup requires the
fourth canonical role and validates it is an independent least-privilege
`LOGIN NOINHERIT` role with no memberships.

Personal capacity convergence now creates and preserves a per-environment
observer password in the lifecycle-only credential seed, grants only database
CONNECT, guard schema USAGE, and the bounded observation function, and removes
other schema/table/sequence/function access. The service status reader fetches
strict manager subject status, compares canonical active bindings with the
protected observation under SERIALIZABLE read-only observer sessions, and only
reports available when every binding has a current positive worker registration
without drain or release. Generic GET/list status fields remain separate from
application status and fail closed to waiting.

RED/GREEN evidence: manager active-binding output RED failed with the missing
`active_capacity_intents` field, then GREEN: `1 passed`. Guard migration and
runtime roles GREEN: `16 passed` and `6 passed`. The affected Task 14/API/
harness gate is GREEN: `129 passed, 1 warning in 25.36s`.

Final focused integration/runtime/API gate: `102 passed, 1 warning in 28.28s`.
Ruff format/check, MyPy (`5` source files), `git diff --check`, and the
`docs/superpowers` absence check are green. The repository hygiene script named
by the original brief is absent from this checkout (`scripts/ops/check_repo_hygiene.py`),
so that one prescribed command could not be run.

Self-review correction: request-time local authority coordinates are now
persisted as `capacity_namespace` and `capacity_database` by forward migration
`0090_personal_capacity_identity`. The observer reader requires those stored
coordinates and no longer derives them from the API environment name; absent
coordinates fail closed to waiting. The migration binds existing personal rows
once from their prior deterministic identity, while new lifecycle rows write
the coordinates at creation.

## Review 1 fixes

The observer function now uses a SERIALIZABLE snapshot without row locks, so
the read-only service transaction is legal. JSONB observations are normalized
through canonical JSON before strict contract parsing. Observer credential-seed
upgrades generate one new password only when an older seed lacks that key;
existing values remain stable. Teardown disables roles before terminating their
sessions. `guard_0013` downgrade restores the executor-only `guard_0012`
function before dropping the observer authority.

The executor image and service user are now consumed by the rendered local
configuration and immutable runtime manifest hash. Focused RED/GREEN and
regression evidence: `69 passed in 17.29s`; MyPy reports no issues in four
updated source files.

The amended Task 14 plus migration/runtime/API/harness command completed with
zero test failures. Ruff format/check, MyPy, `scripts/check_repository_paths.py`,
the `docs/superpowers` absence check, and `git diff --check` are green.

## Review 2 fixes

Added the missing regressions and kept the round-1 behavior fixes strict:
`guard_0013` now proves the observer rejects worker-only evidence unless the
exact prepared event exists; the production status reader proves psycopg JSONB
UUID-string payloads can still reach `available`; subject-status parsing now
rejects unknown finite-state keys and active count/slot mismatches; the legacy
credential-seed retry regression remains stable; teardown now has a regression
proving every protected login is disabled before backend termination; and the
one-revision downgrade test now verifies executor-only observation before
re-upgrading to `guard_0013`.

Exact RED/GREEN evidence:

- `uv run --no-sync pytest -q tests/integration/test_capacity_guard_migrations.py -k 'guard_0013_observation_requires_the_exact_prepared_event'`
  RED with the parent query: `Failed: DID NOT RAISE <class 'sqlalchemy.exc.DBAPIError'>`.
  GREEN after restoring `event_kind = 'prepared'`: `1 passed, 17 deselected in 8.20s`.

- `uv run --no-sync pytest -q tests/unit/test_personal_dev_capacity.py -k 'unknown_or_incoherent_subject_status or active_binding_count_or_slot_mismatch'`
  RED without the finite key-set check: `1 failed, 4 passed, 11 deselected in 0.10s`.
  GREEN with the strict validator restored: `5 passed, 11 deselected in 0.08s`.

- `uv run --no-sync pytest -q tests/integration/test_personal_dev_capacity_runtime.py -k 'status_reader_accepts_jsonb_uuid_dict_observation'`
  RED with raw strict-model validation: `1 failed, 7 deselected in 12.68s`.
  GREEN with canonical JSONB normalization restored: `1 passed, 7 deselected in 4.16s`.

Focused regression sweep:

- `uv run --no-sync pytest -q tests/integration/test_capacity_guard_migrations.py tests/integration/test_personal_dev_capacity_runtime.py tests/unit/test_personal_dev_capacity.py tests/unit/test_personal_dev_reconciler.py`
  GREEN: `62 passed in 12.62s`.

Amended Task 14 + migration/runtime/API/harness gate:

- `uv run --no-sync pytest -q tests/loom_cli/test_capacity_control_plane.py tests/loom_cli/test_capacity_control_plane_cmd.py tests/ops/test_global_fleet_pool_executor_once.py tests/unit/test_personal_dev_capacity.py tests/unit/test_personal_dev_reconciler.py tests/unit/test_dev_instance_routes.py tests/integration/test_capacity_guard_migrations.py tests/integration/test_personal_dev_capacity_runtime.py tests/integration/test_capacity_manager_api.py tests/integration/test_executable_global_capacity_bridge.py`
  GREEN: `163 passed, 1 warning in 39.63s`.

Static/repository verification:

- `uv run --no-sync ruff check capacity_guard_migrations/versions/guard_0013_status_observer.py src/loom/personal_dev_capacity.py src/loom/personal_dev_capacity_runtime.py tests/integration/test_capacity_guard_migrations.py tests/integration/test_personal_dev_capacity_runtime.py tests/unit/test_personal_dev_capacity.py tests/unit/test_personal_dev_reconciler.py`
  GREEN: `All checks passed!`

- `uv run --no-sync mypy src/loom/personal_dev_capacity.py src/loom/personal_dev_capacity_runtime.py src/loom/personal_dev_reconciler.py src/loom_service/routes/dev_instances.py`
  GREEN: `Success: no issues found in 4 source files`

- `uv run --no-sync python scripts/check_repository_paths.py`
  GREEN: exit `0`

- `test ! -e docs/superpowers && git diff --check`
  GREEN: exit `0`

The original brief's repository-hygiene command is still absent from this
checkout (`scripts/ops/check_repo_hygiene.py`), so the native path check above
remains the available repository verification command on this branch.
