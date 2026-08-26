# Concurrent-owner personal development zero-capacity acceptance implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an exact two-owner acceptance-plan and result-evidence contract,
prove bidirectional cross-owner isolation through the supported CLI, and retain
the byte-exact inert rollback and global executable-capacity ceiling `0`.

**Architecture:** Keep schema-v1 single-owner acceptance byte-compatible and
add schema-v2 with exactly two canonical distinct owners. Reuse the existing
acceptance render/runtime interlock; add strict lifecycle/result evidence and
secret-free JSON identity output, then update the bounded #1280 runbook to run
two sessions concurrently, prove six denied cross-owner operations leave target
state unchanged, clean up normally, and restore the exact shadow.

**Tech Stack:** Python 3.11, Pydantic v2, dataclasses, argparse, canonical JSON,
pytest, Ruff, mypy, Bash, jq, Kubernetes read-only/status commands.

**Spec:**
`docs/architecture/personal-dev-concurrent-owner-zero-capacity-acceptance-design.md`

## Global Constraints

- The global executable-new-capacity ceiling remains exactly `0`.
- Never activate a worker, execute or submit a task, call `sbatch`/`scancel`,
  enable a pool executor, or enable physical OLDLAB/GB10 capacity.
- Live personal lifecycle activity is permitted only inside the bounded #1280
  acceptance window and must end with byte-exact inert-shadow restoration.
- `loom-dev` remains shared infrastructure only; personal applications use
  `loom-dev-<name>` and builders use bounded `loom-build-*` sandboxes.
- `min_slots` remains configurable with default `0`; this acceptance always
  sends exact minimum `0`. There are no pool weights.
- Preserve schema-v1 acceptance canonical bytes and behavior. Do not reinterpret
  an old `acceptance_owner` record as a two-owner authorization.
- Schema-v2 authorizes exactly two owners with distinct nonzero user IDs and
  distinct nonzero team IDs, sorted by canonical `(team_id, user_id)` text.
- Never print or retain bearer tokens, session cookies, CSRF values, Secret
  values, private keys, kubeconfig contents, or database credentials.
- All local plans, results, configuration roots, and detailed logs are
  current-user-owned, non-symlink, single-link, and mode `0600`/`0700` as
  appropriate.
- Rendering, result verification, and status are read-only. The runbook is the
  only surface that applies Kubernetes manifests or invokes lifecycle commands.
- Plans and design records stay under `docs/architecture`; never create
  `docs/superpowers`.

---

### Task 1: Versioned exact two-owner acceptance plan

**Files:**

- Modify: `src/loom/personal_dev_control_plane_config.py`
- Modify: `tests/unit/test_personal_dev_control_plane_acceptance_config.py`
- Modify: `tests/unit/test_personal_dev_control_plane_render.py`
- Modify: `tests/unit/test_personal_dev_control_plane_status.py`
- Modify: `tests/loom_cli/test_personal_dev_control_plane_cmd.py`

**Interfaces:**

- Keeps schema-v1
  `load_personal_dev_acceptance_plan(path, expected_sha256)` byte-compatible.
- `PersonalDevAcceptancePlan.acceptance_owners -> tuple[PersonalDevAcceptanceOwner, ...]`
  returns one owner for v1 and exactly two for v2.
- `PersonalDevAcceptancePlan.acceptance_owner` returns the one v1 owner and
  raises `PersonalDevAcceptancePlanError` for v2 so no caller silently selects
  one concurrent owner.
- Schema-v2 canonical JSON replaces `acceptance_owner` with exactly
  `acceptance_owners` and otherwise keeps the existing fields unchanged.

- [ ] **Step 1: Add failing v1 compatibility and v2 owner-contract tests**

  Preserve one exact v1 fixture and derive v2 with:

  ```python
  value["schema_version"] = 2
  owner_0 = value.pop("acceptance_owner")
  owner_1 = {
      "team_id": "00000000-0000-0000-0000-000000000006",
      "user_id": "00000000-0000-0000-0000-000000000005",
  }
  value["acceptance_owners"] = sorted(
      [owner_0, owner_1],
      key=lambda item: (item["team_id"], item["user_id"]),
  )
  value["quotas"]["global_live_instances"] = 2
  value["quotas"]["builder_global_concurrency"] = 2
  ```

  Assert the v1 input round-trips to its exact old bytes and exposes a one-item
  tuple plus the legacy property. Assert v2 round-trips canonical bytes,
  exposes two owners, rejects the legacy property, and works through acceptance
  render, status, and CLI local-input loading without changing resource shape.

  Parameterize v2 rejection of one/three owners, duplicate user, duplicate
  team, zero/noncanonical UUID, reversed ordering, simultaneous singular and
  plural fields, global live-instance limit below two, global builder
  concurrency below two, and per-owner limits below one. Ensure v1 continues to
  accept its historically valid lower global limits.

- [ ] **Step 2: Run the tests and record the expected RED failure**

  Run:

  ```bash
  PYTHONPATH=src:packages/loom-benchmarks:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_control_plane_acceptance_config.py \
    tests/unit/test_personal_dev_control_plane_render.py \
    tests/unit/test_personal_dev_control_plane_status.py \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py
  ```

  Expected: v2 fails because `_AcceptancePlanInput` accepts only schema 1 and a
  singular owner; the exact v1 compatibility assertions pass.

- [ ] **Step 3: Implement the smallest versioned loader and canonical model**

  Split the input at the schema boundary rather than making both owner fields
  optional:

  ```python
  class _AcceptancePlanV1Input(_AcceptancePlanCommonInput):
      schema_version: Literal[1]
      acceptance_owner: _AcceptanceOwnerInput

  class _AcceptancePlanV2Input(_AcceptancePlanCommonInput):
      schema_version: Literal[2]
      acceptance_owners: tuple[_AcceptanceOwnerInput, _AcceptanceOwnerInput]
  ```

  Select the model only after duplicate-rejecting canonical JSON parsing. For
  v2 require exact sorted order, distinct user/team sets, and the concurrency
  floors. Store one tuple on `PersonalDevAcceptancePlan`; branch canonical
  output only by `schema_version`. Keep `manager_runtime_json()` schema and
  interlock behavior unchanged except for the new plan SHA-256.

- [ ] **Step 4: Verify v1 bytes, v2 behavior, and static quality**

  Run the Step 2 suite, then:

  ```bash
  PYTHONPATH=src:packages/loom-benchmarks:. /home/hongjian/loom/.venv/bin/python -m ruff check \
    src/loom/personal_dev_control_plane_config.py \
    tests/unit/test_personal_dev_control_plane_acceptance_config.py \
    tests/unit/test_personal_dev_control_plane_render.py \
    tests/unit/test_personal_dev_control_plane_status.py \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py
  PYTHONPATH=src:packages/loom-benchmarks:. /home/hongjian/loom/.venv/bin/python -m mypy \
    src/loom/personal_dev_control_plane_config.py
  ```

- [ ] **Step 5: Commit the versioned plan contract**

  ```bash
  git add src/loom/personal_dev_control_plane_config.py \
    tests/unit/test_personal_dev_control_plane_acceptance_config.py \
    tests/unit/test_personal_dev_control_plane_render.py \
    tests/unit/test_personal_dev_control_plane_status.py \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py
  git commit -m "feat(dev): bind two-owner acceptance plans"
  ```

---

### Task 2: Canonical concurrent-owner result evidence

**Files:**

- Modify: `src/loom/personal_dev_acceptance_evidence.py`
- Modify: `tests/unit/test_personal_dev_acceptance_evidence.py`

**Interfaces:**

- Produces public strict model `PersonalDevAcceptanceResultV2`.
- Produces:

  ```python
  load_personal_dev_acceptance_result(
      path: Path,
      expected_sha256: str,
      *,
      plan: PersonalDevAcceptancePlan,
      expected_acceptance_manifest_sha256: str,
  ) -> PersonalDevAcceptanceResultV2
  ```

- Reuses the module's bounded owner-only descriptor reader and canonical JSON
  loader; it never retains or prints credential material.

- [ ] **Step 1: Write failing canonical-result and semantic-transition tests**

  Build one canonical v2 result fixture with top-level keys:

  ```python
  {
      "acceptance_manifest_sha256": "a" * 64,
      "acceptance_plan_sha256": plan.sha256,
      "cross_owner_denials": denials,
      "owners": owner_results,
      "release_sha256": plan.release.trusted_release_sha256,
      "schema": "loom-personal-dev-zero-capacity-acceptance-result-v2",
      "shadow_manifest_sha256": plan.release.shadow_manifest_sha256,
      "status_sha256s": {
          "after_denials": "1" * 64,
          "after_destroy": "2" * 64,
          "after_initial": "3" * 64,
          "after_redeploy": "4" * 64,
          "after_updates": "5" * 64,
          "pre_deploy": "6" * 64,
          "pre_rollback": "7" * 64,
          "rollback_shadow": "8" * 64,
      },
  }
  ```

  Each lifecycle snapshot contains exactly the fields named by the spec. Owner
  0 has initial max 2, updated max 3, default destroy, and null redeploy/final
  destroy. Owner 1 has initial max 2, updated max 4, retained destroy, ready
  redeploy, and final destroy. Updates keep subject/incarnation and increment
  generation/epoch; retained redeploy keeps subject ID, rotates incarnation,
  resets generation to 1, and advances epoch.

  The six denial records are ordered owner0→owner1 then owner1→owner0, each in
  `read`, `update`, `destroy` order. Use exact empty SHA-256
  `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`,
  exit code 1, a nonempty stderr digest, and equal nonzero before/after hashes.

  Assert rejection of unsafe file metadata/races, wrong digest, duplicate JSON,
  noncanonical bytes, v1/single-owner plan, extra/missing fields, owner order or
  identity mismatch, malformed names/UUIDs/digests, wrong manifest/release/plan
  binding, incomplete/duplicate/reordered denial matrix, successful/wrong exit,
  nonempty stdout, empty stderr, unequal target hashes, worker availability,
  nonzero minimum, wrong maxima, candidate/generation/epoch regression,
  cross-owner identity equality, wrong keep-data semantics, rotated subject ID,
  unrotated incarnation, and missing final destroy.

- [ ] **Step 2: Run the evidence tests and record RED**

  ```bash
  PYTHONPATH=src:packages/loom-benchmarks:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_acceptance_evidence.py
  ```

  Expected: import fails for the missing result model/loader.

- [ ] **Step 3: Implement strict models and cross-record validation**

  Use frozen `extra="forbid"` models. Keep UUIDs/digests as validated canonical
  strings so `canonical_bytes()` exactly reproduces input JSON. Validate each
  identity against `derive_identity(snapshot.name)`. Centralize lifecycle
  checks in small private functions:

  ```python
  _validate_ready_transition(initial, updated, *, updated_max_slots)
  _validate_destroy(updated, destroyed, *, keep_data)
  _validate_retained_redeploy(destroyed, redeployed, final_destroyed)
  _validate_denial_matrix(plan, result.cross_owner_denials)
  ```

  Compare digest text with `hmac.compare_digest`, require plan schema version 2,
  and return only after `result.canonical_bytes() == payload`.

- [ ] **Step 4: Verify evidence tests, mutation cases, Ruff, and mypy**

  ```bash
  PYTHONPATH=src:packages/loom-benchmarks:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_acceptance_evidence.py \
    tests/unit/test_personal_dev_control_plane_acceptance_config.py
  PYTHONPATH=src:packages/loom-benchmarks:. /home/hongjian/loom/.venv/bin/python -m ruff check \
    src/loom/personal_dev_acceptance_evidence.py \
    tests/unit/test_personal_dev_acceptance_evidence.py
  PYTHONPATH=src:packages/loom-benchmarks:. /home/hongjian/loom/.venv/bin/python -m mypy \
    src/loom/personal_dev_acceptance_evidence.py
  ```

- [ ] **Step 5: Commit the result contract**

  ```bash
  git add src/loom/personal_dev_acceptance_evidence.py \
    tests/unit/test_personal_dev_acceptance_evidence.py
  git commit -m "feat(dev): validate concurrent-owner acceptance evidence"
  ```

---

### Task 3: Secret-free identity and result verification CLI

**Files:**

- Modify: `src/loom_cli/auth_cmd.py`
- Modify: `src/loom_cli/personal_dev_control_plane_cmd.py`
- Modify: `src/loom_cli/service_cmd.py`
- Modify: `tests/loom_cli/test_auth_cmd.py`
- Modify: `tests/loom_cli/test_personal_dev_control_plane_cmd.py`
- Modify: `tests/loom_cli/test_service_cmd.py`

**Interfaces:**

- Adds `loom auth whoami --format text|json`, default `text`.
- Adds personal-only `loom service up --environment dev-<name> --quiet`.
  Quiet mode suppresses actor-side progress and the success summary, never
  suppresses stderr, never changes HTTP/lifecycle behavior, and is rejected
  before action for local, staging, or production targets.
- JSON emits exactly:

  ```json
  {"auth_kind":"session","credential_type":null,"expires_at":null,"principal_type":"user","role":"owner","scopes":["read:own","submit"],"server":"https://loom.example","team_id":"00000000-0000-0000-0000-000000000002","token_prefix":null,"user_id":"00000000-0000-0000-0000-000000000001"}
  ```

  followed by one newline. Values may be null where the server omitted an
  allowlisted non-secret field. No other response field is copied.
- Adds read-only:

  ```text
  loom admin personal-dev-control-plane verify-acceptance-result \
    --acceptance-plan-file PATH --acceptance-plan-sha256 DIGEST \
    --acceptance-result-file PATH --acceptance-result-sha256 DIGEST \
    --acceptance-manifest-sha256 DIGEST
  ```

  It emits one canonical record with schema, plan/result/manifest/release/shadow
  digests, owner count `2`, denial count `6`, and `verified:true`.

- [ ] **Step 1: Write failing secret-free whoami JSON tests**

  Assert default text output remains byte-compatible. For JSON, provide a server
  response containing session CSRF, arbitrary extra fields, a full-token-looking
  value, unsorted/duplicate scopes, names, and the allowlisted identity fields.
  Assert output has only the exact keys above, sorted unique scopes, one newline,
  and none of the omitted values. Assert invalid `--format` exits through
  argparse before HTTP.

- [ ] **Step 2: Write failing result-verification CLI tests**

  Assert a valid v2 plan/result emits one canonical stdout record and no stderr.
  Assert partial arguments, unsafe files, v1 plans, wrong SHA-256, wrong
  acceptance manifest, or semantically invalid result exit 2 before constructing
  any Kubernetes runner or subprocess. Assert the command parser has no apply,
  activate, kubeconfig, database, Secret, Slurm, or capacity mutation option.

  Add service-command tests proving existing personal stdout is unchanged when
  `--quiet` is absent, quiet success and denied server responses write no
  stdout, denied errors remain on stderr, and quiet is rejected before any
  local/protected deployment action for non-personal targets.

- [ ] **Step 3: Run both CLI test files and record RED**

  ```bash
  PYTHONPATH=src:packages/loom-benchmarks:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/loom_cli/test_auth_cmd.py \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py \
    tests/loom_cli/test_service_cmd.py
  ```

- [ ] **Step 4: Implement the minimal read-only handlers**

  JSON whoami constructs an allowlisted dict rather than dumping the response:

  ```python
  record = {
      "auth_kind": data.get("auth_kind"),
      "credential_type": data.get("credential_type"),
      "expires_at": data.get("expires_at"),
      "principal_type": data.get("principal_type"),
      "role": data.get("role"),
      "scopes": sorted(set(_validated_scope_list(data.get("scopes")))),
      "server": cfg.server_url,
      "team_id": data.get("team_id"),
      "token_prefix": data.get("token_prefix"),
      "user_id": data.get("user_id"),
  }
  json.dump(record, sys.stdout, separators=(",", ":"), sort_keys=True)
  sys.stdout.write("\n")
  ```

  The result verifier loads the plan first, computes/compares every supplied
  digest, calls `load_personal_dev_acceptance_result`, and writes only its small
  verification projection. It performs no runner construction or network call.

  Gate the existing `_up_personal` progress prints and final
  `_print_personal_summary` call on `not args.quiet`. Reject `args.quiet` in the
  non-personal dispatch path before Compose, candidate resolution, or protected
  deployment begins. Do not catch or suppress the existing stderr error path.

- [ ] **Step 5: Verify CLI tests, Ruff, and mypy**

  Run Step 3, then:

  ```bash
  PYTHONPATH=src:packages/loom-benchmarks:. /home/hongjian/loom/.venv/bin/python -m ruff check \
    src/loom_cli/auth_cmd.py src/loom_cli/personal_dev_control_plane_cmd.py \
    src/loom_cli/service_cmd.py \
    tests/loom_cli/test_auth_cmd.py \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py \
    tests/loom_cli/test_service_cmd.py
  PYTHONPATH=src:packages/loom-benchmarks:. /home/hongjian/loom/.venv/bin/python -m mypy \
    src/loom_cli/auth_cmd.py src/loom_cli/personal_dev_control_plane_cmd.py \
    src/loom_cli/service_cmd.py
  ```

- [ ] **Step 6: Commit the CLI verification boundary**

  ```bash
  git add src/loom_cli/auth_cmd.py src/loom_cli/personal_dev_control_plane_cmd.py \
    src/loom_cli/service_cmd.py \
    tests/loom_cli/test_auth_cmd.py \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py \
    tests/loom_cli/test_service_cmd.py
  git commit -m "feat(dev): verify two-owner acceptance results"
  ```

---

### Task 4: Exact two-owner live acceptance and rollback runbook

**Files:**

- Modify: `docs/runbooks/personal-dev-zero-capacity-acceptance.md`
- Modify: `docs/runbooks/personal-dev-durable-launch.md`
- Modify: `docs/architecture/multi-dev-environments.md`
- Modify: `docs/architecture/personal-dev-management-plane-deployment.md`
- Modify: `tests/ops/test_personal_dev_control_plane_package_boundary.py`

**Interfaces:**

- The bounded acceptance runbook requires schema v2 and two exact owner-only XDG
  roots. It produces canonical result schema
  `loom-personal-dev-zero-capacity-acceptance-result-v2` only after final
  cleanup and inert rollback.
- The durable-launch runbook accepts only a v2 result that passes
  `verify-acceptance-result` before any operational render/apply.
- No new mutable command exists outside the already approved Kubernetes apply
  and personal lifecycle surfaces.

- [ ] **Step 1: Replace single-owner package-boundary assertions with failing v2 assertions**

  Require all of the following exact shapes:

  ```bash
  owner_0_xdg="<absolute-mode-0700-owner-0-xdg-config-root>"
  owner_1_xdg="<absolute-mode-0700-owner-1-xdg-config-root>"
  XDG_CONFIG_HOME="$owner_0_xdg" loom auth whoami --format json
  XDG_CONFIG_HOME="$owner_1_xdg" loom auth whoami --format json
  ```

  Require two initial deploy PIDs and two update PIDs started before waits,
  exact `--min-slots 0`, maxima 2→3 and 2→4, distinct arbitrary v1/v2 source
  roots, six denied-operation calls in exact directions/order, exit status 1,
  empty stdout checks, `cmp -s` before/after target status after every denial,
  an `assert_live_acceptance` call after every denied operation, normal/default
  owner-0 destroy, owner-1 `--keep-data`, retained redeploy, same `subject_id`,
  changed `subject_incarnation`, final destroy, no dynamic namespace, v2 result
  verification, and byte-exact shadow reapply/status.

  Forbid the old variables/phrases `owner_xdg`, `primary_name`, `retained_name`,
  singular `.acceptance_owner`, and `.subject_id != $previous`.

- [ ] **Step 2: Run the package-boundary test and record RED**

  ```bash
  PYTHONPATH=src:packages/loom-benchmarks:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/ops/test_personal_dev_control_plane_package_boundary.py
  ```

- [ ] **Step 3: Rewrite the owner/session and lifecycle sections exactly**

  Pin both XDG roots/configs independently, require different real paths and
  device/inode identities, capture canonical whoami JSON, and compare owner 0/1
  to `.acceptance_owners[0]`/`[1]`. Define one function that checks both pinned
  sessions before every lifecycle group.

  Implement denial probes through a shell helper that receives actor XDG,
  actor candidate, target XDG/name, actor/target plan indexes, and operation.
  For each operation:

  ```bash
  XDG_CONFIG_HOME="$target_xdg" loom dev status "$target_name" --format json \
    > "$before"
  # run actor read/update/destroy, capturing stdout/stderr and exact rc
  test "$rc" -eq 1
  test ! -s "$stdout"
  XDG_CONFIG_HOME="$target_xdg" loom dev status "$target_name" --format json \
    > "$after"
  cmp -s "$before" "$after"
  assert_live_acceptance "$interlock_status"
  ```

  The update command uses the actor's own updated `candidate_sha`, `--candidate`,
  `--expected-operation-epoch 0`, `--min-slots 0`, and `--quiet`; it does not
  seal or upload another source. Construct the six canonical denial entries
  from file SHA-256 values only, never stderr contents.

- [ ] **Step 4: Correct retained-name fencing and assemble v2 evidence after rollback**

  Capture both fields before owner-1 destroy:

  ```bash
  retained_subject_id="$(jq -r .subject_id "$owner_1_updated")"
  retained_incarnation="$(jq -r .subject_incarnation "$owner_1_updated")"
  ```

  After redeploy require the same subject ID and a different incarnation. Build
  selected snapshots with explicit jq object projections, construct canonical
  v2 result with `jq -cS`, apply and verify the exact inert shadow, then run
  `verify-acceptance-result` against the final result digest. A verification
  failure leaves the system inert and blocks durable launch.

- [ ] **Step 5: Update durable/architecture docs and validate extracted shell**

  State that the v1 single-owner record remains historical compatibility but
  final multi-person launch requires v2. Update the durable runbook to retain
  the v2 plan/result and call the verifier before render. Extract every fenced
  Bash block from both runbooks and run `bash -n` on each non-illustrative
  block. Keep all task/worker/Slurm/capacity mutation strings absent except
  explicit prohibitions.

- [ ] **Step 6: Verify documentation boundaries and commit**

  ```bash
  PYTHONPATH=src:packages/loom-benchmarks:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/ops/test_personal_dev_control_plane_package_boundary.py \
    tests/ops/test_ci_secret_isolation.py
  git diff --check
  git add docs/runbooks/personal-dev-zero-capacity-acceptance.md \
    docs/runbooks/personal-dev-durable-launch.md \
    docs/architecture/multi-dev-environments.md \
    docs/architecture/personal-dev-management-plane-deployment.md \
    tests/ops/test_personal_dev_control_plane_package_boundary.py
  git commit -m "docs(dev): prove concurrent-owner isolation"
  ```

---

### Task 5: Full verification, iterative review, and protected PR

**Files:**

- Modify only files required by verified review findings.

**Interfaces:**

- Produces a clean stacked branch whose diff against the final protected `dev`
  contains only this design/plan and implementation.
- Produces one normal PR with exact-head success for `repository-checks`,
  `images-gate`, `cluster-smoke-gate`, and `staging-smoke-gate`.

- [ ] **Step 1: Reconcile the evidence-hardening dependency**

  Require PR #1581 to be merged byte-identically. Fetch `origin/dev`, verify it
  contains stable patch ID `8676190fb4af08c12d71614d03978ebabc7716c1`, inspect
  any intervening file overlap, and rebase only this branch's commits. Use an
  explicit SHA-bound force-with-lease only if the already-pushed branch needs
  replacement.

- [ ] **Step 2: Run focused and broad regression suites**

  ```bash
  PYTHONPATH=src:packages/loom-benchmarks:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_control_plane_config.py \
    tests/unit/test_personal_dev_control_plane_acceptance_config.py \
    tests/unit/test_personal_dev_control_plane_render.py \
    tests/unit/test_personal_dev_control_plane_status.py \
    tests/unit/test_personal_dev_acceptance_evidence.py \
    tests/loom_cli/test_auth_cmd.py \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py \
    tests/loom_cli/test_personal_dev_deploy.py \
    tests/loom_cli/test_service_cmd.py \
    tests/loom_cli/test_dev_cmd.py \
    tests/unit/test_dev_instance_routes.py \
    tests/ops/test_personal_dev_control_plane_package_boundary.py \
    tests/ops/test_ci_secret_isolation.py
  PYTHONPATH=src:packages/loom-benchmarks:. /home/hongjian/loom/.venv/bin/python -m ruff check \
    src scripts tests packages migrations capacity_guard_migrations capacity_migrations
  PYTHONPATH=src:packages/loom-benchmarks:. /home/hongjian/loom/.venv/bin/python -m mypy src
  PYTHONPATH=src:packages/loom-benchmarks:. /home/hongjian/loom/.venv/bin/python -m loom_cli config codegen --check
  /home/hongjian/loom/.venv/bin/python -m pip check
  ```

- [ ] **Step 3: Review until one complete clean pass**

  Review the complete protected-base diff for schema downgrade/confusion, v1
  byte drift, optional-owner selection, boolean/integer coercion, unsafe file
  reads, unbounded JSON, Secret/credential output, target-state TOCTOU,
  cross-owner information leakage, false denial success, incomplete denial
  matrix, wrong lifecycle fencing, namespace collision, mutable images,
  worker/task/Slurm authority, nonzero ceiling, rollback omission, shell quoting,
  and workflow privilege. Every real finding gets a failing test before a fix.
  Repeat from a fresh diff until a full pass finds no Critical or Important
  issue.

- [ ] **Step 4: Verify completion evidence before push**

  Require clean status, exact base/head, no `docs/superpowers`, no Secret-like
  tracked file, no high-confidence credential string, no unrelated worktree
  change, and byte-identical v1 fixture output. Record the exact commands and
  counts in the SDD ledger.

- [ ] **Step 5: Push, create the PR, and monitor exact-head gates**

  Push `codex/personal-dev-multi-owner-acceptance`, create a normal PR against
  `dev`, and include the safety boundary, compatibility behavior, test evidence,
  and live non-claim in the body. Do not merge or enable auto-merge until the
  exact head/base are reconciled and all four protected gates are successful.
  Diagnose failures from complete logs and re-review every fix.

- [ ] **Step 6: Prepare—not execute—the final live package**

  After merge, publish/download the exact protected trusted release, create a
  clean detached worktree, regenerate launcher/scanner/backup-restore evidence,
  render and review the v2 acceptance plus exact inert rollback, bind the two
  provisioned owner identities without printing credentials, and stop before
  mutation unless the bounded #1280 window, both live sessions, zero ceiling,
  rollback deadline, and all preflight interlocks are simultaneously valid.
