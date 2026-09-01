# Operator Native Material Authority Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the protected OLDLAB operator host installable with a sealed, operator-only material-client inventory without installing any GB10 runtime privilege or capacity surface.

**Architecture:** Extend the existing pre-import launcher with a distinct five-asset operator policy, route the material client through that policy, and add an exact OLDLAB target to the reviewed atomic bootstrap transaction. Preserve the GB10 runtime bootstrap API and default behavior.

**Tech Stack:** Python 3.11 standard library, canonical JSON, descriptor-safe filesystem operations, Git sealed-source validation, pytest, Ruff, mypy, Bash.

**Spec:** `docs/architecture/2026-09-01-personal-dev-native-operator-material-authority-design.md`

## Global Constraints

- Runtime target is exact `gx10-01c7/aarch64`; operator target is exact `TRT-EAI-OLDLAB-1/x86_64`.
- Operator inventory is exactly launcher, material client, authority client, protocol, and crypto helper.
- Operator bootstrap installs no broker, runtime asset, tmpfiles asset, state/lock, or sudoers file.
- Bootstrap never reads, copies, validates, hashes, prints, or consumes private-key or CA material.
- Secret bytes and digests never enter argv, environment, output, receipts, evidence, comments, or Git.
- Kubernetes, database, object storage, DNS, Tasks, Workers, Slurm, and capacity remain untouched.

---

### Task 1: Distinct operator policy and material-client boundary

**Files:**

- Modify: `scripts/ops/personal_dev_native_builder_runtime_authority_launcher.py`
- Modify: `scripts/ops/personal_dev_native_builder_runtime_authority_material_client.py`
- Modify: `tests/ops/test_personal_dev_native_builder_runtime_authority.py`
- Modify: `tests/ops/test_personal_dev_native_builder_runtime_authority_protocol.py`

**Interfaces:**

- Produces `OPERATOR_MATERIAL_POLICY_PATH`,
  `OPERATOR_MATERIAL_ASSET_SPECS`, and
  `load_operator_material_policy(*, policy_path=..., asset_specs=...,
  expected_uid=0, expected_gid=0) -> Mapping[str, object]`.
- Preserves `load_policy()` and the runtime launcher behavior unchanged.
- Makes `_load_validated_client()` accept only the operator policy and subset.

- [ ] **Step 1: Write failing operator-policy tests**

  Add tests constructing the five fixed `AssetSpec` entries and a canonical
  operator policy. Require successful validation plus rejection of the runtime
  sudoers/broker inventory, missing or extra asset names, a changed digest,
  wrong schema, runtime-profile field, duplicate key, noncanonical JSON,
  symlink/hardlink/mode/owner drift, and unsafe parents. Add a material-client
  behavior test whose fake launcher fails unless
  `load_operator_material_policy()` receives the exact subset before client
  load or material open.

- [ ] **Step 2: Run the focused tests and verify RED**

  Run:

  ```bash
  uv run --no-sync pytest -q \
    tests/ops/test_personal_dev_native_builder_runtime_authority.py \
    tests/ops/test_personal_dev_native_builder_runtime_authority_protocol.py
  ```

  Expected: failures because the operator policy constants/function do not
  exist and the material client still invokes the full runtime policy.

- [ ] **Step 3: Implement the minimal distinct policy loader**

  Refactor the launcher’s internal policy validator to accept an exact schema,
  asset mapping, and optional runtime-profile requirement. Keep `load_policy()`
  as the fixed runtime wrapper. Add the fixed operator wrapper with the exact
  four-field schema and five-asset mapping. Change the material client to call
  only that wrapper and assert the exact installed material-client and encoder
  entries before pinning packages.

- [ ] **Step 4: Run focused tests, Ruff, and mypy GREEN**

  ```bash
  uv run --no-sync pytest -q \
    tests/ops/test_personal_dev_native_builder_runtime_authority.py \
    tests/ops/test_personal_dev_native_builder_runtime_authority_protocol.py
  uv run --no-sync ruff check \
    scripts/ops/personal_dev_native_builder_runtime_authority_launcher.py \
    scripts/ops/personal_dev_native_builder_runtime_authority_material_client.py \
    tests/ops/test_personal_dev_native_builder_runtime_authority.py \
    tests/ops/test_personal_dev_native_builder_runtime_authority_protocol.py
  uv run --no-sync mypy --strict \
    scripts/ops/personal_dev_native_builder_runtime_authority_launcher.py \
    scripts/ops/personal_dev_native_builder_runtime_authority_material_client.py
  ```

- [ ] **Step 5: Commit Task 1**

  ```bash
  git add scripts/ops/personal_dev_native_builder_runtime_authority_launcher.py \
    scripts/ops/personal_dev_native_builder_runtime_authority_material_client.py \
    tests/ops/test_personal_dev_native_builder_runtime_authority.py \
    tests/ops/test_personal_dev_native_builder_runtime_authority_protocol.py
  git commit -m "fix(dev): separate operator material policy"
  ```

### Task 2: OLDLAB-only atomic bootstrap target

**Files:**

- Modify: `scripts/ops/install_personal_dev_native_builder_runtime_authority.py`
- Modify: `tests/ops/test_install_personal_dev_native_builder_runtime_authority.py`

**Interfaces:**

- Produces `bootstrap_operator_material(source_sha: str,
  source_tree_sha: str) -> dict[str, object]`.
- Extends the CLI with `--target {runtime,operator-material}`, default
  `runtime`; existing `bootstrap()` remains the runtime API.
- Installs only the five operator assets, canonical operator policy, and empty
  fixed material directory.

- [ ] **Step 1: Write failing target and inventory tests**

  Add an OLDLAB fixture (`TRT-EAI-OLDLAB-1/x86_64`) and require the operator
  receipt to bind target/source/tree/base, exactly five asset digests, and the
  operator-policy digest. Assert the installed tree contains no broker,
  runtime-profile/assets, state, lock, tmpfiles, or sudoers file and that the
  mode-`0700` material directory is empty. Assert runtime remains the CLI
  default and the existing runtime snapshot is byte-identical.

- [ ] **Step 2: Run the installer tests and verify RED**

  ```bash
  uv run --no-sync pytest -q \
    tests/ops/test_install_personal_dev_native_builder_runtime_authority.py
  ```

  Expected: failures because `bootstrap_operator_material` and the target
  selector do not exist.

- [ ] **Step 3: Implement minimal operator bootstrap reuse**

  Reuse the existing sealed-source validation, pinned asset reads, install
  ledger, atomic no-replace writes, fsyncs, retained identity checks, and
  rollback helpers. Capture only `OPERATOR_MATERIAL_ASSET_SPECS`, encode the
  exact operator policy without importing the broker, install/validate the
  subset, and ensure the fixed empty material directory. Add an exact
  OLDLAB/x86_64 clean-root gate and target dispatch; do not alter runtime
  `bootstrap()`.

- [ ] **Step 4: Add failure-injection and mutation coverage**

  For every operator write/directory/publication boundary, inject failure and
  require removal of only objects created by that attempt. Cover idempotence,
  pre-existing exact assets, policy/asset/material-directory drift, path
  replacement between creation and rollback, source mutation during capture,
  wrong host/architecture, unsafe root environment, and proof that neither
  fixed material pathname is opened or hashed.

- [ ] **Step 5: Run installer tests, Ruff, and mypy GREEN**

  ```bash
  uv run --no-sync pytest -q \
    tests/ops/test_install_personal_dev_native_builder_runtime_authority.py
  uv run --no-sync ruff check \
    scripts/ops/install_personal_dev_native_builder_runtime_authority.py \
    tests/ops/test_install_personal_dev_native_builder_runtime_authority.py
  uv run --no-sync mypy --strict \
    scripts/ops/install_personal_dev_native_builder_runtime_authority.py
  ```

- [ ] **Step 6: Commit Task 2**

  ```bash
  git add scripts/ops/install_personal_dev_native_builder_runtime_authority.py \
    tests/ops/test_install_personal_dev_native_builder_runtime_authority.py
  git commit -m "feat(dev): bootstrap operator material boundary"
  ```

### Task 3: Runbook and protected CI ownership

**Files:**

- Modify: `docs/architecture/2026-08-31-personal-dev-native-runtime-authority-design.md`
- Modify: `docs/runbooks/personal-dev-native-builder-runtime.md`
- Modify: `docs/runbooks/personal-dev-native-builder-acceptance.md`
- Modify: `scripts/plan_ci_validations.py`
- Modify: `tests/ops/test_personal_dev_native_builder_runbooks.py`
- Modify: `tests/ops/test_install_personal_dev_native_builder_runtime_authority.py`

**Interfaces:**

- Documents the exact sealed OLDLAB bootstrap command and separate material
  provisioning boundary.
- Routes every new/changed operator authority path through the existing
  `protected-native-authority` full-gate owner.

- [ ] **Step 1: Write failing documentation/route tests**

  Require both runbooks to name the exact operator policy, exact five-asset
  subset, exact `--target operator-material` command, no GB10 sudoers/runtime
  installation on OLDLAB, and separate fixed material provisioning. Require
  all operator authority code/docs to select all protected heavy gates with
  `unowned_runtime=false`.

- [ ] **Step 2: Run focused tests and verify RED**

  ```bash
  uv run --no-sync pytest -q \
    tests/ops/test_personal_dev_native_builder_runbooks.py \
    tests/ops/test_install_personal_dev_native_builder_runtime_authority.py
  ```

- [ ] **Step 3: Update docs and exact route ownership**

  Replace the unsafe “complete installed inventory” instruction with the
  operator subset bootstrap and fixed-input provisioning sequence. Preserve
  the client-to-SSH FD-only data flow and all no-mutation rules. Add only exact
  new paths to protected CI ownership; do not claim unrelated prefixes.

- [ ] **Step 4: Run focused verification GREEN**

  ```bash
  uv run --no-sync pytest -q \
    tests/ops/test_personal_dev_native_builder_runbooks.py \
    tests/ops/test_install_personal_dev_native_builder_runtime_authority.py \
    tests/ops/test_plan_ci_validations.py
  uv run --no-sync ruff check scripts/plan_ci_validations.py tests/ops
  git diff --check
  ```

- [ ] **Step 5: Commit Task 3**

  ```bash
  git add docs/architecture/2026-08-31-personal-dev-native-runtime-authority-design.md \
    docs/runbooks/personal-dev-native-builder-runtime.md \
    docs/runbooks/personal-dev-native-builder-acceptance.md \
    scripts/plan_ci_validations.py \
    tests/ops/test_personal_dev_native_builder_runbooks.py \
    tests/ops/test_install_personal_dev_native_builder_runtime_authority.py
  git commit -m "docs(dev): bind operator material bootstrap"
  ```

### Task 4: Whole-change security closure and protected integration

**Files:**

- Modify Task 1-3 files only for validated findings.
- Add regression tests beside each finding.

**Interfaces:** Produces one reviewed protected commit range and an exact live
operator installation handoff; no new public runtime operation.

- [ ] **Step 1: Perform line-by-line security review**

  Review base-to-head for pre-validation imports, policy/inventory confusion,
  root path traversal, TOCTOU, replacement-aware rollback, fsync ordering,
  idempotent drift, secret I/O, diagnostic leakage, host/architecture gates,
  runtime-target regression, and CI ownership. Fix every Critical/Important
  finding test-first and record any technically rejected finding with evidence.

- [ ] **Step 2: Run complete affected verification**

  ```bash
  uv run --no-sync pytest -q \
    tests/ops/test_personal_dev_native_builder_runtime_authority.py \
    tests/ops/test_personal_dev_native_builder_runtime_authority_protocol.py \
    tests/ops/test_install_personal_dev_native_builder_runtime_authority.py \
    tests/ops/test_personal_dev_native_builder_runbooks.py \
    tests/ops/test_plan_ci_validations.py
  uv run --no-sync ruff check .
  uv run --no-sync mypy --strict scripts/ops src packages
  git diff --check origin/dev...HEAD
  ```

  Also parse every changed fenced Bash block with `bash -n`, run `visudo -cf`
  on the unchanged runtime sudoers asset, scan for secret bytes/digests and
  forbidden OLDLAB broker/sudoers installation, and run the real whole-branch
  changed-path planner requiring `unowned_runtime=false` and all protected
  heavy lanes.

- [ ] **Step 3: Rebase, push, and open a protected `dev` PR**

  Fetch current `origin/dev`; if it moved, rebase and rerun Step 2. Push with a
  lease, open a non-draft PR, and monitor the exact current-head
  `repository-checks`, `images-gate`, `cluster-smoke-gate`, and
  `staging-smoke-gate` CheckRuns from GitHub Actions app id `15368`.

- [ ] **Step 4: Merge and verify provenance**

  After every exact final gate succeeds and `dev` remains unchanged,
  squash-merge without admin bypass. Fetch `origin/dev` and require the squash
  tree to equal the reviewed branch tree and its sole parent to equal the
  verified base. Remove only the merged remote feature branch.

- [ ] **Step 5: Install and verify OLDLAB operator boundary**

  From the exact squash commit, create a root-owned mode-`0700` sealed source
  at `/opt/loom-personal-dev-native-builder-runtime-authority/source`, run the
  installer with `--target operator-material`, then separately provision the
  two fixed protected inputs without recording their bytes or digests. Verify
  exact installed public metadata and run `emit-public-key` with stdout sent to
  `/dev/null`; do not issue a GB10 mutation request in this step.
