# Personal-development acceptance-authority coexistence implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve the approved #1280 sole-owner acceptance and durable-launch
procedures byte-for-byte while publishing the reviewed schema-v2 concurrent-
owner acceptance and multi-owner durable-launch procedures under separate,
unambiguous paths.

**Architecture:** Treat the merged sole-owner runbooks as immutable compatibility
fixtures. Move the already reviewed v2 procedures to new explicit filenames,
route the concurrent-owner design and architecture documentation to those files,
and test both authorities independently so no operator-selected mode can
downgrade a multi-person launch.

**Tech Stack:** Markdown, Bash, Python 3.11, pytest, Ruff, mypy, canonical JSON,
Git, GitHub protected checks.

**Spec:**
`docs/architecture/personal-dev-acceptance-authority-coexistence-design.md`

**Reconciled protected base:**
`6eb36e77dcd7b5648a68e03ca7eb566b2be56db6`. This baseline intentionally
includes #1585's management-ingress NetworkPolicy evidence and #1589's
sequence-free PostgreSQL backup compatibility. The v2 durable procedure must
retain the same management-ingress evidence.

## Global constraints

- The global executable-new-capacity ceiling remains exactly `0`.
- Never activate a worker, submit or execute a task, invoke a Slurm mutation,
  enable a pool executor, or change OLDLAB/GB10 capacity.
- Preserve schema-v1 behavior and the protected-base bytes of both existing
  sole-owner runbooks.
- Keep schema-v2 strict with exactly two distinct owners and all six canonical
  hidden-denial receipts.
- A v1 result cannot certify multi-owner readiness; v2 is mandatory before a
  second person is onboarded.
- Do not add a runtime authority-selection flag or expand the operational-plan
  schema in this correction.
- Do not create `docs/superpowers`.
- Never print or retain bearer tokens, Secret values, kubeconfig contents,
  private keys, or database credentials.
- Do not execute either live acceptance procedure from this branch.

---

### Task 1: Separate the two acceptance procedures

**Files:**

- Create: `docs/runbooks/personal-dev-concurrent-owner-zero-capacity-acceptance.md`
- Modify: `docs/runbooks/personal-dev-zero-capacity-acceptance.md`
- Modify: `tests/ops/test_personal_dev_control_plane_package_boundary.py`

**Interfaces:**

- Preserves: `docs/runbooks/personal-dev-zero-capacity-acceptance.md` as the
  exact #1280 sole-owner/two-environment procedure with SHA-256
  `dc9da9db4a6a54ba7ca0d3eba8ba35b647fb45ad2f56f4dc855f3b1d7d7d6bbf`.
- Produces: the reviewed schema-v2 two-owner procedure at
  `docs/runbooks/personal-dev-concurrent-owner-zero-capacity-acceptance.md`.

- [ ] **Step 1: Add the failing byte-preservation and path-separation tests**

  Add `import hashlib`, a byte-digest helper, and this exact compatibility
  assertion to `tests/ops/test_personal_dev_control_plane_package_boundary.py`:

  ```python
  def _document_sha256(relative: str) -> str:
      return hashlib.sha256((_ROOT / relative).read_bytes()).hexdigest()


  def test_approved_solo_owner_acceptance_runbook_is_byte_preserved() -> None:
      assert _document_sha256(
          "docs/runbooks/personal-dev-zero-capacity-acceptance.md"
      ) == "dc9da9db4a6a54ba7ca0d3eba8ba35b647fb45ad2f56f4dc855f3b1d7d7d6bbf"
  ```

  Restore the protected-base single-owner workflow tests for the existing path.
  Rename the current v2 workflow and authority-boundary tests to start with
  `test_concurrent_owner_` and make them read only:

  ```python
  _read("docs/runbooks/personal-dev-concurrent-owner-zero-capacity-acceptance.md")
  ```

  Keep every current v2 assertion: two pinned XDG roots, two distinct whoami
  projections, concurrent deploy/update PIDs, six ordered denial probes, exact
  GET/PUT/DELETE 404 receipts, empty stdout, unchanged target status, retained
  subject ID with rotated incarnation, exact epoch fencing, v2 result assembly,
  verifier invocation, final cleanup, and inert rollback.

- [ ] **Step 2: Run the package test and record RED**

  Run:

  ```bash
  PYTHONPATH=src:packages/loom-benchmarks:packages/loom-bundle-checksum:. \
    /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/ops/test_personal_dev_control_plane_package_boundary.py
  ```

  Expected: the byte-preservation assertion fails on the replaced existing
  runbook and the concurrent-owner tests fail because the new path is absent.

- [ ] **Step 3: Create the explicit v2 procedure and restore v1 bytes**

  Copy the current reviewed v2 runbook into
  `personal-dev-concurrent-owner-zero-capacity-acceptance.md`, then change only
  its title, authority-scoping prose, and cross-runbook links so it identifies
  itself as the separately reviewed concurrent-owner procedure, contains no
  #1280-window authority claim, and points to
  `personal-dev-multi-owner-durable-launch.md`. Keep every executable command
  and control-flow boundary byte-identical to the reviewed v2 procedure; only
  replace its #1280 evidence-directory placeholder with an explicit
  concurrent-owner evidence-directory placeholder.

  Restore `personal-dev-zero-capacity-acceptance.md` exactly from protected base
  `6eb36e77dcd7b5648a68e03ca7eb566b2be56db6`. Use `apply_patch` for both file
  edits. Do not weaken or branch either procedure at runtime.

- [ ] **Step 4: Verify v1 bytes and v2 boundaries**

  Run the Step 2 command and verify both the exact SHA-256 assertion and all v2
  package-boundary assertions pass. Extract each fenced non-illustrative Bash
  block from both acceptance runbooks and run `bash -n` on the extracted
  scripts using the repository's existing package-boundary extraction helper.

- [ ] **Step 5: Commit the separated acceptance procedures**

  ```bash
  git add docs/runbooks/personal-dev-zero-capacity-acceptance.md \
    docs/runbooks/personal-dev-concurrent-owner-zero-capacity-acceptance.md \
    tests/ops/test_personal_dev_control_plane_package_boundary.py
  git commit -m "docs(dev): preserve sole-owner acceptance path"
  ```

---

### Task 2: Separate durable launch and route both authorities

**Files:**

- Create: `docs/runbooks/personal-dev-multi-owner-durable-launch.md`
- Modify: `docs/runbooks/personal-dev-durable-launch.md`
- Modify: `docs/runbooks/README.md`
- Modify: `docs/architecture/multi-dev-environments.md`
- Modify: `docs/architecture/personal-dev-management-plane-deployment.md`
- Modify: `docs/architecture/personal-dev-concurrent-owner-zero-capacity-acceptance-design.md`
- Modify: `docs/architecture/personal-dev-concurrent-owner-zero-capacity-acceptance-implementation-plan.md`
- Modify: `tests/ops/test_personal_dev_control_plane_package_boundary.py`

**Interfaces:**

- Preserves: `docs/runbooks/personal-dev-durable-launch.md` as the exact
  sole-owner launch procedure with SHA-256
  `46ca8f8dcc0bdcc6f0a0ab673ad08921bb9e48d5585bbd90f51a07515ec87c8f`.
- Produces: `docs/runbooks/personal-dev-multi-owner-durable-launch.md`, which
  verifies the schema-v2 plan/result before any operational render or apply.
- Routes: #1280 to the existing paths and second-owner onboarding to the new
  concurrent-owner paths.

- [ ] **Step 1: Add failing durable byte and routing tests**

  Add:

  ```python
  def test_approved_solo_owner_durable_launch_is_byte_preserved() -> None:
      assert _document_sha256(
          "docs/runbooks/personal-dev-durable-launch.md"
      ) == "46ca8f8dcc0bdcc6f0a0ab673ad08921bb9e48d5585bbd90f51a07515ec87c8f"
  ```

  Restore the protected-base durable-launch assertions for the existing path.
  Rename the current v2 durable-launch test to
  `test_multi_owner_durable_launch_requires_verified_v2_result` and point it to
  `docs/runbooks/personal-dev-multi-owner-durable-launch.md`. It must continue
  proving `verify-acceptance-result` appears before `render-operational`, exact
  owner and denial counts are checked, and the v2 acceptance plan/result and
  forward-manifest digest, exact rollback-manifest bytes, rollback status, and
  their digests are bound.

  Extend the indexing test to require all four distinct paths in the runbook
  index and architecture:

  ```python
  required = (
      "personal-dev-zero-capacity-acceptance.md",
      "personal-dev-durable-launch.md",
      "personal-dev-concurrent-owner-zero-capacity-acceptance.md",
      "personal-dev-multi-owner-durable-launch.md",
  )
  for path in required:
      assert path in runbook_index
      assert path in architecture
  ```

  Assert normalized architecture includes both
  `sole-owner/two-environment` and `before a second person is onboarded`.

- [ ] **Step 2: Run the package test and record RED**

  Run the Task 1 Step 2 command.

  Expected: the durable byte assertion fails, the new multi-owner path is
  absent, and routing assertions fail.

- [ ] **Step 3: Create the v2 durable procedure and restore the v1 procedure**

  Copy the current reviewed v2 durable runbook into
  `personal-dev-multi-owner-durable-launch.md`. Change only its title,
  authority-scoping prose, and links so it points to the concurrent-owner
  acceptance runbook, identifies v2 as the required second-owner gate, and
  contains no #1280-window authority claim. Keep every executable Bash block
  byte-identical to the reviewed v2 procedure.

  Restore `personal-dev-durable-launch.md` exactly from protected base
  `6eb36e77dcd7b5648a68e03ca7eb566b2be56db6` using `apply_patch`.

- [ ] **Step 4: Update indexes, architecture, design, and the original plan**

  Add separate sole-owner and multi-owner entries to `docs/runbooks/README.md`.
  Update the two architecture documents so #1280 keeps its recorded
  sole-owner/two-environment authority and the second owner is blocked until the
  separate v2 acceptance and multi-owner durable launch pass.

  Update the concurrent-owner design to name the new runbook paths. Amend Task
  4 of its implementation plan from replacing the #1280 runbooks to creating
  the two explicit v2 runbooks while preserving the protected-base files.
  Retain the schema-v2 model, CLI, evidence, and safety requirements unchanged.

- [ ] **Step 5: Verify both durable procedures and documentation routing**

  Run:

  ```bash
  PYTHONPATH=src:packages/loom-benchmarks:packages/loom-bundle-checksum:. \
    /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/ops/test_personal_dev_control_plane_package_boundary.py \
    tests/ops/test_ci_secret_isolation.py
  git diff --check
  ```

  Extract and `bash -n` every fenced non-illustrative Bash block from both
  durable-launch runbooks. Confirm the exact protected-base SHA-256 values for
  the two existing paths and confirm the new paths are nonempty regular tracked
  Markdown files.

- [ ] **Step 6: Commit the routed durable procedures**

  ```bash
  git add docs/runbooks/personal-dev-durable-launch.md \
    docs/runbooks/personal-dev-multi-owner-durable-launch.md \
    docs/runbooks/README.md \
    docs/architecture/multi-dev-environments.md \
    docs/architecture/personal-dev-management-plane-deployment.md \
    docs/architecture/personal-dev-concurrent-owner-zero-capacity-acceptance-design.md \
    docs/architecture/personal-dev-concurrent-owner-zero-capacity-acceptance-implementation-plan.md \
    tests/ops/test_personal_dev_control_plane_package_boundary.py
  git commit -m "docs(dev): gate second-owner durable launch"
  ```

---

### Task 3: Full verification, iterative review, and corrected protected head

**Files:**

- Modify only files required by verified review findings.

**Interfaces:**

- Produces one clean #1583 head containing additive schema-v2 capability while
  preserving the approved sole-owner launch paths.
- Produces exact-head success for `repository-checks`, `images-gate`,
  `cluster-smoke-gate`, and `staging-smoke-gate` before owner merge.

- [ ] **Step 1: Run the complete focused regression surface**

  ```bash
  PYTHONPATH=src:packages/loom-benchmarks:packages/loom-bundle-checksum:. \
    /home/hongjian/loom/.venv/bin/python -m pytest -q \
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
  ```

- [ ] **Step 2: Run static, generated, and environment checks**

  ```bash
  PYTHONPATH=src:packages/loom-benchmarks:packages/loom-bundle-checksum:. \
    /home/hongjian/loom/.venv/bin/python -m ruff check \
    src scripts tests packages migrations capacity_guard_migrations capacity_migrations
  PYTHONPATH=src:packages/loom-benchmarks:packages/loom-bundle-checksum:. \
    /home/hongjian/loom/.venv/bin/python -m mypy src
  PYTHONPATH=src:packages/loom-benchmarks:packages/loom-bundle-checksum:. \
    /home/hongjian/loom/.venv/bin/python -m loom_cli config codegen --check
  uv pip check --python /home/hongjian/loom/.venv/bin/python
  ```

- [ ] **Step 3: Review the complete protected-base diff until clean**

  Review `6eb36e77dcd7b5648a68e03ca7eb566b2be56db6..HEAD` for:

  - any changed byte in either protected sole-owner runbook;
  - a v1-to-v2 reinterpretation or authority-selection flag;
  - a link that routes #1280 to the multi-owner path or second-owner onboarding
    to the sole-owner path;
  - missing v2 credential, denial, target-state, lifecycle, cleanup, verifier,
    or rollback evidence;
  - worker/task/Slurm/capacity authority, nonzero ceiling, pool weights, mutable
    images, Secret output, unsafe evidence paths, shell quoting, namespace
    collision, or workflow privilege escalation.

  Every real finding gets a failing test before a fix. Restart the complete diff
  review after every fix and stop only after one full pass finds no Critical or
  Important problem.

- [ ] **Step 4: Verify exact completion evidence**

  Require a clean worktree, `git diff --check`, the two exact v1 runbook SHA-256
  values, both new v2 paths, no `docs/superpowers`, no Secret-like added path,
  no high-confidence credential string, no added production `sbatch`/`scancel`,
  no workflow privilege escalation, and the exact protected dependency patch ID
  `8676190fb4af08c12d71614d03978ebabc7716c1`.

- [ ] **Step 5: Reconcile, push, and monitor the corrected exact head**

  Fetch `origin/dev`, confirm it is an ancestor of the branch and no new overlap
  exists, then push the normal branch update without force if possible. Update
  the #1583 body to explain the separate authority paths and retain the live
  non-claim. Monitor all four required gates on the exact new head. Diagnose any
  failure from complete logs and repeat review/verification after each fix.

- [ ] **Step 6: Stop at the owner boundary**

  Do not merge #1583 and do not execute either live runbook. Report the exact
  CI-approved head for owner merge. After merge, the #1280 sole-owner window and
  the later multi-owner certification remain separately reviewed operational
  actions.
