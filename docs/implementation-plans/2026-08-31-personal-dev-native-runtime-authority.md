# Personal-development Native Runtime Authority Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` or `superpowers:executing-plans`
> to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Replace broad remote `sudo` in the GB10 native-builder rollout with
one fixed, no-argument, secret-safe root authority and prove its full inert →
prepared → staged → active → removed lifecycle.

**Architecture:** A pure protocol module owns canonical framed requests and
strict public request models. A fixed root broker validates installed policy,
invocation identity, host state, and operation transitions, then calls the
existing installer/release converger plus one fixed conformance asset. A
direct-root sealed-source bootstrap installs every executable dependency and
publishes the single `NOSETENV` sudoers rule last. The operator runbooks use
only a pinned explicit OpenSSH transport and this broker for GB10 privilege.

**Tech Stack:** Python 3.11, dataclasses, canonical JSON, framed binary stdin,
Ed25519 public fingerprints, subprocess/systemd/nftables/Docker, Bash,
OpenSSH, sudoers, pytest, Ruff, mypy.

**Spec:**
`docs/architecture/2026-08-31-personal-dev-native-runtime-authority-design.md`

## Global constraints

- No Loom Task or Worker creation and no Slurm command or job.
- Executable new-capacity remains exactly `0` throughout rollout and tests.
- Candidate-controlled bytes execute only as native `linux/arm64` inside KVM
  gVisor; no QEMU and no runc fallback.
- The broker accepts no argv and sudoers exposes only its exact empty-argument
  invocation with `NOPASSWD:NOSETENV`.
- Private key and CA bytes/digests never appear in argv, output, receipts,
  evidence, comments, or persistent staging.
- Root-executed code comes only from an exact root-owned sealed merged source;
  operator-writable staging is data-only.
- The broker owns only the dedicated personal-builder services, nft table,
  runtime paths, bounded image cache, and exact conformance objects.
- Kubernetes manifests, Secrets, PostgreSQL/MinIO, personal namespaces, and
  capacity are outside broker authority.
- Every mutating failure compensates to inactive services and absent exact nft
  table; cleanup never broadens selectors.

---

### Task 1: Canonical framed protocol and secret-safe client

**Files:**

- Create: `scripts/ops/personal_dev_native_builder_runtime_authority_protocol.py`
- Create: `scripts/ops/personal_dev_native_builder_runtime_authority_client.py`
- Create: `tests/ops/test_personal_dev_native_builder_runtime_authority_protocol.py`

**Interfaces:**

- Produces `AuthorityRequest`, `AuthorityRequestHeader`,
  `parse_request(stream: BinaryIO) -> AuthorityRequest`, and
  `encode_request(header: Mapping[str, object], payload: bytes = b"") -> bytes`.
- Produces client subcommands `status`, `prepare`, `stage-agent`, `activate`,
  and `remove`, each writing exactly one binary frame to stdout.
- `stage-agent` reads key/CA bytes through already-opened local paths, but the
  resulting header contains only public identity and exact byte lengths.

- [ ] **Step 1: Write failing protocol tests**

  Cover round trips for all operations; canonical field/order enforcement;
  duplicate keys; wrong magic/version; truncated/oversized/trailing bytes;
  booleans used as integers; mutable or wrong-repository images; non-HTTPS or
  credentialed origins; malformed UUID/key ID/SHA values; payload on public
  operations; and a `stage-agent` frame whose secret byte sequences and
  SHA-256 digests are absent from its header.

- [ ] **Step 2: Verify red**

  Run:

  ```bash
  pytest -q tests/ops/test_personal_dev_native_builder_runtime_authority_protocol.py
  ```

  Expected: collection fails because the protocol module does not exist.

- [ ] **Step 3: Implement the protocol and client minimally**

  Use magic `LOOMNBR1`, a four-byte big-endian header length, canonical JSON
  without newline, exact operation field sets, a 64 KiB header maximum, a 2
  MiB total maximum, exact 32-byte key length, and a 1..1 MiB CA length. Parse
  JSON with an `object_pairs_hook` that rejects duplicates. Re-encode and
  compare headers to enforce canonical bytes. Use strict regex/URL/ipaddress
  validators and reject unknown fields before constructing frozen dataclasses.

- [ ] **Step 4: Verify green and static quality**

  Run the focused pytest module, Ruff on both scripts/tests, and mypy on the
  protocol and client.

- [ ] **Step 5: Commit**

  ```bash
  git add scripts/ops/personal_dev_native_builder_runtime_authority_*.py \
    tests/ops/test_personal_dev_native_builder_runtime_authority_protocol.py
  git commit -m "feat(dev): define native runtime authority protocol"
  ```

### Task 2: Fixed conformance executor

**Files:**

- Create: `scripts/ops/personal_dev_native_builder_conformance.py`
- Create: `tests/ops/test_personal_dev_native_builder_conformance.py`

**Interfaces:**

- Produces `ConformanceInputs` and
  `run_conformance(inputs: ConformanceInputs, runner: Runner) -> dict[str, object]`.
- Accepts only two pinned GHCR arm64 image digests and one public HTTPS origin.
- Returns a canonical-public receipt with two distinct sandbox IDs and fixed
  denial results; no raw logs or environment.

- [ ] **Step 1: Write failing transition and cleanup tests**

  Use a recording fake Docker runner. Require exact dedicated socket use,
  exact primary-daemon foreign probe only, fixed names/labels/subnets, gVisor
  runtime, resource/capability/no-new-privileges settings, native arm64 and
  `/proc/gvisor` checks, separate BuildKit/client IDs, all four denial probes,
  and reverse-order cleanup of only IDs created in the invocation. Inject a
  failure after every create/start/probe and assert exact cleanup.

- [ ] **Step 2: Verify red**

  Run the focused module; expect import failure.

- [ ] **Step 3: Implement fixed argv construction and ownership tracking**

  Never invoke a shell. Store returned object IDs immediately, validate every
  ID as 64 lowercase hex characters, use them for cleanup, refuse pre-existing
  exact names, cap stdout/stderr, and terminate process groups on deadline or
  signal. Do not expose a generic command runner through the CLI.

- [ ] **Step 4: Verify green, Ruff, and mypy**

- [ ] **Step 5: Commit**

  ```bash
  git add scripts/ops/personal_dev_native_builder_conformance.py \
    tests/ops/test_personal_dev_native_builder_conformance.py
  git commit -m "feat(dev): fix native runtime conformance authority"
  ```

### Task 3: Root broker state machine

**Files:**

- Create: `scripts/ops/personal_dev_native_builder_runtime_authority.py`
- Create: `tests/ops/test_personal_dev_native_builder_runtime_authority.py`
- Modify: `scripts/ops/install_personal_dev_native_builder_runtime.py`
- Modify: `scripts/ops/converge_personal_dev_native_builder_release.py`

**Interfaces:**

- Produces `RuntimeAuthority.dispatch(request: AuthorityRequest) ->
  dict[str, object]` and a no-argument `main()`.
- Consumes the existing installer/converger APIs through explicit injected
  adapters; their CLIs and current tests remain backward-compatible.
- Installed policy is a frozen model binding source/tree and SHA-256 of the
  broker, protocol, installer, runtime-profile helper, converger, conformance,
  runtime assets, sudoers, and tmpfiles asset.

- [ ] **Step 1: Write failing identity/policy/status tests**

  Require exact root/sudo identity, no argv, fixed hostname/architecture,
  root-owned no-follow single-link policy/assets, clean environment, exclusive
  nonblocking lock, fixed paths, canonical status receipt, and stable
  secret-free failure output. Reject any Task/Worker/Slurm/Kubernetes/database
  binary in the broker source or recorded commands.

- [ ] **Step 2: Verify red, then implement request-independent boundary code**

  Implement safe file opens, policy validation, invocation/host checks,
  canonical receipts, bounded subprocess/process-group handling, signal
  deferral during cleanup, and the lock. Run the focused tests green.

- [ ] **Step 3: Write failing `prepare` transition tests**

  Require inactive entry, no managed objects, descriptor-bound archive
  metadata/digest, fixed preflight/install/verify-staged, exact nft load,
  dedicated daemon only, double deterministic image plan, apply/verify,
  conformance, and inactive exit. Inject every boundary failure and require
  daemon/nft/probe compensation.

- [ ] **Step 4: Implement `prepare` and verify red-to-green**

  Refactor existing installer/converger only enough to expose typed Python
  entrypoints; retain their CLI parsing and outputs exactly. Never import from
  an operator stage path.

- [ ] **Step 5: Write failing `stage-agent`, `activate`, and `remove` tests**

  Require temporary secret `O_NOFOLLOW|O_EXCL` creation/unlink, expected public
  fingerprint, exact immutable image/origin/instance/key identity, inactive
  staging, compensation on activation failure, zero managed objects before
  exact removal, and retained image cache/system identities.

- [ ] **Step 6: Implement remaining transitions and verify all broker tests**

- [ ] **Step 7: Run existing installer/converger suites and static checks**

  ```bash
  pytest -q tests/ops/test_personal_dev_native_builder_runtime_authority.py \
    tests/ops/test_install_personal_dev_native_builder_runtime.py \
    tests/ops/test_converge_personal_dev_native_builder_release.py
  ruff check scripts/ops/personal_dev_native_builder_runtime_authority.py \
    scripts/ops/install_personal_dev_native_builder_runtime.py \
    scripts/ops/converge_personal_dev_native_builder_release.py \
    tests/ops/test_personal_dev_native_builder_runtime_authority.py
  ```

- [ ] **Step 8: Commit**

  ```bash
  git add scripts/ops/personal_dev_native_builder_runtime_authority.py \
    scripts/ops/install_personal_dev_native_builder_runtime.py \
    scripts/ops/converge_personal_dev_native_builder_release.py \
    tests/ops/test_personal_dev_native_builder_runtime_authority.py
  git commit -m "feat(dev): broker native runtime root transitions"
  ```

### Task 4: Direct-root sealed-source bootstrap

**Files:**

- Create: `scripts/ops/install_personal_dev_native_builder_runtime_authority.py`
- Create: `deploy/personal-dev-native-builder/loom-personal-dev-native-builder-runtime-authority.sudoers`
- Create: `deploy/personal-dev-native-builder/loom-personal-dev-native-builder-runtime-authority.tmpfiles`
- Create: `tests/ops/test_install_personal_dev_native_builder_runtime_authority.py`
- Modify: `scripts/plan_ci_validations.py`

**Interfaces:**

- Produces `bootstrap(source_sha: str, source_tree_sha: str) -> dict[str,
  object]` for direct-root execution only.
- Reuses `staging_rollout_sealed_source.SealedSource` and
  `validate_sealed_source(...)` with the exact authority source and approved
  base.
- Installs the fixed broker at
  `/usr/local/libexec/loom-personal-dev-native-builder-runtime-authority` and
  dependencies under
  `/usr/local/lib/loom-personal-dev-native-builder-runtime-authority/`.

- [ ] **Step 1: Write failing bootstrap/sudoers tests**

  Require direct root and no `SUDO_*`; exact sealed source; validated complete
  asset inventory; root ownership/modes/link counts; atomic no-replace writes;
  fsync; fixed state/lock directories; `visudo -cf`; sudoers published last;
  idempotent byte-identical bootstrap; drift rejection; and rollback of every
  asset/directory created by a failed first attempt. Assert the sudoers file is
  exactly one `qianyi`, `(root)`, `NOPASSWD:NOSETENV`, no-wildcard, quoted-empty
  command rule.

- [ ] **Step 2: Verify red and implement bootstrap transaction**

  Generate the policy from opened source assets, install code/data with
  operation-appropriate modes, validate the installed policy through the
  installed broker library, install tmpfiles/state/lock, validate sudoers, and
  publish sudoers last. Existing non-identical assets are never overwritten.

- [ ] **Step 3: Add CI route ownership and verify green/static checks**

  Route the new scripts and deploy assets to full Python/security validation.
  Run bootstrap tests, sealed-source tests, CI-plan tests, Ruff, mypy, shell
  syntax where applicable, and `git diff --check`.

- [ ] **Step 4: Commit**

  ```bash
  git add scripts/ops/install_personal_dev_native_builder_runtime_authority.py \
    deploy/personal-dev-native-builder/loom-personal-dev-native-builder-runtime-authority.* \
    tests/ops/test_install_personal_dev_native_builder_runtime_authority.py \
    scripts/plan_ci_validations.py
  git commit -m "feat(dev): bootstrap sealed native runtime authority"
  ```

### Task 5: Runbook conversion to fixed pinned transport

**Files:**

- Modify: `docs/runbooks/personal-dev-native-builder-runtime.md`
- Modify: `docs/runbooks/personal-dev-native-builder-acceptance.md`
- Modify: `tests/ops/test_personal_dev_native_builder_runbooks.py`
- Modify: `deploy/worker-pools/gb10/README.md`

**Interfaces:**

- Adds `native_authority_request()` to invoke root-local OpenSSH with `-F
  /dev/null`, exact fixed options, and only the remote broker command.
- Replaces GB10 privileged shell/SCP/Python/systemctl/nft/Docker operations
  with client frames for `status`, `prepare`, `stage-agent`, `activate`, and
  `remove`.
- Retains unprivileged DNS capture and local-only public-key Secret creation.

- [ ] **Step 1: Write failing behavior/policy tests**

  Execute the extracted transport function against fake `sudo`, `ssh`, and
  client binaries. Require exact option argv, no repository SSH config at root,
  no password/interactive auth, remote `sudo -n --` exact broker, stdin
  preservation, and canonical receipt capture. Assert the combined runbooks
  contain no direct remote privileged command or SCP and keep all existing
  no-mutation/secret/ordering assertions.

- [ ] **Step 2: Verify red and convert the runbooks**

  Bind the merged authority source/tree/policy in the window. Convert host
  stages to broker requests; ensure private material flows client → SSH stdin →
  broker frame; retain public-key derivation locally and Kubernetes apply
  locally; record only canonical public receipts.

- [ ] **Step 3: Verify green and extracted Bash syntax**

  Run the complete runbook test module, execute extracted fake-boundary tests,
  parse all Bash blocks with `bash -n`, and scan for stale direct-sudo/SCP
  instructions.

- [ ] **Step 4: Commit**

  ```bash
  git add docs/runbooks/personal-dev-native-builder-runtime.md \
    docs/runbooks/personal-dev-native-builder-acceptance.md \
    deploy/worker-pools/gb10/README.md \
    tests/ops/test_personal_dev_native_builder_runbooks.py
  git commit -m "docs(dev): route native runtime through fixed authority"
  ```

### Task 6: Security and lifecycle review closure

**Files:**

- Modify all Task 1–5 files only for validated findings.
- Add regression tests beside the finding they reproduce.

**Interfaces:** No new public interface; this task hardens the reviewed design.

- [ ] **Step 1: Run focused mutation/security review**

  Mutate canonicality, invocation identity, asset digest, secret cleanup,
  archive ownership, image repository/platform, operation ordering, daemon/nft
  compensation, managed-object ownership, and sudoers argument constraints.
  Confirm a specific test fails for each mutation, then restore.

- [ ] **Step 2: Request independent code review**

  Review exact base/head for spec alignment, root surface, injection/race/TOCTOU,
  signal cleanup, secret lifecycle, transition compensation, and test quality.
  Fix every Critical/Important finding test-first; evaluate Minor findings on
  technical merit.

- [ ] **Step 3: Run complete relevant verification**

  Run all authority/conformance/bootstrap/runbook tests plus every existing
  installer, converger, profile, agent, protocol, executor, probe, route,
  store, and migration test. Run Ruff, mypy, Bash syntax, `git diff --check`,
  forbidden-command scans, and the repository's changed-path validation plan.

- [ ] **Step 4: Commit review fixes**

  ```bash
  git add scripts/ops deploy/personal-dev-native-builder \
    deploy/worker-pools/gb10/README.md docs/runbooks tests/ops \
    scripts/plan_ci_validations.py
  git commit -m "fix(dev): close native runtime authority review"
  ```

### Task 7: Protected integration and operational handoff

**Files:**

- Modify: issue #1280 operational evidence only; no secret-bearing repository
  or issue artifact.

**Interfaces:** Produces one merged protected authority commit and a public
direct-root bootstrap handoff containing source/tree/asset digests and exact
fixed commands.

- [ ] **Step 1: Rebase on current `origin/dev` and rerun full verification**

- [ ] **Step 2: Push, open a `dev` PR, monitor every current-head required CI
  gate, fix failures from root cause, and merge only when all gates succeed**

- [ ] **Step 3: Prepare a root-owned sealed source and administrator handoff**

  The handoff contains only the merged source/tree, approved base, source
  bundle digest, fixed bootstrap command, expected installed public asset
  digests/metadata, `visudo` check, and fixed broker `status` probe. It contains
  no private key, CA, token, kubeconfig, or owner credential.

- [ ] **Step 4: After direct-root bootstrap, open a fresh expiring window and
  execute live acceptance**

  Run `status`, `prepare`, `stage-agent`, `activate`, signed zero-grant
  readiness, two-owner native acceptance, owner-API teardown, retained-data
  redeploy, exact inert rollback, schema-3 evidence verification, and durable
  operational apply.

- [ ] **Step 5: Verify final state from fresh evidence**

  Require zero active native grants, zero dynamic namespaces, unchanged
  Task/Worker/Slurm counts, no managed conformance objects, exact intended
  service/runtime state, no QEMU/runc evidence, and capacity status exactly
  `{"executable_new_capacity_ceiling":0,"status":"ready"}`.

