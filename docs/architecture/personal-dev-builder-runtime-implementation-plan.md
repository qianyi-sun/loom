# Personal-development Builder Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:executing-plans` to implement this plan task-by-task. Do not
> dispatch subagents for this rollout.

**Goal:** Install and prove one exact KVM gVisor builder capability on OLDLAB
agents 2–5, close the incompatible builder scheduling contract, and make the
inert personal-development management shadow ready without enabling personal
workloads or capacity.

**Architecture:** A checked-in public profile binds the gVisor archive, every
installed file, K3s/containerd versions, runsc flags, and RuntimeClass. A
strict node-local installer stages and verifies that profile without restarting
K3s. The RuntimeClass encodes the complete profile digest in two scheduling
labels and sends both amd64 and arm64 target builds to measured amd64 agents,
where the trusted BuildKit image provides arm64 emulation.

**Tech Stack:** Python 3.12 standard library, PyYAML, pytest, Kubernetes
RuntimeClass, K3s v1.36.2, containerd v2.3.2, gVisor runsc, TOML v3
templates, rootless BuildKit, GitHub Actions trusted image releases.

**Spec:** `docs/architecture/personal-dev-builder-runtime.md`

## Global constraints

- Shared infrastructure exists only in `loom-dev`; personal namespaces retain
  the `loom-dev-<owner>` contract.
- The executable-new-capacity ceiling remains exactly `0` throughout.
- Builder preparation and personal lifecycle flags remain false; activation
  replicas remain `0`.
- No personal/build namespace, personal worker, task submission, physical-pool
  mutation, or two-owner acceptance is permitted during this shadow phase.
- Install only gVisor `release-20260810.0`, tag commit
  `5ceb9a5fd5750d6c73dd166441f28306039300d0`, from the exact SHA-512-bound
  archive in the spec.
- Only OLDLAB Kubernetes agents 2–5 are runtime eligible. The control plane is
  never eligible.
- Do not force-drain nodes or bypass PodDisruptionBudgets. Cordon, restart one
  K3s agent, verify continuity, then uncordon.
- Never print kubeconfig bytes, Secret values, environment contents, or
  credential-bearing command output.
- The protected #1280 mutation window remains closed until the new repository
  change is merged, the exact trusted release exists, and every pre-apply stop
  condition passes.

---

### Task 1: Exact runtime profile and RuntimeClass assets

**Files:**

- Create: `deploy/dev-fleet/personal-dev-builder-runtime-profile.json`
- Create: `deploy/dev-fleet/personal-dev-builder-runtime-class.yaml`
- Create: `scripts/ops/personal_dev_builder_runtime_profile.py`
- Create: `tests/ops/test_personal_dev_builder_runtime_profile.py`

**Interfaces:**

- Produces `RuntimeProfile.load(path: Path) -> RuntimeProfile`.
- Produces `RuntimeProfile.sha256`, `RuntimeProfile.selector`,
  `RuntimeProfile.runsc_toml`, and `RuntimeProfile.k3s_template`.
- Produces `load_runtime_profile(path: Path) -> RuntimeProfile` and
  `render_runtime_class(profile: RuntimeProfile) -> dict[str, object]`.
- The installer in Task 2 consumes these exact interfaces.

- [ ] **Step 1: Write strict profile tests**

  Add tests that load the checked-in profile, assert all five member identities,
  assert the exact runsc and K3s bytes, and require the derived selector to be:

  ```python
  assert profile.selector == {
      "kubernetes.io/arch": "amd64",
      "kubernetes.io/os": "linux",
      "loom.dev/personal-dev-runtime-profile-a": profile.sha256[:32],
      "loom.dev/personal-dev-runtime-profile-b": profile.sha256[32:],
  }
  ```

  Parameterize duplicate JSON keys, unknown fields, noncanonical hashes,
  changed paths, changed flags, extra archive members, and changed host
  versions; each must raise `RuntimeProfileError`.

- [ ] **Step 2: Run the focused test and confirm the module is absent**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/ops/test_personal_dev_builder_runtime_profile.py
  ```

  Expected: collection fails because
  `scripts.ops.personal_dev_builder_runtime_profile` does not exist.

- [ ] **Step 3: Add the exact profile bytes**

  Write a reviewable JSON document with a final newline. Its data is exactly:

  ```json
  {
    "archive": {
      "members": {
        "containerd-shim-runsc-v1": {"archive_mode": 493, "install_mode": 365, "sha256": "71b9e90897f39ee51fee8e0345cf675956d95bd1d6458c92f49d984097ffa327", "size": 43208193},
        "gvisor-bin/checkpointgofer": {"archive_mode": 493, "install_mode": 365, "sha256": "a4f6837a9837a8c3499c7e2d1d58931babb140bf228762f1c2b13469256b2bda", "size": 68743833},
        "gvisor-bin/gvisor_sentry": {"archive_mode": 493, "install_mode": 365, "sha256": "871a4b5ca197d37fae7d30ab0aa356fe3156c1f9836e8a40122f7f08c6b46f62", "size": 47910193},
        "gvisor-bin/runsc-metric-server": {"archive_mode": 493, "install_mode": 365, "sha256": "ff3476a1f28cb684bd7340e183e80f8af7a5be5b0b3ca4bdb79bc2a6d92b6cb4", "size": 52294519},
        "runsc": {"archive_mode": 493, "install_mode": 365, "sha256": "670bcd3cbc103f00d8bb5098edc370f32397ee4c134231436bafa659bb3c068e", "size": 104854508}
      },
      "sha512": "3de91138cda15682c11807387f6ecad9e7c8932262018a2813277e1b4efa03efe33b0a948e148c6b1ccfe7345bfab5d5e0d072519505465751273898bae19c62",
      "url": "https://storage.googleapis.com/gvisor/releases/release/20260810/x86_64/gvisor.tar.bz2"
    },
    "host": {
      "architecture": "amd64",
      "containerd_version": "v2.3.2-k3s2",
      "device": "/dev/kvm",
      "k3s_service": "k3s-agent",
      "k3s_version": "v1.36.2+k3s1",
      "modules": ["kvm", "kvm_intel"]
    },
    "installation": {
      "k3s_template": "/var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.tmpl",
      "profile": "/etc/loom/personal-dev-builder-runtime-profile.json",
      "release_root": "/opt/loom/gvisor/release-20260810.0",
      "runsc_config": "/etc/containerd/runsc-personal-dev.toml",
      "shim_link": "/usr/local/bin/containerd-shim-runsc-v1"
    },
    "release": {
      "tag_commit": "5ceb9a5fd5750d6c73dd166441f28306039300d0",
      "version": "release-20260810.0"
    },
    "runtime": {
      "flags": {
        "allow-flag-override": "false",
        "allow-packet-socket-write": "false",
        "allow-suid": "false",
        "debug": "false",
        "directfs": "false",
        "file-access": "exclusive",
        "file-access-mounts": "shared",
        "gvisor-marker-file": "true",
        "host-fifo": "none",
        "host-settings": "check",
        "host-uds": "none",
        "net-raw": "false",
        "network": "sandbox",
        "oci-seccomp": "true",
        "platform": "kvm",
        "platform_device_path": "/dev/kvm",
        "profile": "false",
        "restore-spec-validation": "enforce",
        "sidecar-release-enforcement-policy": "ALWAYS",
        "strace": "false",
        "watchdog-action": "panic"
      },
      "handler": "runsc-personal-dev",
      "runtime_type": "io.containerd.runsc.v1"
    },
    "runtime_class": {
      "name": "loom-personal-dev-builder",
      "profile_label_encoding": "sha256-halves-v1"
    },
    "schema": "loom.personal-dev-builder-runtime-profile.v1"
  }
  ```

  Decimal archive mode `493` is octal `0755`; decimal install mode `365` is
  octal `0555`. The parser rejects any other value.

- [ ] **Step 4: Implement strict parsing and deterministic renderers**

  Use `json.loads(..., object_pairs_hook=...)` to reject duplicates. Require
  the original bytes to equal `json.dumps(value, sort_keys=True, indent=2,
  ensure_ascii=True) + "\n"`, then require the complete nested value to equal
  the fixed schema contract and calculate the digest from those bytes. Render
  runsc TOML with the absolute `binary_name`, sorted flag keys, and a final
  newline. Render the K3s template exactly as the spec. The RuntimeClass
  renderer must return:

  ```python
  {
      "apiVersion": "node.k8s.io/v1",
      "kind": "RuntimeClass",
      "metadata": {
          "name": profile.runtime_class_name,
          "annotations": {
              "loom.dev/runtime-profile-sha256": profile.sha256,
          },
      },
      "handler": profile.handler,
      "scheduling": {"nodeSelector": profile.selector},
  }
  ```

- [ ] **Step 5: Generate and verify the checked-in RuntimeClass**

  Serialize with `yaml.safe_dump(..., sort_keys=False)`. The test reloads the
  YAML and compares it to `render_runtime_class(profile)`, preventing profile,
  selector, annotation, or handler drift.

- [ ] **Step 6: Run tests and commit**

  Run the focused pytest, Ruff on the new module/tests, mypy on the new module,
  and `git diff --check`. Commit:

  ```bash
  git add deploy/dev-fleet/personal-dev-builder-runtime-profile.json \
    deploy/dev-fleet/personal-dev-builder-runtime-class.yaml \
    scripts/ops/personal_dev_builder_runtime_profile.py \
    tests/ops/test_personal_dev_builder_runtime_profile.py
  git commit -m "feat(dev): bind measured gvisor runtime profile"
  ```

---

### Task 2: Fail-closed node installer

**Files:**

- Create: `scripts/ops/install_personal_dev_builder_runtime.py`
- Create: `tests/ops/test_install_personal_dev_builder_runtime.py`

**Interfaces:**

- Consumes `RuntimeProfile` from Task 1.
- Produces `InstallContext`, `CommandResult`, `Runner`, `SubprocessRunner`, and
  `PersonalDevBuilderRuntimeInstaller`.
- Produces methods `preflight(archive: Path)`, `install(archive: Path)`,
  `verify_staged()`, `verify_active()`, and `remove()` returning canonical
  JSON-safe receipt dictionaries.

- [ ] **Step 1: Write archive and destination safety tests**

  Build in-memory bzip2 tar fixtures. Assert exact extraction succeeds and
  reject traversal, absolute paths, duplicates, symlinks, hardlinks, devices,
  missing/extra members, wrong sizes, wrong modes, wrong file hashes, wrong
  archive SHA-512, and trailing members. Assert no destination is published on
  any failure.

- [ ] **Step 2: Write installer state tests**

  Use `InstallContext(root=tmp_path, authority_uid=os.getuid(),
  authority_gid=os.getgid())` and a fake runner. Cover:

  - successful preflight and install;
  - root ownership, `0555` releases, `0444` config/profile, exact shim link;
  - fsync of published files and containing directories;
  - idempotent complete reinstall;
  - partial, writable, multiply linked, non-root-owned, wrong-link, or
    nonidentical destination rejection;
  - control-plane service rejection and exact `k3s-agent` requirement;
  - missing `/dev/kvm`, module, disk, PATH, K3s, or containerd prerequisite;
  - staged verification before restart;
  - active verification of the rendered containerd runtime options; and
  - exact-only removal plus mismatch preservation.

- [ ] **Step 3: Run tests and confirm the installer is absent**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/ops/test_install_personal_dev_builder_runtime.py
  ```

  Expected: collection fails on the missing installer module.

- [ ] **Step 4: Implement safe archive verification**

  Stream the supplied archive through `tarfile.open(mode="r|bz2")`. Validate a
  member before reading it, copy with `os.open(..., O_EXCL|O_NOFOLLOW)`, enforce
  the declared size while streaming, fsync, hash, chmod, and publish the
  complete release directory only after all five members and the archive hash
  match. Never use `extractall`.

- [ ] **Step 5: Implement host preflight and exact publication**

  Validate effective root for the production context, amd64, KVM character
  device, both modules in `/proc/modules`, at least 20 GiB free at the K3s data
  filesystem, exact versions, active `k3s-agent`, inactive `k3s`, shim PATH,
  and destination absence/identity. Publish only the fixed profile paths.
  Reject an unrelated existing K3s template instead of merging it.

- [ ] **Step 6: Implement staged/active verification and removal**

  Staged verification rereads every inode, owner, group, mode, link count,
  target, hash, profile, and config byte. Active verification parses the
  generated containerd TOML with `tomllib` and requires:

  ```python
  runtime = document["plugins"]["io.containerd.cri.v1.runtime"][
      "containerd"
  ]["runtimes"][profile.handler]
  assert runtime == {
      "runtime_type": profile.runtime_type,
      "options": {
          "TypeUrl": "io.containerd.runsc.v1.options",
          "ConfigPath": str(profile.runsc_config_path),
      },
  }
  ```

  `remove` first runs staged verification, then unlinks only the fixed files and
  removes only now-empty managed directories from deepest to shallowest.

- [ ] **Step 7: Add the production CLI**

  Accept exactly one operation and `--profile`. Require `--archive` only for
  `preflight` and `install`, forbid it for the other operations, emit one
  sorted compact JSON receipt on stdout, and emit bounded error classes without
  path contents or command output on stderr.

- [ ] **Step 8: Run tests and commit**

  Run focused pytest, Ruff, strict mypy, and `git diff --check`. Commit:

  ```bash
  git add scripts/ops/install_personal_dev_builder_runtime.py \
    tests/ops/test_install_personal_dev_builder_runtime.py
  git commit -m "feat(dev): install gvisor runtime fail closed"
  ```

---

### Task 3: Compatible cross-platform builder scheduling

**Files:**

- Modify: `src/loom/personal_dev_builder_manifest.py`
- Modify: `tests/unit/test_personal_dev_builder_manifest.py`
- Modify: `tests/unit/test_personal_dev_builder_runtime.py`
- Modify: `src/loom_cli/data/loom-schema.toml`
- Modify: `tests/loom_config/snapshots/loom_service.json`

**Interfaces:**

- Keeps `personal_dev_builder_manifest_documents(...)` unchanged.
- Changes the generated Pod contract to omit both `hostUsers` and
  `nodeSelector`; RuntimeClass scheduling owns host architecture and
  eligibility.
- Target `platform` remains exact in the immutable contract and output checks.

- [ ] **Step 1: Change tests first**

  Replace the existing assertions with:

  ```python
  assert "hostUsers" not in spec
  assert "nodeSelector" not in spec
  assert spec["runtimeClassName"] == "loom-personal-dev-builder"
  ```

  In the runtime test, require neither field in serialized non-secret YAML and
  retain every existing Secret/API-token/security assertion.

- [ ] **Step 2: Run the two tests and observe failure**

  Run the manifest/runtime tests. Expected: both fail because the two fields
  are still emitted.

- [ ] **Step 3: Remove only the incompatible fields**

  Delete `hostUsers: False` and the architecture node selector. Do not change
  resources, RuntimeClass, platform contracts, security context, network
  policy, capability Secret, or artifact verification.

- [ ] **Step 4: Correct the schema description and snapshot**

  Describe the operator-installed class as a measured gVisor kernel boundary;
  do not claim that it supplies Kubernetes host user namespaces. Regenerate or
  mechanically update only the matching schema snapshot entry.

- [ ] **Step 5: Run tests and commit**

  Run builder manifest/runtime/sandbox-builder tests, schema snapshot tests,
  Ruff, mypy, and `git diff --check`. Commit:

  ```bash
  git add src/loom/personal_dev_builder_manifest.py \
    tests/unit/test_personal_dev_builder_manifest.py \
    tests/unit/test_personal_dev_builder_runtime.py \
    src/loom_cli/data/loom-schema.toml \
    tests/loom_config/snapshots/loom_service.json
  git commit -m "fix(dev): schedule builders through measured runtime"
  ```

---

### Task 4: Shadow and acceptance require the exact RuntimeClass profile

**Files:**

- Modify: `src/loom/personal_dev_control_plane_status.py`
- Modify: `src/loom/personal_dev_control_plane_config.py`
- Modify: `src/loom/personal_dev_control_plane_render.py`
- Modify: `deploy/dev-fleet/personal-dev-control-plane.toml`
- Modify: `tests/unit/test_personal_dev_control_plane_config.py`
- Modify: `tests/unit/test_personal_dev_control_plane_render.py`
- Modify: `tests/unit/test_personal_dev_control_plane_status.py`
- Modify: `tests/loom_cli/test_personal_dev_control_plane_cmd.py`

**Interfaces:**

- Extends the non-secret builder profile with exact `runtime_handler` and
  `runtime_profile_sha256` fields while keeping `prepared=false`.
- Extends `RenderedPersonalDevControlPlane` with the three runtime identity
  values needed by status; no new CLI argument is required.
- Keeps public CLI/status JSON shapes unchanged.

- [ ] **Step 1: Rebase on the merged #1465 status normalization**

  Fetch `origin/dev`, require PR #1465 to be merged, and rebase this branch
  before editing the shared status file. Re-run the 147-test baseline plus the
  #1465 server-canonical focused tests.

- [ ] **Step 2: Bind the measured runtime in the non-secret profile**

  Add exact handler and 64-lowercase-hex validators to `_BuilderInput` and
  `PersonalDevBuilderTrust`. Put `runsc-personal-dev` and Task 1's checked-in
  profile file SHA-256 in `personal-dev-control-plane.toml`. Require the
  acceptance plan's class, handler, and profile digest to equal the non-secret
  profile. Add config tests for missing, malformed, and mismatched values.

- [ ] **Step 3: Carry runtime identity through the render result**

  Add `runtime_class_name`, `runtime_handler`, and `runtime_profile_sha256` to
  `RenderedPersonalDevControlPlane`, populated from the non-secret profile in
  shadow and from the already cross-validated plan in acceptance. The profile's
  canonical value already makes these fields part of the render input digest;
  no runtime asset is added to the package YAML.

- [ ] **Step 4: Add drift matrix cases**

  Make healthy shadow and acceptance RuntimeClass fixtures include the exact
  handler, annotation, and derived selector. Add mutations for handler/profile
  mismatch, missing scheduling, missing/extra selector key, changed half-digest,
  wrong OS/architecture, nonempty tolerations, and nonempty overhead. Shadow
  mutations produce `runtime_class_missing`; acceptance mutations produce
  `runtime_class_binding_invalid`.

- [ ] **Step 5: Run config/render/status tests and observe failure**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_control_plane_config.py \
    tests/unit/test_personal_dev_control_plane_render.py \
    tests/unit/test_personal_dev_control_plane_status.py \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py
  ```

  Expected: new required profile fields and scheduling mutations fail under the
  current implementation.

- [ ] **Step 6: Require the exact scheduling contract**

  Derive the selector with one private helper from the validated 64-hex digest.
  Shadow uses the rendered profile binding; acceptance uses the plan binding.
  Require exact handler, annotation, scheduling/nodeSelector, absent or empty
  tolerations, and absent overhead. Continue allowing unrelated metadata
  annotations because server-side apply owns field-manager metadata.

- [ ] **Step 7: Replay actual server canonicalization and commit**

  Run the #1465 focused test set and replay harness against the saved complete
  server-side diff. Then run Ruff, strict mypy, and `git diff --check`. Commit:

  ```bash
  git add src/loom/personal_dev_control_plane_config.py \
    src/loom/personal_dev_control_plane_render.py \
    src/loom/personal_dev_control_plane_status.py \
    deploy/dev-fleet/personal-dev-control-plane.toml \
    tests/unit/test_personal_dev_control_plane_config.py \
    tests/unit/test_personal_dev_control_plane_render.py \
    tests/unit/test_personal_dev_control_plane_status.py \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py
  git commit -m "fix(dev): bind shadow to measured runtime"
  ```

---

### Task 5: Exact rollout and rollback runbook

**Files:**

- Create: `docs/runbooks/personal-dev-builder-runtime.md`
- Modify: `docs/runbooks/README.md`
- Modify: `deploy/dev-fleet/README.md`
- Modify: `docs/architecture/personal-dev-management-plane-deployment.md`
- Modify: `tests/ops/test_personal_dev_control_plane_package_boundary.py`

**Interfaces:**

- Documents the installer CLI from Task 2 and static assets from Task 1.
- Adds no generic deployment command and no automatic restart.

- [ ] **Step 1: Add documentation-boundary tests**

  Assert the runbook names the exact profile, archive hash, agents 2–5,
  control-plane exclusion, cordon-without-drain rule, per-node stop/rollback,
  full profile labels, gVisor marker, rootless BuildKit amd64/arm64 conformance,
  RuntimeClass server-side diff, capacity ceiling zero, and the prohibition on
  personal/build namespaces.

- [ ] **Step 2: Run the boundary test and observe failure**

  Run the package-boundary test. Expected: failure because the runbook and
  index entries are absent.

- [ ] **Step 3: Write the executable runbook**

  Include exact commands for owner-only evidence creation, archive download as
  the unprivileged operator, SHA-512 verification, SSH host identity binding,
  baseline capture, per-node preflight/install/verify, cordon/restart/readiness,
  Pod continuity, smoke, BuildKit cross-build, labels, uncordon, RuntimeClass
  dry-run/diff/apply, and exact rollback. Use the reviewed kubeconfig only via a
  variable and never display it.

- [ ] **Step 4: Update architecture and indexes**

  Link the measured runtime design/runbook and replace any claim that the
  RuntimeClass supplies Kubernetes user namespaces. Keep shadow versus
  acceptance authority explicit.

- [ ] **Step 5: Run docs tests and commit**

  Run the package-boundary tests, Markdown/reference checks used by CI, and
  `git diff --check`. Commit:

  ```bash
  git add docs/runbooks/personal-dev-builder-runtime.md docs/runbooks/README.md \
    deploy/dev-fleet/README.md \
    docs/architecture/personal-dev-management-plane-deployment.md \
    tests/ops/test_personal_dev_control_plane_package_boundary.py
  git commit -m "docs(dev): add measured runtime rollout"
  ```

---

### Task 6: Repository verification, iterative review, and PR

**Files:** all files changed in Tasks 1–5.

- [ ] **Step 1: Run focused verification**

  Run all new tests plus builder, status, CLI, renderer, package-boundary,
  schema snapshot, release, Ruff, and strict mypy suites.

- [ ] **Step 2: Run repository verification proportional to scope**

  Run the repository's authoritative local test command, Go checks if selected
  by ownership, workflow-plan tests, and `git diff --check`. Record exact
  commands and counts.

- [ ] **Step 3: Self-review until clean**

  Review the complete branch diff against the spec. Inspect every filesystem
  mutation, error path, symlink/link-count check, subprocess argument, JSON
  receipt, rollback target, builder security field, RuntimeClass selector, and
  documentation command. Fix and rerun focused/full verification after every
  finding. Stop only when one complete review pass produces no finding.

- [ ] **Step 4: Push and create a normal PR**

  Push `feat/personal-dev-gvisor-runtime`, create a PR against `dev`, enable
  squash auto-merge, and record the PR number. Do not deploy from the PR head.

- [ ] **Step 5: Monitor authoritative checks to squash merge**

  Treat retired carrier checks as non-authoritative only when their exact
  authoritative replacement is successful. Investigate any real failure with
  the systematic-debugging workflow. Fetch and bind the exact merged commit and
  tree after merge.

---

### Task 7: Protected gVisor node rollout

**Files:** owner-only operational evidence outside the Git worktree.

- [ ] **Step 1: Create a fresh empty owner-only evidence directory**

  Name it from UTC time and the exact merged SHA. Copy only public profile and
  RuntimeClass bytes plus the reviewed mode-0600 kubeconfig snapshot; record
  hashes and inode identity without printing contents.

- [ ] **Step 2: Re-prove global stop conditions**

  Require five Ready nodes, no DiskPressure, Longhorn healthy, no personal or
  build namespace, no personal worker, no package-owned shadow resource yet,
  exact Secret key inventory, and executable-new-capacity ceiling zero.

- [ ] **Step 3: Download and bind the gVisor archive**

  Download as the unprivileged operator, require mode 0600/single link/current
  ownership, verify the exact SHA-512, and run local profile/archive preflight.

- [ ] **Step 4: Roll agents 2–5 sequentially**

  For one node at a time, capture Pods, cordon, stage, verify staged, restart
  only `k3s-agent`, wait Ready, verify active, prove Pod/Longhorn continuity,
  run digest-pinned gVisor smoke, apply exact node labels/annotation, and
  uncordon. Re-run global stop conditions before advancing.

- [ ] **Step 5: Prove BuildKit on the canary**

  Use the digest-pinned trusted builder image and its existing
  `buildkit-qemu-aarch64`. Build a minimal digest-pinned base through one RUN
  step for both target platforms inside gVisor. Verify OCI platform metadata and
  `uname -m` output, then delete only the temporary smoke resources.

- [ ] **Step 6: Apply the exact RuntimeClass**

  Save complete server-side dry-run/diff, audit every byte, apply the static
  asset server-side, and require live handler, annotation, and scheduling to
  equal the profile. Keep all personal feature flags disabled.

- [ ] **Step 7: Close runtime rollout evidence**

  Hash receipts and post-state. Update issue #1280 with sanitized hashes,
  nodes, profile, RuntimeClass, and zero-capacity results. Do not include node
  command output that might expose environment or credentials.

---

### Task 8: New trusted release and protected shadow apply

**Files:** fresh owner-only shadow evidence outside the Git worktree.

- [ ] **Step 1: Obtain the protected trusted release for the exact merged SHA**

  Let the default-branch trusted release controller publish the merged commit.
  Download the exact artifact and verify its release/evidence hashes, source
  SHA, source tree, architectures, immutable image digests, and scanner assets.

- [ ] **Step 2: Re-render and review a fresh inert shadow**

  Use the exact merged source and trusted release. Require builder, lifecycle,
  and activation flags false, activation replicas zero, capacity ceiling zero,
  and no dynamic namespace. Save and byte-audit the complete server-side diff,
  including API canonicalization accepted by #1465.

- [ ] **Step 3: Open the explicit #1280 shadow window**

  Record the approved start/expiry, exact source/tree/release/profile/render
  hashes, rollback policy, resource identities, reviewed kubeconfig hash, and
  pre-state. Do not reuse the earlier failed window or evidence directory.

- [ ] **Step 4: Apply only the reviewed shadow**

  Re-run every stop condition immediately before apply. Apply the exact reviewed
  YAML server-side. Wait for PVCs, PostgreSQL, MinIO, migration, and management
  readiness without enabling personal controllers.

- [ ] **Step 5: Require canonical ready shadow status**

  Require `ready=true`, `blockers=[]`, `mode=shadow`, all shared components
  ready, RuntimeClass handler/profile/scheduling exact, manager ceiling zero,
  five Ready nodes, no DiskPressure, Longhorn healthy, and no personal/build
  namespaces or personal workers.

- [ ] **Step 6: Close the shadow window**

  Publish only sanitized evidence hashes and final guardrail state to #1280.
  Preserve all owner-only evidence. Do not proceed to acceptance enablement,
  two-owner applications, task submission, or physical capacity in this plan.
