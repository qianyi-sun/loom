# Task-image builder Phase 1 prerequisites implementation plan

> **Live-convergence status:** Superseded by
> `archive/docs/architecture/2026-08-19-task-image-builder-phase1-isolation-correction.md`.
> PR #1457 remains fail-closed and must not be applied without the isolation
> repair and the separate site-specific host convergence boundary.

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the exact, root-owned cluster prerequisite and evidence framework
for allocation-scoped rootless task-image builders while keeping production
claims disabled and certifying zero nodes.

**Architecture:** A versioned TOML policy is the single source for both native
clusters, the pinned rootless runtime release, fixed resource limits, and every
activation prerequisite. Root-owned, idempotent convergers prepare the
dedicated Unix/Slurm identity, higher-tier overlapping partition, capped QoS,
and validated runtime files without touching legacy reservations or enabling a
builder. A read-only conformance tool validates canonical evidence against a
strict JSON schema and the policy; Phase 1 hard-codes the production
certification gate closed until Phase 2 supplies the node guard/provider
release.

**Tech Stack:** Python 3.11, TOML, JSON Schema draft 2020-12, Bash, Slurm CLI,
rootless BuildKit v0.32.2, RootlessKit v3.1.0, slirp4netns v1.3.4,
fuse-overlayfs v1.17, pytest, Ruff, strict mypy.

## Global Constraints

- Implement only design Phase 1, "cluster prerequisites and certification".
- Do not add a provider, executor, materialization claim, publication path,
  shadow campaign, architecture fence, or legacy retirement from Phases 2-5.
- Do not enable either new rootless builder policy, advertise the
  `loom_rootless_buildkit` Slurm feature, or certify any production node.
- Preserve the legacy exclusive backend and both exact named reservations;
  no script in this phase may create, update, or delete a reservation.
- Dynamic builder partitions overlap the trial-node inventory, use
  `PriorityTier=200`, and never pin a permanent node, request `--exclusive`, or
  reserve capacity.
- The builder QoS permits at most one submitted/running allocation per cluster,
  `cpu=8`, `mem=32768M`, `node=1`, and `MaxWall=02:00:00` in aggregate.
- The dedicated operating-system identity is `loom-builder`, UID `993`, GID
  `980`, with subordinate UID/GID range `3000000:65536`; it is never added to
  Docker or another privileged group.
- Runtime installation is offline-only from pre-staged artifacts whose archive
  and installed-binary SHA-256 digests match the committed manifest.
- The selected snapshotter is exactly `fuse-overlayfs`; runtime fallback or
  `auto` selection is forbidden.
- Read-only conformance must reject incomplete, secret-bearing, stale,
  cross-policy, or self-certified evidence.
- Phase 1 evidence always reports `production_certification_allowed=false`,
  `certified_nodes=[]`, and the missing Phase 2 guard/provider as a blocker.
- Do not create or push any path under `docs/superpowers/**`.
- Do not mutate GB10 through Docker-group/root-equivalent workarounds.
- Do not rerun task/run `4139e767`; incident acceptance remains Phase 4.
- Use the locked `uv==0.11.26` environment and make no dependency changes.
- Integrate only through a PR to `dev`, passing required CI before squash merge.

---

## File map

- `deploy/task-image-builder/prerequisites-v1.toml` is the canonical Phase 1
  policy for cluster identities, shared node sets, Slurm limits, cgroup
  requirements, storage/network prerequisites, and the closed activation gate.
- `deploy/task-image-builder/rootless-runtime-v1.json` pins upstream artifact
  and installed binary digests for both native architectures.
- `scripts/ops/task_image_builder_prerequisite_conformance.py` loads policy and
  evidence, rejects secrets, validates syntax and semantics, and emits a
  canonical pass/fail report without mutation.
- `docs/evidence/task-image-builder-prerequisite-conformance-v1.schema.json`
  defines the strict evidence envelope.
- `deploy/slurm/install-loom-task-image-builder-node-prerequisites.sh` installs
  only the unprivileged identity, subordinate IDs, and verified runtime release
  from an offline artifact directory; its check mode is read-only.
- `deploy/slurm/converge-loom-task-image-builder-prerequisites.sh` converges the
  dedicated Slurm account/association/QoS and higher-tier overlapping partition
  on the exact controller; it never touches reservations or features.
- `tests/ops/test_task_image_builder_prerequisite_profile.py` protects policy,
  release pins, and the closed certification gate.
- `tests/ops/test_task_image_builder_prerequisite_conformance.py` exercises the
  real verifier with hand-derived fixtures.
- `tests/ops/test_task_image_builder_node_prerequisites_install.py` executes the
  sourceable node installer against isolated fake roots and artifact fixtures.
- `tests/ops/test_task_image_builder_prerequisite_converge.py` executes the
  sourceable Slurm converger against fake Slurm commands and configuration.

### Task 1: Pin the Phase 1 policy and runtime supply chain

**Files:**

- Create: `deploy/task-image-builder/prerequisites-v1.toml`
- Create: `deploy/task-image-builder/rootless-runtime-v1.json`
- Create: `tests/ops/test_task_image_builder_prerequisite_profile.py`

**Interfaces:**

- Produces policy schema `loom.task-image-builder-prerequisites/v1` with
  `policy_version`, `production_certification_allowed`, `certified_nodes`,
  `runtime_manifest`, `resource_profile`, and two `clusters` rows.
- Produces runtime schema `loom.task-image-builder-rootless-runtime/v1` with
  architecture-keyed archives and installed binaries.
- Preserves legacy `task_image_builder_policies` unchanged.

- [ ] **Step 1: Write the failing profile contract test**

Load both files with `tomllib`/`json`. Assert exact cluster/node mappings
(`oldlab` to `trt-eai-oldlab-[3-5]`, `gb10` to `trt-gb10-[1-15]`), builder
partition tier 200 above trial tier 100, the aggregate resource ceiling, all
four cgroup constraints, fixed identity/subordinate IDs, exact snapshotter and
network flags, full lowercase SHA-256 values, and the permanently closed Phase
1 gate with an empty certified-node list. Also assert no policy field contains
`exclusive`, `reservation`, `nodelist`, or a Docker socket.

- [ ] **Step 2: Run the test and observe missing policy files**

Run:

```bash
uv run --no-sync pytest -q \
  tests/ops/test_task_image_builder_prerequisite_profile.py
```

Expected: FAIL because neither Phase 1 policy file exists.

- [ ] **Step 3: Add the exact declarative policy**

Use `PriorityTier=200`, `cpu=8`, `memory_mib=32768`, `pids=4096`,
`scratch_bytes=107374182400`, `scratch_inodes=1000000`, and a two-hour wall
limit. Require cgroup v2, `task/cgroup`, `proctrack/cgroup`, core/RAM/swap/device
confinement, zero swap, `pids`/`io` delegation, quota-backed scratch, bpffs,
unprivileged user namespaces, pidfd/sealed-memfd/clone-into-cgroup support, and
fail-closed IPv4/IPv6 byte/packet/flow/DNS policy. Set
`production_certification_allowed=false` and name
`phase2_guard_provider_release_missing` as an unconditional blocker.

- [ ] **Step 4: Pin the reviewed native runtime artifacts**

Pin BuildKit `v0.32.2`, RootlessKit `v3.1.0`, slirp4netns `v1.3.4`, and
fuse-overlayfs `v1.17` for `x86_64` and `aarch64`. Record the upstream archive
digests, the extracted `buildkitd`, `buildctl`, `buildkit-runc`, `rootlesskit`,
and `rootlessctl` digests, and the direct helper-binary digests. Exclude QEMU
and CNI binaries because native builds and the exact slirp network are the
policy.

- [ ] **Step 5: Run the profile test**

Expected: PASS.

- [ ] **Step 6: Commit the policy boundary**

```bash
git add deploy/task-image-builder tests/ops/test_task_image_builder_prerequisite_profile.py
git commit -m "ops(builder): pin rootless prerequisite policy"
```

### Task 2: Add strict read-only conformance evidence

**Files:**

- Create: `docs/evidence/task-image-builder-prerequisite-conformance-v1.schema.json`
- Create: `scripts/ops/task_image_builder_prerequisite_conformance.py`
- Create: `tests/ops/test_task_image_builder_prerequisite_conformance.py`
- Modify: `docs/evidence/README.md`

**Interfaces:**

- Produces `load_policy(path: Path) -> PrerequisitePolicy`.
- Produces `verify_evidence(evidence: Mapping[str, object], policy: PrerequisitePolicy) -> list[str]` and
  `certification_blockers(policy: PrerequisitePolicy) -> tuple[str, ...]`.
- CLI: `plan --policy PATH`, `verify --policy PATH --evidence PATH`, and
  `canonicalize --policy PATH --evidence PATH --output PATH`.
- The CLI has no SSH, Slurm subprocess, Docker, systemd, write-to-host, or
  mutation path; canonicalize writes only the caller-selected evidence output.

- [ ] **Step 1: Write failing schema and semantic-verifier tests**

Build a complete hand-derived two-cluster evidence fixture. Prove that the real
verifier accepts its prerequisite syntax/semantics while the separate
certification result returns the unconditional Phase 1 blocker. Parameterize
failures for wrong policy digest, incomplete node set,
wrong controller/architecture, any disabled cgroup constraint, lower/equal
partition tier, non-overlapping partition nodes, loose QoS, legacy builder
identity, missing subordinate IDs/tool digests/network flags/quota/bpffs,
installed or active guard falsely claimed without a Phase 2 release, nonempty
`certified_nodes`, stale timestamps, and secret-like keys/values.

- [ ] **Step 2: Run the focused tests and observe missing interfaces**

Expected: collection FAIL because the schema and verifier do not exist.

- [ ] **Step 3: Implement bounded parsing and secret rejection**

Limit policy/evidence inputs to 2 MiB, reject symlinks and non-object roots,
check the JSON Schema with a format checker, recursively reject secret-like
field names and bearer/API/URL credential patterns, and derive the policy
digest from canonical JSON produced from normalized TOML.

- [ ] **Step 4: Implement cross-field semantic verification**

Compare every cluster and node observation against policy rather than trusting
reported pass booleans. Require exact partition overlap and tier ordering,
aggregate QoS limits, Slurm plugins/cgroup constraints, identity/subordinate
IDs, runtime binary digests, exact RootlessKit/slirp flags, quota storage,
network-controller prerequisites, absent forbidden sockets, and all expected
nodes. Reject any attempt to self-certify; report the Phase 2 blocker separately
so a valid Phase 1 inventory is distinguishable from production eligibility.

- [ ] **Step 5: Implement deterministic plan/verify/canonicalize CLI behavior**

`plan` prints requirements and `mutations_supported=false`; `verify` prints a
sorted report and returns 0 only for schema/semantic conformance while still
reporting `production_certification_allowed=false`; `canonicalize` writes an
owner-readable canonical copy only after validation and never overwrites a
symlink.

- [ ] **Step 6: Run conformance tests and repository path checks**

```bash
uv run --no-sync pytest -q \
  tests/ops/test_task_image_builder_prerequisite_conformance.py \
  tests/ops/test_repository_path_policy.py
```

Expected: PASS.

- [ ] **Step 7: Commit the evidence boundary**

```bash
git add docs/evidence scripts/ops/task_image_builder_prerequisite_conformance.py \
  tests/ops/test_task_image_builder_prerequisite_conformance.py
git commit -m "feat(builder): verify prerequisite evidence"
```

### Task 3: Install node prerequisites without granting capability

**Files:**

- Create: `deploy/slurm/install-loom-task-image-builder-node-prerequisites.sh`
- Create: `tests/ops/test_task_image_builder_node_prerequisites_install.py`

**Interfaces:**

- CLI: `check <cluster-id> <artifact-dir>` and
  `apply <cluster-id> <artifact-dir>`.
- Consumes the exact Phase 1 TOML/JSON policy files from the same candidate.
- Produces `/opt/loom-task-builder/releases/rootless-runtime-v1/` and an atomic
  `/opt/loom-task-builder/current` symlink plus the dedicated Unix identity and
  exact `/etc/subuid`/`/etc/subgid` rows.
- Does not install a node guard, feature label, credential, service, or policy
  activation artifact.

- [ ] **Step 1: Write failing behavioral installer tests**

Run the sourceable installer with isolated roots and fake identity commands.
Prove check mode performs no writes; apply validates every artifact before any
identity or filesystem change; wrong digest/architecture/host fails closed;
identity/subordinate-ID conflicts fail; the first valid apply creates an exact
root-owned immutable release; a second apply is idempotent; and unexpected
files, symlinks, QEMU/CNI binaries, Docker-group membership, or release drift
are rejected.

- [ ] **Step 2: Run the tests and observe the missing installer**

Expected: FAIL because the installer does not exist.

- [ ] **Step 3: Implement offline artifact validation**

Resolve the policy/manifest relative to the checked-out installer, accept only
the exact six architecture-specific assets, reject symlinks and unsafe modes,
and verify all upstream digests before extracting into a private temporary
directory. Extract only the committed binary allowlist, verify installed-file
digests, and reject archive traversal or extra selected files.

- [ ] **Step 4: Implement fixed identity and atomic release convergence**

Create group GID 980 and system user UID 993 with `/nonexistent` and
`/usr/sbin/nologin`, atomically converge one exact subuid/subgid row, install
root-owned mode-0755 binaries into a versioned directory, write a canonical
receipt, fsync/rename the complete release, and change the `current` symlink
only after readback. Never add supplementary groups.

- [ ] **Step 5: Keep check mode negative for certification**

Check identity, subordinate IDs, required host helpers (`newuidmap` and
`newgidmap`), runtime digests, user namespaces, bpffs, quota filesystem, and all
four Slurm cgroup constraints. Emit structured failures but never advertise a
Slurm feature or certify the node.

- [ ] **Step 6: Run the installer tests**

Expected: PASS.

- [ ] **Step 7: Commit node preparation**

```bash
git add deploy/slurm/install-loom-task-image-builder-node-prerequisites.sh \
  tests/ops/test_task_image_builder_node_prerequisites_install.py
git commit -m "ops(builder): install rootless node prerequisites"
```

### Task 4: Converge bounded Slurm prerequisites without reservations

**Files:**

- Create: `deploy/slurm/converge-loom-task-image-builder-prerequisites.sh`
- Create: `tests/ops/test_task_image_builder_prerequisite_converge.py`

**Interfaces:**

- CLI: `check` and `apply`, controller-root only for apply.
- OLDLAB partition: `loom-task-builder`, nodes
  `trt-eai-oldlab-[3-5]`, `PriorityTier=200`.
- GB10 partition: `loom-task-builder`, nodes `trt-gb10-[1-15]`,
  `PriorityTier=200`.
- Slurm account/user/QoS: `loom-task-builder` / `loom-builder` /
  `loom-task-image-builder` with one-job and aggregate TRES ceilings.

- [ ] **Step 1: Write failing fake-Slurm convergence tests**

Execute check/apply with fake `scontrol`, `sacctmgr`, and exact temporary
`slurm.conf` files. Prove first apply adds only the exact partition and bounded
account/QoS/association, second apply is idempotent, check mode never mutates,
configuration or readback drift fails, a rejected reconfigure restores the
exact backup, and neither command invokes reservation, node-feature,
`--exclusive`, `scancel`, or deletion operations.

- [ ] **Step 2: Run the test and observe the missing converger**

Expected: FAIL because the converger does not exist.

- [ ] **Step 3: Implement exact controller and policy selection**

Require hostname, architecture, ClusterName, and controller readback to match
one policy cluster. Expand both builder and trial partitions through `scontrol
show hostnames`; require exact equal node sets and a strictly higher builder
tier.

- [ ] **Step 4: Implement rollback-safe partition convergence**

Accept only the reviewed pre-state or exact desired line, keep a root-owned
0600 pre-change backup, insert the desired partition beside its exact trial
partition anchor, reconfigure, and restore/reconfigure the backup on any live
readback mismatch.

- [ ] **Step 5: Implement bounded accounting convergence and readback**

Converge the dedicated account, association restricted to the builder
partition/QoS, and `DenyOnLimit` QoS with exact job, wall, and `GrpTRES` values.
Reject existing drift rather than broadening it, and verify one exact
association row. Never inherit the normal trial QoS.

- [ ] **Step 6: Run the Slurm converger tests**

Expected: PASS.

- [ ] **Step 7: Commit controller preparation**

```bash
git add deploy/slurm/converge-loom-task-image-builder-prerequisites.sh \
  tests/ops/test_task_image_builder_prerequisite_converge.py
git commit -m "ops(builder): converge dynamic Slurm prerequisites"
```

### Task 5: Verify the complete Phase 1 boundary and publish the PR

**Files:**

- Modify only if verification finds a defect in the files above.

**Interfaces:**

- Produces no live activation and no positive production certificate.
- Preserves every file under `docs/superpowers/**` as untracked/absent.

- [ ] **Step 1: Run focused tests**

```bash
uv run --no-sync pytest -q \
  tests/ops/test_task_image_builder_prerequisite_profile.py \
  tests/ops/test_task_image_builder_prerequisite_conformance.py \
  tests/ops/test_task_image_builder_node_prerequisites_install.py \
  tests/ops/test_task_image_builder_prerequisite_converge.py \
  tests/ops/test_task_image_builder_deployment_contract.py \
  tests/ops/test_task_image_builder_slurm_converge.py
```

- [ ] **Step 2: Run static and repository checks**

```bash
uv run --no-sync ruff check \
  scripts/ops/task_image_builder_prerequisite_conformance.py \
  tests/ops/test_task_image_builder_prerequisite_*.py \
  tests/ops/test_task_image_builder_node_prerequisites_install.py
bash -n deploy/slurm/install-loom-task-image-builder-node-prerequisites.sh
bash -n deploy/slurm/converge-loom-task-image-builder-prerequisites.sh
uv run --no-sync python scripts/check_repository_paths.py
git diff --check
```

- [ ] **Step 3: Run the complete root test lane**

```bash
uv run --no-sync pytest -q tests/ops
```

- [ ] **Step 4: Self-review every touched file and the diff**

Check mutations against the merged design, verify the permanent gate is closed,
search for reservation/exclusive/Docker-socket regressions and secret material,
and confirm no file under `docs/superpowers/**` is staged.

- [ ] **Step 5: Commit any review corrections and verify again**

Do not amend already reviewed commits; add a focused correction commit.

- [ ] **Step 6: Push, open the PR, and wait for protected CI**

Push `feat/task-image-builder-phase1-prerequisites`, open a PR to `dev` with the
negative live inventory in the description, and do not merge until every
required check succeeds.

- [ ] **Step 7: Squash merge and verify the remote branch**

Use GitHub's squash merge, fetch `origin/dev`, verify the PR merge commit is the
new head/ancestor, and leave live provisioning and certification for a separate
post-merge, explicitly authorized operational action.
