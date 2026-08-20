# Task-image Builder Phase 1 Site Convergence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the root-owned, offline, receipt-backed controller and node
convergence boundary required to prepare OLDLAB and GB10 for allocation-scoped
rootless task-image builders without activating a builder or certifying a node.

**Architecture:** A signed offline host-release bundle and immutable site
policy drive separate controller-identity and compute-node convergers. The
controller-side maintenance orchestrator owns one versioned drain at a time,
waits for natural idleness, applies a fully preflighted node change, activates
node-local Slurm cgroup settings, runs credential-free admission/containment
smokes, and resumes only after positive readback; exact receipts make mutable
configuration rollback deterministic. A separate collector converts observed
controller, node, receipt, and smoke facts into the existing canonical Phase 1
evidence envelope, which remains production-ineligible.

**Tech Stack:** Python 3.11 standard library, Bash, TOML, JSON Schema draft
2020-12, Ubuntu 24.04 signed APT metadata, `gpgv`, `dpkg-deb`, Slurm 23.11.4,
ext4 project quotas, pytest, Ruff, strict mypy.

**Spec:**
`archive/docs/architecture/2026-08-19-task-image-builder-phase1-isolation-correction.md`

## Global Constraints

- Work only in a linked worktree and integrate only through a PR to `dev`, the
  four protected CI gates, and squash merge.
- Never create, modify, or push `docs/superpowers/**`.
- Never enable a builder policy or supervisor, advertise a builder feature,
  install the Phase 2 node guard/provider, certify a node, or rerun task
  `4139e767`.
- `production_certification_allowed=false`, `certified_nodes=[]`, and blocker
  `phase2_guard_provider_release_missing` remain immutable Phase 1 outputs.
- Preserve every legacy QoS, association, reservation, fixed node, backend,
  and supervisor exactly; PR B never invokes a legacy `modify` or `delete`.
- The controller and compute identity is exactly user `loom-builder` UID 993,
  group `loom-task-builder` GID 980, `/nonexistent`, `/usr/sbin/nologin`, with
  no supplementary groups. Only compute nodes receive subordinate ID ranges
  `3000000:65536` and the rootless runtime.
- Node apply is root-only. Docker-group membership, a Docker socket, a
  privileged container, or `nsenter` is never an authority substitute.
- Host apply is offline-only. Every package, repository index, keyring, and
  runtime artifact is staged before maintenance and verified before the first
  host mutation. Verification copies descriptor-pinned regular inputs to an
  owner-private snapshot; apply uses only that snapshot and always cleans it.
- Storage must already be a dedicated ext4 mount at
  `/var/lib/loom-task-builder`, mounted with `prjquota`, distinct from `/`, and
  backed by a non-network block device. Automation never partitions a disk,
  shrinks or remounts `/`, creates a loop device, or uses NFS/Longhorn storage.
- Builder jobs use project ID `300993`, a hard limit of 107374182400 bytes,
  and 1000000 inodes. Cache remains disabled and every smoke cleans its job
  directory. `/var/lib/loom-task-builder/jobs` is UID 993/GID 980 mode `0700`
  and must be observed empty before preparation can succeed.
- OLDLAB's observed compute-node cgroup pre-state is shared symlink
  `/etc/slurm/cgroup.conf -> /shared_work/cgroup.conf`, SHA-256
  `a4a31fa25902b407f1c2d865d5667128725aad5bbaa47c1e2b701c226fff8a2f`.
  Convergence leaves the shared target byte-identical and replaces only the
  drained node's symlink with a root-owned local file.
- GB10's observed node-local cgroup pre-state SHA-256 is
  `333f28cf5d91fd40515551b239ce4e421b92244d047e5c25b260bca1af2ac10b`.
- Desired cgroup settings are exactly `CgroupPlugin=autodetect`,
  `ConstrainCores=yes`, `ConstrainRAMSpace=yes`,
  `ConstrainSwapSpace=yes`, and `ConstrainDevices=yes` with no duplicate or
  unknown constraint rows.
- Phase 1 smoke evidence owns exact `cpuset.cpus.effective` cardinality,
  `memory.max`, `memory.swap.max`, and device-BPF attachment. Exact `cpu.max`
  bandwidth and `pids.max` enforcement are Phase 2 node-guard work.
- `authority-components-v1.json` is the single candidate component authority
  for Slurm, host, maintenance, collection, and conformance receipts.
- Fleet apply handles one node per invocation, never cancels a job, never
  steals another operator's drain, and never resumes after an incomplete or
  unverified rollback.
- Current operational blockers are expected and must remain explicit: neither
  fleet has a dedicated quota mount, and the available GB10 principal has no
  noninteractive administrative authority.

---

### Task 1: Pin the site release, observed pre-states, and package provenance

**Files:**

- Create: `deploy/task-image-builder/host-release-v1.json`
- Modify: `deploy/task-image-builder/prerequisites-v1.toml`
- Modify: `tests/ops/test_task_image_builder_prerequisite_profile.py`

**Interfaces:**

- Consumes the existing rootless runtime manifest and cluster inventory.
- Produces release schema `loom.task-image-builder-host-release/v1` and exact
  `host_release`, `storage`, and per-cluster `cgroup_transition` policy rows.

- [ ] **Step 1: Write the failing profile contract tests**

Add hand-derived assertions for:

```python
assert release["schema"] == "loom.task-image-builder-host-release/v1"
assert release["ubuntu"]["suite"] == "noble-updates"
assert release["ubuntu"]["signer_fingerprint"] == (
    "F6ECB3762474EDA9D21B7022871920D1991BC93C"
)
assert release["ubuntu"]["keyring_sha256"] == (
    "80a36b0a6de2f69f49d2df75ef473ccde121e9e190b9ea01d20a4f63778d5c31"
)
assert release["packages"]["amd64"]["uidmap"]["sha256"] == (
    "a80cb7f72dd18c73cbb0b07b7fbe855504f26bfafae072a9b3d125c89d499b9e"
)
assert release["packages"]["arm64"]["uidmap"]["sha256"] == (
    "052b1852a9ab03d931398a9d0060ef7c312f1b48bc4f4ee4533649bb958b634a"
)
assert release["packages"]["amd64"]["libsubid4"]["sha256"] == (
    "ba97fd28c53560a8d2a2261e8f75a7ab4112535b12f9fe1d50970c30051da0da"
)
assert release["packages"]["arm64"]["libsubid4"]["sha256"] == (
    "00916edc15862421e803bec7e69d548c6ce281badf5d449498085a3b3710639f"
)
assert release["packages"]["amd64"]["quota"]["sha256"] == (
    "55cc08283cd16b26ce305c01252d92989ee561ea47d2d781958ea6a27d5a7e25"
)
assert release["packages"]["arm64"]["quota"]["sha256"] == (
    "2ff4f684f177690caac079d636fa3effdce44e3aa4f6f81f1e24e9ec3e9263b8"
)
assert policy["storage"]["mountpoint"] == "/var/lib/loom-task-builder"
assert policy["storage"]["project_id"] == 300993
assert policy["storage"]["automatic_block_device_changes"] is False
```

Also assert exact package version `1:4.13+dfsg1-4ubuntu3.2`, exact quota
version `4.06-1build6`, repository-index paths for both architectures, the two
observed cgroup hashes/modes, and that no site row contains an activation,
feature, reservation, Docker socket, root filesystem, loop device, or NFS
storage target.

- [ ] **Step 2: Run the profile test and observe the missing release**

Run:

```bash
uv run --no-sync pytest -q \
  tests/ops/test_task_image_builder_prerequisite_profile.py
```

Expected: FAIL because `host-release-v1.json` and the new policy rows are
absent.

- [ ] **Step 3: Add the immutable release and site rows**

The release must contain exact `filename`, `size`, `sha256`, `package`,
`version`, and `architecture` fields for `uidmap`, `libsubid4`, and `quota`.
The policy must select ext4 only for this site release, require a distinct
mount at `/var/lib/loom-task-builder`, and add these fields to the existing
`[storage]` table:

```toml
mountpoint = "/var/lib/loom-task-builder"
project_id = 300993
site_filesystem = "ext4"
required_mount_options = ["prjquota"]
automatic_block_device_changes = false
reject_root_filesystem = true
reject_network_filesystem = true
```

Add these literal transition fields to the OLDLAB `[[clusters]]` row:

```toml
cgroup_transition = "shared_symlink_to_node_local"
cgroup_observed_path = "/shared_work/cgroup.conf"
cgroup_observed_sha256 = "a4a31fa25902b407f1c2d865d5667128725aad5bbaa47c1e2b701c226fff8a2f"
```

Add these literal fields to the GB10 row:

```toml
cgroup_transition = "node_local"
cgroup_observed_path = "/etc/slurm/cgroup.conf"
cgroup_observed_sha256 = "333f28cf5d91fd40515551b239ce4e421b92244d047e5c25b260bca1af2ac10b"
```

- [ ] **Step 4: Run the profile test**

Expected: PASS.

- [ ] **Step 5: Commit the release contract**

```bash
git add deploy/task-image-builder/host-release-v1.json \
  deploy/task-image-builder/prerequisites-v1.toml \
  tests/ops/test_task_image_builder_prerequisite_profile.py
git commit -m "ops(builder): pin Phase 1 host release"
```

### Task 2: Verify the complete signed offline host bundle

**Files:**

- Create: `scripts/ops/task_image_builder_host_release.py`
- Create: `tests/ops/test_task_image_builder_host_release.py`

**Interfaces:**

- Defines these shared boundaries:

```python
@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str

class CommandRunner(Protocol):
    def run(self, args: Sequence[str], *, input_bytes: bytes | None = None) -> CommandResult: ...

@dataclass(frozen=True)
class HostRelease:
    release: str
    signer_fingerprint: str
    keyring_sha256: str
    packages: Mapping[str, Mapping[str, PackageArtifact]]

@dataclass(frozen=True)
class PackageArtifact:
    package: str
    version: str
    architecture: str
    filename: str
    size: int
    sha256: str

@dataclass(frozen=True)
class VerifiedHostBundle:
    architecture: str
    bundle_digest: str
    package_paths: tuple[Path, ...]
    runtime_paths: tuple[Path, ...]
```

- Produces `load_host_release(path: Path) -> HostRelease`.
- Produces `verify_host_bundle(bundle: Path, release: HostRelease,
  architecture: str, runner: CommandRunner) -> VerifiedHostBundle`; returned
  paths point only into its owner-private verified snapshot, whose lifecycle
  is closed explicitly after check/apply.
- CLI: `verify --release PATH --runtime-manifest PATH --bundle PATH
  --architecture {x86_64,aarch64}`; it performs no writes.

- [ ] **Step 1: Write failing bundle-verifier tests**

Use literal temporary bundle fixtures and a deterministic command runner.
For x86_64, prove acceptance only when:

```text
ubuntu-archive-keyring.gpg
apt/InRelease
apt/Packages.xz
packages/uidmap_4.13+dfsg1-4ubuntu3.2_amd64.deb
packages/libsubid4_4.13+dfsg1-4ubuntu3.2_amd64.deb
packages/quota_4.06-1build6_amd64.deb
runtime/buildkit-v0.32.2.linux-amd64.tar.gz
runtime/rootlesskit-x86_64.tar.gz
runtime/slirp4netns-x86_64
runtime/fuse-overlayfs-x86_64
```

The aarch64 fixture substitutes the four literal arm64/aarch64 filenames from
`rootless-runtime-v1.json` and the three exact arm64 `.deb` filenames.

Inputs are regular files with no symlinks, extras, or group/world write bits;
the keyring digest is exact; `gpgv --status-fd` reports exactly the pinned full signer fingerprint;
the InRelease SHA-256 stanza authenticates `Packages.xz`; each Packages stanza
matches literal package/version/architecture/filename/size/digest values; each
`.deb` digest and `dpkg-deb --field` value matches; and every runtime archive
and extracted binary matches `rootless-runtime-v1.json`.

Parameterize wrong signer, expired signature, unsigned index, duplicate
package stanza, path traversal, architecture mismatch, digest mismatch,
unexpected bundle file, group/world-writable input, symlink, setuid payload
outside `/usr/bin/newuidmap` and `/usr/bin/newgidmap`, a dynamic `NEEDED`
entry in an installed runtime binary, and a runtime dependency on
Docker/containerd.

- [ ] **Step 2: Run the verifier tests and observe the missing module**

Run:

```bash
uv run --no-sync pytest -q tests/ops/test_task_image_builder_host_release.py
```

Expected: collection FAIL because the module does not exist.

- [ ] **Step 3: Implement bounded offline verification**

Use the declared dataclasses and protocol. Reject inputs larger than
64 MiB per metadata file or 1 GiB per artifact, open with `O_NOFOLLOW`, parse
RFC822 package stanzas without invoking APT, and invoke only fixed absolute
commands (`/usr/bin/gpgv`, `/usr/bin/dpkg-deb`, `/usr/bin/readelf`,
`/usr/bin/sha256sum`). Accept `x86_64 -> amd64` and `aarch64 -> arm64` as the
only architecture mapping. Require the installed runtime allowlist binaries to
be static ELF files with no `NEEDED` entries. Stream archive members for digest
verification or extract only into a private temporary directory; never write
into the caller's bundle or a host installation path.

- [ ] **Step 4: Run the bundle-verifier and profile tests**

Expected: PASS.

- [ ] **Step 5: Commit the offline supply verifier**

```bash
git add scripts/ops/task_image_builder_host_release.py \
  tests/ops/test_task_image_builder_host_release.py
git commit -m "feat(builder): verify signed offline host bundles"
```

### Task 3: Install the controller submission identity only

**Files:**

- Create: `deploy/slurm/install-loom-task-image-builder-controller-identity.sh`
- Create: `tests/ops/test_task_image_builder_controller_identity_install.py`

**Interfaces:**

- CLI: `check <cluster-id>` and `apply <cluster-id>`.
- Produces only group GID 980 and user UID 993 on the exact policy controller.
- Never installs subordinate IDs, runtime files, packages, services,
  credentials, Slurm authority, or supplementary groups.

- [ ] **Step 1: Write failing behavioral installer tests**

Run the real script with isolated passwd/group fixtures and fake identity
commands. Prove check mode writes nothing; wrong controller/architecture/root
fails; numeric/name collisions fail before `groupadd`/`useradd`; first apply
creates exact identity; second apply is idempotent; any supplementary group is
fatal; and output remains:

```json
{"certified_nodes":[],"production_certification_allowed":false,"state":"controller_identity_prepared"}
```

- [ ] **Step 2: Run the test and observe the missing installer**

Expected: FAIL because the installer is absent.

- [ ] **Step 3: Implement preflight-first controller convergence**

Load identity/controller/architecture from the canonical TOML with Python
3.11, validate the local Slurm controller exactly as PR A does, and complete
all conflict checks before the first mutation. Use `groupadd --system --gid
980 loom-task-builder` and `useradd --system --uid 993 --gid
loom-task-builder --home-dir /nonexistent --shell /usr/sbin/nologin
--no-create-home loom-builder`. Re-read passwd, group, and `id -G`; accept only
primary GID 980 and no supplementary GID.

- [ ] **Step 4: Run controller-installer tests and Bash syntax**

Expected: PASS.

- [ ] **Step 5: Commit the controller identity boundary**

```bash
git add deploy/slurm/install-loom-task-image-builder-controller-identity.sh \
  tests/ops/test_task_image_builder_controller_identity_install.py
git commit -m "feat(builder): converge controller submission identity"
```

### Task 4: Journal additive Slurm convergence across partial failures

**Files:**

- Create: `scripts/ops/task_image_builder_slurm_converge.py`
- Create: `tests/ops/test_task_image_builder_slurm_convergence_receipt.py`

**Interfaces:**

- CLI: `plan|check|apply --cluster-id ID --receipt-dir PATH`.
- Produces schema `loom.task-image-builder-slurm-receipt/v1` containing exact
  candidate/policy/controller/cluster digests, pre/post legacy fingerprints,
  pre/post rootless object states, created-object names, durable-config backup
  digest, command outcome, and a hash-chained event list.
- Delegates the actual absent-or-exact mutations to
  `converge-loom-task-image-builder-prerequisites.sh`; it adds no Slurm grammar.

- [ ] **Step 1: Write failing fake-controller receipt tests**

Use the existing stateful fake Slurm commands. Prove `plan` and `check` are
read-only; `apply` records which absent objects were created; an idempotent
apply records an empty created list; QoS, association, config-reconfigure, and
post-readback failures still produce an fsynced failure receipt with exact
pre/post state; legacy drift prevents delegation; and no receipt writer emits
`modify` or `delete` for any Slurm object.

- [ ] **Step 2: Run the test and observe the missing wrapper**

Run:

```bash
uv run --no-sync pytest -q \
  tests/ops/test_task_image_builder_slurm_convergence_receipt.py
```

Expected: collection FAIL because the wrapper does not exist.

- [ ] **Step 3: Implement fail-safe Slurm receipt collection**

Read semantic state with `task_image_builder_slurm_readback.py`, create the
owner-only receipt before delegation, append intent before running the Bash
converger, and append post-state in `finally` even when delegation fails. Use
the same canonical JSON, exclusive creation, SHA-256 chain, file fsync, and
directory fsync rules as host receipts. Refuse apply unless root, the exact
controller/cluster match, and the receipt root is root-owned mode 0700.

- [ ] **Step 4: Run wrapper and existing Slurm convergence tests**

Expected: PASS.

- [ ] **Step 5: Commit Slurm convergence receipts**

```bash
git add scripts/ops/task_image_builder_slurm_converge.py \
  tests/ops/test_task_image_builder_slurm_convergence_receipt.py
git commit -m "feat(builder): journal additive Slurm convergence"
```

### Task 5: Converge one compute host with a durable rollback receipt

**Files:**

- Create: `scripts/ops/task_image_builder_host_converge.py`
- Modify: `deploy/slurm/install-loom-task-image-builder-node-prerequisites.sh`
- Create: `tests/ops/test_task_image_builder_host_converge.py`
- Modify: `tests/ops/test_task_image_builder_node_prerequisites_install.py`

**Interfaces:**

- CLI: `plan|check|apply|rollback --cluster-id ID --slurm-node NAME
  --bundle PATH --receipt-dir PATH`.
- Produces receipt schema `loom.task-image-builder-host-receipt/v1` with
  operation ID, candidate/policy/release digests, node binding, original
  cgroup kind/path/digest/metadata, package pre-state, storage/quota pre-state,
  created inert artifacts, activation requirement, and hash-chained events.
- `apply` does not restart/reload `slurmd`; it reports
  `activation_required=true` for the controller orchestrator.

- [ ] **Step 1: Write failing preflight and receipt tests**

Use a fake filesystem plus stateful fake system commands. Prove `plan` and
`check` perform zero writes, and every preflight failure occurs before package,
identity, cgroup, storage, or runtime mutation. Cover exact bundle verification,
policy host binding, UID/GID/sub-ID conflicts, forbidden supplementary groups,
forbidden sockets as identity-accessible paths, missing controllers/bpffs,
wrong cgroup pre-state, non-root/NFS/loop/root/shared storage, missing
`prjquota`, insufficient free bytes/inodes, wrong existing project quota, and
receipt collisions.

- [ ] **Step 2: Write failing OLDLAB/GB10 transition tests**

For OLDLAB, start with the exact shared symlink and bytes. Assert apply leaves
`/shared_work/cgroup.conf` unchanged, atomically replaces only the local
symlink with mode `0644` root:root desired bytes, and records enough metadata
to restore the exact symlink. For GB10, start with the exact local SHA and
assert an owner-only byte backup plus atomic desired replacement. Existing
desired state is idempotent only when its receipt and release digest agree;
unknown desired-looking files are rejected.

- [ ] **Step 3: Write failing package, quota, runtime, and rollback tests**

Assert exact offline `.deb` installation order `libsubid4`, `uidmap`, `quota`;
re-read package versions; verify `newuidmap/newgidmap` are root:root mode 4755,
not group/world writable, and have no file capabilities; call the existing
runtime installer only after every preflight passes; create the project
directory with project ID 300993; apply exact 100 GiB/1000000-inode hard
limits; and verify readback. On injected failures, restore cgroup and quota
state, leave inert identity/packages/runtime recorded, and emit
`rollback_verified=true`. A failed restoration must emit
`rollback_verified=false` and never claim the node prepared.

- [ ] **Step 4: Run the tests and observe missing host convergence**

Run:

```bash
uv run --no-sync pytest -q \
  tests/ops/test_task_image_builder_host_converge.py \
  tests/ops/test_task_image_builder_node_prerequisites_install.py
```

Expected: FAIL because the host converger does not exist and the existing node
installer still checks some prerequisites after mutation.

- [ ] **Step 5: Make the existing node installer fully preflight-first**

Move every package/helper, cgroup, bpffs, storage, socket, identity, sub-ID,
and runtime-artifact check ahead of `groupadd`, `useradd`, release directory,
or symlink writes. Keep its public `check/apply` interface and isolated tests
green; do not add package, cgroup, quota, service, or drain authority to it.

- [ ] **Step 6: Implement the host converger and receipt journal**

Use `os.open(..., O_CREAT|O_EXCL|O_NOFOLLOW, 0o600)`, canonical JSON, SHA-256
event chaining, `fsync` on file and parent directory, and atomic
same-filesystem replacements. Refuse receipt paths outside a root-owned mode
0700 receipt root. Execute no shell strings: every external command is an
argument vector selected by code. Rollback accepts only a receipt whose
candidate, policy, release, cluster, node, and current-state digests match.

- [ ] **Step 7: Run host, installer, release, and conformance tests**

Expected: PASS.

- [ ] **Step 8: Commit single-host convergence**

```bash
git add scripts/ops/task_image_builder_host_converge.py \
  deploy/slurm/install-loom-task-image-builder-node-prerequisites.sh \
  tests/ops/test_task_image_builder_host_converge.py \
  tests/ops/test_task_image_builder_node_prerequisites_install.py
git commit -m "feat(builder): converge one rootless builder host"
```

### Task 6: Orchestrate drain ownership, activation, smoke, and safe resume

**Files:**

- Create: `scripts/ops/task_image_builder_node_maintenance.py`
- Create: `tests/ops/test_task_image_builder_node_maintenance.py`

**Interfaces:**

- CLI: `plan|check|apply --cluster-id ID --slurm-node NAME --candidate-root
  PATH --bundle PATH --receipt-root PATH [--ssh-config PATH]`.
- Produces maintenance receipt schema
  `loom.task-image-builder-node-maintenance/v1` and an exact terminal state:
  `prepared`, `blocked`, `rolled_back`, or `drained_rollback_failed`.

- [ ] **Step 1: Write failing fake-controller lifecycle tests**

Exercise the real state machine against stateful fake `scontrol`, `squeue`,
`ssh`, `runuser`, and `sbatch` commands. Prove it records initial state/reason;
rejects foreign drains; applies reason
`loom-task-builder-phase1/host-release-v1/<operation-id>`; never emits
`scancel`; waits until no running/completing job and zero allocated TRES; runs
remote immutable preflight before remote apply; activates only the named
node's `slurmd`; and verifies daemon/config readback before smoke.

- [ ] **Step 2: Write failing admission and containment smoke tests**

Require administrator-run `sbatch --test-only` as `loom-builder` with exact
account, cluster QoS, partition, CPU, memory, and wall time. Require the
equivalent `loom-rollout` request to fail. Queue a pinned maintenance-only
smoke while the Loom drain is still held, verify it is pending on that drain,
resume only the owned drain, and require that smoke to run first. The smoke
must create no image, use no credential, execute no BuildKit, and assert its
cgroup is below the Slurm job cgroup with exact effective-cpuset cardinality,
memory, swap, and device-BPF controls. CPU bandwidth and PID limits are not
Phase 1 claims; the Phase 2 guard must write and read back `cpu.max` and
`pids.max`. The smoke exits after cleaning its job directory. Then require
ordinary-trial `sbatch --test-only` acceptance.

- [ ] **Step 3: Write failing failure/rollback tests**

Inject timeout, disconnect, daemon failure, smoke failure, receipt write
failure, rollback success, rollback failure, and ownership loss. Assert no
resume after any unverified state; successful rollback restores config,
restarts/rechecks `slurmd`, and leaves the node drained with the Loom reason
for operator inspection; rollback failure changes the drain reason to include
the receipt digest and terminal state `drained_rollback_failed`.

- [ ] **Step 4: Run the tests and observe the missing orchestrator**

Expected: collection FAIL because the module is absent.

- [ ] **Step 5: Implement the explicit maintenance state machine**

Represent each transition as an enum and append-only receipt event. Poll by
condition with monotonic deadlines; do not sleep in tests. Transport must use
the checked-in GB10 SSH config when selected and strict host-key checking for
both clusters. Remote mutation runs one candidate-owned absolute command
through `sudo`; no arbitrary remote shell payload is accepted.

- [ ] **Step 6: Run maintenance and single-host tests**

Expected: PASS.

- [ ] **Step 7: Commit the one-node maintenance protocol**

```bash
git add scripts/ops/task_image_builder_node_maintenance.py \
  tests/ops/test_task_image_builder_node_maintenance.py
git commit -m "feat(builder): orchestrate fail-closed node maintenance"
```

### Task 7: Collect canonical two-cluster prerequisite evidence

**Files:**

- Create: `scripts/ops/task_image_builder_prerequisite_evidence.py`
- Create: `tests/ops/test_task_image_builder_prerequisite_evidence.py`
- Modify: `scripts/ops/task_image_builder_prerequisite_conformance.py`
- Modify: `tests/ops/test_task_image_builder_prerequisite_conformance.py`
- Modify: `docs/evidence/task-image-builder-prerequisite-conformance-v1.schema.json`
- Modify: `docs/evidence/README.md`

**Interfaces:**

- CLI: `collect-controller`, `collect-node`, and `assemble` with explicit
  candidate/policy/release/receipt inputs and caller-selected output.
- The existing `verify` and `canonicalize` commands consume the assembled
  envelope; collection and verification remain separate authorities.

- [ ] **Step 1: Write failing collector and schema tests**

Build literal OLDLAB and GB10 fixtures. Require exact controller identity,
node set, physical/Slurm alias binding, package/source signature evidence,
runtime/dependency digests, cgroup readback, dedicated mount/quota readback,
Slurm receipt, maintenance receipt chain, smoke Slurm job/cgroup facts,
cleanup, and unchanged legacy fingerprints. Reject missing/duplicate nodes,
mixed release/policy
digests, stale observations, a prepared claim without terminal receipt,
foreign drain ownership, surviving process/mount/job directory, and any
secret-like field/value.

Bind every fragment and producer receipt to the exact authority manifest and
component digest map. Kernel evidence carries raw sysctl, pidfd, sealed-memfd,
clone3-EBADF, findmnt, controller, and delegation observations. Storage
evidence carries raw findmnt/lsblk, jobs-root metadata and entries, fresh
lsattr/repquota, and the maintenance cleanup command.

- [ ] **Step 2: Run the tests and observe missing evidence interfaces**

Expected: FAIL for the missing collector and schema fields.

- [ ] **Step 3: Implement deterministic collection and stricter verification**

Collectors read only bounded local commands/files/syscall probes/receipts and
write only the selected owner-readable output. They derive claims from raw
observations rather than policy booleans or stale receipt values. `assemble`
sorts clusters/nodes, requires the exact
policy inventories, and performs no SSH. Extend semantic verification from
reported booleans to the new raw package, cgroup, quota, maintenance, and smoke
facts. Even a completely prepared two-cluster envelope returns:

```json
{
  "production_certification_allowed": false,
  "certified_nodes": [],
  "blockers": ["phase2_guard_provider_release_missing"]
}
```

- [ ] **Step 4: Run evidence, conformance, and schema tests**

Expected: PASS.

- [ ] **Step 5: Commit canonical evidence collection**

```bash
git add scripts/ops/task_image_builder_prerequisite_evidence.py \
  scripts/ops/task_image_builder_prerequisite_conformance.py \
  tests/ops/test_task_image_builder_prerequisite_evidence.py \
  tests/ops/test_task_image_builder_prerequisite_conformance.py \
  docs/evidence/task-image-builder-prerequisite-conformance-v1.schema.json \
  docs/evidence/README.md
git commit -m "feat(builder): collect Phase 1 host evidence"
```

### Task 8: Publish the inert operator runbook and verify PR B

**Files:**

- Create: `docs/runbooks/task-image-builder-phase1-site-convergence.md`
- Modify: `docs/runbooks/README.md`
- Modify:
  `archive/docs/architecture/2026-08-19-task-image-builder-phase1-isolation-correction.md`

**Interfaces:**

- Documents separate artifact staging, controller preparation, Slurm additive
  convergence, one-node maintenance, rollback, evidence assembly, and the
  closed Phase 2 boundary.

- [x] **Step 1: Write the runbook with exact safe sequencing**

Document check/plan commands first; the required dedicated ext4 `prjquota`
mount as an externally provisioned prerequisite; current OLDLAB and GB10
blockers; OLDLAB-first order; one node per apply; receipt inspection; explicit
operator action after rollback; and the prohibition on task rerun/activation.
Do not include a command that partitions/remounts root, disables host-key
checking, uses Docker as authority, cancels jobs, or enables a service.

- [x] **Step 2: Run focused verification**

```bash
uv run --no-sync pytest -q \
  tests/ops/test_task_image_builder_prerequisite_profile.py \
  tests/ops/test_task_image_builder_host_release.py \
  tests/ops/test_task_image_builder_controller_identity_install.py \
  tests/ops/test_task_image_builder_slurm_convergence_receipt.py \
  tests/ops/test_task_image_builder_host_converge.py \
  tests/ops/test_task_image_builder_node_prerequisites_install.py \
  tests/ops/test_task_image_builder_node_maintenance.py \
  tests/ops/test_task_image_builder_prerequisite_evidence.py \
  tests/ops/test_task_image_builder_prerequisite_conformance.py \
  tests/ops/test_task_image_builder_prerequisite_converge.py \
  tests/ops/test_task_image_builder_slurm_readback.py \
  tests/ops/test_repository_path_policy.py
```

Expected: PASS.

- [x] **Step 3: Run static and repository checks**

```bash
bash -n deploy/slurm/install-loom-task-image-builder-controller-identity.sh
bash -n deploy/slurm/install-loom-task-image-builder-node-prerequisites.sh
uv run --no-sync ruff check scripts/ops/task_image_builder_*.py tests/ops/test_task_image_builder_*.py
uv run --no-sync mypy --explicit-package-bases scripts/ops/task_image_builder_host_release.py \
  scripts/ops/task_image_builder_host_converge.py \
  scripts/ops/task_image_builder_node_maintenance.py \
  scripts/ops/task_image_builder_prerequisite_evidence.py
git diff --check origin/dev...HEAD
test -z "$(git diff --name-only origin/dev...HEAD -- 'docs/superpowers/**')"
```

Expected: every command exits zero and the forbidden path scan is empty.

- [ ] **Step 4: Run read-only live checks only (controller-owned; skipped)**

On OLDLAB, run controller/node `check` and `plan` modes and confirm they report
the dedicated-mount blocker without writes. On GB10, run non-root inventory and
confirm all 15 aliases remain reachable while `apply` is impossible without
command-scoped administrative authority. Do not drain, install, modify, apply,
restart, submit a smoke, activate, or rerun.

- [x] **Step 5: Commit documentation and verification evidence**

```bash
git add docs/runbooks/task-image-builder-phase1-site-convergence.md \
  docs/runbooks/README.md \
  archive/docs/architecture/2026-08-19-task-image-builder-phase1-isolation-correction.md \
  archive/docs/implementation-plans/2026-08-19-task-image-builder-phase1-site-convergence.md
git commit -m "docs(builder): publish Phase 1 site convergence runbook"
```

- [ ] **Step 6: Push and open PR B (controller-owned; not performed)**

Push `ops/task-image-builder-phase1-site-convergence`, open a non-draft PR to
`dev`, enable squash auto-merge, and require `repository-checks`,
`images-gate`, `cluster-smoke-gate`, and `staging-smoke-gate` on the exact head.
The PR description must state that no live apply, drain, activation,
certification, or task rerun occurred.
