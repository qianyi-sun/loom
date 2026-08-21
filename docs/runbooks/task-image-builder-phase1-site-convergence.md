# Task-image builder Phase 1 site convergence

This runbook is the inert, operator-facing boundary for Phase 1 site
convergence.  It prepares prerequisites only; it does not enable a builder,
advertise a builder feature, install a Phase 2 provider or guard, activate a
policy or supervisor, certify a node, submit a builder job, or rerun task
`4139e767`.

The authoritative design is the [Phase 1 isolation correction](../../archive/docs/architecture/2026-08-19-task-image-builder-phase1-isolation-correction.md).

## Current state and stop conditions

Do not run an `apply` action at the time this runbook was published. The four
live blockers discovered on 2026-08-21 have not been rechecked by this
procedure:

- The dedicated ext4 storage mount at `/var/lib/loom-task-builder` with
  `prjquota` is absent on reachable GB10 nodes.
- The available GB10 principal lacks command-scoped, noninteractive
  administrative authority.
- `trt-gb10-7` is unreachable.
- OLDLAB access is unavailable.

The mount is an externally provisioned prerequisite.  It must be a dedicated,
non-network ext4 mount on a non-root filesystem, mounted with `prjquota`.  Its
binding builder-storage contract is project ID `300993`, hard limits of
`107374182400` bytes (100 GiB) and `1000000` inodes, disabled cache, and
mandatory cleanup of every smoke job directory.  This workflow never
partitions a disk, changes or remounts `/`, creates loop storage, or
substitutes a Docker group or socket for administrative authority.  If
inventory or check/plan output does not prove this prerequisite, retain the
blocker and stop.

The preflight examples below are check/plan-first and safe to use only with the
operator's normal authenticated controller/node access.  The separately marked
future authorized applies are not permitted while the current blockers remain.
This runbook deliberately contains no remote-shell, host-key bypass,
cancellation, service-enable, or activation command.

Phase 1 outputs remain immutable at every point:

```json
{
  "production_certification_allowed": false,
  "certified_nodes": [],
  "blockers": ["phase2_guard_provider_release_missing"]
}
```

## Inputs, staging, and read-only preflight

Use one reviewed candidate checkout, one signed offline bundle per
architecture, and owner-controlled receipt/evidence locations. The candidate,
receipt, evidence, and bundle-parent paths below must already exist; each
bundle output path itself must be absent before assembly. Do not create a
storage mount as part of this workflow.

```bash
CANDIDATE_ROOT=/srv/loom/candidates/PHASE1_CANDIDATE
OLDLAB_BUNDLE=/srv/loom/offline/task-image-builder/oldlab
GB10_BUNDLE=/srv/loom/offline/task-image-builder/gb10
RECEIPT_ROOT=/srv/loom/receipts/task-image-builder-phase1
EVIDENCE_ROOT=/srv/loom/evidence/task-image-builder-phase1
```

Before a maintenance window, assemble the complete signed bundle into an
*absent* output path. Assembly is networked because it fetches the pinned
Ubuntu Snapshot Service closure, but it is inert: it does not install,
activate, or contact a cluster. It verifies the newly assembled bundle before
publishing it. Later bundle verification is offline. Assemble both
architectures before any host change:

```bash
python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_host_bundle.py" assemble \
  --release "$CANDIDATE_ROOT/deploy/task-image-builder/host-release-v2.json" \
  --runtime-manifest "$CANDIDATE_ROOT/deploy/task-image-builder/rootless-runtime-v1.json" \
  --keyring /usr/share/keyrings/ubuntu-archive-keyring.gpg \
  --architecture x86_64 --output "$OLDLAB_BUNDLE"

python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_host_bundle.py" assemble \
  --release "$CANDIDATE_ROOT/deploy/task-image-builder/host-release-v2.json" \
  --runtime-manifest "$CANDIDATE_ROOT/deploy/task-image-builder/rootless-runtime-v1.json" \
  --keyring /usr/share/keyrings/ubuntu-archive-keyring.gpg \
  --architecture aarch64 --output "$GB10_BUNDLE"
```

Stage the verified bundles locally on the target host/controller according to
local transfer policy. They include the pinned Ubuntu metadata, packages,
keyring, and runtime artifacts; staging is not installation or activation.
Verify each bundle offline before any host change:

```bash
python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_host_release.py" verify \
  --release "$CANDIDATE_ROOT/deploy/task-image-builder/host-release-v2.json" \
  --runtime-manifest "$CANDIDATE_ROOT/deploy/task-image-builder/rootless-runtime-v1.json" \
  --bundle "$OLDLAB_BUNDLE" \
  --architecture x86_64

python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_host_release.py" verify \
  --release "$CANDIDATE_ROOT/deploy/task-image-builder/host-release-v2.json" \
  --runtime-manifest "$CANDIDATE_ROOT/deploy/task-image-builder/rootless-runtime-v1.json" \
  --bundle "$GB10_BUNDLE" \
  --architecture aarch64
```

On each controller, inspect the immutable policy plan before proceeding.  It
is an evidence-shape plan, not a convergence action:

```bash
python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_prerequisite_conformance.py" plan \
  --policy "$CANDIDATE_ROOT/deploy/task-image-builder/prerequisites-v1.toml"
```

Retain the candidate revision, policy digest, release digest, bundle-verifier
output, and planned receipt locations with the change record.  A verification
failure, a digest mismatch, unexpected writable artifact, identity conflict,
or missing storage prerequisite is a stop condition.

## Controller preparation and additive Slurm readback

Converge OLDLAB completely before considering GB10.  For a future authorized
window, first run these non-mutating controller assertions on the exact
controller for the selected cluster:

```bash
sudo "$CANDIDATE_ROOT/deploy/slurm/install-loom-task-image-builder-controller-identity.sh" \
  check oldlab

sudo python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_slurm_converge.py" \
  check --cluster-id oldlab \
  --receipt-dir "$RECEIPT_ROOT/oldlab/controller"
```

Always invoke Slurm plan/check/apply through the Python receipt producer above.
The Bash converger is its internal delegate and is not an operator entry point.

The identity contract is only `loom-builder` (UID 993) with primary group
`loom-task-builder` (GID 980), `/nonexistent`, and `/usr/sbin/nologin`; it has
no supplementary groups or Slurm administrative authority.  The Slurm check
must preserve and fingerprint the legacy QoS, association, reservation, fixed
nodes, backend, and supervisor.  Rootless objects are additive-only and use
the OLDLAB QoS `loom-task-image-builder-rootless-oldlab`; a present-but-different
rootless object is a hard failure, never a modification.

These commands are convergence assertions, not a separate success-only
preflight interface.  From a valid absent state, the identity check reports
`controller builder identity is incomplete` and the Slurm check reports
`task-image builder Slurm prerequisites are not converged`; each is an expected
nonzero assertion only after its policy/controller/legacy readback is otherwise
conflict-free.  Treat any identity conflict, legacy fingerprint mismatch,
present-but-different rootless object, unsafe/drift error, or other unexpected
error as fatal; do not apply.

When the absent-state assertions are the only failures, all approvals exist,
and the external storage prerequisite has been proven, the future authorized
sequence is identity apply, successful identity check, additive Slurm apply,
then successful Slurm check:

```bash
sudo "$CANDIDATE_ROOT/deploy/slurm/install-loom-task-image-builder-controller-identity.sh" \
  apply oldlab
sudo "$CANDIDATE_ROOT/deploy/slurm/install-loom-task-image-builder-controller-identity.sh" \
  check oldlab
sudo python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_slurm_converge.py" \
  apply --cluster-id oldlab \
  --receipt-dir "$RECEIPT_ROOT/oldlab/controller"
sudo python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_slurm_converge.py" \
  check --cluster-id oldlab \
  --receipt-dir "$RECEIPT_ROOT/oldlab/controller"
```

Inspect the apply and successful-check output immediately after each boundary.
The successful Slurm check/readback must show exact rootless objects and an
unchanged legacy fingerprint; retain its state receipt with the change record.
Do not continue to any node if an expected success assertion is nonzero or any
receipt/readback is absent or ambiguous.  No builder job is submitted at this
stage.

Repeat the same absent-state assertion, authorized apply, successful check,
and receipt/readback order for `gb10` only after OLDLAB evidence is complete,
all 15 GB10 aliases (including `trt-gb10-7`) are reachable, and
command-scoped noninteractive administrative authority has been granted.  Its
initial non-mutating assertions are:

```bash
sudo "$CANDIDATE_ROOT/deploy/slurm/install-loom-task-image-builder-controller-identity.sh" \
  check gb10

sudo python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_slurm_converge.py" \
  check --cluster-id gb10 \
  --receipt-dir "$RECEIPT_ROOT/gb10/controller"
```

GB10 uses the distinct additive QoS
`loom-task-image-builder-rootless-gb10`.  Neither cluster may use the legacy
`loom-task-image-builder` name for the rootless policy.

## One-node maintenance boundary

Only after the preceding checks and the externally provisioned mount have
passed, an authorized operator may schedule maintenance for one policy node.
Never perform two node applies concurrently, never cancel a job to make the
node idle, and never take over another operator's drain.  OLDLAB order is
`trt-eai-oldlab-3`, then `trt-eai-oldlab-4`, then `trt-eai-oldlab-5`; complete
the evidence for each node before selecting the next.

For the selected node, run host plan then host check, followed by controller
maintenance plan then maintenance check.  These examples use the first
OLDLAB node and create no mutable host or Slurm state:

```bash
python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_host_converge.py" plan \
  --cluster-id oldlab --slurm-node trt-eai-oldlab-3 \
  --bundle "$OLDLAB_BUNDLE" \
  --receipt-dir "$RECEIPT_ROOT/oldlab/trt-eai-oldlab-3/host"

python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_host_converge.py" check \
  --cluster-id oldlab --slurm-node trt-eai-oldlab-3 \
  --bundle "$OLDLAB_BUNDLE" \
  --receipt-dir "$RECEIPT_ROOT/oldlab/trt-eai-oldlab-3/host"

python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_node_maintenance.py" plan \
  --cluster-id oldlab --slurm-node trt-eai-oldlab-3 \
  --candidate-root "$CANDIDATE_ROOT" --bundle "$OLDLAB_BUNDLE" \
  --receipt-root "$RECEIPT_ROOT/oldlab/trt-eai-oldlab-3/maintenance"

python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_node_maintenance.py" check \
  --cluster-id oldlab --slurm-node trt-eai-oldlab-3 \
  --candidate-root "$CANDIDATE_ROOT" --bundle "$OLDLAB_BUNDLE" \
  --receipt-root "$RECEIPT_ROOT/oldlab/trt-eai-oldlab-3/maintenance"
```

`check` is expected to remain negative while the required dedicated mount is
missing; record that result as a blocker and do not escalate it into an apply.
When all documented prerequisites and approvals exist, the authorized
maintenance procedure invokes exactly one node's `apply` action, records its
initial Slurm state/reason, drains only with the versioned Loom reason, waits
for natural idleness, applies the receipt-backed host change, performs the
bounded contained smoke/readback, and resumes only after every readback
passes and Loom still owns the drain.  The maintenance tool, not this runbook,
owns those mutation details and produces the terminal maintenance receipt.

Inspect the host receipt and maintenance receipt after every terminal result.
Accept `prepared` only with a matching candidate/policy/release digest,
verified rollback metadata, storage/quota and cgroup readback, Slurm alias
binding, smoke cleanup facts, and a maintained legacy fingerprint.  A
`blocked`, `rolled_back`, or `drained_rollback_failed` result stops fleet
progress.

After a rollback, an operator must inspect the receipt, the node's actual
Slurm state/reason, restored configuration and mount/quota readback, and the
recorded failure before choosing a separately authorized remediation.  Keep
the node drained when restoration is incomplete or unverified.  Automation
must not resume it, retry it, or advance to another node.

For GB10, use the same one-node protocol only after its authority and complete
reachability gates pass.  Its policy inventory is exactly `trt-gb10-1` through
`trt-gb10-15`; do not omit or substitute an alias.

## Evidence assembly and verification

Evidence is collected only after a controller/node reaches the required
readback state.  Collection reads facts and receipts into caller-selected
outputs; assembly requires the complete inventory for both clusters.  The
following command shapes are inert templates, not a claim that any receipt or
evidence file exists today:

```bash
python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_prerequisite_evidence.py" \
  collect-controller --candidate-root "$CANDIDATE_ROOT" \
  --policy "$CANDIDATE_ROOT/deploy/task-image-builder/prerequisites-v1.toml" \
  --release "$CANDIDATE_ROOT/deploy/task-image-builder/host-release-v2.json" \
  --cluster-id oldlab --slurm-receipt OLDLAB_SLURM_RECEIPT \
  --output "$EVIDENCE_ROOT/oldlab-controller.json"

python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_prerequisite_evidence.py" \
  collect-node --candidate-root "$CANDIDATE_ROOT" \
  --policy "$CANDIDATE_ROOT/deploy/task-image-builder/prerequisites-v1.toml" \
  --release "$CANDIDATE_ROOT/deploy/task-image-builder/host-release-v2.json" \
  --cluster-id oldlab --slurm-node trt-eai-oldlab-3 \
  --host-receipt OLDLAB_NODE_HOST_RECEIPT \
  --maintenance-receipt OLDLAB_NODE_MAINTENANCE_RECEIPT \
  --output "$EVIDENCE_ROOT/oldlab-trt-eai-oldlab-3.json"
```

Collect one controller fragment and one node fragment for every exact policy
node in OLDLAB and GB10.  Then assemble both complete inventories, verify the
envelope, and write a canonical copy to the selected evidence location:

```bash
python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_prerequisite_evidence.py" \
  assemble --candidate-root "$CANDIDATE_ROOT" \
  --policy "$CANDIDATE_ROOT/deploy/task-image-builder/prerequisites-v1.toml" \
  --release "$CANDIDATE_ROOT/deploy/task-image-builder/host-release-v2.json" \
  --controller-evidence OLDLAB_CONTROLLER_EVIDENCE \
  --controller-evidence GB10_CONTROLLER_EVIDENCE \
  --node-evidence OLDLAB_NODE_EVIDENCE --node-evidence GB10_NODE_EVIDENCE \
  --output "$EVIDENCE_ROOT/phase1-assembled.json"

python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_prerequisite_conformance.py" \
  verify --policy "$CANDIDATE_ROOT/deploy/task-image-builder/prerequisites-v1.toml" \
  --evidence "$EVIDENCE_ROOT/phase1-assembled.json"

python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_prerequisite_conformance.py" \
  canonicalize --policy "$CANDIDATE_ROOT/deploy/task-image-builder/prerequisites-v1.toml" \
  --evidence "$EVIDENCE_ROOT/phase1-assembled.json" \
  --output "$EVIDENCE_ROOT/phase1-canonical.json"
```

The two placeholder node-evidence arguments above stand for repeated
`--node-evidence` arguments: one for every policy node, with no duplicates or
omissions.  Do not assemble partial OLDLAB or GB10 evidence.  Successful
verification still reports zero certified nodes, production certification
false, and `phase2_guard_provider_release_missing`.

## Closed Phase 2 boundary

Phase 1 evidence is a prerequisite record, not authorization to build or
operate a rootless builder.  Phase 2 must separately deliver and accept the
node guard, allocation-contained build-environment provider, credential
projection, network policy, publication/retention path, and contained BuildKit
execution.  Until then, do not activate a provider/policy/supervisor, advertise
a node feature, certify production, or rerun task `4139e767`.
