# Task-image builder Phase 1 site convergence

This runbook converges the active Phase 1 native task-image builders. They are
exclusive Slurm allocations on `trt-gb10-2` (arm64) and
`trt-eai-oldlab-6` (x86_64), with the exact
`loom-task-image-builder` QoS and reservation. It does not activate the
allocation-scoped rootless provider. The rootless bundle, QoS, identity, and
node material below are retained only as disabled Phase 2 prerequisites; they
are not a gate or substitute for this active native-builder procedure.

The authoritative design is [task-image builder reliability](../architecture/task-image-builder-reliability.md).

## Phase 1 stop conditions and protected inputs

Run this procedure only from one reviewed immutable candidate. A failure at any
step leaves scale-up closed: do not enable the next supervisor, submit a canary
task, or fall back to the transitional exec transport. Existing ready registry
digests continue to serve trials independently of builder availability.

Use the rollout-only credential only for deployment actions; no supervisor unit
may reference it. The supervisor credential has a distinct, fixed path on both
controllers.

```bash
CANDIDATE_ROOT=/opt/loom-staging-runner/candidates/REVIEWED_GIT_SHA/repo
ROLLOUT_KUBECONFIG=/var/lib/loom-staging-rollout/kubeconfig
SUPERVISOR_KUBECONFIG=/var/lib/loom-staging-rollout/external-supervisor.kubeconfig
MANAGER_IMAGE=REGISTRY/loom-capacity-manager@sha256:IMMUTABLE_DIGEST
AUTHORITY_INCARNATION=REVIEWED_NON_NIL_UUID
```

The only storage floor for an active builder is
`LOOM_WORKER_TASK_IMAGE_MIN_FREE_GB`; the checked-in worker value is 20 GiB.
The builder itself probes Docker's root after bounded owned cleanup, and exits
before creating a control-plane client or claim when the probe is unavailable
or the final free bytes are below that floor. It repeats this admission after
each processed claim. Do not describe an allocation as materializing, or use a
canary to test it, before this storage admission passes.

## Ordered Phase 1 convergence

Perform the following sequence in order. Each numbered acceptance command is a
hard boundary; retain its output in the change record. Reapplying an earlier
declarative object is safe, but never skip forward after a failure.

### 1. Publish the stable witness transport

Render the reviewed capacity control plane and apply it with the rollout
credential. `CAPACITY_EXECUTION_POLICY` and its SHA-256, when the deployment
uses an execution policy, must be the reviewed matched pair; use no execution
policy arguments only when both are deliberately absent.

```bash
uv run --no-sync loom capacity-control-plane render \
  --file "$CANDIDATE_ROOT/deploy/dev-fleet/capacity-control-plane.toml" \
  --manager-image "$MANAGER_IMAGE" \
  --authority-incarnation "$AUTHORITY_INCARNATION" \
  --execution-policy-file "$CAPACITY_EXECUTION_POLICY" \
  --execution-policy-sha256 "$CAPACITY_EXECUTION_POLICY_SHA256" \
  --external-manager-client-cidr REVIEWED_CONTROLLER_CIDR \
  | kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" --namespace loom-dev apply -f -
```

Confirm that the stable ConfigMap—not a pod name—is present and that both keys
change while the publisher is running. Sample twice more than 30 seconds apart
so the evidence spans more than two 30-second witness TTLs. The readers still
perform signature, fingerprint, canonical digest, authority, pool, epoch,
execution-state, ceiling, and expiry validation; a successful readback is not
trust in the ConfigMap alone.

```bash
kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" --namespace loom-dev \
  get configmap loom-global-execution-witness-v1 -o json \
  | jq -e '(.data["gb10.json"] | length > 0) and (.data["oldlab.json"] | length > 0)'
kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" --namespace loom-dev \
  get deploy loom-capacity-manager -o jsonpath='{.spec.template.spec.containers[?(@.name=="witness-publisher")].name}{"\\n"}'
```

Record `metadata.resourceVersion` and SHA-256 digests of both values at all
three samples. Stop if either key is empty, either signed value does not
refresh, or a reader rejects it. The publisher is the only workload token
holder: the manager container retains `automountServiceAccountToken: false`;
the publisher may only `get` and `patch` this ConfigMap.

### 2. Publish and prove the dedicated supervisor credential

On each controller, publish the narrow credential at the fixed path using the
rollout kubeconfig held only for this action. The publisher installs the scoped
database Secret, database-port-forward authority, and ConfigMap `get`; it must
not grant `pods/exec`.

```bash
sudo install -d -m 0700 /var/lib/loom-staging-rollout
sudo env KUBECONFIG="$ROLLOUT_KUBECONFIG" \
  "$CANDIDATE_ROOT/deploy/slurm/publish-external-slurm-autoscaler-kubeconfig.sh" \
  "$SUPERVISOR_KUBECONFIG"
sudo stat -c '%a %U:%G %n' "$SUPERVISOR_KUBECONFIG"
kubectl --kubeconfig "$SUPERVISOR_KUBECONFIG" --namespace loom-dev \
  get configmap loom-global-execution-witness-v1 -o name
kubectl --kubeconfig "$SUPERVISOR_KUBECONFIG" --namespace loom-dev \
  auth can-i create pods/exec
```

Accept only mode `600`, the exact ConfigMap read, and `no` for `pods/exec`.
Do not use `/var/lib/loom-staging-rollout/kubeconfig` in a runtime command or
unit. Repeat the same commands on `gx10-01c7` and `TRT-EAI-OLDLAB-1`.

### 3. Apply the active Phase 1 profile and prove empty-queue reconciliation

Use the protected staging rollout to install the reviewed environment state.
All four active supervisors must name the stable ConfigMap source and the
dedicated supervisor credential: GB10 and OLDLAB trial supervisors, plus GB10
and OLDLAB task-image-builder supervisors. No active argument may contain
`--global-execution-manager-export` or the rollout kubeconfig path.

```bash
loom admin environment-state apply --environment staging \
  --file "$CANDIDATE_ROOT/deploy/environment-state/staging.toml" \
  --var IMAGE_TAG="$IMAGE_TAG" --var ENV_CONFIG_VERSION="$ENV_CONFIG_VERSION" \
  --var GIT_SHA="$GIT_SHA"

systemctl --user status loom-autoscaler-gb10-staging.timer \
  loom-autoscaler-oldlab-staging.timer \
  loom-task-image-builder-gb10-staging.timer \
  loom-task-image-builder-oldlab-staging.timer --no-pager
```

With an empty queue, trigger or wait for one reconciliation on each controller
and retain the JSON results. Accept only a successful signed ConfigMap read,
the exact local Slurm authority, no submitted builder job, and no active
builder job. Verify the fixed Phase 1 allocation shape without submitting a
task: GB10 is `trt-gb10-2`, 19 CPUs, 110000 MiB, `gb10`, `04:00:00`; OLDLAB is
`trt-eai-oldlab-6`, 12 CPUs, 49152 MiB, `all`, `04:00:00`; both are exclusive,
concurrency one, account `loom-staging`, QoS and reservation
`loom-task-image-builder`.

```bash
squeue --name=loom-task-image-builder --format='%i %T %P %N %a %q' --noheader
scontrol show reservation loom-task-image-builder -o
sacctmgr --noheader --parsable2 show qos where name=loom-task-image-builder \
  format=Name,Flags,MaxJobsPU,MaxSubmitJobsPU,MaxWall
```

### 4. Clear GB10 storage only within Loom ownership

First inventory stopped containers. A container is eligible only when it is in
`created`, `dead`, or `exited` and either its own labels or its referenced
image labels show exactly one of `loom.task-image=true`,
`loom.task-sidecar=true`, or `loom.trial-cache=true`. Running containers,
unlabelled containers and images, and all container volumes are outside this
procedure. Repository names and tags do not establish ownership.

```bash
sudo docker ps --all --no-trunc --format '{{.ID}} {{.Status}} {{.Image}}'
sudo docker inspect "$CANDIDATE_CONTAINER_ID" \
  --format '{{.State.Status}} {{json .Config.Labels}} {{.Image}}'
sudo docker image inspect "$CANDIDATE_IMAGE_ID" --format '{{json .Config.Labels}}'
```

After an operator records the stopped state and qualifying label evidence for
that one exact container ID, remove only that ID; this command deliberately
does not pass `--volumes`. Repeat only for separately verified IDs, then let a
builder's own cleanup perform its managed-image TTL/pressure eviction and
fresh probe. Never run `docker system prune`, `docker container prune`, image
prune without the managed label filter, or a wildcard removal.

```bash
sudo docker container rm "$CANDIDATE_CONTAINER_ID"
```

Accept GB10 only after the builder records a reachable Docker root and final
free bytes at or above its required bytes; do not submit a canary just to cause
the check. If an allocation fails, queued demand remains durable but that exact
`(environment, pool)` waits five minutes after its latest failed allocation
before a retry. Do not manually shorten or bypass this cooldown.

### 5. Remove transitional exec authority only after proof

After both controllers have successfully read and validated the stable
ConfigMap, delete the temporary exact-pod `pods/exec` Role and RoleBinding
using the rollout credential. Resolve their real names from the previously
applied transition manifest; do not delete a broader RBAC object by label or
guess a pod name. Then prove denial with the dedicated credential.

```bash
kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" --namespace loom-dev \
  delete role TRANSITION_WITNESS_EXEC_ROLE
kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" --namespace loom-dev \
  delete rolebinding TRANSITION_WITNESS_EXEC_ROLEBINDING
kubectl --kubeconfig "$SUPERVISOR_KUBECONFIG" --namespace loom-dev \
  auth can-i create pods/exec
```

Acceptance is `no`, with all four supervisors still reconciling from
`loom-global-execution-witness-v1`. Do not restore exec as a response to a
read failure: an unavailable witness must leave scale-up closed and drain
legacy-supervisor-owned capacity.

## Disabled Phase 2 prerequisites: inputs, staging, and read-only preflight

This section is intentionally separate from the active Phase 1 rollout above.
Its rootless runtime bundle and evidence are prerequisites for a future Phase 2
decision only. They must not install, enable, or construct a rootless provider,
and their absence must not alter the native Phase 1 builder procedure.

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

## Disabled Phase 2 controller preparation and additive Slurm readback

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

## Disabled Phase 2 one-node maintenance boundary

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

## Disabled Phase 2 evidence assembly and verification

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
omissions.  Do not assemble partial OLDLAB or GB10 evidence. Successful
verification remains a disabled-Phase-2 record: it reports zero certified
nodes, production certification false, and
`phase2_guard_provider_release_missing`.

## Closed Phase 2 boundary

This Phase 2 evidence is a prerequisite record, not authorization to build or
operate a rootless builder. Phase 2 must separately deliver and accept the
node guard, allocation-contained build-environment provider, credential
projection, network policy, publication/retention path, and contained BuildKit
execution.  Until then, do not activate a provider/policy/supervisor, advertise
a node feature, certify production, or rerun task `4139e767`.
