# GB10 remote worker pool

The staging GB10 pool is ARM64 Docker capacity allocated through Slurm. Its
authoritative policy is the `gb10` worker-pool entry in
`deploy/environment-state/staging.toml`; the external autoscaler supervisor on
`gx10-01c7` is the capacity actuator. Per-host node-agent and fixed-worker
units are kept stopped by environment-state reconciliation.

## Current staging policy

The normal worker policy scales from zero to 140 slots across 14 Slurm nodes:
`trt-gb10-1` and `trt-gb10-3` through `trt-gb10-15`. The full readiness and
retired node-agent inventory still covers `trt-gb10-1` through
`trt-gb10-15`; node 2 is excluded only from normal workers because its active
year-long reservation is the separate exclusive ARM64 task-image-builder
capacity. `trt-gb10-16` remains outside Loom's accepted allocation boundary.
Each normal worker job requests 20 CPUs, 115000 MiB, and concurrency 10.
Every trial container is capped at 2 CPUs, 11500 MiB, and 512 PIDs. Jobs are
non-exclusive, resource-aware, limited to the partition's one-day maximum, and
bounded by `max_jobs=14` and `pending_job_cap=2`.

## Task-image builders

Dockerfile-backed task images use the separate
`task-image-builder-gb10` (ARM64) and `task-image-builder-oldlab` (x86_64)
policies. These are scale-to-zero Slurm pools, not permanently reserved hosts:
queued image materializations create at most one exclusive, concurrency-one
builder allocation per architecture; the allocation can process several images
and exits after 120 idle seconds. Trial workers remain non-exclusive and
pull-only.

The x86_64 OLDLAB builder and ARM64 GB10 builder are active on the dedicated
`trt-eai-oldlab-6` and `trt-gb10-2` reservations. Activation requires a
least-privilege token carrying
`task-image:build`, a dedicated builder env file, an owner-only Docker
`config.json` mounted only into the builder container, the declared Slurm
account/QoS/exclusive reservation, native exclusive capacity, and a
registry-retention controller. The
builder token's scope must be exactly `task-image:build`; mint it with
`loom admin tokens worker mint --kind task-image-builder`. The registry
retention controller uses a different token whose scope must be exactly
`task-image:gc`; mint it with
`loom admin tokens worker mint --kind task-image-registry-gc`. Never share
either credential with the other controller or with trial workers.

Each native capacity class remains blocked until safe exclusive capacity is
provisioned. Nodes may remain in shared
Slurm inventory, but the named builder reservation and `--exclusive` allocation
must prevent overlap with packed trial jobs; nodes hosting services outside
Slurm must be removed from the builder inventory. Do not remove activation
blockers or reuse ordinary trial-worker credentials to make either policy
start.

Registry retention must use Loom's fenced protocol; do not apply an independent
age-only registry lifecycle rule to task-image repositories. The deployment-
owned controller repeatedly calls
`POST /api/v1/internal/task-image-materializations/registry-gc/claim` with its
stable `gc_id`, deletes every unique immutable digest in the returned
`registry_images` map and `registry_image_history` list (an already-absent
manifest is success), and calls
`POST /api/v1/internal/task-image-materializations/registry-gc/{id}/complete`
with that same `gc_id` and `lease_epoch` only after every deletion is confirmed.
A 204 claim response ends the pass. The controller must finish within the
server-issued lease; an expired claim is safely reassigned at a higher epoch.
Loom rechecks current-task and nonterminal-trial references before claim and
again at completion, requeueing a rebuild if a reference races deletion. Clear
`registry_retention_not_provisioned` only after this controller, its dedicated
API token, registry deletion credential, monitoring, and scheduled supervisor
have rollout evidence.

The fixed accepted inventory stays explicit even when a host is busy. A
healthy busy node can advertise no free capacity and becomes eligible again
after resources are released. The autoscaler does not reinterpret a busy host
as removed inventory.

## Disabled Pipeline GPU admission surface

Issue #1213 adds a separate, fail-closed Pipeline policy contract in
`worker-plan.csv`, `controller.env.example`, and `slurm/`. It declares the
final `trt-gb10-1` through `trt-gb10-15` topology, one exclusive
`gpu:gb10:1` job and one concurrency-1 `behavior-gpu-gb10` worker per
allocation. `trt-gb10-7` remains present as `drain`/`DOWN`; topology is never
made healthy by deleting a quarantined host.

That Pipeline policy merges with its autoscaler disabled and desired slots
zero. The files do not alter the legacy staging Trial pool described above,
install Slurm bytes, start a supervisor, or enable direct/node-agent task
capacity. A later authorized candidate-bound rollout must verify the exact
bundle, Slurm client and munge connectivity before activation.

The checked-in `slurm/` directory is that disabled Pipeline policy bundle, not
the canonical controller configuration for staging Trial autoscaling. The
Trial autoscaler uses the dedicated `loom-staging` partition installed by
`deploy/slurm/converge-loom-gb10-slurm-partition.sh`; the institution-owned
shared `gb10` partition remains unchanged.

Only tasks declaring `environment.cpu_arch = "arm64"` or `"any"` can run in
this pool. Missing architecture requirements are treated as `x86_64` and are
not claimed by GB10 workers.

## Protected ownership

Shared-staging candidate selection, checkout materialization, worker env
generation, supervisor reconciliation, and release validation belong to the
root-installed rollout authority. Operators use:

```bash
loom-staging-rollout --env staging preflight
loom-staging-rollout --env staging start --dry-run
loom-staging-rollout --env staging start
loom-staging-rollout --env staging status REQUEST_ID
loom-staging-rollout --env staging logs REQUEST_ID
loom-staging-rollout --env staging resume REQUEST_ID
loom-staging-rollout --env staging cancel REQUEST_ID --reason "bounded operational reason"
```

Operators cannot select a ref, SHA, image, host subset, env file, concurrency,
or force flag. Repair a failed immutable request and use `resume`; roll code
back through a merged revert and a new request.

The candidate checkout and generated private worker envs live under the
service-owned `/shared_work2/loom-staging-rollout/` hierarchy. Docker data,
trial scratch, caches, databases, object-store data, and Kubernetes volumes
must stay on node-local storage; `/shared_work2` is only for read-mostly
candidate material and controlled evidence transfer.

## Private service path

Workers reach the staging Control Plane, Gateway, and MinIO through private
managed endpoints. Public ingress remains limited to the SPA and service API.
The candidate-owned generated worker environment pins both worker processes
and Docker bridge containers to OLDLAB-1's private fleet forwards and TLS
trial-cache registry:

```text
LOOM_WORKER_CONTROL_PLANE_URL=http://192.168.50.103:18081
LOOM_WORKER_GATEWAY_URL=http://192.168.50.103:19100
LOOM_WORKER_SUBPROCESS_GATEWAY_URL=http://192.168.50.103:19100
LOOM_WORKER_MINIO_ENDPOINT=http://192.168.50.103:19000
LOOM_WORKER_TRIAL_CACHE_REGISTRY_REPO=192.168.50.103:5443/loom-trial-cache
```

Subprocess agents receive the adapter-specific Gateway facade derived from
that sandbox-facing base URL. Node-local loopback tunnels, host networking,
and `host.docker.internal` are not part of the staging GB10 contract.

Do not install ad hoc tunnels or place `HF_TOKEN` on nodes. The protected
rollout checks durable tunnel units, candidate/source identity, token parity,
and internal `s3://` catalog sources before capacity is accepted.

## SSH trust

The rollout service uses its root-owned Ed25519 identity and the checked-in
`ssh_config` plus pinned `known_hosts`. `trt-gb10-1` is the public jump host;
private nodes use the declared ProxyJump topology. Ambient known-hosts state,
`accept-new`, user-forwarded private keys, and topology changes are rejected.

Trust bootstrap and rotation are administrator procedures that publish only a
service public key. Normal operators inspect the broker request instead of
running candidate internals directly. Uninstall revokes the service key on all
ledger-bound hosts before removing local key material.

## Native-runtime authority

The personal native-builder runbooks use the installed
`/usr/local/libexec/loom-personal-dev-native-builder-runtime-authority` as the
only remote sudo target. The operator encoder writes a framed request to stdin;
root-local OpenSSH runs with `-F /dev/null`, fixed GB10 host/jump, identity, and
known-host settings, then invokes only `sudo -n --` for that launcher. The
non-executable broker library is never a remote command.

`status`, `prepare`, `stage-agent`, `activate`, and `remove` receipts are the
sole privileged host evidence boundary. Do not run remote privileged shells,
Python, systemd, nftables, Docker, or file-copy commands for this runtime. The
public gVisor archive is the one data-only unprivileged SFTP upload; private
key and CA bytes enter only through local root-opened client descriptors and
never through a command argument, environment, receipt, evidence file, or
remote staging path.

## Supervisor and health

Environment-state renders and owns:

```text
loom-autoscaler-gb10-staging.service
loom-autoscaler-gb10-staging.timer
loom-task-image-builder-gb10-staging.service
loom-task-image-builder-gb10-staging.timer
```

The trial-pool timer runs
`scripts/ops/worker_pool_autoscaler_external_once.py --environment staging
--pool-name gb10`, using local database tunnel port `15451`. Drift checks
require the declared service/timer, exact candidate paths, environment and
pool arguments, enabled/active state, Slurm cluster identity, and current
worker-token parity.

The task-image-builder timer independently runs
`scripts/ops/task_image_builder_autoscaler_external_once.py --environment
staging --pool-name task-image-builder-gb10`, using local database tunnel port
`15454`. Its reconciliation submits at most one concurrency-one allocation on
the dedicated `trt-gb10-2` reservation and scales back to zero after the
configured idle interval.

Read-only inspection:

```bash
loom admin worker-pools autoscaler status
loom resources status --format json
loom-staging-rollout --env staging status REQUEST_ID
```

Acceptance requires the full declared inventory, current host health/resource
observations, linked worker registrations for active Slurm allocations,
candidate SHA and environment version parity, Docker backend identity, tunnel
health, an exact TLS registry canary pull from every allocatable node, and a
representative ARM64 trial. A missing or unhealthy host remains visible in
evidence; resource pressure alone is not a trust revocation.

## Maintenance

Drain or prove a worker idle before pruning Loom-labeled containers, networks,
images, or build cache. Normal restarts preserve the remote-worker trajectory
and benchmark cache volumes. Rotate worker tokens with an overlap, confirm all
active allocations use the new generation, then revoke the old prefix.

If a node or supervisor becomes unhealthy, preserve request, Slurm job, worker,
and host evidence; fix the network, resource, Docker, storage, or credential
cause; and resume the same request. Do not reduce inventory or relax gates to
make validation pass.
