# Developer-sandbox Slurm policy

The canonical scheduler and host-containment inputs for the shared developer
sandboxes live in:

- `deploy/slurm/developer-sandboxes/oldlab.toml`
- `deploy/slurm/developer-sandboxes/gb10.toml`

They describe two independent Slurm clusters. Never submit a GB10 request
through the OLDLAB controller or add GB10 nodes to the OLDLAB inventory.

## Admission boundary

Non-exclusive Loom jobs remain disabled until all of these conditions pass for
the exact merged candidate:

1. the cluster uses `proctrack/cgroup`, `task/cgroup`, cgroup job accounting,
   enforced associations/QoS, and multifactor fair-share;
2. CPU, RAM, swap, and device controllers are constrained and delegated;
   opted-in Loom jobs also carry the reviewed finite PID ceiling in the exact
   `loom-cgroup-v1:pids=<N>` Slurm comment;
3. the Docker daemon selected by the Loom worker uses the `cgroupfs` driver so
   an absolute Slurm job-cgroup path can be passed as `CgroupParent`;
4. the three child accounts have equal fair-share under the aggregate
   `loom-dev` budget, with no small per-developer resource ceiling;
5. every worker, trial, verifier, and sidecar is a strict child of the finite
   Slurm job cgroup and the aggregate caps stay within its allocation; and
6. the candidate-bound acceptance artifact passes
   `scripts/ops/nonexclusive_slurm_acceptance.py verify`.

The host Docker cgroup driver cannot be changed while jobs or containers are
active. A rollout must drain one node, verify zero Slurm jobs and zero running
Docker containers on that node, apply the checked-in profile through the
supported host converger, restart Docker and `slurmd`, and read back the
effective settings before moving to the next node. The controller is changed
last. Do not edit `/etc/slurm`, `/etc/docker/daemon.json`, or SlurmDB
associations by hand.

Both target clusters currently run Slurm 23.11.4. In that release, the root
Prolog runs outside cgroups and runs before `PrologFlags=Contain` creates the
extern step, so a Prolog cannot safely set the eventual job cgroup. The
converger therefore installs and enables the persistent
`loom-slurm-job-cgroup-guard.service`, not a Prolog. The root guard discovers
only exact `job_<id>` directories beneath a Slurm scope, looks up that exact
job through the local Slurm controller, and accepts only one of the three
reviewed child accounts plus the fixed `loom-cgroup-v1:pids=<N>` grammar. It
then enables the `cpu`, `memory`, and `pids` subtree controllers and lowers
`pids.max` to the cluster profile's exact value. The batch entry waits for
that exact readback before starting Docker.

A malformed Loom comment, wrong account, wrong ceiling, ambiguous job
identity, mismatched cgroup, missing controller, or drifted readback therefore
keeps the batch entry fail-closed. Jobs outside the Loom comment namespace are
unchanged. The Loom submitter, not an operator command line, emits the comment
from the checked-in pool configuration. `PrologFlags=Contain` remains required
to create the extern/job cgroup; do not add the incompatible newer
`RunInJob` flag to these Slurm 23.11 clusters.

The GB10 profile requires an administrator of that Slurm domain. Membership in
the host `docker` group is not scheduler-administration authority.

## Convergence

Render the secret-free plan on each target host:

```bash
python scripts/ops/developer_sandbox_slurm_policy.py plan \
  --profile deploy/slurm/developer-sandboxes/oldlab.toml
```

`apply` remains plan-only unless `--execute` is present. On a drained compute
node, apply and restart the local daemons:

```bash
sudo python scripts/ops/developer_sandbox_slurm_policy.py apply \
  --profile deploy/slurm/developer-sandboxes/oldlab.toml \
  --execute --restart
```

The command refuses the restart while any Slurm job or Docker container remains
on the node. It validates the desired Docker daemon configuration before the
atomic writes. Every pass holds a per-cluster lock and records a root-only,
fsynced transaction journal below
`/var/lib/loom-developer-sandbox-slurm-policy/transactions/`; its file and
accounting snapshots remain below the adjacent `snapshots/` directory. The
accounting snapshot is a compare-and-swap record scoped only to the Loom QoS,
parent/child accounts, and three exact user associations; the converger never
dumps or loads the whole cluster database. Unrelated accounts and concurrent
unrelated changes are untouched. Owned-field drift or an external reference to
a newly created Loom identity fails closed instead of deleting it. Each phase
is durable before the next mutation. A failed daemon reload, restart,
`scontrol reconfigure`, accounting mutation, or live readback restores both the
files and the exact owned accounting fields automatically. A later invocation
recovers an orphaned non-terminal journal before starting new work.

The live command binds the installed guard configuration and status to the
exact source candidate SHA and refuses a dirty or mismatched policy checkout.
Supply the binding explicitly when operating a materialized candidate:

```bash
sudo python scripts/ops/developer_sandbox_slurm_policy.py apply \
  --profile deploy/slurm/developer-sandboxes/oldlab.toml \
  --candidate-sha "$EXACT_CANDIDATE_SHA" \
  --execute --restart
```

The OLDLAB and GB10 profiles cap opted-in jobs at 32768 and 65536 PIDs,
respectively; `plan` reports the bound before any mutation. A restart pass
also daemon-reloads, enables, and starts the cgroup guard. Unit activity alone
is not health. On every scan the guard atomically publishes a root-only status
record containing its timestamp, candidate/config digests, scanned/verified/
failed counts, bounded failure reasons, and the last real job resource probe.
The final live check requires that status to be fresh and failure-free and
requires a real opted-in job probe with finite CPU, memory, and PID cgroup
readback; the GB10 profile additionally requires a positive allocated GPU TRES
readback.

After launching the bounded acceptance probe, run the combined file and live
check as root. First create the allocation-side artifact from the profile's
exact submit host (OLDLAB and GB10 are independent controllers; never reuse one
domain's command or evidence for the other):

```bash
sudo python scripts/ops/developer_sandbox_slurm_policy.py allocation-probe \
  --profile deploy/slurm/developer-sandboxes/oldlab.toml \
  --candidate-sha "$EXACT_CANDIDATE_SHA" \
  --candidate-root "$EXACT_CANDIDATE_ROOT" \
  --worker-env "$PRIVATE_WORKER_ENV" \
  --batch-uid "$EXPECTED_BATCH_UID" \
  --batch-gid "$EXPECTED_BATCH_GID" \
  --execute

sudo python scripts/ops/developer_sandbox_slurm_policy.py check \
  --profile deploy/slurm/developer-sandboxes/oldlab.toml \
  --candidate-sha "$EXACT_CANDIDATE_SHA" \
  --candidate-root "$EXACT_CANDIDATE_ROOT" \
  --worker-env "$PRIVATE_WORKER_ENV" \
  --batch-uid "$EXPECTED_BATCH_UID" \
  --batch-gid "$EXPECTED_BATCH_GID"
```

The probe submits one bounded, non-exclusive `sbatch` job and executes one
`srun` step inside that allocation. Submission uses immediate parsable output,
not `sbatch --wait`; a root-only mode-`0600` inflight record is persisted below
a non-symlink root-owned mode-`0700` directory before bounded polling begins.
Every timeout, error, or recovery first reads the exact base job from `sacct`.
An already-terminal job is archived without `scancel`; a non-terminal or
temporarily unknown job receives `scancel` for that exact cluster/job ID,
followed by terminal accounting readback. Cancellation or readback failure
remains durably fail-closed, and the next invocation recovers an interrupted
inflight or exact deterministic-name orphan before submitting anything new.

The final root-only artifact binds the full candidate SHA/tree,
cluster/controller/submit-host route, account/QoS, job, allocation node, TRES,
guard resource readback, expected numeric batch UID/GID, and completion of both
batch and step. The worker env must be owned by that same UID/GID with exact
mode `0600`; only its safe metadata, digest, and key names are recorded, never
its values. GB10 requests and reads back a positive GPU TRES. The final check
accepts only a fresh artifact for the exact profile, candidate, and identity.

Before submission and again during the final check, the candidate verifier
walks the safe non-symlink, non-group/world-writable parent chain; opens the
private env as the same inode with exact mode `0600`; rejects duplicate,
malformed, or empty env assignments; and compares every repository file's raw
blob bytes and executable mode directly with the candidate commit tree. It
also rejects non-zero index stages, skip-worktree/assume-unchanged flags,
extra or missing files, and filter/working-tree-encoding attributes. `git
status` is not the trust decision.

The `file_plan` and `live_readback` sections are deliberately separate. The
live section verifies effective `scontrol show config` task/proctrack/
accounting/fair-share settings, `cgroup.conf`, Docker's cgroup driver, guard
enablement/activity/fresh health, and the exact QoS, account hierarchy,
per-user associations, fair-share, and group TRES returned by `sacctmgr`.

Run the controller last. The controller invocation may also converge the
parent account, equal-share child accounts, user associations, aggregate TRES
budget, and QoS:

```bash
sudo python scripts/ops/developer_sandbox_slurm_policy.py apply \
  --profile deploy/slurm/developer-sandboxes/oldlab.toml \
  --execute --restart --apply-accounting
```

Repeat with `gb10.toml` only from the independently authorized GB10
administrator path. A failed or partial pass is rerun idempotently; do not
repair SlurmDB rows or daemon JSON by hand.

To restore the file snapshot and exact Loom-owned accounting fields from the
last committed transaction, use the same locked, journaled path:

```bash
sudo python scripts/ops/developer_sandbox_slurm_policy.py rollback \
  --profile deploy/slurm/developer-sandboxes/oldlab.toml \
  --candidate-sha "$EXACT_CANDIDATE_SHA" \
  --execute
```

Rollback also reloads/reconfigures services, performs live readback, and
automatically restores the pre-rollback state if any rollback phase fails. Do
not delete or edit the journal or snapshot tree by hand.

## Fair-share contract

`loom-dev` is the aggregate capacity boundary. Its three children are:

- `loom-dev-qianyi`
- `loom-dev-hongjian`
- `loom-dev-devansh`

Each child has equal fair-share and uses the `loom-dev` QoS. The aggregate
account may use the reviewed pool budget when idle, while the children have no
fixed one- or two-worker ceiling. The shared-capacity broker still clamps
active plus pending slots before submission. Slurm fair-share and current
cluster availability determine actual capacity; the profiles do not create a
reservation or a guaranteed full-pool entitlement.
