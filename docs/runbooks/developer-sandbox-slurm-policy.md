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
atomic writes and snapshots the replaced files below
`/var/lib/loom-developer-sandbox-slurm-policy/snapshots/`.
The OLDLAB and GB10 profiles cap opted-in jobs at 32768 and 65536 PIDs,
respectively; `plan` reports the bound before any mutation. A restart pass
also daemon-reloads, enables, and starts the cgroup guard. Read back both
`systemctl is-active loom-slurm-job-cgroup-guard.service` and an opted-in
job's exact `pids.max` before undraining the node.

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
