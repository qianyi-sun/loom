# Local Dev Workflow

This runbook is for a developer running Loom on their own laptop to test
changes **before pushing**. Local dev is a personal, throwaway setup — it is
NOT a formal Loom environment. It is not part of the environment identity
contract, it is not routed on `yylx.world`, and the operator rollout never
touches it.

Use it to catch breakage on your machine first. The shared environments are
where integration actually happens:

- **local** — pre-push testing on your laptop (this runbook).
- **dev** (`yylx.world/dev`) — post-merge integration of `refs/heads/dev`.
- **staging** / **production** — release-gated deploys.

If you only need unit tests, run `pytest` directly; you do not need a local
cluster. Spin up a local cluster when you need the full API + worker path.

## Prerequisites

- A single-node local Kubernetes cluster: **kind** or **k3s**. One node is
  enough — local dev is single-node by construction.
- `kubectl`, `uv`, and Docker on your PATH.
- For the **default** worker path: Slurm client tools (`sbatch`, `squeue`,
  `sacct`, `scancel`) on your PATH and working submit credentials to the
  shared Slurm cluster (your own SSH-forwarded login environment). You submit
  trials with **your own** credentials.
- For the **fallback** worker path (offline / no Slurm): nothing extra — the
  in-cluster `loom-worker` runs trials on your local node.

## 1. Copy and fill the template

```
cp deploy/local/local.example.cluster.toml deploy/local/local.cluster.toml
```

`deploy/local/local.cluster.toml` is git-ignored and developer-specific.
Edit the placeholders (`<PLACEHOLDER>` / "CHANGE ME"). The template already
pins the local-safe shape: single node, `host_path` storage, 1-replica
Postgres, standalone MinIO, localhost routing, and `pgbouncer` off. You
choose the worker path in the next step.

## 2. Bring up the local cluster

Create a single-node cluster with your tool of choice, for example:

```
kind create cluster --name loom-local
# or: k3d cluster create loom-local   (single server node)
```

Render and apply the manifests from your filled config, then reach the API
and web through port-forwards (there is no public ingress or TLS locally):

```
kubectl -n loom-local port-forward svc/loom-service 8080:80
```

## 3. Choose the worker execution path

### Default: external Slurm

Keep `[k8s_worker] enabled = false` in your cluster config and configure the
control-plane elastic Slurm worker controller through **env vars** (it runs
`sbatch`/`squeue` as local subprocesses on the host running
loom-control-plane — your laptop). The controller submits workers to the
shared Slurm cluster with your credentials:

```
LOOM_CONTROL_PLANE_SLURM_WORKER_CONTROLLER_ENABLED=true
LOOM_CONTROL_PLANE_SLURM_WORKER_CONTROLLER_ENVIRONMENT=local-<your-name>
LOOM_CONTROL_PLANE_SLURM_WORKER_CONTROLLER_POOL_NAME=<your-slurm-pool>
LOOM_CONTROL_PLANE_SLURM_WORKER_CONTROLLER_PARTITION=<slurm-partition>
LOOM_CONTROL_PLANE_SLURM_WORKER_CONTROLLER_ENV_FILE=/abs/path/worker.env
LOOM_CONTROL_PLANE_SLURM_WORKER_CONTROLLER_REPO_DIR=/abs/path/to/loom
LOOM_CONTROL_PLANE_SLURM_WORKER_CONTROLLER_MAX_JOBS=2
```

The remaining `SLURM_WORKER_CONTROLLER_*` knobs (requested CPUs/memory/
concurrency, pending-job cap, `sbatch`/`squeue` paths, timeouts) keep their
schema defaults; override them only if your submit host differs. Confirm
`sbatch --version` works from the same shell that starts the control plane —
if it does not, fix your Slurm submit reachability before continuing.

### Fallback: in-cluster k8s_worker (offline / no Slurm)

If you cannot reach Slurm, run trials on your local node instead. In
`deploy/local/local.cluster.toml` set:

```
[k8s_worker]
enabled = true
```

Leave every `SLURM_WORKER_CONTROLLER_*` env var UNSET. Re-render and apply.
The local `loom-worker` Deployment becomes the trial execution path and
`POST /api/v1/batches` accepts the `k8s-worker` pool. Pick exactly one path;
do not enable both Slurm submission and the k8s_worker pool.

## 4. Run the API + backing services

Postgres and MinIO run as single pods in the local cluster (from the
template). Run the Loom services with `uv`:

```
uv run python -m loom_control_plane
uv run python -m loom_service
```

Point them at the local Postgres/MinIO via the standard service env vars
(port-forward the `postgres` and `minio` services, or set the in-cluster
service DNS if you run the services as pods). Then submit a small batch and
confirm trials reach a terminal state.

## 5. Tear down

```
kind delete cluster --name loom-local   # or: k3d cluster delete loom-local
```

Nothing here is tracked by the operator or the environment identity
contract, so teardown is unconditional — delete and recreate freely.
