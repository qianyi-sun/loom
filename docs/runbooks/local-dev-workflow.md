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

## Topology

There is **one** local topology: the whole Loom stack (control plane, service,
worker, Postgres, MinIO) runs **inside a single-node local cluster**, and trials
execute **in-cluster** via the `k8s_worker` pool. Do not also start the control
plane or service as local `uv` processes — that would create a second,
conflicting authority against the same database and object store.

External Slurm is an advanced, optional worker path (last section) and is
usually not usable from a laptop; see the #827 caveat there.

## Prerequisites

- A single-node local Kubernetes cluster tool: **kind** or **k3d**.
- `kubectl`, `uv`, and Docker on your PATH.
- Nothing else for the default (in-cluster) worker path.
- Only for the advanced Slurm path: Slurm client tools + a submit host with a
  filesystem visible to the compute nodes (see the last section).

## 1. Copy and fill the template

```
cp deploy/local/local.example.cluster.toml deploy/local/local.cluster.toml
```

`deploy/local/local.cluster.toml` is git-ignored and developer-specific. Edit
the placeholders (`<PLACEHOLDER>` / "CHANGE ME"). The template already pins the
local-safe shape: single node, `host_path` storage, 1-replica Postgres,
standalone MinIO, localhost routing, `pgbouncer` off, and **`k8s_worker`
enabled** (the default in-cluster worker path).

## 2. Create the cluster and load images

```
kind create cluster --name loom-local
# or: k3d cluster create loom-local   (single server node)

# Build/load the loom images into the local cluster so it can pull them:
uv run python -m loom_cli cluster load-images \
  --cluster-name loom-local
```

## 3. Bootstrap secrets, then bring the stack up

`loom cluster up` composes preflight → render → `kubectl apply` → wait-for-ready
for the whole stack. Bootstrap the required Secrets first (local dev uses
throwaway values; never real production secret refs):

```
# --smoke-defaults writes throwaway local secret values (never real prod refs):
uv run python -m loom_cli cluster bootstrap-secrets \
  --namespace loom-local --smoke-defaults --no-pgbouncer

# up waits for readiness by default (pass --no-wait to skip):
uv run python -m loom_cli cluster up \
  --config deploy/local/local.cluster.toml
```

`up` composes preflight → render → `kubectl apply` → wait, deploying the control
plane, service, `loom-worker`, single-pod Postgres, and standalone MinIO into
the cluster. There is no separate local process to start.

## 4. Reach the API and run a trial

There is no public ingress or TLS locally — reach the service through a
port-forward (the exact namespace/svc are printed by `cluster up`):

```
kubectl -n loom-local port-forward svc/loom-service 8080:80
```

Then submit a small batch through the CLI/API against `http://localhost:8080`
and confirm the trials reach a terminal state. With `k8s_worker` enabled,
`POST /api/v1/batches` accepts the `k8s-worker` pool and the in-cluster
`loom-worker` Deployment executes the trials on your local node.

## 5. Advanced optional: external Slurm

Use this **only** if you have a submit host that can reach the shared Slurm
cluster AND make your candidate + worker env-file visible on the allocated
**compute** nodes. This is the exact blocker recorded in **#827**: `env_file`
and `repo_dir` are read inside the sbatch script *on the compute node*, so
laptop-absolute paths and an SSH-forwarded login shell do **not** make them
compute-visible. On a plain laptop this path does not work — keep the default
`k8s_worker` path above.

If you do have a compute-visible shared filesystem: set `[k8s_worker] enabled =
false` in your config (pick exactly ONE path), re-run `cluster up`, and set the
elastic Slurm worker controller on the **control-plane process** using the real
`LOOM_CP_` prefix (`LOOM_CONTROL_PLANE_` is NOT recognized):

```
LOOM_CP_SLURM_WORKER_CONTROLLER_ENABLED=true
LOOM_CP_SLURM_WORKER_CONTROLLER_ENVIRONMENT=local-<your-name>
LOOM_CP_SLURM_WORKER_CONTROLLER_POOL_NAME=<your-slurm-pool>
LOOM_CP_SLURM_WORKER_CONTROLLER_PARTITION=<slurm-partition>
# MUST be readable on the COMPUTE NODES (shared filesystem), not just locally:
LOOM_CP_SLURM_WORKER_CONTROLLER_ENV_FILE=/shared/abs/path/worker.env
LOOM_CP_SLURM_WORKER_CONTROLLER_REPO_DIR=/shared/abs/path/to/loom
LOOM_CP_SLURM_WORKER_CONTROLLER_MAX_JOBS=2
```

The remaining `SLURM_WORKER_CONTROLLER_*` knobs keep their schema defaults;
override only if your submit host differs. Confirm `sbatch --version` works from
the control-plane process environment before continuing. Pick exactly one worker
path — never enable both the `k8s_worker` pool and Slurm submission.

## 6. Tear down

```
kind delete cluster --name loom-local   # or: k3d cluster delete loom-local
```

Nothing here is tracked by the operator or the environment identity contract,
so teardown is unconditional — delete and recreate freely.
