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

## 2. Create the kind cluster (ingress + Docker socket)

This walkthrough uses **kind** (`cluster load-images` loads via `kind load` and
is kind-only). A plain `kind create cluster` is not enough for the local stack:
`cluster up` preflight requires an IngressClass, and the default `k8s_worker`
mounts the host `/var/run/docker.sock`, which the kind node does not expose
unless you mount it. Create the cluster with a config that provides both, then
install the pinned ingress controller:

```
cat > /tmp/loom-kind-config.yaml <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    kubeadmConfigPatches:
      - |
        kind: InitConfiguration
        nodeRegistration:
          kubeletExtraArgs:
            node-labels: "ingress-ready=true"
    extraPortMappings:
      - { containerPort: 80,  hostPort: 80,  protocol: TCP }
      - { containerPort: 443, hostPort: 443, protocol: TCP }
    extraMounts:
      # expose the host Docker socket so the k8s_worker hostPath mount works:
      - { hostPath: /var/run/docker.sock, containerPath: /var/run/docker.sock }
EOF

kind create cluster --name loom-local --config /tmp/loom-kind-config.yaml --wait 60s

# Install the repo-pinned ingress-nginx so preflight's IngressClass check passes:
kubectl apply -f deploy/k8s/ingress-nginx-kind.yaml
kubectl wait --namespace ingress-nginx --for=condition=ready pod \
  --selector=app.kubernetes.io/component=controller --timeout=120s
```

(For **k3d** — which ships Traefik + can mount the socket via `--volume
/var/run/docker.sock:/var/run/docker.sock` — use `k3d cluster create` and
`k3d image import`; the rest is the same.)

## 3. Build and load the loom images

`cluster load-images` imports **already-built** local tags (it does not build).
The local manifests reference tag `0.7`, so build each loom image the render
needs with that tag, then render and load the tags found in the manifests:

```
docker build -f deploy/Dockerfile.control-plane       -t loom-control-plane:0.7 .
docker build -f deploy/Dockerfile.service             -t loom-service:0.7 .
docker build -f deploy/Dockerfile.worker              -t loom-worker:0.7 .
docker build -f deploy/Dockerfile.web                 -t loom-web:0.7 .
docker build -f deploy/Dockerfile.gateway             -t loom-llm-gateway:0.7 .
docker build -f deploy/Dockerfile.egress-xds          -t loom-egress-xds:0.7 .
docker build -f deploy/Dockerfile.family-orchestrator -t loom-family-orchestrator:0.7 .

uv run --no-sync python -m loom_cli cluster render \
  --config deploy/local/local.cluster.toml > /tmp/loom-local-rendered.yaml

uv run --no-sync python -m loom_cli cluster load-images \
  --cluster-name loom-local --from-manifest /tmp/loom-local-rendered.yaml
```

## 4. Create the namespace, bootstrap secrets, then bring the stack up

`bootstrap-secrets` **prints** `kubectl create secret` commands to stdout (it
does not run them) and targets a namespace that must already exist. Create the
namespace, `eval` the printed commands, then bring the stack up:

```
kubectl create namespace loom-local

# --smoke-defaults = throwaway local secret values (never real prod refs).
# --admin-secret adds a second `kubectl create secret` for `loom-admin-secret`
# (the singleton admin bearer the control-plane + service mount, which `up`
# preflight requires); it's opt-in so callers that create it themselves don't
# collide. eval runs both printed commands:
eval "$(uv run --no-sync python -m loom_cli cluster bootstrap-secrets \
  --namespace loom-local --smoke-defaults --no-pgbouncer --admin-secret)"

# up applies, waits for Postgres, then runs the DB migration Job
# (`alembic upgrade head`, via --migrate). --no-wait skips the readiness
# wait: the workers won't pass readiness until step 5 seeds their token, so
# don't block here. Namespace is inferred from the config.
uv run --no-sync python -m loom_cli cluster up \
  --config deploy/local/local.cluster.toml --migrate --no-wait
```

`up` deploys the control plane, service, `loom-worker`, single-pod Postgres, and
standalone MinIO into the cluster. `--migrate` waits for Postgres and then runs
the schema migration Job (the app pods assert schema-at-head and won't start
otherwise); it is opt-in because `cluster up` leaves migration an explicit
operator step for staging/production. Readiness is confirmed after step 5.

> The render + command contract above is covered by a render regression
> (`tests/loom_cli/test_cluster_render.py::test_local_example_template_renders`).
> The full kind end-to-end (image build → ingress → apply) is environment-gated;
> run it once on your machine to confirm your Docker/kind versions.

## 5. Seed the dev worker token

Workers authenticate to the control-plane with a **DB-registered** token
(`worker:report` scope). The `--smoke-defaults` placeholder in
`loom-secrets/worker-token` isn't registered, so a fresh stack logs `401` at
`/workers/register` until you seed a matching row. `ensure-dev-worker-token`
seeds a token whose plaintext equals that placeholder, so the workers already
carry the right value — **no mint, secret patch, or restart needed**. Run it
against the cluster Postgres via a port-forward (the same way the CI smokes run
alembic):

```
kubectl -n loom-local port-forward svc/loom-postgres 15432:5432 &
PF=$!; sleep 3

CP_DB_URL=$(kubectl -n loom-local get secret loom-secrets \
  -o jsonpath='{.data.cp-db-url}' | base64 -d \
  | sed -E 's#@[^/]+/#@localhost:15432/#')

LOOM_DB_URL="$CP_DB_URL" uv run --no-sync python -m loom_cli \
  admin ensure-dev-worker-token
kill $PF
```

Crash-looping workers recover on their next retry (no manual restart). Confirm
the whole stack reaches ready:

```
kubectl -n loom-local wait --for=condition=ready pod --all --timeout=180s
```

`kubectl -n loom-local logs loom-worker-0 --tail=5` should show
`/workers/register` → `200` and `/trials/claim` polling.

> **LOCAL/DEV ONLY.** `ensure-dev-worker-token` installs a fixed, guessable
> worker credential (the throwaway smoke value). Never run it against a real
> environment — those mint per-worker tokens via `loom admin tokens worker mint`.

## 6. Provision a task and run an oracle trial

A fresh cluster has **no tasks** (`GET /api/v1/tasks` is empty; note `loom
datasets list` shows pip *adapters*, not cluster tasks). Publish the
checked-in oracle smoke bundle into MinIO + the DB — egress-free, no
HuggingFace — then submit an **oracle** trial: the OracleAgent runs the
task's `solution/solve.sh` deterministically, so it needs **no model or
provider** and works fully offline.

The host CLI can't resolve in-cluster services, so port-forward first:

```
kubectl -n loom-local port-forward svc/loom-postgres 15432:5432 &
kubectl -n loom-local port-forward svc/loom-minio    19000:9000 &
kubectl -n loom-local port-forward svc/loom-service  8090:8090  &
sleep 3

export LOOM_DB_URL='postgresql+psycopg://loom:loom@127.0.0.1:15432/loom'
export LOOM_MINIO_ENDPOINT='http://127.0.0.1:19000'
export LOOM_MINIO_ACCESS_KEY=minioadmin LOOM_MINIO_SECRET_KEY=minioadmin

# 1. Publish the bundle → benchmark `loom-smoke` + task
#    `loom-smoke/gb10-oracle-hello-world` (uploads objects + inserts DB rows).
uv run --no-sync python -m loom_cli datasets publish-local \
  deploy/catalog/gb10-smoke --bucket loom-benchmarks --imported-by local:dev

# 2. Mint a user-owned submit token (the trial route rejects non-user tokens).
SMOKE_TOKEN=$(uv run --no-sync python -m loom_cli admin ensure-smoke-user \
  --format json | jq -r .token)

# 3. Submit the oracle trial — same payload the release-gate smoke uses
#    (src/loom_cli/rollout/steps/s13_smoke.py). Omit required_worker_pool so
#    any local worker can claim it (the default `gb10` would strand it).
TRIAL=$(curl -sS -X POST http://localhost:8090/api/v1/trials \
  -H "Authorization: Bearer ${SMOKE_TOKEN}" -H 'Content-Type: application/json' \
  -d '{"task_id":"loom-smoke/gb10-oracle-hello-world",
       "config":{"agent_name":"oracle","agent_model":null},
       "idempotency_key":"local-oracle-1"}' | jq -r '.id')

# 4. Poll to a terminal state.
watch -n3 "curl -sS http://localhost:8090/api/v1/trials/${TRIAL} \
  -H 'Authorization: Bearer ${SMOKE_TOKEN}' | jq -r '.state'"
```

The oracle runs `solution/solve.sh` in an in-cluster sandbox **built from
the task's `environment/Dockerfile`** — so the worker node needs enough
egress to pull the base image and any `RUN pip install` in that Dockerfile.
On a normal machine the sandbox builds and the trial reaches `succeeded`;
behind a TLS-intercepting proxy or air-gapped, pre-build + side-load the
task image (see the restricted-egress section). Model-based agents instead
need a real provider connection + key — the `--smoke-defaults` provider
keys are placeholders.

## 7. Advanced optional: external Slurm

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

## 8. Tear down

```
kind delete cluster --name loom-local   # or: k3d cluster delete loom-local
```

Nothing here is tracked by the operator or the environment identity contract,
so teardown is unconditional — delete and recreate freely.

## Troubleshooting: restricted-egress / air-gapped clusters

The default flow above assumes the kind node and your `docker build` have clean
internet egress. Behind a TLS-intercepting corporate proxy (self-signed CA) or
fully offline, several steps need the images side-loaded from the host instead:

- **`docker build` fails with a pip/npm TLS error** (`CERTIFICATE_VERIFY_FAILED`,
  self-signed cert). The build container's egress is intercepted. Build on a host
  with clean egress, or pull the pre-built release images from a registry you can
  reach and retag them to `:0.7`.
- **kind node `ImagePullBackOff`** for `registry.k8s.io/ingress-nginx/*`,
  `minio/minio`, `envoyproxy/envoy`, etc. The node can't pull. Pull each on the
  host (`docker pull`) and import it into the node:
  `docker save <image> | docker exec -i loom-local-control-plane ctr -n k8s.io images import -`.
  (`kind load` may fail on registry.k8s.io images that carry attestation
  manifests — the `ctr import` above avoids that.) For the pinned ingress images,
  strip the `@sha256:` digest from `deploy/k8s/ingress-nginx-kind.yaml` so they
  resolve against the tag you imported.
- **`loom-minio` stuck `ErrImagePull` after `up`.** The rendered `minio/minio`
  reference is untagged, so its `imagePullPolicy` defaults to `Always` — kubelet
  re-pulls on every pod start even when the image is cached. Offline, patch it:
  `kubectl -n loom-local patch statefulset/loom-minio --type=json -p
  '[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]'`
  then delete the pod so it recreates from cache.
