# Cluster deployment

**Status: shipped.** The full `loom cluster` CLI is operational —
`status`, `render`, `preflight`, `audit`, `up`, `down`, `doctor`,
`bootstrap-secrets`. See [`../operator-runbook.md`](../operator-runbook.md)
for day-2 procedures and [`config-consolidation.md`](config-consolidation.md)
for the schema that drives manifest generation and Secret bootstrap.
Originally tracked pre-migration as carinrc#76 (cluster deploy CLI) and
carinrc#146 (schema unification).

`loom cluster` is the multi-node deployment mode. A control node runs the API services + storage; worker nodes spawn trial sandboxes via `docker.sock` onto per-trial Docker bridges. Users supply OpenAI-compatible model endpoints via the per-team `provider_connections` API; `loom-llm-gateway` mediates every LLM call.

The cluster does not host model inference in v1.0. It can reach third-party or
team-operated OpenAI-compatible endpoints through approved provider
connections, but it does not run vLLM jobs or serve checkpoints for users.

`loom service` is the single-box compose mode (existing). The two share `loom-llm-gateway` + the egress proxy + the sandbox routing pattern (see [Sandbox→gateway](#sandboxgateway-flow)); the cluster mode adds per-node Docker singletons + a k8s manifest set.

## Topology

```
┌────────── Control node (tainted) ──────────────┐    ┌── Worker node × N ──────────────────────┐
│  postgres  (StatefulSet, 1 replica, local PVC)  │    │  loom-worker (DaemonSet pod)            │
│  minio     (StatefulSet, 1 replica, local PVC)  │    │  loom-llm-gateway-sandbox               │
│  loom-service / control-plane / llm-gateway /   │    │      (Docker singleton, worker-spawned, │
│      web (Deployments, ≥2 replicas, spread)     │ ←→ │       on two bridges: loom-uplink +     │
│  loom-egress-proxy (Deployment, ≥2)             │    │       per-trial sandbox-<id>)           │
│  loom-egress-xds   (Deployment, 1)              │    │  loom-gateway-router (DaemonSet pod,    │
│  loom-gateway-router (DaemonSet, hostPort 30443)│    │       hostPort 30443, fwds to gateway)  │
│  ingress (nginx)                                │    │  docker.sock (hostPath)                 │
│  loom-worker (DaemonSet, --co-locate-workers-   │    │  bench-cache  (hostPath; read-through)  │
│     on-control opt-in; tainted off by default)  │    │  trajectory-cache (hostPath; write-     │
│                                                 │    │     through to MinIO)                   │
└─────────────────────────────────────────────────┘    └─────────────────────────────────────────┘
```

Storage is an orthogonal flag: `--storage embedded` (in-cluster Postgres + MinIO; the default) or `--storage external` (managed Postgres + S3). External is the only HA path; embedded is intentionally simple. For public-beta, staging, and production-like evidence environments, kind node-local `local-path` volumes are not a durable boundary by themselves; protected environments need host-managed/external storage or a fresh verified backup manifest before any operation that can delete PVCs, namespaces, kind clusters, or Docker volumes.

The short-term durable embedded path is explicit static host storage. Set
`persistent_storage_backend = "static-host-path"` and
`persistent_storage_host_path_root = "/data/<environment>"` in
`cluster-config.toml`; render then creates Retain `PersistentVolume` objects
for Postgres, MinIO, and worker trajectories, and binds the matching PVCs with
`storageClassName: ""` plus fixed `volumeName`s. `loom cluster preflight
--config cluster-config.toml` audits existing critical PVCs first; if none
exist yet, it accepts the static-host-path render config for first apply. If
critical PVCs already exist, the live PV bindings win and must be Retain, not
local-path provisioned, and hostPath volumes must sit under `/data/`.
The same `--config` also gives schema-doctor the target rendered Deployments:
preflight validates live Secrets, but it checks env-var drift against the
manifest that will be applied rather than stale pods from the previous rollout.

The SPA (`loom-web`) ships in the manifest set with `replicas: 0`; operators scale up when SPA work resumes.

## Prerequisites

`loom cluster` requires the following on every worker node, beyond a working Kubernetes cluster:

| Prerequisite | Why |
|---|---|
| **Docker engine installed** alongside the CRI k8s uses | Worker spawns sandbox containers + manages per-trial bridges via `docker.sock`. Containerd-only nodes (most modern k8s) need Docker as a separate package install. |
| **Pod Security Standards = `privileged` on the `loom` namespace** | The worker DaemonSet, sandbox singleton, egress proxy, and preflight Job all need hostPath / hostNetwork access. |
| **`hostPort: 30443` available + reachable from Docker bridges** | Gateway router binds this port; the singleton dials it from `loom-uplink`. |
| **`10.42.0.0/16` (or `--sandbox-cidr`) free on every worker node** | Per-trial bridges are allocated from this range. |
| **etcd encryption at rest** | The `k8s-secret` SecretStore backend holds bootstrap creds + master key in etcd; without encryption-at-rest, an etcd snapshot is full Loom takeover. |

Managed k8s offerings that block hostPath / hostNetwork via locked-down PSS (EKS Fargate, GKE Autopilot, Cloud Run) cannot run `loom cluster`. Operators on those use `loom service` (single-box).

`loom cluster up` runs a preflight Job (or SSH, by operator choice) that verifies every prerequisite and reports a per-worker punch list before any apply.

## Sandbox→gateway flow

The security pivot. Trial sandbox containers are spawned by the worker via `docker.sock` onto a Docker bridge — they are NOT k8s pods. Without explicit routing they could dial the public internet via the host's default route, bypassing the gateway. The shipped path:

```
┌─ sandbox container (on per-trial --internal bridge) ──┐
│  OPENAI_BASE_URL=https://loom-sandbox-gateway.local:  │   stock SDK honors *_BASE_URL +
│      8443/openai/v1                                    │   uses *_API_KEY in headers.
│  OPENAI_API_KEY=<step-JWT>                             │
│  SSL_CERT_FILE=/etc/ssl/loom-ca/loom-ca.crt            │
│  /etc/hosts: loom-sandbox-gateway.local → 10.42.N.2    │   worker --add-host injects
│                                                         │   per-trial bridge IP.
└──────────────────┬─────────────────────────────────────┘
                   │ HTTPS (TLS via loom-ca)
                   ▼
┌─ loom-llm-gateway-sandbox (worker-spawned Docker      ┐
│  singleton per node, on loom-uplink + per-trial       │   TLS-terminates, validates step-JWT,
│  bridges; NOT --network host)                          │   forwards to gateway via uplink →
└──────────────────┬─────────────────────────────────────┘   gateway-router hostPort.
                   │ HTTPS, host-bridge-gw:30443
                   ▼
┌─ loom-gateway-router (DaemonSet pod, hostPort 30443) ─┐   TCP proxy to in-cluster Service.
└──────────────────┬─────────────────────────────────────┘
                   │
                   ▼
┌─ loom-llm-gateway (k8s Service, ≥2 replicas) ─────────┐
│  resolves connection, decrypts API key, rate-limits,  │   trust boundary: knows team_id,
│  calls upstream via egress proxy                       │   records usage.
└──────────────────┬─────────────────────────────────────┘
                   │ HTTP proxy via base_url scheme/port
                   │   x-loom-connection-id + :authority match
                   ▼
┌─ loom-egress-proxy (Envoy, ≥2 replicas) ──────────────┐   second wall: even if gateway
│  validates target_ip:port ∈ connection allowlist      │   compromised, only operator-
│  [connection_id]; per-team local_ratelimit            │   resolved endpoints are dialed.
└──────────────────┬─────────────────────────────────────┘
                   ▼
            OpenAI / Anthropic / Google
```

The worker process and the subprocess agent do not share a network
view. `LOOM_WORKER_GATEWAY_URL` remains the worker-pod URL for
worker-side gateway clients. Subprocess agents launched inside Docker
sandboxes use `LOOM_WORKER_SUBPROCESS_GATEWAY_URL`; the default k8s
render sets it from `worker_subprocess_gateway_url`, whose default is
`http://host.docker.internal:30443/openai/v1`. That reaches the
node-local `loom-gateway-router` hostPort and then the in-cluster
gateway facade on bare-node k8s deployments. The worker treats this as
the sandbox-facing gateway-router URL and normalizes it by adapter
dialect before setting SDK environment variables: OpenAI adapters use
`/openai/v1`, Anthropic adapters use `/anthropic`, and Gemini adapters
use `/google`. Kind-on-host deployments that launch sandboxes through
the platform host Docker socket must review host port ownership; if
another service owns `30443`, set
`worker_subprocess_gateway_url` to a dedicated host bridge such as
`http://host.docker.internal:30444/openai/v1` and install the
`worker_service_tunnels.py --subprocess-gateway-local-port 30444` managed
tunnel to `svc/loom-llm-gateway:9100`. `DockerDriver` adds the Linux Docker
host-gateway mapping when that hostname is used.

### Network isolation

Per-trial bridges are `--internal` (Docker primitive — no host route). At trial start the worker:

1. Allocates a free `/24` from `--sandbox-cidr` (default `10.42.0.0/16`) by inspecting `docker network ls`.
2. `docker network create --internal --subnet 10.42.<idx>.0/24 sandbox-<trial>`.
3. `docker network connect --ip 10.42.<idx>.2 sandbox-<trial> loom-llm-gateway-sandbox` (singleton joins at a pinned IP).
4. Runs the sandbox with `--network sandbox-<trial>` + `--add-host loom-sandbox-gateway.local:10.42.<idx>.2`.

The singleton is a worker-spawned Docker container, NOT a k8s pod — `docker network connect` can't mutate a kubelet-managed pod's net namespace on containerd-based clusters. The singleton's two-bridge attachment (one `--internal` for sandbox traffic, one normal `loom-uplink` for reaching the gateway router) is the configuration Docker accepts; `--network host` + bridge attachments is rejected.

Verified end-to-end by [`cluster-deploy-spikes/01-sandbox-bridge.sh`](cluster-deploy-spikes/01-sandbox-bridge.sh).

### Authentication

Step JWTs (Ed25519, 1-hour lifetime, scoped to `(trial_id, team_id, provider_connection_id, step_id)`) are minted by control-plane, signed with a private key in `k8s-secret`, verified by gateway + singletons holding the public key only. Public keys distribute as a `loom-jwt-public-keys` ConfigMap → worker watches → bind-mount into singleton → fsnotify reload.

The SDK puts the JWT in its provider-specific header (OpenAI: `Authorization: Bearer`; Anthropic: `x-api-key`). TLS terminates at the singleton (loom-ca-signed cert for `loom-sandbox-gateway.local`); the JWT passes in cleartext to the gateway, which is the trust boundary.

JWT refresh for steps longer than the JWT lifetime: worker bind-mounts a per-trial host directory (`/var/lib/loom/sandboxes/<trial>/`) to the sandbox's `/run/loom/`; rotation writes a new JWT via `step-jwt.tmp` + `rename(2)`. The agent-runtime base image's fsnotify watcher rereads on change. Sandboxes that aren't built from the agent-runtime base can't refresh, so the control-plane caps step duration at JWT_LIFETIME − 5 min for those.

Verified by [`cluster-deploy-spikes/04-jwt-fsnotify-rotation.sh`](cluster-deploy-spikes/04-jwt-fsnotify-rotation.sh) and [`05-add-host-ssl-cert-file.sh`](cluster-deploy-spikes/05-add-host-ssl-cert-file.sh).

### Layered SSRF defense

Four layers; any single one closes the path if the others fail:

1. **`--internal` sandbox bridge** — no host route, so direct dial fails.
2. **Loom-ca TLS cert** — only the singleton validates as the SDK's target.
3. **Step-JWT auth at gateway** — anonymous calls rejected; per-trial scope.
4. **Egress proxy IP allowlist** — even with a compromised gateway, only `resolved_egress_ips[connection_id]` is reachable.

`POST /provider-connections` rejects RFC1918 / CGNAT / loopback /
link-local / ULA by default; `team.allow_private_endpoints=true`
permits RFC1918 + CGNAT + ULA for on-prem providers (defaulted on in
`loom service` single-box mode). Loopback and link-local remain
rejected even with the private-endpoint flag.

## Data architecture

| Data | Canonical home | Cache layer |
|---|---|---|
| Benchmark task tree | MinIO `benchmarks/<slug>/<version>/` | per-worker `/var/lib/loom/benchmarks/<slug>/<version>/` (read-through; `flock(2)` prevents thundering herd) |
| Trial trajectory `events.jsonl` | MinIO `trajectories/<trial_id>/` | per-worker `/var/lib/loom/trajectories/<trial_id>/` (write-through; multipart upload per flush) |
| ATIF projection JSON | MinIO `atif/<trial_id>/` | none |
| Postgres rows | Postgres on control node | none |

`TrajectoryWriter` appends to the local file (low-latency) and flushes batches to MinIO every `LOOM_TRAJECTORY_FLUSH_INTERVAL_MS` (default 1000 ms). Node failure mid-run loses at most one unflushed batch; completed trials are fully in MinIO.

Bench-cache eviction is LRU when `/var/lib/loom/benchmarks/` exceeds `LOOM_BENCH_CACHE_QUOTA_GB` (default 200). In-use protection: trial start writes `/var/lib/loom/benchmarks/.in-use/<slug>__<version>__<pid>`; eviction skips groups with a live PID; trial-start and eviction serialize via shared/exclusive flock on `.lock/<slug>__<version>`. Stale entries are reaped on worker startup by walking `.in-use/` and `kill(pid, 0)`.

MinIO bucket lifecycle: `AbortIncompleteMultipartUpload after 7 days` for non-trajectory prefixes, `after 14 days` for `trajectories/` (covers SWE-Bench-class long trials).

## Component map

| Component | Form | Lives where | Responsibility |
|---|---|---|---|
| `loom-worker` | DaemonSet (one pod per worker node) | `loom` namespace | Spawns sandboxes via docker.sock; manages per-trial Docker bridges + singleton lifecycle; writes step-JWT files; trajectory write-through; bench-cache read-through. |
| `loom-llm-gateway-sandbox` | Worker-spawned Docker container (one per worker node) | Host Docker; NOT a k8s pod | TLS-terminates sandbox traffic; validates step-JWT; forwards to gateway-router. Worker manages lifecycle via `docker events`. |
| `loom-gateway-router` | DaemonSet (one pod per worker node, hostPort 30443) | `loom` namespace | TCP proxy: `host:30443` → in-cluster `loom-llm-gateway.loom.svc:9100`. |
| `loom-llm-gateway` | Deployment (≥2 replicas) | `loom` namespace | Provider-facade routes (`/openai/v1/...`, `/anthropic/v1/...`) plus BYO OpenAI-compatible `/v1/chat/completions` and `/v1/responses`; resolves `provider_connection_id`; decrypts API key; forwards through the egress proxy. |
| `loom-egress-proxy` | Deployment (≥2 replicas, Envoy) | `loom` namespace | Validates `target_ip ∈ resolved_egress_ips[connection_id]`; per-team rate limit; HTTPS CONNECT or HTTP forward-proxy routing to the provider endpoint declared by `base_url`. |
| `loom-egress-xds` | Deployment (1 replica, Python control plane) | `loom` namespace | Reads `provider_connections` from Postgres; serves Envoy CDS/EDS via gRPC. Vendored from `envoyproxy/python-control-plane`. |
| `loom-service` | Deployment (≥2 replicas) | `loom` namespace | REST API; `POST /trials`, `POST /batches`, `POST /provider-connections`, etc. |
| `loom-control-plane` | Deployment (≥2 replicas) | `loom` namespace | Schedules trials; mints step JWTs; serves Worker claim path. |
| `loom-web` | Deployment (replicas: 0 by default) | `loom` namespace | SPA, paused; operator scales up when work resumes. |
| `postgres`, `minio` | StatefulSet (1 replica each, control node) | `loom` namespace | State + object store. `--storage external` substitutes managed equivalents. |

## Public / internal boundary

What the public Internet can reach (left) vs. what stays cluster-internal
(right). `loom cluster audit` enforces this statically on every render
and the kind smoke runs it before `kubectl apply` — see #77.

| Component | Public via Ingress | Reason |
|---|---|---|
| `loom-service` | Yes — `https://<ingress_host>/api/v1/*` | The user-facing REST surface and authenticated service-proxied downloads. |
| `loom-web` | Yes — `https://<ingress_host>/` | The React SPA. Replica count is 0 by default; operators scale up to enable. |
| `loom-llm-gateway` | **No** | LLM calls stay behind the sandbox singleton / gateway-router path. Public browser and CLI clients use `loom-service` under `/api/v1`. |
| `loom-control-plane` | **No** — port-forward for operator-side admin curls | Worker claim path + admin token issuance must not be reachable from the public Internet. |
| `loom-worker` | **No** | Worker pods talk only to control-plane via cluster DNS. |
| `postgres`, `minio` | **No** | State + object store sit behind `loom_service` signed-URL routes. |
| `loom-egress-proxy`, `loom-egress-xds` | **No** | Egress validators; only reachable from `loom-llm-gateway`. |
| `loom-gateway-router` | **No** Ingress (binds `hostPort 30443` on each node for sandbox-Docker → gateway routing) | NOT for public Internet — sandbox bridges dial `<node>:30443` from inside the cluster network. Auditor allows this hostPort by exception when shipped from the canonical template. |

Out-of-cluster remote workers are a separate private-network integration, not
public Ingress. When a staging deployment uses them, operators expose only the
worker-facing Control Plane, Gateway, and MinIO endpoints on trusted LAN/VPN
addresses with `scripts/ops/worker_service_tunnels.py` systemd units, keep the
tunnel watchdog timer active, and verify those URLs from the worker hosts after
every rollout.

Docker-backed worker nodes must also run with an open-file limit high enough
for concurrent sandbox/container cleanup. Compose-based dev and remote-worker
deployments set `nofile=65536`; Kubernetes deployments should set an
equivalent node/container-runtime limit before raising worker concurrency.

The public Ingress is TLS-first. `cluster-config.toml` controls
`ingress_host`, `ingress_class_name`, `ingress_tls_secret_name`, and optional
`ingress_cert_manager_cluster_issuer`. With cert-manager, the rendered Ingress
gets a `cert-manager.io/cluster-issuer` annotation; without cert-manager,
operators pre-create the named TLS Secret.
Production-like deployments should use the committed environment-specific
render inputs under `deploy/environments/` so `development`, `staging`, and
`production` keep separate namespaces, public hosts, object buckets, worker
tokens, provider namespaces, SecretStore keys, and database names.
When `ingress_host` is an IP literal for a lab or invite-only staging
entrypoint, the renderer omits `spec.rules[].host` and `tls.hosts` because the
Kubernetes API rejects IP literals in those fields. Operators must still
pre-create `ingress_tls_secret_name` with an IP SAN certificate so clients can
reach `https://<ip-address>`.

The audit checks:

1. **Service type.** Every `Service` must be `ClusterIP`. `LoadBalancer` or `NodePort` would publish a pod to external traffic; the public surface is supposed to flow through the shared Ingress.
2. **Ingress TLS and paths.** Every Ingress must declare TLS. `loom-service` may appear only at `/api/v1`; `loom-web` may appear only at `/`; `defaultBackend` is rejected because it hides catch-all routing.
3. **Ingress backends.** Only `loom-service` and `loom-web` are allowed. Control Plane, LLM Gateway, Postgres, MinIO, worker, egress, worker-token admin, and batch-runner bootstrap surfaces must not be reachable from public Ingress.
4. **`hostPort` declarations.** Any container port that binds to the node interface is flagged unless it is the explicitly allowlisted `loom-gateway-router` path used by sandbox-Docker → gateway routing.
5. **NetworkPolicy coverage.** Required Loom components must have a matching NetworkPolicy selector so Kubernetes does not fall back to namespace default allow-all.

Operators run the audit ad-hoc with `loom cluster audit [--config cluster-config.toml]`; it exits 0 on clean, 1 on any violation. CI runs it on every PR touching the cluster CLI or templates via the `cluster-smoke` workflow.

## CLI surface

Cluster commands use the optional Kubernetes client dependency so normal laptop
or library users do not pay for it:

```bash
pip install "loom[cluster]"
# or, in a uv-managed checkout:
uv sync --extra cluster
```

### Read-only status inspector

Implemented Phase 1A:

```bash
loom cluster status [--context NAME] [--namespace NS] [--format table|json]
```

`status` reads the target kube context and reports readiness for the component
map above:

- Deployments: `loom-service`, `loom-control-plane`, `loom-llm-gateway`,
  `loom-web`, `loom-worker`.
- StatefulSets: `postgres`, `minio`.
- Ingress host/path/TLS visibility.
- Warning when the `loom-secrets` Secret is absent.
- Deployment rollout convergence: `observedGeneration >= generation` and
  `updatedReplicas >= spec.replicas`.
- Managed Deployment pod health: selected pods in blocking CrashLoop, image
  pull, config, start, OOM, or failed states keep the component not-ready even
  when old ready pods still satisfy Deployment-level ready counts. Failure to
  inspect pods is also not-ready; the rollout gate does not pass open when this
  required query fails.
- Visible kube-system rollout-controller failures for `kube-apiserver`,
  `kube-controller-manager`, `kube-scheduler`, or `etcd`.

Exit codes:

| Code | Meaning |
|---|---|
| `0` | Cluster is reachable and every expected component is ready, all Deployment generations are observed, updated replicas have converged, selected managed pods are inspectable and have no blocking failure states, and visible kube-system rollout-controller pods are healthy. |
| `1` | Cluster is reachable but at least one expected component is not ready. |
| `2` | Cluster is unreachable, kubeconfig/context is invalid, or the optional `kubernetes` package is not installed. |

A `0/0` workload, including the default paused `loom-web`, is reported as
not-ready instead of ready. Missing workloads are shown as `not-found` rows so
operators see which deployment slice has not been applied yet.

### Planned deployment verbs

```
loom service {up,down,status,logs}                               # single-box (compose; existing)
loom cluster {render,preflight,up,down} --nodes HOSTFILE
    [--control-node HOST] [--kubeconfig PATH] [--namespace NS]
    [--storage embedded|external]
    [--postgres-url URL] [--s3-endpoint URL] [--s3-bucket NAME]   # required if --storage external
    [--backup-target s3://...]                                    # required to enable backup CronJob
    [--registry URL]                                               # image registry override
    # ingress_host, ingress_class_name, ingress_tls_secret_name, and
    # ingress_cert_manager_cluster_issuer live in cluster-config.toml.
    [--co-locate-workers-on-control]                              # opt-in; control node tainted by default
    [--etcd-encryption-attested | --allow-plaintext-etcd]         # one required
    [--worker-concurrency N] [--sandbox-cidr CIDR]
    [--gateway-host-port 30443]
    [--preflight-via job|ssh] [--skip-namespace-bootstrap]
```

### User-facing CLI

`loom auth` / `loom providers` / `loom eval` talk to any deployed Loom (cluster or single-box, after `loom auth login`):

```
loom auth login --server URL --username USER --password {env:VAR | file:PATH | -}
loom auth login --server URL --token {env:VAR | file:PATH | -}    # user-owned API token for automation
loom auth {status,whoami,logout}

loom providers create --name N --type {openai-compatible,anthropic,google,custom} \
    --base-url URL --api-key {env:VAR | file:PATH | -}
    [--allowed-models LIST]
    [--input-usd-per-1m FLOAT --output-usd-per-1m FLOAT]          # both or neither
loom providers {list,show,test} NAME
loom providers models NAME [--refresh] [--preflight MODEL] [--hide MODEL] [--unhide MODEL]
loom providers update NAME [--base-url URL] [--api-key SOURCE] ...
loom providers delete NAME                                         # soft-delete

loom eval run --provider N --model M --agent A --task ID
loom eval batch create --agent oracle [--name N | --name-suffix S]
    [--benchmark B | --task-filter JSON]
loom eval batch create --provider N --model M --agent A
    [--name N | --name-suffix S] [--benchmark B | --task-filter JSON]
    [--n-per-task N] [--backend B] [--team-id UUID]
loom eval batch {list,show,cancel}
loom eval usage --start YYYY-MM-DD --end YYYY-MM-DD [--group-by day|week|month] [--include-batches]
    [--team-id UUID] [--user-id UUID] [--provider-connection-id UUID]
    [--model MODEL] [--benchmark-id ID] [--batch-id UUID]
    [--status STATE] [--pricing-mode priced|tokens-only|price-unknown|failed-upstream]
    [--breakdown-by team|user|provider_connection|model|benchmark|batch|status|pricing_mode]
loom admin rate-cards sync-yibuapi [--group GROUP] [--source-url URL]
loom eval trial {list,show}
loom eval trial show TRIAL_ID
loom eval trial download TRIAL_ID --kind atif --output atif.json
loom eval trial download TRIAL_ID --kind trajectory --output events.jsonl
loom eval trial download TRIAL_ID --kind artifact --artifact-key KEY --output artifact.bin
```

Argv hygiene: every secret-bearing flag accepts only `env:VAR`, `file:PATH`, or `-` (stdin). Literals are rejected at argparse-time.
CLI text/JSON output redacts raw bearer tokens, provider keys, internal service
hostnames, and raw signed object-store URLs.

`loom run` (existing, local-stateless) stays. The distinction: `loom run` runs one trial on your machine with no server; `loom eval` submits to a deployed Loom (batches, persistence, sharing).

## Schema additions

Migration `0018_provider_connections.py` (down_revision `"0017"`) adds three tables in the `loom` schema:

**`secrets`** (backs the `local-encrypted` SecretStore):

```
ref                   text PK            -- "loom://<namespace>/<uuid>"
ciphertext            bytea NOT NULL
nonce                 bytea NOT NULL     -- 12-byte AES-GCM nonce
master_key_version    int NOT NULL       -- bumped on rotation
created_at            timestamptz NOT NULL DEFAULT now()
```

**`provider_connections`** (team-scoped, soft-deleted):

```
id                          UUID PK
team_id                     UUID FK → teams.id  ON DELETE RESTRICT
provider_type               text       -- 'openai-compatible' | 'anthropic' | 'google' | 'custom'
display_name                text       -- UNIQUE per (team_id, display_name) WHERE deleted_at IS NULL
base_url                    text
upstream_host               text       -- hostname derived from base_url; re-derived on PATCH
resolved_egress_ips         inet[]     -- populated by background re-resolver
egress_ips_min_ttl_seconds  int NOT NULL DEFAULT 300
encrypted_api_key_ref       text       -- ref into `secrets` OR "k8s://ns/name"
allowed_models              text[] | NULL
status                      text       -- 'pending' | 'valid' | 'invalid' | 'disabled'
last_validated_at           timestamptz | NULL
last_validation_error       text | NULL
pricing_source              text       -- 'rate-card' | 'tokens-only' | 'operator-supplied'
pricing_data                jsonb | NULL
rate_card_provider          text | NULL -- rate-card namespace override for facade calls
deleted_at                  timestamptz | NULL
created_by                  text
created_at, updated_at      timestamptz
```

**`provider_models_cache`** (background-refreshed; 1-hour TTL on read):

```
provider_connection_id  UUID FK ON DELETE CASCADE
model_id                text       -- (PK with provider_connection_id)
family, context_length, capabilities, last_seen_at, visible, hidden_reason, upstream_present
last_preflight_status   text | NULL -- NULL | 'valid' | 'failed'
last_preflight_at       timestamptz | NULL
last_preflight_http_status int | NULL
last_preflight_error_code text | NULL
last_preflight_error_message text | NULL -- redacted, user-facing
```

`capabilities.source` is `"discovered"` for upstream `/models` entries
and `"manual"` for user-entered model ids. Manual rows stay visible even
when a later refresh does not return the id, which keeps self-hosted
OpenAI-compatible endpoints usable when discovery is absent or noisy.
Model discovery and entitlement are separate: `--refresh` only records
advertised ids, while `--preflight MODEL` sends one minimal generation
request and updates the nullable `last_preflight_*` fields. A known failed
preflight warns in the SPA and blocks new batch creation for that exact
provider/model pair; untested rows remain selectable.

`GET /api/v1/models` returns a launch-safe catalog by default. It
includes legacy rate-card tuples plus team-visible provider-connection
cache rows tagged with `source`, `agent_capable`, `recommended`,
`visibility`, `hidden_reason`, `provider_connection_id`, freshness metadata,
and preflight status/error metadata. `view=raw` includes suppressed tool/API
entries such as
Amap/APISports/TuShare-style ids with classifier reasons for debugging.

`Trial` and `Batch` payloads gain `provider_connection_id` + `provider_model_id` (both nullable for legacy/local or non-model-backed paths). In v1.0 service-mode submissions, model-backed agents should use a team-owned provider connection rather than an implied platform-hosted model provider. Trial FK has no cascade — soft-delete keeps audit/billing references valid. Batch fan-out forwards the batch-level provider fields to every materialized trial; per-combination provider connections are intentionally not part of this schema slice.

Benchmark task images remain model/provider/agent agnostic. The task image owns
benchmark dependencies, task assets, harness code, and verifier behavior only.
User choices for agent, provider, and model live in the submitted run/trial
payload (`trial_config.agent_name`, `trial_config.agent_model`,
`provider_connection_id`, `provider_model_id`) and are injected by the service,
worker, and sandbox launch boundary at execution time. Agent runtime bits may
come from the worker image, a cached layered sandbox image, or an install
script, but changing a selected model/provider/agent must not require
republishing a benchmark task image.
When `trial_config.agent_model.max_output_tokens` is set, the worker gateway
client forwards it to the LLM Gateway as the provider `max_tokens` request
limit; operators can use this for long-tail acceptance reruns without changing
the benchmark bundle.

## Secrets, SSRF, gateway hot path

`SecretStore` Protocol in `src/loom/security/secret_store.py`:

```python
class SecretStore(Protocol):
    async def put(self, *, namespace: str, key: str, value: str) -> str: ...
    async def get(self, ref: str) -> str: ...
    async def delete(self, ref: str) -> None: ...
    async def list_refs(self, *, namespace: str | None = None) -> AsyncIterator[str]: ...
    async def rewrap(self, ref: str, *, new_master_key: bytes) -> str: ...
```

Two impls ship: `local-encrypted` (AES-GCM, `LOOM_SECRET_STORE_MASTER_KEY`; ciphertext in `secrets` table) for user-API-key data path, and `k8s-secret` (one k8s Secret per ref) for bootstrap-supplied infra credentials. Both impls live in both deployment modes. In cluster mode, `loom-service` and `loom-llm-gateway` read `LOOM_SECRET_STORE_MASTER_KEY` from `loom-secrets/secret-store-master-key`; `loom cluster bootstrap-secrets --rotate` generates this key for new clusters.

`loom-service` and `loom-llm-gateway` also perform a startup validation
pass over existing `secrets` refs. Empty stores start without loading a
master key; non-empty stores must decrypt with the configured
`LOOM_SECRET_STORE_MASTER_KEY` or `LOOM_SECRET_STORE_MASTER_KEYS`
fallback set. A mismatch fails startup with an operator-facing
SecretStore validation error rather than letting provider traffic hit an
AEAD decrypt failure on the first model request. These startup DB probes
use bounded retry for transient DNS, connection, or Postgres-starting
failures so cluster sandbox/CoreDNS churn does not turn a recoverable
dependency blip into CrashLoop evidence. The retry boundary is intentionally
narrow: Alembic revision mismatch, missing migrations, bad credentials, and
SecretStore decrypt failures remain immediate hard failures. Worker startup
uses the same retry boundary for its initial Control Plane registration; 4xx
registration failures such as bad worker tokens are not retried.

### Provider egress contract

Provider `base_url` values may use `https://` or `http://`. The xDS
snapshot derives the upstream scheme and port from that URL: explicit
ports are preserved, while HTTPS defaults to `443` and HTTP defaults
to `80`. HTTPS providers route through Envoy CONNECT matches on
`(x-loom-connection-id, :authority=<upstream_host>:<port>)`; HTTP
providers route as normal forward-proxy requests with the same
connection-id + authority pair and the internal connection header is
removed before the request is forwarded upstream.

This means private on-prem OpenAI-compatible endpoints such as
`http://192.168.32.1:28001/v1` are routable through egress when the
team/operator has enabled `allow_private_endpoints`, while public
HTTPS providers keep the existing CONNECT path.

### Re-resolver

A singleton Postgres-advisory-lock-protected task on whichever gateway replica acquires `pg_try_advisory_lock(hashtext('egress_resolver_v1'))`. Resolves `upstream_host` for every connection every `max(observed_dns_ttl, egress_ips_min_ttl_seconds)`; writes the union of (current ∪ newly resolved) to `resolved_egress_ips`, dropping IPs older than `egress_ip_window_hours` (default 24). Hard-capped at 256 IPs per connection (FIFO eviction by `last_seen`). NOTIFY + 30 s polling fallback distributes updates to consumers.

### Cache + config update durability

Cache key includes `provider_connections.updated_at`; every gateway call does a cheap indexed `SELECT id, updated_at FROM provider_connections WHERE id = $1` and re-fetches on mismatch. Stale-cache is impossible regardless of pubsub reliability; cost is one indexed PK lookup per call (~100 µs p99).

Rewrap during master-key rotation bumps `provider_connections.updated_at` for every affected connection in the same transaction; otherwise post-rotation caches stay valid but their decrypted plaintext can't be re-encrypted in the same key version.

### Cost computation

| `pricing_source` | Behavior |
|---|---|
| `rate-card` | Look up `(rate_card_provider, model_id)` for facade-routed calls, falling back to safe type defaults (`anthropic`, `google`, `openai` for `openai-compatible`). For YibuAPI, sync the official catalog with `loom admin rate-cards sync-yibuapi` and set `rate_card_provider=yibuapi`. Missing entries record `cost_usd=0` with a missing-rate-card marker and surface as `cost_status=price_unknown`. |
| `tokens-only` | Record tokens; `cost_usd = 0`. Default for `openai-compatible` and `custom`; use this for user-managed/self-deployed APIs so API and CLI views return token totals with `cost_status=not_applicable`. |
| `operator-supplied` | Use `pricing_data.{input_usd_per_1m, output_usd_per_1m}`; route-validates non-null + non-negative. |

Provider usage completeness is tracked separately from pricing. Facade calls
that receive missing or partial provider usage blocks still write an
`llm_calls` row, but mark `provider_extras._loom_usage_status` as `missing` or
`partial`; `/api/v1/trials`, `/api/v1/batches`, and `/api/v1/usage` surface the
corresponding incomplete-usage counts and `usage_estimate_confidence`.

## Worker concurrency

One `loom-worker` Deployment replica runs one worker process. That
process handles N concurrent trials via the existing asyncio Semaphore,
gated by `LOOM_WORKER_MAX_CONCURRENT`. In Kubernetes render output this
comes from `[worker_capacity].max_concurrent` in `cluster-config.toml`;
the default render is three worker replicas at 16 trials each, or 48
in-cluster slots before remote workers are attached. Rendered Kubernetes
workers advertise `LOOM_WORKER_POOL_NAME=k8s-worker` so Monitor,
`loom resources status`, and slot metrics group the baseline separately from
remote GB10/OLDLAB pools. The service-mode
worker runtime default remains conservative for non-rendered local
processes, so production operators should use cluster render config
rather than hand-patching the Deployment env block. The old
top-level `cluster-config.toml` `worker_max_concurrent` field remains
removed.

Example production capacity override:

```toml
[replicas]
worker = 8

[worker_capacity]
max_concurrent = 32
cpu_request = "4"
cpu_limit = "32"
memory_request = "16Gi"
memory_limit = "128Gi"
```

Workers also size their Python blocking-I/O executor from concurrency as
`max(32, min(LOOM_WORKER_MAX_CONCURRENT * 4, 256))` unless
`LOOM_WORKER_BLOCKING_IO_MAX_WORKERS` is explicitly set. This executor
covers blocking Docker, S3/MinIO, Hugging Face, and filesystem calls and
must not be counted as extra trial capacity.

Layered trial-cache image builds are gated separately by
`LOOM_WORKER_TRIAL_CACHE_BUILD_MAX_CONCURRENT` (default `1`). The rendered
k8s Deployment keeps shared-node Docker builds serialized even when
`LOOM_WORKER_MAX_CONCURRENT` is higher, because different cold cache keys can
still contend on the same host Docker/containerd path.

Docker SDK and S3 object-store behavior are separately tunable. The worker
passes `LOOM_WORKER_DOCKER_API_TIMEOUT_SEC` into every worker-created
docker-py client so large image pulls, task Dockerfile builds, and task
sidecars do not fail at docker-py's default read timeout. It also passes
`LOOM_WORKER_MINIO_MAX_POOL_CONNECTIONS`, `LOOM_WORKER_MINIO_CONNECT_TIMEOUT_SEC`,
`LOOM_WORKER_MINIO_READ_TIMEOUT_SEC`,
`LOOM_WORKER_MINIO_OPERATION_TIMEOUT_SEC`, and
`LOOM_WORKER_MINIO_OPERATION_ATTEMPTS` into the worker object store used for
task materialization, artifacts, and trajectory upload. S3 prefix
materialization retries prefix listing and each object download separately, so
a transient disconnect on one task-bundle object does not restart the whole
prefix. Pre-start task bundle materialization is also bounded by
`LOOM_WORKER_TASK_MATERIALIZE_TIMEOUT_SEC` (default 300 seconds); when it
expires, the worker writes a redacted setup failure back to the control plane
instead of leaving the trial indefinitely `claimed`.

Private or gated `hf://` benchmark sources use the standard `HF_TOKEN`
environment variable read by `huggingface_hub`. The default k8s render wires
`HF_TOKEN` from the optional `loom-secrets/huggingface-api-key` key, so public
deployments without gated HF sources continue to boot without that key.

Use the CPU/memory formula as an upper-bound planning heuristic, not as
the default:

```
LOOM_WORKER_MAX_CONCURRENT <= min(cpu_cores // 2, memory_gb // 8, 32)
```

One worker process per worker host is preferred for remote pools because
one process owns docker.sock cleanly, intra-process file locks are
simpler than cross-process flock dances, and DRF claim is per-worker-id.
For OLDLAB-style pools, inventory every candidate node, attach every
usable node unless it has a recorded exclusion reason, and tune each
node from measured CPU, RAM, Docker cleanup, object-store, gateway, and
control-plane pressure. The staged OLDLAB plan lives in
`deploy/worker-pools/oldlab/` and starts with `12 CPU / 58000M /
concurrency=6` per node. Do not treat either that conservative slice or an old
four-node QA result as the permanent production ceiling.

## Multi-tenancy boundaries

| Boundary | Mechanism |
|---|---|
| Provider connections | `team_id` FK; routes filter on `ctx.team_id`; cross-team access returns 404. |
| Trial / batch ownership | `team_id` on rows; cross-team reads 404. |
| Sandbox LLM egress | Egress proxy enforces per-connection allowlist via `X-Loom-Connection-Id` from gateway. |
| Sandbox non-LLM egress | Per-trial `--internal` bridge: only the singleton is reachable. |
| Per-team Postgres tenancy | Shared schema with `team_id` columns + row-level checks. Not separate schemas. |
| Per-team k8s namespace | NOT shipped — single `loom` namespace, multi-team enforced at row level. |
| Per-team MinIO bucket | NOT shipped — shared buckets prefixed by `team_id`; cross-team blocked at API layer, not at MinIO ACL. |

Threat model: trusted users, untrusted prompts. Not suitable for hostile-tenant SaaS.

## Upgrade path

`loom service` → `loom cluster` migration:

```bash
# On the single-box install:
loom admin export --out /tmp/loom-export.tar.gz.age --passphrase {env:VAR | file:PATH | -}
    # pg_dump + mc mirror + age-encrypted master-key wrapping

# On the new control node:
loom cluster up --nodes hostfile --import /tmp/loom-export.tar.gz.age \
    --import-passphrase {env:VAR | file:PATH | -}
```

Same encrypted-tarball format as the backup CronJob's output, so DR and migration share a code path. Passphrase is operator-owned; loss = loss of the encrypted bundle.

For the current public-beta/staging first-phase durability guard, operators
create component backups first, then write a metadata-only manifest:

```bash
loom cluster backup manifest \
  --environment public-beta \
  --namespace loom-public-beta \
  --postgres-dump /data/loom-public-beta/backups/20260629T120000Z/postgres/loom.dump \
  --minio-snapshot /data/loom-public-beta/backups/20260629T120000Z/minio \
  --k8s-secrets /data/loom-public-beta/backups/20260629T120000Z/secrets \
  --output /data/loom-public-beta/backups/20260629T120000Z/backup-manifest.json

loom cluster preflight \
  --environment public-beta \
  --namespace loom-public-beta \
  --config cluster-config.toml \
  --backup-manifest /data/loom-public-beta/backups/20260629T120000Z/backup-manifest.json
```

`loom cluster up` accepts the same environment and backup-manifest flags and
threads them through preflight before apply. Pass the same `--config` so the
preflight and rendered manifest prove the same storage boundary and target
schema surface. Protected rollouts should first run `loom cluster
release-manifest` against that exact config and image tag, storing the artifact
beside the rendered YAML. The artifact records the expected git SHA, image tag,
CLI version, rendered Deployment images, cluster-config/rendered-manifest
hashes, Alembic heads, and external-worker desired-state fingerprints before
any apply starts. After readiness passes, `up` compares rendered Deployment
container images with the live Deployment specs and fails if a concurrent
operator mutation drifted the live image away from the release manifest. On
success, `up` has also rejected managed Deployment pods stuck in blocking
CrashLoop/image/config/start/OOM/failed states, then prints the rendered and
live image for each managed Deployment/container so the rollout log captures
image-convergence evidence.
`loom cluster down --with-volumes` and `--delete-namespace` refuse protected environments
unless the manifest is recent and the operator passes `--acknowledge-data-loss
<environment>`. This guard distinguishes ordinary pod or service restarts from
destructive state removal.

## Common pitfalls

- **`docker cp` into a tmpfs mount of a running container is a no-op silently.** Use bind-mount + host-side write + atomic rename for any file the container needs to re-read. ([Spike 04](cluster-deploy-spikes/04-jwt-fsnotify-rotation.sh) verifies the correct mechanism and pins the bug as a negative assertion.)
- **`docker network connect` to a k8s pod doesn't work** on containerd-based clusters. The kubelet owns the pod's net namespace. For per-trial Docker bridge join, use a worker-spawned Docker container (the singleton model), not a k8s pod.
- **`--network host` containers cannot be attached to additional bridges.** Docker rejects the combination. The singleton uses two bridges (`loom-uplink` + per-trial `--internal`), neither of which is host network.
- **`enable_icc=false` on a Docker bridge blocks ALL container-to-container traffic**, including sandbox → gateway. The flag is not a partial-isolation tool; either use per-trial bridges or accept shared-bridge ICC.
- **`hostPort` reachability from Docker bridges depends on CNI portmap behavior**, NOT a universal guarantee. `loom cluster up` ships a preflight TCP-connect probe ([spike 03](cluster-deploy-spikes/03-hostport-from-bridge.sh)).
- **Preflight `hostPath` must mount `/var/run` (directory), not `/var/run/docker.sock` directly.** Direct socket mount fails the pod when Docker is missing; the parent-directory mount lets the container `test -S` for the actual check. ([Spike 02](cluster-deploy-spikes/02-preflight-hostpath.sh).)
- **PSS Restricted blocks runtime `setgid` calls.** Use `securityContext.supplementalGroups` (templated at deploy time from preflight-discovered docker.sock gid), not in-process gid switching.
- **Worker pod's `docker run -v <host>:<container>` resolves the host path against the actual host filesystem**, not the worker pod's mount tree. The worker must hostPath-mount the same paths it later passes to `docker run`.

## See also

- [cluster-deploy-spikes/](cluster-deploy-spikes/README.md) — executable proofs of the load-bearing mechanisms (per-trial bridges, preflight hostPath, hostPort routing, JWT refresh, TLS round-trip). CI gate; new mechanisms must add a spike.
- carinrc#49 (historical archive) — production cluster + user-supplied provider gateway requirements.
- carinrc#50 (historical archive PR) — implementation plan (phases, considered alternatives, risks, design changelog).
- [service-mode.md](service-mode.md) — single-host architecture today.
- [cli-mode.md](cli-mode.md) — local-stateless `loom run`.
- [drf-scheduling.md](drf-scheduling.md) — how the claim path matches workers to trials.
