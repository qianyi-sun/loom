# Cluster deployment

**Status: design with first CLI slice** — this is the target architecture for
`loom cluster`. The read-only `loom cluster status` inspector is implemented as
Phase 1A of [#76](https://github.com/carinrc/loom/issues/76); render, preflight,
up, and down remain planned follow-up slices. See
[PR #50](https://github.com/carinrc/loom/pull/50) for the implementation plan.

`loom cluster` is the multi-node deployment mode. A control node runs the API services + storage; worker nodes spawn trial sandboxes via `docker.sock` onto per-trial Docker bridges. Users supply OpenAI-compatible model endpoints via the per-team `provider_connections` API; `loom-llm-gateway` mediates every LLM call.

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

Storage is an orthogonal flag: `--storage embedded` (in-cluster Postgres + MinIO; the default) or `--storage external` (managed Postgres + S3). External is the only HA path; embedded is intentionally simple.

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
                   │ HTTPS CONNECT (target_ip, 443)
                   │   X-Loom-Connection-Id, X-Loom-Team-Id
                   ▼
┌─ loom-egress-proxy (Envoy, ≥2 replicas) ──────────────┐   second wall: even if gateway
│  validates target_ip ∈ resolved_egress_ips            │   compromised, only operator-
│  [connection_id]; per-team local_ratelimit            │   resolved IPs are dialed.
└──────────────────┬─────────────────────────────────────┘
                   ▼
            OpenAI / Anthropic / Google
```

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

`POST /provider-connections` rejects RFC1918 / loopback / link-local / ULA by default; `team.allow_private_endpoints=true` permits RFC1918 + loopback for on-prem providers (defaulted on in `loom service` single-box mode).

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
| `loom-llm-gateway` | Deployment (≥2 replicas) | `loom` namespace | Provider-facade routes (`/openai/v1/...`, `/anthropic/v1/...`); resolves `provider_connection_id`; decrypts API key; forwards via egress proxy. |
| `loom-egress-proxy` | Deployment (≥2 replicas, Envoy) | `loom` namespace | Validates `target_ip ∈ resolved_egress_ips[connection_id]`; per-team rate limit; HTTPS CONNECT to provider. |
| `loom-egress-xds` | Deployment (1 replica, Python control plane) | `loom` namespace | Reads `provider_connections` from Postgres; serves Envoy CDS/EDS via gRPC. Vendored from `envoyproxy/python-control-plane`. |
| `loom-service` | Deployment (≥2 replicas) | `loom` namespace | REST API; `POST /trials`, `POST /batches`, `POST /provider-connections`, etc. |
| `loom-control-plane` | Deployment (≥2 replicas) | `loom` namespace | Schedules trials; mints step JWTs; serves Worker claim path. |
| `loom-web` | Deployment (replicas: 0 by default) | `loom` namespace | SPA, paused; operator scales up when work resumes. |
| `postgres`, `minio` | StatefulSet (1 replica each, control node) | `loom` namespace | State + object store. `--storage external` substitutes managed equivalents. |

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
  `loom-web`.
- DaemonSet: `loom-worker`.
- StatefulSets: `postgres`, `minio`.
- Ingress host/path/TLS visibility.
- Warning when the `loom-secrets` Secret is absent.

Exit codes:

| Code | Meaning |
|---|---|
| `0` | Cluster is reachable and every expected component is ready. |
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
    [--registry URL] [--ingress nginx|none] [--tls-cert PATH --tls-key PATH]
    [--co-locate-workers-on-control]                              # opt-in; control node tainted by default
    [--etcd-encryption-attested | --allow-plaintext-etcd]         # one required
    [--worker-concurrency N] [--sandbox-cidr CIDR]
    [--gateway-host-port 30443]
    [--preflight-via job|ssh] [--skip-namespace-bootstrap]
```

### User-facing CLI

`loom auth` / `loom providers` / `loom eval` talk to any deployed Loom (cluster or single-box, after `loom auth login`):

```
loom auth login --server URL --token {env:VAR | file:PATH | -}    # --token required; no interactive paste
loom auth {status,logout}

loom providers create --name N --type {openai-compatible,anthropic,google,custom} \
    --base-url URL --api-key {env:VAR | file:PATH | -}
    [--allowed-models LIST]
    [--input-usd-per-1m FLOAT --output-usd-per-1m FLOAT]          # both or neither
loom providers {list,show,test} NAME
loom providers models NAME [--refresh] [--hide MODEL] [--unhide MODEL]
loom providers update NAME [--base-url URL] [--api-key SOURCE] ...
loom providers delete NAME                                         # soft-delete

loom eval run --provider N --model M --agent A --benchmark B
    [--task ID | --task-filter JSON] [--backend B] [--name N]
loom eval batch create --provider N --model M --agent A --benchmark B
    [--combinations FILE | --task-filter JSON] [--concurrency N] [--name N]
loom eval batch {list,show,cancel}
loom eval trial {list,show} | trajectory ID | atif ID
```

Argv hygiene: every secret-bearing flag accepts only `env:VAR`, `file:PATH`, or `-` (stdin). Literals are rejected at argparse-time.

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
upstream_host               text       -- derived from base_url; re-derived on PATCH
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
```

`capabilities.source` is `"discovered"` for upstream `/models` entries
and `"manual"` for user-entered model ids. Manual rows stay visible even
when a later refresh does not return the id, which keeps self-hosted
OpenAI-compatible endpoints usable when discovery is absent or noisy.

`GET /api/v1/models` returns a launch-safe catalog by default. It
includes legacy rate-card tuples plus team-visible provider-connection
cache rows tagged with `source`, `agent_capable`, `recommended`,
`visibility`, `hidden_reason`, `provider_connection_id`, and freshness
metadata. `view=raw` includes suppressed tool/API entries such as
Amap/APISports/TuShare-style ids with classifier reasons for debugging.

`Trial` and `Batch` payloads gain `provider_connection_id` + `provider_model_id` (both nullable; null = use platform-default provider). Trial FK has no cascade — soft-delete keeps audit/billing references valid. Batch fan-out forwards the batch-level provider fields to every materialized trial; per-combination provider connections are intentionally not part of this schema slice.

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

Two impls ship: `local-encrypted` (AES-GCM, `LOOM_SECRET_STORE_MASTER_KEY`; ciphertext in `secrets` table) for user-API-key data path, and `k8s-secret` (one k8s Secret per ref) for bootstrap-supplied infra credentials. Both impls live in both deployment modes.

### Re-resolver

A singleton Postgres-advisory-lock-protected task on whichever gateway replica acquires `pg_try_advisory_lock(hashtext('egress_resolver_v1'))`. Resolves `upstream_host` for every connection every `max(observed_dns_ttl, egress_ips_min_ttl_seconds)`; writes the union of (current ∪ newly resolved) to `resolved_egress_ips`, dropping IPs older than `egress_ip_window_hours` (default 24). Hard-capped at 256 IPs per connection (FIFO eviction by `last_seen`). NOTIFY + 30 s polling fallback distributes updates to consumers.

### Cache + config update durability

Cache key includes `provider_connections.updated_at`; every gateway call does a cheap indexed `SELECT id, updated_at FROM provider_connections WHERE id = $1` and re-fetches on mismatch. Stale-cache is impossible regardless of pubsub reliability; cost is one indexed PK lookup per call (~100 µs p99).

Rewrap during master-key rotation bumps `provider_connections.updated_at` for every affected connection in the same transaction; otherwise post-rotation caches stay valid but their decrypted plaintext can't be re-encrypted in the same key version.

### Cost computation

| `pricing_source` | Behavior |
|---|---|
| `rate-card` | Look up `(rate_card_provider, model_id)` for facade-routed calls, falling back to safe type defaults (`anthropic`, `google`, `openai` for `openai-compatible`). Missing entries record `cost_usd=0` with a missing-rate-card marker. |
| `tokens-only` | Record tokens; `cost_usd = 0`. Default for `openai-compatible` and `custom`. |
| `operator-supplied` | Use `pricing_data.{input_usd_per_1m, output_usd_per_1m}`; route-validates non-null + non-negative. |

## Worker concurrency

One `loom-worker` DaemonSet pod per node. The process handles N concurrent trials via the existing asyncio Semaphore, gated by `LOOM_WORKER_CONCURRENCY`:

```
LOOM_WORKER_CONCURRENCY = min(cpu_cores // 2, memory_gb // 8, 32)
```

One DaemonSet pod (not N replicas) because one process owns docker.sock cleanly, intra-process file locks are simpler than cross-process flock dances, and DRF claim is per-worker-id. The pod advertises `workers.capabilities.max_concurrent = LOOM_WORKER_CONCURRENCY`.

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
- [#49](https://github.com/carinrc/loom/issues/49) — production cluster + user-supplied provider gateway requirements.
- [#50](https://github.com/carinrc/loom/pull/50) — implementation plan (phases, considered alternatives, risks, design changelog).
- [service-mode.md](service-mode.md) — single-host architecture today.
- [cli-mode.md](cli-mode.md) — local-stateless `loom run`.
- [drf-scheduling.md](drf-scheduling.md) — how the claim path matches workers to trials.
