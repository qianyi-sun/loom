# Multi-node topology

**Status:** design
**Issues:** #637 (Postgres HA), #610 (MinIO distributed), #641 (topology schema), #642 (HA templates)

## Motivation

Loom's platform tier — Postgres, MinIO, LLM gateway, control-plane, service, pgbouncer — runs today as a single-node kind cluster on `bb8-1`. That's fine for a pilot with ~50 concurrent trials but has two hard limits:

1. **Every stateful pod is a SPOF.** Postgres pod restart = ~150 in-flight trials fail. MinIO pod restart = trial finalize stalls until it recovers. Under continuous autoscaling to 100+ concurrent trials, restart events (kernel patches, image upgrades, node reboots) become user-visible incidents.
2. **hostPath storage doesn't survive node loss.** All Postgres and MinIO data lives on `bb8-1`'s local disk. If `bb8-1` fails, the platform is offline until manual restore.

The multi-node topology solves both by moving to a real five-node k8s cluster (k3s) with replicated storage (Longhorn) and operator-managed HA services (CloudNativePG for Postgres, distributed MinIO). Pgbouncer already runs 2 replicas so no change there.

## The seam: `topology.multi_node`

Introduced in #641 (schema) and #642 (templates). Single knob that cascades to every stateful shape:

```toml
[render_config.topology]
fields = {
    multi_node        = false,
    storage_backend   = "host_path",     # or "longhorn"
    postgres_replicas = 1,               # or 3 under multi_node
    minio_replicas    = 1,               # or 4 under multi_node
    min_available     = 1,
    anti_affinity     = "preferred",     # or "required"
}
```

Profiles pin explicit values so a schema-default flip cannot silently break existing deployments:

- `development.cluster.toml`: pinned `multi_node = false` (shared dev is a lightweight env co-located with staging/prod on the fleet — single-node-shaped backends by choice, not distributed; trial execution runs on the shared external-Slurm workers, arbitrated per-env by the autoscaler)
- `staging.cluster.toml`: pinned `multi_node = false` **temporarily**; flipped `true` in the same PR that lands k3s cutover
- `production.cluster.toml`: pinned `multi_node = true`, `storage_backend = "longhorn"`, 3 Postgres replicas, 4 MinIO replicas, required anti-affinity

## Cluster shape

**Physical nodes (OLDLAB, x86_64):**

| Node | Role | Notes |
|---|---|---|
| bb8-1 (`trt-eai-oldlab-1`) | k3s server + Longhorn + Slurm worker | Control plane; holds the Loom repo checkouts |
| bb8-2 (`trt-eai-oldlab-2`) | k3s agent + Longhorn + Slurm worker | Storage + compute |
| bb8-3 (`trt-eai-oldlab-3`) | k3s agent + Longhorn + Slurm worker | Same |
| bb8-4 (`trt-eai-oldlab-4`) | k3s agent + Longhorn + Slurm worker | Same |
| bb8-5 (`trt-eai-oldlab-5`) | k3s agent + Longhorn + Slurm worker | Same |

All five OLDLAB nodes are `Ready` in the k3s cluster and Longhorn-schedulable.

GB10 (arm64, 15 nodes) stays worker-only under Slurm. Not part of the k3s cluster — Loom's workers connect back to the k8s cluster's services via network, they don't need to be k8s pods themselves. This keeps the k3s cluster homogeneous x86_64 and avoids arm64-specific storage/scheduling complications.

**Double-duty colocation.** All five nodes run the k3s control plane + MinIO/Longhorn storage **and** OLDLAB Slurm workers on the same hardware. The noisy-neighbor risk (Postgres/MinIO p99 vs worker image-build spikes) is bounded by non-exclusive Slurm workers with headroom + per-container `cpus`/`mem`/`pids` caps (#896) and Longhorn replica placement across nodes. Activation of packed (`exclusive=false`) workers stays gated on #896's container-isolation evidence — a worker container escaping its sbatch cgroup could otherwise contend with Longhorn/MinIO/k3s unbounded.

## Postgres HA via CloudNativePG

CNPG (postgresql.cnpg.io/v1) is a Kubernetes operator that manages Postgres clusters with streaming replication, automatic failover, and PDB enforcement. Chosen over alternatives:

- **Bitnami PostgreSQL Helm chart / Patroni**: older, requires an external etcd cluster for leader election. CNPG uses k8s API for coordination — one fewer moving part.
- **Zalando postgres-operator**: mature but heavier config surface; the Loom team has less familiarity with it.
- **Stolon**: less active development.
- **Managed cloud (RDS, CloudSQL)**: rejected in #609's decision matrix because it introduces cross-cluster latency and a monthly bill for on-prem hardware we already own.

Rendered shape (`src/loom_cli/templates/k8s/postgres-cnpg.yaml.j2`, fires when `topology.multi_node=true`):

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: loom-postgres
spec:
  instances: {{ topology.postgres_replicas }}          # 3 in production
  imageName: {{ postgres_image }}
  postgresql:
    parameters:
      max_connections: "{{ postgres.max_connections }}" # 150
  bootstrap:
    initdb: { database: loom, owner: loom, secret: {...} }
  storage:
    size: {{ postgres_storage_gi }}Gi
    storageClass: longhorn                              # from topology.storage_backend
  affinity:
    enablePodAntiAffinity: true
    podAntiAffinityType: required                       # from topology.anti_affinity
```

### The ExternalName aliasing trick

CNPG creates three Services per Cluster: `{name}-rw` (primary), `{name}-ro` (replicas only), `{name}-r` (any). But Loom's config expects `loom-postgres:5432` — pgbouncer's `POSTGRESQL_HOST` env var, Alembic's `LOOM_DB_URL` DSN, LISTEN watchers, and NetworkPolicy selectors all hardcode that name.

Rather than update ~10 places to know CNPG exists, the template renders a backwards-compatibility Service:

```yaml
apiVersion: v1
kind: Service
metadata: { name: loom-postgres }
spec:
  type: ExternalName
  externalName: loom-postgres-rw.{{ namespace }}.svc.cluster.local
  ports: [ { port: 5432, targetPort: 5432 } ]
```

DNS resolves `loom-postgres` → `loom-postgres-rw` at the cluster level. Every existing consumer keeps working with zero code change. Failover: CNPG updates the `loom-postgres-rw` endpoint pointer under the hood — the ExternalName record is stable.

### Credential handling

CNPG expects a Secret with `username` + `password` keys (Kubernetes basic-auth shape). Existing `loom-secrets` uses `postgres-user` + `postgres-password`. The runbook creates a `loom-postgres-cnpg-credentials` Secret at cluster stand-up time that mirrors the values. When rotating passwords, both Secrets must be updated together — no automatic sync.

## Distributed MinIO

Standard 4-pod distributed mode with 2-parity erasure coding. Rendered shape (`minio-distributed.yaml.j2`, fires on `multi_node=true`):

- **StatefulSet**: `topology.minio_replicas` pods (4 in production). `serviceName: loom-minio-headless` for peer discovery. Anti-affinity mirrors `topology.anti_affinity`.
- **Peer discovery**: `args: [server, http://loom-minio-{0...3}.loom-minio-headless.<ns>.svc.cluster.local:9000/data]`. Each pod resolves peers via the headless Service's DNS records.
- **Public Service**: `loom-minio` (unchanged from single-node) — clients (control-plane, service, workers) address it as before.
- **PodDisruptionBudget**: `minAvailable = topology.minio_replicas - 1`. With 4 pods and 2-parity erasure, MinIO tolerates one pod loss without downtime; the PDB prevents voluntary loss of two simultaneously (e.g., during node drain).

MinIO's distributed mode requires all pods to be running before write quorum is achieved. `podManagementPolicy: Parallel` avoids the default ordered startup that would block waiting for pod 0 to be Ready before starting pod 1.

## Storage backend switching

`topology.storage_backend` picks which storage class the templates request:

- `host_path` (single-node dev): `storageClassName: ""` + `volumeName: <pinned>`. Uses local disk with explicit PV pinning. Not portable across nodes.
- `longhorn` (multi-node production): `storageClassName: longhorn`. Longhorn provisions replicated block storage via its DaemonSet on each participating node. Volumes survive node loss up to the replica count (default 3, across the five-node cluster).

Longhorn install is a runbook step, not part of `loom cluster render` output.

## Cutover from single-node to multi-node

**Verified prerequisites — already in place, no action needed:**

- k3s cluster: five OLDLAB nodes `Ready` (`trt-eai-oldlab-1..5`), bb8-1 the server.
- CloudNativePG + Longhorn: installed and healthy; Longhorn schedulable on all five nodes.
- 4-pod distributed MinIO: rendered from `minio-distributed.yaml.j2` with `topology.multi_node=true / minio_replicas=4 / anti_affinity=required / storage_backend=longhorn` + `persistent_storage_backend=dynamic`, deployed to a scratch namespace, and confirmed to form write quorum with all four pods spread across four distinct nodes on Longhorn (then torn down; the cutover re-creates it with the real secret).

Live staging still runs on the single-node **kind** cluster on bb8-1 (`loom-staging` namespace, host-path MinIO PV `loom-staging-minio-data`, ingress on host `:443`). The k3s cluster is a **fully separate** API server fronted by nginx on host `:8443` with Longhorn (not host-path) storage — so k3s staging work cannot collide with the live kind cluster.

**Config change (belongs in the cutover PR, not merged early):** flip `deploy/environments/staging.cluster.toml` `[topology]` to `multi_node=true`, `storage_backend="longhorn"`, `minio_replicas=4`, add `anti_affinity="required"`, and set `persistent_storage_backend="dynamic"` (so the static host-path MinIO PV stops rendering and can't shadow the StatefulSet PVC). Merging this ahead of the window would render against the live kind cluster and strand it.

**Maintenance window (irreversible — data + DNS):**

1. Pause new trial submissions.
2. `pg_dump` from the kind single-node Postgres → `pg_restore` into the k3s CNPG primary (created by `loom cluster up` from the flipped config).
3. `mc mirror` from the kind single-node MinIO → the new 4-pod distributed pool.
4. `loom cluster up`/`apply` the flipped staging config to k3s.
5. Verify: pgbouncer reaches the CNPG primary via the `loom-postgres` ExternalName, LISTEN watchers reach direct Postgres, workers finalize trials to distributed MinIO, MinIO reports 4 pods online.
6. Repoint the public entrypoint from kind (`:443`) to k3s (`:8443`) and serve a **302 `/staging → /dev` redirect** on the old cluster. The `/dev` basename is baked into the SPA build's React-Router basename and loom-service `public_base_url`, so an ingress path-rewrite alone does NOT roll a client back — a redirect or DNS re-point does (#879).
7. Keep the kind single-node Postgres/MinIO PVs on bb8-1 for 24–72h as the rollback anchor.
8. Decommission the kind PVs after monitoring shows the k3s cluster stable.

**Rollback (any time before step 8):** re-point the entrypoint back to kind `:443` (and the `/staging → /dev` redirect back to the old cluster). No data loss — the kind Postgres/MinIO are untouched until step 8. A bare `topology.multi_node=false` re-render is NOT sufficient once DNS has moved; the entrypoint re-point is the real rollback lever.

## Failure modes and alerts

New Prometheus alerts (see `deploy/k8s/prometheus-rules.yaml`, groups `loom.pgbouncer.health` and `loom.minio.health`):

- **`LoomMinioWriteLatencyHigh`**: p95 write latency > 2s for 10m. Under single-node = disk saturation. Under distributed = erasure recovery.
- **`LoomMinioRequestErrorRateHigh`**: server-error rate > 1% for 5m. Trial finalize is lossy.
- **`LoomMinioNodeOffline`**: `minio_cluster_nodes_offline_total > 0`. Peer unreachable — erasure quorum still met but margin narrowed.
- **`LoomPgbouncerClientWaiting`**: backend pool saturated. Bump `pgbouncer.default_pool_size`.
- **`LoomPgbouncerScrapeDown`**: pgbouncer path is down. Fall back to `pgbouncer.enabled=false`.

CNPG-side alerts come from the operator's built-in `PodMonitor` (`monitoring.enablePodMonitor: true` in the Cluster spec). Rollout/lag/replication alerts are provided by CNPG's default rules, not duplicated here.

## Non-goals for v1

- **Cross-region failover.** Both replicas + backups live in the same physical LAN. Disaster recovery is out of scope until we have off-site backups.
- **Multi-writer Postgres.** CNPG runs single-primary with read replicas. No BDR-style multi-master.
- **Packed workers without isolation caps.** Double-duty colocation is the model and Loom Slurm workers always use `exclusive=false`. Until #896's per-container caps and container-isolation evidence pass, the fail-closed state is a disabled worker policy, not whole-node exclusivity.
- **arm64 in the control-plane cluster.** GB10 stays worker-only. If the control plane ever needs to reach 5+ nodes, add more OLDLAB or purchase more x86 hardware — do not extend into GB10.

## Related documents

- `docs/architecture/pgbouncer-transaction-mode-design.md` — connection multiplexer that sits in front of Postgres (CNPG or single-pod).
- `docs/runbooks/operator-runbook.md` — production runbook, includes cluster stand-up, bootstrap, alerts.
- Issue #637 — Postgres HA follow-up (this doc's home).
- Issue #610 — MinIO distributed follow-up.
