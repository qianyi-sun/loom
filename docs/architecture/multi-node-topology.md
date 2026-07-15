# Multi-node topology

**Status:** design
**Issues:** #637 (Postgres HA), #610 (MinIO distributed), #641 (topology schema), #642 (HA templates)

## Motivation

Loom's platform tier — Postgres, MinIO, LLM gateway, control-plane, service, pgbouncer — runs today as a single-node kind cluster on `bb8-1`. That's fine for a pilot with ~50 concurrent trials but has two hard limits:

1. **Every stateful pod is a SPOF.** Postgres pod restart = ~150 in-flight trials fail. MinIO pod restart = trial finalize stalls until it recovers. Under continuous autoscaling to 100+ concurrent trials, restart events (kernel patches, image upgrades, node reboots) become user-visible incidents.
2. **hostPath storage doesn't survive node loss.** All Postgres and MinIO data lives on `bb8-1`'s local disk. If `bb8-1` fails, the platform is offline until manual restore.

The multi-node topology solves both by moving to a real 3-node k8s cluster (k3s) with replicated storage (Longhorn) and operator-managed HA services (CloudNativePG for Postgres, distributed MinIO). Pgbouncer already runs 2 replicas so no change there.

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

- `local.cluster.toml`: pinned `multi_node = false` (kind is single-node by construction)
- `staging.cluster.toml`: pinned `multi_node = false` **temporarily**; flipped `true` in the same PR that lands k3s cutover
- `production.cluster.toml`: pinned `multi_node = true`, `storage_backend = "longhorn"`, 3 Postgres replicas, 4 MinIO replicas, required anti-affinity

## Cluster shape

**Physical nodes (OLDLAB, x86_64):**

| Node | Role in v1 | Rationale |
|---|---|---|
| bb8-1 | k3s server + Longhorn storage node | Has existing kind cluster + Loom repo checkouts; smoothest transition point |
| bb8-2 | k3s agent + Longhorn storage node | Fresh, no other services running |
| bb8-3 | k3s agent + Longhorn storage node | Same |
| bb8-4 | Slurm worker (unchanged) | Kept out of k8s to preserve OLDLAB Slurm slots |
| bb8-5 | Slurm worker (unchanged) | Same |

GB10 (arm64, 15 nodes) stays worker-only under Slurm. Not part of the k3s cluster — Loom's workers connect back to the k8s cluster's services via network, they don't need to be k8s pods themselves. This keeps the k3s cluster homogeneous x86_64 and avoids arm64-specific storage/scheduling complications.

**Why not colocate control plane + Slurm workers on bb8-1..3?** Analysis in the #641 review conversation: the ~12 marginal Slurm slots you'd recover aren't worth the noisy-neighbor debugging (Postgres p99 correlated with worker image-build spikes). Revisit after the multi-node cluster runs stably for a few weeks.

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
- `longhorn` (multi-node production): `storageClassName: longhorn`. Longhorn provisions replicated block storage via its DaemonSet on each participating node. Volumes survive node loss up to the replica count (default 3, matching our 3-node cluster).

Longhorn install is a runbook step, not part of `loom cluster render` output.

## Cutover from single-node to multi-node

The manifests are safe to apply on any cluster where the operators are installed, but data migration is out-of-band:

1. Land #641 (schema) and #642 (templates) on `dev` (both done).
2. Provision k3s cluster on bb8-1..3.
3. Install CloudNativePG operator: `kubectl apply -f https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/main/releases/cnpg-1.24.0.yaml` (verify current version in the CNPG release notes).
4. Install Longhorn: `kubectl apply -f https://raw.githubusercontent.com/longhorn/longhorn/master/deploy/longhorn.yaml`.
5. In a maintenance window:
   - Pause new trial submissions.
   - `pg_dump` from current single-node Postgres → `pg_restore` into the fresh CNPG cluster's primary.
   - `mc mirror` from current single-node MinIO → new distributed pool.
   - Flip `staging.cluster.toml`'s `topology.multi_node = false` → `true` and re-render.
   - `kubectl apply` the new manifests.
   - Verify pgbouncer connects via the `loom-postgres` ExternalName, LISTEN watchers reach direct Postgres, workers finalize trials to distributed MinIO.
   - Keep the old single-node Postgres/MinIO PVs on bb8-1 for 24-72h as rollback.
6. Decommission old PVs after monitoring shows the new cluster stable.

Rollback (any time in step 5): revert `topology.multi_node` to `false`, re-render, apply. All services point back at single-node Postgres/MinIO — no data loss because we didn't tear them down yet.

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
- **Colocating Slurm workers on control-plane nodes.** Deferred; see the #641 review discussion.
- **arm64 in the control-plane cluster.** GB10 stays worker-only. If the control plane ever needs to reach 5+ nodes, add more OLDLAB or purchase more x86 hardware — do not extend into GB10.

## Related documents

- `docs/architecture/pgbouncer-transaction-mode-design.md` — connection multiplexer that sits in front of Postgres (CNPG or single-pod).
- `docs/runbooks/operator-runbook.md` — production runbook, includes cluster stand-up, bootstrap, alerts.
- Issue #637 — Postgres HA follow-up (this doc's home).
- Issue #610 — MinIO distributed follow-up.
