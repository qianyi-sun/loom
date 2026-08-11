# Multi-Node Topology

Loom can render either single-node stateful services or an HA-shaped
multi-node deployment through the `topology` cluster configuration table.
Profiles pin this table explicitly so the schema defaults do not change an
environment's stateful shape.

```toml
[topology]
multi_node = true
storage_backend = "longhorn"
postgres_replicas = 3
minio_replicas = 4
min_available = 1
anti_affinity = "required"
```

`deploy/environments/staging.multinode.cluster.toml` is the checked-in
multi-node staging profile. The current development, staging single-node, and
production profiles set `multi_node = false`; select the profile that matches
the cluster and its installed storage/operator dependencies.

## Rendered shapes

| Setting | Postgres | Object storage |
| --- | --- | --- |
| `multi_node = false` | One Postgres StatefulSet | One MinIO StatefulSet |
| `multi_node = true` | CloudNativePG `Cluster` with `postgres_replicas` instances | Distributed MinIO StatefulSet with `minio_replicas` instances |

Multi-node Postgres requires the CloudNativePG operator and its CRDs. Loom
keeps the stable `loom-postgres` service name through an ExternalName pointing
to the CloudNativePG primary service. Consumers, PgBouncer, migrations, and
NetworkPolicy therefore use the same hostname in both topologies.

CloudNativePG consumes a `kubernetes.io/basic-auth` Secret named
`loom-postgres-cnpg-credentials`. Operators must keep it aligned with the Loom
database credentials during rotation.

Distributed MinIO uses a headless service for peer discovery, parallel pod
startup, one PVC per replica, and a disruption budget of
`minio_replicas - 1`. All configured replicas must start to establish the
expected erasure set.

## Storage and placement

`storage_backend = "longhorn"` requests the `longhorn` StorageClass for the
multi-node Postgres and MinIO volumes. Loom renders the claims but does not
install Longhorn or CloudNativePG. Their controllers, nodes, and storage class
must exist before applying the manifests.

`anti_affinity` selects preferred or required cross-host placement. Required
anti-affinity needs enough eligible Kubernetes nodes for every replica.
`min_available` controls the applicable disruption budgets; MinIO also retains
its replica-derived quorum budget.

GB10 machines are external workers, not Kubernetes control-plane or storage
nodes. They reach the cluster's worker, object-store, and Gateway router
surfaces through the configured external transport.

## Operations

Run `loom cluster preflight` before applying a multi-node profile. After
deployment, verify:

- CloudNativePG reports one primary and the expected replicas;
- `loom-postgres` resolves to the read/write service;
- every MinIO pod is Ready and peer health reports the full set;
- PVCs use the intended StorageClass and replicas are on distinct nodes;
- PgBouncer can reach the stable database service; and
- NetworkPolicy selects both normal Loom pods and CloudNativePG-managed pods.

See [PgBouncer](pgbouncer.md), [cluster deployment](cluster-deploy.md), and the
[operator runbook](../runbooks/operator-runbook.md).
