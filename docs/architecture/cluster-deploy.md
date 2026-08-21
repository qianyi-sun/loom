# Cluster Deployment

`loom cluster` renders and operates Loom on Kubernetes. It is the direct
cluster interface for unprotected shared deployments and the underlying
rollout surface for protected staging and production. Within `loom service`,
the `local` target manages Docker Compose, while a `dev-<name>` target submits a
sealed personal candidate through the remote environment API; protected
targets redirect operators to the cluster rollout workflow. Cluster behavior
is driven by a TOML profile validated against the [configuration
schema](configuration.md).

## Components

| Component | Role |
| --- | --- |
| `loom-service` | Public REST API, authentication, catalog, and SPA backend |
| `loom-web` | Static React application and runtime frontend configuration |
| `loom-control-plane` | Trial state, scheduling, worker APIs, and step-token minting |
| `loom-worker` | Optional in-cluster trial executor; external pools can replace it |
| `loom-llm-gateway` | Provider routing, credential use, attribution, and usage accounting |
| Postgres and PgBouncer | Durable application state and pooled client connections |
| MinIO or another configured S3-compatible store | Task bundles, trajectories, and artifacts |
| monitoring stack | Prometheus rules and Grafana dashboards when enabled |

The stateful shape is selected by `topology`. Single-node profiles render
Postgres and MinIO StatefulSets. Multi-node profiles render CloudNativePG and
distributed MinIO as described in [Multi-Node Topology](multi-node-topology.md).

## Public and internal boundary

Ingress exposes only the web application and `loom-service`:

- the SPA at the configured frontend route prefix; and
- `/api/v1` under the matching API prefix.

The Control Plane, LLM Gateway, Postgres, PgBouncer, object store, and egress
components have no public Ingress backend. Router hostPorts are internal
worker/fleet transport surfaces and are validated against the render contract.

`loom cluster audit` rejects public Services, unexpected Ingress backends,
missing TLS, unsafe paths, unsupported hostPorts, missing selecting
NetworkPolicy, and other boundary violations. The target CNI must enforce
NetworkPolicy; Kubernetes accepting an object is not proof of packet filtering.

## Sandbox-to-Gateway flow

Workers give supported subprocess agents a sandbox-reachable Gateway facade
URL. The sandbox authenticates with a short-lived step JWT bound to the trial,
team, step, and optional provider connection. Provider credentials remain in
the Gateway and secret store.

With worker sandbox isolation enabled and healthy, a per-trial internal Docker
bridge reaches the node-local sandbox Gateway singleton, which forwards to the
cluster Gateway router. The optional egress proxy/xDS path restricts provider
connections to their resolved upstream identities. See
[Sandbox Isolation](sandbox-isolation.md) for defaults and fallback behavior.

## Configuration and secrets

Durable environment profiles live under `deploy/environments/`. Profiles set
namespace, runtime and frontend identity, route prefixes, image sources,
storage, replica counts, worker shape, external pool transport, and rollout
policy.

Runtime secrets are projected from Kubernetes Secrets according to
`config/loom-schema.toml`. The singleton operator secret is mounted as a file;
database, object-store, worker, JWT, provider, and other credentials must not
be placed in a committed profile or command arguments.

Generate the `kubectl create secret` command from the schema with
`loom cluster bootstrap-secrets`. Review the output and provide values through
the documented secure input mechanism before applying it. Multi-node Postgres
also requires the CloudNativePG basic-auth Secret.

## Current CLI surface

Read-only and render commands:

```text
loom cluster status
loom cluster render
loom cluster reconcile --shadow
loom cluster preflight
loom cluster audit
loom cluster doctor
loom cluster rollout-evidence
```

Lifecycle and release commands:

```text
loom cluster up
loom cluster down
loom cluster backup ...
loom cluster render-migration
loom cluster release-manifest
loom cluster minio-storage-preflight
loom cluster release-gate
loom cluster rollout ...
```

Bootstrap and maintenance commands:

```text
loom cluster bootstrap-secrets
loom cluster derive-pool-dsn
loom cluster bootstrap-storage-lifecycle
loom cluster bootstrap-evidence-paths
loom cluster taskset-fence-canary
```

Use each subcommand's `--help` for its exact required arguments. `down`
preserves PVCs and the namespace unless the explicit volume or namespace
deletion flags are supplied.

## Apply sequence

For an ordinary unprotected cluster:

```bash
uv run --no-sync loom cluster preflight --config deploy/environments/ENV.cluster.toml
uv run --no-sync loom cluster render --config deploy/environments/ENV.cluster.toml > /tmp/loom-rendered.yaml
uv run --no-sync loom cluster audit --config deploy/environments/ENV.cluster.toml
kubectl apply -f /tmp/loom-rendered.yaml
uv run --no-sync loom cluster status --config deploy/environments/ENV.cluster.toml
```

`loom cluster up` composes preflight, render, apply, and readiness waiting. A
protected staging or production rollout additionally requires candidate-bound
backup, migration, environment-state, release-manifest, release-gate, and smoke
evidence. Use the installed [protected staging rollout](staging-rollout.md) for
shared staging instead of running lower-level mutation commands directly.

## Preflight and diagnosis

Preflight checks connectivity, namespace/profile identity, required Secrets,
IngressClass, storage, Pod Security labels, backup policy, and the protected
environment guards applicable to the selected profile. Exit status is `0` for
pass, `1` for failed checks, and `2` when the cluster is unreachable.

`doctor` compares schema-derived settings with the live cluster. `status`
reports readiness and public endpoints. `reconcile --shadow` emits desired vs
live drift without writing. Use `--format json` where offered for automation
rather than parsing tables.

Common deployment failures are missing CRDs or storage classes for the chosen
topology, a namespace/profile mismatch, image pull failure, unsafe Secret
permissions, absent NetworkPolicy enforcement, PgBouncer/Postgres credential
drift, and an external worker URL that is reachable from the worker process but
not its Docker sandboxes.

See the [operator runbook](../runbooks/operator-runbook.md) for command-specific
procedures and [service mode](service-mode.md) for application data flows.
