# Independent Nebius integration platform

This lane targets `codex/nebius-main`. It creates an independent platform in
`loom-nebius-platform` and execution in `loom-nebius-platform-execution`.
It never attaches to canonical staging or invokes its rollout broker. Existing
infrastructure and data remain separate until the migration acceptance decision.
This renderer accepts only `environment=development`, with the public UI at
the dedicated origin's root. Staging/production route and promotion semantics
are outside this integration lane and are explicitly rejected.

## Inputs and rendering

Copy `deploy/nebius/integration.platform.json.example` to a protected operator
directory and fill its non-secret values from the reviewed Terraform outputs.
All application images come from one signed Nebius candidate. PostgreSQL 16
Bookworm and the matching `pg_dump` image must also be mirrored to Nebius and
pinned by digest; the PostgreSQL pod uses the image's UID/GID 999. Native CSI
storage uses the explicitly selected `compute-csi-default-sc` class.

```sh
python scripts/ops/render_nebius_platform.py \
  --environment-config /protected/integration.platform.json \
  --candidate /protected/candidate.json \
  --runtime-profile /protected/runtime-profile.json \
  --trusted-keyring /protected/trusted-keyring.json \
  --output /protected/rendered-candidate
```

The trusted keyring is independently configured; a key shipped only alongside
an untrusted candidate cannot authorize it. Rendering verifies the signed
candidate and runtime profile, then produces ordered YAML, the source inputs,
and a SHA-256 inventory. It performs no network or cloud mutation. The output
directory must be empty, preventing stale manifests from entering deployment.

The example execution price records the September 8, 2026 cpu-e2 eu-north1
[official rates](https://docs.nebius.com/compute/resources/pricing): 12,000 micro-USD/vCPU-hour,
3,200 micro-USD/GiB-hour RAM, and a conservative 98 micro-USD/GiB-hour
NETWORK_SSD estimate. Preserve source identity and observation timestamps when
reusing the snapshot. This is request-based task attribution, excluding idle
capacity, system nodes and other cloud fees; it is not actual billed spend.
No budget policy is introduced by this lane.

The integration system nodes carry `loom.nebius/platform=integration` and
`loom.nebius/node-role=system`, with the matching dedicated platform taint.
Execution nodes carry the same platform label and `node-role=integration-execution`, with
the execution taint. The distinct node-role keeps integration nodes outside the historical collector's
`node-role=execution` inventory. A dedicated execution group avoids competing autoscalers
and duplicate quota accounting against the older development pool. The example
starts with four admitted tasks and at most two execution nodes; these are
deployment bounds, not proof of available capacity or workload acceptance.

## Secret prerequisites

Provision these through the protected operator secret path. Do not put secret
values in environment JSON, rendered YAML, evidence bundles or command lines.

| Namespace | Secret | Keys |
| --- | --- | --- |
| platform | `loom-platform-db` | `postgres-password`, `admin-url`, `service-url`, `control-plane-url`, `gateway-url`, `actuator-url`, the matching four `*-password` keys, `ca.crt` |
| platform | `loom-platform-db-tls` | `tls.crt`, `tls.key` |
| platform | `loom-platform-public-tls` | `tls.crt`, `tls.key` |
| platform | `loom-platform-storage` | canonical `access-key`, `secret-key`; distinct source `source-access-key`, `source-secret-key`; distinct backup `backup-access-key`, `backup-secret-key` |
| platform | `loom-admin-secret` | `secrets.toml`, containing the established Loom admin verifier/token format |
| platform | `loom-platform-auth` | `jwt-signing-key`, `secret-store-master-key` |
| platform | `loom-model-provider` | `api-key` for the configured `local_yibu` Gateway provider |
| platform | `loom-platform-collector` | `token`, a stable `loom_ecc_` token |
| execution | `loom-execution-actuator-db` | `db-url`, `ca.crt`; matching only the actuator role |
| execution | `loom-execution-capacity-collector-nebius` | `credentials.json`, existing collector service-account credential format |
| execution | `loom-execution-capacity-collector-control-plane` | `token`, identical to the platform collector token |

Here “platform” and “execution” mean the configured namespace values. Public and
database TLS Secret names are configurable. Database URLs target
`loom-postgres.<platform-namespace>.svc:5432/loom`, require `sslmode=verify-full`
and `sslrootcert=/var/run/loom-db/ca.crt`, and encode passwords as URL values.
Use the `postgresql+psycopg://` SQLAlchemy URL accepted by the existing app settings;
the bootstrap converts the admin connection to psycopg. The database certificate
must cover that exact service DNS name. The PostgreSQL superuser is `postgres`;
the four application usernames are `loom_service`, `loom_control_plane`,
`loom_gateway`, and `loom_actuator`.

Applications receive their own DB URL through `secretKeyRef` and mount **only**
the CA key, never the DB Secret's other passwords. Gateway and actuator have an
explicit table inventory; they cannot administer users/roles, mint tokens, or
write encrypted credentials. Service and CP retain application DML permissions;
only the bootstrap/migration job uses the database superuser. Future schema
changes must update narrower grants alongside their runtime consumers.

Canonical artifacts/trajectories, transient execution source, and backup storage
use distinct buckets and identities. Canonical outputs remain durable after
execution source cleanup; source retention remains 86,400 seconds. Neither
Terraform's evidence bucket nor a source spool replaces canonical outputs.

## Deployment phases

The operator command must check the reviewed cluster ID and current Kubernetes
API endpoint before applying anything. Apply only the generated files:

1. `00-namespaces.yaml`: independent namespaces.
2. `10-config-network.yaml`: immutable candidate/configuration inputs, private
   service routing and database ingress boundaries.
3. `20-database.yaml`: PostgreSQL TLS service and retained native-CSI PVC;
   wait for `StatefulSet/loom-postgres`.
4. `30-migrate.yaml`: candidate-specific migration Job; wait for completion.
   It creates/reconciles roles, applies the actual Alembic head, grants current
   table permissions and registers the collector token without reviving a
   revoked identity.
5. `40-services.yaml`: service, CP, Gateway and web; wait for readiness.
6. `50-configure.yaml`: register only this environment's execution catalog,
   capacity/admission policies and new-target operator intent. It does not
   manufacture healthy execution or capacity observations.
7. `60-execution.yaml`: actuator, bounded RBAC, network policies and collector.
   The actuator publishes observed readiness; the collector publishes actual
   Nebius/Kubernetes resource observations.
8. `70-public.yaml`: Nebius LoadBalancer pinned to the reviewed allocation.
9. `80-backup.yaml`: scheduled database dumps and verified uploads. The deploy
   command also installs this before a fresh bootstrap migration so interrupted
   initial installs remain resumable.

Only web/API HTTPS is public: the web image's existing SPA listener stays on
8080, and a namespace-local Nginx listener terminates TLS on 8443, proxies
`/api/v1/` to the service and all other public paths to that SPA listener.
LoadBalancer 443 maps to 8443. CP, Gateway and PostgreSQL are private services.
Point `nebius.yylx.world` at the reviewed static public allocation and install a
valid public certificate before accepting external traffic. No cluster-wide
ingress controller or certificate CRD is required by this lane. Certificate
renewal must reconcile the Secret and restart the web deployment; Secret volume
updates alone do not cause Nginx to reload its TLS context.

No worker Deployment, Slurm controller, pipeline orchestrator or family
orchestrator is installed. This first lane implements the existing CPU Pod
execution contract. Harbor/browser/GPU/ARM/host-specific compatibility still
requires #1550 and must not be inferred from the presence of the platform.

## Backups, upgrades and failure diagnosis

The PostgreSQL StatefulSet has one replica for this integration environment.
Its PVC is retained on scale-down/deletion. This is not database HA. A CronJob
runs `pg_dump --format=custom --no-owner` every six hours using the pinned
PostgreSQL client, then uploads with the separate backup identity. Upload success
requires matching object size and SHA-256 metadata on readback. Dumps use a
transient volume; backup objects have no automatic deletion in this lane.

Before any upgrade mutation of an existing database, run and wait for a Job
from the **currently deployed** backup CronJob. A successful dump is not restore
proof. Restore acceptance means creating a distinct disposable database/PVC,
running the matching `pg_restore`, checking the migration revision and record
counts, and verifying sampled artifact references against canonical storage.
Retain the backup manifest, candidate and restoration evidence until the owner
accepts cleanup. No automatic down-migration or production promotion occurs.

Do not manually erase a failed Job or change live state to make deployment
appear green. Candidate-specific Job names allow retry to be a deliberate
operator action. Bootstrap error output includes phase, exception class,
SQLSTATE where available, and the last safe Alembic revision/error class. Raw
driver errors, URLs, SQL values and credentials are never emitted. Collect
Kubernetes event reasons and container states for the named phase to distinguish
image/pull, scheduling, PVC, TLS, schema and application readiness failures.

Acceptance still requires an external machine to authenticate, submit a real
task and retrieve complete reward, trajectory, artifacts and usage; then test
worker loss, cancellation/retry, repeated upgrade and restore. Neither a render,
a green CI run, an available public port nor a successful schema migration
establishes that acceptance.
