# Independent Nebius integration platform

This lane targets `codex/nebius-main`. CI and candidate publication use GitHub
hosted runners; the platform needs no ARC controller or Nebius CI runner pool. It creates an independent platform in
`loom-nebius-platform` and execution in `loom-nebius-platform-execution`.
It never attaches to canonical staging or invokes its rollout broker. Existing
infrastructure and data remain separate until the migration acceptance decision.
This renderer accepts only `environment=development`, with the public UI at
the dedicated origin's root. Staging/production route and promotion semantics
are outside this integration lane and are explicitly rejected.

## Inputs and rendering

Copy `deploy/nebius/integration.platform.json.example` to a protected operator
directory and fill its non-secret values from the reviewed Terraform outputs.
Set `quota_parent_id` from the Terraform input `tenant_id`, while `project_id`
identifies the project containing the platform resources. This deployment reads
the tenant's allocated quota limits. Using the project ID instead returns
usage-only entries without active allowance limits and prevents collection.

All application images come from one published Nebius candidate. PostgreSQL 16
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

The keyring is passed through to the existing control plane, which verifies its
runtime image admission records. The publisher records commit and image digests;
rendering does not re-verify a separate candidate signature or profile hash.
It writes phase YAML with the environment settings in the application ConfigMap;
there are no duplicate candidate/configuration files or file-hash inventories.
Re-rendering replaces these known files and preserves operator notes. Deployment
reads the reviewed output once instead of hashing and regenerating it again.
A single configuration revision triggers Pod updates and new configure Jobs when
environment settings, the runtime profile or trusted public keys change, while
retries reuse the same completed Jobs.

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
| platform | `loom-platform-batch-runner` | `token`, a distinct stable `loom_br_` token with only `submit:batch` scope |
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

Generate the batch-runner token once as `loom_br_` plus at least 32 random
URL-safe bytes and keep it in the protected Secret provisioning input. On a
fresh install, database bootstrap registers it as a non-expiring worker token
with only `submit:batch` scope and no team binding. The migration Job reads
`LOOM_BATCH_RUNNER_TOKEN`; Service reads the same required Secret through
`LOOM_SVC_BATCH_RUNNER_CP_TOKEN`. Without it, Service accepts Batches but cannot
create their Trials. Do not reuse the collector or admin token.

For an existing installation, the authenticated CP endpoint
`POST /admin/batch-runner-tokens` with an empty JSON object can mint the same
non-expiring credential contract. Store its returned token directly in the
protected Secret input without logging it. Future bootstrap runs preserve that
token and reject revoked identities or different scopes, expiry or team binding;
rotation requires a newly issued token and a Service rollout.

Applications receive their own DB URL through `secretKeyRef` and mount **only**
the CA key, never the DB Secret's other passwords. Gateway and actuator have an
explicit table inventory; they cannot administer users/roles, mint tokens, or
write encrypted credentials. Service and CP retain application DML permissions;
only the bootstrap/migration job uses the database superuser. Future schema
changes must update narrower grants alongside their runtime consumers.

The actuator's database grants also cover the existing transaction locks and
invoker-rights triggers used by its command/reconciliation path:

| Table | Additional actuator privilege | Runtime use |
| --- | --- | --- |
| `execution_capacity_policies` | `UPDATE(updated_at)` | `reserve_execution_provisioning` takes a row lock; capacity limits and enablement remain read-only |
| `execution_admission_policies` | `UPDATE(active_count, counter_updated_at)` | Terminal lease triggers release admission counters |
| `execution_budget_policies` | `UPDATE(daily_reserved_microusd, monthly_reserved_microusd, updated_at)` | Terminal leases without a started Pod release their reserved budget |
| `team_quotas` | `SELECT(team_id, in_flight_count)`, `UPDATE(in_flight_count)` | Trial terminal projection decrements its team's active count |

[PostgreSQL row locks require UPDATE on at least one column](https://www.postgresql.org/docs/16/ddl-priv.html).
The capacity grant permits changing its bookkeeping timestamp, not policy
identity or settings. Admission and budget grants cover actual counter writes,
not their ceilings, scopes, enablement or emergency-stop settings. Price
snapshots, target-price bindings and capacity observations remain read-only.
No table-wide policy update, new budget policy or privileged database function
is introduced. Check this restricted-role path when changing its runtime SQL
or triggers; a superuser-only test does not exercise these grants.

Gateway call recording creates or verifies the Trial and event-stream rows in
`data_lifecycle_authorities`. Its role has `SELECT` and `INSERT` on that table,
without `UPDATE` or `DELETE`: it may bind a call to its owner, but cannot change
existing retention, pinning or deletion state. These permissions are installed
by database bootstrap, including repeat runs. A Gateway 500 during call recording
can occur **after the upstream model returned successfully**; an empty
`llm_calls` result then means missing persisted usage, not proof of zero upstream
calls. Inspect the Gateway exception before retrying a metered request.

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
   table permissions and registers the collector and batch-runner tokens without
   reviving revoked identities or changing their authority.
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
requires matching object size and SHA-256 metadata on readback. S3 metadata
key casing is normalized for Nebius responses such as `Sha256`; missing,
ambiguous or mismatched digests still fail verification. Dumps use a
transient volume; backup objects have no automatic deletion in this lane.

Before any upgrade mutation of an existing database, run and wait for a Job
from the **currently deployed** backup CronJob. A successful dump is not restore
proof. Restore acceptance means creating a distinct disposable database/PVC,
running the matching `pg_restore`, checking the migration revision and record
counts, and verifying sampled artifact references against canonical storage.
Retain the backup manifest, candidate and restoration evidence until the owner
accepts cleanup. No automatic down-migration or production promotion occurs.

The operator stops on a Job's `Failed` condition instead of waiting for the
completion deadline; pending Jobs retain their bounded timeout. PostgreSQL TLS
URLs may contain percent-encoded CA paths or credentials. The Alembic INI
adapter escapes percent signs only when writing its config, preserving the
original URL passed to the database driver. An early `MigrationError` with
`ValueError` and revision `unknown` can identify this configuration boundary;
never log the raw exception, which can contain the complete connection string.

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

## First bounded ordinary-user acceptance

After the published candidate is deployed, authenticate an ordinary member through
the public HTTPS origin. Confirm its session can read its own team and submit
tasks, and cannot access admin actions. Build the candidate-bound CPU TaskSet
with `scripts/ops/build_nebius_acceptance_taskset.py`. Use the existing acceptance
CLI with an explicit logical target and a single four-Trial stage:

```sh
loom eval nebius-acceptance \
  --taskset-dir /protected/nebius-acceptance-taskset \
  --model MODEL_ID --candidate-sha MERGED_CANDIDATE_SHA \
  --capacity-policy /protected/integration-acceptance-policy.json \
  --environment development --target-id nebius-eu-north1-integration \
  --stage 4 --output /protected/integration-acceptance
```

The acceptance policy uses `loom.nebius-development-capacity.v1`,
`target_id=nebius-eu-north1-integration`, `accepted_concurrency=4`,
`target_concurrency=4`, and enabled global `*` and pool `nebius-cpu` admission
limits of four, matching the deployed policies. The explicit target is checked
against that policy and every authenticated monitor snapshot, including later
read-only cleanup verification. The monitor exposes only the logical target
identifier; private cluster, node group and API bindings remain filtered.
Omitting `--target-id` preserves the existing environment-derived behavior.

A single `--stage 4` creates four CPU Trials total. Do not use larger/default
staged concurrency profiles for this first bootstrap. Acceptance requires real
node-backed overlap, canonical successes, authenticated complete bundles and
checksums, both declared artifacts, trajectory, verifier output and usage, then
scale-to-zero. Source retention may still be pending and must be reported
separately; resume its existing read-only cleanup command with the original
evidence. This does not prove worker-loss recovery, repeated upgrades or restore.

Service execution persists the verifier's named rewards and a scalar
`aggregate_reward` for the ordinary Trial API and Batch summaries. It uses the
same rule as the worker: a single metric retains its value, multiple metrics
use their mean, and absent rewards remain null. Zero is a valid score. Finalize
replays and archive materialization preserve this projection and the raw runtime
result. Installing this fix does not backfill older Trial rows; repair any
historical missing scalar only from that row's committed verifier rewards,
without rerunning the Trial or changing its state, raw rewards or usage.

Catalog configuration can be reapplied after a Control Plane restart. Execution
class network capabilities are a set: JSON ordering does not change the class
definition. Existing catalog rows retain their stored representation; reapplying
the same definition succeeds, while changing a class or target under its existing
ID still returns a conflict. Catalog digest columns remain for database schema
compatibility and do not decide semantic equality.
