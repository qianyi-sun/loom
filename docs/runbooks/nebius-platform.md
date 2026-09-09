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
and duplicate quota accounting against the older development pool.

Terraform owns the execution group's static autoscaling limit: the default is
[the native API ceiling of 100 nodes](https://github.com/nebius/api/blob/main/nebius/mk8s/v1/node_group.proto),
with explicit lower `integration_platform.execution_max_nodes` values preserved.
No runtime component edits cloud limits. The example CP policy uses the same
technical envelope: 100 nodes, 1,600,000m CPU, 6,553,600Mi RAM and 8,192,000Mi raw
80Gi boot-disk storage. `node_storage_mib=65536` remains the task shape; provider
quota accounting uses the collector's separate raw node shape. These ceilings
are not evidence of quota, usable Pod capacity or workload acceptance. Admission
uses fresh quota observations and actual Pod placement, including DaemonSet
requests and Pod slots. A quota increase does not require changing a second
small integration concurrency limit. The independent create-rate limit remains
explicit at four creates per minute. It is separate from the concurrency ceiling,
but short-task throughput still depends on this ramp rate; deliberately adjust
the existing operator policy when planning a larger bounded acceptance run.

The example `max_concurrent: null` creates no global or pool concurrency policy.
Upgrade bootstrap disables only the exact old enabled four-task rows bearing
`Nebius integration environment capacity`; other operator rows remain intact.
A positive explicit value still installs global and pool admission limits. The
capacity migration widens only the exact historical two-node bootstrap policy,
including all its old limits and reason. A differing existing operator capacity
policy is retained and reported as `retained_operator_capacity_policy: true` by
the configure Job. Use the existing capacity/admission admin APIs for deliberate
updates to those retained policies, and record matching limits in environment
configuration. Fresh installs apply their explicit environment policy.

The execution namespace ResourceQuota follows the configured CPU/RAM/storage
ceilings and 64 Pods per allowed node, plus the rendered actuator rollout and
collector requests. The default hard limits are 6403 Pods, 1600300m CPU,
6553984Mi RAM and 8192000Mi ephemeral storage. CP observations deduct actual
DaemonSet usage when computing fit; no dated DaemonSet count is baked into this
namespace envelope. An optional `execution_resource_quota` map preserves a
deliberate Kubernetes restriction using `pods`, `requests.cpu`,
`requests.memory` or `requests.ephemeral-storage` string quantities; supplied
keys override the derived values, including an explicit zero. The renderer
never reads live quota to infer policy. Existing manually configured Kubernetes
limits must be copied into this map before rendering an upgrade.

Scale-from-zero also requires a previously collected **placement-format node
sample** matching the current native node template and DaemonSet inventory.
Older aggregate-only capacity observations cannot supply the missing allocatable
resources or Pod slots. If upgrading while the execution group is at zero with
no compatible sample, admission reports
`execution_capacity_node_allocatable_unknown`; deployment alone cannot unblock
or automatically scale that first workload. Plan and obtain authorization for a
bounded initial node warmup through the existing Terraform/operator path,
collect a fresh placement observation from the Ready node, and read back its
native shape, allocatable resources, Pod slots and resident DaemonSet requests.
Only then verify ordinary task admission and native scale-to-zero. A template or
DaemonSet change can invalidate that sample and requires the same explicit
warmup planning. Do not replace this dependency with guessed capacity or a
runtime cloud writer.

## Single-region capacity admission and recovery

The control plane reserves capacity in the same transaction as the budget and
admission records, before claiming a Trial or emitting its create command. A
capacity wait rolls back those records and keeps the execution attempt and
deadline untouched. The scheduler inspects at most32 candidates in its existing
fair-share order, records a bounded retry time (normally15seconds), and can run a
smaller compatible task behind an oversized or quota-blocked head. The direct
admin reservation API returns409 with the limiting reason and Retry-After.

Placement uses each Node's independent CPU, RAM, ephemeral-storage and Pod-slot
requests. Pending Pods and durable authorizations are absorbed by lease and
**resource generation** identity; terminating nonterminal Pods still consume
resources. Cold nodes use matching observed allocatable capacity minus resident
DaemonSet requests. Raw preset and boot-disk quantities separately charge the
provider allowance; the current80GiB boot disk must not be charged as64GiB.
The existing immutable observation JSON stores this evidence; no extra state
service, hash inventory or cloud writer is introduced.

Admissions serialize on the existing database lock and include all pools sharing
each native tenant/region/service/quota identity. Separate CPU quotas can still
share SSD quota. Registered or creating native nodes count toward known usage;
remaining authorized demand is packed separately per target. Zero and reduced
quotas stop new demand without rewriting running leases. Fresh quota evidence is
required even when a compatible old node-template sample supplies cold-start fit.
Provider/account snapshots are not atomic with external account users, and a
RUNNING group is not a reservation of future physical stock. Native rejection
and observed supply shortage remain distinct from quota exhaustion and normal
provisioning delay.

For a Job that has **never been scheduled or started** and remains Unschedulable
past its existing execution deadline, the actuator uses the existing fenced
retry/cleanup transition. The old authorization remains until deletion completes;
the scheduler cannot issue the next attempt before that cleanup and its15second
backoff. Attempt history is retained. If the existing team attempt ceiling is
exhausted, the Trial stays queued with `infra_recovery_exhausted` rather than being
reported as an agent/verifier failure. Ordinary Pending cold starts and already
started workloads do not enter this recovery path. The Pod's native
`cluster-autoscaler.kubernetes.io/safe-to-evict: "false"` annotation protects active
work from voluntary autoscaler eviction; terminal cleanup still removes the Pod.
It does not prevent forced deletion or hardware failure.

This implementation is the single-region slice of #1884. Cross-region target
selection, connectivity and provider-fault acceptance remain subsequent work.
#1538 must use fresh quota and a separately bounded resource/Trial plan to verify
actual overlap, recovery and native scale-to-zero; passing local tests or raising
the technical maximum is not live capacity acceptance. CI and publication remain
GitHub-hosted.

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
| `team_quotas` | `SELECT(team_id, in_flight_count, max_attempts_ceiling)`, `UPDATE(in_flight_count)` | Trial terminal projection decrements its team's active count; deadline-bounded infrastructure retry reads the attempt ceiling |

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
8080, and a Caddy sidecar in the same Pod terminates TLS on 8443, proxies
`/api/v1/` to the service and all other public paths to that SPA listener.
LoadBalancer 443 maps to 8443. CP, Gateway and PostgreSQL are private services.
Point `nebius.yylx.world` at the reviewed static public allocation and install a
valid public certificate before accepting external traffic. Caddy obtains and
renews a managed certificate through Let's Encrypt TLS-ALPN-01 over that existing
TCP 443 path. No port 80, DNS API credential, certificate CRD, additional RBAC or
cluster-wide ingress controller is needed. The sidecar uses the same published
web image and runs as UID/GID 101; the ordinary Nginx entrypoint, SPA runtime
configuration and browser security headers remain unchanged.

The single additional resource is `loom-web-tls`, a 4 GiB `ReadWriteOnce` PVC in
the platform namespace using the configured native CSI storage class. It stores
Caddy's account, private keys and managed certificates across restarts. The PVC
is rendered in `40-services.yaml`, after the mandatory pre-upgrade backup. Keep
this PVC during rollback or web Deployment replacement. Rolling updates share
it on the one integration system node; adding more system nodes requires
reviewing the single-node RWO rollout constraint. The sidecar requests 25m CPU
and 64 MiB memory, with limits of 500m CPU and 256 MiB memory.

The optional boolean `public_tls_bootstrap` defaults to `false`, the steady
configuration. A **first installation or the first switch from manual Nginx TLS
requires two passes** through the existing renderer and deploy command:

1. Set `public_tls_bootstrap=true` and supply a valid public TLS Secret before
   the first pass. Caddy loads it without certificate-selection tags, preserving
   initial HTTPS and readiness while it obtains a managed certificate into the
   PVC. Without a valid bootstrap on an empty PVC, readiness keeps the Pod out
   of the public Service and the CA cannot reach its TLS-ALPN challenge.
2. Verify successful managed issuance and its persisted certificate expiry, then
   set `public_tls_bootstrap=false`, re-render the **same published candidate**
   and run the existing deploy command again. This configuration change takes
   a fresh mandatory backup and rolls the Pods. It removes the manual loader
   and Secret mount; the managed certificate and account remain on the PVC.
   Keep the old Secret for rollback, but do not leave bootstrap mode enabled.

Bootstrap mode is transitional because CertMagic treats the final encoded
NotAfter second inclusively while some TLS clients already reject that second.
Removing the manual loader after issuance avoids relying on expiry-time
selection. Caddyfile `tls cert key` is also unsuitable: it pins selection to the
manual certificate even after expiry. Subsequent upgrades use bootstrap=false;
ordinary renewal and in-memory certificate replacement are native Caddy
operations and need no Secret synchronization or restart. If TLS storage is
lost, explicitly repeat the bootstrap sequence using a currently valid
certificate. An identical final configuration reapply uses the deployer's
existing idempotence behavior.

HTTPS readiness checks both TLS and the SPA; `default_sni` handles Kubelet's
numeric Pod-IP probe, while its Host header selects the public route. Kubelet
skips certificate validation, so readiness alone is not public TLS acceptance.

The web build uses stock Caddy v2.11.4 with Go 1.26.8 and `x/crypto` v0.55.0.
The dependency override fixes CVE-2026-56854 in the official v2.11.4 binary's
`x/crypto` v0.52.0; remove it when an upstream Caddy release incorporates the
fix. The resulting binary is scanned as part of the existing web-image policy.
See [Caddy automatic HTTPS](https://caddyserver.com/docs/automatic-https),
[the upstream selector](https://github.com/caddyserver/certmagic/blob/v0.25.3/handshake.go),
and [Nebius CSI storage](https://docs.nebius.com/kubernetes/storage/disk-over-csi).

Developer verification uses an explicitly built web image and a disposable
Pebble CA; it exercises the two-pass bootstrap transition, real TLS-ALPN issuance and renewal,
restart with the CA unavailable, SPA headers, API forwarding and SSE:

```sh
docker buildx build --platform linux/amd64 --load -f deploy/Dockerfile.web -t loom-web-tls:test .
LOOM_NEBIUS_WEB_IMAGE=loom-web-tls:test uv run --extra dev pytest -q -s \
  tests/integration/test_nebius_public_tls.py
```

Live acceptance separately checks the published candidate, PVC binding, both
web-container readiness states, Caddy's successful issuance log and stored
certificate expiry, and trusted public HTTPS. A still-valid bootstrap certificate
on the public endpoint alone does not prove managed issuance or renewal.

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
limits of four explicitly installed for this bounded acceptance. These are
acceptance-specific controls, independent of the default quota-following lane. The explicit target is checked
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
result. Alembic revision `0133` backfills older service-execution results that
have a missing/null scalar and nonempty finite numeric rewards matching the
runtime verifier rewards. It leaves explicit scores, unrelated results and
invalid/missing rewards untouched. The migration changes only the derived
scalar, without rerunning the Trial or changing state, raw rewards, usage or
artifacts; downgrading does not erase corrected scores. Apply it through the
ordinary deployment migration phase rather than a separate live repair script.

Catalog configuration can be reapplied after a Control Plane restart. Execution
class network capabilities are a set: JSON ordering does not change the class
definition. Existing catalog rows retain their stored representation; reapplying
the same definition succeeds, while changing a class or target under its existing
ID still returns a conflict. Catalog digest columns remain for database schema
compatibility and do not decide semantic equality.


## Optional regional execution

`deploy/nebius/regional.platform.json.example` adds explicit EU secondary targets
to the same logical pool. The original configuration remains valid. Fill the secondary's reviewed regional
price rates and timestamps; the example's zero rates deliberately cannot pass
render validation. Each secondary has its own native cluster/node group, namespace, quota observation,
capacity policy, price binding and dedicated actuator/collector pair. Those
processes run on the existing primary system node in the primary execution
namespace; their Kubernetes clients connect to the named remote API using an
explicit CA and automatically refreshed native IAM tokens. Database, Service,
CP, Gateway, canonical storage and web/TLS remain in the primary region.

The optional Terraform `regional_execution_targets` map in the existing platform
root composes `modules/regional-execution`. Each entry creates exactly one MK8s
control plane with audit logging, one fixed system node and one min-zero CPU
execution group (technical default maximum100, explicit lower values honored).
Inputs identify an existing regional project/subnet. Existing-ID mode supplies a
regional registry-pull identity and creates only these three infrastructure resources.
`node_platform` is required per regional Terraform target, rather than inherited
from the north-region platform. The west example uses native `cpu-d3` (AMD EPYC
Genoa), whose catalog includes the `4vcpu-16gb` system and `16vcpu-64gb` execution
presets. The west example uses the native compatibility matrix's Kubernetes1.33
with Ubuntu24.04. The primary retains `cpu-e2`. Verify native platform/preset availability
and MK8s version compatibility in the destination before an activation plan;
mocked Terraform tests do not prove either. The separate80Gi NETWORK_SSD boot
disk remains unchanged. Regional price SKU is `cpu-d3-16vcpu-64gb`; enter current
reviewed CPU/RAM/storage rates rather than copying the north-region Intel price.
Neither mode creates a network, registry, bucket, database, public workload
Service, VPN or CI runner. Before applying, separately review regional CPU and
MK8s availability, project quota, cross-region registry access, cost, public API activation
and any prerequisite identity grants; the example is not a cloud authorization.
See the [official regional service matrix](https://docs.nebius.com/overview/regions).

For a fresh regional identity set, use
`deploy/terraform/nebius/regional-execution-managed-identities.tfvars.json.example`.
Omit `node_registry_pull_service_account_id` and supply `managed_identities` with
three distinct SPKI PEM **public** keys (`actuator_public_key`,
`collector_public_key`, `gateway_public_key`), `collector_viewer_group_id` and
`registry_pull_group_id`. This mode adds four regional service accounts, three
native authorized-public-key registrations for the runtime accounts, and two
memberships: collector in the existing observer group and node-pull in the
existing registry viewer group. Together with the three infrastructure resources,
this is **12 creates** for a new target, with no change to existing resources.
The node groups wait for memberships and attach the new regional node-pull
account, avoiding an unverified cross-project identity attachment. Node-pull has
no registered key; actuator and Gateway receive no cloud group membership.

Read back both existing groups' permissions before plan review: the observer
group must provide the collector's required read-only tenant/project coverage,
and the pull group must be viewer-only on the intended registry. Group IDs do
not themselves prove those authorities. The module creates no group or access
permit and grants no editor role. Supply only public key text to Terraform;
private key generation and storage stay in the protected operator path, outside
Terraform inputs, outputs and state. Authorized keys omit expiration, with
rotation handled through an explicit reviewed update. After an approved apply,
the output's `service_account_ids` map populates the environment's three runtime
identities; `authorized_public_key_ids` supplies the native key references for
protected credential files. The separate node-pull and membership IDs support
readback. Outputs contain no key data or credentials. Existing-ID mode leaves
runtime identity/key management with the operator and creates no IAM resources.

The managed public API uses TLS, short-lived native IAM authentication and the
explicit RBAC below. `public_control_plane_cidrs` is optional and defaults to[]
([native empty-list semantics](https://pkg.go.dev/github.com/nebius/gosdk@v0.2.27/proto/nebius/mk8s/v1#PublicEndpointSpec)
impose no source-IP restriction); a nonempty operator allowlist is preserved. An
operator choosing that additional network restriction must establish the actual
provider-confirmed primary egress CIDRs and approved operator addresses. The
inbound Loom load-balancer allocation is not evidence of outbound source IP.
No fixed-egress/NAT or VPN prerequisite is introduced for the default path.

`public_gateway_ipv4` must be the read-back IPv4 of the existing fixed public
allocation. Remote task NetworkPolicy permits DNS plus only that /32 on TCP443.
The public Caddy listener forwards the `/internal/service-execution/` broker
prefix and the existing authenticated model POST endpoints; `/admin` and other
internal/model-prefix paths return404. Credentials and raw database access are
never exposed. Inputs and outputs pass through the authenticated broker and
stay in primary canonical storage. Model calls go through the runtime's local
proxy, which derives the public Gateway origin from its broker URL; immutable
execution plans do not need an endpoint rewrite.

Secondary catalog targets require projected Pod identity with audience
`loom-execution`, namespace-local ServiceAccount `loom-execution-attempt`, and
native TokenReview at their own cluster. Primary execution retains the existing
internal Pod-IP authorization and internal broker URL. A public proxy does not
make a primary Pod's legacy identity valid; those calls fail closed. No primary
IAM identity change is required by this extension.

The environment lists three distinct native service-account IDs per secondary.
The remote RBAC subjects are Kubernetes `User` entries containing those Nebius
IDs, not local ServiceAccount subjects:

| Identity | Remote Kubernetes authority |
| --- | --- |
| actuator | Namespace Jobs create/get/list/watch/delete and Pods get/list/watch |
| collector | Cluster Nodes/Pods and DaemonSets get/list |
| gateway | Only `authentication.k8s.io/tokenreviews` create |

None can read Secrets through those roles; the attempt ServiceAccount has no
API role and its token is projected only for broker identity. Existing native
cloud viewer permissions for the collector must independently cover its
regional node group/preset and tenant quota scope. Runtime Kubernetes authority
never grants cloud mutation or project editor rights.

Install protected Secrets before the primary apply. For each target `TARGET`,
`TARGET-actuator-kubernetes` and `TARGET-collector-kubernetes` belong in the
primary execution namespace; `TARGET-gateway-kubernetes` belongs in the primary
platform namespace. Each has `ca.crt` (the remote cluster CA) and
`credentials.json` (the matching native service-account credentials). The
collector uses its one Secret for both read-only cloud observation and remote
Kubernetes access; its existing init container makes the provider SDK's owned
credential copy. Existing actuator DB and collector CP observation-token Secrets
are reused. Runtime mounts use0440 with the existing non-root fsGroup, and the
ordinary deploy preflight checks all Secret key names without printing values.

Keep regional credentials/API CA and exact cluster binding under the existing
protected operator path. Review Terraform with both the existing platform
input and the optional regional input; retain the same versioned platform state:

```sh
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/platform plan -var-file=/protected/platform.tfvars.json -var-file=/protected/regional-execution.tfvars.json -out=/protected/regional.tfplan
```

Only an approved plan may be applied. Native endpoint/namespace/RBAC readiness
must precede switching the primary candidate to reference a secondary. Render
primary and remote files to separate sibling directories:

```sh
uv run --no-sync python scripts/ops/render_nebius_platform.py --environment-config /protected/regional.environment.json --candidate /protected/candidate.json --runtime-profile /protected/runtime-profile.json --trusted-keyring /protected/trusted-keyring.json --output /protected/primary-render --regional-output /protected/regional-render
kubectl --kubeconfig /protected/eu-west1.kubeconfig apply -f /protected/regional-render/nebius-eu-west1-integration.yaml
```

The second command is a separately authorized remote cluster mutation. Confirm
its kubeconfig server, CA and cluster identity against the reviewed regional
outputs first. Never pass the remote directory to `deploy_nebius_platform.py` or
apply it through the primary kubeconfig. The primary renderer emits no remote
namespace or regional ClusterRole objects in its deployment files. Apply the
primary directory through the existing backup-first deploy command. It now waits
for every configured primary-hosted actuator Deployment, including secondary
targets. Actuator readiness requires successful command and full reconciliation
loops; full reconciliation includes listing Jobs in the remote namespace.
That proves API connectivity/reconciliation, not a successful regional task,
capacity warmup, TokenReview authorization or regional workload acceptance.

The ordinary regional collector runs every minute. To collect an initial sample
without waiting for the schedule, use its existing CronJob after the relevant
node warmup and mutation are authorized:

```sh
kubectl --kubeconfig /protected/primary.kubeconfig -n PRIMARY_EXECUTION_NAMESPACE create job regional-capacity-initial --from=cronjob/nebius-eu-west1-integration-collector
kubectl --kubeconfig /protected/primary.kubeconfig -n PRIMARY_EXECUTION_NAMESPACE wait --for=condition=complete job/regional-capacity-initial --timeout=180s
```

Read back the target's authenticated CP capacity status and a compatible
placement-format Ready-node sample. A new min-zero pool has no historical
allocatable sample, so the bounded warmup described above must be planned and
authorized; source configuration does not bootstrap a guessed sample. Then
perform separately authorized remote task, data-path, model-attribution and
scale-down acceptance. Each region's optional `execution_resource_quota` map
applies only to that remote namespace; the primary namespace's override is not
silently copied to remote pools.

Regional process names are rewritten only in Kubernetes metadata, Pod labels/selectors,
ServiceAccount references and collector ConfigMap/native Secret references. Published
image references (including collector init containers), configuration values and
commands retain their original values. The regional processes reuse the existing
actuator database Secret and collector CP token. A renderer-only repair can therefore
rerender the same published candidate and runtime profile without rebuilding images.
