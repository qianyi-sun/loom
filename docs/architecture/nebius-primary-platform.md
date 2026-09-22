# Nebius platform contract

Hosted Loom runs only on Nebius. Platform services, supported Kubernetes
execution, database, object storage, registry, backups and monitoring belong to
that platform. GitHub-hosted CI, image builds and publication remain supported,
as do local development and external user-selected inference APIs.

## Platform boundaries

System services and elastic execution pools have separate capacity policies.
Environment identities, database roles, storage scopes and credentials stay
separate. Public web/API access uses authenticated HTTPS; database and execution
management stay private. The [native execution contract](nebius-service-execution.md)
owns target placement, durable attempts, cancellation and fenced publication.

The primary region is `eu-north1`. Secondary-region routing remains disabled
pending separate qualification; checked-in regional support is not evidence
that a region is operationally accepted. Current deployment and recovery
procedures are indexed in [runbooks](../runbooks/README.md).

## Managed environment identity and rendering

`loom.nebius_environment_contract` separates an environment's UUID/incarnation
from its class (`development`, `staging`, `production`), owner and mutable
deployment generation. Multiple developers can have development environments;
the class is not an instance identifier or an execution-capacity allocation.
Fresh shared dev is `loom-dev`; a personal application's namespace is
`loom-dev-<slug>`. Its execution namespace is `loom-run-<incarnation-hex>` and its
build namespace adds `-build`. Slugs such as `alice-exec` cannot collide with
Alice's auxiliary namespaces. A deployment update preserves these identities.

The separate, protected `FoundationBinding` contains operator-owned installation
settings, a public DNS zone, shared ingress identity and a configurable pool-wide
warm floor (default zero). It cannot be supplied by feature source. Constructing
this object validates inputs; it does not provision infrastructure or change the
native autoscaler's settings.

`loom.nebius_environment_render.render_environment` reuses the standalone stack
templates for a registered child. Each render contains its own PostgreSQL
StatefulSet/PVC, namespaced credential references, incarnation-derived bucket
names and one environment-local V1 execution topology. Importing an existing
binding requires exact namespace, target and hostname matches to its protected
installation input and preserves existing bucket names. Import is an operator
operation, not a user-selected namespace override.

Each child has one HTTPS hostname, not a different public port: the shared
ingress routes `/api` to that child's service and `/` to its static web server.
The ingress controller owns the default wildcard certificate; no TLS private key
or per-child public LoadBalancer/Caddy volume is rendered. Imported hosts must
also fit that certificate's configured DNS zone. NetworkPolicy restricts this
ingress to the configured controller namespace and Pod identity; database access
remains namespace/role scoped. Actual DNS, certificate validity, credentials and
object-store permissions require provisioning and installed verification.

The renderer reports a conservative platform **request** envelope: all steady
Pods, simultaneous Deployment surge, migration/configuration/backup Jobs,
init-container peaks, backup scratch space and retained database storage. This
is input for platform admission, not measured usage or a physical reservation.
Execution tasks/builds are excluded from that envelope and belong on the shared
execution pool.

Migration `0154` adds `nebius_environments` and
`nebius_environment_namespaces` without changing retained `dev_instances`.
The management database owns these rows. A single physical-name index covers
all three namespace roles, preventing cross-role collisions even under concurrent
transactions. Registration and the complete namespace set must be reserved in
one transaction before resource creation. Active/suspended/destroyed rows retain
slug and hostname claims until verified purge; incarnation and target IDs are
never reused. Purge verification and the transaction coordinator are lifecycle
responsibilities, not capabilities conferred by inserting a row. A downgrade
refuses to discard nonempty registration history.

This is an **offline provisioning contract, not operational multi-person
acceptance**. The managed format keeps the scheduler and capacity policy disabled,
renders no actuator/collector Pods, grants only observer RBAC, and sets zero-Pod
quotas in execution/build namespaces. The standalone deploy helper rejects this
format. The management request layer below verifies candidates and reserves
identities, but rendering does not provision resources or establish shared
admission/write enforcement or installed concurrent-owner execution.

## Independent management service runtime

`LOOM_SVC_SERVICE_MODE=management` selects the identity and environment-management runtime in
the existing Service image. It must use a **separate management database** via
`LOOM_SVC_DB_URL` (and its optional pool URL), not a child application's database.
It checks the current schema and existing encrypted secrets before serving.
Its database/admin credentials are installation-owned; do not put them into
personal deployments.

Management retains account/session, invitation, token, team and administrative
identity/audit APIs. It does not expose workload, pipeline, provider or storage
routes, initialize child Control Plane/Gateway/object-store clients, validate a
child execution profile, or start batch/materialization/GC loops. Storage keys
are unnecessary in this mode; they remain required by default application mode.
The local-execution flag cannot turn management into a workload service.

`/api/v1/health` is process liveness. In management mode `/api/v1/health/ready`
is an unauthenticated, bounded, read-only database probe returning only component
status, with HTTP 503 on failure. When the optional provisioner is configured,
its supervised-loop health is included; a dead or recovering worker is not ready.
It reports no identities, credentials or database errors and has no dependency on
child availability. Application-mode readiness
and its authentication contract are unchanged. Hosted sessions retain secure
host-only cookies and sibling-origin rejection in either mode.

The optional provider worker can provision an execution-disabled child and perform
retained teardown. These are implementation capabilities, **not installed Nebius
acceptance**. Public DNS/TLS, IAM isolation, protected installation, multi-owner
execution and lifecycle acceptance must still be qualified before enabling owner
creation. A healthy management process is not evidence that personal environments
or shared execution are operational.

### Managed provisioning requests

`LOOM_SVC_ENVIRONMENT_MANAGEMENT_CONFIG_FILE` optionally enables the request
layer. This protected installation JSON uses schema
`loom.nebius-management-installation.v1` and contains `foundation`, `registry_prefix`,
`keyring`, `publications` and `platform_budget`. `foundation` is the validated
shared infrastructure binding; its `platform_config_json` holds the standalone
configuration as JSON text. Neither the foundation nor resource requests come
from developer input. The budget supplies nonnegative `cpu_millis`, `memory_mib`,
`storage_mib` and `ephemeral_storage_mib` available **after** fixed platform and
management headroom. Startup inserts an absent budget or verifies an exact match;
a changed allowance is rejected, not silently resized.

Optional `provider_runtime` starts the worker. It supplies a `kubernetes` object
with explicit HTTPS `endpoint`, CA `ca_file` and private Nebius `credentials_file`,
plus a separate private `cloud_credentials_file`. Both credential files are bounded
regular files, with no world access or group write; projected read-only Secret
files are supported. No ambient kubeconfig, login, proxy or insecure transport is
used. Native SDK token renewal stays pinned to the configured origin. `concurrency`
defaults to four provisioning operations (range 1–16), and `poll_seconds` defaults
to five (range 1–60); these are not task-capacity shares. Shutdown cancels operations
and lease heartbeats before closing HTTP/SDK/database clients. Database outages
leave uncertain intents charged and restart polling, without exposing exception
contents. Without this option, accepted requests remain pending.

`LOOM_SVC_ENVIRONMENT_MANAGEMENT_GITHUB_TOKEN` is a read-only credential for
publication metadata, PR checks and artifacts. Configuration is rejected outside
management mode or without that credential. It must not enter child manifests,
operation records or redirected artifact-download requests. Invalid configuration
fails startup without echoing its contents. Without the installation file,
management still serves identity/readiness; environment routes return 503.

Each protected publication binds a `candidate_id` UUID to `source_sha`, `run_id`,
`run_attempt`, `artifact_id`, `artifact_sha256` and `pull_request`. New requests
verify the successful same-repository `dev` publication attempt, the exact merged
PR's squash SHA, and all four required GitHub-Actions-app checks on that PR head.
There is no assumption that CI ran on the subsequent `dev` push. The exact named
artifact must be unexpired, its downloaded bytes must match both pinned and GitHub
digests, and its seven image identities must match the configured registry.
Runtime image signatures, platform and vulnerability policy are checked with the
installation keyring. Evidence timestamps remain metadata, not a new image TTL.
Unknown candidates return 404; unavailable or invalid publication authority fails
closed with a sanitized 503.

The authenticated API supports:

- `POST /api/v1/environments`: `{slug, candidate_id}` and an `Idempotency-Key`;
  returns 202 with the durable operation UUID, not a readiness assertion.
- `GET /api/v1/environments`: this user's current team's retained registrations.
- `GET /api/v1/environments/{environment_id}`: desired registration and operation.
- `GET /api/v1/environment-operations/{operation_id}`: current operation state.
- `POST /api/v1/environment-operations/{operation_id}/retry`: explicitly retry the
  owner's current blocked operation without changing its plan or identities.
- `POST /api/v1/environments/{environment_id}/operations`: an `Idempotency-Key`
  and `{action: "destroy_retained", expected_generation}` request retained cleanup.
- `POST /api/v1/environments/{environment_id}/login`: return an environment-bound,
  90-second one-use child login proof, never the management or child admin token.

The actual user/team and scopes come from existing authentication; owner fields
in the body are rejected, generic credentials without a user are insufficient,
and cookie mutations retain CSRF enforcement. Another owner's lookup is forbidden.
Migration `0155` adds a cluster-locked platform allowance/reservation and ordered
operation/resource journal. Creation atomically reserves all namespaces, resource
costs, registration and immutable resource intents before any provider action.
Over-capacity requests return 409 `platform_capacity_exhausted` with needed and
available values. This is application/PVC accounting, not execution-pool admission.

Same-user, same-key, same-request retries recover the existing operation even if
publication is now unavailable; changed requests conflict. The journal uses
database-time expiring leases and increasing runner epochs to reject stale or
out-of-order confirmations. Provider identities cannot change on replay, and
completion requires all steps through application readiness. Kubernetes creation
is create-only with exact frozen-field/ownership and UID readback. Native IAM
effects have individual intents and deterministic idempotency keys. Credentials
are encrypted atomically with their journal confirmation before immutable child
Secrets are published. Each child gets distinct DB roles, TLS, session/secret-store
keys, admin/collector/batch credentials and canonical/source/backup object identities;
the installation's model-provider credentials are not copied. IAM permissions are
bucket-scoped, not project-wide data grants. Database TLS leaf certificates last
365 days; rotation remains a lifecycle obligation, not an automatic immutable-Secret
feature.

Readiness waits for database/migration, all four application Deployments, configure
completion and an authenticated exact-owner readback over the child's public HTTPS
host. The child loads its protected identity from
`LOOM_SVC_MANAGED_ENVIRONMENT_CONFIG_FILE`. Owner enrollment creates a non-platform-
admin identity without copying a password. Management-issued proof is consumed by
the child's existing `/api/v1/auth/login/complete` route, creating a new child
session. Proof expiry is checked after database locks; concurrent replay cannot
create two sessions. Login requires a mutation-capable management user session;
an attributed bearer must also carry every child-owner scope (`read:own`, `submit`,
`tokens:manage`, `providers:manage`, `team:manage`). Read-only or attenuated bearer
credentials cannot be exchanged for owner authority. The CLI/browser context-
selection UX is not yet implemented.

Retained destroy advances the desired generation immediately, fencing earlier
workers and management login issuance. It revokes the ready child's owner/team
identity and delivered object access keys, closes Pod admission with zero-Pod
quotas, suspends Jobs/CronJobs and scales application/database controllers to zero.
Controller names stay occupied by stopped objects so delayed create requests cannot
restart them. Changes test both UID and resource version; foreign/replaced/drifted
objects block cleanup. Discovered backup Jobs and completed Pods are journaled by
UID before their cleanup. Completion requires stopped-controller readback, complete
Pod inventory and enforced zero-Pod quota usage. Only then are CPU/RAM/ephemeral
reservations released. Namespaces, PVCs, buckets, storage reservations and name claims
remain; there is no data purge, slug reuse or automatic result-expiry policy.
Quota scopes and selectors must match the frozen intent, not merely contain its
fields: a scoped zero-Pod quota is not evidence that all Pod admission is closed.

After logging in to the selected management origin, the request/status commands are:

```sh
loom dev create alice --candidate <approved-candidate-uuid> --idempotency-key create-alice-1
loom dev list
loom dev status <environment-uuid>
loom dev wait <operation-uuid> --timeout 60
loom dev destroy <environment-uuid>
loom dev retry <blocked-operation-uuid>
```

`loom service up --environment dev-alice --candidate <approved-candidate-uuid>`
dispatches the same create request. It is not yet an update command or arbitrary
source deployment. Reuse the printed idempotency key after a lost response; `wait`
timeout exits 2 without cancelling the operation. Hosted-target errors never fall
back to local Compose. No target (or explicit `--environment local`) retains local
Compose and prints that target. Destroy reads the current generation unless
`--expected-generation` is supplied and prints the exact generation/idempotency-key
retry command before mutation. Retained destroy is not suspend/resume or data purge.
Explicit retry retains the runner epoch and all confirmed resource identities;
pending/running/completed calls are no-ops. After the bounded automatic retry budget
is exhausted, each explicit retry permits one additional reconciliation. It cannot
revive a generation superseded by destroy or authorize adoption of a replaced object.
Suspend/resume/update, arbitrary-source publication, shared execution and
shared-target management remain separate delivery work.

## Supported workload boundary

Native Kubernetes execution is the hosted path. OLDLAB, GB10, Slurm and remote
shared-cluster workers are retired, with no fallback route. Desktop/GUI and
Behavior GPU hosted workloads are unsupported. Other task and pipeline classes
require conversion according to the
[compatibility inventory](../evidence/service-workload-compatibility-v2.json).
Local execution and retained result access remain supported. Repository
retirement does not claim workload parity or implement those replacements.

Durable Trial/attempt identity, generation fencing, verifier rewards,
trajectories, artifacts, usage and provenance survive compute cleanup.
Kubernetes completion alone does not establish successful Loom finalization.
The [retirement record](../historical/shared-cluster-retirement-2026-09.md)
records the removed architecture and remaining compatibility obligations.

## Delivery and acceptance

Feature branches start from `dev`; `main` is reserved for release promotion.
[Contribution policy](../../CONTRIBUTING.md) owns required checks and merge
rules. [CI](../contributing/ci.md) describes validation selection;
[candidate publication](../runbooks/nebius-candidate.md) records commit identity
and immutable image references for deployment and rollback.

Source merge and credential-free CI do not establish live workload, recovery,
capacity or migration acceptance. Such evidence belongs to an exact candidate,
environment and explicitly authorized operation. No documentation cleanup
authorizes infrastructure shutdown, credential revocation or live data changes.
Workload qualification and operational acceptance remain tracked by #1550 and
#1538. Staging/production rollout automation still requires environment inputs
and approval wiring; see the retirement record for that boundary.

## Database lineage when moving from the isolated branch

The branches independently used revisions `0133`–`0135` for different changes.
`dev` keeps its published history through `0143`; the Nebius reward projection,
zero-quota observation and native-build observation migrations are appended as
`0144`, `0145` and `0146`. Fresh databases and existing `dev` databases upgrade
through that single chain. Nebius subsequently added native resource usage at `0136`;
`dev` preserves its published migrations through `0149` and appends native usage as `0150`.
The deployed Nebius series through `d07718e2` is retained by the conversion candidate.

An existing isolated-branch Nebius database at `0133`, `0134`, `0135` or `0136` is **not**
a database at the corresponding `dev` revision. Do not run this checkout's
normal upgrade against it or stamp it to a `dev` revision: that could skip
required schema changes. Moving an existing Nebius deployment requires
the [qualified lineage conversion](../runbooks/nebius-lineage-conversion.md), with
backup/restore evidence,
before selecting this `dev` candidate for deployment. Keep the previous
branch-bound candidate for that deployment until the conversion is qualified.

## Native task-image capacity fairness

Native build and trial admission share the existing capacity transaction lock,
placement model and provider quota identities. A lock alone does not prevent a
new trial from overtaking a builder whose capacity reservation was rejected.
The controller therefore retains one renewable waiting head per target in
`task_image_capacity_waits`, without consuming an attempt, retry budget, create
slot or cost reservation. Claim/render/admission run in a savepoint; a rejected
claim rolls back before the waiting record commits under the same outer lock.

A waiting record expires after 120 seconds unless a controller renews it after
validating actual demand and a realizable native shape. Cancellation, changed
materialization epoch, disabled target/policy and incompatible resource evidence
invalidate it. Compatible historical allocatable samples remain usable after
scale-to-zero. An impossible node/allowance combination cannot block other work.
An observed Ready node can also establish fit after its managed Pods drain,
without a compatible cold-node sample. Unknown foreign/DaemonSet resource and
slot occupancy remains charged; this does not establish cold-node capacity.
Changing a claim's epoch, target/pool or resource envelope loses its old waiting
priority and rejoins at the tail.

New admissions preserve waiting headroom in bin-packing, pending/create limits
and shared native quota accounting, including independent CPU pools sharing SSD
quota. Waiting itself is not an actual create or node-cost event. A builder
excludes its own head and respects older heads; already committed reservations
retain their precedence. Real reservations remain charged until UID-fenced
cleanup, even after cancellation or lease expiry. No running trial is preempted
and no machine is permanently reserved.

Fairness acceptance requires the matching controller and all capacity-admission
writers to be deployed. Mixed-version rollout and fixture tests alone do not
prove live no-overtaking behavior. This admission mechanism does not certify
Phase 2 rootless containment, signed publication or ARM support; those retain
their separate activation requirements.
