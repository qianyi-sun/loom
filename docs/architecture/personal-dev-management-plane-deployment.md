# Personal development management-plane deployment

## Status and purpose

The personal candidate, lifecycle, builder, activation, teardown, capacity
projection, and render-only shared-management package are implemented, but an
ordinary Loom deployment leaves the personal mutation paths disabled. The
package includes deterministic preparation of one exact release-bound scanner
cache. Shadow deployment, bounded acceptance, and durable operational launch
remain distinct controlled operations.

This design adds that package without reintroducing a shared development
application. `loom-dev` contains trusted shared infrastructure only. Candidate
Control Plane, Gateway, Service, web, migration, and capacity-agent workloads
exist only in `loom-dev-<owner>` namespaces created through the personal
lifecycle.

The package does not activate physical capacity. The global capacity manager
remains the only allocation authority across OLDLAB and GB10, and its
executable new-capacity ceiling must remain exactly zero throughout the live
personal-application acceptance and the initial durable operational mode.

## Current boundary

The following repository capabilities already exist:

- `loom service up --environment dev-<name>` seals arbitrary committed,
  modified, deleted, and permitted untracked source into an immutable personal
  candidate;
- the management `loom-service` can build, prepare, activate, update, resize,
  and destroy owner-bound personal environments;
- the restricted builder, stable-route activation agent, and per-person
  capacity agent have independent authorities;
- `min_slots` defaults to `0`, users choose no pool weights, and physical pool
  selection comes from task constraints plus global-manager allocation; and
- the capacity-manager package separately renders a shadow or prepared
  zero-ceiling global authority in `loom-dev`.

The dedicated package is the deterministic, reviewable deployment of the
personal management service and its shared storage, credential, RBAC, network,
migration, scanner, builder, and activation dependencies. The generic cluster
renderer is not that capability: it renders a shared Control Plane, Gateway,
web application, and other components that do not belong in `loom-dev` under
the accepted personal topology. The old rollout broker is also not a suitable
base: its acting path remains staging-specific and is being retired.

## Decision

Add a dedicated render-only `personal-dev-control-plane` operator package. It
has a narrow input schema, emits only the shared management plane, and exposes
read-only status. It has no apply operation and no physical-capacity activation
operation.

The package is delivered in three repository increments:

1. **Shadow foundation.** Render storage, migration, management service,
   restricted-builder authority, activation-agent authority, RBAC, and network
   policy with personal lifecycle mutation disabled and the activation agent
   at zero replicas. Prove deployment and rollback at this state.
2. **Zero-capacity personal acceptance interlocks.** Accept one canonical,
   digest-pinned operator plan and render the same release with the personal
   controller and builder enabled and the activation agent at one replica.
   Runtime startup and operator status both recheck the global manager and
   refuse any nonzero executable ceiling. Schema v1 preserves the #1280
   sole-owner/two-environment authority; schema v2 authorizes exactly two
   distinct owners only through a separate gate before a second person is
   onboarded. Neither contract can allocate a worker.
3. **Durable zero-capacity operational contracts.** After the applicable
   acceptance runbook has retired every owner-controlled environment and
   restored the reviewed shadow, accept a separate canonical operational plan.
   `render-operational` enables the same personal application authorities
   without retaining the expiring acceptance binding. `status-operational`
   continuously verifies the immutable release and plan, the global-manager
   authority and execution boundary, executable ceiling `0`, valid dynamic
   namespace ownership, and zero personal workers. A successful operational
   launch may remain enabled; it still cannot execute a task.

The global manager's later prepared-to-active and one-slot execution interlock
remains a separate #906/#822 package and review. Operational personal
application readiness is not evidence that x86_64, arm64, or
architecture-neutral task capacity is installed, active, or accepted.

## Topology

The renderer emits these resources, all pinned to the exact `loom-dev`
namespace unless Kubernetes requires cluster scope:

- the shared `loom-dev` Namespace under the same `loom-operator` ownership
  label as the global-capacity package, with package-specific render evidence
  kept in annotations instead of competing for Namespace ownership;
- `loom-dev-postgres` StatefulSet, headless/service endpoints, and persistent
  volume claim for the management database plus derived personal databases;
- `loom-dev-minio` StatefulSet, Service, and persistent volume claim for source,
  build, evidence, and personal buckets; its pinned client sidecar creates the
  base `artifacts` and `trajectories` buckets idempotently over loopback and
  withholds readiness until both exist;
- an immutable, release-bound migration Job for the management database, with
  a bounded database wait and a dedicated DNS/PostgreSQL-only egress policy;
- a 20 GiB scanner-cache PVC plus the immutable
  `personal_dev_scanner_cache` image. A credential-free init container alone
  mounts the PVC root and atomically prepares the exact digest-named generation;
  management mounts only that generation and overlays its `fanal/` directory
  with a bounded disposable volume. The complete design is the
  [scanner-cache preparation architecture](personal-dev-scanner-cache-preparation.md);
- `loom-service` as the management API and lifecycle reconciler, using the
  trusted release's immutable service image;
- the existing Loom React SPA as `loom-personal-dev-web`, using the trusted
  release's immutable Web image. The public Ingress sends `/api` to the
  management Service and `/` to this stateless Web Service, so `/auth/reset`
  and the other account routes use the single existing frontend and REST API;
- `loom-personal-dev-activation-agent`, using its separately published immutable
  image and private signing authority;
- service accounts and least-privilege RBAC for the management service,
  restricted builder jobs, and activation agent;
- NetworkPolicies that allow only the required DNS, PostgreSQL, MinIO, registry,
  global-manager, ingress, and personal-namespace control paths, with separate
  PostgreSQL and MinIO ingress policies so migration and builder callers cannot
  inherit each other's storage access. A package-owned additive policy selects
  only the global capacity-manager Pod and admits TCP 8443 only from the
  personal-management Pod; it does not replace or widen the capacity package's
  ingress policy. A separate additive policy selects only cert-manager's exact
  HTTP-01 solver label and admits the deliberately public ACME challenge only
  on the profile-pinned TCP 8089 solver port while retaining default-deny
  egress. It does not constrain source identity because cross-node VXLAN and
  host-network ingress paths do not preserve a stable source identity at the
  destination NetworkPolicy boundary. The management ingress policy uses the
  same source-independent boundary: it selects only the management Pod, is
  ingress-only, admits only TCP 8090, and grants no egress. TLS and application
  authentication/authorization remain the public management boundary. The
  legacy `ingress_controller_source_cidrs` profile input is accepted only so a
  preserved rollback profile remains loadable and does not affect rendering;
  and
- internal Services plus one operator-configured public Ingress with the exact
  `/api` management and `/` Web path split. No shared Control Plane, Gateway,
  worker, family orchestrator, or pipeline orchestrator is rendered.

The personal builder image is not a long-lived Deployment. The management
service supplies its exact immutable reference to attempt-scoped Jobs. The
candidate-independent capacity agent uses the exact immutable `loom-service`
image and is installed only inside an activated personal namespace by the
existing lifecycle.

The management principal has two disjoint admission-constrained namespace
families: `loom-dev-*` for personal applications and `loom-build-*` for
attempt-scoped builder sandboxes. Builder namespaces admit only their existing
ResourceQuota, LimitRange, ConfigMap, capability Secret, NetworkPolicy, and Job
contract; they cannot host a personal application or alter shared
infrastructure. The activation principal remains limited to `loom-dev-*` and
receives authority there only through the per-namespace RoleBinding rendered
by the lifecycle. Every resource mutation rechecks the complete namespace
shape, reserved-name exclusion, exact family resource name, and managed-by
label. Personal application objects use `loom-dev-instance-controller`, while
the separately fenced capacity-agent objects retain
`loom-personal-dev-lifecycle`; builder objects use
`loom-personal-dev-builder-controller`. A broad prefix or a pre-existing
malformed namespace cannot bypass the policy.

The namespace-local management role can get only the four fixed lifecycle
Secrets; it cannot list or watch arbitrary application or TLS Secrets.
Management may prepare only generation-suffixed candidate Services and has no
Ingress mutation verb. Stable Services and Ingresses remain exclusive to the
candidate-independent activation principal. Its admission contract binds the
namespace owner and deployment generation, permits only internal ClusterIP
Services with exact selectors and ports, and fixes every Ingress host, backend,
TLS Secret, class, and annotation. Admission applies the same fixed Secret set
to personal Deployment and Job volume/environment references, rejects
projected or CSI Secret paths, and keeps API-token automount disabled.
Builder Jobs are likewise limited to attempt-capability Secrets and the
unprivileged default service account, without projected API credentials. Both
workload families reject `imagePullSecrets`, closing the remaining indirect
Secret reference path.

Admission deliberately does not duplicate every field of the dynamic
Deployment, Job, ConfigMap, quota, or NetworkPolicy constructors in CEL. The
digest-pinned, CI-approved management service is the trusted manifest
generator: arbitrary uploaded source enters only the attempt-scoped builder
and then the fixed personal component images. It cannot supply Kubernetes
objects, labels, RBAC, Secret references, service accounts, routes, or network
policy. Admission independently fences the authority-bearing boundaries above
so a personal image has no Kubernetes credential and cannot widen them. A
future threat model that treats the management release itself as untrusted
would require a separate signed-manifest admission authority; partially
reimplementing its constructors in CEL would not contain a process that
already holds the lifecycle, publisher, and shared-storage credentials.

The builder RuntimeClass is an independently installed measured gVisor cluster
capability, specified by the
[builder runtime design](personal-dev-builder-runtime.md) and installed through
its [protected rollout](../runbooks/personal-dev-builder-runtime.md). It does
not supply Kubernetes host user namespaces. The shadow package records the
exact class, handler, and runtime-profile digest and prepares the release-bound
scanner cache, while builder preparation remains false. The protected trusted
release binds the scanner binary, both databases and metadata files, cache
identity, checked-in source lock, and immutable cache image. The later
acceptance and operational plans additionally bind the same RuntimeClass
profile and finding policy. The launcher profile and scanner finding policy are
canonical artifacts rendered from the exact trusted-release checkout: they
bind the release source identity, the authority-bearing launcher source files,
the fixed offline scanner argv and rejection fields, and the complete scanner
release identities. Acceptance and operational render/status rederive them and
reject arbitrary files, source drift, semantic drift, or digest drift. Shadow,
acceptance, and operational status all fail until the observed handler,
annotation, and complete scheduling selector match.

## Configuration and immutable release binding

`deploy/dev-fleet/personal-dev-control-plane.toml` is the non-secret live
profile. The loader accepts one current-user-owned, single-link regular file,
pins its descriptor identity across the bounded read, and rejects symlink,
replacement, or in-place races. Its fixed invariants are:

- namespace: `loom-dev`;
- personal namespace prefix: `loom-dev-`;
- minimum slots default: `0`;
- user-visible maximum slots: finite and at most `8` per environment;
- pool capabilities: exactly OLDLAB x86_64 and GB10 arm64, with no weight;
- in-cluster worker and personal Control Plane Slurm actuator: disabled;
- personal candidate scope: `personal-dev-only`, never promotable; and
- executable new-capacity ceiling: `0`.

The render command requires complete digest references for:

- `loom-service`;
- `loom-web`, represented by `loom_web` in trusted-release schema 3;
- `loom-personal-dev-builder`;
- `loom-personal-dev-activation-agent`;
- `loom-personal-dev-scanner-cache`, represented by
  `personal_dev_scanner_cache` since trusted-release schema 2;
- PostgreSQL; and
- MinIO plus the separate MinIO client sidecar used for bounded tenant
  administration.

Mutable tags, missing architectures, zero digests, duplicate repositories, or
an image set from different trusted releases fail before YAML is written. The
trusted release record binds the exact source commit, source tree, per-platform
members, final multi-architecture digests, checked-in scanner lock, scanner
binary, database files and metadata, and framed cache identity. Every managed
workload records the canonical render-input digest and trusted-release digest.
Schema 3 is the current forward format. The loader retains narrow schema-1
profile/schema-2 release compatibility only to re-render and verify a preserved
pre-Web rollback manifest; it never synthesizes a Web image or changes the old
API-only route.
The final YAML digest is external evidence because embedding it in the YAML
itself would be a self-referential hash; the later acceptance plan binds that
external digest.

Shadow rendering is the default. Acceptance rendering additionally requires an
owner-only canonical JSON plan and its independently supplied SHA-256. The plan
binds:

- exact source commit and tree;
- all image digests and the shadow manifest digest;
- management schema head and storage backup/restore evidence digest;
- activation public-key digest and separate agent key identifier;
- builder RuntimeClass/profile, scanner, offline database, finding policy,
  publisher, registry prefix, and protocol-map digests;
- global-manager authority incarnation, configuration epoch, execution state,
  execution epoch, and executable ceiling `0`;
- exact lifecycle and reporter principal identifiers;
- one exact acceptance-owner identifier pair for historical schema v1, or two
  canonically ordered distinct owner pairs for schema v2, plus finite quotas;
  and
- change-window, rollback-manifest, and expiry timestamps.

Operational rendering is a separate third mode. It requires an owner-only
canonical operational plan and its independently supplied SHA-256. The
operational plan binds the same immutable source, release, storage,
RuntimeClass, scanner, publisher, registry, activation, quota, principal, and
global-manager zero-capacity boundaries needed for acceptance. It also binds
the exact byte-reviewed shadow rollback manifest. For the multi-owner durable
launch route, it binds the completed, strictly verified schema-v2 two-owner
acceptance result. The preserved #1280 sole-owner/two-environment route instead
binds its historical schema-v1 result under the separately reviewed sole-owner
durable-launch procedure. Neither result substitutes for the other: v1 cannot
authorize final multi-person launch, and v2 does not retroactively authorize
the #1280 window. The operational plan does not contain acceptance owners or
an acceptance window, and an operator must not simulate durability by choosing
a distant acceptance expiry. Configuration-epoch advancement is permitted
only as the same monotonic progress accepted during the bounded test; authority
incarnation, execution state and epoch, observer principal, and executable
ceiling remain exact fail-closed bindings.

The file must be a current-user-owned, non-symlink, single-link regular file
with mode `0600`, canonical JSON bytes, no trailing newline, and an exact digest
match. It is control evidence, not a credential.

The storage evidence is produced by the
[personal-development backup and isolated restore procedure](../runbooks/personal-dev-backup-restore-evidence.md).
It dumps live Postgres and proves the live MinIO buckets empty while the plane
is in ready shadow with ceiling zero and no personal worker, restores both into
private disposable Docker resources using the exact trusted-release images,
and compares schema, per-table row counts and canonical-row digests, exact
sequence state, and the required empty MinIO bucket set. Nonempty object state
fails closed because the pinned streaming client cannot preserve Loom's
required content type and custom metadata. The canonical record contains only
state digests, Secret names/key-name inventory digest, fixed PVC identities,
zero-capacity observations, and cleanup proof. It rejects Secret values and
cannot be substituted by readiness, object counts alone, or an operator-authored
claim.

Repository implementation does not establish the live prerequisites by
itself. DNS and TLS, provisioned Secret inventories, the measured gVisor
RuntimeClass, candidate GHCR publication, backup/restore evidence, the
applicable verified acceptance gate (schema v1 for the reviewed #1280
sole-owner route, or schema v2 before a multi-person launch), and a successful
operational apply/status remain separate operational gates. Neither acceptance
schema supersedes the other route's authority. None of these gates may be
inferred from a successful render or from scanner-cache preparation, and the
executable ceiling remains zero throughout them.

## Credential boundaries

The renderer never creates Secret values. The operator provisions three
pre-reviewed Secrets through the approved secret channel:

1. `loom-personal-dev-management` contains the bounded management database,
   shared-storage, registry publisher, admin-verifier, and capacity lifecycle
   and reporter files.
2. `loom-personal-dev-activation-public` contains only the activation public
   key consumed by the management service.
3. `loom-personal-dev-activation-agent` contains only the matching activation
   private key consumed by the independent agent.

Lifecycle and reporter mTLS identities are distinct. The management pod never
receives the activation private key. The activation agent never receives
database, MinIO root, registry publisher, capacity lifecycle, or capacity
reporter authority. Candidate and builder pods receive neither authority.

Credential init containers copy file-shaped credentials from one pinned
Kubernetes Secret projection into memory-backed runtime directories, require the exact file-key
set, and install owner-only regular files. They detect projection-generation
changes and fail closed rather than mix credentials. Application containers
mount only the copied directories read-only; they do not mount the raw
projected Secret. Scalar settings already consumed through the schema-backed
service environment (database URLs, MinIO keys, and the SecretStore master key)
use bounded `secretKeyRef` entries and never appear in rendered YAML or command
arguments.

The registry credential is projected and copied as `config.json`, the exact
filename required by Docker-compatible tooling; no symlink or post-copy rename
bridges a differently named Secret key.

## State transitions and failure handling

### Shadow

The shadow manifest sets `LOOM_SVC_DEV_INSTANCES_ENABLED=false`,
`LOOM_SVC_PERSONAL_DEV_BUILDER_ENABLED=false`, and activation-agent replicas to
`0`. Migration and shared storage may be proven, but no personal candidate can
be accepted and no stable personal route can be changed.

`loom admin personal-dev-control-plane status` succeeds only when:

- every observed image and manifest digest matches the render;
- storage is ready and the migration Job completed at the exact schema head;
- the release-bound scanner cache init completed and management mounts only the
  expected digest-named generation;
- credential init completed without exposing Secret bytes;
- the management service is healthy with both mutation features disabled;
- the activation agent is absent or scaled to zero;
- the global manager reports a healthy executable ceiling of `0`; and
- no unexpected `loom-dev-*` or `loom-build-*` namespace or package-owned
  cluster-scoped binding exists.

The status command accepts only an owner-only, flattened, self-contained
kubeconfig. External certificate, key, or token files, legacy auth providers,
and exec credential plugins are rejected before observation. Every kubectl
process receives a read-only anonymous snapshot of the exact validated bytes,
so a path or in-place rewrite cannot change the authority it consumes.

### Zero-capacity acceptance

The acceptance manifest uses the same immutable release and persistent storage,
pins the canonical acceptance plan, enables the personal controller and
restricted builder, and scales the activation agent to one. Before becoming
ready, the management service rehashes the installed scanner binary, both
databases and metadata files, and canonical cache identity before constructing
builder, registry, or Kubernetes clients. It also validates all credential
inputs and connects to the exact global-manager identity. Any missing or
changed binding, stale plan, nonzero manager ceiling, or unready dependency
keeps the service unready and the agent unable to obtain an intent.

Acceptance status distinguishes application readiness, capacity publication,
and worker availability. For this phase, application and non-executable
capacity publication may become ready; `worker_available` must remain false.

Acceptance is not a steady-state mode. Its finite window and owner-specific
evidence exist to prove the behavior under test. After the acceptance owner completes
create, concurrent two-environment build, update, isolation, destroy, retained-data redeploy,
and final destroy, the acceptance runbook reapplies the exact shadow it
reviewed before forward mutation. Leaving the acceptance manifest active past
that procedure is configuration drift even if its expiry has not yet arrived.

### Durable zero-capacity operational mode

The operational manifest uses the same shared storage identities and immutable
release, enables the personal controller and restricted builder, and scales the
activation agent to one. It is generated only by `render-operational` from the
canonical operational plan after the complete acceptance-and-shadow-rollback
result exists. The service continuously verifies the plan's global-manager
authority, monotonic configuration-epoch floor, exact execution state and
epoch, and executable ceiling `0`; it does not rely on an acceptance expiry.

`status-operational` succeeds only when the shared package matches the exact
render, management and activation are ready with the intended authorities,
all observed `loom-dev-*` and transient `loom-build-*` namespaces satisfy their
managed identity contracts, the global manager still satisfies the operational
binding, the ceiling is `0`, no personal worker exists, and
`worker_available=false`. DNS, certificate, and authenticated stable-route
checks remain live launch evidence outside the Kubernetes render.

This mode supports persistent personal application development only. A healthy
route, successful source build, ready personal namespace, or non-executable
demand publication does not prove task scheduling, model execution, GPU
capacity, or #906/#822 acceptance. A task-capacity transition requires its own
protected plan, executors, canaries, status, and rollback.

### Rollback

Rollback reapplies the byte-reviewed shadow manifest for the same release. It
disables new personal operations and stops activation polling without deleting
storage. Existing acceptance namespaces are retired through the normal
manager-first destroy path before shared control-plane rollback. If a partial
operation cannot be retired safely, the operator retains the zero-capacity
state and does not delete its namespace, database, buckets, or evidence.

No rollback step changes the global-manager ceiling or restarts a legacy fleet
writer.

Operational rollback uses the same ordering. Stop admitting new operations,
retire every personal environment through its authenticated manager-first
destroy operation, wait until all `loom-dev-*` and `loom-build-*` namespaces
are gone, and only then reapply the byte-reviewed shadow manifest. Never delete
a namespace or PVC directly. If an environment cannot be retired safely, keep
the zero-capacity operational plane fail closed, retain its data and evidence,
and do not apply shadow over unresolved dynamic authority.

## Live acceptance authorities

The #1280 owner may complete the approved
[sole-owner zero-capacity acceptance](../runbooks/personal-dev-zero-capacity-acceptance.md)
and corresponding
[sole-owner durable launch](../runbooks/personal-dev-durable-launch.md). That
sole-owner/two-environment procedure proves one authenticated owner can exercise
two isolated arbitrary-source deployments at ceiling zero; it does not certify
cross-owner isolation.

Before a second person is onboarded, the plane must return to the exact inert
shadow and a separately reviewed window must execute the
[concurrent-owner zero-capacity acceptance](../runbooks/personal-dev-concurrent-owner-zero-capacity-acceptance.md):

1. Two exact plan owners authenticate through separate pinned owner-only XDG
   roots using existing non-rotating user-owned API bearer tokens, and each
   secret-free identity record matches its canonical v2 plan entry. Browser,
   legacy team, service, and administrator credentials fail this bounded gate.
2. Both owners start distinctly named arbitrary-source deploys before either
   wait, using exact minimum `0` and maximum `2`.
3. Both owners start independent arbitrary-source updates before either wait;
   owner 0 moves to maximum `3` and owner 1 to maximum `4`.
4. Candidate, subject, incarnation, database, bucket, host, route, worker-pool,
   and namespace identities remain disjoint across owners.
5. Owner 0 attempts read, update, and destroy against owner 1; then owner 1
   attempts the same three operations against owner 0. Every call exits 1,
   emits no stdout, emits the exact method/phase-bound hidden-resource receipt,
   and leaves byte-exact target status unchanged.
6. Application readiness and non-executable demand publication remain ready
   after every denial, while worker availability is false and the manager
   ceiling remains `0`.
7. Owner 0 destroys normally. Owner 1 destroys with `--keep-data`, redeploys
   the retained name with the same `subject_id` and a rotated
   `subject_incarnation`, then destroys it normally.
8. With every dynamic namespace absent, the operator reapplies and verifies the
   byte-exact inert shadow, assembles the canonical v2 result, and runs the
   strict read-only result verifier.

Physical one-slot x86_64, arm64, and architecture-neutral task execution is not
part of this acceptance. It follows the separately reviewed #906 activation
interlock after both pool executors and no-dual-writer evidence are complete.

After all eight steps, the concurrent-owner acceptance runbook has completed
final manager-first cleanup, returned to the byte-reviewed shadow, and produced
a verified schema-v2 result. Multi-person durable launch is a subsequent
operation under the
[multi-owner durable launch runbook](../runbooks/personal-dev-multi-owner-durable-launch.md):
render and review both operational and shadow manifests, apply operational,
prove `status-operational`, DNS/TLS, and stable-route behavior, and retain the
operational manifest only while ceiling `0` and `worker_available=false` remain
true.

## Verification

Repository verification includes:

- strict TOML, acceptance-plan, and operational-plan model tests;
- render snapshots and YAML schema/identity tests;
- rejection of mutable/zero/mixed-release images, unsafe evidence files,
  incomplete Secret key sets, broad RBAC, shared application workloads,
  `loom-dev-shared`, nonzero ceilings, pool weights, and enabled in-cluster or
  personal Slurm controllers;
- credential projection race, ownership, mode, link, size, and exact-key tests;
- atomic scanner generation, protected ownership/mode, startup rehash, render,
  and status-drift tests;
- shadow, acceptance, and operational status matrices with manager
  identity/ceiling drift;
- startup tests proving disabled shadow behavior, fail-closed acceptance, and
  durable zero-capacity operational behavior;
- server-side diff tests against an isolated disposable cluster; and
- the existing personal source, candidate, builder, reconciler, activation,
  capacity, teardown, migration, package-boundary, and secret-scan suites.

No test or renderer uses a live production database, registry credential,
Kubernetes Secret value, Slurm mutation, or scheduler submission.
