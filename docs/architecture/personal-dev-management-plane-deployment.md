# Personal development management-plane deployment

## Status and purpose

The personal candidate, lifecycle, builder, activation, teardown, and capacity
projection code is implemented, but an ordinary Loom deployment leaves it
disabled. The repository currently has no protected package that deploys the
shared management plane needed to exercise those paths with two owners.

This design adds that package without reintroducing a shared development
application. `loom-dev` contains trusted shared infrastructure only. Candidate
Control Plane, Gateway, Service, web, migration, and capacity-agent workloads
exist only in `loom-dev-<owner>` namespaces created through the personal
lifecycle.

The package does not activate physical capacity. The global capacity manager
remains the only allocation authority across OLDLAB and GB10, and its
executable new-capacity ceiling must remain exactly zero throughout the live
personal-application acceptance.

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

The missing operational capability is a deterministic, reviewable deployment
of the personal management service and its shared storage, credential, RBAC,
network, migration, builder, and activation dependencies. The generic cluster
renderer is not that capability: it renders a shared Control Plane, Gateway,
web application, and other components that do not belong in `loom-dev` under
the accepted personal topology. The old rollout broker is also not a suitable
base: its acting path remains staging-specific and is being retired.

## Decision

Add a dedicated render-only `personal-dev-control-plane` operator package. It
has a narrow input schema, emits only the shared management plane, and exposes
read-only status. It has no apply operation and no physical-capacity activation
operation.

The package is delivered in two repository increments:

1. **Shadow foundation.** Render storage, migration, management service,
   restricted-builder authority, activation-agent authority, RBAC, and network
   policy with personal lifecycle mutation disabled and the activation agent
   at zero replicas. Prove deployment and rollback at this state.
2. **Zero-capacity personal acceptance interlock.** Accept one canonical,
   digest-pinned operator plan and render the same release with the personal
   controller and builder enabled and the activation agent at one replica.
   Runtime startup and operator status both recheck the global manager and
   refuse any nonzero executable ceiling. This increment supports #1280's
   two-owner application acceptance but still cannot allocate a worker.

The global manager's later prepared-to-active and one-slot execution interlock
remains a separate #906 package and review.

## Topology

The renderer emits these resources, all pinned to the exact `loom-dev`
namespace unless Kubernetes requires cluster scope:

- `loom-dev-postgres` StatefulSet, headless/service endpoints, and persistent
  volume claim for the management database plus derived personal databases;
- `loom-dev-minio` StatefulSet, Service, and persistent volume claim for source,
  build, evidence, and personal buckets;
- an immutable, release-bound migration Job for the management database;
- `loom-service` as the management API and lifecycle reconciler, using the
  trusted release's immutable service image;
- `loom-personal-dev-activation-agent`, using its separately published immutable
  image and private signing authority;
- service accounts and least-privilege RBAC for the management service,
  restricted builder jobs, and activation agent;
- NetworkPolicies that allow only the required DNS, PostgreSQL, MinIO, registry,
  global-manager, ingress, and personal-namespace control paths; and
- internal Services plus one operator-configured management API Ingress. No
  shared Control Plane, Gateway, worker, family orchestrator, pipeline
  orchestrator, or web Deployment is rendered.

The personal builder image is not a long-lived Deployment. The management
service supplies its exact immutable reference to attempt-scoped Jobs. The
candidate-independent capacity agent uses the exact immutable `loom-service`
image and is installed only inside an activated personal namespace by the
existing lifecycle.

The builder RuntimeClass is an independently installed cluster capability. The
package records its exact name and expected handler/profile digest but does not
invent a runtime handler. Status fails until the observed RuntimeClass and the
operator-owned profile digest match the activation plan.

## Configuration and immutable release binding

`deploy/dev-fleet/personal-dev-control-plane.toml` is the non-secret live
profile. Its fixed invariants are:

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
- `loom-personal-dev-builder`;
- `loom-personal-dev-activation-agent`;
- PostgreSQL; and
- MinIO.

Mutable tags, missing architectures, zero digests, duplicate repositories, or
an image set from different trusted releases fail before YAML is written. The
trusted release record binds the exact source commit, source tree, per-platform
members, and final multi-architecture digests. Every managed workload records
the canonical render-input digest and trusted-release digest. The final YAML
digest is external evidence because embedding it in the YAML itself would be a
self-referential hash; the later acceptance plan binds that external digest.

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
- two distinct acceptance owner identifiers and finite quotas; and
- change-window, rollback-manifest, and expiry timestamps.

The file must be a current-user-owned, non-symlink, single-link regular file
with mode `0600`, canonical JSON bytes, no trailing newline, and an exact digest
match. It is control evidence, not a credential.

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

Init containers copy file-shaped credentials from one pinned Kubernetes Secret
projection into memory-backed runtime directories, require the exact file-key
set, and install owner-only regular files. They detect projection-generation
changes and fail closed rather than mix credentials. Application containers
mount only the copied directories read-only; they do not mount the raw
projected Secret. Scalar settings already consumed through the schema-backed
service environment (database URLs, MinIO keys, and the SecretStore master key)
use bounded `secretKeyRef` entries and never appear in rendered YAML or command
arguments.

## State transitions and failure handling

### Shadow

The shadow manifest sets `LOOM_SVC_DEV_INSTANCES_ENABLED=false`,
`LOOM_SVC_PERSONAL_DEV_BUILDER_ENABLED=false`, and activation-agent replicas to
`0`. Migration and shared storage may be proven, but no personal candidate can
be accepted and no stable personal route can be changed.

`loom admin personal-dev-control-plane status` succeeds only when:

- every observed image and manifest digest matches the render;
- storage is ready and the migration Job completed at the exact schema head;
- credential init completed without exposing Secret bytes;
- the management service is healthy with both mutation features disabled;
- the activation agent is absent or scaled to zero;
- the global manager reports a healthy executable ceiling of `0`; and
- no unexpected `loom-dev-*` namespace or package-owned cluster-scoped binding
  exists.

### Zero-capacity acceptance

The acceptance manifest uses the same immutable release and persistent storage,
pins the canonical acceptance plan, enables the personal controller and
restricted builder, and scales the activation agent to one. Before becoming
ready, the management service validates all builder and credential inputs and
connects to the exact global-manager identity. Any missing or changed binding,
stale plan, nonzero manager ceiling, or unready dependency keeps the service
unready and the agent unable to obtain an intent.

Acceptance status distinguishes application readiness, capacity publication,
and worker availability. For this phase, application and non-executable
capacity publication may become ready; `worker_available` must remain false.

### Rollback

Rollback reapplies the byte-reviewed shadow manifest for the same release. It
disables new personal operations and stops activation polling without deleting
storage. Existing acceptance namespaces are retired through the normal
manager-first destroy path before shared control-plane rollback. If a partial
operation cannot be retired safely, the operator retains the zero-capacity
state and does not delete its namespace, database, buckets, or evidence.

No rollback step changes the global-manager ceiling or restarts a legacy fleet
writer.

## Live acceptance

After the shadow rehearsal and acceptance interlock are merged, published, and
deployed, #1280 closes only after this controlled test:

1. Two distinct user/team owners authenticate to the management API.
2. Each owner deploys a distinct arbitrary source snapshot with
   `loom service up --environment dev-<name> --min-slots 0`.
3. Both builds and lifecycle operations run concurrently without shared mutable
   candidate, attempt, registry, database, bucket, credential, or namespace
   authority.
4. Each owner performs an independent source update and capacity-policy update.
5. Cross-owner list/detail/mutation, Secret, database, bucket, and namespace
   access is rejected.
6. Application readiness and initial non-executable demand publication succeed,
   while worker availability remains false and the manager ceiling remains `0`.
7. One owner destroys with data deletion and the other with `--keep-data`;
   neither operation mutates `loom-dev` or the sibling namespace.
8. The retained environment name is redeployed and proves authority rotation.

Physical one-slot x86_64, arm64, and architecture-neutral task execution is not
part of this acceptance. It follows the separately reviewed #906 activation
interlock after both pool executors and no-dual-writer evidence are complete.

## Verification

Repository verification includes:

- strict TOML and acceptance-plan model tests;
- render snapshots and YAML schema/identity tests;
- rejection of mutable/zero/mixed-release images, unsafe evidence files,
  incomplete Secret key sets, broad RBAC, shared application workloads,
  `loom-dev-shared`, nonzero ceilings, pool weights, and enabled in-cluster or
  personal Slurm controllers;
- credential projection race, ownership, mode, link, size, and exact-key tests;
- shadow and acceptance status matrices with manager identity/ceiling drift;
- startup tests proving disabled shadow behavior and fail-closed acceptance;
- server-side diff tests against an isolated disposable cluster; and
- the existing personal source, candidate, builder, reconciler, activation,
  capacity, teardown, migration, package-boundary, and secret-scan suites.

No test or renderer uses a live production database, registry credential,
Kubernetes Secret value, Slurm mutation, or scheduler submission.
