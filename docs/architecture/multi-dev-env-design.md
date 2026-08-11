# Multiple per-developer development environments

Status: target contract approved. The earlier lifecycle implementation is
inert and requires convergence onto Packages 2–5 of
`docs/architecture/global-fleet-capacity-manager-design.md`; live activation
is not authorized.

Refs: #1178, #1192, #1193, #906.

## User contract

`loom service up` always requires an explicit target. Local Compose remains
source-fresh, while a personal shared-fleet target seals and submits the
current allowed source contexts to the trusted personal-dev candidate
pipeline:

```text
loom service up --environment local
loom service up --environment dev-<name> [--candidate <digest>]
```

Staging and production selectors route only to their existing protected
rollout authorities. They never accept local or personal-dev-only source.

The existing `loom dev` commands remain lower-level authenticated lifecycle
interfaces:

```text
loom dev create <name> [--min-slots N] [--max-slots N] [--no-wait]
loom dev apply <name> --candidate <digest> --expected-operation-epoch N \
  [--min-slots N] [--max-slots N] [--no-wait]
loom dev list [--mine]
loom dev status <name>
loom dev destroy <name> [--keep-data] [--no-wait]
```

The CLI is a thin client over `/api/v1/dev-instances`. Mutations return a
durably claimed `202`; the service lifecycle runner uses an independent
database session to execute the operation. The CLI polls by default and
`--no-wait` returns the claimed `provisioning` or `deleting` state immediately.
Reissuing the command resumes the same fenced operation after a process
restart.

## Identity and ownership

The only client-selected identity is a validated lowercase `name`. Every
resource is derived server-side and cannot be overridden:

| Resource | Derived value |
|---|---|
| environment / worker pool | `dev-<name>` |
| namespace | `loom-dev-<name>` |
| database and role | `loom_dev_<name>` |
| buckets | `loom-dev-<name>-{tasks,trajectories,artifacts}` |
| service endpoint | `<name>.dev.yylx.world` |
| worker control plane | `cp-<name>.dev.yylx.world` |
| worker gateway | `gw-<name>.dev.yylx.world` |

The management database owns a `dev_instances` registry row containing the
owner, requested capacity ceiling, status, candidate SHA, deployment
generation, operation epoch/id, durable cleanup checkpoint, timestamps, and a
bounded failure reason. Reads are owner-scoped unless the caller is a platform
admin. Cross-owner detail and deletion return 404. Mutations require a real
submitting user; legacy shared team credentials and internal workers cannot
create an environment.

## Shared infrastructure with per-instance authority

One operator-owned `loom-dev` namespace runs the trusted development lifecycle
service, global capacity manager, candidate-builder coordinator, management
PostgreSQL, shared application PostgreSQL, and shared MinIO. It is
infrastructure, not a static shared application or capacity subject. There is
no `loom-dev-shared` namespace.

Per-instance manifests use `loom-dev-<name>` and contain the candidate
frontend, migration Job, Control Plane, Gateway, Loom Service, Services, and
Ingress, plus a trusted capacity agent and claim guard installed independently
from the candidate. They never render PostgreSQL, MinIO, persistent volumes,
shared-root credentials, or inline secrets.

Logical isolation is enforced by separate authorities, not naming alone:

- Postgres creates one login role and database, revokes `CONNECT` and
  `TEMPORARY` from `PUBLIC`, and grants them only to the derived role.
- MinIO creates three derived buckets and a dedicated user whose policy names
  only those bucket and object ARNs. The shared root credential remains in the
  fixture's management sidecar and is never copied to an instance namespace.
- Kubernetes stores the role DSN, bucket-scoped MinIO credential, JWT key,
  secret-store master key, and internal admin token in namespace-local
  Secrets. Secret values travel over stdin or bound database/HTTP parameters,
  never argv, manifests, logs, registry responses, or CLI diagnostics.
- The namespace enforces Restricted Pod Security. Runtime and migration pods
  disable service-account token mounting, run as non-root, drop capabilities,
  disallow privilege escalation, and use the runtime-default seccomp profile.

Arbitrary committed, uncommitted, and permitted untracked feature source is
sealed by content digest and built only in an attempt-scoped restricted
sandbox. The trusted pipeline validates infrastructure safety and publishes an
immutable `personal-dev-only` source/image/profile attestation; it does not
assert feature correctness. Personal candidates cannot be promoted to staging
or production.

The first Package 4 implementation slice supplies the producer and consumer
side of that source boundary. It inventories tracked plus non-ignored
untracked files from the exact Git worktree, omits deleted files and records
their provenance, excludes sensitive and ignored paths, and rejects links,
special files, hard-linked files, unsafe contexts, invalid paths, and bounded
size/count overflow. Every file is read through no-follow directory
descriptors with before/after identity checks. A second full scan after archive
creation rejects checkout races. The deterministic USTAR archive and canonical
manifest receive independent SHA-256 digests and carry the immutable
`personal-dev-only` scope.

The isolated-builder verifier opens the archive no-follow, checks its expected
artifact digest before parsing, enforces canonical metadata, exact tar size,
zero-filled trailer, and limits without extracting paths, revalidates every
member against the manifest, and checks the expected source digest.

The authenticated-intake Package 4 slice makes the management service
streams the upload into a bounded no-follow temporary file, verifies both
digests and the canonical archive independently, publishes the verified bytes
under an owner-scoped content-addressed object key, and idempotently records an
immutable `uploaded` candidate. Upload deliberately does not enqueue a build.
Only a later lifecycle operation that has resolved the environment owner,
subject/incarnation, and expected operation epoch may create the first queued
build attempt. Build attempts carry those bindings, use monotonic lease epochs,
and reject stale start, heartbeat, and completion calls. This keeps candidate
publication reusable while preventing an upload or stale build from mutating
an environment.

The following restricted-builder slice adds a lease-fenced coordinator with
finite global and per-owner concurrency, retained-source quotas, and an
attempt/lease-unique Kubernetes namespace. Two native jobs build the complete
amd64 and arm64 image set with a digest-pinned rootless BuildKit wrapper. The
jobs have no service-account token, registry credential, Docker socket, host
namespace, or general object-store credential; an S3 POST policy bounds each
exact output object as well as its key, metadata, lifetime, and size.

The trusted exporter reacquires and verifies both bundles without extracting
candidate paths, validates every OCI descriptor/blob/config/platform digest,
scans all images before any registry push, preserves each native manifest
digest, independently verifies the joined multi-architecture indices, and
stores each bounded scan report plus the aggregate safety record under
attempt/lease-unique content-addressed object keys referenced by publication.
The shipped builder authority is inert by default. Startup verifies the exact
builder image, RuntimeClass name, scanner binary and offline database identity,
finding-policy digest, registry tools, publisher identity, protocol map, and
trusted launcher-profile digest before the background loop exists. An apply
request receives `503` before lifecycle mutation while that authority is
inert. Live enablement still requires measured RuntimeClass/PID and CNI egress
evidence plus read-only scanner databases and scoped registry credentials.

The following protected-capacity slice installs the environment agent from an
operator-pinned fleet image, creates independently owned guard and agent roles,
runs the separately versioned guard migrations, and gates lifecycle completion
on an exact, durably checkpointed projection to the global manager. Migration
login and owner membership exist only while one convergence is in
flight; the login credential, membership, and database CREATE authority are
sealed again before the agent is installed. Reporter tokens are dynamically
registered by hash; lifecycle authority and mTLS files
remain outside candidate containers. The lifecycle projection mTLS identity
also remains outside every personal namespace; installed agents receive a
separate reporter-only mTLS identity. Capacity-only changes retain deployment,
candidate, reporter, and installation evidence while advancing only the global
and local configuration generation. Replacement deployments rotate the
reporter. Protected demand capture survives ordinary pod restarts by replaying
the exact durable high-water observation, while an explicitly superseded
configuration retires its old observation and continues the monotonic sequence.
Reporter identity, token, and agent-database password are first written to a
separate lifecycle-owned credential-seed Secret before protected database
mutation. A retry therefore cannot rotate identity inside the same deployment
generation after a partial Kubernetes failure. Readiness verifies that seed
against the runtime Secret both before and after rollout completion.
The manager and local claim guard remain zero-executable until #906 activates
the fleet-wide cutover. Manager-first zero-slot destroy is durable and
checkpointed, but live drain/cancellation remains blocked on the Package 2/3
claim and executor protocols.

Candidate artifact collection is a separate two-phase authority. A terminal
candidate must first be observed without an active environment, lifecycle
operation, or build attempt; that observation starts the configured grace
period. Collection later leases and persists a canonical deletion manifest,
heartbeats while deleting, and commits `collected` only with the exact current
lease and manifest digest. The manifest names only the owner's canonical source
generation, attempt-specific build/evidence prefixes, and attempt-isolated
registry tags. A fresh owner-scoped generation key fences every rehydration, so
an expired collector cannot delete a later upload even if one remote request
outlives its database lease. Current objects, versions, delete markers, and
multipart uploads are all removed. Each completion appends an
update/delete-protected evidence row with
the exact lease and manifest, while collected candidates no longer consume
retained-artifact quota. Re-uploading the exact source rehydrates the same
candidate metadata under the owner quota and requires a fresh build without
altering prior collection evidence; an apply racing collection either clears
the initial unreferenced mark or fails closed after collection has acquired the
candidate row.

The following lifecycle-authority slice adds immutable subject UUID/lifecycle
incarnation identity and a durable `dev_lifecycle_operations` row for every
apply. `PUT /api/v1/dev-instances/{name}` atomically binds the authenticated
owner, expected operation epoch, candidate, min/max policy, idempotency key,
deployment generation, and a fresh immutable operation-attempt identity before
enqueueing the candidate build in the same transaction. A retry with the same
key or exact request returns the existing logical operation. A failed
pre-activation attempt is preserved and may resume the same logical operation
only with a fresh attempt identity; database triggers make request and attempt
bindings immutable and validate the current-attempt pointer at commit.

Capacity-only changes to a ready environment update policy atomically without
creating a generation. Candidate changes retain the old candidate and capacity
projection while the replacement is prepared. Central activation cannot
commit until separate readiness evidence and an environment acknowledgement
are present. Finite configurable global, per-owner, and aggregate slot limits
serialize only lifecycle admission, so unrelated environments can perform
their external provisioning concurrently. At that intermediate slice, the
candidate-aware restricted builder, protected environment agent, and
global-capacity projection remained activation-blocking work.

The next reconciler slice captures only the hash of the credential verified by
the apply request on the immutable operation-attempt row. A delayed worker
reloads that exact bearer or browser-session row and fails closed if the
credential, owner, team, membership, expiry, or revocation state changed.
Database leases with monotonic epochs and periodic heartbeats permit multiple
management-service replicas without process-local ownership.

Once the candidate publication is ready, the trusted preparation runtime
requires explicit activation, migration-compatibility, capacity-agent, claim
guard, and worker protocol versions before external mutation. It renders only
immutable OCI references, preserves existing fixture secrets, and creates
generation-suffixed Deployments and preview Services. Stable Services and the
Ingress are a separate activation manifest and are not applied during
preparation, so an update cannot replace the serving generation early. Every
candidate object is stamped with subject, incarnation, logical operation,
attempt, operation epoch, candidate, and deployment generation.

Preparation publishes a canonical readiness digest and stops at a central
`activation_intent`. The management API accepts a local activation
acknowledgement only when it is fresh, Ed25519-authenticated by an agent-only
private key, bound to the exact intent, and durably inserted into an append-only
table. The management service receives only the public verification key and
therefore cannot manufacture acknowledgement evidence from its own readiness
observation. Signed intent polling prevents disclosure to an unauthenticated
caller. The independent agent re-observes the exact immutable generation,
proves both candidate legacy capacity paths remain disabled, converges only
the stable Services and Ingress, reads them back, and signs that local digest.
The lifecycle next converges the protected local capacity installation, records
the complete outbound projection before network mutation, resolves concurrent
manager epochs without changing any other request field, and records the exact
manager acknowledgement only after the exact installed agent has successfully
published through its restricted egress path. Only then does it mark a
create/update ready or apply a capacity-only min/max change. This is readiness of the zero-executable
control path; final fleet activation is still required before it may enable
live worker capacity.

When this authority is enabled, the earlier candidate-less `POST
/dev-instances` mutation path is retired and the service no longer constructs
its service-wide-candidate runner. Personal rows also fail closed on the old
delete path until the durable drain/delete operation lands. This prevents the
legacy provisioner from becoming a second lifecycle writer while the remaining
reconciler and destroy slices are implemented.

After migration, the service copies only the requesting user, current team,
quota policy, membership, and the hash of the credential used for creation
into the isolated database. It does not copy raw credentials or unrelated
users/tokens. The existing CLI bearer/session therefore works at the new
endpoint, and normal password login remains possible.

## Capacity model

Each personal environment is a subject of the one global capacity manager
shared with staging and production. Its `max_slots` is a demand ceiling, not a
reservation. A fourth developer needs no code or configuration allow-list
change, and all personal environments share the development tier and owner
quotas.

Trusted environment agents publish demand and enforce protected claim
admission. The manager performs one allocation across `oldlab` and `gb10` and
publishes candidate/generation-bound grants. One pool-local executor per Slurm
controller applies only its pool's fenced intents:

```text
registry + per-instance demand + lease observations
                         |
                         v
          one global fleet allocation transaction
                         |
             exact grants and launch permits
                         |
                         v
             pool-local Slurm executors
```

Missing, expired, or mismatched grants clamp desired capacity to zero even
when work is queued. Pending Loom submissions are cancelled only with complete
ownership proof; active workers are fenced to drain and remain charged until
terminal observation. No per-environment allocator or autoscaler may raise
capacity beside the global writer.

Worker env files are owner-only, use a path derived from the environment, and
are reusable only when operation epoch, generation, full candidate SHA, and
image tag all match. Credentials can be minted only while the instance has a
non-zero external policy; drain serializes that policy to zero before bulk
revocation. Recreate also revokes credentials preserved in a keep-data
database before readiness. Files for registry-removed environments are
narrowly pruned on the next supervisor tick. A candidate change prepares a
new generation while the old generation remains current, then uses the
acknowledged two-phase cutover and drain rules in the global fleet design;
capacity-only changes do not redeploy.

## Lifecycle and recovery

Create validates before mutation, claims a monotonic logical operation and
attempt, and then converges:

1. database role/database and isolation grants;
2. buckets;
3. namespace-local secrets;
4. bucket-scoped MinIO user/policy;
5. migration, then candidate-bound runtime Deployments;
6. owner access bootstrap;
7. capped external-Slurm policy;
8. `ready`.

Destroy reverses authority safely: drain and delete the policy, delete the
namespace, then remove the database, buckets, MinIO tenant, and vault state.
Each destructive boundary is checkpointed in the registry. A retry after the
namespace is gone therefore skips the now-unreachable control-plane step and
continues at the exact remaining cleanup action. `--keep-data` stops after
namespace deletion and records that choice; database and buckets remain for a
later approved recovery/recreate.

Unexpected failures expose only `provisioning_failed` or `deletion_failed`.
Detailed protected logs may contain bounded executor diagnostics but not
credentials. Pre-activation create/update failures preserve their failed
attempt and may retry only through a fresh attempt identity. Concurrent
operations are owner-safe and fenced by subject incarnation, operation epoch,
logical operation ID, and attempt ID.
Deletion is rejected while creation is active to prevent a policy/deployment
race; the caller waits or resumes creation first.

## Activation boundary

Repository completeness is not live readiness. Activation still requires all
Packages 2–5, explicit global and pending budgets, wildcard DNS/TLS, fixture
credentials and storage, protected database roles, isolated-builder and
namespace RBAC, both pool-local executors, a fleet-wide legacy-writer freeze
and adoption proof, zero-capacity dry runs, rollback evidence, #896, and the
re-scoped operations gate in #906. Developer commands never install units,
edit DNS, disable a legacy writer, or raise the executable global ceiling.
