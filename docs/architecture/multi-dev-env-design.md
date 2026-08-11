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

The next Package 4 slice supplies authenticated intake. The management service
streams the upload into a bounded no-follow temporary file, verifies both
digests and the canonical archive independently, publishes the verified bytes
under an owner-scoped content-addressed object key, and idempotently records an
immutable `uploaded` candidate. Upload deliberately does not enqueue a build.
Only a later lifecycle operation that has resolved the environment owner,
subject/incarnation, and expected operation epoch may create the first queued
build attempt. Build attempts carry those bindings, use monotonic lease epochs,
and reject stale start, heartbeat, and completion calls. This keeps candidate
publication reusable while preventing an upload or stale build from mutating
an environment. The restricted builder runtime, safety attestation, image
publication, and create-or-update cutover remain activation-blocking work.

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
narrowly pruned on the next supervisor tick. A candidate/shape change to a
ready environment is rejected until the owner performs a drain-first destroy
(optionally preserving data), preventing old and new grants or credentials
from overlapping.

## Lifecycle and recovery

Create validates before mutation, claims a monotonic operation, and converges:

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
credentials. Concurrent operations are owner-safe and fenced by operation ID.
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
