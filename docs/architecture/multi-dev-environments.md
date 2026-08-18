# Personal development environments

Loom contains an opt-in controller for isolated, persistent development
environments on the shared fixture. It is disabled by default:
`LOOM_SVC_DEV_INSTANCES_ENABLED=false`. Enabling it also requires the fixture
database authority, Kubernetes access, MinIO authority, an activation public
key, the restricted candidate-builder configuration, and the personal-capacity
runtime. That runtime requires the global capacity-manager endpoint, distinct
lifecycle and reporter credentials, an immutable capacity-agent image, and the
operator-owned pool capability map.

`loom service up` requires an explicit target. `--environment local` manages
the Docker Compose stack; `--environment dev-<name>` drives the authenticated
personal-candidate lifecycle described here. Staging and production remain
protected rollout targets: the command validates their full Git candidate and
then directs the operator to `loom cluster rollout` rather than mutating them.

The shared management-plane architecture and trust boundaries are specified in
the [personal management-plane deployment design](personal-dev-management-plane-deployment.md).
Its exact inert deployment procedure is the
[personal management-plane shadow rehearsal](../runbooks/personal-dev-management-plane-shadow.md).
That package installs shared infrastructure only in `loom-dev`; it neither
creates a personal application namespace nor enables the lifecycle controller,
builder, activation agent, or physical capacity. Personal application
acceptance remains a later gate after both the personal-management shadow and
the separate global-capacity zero-ceiling shadow report ready.
The controlled
[zero-capacity acceptance](../runbooks/personal-dev-zero-capacity-acceptance.md)
then exercises two concurrent owners with distinct authenticated sessions,
source snapshots, lifecycle operations, namespaces, storage identities, and
capacity publications. Acceptance requires `worker_available=false` and an
exact global executable-new-capacity ceiling of zero throughout. Its plan pins
the manager configuration epoch as a monotonic floor rather than a constant:
every personal projection advances the global epoch, and other environments
share the same authority, so advancement is valid while regression fails the
acceptance interlock.

## Checked-in interfaces

The candidate-aware lifecycle has a high-level CLI entry point as well as
library and HTTP interfaces.

| Interface | Current behavior |
| --- | --- |
| `loom service up --environment dev-<name>` | Seals and uploads the selected source, or resolves an owned ready candidate supplied with `--candidate`; resolves the expected operation epoch, applies the exact candidate and capacity policy, and waits for activation plus the initial non-executable capacity publication unless `--no-wait` is set. |
| `create_personal_dev_source_snapshot()` | Library API used by the CLI to seal a Git worktree into a deterministic source archive. |
| `POST /api/v1/personal-dev-candidates` | Accepts a multipart `source` archive plus `source_sha256` and `archive_sha256`. Upload creates or returns an owner-scoped immutable candidate but does not start a build. |
| `GET /api/v1/personal-dev-candidates[/{id}]` | Lists or reads visible candidates, including build state, artifact state, collection blocker, and collection time. |
| `PUT /api/v1/dev-instances/{name}` | Compare-and-set apply for a candidate, capacity policy, expected operation epoch, and idempotency key. It queues the candidate build when required. |
| `DELETE /api/v1/dev-instances/{name}` | Compare-and-set manager-first teardown for a ready personal environment. It binds `keep_data`, the expected operation epoch, and an idempotency key before queuing durable cleanup. |
| `GET /api/v1/dev-instances/{name}/operations/{id}` | Reads the owner-scoped durable lifecycle operation. |
| internal activation endpoints | Let the independently keyed activation agent poll exact intents and publish signed acknowledgements. |

The apply payload is:

```json
{
  "candidate_id": "UUID",
  "candidate_sha": "64-character lowercase SHA-256",
  "min_slots": 0,
  "max_slots": 2,
  "expected_operation_epoch": 0,
  "idempotency_key": "UUID"
}
```

The response binds the subject and lifecycle incarnation, logical operation,
attempt, candidate, operation epoch, deployment generation, checkpoint, and
public failure reason. New or in-progress operations return `202`; an already
succeeded operation returns `200`. Reusing an idempotency key for different
inputs, using a stale epoch, or racing another operation returns `409`.

## Command-line deployment

From the Git worktree to deploy, authenticate to the target server and run:

```text
loom service up --environment dev-<name>
```

The current directory is the default `--source-root`, and `.` is the default
source context. Repeat `--source-context` to restrict sealing to explicit
relative subtrees. `--min-slots` defaults to `0`, `--max-slots` defaults to
`2`, the readiness timeout defaults to 7200 seconds, and the command resolves
the current operation epoch before it mutates the environment. An existing
environment in a non-ready lifecycle state blocks a new apply rather than
being overwritten.

To redeploy one retained, owned candidate that is already `ready`, use its
64-character content digest:

```text
loom service up --environment dev-<name> --candidate <candidate-sha256>
```

The client rejects ambiguous, non-ready, non-personal, or promotable candidate
bindings. It also revalidates the operation identity and the final candidate,
capacity, epoch, and environment projection while waiting. The command cannot
make a disabled server usable: the controller, restricted builder, activation
agent, storage, database, and Kubernetes authorities must be configured by the
operator.

## `loom dev` compatibility commands

The installed CLI also exposes a candidate-less compatibility client:

```text
loom dev create <name> [--min-slots N] [--max-slots N] [--no-wait]
loom dev list [--mine] [--include-deleted]
loom dev status <name>
loom dev destroy <name> [--keep-data] [--idempotency-key UUID] [--no-wait]
```

These commands call `/api/v1/dev-instances`; they never shell out to
`kubectl` or edit capacity policy directly. Create and destroy poll unless
`--no-wait` is supplied, and every command supports `--format json`.

These compatibility commands do not expose candidate upload or candidate-aware
apply themselves; use `loom service up --environment dev-<name>` for that
workflow. When the current personal-development controller is enabled,
candidate-less `POST /dev-instances` returns `410`. Destroy is supported for a
ready, owner-bound candidate environment with complete activation and capacity
evidence; other lifecycle states fail closed. Ordinary deployments leave the
controller disabled, so candidate-aware deployment and teardown succeed only
against an explicitly enabled and fully configured personal-development
service.

## Identity and visibility

Names are lowercase RFC 1123-style labels, 1–20 characters, beginning with a
letter. Reserved base-environment names are rejected. Every resource identity
is derived server-side and cannot be overridden:

| Resource | Derived value |
| --- | --- |
| Environment, provider namespace, and worker pool | `dev-<name>` |
| Namespace | `loom-dev-<name>` |
| Database and role | `loom_dev_<name>` (dashes become underscores) |
| Buckets | `loom-dev-<name>-tasks`, `-trajectories`, `-artifacts` |
| Public host | `<name>.dev.yylx.world` |
| Control Plane host | `cp-<name>.dev.yylx.world` |
| Gateway host | `gw-<name>.dev.yylx.world` |
| Path route | `/dev-<name>` |

Candidate and environment reads are owner-scoped unless the caller is a
platform admin. Cross-owner detail reads and mutations return `404`. Mutations
require a submitting user with a current team; service, worker, and legacy
shared-team credentials cannot create personal authority.

## Source and candidate pipeline

Source sealing includes the current contents of tracked and non-ignored
untracked regular files, records tracked deletions, and excludes ignored,
sensitive, and Git-internal paths. It rejects links, special files,
hard-linked files, invalid paths, checkout races, and configured count or size
overflow. The defaults cap the source at 100,000 files, 256 MiB total, and
64 MiB per file. The canonical USTAR archive and canonical manifest have
independent SHA-256 identities.

Candidate intake streams the archive into a bounded no-follow temporary file,
verifies both digests and the canonical archive, then publishes it under an
owner-scoped candidate-and-generation object key. Upload alone leaves the
candidate in `uploaded`; a matching environment apply atomically creates the
lifecycle operation and queues the first build attempt.

The restricted builder is separately gated by
`LOOM_SVC_PERSONAL_DEV_BUILDER_ENABLED`, which also defaults to `false`.
Startup validates its immutable builder image, RuntimeClass, scanner and
offline database identity, finding-policy digest, registry tools and
publisher, trusted launcher profile, and protocol map. If the builder is not
available, candidate apply fails with `503` before lifecycle mutation.

An enabled build uses lease-fenced amd64 and arm64 jobs for the complete
personal image set. Build jobs have no service-account token, registry
credential, Docker socket, or broad object-store credential. The trusted
exporter revalidates OCI descriptors and platform digests, scans all images,
and publishes immutable multi-architecture references. Candidate and
operation responses always identify this output as `personal-dev-only` and
`promotable: false`; it is not staging or production release evidence.

## Candidate artifact collection

When both the personal-development controller and restricted builder are
enabled, the service also runs the bounded artifact collector. A candidate in
`uploaded`, `ready`, or `failed` first becomes eligible only when no
non-deleted environment, active lifecycle operation, or queued/running build
references it. The first unreferenced observation starts a grace period; any
new environment apply, build claim, or exact re-upload clears that observation
before collection begins. The default grace period is 24 hours, the collection
lease is 15 minutes, and the idle poll interval is 30 seconds.

After the grace period, the collector persists and leases a canonical deletion
manifest. It names the exact owner, candidate, source generation and object,
attempt-specific build and evidence prefixes, and attempt-and-lease-specific
registry tags. The collector heartbeats while deleting current objects,
versions, delete markers, multipart uploads, and registry tags, then records
`collected` only if the lease epoch and manifest digest are still current. An
expired lease reuses the persisted manifest. Missing registry authority or an
invalid manifest blocks that candidate and exposes the reason instead of
widening the deletion scope.

Candidate reads expose `artifact_state` as `retained`, `collecting`, or
`collected`, plus `artifact_gc_blocked_reason` and `artifact_collected_at`.
Collection in progress makes re-upload and apply fail closed. Collected
candidates stop consuming the retained-count and retained-byte quotas. An
exact later re-upload reuses the candidate identity with a fresh source
generation, returns it to `uploaded`, and requires a fresh build; prior
collection evidence remains append-only.

## Preparation and activation

For a ready candidate, the service-owned reconciler uses a database lease and
monotonic lease epoch. It revalidates the exact credential captured by hash on
the lifecycle attempt, creates or reuses the derived database and buckets,
converges the bucket-scoped MinIO tenant and namespace secrets, runs the
migration Job, and prepares generation-suffixed Control Plane, Gateway,
Service, and web Deployments from immutable OCI references. Stable Services
and Ingress are not part of preparation.

The reconciler records a readiness digest and publishes an activation intent.
The independent activation agent proves possession of its Ed25519 private key,
rechecks the current intent immediately before mutation, re-observes the exact
generation, and requires both the Control Plane Slurm controller and in-cluster
worker capacity path to remain disabled. It alone applies the stable Services
and Ingress and returns a signed local-activation digest. The management service
has only the public key.

After the acknowledgement, the reconciler converges the candidate-independent
capacity agent and its protected state, prepares the personal subject against
the global manager's current configuration epoch, and publishes it through
`PUT /v1/development-projections/{subject_id}`. It then waits for the exact
installed agent to become ready after its first successful demand report. Only
after recording that non-executable projection and publication evidence does
the reconciler mark the operation succeeded and the environment `ready`.

The agent is packaged separately and has an operator-edited example at
`deploy/dev-fleet/personal-dev-activation-agent.yaml.example`; Loom does not
apply that example automatically.

## Manager-first destruction

`loom dev destroy <name>` first reads the current owner-scoped environment and
submits its exact operation epoch. The default idempotency key is derived from
the name, epoch, and `keep_data` choice; `--idempotency-key` supplies a stable
key for an explicit retry. Unless `--no-wait` is set, the command polls until
the environment reaches `deleted`.

Teardown retires authority before deleting local resources. The reconciler
publishes a `destroy` projection that makes the global-manager subject
`disabled` with zero minimum and maximum demand. It then seals every protected
database login, deletes the namespace, and advances restart-safe cleanup
checkpoints. Without `--keep-data`, it drops the dedicated database, deletes
the three buckets and bucket tenant, and removes stored credentials. With
`--keep-data`, the database and buckets remain, but the tenant and credentials
are removed so retained data has no live runtime authority.

A later candidate-aware deployment of the same name starts a new subject
incarnation through the normal build, activation, and capacity-publication
path. Reusing retained data therefore rotates the personal authority rather
than reviving the deleted subject or its reporter credentials.

## Isolation and limits

Each environment uses its own database login/database, three buckets,
bucket-scoped object-store identity, namespace-local secrets, migration Job,
runtime Deployments, Services, and Ingress. Candidate runtime and migration
pods disable service-account token mounting, run as non-root, drop
capabilities, disallow privilege escalation, and use the runtime-default
seccomp profile. Shared root storage credentials are not copied into the
instance namespace.

The default lifecycle envelope admits at most 16 live personal environments,
2 per owner, aggregate per-owner minimum slots of 8, and aggregate per-owner
maximum slots of 16. A single environment still has the shared development
ceiling of 8. Candidate retention defaults to 8 archives and 3 GiB per owner;
builder concurrency defaults to 4 globally and 1 per owner. All values are
operator-configurable through the schema-backed service settings.

`min_slots` and `max_slots` are stored demand policy, not reserved physical
capacity. Candidate activation deliberately verifies that the Control Plane
Slurm controller and in-cluster worker path are disabled. The current
candidate-aware path publishes its protected demand and subject configuration
to the shadow-only global capacity manager, whose executable-new-capacity
ceiling is fixed at zero. It does not receive or execute a worker-capacity
grant, and the personal lifecycle does not consume the manager's dry-run
reservation, permit, or pool-executor routes. Consequently, `ready` confirms
the immutable application generation, stable-route acknowledgement,
capacity-agent installation, subject projection, and initial demand
publication; it does not mean worker slots are live.
The separate global development-fleet supervisor consumes 40-character Git
candidate identities and existing per-instance external Slurm policies, so it
cannot consume these 64-character personal-candidate bindings directly.

## Capacity status projection

The personal-environment API keeps `application_status`, `capacity_status`,
`capacity_prepared`, and `worker_available` separate. Application readiness
remains a lifecycle property and pod readiness never implies worker capacity.
The manager checkpoint carries configuration epoch, execution state, execution
epoch, and executable ceiling; personal reconciliation uses its configuration
epoch only for projection fencing. In the checked-in inert deployment, capacity
is `shadow` or `prepared` and worker availability is false. If a later
authorized active manager records exact physical capacity, a matching protected
guard-database worker registration still has to prove the same subject,
incarnation, deployment generation, and intent before availability can be
reported.
That conjunction is read under a dedicated `NOINHERIT` observer database login
with no memberships or mutation-function grants. Missing, stale, malformed, or
unreachable manager, credential, or protected observation evidence fails
closed to `waiting`; it does not change application readiness into a 5xx.
