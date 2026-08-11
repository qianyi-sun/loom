# Personal development environments

Loom contains an opt-in controller for isolated, persistent development
environments on the shared fixture. It is disabled by default:
`LOOM_SVC_DEV_INSTANCES_ENABLED=false`. Enabling it also requires the fixture
database authority, Kubernetes access, MinIO authority, an activation public
key, and the restricted candidate-builder configuration.

This controller is separate from `loom service`. The `loom service` commands
manage only the local Docker Compose stack and do not accept a
development/staging/production selector.

## Checked-in interfaces

The candidate-aware lifecycle is currently an API and library surface, not a
complete `loom dev` CLI workflow.

| Interface | Current behavior |
| --- | --- |
| `create_personal_dev_source_snapshot()` | Seals a Git worktree into a deterministic source archive. It is a Python library API; no top-level CLI command wraps it. |
| `POST /api/v1/personal-dev-candidates` | Accepts a multipart `source` archive plus `source_sha256` and `archive_sha256`. Upload creates or returns an owner-scoped immutable candidate but does not start a build. |
| `GET /api/v1/personal-dev-candidates[/{id}]` | Lists or reads visible candidates and their build state. |
| `PUT /api/v1/dev-instances/{name}` | Compare-and-set apply for a candidate, capacity policy, expected operation epoch, and idempotency key. It queues the candidate build when required. |
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

## `loom dev` compatibility commands

The installed CLI still exposes the earlier candidate-less client:

```text
loom dev create <name> [--min-slots N] [--max-slots N] [--no-wait]
loom dev list [--mine] [--include-deleted]
loom dev status <name>
loom dev destroy <name> [--keep-data] [--no-wait]
```

These commands call `/api/v1/dev-instances`; they never shell out to
`kubectl` or edit capacity policy directly. Create and destroy poll unless
`--no-wait` is supplied, and every command supports `--format json`.

They do not expose candidate upload or candidate-aware apply. When the current
personal-development controller is enabled, candidate-less `POST
/dev-instances` returns `410`. Deletion of a candidate-backed environment
returns `409` because the guarded drain/delete authority is not present.
Ordinary deployments leave the controller disabled, so they do not provide an
end-to-end shared-fleet environment command by default.

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
| Interim route path | `/dev-<name>` |

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
owner-scoped content-addressed object key. Upload alone leaves the candidate
in `uploaded`; a matching environment apply atomically creates the lifecycle
operation and queues the first build attempt.

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
generation, and requires both candidate legacy-capacity paths to remain
disabled. It alone applies the stable Services and Ingress and returns a signed
local-activation digest. The management service has only the public key. After
the acknowledgement, the reconciler marks the operation succeeded and the
environment `ready`.

The agent is packaged separately and has an operator-edited example at
`deploy/dev-fleet/personal-dev-activation-agent.yaml.example`; Loom does not
apply that example automatically.

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
capacity. Candidate activation deliberately verifies that the legacy Control
Plane Slurm controller and in-cluster worker path are disabled. The current
candidate-aware path does not publish or execute an external worker-capacity
grant, so `ready` means the immutable application generation and stable routes
were acknowledged, not that worker slots are live. Candidate-backed drain,
destruction, and `--keep-data` recovery are also unavailable through the
guarded API. The separate global development-fleet supervisor consumes
40-character Git candidate identities and existing per-instance external
Slurm policies, so it cannot consume these 64-character personal-candidate
bindings directly.
