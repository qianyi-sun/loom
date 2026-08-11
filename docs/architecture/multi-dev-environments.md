# Per-developer environments

Loom can manage isolated persistent development environments on the shared
fleet through the authenticated `/api/v1/dev-instances` API and `loom dev`
CLI. This is separate from `loom service`, which manages only the local Docker
Compose stack.

## CLI

```text
loom dev create <name> [--min-slots N] [--max-slots N] [--no-wait]
loom dev list [--mine] [--include-deleted]
loom dev status <name>
loom dev destroy <name> [--keep-data] [--no-wait]
```

Create and destroy return `202`. The CLI polls for `ready` or `deleted` unless
`--no-wait` is supplied. `--format json` is available on every command.
`max_slots` defaults to 2 and cannot exceed the shared development-fleet cap of
8; it is a demand ceiling rather than a reservation.

The service must have its development-instance provisioner configured. When it
is unavailable, lifecycle mutations are rejected rather than falling back to
local shell or Kubernetes commands.

## Identity

Names are lowercase RFC 1123-style labels, 1–20 characters, beginning with a
letter. Reserved base-environment names are rejected. Every resource identity
is derived server-side from the name and cannot be overridden:

| Resource | Derived value |
|---|---|
| Environment and worker pool | `dev-<name>` |
| Namespace | `loom-dev-<name>` |
| Database and role | `loom_dev_<name>` (dashes become underscores) |
| Buckets | `loom-dev-<name>-tasks`, `-trajectories`, `-artifacts` |
| Public host | `<name>.dev.yylx.world` |
| Control Plane host | `cp-<name>.dev.yylx.world` |
| Gateway host | `gw-<name>.dev.yylx.world` |
| Interim route path | `/dev-<name>` |

The registry row records owner, capacity request, lifecycle status, operation
identity/epoch, cleanup checkpoint, candidate/deployment identity, timestamps,
and a bounded failure reason. Owners see their own rows; platform admins can
list across owners. Unauthorized detail and deletion return 404.

## Isolation and provisioning

Each instance receives its own database login/database, three buckets,
bucket-scoped object-store identity, namespace-local Secrets, runtime services,
migration job, and capacity policy. Shared root storage credentials are not
copied into the instance namespace. Runtime and migration pods use the
restricted security context rendered by the instance manifest.

Provisioning is checkpointed and converges the database, buckets, credentials,
namespace, migration, services, owner access, and capacity policy before the
row becomes `ready`. Destroy first drains and removes capacity authority, then
removes the namespace and instance resources. `--keep-data` preserves the
database, buckets, and credentials for a later recovery.

Lifecycle operations are fenced by operation ID and epoch. Reissuing a request
resumes a durable operation; conflicting create/destroy requests return a
conflict instead of running concurrently. Exposed states are `provisioning`,
`ready`, `deleting`, `failed`, and `deleted`.

## Capacity boundary

Development instances use the Slurm actuator and the global development-fleet
budget. Missing or mismatched capacity grants clamp desired capacity to zero.
The instance API does not install host units, change DNS, or bypass global
capacity authority.

