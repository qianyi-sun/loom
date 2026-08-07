# Multiple per-developer development environments

Status: repository implementation complete; live activation remains an
operator change. See `deploy/dev-fleet/README.md` and
`docs/architecture/global-dev-fleet-autoscaler.md`.

Refs: #1178, #1192, #1193, #906.

## User contract

`loom service up` is the local Docker Compose workflow. It intentionally has
no `dev`, `staging`, or `production` selector.

Shared-fleet environments use an explicit, authenticated lifecycle:

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

## Shared data plane with per-instance authority

One operator-owned `loom-dev-shared` namespace runs the development Postgres
and MinIO servers. Per-instance manifests contain only a migration Job plus
control-plane, gateway, service, Services, and Ingress. They never render
Postgres, MinIO, persistent volumes, or inline secrets.

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

After migration, the service copies only the requesting user, current team,
quota policy, membership, and the hash of the credential used for creation
into the isolated database. It does not copy raw credentials or unrelated
users/tokens. The existing CLI bearer/session therefore works at the new
endpoint, and normal password login remains possible.

## Capacity model

Each environment has a local external-Slurm autoscaler policy. Its
`max_slots` is a demand ceiling, not a reservation. A fourth developer does
not need a code or configuration allow-list change, and all environments may
request the full development burst ceiling.

Exactly one submit-host supervisor reads the complete registry cohort and
each instance database's pool-scoped demand/capacity observation. It updates
the transactional global lease authority and passes the resulting
candidate/generation-bound grant to each existing Slurm reconciler. The grant
is the only development scale-up authority:

```text
registry + per-instance demand + lease observations
                         |
                         v
          one global dev capacity transaction
                         |
             exact, expiring grants
                         |
                         v
          existing per-environment Slurm actuators
```

Missing, expired, or mismatched grants clamp desired capacity to zero even
when work is queued. Pending Slurm jobs are cancelled immediately; active
workers are fenced to drain and remain charged until terminal observation.
This is why the global authority exists: the per-environment autoscaler owns
worker mechanics but cannot make an atomic decision across every developer's
simultaneous request.

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

Repository completeness is not live readiness. Activation still requires an
approved candidate, explicit global and pending budgets, wildcard DNS/TLS,
fixture credentials and storage, namespace/pod-exec RBAC, submit-host Slurm
access, a manual zero/dry-run, rollback evidence, and the owning operations
gate in #906. Developer commands never install units, edit DNS, or raise the
global budget.
