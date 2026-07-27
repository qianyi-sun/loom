# Candidate-bound developer sandboxes

This runbook covers the bounded, pre-merge Docker Compose sandboxes for
`qianyi`, `hongjian`, and `devansh` on the SSH target `oldlab-2`. It does not
configure Slurm,
remote-worker pools, orphan cleanup, capacity brokerage, staging, or production.

The checked-in profiles under `deploy/developer-sandboxes/` reserve a unique
Compose project, loopback host ports, database, MinIO buckets, provider
namespace, and state/cache/evidence/runtime roots for each developer. Profile
validation rejects unknown fields, unsafe roots, non-loopback bindings, and any
cross-profile collision.

## Host path contract

Provision the shared candidate namespace on the `/shared_work` NFS source, not
independently on each client. `/shared_work/loom`, `candidates`, `sandboxes`,
and each developer child are owned by `loom-rollout:sharedwork` with mode
`2750`. The resulting device/inode, UID/GID, and mode must read back identically
from every OLDLAB NFS client. Developers have read/traverse access but cannot
mutate a published candidate.

On `oldlab-2`, `/srv/loom` and `/srv/loom/developer-sandboxes` are
`root:sharedwork` mode `2750`. Each developer root and its `cache`, `evidence`,
and `runtime` children are owned by that developer and mode `0700`. Clear an
inherited setgid bit explicitly after creating those private children beneath
the setgid parent. Resolve owners by account name on the target host; do not
copy numeric UIDs from another node.

The capacity-broker state root is separate:
`/var/lib/loom-shared-capacity` stays `root:root` mode `0700` until a dedicated
broker service identity is installed. In that bootstrap state, only a
root-invoked broker may initialize the database. Never grant a sandbox account
write access to the broker authority.

## Preconditions

Materialize a clean candidate checkout at the exact profile path:

```text
<candidate_root>/<full-lowercase-40-character-commit-SHA>
```

The checkout's `HEAD` must equal the requested SHA, its tree must resolve, and
`git status --porcelain=v1 --untracked-files=all` must be empty.

Create a mode `0600` Compose env file containing these keys:

```text
LOOM_DEV_POSTGRES_USER
LOOM_DEV_POSTGRES_PASSWORD
LOOM_DEV_MINIO_ROOT_USER
LOOM_DEV_MINIO_ROOT_PASSWORD
LOOM_CP_STEP_JWT_SIGNING_KEY
LOOM_SECRET_STORE_MASTER_KEY
LOOM_WORKER_TOKEN
```

Create a separate mode `0600`, non-symlink admin secret TOML with the normal
`[admin].token` format. The planner validates both files but never reads secret
values into the child-process environment or prints them in its JSON output.

## Plan first

From the candidate checkout, render a mutation-free plan:

```bash
uv run --no-sync python scripts/ops/developer_sandbox.py plan \
  --operation create \
  --profile deploy/developer-sandboxes/qianyi.toml \
  --source-repo /shared_work/loom/candidates/sandboxes/qianyi/<SHA> \
  --candidate-sha <SHA> \
  --secrets-env /secure/path/qianyi.env \
  --admin-secret-file /secure/path/qianyi-admin.toml
```

`create`, `update`, `check`, and `destroy` also default to plan-only mode. Add
`--execute` only while logged into the `oldlab-2` SSH target. The connection
alias is separate from host identity: execution lowercases the canonical local
hostname, removes only a trailing DNS dot, and then requires the exact profile
identity `trt-eai-oldlab-2`. Any other hostname fails closed.

Create and update validate Compose, start Postgres and MinIO, apply migrations,
then build and start the exact candidate. Successful mutations write a
mode-`0600` candidate/state binding and an evidence record. `check` requires the
requested SHA to match state and requires every expected service to be running
and not unhealthy.

Destroy is project-scoped and preserves named volumes by default:

```bash
uv run --no-sync python scripts/ops/developer_sandbox.py destroy ... --execute
```

Use `--delete-volumes` only when the developer explicitly intends to delete
that sandbox project's persistent data. No command in this workflow is rollout
or staging evidence.
