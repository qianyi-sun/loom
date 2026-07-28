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

## Persistent host installer

`scripts/ops/developer_sandbox_host.py` is the root-side converger for the
three sandbox stacks. It is plan-only by default and has a fixed repository,
host, account, group, NFS namespace, state namespace, and systemd unit. It does
not accept a remote URL, ref, path, user, port, or secret-value override.

Render the complete three-sandbox plan from an exact checkout:

```bash
uv run --no-sync python scripts/ops/developer_sandbox_host.py plan \
  --candidate-sha <full-lowercase-40-character-commit-SHA>
```

The JSON plan contains the exact candidate path, Compose project, private
state and secret paths, all ten reserved ports, expected owner/mode, unit name,
and read-only NFS readback commands for `oldlab-1` through `oldlab-5`. It never
contains raw credential values.

After separate live-host authorization, run the same command on
`trt-eai-oldlab-2` as root with `install --execute`:

```bash
uv run --no-sync python scripts/ops/developer_sandbox_host.py install \
  --candidate-sha <SHA> \
  --execute
```

The installer performs these bounded steps:

1. require root and the canonical host name `trt-eai-oldlab-2`;
2. require `/shared_work` to be the NFS mount and resolve the local
   `loom-rollout`, `sharedwork`, and three developer identities by name, then
   verify each developer can reach the local Docker daemon through their
   normal supplementary groups;
3. fetch the exact commit from the fixed `qianyi-sun/loom` remote into a
   private temporary directory under each developer's NFS candidate root,
   verify `HEAD`, commit, tree, and cleanliness, remove every write bit, and
   atomically rename it to `<candidate_root>/<SHA>`;
4. converge each local state/cache/evidence/runtime/secrets directory to its
   developer owner and mode `0700`;
5. create the per-developer `secrets/sandbox.env` and `secrets/admin.toml`
   once, atomically, as owner-only mode `0600` files; existing valid files are
   never rotated or overwritten;
6. persist the root-only exact desired-state record under
   `/etc/loom/developer-sandboxes/desired/<developer>.json`, install the fixed
   host entry and systemd template, and enable/start each unit.

The durable locations for one sandbox are therefore:

| Purpose | Path |
| --- | --- |
| immutable candidate | `/shared_work/loom/candidates/sandboxes/<developer>/<SHA>` |
| lifecycle binding | `/srv/loom/developer-sandboxes/<developer>/sandbox-state.json` |
| secrets | `/srv/loom/developer-sandboxes/<developer>/secrets/sandbox.env` |
| admin singleton | `/srv/loom/developer-sandboxes/<developer>/secrets/admin.toml` |
| cache | `/srv/loom/developer-sandboxes/<developer>/cache` |
| evidence | `/srv/loom/developer-sandboxes/<developer>/evidence` |
| runtime | `/srv/loom/developer-sandboxes/<developer>/runtime` |
| desired candidate | `/etc/loom/developer-sandboxes/desired/<developer>.json` |
| installed fixed profile | `/etc/loom/developer-sandboxes/profiles/<developer>.toml` |

Do not copy or reveal the private files during readback. Compare only
owner/mode, required key names, and secret fingerprints when an authorized
isolation procedure requires it.

### Persistent create, update, and check

The enabled `loom-developer-sandbox@.service` is a replayable oneshot. It
selects `create`, `update`, or `check` from the persisted lifecycle binding,
then finishes with the full Compose health check and exact loopback-port
readback. It is safe to invoke repeatedly:

```bash
sudo systemctl start loom-developer-sandbox@qianyi.service
sudo systemctl start loom-developer-sandbox@hongjian.service
sudo systemctl start loom-developer-sandbox@devansh.service
```

An installer or unit rerun with the same SHA performs an idempotent forced
Compose convergence followed by a check; this repairs a partially created or
mixed stack instead of trusting state alone. A rerun with a different exact
SHA records the old SHA as the sole rollback target and converges an update.
Named Compose volumes remain attached to the fixed per-developer Compose
project.

On a fresh database, the initial worker token is deliberately only bootstrap
material. After the sandbox Control Plane is healthy, the host entry checks the
token without registering a worker. If it is rejected, the entry uses that
sandbox's loopback Admin API and private admin file to mint worker and
batch-runner tokens, atomically replaces only those env-file values, and
force-converges the stack so worker and Service receive them. Raw tokens never
enter argv, JSON output, systemd `Environment=`, or logs.

Run a read-only installed-state check with:

```bash
uv run --no-sync python scripts/ops/developer_sandbox_host.py check \
  --candidate-sha <SHA> \
  --sandbox all \
  --execute
```

This validates the NFS mount, candidate owner and immutability, private
owner/mode, secret-file shape, exact lifecycle SHA, Compose health, and all
reserved loopback listeners. Run the plan's five fixed `ssh ... stat` commands
separately and require identical inode, UID, GID, and mode for each candidate
root and exact candidate. Device numbers may differ between NFS clients.

### Safe rollback

Rollback is limited to the exact `previous_sha` stored by the last successful
desired-state change:

```bash
uv run --no-sync python scripts/ops/developer_sandbox_host.py rollback \
  --sandbox qianyi \
  --candidate-sha <RECORDED-PREVIOUS-SHA> \
  --execute
```

The target must already be a clean immutable published candidate. Both forward
update and rollback require the current and target candidates to have the same
Git tree for `migrations/`; the rollback then atomically swaps desired/current
history and runs the normal unit. If an update unit fails, the installer
restores desired state and attempts the previous same-migration candidate once.
For a first-create failure, it retains the new desired record so a repeat can
repair the partial stack. This rollback preserves named volumes.

If the migration trees differ, code-only rollback is intentionally refused.
Use a separately reviewed database/object-store backup and restore procedure;
do not run Alembic downgrade ad hoc. Likewise, never add
`--delete-volumes` to recovery unless the developer explicitly authorizes
irreversible sandbox-data deletion.

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
