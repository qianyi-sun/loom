# Candidate-bound developer sandboxes

This runbook covers the bounded Docker Compose sandboxes for `qianyi`,
`hongjian`, and `devansh` on the SSH target `oldlab-2`, including the
candidate-bound private link used by their OLDLAB and GB10 Slurm workers. The
capacity broker, Slurm policy, cgroup enforcement, and pressure-drain runbooks
remain separate. Nothing here authorizes staging or production.

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

## Candidate-bound remote data-plane link

The three sandboxes keep their Control Plane, Gateway, and MinIO host ports
bound to `127.0.0.1`. Do not change those Compose bindings to `0.0.0.0` or a
LAN address. One root-owned multi-service relay per sandbox listens on the
exact oldlab2 private address `192.168.50.14`, requires TLS 1.3 plus a client
certificate, checks the client URI SAN against both the sandbox identity and
the one active 40-character candidate SHA, and only then forwards to the exact
loopback service.

| Sandbox | Control Plane | Gateway | MinIO |
| --- | --- | --- | --- |
| `qianyi` | `26080 → 20080` | `26100 → 20100` | `26900 → 20900` |
| `hongjian` | `27080 → 21080` | `27100 → 21100` | `27900 → 21900` |
| `devansh` | `28080 → 22080` | `28100 → 22100` | `28900 → 22900` |

Every mapping is `https://192.168.50.14:<listener>` to
`127.0.0.1:<target>`.

The server certificate has only the `192.168.50.14` IP SAN. Each candidate
uses a new CA and client certificate. The client URI SAN is exactly:

```text
spiffe://loom/developer-sandbox/<sandbox>/candidate/<SHA>/worker
```

There is no previous-SHA grace set. Activating or rolling back atomically
switches the `current` symlink to one installed SHA and restarts the relay.
Every other candidate is rejected before the loopback connection is opened.

### Host-local secret contract

OLDLAB `/shared_work` and GB10 `/shared_work` are independent NFS domains.
Candidate source and the secret-free worker env can use the same logical paths
in each domain, but TLS keys and worker bearer tokens must be independently
installed by root on every eligible worker node:

```text
/etc/loom/developer-sandbox-links/clients/<sandbox>/<SHA>/
  ca.pem
  client.pem
  client-key.pem
  worker-token
  minio-access-key
  minio-secret-key
  metadata.json
```

The private key and all three secret files are root-owned mode `0600`. They
must never be placed below `/shared_work`, embedded in an image, passed as
command arguments, or rendered by `docker compose config`. Installing the GB10
copy is a real GB10 root/Slurm-admin prerequisite; Docker group membership is
not installation authority.

Each NFS domain materializes a mode `0600` worker env containing the normal
non-secret worker settings plus these exact candidate-bound references:

```text
LOOM_WORKER_CONTROL_PLANE_URL=http://sandbox-link:8080
LOOM_WORKER_GATEWAY_URL=http://sandbox-link:9100
LOOM_WORKER_MINIO_ENDPOINT=http://sandbox-link:9000
LOOM_SANDBOX_LINK_CP_UPSTREAM=https://192.168.50.14:<cp-port>
LOOM_SANDBOX_LINK_GATEWAY_UPSTREAM=https://192.168.50.14:<gateway-port>
LOOM_SANDBOX_LINK_MINIO_UPSTREAM=https://192.168.50.14:<minio-port>
LOOM_WORKER_TOKEN_FILE_HOST=/etc/loom/developer-sandbox-links/clients/<sandbox>/<SHA>/worker-token
LOOM_WORKER_MINIO_ACCESS_KEY_FILE_HOST=/etc/loom/developer-sandbox-links/clients/<sandbox>/<SHA>/minio-access-key
LOOM_WORKER_MINIO_SECRET_KEY_FILE_HOST=/etc/loom/developer-sandbox-links/clients/<sandbox>/<SHA>/minio-secret-key
LOOM_WORKER_CP_TLS_CA_FILE_HOST=/etc/loom/developer-sandbox-links/clients/<sandbox>/<SHA>/ca.pem
LOOM_WORKER_CP_TLS_CERT_FILE_HOST=/etc/loom/developer-sandbox-links/clients/<sandbox>/<SHA>/client.pem
LOOM_WORKER_CP_TLS_KEY_FILE_HOST=/etc/loom/developer-sandbox-links/clients/<sandbox>/<SHA>/client-key.pem
```

It must not contain raw worker, MinIO, provider, API, password, or key
credentials. Validate both the OLDLAB and GB10 copies:

```bash
python scripts/ops/developer_sandbox_remote_link_host.py validate-env \
  --sandbox qianyi \
  --candidate-sha <SHA> \
  --env-file /shared_work/loom/runtime/sandboxes/qianyi/<SHA>/worker-oldlab.env
```

The non-exclusive sandbox Slurm job must append
`-f deploy/docker-compose.remote-worker.sandbox-link.yml` after the base remote
worker Compose file. A Compose-private `sandbox-link` sidecar owns the three
mTLS credentials and exposes only ports 8080, 9100, and 9000 to the worker
network; it publishes no host ports. The worker sees only its bearer token and
MinIO key files. The sidecar is read-only, cannot restart outside the Slurm
allocation, has positive CPU/memory/PID limits, inherits the job cgroup parent,
and carries the same sandbox/candidate/job/Compose cleanup labels. Missing,
writable, malformed, or cross-candidate material fails before registration.

### Plan, install, activate

All mutation commands are plan-only without `--execute`.

1. While the six sandbox capacity policies remain disabled, prepare one
   root-private issuance tree on oldlab2:

   ```bash
   sudo python scripts/ops/developer_sandbox_remote_link_host.py \
     prepare-rotation --sandbox qianyi --candidate-sha <SHA> --execute
   ```

2. Install, but do not activate, the server candidate:

   ```bash
   sudo python scripts/ops/developer_sandbox_remote_link_host.py \
     install-server --sandbox qianyi --candidate-sha <SHA> \
     --credential-source \
       /var/lib/loom/developer-sandbox-links/issuance/qianyi/<SHA>/server \
     --execute
   ```

3. Transfer each node's directory over the administrative encrypted transport
   to a root-private local inbox on that node. Never stage it on either NFS
   domain. With a host-local file containing only the already-minted sandbox
   worker token, the node administrator installs the exact node certificate:

   ```bash
   sudo python /secure/source/developer_sandbox_remote_link_host.py \
     install-client --sandbox qianyi --candidate-sha <SHA> \
     --node trt-gb10-2 \
     --credential-source /secure/inbox/qianyi/<SHA>/trt-gb10-2 \
     --worker-token-file /secure/inbox/qianyi/<SHA>/worker-token \
     --minio-access-key-file /secure/inbox/qianyi/<SHA>/minio-access-key \
     --minio-secret-key-file /secure/inbox/qianyi/<SHA>/minio-secret-key \
     --execute
   ```

   Repeat for all five OLDLAB nodes and the 14 configured GB10 nodes. GB10-7
   is not in the current capacity inventory and must not silently broaden it.

4. Confirm the sandbox itself is healthy, both env copies validate, and all
   clients are installed. Activate exactly one server SHA:

   ```bash
   sudo /usr/local/libexec/loom-developer-sandbox-remote-link-host \
     activate-server --sandbox qianyi --candidate-sha <SHA> --execute
   ```

5. From the operator host, require route, TLS handshake, exact client identity,
   and `/healthz` from every eligible worker node:

   ```bash
   python scripts/ops/developer_sandbox_remote_link_host.py fleet-check \
     --sandbox qianyi --candidate-sha <SHA> --execute
   ```

   `fleet-check` uses non-interactive SSH and the node's root-installed
   `check-client`; one inaccessible node, missing root authority, route failure,
   certificate mismatch, TLS downgrade, secret-file metadata drift, or any
   unhealthy Control Plane/Gateway/MinIO response fails the whole gate. A green
   check atomically persists a root-owned mode-`0600` receipt at
   `/var/lib/loom-developer-sandbox-links/attestations/<sandbox>/<SHA>/fleet.json`.
   Its canonical digest binds the exact five OLDLAB and 14 GB10 nodes, oldlab2
   relay state, all three listeners, bundle generation, and 15-minute expiry.
   Do not enable capacity from a missing, stale, incomplete, or digest-mismatched
   receipt.

### Rollback and readback

Rollback is an explicit atomic switch to a previously installed exact SHA; it
does not accept old and new certificates simultaneously:

```bash
sudo /usr/local/libexec/loom-developer-sandbox-remote-link-host \
  rollback-server --sandbox qianyi --candidate-sha <PRIOR_SHA> --execute
```

After rollback, restore the prior candidate-bound env references and rerun
`fleet-check` for the prior SHA before re-enabling capacity. Readback output is
secret-free: it includes sandbox, node, candidate SHA, route/health status, and
certificate fingerprints, never certificate bodies, private keys, tokens, or
environment dumps.

## Cross-sandbox negative probes (A3)

Automated negatives live in-repo and do not require `oldlab-2`:

1. Static profile isolation:

```bash
uv run --no-sync python scripts/validate_developer_sandbox_isolation.py \
  --profiles-dir deploy/developer-sandboxes --json
```

2. CI dual-stack crossover tests:
   `tests/integration/test_developer_sandbox_crossover.py` (foreign worker/admin
   tokens → 401; foreign MinIO creds / bucket names rejected).

3. Secret-safe probe helper (default dry-run; pass secret **file paths** only):

```bash
uv run --no-sync python scripts/ops/developer_sandbox_crossover_probe.py \
  --qianyi-cp-url http://127.0.0.1:20080 \
  --qianyi-worker-token-file /secure/qianyi.env \
  --qianyi-admin-secret-file /secure/qianyi-admin.toml \
  --hongjian-cp-url http://127.0.0.1:21080 \
  --hongjian-worker-token-file /secure/hongjian.env \
  --hongjian-admin-secret-file /secure/hongjian-admin.toml \
  --write-evidence /tmp/a3-crossover-dry-run.json
```

Add `--execute` only after three sandboxes are installed (see installer
follow-up) for the live six-edge pairwise matrix. Evidence JSON records
fingerprints and status codes only — never raw `loom_w_*` / admin / MinIO
secrets.

A3 crossover evidence is **not** `#896` soak evidence.

### Profile identity notes

- `provider_connection_namespace` (`sandbox-<owner>`) is profile identity today;
  there is no `src/` runtime consumer that enforces it yet.
- `object_store.task_bucket` / planner `LOOM_DEV_TASK_BUCKET` name MinIO task
  buckets for ops; Compose wires trajectories/artifacts buckets into
  `loom-service`. Do not treat unset compose task-bucket env as a security
  boundary.

## Shared capacity brokerage

Capacity brokerage and the broker→WPAP handoff adapter are documented in
[`shared-sandbox-capacity-broker.md`](shared-sandbox-capacity-broker.md). This
sandbox runbook does not configure Slurm packing or enable shared-worker pools.
