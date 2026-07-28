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
