# GB10 Remote Worker Pool

This directory records the staging GB10 worker-pool policy validated for issue
#518. The pool attaches ARM64 GB10 hosts to the OLDLAB-1 staging control plane
as Slurm-managed Docker workers. Slurm owns normal capacity request/release;
the node-agent remains the host-local convergence mechanism.

## Topology

- Control plane host: OLDLAB-1, reached as `platform-dev`, `oldlab-1`, or
  `oldlab1`.
- Kubernetes namespace: `loom-staging` in `kind-loom-staging`.
- Worker hosts: `trt-gb10-1` through `trt-gb10-15`.
- Temporary staging active set: every inventory host except `trt-gb10-7`.
  Issue #822 records that node as `unreachable`; staging remains fail-closed on
  the other 14 hosts and 140 slots until a separate merged re-admission change
  restores the node.
- Slurm partition and Loom pool: `gb10` and `gb10`.
- Release-managed SSH topology:
  `deploy/worker-pools/gb10/ssh_config`. `trt-gb10-1` is the only public
  entrypoint on port `2221`; `trt-gb10-2` through `trt-gb10-15` use their
  private addresses on port `22` through `ProxyJump trt-gb10-1`.
- Host checkout and Compose root:
  `/home/qianyi/loom-worker-build-staging`.
- Host runtime env: `/home/qianyi/loom-worker-build-staging/.env`, mode 0600.

The worker-facing OLDLAB-1 services stay private. Public internet traffic must
continue to reach only Web/API over TLS. Remote workers use the existing local
tunnel endpoints:

```text
LOOM_WORKER_CONTROL_PLANE_URL=http://127.0.0.1:18081
LOOM_WORKER_GATEWAY_URL=http://127.0.0.1:19100
LOOM_WORKER_MINIO_ENDPOINT=http://127.0.0.1:19000
LOOM_WORKER_SUBPROCESS_GATEWAY_URL=http://host.docker.internal:19100
```

The worker normalizes the subprocess gateway root by adapter:
Codex/OpenAI-compatible agents receive `/openai/v1`, Claude Code receives
`/anthropic`, and Gemini receives `/google`. A GB10 env that pins Codex to an
explicit `/anthropic` facade is invalid.

## Protected Rollout Ownership

Normal checkout, env, desired-state, service, and release convergence belongs
to the root-installed `loom-staging-rollout` broker on `platform-dev`. The
detached unit runs as the non-login `loom-rollout` service account. Every new
request fresh-fetches and pins the current merged `refs/heads/dev`; an operator
cannot select a commit, ref, image tag, host subset, env file, credential,
concurrency, or force flag.

Before the driver may mutate a host unit, the deploy identity must report
`Linger=yes` from `loginctl show-user`. Bootstrap this once through the host's
normal user/admin path with `loginctl enable-linger "$USER"`; a missing linger
grant fails rollout step 12 closed.

The driver preserves this order for every active host:

1. apply the candidate-owned environment-state profile;
2. validate the exact desired image, env version, and source SHA;
3. prepare the host checkout and generated private env;
4. retire the legacy direct-worker service;
5. install the candidate's node-agent service and timer, start the oneshot once,
   then enable and restart the periodic timer with bounded host concurrency;
6. verify the installed unit bytes, linger grant, successful oneshot result,
   and enabled/active/waiting timer;
7. require all 14 declared active-host reports and fresh linked worker registrations;
8. pass environment-state, release-gate, and smoke.

Use only the public broker interface:

```bash
loom-staging-rollout start --dry-run
loom-staging-rollout start
loom-staging-rollout status REQUEST_ID
loom-staging-rollout logs REQUEST_ID
loom-staging-rollout resume REQUEST_ID
loom-staging-rollout cancel REQUEST_ID --reason "bounded operational reason"
loom-staging-rollout cleanup-incomplete-backup REQUEST_ID
```

A disconnected terminal does not stop the service unit. Do not run a personal
systemd unit, hand-edit host checkouts or `.env`, write desired-state rows with
SQL/curl, invoke per-host `gb10-agent apply`, force a lifecycle/rollout lock,
or substitute an arbitrary commit. A code rollback is a merged revert on
`dev`, followed by another broker request. A host-runtime failure is repaired
at its root cause and then continued with `resume REQUEST_ID` against the same
immutable envelope and SHA.
The cleanup command is not a retention or arbitrary delete surface: it accepts
only a failed pre-launch request and refuses manifest-backed or `latest`
backups.

## SSH Trust

The broker uses the service-owned
`/var/lib/loom-staging-rollout/gb10-deploy-ed25519`; operators never receive or
forward that private key. The fixed trust command validates the exact 15
checked-in aliases, literal host addresses, remote user, service identity,
auth flags, and jump topology. SSH host authentication is pinned to the
checked-in `known_hosts` authority, installed root-owned at
`/etc/loom/staging-rollout-gb10-known-hosts`; ambient user or global
known-hosts state and `accept-new` are not permitted. The fixed physical
inventory remains all 15 hosts, while the active-host policy may temporarily
exclude a quarantined host such as `trt-gb10-7` without weakening revocation
coverage for trust granted under an earlier policy.
Bootstrap and normal checks target the 14-host active set; the full inventory
remains authoritative for topology validation, legacy trust cleanup, and later
#822 re-admission.

Use an explicitly approved Ed25519 admin identity to bootstrap the service
public key once per new service-key lifecycle, or as controlled recovery when
`check` reports drift. Use `bootstrap --bootstrap-identity PATH` for a fresh
canonical marker. If that approved bootstrap key itself occupies the one
canonical `loom-staging-rollout` marker, use the explicit
`rotate-bootstrap --bootstrap-identity PATH` transition instead. Rotation
accepts only the exact canonical bootstrap or service entry, processes private
hosts before the jump host, and is idempotent across partial retries. Missing,
duplicate, option-prefixed, unrelated, or tombstoned markers fail unchanged.
Both operations derive and send only public keys; they do not copy or print a
private key. Normal verification uses the service identity:

```bash
sudo /opt/loom-staging-runner/venv/bin/python \
  /usr/local/libexec/loom-staging-rollout-gb10-trust check

loom-staging-rollout start --dry-run
```

Any missing active host, missing pinned host key, ambiguous authorized-key
match, wrong topology, or failed SSH probe fails closed. Uninstall revokes only
this service public key on every
host recorded in the durable revocation ledger before deleting locally
generated key material; do not delete or replace the local key or install
ledger first. Trust bootstrap, check, revocation, installer migration, and
uninstall share one root-owned lifecycle lock. Revocation converts the managed
remote key into an inert `restrict` plus fixed-command tombstone so an
interrupted local ledger update can reconnect with the same key and finish
safely. Bootstrap with the approved admin identity restores that tombstone;
uninstall intentionally leaves it in `authorized_keys` for later controlled
admin cleanup.

## Tunnel Recovery

OLDLAB-1 owns durable private service tunnels and their watchdog. Installed
units must use durable `kubectl`, kubeconfig, script, and env paths rather than
`/tmp`. The broker runs the local, watchdog, and all-declared-host tunnel gates
inside the candidate-bound request and records only safe health-check URLs.
Operators inspect those artifacts with
`loom-staging-rollout status REQUEST_ID` and
`loom-staging-rollout logs REQUEST_ID`; they do not rerun the helper with
interactive env, kubeconfig, or host inputs.

Repair is root-owned broker maintenance. Disable admission, prove the active
request terminal, repair the durable installed units from clean merged `dev`,
restore admission, and resume the original request. Do not substitute an
untracked port-forward or an operator-owned tunnel as rollout evidence.

## Node-Agent Lifecycle Contract

Each GB10 host periodically compares Control Plane desired image, pool,
concurrency, env version, source SHA, and host intent with its local state. The
Control Plane does not SSH into GB10 and does not store worker tokens or MinIO
credentials. The broker materializes protected env sources and the node-agent
reads them locally.

Apply is restartable. When metadata already matches and host intent is active,
the node-agent still reconciles the worker service. On source or env drift it
fetches the approved repository, checks out the candidate SHA, prepares the
image, drains the old worker, and starts the target worker. Temporary Compose
env files are private and remain outside the checkout. A missing registry image
may trigger a candidate-bound local build; it may not fall back to a different
tree. The periodic active/no-drift path reuses a container whose runtime image
already matches the desired tag, including a worker that exited normally after
its idle window; it does not pull or rebuild that image on every timer tick.
No-container or runtime-image drift still performs the candidate-bound
pull/build before Compose reconciliation. `draining` and `stopped` intents
never pull, build, or start the worker.

The node-agent user service is `Type=oneshot`; successful convergence may end
as `ActiveState=inactive` / `SubState=dead` with `Result=success`. Release
evidence therefore uses the Control Plane report and linked worker heartbeat,
not `is-active` alone. Every active host must report:

- the requested staging image, env version, source SHA, and clean checkout;
- applied active intent and expected capacity;
- `worker_id` present, `worker_status=active`, and a fresh heartbeat;
- `docker` in `worker_backend_names`.

The legacy `loom-gb10-worker.service` direct Compose path must remain disabled
during release-managed operation. It can otherwise race the node-agent and
recreate a worker outside desired-state control.

Do not run a second environment's node-agent identity against the same local
tunnel ports and Compose root. In particular, a legacy production timer must
remain stopped or use an isolated tunnel/runtime while the staging compatibility
pool is active; restoring shared connectivity can otherwise make both desired
states reconcile the same host concurrently.

## Current Validated State

Evidence date: 2026-06-25. This snapshot is historical and must be refreshed by
the candidate-bound release gate before use.

- `trt-gb10-1..15` were worker-enabled and `aarch64`.
- Docker and the private Control Plane, Gateway, and MinIO endpoints passed on
  all 15 hosts.
- Every worker advertised `cpu_arch=arm64` and pool `gb10`.
- At that evidence date, declared trial concurrency was 10 per host and 150
  total slots. The current #822 exception declares 14 active hosts and 140
  staging slots while retaining node 7 in the physical inventory.
- Canary batch `b18d1a92-909a-443f-a768-f0aae8229cea` finished succeeded;
  trial `6e833772-ae85-4bf0-9621-904cb9bca0ea` ran on `trt-gb10-6` and scored
  `1.0`.
- The fresh 2026-06-25 recheck observed 15/15 active ARM64 worker rows.

See `inventory-2026-06-25.txt` and `smoke-evidence-2026-06-25.json` for the
non-secret historical evidence.

## External autoscaler supervisor (systemd)

Each environment's env-state profile carries an
`external_slurm_autoscaler_supervisors` section. Every entry renders a
`systemctl --user` service plus timer that periodically runs the repo
entrypoint `scripts/ops/worker_pool_autoscaler_external_once.py` for one pool.
`loom admin environment-state apply` writes the unit files under
`~/.config/systemd/user`, and `check` reports drift when a unit is missing,
points at a stale checkout, omits `--pool-name`, or is not enabled/active as
declared.

Each supervisor tunnels to the environment's Postgres on a reserved local port,
so no two supervisors on one host collide. The `--db-local-port` scheme is:

| pool   | development | staging | production |
| ------ | ----------- | ------- | ---------- |
| oldlab | 15447       | 15448   | 15449      |
| gb10   | 15450       | 15451   | 15452      |

Supporting layout, shared across environments:

- Runner checkout and virtualenv: `/opt/loom-<environment>-runner/repo` and
  `/opt/loom-<environment>-runner/venv`.
- Kubeconfig: `/etc/loom/kubeconfig/<environment>.yaml`.
- Health check: `systemctl --user is-active loom-autoscaler-gb10-<env>.timer`.

The staging GB10 supervisor ships `enabled=true` and `active=true`: applying the
staging profile installs, enables (for boot), and starts the timer. The
development GB10 supervisor ships `enabled=false` and `active=false`
(fail-closed): applying the profile writes the unit files but does not enable or
start the timer, mirroring the pool's own `enabled=false` gate pending #827
(external-Slurm acceptance) and #896 (container isolation).

## Storage Policy

Use each GB10 node's local ext4 root disk for Docker data and worker hot paths.
Do not put Docker `overlay2`, trajectory/benchmark cache, trial scratch,
Postgres, MinIO backend data, kind volumes, or Kubernetes PV data on
`/shared_work2`. That NFS export is suitable for immutable/read-mostly candidate
checkout staging or evidence transfer, not high-churn runtime state. Trial
artifacts return through Loom's artifact/trajectory object-store path.

The exporter account has no general noninteractive root authority. A reviewed
sealed-cumulative repair uses the repository-owned
`staging_rollout_shared_work2_export_authority.py` boundary instead. An
external administrator first provisions the exact detached root-owned checkout
at `/opt/loom-staging-exporter-authority/source` and runs its `bootstrap` verb
locally as root with the independently reviewed commit and tree. Bootstrap
owns idempotent creation of `/usr/local/libexec`, `/etc/loom`, and its dedicated
state root as root-owned mode-`0755` directories in the same transaction as its
assets. Pre-existing exact directories are retained; a wrong type, owner, or
mode fails closed. Any failed first publication removes every directory and
asset created by that attempt in reverse order. Bootstrap is not present in
the operator sudoers rule. It grants only two exact commands to `qianyi`:

```text
sudo /usr/local/libexec/loom-staging-rollout-shared-work2-export-authority install
sudo /usr/local/libexec/loom-staging-rollout-shared-work2-export-authority check
```

The commands accept no further arguments. Root reloads the fixed sealed policy,
validates wrapper/sudoers/source identity, and invokes only the exact export
fragment helper. `check` is read-only; `install` is locked, journaled, atomic,
idempotent, creates a missing exact root-owned mode-`0755` `/etc/exports.d`,
and rolls back the fragment and a directory created by that attempt if
`exportfs -ra` fails. A pre-existing wrong directory fails closed.
Do not request a sudo password, allow a shell or wildcard command, enable root
SSH, or reuse the rejected 14-host root-helper design.

When the exporter operator already has access to the rootful Docker daemon, a
sealed one-time bootstrap image may replace the external console action. Build
that ARM64 image only from the reviewed cumulative source with
`Containerfile.shared-work2-export-bootstrap`, export and hash the exact image,
and run it by content ID with no command override. The accepted invocation has
no network, a read-only container root, no new privileges, and only
`CHOWN`, `DAC_OVERRIDE`, and `FOWNER`; it binds the bundle read-only and the
four necessary host parent directories with recursive bind propagation
disabled. Those parent binds are required because the exact authority
directories are initially absent and must remain inside one rollback domain.
The fixed Python entrypoint validates its capability, mount, usable-network,
bundle, and sealed-source identities before any mutation. An interactive entrypoint,
`--privileged`, a writable host-root bind, a recursive `/var/lib` bind, or a
different image/argv/environment is not an accepted bootstrap channel.
The build and entrypoint both reject a non-ARM64 target so an AMD64 artifact
cannot depend on optional host emulation.

## Health And Scheduling Gates

The broker's environment-state and release-gate artifacts are authoritative.
They must cover all 14 active desired node reports and all 14 fresh linked
worker registrations. Node 7 must remain `stopped`/`unreachable` and absent
from rollout targets. Tunnel health, Docker reachability, source cleanliness,
image/env identity, capacity, and worker heartbeat failures on any active host
remain red; a runtime skip or partially healthy active fleet is not accepted.
After rollout, close the operator SSH session, wait longer than one timer
period, stop the worker on one active canary, and require the timer to restore
the candidate image plus a fresh heartbeat within the next period. Recheck that
node 7 has no fresh active worker.

GB10 workers must not claim legacy tasks that lack `environment.cpu_arch`;
Loom treats those requirements as `x86_64`. Only tasks explicitly marked
`arm64` or `any` may run on GB10. Use `any` only after the task image, verifier,
and artifacts are credible on both architectures. Legacy or `x86_64` claims by
`trt-gb10-*` remain invalid unless a later issue adds a credible compatibility
boundary.

## Maintenance And Cleanup

Capacity cleanup is separate from rollout mutation. Drain or prove a worker
idle before pruning trial containers, networks, images, or build cache. Never
delete `remote_worker_trajectories` or `remote_worker_benchmarks` during a
normal restart. Delete those volumes only for an explicitly approved local
cache wipe.

If a host becomes unstable, preserve request and node evidence, fix the host
resource/runtime cause, and resume the same request. Do not reduce the declared
fleet, lower a gate, change desired state interactively, force apply, or choose
a replacement SHA merely to make staging pass.
