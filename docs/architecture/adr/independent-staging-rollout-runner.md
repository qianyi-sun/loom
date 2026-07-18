# ADR: Independent Staging Rollout Runner

Status: accepted

Date: 2026-07-13

Tracking: [#803](https://github.com/qianyi-sun/loom/issues/803)

Temporary amendment (2026-07-14): [#822](https://github.com/qianyi-sun/loom/issues/822)
keeps the full 15-host SSH/trust inventory but excludes `trt-gb10-7` from the
active staging target. Until a separate merged re-admission change passes, the
acceptance boundary is all 14 active GB10 hosts and 140 slots, with node 7
persistently stopped/unreachable and no runtime override.

## Context

The rollout driver was restartable, but the supported staging procedure still
depended on Qianyi's checkout, user-level systemd manager, private GB10 key,
credentials, and terminal. Sharing those inputs with more users would not fix
stale refs, whole-rollout concurrency, backup ownership, or attribution. It
would also weaken the existing owner-only private-key check.

Hongjian, Devansh, and Qianyi need to update and test the same protected
staging environment independently. The owner policy permits testing only after
code has merged. No operator may select a pull-request ref, feature branch,
tag, historical commit, local checkout, image tag, alternate remote, or a
different physical target.

The three operators already have host-admin and Docker authority on
`platform-dev`. The runner therefore provides a narrow, attributable, and
repeatable operational interface; it is not a confidentiality boundary against
a deliberately malicious root-equivalent administrator.

## Decision

Install one root-managed runner on `platform-dev` with a non-login
`loom-rollout` service account and a `loom-staging-operators` group containing
`qianyi`, `hongjian`, and `devansh`. The root-owned client exposes only:

```text
loom-staging-rollout start [--dry-run]
loom-staging-rollout status [REQUEST_ID]
loom-staging-rollout logs REQUEST_ID [--follow]
loom-staging-rollout resume REQUEST_ID
loom-staging-rollout cancel REQUEST_ID --reason TEXT
loom-staging-rollout cleanup-incomplete-backup REQUEST_ID
```

The broker derives the caller from the authenticated sudo context and records
the initiating and attempt operators. It accepts no caller, repository, ref,
SHA, tag, path, cluster, secret, concurrency, skip, force-lock, or arbitrary
command override.

### Merged-only candidate binding

For every new request, the broker validates the fixed
`https://github.com/qianyi-sun/loom.git` origin, fresh-fetches exactly
`refs/heads/dev`, resolves its 40-character head SHA, and derives
`staging-<sha7>`. That immutable binding is written to a private request and
attempt envelope before the driver starts. A resume reuses the original SHA,
image tag, backup, config digest, and rollout ID even when `dev` has advanced.

Rollback of code is a merged revert on `dev` followed by a new request. Direct
deployment of an older SHA is not an operational rollback mechanism.

### Coordinator-only sealed cumulative repair amendment

For an explicitly approved replacement-attempt incident, Qianyi may batch
multiple independently reviewed local fixes and defer the single durable PR
until staging acceptance succeeds. The default remains merged-only. The root
installer exposes one explicit `sealed-cumulative` mode that accepts no path or
ref override and binds the fixed invocation checkout to an exact commit, tree,
and approved merged base. It requires a standalone root-owned clean detached
checkout, rejects Git indirection and non-linear or over-32-commit history,
and copies only that exact commit into the root install source and service
candidate without fetching or resolving `origin/dev`.

The checkout validator does not follow repository symlinks. It permits only
mode-`120000` paths that agree between the exact sealed tree and stage-zero
index, whose no-follow ownership and link count are safe, and whose literal
`readlink` payload hashes to the exact tree blob. Absolute, escaping, chained,
directory-targeting, untracked, type-drifted, `.git`, or otherwise dirty
symlinks remain fail-closed.

The shared-work2 exporter consumes the same exact source validator before its
first mutation. The install ledger and protected config record the source mode,
commit, tree, and base; immutable requests and attempt envelopes carry and
revalidate the same provenance. While that config is installed, broker
`start`/`resume` is restricted to Qianyi before preflight or request creation.
Status and logs remain available to the other operators. Manual file copying,
Git-object injection, alternate source paths, and implicit fallback to merged
or cached refs remain unsupported. After the accepted rollout, the accumulated
changes return through the normal reviewed PR and protected-gate process.

The exporter does not gain general remote-root authority from this amendment.
Its coordinator account may invoke only two exact `NOSETENV` sudo commands:
the fixed root wrapper's `install` and read-only `check` verbs. The wrapper
accepts no SHA, path, network, environment, or arbitrary argument. It reloads a
root-owned policy, verifies its own bytes and sudoers bytes, revalidates the
fixed root-owned detached checkout against the exact cumulative commit, tree,
and approved base, and only then invokes the one fixed export helper with those
policy values. The helper itself revalidates the same source and exact export
fragment before mutation. Install is serialized and journaled; check takes a
shared read-only lock and writes no evidence file.

The exporter currently has no supported noninteractive root bootstrap channel.
Therefore a one-time external administrator must provision the fixed sealed
checkout and run the reviewed bootstrap entrypoint locally as root. Bootstrap
validates the sealed checkout and sudoers asset before mutation, installs the
root-owned wrapper, validator, policy, lock, journal, and exact command-only
sudoers rule with sudoers last. Its root-owned mode-`0755` directory roots are
part of the same transaction; pre-existing exact directories are retained,
wrong metadata fails closed, and a failed first publication removes all
directories and files created by that attempt in reverse order. This explicit
external authority requirement must not be replaced with a password request,
root SSH, a sudo wildcard, or the abandoned multi-host root-helper design.

An operator who already controls the rootful Docker daemon may satisfy the
same one-time boundary with the content-addressed exporter-bootstrap image.
That path grants no new authority: the GB10 image is ARM64-only, has a fixed Python entrypoint,
network disabled, a read-only root filesystem, no-new-privileges, and only the
three filesystem capabilities required to publish root-owned exact assets. It
binds the sealed bundle read-only and the four required host parents with
recursive bind propagation disabled. The entrypoint proves the mount roots,
capability mask, bundle digest, source identity, detached checkout, and
bootstrap result, and rolls back a newly published source if bootstrap fails.
Privileged containers, writable host-root binds, interactive entrypoints, and
recursive exposure of Docker state are outside the accepted authority model.
Both the build and entrypoint reject an architecture mismatch.

Candidate-bound authenticated staging browser acceptance follows the same
authority. Only a broker-owned rollout step may exchange the singleton admin
bearer, and its sanitized report must bind the request/attempt envelope's
resolved SHA to the build SHA read from the running service. Pull-request kind
CI remains credential-free and non-protected; its `development` runtime may
only prove that the staging-only exchange returns `404`. A kind artifact,
manual invocation, ambient checkout, or unmerged ref is never candidate
evidence. Until the broker-owned positive browser step completes, this
acceptance row remains unmet.

### Service authority and secrets

The root installation owns the runner source, venv, client, broker, sudoers,
policy config, and install ledger. The service account owns the candidate
checkout, request ledger, kubeconfig, generated worker env, runtime locks, and
a dedicated Ed25519 GB10 deploy identity. Under #822, that public key is
bootstrapped to the exact 14-host active set; the full 15-host checked-in
topology remains validated and is retained for legacy-ledger revocation. The
private key remains mode 0600 and is never shared with an operator.
The `loom-rollout` passwd UID and `id -u loom-rollout` must resolve to the same
nonzero value. Its passwd primary GID, the GID of the named `loom-rollout`
group, and `id -g loom-rollout` must likewise resolve to the same nonzero value.
Installation and readiness checks fail closed when those service identity
views disagree.

The installer itself has an operator-established bootstrap prerequisite: invoke
it only from a clean root-owned checkout beneath a root-owned,
non-group/world-writable parent chain. A user-owned checkout is unsupported and
must not be added to Git's global `safe.directory` list. Because Python has
already loaded the checkout before the in-process ownership check runs, that
check detects accidental pre-mutation drift; it is not an adversarial bootstrap
trust boundary. This matches the declared operational threat model in which
the installing root/Docker operators are already root-equivalent. Both the
installer-managed Git path and the installed broker establish a fixed `0077`
umask before service-owned candidate Git commands; the broker also disables
system/global Git configuration and terminal prompts in its exact child
environment. During a
maintenance/inactivity transaction, an existing candidate may have group/world
write bits removed only after its full tree has passed service ownership,
ordinary-file/directory type, and contained symlink validation; any other drift
fails closed. Readiness revalidates that complete tree before running Git or
loading candidate configuration.

The private generated-worker-env directory is not self-bootstrapping: the
candidate env-state step can derive a new release file only from an existing
complete template. During an inactive, admission-closed install transaction,
the installer therefore migrates the newest fixed-name legacy staging GB10 env
into one service-owned mode-`0600` bootstrap file when the private directory has
no template. It opens only a bounded, single-link regular source that is not
group- or world-writable,
validates UTF-8 dotenv structure and all required control-plane, Gateway,
worker-token, and MinIO keys, and copies atomically without reporting values.
Existing private templates are preserved. Install readiness fails when neither
a safe private template nor a safe legacy bootstrap source exists. Runtime
materialization likewise rejects symlinked, hard-linked, non-regular, oversized,
or non-`0600` private env sources before reading them.

Shared GB10 worker checkouts use the GB10 NFS export mounted at
`/shared_work2`, not the separate OLDLAB/token storage at `/shared_work`.
The exporter allowance is a repository-managed exact
`192.168.50.103/32` entry with the existing `/shared_work2` export options;
the platform-dev installer owns a fixed `shared_work2.mount` unit for exact
source `192.168.20.12:/shared_work2`, NFSv4.2, hard TCP mounts, and
`nosuid,nodev,noexec`. Both installer and broker verify the exact mountinfo
source, mountpoint, filesystem type, options, and device identity so a local
empty directory cannot satisfy readiness. During an inactive,
admission-closed transaction, the installer creates the previously absent
`/shared_work2/qianyi` parent as `qianyi:sharedwork` mode `2775`, then creates
`/shared_work2/qianyi/.loom-staging-rollout/worker-repos` as
`loom-rollout:sharedwork` mode `2750` and records the resolved owner and
consumer UID/GID values. It does not add the service to `sharedwork` or grant
write on the operator parent. Runtime checks prove the service can write/search
the dedicated root and the `qianyi` Slurm submitter can read/search but not
write it. Secret/token env files remain private mode `0600` platform-dev-local
state and are never materialized into `/shared_work2`.

The constrained Docker bootstrap may transactionally advance an exporter
authority only before its first successful install. It holds the old exclusive
authority lock, requires an empty journal and either no export fragment or the
exact old fragment bound to the single exact canonical NFS `etab` record,
proves a same-base descendant sealed commit, disables sudoers first, and keeps rollback
copies beside the fixed files. New bootstrap failure restores the old exact
identity with sudoers last; success removes the copies only after new identity
validation. This is not a general authority-upgrade mechanism and refuses any
authority with install evidence.

Candidate checkout publication is a single-writer lifecycle. Step 11 accepts
only the exact image-tagged direct child, verifies every authority path with
no-follow metadata, and atomically claims that final child with no-replace
`mkdir` at private mode `2700`, inheriting sharedwork/setgid from the authority
root. It clones with `--no-hardlinks` while consumers cannot search the claimed
directory. It permits tracked git symlinks but rejects authority symlinks,
foreign ownership, group/other write, hard-linked or special files, and a
non-exact resolved HEAD. After complete validation, an inode-bound `fchmod`
from `2700` to immutable consumer-readable `0750` is the publication point.
The candidate path is immutable: an existing exact checkout is reused only
after full index/physical-tree validation, while any private, different HEAD,
mode/content drift, or extra directory fails without replacement.
Materialization cleans only the unchanged private inode it created and never
removes or takes over an ambient path. Install/check also performs a bounded,
self-cleaning private-claim/access-gate/publish/collision probe as the service
identity. The protocol requires only NFSv4 `mkdir` and mode semantics and does
not assume optional Linux `RENAME_NOREPLACE` support from the NFS server.
When a sealed cumulative source refreshes the service-owned local candidate,
the fixed local fetch uses an exact `git upload-pack` command whose sole
`safe.directory` value is the root-owned install source's `.git` directory.
It does not persist a wildcard or user-controlled Git safety exception.

The broker preflight verifies the fixed 14 active GB10 nodes can consume the
shared root as `qianyi` without writing it. The 13 NFS clients must report the
exact source and NFSv4 mount identity; `trt-gb10-2` must report the ext4 export
backend at the same mountpoint. After publication and before
environment-state mutation, step 11 captures verifier bytes from the exact
resolved commit's Git blob and streams them over the protected SSH stdin path.
It does not execute code from the mutable rollout worktree or checkout under
verification. Exact HEAD/status/index/mode/readability
and non-write checks must agree on all nodes; per-node NFS device/inode evidence
is retained without cross-node equality assumptions. Non-zero transport or
verifier exits receive a thirteen-observation, 390-second bounded retry before
failing closed, while valid divergent content evidence is never retried.

The root venv is built only with a fixed root-owned `/usr/local/bin/uv` and the
safe resolved target of `/usr/bin/python3`, which must be Python 3.11 or newer
and remain under `/usr`. Because the repository intentionally treats
`pyproject.toml` rather than a tracked `uv.lock` as its cross-environment
dependency authority, the installer synchronizes the freshly selected merged
candidate without editable installs and without the inapplicable `--frozen`
flag. A source-SHA change forces another synchronization. Every synchronization
also uses uv's exact `--reinstall-package loom` boundary, so a same-SHA repair
restores deleted or corrupt package resources instead of accepting uv's
already-satisfied result. A previous install record in any state other than
`ready` forces both candidate-checkout and venv resynchronization even when its
recorded source SHA matches the newly fetched `dev` head. A crash after the
provisional record is published therefore cannot let retry bless the old wheel
as the new source.
The non-editable wheel bundles exact, repository-tested copies of the canonical
Loom schema, Grafana dashboards, Envoy bootstrap, and imported Jinja template
partial through `importlib.resources`; runtime code never derives those
resources from the wheel's parent directories. CI builds and installs the wheel
before exercising a full default manifest render. The installer first uses
`sudo -u loom-rollout`, a clean environment, and Python `-I -B` to import the
installed broker and render those packaged resources without requiring a
pre-existing host configuration. It then writes the fixed operator config as
`root:loom-rollout` mode `0640` and, before restoring admission, runs the full
service-user broker probe, which also loads that config. The config is readable
by the service group but is never group- or world-writable and contains no raw
secret values. It must be a regular file with exactly one link. The installed
loader and host readiness check enforce the same `root:loom-rollout`, `0640`,
`nlink=1` authority. The installed broker entry point uses the same
isolated/no-bytecode interpreter boundary, so an operator-controlled working
directory cannot shadow `loom_cli`.

The service account receives traverse-only parent ACLs and leaf-read ACLs only
for the declared staging token and catalog inputs. Preflight traverses those
parents with Linux `O_PATH` and `O_NOFOLLOW`, so it neither needs nor receives
directory-listing permission. The staging data root remains read/traverse-only;
its declared rollout, Postgres, MinIO, backup, and pre-existing
`environment-state` subdirectories receive access/default `rwx` ACLs. Secret
values stay in protected file sources and are excluded from argv,
request/status JSON, journals, rollout evidence, and summaries. This does not
make secrets inaccessible to the already
root-equivalent administrators; a stronger boundary would require a separate
runner host and credential rotation.

The directory ACL is also the migration boundary for legacy environment-state
profiles. A pre-existing operator-owned mode-`0600` leaf is not made readable
to the service account. Candidate materialization treats an unreadable legacy
leaf as stale and atomically replaces it with a service-owned mode-`0600`
candidate copy, rather than widening the old leaf ACL or consuming its bytes.
The destination is inspected with no-follow metadata first: a symlink or any
other non-regular entry fails closed before read, chmod, or replacement, so an
external referent cannot become part of the materialization authority.

ACL convergence is fail-closed. The installer computes the smallest explicit
mask expansion needed by the service entry and compares the complete effective
ACL before and after. Only the declared human operators may gain permissions
that were already present in their raw named-user entries; every other user or
group must have zero effective-permission gain. In particular, the path-scoped
numeric UID 2012 entry used by OLDLAB-2 is preserved but may never be unmasked
by this installer. A missing default ACL is initialized from the directory's
effective access base only when required for service-owned data, and that
complete initialization remains reversible. POSIX ACL masks are also reflected in the
numeric group mode bits, so validation uses raw and effective ACL entries rather
than claiming that `st_mode` is unchanged.

### Backup and locking

A non-dry `start` creates and verifies a new immutable backup manifest before
the rollout can mutate staging. It never relies on a mutable `latest` pointer.
A failed backup keeps the prior valid backup and prevents unit launch. An
aggregate-only live inventory on 2026-07-15 (no keys, credential values, or
payloads emitted) measured 579,714 protected objects and
12,517,813,079 bytes. The root-owned config therefore fixes the reviewed
staging policy at 1,000,000 MinIO objects (1,000,004 files across the complete
bundle) and 16,000,000 conservative entries, while the byte, inode, page, depth,
elapsed-time, path-safety, free-capacity, and immutable-publication bounds
remain independent and fail closed. Object exhaustion has the stable public
reason `backup_object_limit_exceeded`.

The MinIO transport is broker-owned and collision-free. Before `pg_dump`, the
broker launches a localhost-only `kubectl port-forward` with an ephemeral local
port (`:9000`), accepts the port only from the exact child readiness record,
and keeps that child alive through the mirror. Cleanup targets and waits for
only that child; unrelated long-running or concurrent port-forwards, including
anything bound to the historical port `19000`, are never reused or terminated.
No manifest, request envelope, attempt, or rollout unit may be published unless
the transport was ready and its bounded cleanup was confirmed. Transport
startup/cleanup failures use the redacted public token
`backup_transport_failed` while the immutable manifest, capacity limits,
secret redaction, and supported incomplete-backup cleanup contracts remain
unchanged.

The measured conservative entry charge is 6,420,179, maximum mirror depth is
18, and maximum direct fanout is 6,530. At 80% utilization (800,000 objects or
12,800,000 entries), the platform owner must repeat the aggregate shape review
in a merged PR; runtime auto-growth is forbidden.

`cleanup-incomplete-backup` is the only supported incomplete-backup removal
path. It runs under the same admission/launch lock, accepts only a failed
request before any envelope or attempt, selects exactly that request's
timestamped root, and performs bounded no-follow validation before removal. A
manifest, `latest`
target, symlink, hard link, special file, ownership/mode drift, or traversal
limit causes refusal. The command is idempotent and the request remains failed
in the append-only audit ledger.

The broker launch mutex atomically arbitrates request admission. One separate
full-lifecycle lock covers the detached driver from pending through terminal
bookkeeping, so two image tags cannot interleave. Existing short protected
mutation leases remain in place for `cluster up` and environment-state steps;
reusing one of those leases for the parent driver would self-deadlock.
Update and uninstall publish the root-owned maintenance marker while holding
the same launch mutex, then prove inactivity from the protected active pointer
and every matching service user-manager rollout unit. A loaded unit is safe only
in terminal `inactive` or `failed` state; every other state blocks, while
malformed output, stderr, or unsafe metadata returns unknown and fails closed.
That proof deliberately does not import the installed broker being replaced, so
a broken package can be repaired without creating an admission bypass. A valid
but stale `active.json` also blocks until the supported broker reconciliation
path clears it; operators must not delete the pointer by hand. An existing
hard-linked config is not edited through the shared inode: only after this
protected-pointer plus systemd-unit inactivity proof may the installer
atomically replace the canonical path with a new single-link inode. Thus config
detachment cannot occur before the same independent no-active-rollout gate used
for every other installed-file mutation.

### Failure, recovery, and break-glass

An active request rejects another `start` or `resume` with safe request
metadata; it is never queued or preempted. Disconnects do not stop the
service-owned user unit. Operators inspect it through `status` and `logs`,
resume only the recorded request, and cancel only with a bounded recorded
reason. They do not edit `state.json`, envelopes, locks, or evidence.

Root break-glass first disables new admission and preserves the private request
ledger. Repair or reinstall the broker from a clean, freshly fetched merged
`dev` checkout, then resume the existing request through the same service-owned
envelope and pinned SHA. Do not fall back to Qianyi's user unit, private key,
an arbitrary driver argv, or a newly selected candidate.

Uninstall is fail-closed: remove admission, acquire maintenance under the same
launch lock, prove no request is active, revoke the exact service public key on
every host in the root-owned revocation ledger, remove only installer-recorded
ACLs/memberships/linger/key material, and retain request and rollout evidence.
A legacy ledger can contain all 15 inventory hosts; a #822-era install records
the 14 active hosts. A failed upgrade retains that durable ledger so uninstall
cannot leave stale remote trust.

Before the first ACL mutation, the root-owned provisional install record stores
the complete ACL preimage and expected postimage for every required mask
change. Retry and uninstall accept only one of those two exact states. Uninstall
restores the preimage, deletes an installer-created default ACL when its
preimage was absent, and removes a service entry only when the separate grant
ledger proves the installer created it; any third-party drift stops recovery
without discarding the ledger. The first such transaction upgrades legacy v1
or trust-ledger v2 install records to v3 before touching an ACL. Older v1/v2
installers reject v3 rather than silently ignoring the reversible ACL ledger.
During a v1 trust-ledger migration, the provisional v3 record separately pins
the legacy source SHA until migration succeeds, so a crash cannot reinterpret
the new candidate as the old trust topology. An interrupted v2 migration that
already lost that binding is ambiguous and fails closed.

## Alternatives considered

### Share Qianyi's key and personal runner

Rejected. It creates an unrevocable shared human identity, conflicts with the
private-key mode gate, preserves the stale-checkout and manual-backup problems,
and cannot provide whole-rollout attribution or serialization.

### Give every operator a complete runner and key

Rejected. Three independently drifting checkouts, venvs, kubeconfigs, backup
procedures, and 15-host key distributions enlarge the failure surface without
solving contention over one staging environment.

### Continue `sudo -u qianyi systemd-run --user`

Rejected as a supported path. It attributes work to the wrong user, depends on
Qianyi's checkout and linger, and permits stale or unbounded candidate inputs.

### Reuse the protected mutation lease for the whole driver

Rejected. Child rollout steps acquire that lease themselves. The parent needs
a separate lifecycle lock.

The child CLI surfaces nevertheless share one security contract:
`src/loom_cli/rollout_lock_cli.py` owns their rollout-lock argument tracking,
broker-envelope loading, real-file checks, and fixed evidence-path validation.
`loom cluster up` and `loom admin environment-state` import those helpers so
their protected-step admission rules cannot drift independently.

## Consequences

All three operators receive the same narrow staging command surface and can
start, observe, resume, or cancel work without another person's login session
or private key. A new rollout may take longer because backup creation is now a
required broker-owned phase. Only one full staging rollout can be active.

The service and audit boundary remains operational rather than adversarial
while the operators retain root/Docker access. Production authority and GitHub
Environment approval are unchanged. The backup, protected preflight,
environment-state, release-gate, and smoke gates apply to all 14 active GB10
hosts under #822; the full 15-host inventory remains validated and cannot be
changed through a runtime host override.

## Validation and acceptance

Repository tests cover fixed merged-only candidate selection, immutable
envelopes, caller attribution, lifecycle locking, backup-before-launch,
redaction, cancellation/resume, installer idempotence and recovery, exact GB10
trust bootstrap/revocation, secret-boundary scanning, advisory CODEOWNERS
routing, and full CI selection. ACL coverage includes safe operator-only mask
expansion, zero new UID 2012/unknown/group permissions, plan/apply drift,
preimage-ledger retry, and exact uninstall restoration for existing and absent
default masks.

Repository verification is necessary but not live acceptance. Installation on
shared `platform-dev` is allowed only after the implementation has merged into
`dev`. Live acceptance must then prove both Hongjian and Devansh dry-run with
distinct authenticated identities and the same fresh merged SHA, unauthorized
and concurrent-start rejection, detached cross-operator observation, all 14
active GB10 hosts converged, node 7 stopped/unreachable, every existing release
gate and smoke passed, and zero
raw secret matches. #803 remains open for its broader identity inventory and
rotation scope after this operator-independence slice lands.
