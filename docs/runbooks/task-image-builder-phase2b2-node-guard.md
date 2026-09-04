# Task-image builder Phase 2B2 node guard

## Scope and safety state

Phase 2B2 produces and may stage a content-addressed node-guard release. It does
not activate the guard or the rootless builder provider. Phase 1 remains the
active rollback path.

The expected post-stage state is deliberately inert:

- no `/etc/loom/task-image-builder-guard/config-v1.json`;
- no TLS key, client certificate, CA, or node bearer;
- no `/etc/loom/task-image-builder-guard/activation-v1.json`;
- no `/opt/loom-task-image-builder-guard/current` link;
- no installed, enabled, or running node-guard unit;
- no guard socket or guard-owned bpffs pins;
- no `loom_rootless_buildkit` feature advertisement;
- both rootless provider policies disabled;
- `production_certification_allowed=false`, no certified nodes, and
  `phase2_guard_provider_release_missing` retained.

Do not create any missing item to make conformance pass. Its absence is the
Phase 2B2 acceptance condition.

## Build one native release

Build on the same architecture as the target. Use the versioned ELF `bpftool`
binary, not Ubuntu's `/usr/sbin/bpftool` shell dispatcher.
Run every script from the reviewed checkout path shown below. The installer and
conformance entry points resolve and validate that checkout-relative path, then
pin imports to its repository root and `src` directory; they do not require or
honor an ambient `PYTHONPATH`.

```bash
repo=/absolute/path/to/reviewed/loom
output=/absolute/path/to/new/guard-release-output
bpftool=/usr/lib/linux-tools/"$(uname -r)"/bpftool
architecture="$(uname -m)"

test "$architecture" = x86_64 -o "$architecture" = aarch64
test -x "$bpftool"
file "$bpftool"

/usr/bin/python3 "$repo/scripts/ops/task_image_builder_guard_release.py" \
  --source-root "$repo" \
  --bpftool "$bpftool" \
  --output "$output" \
  --architecture "$architecture"
```

The command prints the release SHA-256. It atomically creates
`$output/<release-sha256>/` and a byte-identical sidecar manifest. It refuses to
replace either name. Transfer the complete digest-named directory through the
reviewed host-release channel; do not reconstruct or copy individual members.

## Stage without activation

Set `release_sha256` from the reviewed transfer manifest, not by trusting the
candidate directory name.

```bash
repo=/absolute/path/to/reviewed/loom
bundle=/absolute/path/to/transferred/digest-named-directory
release_sha256='<reviewed-64-lowercase-hex-release-sha256>'
architecture="$(uname -m)"

sudo /usr/bin/python3 "$repo/scripts/ops/install_task_image_builder_guard.py" \
  --live \
  --root / \
  --bundle "$bundle" \
  --release-sha256 "$release_sha256" \
  --architecture "$architecture"
```

The only durable writes are the immutable release under
`/opt/loom-task-image-builder-guard/releases/<release-sha256>/` and its `0600`
stage receipt under `/var/lib/loom-task-image-builder-guard/staged/`. Repeating
the exact command is idempotent. A same-name collision leaves the existing
release untouched and retains the incoming candidate as a hidden `.conflict.*`
directory for incident inspection.

The staging command never invokes `systemctl`, Slurm, or the authority service.
Do not copy the unit template to `/etc/systemd/system`, run `daemon-reload`,
create `current`, or create configuration/credentials/activation files in this
phase.

## Conformance and read-only inspection

Offline conformance against a controlled filesystem root:

```bash
/usr/bin/python3 "$repo/scripts/ops/task_image_builder_guard_conformance.py" \
  --root /absolute/path/to/controlled-root \
  --source-root "$repo" \
  --staged-release /absolute/path/to/controlled-root/opt/loom-task-image-builder-guard/releases/"$release_sha256"
```

Live conformance additionally checks all required cgroup-v2 controllers and the
exact cgroup2/bpffs mounts; probes pidfd, sealed-memfd, `BPF_LINK_CREATE`, and
staged-`bpftool` features; and creates isolated empty scratch cgroups to load,
attach, pin, read back, and remove all three default-deny BPF policy scopes. It
never attaches to a Slurm or foreign cgroup. Any partial probe or cleanup
ambiguity fails conformance and reports both the probe and cleanup failures:

```bash
sudo /usr/bin/python3 "$repo/scripts/ops/task_image_builder_guard_conformance.py" \
  --live \
  --root / \
  --source-root "$repo" \
  --staged-release /opt/loom-task-image-builder-guard/releases/"$release_sha256"
```

Every successful report has schema
`loom.task-image-builder-guard-conformance/v1`,
`production_ready=false`, and blocker
`phase2_guard_provider_release_missing`. A successful Phase 2B2 report is not
activation authority.

Useful read-only checks are:

```bash
find /opt/loom-task-image-builder-guard/releases/"$release_sha256" \
  -maxdepth 1 -type f -printf '%m %u:%g %f\n' | sort
sha256sum /opt/loom-task-image-builder-guard/releases/"$release_sha256"/*
systemctl is-enabled loom-task-image-builder-node-guard.service || true
systemctl is-active loom-task-image-builder-node-guard.service || true
test ! -e /etc/loom/task-image-builder-guard/activation-v1.json
test ! -e /opt/loom-task-image-builder-guard/current
test ! -S /run/loom-task-image-builder-guard/guard.sock
```

## Runtime quarantine semantics

After later activation, any ambiguous peer, Slurm, cgroup, BPF identity, or
cleanup state remains default-denied and pinned. The guard removes only the
exact node feature `loom_rootless_buildkit`. It does not drain a node, change
node state, cancel any job, remove ambiguous pins, or modify a cgroup outside
the exact Loom allocation subtree. Operators must preserve the ledger and pins
until the allocation identity and terminal/empty state are proven.

The live authority configuration binds two distinct network identities. The
canonical `base_url` hostname remains the HTTP Host and TLS SNI/certificate
identity, while `connect_ip` is a reviewed canonical numeric IPv4 or IPv6
address permitted by the same release's BPF policy. The guard opens the socket
directly to `connect_ip`; it performs no runtime DNS lookup. Each configured
authority timeout is one absolute monotonic deadline spanning TCP connect, TLS
handshake, request transmission, response headers, and the bounded response
body. A timer shuts down a stalled active socket and returns only
`authority_deadline_exceeded`.

For a new local packet, the kernel atomically marks received rights
close-on-exec. The guard extracts only the kernel SCM credentials, compares them
with `SO_PEERCRED`, and opens/checks the peer pidfd before request parsing,
descriptor validation, socket-path inspection, or the bounded `AF_NETLINK`
`SOCK_DIAG` query that resolves the connected client's exact socket inode. Raw
received rights remain guard-owned and are closed if pidfd capture or protocol
validation fails. The diagnostic socket is guard-owned, local-only, and closed
before containment begins.

The guard then stops the pidfd-bound, single-threaded supervisor, proves that no
other process shares its descriptor table, and takes stable inventories of
every visible process's descriptor table. Exactly one descriptor in the whole
visible process table may hold the connected client socket inode, and that
holder must be the stopped supervisor. Its sole socket must be that Unix
`SOCK_SEQPACKET` connection to the exact guard socket; any INET, INET6, netlink,
other Unix, unknown, duplicated, or transferred socket fails closed.

While the supervisor remains stopped, the guard proves the batch had no prior
descendants, attaches default-deny policy, moves only that supervisor to
`trusted-service`, enables `io` and `pids` only on the empty batch and
`loom-builder` parents, and applies the positive limits. `build-egress` remains
a leaf with no enabled domain controller so Phase 2C can use
`clone3(CLONE_INTO_CGROUP)` without `EBUSY`. The guard changes ownership only
on `loom-builder/cgroup.procs` (the nearest common ancestor) and
`build-egress/cgroup.procs` (the destination), to the exact configured
supervisor UID/GID with mode `0644`. Batch and `trusted-service` migration files
remain root-owned. These permissions allow a one-way child launch into the
contained build scope without delegating controller settings, resource limits,
directory creation, the Slurm ancestor, or a path out of the Loom subtree. The
guard binds the leaf controller state and exact file ownership/modes into the
attachment proof and verifies them again after restart. It resumes the
supervisor on success and normal failure paths. A guard crash during this
bounded stop window can leave the allocation supervisor stopped; preserve the
ledger and cancel the exact Slurm allocation rather than sending a broad resume
signal.

On guard restart, exact live ledger entries are reopened only after their
Slurm, cgroup, process, resource, and pin identities are revalidated. Every
recovered allocation receives an immediate new authority attestation before
the service reports readiness; a failed or withdrawn attestation closes its
live handles, leaves deny policy pinned, and withdraws only the builder feature.
An `intent`, `challenge_pending`, or `challenged` entry is retained only after a
stable double-read proves a clean pre-containment batch with no descendants or
published pins. `containment_pending` is never probed as recoverable: because a
crash may have happened between any kernel mutation and its journal write, it
is quarantined unconditionally with deny pins preserved.

Systemd startup extensions and watchdog notifications are emitted only while a
single shared main-loop progress clock advances. Authority I/O, bounded pinned
commands, stopped-peer process/descriptor inventories, and per-ledger
reconciliation all mark that clock. Notifications stop after 75 seconds without
progress, and pre-ready startup extension stops after an absolute 900-second cap
even if individual operations continue making progress. A stalled service
therefore cannot use its notifier thread to appear healthy indefinitely.

## Phase 2C handoff

Promotion remains blocked until a separate protected increment supplies and
tests all of the following:

- the allocation supervisor and its digest-bound release;
- sealed bootstrap/session consumption in locked non-dumpable memory;
- RootlessKit plus rootless OCI BuildKit startup inside `build-egress`;
- quota-backed, inode-bounded, empty-at-start job storage;
- session-bound claim, heartbeat, renewal, termination, and typed cleanup;
- native no-cache builds on OLDLAB/x86_64 and GB10/arm64; and
- a composite provider release that binds the supervisor, guard, policies,
  runtime binaries, and authority API.

The `device_program_tags` values in the checked-in OLDLAB and GB10 example
configs are inert placeholders, not deployable defaults. During Phase 2C, an
operator with Slurm administrative authority must inspect the actual inherited
`cgroup_device` program on every candidate node using the release-pinned
`bpftool`, certify its kernel-reported instruction tag against the reviewed
Slurm device policy, and render the exact certified tag set into that node's
root-owned live config. Phase 2B2 accepts only self-contained programs whose
`bpftool prog show` identity has no `map_ids`: Linux excludes map references
from the instruction tag, so a map-backed program cannot be certified by tag
alone. If the deployed Slurm device policy is map-backed, Phase 2C must bind and
re-read the exact map identities, schemas, and immutable entries before that
node can be certified. A missing, additional, reordered, changed, or map-backed
tag closes containment and withdraws only the builder feature; it must never be
worked around by copying the example value.

Only the later shadow/certification sequence may render the unit's
`@LOOM_GUARD_RELEASE_SHA256@` token, provision node credentials and policy,
install the rendered unit, create the activation marker, and advertise the
feature.
