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

Live conformance additionally performs non-attaching pidfd, sealed-memfd,
cgroup-v2, bpffs, and staged-`bpftool` feature probes:

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

Only the later shadow/certification sequence may render the unit's
`@LOOM_GUARD_RELEASE_SHA256@` token, provision node credentials and policy,
install the rendered unit, create the activation marker, and advertise the
feature.
