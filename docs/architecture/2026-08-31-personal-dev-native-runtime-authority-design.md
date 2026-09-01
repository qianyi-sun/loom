# Personal-development native runtime authority

## Status

Approved for implementation under the standing operational direction for
issue #1280. This design closes the GB10 root-authority gap without granting a
shell, extending the Slurm broker, or weakening the native builder boundary.

## Problem

The native arm64 runtime runbook currently authenticates to `gx10-01c7` as an
unprivileged operator and then invokes many independent `sudo` commands. The
checked-in pinned SSH identity works, but the remote account has no
noninteractive authority for those commands. Root SSH is disabled, and the
existing external-supervisor broker deliberately accepts only typed
Slurm/capacity requests.

Granting passwordless shell, `systemctl`, `docker`, `nft`, or Python commands
would expose much more root authority than this runtime needs. Using Docker
group membership as a root path is also prohibited. The runtime therefore
needs a separate fixed broker whose entire root surface is the lifecycle of
the dedicated personal-development native builder.

## Goals

- Permit the reviewed operator to prepare, stage, activate, observe, and
  remove only the dedicated GB10 personal-development native builder runtime.
- Reuse the checked-in `qianyi` SSH topology, root-owned deployment identity,
  and root-owned known-hosts database without reading a mutable user SSH
  configuration as root.
- Keep private key and CA values out of process arguments, output, evidence,
  issue comments, and persistent staging.
- Execute only root-owned code from an exact merged, protected-CI-approved
  authority source and tree.
- Preserve the current runtime contracts: native arm64, KVM gVisor, separate
  RootlessKit BuildKit/client sandboxes, no QEMU, no runc fallback, exact
  current/previous image retention, and an inactive-by-default installation.
- Preserve global capacity and workload boundaries: no Loom Task or Worker,
  no Slurm command or job, no executable-capacity increase, no Kubernetes or
  database credential, and no personal/build namespace authority.
- Produce canonical, bounded, secret-free receipts suitable for owner-only
  rollout evidence.

## Non-goals

- This broker does not allocate GB10 nodes or integrate with the global
  capacity manager.
- It does not replace the native builder agent or its signed grant protocol.
- It does not render or apply Kubernetes management manifests, create the
  public-key Secret, manage PostgreSQL/MinIO data, or own personal environment
  cleanup.
- It does not provide a generic package installer, command allowlist, root
  shell, arbitrary Docker API, or arbitrary systemd/nftables interface.
- It does not make an unmerged checkout operational authority.

## Chosen approach

Install a dedicated, no-argument root broker and one exact sudoers rule:

```text
qianyi ALL=(root) NOPASSWD:NOSETENV: /usr/local/libexec/loom-personal-dev-native-builder-runtime-authority ""
```

The operator sends a bounded framed request on standard input through the
pinned SSH transport. The broker validates the sudo identity, canonical
request, operation-specific fields, root-owned policy and assets, host
identity, and current state before performing any mutation. It replaces its
environment and invokes only fixed absolute paths.

This is separate from the external-supervisor broker. The two brokers share no
request types, state directories, service identities, locks, or sudoers rules.

Rejected alternatives:

1. Broad `NOPASSWD` for shell, Docker, systemd, nftables, or Python: excessive
   root authority and unsafe argument expansion.
2. Extend the Slurm external-supervisor broker: couples unrelated trust
   domains, expands an independently reviewed capacity envelope, and conflicts
   with the separate GB10 autoscaler work.
3. Root SSH or Docker-group escalation: bypasses the intended fixed authority
   and is explicitly prohibited.
4. Root OpenSSH reading the repository `ssh_config`: a mutable configuration
   can contain executable SSH directives. The client instead uses `-F
   /dev/null` and validated explicit options matching the checked-in topology.

## Components

### Root broker

`scripts/ops/personal_dev_native_builder_runtime_authority.py` is installed as
the fixed libexec entrypoint. It:

- accepts no arguments;
- requires effective/root UID and GID plus the exact `SUDO_USER`, `SUDO_UID`,
  `SUDO_GID`, and `SUDO_COMMAND` values for `qianyi`;
- holds one nonblocking exclusive lock for every request;
- validates its root-owned policy, executable, supporting assets, hostname
  `gx10-01c7`, architecture `aarch64`, cgroup v2, KVM, and dedicated resource
  boundaries;
- parses one framed request with strict maximum lengths, duplicate-field
  rejection, canonical JSON, exact operation fields, and no trailing bytes;
- invokes only the installed runtime installer, release converger, fixed
  conformance program, `/usr/bin/systemctl`, `/usr/sbin/nft`, and the dedicated
  Docker socket as required by the selected operation;
- emits one canonical JSON receipt on success and a stable secret-free error
  on failure.

The broker imports no module and executes no script from operator-writable
staging. All executable assets come from the root-owned authority source.

### Administrative bootstrap

`scripts/ops/install_personal_dev_native_builder_runtime_authority.py` is a
one-time direct-root bootstrap. It is not a sudoer command. It requires:

- direct external root execution with no `SUDO_*` environment;
- a root-owned mode-`0700` sealed source directory at a fixed path;
- the exact merged source SHA, tree SHA, approved base ancestry, and a clean
  no-symlink/no-hardlink source inventory;
- the expected host and architecture.

The default `runtime` target atomically installs the GB10 broker, fixed
supporting assets, runtime policy, state directories, lock/tmpfiles
configuration, and sudoers rule. Sudoers is validated and published last. The
separate `operator-material` target described below has a distinct inventory
and installs none of those GB10 capacity or privilege surfaces. Any failure
rolls back every asset created by that bootstrap attempt. Existing assets must
be byte-identical and have exact root ownership/modes; drift fails closed.

The policy binds the authority source/tree and SHA-256 of every root-executed
asset. Updating root code therefore requires a new merged candidate and a new
direct-root bootstrap; an operator request cannot update the broker.

### Operator encoder and pinned transport

`scripts/ops/personal_dev_native_builder_runtime_authority_client.py` validates
an owner-only request file and writes the binary frame to stdout. It accepts
private material only through distinct already-open descriptors numbered at
least `3`, including descriptor-based public-key emission; it has no key or CA
pathname option.

The separately installed
`personal_dev_native_builder_runtime_authority_material_client.py` is a sealed,
policy-bound root asset. OLDLAB installs it with the explicit
`--target operator-material` bootstrap on exact host
`TRT-EAI-OLDLAB-1/x86_64`. Its distinct policy at
`/etc/loom/personal-dev-native-builder-operator-material-authority.json` binds
exactly the launcher, material client, FD-only authority client, framed
protocol module, and stdlib crypto helper. This operator target installs no
GB10 broker, runtime profile or runtime assets, tmpfiles/state/lock surface, or
sudoers rule.

The bootstrap creates only an empty mode-`0700` material directory; a direct
administrator separately provisions the two fixed inputs afterward. The
bootstrap never opens, copies, validates, hashes, prints, or otherwise consumes
either input. The material client opens only those fixed
administrator-provisioned paths, validates their metadata without following
links, and invokes the FD-only client from the validated installed library.
The installer first validates those installed assets against a temporary policy
and publishes the fixed operator policy last as the activation commit point.
The operator cannot select either pathname, and no checkout or runbook shell is
executed as root. Its root-owned entrypoint necessarily starts before it can
validate itself, but its pre-validation module body is stdlib-only and performs
no material I/O or application-module load. It validates the complete distinct
five-asset operator policy before opening either material file or loading the
encoder.

The material client's local `sudo` invocation uses the separately provisioned
protected-operator-host authorization already required by the fixed root-local
SSH and SFTP transport. It is not another GB10 authority sudo target: the
authority sudoers asset continues to authorize only the empty-argument remote
broker launcher.
Neither component writes secret data to diagnostics or includes secret paths
or values in the request header.

The runbook invokes root-local `/usr/bin/ssh` with:

- `-F /dev/null`;
- exact `HostName`, `Port`, and `User` values validated against
  `deploy/worker-pools/gb10/ssh_config`;
- the fixed root-owned deployment identity and known-hosts paths;
- `IdentitiesOnly`, public-key-only, batch, strict-host-key, and
  no-update-host-key settings;
- the single remote command `sudo -n -- <fixed-broker>`.

The same transport is not used for arbitrary remote commands or SCP after the
authority is installed.

### Fixed conformance asset

The current two-container shell probe becomes a root-owned fixed asset. It
accepts only validated immutable builder/agent image references and the
validated public HTTPS origin. It retains the existing exact names, labels,
resource limits, gVisor runtime, network ranges, denial probes, separate
sandbox identities, and cleanup trap.

It may touch the primary Docker daemon only to create and remove its one exact
foreign denial-probe container. Pre-existing exact names or unexpected labels
are blockers; the probe never removes an object it did not create in the
current invocation.

## Request framing

Each request is:

1. fixed ASCII magic and protocol version;
2. unsigned 32-bit big-endian canonical-JSON header length;
3. canonical UTF-8 JSON header with no trailing newline;
4. the exact operation payload bytes declared by the header;
5. EOF.

The total request is bounded. JSON rejects duplicate keys, noncanonical bytes,
unknown fields, booleans where integers are required, non-ASCII identifiers,
unpinned image repositories, mutable tags, malformed digests, broad URLs,
unexpected paths, and nonzero payloads for non-secret operations.

Only `stage-agent` has a payload. Its header declares exactly a 32-byte Ed25519
private key and a bounded CA byte count. Neither value nor its digest appears
in the header or receipt. The broker reads the frame once, writes root-only
temporary files with `O_NOFOLLOW|O_EXCL`, validates the derived public-key
fingerprint and CA contract through the fixed installer, and unlinks both
temporary files in a `finally` path.

## Operations and state machine

### `status`

Read-only. Reports canonical service states, installed/staged/active runtime
identity, fixed asset/policy identity, managed container/network counts, and
nftables presence. It never returns key, CA, environment, command line, image
credential, or arbitrary journal content.

### `prepare`

Allowed only when both dedicated services are inactive and no managed
container/network exists. It:

1. validates and takes ownership of the one exact operator-staged gVisor
   archive using an opened descriptor, pinned SHA-512, safe metadata, and a
   root-private copy;
2. runs fixed preflight, install, and staged verification;
3. loads the exact dedicated nftables table and starts only the dedicated
   daemon;
4. plans twice and compares exact release convergence, then applies and
   verifies the exact current/previous immutable arm64 images;
5. runs the fixed two-container KVM-gVisor conformance and denial probes;
6. stops the dedicated daemon, removes only the exact nftables table, and
   verifies the runtime is inert.

Success leaves installed files and the bounded image cache inert. Failure
compensates daemon/nftables/probe objects and also leaves both services
inactive. A partial byte drift remains a blocker for an explicit `remove` or
new administrative bootstrap; it is never overwritten broadly.

### `stage-agent`

Allowed only after successful prepared identity and while both services are
inactive. It consumes the framed key/CA payload, stages the exact immutable
agent/builder images, HTTPS management origin, instance ID, key ID, expected
public fingerprint, and fixed concurrency/profile values through the installed
runtime installer. Temporary secret files are always unlinked. Success returns
only public identity and staged runtime digests.

### `activate`

Allowed only for the exact prepared and staged identity. It loads the fixed
nftables table, starts the dedicated daemon and agent, runs fixed active
verification, and returns their exact public identity. Any failure stops the
agent and daemon and removes the exact table.

Activation grants no work by itself. Management remains responsible for the
signed zero-grant readiness gate before owner admission.

### `remove`

Allowed only with zero managed build/conformance containers and networks. It
stops the exact agent and daemon, removes the exact nftables table if present,
and invokes the fixed byte-identical installer removal. It preserves the
dedicated image cache and system identities as inert state and never touches
the primary Docker daemon's unrelated objects.

The operator runbook must first prove zero native grants and zero dynamic
personal/build namespaces. The broker independently enforces every host-local
condition it can observe and refuses busy removal.

## Failure, concurrency, and recovery

- One root-owned lock serializes all operations and fails busy rather than
  waiting behind an unknown actor.
- Signal handlers compensate only objects created or activated by the current
  request and reap every child process.
- Subprocesses use fixed absolute paths, a minimal fixed environment, bounded
  stdin/stdout/stderr, process groups, and operation-specific deadlines.
- Secret staging is cleanup-first and never journaled.
- Prepare and activation failures return to inactive service/nftables state.
- Remove is exact and fail-closed; it does not broaden selectors after a
  mismatch.
- Receipts include request/authority/runtime public identities and transition
  state, but no input path, secret, raw command output, or environment.

## Runbook changes

The native runtime and acceptance runbooks will:

- validate the pinned SSH configuration and local protected materials;
- perform DNS and Kubernetes/database/Slurm read-only gates as today;
- download the pinned gVisor archive unprivileged;
- call only the fixed broker for GB10 root operations;
- use `status`, `prepare`, `stage-agent`, `activate`, and `remove` receipts as
  the host evidence boundary;
- retain local-only creation of the Kubernetes public-key Secret;
- retain owner-API teardown, exact inert shadow, and final capacity/Task/
  Worker/Slurm verification.

Direct remote `sudo` for shell, Python, systemctl, Docker, or nftables is
forbidden after this change.

## Testing

Testing proceeds red/green and includes:

- frame parser/encoder round trips and rejection of truncation, trailing data,
  oversized input, duplicate/noncanonical JSON, wrong payload lengths, unknown
  operations/fields, mutable images, unsafe URLs, and invalid identifiers;
- invocation identity, clean environment, no-argument sudoers, root-owned
  policy/asset, host identity, lock, and bootstrap rollback tests;
- fake-host prepare/stage/activate/status/remove transitions, idempotency, busy
  removal, archive races, asset drift, and compensation at every injected
  failure boundary;
- secret tests proving values and digests never reach argv, receipts, errors,
  evidence, or retained staging;
- conformance tests preserving two separate KVM-gVisor sandboxes and all four
  network denial directions with exact cleanup ownership;
- runbook policy tests rejecting any direct remote privileged command outside
  the fixed broker;
- the complete existing installer, converger, runtime-profile, runbook,
  agent/protocol/executor, migration, and store suites.

Live acceptance is separate from unit/integration success. It requires a
merged protected authority candidate, direct-root bootstrap evidence, a fresh
runtime/acceptance window, two owners, owner-API cleanup, exact shadow recovery,
zero native grants and dynamic namespaces, unchanged Task/Worker/Slurm counts,
and executable new-capacity ceiling `0`.

## Rollout and administrative handoff

1. Merge the authority implementation through current-head protected CI.
2. Prepare a root-owned sealed source on `gx10-01c7` for that exact commit/tree.
3. A direct root administrator runs the bootstrap and records only public
   source/tree/asset digests and installed path metadata.
4. Verify the fixed no-argument sudo invocation and `status` receipt through
   the pinned transport.
5. Open a fresh expiring runtime/acceptance window; the current window is not
   reused after expiry or source change.
6. Run prepare, conformance, stage, activation, two-owner acceptance, owner API
   cleanup, exact inert rollback, schema-3 evidence verification, and durable
   operational apply.
7. Finish with zero grants/namespaces/residue, unchanged Task/Worker/Slurm
   counts, and capacity ceiling `0`.

Until step 3, the correct live state is the current inert shadow with no GB10
dedicated runtime service, public-key Secret, or executable capacity.
