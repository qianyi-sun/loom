# Operator Native Material Authority Design

## Problem

The native-builder material client is intended to run as root on the protected
OLDLAB operator host, but it currently validates the complete GB10 runtime
authority inventory. That inventory includes the GB10 broker, runtime assets,
tmpfiles configuration, and the `qianyi` broker sudoers rule. Installing it on
OLDLAB would violate least privilege, while installing only the material client
would fail its complete-policy validation. The checked-in runbook therefore
describes an operator-host prerequisite that cannot yet be installed safely.

## Scope and invariants

This change adds one sealed operator-only installation target. It does not add
a second runtime broker, change the framed request protocol, authorize a new
remote command, or mutate Kubernetes, PostgreSQL, MinIO, Slurm, Tasks, Workers,
DNS, or executable capacity.

- The runtime target remains exact `gx10-01c7/aarch64`; the operator target is
  exact `TRT-EAI-OLDLAB-1/x86_64`.
- Both targets require effective and real root, a clean root environment, the
  same fixed root-owned sealed source, and exact source commit/tree ancestry.
- The operator policy contains only the launcher, material client, FD-only
  request encoder, protocol, and stdlib crypto helper.
- The operator target installs no broker, runtime profile, Docker/nft/systemd
  asset, tmpfiles asset, runtime state/lock, or sudoers file.
- The installer creates only the fixed mode-`0700` material directory. It does
  not open, copy, validate, hash, print, or otherwise consume private-key or CA
  material.
- Private-key and CA values and digests remain forbidden in argv, environment,
  receipts, logs, evidence, GitHub comments, and repository files.
- Existing protected operator-host sudo authorization remains a separate host
  responsibility. This change publishes no new local sudo authority.

## Installed topology

The runtime launcher retains its current full `ASSET_SPECS` and runtime policy.
It additionally defines an immutable `OPERATOR_MATERIAL_ASSET_SPECS` containing
exactly:

1. `/usr/local/libexec/loom-personal-dev-native-builder-runtime-authority`;
2. `/usr/local/libexec/loom-personal-dev-native-builder-runtime-authority-material-client`;
3. the installed FD-only authority client;
4. the installed framed protocol module;
5. the installed stdlib crypto helper.

The operator policy is canonical ASCII JSON at
`/etc/loom/personal-dev-native-builder-operator-material-authority.json`, mode
`0444`, owned by root with one link. Its schema is
`loom.personal-dev-native-builder-operator-material-authority-policy.v1` and
its exact fields are `asset_sha256`, `authority_source_sha`,
`authority_source_tree`, and `schema`.

The fixed protected inputs remain:

- `/etc/loom/personal-dev-native-builder-authority-material/agent-ed25519`,
  root:root, mode `0400`, one link, exactly 32 bytes;
- `/etc/loom/personal-dev-native-builder-authority-material/service-ca.pem`,
  root:root, mode `0444`, one link, 1 through 1 MiB.

The operator installer creates the parent material directory only. A direct
administrator provisions the two files separately after installation.

## Validation and data flow

`load_operator_material_policy()` validates the operator policy against the
exact immutable subset and verifies every installed asset before the material
client opens either protected input. The material client rejects any subset
drift, then pins only the installed package namespace and loads the FD-only
encoder. Secret data flows only through already-open descriptors numbered at
least 3, then through the existing SSH stdin frame to the GB10 broker.

The existing bootstrap script gains an explicit `--target` choice:

- `runtime` (default, preserving the existing CLI and `bootstrap()` API);
- `operator-material`, which calls `bootstrap_operator_material()`.

The operator bootstrap reuses the existing descriptor-bound source capture,
atomic no-replace writes, retained-identity checks, fsync ordering, idempotent
drift validation, created-object ownership ledger, and reverse rollback. It
captures only the five operator assets, builds the distinct canonical policy,
installs the complete subset, and ensures the empty material root. It validates
the installed assets against a retained-identity temporary policy, removes that
staging file, and publishes the fixed policy last as the activation commit
point. It then returns a bounded public receipt containing only source and
installed-code identities.

## Failure and recovery

Wrong host/architecture, non-direct root, unsafe environment, invalid sealed
source, unknown or missing subset assets, noncanonical policy, installation
drift, symlinks/hardlinks, replacement races, unsafe parent directories, or
failed installed-policy validation fail closed. A failed first installation
removes only objects whose retained identities prove they were created by that
attempt. Pre-existing exact files and any separately provisioned material are
never removed by rollback.

The runtime target and its sudoers-last transaction remain unchanged. The
operator target has no privilege file to stage or publish.

## Verification

Tests must prove:

- the operator policy accepts exactly five fixed assets and rejects runtime or
  sudoers inventory, unknown fields, missing assets, digest drift, and
  noncanonical bytes;
- the material client loads only the operator policy before material I/O;
- the operator target is host/architecture and clean-root gated;
- only the five code assets, policy, and empty mode-`0700` material directory
  are installed, with no GB10-only asset or sudoers path;
- first-install rollback, idempotence, pre-existing drift rejection, retained
  object identity, and source mutation detection match the runtime installer;
- the bootstrap never opens or digests either material file;
- runbooks contain the exact target command and preserve the FD-only boundary;
- all changed paths select the protected native-authority CI lanes.

Live verification after protected merge uses the exact merged source/tree,
installs the operator subset on OLDLAB, separately provisions the fixed inputs
without printing or hashing them, and proves `emit-public-key` succeeds with
stdout discarded before any GB10 activation request.
