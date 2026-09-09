# Task-image builder Phase 2C supervisor runbook

Phase 2C stages a content-addressed provider release for the allocation
supervisor and native rootless BuildKit executor. It does not activate the
provider. A staged release is evidence only; production certification stays
false until later Phase 2D publication authority and protected shadow acceptance
complete.

## Safety boundary

- Keep `production_certification_allowed = false`, `certified_nodes = []`, and
  `phase2_guard_provider_release_missing` present in
  `deploy/task-image-builder/prerequisites-v1.toml`.
- Keep every `deploy/task-image-builder/rootless-provider-v1.toml` policy
  `enabled = false` with non-empty activation blockers.
- Do not create `/opt/loom-task-image-builder-provider/current`, live
  credentials, live supervisor configs, systemd units, sockets, BPF pins, Slurm
  feature advertisements, or production-ready receipts.
- Use only explicit digest-named release directories under
  `/opt/loom-task-image-builder-provider/releases/<sha256>/`.

The supervisor derives its release selector from the kernel's running executable
path and verifies that inode against the exact root-owned 0555 installed member.
It does not embed the composite digest in its ELF or accept a release selector
from arguments, environment variables or a `current` link. Later activation must
install a root-owned 0444 `/etc/loom-task-image-builder/supervisor-config.json`
naming the same release. Staging must not create that file; offline conformance
rejects it as a live surface. Installer hash checks and the guard's independent
ELF/grant checks remain mandatory.

## Assemble an offline provider release

Build the guard bundles and rootless runtimes through their pinned release
procedures first. When guard sources change, refresh both architecture bundle
digests in `deploy/task-image-builder/provider-release-v1.json` together with its
guard-spec digest. Updating only the spec digest leaves an incompatible input
pair that the assembler correctly rejects. Derive bundle digests from repeated
assembly and verify the retained tool bytes against their previously pinned
release; cache presence alone is not provenance. Offline assembly does not
establish native containment or authorize activation.

Then certify both architectures in one all-or-nothing
assembler invocation:

```bash
python3 scripts/ops/task_image_builder_provider_release.py \
  --source-root "$CANDIDATE_ROOT" \
  --output-root "$OUTPUT_ROOT" \
  --guard-release-directory-x86-64 "$GUARD_RELEASE_DIR_X86_64" \
  --guard-release-directory-aarch64 "$GUARD_RELEASE_DIR_AARCH64" \
  --runtime-root-x86-64 "$RUNTIME_ROOT_X86_64" \
  --runtime-root-aarch64 "$RUNTIME_ROOT_AARCH64"
```

The command refuses mutable inputs, symlinks, unsafe modes, mismatched digests,
vulnerable runtime metadata, non-native ELF output, destination collisions, and
any nondeterministic supervisor output across the two clean pinned builds per
architecture. It publishes neither architecture unless both architectures pass.

## Stage on a node without activation

Use the node prerequisite installer in `check` mode before `apply`:

```bash
sudo deploy/slurm/install-loom-task-image-builder-node-prerequisites.sh \
  check oldlab trt-eai-oldlab-3 "$PROVIDER_RELEASE_DIR"
```

If the check is clean, stage the exact same digest directory:

```bash
sudo deploy/slurm/install-loom-task-image-builder-node-prerequisites.sh \
  apply oldlab trt-eai-oldlab-3 "$PROVIDER_RELEASE_DIR"
```

The installer validates the policy and host release, copies into a hidden
sibling, fsyncs every file and directory, atomically renames the digest
directory, and writes a mode-0600 staging receipt. It must not call `systemctl`,
render a live unit, publish a `current` link, change provider policy, or
advertise `loom_rootless_buildkit`.

## Offline conformance

Run conformance only against an explicit staged digest directory:

```bash
python3 scripts/ops/task_image_builder_provider_conformance.py \
  --staged-release "$STAGED_RELEASE_DIR" \
  --source-root "$REVIEWED_SOURCE_ROOT" \
  --root /
```

Expected output keeps:

- `production_ready: false`
- `blockers: ["phase2_guard_provider_release_missing"]`
- no live activation path, unit, socket, feature, current link, credential, or
  BPF pin

`--live` is intentionally fail-closed in Phase 2C. The approved Phase 2C
boundary forbids live guard configuration, credentials, sockets, and BPF pins,
so this script cannot honestly prove exact launched PIDs, scratch cgroup
inode/descendants, staged binary argv, no-cache OCI output, attributable BPF
denial counters, cleanup readback, or guard-restart/lost-attestation behavior.
Those live checks require a later controller ruling or a later increment that
explicitly authorizes the guard surfaces needed to collect them.

Treat any production-ready result, stale Slurm feature, live probe pass claim,
or live provider surface as a failed Phase 2C conformance result.

## Rollback and cleanup

Because Phase 2C is inert, rollback is removal of staged evidence only after the
operator has captured the receipt and conformance output. Never delete Phase 1
reservations, units, timers, images, or task materialization state. If a staged
digest directory differs from the release manifest, preserve it as evidence and
stage a corrected digest under a new content-addressed path.

## Phase 2D handoff

The next protected increment supplies renewable repository-scoped registry
credentials, digest-by-digest OCI graph verification, signed publication
statements, keyset rotation and revocation, reference-aware partial retention,
immutable execution grants, and one-use trial-start authorization. Do not call a
Phase 2C node production-ready before that handoff lands and protected shadow
acceptance passes.
