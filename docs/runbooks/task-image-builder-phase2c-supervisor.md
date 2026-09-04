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

## Assemble an offline provider release

Build the guard bundle and rootless runtime through their pinned release
procedures first. Then assemble one architecture at a time:

```bash
python3 scripts/ops/task_image_builder_provider_release.py \
  --source-root "$CANDIDATE_ROOT" \
  --output-root "$OUTPUT_ROOT" \
  --architecture x86_64 \
  --guard-release-directory "$GUARD_RELEASE_DIR" \
  --runtime-root "$RUNTIME_ROOT"
```

Repeat for `aarch64`. The command refuses mutable inputs, symlinks, unsafe
modes, mismatched digests, vulnerable runtime metadata, non-native ELF output,
and destination collisions. Rebuild in a clean directory and compare the
resulting release SHA-256 plus every emitted byte before staging.

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

## Conformance

Run conformance only against an explicit staged digest directory and explicit
empty scratch roots:

```bash
python3 scripts/ops/task_image_builder_provider_conformance.py \
  --staged-release "$STAGED_RELEASE_DIR" \
  --source-root "$REVIEWED_SOURCE_ROOT" \
  --root / \
  --scratch-root "$EMPTY_SCRATCH_ROOT" \
  --storage-root "$EMPTY_STORAGE_ROOT" \
  --scratch-cgroup-root "$EMPTY_SCRATCH_CGROUP_ROOT" \
  --live
```

Expected output keeps:

- `production_ready: false`
- `blockers: ["phase2_guard_provider_release_missing"]`
- no live activation path, unit, socket, feature, current link, credential, or
  BPF pin

Treat any production-ready result, stale Slurm feature, non-empty scratch root,
cleanup residue, missing project quota, missing subuid/subgid, runtime flag
drift, or process ancestry ambiguity as a failed Phase 2C conformance result.

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
