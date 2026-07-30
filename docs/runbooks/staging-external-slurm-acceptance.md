# Staging external Slurm acceptance authority

Issue #827 uses an independent, root-installed authority to prove that the
staging GB10 external runner can execute the exact candidate on every allowed
node and then cleanly stop it. Repository state cannot attest itself:
`deploy/environment-state/staging.toml` declares only
`require_external_allocation_authority = true`. It must not contain an artifact
path, digest, service-identity receipt, or pass result.

## Fixed trust and identity

The authority is installed on `trt-eai-oldlab-1` and runs as root. Its probe
runs the fixed `staging-allocation-probe` transaction through the pinned node
transport to `trt-gb10-1`; it never calls a candidate checkout or accepts a
caller path. `loom-rollout` (995:982, `/var/lib/loom-staging-rollout`,
`/bin/sh`) is only the producer/orchestrator. Allocations run as the independent
non-login batch identity `loom-staging-worker` (31024:31024, `/nonexistent`,
`/usr/sbin/nologin`) with exactly the `docker` supplementary group. The Slurm
account and QoS are pinned in
`deploy/developer-sandboxes/staging-external-slurm-authority.toml`.

The persistent infrastructure and allocation acceptance set is exactly
`trt-gb10-1..15`, with `excluded_nodes=[]`. The 2026-07-29 owner correction
supersedes #822's static exclusion of `trt-gb10-7`. Neither an operator nor the
candidate can select a smaller set, choose another submit host, another
account, or another evidence root. A candidate-owned drain/quiescence gate
defers disruptive convergence while any host has external work and must never
cancel or preempt that work.

On first `bootstrap`, the fixed root authority persistently generates an
Ed25519 key pair when both key files are absent. It installs the private key as
root-owned `0600` at
`/etc/loom/staging-external-slurm-authority/authority-private.pem` and the public
key as root-owned `0644` at
`/etc/loom/staging-external-slurm-authority/authority-public.pem`, using
fsynced, atomic no-replace publication. If a crash leaves only a valid private
key, the next bootstrap derives and installs its public key. A lone public key,
unsafe metadata, invalid key, or mismatched pair blocks bootstrap. Existing
valid pairs are read back with a real sign/verify challenge and are never
rewritten. Never copy the private key into the repository, rollout evidence,
shared storage, logs, or a service-user home.

## Root installation

The administrator must verify the reviewed candidate and asset digests before
installing. The node-authority `bootstrap`/`upgrade` transaction installs and
reads back the fixed source module, fixed consumer module, exact-candidate venv
wrapper, config, service, sudoers, mount, and tmpfiles assets. Do not install
the authority from an ambient checkout. The resulting layout is:

```bash
/usr/local/lib/loom-staging-external-slurm-authority/
  staging_external_slurm_acceptance_authority.py
  loom_cli/external_slurm_acceptance.py
/usr/local/libexec/loom-staging-external-slurm-authority
/etc/loom/staging-external-slurm-authority/authority.toml
/etc/sudoers.d/loom-staging-external-slurm-authority
/etc/systemd/system/loom-staging-external-slurm-authority.service
/etc/systemd/system/srv-loom-staging\x2dshared.mount
/etc/tmpfiles.d/loom-staging-shared.conf
```

The wrapper requires
`/opt/loom-staging-runner/candidates/<candidate-sha>/venv/bin/python`, verifies
that its `sys.prefix` is the exact candidate venv, and invokes the fixed source
with isolated Python. The fixed source loads its fixed consumer module by
absolute file identity, so an ambient or candidate `loom_cli` package cannot
shadow the verifier.

## Infrastructure convergence

Run exactly one external entry point on oldlab1:

```bash
sudo -n /usr/local/libexec/loom-staging-external-slurm-authority \
  converge-infrastructure \
  --candidate-sha "$candidate_sha" \
  --candidate-tree "$candidate_tree"
```

It sends one closed transaction to oldlab2. Oldlab2 serially converges
`trt-gb10-2` shared-source ownership, `trt-gb10-1` accounting, and the exact
ordered fifteen-node infrastructure bootstrap set including node 7. Oldlab2 then
sends the aggregate to oldlab1, which deep-verifies and atomically installs
`/var/lib/loom-developer-sandbox-node-authority/staging-infrastructure/<candidate-sha>.json`
as root:root `0600`.

The convergence ID binds the exact candidate, tree, requested time, and
canonical request digest. Producer and consumer keep root-owned locks,
journals, immutable generation receipts, and monotonic high-water records.
Same-ID retry is accepted only with byte-identical input. Tamper, replay,
generation regression/skip, future timestamps, expired receipts, wrong
operation order, wrong inner receipt, and any node-set drift fail closed. An
interrupted call reuses its active ID; after successful local bootstrap and
readback the active journal is removed, so the next explicit call creates the
next generation.

`bootstrap --execute` performs collision checks before creating the fixed batch
identity, starts the exact NFSv4 mount
`192.168.20.12:/shared_work2/loom/staging` at
`/srv/loom/staging-shared`, and reads back source, filesystem type, target,
device, and root inode. The same system mount and identity must converge on
`oldlab1` and all fifteen GB10 infrastructure nodes; `trt-gb10-2` uses the same self-NFS
submount identity, not its local export path. Final roots are exactly
`candidates`, `generated`, and `results` below that mount and are owned by
31024:31024 on `oldlab1` and all fifteen GB10 infrastructure nodes. Node 7 is
admitted through the same 15-node allocation and health contract after its
candidate-owned quiescence gate passes. A directory under
`/shared_work2/qianyi` is never an authority surface.

## Prepare, probe, activate

Protected rollout step 11 performs these phases before the first
environment-state mutation:

1. `bootstrap` converges and reads back the persistent mount, independent batch
   identity, and controller-owned Slurm account/QoS/association transaction.
2. The unprivileged rollout writes only fixed private inputs under
   `/var/lib/loom-staging-rollout/prepared/{candidates,generated}`. `prepare`
   claims those exact candidate-derived paths, freezes descriptor-pinned source
   inodes, copies the repository as root-owned/batch-readable immutable content,
   and publishes the worker env as `31024:31024` mode `0600` with
   `renameat2(RENAME_NOREPLACE)`. Its root-owned journal supports recovery
   without overwriting or deleting foreign content. Both the producer and final
   repository reject hidden index flags, extra files, clean/encoding filters,
   and any tracked raw byte that differs from the commit tree. The worker env
   uses a closed Compose-compatible key schema and rejects duplicate, invalid,
   unknown, interpolated, or stale candidate/pool/concurrency/image bindings.
3. `probe` invokes the fixed allocation action for all fifteen nodes. Every
   node row must prove `sbatch`, `srun`, exact candidate/tree and paths, worker
   registration and at least two bounded heartbeats, ordered cancel,
   stop/terminal timestamps, Slurm `COMPLETED`, mount/inode binding, and zero
   orphan containers, networks, and volumes. It signs canonical JSON with the
   preinstalled Ed25519 key.
4. `activate` independently reloads the fixed profile and public key, verifies
   the detached signature, freshness, exact candidate, exact node matrix, and
   closed cleanup receipt. The signed artifact, monotonic/current pointer, and
   secret-safe pass summary all record the full ordered node set and
   `excluded_nodes=[]`.

`loom admin environment-state apply` repeats `activate` before resolving the
admin credential or performing any control-plane/systemd mutation.
`environment-state check`, release-gate, and the systemd verifier independently
repeat verification. A missing, stale, partial, tampered, or wrong-candidate
receipt therefore fails closed.

The inner allocation request is canonical JSON with exactly:

```text
schema_version, kind=staging_external_slurm_allocation_request,
request_id, candidate_sha, candidate_tree
```

The outer node-authority envelope is fixed to environment/scope `staging`,
GB10, and `trt-gb10-1`. Neither layer accepts repository/env/result paths,
identity, account, QoS, resource values, config paths, or a node subset.

Before external Slurm is enabled, the legacy GB10 node-agent desired state must
read back `target_slots=0` with all fifteen host intents `stopped`. The release
gate rejects simultaneous node-agent and external-Slurm authority. Steady-state
submit/cancel must use the same root broker; direct `sbatch` or `scancel` by
`loom-rollout` is forbidden.

## Rollback

If any phase fails, keep the policy and timer at the previously installed
state. Diagnose and durably fix the producer, fixed allocation action, or
candidate; never edit a receipt, re-sign candidate-produced content, disable
verification, or add pass fields to TOML.

For an intentional rollback, apply a separately reviewed environment profile
that disables the policy and supervisor. Preserve immutable signed receipts for
audit. Do not repoint `current.json` or replace an existing candidate
generation by hand.
