# CI Runner Routing and Isolation

Loom CI uses hosted GitHub runners by default and can route selected x86_64
work classes to the isolated OLDLAB runner pool. Routing is an optimization;
the workflow's validation contract and required checks do not change with the
selected runner.

## Routing contract

The routing action under `.github/actions/ci-runner-route/` consumes one
versioned, signed route document and produces a frozen `runs-on` array for each
requested job key. A missing, invalid, expired, incomplete, or inconsistent
document selects the hosted fallback. Jobs also verify their actual runner
identity and architecture before executing.

The self-hosted label set is fixed and work-class-specific. Workflows do not
accept a caller-provided runner label, repository ref, image, or host command.
Jobs that require macOS, arm64, deployment authority, publishing credentials,
or another protected environment remain on their explicit hosted or protected
runner class.

The route publisher runs on hosted infrastructure and publishes the complete
generation atomically. Superseded generations cannot partially update the
matrix.

## Isolated pool

`scripts/ops/ci_runner_pool.py` manages the pool described by
`deploy/ci-runners/oldlab5.toml`. Each job gets a short-lived GitHub JIT runner
inside a fresh QEMU/KVM guest. The host does not run repository job code
directly.

The guest image and boot assets are pinned by digest. The manager validates the
candidate image, reserves a work-class slot, creates a one-job JIT
registration, starts the VM, observes completion, removes the runner
registration, and destroys the guest. Stale idle guests and registrations are
reconciled; busy runners are drained before removal.

Runner credentials come from a current-owner `0600` file and are excluded from
arguments, logs, route documents, and repository artifacts. Pool state and
cache roots are service-owned. The guest has Docker and sudo because CI jobs
need them, so VM destruction is the security boundary between jobs.

## Capacity and fallback

Work classes have explicit slot ceilings and resource shapes. The router may
prefer self-hosted capacity only when the published generation reports a
healthy compatible slot. Hosted fallback remains valid when capacity is full,
the pool is draining, or health cannot be proven.

Queue and runner metrics use bounded work-class labels. Dynamic PR, branch,
actor, and repository-provided strings are not metric labels.

## Operations

The pool tool exposes build, preflight, reconcile, status, and drain
operations. Run its `--help` against the installed candidate before use. A
safe rollout validates the pinned runner release and checksums, QEMU/KVM,
Docker, disk and memory headroom, guest boot, JIT registration, one disposable
job, teardown, and hosted fallback.

Draining stops new self-hosted selection, waits for busy jobs, removes idle JIT
registrations and guests, and leaves workflows using hosted runners. Do not
delete a busy GitHub runner or VM to accelerate drain unless the job itself is
being explicitly cancelled.

The current runner version floor and Node runtime compatibility are recorded
in `config/ci-upgrade-policy.json`. Workflow routing is implemented in
`.github/workflows/ci.yml`, `images.yml`, `cluster-smoke.yml`, and
`staging-smoke.yml`.

## Release image evidence

Image routing remains separate from release authority. The untrusted build and
trusted publication scans both use pinned Trivy v0.70.0 with `scan-type: image`,
`vuln-type: os,library`, `timeout: 10m0s`, `severity: CRITICAL`, `exit-code:
'1'`, `ignore-unfixed: 'false'`, `scanners: vuln`, and `cache: 'false'`. Before
each trusted scan, a repository helper writes the fixed config and empty ignore
file outside the checkout. The signed release predicate binds every reviewed
field, scanner name/version, controlled-file hashes, pinned action identity,
and resulting report digest.

The hosted publisher rebuilds every architecture archive from the protected
release commit and captures the single digest emitted by each architecture
push. Official evidence accepts only `trusted-rebuild` and binds the release
head, tree, ref, and current run. PR candidate archives remain untrusted CI
evidence only and are never downloaded, loaded, scanned as release, attested,
or published by the publisher. After immutable-digest attestation verification,
each architecture uploads one uniquely named canonical record. The manifest job
downloads and accepts exactly the current image's AMD64 and ARM64 records,
verifies their recorded registry subjects, and joins only their immutable
digests.

Manifest creation writes only the temporary `manifest-${HEAD_SHA}` tag and
captures the creation digest directly. Registry validation and final
attestation verification use that digest, never a mutable-tag rediscovery. The
official SHA and branch tags are promoted only after that verification.
