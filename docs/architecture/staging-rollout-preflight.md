# Staging rollout preflight invariants

Protected staging rollout uses one predicate implementation per invariant. A
predicate is not copied between broker preflight, release gate, smoke, browser,
and final convergence. It is registered as a reusable check with a stable check
ID and failure code, then invoked through the operation that applies at that
stage (`probe`, `plan`, `apply`, or `verify`).

The checked-in coverage authority is
`config/staging-rollout-preflight-coverage.json`. Every rollout step and every
legacy broker predicate must map to its earliest possible stage. A newly
observed rollout failure whose predicate could have run earlier is a preflight
coverage defect: a regression fixture and an earlier-stage check are required
before another attempt.

## Check contract

`loom_cli.rollout.preflight_contract.CheckSpec` binds:

- stable `check_id` and normalized `failure_code`;
- tier and stage capability;
- dependencies and mutation class;
- exact input keys used for a deterministic fingerprint;
- a typed evidence schema;
- timeout and evidence freshness TTL;
- bounded remediation text and secret-redaction policy;
- a technical justification for every final-only predicate.

Static and baseline checks cannot expose mutation. Rehearsal mutation is
isolated. Protected mutation is permitted only in Tier 4. Evidence cannot carry
raw credentials: token material is represented only by bounded metadata
fingerprints, owner/mode/ACL facts, and safe read-stability results.

The DAG executes dependency-ready checks with bounded concurrency. A failed
dependency blocks only its consumers; unrelated checks continue, so one report
contains all independent blockers. Every result records the implementation
digest, input fingerprint, evidence hash, discovered stage, start/finish time,
and expiry.

## Tiers

### Tier 0: admission, under two minutes

Before request publication or backup I/O, verify exact candidate/tree/install
identity, required tools, Docker/buildx/kind/kubectl, read-only systemd user manager,
credential metadata, SSH topology, GB10 mount/boot/service readiness, capacity
high-water limits, lifecycle launch/cancel behavior, and backup lease
eligibility. Browser token owner/mode/ACL/no-follow/read-stability and the GB10
timer/transient-unit classifier belong here, not at final acceptance.

### Tier 1: candidate-static outputs

Build each image once, bind immutable digests, render all manifests, perform
schema/server-side dry-run validation, validate labels/entrypoints/platforms,
validate the migration graph, render and verify systemd units, and launch the
browser runner against its report contract. These artifacts are immutable
inputs to rehearsal and final rollout; final rollout does not rebuild them.

### Tier 2: current staging readonly baseline

Readonly credentials probe health, auth, catalog/task/provider compatibility,
MinIO, PostgreSQL, ingress/TLS/DNS, worker capacity, and candidate-independent
release-gate predicates. An existing infrastructure failure is reported before
any protected mutation.

### Tier 3: isolated exact-candidate rehearsal

Use an isolated namespace, route, cloned leased DB snapshot, object prefixes,
and unit names. Apply the exact migration plan and immutable image digests, then
run environment-state, release, API, admin-on-behalf, smoke, and authenticated
browser acceptance. Cleanup is journaled and verified. Rehearsal credentials
cannot mutate protected staging, and live MinIO filesystem snapshots remain
forbidden.

### Tier 4: justified final-only checks

Only protected apply, observed live convergence, post-apply attestation drift,
and bounded final live-route smoke/browser acceptance remain. Earlier tiers do
not weaken these final gates; they ensure the final gate is not the first
consumer of a token, host prerequisite, migration, image, task, or browser
contract.

## Immutable attestation

A successful Tier 0–3 run issues one immutable `PreflightAttestation` bound to:

- candidate SHA/tree and immutable image digests;
- runner source/tree, install record, and config hashes;
- staging mutation epoch, backup lease, and DB snapshot identity;
- schema revision and migration-plan digest;
- environment, namespace, and route;
- secret metadata fingerprints only;
- GB10 inventory, boot IDs, mount and unit probe digests;
- browser runner image and report schema;
- check implementation digests and evidence hashes.

Rollout start refuses a missing, expired, incomplete, or drifted attestation.
An unchanged candidate and epoch may reuse expensive evidence until its minimum
TTL expires; start rechecks only drift-sensitive inputs. Candidate, config,
token metadata, epoch, host boot ID, mount, or implementation drift invalidates
the attestation.

Systemd readiness is single-source as well. The broker-facing Tier 0 probe and
the final GB10 convergence step consume the same user-manager, oneshot service,
and timer-state classifiers. The read-only probe binds the manager version,
`Linger=yes`, host boot ID, and a bounded user-manager RPC latency. Actual
`systemd-run` activation is not disguised as a static read: the checked-in
`rehearsal.systemd-launch` predicate belongs to the isolated rehearsal tier,
where a request-specific unit and cleanup journal prove launch/cancel latency
without mutating protected staging.

The initial contract and coverage manifest are additive foundations. Existing
broker predicates remain mapped while adapters are converted to the shared
implementations. The coverage test prevents an unmapped legacy predicate or
rollout step during that transition.
