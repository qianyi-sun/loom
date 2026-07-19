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

The canonical JSON form is strict-schema, duplicate-key rejecting, and
digest-addressed. It round-trips every binding and per-check digest before
publication. `PreflightAttestationStore` publishes the result exactly once
under the private service-owned state root using a mode-`0600` temporary file,
fsync, hard-link no-replace publication, and directory fsync. Reads traverse
with no-follow descriptors, revalidate file authority and read stability, and
recompute the payload digest. An existing exact digest may be reused; a
collision, symlink, mode/owner drift, unknown field, timestamp drift, or payload
change fails closed.

Candidate identity is also a shared predicate. Candidate resolution fixes the
commit, then `verify_bound_candidate` proves the protected config and checkout
authority, sole approved origin with no push URL, clean status, exact commit and
tree, and—under sealed-cumulative mode—detached HEAD, approved base, and a
bounded linear history. The `candidate.identity` DAG check calls that same
verifier and emits only the exact Git identities, history count, and evidence
digest. Later consumers must use the attested tree instead of re-resolving a
branch or maintaining another identity test.

Runtime readiness is similarly single-source. The fixed executable and Python
module allowlists live with `probe_runtime_readiness`; the legacy broker adapter
and the `tools.runtime` DAG check call that same probe. It evaluates every
requirement even after an earlier miss so one Tier 0 report contains the full
blocker set. Evidence records only `available`/`missing` status, counts, and
requirement/result digests—never executable paths, import exceptions, or child
process diagnostics. The DAG check additionally binds the probe to the exact
runner install hash before it imports candidate code.

Systemd readiness is single-source as well. The broker-facing Tier 0 probe and
the final GB10 convergence step consume the same user-manager, oneshot service,
and timer-state classifiers. The read-only probe binds the manager version,
`Linger=yes`, host boot ID, and a bounded user-manager RPC latency. Actual
`systemd-run` activation is not disguised as a static read: the checked-in
`rehearsal.systemd-launch` predicate belongs to the isolated rehearsal tier,
where a request-specific unit and cleanup journal prove launch/cancel latency
without mutating protected staging.

The GB10 Tier 0 host check runs one fixed read-only probe per declared host
with bounded concurrency. It validates the manager version, linger, boot ID,
completed oneshot result, enabled timer, and only the documented
`active/running` to `active/waiting` transient transition. All hosts are
reported in one result; the attestation retains per-host boot IDs and evidence
digests, while raw SSH output and service diagnostics are never published.

The initial contract and coverage manifest are additive foundations. Existing
broker predicates remain mapped while adapters are converted to the shared
implementations. The coverage test prevents an unmapped legacy predicate or
rollout step during that transition.
