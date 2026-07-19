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

Execution has an explicit pre-backup boundary. Tiers 0–2 first produce one
digest-addressed `PreflightAssessment`; no preliminary request or backup job may
be published if it contains a blocker. The same registered check plan and
immutable build/baseline artifacts are retained while a request-specific
checkpoint is created and restore-verified.
Tier 3 reruns the earlier probes as drift checks, reuses the immutable image,
manifest, and baseline artifacts instead of rebuilding them, and refuses to
attest if any input fingerprint, implementation digest, evidence hash, or TTL
differs from the assessment. This removes the previous circular dependency in
which clone rehearsal needed a verified lease while the broker demanded the
rehearsal attestation before it could create that lease.

Persistence makes that distinction explicit. `preflight.json` reserves the
candidate/tree, epoch, config, registry, coverage, and assessment identity;
the complete per-check evidence is separately published as immutable
`assessment.json` and must round-trip to that digest. The detached
`preflight-backup/job.json` may bind only that preliminary
authority. After restore verification and Tier 3, `request.json` is published
beside (never over) the preliminary record and binds the complete attestation.
Attempt envelopes and protected launch refuse a directory that contains only
the preliminary record.

The detached checkpoint coordinator owns the intervening state machine. It
reserves the sole transient payload, creates the critical checkpoint outside
the launch lock, publishes manifest-only rotation authority, runs the isolated
restore rehearsal, publishes the resulting lease by digest, and only then
atomically promotes the candidate. Cancellation or restore failure seals the
candidate without replacing the prior active lease. The full request and
attempt remain unavailable until this coordinator has completed successfully.

The broker performs the expensive Tier 0-2 assessment before acquiring
`launch.lock`. Under the short lock it rechecks admission and the mutation
epoch, publishes the preliminary request, complete assessment, immutable backup
job, and exact payload reservation, then launches one fixed transient backup
unit. It releases the lock immediately after `systemd-run`; neither checkpoint
I/O nor rehearsal runs inside it. An epoch change refuses publication, and an
existing candidate or unacknowledged retirement blocks concurrent admission.
Tier 3 therefore binds `checkpoint.evidence.sha256`, not a lease digest: asking
the DB-clone rehearsal to depend on a lease would be circular because the lease
is intentionally issued only after that rehearsal succeeds. Tier 0 may still
reuse a previously active eligible lease when its complete identity and epoch
remain unchanged.

The epoch read itself is a single fixed, read-only PostgreSQL query executed
through the protected staging pod. Its provider requires the exact
`staging`/`loom-staging` row, rejects missing, duplicate-schema, cross-environment,
negative, or non-integer authority, and returns no database credentials or
free-form diagnostics. Broker admission, checkpoint inventory, baseline probes,
and attestation drift checks must bind this same value rather than inventing a
process-local epoch.

The pipeline exposes this ordering directly. `assess` publishes Tier 0-2
evidence; `rehearse` executes and hashes the complete Tier 0-3 DAG without
publishing an attestation; and `attest` accepts only that unchanged passing
rehearsal after the coordinator has published its restore-verified lease. The
compatibility `authorize` call is only a composition of those operations and
cannot bypass their validation.

`CandidatePreflightAuthorizer` retains no process-local pending plan. Its
pre-backup planner produces only the assessment; after checkpoint publication,
the detached worker invokes a checkpoint-bound planner, requires the same
registry and coverage digests, and binds the exact checkpoint evidence and
mutation epoch before constructing `RehearsalLeaseAttestor`. Repeated
assessments are independent, while restart continuation comes only from the
immutable request-store assessment rather than RAM.

`CandidatePreflightRuntime` is the only plan-assembly boundary. It fixes the
complete Tier 0–2 implementation set and input bindings to one immutable
candidate before admission. The preliminary plan contains fail-closed Tier 3
placeholders solely to seal the complete registry identity; the assessment
never executes them. The checkpoint planner then rebuilds that identical
registry with the exact isolated actions, checkpoint evidence digest, and
rehearsal plan digest. Missing input keys, candidate drift, cross-environment
checkpoint identity, or a registry digest change fail before rehearsal or
request promotion.

Runtime plan refresh creates new probe sessions for every assessment and
checkpoint-bound rehearsal while requiring each refreshed check to retain the
same contract and implementation digests. This prevents a cached Tier 2
baseline from hiding mutation-epoch or infrastructure drift. Build-once image
artifacts remain owned by their exact candidate session and are re-inspected,
not rebuilt, when the refreshed checks consume them.

`PreflightRuntimeSources` is the typed composition root for those checks. It
accepts only the low-level command/probe boundaries and exact expected
identities, derives every context input from the same registered-check helpers,
and assembles the entire Tier 0–2 coverage set. A fresh-checkpoint sentinel is
explicit and cannot claim lease reuse; an active lease must instead supply its
complete manifest, component, epoch, snapshot, schema, environment, namespace,
and inventory identity. The composition root shares one candidate image-build
session while refreshing baseline and metadata probes between stages.

Tier 2 is additionally gated by `readonly.authority`. Its evidence comes from
the server-observed capability set for the dedicated
`loom-rollout-readonly` principal, not from the fact that a probe happened to
use GET. Any Kubernetes write verb, wildcard authority, secret/token read, or
HTTP mutation method blocks admission before a baseline probe can run.
The application credential is a database-backed `readonly_probe` principal
with the exact `read:own` scope. The service admits that type only for GET or
HEAD, never updates token usage timestamps while authenticating it, and all
other services reject the type by default. A normal team, worker, browser, or
admin credential cannot satisfy this authority check merely because the
selected baseline endpoints happen to be reads.
The Kubernetes side is declared in
`deploy/k8s/staging-rollout-readonly.yaml`: one namespace-scoped service
account, Role, and RoleBinding, with token automount disabled and only
`get/list/watch`. No ClusterRole, secret access, token material, wildcard, or
write verb is admitted. Runtime credentials use a bounded TokenRequest and are
validated by the server-observed review before any Tier 2 call.

`CandidatePreflightOrchestrator` is the restart boundary between admission and
rehearsal. The broker executes Tier 0–2 from a runtime factory before it
publishes a request. The detached worker later rebuilds that runtime from the
immutable candidate and mutation epoch, then binds Tier 3 to the published
checkpoint. Registry and implementation digests must be byte-identical across
both processes; no in-memory pending plan or cached live probe is rollout
authority.

`DetachedPreflightBackupRunner` is the worker-side composition root. It first
revalidates the persisted assessment against the immutable request, then owns
the checkpoint, rehearsal, restore proof, lease, attestation, rotation and
retirement sequence. Restore and attestation each rebuild their attestor from
recorded authority; only the persisted rehearsal bridges them.

Restore evidence is derived from the immutable rehearsal itself, not from an
operator assertion. Every Tier 3 action must share one isolation ID, candidate,
and mutation epoch; every action must report no protected mutation; the DB clone
must pass; and the terminal cleanup must be verified. The report digest binds
all Tier 3 implementation, input, evidence, and journal digests together with
the critical-checkpoint evidence digest.

The complete Tier 3 rehearsal is published once as
`preflight-backup/rehearsal.json` before restore evidence is issued. Reads
strictly reconstruct every typed execution, recompute the rehearsal digest, and
reject replacement or drift. `RehearsalLeaseAttestor` is the only coordinator
bridge from that record to restore proof and then to the final attestation: it
first persists the passing rehearsal, derives the exact restore proof, later
re-reads the same immutable record, and binds the newly published backup lease.
This makes process restart or worker handoff unable to substitute a second
rehearsal, an operator assertion, or a pre-existing lease.

The resulting attestation binds the restore-verified lease ID and digest, the
checkpoint manifest and component-set digests, DB snapshot identity, schema
revision, and immutable-object inventory root. Pre-backup reuse evidence cannot
silently substitute for a newly restored lease; the post-rehearsal binding is
constructed from the published `BackupLease` itself.

The detached worker's verified state records the exact manifest digest, lease
digest, and attestation digest as one compare-and-swap transition. A later
request promotion cannot select an unrelated or merely pre-existing
attestation, and failure to publish the attestation leaves the new payload
unpromoted.

Only after that `backup_verified` transition does the worker promote
`preflight.json` into the immutable rollout `request.json` and publish attempt
1's exact driver envelope. The shared lifecycle graph then advances by CAS to
`launch_pending`, launches the fixed rollout unit, and records
`launch_running`. A finalization crash therefore leaves either reusable
verified backup authority or an explicit pending-launch record; it cannot leave
an unattested attempt runnable.

## Tiers

Historical late failures are executable fixtures rather than names in a list.
`replay_regression_manifest` requires one case for every checked-in fixture,
verifies the production `RegisteredCheck` identity and declared tier, injects
the fault through that check's real probe, validates its evidence schema, and
requires the normalized failure code. Missing fixtures, an implementation
substitution, or a historical fault that unexpectedly passes is a blocking
coverage defect.

### Tier 0: admission, under two minutes

Before request publication or backup I/O, verify exact candidate/tree/install
identity, required tools, Docker/buildx/kind/kubectl, read-only systemd user manager,
credential metadata, SSH topology, GB10 mount/boot/service readiness, capacity
high-water limits, lifecycle launch/cancel behavior, and backup lease
eligibility. Browser token owner/mode/ACL/no-follow/read-stability and the GB10
timer/transient-unit classifier belong here, not at final acceptance.

Lease eligibility is an admission decision, not a requirement that a reusable
lease already exist. An exact, fresh, restore-verified active lease selects
`reuse`; an absent, expired, or epoch-stale lease selects `fresh` after capacity
admission succeeds. Unreadable lease authority, cross-environment state, or
input/digest drift remains a blocker. This allows a first rollout to create its
checkpoint without weakening fail-closed reuse or making Tier 0 circular.

### Tier 1: candidate-static outputs

Build each image once, bind immutable digests, render all manifests, perform
schema/server-side dry-run validation, validate labels/entrypoints/platforms,
validate the migration graph, render and verify systemd units, and launch the
browser runner against its report contract. These artifacts are immutable
inputs to rehearsal and final rollout; final rollout does not rebuild them.

`migration.plan` loads the exact candidate's Alembic graph without a database
connection. The checked-in staging policy requires one closed linear chain,
the declared head, expand/contract before destructive upgrades, revision-owned
fail-closed downgrade behavior, and rehearsal before protected apply. Its plan
digest binds every revision source hash and the policy digest; clone rehearsal
and final apply must consume that same plan rather than rediscovering a head.

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

`RehearsalActionSource` is the sole plan and resource-name authority for these
actions. Its identity factory and action factory consume the same checkpoint,
candidate tree, image artifact set, migration plan, browser report schema and
route origin. The represented username, team UUID, admin audit actor, smoke
task, required worker pool and agent are strict non-secret authority fields in
that same plan; changing any of them changes the isolation identity before the
helper can run. Namespace, database, object-prefix, route and transient-unit
names are derived from that immutable plan and must retain their dedicated
`rehearsal` prefixes; a backend cannot substitute `loom-staging` or another
protected resource name. A change to any bound input changes both the plan
digest and isolation identity before execution.

`AdminSmokeContract` is the sole admin-on-behalf payload and result predicate
for both Tier 3 and the final smoke step. Tier 3 exercises health, admin
identity, benchmark/task catalog, exact represented-user submission and cloned
database persistence. Terminal worker completion remains final-only because an
isolated namespace cannot safely borrow a protected GB10 worker or its Docker
authority; the final gate consumes the same identity, payload, fanout-failure
and terminal-result predicates and is not weakened.

Tier 3 is therefore the first stage after the request-specific backup worker
has published a restore-verified lease. The pre-backup assessment alone is
never launch authority; only the complete Tier 0–3 attestation can be promoted
into the immutable rollout request and driver envelope.

`rehearsal.cleanup` is an explicit always-after-dependencies check. It waits
for every isolated action to reach a terminal outcome, but it is not suppressed
when one of those actions fails. This exception is restricted by the reusable
check contract to dependent Tier 3 isolated cleanup; static, readonly and
protected checks cannot opt out of ordinary dependency blocking. A failed
rehearsal therefore still produces cleanup evidence instead of leaking its
namespace, database clone, object prefix or transient unit.

Each concrete action is wrapped by `JournaledRehearsalBackend`. The
service-owned mode-0700 root contains one mode-0700 directory per derived
isolation identity and one immutable mode-0600 no-follow terminal record per
step. Exact records are reused after a worker restart; a candidate, epoch or
plan collision fails closed. Runner exceptions become a normalized
`isolated-action-failed` blocker without persisting diagnostics, and are not
silently executed again. Concrete resource operations retain their own
idempotent cleanup ledger below this terminal evidence boundary.

The journal invokes concrete work only through the installed
`loom-staging-rollout-rehearsal` helper. Its caller supplies a fixed executable,
`execute`, one allowlisted check ID, the private immutable plan path and its
digest; no shell, arbitrary argv, ambient environment or secret value crosses
the boundary. The helper returns one bounded strict-JSON result on stdout and
no stderr. Exit status, plan digest, check identity, blockers and cleanup proof
must agree before the terminal record can be published.

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

The root installer retains its full transactional ledger at
`/etc/loom/staging-rollout.install.json` as `root:root` mode `0600`; the broker
does not gain access to that ledger. After a ready install, root also publishes
`staging-rollout.install-attestation.json` as a single-link
`root:loom-rollout` mode-`0640` statement containing only source identity, the
full-record digest, and fixed installed-asset digests. `runner.install` reads it
through the common no-follow authority reader, binds it to the candidate and
config inputs, then re-reads and hashes every live asset. Missing, stale,
rewritten, relinked, or metadata-drifted statements and assets fail before
candidate imports, request publication, or backup.

Runtime readiness is similarly single-source. The fixed executable and Python
module allowlists live with `probe_runtime_readiness`; the legacy broker adapter
and the `tools.runtime` DAG check call that same probe. It evaluates every
requirement even after an earlier miss so one Tier 0 report contains the full
blocker set. Evidence records only `available`/`missing` status, counts, and
requirement/result digests—never executable paths, import exceptions, or child
process diagnostics. The DAG check additionally binds the probe to the exact
runner install hash before it imports candidate code.

Docker readiness follows the same rule. `probe_docker_runtime` owns the fixed,
read-only daemon and buildx predicates; both the compatibility preflight report
and Tier 0 `docker.runtime` consume its complete aggregate. A failing daemon
probe does not suppress the buildx probe, and neither command's stdout or stderr
is admitted to evidence.

`probe_kubernetes_client` likewise owns the fixed kubeconfig, current-context,
and target-namespace predicate. It always probes both context and namespace,
emits only the allowlisted context/namespace plus booleans and a digest, and is
bound by the DAG to the kubeconfig metadata digest. The compatibility report
uses this implementation instead of maintaining a second context policy.

Systemd readiness is single-source as well. The broker-facing Tier 0 probe and
the final GB10 convergence step consume the same user-manager, oneshot service,
and timer-state classifiers. The read-only probe binds the manager version,
`Linger=yes`, host boot ID, and a bounded user-manager RPC latency. Actual
`systemd-run` activation is not disguised as a static read: the checked-in
`rehearsal.systemd-launch` predicate belongs to the isolated rehearsal tier,
where a request-specific unit and cleanup journal prove launch/cancel latency
without mutating protected staging.

Backup and launch admission use the shared `lifecycle_protocol` transition
table rather than step-local status strings. A request may publish launch only
after backup verification; pending/running backup may instead enter the
cancel-requested path and must seal as failed before any new admission. Tier 0
`lifecycle.launch-cancel` exercises success, pre-start cancellation,
in-flight cancellation, backup failure, and forbidden early-launch sequences
against that same implementation. The probe is static; it does not create a
request, unit, backup, or protected-staging mutation.

The GB10 Tier 0 path first runs `gb10.ssh-topology` with the exact SSH-config
digest, no-follow identity metadata, strict known-hosts policy, and BatchMode.
It probes every declared host concurrently and reports the full reachable and
failed sets without admitting remote diagnostics. Shared-mount identity is a
separate dependent predicate, so connectivity cannot mask a mount blocker.

The subsequent host check runs one fixed read-only probe per declared host
with bounded concurrency. It validates the manager version, linger, boot ID,
completed oneshot result, enabled timer, and only the documented
`active/running` to `active/waiting` transient transition. All hosts are
reported in one result; the attestation retains per-host boot IDs and evidence
digests, while raw SSH output and service diagnostics are never published.

The initial contract and coverage manifest are additive foundations. Existing
broker predicates remain mapped while adapters are converted to the shared
implementations. The coverage test prevents an unmapped legacy predicate or
rollout step during that transition.

Before a DAG may issue an attestation, its registry must exactly match every
manifest entry through the requested tier: one implementation per check with
the same failure code, dependencies, stage, mutation class, and final-only
justification. Missing, duplicate, unexpected, or drifted implementations fail
closed instead of silently shrinking coverage.

The runtime `PreflightRegistry` hashes the checked-in manifest together with
every check's complete contract and implementation. Input keys, evidence
schema, timeout, freshness TTL, remediation, or redaction-policy drift therefore
changes the registry identity even when a check keeps the same name. The
registry is order-independent and constructs the only DAG eligible to issue an
attestation.
