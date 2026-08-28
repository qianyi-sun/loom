# Staging rollout preflight invariants

Protected staging rollout uses one predicate implementation per invariant. A
predicate is not copied between broker preflight, release gate, smoke, browser,
and final convergence. It is registered as a reusable check with a stable check
ID and failure code, then invoked through the operation that applies at that
stage (`probe`, `plan`, `apply`, or `verify`).

The checked-in coverage authority is
`src/loom_cli/data/staging-rollout-preflight-coverage.json`, packaged as
runtime data in the installed wheel. Every rollout step and every
legacy broker predicate must map to its earliest possible stage. A newly
observed rollout failure whose predicate could have run earlier is a preflight
coverage defect: a regression fixture and an earlier-stage check are required
before another attempt.
The same manifest separately enumerates every concrete Tier 4 subpredicate.
Each one either points to the earlier check that already proves its reusable
contract or carries a technical final-only justification. The code-level
`FINAL_PREDICATE_IDS` authority must match that manifest exactly, so a late
migration, GB10, smoke, browser, drift, convergence, or summary predicate
cannot remain unclassified while still producing an accepted coverage digest.
The legacy protected-driver failure map is validated in the reverse direction:
every mapped step must also appear as a consumer of its declared check. Merely
naming an existing check ID cannot hide an uncovered late predicate.

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
digest, input fingerprint, schema-validated redacted evidence, evidence hash,
discovered stage, start/finish time, and expiry. The public blocker report
includes that same typed evidence rather than only its digest; it therefore
remains actionable without exposing a credential value or unbounded child
output.

Execution has an explicit pre-backup boundary. Tiers 0–2 first produce one
digest-addressed `PreflightAssessment`; no preliminary request or backup job may
be published if it contains a blocker. The same registered check plan and
immutable build/baseline artifacts are retained while a request-specific
checkpoint is created and restore-verified.
The broker derives the one passing `artifacts.publish` reference from that
assessment before it publishes a request. Passing `preflight`, preview, and
backup-pending output exposes only its secret-free bundle digest as
`preflight_artifact_bundle_sha256`; `assessment.json` remains the complete
immutable evidence authority.
Tier 3 reruns the earlier probes as drift checks, reuses the immutable image,
manifest, and baseline artifacts instead of rebuilding them, and refuses to
attest if a rerun is stale or failed, if a check implementation changed, or if
an authority input fingerprint changed. The one declared exception is
`backup.lease-eligibility`: its input must transition from the pre-backup
fresh-checkpoint sentinel to the newly restore-verified checkpoint. Volatile
latency, transient-unit and host observations may produce a new evidence hash;
the fresh passing evidence becomes the attestation authority and is rechecked
again at final admission. Candidate-static server-schema, field-ownership and
browser-runtime evidence uses the same one-hour freshness budget as the other
immutable Tier 1 artifacts, so the bounded cleanup wait cannot expire it while
the current DAG is still running. This avoids a circular dependency in which
clone rehearsal needs a verified lease while the broker requires rehearsal
attestation before creating that lease.

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
I/O nor rehearsal runs inside it. An epoch change refuses publication. An
existing candidate or a rotation already holding two physical payloads blocks
concurrent admission; one failed retirement may share the bounded transient
window with its replacement so a protected legacy `latest` pointer cannot
deadlock recovery.
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

Rendered-manifest image authority is profile-aware and explicit. The installed
composition derives the required Kubernetes image set from the exact cluster
profile; for example, the GB10 staging profile intentionally excludes
`loom-worker` while `k8s_worker.enabled=false`. The rendered artifact must
contain every enabled rollout image, must contain no disabled rollout image,
and binds each observed image to the corresponding build-once image ID. The
enabled image-name set is stored in the private artifact descriptor and is
revalidated when the detached worker reloads it, so an intentionally disabled
workload cannot be confused with an accidentally missing manifest.

The same Tier 1 render check validates workload identity before any Kubernetes
apply. A Pod, controller, Job, or CronJob that sets effective
`runAsNonRoot: true` must also set an explicit positive numeric `runAsUser` at
pod or container scope. Depending on an image's ambient user metadata is a
`manifests.render.failed` blocker, because otherwise a root-default image can
survive static rendering and fail only at protected container creation. This
predicate is shared by preflight artifact publication and the final consumer;
the final path does not carry a second, weaker copy.

The render check also evaluates the in-namespace NetworkPolicy graph. For every
explicit pod-selector egress edge whose destination is ingress-isolated in the
same render, at least one target ingress policy must admit the source selector
on a compatible port and protocol. Kubernetes policy-union semantics are
preserved; external namespaces and IP-block destinations remain runtime probes.
This shifts lifecycle-to-MinIO, gateway-router-to-LLM-gateway, and
xDS-to-Postgres/pgBouncer asymmetry to `manifests.render.failed` before an exact
artifact can be published.

`images.contract` complements the manifest predicate with an actual bounded
runtime probe. Every declared non-root import is launched from the immutable
build-once image using its exact UID/GID, `--network none`, and a read-only root
filesystem. The probe imports only its fixed module and carries no staging
credential. A file-mode, package-layout, interpreter, or image-user mismatch is
therefore an `images.contract.invalid` Tier 1 blocker rather than the first
protected CronJob launch discovering it. The probe specification is included
in the image-plan digest and is rerun when the immutable image IDs are verified;
the rollout consumes those IDs and does not rebuild the images.

Tier 1 also normalizes the environment profile's rate-card sync and hosted
provider pricing defaults into one canonical, secret-free
`ProductionDefaultsArtifact`. Provider ordering, optional sync inputs,
candidate SHA/tree and environment are digest-bound. The artifact is published
beside the rendered and migration manifests, so rehearsal and final protected
apply consume the same desired values instead of reopening an ambient or
mutable environment profile after backup.

The rehearsal identity includes that defaults digest. Protected apply later
uses one journaled `production-defaults` component after the mutation epoch,
migration and Kubernetes manifests are exact. Its single classifier reads the
token-scoped provider view from the Service API and only the relevant global
rate-card projection from PostgreSQL; apply uses that same Service API with the
attested `file:` service-token path, then the same
classifier must observe exact state before terminal evidence is published.
Missing or duplicate provider identities, token metadata drift, an unexpected
route or HTTP operation, and a changed artifact all fail closed. The token
value is never written to the plan, journal, argv or evidence.

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
The capability probe consumes only the minimal SELECT-only role, schema, and
mutation-epoch authority digest. It never consumes baseline rows, lifecycle
capacity, or immutable-object inventory. Those predicates remain independent
registered checks, so a missing capacity row cannot mask a valid readonly
principal or collapse all Tier 2 failures into `readonly.authority.unsafe`.
The complete database snapshot likewise preserves a missing capacity row as
`capacity = null` while retaining its exact role, schema, mutation epoch,
catalog counts and immutable-object inventory. Only `staging.storage-db`
reports `dependency-capacity-unready`; health, authentication, catalog and
network probes continue independently so the DAG returns the real complete
blocker set in one pass.
The Kubernetes side is declared in
`deploy/k8s/staging-rollout-readonly.yaml`: one namespace-scoped service
account, Role, and RoleBinding, with token automount disabled and only
`get/list/watch` plus `create` on the non-object-mutating
`pods/portforward` subresource for `loom-minio-0` and the exact
`loom-postgres-1` through `loom-postgres-3` pods. Those are transports to a
separately authenticated SELECT-only PostgreSQL role and a fixed non-mutating
MinIO identity; they cannot
create or update a Kubernetes object and cannot connect to another pod. MinIO
readiness uses a bounded bucket-versioning read through that exact localhost
transport. Exact-version reads prove pinned object recoverability without a
privileged Kubernetes credential. No ClusterRole, secret access, token material, wildcard, or other write
verb is admitted. Runtime credentials use a bounded TokenRequest and are
validated by the server-observed review before any Tier 2 call.
The TokenRequest is rendered into a minified kubeconfig with no inherited
client certificate or root credential. The shared renderer accepts only the
exact service-account subject, the fixed staging API audience
`https://kubernetes.default.svc.cluster.local`, a lifetime no longer than 24
hours, and at least two hours of remaining validity. The credential installer
requires four hours of remaining validity for install-time reuse, so the full
post-install preflight cannot consume the runtime safety margin. It also rotates
a token whose metadata is otherwise fresh but whose audience cannot be accepted
by the fixed API server. The PostgreSQL transport uses kubectl's
explicit loopback `--address=127.0.0.1` option plus a numeric `LOCAL:5432`
mapping; an address must never be encoded as a third port-mapping field because
kubectl interprets that field as a named pod port. Installer SQL uses
`kubectl exec --stdin` so `psql -f -` receives the fixed transaction; a silent
EOF success is not convergence evidence. PostgreSQL
baseline evidence is collected in one `REPEATABLE READ, READ ONLY`
transaction as `loom_rollout_readonly`; the role is required to be
non-superuser, `NOINHERIT`, `NOCREATEROLE`, `NOCREATEDB`, `NOREPLICATION` and
`NOBYPASSRLS`. The bootstrap also revokes the database's default `PUBLIC`
`TEMP` grant; revoking it only from the named role cannot override an inherited
`PUBLIC` grant. The staging database inventory permits only its `loom` owner
and this readonly login, so the owner retains its implicit authority while the
probe cannot create session-local tables. A pre-0069 database has no
mutation-epoch table, so the source
accepts only its exact four-digit Alembic revision and binds the one-time
bootstrap epoch `0` to that revision. Revision 0069 and later must provide the
exact staging epoch row; revision 0070 and later must also provide exact
capacity authority. A missing newer table never falls back to legacy mode.
The root installer creates or reuses one 256-bit credential in a root-owned
0600 file, converges the exact role through `psql` stdin, and publishes a
service-owned 0600 copy. The password is never placed in argv, stdout,
install evidence, or an attestation. Reinstall revokes table and sequence
authority before granting SELECT back to the fixed allowlist, so privilege
drift cannot silently accumulate.
The concurrent DAG shares one process-local, single-flight database snapshot
for Tier 2 baseline and mutation-epoch binding. Critical checkpoint inventory
uses a separate single-flight `REPEATABLE READ, READ ONLY` snapshot and remains
strict: invalid version, digest or authoritative-source metadata blocks backup
lease eligibility without masking independent health, authentication, catalog,
storage or network evidence. Large valid inventories are read in deterministic
1024-row pages without relaxing the per-query row bound, and the staging
admission object limit is also the fail-closed inventory ceiling. Tier 0
capacity separately shares one fresh,
single-flight read-only MinIO/filesystem snapshot, so pre-0070 staging remains
measurable without weakening database schema authority. Its live inventory may
use up to 60 seconds inside the overall sub-two-minute Tier 0 budget; a measured
31-second high-water inventory must return its exact object/byte/disk/inode
evidence instead of being misclassified as a timeout. The critical checkpoint's
pinned benchmark/catalog/system inventory is derived from that same snapshot,
never from a privileged `kubectl exec`. A pre-0069 snapshot binds an explicitly
empty legacy inventory because typed object authority does not exist yet;
revision 0069 and later must return a sorted, unique, fully classified inventory
from the lifecycle tables. The exact tunnel and DB
session are always rolled back and closed after evidence collection.
Installed preflight treats the readonly application token, readonly kubeconfig,
rehearsal kubeconfig, and server-dry-run kubeconfig as distinct private
credential authorities; all four must pass the same stable-read, owner, mode,
ACL, and metadata-fingerprint gate. Token values never enter an attestation or
install record.

`CandidatePreflightOrchestrator` is the restart boundary between admission and
rehearsal. The broker executes Tier 0–2 from a runtime factory before it
publishes a request. The detached worker later rebuilds that runtime from the
immutable candidate and mutation epoch, then binds Tier 3 to the published
checkpoint. Registry and implementation digests must be byte-identical across
both processes; no in-memory pending plan or cached live probe is rollout
authority.

## Guarded launch boundary

The Tier 0–2 assessment remains outside the lifecycle mutation guard. For a
non-preview launch, the broker binds the exact candidate and epoch first, then
acquires the request-bound guard before publishing the detached backup job.
The guard's ready evidence must agree with the request ID, candidate SHA and
tree, original mutation epoch, database backend PID, and entry-anchored
absolute deadline at broker admission, backup-worker continuation, and detached
attempt execution. This prevents an in-memory handoff, a stale request, or a
different candidate from turning a valid assessment into mutation authority.

The guard suspends the legacy lifecycle CronJob and requires a stable empty
inventory of exact owner-UID nonterminal Jobs before and after taking the
shared PostgreSQL advisory lock. A dedicated autocommit session continuously
proves the same backend owns that exact lock. The request-bound backup and
attempt units each declare `After=` on the exact guard, without `BindsTo=`, and
the guard independently checks their exact owner liveness. This lets the owner
synchronously complete verified guard release before publishing its terminal
event and clearing the active pointer. A fixed request-bound guard
`ExecStopPost` is a no-op only for exact released evidence; ready, missing, or
unsafe evidence instead requires a fully validated exact owner inventory and
hard-kills each live backup or attempt control group with `SIGKILL`. Exact unit
names, never wildcard kill targets or graceful owner stops, prevent recursive
release deadlock and cross-request fencing. If that stop-post action cannot
prove dispatch, the minute reconciler retries the same fence while keeping the
CronJob suspended. Even a successful kill is followed by two fresh, complete,
stable-empty exact-owner inventories and an unchanged-evidence read before
restoration; any live/deactivating owner or uncertainty defers restoration to a
later timer run. Released evidence plus surviving annotations is rejected as a
normal-release contradiction. The guard uses `Restart=no` and one
entry-anchored absolute lifetime. Once the backup worker has started the
detached rollout attempt, it hands that same guard to the attempt; guard
ownership ends only through the verified release path or guarded orphan
reconciliation. Thus preflight evidence, backup, protected apply, and final
admission have one continuous candidate- and request-bound mutation exclusion
window, while the regular lifecycle lock still governs the broader rollout
lifecycle. Resume reloads the immutable original preflight request, reacquires
the guard, and refuses original-epoch or current-epoch drift before launch.

The guard CLI keeps fixed Kubernetes subprocesses at 120 seconds and caps each
fixed systemd inventory or kill at 30 seconds. A stop arriving just after a
false stop check can wait through one 30-second owner inventory, the one-second
poll sleep, and the next 15-second lock-health query. CronJob restoration,
advisory unlock, database-tunnel teardown, and the evidence-publication margin
raise the complete normal-release bound to 342 seconds, so the guard emits
`TimeoutStopSec=343s`. Its largest immediate unsafe fence is one inventory plus
two exact kills, or 90 seconds; broker and worker systemd clients use 434
seconds, strictly above the service stop plus that stop-post fence. The
reconciliation service allows 12 minutes for its conservative 571-second
sequence: three Kubernetes commands, two 15-second candidate-identity
commands, six systemd commands, and the stable-absence poll. Any timeout still
preserves the freeze for the next minute-timer retry.

Preview is deliberately narrower. `start --dry-run` performs normal admission,
candidate resolution, assessment, and final epoch-drift checking, then writes a
preliminary preview request and immutable assessment to the service ledger. It
does not acquire the guard or advisory lock, suspend the CronJob, reserve a
backup payload, or start any detached unit. The preview is therefore
mutation-free for the staging cluster, backup state, and lifecycle data, but
it is not an assertion that no service-owned evidence record is written.

`DetachedPreflightBackupRunner` is the worker-side composition root. It first
revalidates the persisted assessment against the immutable request, then owns
the checkpoint, rehearsal, restore proof, lease, attestation, rotation and
retirement sequence. Its installed composition must include the private-store
active-payload resolver and the evidence-first exact payload retirer; leaving
either callback unbound is not a valid production composition. Restore and
attestation each rebuild their attestor from
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

Known late-failure classes are executable fixtures rather than names in a list.
`replay_regression_manifest` requires one case for every checked-in fixture,
verifies the production `RegisteredCheck` identity and declared tier, injects
the fault through that check's real probe, validates its evidence schema, and
requires the normalized failure code. Missing fixtures, an implementation
substitution, or a recorded fault that unexpectedly passes is a blocking
coverage defect.

### Tier 0: admission, under two minutes

Before request publication or backup I/O, verify exact candidate/tree/install
identity, required tools, Docker/buildx/kubectl, read-only systemd user manager,
credential metadata, SSH topology, GB10 mount/boot/service readiness, capacity
high-water limits, lifecycle launch/cancel behavior, and backup lease
eligibility. Browser token owner/mode/ACL/no-follow/read-stability and the GB10
timer/transient-unit classifier belong here, not at final acceptance.

`capacity.high-water` always consumes one fresh, process-local single-flight
snapshot from the installed read-only MinIO authority and the canonical
staging MinIO filesystem. The same predicate and policy digest therefore work
before and after migration `0070`; a database capacity row is not substituted
for live object, byte, disk, or inode observation. The dedicated S3 policy
allows bucket location/versioning, list/version-list, and exact-version reads
for the two execution buckets, while the TokenRequest permits port-forward only to the
exact MinIO and PostgreSQL pods. Credentials and object keys never enter DAG
evidence.
The requestless admission path reads the readonly role, schema revision, and
mutation epoch through one minimal typed probe before it constructs the DAG.
The full Tier 2 database baseline reuses that exact probe and then evaluates
baseline rows, runtime capacity authority, and immutable-object inventory. A
missing migration-`0070` capacity row therefore remains a blocking Tier 2
failure, but it cannot prevent the DAG from reporting independent Tier 0–2
blockers or mask the fresh `capacity.high-water` observation.
The backup admission adapter follows the same rule: if its strict immutable
inventory cannot be read, it supplies only an all-zero unavailable binding and
the registered `backup.lease-eligibility` check fails closed. It never treats
an unreadable inventory as an absent lease or a fresh-checkpoint authorization.
The root installer converges that non-mutating identity through the exact MinIO
pod and must invoke `kubectl exec --stdin`: the generated access and secret are
delivered only on stdin, never argv, environment, logs, or evidence. A missing
stdin-forwarding flag fails before policy publication and is a preflight
coverage defect, not a retryable rollout failure.
MinIO IAM publication may briefly return a stale read immediately after an
exact policy attachment. The installer performs only five bounded read-only
observations over 2.75 seconds of the same immutable user/policy contract;
every field must converge exactly, and persistent absence or drift remains a
hard failure.

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

Task-image builder rollout validation has four distinct phases. Candidate-static
rehearsal reads the candidate policy, validates local Slurm authority, and hashes
purely rendered requests; it does not read a release-specific builder
environment, repository, token, registry configuration, or production database.
After isolated rehearsal succeeds, the protected host-local oneshot atomically
derives the exact release builder environment. The same oneshot then validates
the materialized files, dedicated token scope, registry authorization, and an
`sbatch --test-only` request for every policy-authorized node. Only after those
checks does it call reconciliation. The protected supervisor transport starts
that oneshot, requires its success, and only then enables and starts the timer.

Predecessor admission has one bounded recovery bridge for correcting a broken
supervisor policy. A canonical predecessor whose exact unit bytes and active,
enabled, fresh timer are still authoritative may be classified as
`repairable` when its oneshot has a nonzero `exit-code` result and the target
Git candidate SHA/tree are different. The transition journal records
`supervisor-repair`; successful protected apply must produce the normal exact
runtime proof. If target activation fails, compensation restores the canonical
predecessor bytes and accepts only that same fail-closed timer/oneshot shape so
the journal can close. Same-candidate failures, missing or stale timers,
candidate-only live units, mixed bytes, and every other drift remain blocking.

A failed post-materialization check therefore produces protected rollout
failure evidence without submitting a builder or enabling periodic scale-up.
Drain-only reconciliation remains available without live builder credentials,
so a credential failure cannot prevent the system from reducing capacity.

Manifest readiness is two independent checks backed by one apply contract.
`manifests.server-schema` uses strict server-side dry-run with conflict forcing
only inside that mutation-free request, so an existing field manager cannot
hide API schema or defaulting failures. `manifests.field-ownership` uses the
exact fixed-manager, no-force server-side apply contract consumed by final
protected convergence. Ownership can therefore block protected apply without
blocking immutable artifact publication or isolated rehearsal, and
`--force-conflicts` cannot leak into the final apply command.

Recognized legacy field ownership is converged only through the separate
`manifest-ownership` maintenance protocol. It accepts every existing exact
rendered resource, binds each live UID, resourceVersion, optional generation,
managed-fields digest and semantic state, and builds an overlay from live
values only. A force-conflicts server dry-run must prove that overlay is a
semantic no-op before an operator can approve its inventory digest. Apply then
requires the same explicit `--artifact-bundle-sha256` used for inventory, the
root-owned maintenance admission freeze, an idle rollout pointer, a no-replace
private journal, and a compare-and-swap mutation-epoch claim. The artifact
digest is part of the approved inventory hash, so apply cannot substitute a
different publication even when it contains equivalent manifests. The
force operation transfers only fields already present. A following selective
JSON Patch stage removes only checked-in recognized legacy managed-field
entries after a semantic-no-op server dry-run; it retains the canonical Loom
manager and controller ownership and never performs a blanket managed-fields
reset. The three NetworkPolicies subsequently converge through the normal
no-force contract, while the suspended lifecycle CronJob remains suspended
until lifecycle inventory and deletion authority are separately accepted.
UID/resourceVersion preconditions, cleanup evidence, a post-apply live readback
and a final full no-force dry-run make concurrent drift fail closed. This is a
one-time ownership migration, not a general force flag or fallback apply path.

The one-shot `lifecycle-capacity` maintenance protocol has the same explicit
artifact selection rule. Inventory and apply require
`--artifact-bundle-sha256`; its plan binds that digest, and execution reloads
only the digest embedded in the claimed plan. Candidate, tree, epoch,
rendered-manifest, image, or registry drift fails before the capacity Job is
applied.

The maintenance marker is entered and left only by the exact installed root
host helper. Entry is serialized with broker launch admission and proves that
the active pointer and the exact backup, rollout, and mutation-guard unit sets
are idle. The helper also validates every durable preflight-backup phase before
that unit inventory. Because marker entry is serialized with launch admission,
a valid durable phase with no live request-bound unit is orphaned history, not
executable work; malformed durable state remains a hard refusal. The marker
remains the authoritative admission freeze for the whole
inventory/apply/readback window; ordinary host health reports it explicitly
instead of treating maintenance as a normal ready state. Exit repeats the idle
proof before removing the marker.

`migration.plan` loads the exact candidate's Alembic graph without a database
connection. The checked-in staging policy requires one closed single-head DAG,
the declared head, expand/contract before destructive upgrades, revision-owned
fail-closed downgrade behavior, and rehearsal before protected apply. Its plan
digest binds every revision source hash and the policy digest; clone rehearsal
and final apply must consume that same plan rather than rediscovering a head.
`migration.manifest` then renders the one exact Kubernetes Job with a
deterministic candidate/tree/plan-derived name, binds the built control-plane
image ID, and performs server-side dry-run. The private artifact publication
stores that Job beside the standing render under its own content and artifact
digests. Rehearsal and final apply may reconstruct and verify it, but may not
rerender it after checkpoint creation.

### Tier 2: current staging readonly baseline

Readonly credentials probe health, auth, catalog/task/provider compatibility,
MinIO, PostgreSQL, ingress/TLS/DNS, worker capacity, and candidate-independent
release-gate predicates. An existing infrastructure failure is reported before
any protected mutation. The five independent baseline probes depend only on
their minimum readonly credential/tool authorities; a health failure does not
prevent auth, storage/database, network, or catalog/task evidence from being
collected in the same bounded DAG run.

### Tier 3: isolated exact-candidate rehearsal

Use an isolated namespace, route, cloned leased DB snapshot, object prefixes,
and unit names. Apply the exact migration plan and immutable image digests, then
run environment-state, release, API, admin-on-behalf, smoke, and authenticated
browser acceptance. Cleanup is journaled and verified. Rehearsal credentials
cannot mutate protected staging, and live MinIO filesystem snapshots remain
forbidden.

The systemd rehearsal uses the same `RehearsalSystemdActivation` predicate on
platform-dev and every fixed active GB10 host. It creates only the plan-derived
`loom-preflight-*` transient unit, binds the observed boot IDs and fleet
evidence digest, removes the unit immediately on every remote host, and records
that cleanup in the rehearsal journal. Any inaccessible or identity-drifted
unit blocks the rehearsal; protected node-agent services and timers are never
activated by this check.
Successful transient activation is determined by the command exit status and
the exact post-start unit properties, not by the mere presence of bounded
systemd warning text. This preserves fail-closed identity and cleanup checks
without misclassifying a non-root firewall warning after the unit has actually
started and converged. Kubernetes rehearsal manifests likewise match the exact
declared pod fields plus the fixed API-server termination-message and
readiness-success defaults; unknown defaulting or desired-state drift still
fails before restore. Each exact image is published to the configured registry,
resolved back to the attested OCI index, and bound to the single target-platform
manifest, config digest, and exact candidate revision label. Every unresolved
or non-derived digest remains invalid.

The isolated database rehearsal first streams the exact checkpoint dump into
the pod-scoped emptyDir, verifies its SHA-256 against the attested PostgreSQL
snapshot identity, and then restores from that regular file with four bounded
`pg_restore` jobs bound to the isolated `loom_rehearsal` database role.
The transfer has the same 600-second bound as source dump creation, while
`pg_restore` retains its independent 1,470-second bound; transfer, digest
verification, restore and cleanup share one combined 40-minute helper budget.
A transfer timeout, another transfer failure, and a non-zero restore command
remain distinct secret-safe blocker codes; raw command output is never stored
in rehearsal evidence. The staged dump is removed before the database proof can pass. This
avoids the single-stream restore bottleneck without weakening checkpoint
identity or keeping rehearsal payloads after verification. Every subsequent
database identity and migration verification query uses that same explicit
isolated role; no libpq operating-system-user fallback is accepted.

`RehearsalActionSource` is the sole plan and resource-name authority for these
actions. Its identity factory and action factory consume the same checkpoint,
candidate tree, image artifact set, migration plan and manifest artifact,
browser report schema and route origin. The represented username, team UUID,
admin audit actor, smoke
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

The installed `FinalSmokeExecutor` drives those shared predicates through one
transport whose fixed base URL must equal the attested canonical staging route.
It reopens the admin token with the same no-follow metadata and redacted-content
authority, recovers only the exact deterministic request/attempt batch, and
records bounded response hashes rather than bodies. A recovered exact batch is
polled instead of resubmitted; fanout incompatibility and non-success terminal
state remain fail-closed. Rehearsal and final smoke obtain the represented
identity, task, worker pool, agent, and audit actor from one shared authority
factory; final smoke is only the canonical protected-route terminal proof.

`browser_report_contract` is likewise the sole sanitized browser-report
predicate for rehearsal and final acceptance. Both modes require the complete
schema-v4 check set, exact route, candidate identity, represented user, audit
event, Chromium identity, and logout proof; they differ only in the typed
rollout-envelope or rehearsal-plan binding. The attested report-schema digest
is computed from that same contract, so adding or weakening a consumed field
cannot leave an unchanged digest behind. The final plan also carries the
SHA-256 of the exact immutable driver-envelope bytes; the installed helper
reopens that sibling envelope with the private-file authority reader and
revalidates request, attempt, candidate, config and attestation identities
before a live browser session may cite the digest.

The installed `FinalBrowserExecutor` consumes that same report predicate and
the exact final-plan browser image digest. It reopens the admin token through
the no-follow credential authority, checks both attested metadata and the
root-configured redacted content fingerprint, and writes only into the private
request-attempt evidence directory. A complete existing report is reusable;
an existing partial or drifted report is terminal evidence and forbids another
browser session instead of silently repeating a protected mutation. This
executor is dispatched only by the fixed installed final helper after the
complete protected component chain and post-apply drift check succeed.

The Tier 3 API probe runs from the exact `loom-service` image with a fixed
module invocation. It reads a dedicated root-owned mode-`0440` regular-file
projection of the cloned admin secret with `O_NOFOLLOW`, stable metadata and a
bounded TOML parser; no token, response body, or token fingerprint is emitted.
It records only bounded response hashes and the non-secret batch identity, then
requires an exact immediate API readback from the cloned database. A queued
batch is valid rehearsal evidence, but it is never reported as terminal worker
success.

The candidate web image accepts a nested route only when
`LOOM_FRONTEND_REHEARSAL_ID` is present, the environment is exactly `staging`,
and both frontend route fields equal `/dev/rehearsal/<24-hex-id>`. Normal
staging and production route acceptance remains unchanged. The runtime loader
detects this exact nested route before the general `/dev` prefix, so it fetches
the isolated web pod's runtime config and keeps API calls inside the rehearsal
namespace. Invalid rehearsal identifiers fall back to the normal `/dev`
contract and cannot claim isolated authority. Secret cloning also
derives one in-namespace `admin-token` key from the validated singleton admin
TOML for the browser Job; the raw value stays inside the sensitive apply
artifact and is never written to the rehearsal plan or evidence.

The isolated release includes the exact candidate `loom-llm-gateway`
deployment and service as well as the web, service, and control-plane
workloads. This keeps the authenticated rate-card browser check inside the
rehearsal namespace and cloned database instead of crossing into protected
staging or failing against an absent upstream. The gateway image makes its
editable source and migration tree read-only accessible to the rehearsal's
fixed non-root UID; sealed checkout file modes therefore cannot make the
candidate start only as root.

Browser report schema v4 has two mutually exclusive bindings. Protected final
acceptance retains the broker request/attempt/envelope binding; Tier 3 instead
binds the rehearsal plan digest, isolation ID, and resolved candidate SHA. The
script rejects mixed or partial modes, and only isolated rehearsal may emit the
complete sanitized report on stdout for capture from a terminal Kubernetes
Job. Final rollout continues to publish its private report file only.

The browser rehearsal publishes one exact-host Ingress under
`/dev/rehearsal/<isolation-id>` and rewrites only that prefix to the isolated
web/API Services. A repository-owned, resource-name-scoped Role may read the
`ingress-nginx-controller` ClusterIP; the browser Job pins `yylx.world` to that
single address and a dedicated NetworkPolicy permits only TCP 443 to it. The
controller may enter the isolated namespace, but no rehearsal principal may
read or mutate another namespace. The Job uses the exact preflight-built image,
no service-account token, restricted pod security, a read-only root filesystem,
bounded memory/CPU/tmpfs and one terminal execution. An init container copies
the cloned `admin-token` Secret projection into a same-UID mode-0600 no-follow
file; neither manifest nor evidence contains the token. Exact Job/Pod image IDs,
Ingress and NetworkPolicy specs, successful init/main termination, every schema
v4 browser predicate, logout cleanup and rehearsal binding must all read back
before the check can pass. Namespace deletion remains the only cleanup path.

The isolated `loom-web` pod keeps its image root filesystem read-only while
honouring the image's runtime configuration contract. One fixed non-root init
container from that same exact candidate image copies only the baked webroot
into a size-limited request-local `emptyDir`; the main container mounts that
volume at `/usr/share/nginx/html`. Runtime `index.html` and configuration writes
therefore cannot widen filesystem authority or escape the rehearsal namespace.
An input manifest that already contains an init container is still rejected,
and no mutable host or persistent volume is admitted.

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
Transient systemd units receive a bounded five-second garbage-collection
window after the exact stop/reset sequence; any other load state or expiry
fails closed before namespace deletion. Cleanup journaling retains the precise
resource blocker and records incomplete verification under a separate key, so
the generic evidence guard cannot erase the actionable cause.
If `stop` garbage-collects the transient unit before `reset-failed` acquires
it, the reset error is accepted only after a separate exact load-state readback
proves `not-found`; loaded or unavailable state still fails closed.
Namespace absence is likewise read through `kubectl get
--ignore-not-found=true`: only a successful empty response proves absence.
After the UID/resourceVersion-preconditioned delete, cleanup polls that same
authoritative readback for at most 300 seconds instead of delegating the wait to
`kubectl wait`. Every still-present response must retain the exact rehearsal
identity and original namespace UID; name reuse or identity drift fails closed.
A successful empty response completes cleanup immediately, a still-present
namespace at the absolute deadline is a timeout, and transport or parse failure
remains unavailable.

The restored database snapshot necessarily contains frozen worker heartbeat
timestamps. Before the isolated API admission probe, the rehearsal inserts or
refreshes one deterministic, plan-bound Docker worker row in the cloned
database only. Exact id, hostname, candidate version, pool and capabilities
must match on retry. GB10 host readiness remains a separate prerequisite; the
synthetic row never registers with or grants authority over protected staging.
The admission readback accepts the service's persisted `submitted` state as
well as later pending/running/finished states; terminal success remains a
separate final smoke predicate.
The browser rehearsal binds its restricted egress and host alias to the exact
IPv4 ClusterIP of the fixed ingress-nginx controller Service. Both ClusterIP
and NodePort exposure are accepted because NodePort retains that same in-cluster
service identity; LoadBalancer, ExternalName and identity drift fail closed.
The browser pod resolves the fixed Service ClusterIP, while its egress policy
selects the exact ingress-nginx controller namespace and pod identity. This
keeps the allowlist valid after Service DNAT without opening arbitrary cluster
or Internet egress.
Ingress regex paths remain absolute Kubernetes paths (the ingress controller
adds regex anchoring), and the 64-byte rehearsal plan digest is carried in a
pod annotation rather than an overlong label. Candidate and workload selector
labels remain bounded and exact.
The installed browser helper budget covers both registry publication and the
900-second Job completion wait. The outer helper therefore cannot expire
before the inner wait publishes either success or a normalized terminal
browser blocker; the acceptance wait itself is not weakened.
An HTTP rejection from the admission probe is normalized to an allowlisted
request identity and reason code. The terminal blocker retains those values
and the response SHA-256, but never the response body, token or free-form server
detail. Non-HTTP failures likewise retain only an exact allowlisted request
identity and fixed reason such as transport unavailable, invalid response, or
contract drift; malformed or unapproved probe output falls back to the generic
fail-closed blocker.

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
candidate-bound live Slurm capacity acceptance, and bounded final
live-route smoke/browser acceptance remain. Protected apply, capacity, live
smoke, and authenticated browser acceptance are the four checks classified as
protected staging mutation. Capacity submits real allocations through the
fixed Slurm authority; smoke submits bounded live work; browser acceptance
creates and revokes its bounded admin session and writes the corresponding
audit event. None may be represented as a read-only verify operation. Earlier
tiers do not weaken these final gates; they ensure the final gate is not the
first consumer of a token, host prerequisite, migration, image, task, or
browser contract.

Immediately before the protected chain, the service worker reloads the
digest-addressed attestation and rebuilds the shared Tier 0 registry from the
installed authority. It reruns only drift-sensitive checks and requires exact
candidate/tree, mutation epoch, runner install/config, credential metadata,
GB10 inventory/boot IDs, and shared-mount evidence. A missing, expired, or
drifted value terminates the pending attempt before its active pointer becomes
`running` and before any final action is called.

Tier 4 itself uses the same registered checks as the coverage manifest. Each
normalized `CheckExecution` is published once beneath the immutable
request/attempt directory. The journal is private, no-follow, single-link and
no-replace. A worker restart may reuse only a passed, unexpired execution with
the exact input fingerprint, implementation digest, stage and operation. This
means a published successful `final.protected-apply` is never repeated after a
worker crash. An incomplete or failed outer execution is not resume authority
by itself. The one recoverable crash window is an exact service-owned final
plan whose component journal contains the matching mutation-epoch intent and
terminal at `starting_epoch + 1`. A resumed attempt must bind that plan to the
same request candidate and attestation, pass post-apply admission against the
advanced live epoch, and rerun the protected action through a new component
journal. Each component is then reclassified: exact effects receive terminal
evidence without repeating mutation, while any partial, ambiguous, or drifted
state still fails closed.

An attested worker may not use the 18-step compatibility driver. The production
worker composition always injects the seven-check `FinalGateRunner`;
an unavailable composition fails with
`final.protected-apply.runner-unavailable@final-only`, while a missing or
invalid installed helper fails its fixed command boundary without invoking a
protected action. These fail-closed transitions prevent an installation without
the complete seven-check composition from rebuilding images, rerendering
manifests, or rediscovering an early predicate after backup.

The final DAG order is exact:
`final.protected-apply`, `final.convergence`, `final.drift`,
`final.capacity`, `final.smoke`, `final.browser`, then `final.summary`.
`final.capacity` is the added fourth member of the protected-staging mutation
set and occupies the fourth position in this chain, after the
apply/convergence/drift boundary and immediately before smoke. No action map
may omit, reorder, or parallelize this chain.

Before those actions can run, `FinalGatePlan` joins the driver envelope, the
digest-addressed attestation, and the build-once preflight artifact publication
into one strict canonical record. It binds request/attempt, candidate/tree,
starting epoch, image digests, rendered manifest, migration plan, browser
schema, production-defaults artifact, restore-verified backup lease and snapshot,
environment/route, runner
source/install/config, secret metadata fingerprints, GB10 probe identities,
and every attested implementation/evidence digest. The service publishes this
record once as a mode-`0600`, single-link, no-follow file beneath the existing
request/attempt directory. A conflicting publisher, changed input, unexpected
field, content change, path traversal, owner/mode/link drift, or digest mismatch
fails closed. Installed actions receive only this fixed plan path and digest;
they do not resolve Git refs, reread ambient configuration, or rediscover
preflight artifacts.

Final admission rebuilds the same registered-check implementations and reruns
only the drift-sensitive Tier 0 checks plus the six Tier 2 readonly staging
baseline checks. It does not rebuild images or rerender Tier 1 artifacts. Every
Tier 2 execution must retain the attested mutation epoch, readonly principal,
empty blocker set, implementation digest, resource evidence, and exact
evidence hash. A health, auth, catalog/task, storage/database, network, or
aggregate release-baseline change after rehearsal therefore stops before the
first protected component rather than being rediscovered after apply.
The worker passes that exact `FinalAttestationAdmission` object into the final
runner; a final runner without admission evidence fails before the attempt is
marked running. `FinalGateActionSource` derives the protected baseline embedded
in `FinalGatePlan` from these freshly rechecked Tier 2 executions, not from the
older rehearsal record. The final runner also requires the admission's full
attestation to equal the digest-addressed attestation it reloads.

`FinalGateActionSource` obtains the artifact bundle identity only from the
immutable passing `artifacts.publish` execution in the request's Tier 0–3
rehearsal. Its implementation and evidence hashes must match the attestation,
and every image/manifest/migration/production-defaults artifact digest must
match the digest-addressed publication.
Only then does it publish the plan and construct the complete seven-action map
around the fixed installed helper. Directory scanning, newest-artifact
selection, ambient argv and partial action maps are not accepted.

The installed final helper runs with the protected staging kubeconfig, not the
rehearsal credential. Before dispatching any action it reloads the strict plan,
the exact digest-addressed artifact publication, and the restore-verified
checkpoint named by the backup lease's source request. Candidate, manifest,
snapshot, epoch, schema, object-inventory or path drift fails before the
executor is entered. A successful protected apply must report exactly one
mutation-epoch advance and protected-mutation evidence. The later live smoke
and browser acceptance also report protected-mutation evidence, but execute
inside that already claimed rollout epoch and must observe the same single
epoch advance rather than incrementing it again. Convergence, drift and summary
remain read-only. Before dispatch, the installed executor re-verifies the
root-issued runner install attestation, live fixed asset digests, config hash,
source identity, and sealed base.

Protected apply is further divided into an ordered component journal beneath
the request attempt. Each component publishes an immutable intent before
observing or changing live state. The shared classifier must return exactly
`ready`, `exact`, or `drifted`: `ready` proves the live precondition still
matches its attested baseline and may call apply, while `drifted` stops
the chain, and terminal evidence is published only after an `exact` readback.
After a crash, an existing intent is classified before any retry; an exact
effect is recorded without repetition, a still-ready precondition may be applied, and a
partial or ambiguous effect fails closed. Reused terminal records are also
reclassified and must retain the same evidence digest and epoch. The chain is
serialized by a private attempt-scoped lock and neither the plan nor a
component implementation/fingerprint may change underneath an intent. A
classifier exception before apply, after apply, or while revalidating a terminal
publishes one immutable coded, secret-safe diagnostic beside the intent before
the original exception propagates; exception messages and response bodies are
never recorded.

The protected environment-state component treats only bounded HTTP transport
failures and readiness responses (`408`, `425`, `429`, `500`, `502`, `503`, or
`504`) as transient. It makes at most 13 observations or idempotent PUT
attempts with five seconds between attempts. The mutation epoch is rechecked
before every observation and again immediately before each PUT sequence. After
a lost mutation response, the next attempt first re-observes desired state and
returns without another PUT when this candidate is already exact. Epoch drift,
credential or authority failures, malformed successful responses, and local
runner-prerequisite failures remain immediately fail-closed.

The final plan also carries one canonical protected baseline extracted from
exactly the six passing Tier 2 executions. It binds their common readonly
principal, mutation epoch, resource digests, implementation digests and
attested evidence hashes. Missing, duplicate, failed, non-readonly, mixed
principal, epoch-drifted or unattested baseline evidence cannot create a final
plan. Component classifiers use these exact resource digests as their
pre-apply authority instead of treating any arbitrary live difference as safe
to overwrite.

For a database that already has lifecycle authority, the first protected
component is a single PostgreSQL compare-and-swap epoch claim. Its fixed query
updates the staging/`loom-staging` row and appends the matching epoch event in
one statement, bound to the request ID and final-plan digest. Classification
returns `ready` only at the attested starting epoch, `exact` only for the next
epoch owned by that same request and plan, and `drifted` for every concurrent
or malformed state. A crash after the transaction is therefore recovered by
readback, never by a second increment.

Schema bootstrap for a database below revision 0069 is explicit rather than
treating a missing epoch row as current authority. Only a plan attesting epoch
zero, a baseline schema below 0069, and a rehearsed target at or above 0069 may
bootstrap the staging row. The exact migration component must converge first;
then one SQL statement inserts the bootstrap row if absent, claims epoch one,
and appends its event. Missing authority for any already-0069 database is
drift, and a generic migration never seeds a staging row into another
environment's database.

The migration/epoch composition has one typed command runner and the same
attempt journal in both orderings. Existing lifecycle authority claims the
next epoch before the migration Job; the pre-0069 bootstrap path migrates first
and then bootstraps and claims epoch one. Commands are argv-only `kubectl`
invocations under a fixed non-inheriting kubeconfig environment, with bounded
input/output and redacted failures. Re-entering the composition reclassifies
terminal records and cannot repeat either the epoch CAS or the migration. The
installed final helper composes this boundary with the manifest, GB10,
production-defaults, convergence, smoke, browser, and summary executors; a
partial action map is not rollout authority.

The next component consumes the exact Tier 1 `rendered.yaml` publication and
never invokes the renderer or image builder. It first requires the journaled
epoch component to classify as exact for this request and plan, then runs a
bounded server-side `kubectl diff`. Exit zero is exact; exit one is ready to
converge under the claimed epoch; every command error is fail-closed. Apply uses
server-side apply with the fixed `loom-staging-rollout` field manager, strict
validation, and no `--force-conflicts`, then must reclassify to an empty diff.
Diff output is discarded so a Secret difference cannot enter logs or evidence.
The component binds the fresh protected-baseline digest, candidate/tree,
rendered-manifest digest, namespace, and starting epoch in its immutable intent.

`final.convergence` then reuses those same migration, epoch and manifest
classifiers in read-only mode. It requires the target schema, the exact
request-owned single epoch advance and an empty server-side manifest diff. It
does not call any component apply method, rerender a manifest or rebuild an
image; drift is returned as bounded per-component blocker evidence.

The post-apply drift check never tries to compare the intentionally changed
live baseline with its pre-apply evidence. It requires the epoch claimed by
the exact request, then reruns the same final-admission implementations for the
epoch-independent candidate, runner install, credential metadata and GB10
mount/boot identities. Those records must remain unexpired and retain their
attested implementation digests; image builds, Tier 1 artifacts, the changed
live baseline and backup eligibility are not repeated after mutation.
The service keeps the exact process-local preflight plan in final admission and
routes `final.drift` directly to that shared validator after convergence; the
installed generic helper cannot substitute a second implementation.

`final.capacity` then sends only the typed `accept_capacity` request through
the forced SSH broker to the root-owned installed GB10 acceptance authority on
`gx10-01c7`. The authority is fixed to cluster `trt-gb10`, partition/account/QoS
`loom-staging`, service identity `loom-rollout` UID 995/GID 2007, and nodes
`trt-gb10-1` through `trt-gb10-15`; node 16 is never eligible. The broker's
sanitized `PATH` is authoritative, while the authority names system
executables by absolute path. Capacity never fetches, builds, publishes, or
imports candidate runtime code.

Before any trusted registry probe or CA snapshot, the controller and every
allocation run the same bounded no-follow verifier over the complete
image-tagged worker checkout and mode-`0600` env file. It rejects Git common
directory, worktree config, graft, alternate-object and index/tree
redirection; ignored or untracked physical entries; and content, mode,
owner/group, link, or replacement drift. The env parser accepts only unique
shell-style keys, requires the complete candidate/pool/concurrency/service
contract and non-empty secrets without recording their values, and binds the
full-file SHA-256. Checkout/index/tree hashes plus root, target, `.git`, and env
device/inode identities are carried into each allocation and must match before
the trusted snapshot is used.

The allocation probe prints a fixed start marker before doing node work. A
busy deferral is valid only when that marker is absent, the node readback is
actually allocated or mixed, Slurm reports the fixed busy condition, and one
structured job-specific accounting row proves the named allocation remained
unstarted. A started probe that later emits a busy phrase is a capacity
failure. Each request runs in a nonce-named transient service in
`system.slice`, with `KillMode=control-group`, a bounded runtime, and a
root-owned volatile job-state handoff. The same nonce binds every probe job
name; the authority persists the exact Slurm ID before waiting, and both the
authority and broker perform interruption-immune exact cancellation plus empty
job-specific scheduler readback. Scheduler disappearance requires two
consecutive empty exact-name observations, while recursive cgroup emptiness is
proved by the unit's unique `cgroup.events` `populated 0` row. A later locked
request removes only verified root-owned atomic-state temporaries and
reconciles any strict nonce-bound stale unit/state before launching. Normal,
timeout, and handled-signal paths apply bounded TERM then KILL as needed and
prove the request cgroup and nonce-bound Slurm job empty before returning. An
uncatchable broker death cannot itself perform this cleanup: systemd
`RuntimeMaxSec` bounds the transient unit, the next locked request reconciles
its stale unit/state, and the per-probe Slurm time limit is the terminal
backstop.

Canonical acceptance evidence lasts at most 30 minutes. Every non-durable
dependent must recheck it against a live clock when that consumer starts, and
`final.summary` rechecks every predecessor at seal time, so evidence that
expires before smoke or summary cannot authorize either action. The authority
first publishes canonical JSON at
`/var/lib/loom-gb10-slurm-authority/evidence/<canonical-evidence-sha256>.json`
with no-replace,
idempotent collision checks and file/directory fsyncs, then atomically replaces
`current.json` and fsyncs `STATE_ROOT`. The local `final.capacity` execution
stores that same canonical evidence digest, making the immutable authority path
derivable without trusting the mutable pointer.

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
TTL expires. Tier 1 build artifacts and Tier 3 isolated rehearsal define that
minimum because they are not repeated. Tier 0 and Tier 2 observations may age
while the detached checkpoint and rehearsal are still running, but they remain
only exact evidence-hash references: final admission reruns those checks and
requires the current evidence and implementation identities to match before
protected mutation. Candidate, config, token metadata, epoch, host boot ID,
mount, or implementation drift therefore still invalidates the attestation.

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
read-only daemon, buildx, and host inotify-instance-capacity predicates; both the
compatibility preflight report and Tier 0 `docker.runtime` consume its complete
aggregate. A failing daemon probe does not suppress the buildx or fixed
`/usr/sbin/sysctl` probe,
and no command's raw stdout or stderr is admitted to evidence. The exact host
installer owns `/etc/sysctl.d/90-loom-staging-rollout.conf`, converges
`fs.inotify.max_user_instances=1024`, and binds that asset into the root-issued
install attestation. This prevents Docker container-start queueing from first
appearing after image construction when Kubernetes already consumes the host's
small default inotify instance budget.

`probe_kubernetes_client` likewise owns the fixed kubeconfig, current-context,
and target-namespace predicate. It always probes both context and namespace,
emits only the allowlisted context/namespace plus booleans and a digest, and is
bound by the DAG to the kubeconfig metadata digest. The compatibility report
uses this implementation instead of maintaining a second context policy.

Systemd readiness is single-source as well. The broker-facing Tier 0 probe and
the final GB10 convergence step consume the same user-manager, oneshot service,
and timer-state classifiers. The read-only probe binds the manager version,
`Linger=yes`, host boot ID, and a bounded user-manager RPC latency. Actual
`systemd-run` activation is not disguised as a read. Tier 0
`lifecycle.launch-cancel` creates one deterministic isolated unit with the
same `systemd-run --user --collect --service-type=exec`, UMask, and working
directory builder used by backup and rollout units. It runs a fixed sleep,
verifies the transient PID, stops and resets the exact unit, and requires a
final `not-found` readback inside the short RPC budget before any request or
backup can exist. Only hashed evidence is retained. The checked-in
`rehearsal.systemd-launch` predicate belongs to the isolated rehearsal tier,
where a request-specific unit and cleanup journal prove launch/cancel latency
without mutating protected staging. The action also executes that exact
isolated unit contract across the fixed GB10 inventory after SSH, mount,
candidate-source and host-readiness checks have passed, then proves every
remote unit absent before the step can succeed.

Backup and launch admission use the shared `lifecycle_protocol` transition
table rather than step-local status strings. A request may publish launch only
after backup verification; pending/running backup may instead enter the
cancel-requested path and must seal as failed before any new admission. Tier 0
`lifecycle.launch-cancel` exercises success, pre-start cancellation,
in-flight cancellation, backup failure, and forbidden early-launch sequences
against that same implementation. The probe does not create a request, backup,
or protected-staging mutation. Its one transient self-test unit is an explicitly
isolated Tier 0 mutation and must be absent before the check can pass.

The GB10 Tier 0 path first runs `gb10.ssh-topology` with the exact SSH-config
digest, no-follow identity metadata, strict known-hosts policy, and BatchMode.
It probes every declared host concurrently and reports the full reachable and
failed sets without admitting remote diagnostics. Shared-mount identity is a
separate dependent predicate, so connectivity cannot mask a mount blocker.

The subsequent host check runs one fixed read-only probe per declared host
with bounded concurrency. Nested GB10 fleet probes use four workers each, so
the dependency DAG cannot exceed eight concurrent SSH operations when mount,
host-readiness, and candidate-source checks overlap. This avoids turning the
shared NFS checkout probe into a load-induced false blocker while retaining a
complete 15-host result inside the Tier 0 budget. It validates the manager
version, linger, boot ID,
completed oneshot result, enabled timer, and only the documented
`active/running` to `active/waiting` transient transition. The exact
loaded/enabled `active/elapsed` state is also admitted as
protected-repairable when manager, linger, service result, target unit, and
daemon-reload evidence all remain exact. This does not make arbitrary timer
drift healthy: protected GB10 prep may remove only the recognized byte-exact
`deploy-window.conf`, must reject any unknown effective drop-in, start the oneshot,
restart the candidate timer, and verify `active/waiting`. This narrow bridge
avoids a preflight deadlock after a host reboot while keeping the mutation
inside the existing protected component. All hosts are
reported in one result; the attestation retains per-host boot IDs and evidence
digests, while raw SSH output and service diagnostics are never published.

The root installer prepares the exact image-tagged shared candidate checkout
before it invokes the requestless Tier 0–2 assessment. Preparation runs as the
fixed `loom-rollout` service account and calls the same no-replace,
inode-bound materializer later consumed by environment-state; it creates only
the immutable checkout beneath the fixed `/shared_work2` authority. It never
installs, starts, enables, or restarts a GB10 unit. Installer `check` and Tier 0
then use the same read-only verifier, so the candidate-source gate cannot be
made circular by asking a rollout mutation step to satisfy its own admission
predicate. A newly published NFS checkout receives at most six exact probe
observations with capped exponential backoff (60 seconds total sleep). This
absorbs only the bounded cross-host visibility window; divergent content or an
unsettled checkout still fails inside the 180-second check budget.

The coverage manifest maps every broker predicate and rollout step to the
shared check implementations. Its coverage test rejects any unmapped predicate
or rollout step.

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
