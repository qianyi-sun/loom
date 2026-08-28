# Protected Staging Rollout

The installed staging rollout authority lets approved OS users run the same
service-owned deployment workflow without sharing a personal checkout,
identity, or long-lived command line. The root-owned client sends a fixed
request to the broker; the broker validates the caller, selects a freshly
fetched allowed branch head, creates a protected backup, and launches a
detached systemd unit.

## Command surface

Every command selects an installed authority explicitly:

```text
loom-staging-rollout --env {dev,staging,prod} start [--dry-run]
loom-staging-rollout --env ENV preflight
loom-staging-rollout --env ENV status [REQUEST_ID]
loom-staging-rollout --env ENV logs [--follow] REQUEST_ID
loom-staging-rollout --env ENV resume REQUEST_ID
loom-staging-rollout --env ENV cancel --reason REASON REQUEST_ID
loom-staging-rollout --env ENV cleanup-incomplete-backup REQUEST_ID
loom-staging-rollout --env ENV manifest-ownership inventory --artifact-bundle-sha256 DIGEST
loom-staging-rollout --env ENV manifest-ownership apply --artifact-bundle-sha256 DIGEST --request-id REQUEST_ID --approved-inventory-sha256 SHA256
loom-staging-rollout --env ENV lifecycle-capacity inventory --artifact-bundle-sha256 DIGEST
loom-staging-rollout --env ENV lifecycle-capacity apply --artifact-bundle-sha256 DIGEST --approved-plan-sha256 SHA256
loom-staging-rollout --env ENV backup-recovery {inventory,apply} ...
loom-staging-rollout --env ENV backup-retention {inventory,apply} ...
```

`start` accepts no ref, SHA, tag, image, checkout, environment file, token, or
arbitrary passthrough argument. The installed policy selects the remote and
branch, and the broker binds the request to the freshly fetched head. Use
`--dry-run` to exercise admission and candidate resolution without staging
cluster, backup, or lifecycle-data mutation. It does not acquire a mutation
guard, suspend the lifecycle CronJob, take the advisory lock, or launch a
backup or rollout worker. It does publish the request-bound preliminary
assessment and preview events in the service-owned ledger, so its immutable
evidence can be inspected without being mistaken for a launch authority.
Passing `preflight`, staged preview, and backup-pending responses include
`preflight_artifact_bundle_sha256`. Use that secret-free evidence value as
`DIGEST`; maintenance reopens only that immutable publication and revalidates
its candidate, tree, mutation epoch, rendered content, images, and approved
inventory or plan. It never discovers a publication by listing the artifact
store.

## Authority and persistence

The installation owns its checkout, configuration, kubeconfig, credential
sources, runtime directory, and state directory. Ordinary operators can use
the client but cannot edit those inputs. Caller identity comes from the OS
credential boundary, not from an argument or environment variable.

For an installation using the `sealed-cumulative` source mode, the coordinator
surface is limited to the authenticated OS users `qianyi`, `hongjian`, and
`devansh`. This covers preflight, start, resume, and the protected maintenance
operations; operator-group membership alone does not grant that authority.
Candidate binding, maintenance, approval-digest, and lifecycle checks still
apply independently.

Request and attempt envelopes are immutable. The service-owned ledger records
the request ID, authenticated caller, candidate identity, backup binding,
systemd unit, rollout ID, attempt number, state transitions, cancellation, and
sanitized summary. Atomic file publication and directory syncing protect the
ledger against partial writes; host root can still alter host state and is
outside this audit boundary.

Only one request may own an environment's full rollout lifecycle at a time.
The lifecycle lock is separate from the shorter mutation locks acquired by
individual rollout steps.

## Lifecycle mutation guard

After Tier 0--2 preflight has bound the candidate and staging mutation epoch,
a non-preview `start` acquires a request-bound mutation guard before publishing
the backup job. The service-owned transient guard unit first suspends the
legacy `loom-staging-data-lifecycle` CronJob with a resource-version-checked
patch and annotations carrying the request ID, candidate SHA, and candidate
tree. It verifies the exact CronJob identity, lists nonterminal Jobs by the
CronJob's exact controller UID, validates every Job's label and sole controller
owner, and requires two consecutive empty inventories. After acquiring the
database lock, it repeats that stable-empty inventory before publishing
readiness. The fixed PostgreSQL staging-mutation advisory lock lives on a
dedicated autocommit session; the guard binds its backend PID and continuously
proves that same backend still owns the exact lock. Private ready evidence is
bound to the request ID, candidate SHA/tree, epoch, guard PID, database backend
PID, entry-anchored absolute deadline, CronJob UID, and suspension resource
version.

The broker rechecks that evidence and the epoch before it publishes the
request-bound detached backup job. The backup worker independently rechecks
the same complete binding before backup work, then hands ownership to the
detached rollout attempt only after it has launched that attempt. The attempt
again requires ready evidence and retains the guard through protected apply,
final gates, and every terminal path. A launch or backup failure before
handoff releases it; the attempt releases it on completion, failure,
cancellation, or an exception. The request-bound backup and attempt units use
`After=` for the exact guard unit, while the guard independently checks that an
exact backup or attempt owner, including one still deactivating, remains
present. They deliberately do not use `BindsTo=`: a worker synchronously stops
the guard during normal release and must remain alive to publish its terminal
event and clear the active pointer after that verified stop returns. Normal
release restores the exact annotated CronJob to unsuspended state while
removing the annotations, confirms that restoration, unlocks PostgreSQL, and
publishes released evidence. The guard's fixed request-bound `ExecStopPost`
then treats exact released evidence as a strict no-op. If evidence is ready,
missing, or unsafe instead, it validates the complete exact owner inventory and
uses `systemctl --user kill --kill-whom=all --signal=SIGKILL` on each live
request-bound backup or attempt unit. It never uses a wildcard in a kill call
or recursively stops an owner. Loss of the ready database session, backend, or
advisory lock is irreversible for that guard process: it leaves the CronJob
suspended and retains ready evidence while the stop fence terminates unsafe
owners for reconciliation. Binding, owner-inventory, or restoration drift
fails closed rather than resuming a different CronJob or candidate.

The guard CLI retains a 120-second ceiling for each fixed Kubernetes command
and caps every fixed systemd owner inventory or kill at 30 seconds. If a stop
arrives just after a false stop check, reaction can include one 30-second owner
inventory, the one-second poll sleep, and the next 15-second lock-health query.
Including CronJob restoration, advisory unlock, database-tunnel teardown, and
the evidence-publication margin makes the complete normal-release bound 342
seconds, so the guard emits `TimeoutStopSec=343s`. An unsafe stop fence can
issue at most one inventory and two exact owner kills, for a 90-second command
bound. Broker and worker systemd operations use a 434-second client ceiling,
strictly above the 343-second service stop plus that stop-post fence.

The transient guard has a finite `RuntimeMaxSec` of 30 hours, derived from the
bounded backup and final-gate budgets, readiness allowance, and operational
margin. It uses `Restart=no`; one absolute lifetime begins at guard-unit entry,
and its internal deadline expires five minutes before systemd's runtime bound.
It is never an indefinite freeze. A root-owned persistent systemd
timer invokes reconciliation every minute. Reconciliation runs as the rollout
service user and restores only an exact suspended, fully annotated guard whose
request unit is absent and whose candidate, CronJob UID, and (when present)
private evidence remain authoritative. Before restoration it retries the same
exact hard fence, then requires two fresh complete owner inventories separated
by a poll to show no live or deactivating owner. A successful `systemctl kill`
only proves signal dispatch and never authorizes restoration. Evidence must
also remain unchanged across the fence and absence proof. Any list, kill,
recheck, sleep, or evidence uncertainty keeps the annotated CronJob suspended
for the next timer run. Exact released evidence with surviving annotations is
contradictory—normal release already removes them—and also fails closed. An
active verified unit is left alone; an unannotated suspension, incomplete
annotation, unsafe evidence, or identity drift fails closed after fencing when
an exact request can be derived. The stop fence covers main-process failure,
hard death, and finite-runtime termination, while reconciliation makes that
fence durably retryable before restoration. Together these mechanisms cover
crashes, restarts, reboots, and expiry without treating arbitrary CronJob state
as safe to change.

The reconciliation oneshot has a conservative 571-second bound: three
Kubernetes commands at 120 seconds, two candidate-identity commands at 15
seconds, six systemd commands at 30 seconds, and the one-second stable-absence
poll. Its `TimeoutStartSec=12min` supplies more than one minute of margin over
that complete fail-closed sequence.

## Lifecycle

1. `preflight` validates the installed authority and its protected inputs.
2. `start` authenticates the caller, fetches the configured remote, validates
   the exact candidate, creates and verifies a request-bound backup, and starts
   the detached attempt.
3. `status` and `logs` expose redacted progress without requiring access to
   secrets or the service checkout.
4. `cancel` records the operator and reason, terminates the active attempt, and
   preserves resumable state.
5. `resume` reloads the immutable original preflight request, reacquires the
   guard, and refuses candidate SHA/tree, guard epoch, or live epoch drift from
   the original epoch before creating a new attributed attempt. Any prelaunch
   refusal releases the newly acquired guard.

An incomplete backup can be removed only through the request-bound cleanup
command. A manifest- and restore-verified candidate whose detached job reached
launch publication before rotation promotion is recovered through the
digest-approved `backup-recovery` inventory/apply protocol. Recovery
cross-checks the immutable job, lease, attestation, request, and Attempt before
promoting the candidate; it never deletes either payload. The superseded
active payload is queued for the separate `backup-retention` inventory/apply
protocol.

Backup recovery and retention are available to both authenticated
`sealed-cumulative` and exact installer-pinned `merged-dev` runners. Their
authority comes from coordinator identity, the staging/`loom-staging` binding,
the lifecycle launch lock and maintenance-idle proof, and an exact approved
plan digest—not from the source-mode label. This is intentionally narrower
than manifest ownership or lifecycle-capacity maintenance: rotation
maintenance consumes only canonical persisted rotation, job, lease, receipt,
and backup-payload evidence and cannot select source-derived cluster mutation
targets. The service constructors, broker admission, and production composition
share one policy predicate so a valid rotation cannot become unmaintainable
during a sealed-to-merged installation migration.

Every host install or upgrade fails closed on durable nonterminal
preflight-backup state, including a fresh reinstall after uninstall: uninstall
retains both `/var/lib/loom/staging-rollout` and the root receipt directory.
Immediately after invocation-checkout authentication, before install-source
preparation or the install transaction's root-directory convergence, any
lstat-visible entry at either retained namespace must be an actual directory;
a file or symlink (including a dangling symlink) refuses installation. A safe
directory entry selects the later maintenance/activity fence rather than
authorizing it. Because uninstall also removes tmpfiles configuration and
`/run/loom-staging-rollout`, that fence authenticates the unchanged canonical
service account and creates only the ephemeral directory with its exact UID/GID
and mode `0700`; it does not depend on removed tmpfiles authority. The fence
then proves service state, marker, and unit authority before install-record,
account, service-directory, candidate, runtime, or admission convergence.
Receipt-root and receipt-file semantics are validated only when an active
durable request can consume that receipt; an unreferenced receipt namespace is
not eagerly interpreted, and unsafe receipt metadata never authorizes a
receipt. Receipt authority is rejected when its candidate matches either the
installed source or the exact prospective source. A current-candidate request
remains owned by normal recovery or `resume`. A superseded record that is
absent from the current backup rotation is not silently ignored or edited.
Root must run the recovery command from a clean, root-owned checkout whose only
remote is the approved origin and whose HEAD is the exact current `dev` ref.
Recovery Git runs with sanitized system/global configuration and rejects replace
refs, grafts, shallow history, and local or HTTP object alternates. The installed
source must be an ancestor of the authenticated head, every recovered candidate
must be a strict ancestor, and the candidate tree recorded by the immutable job
must equal Git's exact `candidate^{tree}`. Both the installed source and current
head are forbidden recovery candidates after the checkout's rollout assets are
validated and before maintenance or receipt publication. A ready
`sealed-cumulative` installation may use this migration only when its exact
recorded commit has since become an ancestor of the authenticated current
`dev`; its sealed SHA/tree/base and linear-history proofs remain mandatory.
An unmerged sealed source refuses before maintenance. Recovery does not rewrite
the install record: after retained activity is reconciled, the normal installer
performs the separate migration to `merged-dev`.

`orphaned-backup-recovery inventory` validates the complete canonical v3
rotation, preflight-job, job-state, lease, and nested timestamp contracts.
Receipt consumption independently repeats the canonical immutable-job
validation; it never relies on the strictness of the version that produced the
receipt. The inventory includes every exact eligible record even when its
receipt is already valid, so the approved plan digest is stable across a
completed replay or a crash after publishing only some receipts. Apply enters
the normal maintenance admission freeze, proves the active pointer is absent and
no rollout, backup, or guard unit is live across two inventories, then
recomputes the exact digest-approved plan and repeats candidate-history
validation on that same plan before publishing a root-owned receipt. An idle
host decision reads the complete retained authority, inventories every owner
unit, and reads the complete retained authority again; a worker's final atomic
state publication therefore cannot race receipt consumption. Receipt consumption
carries the payload ID derived from the same immutable `job.json` bytes that
validated the receipt into the live-rotation check; it never reopens the job and
mixes identities from two reads.

Each systemd inventory has a 30-second ceiling and request enumeration stops at
10,000 entries. One watchdog starts before lifecycle-lock acquisition and
enforces the 600-second monotonic deadline across checkout authentication,
subprocesses, filesystem reads, maintenance, planning, and publication. Apply
rechecks the deadline after receipt fsync and before atomic replacement, so an
overrun cannot start another publication. Expiry interrupts normally
interruptible kernel operations and subprocesses and refuses the command. The
watchdog is main-thread only and refuses before lifecycle mutation if a foreign
`ITIMER_REAL` is active or `SIGALRM` is pending, so it never consumes another
subsystem's timer or pending signal. Otherwise it captures the main-thread
identity, installs and unblocks its `SIGALRM` handler, and starts a dedicated
sender thread. That sender waits on the monotonic deadline and then targets the
main thread with `signal.pthread_kill`; the watchdog never arms `ITIMER_REAL`.
Restoration catches an owned expiry delivered immediately before its signal-block
call and retries that block a bounded number of times. Only after independently
verifying that `SIGALRM` is blocked does it set the sender cancellation event,
bounded-join the sender, and verify that the sender stopped. While the watchdog
handler is still installed, it synchronously drains and verifies absence of any
owned pending delivery. Only that proven state may restore and verify the prior
handler and exact signal mask. If blocking cannot be proven, restoration aborts
before restoring the recorded prior handler or exact mask; the current handler is
left unchanged and the mask state remains explicitly unverified. Once blocking
is proven, failure to stop the sender or to prove handler ownership and pending
delivery leaves `SIGALRM` blocked and the current handler unchanged rather than
exposing the recorded prior handler. If exact mask restoration itself fails
after the prior handler is restored, the stopped sender and drained pending set
still prevent owned delivery. A restoration failure takes precedence over an
owned expiry, which takes precedence over any body error; without either
watchdog condition, the original body error is preserved.

Maintenance enable owns its previously empty marker slot before creation and
rolls back a partial publication even if interruption occurs as the create call
returns; recovery owns cleanup before enable begins and removes a successful
marker directly without another fallible status probe. An absent cleanup marker
is an idempotent no-op and does not wait on the launch lock. Cleanup receives the
same absolute deadline and reuses the exact service UID/GID authenticated before
maintenance enable; after expiry it performs no fresh NSS identity discovery.
Launch-lock acquisition stays interruptible before the dedicated sender delivers
`SIGALRM` and also uses nonblocking attempts bounded by the same monotonic
deadline after delivery is observed. Cleanup starts no second sender and polls
that absolute deadline instead. If the cached identity is unavailable or the
lock remains unavailable at expiry, recovery reports failure and retains the
valid marker fail-closed. After lock acquisition and marker metadata validation,
`SIGALRM` delivery is deferred only across unlink plus parent-directory fsync; a
timeout made pending there is reported immediately after that critical section.
As with every userspace watchdog, the process cannot preempt a kernel task in
uninterruptible I/O sleep; that host fault remains fail-closed in maintenance
and requires host-level diagnosis rather than being reported as a completed
recovery.

Host inactivity accepts a receipt only while its bytes remain exact, its
candidate differs from every current and prospective source at that receipt
use, and its payload remains absent from the live rotation. Unsafe metadata, a
referenced payload, concurrent drift, malformed canonical evidence, or an
unknown receipt keeps install, reinstall, upgrade, and uninstall blocked. The
receipt does not rewrite request history or claim that partial backup data was
deleted.

## Failure behavior

Admission rejects unauthorized callers, unsafe installation ownership, a dirty
service checkout, a wrong remote, stale or malformed candidate identity, an
active lifecycle owner, missing credentials, and invalid backup state before
deployment mutation. Driver failures preserve evidence and resumable state.

Recovery never substitutes a local SHA or edits the request ledger. Repair the
installed authority from trusted repository content, then use `resume` for the
existing request. A code rollback is a merged revert followed by another
normal rollout request.

The rollout runs the candidate-bound preflight, manifest application,
migration, environment-state reconciliation, external fleet preparation,
release gate, and smoke sequence implemented under `src/loom_cli/rollout/`.
Current check details are documented in
[staging rollout preflight](staging-rollout-preflight.md), and operator command
examples are in the [operator runbook](../runbooks/operator-runbook.md).
