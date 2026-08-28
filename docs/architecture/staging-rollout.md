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
