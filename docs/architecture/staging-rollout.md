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
loom-staging-rollout --env ENV manifest-ownership {inventory,apply} ...
loom-staging-rollout --env ENV lifecycle-capacity {inventory,apply} ...
loom-staging-rollout --env ENV backup-recovery {inventory,apply} ...
loom-staging-rollout --env ENV backup-retention {inventory,apply} ...
```

`start` accepts no ref, SHA, tag, image, checkout, environment file, token, or
arbitrary passthrough argument. The installed policy selects the remote and
branch, and the broker binds the request to the freshly fetched head. Use
`--dry-run` to exercise admission and candidate resolution without mutation.

## Authority and persistence

The installation owns its checkout, configuration, kubeconfig, credential
sources, runtime directory, and state directory. Ordinary operators can use
the client but cannot edit those inputs. Caller identity comes from the OS
credential boundary, not from an argument or environment variable.

Request and attempt envelopes are immutable. The service-owned ledger records
the request ID, authenticated caller, candidate identity, backup binding,
systemd unit, rollout ID, attempt number, state transitions, cancellation, and
sanitized summary. Atomic file publication and directory syncing protect the
ledger against partial writes; host root can still alter host state and is
outside this audit boundary.

Only one request may own an environment's full rollout lifecycle at a time.
The lifecycle lock is separate from the shorter mutation locks acquired by
individual rollout steps.

## Lifecycle

1. `preflight` validates the installed authority and its protected inputs.
2. `start` authenticates the caller, fetches the configured remote, validates
   the exact candidate, creates and verifies a request-bound backup, and starts
   the detached attempt.
3. `status` and `logs` expose redacted progress without requiring access to
   secrets or the service checkout.
4. `cancel` records the operator and reason, terminates the active attempt, and
   preserves resumable state.
5. `resume` reuses the original request envelope, candidate, and backup while
   creating a new attributed attempt.

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
