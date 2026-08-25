# Preflight artifact lookup and retention

## Context

Staging preflight publishes immutable, content-addressed bundles under
`preflight-artifacts/<bundle-digest>/`. The Tier 1 `artifacts.publish` check
records the exact bundle digest and the component digests in the immutable
`PreflightAssessment`. Detached rehearsal, final-gate plans, and maintenance
operations must consume those build-once outputs without rebuilding them.

The original detached lookup ignored that recorded digest. It enumerated the
whole store, read every descriptor, selected entries by candidate SHA, tree,
and mutation epoch, and rejected the store once it contained more than 256
entries. Consequently, 257 valid immutable publications made every detached
lookup fail before backup mutation. Increasing the limit would only postpone
the same failure and preserve an unnecessary O(n) selection authority.

## Decision

Artifact selection is digest-addressed. A consumer must carry an exact
`PreflightArtifactReference` derived from immutable preflight evidence or an
explicit operator-approved maintenance input. The store opens only that digest
directory and then revalidates the expected candidate SHA, candidate tree,
mutation epoch, image contract, manifests, migration plan, and production
defaults. Unrelated siblings do not participate in lookup.

Lookup correction and retention are delivered as two independently deployable
changes:

1. Remove every store-wide runtime lookup and thread the evidence-bound digest
   through detached rehearsal and maintenance operations. This permanently
   removes store cardinality from rollout availability and can unblock the
   current 257-entry store without deleting evidence.
2. Add an evidence-first retention authority. No deletion is introduced until
   readers, publishers, reference inventory, and retirement share a safe
   locking and approval protocol.

The split keeps the availability correction small and reversible while
preventing pressure to delete an arbitrary historical publication during an
incident.

## Exact reference authority

`PreflightArtifactReference` is a strict projection of the one passing
`artifacts.publish` execution in a `PreflightAssessment`. It contains the
bundle digest plus the image, rendered-manifest, migration, and production
defaults digests already emitted by that check. Construction fails if the
execution is absent, duplicated, failed, malformed, or inconsistent with its
typed evidence.

The broker includes the bundle digest in its secret-free preflight, preview,
and pending responses. It does not add another mutable pointer or duplicate the
digest into `preflight.json`; `assessment.json` remains the immutable source of
truth.

The detached worker reloads `assessment.json`, derives the reference, and
passes it through `CandidatePreflightOrchestrator` and
`InstalledDeepPreflightComposition` to the artifact store. Final-gate and
rehearsal helper paths already read by bundle digest and retain their existing
identity checks.

Manifest-ownership and lifecycle-capacity maintenance require
`--artifact-bundle-sha256` for both inventory and apply. Their approved
inventory or plan binds that digest. Apply reopens the same digest and refuses
candidate, epoch, plan, or rendered-content drift. Maintenance can therefore
never silently switch to another publication between inventory and apply.

No runtime caller retains the legacy candidate/tree/epoch enumeration method.
Historical duplicate publications remain readable by their individual
digests; ambiguity is resolved by evidence, not directory order.

## Retention authority

Retention is a separate installed coordinator-only command with the same
inventory, digest approval, revalidation, and receipt shape as backup
retention. It never runs automatically inside publication or lookup.

Before deletion is enabled, the store gains a service-owned mode-0600 lock file
outside the digest directory root. Reads hold a shared lock across complete
multi-file reconstruction. Publication and retention apply hold an exclusive
lock. Retention inventory holds a shared lock. This prevents deletion or
replacement from racing a reader between `artifact.json`, manifests, and
production defaults.

The reference inventory protects:

- the active rollout pointer and every nonterminal preflight backup job;
- any backup-rotation candidate or active attempt that can still advance;
- promoted failed or cancelled attempts while their checkpoint and
  attestation remain eligible for resume;
- the most recent successful installed release;
- every digest pinned by an in-flight manifest-ownership, lifecycle-capacity,
  backup, recovery, or retention claim;
- every publication younger than the fixed seven-day grace interval.

Preview requests, terminal preflight failures after request-bound cleanup,
superseded successful releases, and expired non-active resume chains become
candidates only after the grace interval. Immutable request assessments,
rehearsals, attestations, final plans, and transition journals remain as audit
evidence after their large bundle payload is retired.

Inventory records each digest directory's device, inode, owner, mode, link
count, timestamps, exact file identities, sizes, and component hashes. Unsafe,
unknown, or changing entries are protected as opaque evidence and block broad
cleanup. The operator approves the exact inventory digest. Apply acquires the
exclusive lifecycle and artifact-store locks, recomputes live references and
metadata, and refuses any protected or candidate drift. Each approved bundle
is renamed to a private quarantine name, the parent is fsynced, its exact four
regular files are removed without following links, and a digest-bound
retirement receipt is published. Restart reconciliation either completes an
approved quarantine or refuses an unreceipted disappearance.

Retention is bounded to at most 32 candidate bundles per plan, so one run
cannot monopolize the service or turn an unexpectedly large store into an
unreviewable deletion request. Additional candidates require another inventory
and approval cycle. The grace interval and batch bound are code-level policy;
they are not ambient environment overrides.

## Failure behavior

Lookup fails closed for a missing requested digest, unsafe path metadata,
descriptor/content drift, expected-identity mismatch, or image/registry drift.
It does not fail because an unrelated sibling exists or because the store has
grown.

Retention never guesses a replacement artifact and never deletes a referenced,
young, unsafe, or unapproved directory. A changed reference set, active
rollout, lifecycle claim, filesystem identity, or receipt state aborts apply
before the affected directory is removed. Partial quarantine is recoverable
only from its exact approved plan.

## Verification

The lookup change must prove:

- a requested bundle loads with more than 256 valid siblings;
- unrelated malformed siblings do not affect digest-addressed lookup;
- a wrong digest or expected candidate/tree/epoch fails closed;
- assessment evidence must contain one passing typed publication reference;
- detached rehearsal, manifest ownership, and lifecycle capacity use the
  explicit digest through restart and inventory/apply boundaries;
- broker responses expose only the safe digest metadata.

The retention change must prove:

- active, nonterminal, resumable, current-release, maintenance-pinned, and
  grace-period references are preserved;
- terminal unreferenced bundles alone enter a bounded approved plan;
- symlinks, hard links, unexpected files, ownership/mode drift, reference
  changes, concurrent publication, and unreceipted disappearance fail closed;
- interrupted quarantine resumes idempotently from the exact claim;
- readers and publishers cannot race an exclusive retention apply;
- a store larger than the prior limit converges without affecting a protected
  rollout reference.

After the lookup PR is installed, the current failed request is cleaned through
its request-bound backup cleanup command, followed by protected preflight, a
dry run, and one fresh rollout. Retention is applied only after its separate PR
passes CI and its inventory has been reviewed.
