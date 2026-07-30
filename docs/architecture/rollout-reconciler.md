# Rollout reconciler

**Status:** design (read-only foundation implemented; write cutover gated)
**Tracking:** #1085 (RFC), #1097 (Phase 4)

The staging/production rollout runs as a fixed imperative step-pipeline: preflight →
mandatory backup + restore rehearsal → an ordered protected-apply of components →
final gates. It is correct but brittle in four recurring ways:

1. **Masked failures.** A worker or component collapses an arbitrary failure to a
   generic code, so the real cause is lost (addressed by the structured-error work,
   #1077 / #1085 phases 1a–1b).
2. **Crash on re-apply.** Idempotent-looking components raised when re-applied an
   already-exact state (#1061, #1081).
3. **Field-ownership conflict.** A manual `kubectl set` leaves a foreign field
   manager beside the rollout's, tripping every subsequent apply (#928, #1093).
4. **A flaky gate blocks a good deploy.** A transient rehearsal-browser timeout
   fails the whole rollout, with no notion that it was worth a retry.

The reconciler reframes the rollout by separating concerns the pipeline conflates:

| Concern | Pipeline | Reconciler |
|---|---|---|
| **What** to run | commit → re-installed runner per version | immutable content-addressed artifact (`artifact = f(commit)`) |
| **How** to deploy | fixed imperative step-pipeline | a reconciler driving desired → live |
| **Whether** it's safe | fail-closed gates inline | typed checks with declared semantics |
| **Who** may write | broker, per step | one routine writer + audited break-glass |

## Model

- **Immutable, content-addressed artifacts.** `artifact = f(commit)` — image digests +
  rendered manifests + env-state, pinned by hash. "Deploy X" points desired-state at
  digest X; the reconciler fetches-and-verifies by digest instead of re-installing a
  runner per version.
- **Declarative → reconcile, imperative → ledger.** The naturally-declarative parts
  (k8s manifests, env-state) are idempotent by construction — each loop re-reads live
  state and converges, which removes the crash-on-re-apply class and the
  snapshot-drift class. Ordered, irreversible operations (DB migrations, the
  mutation-epoch, external-supervisor transitions) are **not** re-run to converge;
  they advance a forward-only **version ledger** (record applied position; advance,
  never naively replay).
- **Typed checks.** Each check declares its failure semantics: **transient** →
  bounded retry with backoff, then escalate; **durable** → block immediately. A
  transient that keeps failing escalates to a block, so a durable fault masquerading
  as transient cannot retry forever. Classification is owned and auditable.
- **Structured, never-masked, secret-safe errors.** Every failure is a typed record
  `{code, component, location, structured_fields}`. Secret-safety is a *closed
  schema* — free text originates only where the source component certifies it safe,
  never a best-effort scrub of arbitrary text (the #1077 lesson).
- **Single routine writer + audited break-glass.** The reconciler is the sole
  *routine* writer of managed fields; humans change the desired-state store, not the
  cluster. A first-class, logged, ownership-aware break-glass exists for emergencies,
  and the reconciler **adopts** any break-glass change back on the next loop — so a
  manual fix cannot leave a permanent ownership conflict. This fixes the
  field-ownership root cause and the "what if the reconciler is down" objection.
- **Rollback, honestly.** Re-pointing the artifact rolls back *code* instantly but
  **not** data/schema. Rollback safety therefore requires **expand/contract**
  (backward-compatible) migrations so code N-1 and N both run against the migrated
  schema; a code re-point is then always safe without a data restore. Keep a
  *deterministic* "backup is restorable" check, run **async**, rather than gating
  every deploy on a flaky end-to-end browser rehearsal.

## Components

- **Reconciler** (per environment, long-lived): watches the desired-state store + live
  cluster, converges idempotently, bounded-retries transients, emits structured
  events. Leases + optimistic concurrency (compare-and-swap on the desired version)
  so concurrent actors see each other's intent and cannot silently collide.
- **Verification plane:** typed pre/post checks; results structured and never masked.
- **Policy engine:** per-environment profile — which checks block, approval
  requirements, artifact-source constraints. One engine; staging and prod differ only
  by profile.
- **Desired-state store:** the per-environment pinned target (CAS pointer).
- **Version ledger:** the per-component applied position for imperative ops.

## Migration — incremental, shadow-first (no big-bang)

Read-only/additive increments run alongside the untouched pipeline; only the write
cutover changes behaviour, and it is gated behind proven shadow parity + explicit
owner sign-off. Staging first, prod last.

1. **Read-only shadow observer** — render desired, read live read-only, report drift.
   Never writes; proves the comparison against reality. (`loom cluster reconcile
   --shadow`.)
2. **Desired-state store** — the CAS-guarded pinned target.
3. **Typed check plane** — classify existing checks transient/durable + bounded retry.
4. **Version ledger** — forward-only applied positions for imperative ops.
5. **Reconciler write path (staging)** — the reconciler becomes the sole routine
   writer, with leases + CAS + break-glass adoption; staging cuts over from the
   pipeline. **The only behaviour change — gated.**
6. **Rollback = re-point + expand/contract migrations**; the browser rehearsal is
   demoted to async restorability verification.
7. **Prod last**, after staging is proven.

### Implemented (read-only foundation)

| Increment | Module | Notes |
|---|---|---|
| 1a drift engine | `loom_cli/rollout/shadow_reconcile.py` | pure, secret-safe, field-level |
| 1b shadow CLI | `loom_cli.cluster_cmd` `reconcile --shadow` | live-validated on the five-node k3s |
| 2 desired-state store | `loom_cli/rollout/desired_state_store.py` | per-env CAS pointer |
| 3 typed check semantics | `loom_cli/rollout/check_semantics.py` | transient/durable + bounded retry |
| 4 version ledger | `loom_cli/rollout/version_ledger.py` | forward-only applied positions |

Increments 5+ (the write cutover) are not implemented: they change the live deploy
path and require proven shadow parity, an owner/operator go-ahead, and the multi-node
staging cutover ahead of them.

## Open questions

- Break-glass **adoption** semantics — how the reconciler cleanly re-absorbs a manual
  change on the next loop.
- Retry-escalation thresholds and who owns the transient/durable classification of
  each check id.
- Which pipeline ceremony is load-bearing (hardened against a real past incident) vs
  removable — to be settled per component during the cutover, not assumed away.
