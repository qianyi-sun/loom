# Personal-development acceptance-authority coexistence design

## Status and purpose

This design corrects an authority collision discovered before merging the
concurrent-owner acceptance package. Issue #1280 records an explicit owner
decision to launch the zero-capacity personal application plane with one
authenticated owner exercising two isolated environments. The broader
multi-person development goal separately requires proof that two distinct
owners cannot read, update, or destroy one another's environments.

Both requirements are valid, but they must not share one mutable procedure or
silently replace one another. The repository therefore preserves the approved
sole-owner launch procedure and adds a separately named multi-owner
certification procedure.

## Fixed invariants

- The global executable-new-capacity ceiling remains exactly `0` in both
  procedures.
- Neither procedure activates a worker, submits or executes a task, invokes a
  Slurm mutation, enables a pool executor, or changes OLDLAB/GB10 capacity.
- `loom-dev` remains shared management infrastructure. Personal application
  namespaces remain `loom-dev-<name>` and bounded builder sandboxes remain
  `loom-build-*`.
- `min_slots` stays configurable with default `0`; both acceptance procedures
  use exact minimum `0`. Users configure no pool weights.
- Schema-v1 acceptance bytes and behavior remain compatible with the merged
  sole-owner contract.
- Schema-v2 remains strict: exactly two canonically ordered owners with
  distinct nonzero user and team identifiers, six operation-specific hidden
  denial receipts, complete cleanup, and byte-exact inert rollback.
- A schema-v1 result never certifies multi-owner isolation. A schema-v2 result
  never authorizes the #1280 sole-owner window unless the recorded owner
  explicitly changes that window's authority.
- Credential material, Secret values, kubeconfig contents, private keys, and
  database credentials are never printed or retained in public evidence.

## Decision

Use physically separate runbooks rather than a runtime mode flag.

The existing paths keep their merged #1280 meaning and bytes:

- `docs/runbooks/personal-dev-zero-capacity-acceptance.md` remains the
  sole-owner/two-environment bounded acceptance.
- `docs/runbooks/personal-dev-durable-launch.md` remains the corresponding
  sole-owner zero-capacity durable launch.

The concurrent-owner package adds two explicit paths:

- `docs/runbooks/personal-dev-concurrent-owner-zero-capacity-acceptance.md`
  performs the schema-v2 two-owner lifecycle, cross-owner isolation proof,
  cleanup, and inert rollback.
- `docs/runbooks/personal-dev-multi-owner-durable-launch.md` requires the
  strict schema-v2 result verifier before rendering or applying the durable
  multi-owner operational plane.

The schema-v2 plan, result model, hidden-denial CLI, and read-only verifier stay
shared implementation interfaces. They are additive and do not reinterpret a
schema-v1 plan or result.

## Authority and progression

The two procedures form separate gates, not alternatives selected for
convenience:

1. The #1280 owner may run the existing sole-owner bounded acceptance and
   durable zero-capacity launch under its reviewed change window.
2. Before a second person is onboarded, operators must return the plane to the
   exact reviewed inert shadow, open a separately reviewed multi-owner window,
   run the schema-v2 concurrent-owner procedure with two valid non-rotating
   user-owned API tokens, and restore the shadow.
3. The durable plane may be relaunched for multi-person use only through the
   multi-owner durable-launch runbook bound to that verified v2 result.

This progression prevents a historical v1 result from being presented as
multi-owner evidence while avoiding a retroactive requirement that blocks the
already approved sole-owner launch.

## File and interface boundaries

The existing sole-owner runbooks are byte-preservation fixtures. The new v2
runbooks own all concurrent-owner shell variables, two-session pinning,
operation-specific denial receipts, retained-name epoch fencing, v2 result
assembly, and result verification.

Architecture documents describe both gates explicitly:

- the immediate #1280 launch gate is sole-owner/two-environment;
- multi-person readiness requires the separate schema-v2 gate before the
  second owner is admitted;
- successful application-plane acceptance is never task-capacity evidence.

The concurrent-owner design and implementation plan refer only to the new v2
runbook paths. The operational-plan model continues binding the exact
acceptance-result digest. No schema or renderer change is needed because the
procedures are separated before render and each runbook validates the result
schema it permits.

## Failure behavior

- Missing, mixed, or ambiguous runbook authority stops before source sealing or
  Kubernetes apply.
- The sole-owner procedure rejects schema-v2 substitution; the multi-owner
  procedure rejects schema-v1 substitution.
- Multi-owner verification failure leaves the shared plane in the inert shadow
  and blocks multi-owner durable launch.
- Any interlock drift, nonzero ceiling, worker presence, incomplete cleanup,
  failed hidden-denial receipt, or changed target state stops the applicable
  procedure.
- Operators never improvise direct namespace, PVC, database, bucket, worker,
  or capacity cleanup.

## Verification strategy

Repository tests must prove:

1. Both existing sole-owner runbooks are byte-identical to protected base
   `65dac936a0b0be9898e5cb5ba013811c86f237c6`.
2. New v2 runbooks contain all two-owner concurrency, cross-owner isolation,
   retained-data recovery, final cleanup, strict result verification, and
   inert-rollback boundaries previously reviewed for #1583.
3. Architecture and durable-launch links route sole-owner and multi-owner
   procedures to distinct paths without calling either one a substitute for
   the other.
4. Existing schema-v1 compatibility, schema-v2 model, CLI, evidence, package
   boundary, and secret-isolation tests pass.
5. The complete protected-base diff contains no worker/task/Slurm authority,
   nonzero capacity transition, Secret-like tracked path, `docs/superpowers`,
   or workflow privilege escalation.

The protected base intentionally includes #1585's explicit
`loom-personal-dev-management-ingress` NetworkPolicy evidence in the
sole-owner durable procedure. The multi-owner durable procedure carries the
same evidence. It also preserves #1589's compatibility with canonical
table-only PostgreSQL backup inventories.

After local verification and iterative review produce one complete clean pass,
the corrected exact head must pass `repository-checks`, `images-gate`,
`cluster-smoke-gate`, and `staging-smoke-gate` before owner merge.

## Rejected alternatives

### One runbook with an operator-selected mode

An operator flag would let the same procedure choose v1 or v2 at execution
time. That creates a downgrade surface: a multi-person launch could select the
sole-owner branch without changing the canonical operational plan. Physical
separation makes the authority visible in the reviewed path and tests.

### Operational-plan schema expansion in this correction

A new plan schema could bind an acceptance-result schema and dispatch one
generic runbook. It would require renderer, runtime, status, migration, and
rollback changes unrelated to the already implemented evidence package. The
separate-runbook design achieves an exact fail-closed boundary without widening
runtime configuration.

### Keep the v2 replacement unchanged

Making v2 the only durable-launch route contradicts the recorded #1280
sole-owner decision and blocks the current zero-capacity launch until a second
credential owner exists. It is rejected unless the issue owner explicitly
supersedes that decision.
