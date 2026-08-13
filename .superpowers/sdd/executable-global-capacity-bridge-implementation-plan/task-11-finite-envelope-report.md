# Task 11: Finite Authority Envelope and Active Immutable Fact Flow

## Status

Implemented the finite executable authority envelope and active immutable fact
flow without adding an activation, lifecycle, CLI, renderer, deployment, or API
route that can select a positive ceiling.

## Delivered behavior

- `ExecutionPreparationPolicyV2.executable_new_capacity_ceiling` is now a
  required strict `PositiveQuantity`; zero, negative, Boolean, float, and
  values above `MAX_QUANTITY` are rejected.
- The execution-epoch ORM and migration agree that requested ceiling is
  positive, rather than exactly one.
- Preparation locks the exact OLDLAB and GB10 pool rows for the bound
  configuration, verifies their generations, uses checked finite addition of
  their `max_slots`, and rejects a requested/policy ceiling above the sum.
- Shadow-only mutation requires exact shadow state, execution epoch zero, no
  manifest, and ceiling zero. Configuration proposal/activation, personal
  projection, and shadow commits therefore remain unavailable in active,
  prepared, drain-only, retired, or contradictory authority states.
- Demand and pool reports use a separate fact guard. Shadow remains accepted;
  active authority locks and validates its exact epoch, manifest, positive
  ceiling/rate, and writer fence, then binds reports to the immutable
  configuration epoch. Prepared and drain-only states reject facts.
- Active demand facts verify the exact subject, reporter, configuration and
  deployment generations, acknowledgement, and candidate provenance. Active
  pool facts verify the exact configured pool, reporter, and execution-pool
  generation.

## TDD evidence

The focused RED gate initially failed as required: ceiling two was rejected by
the `Literal[1]` policy and finite-envelope fixtures could not be constructed.
The active fact test was written before the authority implementation; after the
finite contract change it exercises the former ceiling-only authority denial.

The focused GREEN gate passed:

```text
11 passed, 60 deselected
```

## Verification

```text
126 passed, 1 warning
ruff format --check: passed (8 files)
ruff check: passed
mypy: Success: no issues found in 3 source files
```

The pytest warning is the existing Starlette `TestClient` deprecation warning
for `httpx`; it does not affect the task behavior.

## Scope notes

The worktree already contained an unrelated modification to
`docs/architecture/executable-global-capacity-bridge-implementation-plan.md`.
It was preserved and is not part of this task.
