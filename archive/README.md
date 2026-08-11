# Loom archive

This directory preserves documents and evidence that are useful for research or
audit context but do not describe the current Loom product. Nothing under
`archive/` is an implementation contract, operator procedure, release gate, or
statement of supported behavior.

Use the [current documentation index](../docs/index.md) for current behavior.
Use Git history when an
exact pre-archive version or its original repository layout matters.

The archive mirrors the original top-level repository tree so related material
remains grouped and its former purpose is visible. When a current replacement
uses the original filename, the archived predecessor has a descriptive suffix
such as `-design`, `-history`, `-plan`, or `-status`.

Archived files are intentionally not linked from procedural steps as current
authority. A current page may link here only to explain where non-current
material is kept. Preserve original dates, issue references, and delivery
language inside archived artifacts; their value is as records, not as current
instructions.

## Contents

- [`docs/architecture/`](docs/architecture/) — superseded decisions,
  unimplemented designs, implementation plans, and dated assessments.
- [`docs/contributing/`](docs/contributing/) — completed repository migration
  notes.
- [`docs/evidence/`](docs/evidence/) — dated benchmark, rollout, and
  issue-specific reports in both narrative and machine-readable form. Only
  current evidence schemas stay under the active `docs/evidence/` tree.
- [`docs/research/`](docs/research/) — paper notes, roadmaps, and third-party
  snapshots.
- [`docs/runbooks/`](docs/runbooks/) — completed one-time migrations,
  release-era checklists, and history-heavy predecessors of current
  procedures.
- [`docs/score-alignment/`](docs/score-alignment/) — prior program material,
  live-run manifests, and frozen Layer 2 and Layer 3 reports. The active score
  contract remains under `docs/score-alignment/`.
- [`deploy/`](deploy/) — retired deployment designs, dated fleet evidence,
  superseded worker-pool inputs, and completed infrastructure experiments.
