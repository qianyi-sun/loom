# Evidence

Data-file snapshots that back specific score-alignment, benchmark-support,
or release-gate claims made in the runbooks and score-alignment layers.
Each entry is a frozen artifact of a specific run — do not edit an existing
evidence file; land a new dated one.

## Naming

- **Dated results:** `YYYY-MM-DD-<benchmark>-<harness>-<subset>.<ext>` for
  cross-implementation runs and score comparisons, e.g.
  `2026-06-26-aime-24-loom-full-30.json`.
- **Issue-scoped bundles:** `issue-<N>/` subfolders for evidence attached
  to a specific issue's gate. Include the issue's `README.md` inside
  summarizing what the bundle proves.

## Current bundles

- **`issue-217/`** — Terminal-Bench-2 rollout evidence (g3 public-beta
  smoke, g6 provider matrix, hello-world ATIF/trajectory).
- **`issue-222/`** — SLB v1.0 support gate: per-task results and
  supporting summary.
- Dated JSON/CSV files at the root — AIME, GPQA, and SLB
  cross-implementation comparisons referenced from
  [`../score-alignment/layer-3.md`](../score-alignment/layer-3.md).

## When to add

When a runbook step, score-alignment layer, or launch gate cites specific
numeric evidence, save the source file here with a dated name and reference
it from the citing doc. Avoid inlining large JSON in prose; link out.

Do not remove past evidence — it backstops old audit trails. If a claim is
superseded, add a new evidence file rather than editing the old one.
