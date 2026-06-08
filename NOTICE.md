# NOTICE

This repository is being rebuilt as **Loom** — an agent evaluation
and training-data generation runtime — replacing the previous
`agentic-data-platform` content that integrated `harbor==0.9.0`.

The Loom design specs and implementation plans live under
`docs/specs/` and `docs/plans/`. Plans 0–22 have shipped (runtime
core, multi-dialect Gateway, benchmark + agent integrations, service
layer + SPA); the harbor-parity arc (Plans 23–27) is specified and
ready to execute.

All pre-Loom code, tests, docs, and configuration artifacts have been
moved to `legacy/` and are no longer wired into the build. They are
preserved for historical reference and selective salvage as the
rebuild proceeds.

The directory `agentic-data-platform/` (which is this repository's
working copy name) may be renamed to `loom/` in a future cleanup —
that's a high-blast-radius operation deferred until Plan 1 has shipped
and the rebuild is confirmed working.

— 2026-06-05
