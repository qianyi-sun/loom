# Loom docs

What you're looking for, where it lives. Docs are organized into topic
folders; each folder has its own `README.md` with a full contents list and
reading order.

## I want to use Loom

- **[user-guide.md](user-guide.md)** — install, quickstarts (laptop-only,
  local service, CLI-to-server, web app), backends, model sources (hosted +
  local LLMs), web platform workflows, benchmarks and datasets, CLI
  reference, troubleshooting
- **[integrations/provider-onboarding.md](integrations/provider-onboarding.md)**
  — hosted third-party API setup, user-operated Slurm/vLLM checkpoint
  deployment, provider testing, model refresh, and safe registration
- **[integrations/live-streaming.md](integrations/live-streaming.md)** — SSE
  `/stream` + seq-cursor `/events?after_seq=N` API for real-time
  trajectory event consumption; SPA `useTrialEventStream` hook contract

## I want to author a benchmark task

- **[integrations/authoring-a-task.md](integrations/authoring-a-task.md)** —
  `task.toml` schema, on-disk layout, agent/verifier choices, network
  policies, healthchecks, validation, gotchas
- **[user-guide.md#operator-registered-benchmarks-via-configbenchmarkstoml](user-guide.md)**
  — if you already have a folder of `task.toml` bundles, register the whole
  folder via `config/benchmarks.toml` instead of writing a Python adapter
  (`[[local]]`). Or reuse an existing adapter against a fork of its
  upstream (`[[remap]]`).

## I want to run / operate a Loom cluster

Start with the **[runbooks/](runbooks/README.md)** index — it sequences
the 6 runbooks (master operator runbook, first-prod release, staging
launch gate, staging migration, canary, remote worker pool).

Direct links to the ones you're most likely to open:

- **[runbooks/operator-runbook.md](runbooks/operator-runbook.md)** —
  deployment, upgrade/rollback, rate-card management, token rotation,
  alarm response, backup/restore, capacity planning
- **[runbooks/first-prod-release-runbook.md](runbooks/first-prod-release-runbook.md)**
  — executable first `main`-based production runbook for bootstrap,
  temporary staging leases, frontend route checks, prod release, rollback
  prep, and emergency staging drain
- **[runbooks/staging-launch.md](runbooks/staging-launch.md)** — staging
  release gate, onboarding evidence, two-team Run Library smoke, and
  launch decision checklist

## I want to verify Loom's benchmark reward math

- **[score-alignment/](score-alignment/README.md)** — Layer 1 reward-
  contract manifest, Layer 2 adapter-level reports, Layer 3 paired-run
  alignment reports, and the machine-readable `manifest.json` consumed by
  `scripts/benchmark_score_alignment_gate.py`.

## I want to understand how Loom is built

Start with **[architecture/README.md](architecture/README.md)** — full index
with reading order for new contributors, extension protocols, scheduling
+ gateway + cost, auth/isolation, storage, deployment shapes, local LLMs,
observability, and ADRs.

Quickest entry points:

- **[architecture/overview.md](architecture/overview.md)** — one-pager
  (component map, modes, data flow)
- **[architecture/service-mode.md](architecture/service-mode.md)** —
  Control Plane + Worker + LLM Gateway + Postgres + MinIO
- **[architecture/terminus2-runtime.md](architecture/terminus2-runtime.md)** —
  Harbor-embedded `terminus-2` agent: events, worker image, staging smoke
- **[architecture/cli-mode.md](architecture/cli-mode.md)** — the `loom` CLI
  as a stateless wrapper around `Trial.run()`
- **[architecture/adr/](architecture/adr/README.md)** — architecture
  decision records, including the v1 workload-trust boundary
- **[agent/domain-model.md](agent/domain-model.md)** — release candidate,
  Ready-to-Promote, Release, and Workload Trust Mode definitions
- **[architecture/v1-release-ready-program.md](architecture/v1-release-ready-program.md)**
  — evidence-backed v1 readiness program and its release-boundary workstreams

## I'm contributing to Loom

- **[contributing/](contributing/README.md)** — folder index
- **[contributing/contributor-quickstart.md](contributing/contributor-quickstart.md)**
  — repo layout, dev setup, tests + coverage gates, workflow + merge
  mechanics
- **[contributing/loom-vs-harbor.md](contributing/loom-vs-harbor.md)** —
  what Loom does better, what Loom does worse, and why we replaced Harbor
  instead of forking
- **[contributing/repo-migration.md](contributing/repo-migration.md)** —
  canonical repository URL, migrated GitHub settings, issue-tracker
  status, and local remote update commands

## Evidence and research

- **[evidence/](evidence/README.md)** — data-file snapshots that back
  specific score-alignment, benchmark-support, and release-gate claims.
- **[research/](research/README.md)** — research corpus and roadmaps for
  Loom's agent-evaluation platform work.

## Reference

- `../CONTRIBUTING.md` — PR workflow, commit style, Definition of Done
- GitHub releases + `git log` — what shipped when (no separate CHANGELOG;
  release notes auto-generated from squash-merge PR titles)
