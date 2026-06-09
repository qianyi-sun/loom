# Loom docs

What you're looking for, where it lives:

## I want to use Loom

- **[user-guide.md](user-guide.md)** — `loom` CLI: install, `run`,
  `datasets`, `config`, troubleshooting

## I want to author a benchmark task

- **[authoring-a-task.md](authoring-a-task.md)** — `task.toml` schema,
  on-disk layout, agent/verifier choices, network policies,
  healthchecks, validation, gotchas

## I want to run / operate a Loom cluster

- **[operator-runbook.md](operator-runbook.md)** — deployment,
  upgrade/rollback, rate-card management, token rotation, alarm
  response, backup/restore, capacity planning

## I want to understand how Loom is built

Start with the overview, drill into the area you care about:

- **[architecture/overview.md](architecture/overview.md)** — the
  one-pager (component map, modes, data flow)
- **[architecture/driver-protocol.md](architecture/driver-protocol.md)**
  — sandbox lifecycle contract; DockerDriver, FakeDriver,
  DaytonaDriver; how to add a cloud backend
- **[architecture/benchmark-adapter.md](architecture/benchmark-adapter.md)**
  — `BenchmarkAdapter` Protocol; the 14 shipped adapters;
  entry-point discovery; how to add a new dataset
- **[architecture/agent-adapter.md](architecture/agent-adapter.md)**
  — `loom-launcher` framework; the 11 shipped CLI adapters;
  `SubprocessAgent`; how to add an agent
- **[architecture/trajectory-and-atif.md](architecture/trajectory-and-atif.md)**
  — event-sourced JSONL trajectories; ATIF v1.7 projection; how
  trajectories flow from sandbox to MinIO to ATIF
- **[architecture/cli-mode.md](architecture/cli-mode.md)** — how the
  `loom` CLI reuses `Trial.run()` statelessly with no server stack
- **[architecture/service-mode.md](architecture/service-mode.md)** —
  Control Plane + Worker + LLM Gateway + Postgres + MinIO; DRF
  scheduling; auth model; persistence schema

## I want to know how Loom compares to Harbor

- **[loom-vs-harbor.md](loom-vs-harbor.md)** — what Loom does better,
  what Loom does worse, and why we replaced Harbor instead of forking

## Reference

- `../CONTRIBUTING.md` — single-owner workflow, commit style,
  Definition of Done
- GitHub releases + `git log` — what shipped when (no separate
  CHANGELOG; release notes auto-generated from squash-merge PR titles)
