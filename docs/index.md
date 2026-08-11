# Loom documentation

The documents in this directory describe the current Loom interfaces,
architecture, and operating procedures. Historical decisions, completed
migrations, research notes, and implementation plans live outside this tree in
[`../archive/`](../archive/).

## Use Loom

- **[User guide](user-guide.md)** — installation, local and service-mode
  quickstarts, backends, model sources, web workflows, benchmarks, CLI
  reference, and troubleshooting.
- **[Provider onboarding](integrations/provider-onboarding.md)** — hosted API
  providers and operator-managed Slurm/vLLM endpoints.
- **[Live event streaming](integrations/live-streaming.md)** — trial event SSE
  and cursor-based event reads.

## Author benchmarks and tasks

- **[Integration guides](integrations/README.md)** — provider registration,
  task authoring, and trial-event streaming.
- **[Task authoring](integrations/authoring-a-task.md)** — `task.toml`, task
  bundles, agents, verifiers, network policies, health checks, and validation.
- **[Benchmark adapters](architecture/benchmark-adapter.md)** — Python
  adapters, entry-point discovery, and the operator-owned
  `config/benchmarks.toml` registry.
- **[Benchmark score contract](score-alignment/README.md)** — the
  machine-checked reward-semantics manifest for the supported catalog.
- **[Validation artifact schemas](evidence/README.md)** — current
  machine-readable evidence contracts consumed by repository checks.

## Operate Loom

- **[Runbook index](runbooks/README.md)** — the entry point for deployment,
  release, capacity, recovery, and local-development procedures.
- **[Operator runbook](runbooks/operator-runbook.md)** — steady-state cluster
  operations, upgrades, rollback, credentials, storage, and incidents.
- **[Remote worker pools](runbooks/remote-worker-pool.md)** — external Docker
  workers and Slurm-backed capacity.

## Understand the implementation

- **[Architecture index](architecture/README.md)** — current component and
  protocol references.
- **[Overview](architecture/overview.md)** — component map, execution modes,
  and data flow.
- **[Service mode](architecture/service-mode.md)** — Control Plane, Worker,
  LLM Gateway, Postgres, and object storage.
- **[CLI mode](architecture/cli-mode.md)** — local execution through the same
  trial runtime without the service stack.
- **[Domain model](agent/domain-model.md)** — release, promotion, and workload
  trust terms used by code and runbooks.

## Contribute

- **[Contributor documentation](contributing/README.md)** — development setup,
  repository workflow, and the current Loom/Harbor comparison.
- [`../CONTRIBUTING.md`](../CONTRIBUTING.md) — change workflow, commit style,
  and required checks.
