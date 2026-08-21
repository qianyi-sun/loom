# Architecture

Current contracts for Loom's implementation. Start with
[`overview.md`](overview.md), then follow the area-specific references below.
Design proposals, implementation plans, and decision history are kept in
[`../../archive/docs/architecture/`](../../archive/docs/architecture/) rather
than mixed with current behavior.

## Core execution

- **[Overview](overview.md)** — components, execution modes, and main data
  flows.
- **[Service mode](service-mode.md)** — Control Plane, Worker, LLM Gateway,
  Postgres, object storage, and REST service.
- **[CLI mode](cli-mode.md)** — stateless local execution through
  `Trial.run()`.
- **[Trajectories and ATIF](trajectory-and-atif.md)** — append-only event
  storage, streaming, and ATIF projection.
- **[Family runs](family-runs.md)** — ordered related trials, state adapters,
  and the optional family orchestrator.
- **[Pipeline orchestrator](pipeline-orchestrator.md)** — persisted RunGraph
  reconciliation, the disabled-by-default controller boundary, and the
  executable BEHAVIOR rollout-stage adapter.

## Extension contracts

- **[Driver protocol](driver-protocol.md)** — sandbox lifecycle and included
  driver capabilities.
- **[Benchmark adapter](benchmark-adapter.md)** — adapter discovery, catalog
  entries, task materialization, and operator registration.
- **[Benchmark onboarding](benchmark-onboarding-pipeline.md)** — catalog
  readiness, publication, and user-owned benchmark intake.
- **[User-brought TaskSets](user-brought-tasksets.md)** — team-owned TaskSet
  registration and materialization within the current trust boundary.
- **[Agent adapter](agent-adapter.md)** — `loom-launcher`, built-in agents, and
  per-trial installation caching.
- **[Terminus-2 runtime](terminus2-runtime.md)** — the Harbor-embedded runtime
  and its Loom integration.
- **[Verifier protocol](verifier-protocol.md)** — verifier result schema and
  built-in verifiers.

## Scheduling, capacity, and cost

- **[DRF scheduling](drf-scheduling.md)** — eligibility, fairness, claim
  fencing, and recovery.
- **[Global fleet capacity manager](global-fleet-capacity-manager.md)** — the
  shadow-allocation service, personal subject projections, demand reports,
  fenced dry-run grant/executor records, and the non-executable authority
  boundary.
- **[Global development-fleet autoscaler](global-dev-fleet-autoscaler.md)** —
  implemented supervisor contract and the checked-in disabled boundary.
- **[GB10 capacity](gb10-dynamic-capacity.md)** — inventory, health, dynamic
  allocatable capacity, and placement rules.
- **[LLM Gateway](llm-gateway.md)** — provider dialects, attribution, and
  routing.
- **[Cost and rate cards](cost-and-rate-cards.md)** — usage snapshots,
  projected cost, and rate-card lookup.

## Security and tenancy

- **[Authentication and teams](auth-and-teams.md)** — accounts,
  teams, sessions, setup/reset links, audit events, and operator controls.
- **[Authentication threat model](auth-threat-model.md)** — enforced trust
  boundaries and mitigations.
- **[Sandbox isolation](sandbox-isolation.md)** — network enforcement and the
  supported workload trust mode.
- **[Environment naming](env-naming-convention.md)** — canonical environment
  identities and route prefixes.

## Data, storage, and shared results

- **[Staging data lifecycle](staging-data-lifecycle.md)** — data authority,
  garbage collection, rollback leases, and checkpoints.
- **[Staging rollout preflight](staging-rollout-preflight.md)** — candidate
  checks, rehearsal, and attestations.
- **[Storage retention](storage-retention.md)** — lifecycle policy rendering
  and apply behavior for supported object stores.
- **[Run Library](run-library.md)** — shared completed-run metadata, artifacts,
  provenance, and access boundaries.

## Deployment and operations

- **[Cluster deployment](cluster-deploy.md)** — `loom cluster` rendering,
  preflight, lifecycle, diagnostics, and secret bootstrap.
- **[Protected staging rollout](staging-rollout.md)** —
  candidate binding, locking, backup, evidence, and operator authority.
- **[Personal development environments](multi-dev-environments.md)** —
  opt-in source-fresh CLI/API lifecycle, identity, activation, capacity
  publication, candidate artifact collection, manager-first teardown, and
  limits.
- **[Multi-node topology](multi-node-topology.md)** — Postgres, MinIO,
  storage, anti-affinity, and disruption budgets.
- **[PgBouncer transaction mode](pgbouncer.md)** —
  pooled database URLs, rendering, health checks, and fallback.
- **[Configuration schema](configuration.md)** — generated settings,
  cluster configuration, and secret projection from `loom-schema.toml`.
- **[CI runner acceleration](ci-runner-acceleration.md)** — current
  hosted/self-hosted runner selection and isolation requirements.

## Local model serving

- **[Local LLMs](local-llm.md)** — local OpenAI-compatible endpoints and the
  inline vLLM helper.
- **[Multiple local model servers](multi-server-local-llm.md)** — `loom serve`
  and multi-model loading.
- **[Responses API support](responses-api.md)** — capability
  probing and Responses-to-Chat fallback.

## Web application and observability

- **[Human-readable web UX](human-readable-spa-ux.md)** — default and
  diagnostics presentation rules.
- **[Frontend error recovery](frontend-error-recovery.md)** — safe recovery
  boundaries and browser diagnostics.
- **[Frontend quality gate](frontend-quality-gate.md)** — required type,
  test, build, accessibility, and route checks.
- **[Observability](observability.md)** — metrics, dashboards, alerts, and
  alert-specific triage.
