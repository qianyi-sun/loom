# Architecture

Design docs, protocol specs, and ADRs for how Loom is built. Start with
[`overview.md`](overview.md); drill into the area you care about from
there.

## Reading order for new contributors

1. **[overview.md](overview.md)** — one-pager: component map, modes, data
   flow.
2. **[service-mode.md](service-mode.md)** — Control Plane + Worker + LLM
   Gateway + Postgres + MinIO. The main production shape.
3. **[cli-mode.md](cli-mode.md)** — the `loom` CLI reusing `Trial.run()`
   statelessly, no server stack.
4. **[trajectory-and-atif.md](trajectory-and-atif.md)** — event-sourced
   trajectories, ATIF projection, MinIO layout.

## Extension protocols

- **[driver-protocol.md](driver-protocol.md)** — sandbox lifecycle contract;
  DockerDriver, FakeDriver, DaytonaDriver; how to add a cloud backend.
- **[benchmark-adapter.md](benchmark-adapter.md)** — `BenchmarkAdapter`
  Protocol; the shipped adapters; entry-point discovery; how to add a new
  dataset; the operator-facing `config/benchmarks.toml` registry for
  `[[local]]` folders and `[[remap]]` adapter reuse.
- **[benchmark-onboarding-pipeline.md](benchmark-onboarding-pipeline.md)** —
  scalable benchmark lifecycle, runnable task configs, readiness states, and
  user-owned benchmark onboarding.
- **[user-brought-tasksets.md](user-brought-tasksets.md)** — team-owned
  TaskSet intake, materialization, and run creation.
- **[agent-adapter.md](agent-adapter.md)** — `loom-launcher` framework;
  shipped CLI adapters; `SubprocessAgent`; per-trial install + content-
  addressed image cache; how to add an agent.
- **[verifier-protocol.md](verifier-protocol.md)** — typed `VerifierResult`;
  the five shipped verifiers (pytest, script, structured, llm_judge,
  composite); how to add one.

## Scheduling, gateway, cost

- **[drf-scheduling.md](drf-scheduling.md)** — the single-SQL claim query;
  DRF tie-break + priority + FIFO; caps eligibility; crash recovery.
- **[llm-gateway.md](llm-gateway.md)** — multi-dialect routing;
  per-(team, trial, step) attribution; why we centralize the LLM call.
- **[cost-and-rate-cards.md](cost-and-rate-cards.md)** — usage frozen, cost
  derived; rate-card shape; CLI vs. service storage; re-pricing history.
- **[issue45-worker-autoscaler-design.md](issue45-worker-autoscaler-design.md)**
  — resource-aware OLDLAB Slurm autoscaling and GB10 desired-state
  reconciliation.

## Auth, isolation, sandboxing

- **[auth-threat-model.md](auth-threat-model.md)** — threat model for
  singleton admin auth, username registration, password reset, audit,
  rotation, and production rollout gates.
- **[auth-registration-spec.md](auth-registration-spec.md)** —
  implementation spec for singleton admin secret, no-email
  username/password accounts, admin-approved setup/reset links, audit
  events, operator rotation commands, and DB-admin removal.
- **[sandbox-isolation.md](sandbox-isolation.md)** — honest description of
  the sandbox trust boundary as shipped: what iptables policies enforce,
  what's still aspirational, the known `Public`-policy metadata-IP gap.
- **[adr/v1-workload-trust-contract.md](adr/v1-workload-trust-contract.md)**
  — v1's `internal_trusted` release boundary; user TaskSet transforms are
  unavailable, and #758 owns future untrusted arbitrary-code isolation.

## Storage and Run Library

- **[storage-retention.md](storage-retention.md)** — operator-configurable
  object-store retention policy; provider-neutral rules rendered into
  S3-compatible lifecycle dicts; idempotent apply via
  `loom cluster bootstrap-storage-lifecycle`.
- **[storage-backend-pluggability.md](storage-backend-pluggability.md)** —
  design spec for swapping MinIO for managed object storage (AWS S3, GCS)
  as a first-class deployment shape at v1.0; what the operator pre-creates
  vs. what Loom bootstraps; per-backend monitoring and backup posture.
- **[run-library.md](run-library.md)** — org-wide completed-run metadata,
  typed safe shared artifacts, metadata export, clone/reuse provenance,
  and the team boundary for shared results.
- **[loom-spa-v3.md](loom-spa-v3.md)** — trial-centric simplification of the
  SPA; batch/trial/artifact routing.

## Deployment shapes

- **[cluster-deploy.md](cluster-deploy.md)** — the `loom cluster` CLI:
  `render`, `preflight`, `audit`, `up`, `down`, `doctor`,
  `bootstrap-secrets`.
- **[multi-node-topology.md](multi-node-topology.md)** — Postgres HA
  (#637), distributed MinIO (#610), topology schema (#641), HA templates
  (#642).
- **[pgbouncer-transaction-mode-design.md](pgbouncer-transaction-mode-design.md)**
  — transaction-mode connection multiplexing design.
- **[config-consolidation.md](config-consolidation.md)** —
  `config/loom-schema.toml` as the single source of truth for Pydantic
  Settings + k8s env blocks + Secret bootstrap + operator cluster knobs.
- **[cluster-deploy-spikes/](cluster-deploy-spikes/README.md)** — the
  archived Cluster-H investigation spikes referenced from cluster-deploy
  and the images CI workflow.

## Local LLMs

- **[local-llm.md](local-llm.md)** — local OpenAI-compatible server
  dispatch (vLLM / ollama / llama.cpp / lm-studio); inline
  `--local-server`; local CLI vLLM helper (`--model hf:` / `/path/`); not
  hosted platform inference.
- **[multi-server-local-llm.md](multi-server-local-llm.md)** — `loom
  serve` + repeatable `--model` for comparing N models on the same
  dataset; sequential-by-default load loop; `--parallel-models` opt-in.

## Observability

- **[observability.md](observability.md)** — metric naming convention; the
  5 Grafana dashboards + what each covers; Prometheus alert rules; on-call
  triage path per alert.

## Human-readable SPA

- **[human-readable-spa-ux.md](human-readable-spa-ux.md)** — two-layer
  default/diagnostics rule; humanizer libraries; SPA specifications for
  New Batch, Monitor, Trial Detail, Batch Detail, Providers.
- **[human-readable-spa-ux-implementation-plan.md](human-readable-spa-ux-implementation-plan.md)**
  — historical implementation plan for the human-readable UX rollout.

## Provider probes

- **[responses-api-support-probe.md](responses-api-support-probe.md)** —
  proactive probe for Responses API support extending the Chat-fallback
  shim.

## Self-service runtime registration (post-v1)

- **[self-service-runtime-registration.md](self-service-runtime-registration.md)**
  — `ModelEndpoint`, `ServingDeployment`, `EvaluationHarness`
  registration, validation, GB10 serving, and run references.
- **[pipeline-platform-governance.md](pipeline-platform-governance.md)** —
  post-v1 governance baseline for pipeline extensibility, typed artifacts,
  RunGraph, recipes, plugins, SkillMarkdown injection, and data-production
  planning.

## ADRs

Architecture decision records — durable "we decided X because Y" notes for
choices that shape post-v1 architecture. See [`adr/`](adr/README.md).
