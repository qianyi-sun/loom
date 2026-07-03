# Loom docs

What you're looking for, where it lives:

## I want to use Loom

- **[user-guide.md](user-guide.md)** — `loom` CLI: install, `run`,
  `datasets`, `config`, troubleshooting
- **[provider-onboarding.md](provider-onboarding.md)** — hosted
  third-party API setup, user-operated Slurm/vLLM checkpoint deployment,
  provider testing, model refresh, and safe registration
- **[live-streaming.md](live-streaming.md)** — SSE `/stream` +
  seq-cursor `/events?after_seq=N` API for real-time trajectory
  event consumption; SPA `useTrialEventStream` hook contract

## I want to author a benchmark task

- **[authoring-a-task.md](authoring-a-task.md)** — `task.toml` schema,
  on-disk layout, agent/verifier choices, network policies,
  healthchecks, validation, gotchas
- **[user-guide.md#operator-registered-benchmarks-via-configbenchmarkstoml](user-guide.md)**
  — if you already have a folder of `task.toml` bundles, register
  the whole folder via `config/benchmarks.toml` instead of writing
  a Python adapter (`[[local]]`). Or reuse an existing adapter
  against a fork of its upstream (`[[remap]]`).

## I want to run / operate a Loom cluster

- **[operator-runbook.md](operator-runbook.md)** — deployment,
  upgrade/rollback, rate-card management, token rotation, alarm
  response, backup/restore, capacity planning
- **[staging-launch.md](staging-launch.md)** — staging release
  gate, username/password onboarding evidence, two-team Run Library smoke, and
  launch decision checklist
- **[benchmark-score-alignment.md](benchmark-score-alignment.md)** — v1.0
  Layer 1 score-credibility manifest: canonical references, score semantics,
  Harbor/upstream parity decisions, and same-output replay cases
- **[remote-worker-pool.md](remote-worker-pool.md)** — join extra
  Docker-capable hosts to an existing control node for shared-dev or
  staging capacity before full Kubernetes cluster mode

## I want to understand how Loom is built

Start with the overview, drill into the area you care about:

- **[architecture/overview.md](architecture/overview.md)** — the
  one-pager (component map, modes, data flow)
- **[architecture/driver-protocol.md](architecture/driver-protocol.md)**
  — sandbox lifecycle contract; DockerDriver, FakeDriver,
  DaytonaDriver; how to add a cloud backend
- **[architecture/benchmark-adapter.md](architecture/benchmark-adapter.md)**
  — `BenchmarkAdapter` Protocol; the 19 shipped adapters;
  entry-point discovery; how to add a new dataset; operator-facing
  `config/benchmarks.toml` registry for `[[local]]` folders +
  `[[remap]]` adapter reuse
- **[architecture/benchmark-onboarding-pipeline.md](architecture/benchmark-onboarding-pipeline.md)**
  — scalable benchmark lifecycle, runnable task configs,
  readiness states, and user-owned benchmark onboarding
- **[architecture/agent-adapter.md](architecture/agent-adapter.md)**
  — `loom-launcher` framework; the 11 shipped CLI adapters;
  `SubprocessAgent`; per-trial install + content-addressed image
  cache; how to add an agent
- **[architecture/verifier-protocol.md](architecture/verifier-protocol.md)**
  — typed `VerifierResult`; the 5 shipped verifiers (pytest,
  script, structured, llm_judge, composite); how to add one
- **[architecture/trajectory-and-atif.md](architecture/trajectory-and-atif.md)**
  — event-sourced JSONL trajectories; ATIF v1.7 projection; how
  trajectories flow from sandbox to MinIO to ATIF
- **[architecture/run-library.md](architecture/run-library.md)** —
  org-wide completed-run metadata, typed safe shared artifacts, metadata
  export, clone/reuse provenance, and the team boundary for shared results
- **[architecture/storage-retention.md](architecture/storage-retention.md)** —
  operator-configurable object-store retention policy; provider-neutral
  rules rendered into S3-compatible lifecycle dicts; idempotent apply via
  `loom cluster bootstrap-storage-lifecycle`
- **[architecture/storage-backend-pluggability.md](architecture/storage-backend-pluggability.md)** —
  design spec for swapping MinIO for managed object storage (AWS S3,
  GCS) as a first-class deployment shape at v1.0; what the operator
  pre-creates vs. what Loom bootstraps; per-backend monitoring and
  backup posture
- **[architecture/pipeline-platform-governance.md](architecture/pipeline-platform-governance.md)** —
  post-v1 governance baseline for pipeline extensibility, typed
  artifacts, RunGraph, recipes, plugins, SkillMarkdown injection, and
  data-production planning
- **[architecture/adr-typed-artifacts-lineage-sharing.md](architecture/adr-typed-artifacts-lineage-sharing.md)** —
  post-v1 ADR for typed artifact base schema, lineage, clone/reuse,
  retention, redaction, and Run Library sharing policy
- **[architecture/adr-skill-artifact-injection.md](architecture/adr-skill-artifact-injection.md)** —
  post-v1 ADR for SkillMarkdown artifacts and generic trial-time skill
  injection
- **[architecture/self-service-runtime-registration.md](architecture/self-service-runtime-registration.md)** —
  design for self-service `ModelEndpoint`, `ServingDeployment`, and
  `EvaluationHarness` registration, validation, GB10 serving, and run
  references
- **[architecture/cli-mode.md](architecture/cli-mode.md)** — how the
  `loom` CLI reuses `Trial.run()` statelessly with no server stack
- **[architecture/local-llm.md](architecture/local-llm.md)** — local
  OpenAI-compatible server dispatch (vLLM / ollama / llama.cpp /
  lm-studio); inline `--local-server`; local CLI vLLM helper
  (`--model hf:` / `/path/`), not hosted platform inference
- **[architecture/multi-server-local-llm.md](architecture/multi-server-local-llm.md)**
  — `loom serve` + repeatable `--model` for comparing N models on the
  same dataset; sequential-by-default load loop; `--parallel-models` opt-in
- **[architecture/service-mode.md](architecture/service-mode.md)** —
  Control Plane + Worker + LLM Gateway + Postgres + MinIO; auth
  model; persistence schema
- **[architecture/auth-threat-model.md](architecture/auth-threat-model.md)** —
  threat model for singleton admin auth, username registration, password reset,
  audit, rotation, and production rollout gates
- **[architecture/sandbox-isolation.md](architecture/sandbox-isolation.md)** —
  honest description of the sandbox trust boundary as shipped: what
  iptables policies enforce, what's still aspirational, the known
  `Public`-policy metadata-IP gap, and the #78 roadmap
- **[architecture/auth-registration-spec.md](architecture/auth-registration-spec.md)** —
  implementation spec for singleton admin secret, no-email username/password
  accounts, admin-approved setup/reset links, audit events, operator rotation
  commands, and DB-admin removal
- **[architecture/config-consolidation.md](architecture/config-consolidation.md)** —
  `config/loom-schema.toml` as the single source of truth for
  Pydantic Settings + k8s env blocks + Secret bootstrap + operator
  cluster knobs; `loom cluster doctor` + `loom cluster bootstrap-secrets`
  + `loom config codegen` (shipped #150)
- **[architecture/drf-scheduling.md](architecture/drf-scheduling.md)**
  — the single-SQL claim query; DRF tie-break + priority + FIFO;
  caps eligibility; crash recovery
- **[architecture/llm-gateway.md](architecture/llm-gateway.md)** —
  multi-dialect routing; per-(team, trial, step) attribution; why
  we centralise the LLM call
- **[architecture/cost-and-rate-cards.md](architecture/cost-and-rate-cards.md)**
  — usage frozen, cost derived; rate-card shape; CLI vs. service
  storage; re-pricing history
- **[architecture/observability.md](architecture/observability.md)** —
  metric naming convention; the 5 Grafana dashboards + what each covers;
  Prometheus alert rules; on-call triage path per alert
## I want to know how Loom compares to Harbor

- **[loom-vs-harbor.md](loom-vs-harbor.md)** — what Loom does better,
  what Loom does worse, and why we replaced Harbor instead of forking

## I'm contributing to Loom

- **[contributor-quickstart.md](contributor-quickstart.md)** — repo
  layout, dev setup, tests + coverage gates, workflow + merge
  mechanics
- **[repo-migration.md](repo-migration.md)** — canonical repository URL,
  migrated GitHub settings, issue-tracker status, and local remote update
  commands

## Reference

- `../CONTRIBUTING.md` — PR workflow, commit style, Definition of Done
- GitHub releases + `git log` — what shipped when (no separate
  CHANGELOG; release notes auto-generated from squash-merge PR titles)
