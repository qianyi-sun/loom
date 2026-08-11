# Architecture Decision Records (ADRs)

> Archived decision-record index. Current architecture contracts live in
> `docs/architecture/`.

Durable "we decided X because Y" notes for architecture choices that shape
post-v1 platform work. ADRs are not implementation specs — they record the
decision, the alternatives considered, and the consequences. Once accepted,
they don't get rewritten; supersede with a new ADR if needed.

## Records

- **[pipeline-run-graph-v1.md](pipeline-run-graph-v1.md)** — accepted
  official-Recipe-only Pipeline RunGraph v1 contract: immutable graph
  snapshots, container plus automatic outcome-gate nodes, manifest fan-out,
  PipelineRun/StageRun/ExecutionAttempt separation from Trial, and explicit
  merge-versus-deployment authority.
- **[env-domain-topology.md](env-domain-topology.md)** — accepted single-origin
  environment routing: `/dev`, `/staging`, and `/prod`, with explicit
  path-prefix isolation requirements and no environment subdomains.
- **[independent-staging-rollout-runner.md](independent-staging-rollout-runner.md)**
  — decision record for the independently operated staging rollout runner.
- **[v1-workload-trust-contract.md](v1-workload-trust-contract.md)** — v1's
  machine-enforced `internal_trusted` contract; TaskSet transforms fail closed
  and post-v1 #758 owns untrusted arbitrary-code isolation.

- **[typed-artifacts-lineage-sharing.md](typed-artifacts-lineage-sharing.md)**
  — typed artifact base schema, lineage, clone/reuse, retention, redaction,
  and Run Library sharing policy. Implemented platform baseline.
- **[skill-artifact-injection.md](skill-artifact-injection.md)** —
  SkillMarkdown artifacts and generic trial-time skill injection. Post-v1
  planning baseline.

## Archive policy

Keep decision status, dates, alternatives, and consequences in this archive.
When implementation changes, update the active architecture contract and retain
the ADR here; active pages may link to this directory only as non-authoritative
decision context.
