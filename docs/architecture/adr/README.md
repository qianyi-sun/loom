# Architecture Decision Records (ADRs)

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
- **[v1-workload-trust-contract.md](v1-workload-trust-contract.md)** — v1's
  machine-enforced `internal_trusted` contract; TaskSet transforms fail closed
  and post-v1 #758 owns untrusted arbitrary-code isolation.

- **[typed-artifacts-lineage-sharing.md](typed-artifacts-lineage-sharing.md)**
  — typed artifact base schema, lineage, clone/reuse, retention, redaction,
  and Run Library sharing policy. Implemented platform baseline.
- **[skill-artifact-injection.md](skill-artifact-injection.md)** —
  SkillMarkdown artifacts and generic trial-time skill injection. Post-v1
  planning baseline.

## Writing a new ADR

Keep filename as a short kebab-case description of the decision. Structure
inside:

- `Status:` accepted / superseded / rejected
- `Date:`
- `Context:` — the problem and constraints
- `Decision:` — what we're doing
- `Alternatives considered:` — with why they lost
- `Consequences:` — what changes downstream

Link the ADR from the relevant architecture doc in
[`../README.md`](../README.md) so it's discoverable in context.
