# Loom

Agent evaluation and training-data generation runtime, in active rebuild.

Loom replaces the `harbor==0.9.0` integration this repo previously
shipped with a tailored, distributed runtime designed for shared use
across our research subteams. The first product workflow targeting
Loom is SkillFlow + SkillLearnBench (terminal-agent skill learning)
under pilot group.

## Status

Pre-implementation. The full design and the seven implementation
plans are written. Code starts landing once Plan 1 begins.

- **Design spec:** `docs/superpowers/specs/2026-06-05-loom-runtime-core-design.md`
- **Implementation plans:** `docs/superpowers/plans/`
- **Cross-plan consistency review:** `docs/superpowers/plans/2026-06-05-loom-cross-plan-review.md`
- **Harbor design review** (architectural research informing the rebuild): `notes/harbor-design-review.md`
- **Pre-Loom repo content** (read-only reference): `legacy/`

## Quick architecture (one paragraph)

A FastAPI **Control Plane** owns the trial state machine, DRF
fairness scheduling, and the trajectory index. **Workers** poll for
trials, run them in-process against a **Driver** (Docker in v1),
emit append-only JSONL trajectories to MinIO, and report state via
fenced HTTP PATCH endpoints. An **LLM Gateway** (LiteLLM-backed)
proxies model calls so every trajectory carries faithful token usage
and cost. Postgres + MinIO are the only stateful services; the LLM
Gateway and Control Plane are stateless. ATIF v1.7 projection at
trial finalize gives downstream tooling a stable trajectory format.

Full design: see the spec linked above.

## Repo layout

```
src/loom/                          # foundation library + per-service packages
src/loom_control_plane/            # FastAPI Control Plane service
src/loom_llm_gateway/              # OpenAI-compatible LLM Gateway service
src/loom_worker/                   # Worker process
migrations/                        # Alembic
tests/{unit,contract,integration,system,property,fixtures}/
docs/superpowers/{specs,plans}/    # design + roadmap
deploy/                            # docker-compose + k8s manifests
legacy/                            # everything from before Loom — read-only
```

## How to read this repo

1. `docs/superpowers/specs/2026-06-05-loom-runtime-core-design.md` — the design
2. `docs/superpowers/plans/2026-06-05-loom-cross-plan-review.md` — the cross-plan summary
3. `docs/superpowers/plans/2026-06-05-loom-plan-0-repo-prep.md` (Plan 0) — repo bootstrap
4. `docs/superpowers/plans/2026-06-05-loom-foundation-library.md` (Plan 1) and onward

## Contributing

`CONTRIBUTING.md` — generic process, still applicable.
`SECURITY.md` — generic policy, still applicable.
