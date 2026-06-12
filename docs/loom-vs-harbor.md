# Loom vs. Harbor

Loom was built ground-up to replace
[Harbor](https://github.com/harbor-framework/harbor) for the CARIN
Research Center. After a three-round audit of Harbor's source (2026-06-05),
we concluded "replace, not fork." This doc captures the concrete
tradeoffs so contributors can judge when Loom is the right choice
and what gaps remain.

## Where Loom is better

Each bullet cites the design decision behind it; "evidence" points
into Harbor's tree where the contrast lives.

| Area | Loom | Harbor |
|---|---|---|
| Trial model | One `Trial` class — every task has `steps`; single-step is a one-element list (`src/loom/trial/trial.py`). | Two classes (`SingleStepTrial` + `MultiStepTrial`) with `Trial.create()` dispatching on `task.has_steps`; ~300 LOC of duplicated lifecycle code. |
| Trajectory model | **Event-sourced JSONL** appended as the trial runs (`src/loom/trajectory/writer.py`); ATIF v1.7 projected at finalize. Partial trajectories survive worker crashes; events stream to subscribers. | Mutable `AgentContext` struct + `Trajectory` validated as a complete array at end. Can't be built incrementally without violating the model; crashed runs lose partial state. |
| Backend abstraction | Six-method `Driver` Protocol + declarative `Capabilities` (`src/loom/driver/base.py`). New cloud backends are ~150–300 LOC adapters — proven with Daytona (`src/loom_drivers/daytona/`) and Modal (`src/loom_drivers/modal/`). | `BaseEnvironment` subclasses are 500–1100 LOC each. Modal alone is 1100 LOC; ~75–80% of Docker's 640 LOC is backend-specific. Strategy patterns reimplemented per backend. |
| Cost accounting | Raw token usage frozen per call; cost derived at query time from versioned rate cards (`rate_cards` table). Historical cost re-computable when prices change. Per-call attribution via `llm_calls` table. | `cost_usd` computed at emit and frozen forever in the trajectory. RFC0001 acknowledges "pricing can change, making historical trajectories inaccurate." Provider-specific charges hide in `metrics.extra` with no schema. |
| Verifier surface | Typed `VerifierResult { rewards, checks: list[CheckResult], confidence, structured, error: VerifierError }` (`src/loom/verifier/base.py`); five concrete verifiers (Pytest, Script, Structured, LLMJudge, Composite). | Single concrete verifier; `VerifierResult.rewards: dict[str, float|int] | None` only. Failures leak as exceptions (`MissingTestDirError`, `ParsingError`, `VerifierOutputNotFound`). |
| MCP integration | Typed `mcp: Sequence[MCPConnection]` channel passed to `AgentRuntime.run()`. | MCP server descriptions appended into the instruction string as prose. |
| `TrialResult` schema | `steps: list[StepResult]` is the single source of truth; trajectory pointers (`trajectory_uri`, `atif_uri`, `atif_schema_version`) are first-class fields. | `agent_result` + `verifier_result` duplicated at trial level AND in `step_results[0]`. Trajectory orphaned — referenced only by `trial_uri`. Timing fields duplicated at trial + step levels. |
| LLM Gateway | Centralized FastAPI service with multi-dialect routing (Anthropic native, OpenAI Chat + Responses, Google Gemini), bearer auth, per-(team, trial, step) attribution, rate-card lookup. | No equivalent — agents call providers directly; cost is opportunistic. |
| Scheduling | DRF (Dominant Resource Fairness) across teams via Postgres `FOR UPDATE SKIP LOCKED` claim SQL (`src/loom_control_plane/scheduler/claim.py`). Multi-worker, fair, no double-claiming. | Single `asyncio.Semaphore(n_concurrent)` gates all trials; no per-team, per-backend, or per-provider quota. |
| Adapter packaging | Out-of-tree PyPI packages discovered via `loom.benchmarks` entry-points; pluggable without forking the runtime (`packages/loom-benchmarks/`, `packages/loom-benchmark-terminal-bench-2/`). | 80 in-tree adapters under `adapters/`; coupled to repo commits via `registry_commit_sha`. |
| Worker fencing + cleanup | Fenced state PATCH (`worker_id` match in UPDATE WHERE) — two workers can never both own a trial. `LiveSandboxRegistry` + atexit drain cloud sandboxes within a bounded budget. | `asyncio.shield` on env stop prevents leaks, but no fencing across workers — single-worker assumption baked in. |
| Two-mode design | Same `Trial.run()` runs in service mode (full cluster) AND CLI mode (`loom run` on a laptop with no server stack). Trajectories are bit-identical. | Cluster-only; no equivalent of `loom run` for one-shot laptop use. |

## Where Loom is worse (current gaps)

These are real gaps that contributors should know about. Issues are
filed where they're tracked; other items are future work that needs a
plan written.

| Gap | Status | Harbor has it via |
|---|---|---|
| **Cloud backend coverage** — Loom ships 3 backends (Docker, Daytona, Modal). Harbor ships ~15: apple_container, cwsandbox, e2b, gke, islo, modal, novita, runloop, singularity, tensorlake, wandb, daytona, docker. | Each remaining backend is ~150–300 LOC against the `Driver` Protocol — the architecture work is done, only the per-provider plumbing remains. | Plenty of incumbent `BaseEnvironment` subclasses. |
| **Adapter slate** — Loom ships 14 benchmark adapters (HumanEval, SWE-Bench family, MBPP, LiveCodeBench, BFCL, GAIA, AIME, OSWorld, WebArena, SkillFlow, SkillLearnBench, Terminal-Bench-2). Harbor ships ~80. | Adding adapters is mechanical against the `BenchmarkAdapter` Protocol; ~200-400 LOC each. We add as research demand arrives. | 80 in-tree adapters under `adapters/`. |
| **Skills system** — Loom doesn't model "skills" (local `SKILL.md` directories resolved by sha256 digest). | Punted at v1; `skills.py` had minimal usage sites in Harbor itself, so this is research infrastructure that hasn't fully landed there either. Revisit when an agent runtime needs it. | `skills.py`. |
| **Leaderboard + viewer UI** — Loom's SPA shows trials + batches + usage; no public leaderboard. | Build as a standalone tool against the ATIF JSONL format when there's demand. | Two separate apps (`leaderboard/`, `viewer/`). |
| **`Capabilities.gpu_types`** — Loom's `Capabilities` now carries a `frozenset[str]` of supported GPU SKUs (added with the Modal driver in #253). Workers match on `gpu_vendor` + `gpu_types`. Scheduler's `requires_caps` filter doesn't yet include `gpu_types`; that's a follow-up when the first GPU-typed `Task` lands. | `Capabilities.gpu_types` populated by `ModalDriver` (13 SKUs); Docker / Daytona leave it empty. Scheduler integration: TBD. | Modal driver passes GPU type via SDK; no scheduler integration. |
| **`verifier_env_mode = "separate"`** — Loom v0.7 verifiers share the agent container, so agent images must ship verifier deps (e.g., `pytest`). Harbor supports SEPARATE for trustworthy grading + SHARED for cheap dev loops. | Tracked for v1.5 (no issue yet — file before starting). Architecture lift is modest because `Driver` Protocol already supports multiple containers per trial. | `SEPARATE` mode in `BaseEnvironment`. |
| **`/admin/tasks` ingestion endpoint** — Operators currently insert into the `tasks` table via SQL (see `scripts/seed_test_data.py`). | Tracked for v1.5 (no issue yet). Should accept a tarball + validate against `TaskConfig`. | `harbor task upload` CLI. |
| **ATIF schema migration tool** — When ATIF schema bumps (currently v1.7), we have no `loom traj migrate --from v1 --to v2` tool. | File when the first schema-bump deprecation hits. Discipline gap, not architectural. | RFC0001 admits Harbor's v1.7 broke `SubagentTrajectoryRef` with no automated migration — we wouldn't be worse than them today, just better in the future. |
| **`is_copied_context` flag on trajectory steps** — Harbor tags steps copied from prior trajectories so SFT pipelines exclude them. Loom doesn't model copied-context. | Adopt when first SFT pipeline needs it. Adding the flag to `_EventBase` + threading through trajectory writer is ~50 LOC. | First-class field on Harbor trajectory steps per RFC0001. |
| **Subagent trajectory embedding** — Loom's trajectory model is flat; Harbor's ATIF v1.7 supports `subagent_trajectories[]` with separate `trajectory_id` (document-scoped) and `session_id` (run-scoped). | Adopt when first multi-agent flow needs it. The split is well-specified in RFC0001 — copy the idea. | `session_id` / `trajectory_id` split per ATIF v1.7. |

## Design rationale (why replace, not fork)

Three reasons Loom is a rebuild rather than a Harbor fork:

1. **State model.** Harbor's mutable `AgentContext` + post-hoc
   `Trajectory` validation makes mid-trial crashes lose data and
   makes streaming/replay structurally impossible. Switching to an
   event log is a foundational change; doing it in-tree would have
   touched almost every file in `harbor/trial/`, `harbor/agents/`,
   and `harbor/models/trajectories/`.

2. **Backend coupling.** Harbor's 500–1100 LOC per-backend
   subclasses are an artifact of `BaseEnvironment` being abstract
   over too much. Pulling backend-specific code out behind a
   six-method Driver Protocol means rewriting the orchestrator core
   — again, a foundational change, not a refactor.

3. **Cost accounting.** Snapshot-cost-at-emit is encoded into the
   trajectory schema and every consumer of it. Splitting raw usage
   from derived cost means changing the schema, the writer, the
   reader, the ATIF projection, the rate-card store, and the
   `llm_calls` table — none of those exist in Harbor today.

The three changes interact: the Driver Protocol assumes event-sourced
trajectories (drivers emit `EnvStart`/`EnvReady`/`EnvExec` events
inline); the cost model assumes a centralized Gateway that owns rate
cards (which assumes a service-mode shape); the service-mode shape
assumes fenced multi-worker scheduling. After three audit rounds, the
team concluded these couldn't be retrofitted into Harbor without
effectively rewriting it, so we built Loom directly.

What we kept from Harbor (because it's right):
- Multi-phase network policy with per-phase context manager
- SHARED vs SEPARATE verifier environment modes (Loom v0.7 only has
  SHARED — see gap above)
- `asyncio.shield` on env stop
- Hooks (`TrialEvent` enum + async callbacks)
- `force_build`, timeout multipliers, override timeouts
- `is_copied_context` and `session_id`/`trajectory_id` split (still
  to adopt — see gaps above)

## See also

- [architecture/overview.md](architecture/overview.md) — how Loom is
  put together
- [architecture/driver-protocol.md](architecture/driver-protocol.md)
  — the six-method Driver Protocol that replaces Harbor's
  `BaseEnvironment`
- [architecture/trajectory-and-atif.md](architecture/trajectory-and-atif.md)
  — event-sourced trajectories + ATIF v1.7 projection
- [architecture/service-mode.md](architecture/service-mode.md) —
  Control Plane, Worker, Gateway, DRF scheduling
