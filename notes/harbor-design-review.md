# Harbor Design Review — Improvement Opportunities

**Status:** Finalized 2026-06-05. Open queue cleared. Ready to drive v1 scoping.

Working notes for the Harbor (https://github.com/harbor-framework/harbor) clone we're building. Each item is a candidate architectural change relative to upstream Harbor. Living document — edit freely.

## Status legend
- **Real** — improvement is validated by concrete evidence in Harbor's source code or its own RFCs
- **Speculative** — plausible improvement, not yet validated against the code
- **Rejected** — investigated further, Harbor's design is actually sound here
- **Open question** — needs a decision before we know if it's worth doing

## Evidence sources
- Read in full: `trial/trial.py`, `trial/single_step.py`, `trial/multi_step.py`, `agents/base.py`, `environments/definition.py`
- Read via agent (round 1): top of `environments/docker/docker.py` (640 LOC), `models/agent/context.py`, `models/trajectories/trajectory.py`, `verifier/base.py`, `job.py`, `trial/queue.py`, `storage/base.py`, `publisher/publisher.py`
- Read via agent (round 2): `adapters/swebench/` package + `parity_summary.csv` + `adapter_metadata.json`, `models/task/config.py`, `registry/client/`, `environments/modal.py` (~1100 LOC), `skills.py`, `rfcs/0001-trajectory-format.md` (full content w/ verbatim quotes)
- Still unread (not load-bearing for v1 scope): the other ~13 env backends, registry/storage Supabase implementations, leaderboard, viewer, the other 79 adapters

---

## High-impact architectural changes

### H1. Collapse SingleStepTrial and MultiStepTrial into one model
**Status:** Real — migration path now clear
**Evidence:** `trial/trial.py` (~600 LOC) + `trial/multi_step.py` (~250 LOC) + `trial/single_step.py` (~80 LOC). `Trial.create()` already dispatches on `task.has_steps`. Multi-step is the strict superset — single-step is a degenerate 1-element list. Round-2 read of `models/task/config.py` confirms the field decomposition:
- **Task-level only (not per-step):** `schema_version`, `task` (package identity), `source`, `solution`
- **Inherits to implicit single step:** `agent`, `verifier`, `environment`, `artifacts`
- **Step-only fields:** `name`, `min_reward` (gating threshold), `healthcheck`, per-step `agent`/`verifier` overrides, per-step `artifacts`
- **Multi-step aggregation:** `multi_step_reward_strategy` (FINAL or MEAN)
**Improvement:** One `Trial` class. Every task has `steps`; if a task omits the field, Harbor synthesizes `steps = [StepConfig(name="main", agent=task.agent, verifier=task.verifier, artifacts=task.artifacts)]`. Removes ~300 LOC of duplication and one branch in the public API.
**Risk:** Tasks that don't declare steps need an unsurprising default that matches today's SingleStep semantics (no per-step workdir, no step setup script, SHARED verifier mode, no `min_reward` gate).
**Open questions:**
- Do we ship a `harbor task migrate` for upstream Harbor tasks, or accept incompatibility? (Recommend: ship it — adapters already emit Harbor's task format, so compatibility is the cheaper path to leverage the ~80 existing adapters.)

### H2. Event-sourced trajectory model (vs. mutable struct)
**Status:** Real
**Evidence:** `models/agent/context.py` — `AgentContext` is a static Pydantic blob with only token/cost counters, `rollout_details` list, metadata, and `is_empty()`. No append methods. `models/trajectories/trajectory.py` — `Trajectory` validates `steps` as a complete sequential array; can't be built incrementally without violating the model. RFC0001 (ATIF v1.7) confirms: trajectories are materialized at end.
**Improvement:** Replace the struct with an append-only event log. `TrajectoryBuilder.append(event)` writes JSONL to disk as events happen. `Trajectory.from_log(path)` derives the materialized view. Wins: partial trajectories survive crashes naturally; rollouts can stream to subscribers (training, dashboards); replay/fork is trivial; the struct view is still derivable for compatibility.
**Risk:** Event schema discipline becomes load-bearing. JSONL parsing on hot paths is slower than direct struct access (mitigated by lazy materialization).
**Open questions:**
- Compatible with ATIF v1.7 output schema for downstream tooling?
- Use OpenTelemetry semantics, or invent our own?

### H3. Backend-as-data: thin EnvironmentDriver protocol
**Status:** Real — confirmed by second backend data point
**Evidence:** 
- Docker: `environments/docker/docker.py` is 640 LOC across 9 files. ~75–80% Docker-specific (image sanitization, compose orchestration, volume/mount mgmt, platform detection, Windows/Unix splits). Only 20–25% generic lifecycle.
- Modal: `environments/modal.py` is ~1100 LOC. Breakdown: ~50% backend API plumbing (Modal SDK image/sandbox creation, file ops, exec, Docker Compose construction, retry decorators), ~32% lifecycle methods (start with direct + DinD strategies, stop, teardown, polling), ~18% high-level orchestration.
- Same strategy pattern reused (direct vs DinD in Modal mirrors Docker's compose vs single-container). Both backends reimplement substantial similar logic.
- Multiply by ~15 backends in the repo (`apple_container`, `cwsandbox`, `e2b`, `gke`, `islo`, `modal`, `novita`, `runloop`, `singularity`, `tensorlake`, `wandb`, `daytona`, `docker`).
**Improvement:** Define `EnvironmentDriver` as a 6-method Protocol (`start`, `stop`, `exec`, `upload`, `download`, `set_network_policy`) + a declarative `Capabilities` dataclass. `BaseEnvironment` becomes a non-abstract orchestrator that takes a Driver. New backends are 100–200 LOC adapters, not 500+ LOC subclasses.
**Risk:** Some backends genuinely need lifecycle hooks the protocol doesn't expose (e.g., compose's multi-service teardown). Need an escape hatch (`Driver.optional_hooks()` returning a typed feature set).
**Open questions:**
- Where do healthcheck semantics live — in the Driver or in the orchestrator?
- Network policy: declarative spec interpreted by orchestrator, or driver-implements-policy?

### H4. Decouple publisher AND registry from concrete backends (DIP)
**Status:** Real — scope extended to registry
**Evidence:** 
- `publisher/publisher.py` directly instantiates `SupabaseStorage()` and `RegistryDB()` in `__init__`. No constructor injection.
- `registry/client/` has a `BaseRegistryClient` ABC accepting `url`/`path` params — *interface is abstracted* — but the only concrete implementation appears to assume a database client (Supabase-style), not pluggable HTTP/filesystem.
- Storage layer (`storage/base.py`) is the only fully-abstracted boundary today (Supabase + resumable local).
**Improvement:** Three-part fix:
1. Publisher takes `storage: StorageBackend` and `registry: RegistryClient` via constructor injection.
2. `RegistryClient` gets concrete HTTP and filesystem implementations alongside the Supabase one — useful for self-hosted deployments and air-gapped CI.
3. Consider whether `Publisher` is even needed: `Job` could write to a `ResultSink` protocol directly (sinks = local-FS, S3, registry-HTTP, leaderboard-HTTP). Removes one layer.
**Risk:** Slightly more config surface (users now wire sinks explicitly). Mitigate with sensible defaults.
**Open questions:**
- ResultSink protocol or keep Publisher as a coordinator over multiple sinks?

### H5. Trajectory schema: split raw usage from derived cost
**Status:** Real — but scope narrowed significantly after reading RFC0001 in full
**Evidence (verbatim from RFC0001):**
> "Monetary cost of the API call based on current provider pricing" — formula: `cost_usd = (non_cached_tokens × input_rate) + (cached_tokens × cache_rate) + (completion_tokens × output_rate)`. ATIF stores "actual executed costs, not pricing metadata." Authors acknowledge "pricing can change over time, making historical trajectories inaccurate." Provider-specific charges (e.g., Anthropic cache creation tokens) go in `metrics.extra`.

This is a deliberate snapshot tradeoff Harbor made. Two consequences:
1. Historical trajectory cost is frozen at the rate card in effect when emitted, even if pricing later changed. Can't recompute.
2. Provider-specific charges aren't first-class — they hide in `metrics.extra` with no schema.
**Improvement:** Trajectory step records both:
- **Raw usage** (provider, model, tier, region, input_tokens, cached_input_tokens, cache_write_tokens, output_tokens, thinking_tokens, plus any provider-specific counters as named fields not opaque extras) — frozen forever
- **Derived cost** computed at query time from current/historical rate cards keyed by `(provider, model, tier, date)`

Plus: rate-card table versioned and shipped with Harbor; users can pin or refresh.
**Risk:** Slightly more storage (a few more counters per step). Rate-card maintenance is now Harbor's job; mitigate by sourcing from provider docs in CI.
**Open questions:**
- Snapshot the rate card per trial (heavier) or compute on read (faster ingest, harder reproducibility)? Recommend snapshot rate-card hash + queryable history table.

*Note: The previous audit framed this as "ATIF redesign" with subagents and step granularity also included. After reading RFC0001 in full, those concerns are already addressed in v1.7 — see R2/R3 in Rejected.*

---

## Medium-impact changes

### M1. Per-provider concurrency quotas in the job queue
**Status:** Speculative (likely real)
**Evidence:** `job.py` + `trial/queue.py` — single `asyncio.Semaphore(n_concurrent)` gates all trials. No per-backend, per-region, or per-provider quota. Treats Modal and local Docker identically.
**Improvement:** Layered semaphores — global + per-driver. Config-driven (`concurrency: {docker: 4, modal: 50, daytona: 20}`). Composable.
**Risk:** Low. Mostly additive.
**Open questions:**
- Sufficient, or do we also need rate-limit-aware retries (token-bucket per provider)?

### M2. Adapters as out-of-tree packages
**Status:** Real with caveats — adapter contract is loose, coupling is reproducibility-related
**Evidence (round 2):**
- Adapters are **offline task generators**, NOT runtime plugins. Confirmed: `adapters/swebench/` is a standalone Python package (`pyproject.toml`, `src/swebench_adapter/`, CLI entry `swebench = "swebench_adapter.main:main"`). Invoked as `uv run swebench [--limit N]`, emits task directories to `datasets/swebench-verified/`.
- Output is a standard Harbor task tree: `task.toml`, `instruction.md`, `environment/Dockerfile`, `solution/solve.sh`, `tests/`. No runtime coupling to Harbor.
- `adapter_metadata.json` per generated dataset records parity validation (subset size, sampling rate, seed, agent compatibility) and pins a `registry_commit_sha`.
- `parity_summary.csv` tracks statistical equivalence (mean ± SD) between original benchmark and adapter implementation across multiple runs — a quality contract, not a runtime contract.
**Improvement:** Out-of-tree adapters are already feasible at the packaging level (just `pip install harbor-adapter-X` and run its CLI). What's missing for "marketplace" feel:
1. A discovery manifest (`adapters.toml` in a registry) so users find adapters without trawling PyPI.
2. A `task.toml` schema version pin per adapter (so adapter v1.4 targets Harbor task schema v2.1) — currently the coupling is via `registry_commit_sha` which is fragile.
3. CI that runs `parity_summary.csv` regeneration on each Harbor release to catch drift.
**Risk:** Curation cost. If we don't gate adapter quality, the ecosystem fills with broken ones.
**New finding:** Adapter versioning today pins a `registry_commit_sha` — implicit dependency on Harbor's registry infrastructure even for offline generation. We should use a stable task schema version instead (`task_schema_version = "2.0"`) so adapters survive without our registry running.
**Open questions:**
- Do we want adapters at all in v1 scope, or ship adapters as a v2 concern after the core runtime is solid? (Recommend v2 — bootstrap with one or two hand-written tasks instead.)

### M3. ATIF migration tooling
**Status:** Real (per RFC)
**Evidence:** RFC0001 admits v1.7 broke `SubagentTrajectoryRef` with no automated migration.
**Improvement:** `harbor traj migrate --from v1 --to v2 trajectory.jsonl` as a first-class tool. Encode every schema bump as a forward-only migration function.
**Risk:** Low. Just discipline.
**Open questions:** None.

---

## Low-impact / DX changes

### L1. TrialBuilder to clean up Trial.__init__
**Status:** Modest improvement
**Evidence:** `Trial.__init__` orchestrates 6 distinct init phases (paths, skills, hooks, state flags, components, error cleanup) in synchronous order. The async factory `Trial.create()` already exists alongside it.
**Improvement:** Move init phases into a `TrialBuilder` with explicit dependency order. `__init__` only stores already-constructed deps.
**Risk:** Refactor pain, modest reward. Worth doing only if we hit init complexity issues.

### L2. Verifier discovery via entry points
**Status:** Modest improvement
**Evidence:** Verifiers loaded by import path in config — works fine, just verbose.
**Improvement:** Optional Python entry points (`harbor.verifiers`) for built-in/pip-installed verifiers. Import path still works as fallback.
**Risk:** None.

---

## Rejected ideas

### R1. Verifier-as-agent unification
**Status:** Rejected
**Investigation:** `BaseVerifier.verify()` consumes a finished trajectory/env state and emits `VerifierResult { rewards }`. `BaseAgent.run()` takes an instruction + populates a trajectory. These are genuinely different contracts (producer vs. judge). A common base class would force the wrong abstraction.
**Salvage:** What we *can* unify is the **factory/discovery** layer (L2 above), not the runtime contract. Also, an *LLM-judge verifier* should still exist as a concrete `BaseVerifier` subclass that internally calls an LLM — that doesn't require unifying base classes.

### R2. "ATIF subagent semantics are undefined"
**Status:** Rejected — Harbor v1.7 explicitly addressed this
**Investigation:** RFC0001 verbatim: *"v1.7 redesign separates document-level identity from run-level identity"* — `trajectory_id` uniquely identifies a trajectory document and is required for embedded subagents; `session_id` is run-scoped and shared across parent + sibling subagents, *"NOT a valid resolution key"*. Embedded trajectories live in a `subagent_trajectories` array with independent step numbering. This is well-defined.
**Why I got it wrong:** The round-1 audit paraphrased "no guidance on traversal order, conflict resolution" — but the actual RFC text shows v1.7 *is* the fix for an earlier ambiguity. There's still no documented depth limit and no preorder/postorder traversal spec, but those are minor and can be conventions, not schema changes.

### R3. "Step granularity ambiguous, bad for RL"
**Status:** Rejected — `llm_call_count` semantics are explicit and there's an SFT-aware flag
**Investigation:** RFC0001 verbatim: *"`llm_call_count = 1` (one inference per step); `llm_call_count > 1` (aggregated metrics, per-call attribution unavailable); `llm_call_count = 0` (deterministic dispatch without LLM; metrics and reasoning_content MUST be absent)."* And: `is_copied_context = True` flags steps retained from prior trajectories so *"SFT pipelines exclude them."* Harbor already thinks about training data quality.
**Why I got it wrong:** Round-1 audit framing of "aspirational one-LLM-per-step" was misleading. The spec explicitly allows `>1` for batched/aggregated steps and the flag carries information consumers can use. The right move is to *follow* this convention in our clone, not redesign it.

### R4. "Multimodal path references will rot"
**Status:** Soft-rejected — it's a deliberate tradeoff
**Investigation:** RFC0001 verbatim: *"Images reference external files by relative or absolute path rather than embedding base64 data, avoiding trajectory bloat."* The bloat argument is real (base64 inflates JSONL ~33% and breaks streaming).
**What we could still do:** If we control the storage layer (we do via H4's ResultSink), make image paths *content-addressed* (`/blobs/sha256/<hash>`) instead of free-form. Same on-disk format, but moves/renames don't break references. Cheap upgrade. Filed as part of H4 rather than its own item.

---

## What we should NOT change (working as designed)

Things that look obvious in hindsight but exist for hard-won reasons — keep these as-is:

- **Mutable `AgentContext` during `agent.run()`** — survives timeouts with partial data. (Replaced by H2's event log, which solves the same problem better.)
- **`capabilities.mounted` flag driving different I/O codepaths** — local Docker uses bind mounts (free), remote sandboxes copy. Same trial code, two physical realities.
- **Multi-phase network policy with `_phase_network_policy` context manager** — different policies during agent run vs verifier run is a real requirement.
- **SHARED vs SEPARATE verifier environment modes** — SEPARATE is essential for trustworthy grading; SHARED is essential for cheap dev loop. Both must exist.
- **`asyncio.shield` on env stop** — cancellation must not leave containers running. Hard-won.
- **Hooks (`TrialEvent` enum + async callbacks)** — bake this in from day one. Impossible to retrofit cleanly.
- **`force_build`, timeout multipliers, override timeouts** — job-time knobs over task-defined defaults. Important for ops.
- **`is_copied_context` flag on trajectory steps** — Harbor tags steps copied from prior trajectories so SFT pipelines can exclude them. Adopt this; don't reinvent.
- **`session_id` (run-scoped) vs `trajectory_id` (document-scoped) split from ATIF v1.7** — solves subagent ID collision correctly. Adopt.

---

## Explicitly deferred (not in v1)

### D1. Skills
**Why deferred:** `skills.py` defines skills as local directories containing `SKILL.md`, resolved with a sha256 digest. No registry fetch; purely local-only. Round-2 audit found minimal usage sites in core Harbor — it's infrastructure for a feature that hasn't fully landed. We can ship v1 with a stub (`skills: []` accepted but ignored) and add real resolution in v2 when we know what skills our agents actually need.

### D2. Multi-cloud env backends beyond Docker
**Why deferred:** Each backend is ~500–1100 LOC. v1 with just Docker locally covers >90% of the dev loop. Modal/Daytona/e2b are v2 once H3 (Driver protocol) is in place — adding them should then take ~150 LOC each, not 1000.

### D3. Adapters (the 80+ benchmark converters)
**Why deferred:** Per M2, adapters are offline tools. v1 can ship with 2–3 hand-written tasks for testing. The adapter ecosystem is a v2+ concern requiring marketplace tooling we don't yet have.

### D4. Leaderboard and viewer
**Why deferred:** Pure consumers of TrialResult JSONL. Can be built later as standalone tools against the result format. Not on the critical path.

---

## Open investigation queue
All round-1 and round-2 questions answered. Empty.

Items intentionally left unread (not load-bearing for v1):
- ~13 env backends beyond Docker and Modal — Driver protocol design doesn't need more data points
- Supabase storage/registry implementations — we're replacing them
- Leaderboard, viewer — deferred per D4
- 79 of the 80 adapters — pattern established by spot-checking swebench

---

## v1 scope recommendation
Based on this finalized review, minimum-viable Harbor clone is:
1. **H1** — one `Trial` class with implicit-single-step default
2. **H2** — event-sourced trajectory (`TrajectoryBuilder.append` writing JSONL)
3. **H3 (Docker only)** — `EnvironmentDriver` protocol + one `DockerDriver`
4. **H4 (lite)** — `ResultSink` protocol, with `LocalFSSink` as the only implementation
5. **H5** — split raw usage from derived cost in trajectory schema from day one
6. Adopt: ATIF v1.7 session/trajectory ID split, `is_copied_context`, capability flags, hooks system, `asyncio.shield` cleanup, mounted/non-mounted I/O strategy
7. Defer: skills, multi-backend, adapters, leaderboard, per-provider quotas (M1), TrialBuilder (L1), entry-point discovery (L2), migration tooling (M3)

Target: ~1.5k LOC for v1, single backend, fully tested.

---

## Decision log
Use this section as we make hard calls during implementation.

| Date | Decision | Reasoning |
|------|----------|-----------|
| 2026-06-05 | Document opened with 5 original improvements + 8 from round-1 audit | Initial review |
| 2026-06-05 | Round-2 audit completed; H1 migration path validated, H3 confirmed by Modal, H4 scope extended to registry, H5 narrowed to cost-accounting only, M2 promoted to Real with caveats | Finalization pass |
| 2026-06-05 | Three former improvements rejected after RFC0001 read: R2 (subagents), R3 (step granularity), R4 (multimodal — soft) | ATIF v1.7 already addresses them |
| 2026-06-05 | Skills (D1), multi-backend (D2), adapters (D3), leaderboard (D4) deferred from v1 | Scope discipline; not on critical path |
| 2026-06-05 | v1 scope: H1 + H2 + H3 (Docker only) + H4-lite + H5; ~1.5k LOC target | Smallest version that captures the essence and validates our improvements |
| 2026-06-05 | Round-3 audit completed (concrete agents, verifier zoo, TrialResult schema, network policy plumbing); 4 new warts logged in addendum below | Final contract-validation pass before spec writing |
| 2026-06-05 | All 7 Loom implementation plans written + committed; runtime-core spec is implementation-ready | See `docs/plans/2026-06-05-loom-cross-plan-review.md` |

---

## Addendum — Round-3 audit findings (concrete-contract validation)

Round 3 read Harbor's *concrete* implementations (Oracle/Terminus2/InstalledBase agents, the single concrete Verifier, full TrialResult schema, network policy + healthcheck plumbing) to find warts that only appear when the abstract contracts meet real code. Findings:

### W1. Agent contract is loose on streaming vs batching
**Evidence:** `agents/oracle.py` builds the trajectory post-hoc and assigns in `finally`. `agents/terminus_2/terminus_2.py` streams into `self._trajectory_steps` as it goes. The `BaseAgent` Protocol does not enforce either pattern.
**Loom fix (already in the spec):** Make streaming *emergent* from the architecture rather than enforced on the agent — Gateway-backed LLM calls emit events as they happen; in-box CLIs write to `/loom/trajectory.jsonl` and the host tails. No "step_end check" theater on the Protocol.

### W2. MCP servers are wired as prose injection, not a typed channel
**Evidence:** Terminus2 appends MCP server descriptions into the instruction string (`"MCP Servers:\n..."`), not as a typed field.
**Loom fix:** Spec §2.1 defines `mcp: Sequence[MCPConnection]` as a typed channel passed to `AgentRuntime.run()`. Plan 1 Task 7 implements `MCPConnection` with a transport-aware validator. Plan 3 Tasks 5, 6 thread it through `LiteLLMAgent` and `ClaudeCodeAgent`.

### W3. Verifier interface is anemic
**Evidence:** `harbor/verifier/verifier.py` is the only concrete verifier. `VerifierResult.rewards: dict[str, float|int] | None` — that's it. Failures leak as exceptions (`MissingTestDirError`, `ParsingError`, `VerifierOutputNotFound`).
**Loom fix:** Spec §2.4 defines rich `VerifierResult { rewards, checks: list[CheckResult], confidence, structured, error: VerifierError }`. The `VerifierError` is a struct field, not a raised exception. Plan 1 Task 19 implements; Plan 3 Tasks 7–12 use across five concrete verifiers.

### W4. `TrialResult` has accidental redundancy and orphaned trajectory
**Evidence:** Harbor's `TrialResult` has both `agent_result` and `verifier_result` at the trial level AND in `step_results[0]`. Trajectory is NOT in TrialResult — referenced only by `trial_uri`. Timing fields duplicated at trial + step levels.
**Loom fix:** Spec §4.5 — trial-level `agent_result`/`verifier_result` removed; `steps: list[StepResult]` is the single source of truth. Trajectory pointers (`trajectory_uri`, `atif_uri`, `atif_schema_version`) are first-class fields on `TrialResult`, not orphaned. Plan 1 Tasks 17–18 implement.

### Confirmed by round-3 (no fix needed)

- **H3 Driver protocol scope** — `set_network_policy` and `run_healthcheck` *should* stay on the Driver. Harbor's `TrialNetworkPlan` is pure data resolved by the orchestrator; the *application* of policy is backend-specific (iptables for Docker, provider APIs elsewhere). The Driver decides *how*; the orchestrator decides *when*. Plan 2 Tasks 11–13 implement this split for `DockerDriver`.

These four warts are the final inputs that shaped Spec §2.1, §2.4, §4.5 to their committed form.
