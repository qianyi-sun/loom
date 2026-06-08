# Loom — Harbor Parity Arc Design

**Status:** DRAFT — awaiting user review.
**Date:** 2026-06-08
**Owner:** Hongjian + Claude.
**Scope:** Close every capability gap between Loom and Harbor (https://github.com/harbor-framework/harbor) so Loom is a strict superset. Five sub-projects, executed in priority order. Each sub-project ships its own implementation plan; this document defines the arc, the decomposition, the order, and the success criteria.

---

## 1. Goal

After the service-layer arc (Plans 17–22), Loom is feature-rich for **multi-tenant evaluation + training-data generation**, but loses against Harbor on five concrete axes:

1. No ad-hoc CLI (`harbor run --dataset X --agent Y`); Loom requires the full server stack
2. No dynamic dataset discovery (`harbor datasets list`); Loom has a static importer
3. Not the official Terminal-Bench-2.0 harness; Harbor is canonical
4. No Daytona cloud-execution driver; Harbor scales to "thousands of environments in parallel"
5. No Modal cloud-execution driver; same reason

This arc closes all five. Where Loom is already ahead (multi-tenant teams + quotas, LLM Gateway with rate cards, event-sourced trajectories + ATIF, fenced state PATCH crash semantics, campaigns with idempotency keys, RBAC scopes), we keep that surface intact.

**Definition of done for the arc:** every Harbor workflow has a Loom equivalent that is at least as fast, at least as ergonomic, and reuses Loom's stronger primitives (cost accounting, trajectory schema, multi-tenancy) where Harbor has none.

---

## 2. Why this priority order

Per user direction (2026-06-08): `loom run` CLI + dataset discovery are most important; TB-2 is more important than the runtime gap mention but lower than UX. Cloud backends (Daytona/Modal) are unspecified — placed after TB-2 because they're heavier infrastructure with smaller user-visible impact (researchers can use one cloud backend; they need the CLI before any backend matters).

Each plan builds on the previous:

```
Plan 23 (CLI)
  ↓ exposes --dataset, --agent, --backend flags
Plan 24 (Discovery)
  ↓ makes --dataset list work without code
Plan 25 (TB-2 adapter)
  ↓ proves CLI + discovery work end-to-end on the canonical benchmark
Plan 26 (Daytona driver)
  ↓ first cloud backend; exercises Driver Protocol with second non-trivial impl
Plan 27 (Modal driver)
  ↓ second cloud backend; reuses Plan 26 patterns
```

---

## 3. Sub-projects (priority-ordered)

### Plan 23 — Ad-hoc `loom run` CLI

**Motivation:** Today a researcher needs Postgres + MinIO + Control Plane + Gateway + Worker just to test one trial. Harbor lets them `harbor run --dataset terminal-bench --agent claude-code --concurrency 8` from a laptop. This is the single biggest UX gap.

**Approach:** Stateless one-shot mode that bypasses the server stack entirely. Reuse `Trial.run()` from Plan 3 directly, wire it to local-only impls of every Protocol (FakeObjectStore writing to `./loom-runs/<run-id>/`, FakeControlPlaneClient that no-ops state PATCHes, in-process LLM Gateway client that calls upstream directly), driver factory chosen via `--backend` (docker default; daytona/modal added later).

```
src/loom_cli/                       (NEW package)
  __init__.py
  __main__.py                       # python -m loom_cli  → loom CLI
  run_cmd.py                        # `loom run` orchestrator
  datasets_cmd.py                   # placeholder, populated by Plan 24
  config.py                         # auth / backend config (XDG paths)
  local_runner.py                   # stateless Trial.run() wrapper
  output.py                         # human-readable + --json formats
```

**CLI surface:**

```bash
loom run \
  --dataset humaneval               # dataset slug from registry (Plan 24)
  --task humaneval/HumanEval-42     # OR a single task id
  --agent claude-code               # one of Loom's 11 adapters
  --model claude-opus-4-7           # passed to LLM Gateway
  --backend docker                  # docker | daytona | modal
  --concurrency 8                   # parallel trials
  --output-dir ./runs/              # trajectories + ATIF land here
  --json                            # machine-readable result stream

loom run --help                     # lists agents, backends, datasets
loom config set token <api-key>     # store in ~/.config/loom/config.toml
loom config show                    # current config
```

**Key design choices:**
- **No service required** — the CLI is self-contained. If `LOOM_SERVER_URL` is set, results are *additionally* posted to the CP for org-wide visibility, but absence is fine.
- **LLM calls** — a new `UpstreamDirectGatewayClient` implements the `LLMGatewayClient` Protocol against openai/anthropic/gemini SDKs directly, with cost tracking via a local rate-card file (`~/.config/loom/rate-cards.toml`, seeded from the in-repo defaults). If `--gateway-url` is provided, route through the multi-dialect Gateway for proper cost attribution and rate-card centralization.
- **Trajectories** — written to `./runs/<trial-id>/events.jsonl` + `atif.json` on disk. Same schema as the service deployment so they're cross-compatible.
- **Concurrency** — local asyncio.Semaphore; no DRF in single-tenant mode. Cloud backends naturally scale across machines.

**Success criteria:**
- `pip install loom && loom run --dataset humaneval --agent claude-code` works on a fresh machine in <10 minutes (pulling docker image is the main cost)
- A trial run via CLI produces a trajectory bit-identical (modulo timestamps + UUIDs) to one produced via the server stack
- `loom run --json` output is stable and documented
- Existing service-mode tests still pass (CLI must not regress runtime core)

**Estimated complexity:** 1 plan, ~8–12 tasks. Most of the orchestration code is reuse.

**Dependencies:** none — can start immediately.

---

### Plan 24 — Dataset Discovery (`loom datasets list`)

**Motivation:** Harbor researchers `harbor datasets list` to see what's available. Loom currently requires reading `packages/loom-benchmarks/src/loom_benchmarks/adapters/` to know what exists. The static benchmark adapter set should be discoverable + extensible without rebuilding Loom.

**Approach:** Three discovery sources unioned at query time:

1. **Built-in registry** — entry-points-based: each adapter in `loom_benchmarks` declares itself via `pyproject.toml` `[project.entry-points."loom.benchmarks"]`. The CLI introspects installed entry points. Adding a third-party adapter = `pip install loom-benchmark-xyz`.
2. **Remote registry** — HTTP-fetched index. The arc ships an **in-tree default registry** at `src/loom_cli/registry/default-registry.json` listing the 13 current adapters + TB-2 (post-Plan 25). The registry URL is configurable via `--registry-url` or `LOOM_REGISTRY_URL` env. A hosted version at a stable URL (e.g., GitHub Pages on the loom repo) is a post-arc operational concern, not a Plan 24 deliverable. CLI shows adapters not pip-installed as "available, not installed" and offers `loom datasets install <slug>`.
3. **Local datasets** — datasets already imported into the connected Loom service (if configured). `loom datasets list --remote` queries `/api/v1/benchmarks` on the CP.

**CLI surface:**

```bash
loom datasets list                  # union of all three sources
loom datasets list --installed      # only entry-points
loom datasets list --available      # only remote registry
loom datasets list --remote         # only CP-connected service
loom datasets show humaneval        # details, license, sample count
loom datasets install swe-bench     # pip install loom-benchmark-swe-bench
```

**Output format (text + --json):**

```
SLUG                     SOURCE       LICENSE        TASKS  STATUS
humaneval                builtin      MIT            164    installed
swe-bench-verified       builtin      MIT            500    installed
terminal-bench-2          registry    Apache-2.0     —      available
custom-rl-bench          remote       proprietary    32     remote-only
```

**Key design choices:**
- **Entry points over magic discovery** — predictable, opt-in, testable
- **Remote registry is dumb JSON** — no auth required for read; signed with sigstore for trust (post-v1 hardening)
- **`install` is a thin pip wrapper** — no custom package manager; relies on PyPI
- **Backwards compatible** — current `loom_benchmark_tool import` still works for direct DB ingestion

**Success criteria:**
- `loom datasets list` returns all 13 current adapters via entry points (no hard-coded list in CLI)
- `loom datasets install <slug>` adds a new adapter and `loom datasets list` immediately reflects it
- Remote registry contract documented; a sample registry hosted somewhere
- `loom run --dataset <slug>` works for any source listed

**Estimated complexity:** 1 plan, ~6–10 tasks. Bulk of work is the entry-point migration + remote registry schema.

**Dependencies:** Plan 23 (CLI scaffold). Tightly coupled — could be one big plan, but cleaner as two.

---

### Plan 25 — Terminal-Bench-2.0 Canonical Adapter

**Motivation:** Harbor is the *official* TB-2 harness. For Loom to credibly replace Harbor, researchers must be able to run TB-2 in Loom and publish scores. This is more than a benchmark adapter — TB-2 has a specific protocol (per-task agent containers, terminal interaction model, judging rubric) that Loom's existing adapter framework needs to accommodate.

**Approach:** Three deliverables:

1. **`packages/loom-benchmark-terminal-bench-2/`** — new sibling package. Inherits `BenchmarkAdapter` Protocol from `loom-benchmarks`. Fetches TB-2 dataset from upstream (HuggingFace or GitHub release). Converts each TB-2 task into Loom's `Task` schema: agent container = TB-2's specified base image; verifier = TB-2's judging script wrapped as `ScriptVerifier`; instruction = TB-2's task prompt.
2. **TB-2 compatibility shims in `loom-benchmarks`** — if TB-2's task schema needs primitives Loom lacks (e.g., specific environment variables, secret injection, custom timeouts), extend the adapter Protocol additively. Document drift.
3. **Score reporting parity** — TB-2 reports scores in a specific JSON shape. Add a `loom run --tb2-report` flag that emits the canonical TB-2 result file alongside Loom's native ATIF. Scores must match a sanctioned Harbor run within float precision.

**Validation strategy:** Pick a small TB-2 subset (10 tasks), run via Harbor and Loom, diff the score JSON. Publish the diff as part of the plan's test suite to catch regressions.

**Version pinning:** Plan 25 targets a specific TB-2 dataset commit + judge-script SHA, declared in `loom_benchmark_terminal_bench_2.UPSTREAM_REVISION`. The plan's first task probes the current TB-2 release and pins the SHA in code. Upgrades to a new TB-2 revision are a follow-up plan; CI catches silent upstream drift via a hash-check test.

**Key design choices:**
- **Adapter lives out-of-tree** — `loom-benchmark-terminal-bench-2` is a separate PyPI package, like the other agent adapters. Keeps Loom core lean.
- **Reuse, don't fork, TB-2's judging logic** — wrap the upstream judge script via `ScriptVerifier` (already supports this exact use case). Don't reimplement.
- **Cost accounting** — TB-2 doesn't track LLM cost natively. Loom's Gateway does. This is a Loom-side bonus; surface in the report.
- **Defer "publish to TB-2 leaderboard"** — that requires Harbor team coordination. The adapter unblocks it but doesn't ship the submission flow.

**Success criteria:**
- 10-task TB-2 subset runs end-to-end via `loom run --dataset terminal-bench-2 --agent claude-code`
- Score JSON byte-identical to a Harbor reference run on the same tasks + agent + model (modulo timestamps)
- Adapter passes the existing `test_benchmark_license_happy_path.py` pattern
- Documentation references TB-2 upstream + Loom adapter docs

**Estimated complexity:** 1 plan, ~10–14 tasks. Higher than typical adapter because of protocol-fidelity work.

**Dependencies:** Plans 23 + 24. Without the CLI + discovery, TB-2 has nowhere to plug in.

---

### Plan 26 — Daytona Cloud Driver

**Motivation:** Harbor's headline feature is "thousands of environments in parallel" via Daytona. Loom's Driver Protocol was designed (Plan 2) to allow swapping backends — but only `FakeDriver` and `DockerDriver` exist. Adding `DaytonaDriver` validates the abstraction and unlocks horizontal cloud scale.

**Approach:** Implement the Driver Protocol against Daytona's API (https://daytona.io/docs). The Protocol surface is small: `start(image)`, `exec(command)`, `stop()`, `network_policy` enforcement. Most of the work is auth, image management, and lifecycle.

**SDK probe first.** Plan 26 Task 1 is a non-code probe: stand up a Daytona workspace via their SDK against a test image, document the exact methods used for `create`, `exec`, `delete`, `network`, and confirm assumed semantics (does `exec` stream? are workspaces single-tenant?). If any Driver Protocol assumption breaks, fix it in the Protocol itself before writing the driver — same fix applies to Modal (Plan 27).

```
src/loom_drivers/daytona/           (NEW package, or inside loom-launcher?)
  driver.py                         # DaytonaDriver implementing Driver Protocol
  client.py                         # async httpx wrapper for Daytona REST
  network.py                        # NetworkPolicy → Daytona security groups
  images.py                         # image pull/cache/warm pool
  config.py                         # DAYTONA_* env / config
  exec_stream.py                    # streaming exec output back to caller
```

**CLI integration:**

```bash
loom run --backend daytona \
  --dataset humaneval \
  --agent claude-code \
  --concurrency 100               # now meaningfully parallel
```

**Key design choices:**
- **Driver Protocol is the contract; no new Protocols** — if Daytona forces an abstraction change, that's a Driver Protocol bug we fix in Plan 2's surface, not a Daytona-specific carve-out
- **Auth via Daytona API key** — stored in `~/.config/loom/config.toml` or `LOOM_DAYTONA_API_KEY` env
- **Image caching** — Daytona has its own image registry; cache base images in the user's Daytona workspace for cold-start latency
- **Cost reporting** — Daytona bills compute-seconds; surface this through Loom's existing `usage` rollup so cloud spend shows up alongside LLM spend
- **NetworkPolicy** — Daytona supports egress rules; map Loom's `NetworkPolicy.Allowlist` → Daytona security group. `Public` policy = unconstrained
- **Cleanup** — `stop()` deletes the Daytona workspace; finalizer ensures no orphans on cancel

**Success criteria:**
- `loom run --backend daytona` runs 100 parallel humaneval trials in <5 minutes (Daytona pool warm)
- A trial run via `daytona` is byte-equivalent in trajectory + ATIF to one run via `docker` (same agent, same task, same model)
- NetworkPolicy enforcement verified: an allowlist-restricted trial cannot egress to a non-allowlisted host
- Cost integration: Daytona compute-seconds appear in `loom_service` `/usage` rollup

**Estimated complexity:** 1 plan, ~12–18 tasks. Cloud driver work is mostly auth + lifecycle + retry plumbing.

**Dependencies:** Plan 23 (so `--backend` flag exists). Plan 24 helpful but not strictly required. TB-2 (25) parallelizes well — these can interleave.

---

### Plan 27 — Modal Cloud Driver

**Motivation:** Harbor supports Modal in addition to Daytona. Modal's model differs (serverless functions, container snapshots, GPU options) and is often cheaper for bursty workloads. Different users prefer different cloud backends; full Harbor parity requires both.

**Approach:** Mirror Plan 26 structure. Plan 27 Task 1 is again a non-code SDK probe (Modal's SDK is sync, so confirm the executor-bridge pattern works before writing the full driver):

```
src/loom_drivers/modal/             (NEW package)
  driver.py                         # ModalDriver implementing Driver Protocol
  client.py                         # modal-client wrapper (their SDK is sync; bridge)
  images.py                         # Modal Image management
  ...
```

**CLI integration:**

```bash
loom run --backend modal ...
```

**Key design choices specific to Modal:**
- **Sync SDK bridging** — Modal's Python SDK is sync; wrap in `run_in_executor`. Don't fight their abstraction
- **Snapshots over images** — Modal supports container snapshots for fast cold starts. Use for hot-path agent containers
- **GPU support (bonus)** — Modal makes GPU trials easy. Surface `--gpu` flag in CLI even though the docker driver doesn't support it (capability detection)
- **Cost reporting** — same pattern as Plan 26 (Modal exposes per-call cost)

**Success criteria:**
- `loom run --backend modal` parity with Daytona for the canonical test (100 humaneval, <5 min)
- GPU trial via `loom run --backend modal --gpu` runs an agent that needs CUDA (validates the capability)
- Cost in `/usage` rollup
- Driver Protocol unchanged between Plans 26 and 27 (the proof that the abstraction holds)

**Estimated complexity:** 1 plan, ~10–14 tasks (lighter than Plan 26 because Driver Protocol is now battle-tested).

**Dependencies:** Plan 26 (validates the Driver Protocol shape for cloud).

---

## 4. Cross-cutting concerns

### 4.1 Single-binary install story

After Plan 23, the recommended install is:

```bash
pip install loom
loom run --dataset humaneval --agent claude-code
```

This means `loom` (the CLI) must work on PyPI as a single package that pulls in the right dependencies. Action items folded into Plan 23:
- Top-level `loom` package on PyPI re-exports the CLI entry point
- `loom-runtime`, `loom-benchmarks`, `loom-launcher` published separately as today
- `loom[full]` extra installs cloud drivers + all benchmark adapters

### 4.2 Backwards compatibility

The arc adds capabilities; it does not change the server-mode surface. Existing Plans 17–22 service deployments continue to work unchanged. The CLI is additive.

The one exception: Plan 24's entry-point migration changes how `loom_benchmark_tool import` discovers adapters. We ship a compat shim so the existing import command keeps working through one deprecation cycle.

### 4.3 Documentation gap closure

Each plan ships:
- Updated `README.md` for the relevant package
- A section in the operator runbook (`docs/`)
- A migration note in `CHANGELOG.md` under [Unreleased]

After Plan 27 ships, the README should answer: "Why use Loom over Harbor?" with concrete bullet points.

### 4.4 Skipped from this arc (intentionally)

- **Apps/Viewer parity** — Loom's SPA (11 pages) already covers what Harbor's viewer shows
- **Submission to TB-2 leaderboard** — requires Harbor team coordination; standalone follow-up
- **Skills system** (D1 deferred from runtime-core review) — Harbor doesn't have Skills either; not a parity gap
- **Multi-cloud beyond Daytona/Modal** — AWS Bedrock, GCP Cloud Run, etc. are post-arc
- **OAuth / SSO for the CLI** — out of scope; CLI uses bearer token same as service

---

## 5. Risks + open questions

1. **Daytona / Modal API stability** — both are venture-backed startups whose APIs may evolve. Plans 26+27 should pin SDK versions and document the supported range.
2. **TB-2 protocol fidelity** — if upstream TB-2 changes its judging script shape, Plan 25's adapter needs prompt updates. Mitigation: pin the TB-2 dataset version Loom targets.
3. **Single-package install footprint** — the `loom[full]` install with Daytona + Modal SDKs + 13 benchmark adapters could be 500 MB+. Decompose extras carefully (`loom[daytona]`, `loom[modal]`, `loom[bench-tb2]`).
4. **Cost-tracking precision** — cloud driver cost telemetry depends on the provider's API. If Daytona/Modal don't surface per-trial cost in real time, fall back to wall-clock × hourly rate from a config file.
5. **CLI authentication UX** — for unauthenticated `loom run` (laptop, no server), the user still needs upstream LLM API keys. Decide if we store them in CLI config or require env vars. Suggested: env vars only (no key persistence; matches industry norm).

---

## 6. Out-of-arc but worth noting

After this arc closes, Loom is a strict superset of Harbor. To go *further*:

- **First-party RL training loop** — Loom's campaigns already collect rollouts. Adding a `loom train` command that integrates with TRL or Open-Instruct would close the gap to "RL platform"
- **Verifier marketplace** — third-party `Verifier` implementations as out-of-tree packages, mirroring the launcher pattern
- **Leaderboard service** — separate microservice that ingests Loom ATIF + Harbor TB-2 reports and shows cross-platform comparison

These are post-Harbor-parity opportunities, not gaps.

---

## 7. Approval checkpoints

This arc spec is the first checkpoint. Future checkpoints:
- After Plan 23 ships: validate CLI UX against Harbor's UX side-by-side. Adjust priority of later plans if needed.
- After Plan 25 ships: run Loom + Harbor on a shared TB-2 subset and publish the diff. If diff > tolerance, pause cloud-driver work and fix.
- After Plan 27 ships: tag `loom-harbor-parity-v0.27` and update the project memory + README.

---

## 8. Decision log

| Decision | Rationale |
|---|---|
| Five plans, sequenced (not one mega-plan) | Each is independently shippable; each builds on the previous; reviewable in isolation |
| CLI before cloud drivers | CLI is the entry point; cloud drivers have nothing to plug into without it |
| TB-2 before cloud drivers | TB-2 is the canonical Harbor benchmark; without it the parity claim is weak |
| Entry-points for discovery | Standard Python pattern; opt-in; no magic; testable |
| Stateless CLI mode (no server) | Matches Harbor UX; reuses Trial.run() so no runtime forking |
| Out-of-tree TB-2 adapter | Keeps Loom core lean; same pattern as agent/benchmark adapters |
| Defer Skills, leaderboard submission, GPU drivers (beyond Modal) | Not Harbor-parity gaps; YAGNI for this arc |
