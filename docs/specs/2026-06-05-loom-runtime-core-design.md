# Loom — Runtime Core Design (v1)

**Status:** 🚧 DRAFT — in-progress brainstorm. Approved sections move into "Locked-in design" below as we go. Final polish + spec self-review happens once all sections are signed off.
**Date opened:** 2026-06-05
**Owner:** Hongjian + Claude (brainstorm)
**Scope:** Runtime core only — the Trial / Agent / Driver / Verifier / Trajectory primitives plus the minimum control plane + worker fabric to run them distributed. Service layer (FastAPI for teams, dashboard, RBAC), benchmark integrations (SkillFlow/SkillLearnBench), and agent integrations (Claude Code etc.) are *separate specs* and not in this scope.
**Companion document:** `notes/harbor-design-review.md` — captures the architectural improvements identified vs. Harbor; many decisions here flow from it.

---

## Decisions locked in (running log)

| Date | Decision | Reasoning |
|------|----------|-----------|
| 2026-06-05 | Project name: **Loom**. Repo directory rename deferred (memory paths still reference `agentic-data-platform`). | User selected; nautical-adjacent, weaving = trajectory semantics. |
| 2026-06-05 | Primary success criterion: training-data generation AND eval, both first-class in v1 | User choice; SkillFlow/SkillLearnBench are skill-learning, training data quality is non-negotiable |
| 2026-06-05 | Agent location: hybrid — out-of-box default, in-box opt-in for agents that need to live inside the sandbox | User choice; preserves flexibility for legacy CLIs while keeping the clean default |
| 2026-06-05 | Scale model: multi-node, control plane + workers from day one | User choice; aligns with existing platform direction and avoids v1.5 rewrite |
| 2026-06-05 | Trust model: internal teams, mutually trusting | User choice; no gVisor / supply-chain enforcement in v1 |
| 2026-06-05 | LLM access: hybrid — LiteLLM-backed gateway by default; direct SDK with instrumentation as opt-in | User choice; gateway is the bulletproof attribution path |
| 2026-06-05 | Trajectory schema: internal event-sourced JSONL + ATIF v1.7 projection at finalize | User choice; evolve internally, interop externally |
| 2026-06-05 | Architecture approach: Pipelined Trial, distributed-from-day-one (Approach A) | User choice over library-first (B) and OTEL-native (C) |
| 2026-06-05 | Section 1 (Architecture) approved | Three tiers, Postgres-only state, MinIO for trajectories, LLM Gateway as sibling, no Redis, fairness via DRF SQL, fail-and-retry on worker crash |
| 2026-06-05 | Section 2 (Components & Contracts) approved with 15 explicit decisions | See end of §2 for the list; key calls: AgentRuntime Protocol minimal (run only), Driver = 7 methods ≤300 LOC, rich VerifierResult w/ structured error field, DRF-via-SQL scheduler, 6-state FSM w/ failure_reason enum, worker fencing on state updates, source-state-aware cancellation |
| 2026-06-05 | §2 addendums (Driver idempotency contracts, verifier file-based output pattern) added during §3 self-review | Did not contradict any prior §2 decision; labeled inline as "addendum from §3 self-review" |
| 2026-06-05 | Section 3 (Data Flow) approved with 19 explicit decisions | Key calls: requires_caps derived by Control Plane, claim is HTTP poll, finalize+state-PATCH always run (even cancel), local-first trajectory durability, flush triggers 1MB/100ev/10s, ATIF projection from local file, verifier_env_mode shared default, terminal-fail returns normally, orphan trajectory sweep on worker startup, heartbeat on dedicated OS thread, worker_id assigned at register, POSIX glob artifacts Linux-only v1, hard timeouts on finalize (60s) + state PATCH (15s), shutdown ordering stops heartbeat before drain |
| 2026-06-05 | Section 4 (Data Model) self-reviewed and approved with 11 explicit decisions | task.toml as on-disk format; tasks content-addressed via dirhash; implicit single-step synthesis; raw usage + derived cost via rate_card_hash; provider_extras as named int counters; TrialResult.steps is source of truth (no trial-level redundancy); MinIO data plane / Postgres index plane; (trial_id, step_id, seq) ordering; ATIF projection as pure function; rate_cards admin-managed; excerpt strategies first-class |
| 2026-06-05 | Section 5 (Error Handling) self-reviewed and approved with 8 explicit decisions | Phase-local timeout classification; semantic errors not retry-eligible by default; worker-crash reclaim doesn't consume attempt budget; continue-on-step-failure default; verifier soft-failure via result.error (not exception); error-contribution-as-zero in aggregation; system errors via OTEL/Prometheus (not trajectory); workers degrade gracefully on Control Plane outage |
| 2026-06-05 | Section 6 (Testing Strategy) self-reviewed and approved with 8 explicit decisions | 5 tiers (unit/contract/integration/E2E/property); real PG+MinIO via testcontainers; real Docker only in E2E; LLM Gateway fake; E2E on separate label; ≥90% coverage on loom/; 3 state-machine invariants in property tests; perf is v1.1 |
| 2026-06-05 | Section 7 (Cross-cutting Concerns) added and approved with 7 explicit decisions | Surfaced during whole-spec self-review as a real gap. Alembic for migrations; structured JSON logs to stdout with correlation IDs; enumerated Prometheus metrics with bounded cardinality; LLM API keys live only in Gateway; pydantic-settings v2 config; `/metrics` on 9090; lint rules forbid print/f-string-logs/inline-secrets |
| 2026-06-05 | Inline addendums during whole-spec review: MCPConnection model_validator, OracleAgent as concrete production utility, solution/ dir in task layout, ATIF llm_call_count > 1 message-handling explicit, RequiredCapabilities relationship documented | Closed real ambiguities surfaced by reading the whole spec critically; no contradictions with prior decisions |
| 2026-06-05 | Final spec self-review pass — internally consistent (12-claim cross-section table), scope-appropriate (7 sections, 77 decisions, ~2,200 lines), no placeholders, no load-bearing ambiguity | Spec self-approved; ready to commit + invoke writing-plans per the user's "review yourself until you approve and continue" directive |

---

## Open items after spec completion

All initially-deferred sections (§2–§6) are written and approved. What remains:

**Belongs to other specs (not v1 runtime core):**
- `loom` CLI surface design (`loom trial run`, `loom worker start`, `loom traj fetch`, `loom cost_replay`, etc.) — small standalone spec or folded into the service layer spec.
- Docker registry / image distribution policy (where sandbox images live, build cache, auth) — ops doc.
- Worker version compatibility matrix (forward/backward) — ops doc.
- Service layer (FastAPI team-facing API, dashboard, RBAC) — task #3 spec.
- Benchmark integrations (SkillFlow, SkillLearnBench) — task #2 spec.
- Agent integrations (Claude Code shim, OpenHands shim) — task #1 spec.

**v1.5 / v2 explicit deferrals already noted inline:** webhook notifications, SSE live tail, streaming exec, large-artifact streaming upload, fallback for non-Loom-aware in-box CLIs, automatic re-upload of orphaned local trajectories, subprocess isolation per trial, durable mid-step resume, cross-team priority preemption, elastic worker scaling, additional sandbox backends.

---

## Section 1 — Architecture ✅ APPROVED 2026-06-05

### Three tiers

```
                  ┌──────────────────────────────┐
                  │       LLM Gateway            │
                  │  (LiteLLM, OAI-compatible)   │
                  │  • fans out to providers     │
                  │  • captures req/resp/tokens  │
                  │  • emits trajectory events   │
                  │    keyed (team, trial, step) │
                  └──────────────┬───────────────┘
                                 │ HTTP
       ┌──────────────────────── │ ────────────────────────────────┐
       │                Loom Control Plane (FastAPI)                │
       │                                                             │
       │  ┌──────────────┐  ┌──────────────────────────────────┐    │
       │  │ Submit API   │  │   State Store (Postgres)         │    │
       │  │ /trials      │  │  • trial FSM rows                │    │
       │  │ /jobs        │  │  • workers table (TTL heartbeat) │    │
       │  │ /results     │  │  • teams, tasks, agents registry │    │
       │  │ /workers     │  │  • trajectory_index (file-ptrs)  │    │
       │  │ /events:tail │  │  • queue (SKIP LOCKED selection) │    │
       │  └──────────────┘  └──────────────────────────────────┘    │
       │                                                             │
       │  • signed-URL minting for direct MinIO uploads              │
       │  • bearer-token auth (workers + teams)                       │
       │  • fairness scheduling: weighted-fair per team in queue      │
       └──────────────┬───────────────────────────┬─────────────────┘
                      │ HTTP (claim, state PATCH) │ HTTP (index update)
                      ▼                           │
            ┌──────────────────┐                  │
            │     Worker       │                  │
            │  • poll claim    │                  │
            │  • TrialRunner   │                  │
            │  • emit events ──┼─── signed PUT ───┼──► MinIO
            │  • heartbeat     │                  │    artifacts/
            │  • stream state  │                  │    trajectories/
            └────────┬─────────┘                  │    env-snapshots/
                     │ DriverProtocol             │
                     ▼                            │
            ┌──────────────────────────────┐      │
            │  Sandbox via Driver          │      │
            │  (v1: DockerDriver only)     │      │
            │  start/stop/exec/upload/     │      │
            │  download/set_network_policy │      │
            │  /healthcheck                │      │
            └──────────────────────────────┘      │
                                                   ▼
                                          ┌──────────────────┐
                                          │  MinIO (S3-API)  │
                                          │  trajectories as │
                                          │  append-only     │
                                          │  JSONL files     │
                                          └──────────────────┘
```

### Tier responsibilities (one sentence each)

- **LLM Gateway** is a sibling service (independent scaling, independent failure domain) that proxies all model calls and is the *primary trajectory event emitter* for everything LLM-shaped. Operationally: one logical service, replicable behind a load balancer.
- **Control Plane** is the index manager + scheduler + auth boundary. It owns the trial state machine, the queue, the worker registry, and the trajectory index — but **not** the trajectory payloads. Stateless across instances (all state in Postgres + MinIO), so it scales horizontally behind a load balancer.
- **Workers** are stateless pollers that claim trials, run them in-process (asyncio, 5–20 concurrent trials each), and stream events directly to MinIO with index updates back to Control Plane.
- **Driver** is the thin Protocol that workers use to talk to a concrete sandbox. v1 ships **only `DockerDriver`** (~300 LOC target). Modal / k8s / e2b are v2.

### Critical boundaries and rules

- **Control Plane is the writer for *state*, not *data*.** Trial FSM transitions and index rows go through Control Plane HTTP endpoints (one writer, one set of invariants). Trajectory payloads and artifacts are written by workers directly to MinIO via signed URLs; Control Plane only receives a small index update at flush time (file path + offset + checksum + event count).
- **No Redis.** Postgres handles the queue (`SELECT ... FOR UPDATE SKIP LOCKED` with a fairness priority column) and heartbeats (workers PATCH a TTL column every 5s; expired workers' in-flight trials are reclaimed). At our scale (stakeholder groups, ~1k trials/day) Postgres is the obvious right call. We can add Redis later if a real bottleneck appears, not before.
- **LLM Gateway is a peer, not a child.** Workers and (in-box) agents both POST to the Gateway. Gateway emits trajectory events directly to MinIO using the same signed-URL pattern as workers, attributing each event to the `(team, trial_id, step_id)` triple in the request.
- **Worker has two modes** (full design in Section 2): out-of-box (agent process lives on the worker; uses Driver to drive sandbox) and in-box (agent CLI lives in the sandbox; worker monitors + collects). Same Trial state machine; different `AgentRuntime` impl.
- **Trial-on-worker is atomic in v1.** If a worker dies mid-trial, the heartbeat expires, the trial is reclaimed by the queue, and the retry policy decides: restart from scratch (v1) or resume from last step (v2). Multi-step durable resumption is a v2 concern — it requires sandbox state checkpointing we're not designing yet.
- **Fairness scheduling lives in the queue.** Each `trials` row has `team_id` + `submit_priority`. The claim query orders by `(weighted_fair_key(team_id), submit_priority, submitted_at)` so no single team can starve others. Mechanism detail in Section 2.
- **Auth:** bearer tokens for workers (issued at worker provisioning) and teams (issued via the platform's existing auth surface). Internal trust — no per-tenant secret isolation in v1, no audit log requirements beyond the trajectory record itself.

### What's explicitly NOT in Section 1 (named so we don't pretend)

- **Live dashboard streaming.** v1 dashboards poll the trajectory file or the trajectory_index every 5s. SSE / WebSocket `GET /events:tail` is *designed for* (endpoint reserved) but not implemented in v1.
- **Elastic worker scaling.** v1 workers are deployed manually or via k8s Deployment. Auto-scaling is v2.
- **System observability of the runtime itself.** OpenTelemetry traces of Control Plane and Worker (not trial trajectories) are a cross-cutting concern handled in a later section.

### Failure model in one line

Control Plane is the source of truth for state; trajectory and artifact storage is direct-to-MinIO with the Control Plane as index manager; everything below the worker is ephemeral; crashed work is reclaimed via heartbeat expiry and re-queued under the retry policy.

---

## Section 2 — Components & Contracts ✅ APPROVED 2026-06-05

This section locks in the core Protocols + the contracts every Loom component must respect. All type sketches use Python 3.12 syntax for clarity; final implementations may differ in surface but must preserve the contract. *Revised after a second self-review pass; the changes are noted inline where load-bearing.*

### 2.1 `AgentRuntime` — the hybrid mode design

One Protocol with the minimum surface. **`setup()` is only on the in-box base** because out-of-box agents have nothing to install. The Protocol does not enforce streaming semantics — streaming is emergent from the architecture, not an agent obligation.

```python
class AgentRuntime(Protocol):
    """One trial's worth of agent execution. Owns LLM calls, tool dispatch,
    and trajectory emission."""

    mode: Literal["out-of-box", "in-box"]
    name: str
    version: str
    supports_os: frozenset[OS]
    model: ModelSpec                       # which LLM the agent calls; resolved per-trial

    async def run(
        self,
        instruction: str,
        env: Driver,
        trajectory: TrajectoryWriter,
        mcp: Sequence[MCPConnection],      # TYPED channel
        skills_dir: PurePosixPath | None,
    ) -> None: ...

class InBoxAgentRuntime(AgentRuntime, Protocol):
    """In-box agents additionally need installation in the sandbox."""

    async def setup(self, env: Driver) -> None:
        """Install agent CLI and wire MCP/skills paths inside the sandbox."""
```

**Why streaming is emergent, not enforced:** Out-of-box agents make LLM calls via the Gateway, which emits events as calls happen. In-box Loom-aware CLIs emit to `/loom/trajectory.jsonl` which the host tails. We cannot tell from outside an `async def run(): ...` whether events arrived in real time or were buffered — checking "did `step_end` arrive?" is theater. The architecture provides streaming; the Protocol shouldn't pretend to.

Two concrete bases ship in v1, plus one production utility:
- `OutOfBoxAgentRuntime` → `LiteLLMAgent` — generic tool-loop using LiteLLM via the Gateway. All future out-of-box agents specialize this.
- `InBoxAgentRuntime` → `ClaudeCodeAgent` — Loom-aware via Claude Code's hook system, emits JSONL to `/loom/trajectory.jsonl`.
- `OutOfBoxAgentRuntime` → `OracleAgent` (utility) — runs `solution/solve.sh` from the task directory inside the sandbox; no LLM calls; deterministic upper bound. Emits a single `step_start`/`step_end` pair with an `env_exec` event for the solve script. Used in CI as a baseline and by `loom trial run --agent oracle` for sanity checks.

**In-box CLIs in v1 must be Loom-aware.** No fallback for non-aware CLIs. (Previous draft included a "Gateway-only event capture with warning" fallback; removed because it produces silently-inferior trajectories that look real but are missing tool-use and env-effect events. If a real legacy CLI shows up, fallback can be added in v1.5.)

### 2.2 `Driver` Protocol — what every backend implements

```python
class Driver(Protocol):
    capabilities: Capabilities
    os: OS

    async def start(self, *, force_build: bool) -> None: ...
    async def stop(self, *, delete: bool) -> None: ...

    async def exec(
        self,
        cmd: str,
        *,
        user: str | int | None = None,         # REQUIRED per-call; NO default-state on Driver
        cwd: PurePosixPath | None = None,
        env: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
    ) -> ExecResult: ...

    async def upload(self, src: Path, dst: PurePosixPath) -> None: ...
    async def download(self, src: PurePosixPath, dst: Path) -> None: ...

    async def set_network_policy(self, policy: NetworkPolicy) -> None: ...
    async def run_healthcheck(self, hc: HealthcheckSpec | None = None) -> None: ...
```

7 methods, **no stateful context manager**. Each `exec(...)` is self-contained — across 5–20 async-concurrent trials on a worker, no shared mutable state on the Driver. (Previous draft had `with_default_user(user)` as a stateful context manager; removed because it isn't safe under concurrent reentrancy. The convenience of "use root for this block" lives as a higher-level wrapper.)

**Idempotency and ordering contracts (addendum from §3 self-review):**
- `stop()` MUST be safe to call (a) before `start()` ever succeeded, (b) after `start()` succeeded, (c) multiple times. Callers in §3 invoke `stop()` from `finally` blocks where `start()` may have failed; the driver must absorb this without raising.
- `start()` MUST be safe to call exactly once per Driver instance. Re-`start()`ing an already-started driver raises `DriverAlreadyStartedError`. Workers construct one Driver per trial and discard it after `stop()`.
- `exec()` and `upload`/`download` MUST raise `DriverNotStartedError` if called before `start()` or after `stop()`.

`ExecResult`:
```python
@dataclass(frozen=True)
class ExecResult:
    return_code: int
    stdout: bytes               # capped at MAX_EXEC_STREAM_BYTES (default 10 MB)
    stderr: bytes               # capped at MAX_EXEC_STREAM_BYTES (default 10 MB)
    truncated: bool             # True if either stream hit the cap
    duration_sec: float
```

Hard cap prevents OOM from runaway commands. Streaming exec deferred to v1.5; v1 buffers each command.

Network policy and healthcheck stay on the Driver because their *implementations* are backend-specific (iptables for Docker, provider API for Modal). Orchestrator decides *when* to call them; driver decides *how*.

**New driver LOC budget: ≤300 LOC.** v1 ships `DockerDriver` only.

### 2.3 `Capabilities`

```python
@dataclass(frozen=True)
class Capabilities:
    os: OS                                                # scalar: "linux" | "windows"
    gpu_vendor: GPUVendor                                 # scalar: "none" | "nvidia"
    network_policies: frozenset[NetworkPolicyKind]        # ⊆ {public, no-network, allowlist}
    dynamic_network_policy: bool                          # can switch post-start
    mounted_fs: bool                                      # bind mounts available
    resource_modes: frozenset[ResourceMode]               # ⊆ {auto, limit, guarantee}
```

Scalar fields match the SQL claim query (see 2.6). Set-valued fields serialize as JSON arrays.

Changes from previous draft:
- `os` and `gpu_vendor` are now **scalar**, not `frozenset` — this matches Postgres claim-query semantics.
- `resource_modes` trimmed from 5 (Harbor's `auto, ignore, request, limit, guarantee`) to 3 — we don't need `ignore` (silent failure) or `request` (Kubernetes-only).
- GPU matching is **vendor-only in v1**. A100 vs H100 (and memory size) is v2.

A worker advertises a list of `Capabilities` (one row per backend configuration it can construct). The scheduler matches each axis explicitly — see 2.6.

**`RequiredCapabilities`** is the trial-side counterpart — same field shape as `Capabilities` (scalar `os`, `gpu_vendor`, `mounted_fs`; set-valued `network_policies`), but interpreted as a *requirement* rather than an offering. The scheduler matches `RequiredCapabilities ⊆ Capabilities` per worker row. Persisted as JSONB in `trials.requires_caps`.

### 2.4 `Verifier` Protocol — fixes Harbor's anemic result

```python
class Verifier(Protocol):
    name: str

    async def verify(
        self,
        task: Task,
        env: Driver,
        artifacts_dir: PurePosixPath,
        trajectory: TrajectoryReader,      # finished trajectory, read-only
    ) -> VerifierResult: ...

@dataclass(frozen=True)
class VerifierResult:
    rewards: dict[str, float]              # e.g. {"passed": 1.0, "pytest_pass_rate": 0.83}
    checks: list[CheckResult]              # per-check breakdown
    confidence: float | None               # 0.0–1.0; for LLM judges
    structured: dict[str, Any] | None      # schemaless extras
    error: VerifierError | None            # structured failure, NOT exception

@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    score: float | None
    message: str | None
    duration_sec: float | None

class Aggregator(Enum):
    MEAN = "mean"
    MIN = "min"
    MAX = "max"
    WEIGHTED = "weighted"

class AggregatorFn(Protocol):
    def __call__(self, results: list[VerifierResult]) -> VerifierResult: ...
```

v1 concretes: `PytestVerifier`, `ScriptVerifier`, `LLMJudgeVerifier`, `StructuredOutputVerifier`, `CompositeVerifier(verifiers, aggregator)`.

`CompositeVerifier.aggregator: Aggregator | AggregatorFn` — built-in enum or custom callable.

**Open question (resolved in Section 4):**
- `LLMJudgeVerifier` needs to send a *trajectory excerpt* to the LLM Gateway. The excerpt strategy (full / last-N-steps / tool-calls-only / configurable) and `TrajectoryReader`'s materialization (stream from MinIO / local cache / Control Plane proxy) are deferred to Section 4 once the trajectory storage format is locked in.

`PytestVerifier` requires `pytest` + `pytest-jsonreport` in the verifier env. v1 ships a base verifier image with both pre-installed; tasks that override the image must include them.

**Verifier output pattern (addendum from §3 self-review):** verifiers MUST write their structured output to a file inside the sandbox (e.g., `/loom/verifier/report.json` or `/loom/verifier/junit.xml`), then `driver.download()` the file and parse it from local disk. They MUST NOT parse `ExecResult.stdout` for structured data — the 10 MB stdout cap will truncate long runs mid-document. Free-form logging via stdout/stderr is fine; structured results go through files. Concrete verifiers:
- `PytestVerifier` → `pytest --junitxml=/loom/verifier/junit.xml --json-report --json-report-file=/loom/verifier/report.json`
- `ScriptVerifier` → contract: script writes JSON to `$LOOM_VERIFIER_OUTPUT` (env var pointing at `/loom/verifier/output.json`)
- `LLMJudgeVerifier` → constructs prompt, sends through Gateway, writes response to `/loom/verifier/judge.json`
- `StructuredOutputVerifier` → validates the artifact files directly (no exec needed)

### 2.5 Trial composition

```python
@dataclass
class Trial:
    id: UUID
    task: Task
    agent: AgentRuntime
    driver: Driver
    verifier: Verifier
    config: TrialConfig
    trajectory: TrajectoryWriter

    async def run(self) -> TrialResult: ...
```

**No `SingleStep`/`MultiStep` subclasses.** Tasks without `steps` synthesize `[Step(name="main", ...)]` at parse time. Collapses Harbor's ~1000 LOC into ~400.

**`Trial.run()` body is deferred to Section 3 (data flow)** — env start → per-step (prepare → agent → artifacts → verifier) → finalize → ATIF projection → index update. Trial owns the `TrajectoryWriter` lifecycle via `async with` so a crash or cancellation still flushes events.

### 2.6 Fairness scheduler — DRF over scalar capabilities

Each `trials` row stores `requires_caps` as a JSONB object with scalar fields matching `Capabilities`. The claim query matches each axis explicitly (rewritten from previous draft, which used ambiguous `<@` containment without specifying the schema):

```sql
WITH next AS (
  SELECT t.id
  FROM trials t
  JOIN team_quotas q ON q.team_id = t.team_id
  WHERE t.state = 'queued'
    AND t.attempt_count < q.max_attempts
    AND (t.next_attempt_at IS NULL OR t.next_attempt_at <= NOW())
    AND t.requires_caps->>'os'         = ANY(:worker_os)
    AND t.requires_caps->>'gpu_vendor' = ANY(:worker_gpu_vendors)
    AND (t.requires_caps->'network_policies') <@ (:worker_network_policies)::jsonb
    AND (t.requires_caps->>'mounted_fs')::bool = ANY(:worker_mounted_fs_options)
  ORDER BY
    (q.in_flight_count * 1.0) / NULLIF(q.fair_share_weight, 0) ASC,  -- DRF
    t.submit_priority DESC,
    t.submitted_at ASC
  LIMIT 1
  FOR UPDATE OF t SKIP LOCKED
)
UPDATE trials t
SET state='claimed', worker_id=:worker_id, claimed_at=NOW(),
    attempt_count = attempt_count + 1
FROM next WHERE t.id = next.id
RETURNING t.*;
```

Changes from previous draft:
- **Scalar cap matching** per axis, not a single ambiguous `<@`.
- **`q.in_flight_count` is a materialized column** on `team_quotas`, updated by a trigger on `trials.state` transitions — avoids per-row recomputation under load.
- **`attempt_count` + `next_attempt_at`** support retry semantics (see 2.8).

`team_quotas.in_flight_count` is maintained by a trigger on `trials.state`. The transition matrix is symmetric — *any* flip between "in-flight" and "not in-flight" adjusts the counter, including reclamation `(claimed|running) → queued`:

```sql
CREATE OR REPLACE FUNCTION trials_inflight_delta() RETURNS TRIGGER AS $$
DECLARE
    was_active boolean := OLD.state IN ('claimed', 'running');
    is_active  boolean := NEW.state IN ('claimed', 'running');
BEGIN
    IF was_active AND NOT is_active THEN
        UPDATE team_quotas SET in_flight_count = in_flight_count - 1
         WHERE team_id = OLD.team_id;
    ELSIF is_active AND NOT was_active THEN
        UPDATE team_quotas SET in_flight_count = in_flight_count + 1
         WHERE team_id = NEW.team_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trials_inflight_count
    AFTER UPDATE OF state ON trials
    FOR EACH ROW EXECUTE FUNCTION trials_inflight_delta();
```

Covers all flips: forward claim, reclamation back to `queued`, retry re-claim, terminal transitions.

Priority semantics: within a team, higher `submit_priority` wins; across teams, DRF dominates. No cross-team priority preemption in v1 (v2 once we know whether it's needed).

### 2.7 Auth — bearer tokens with security baseline

| Token type | Scopes | Issued by | TTL | Revocation |
|---|---|---|---|---|
| Worker | `worker:claim`, `worker:report`, `worker:index` | Admin via `POST /admin/worker-tokens` at provision | 90 days default; renewable | `DELETE /admin/worker-tokens/{token_hash_prefix}` |
| Team | `submit`, `read:own`, `read:team` | Platform auth surface (existing) | 90 days default | Per platform conventions |
| LLM Gateway | reuses requesting token (worker OR team) | n/a | n/a | revocation cascades from source |

Format and storage (new from previous draft):
- Tokens are **32 random bytes** (base64url → 43 chars), prefixed with type tag (e.g. `loom_w_...` for worker).
- Storage: `tokens(token_hash, scopes, team_id?, expires_at, last_used_at, revoked_at)`. `token_hash = sha256(token)`. Raw token never stored.
- **Comparison is by indexed hash lookup only**; raw tokens never compared with `==` in app code.
- Validation middleware: extract bearer → hash → lookup → check expiry + revocation → bind scopes to request context.

LLM Gateway accepts `Authorization: Bearer <token>` and requires the request body to include `{"loom": {"team_id", "trial_id", "step_id"}}`. The worker is responsible for injecting trial/step IDs on every proxied call.

### 2.8 Lifecycle, retry, and cancellation (NEW)

**Trial state machine (6 states):**

```
        ┌─────────┐
        │ queued  │◄──────────────────────┐
        └────┬────┘                       │
             │ scheduler claims           │ heartbeat expiry
             ▼                            │ OR retry policy → re-queue
        ┌─────────┐                       │
        │ claimed │                       │
        └────┬────┘                       │
             │ worker accepts             │
             ▼                            │
        ┌─────────┐                       │
        │ running │───────────────────────┘
        └────┬────┘
             │
             ▼ terminal (no further transitions)
   ┌────────────────────┐
   │ succeeded          │
   │ failed (w/ reason) │   failure_reason ∈ {agent_error, env_error,
   │ cancelled          │     verifier_error, timeout, exhausted_retries, …}
   └────────────────────┘
```

**Retry policy** lives on `TrialConfig.retry`:
- `max_attempts: int` (default 1 = no retry)
- `retry_on: set[RetryReason]` ⊆ `{worker_crash, env_start_failure, agent_timeout, verifier_timeout}`
- `backoff: BackoffSpec` = `{base_sec, max_sec, multiplier, jitter}`

On retry-eligible failure, worker calls `PATCH /trials/{id}/state {state: 'queued', next_attempt_at: NOW() + backoff(attempt_count)}`. Scheduler picks it up after the delay. After `max_attempts`, state becomes `failed` with `failure_reason = 'exhausted_retries'`.

**Worker crash detection:** Control Plane runs every 30s:
```sql
UPDATE trials
   SET state='queued', worker_id=NULL,
       next_attempt_at = NOW() + INTERVAL '30 seconds'
 WHERE state IN ('claimed','running')
   AND worker_id IN (
       SELECT id FROM workers
        WHERE last_seen_at < NOW() - INTERVAL '15 seconds'
   );
```

**Worker fencing on state updates (race-condition guard):** every state-modifying endpoint a worker calls includes its `worker_id`, and Control Plane rejects the update if the trial's current `worker_id` doesn't match. Specifically: `PATCH /trials/{id}/state` performs

```sql
UPDATE trials SET state=:new_state, ... WHERE id=:trial_id AND worker_id=:worker_id
RETURNING *;
```

A zero-row result means the worker lost the claim (e.g., it was reclaimed by the crash detector after a heartbeat lapse) and the worker MUST abort the trial locally, write a `worker_lost_claim` event to the local trajectory buffer (which will be discarded), stop the sandbox, and continue serving other trials. This prevents two workers from racing on the same trial after a slow-worker reclaim.

**Cancellation:** `POST /trials/{id}/cancel` is source-state aware. Behavior depends on the trial's current state:

| Source state | Effect | Notes |
|---|---|---|
| `queued` | direct `UPDATE ... SET state='cancelled', cancellation_requested_at=NOW()` | no worker involved |
| `claimed` | same UPDATE; worker discovers on next state check before starting work | worker releases the sandbox if already started building it |
| `running` | same UPDATE; worker polls `state` at each step boundary and bails out cleanly. Hard mid-step cancellation is best-effort via `asyncio.CancelledError` (trial loop respects it; agent code may block) | v2 adds heartbeat-driven soft cancellation |
| terminal (`succeeded`, `failed`, `cancelled`) | HTTP 409 conflict; trial is already done | idempotent for `cancelled` (returns 200 with same body) |

Workers poll their assigned trial's state every 5 seconds (or piggyback the check on heartbeat) — adds at most a 5s tail to cancellation latency for running trials.

**Worker SIGTERM behavior** (graceful drain):
1. Stop polling the queue (no new claims)
2. Wait up to `--drain-timeout-sec` (default 600s) for in-flight trials to complete
3. On timeout: send `asyncio.CancelledError` to remaining trials, write a `worker_drain_interrupted` event to each trajectory, mark for reclamation
4. Exit

**OutOfBox agent crash blast radius:** if an OutOfBox agent throws an unrecoverable exception, the worker's TrialRunner catches it, marks the trial `failed` with structured error info, and continues serving other trials. The worker itself does NOT crash. **Subprocess isolation per trial is deferred to v2** — for v1, accept the rare risk of agent state corruption and design `TrialRunner` with defensive try/except wrappers.

### Forward declarations — defined in Section 4 (data model)

Used in Section 2, defined later:
- `TrajectoryWriter`, `TrajectoryReader`
- `MCPConnection`
- `HealthcheckSpec` — `{command, start_period_sec, interval_sec, timeout_sec, retries}`
- `NetworkPolicy` — tagged union of `Public`, `NoNetwork`, `Allowlist(domains: list[str])`
- `NetworkPolicyKind` — enum tag of the above for capability matching
- `TrialConfig` — runtime config including overrides, retry policy, timeout multipliers
- `ModelSpec` — `{provider, name, tier?, region?, max_input_tokens?, max_output_tokens?}`
- `MAX_EXEC_STREAM_BYTES` — configurable per-trial, default 10 MB

---

### Decisions in Section 2 that need explicit approval

1. **`AgentRuntime` Protocol has only `run()`.** `setup()` is on `InBoxAgentRuntime` only — out-of-box agents have nothing to install.
2. **`supports_atif` flag dropped.** ATIF projection is our code.
3. **Streaming is emergent, not enforced** on the Protocol.
4. **`with_default_user` removed from Driver.** `Driver.exec()` takes explicit `user` per call.
5. **In-box runs require Loom-aware CLIs in v1.** No fallback for non-aware CLIs (v1.5 if needed).
6. **`VerifierError` is a struct field**, not an exception.
7. **`CompositeVerifier` ships in v1** with enum + `AggregatorFn` Protocol.
8. **DRF over scalar caps via SQL** is the fairness mechanism. No Redis. Materialized `in_flight_count` with a symmetric trigger that covers all transitions including reclamation.
9. **Retry semantics**: `max_attempts`, `retry_on`, `backoff` on `TrialConfig`.
10. **6-state trial FSM** (queued, claimed, running, succeeded, failed, cancelled) with `failure_reason` enum distinguishing exhausted-retries from other failures.
11. **Worker SIGTERM = graceful drain** up to `--drain-timeout-sec`, then forced cancellation.
12. **OutOfBox agent crashes are caught by TrialRunner**; subprocess isolation per trial is v2.
13. **`set_network_policy` and `run_healthcheck` stay on the Driver** (orchestrator decides *when*; driver decides *how*).
14. **Worker fencing on state updates** — every worker-issued state-change includes `worker_id`; Control Plane rejects updates if the trial no longer belongs to that worker. Prevents the slow-worker-reclaim race.
15. **Cancellation is source-state aware** — `queued` cancels directly; `claimed` releases on worker's next state check; `running` bails at the next step boundary; terminal returns 409 (or idempotent 200 for already-cancelled).

---

## Section 3 — Data Flow ✅ APPROVED 2026-06-05

End-to-end: trial submission → claim → execution → trajectory streaming → finalize → result. This section pins down the `Trial.run()` body deferred from §2.5 and the trajectory-streaming protocol referenced from §1.

### 3.1 Trial submission

`POST /trials` with body:
```json
{
  "task_ref": {"kind": "package", "id": "skillflow@0.4.0/task-17"},
  "agent": {"name": "litellm-agent", "version": "1.0", "model": {"provider": "anthropic", "name": "claude-opus-4-7"}},
  "verifier": {"name": "pytest", "config": {...}},
  "config": {
    "force_build": false,
    "verifier_env_mode": "shared",
    "agent_timeout_sec": 1800,
    "verifier_timeout_sec": 300,
    "retry": {"max_attempts": 2, "retry_on": ["worker_crash"], "backoff": {"base_sec": 30, "multiplier": 2.0, "max_sec": 300}},
    "submit_priority": 100
  }
}
```

Control Plane:
1. Auth: extract token → resolve `team_id` + scopes; require `submit`.
2. Resolve `task_ref` (local path / git / package registry) → canonical `task_id` + content hash.
3. Resolve `agent`/`verifier` against the registry → produce executable spec.
4. **Derive `requires_caps`** from task config + agent + verifier (see 3.1.1).
5. INSERT into `trials(id, team_id, task_id, requires_caps, config, state='queued', submit_priority, submitted_at, attempt_count=0)`.
6. Return `{trial_id, state, submitted_at}`.

Submitters poll `GET /trials/{id}` for status. Webhook notification (`POST /trials/{id}/notify`) is **v1.5**.

#### 3.1.1 `requires_caps` derivation

```python
def derive_requires_caps(task: Task, agent: AgentSpec, verifier: VerifierSpec) -> RequiredCaps:
    return RequiredCaps(
        os=task.environment.os,                    # scalar from task config
        gpu_vendor=task.environment.gpu_vendor or "none",
        network_policies=task.environment.network_policies | verifier.network_policies,
        # mounted_fs intentionally omitted in v1 (every driver supports either)
    )
```

The submitter cannot override caps — they're a function of the task + agent + verifier choices. Prevents users from claiming smaller workers than they need.

### 3.2 Worker startup, claim loop, and heartbeat

**Token + ID relationship:** the worker token is provisioned at deploy time and carries scopes (`worker:claim`, `worker:report`, `worker:index`) but does NOT encode `worker_id`. The Control Plane assigns `worker_id` at `POST /workers/register` and returns it; the worker uses that ID on every subsequent state-fenced call. This decouples token rotation from worker identity (you can rotate the token without re-registering).

**Startup sequence:**

1. **Orphan cleanup.** Worker scans its local trajectory cache directory (e.g., `/var/lib/loom/trajectories/`) for any JSONL files left from a previous run. For each, fetch the trial's current state via `GET /trials/{id}` (using the worker token). Delete the file if state is terminal (`succeeded`, `failed`, `cancelled`) OR `worker_id != my_assigned_worker_id` OR trial unknown (404). Anything that's still `running`/`claimed` and owned by *this* worker_id is a recovery candidate — for v1, just log a warning and delete (no resume; v2 may add resume).
2. **Registration.** `POST /workers/register {capabilities: [Capabilities, ...], hostname, version, drain_timeout_sec}` → `{worker_id, heartbeat_interval_sec: 5, claim_poll_interval_sec: 1}`.
3. **Heartbeat thread.** Heartbeat runs on a **dedicated OS thread**, not the asyncio loop. It calls `PATCH /workers/{id}/heartbeat` every 5s using a `requests.Session` (or `httpx` sync client). This insulates heartbeat from event-loop starvation: if an agent step blocks the loop (CPU-bound work, a non-cooperative library), heartbeats still fire and the worker is not falsely reclaimed.
4. **Main claim loop** runs on the asyncio loop:

```python
async def main_loop():
    while not shutting_down:
        if in_flight < max_concurrent:
            trial = await cp.claim(worker_id, advertised_caps)
            if trial is not None:
                asyncio.create_task(TrialRunner(trial, driver_factory).run())
        await asyncio.sleep(claim_poll_interval_sec)  # 1s default
```

`cp.claim` is the §2.6 DRF SQL query exposed as `POST /trials/claim`. Returns one trial row or 204.

**Documented constraint:** agent implementations MUST cooperate with asyncio. Long CPU-bound work in an agent that blocks the loop will starve other trials on the same worker (though heartbeat keeps the worker alive). Recommended pattern: offload CPU work via `asyncio.to_thread()` or a process pool.

**Shutdown ordering (SIGTERM):**

1. Worker signal handler sets `shutting_down = True` (atomic flag visible to both heartbeat thread and main loop).
2. Heartbeat thread observes the flag at its next tick (within 5s) and exits cleanly — no further `last_seen_at` updates. *This is intentional*: after we stop heartbeating, the crash detector will eventually reclaim any trial we abandon, providing a hard backstop if the drain phase wedges.
3. Main loop stops polling for new claims and enters drain phase: waits up to `--drain-timeout-sec` (default 600s) for all in-flight `TrialRunner` tasks to complete. Each completing trial does its own terminal state PATCH via the fenced endpoint.
4. On drain timeout: `asyncio.CancelledError` is sent to remaining `TrialRunner`s. Each one runs its outer `finally` (finalize + state PATCH), which respects the 15s/60s timeouts from §3.3, so worst-case the process exits within ~75s after drain timeout.
5. Process exits.

Steps 2 and 3 race intentionally — step 2 stops the lie ("we're alive") at the right time even if step 3 takes minutes. This makes orphaned trials self-healing.

### 3.3 `Trial.run()` — the central algorithm

```python
async def run(self) -> TrialResult:
    """Single trial lifecycle. Owns sandbox, agent, verifier, trajectory.
    Finalize + terminal state PATCH always run, including under cancellation."""
    self.result.started_at = now()
    await self._patch_state('running')                       # fenced by worker_id
    cancelled = False

    try:
        async with self.trajectory:                          # flush-on-exit guaranteed
            try:
                # 1. Provision sandbox
                await self.driver.start(force_build=self.config.force_build)
                await self.driver.run_healthcheck(self.task.healthcheck)

                # 2. In-box only: install agent CLI
                if isinstance(self.agent, InBoxAgentRuntime):
                    await self.agent.setup(env=self.driver)

                # 3. Per-step loop
                for step in self.task.steps:
                    await self._run_step(step)
                    if self._should_stop_after_step(step):
                        break

                # 4. Aggregate step rewards into trial reward
                self.result.reward = self._aggregate_step_rewards()
                self.result.state = 'succeeded'

            except CancelledError:
                cancelled = True
                self.result.state = 'cancelled'
                # trajectory still open — record the cancellation event
                await self.trajectory.append(TrialCancelledEvent())
            except Exception as exc:
                self.result.state = 'failed'
                self.result.failure_reason = classify_failure(exc)
                await self.trajectory.append(TrialErrorEvent.from_exc(exc))
                # terminal-fail is normal — do NOT re-raise here
            finally:
                # 5. Stop sandbox, shielded — outer cancel can't abandon a container.
                #    driver.stop() is idempotent per §2.2, safe even if start() failed.
                await asyncio.shield(
                    self.driver.stop(delete=self.config.delete_env)
                )

        # async-with has exited — trajectory's final flush is complete and the
        # local file is byte-identical to MinIO.

    finally:
        # 6. Finalize trajectory (project ATIF, upload, update index).
        #    Wrapped so a finalize failure can't prevent the terminal state PATCH.
        #    Hard-timed so an unreachable MinIO doesn't orphan the trial.
        try:
            await asyncio.wait_for(
                asyncio.shield(self._finalize_trajectory()),
                timeout=FINALIZE_TIMEOUT_SEC,           # 60s default
            )
        except (Exception, asyncio.TimeoutError):
            if self.result.state == 'succeeded':
                self.result.state = 'failed'
                self.result.failure_reason = 'trajectory_flush_failed'

        # 7. Final state transition (fenced, shielded, hard-timed). ALWAYS runs —
        #    including on cancellation, before we re-raise. If the state PATCH
        #    times out, the worker crash detector eventually reclaims the trial.
        try:
            await asyncio.wait_for(
                asyncio.shield(self._patch_state(
                    self.result.state,
                    failure_reason=self.result.failure_reason,
                )),
                timeout=STATE_PATCH_TIMEOUT_SEC,         # 15s default
            )
        except asyncio.TimeoutError:
            # Best effort — worker is responsible for logging; the crash detector
            # will move the trial back to queued once heartbeat lapses.
            pass

        # 8. Re-raise CancelledError AFTER terminal state is recorded.
        if cancelled:
            raise CancelledError()

    return self.result
```

Key properties:
- **Finalize + state PATCH always run** — on success, on terminal failure, and on cancellation. The outer `try/finally` is the gate. Cancellation propagates only after the terminal state is durably recorded.
- The `async with self.trajectory` enclosure guarantees the writer flushes its buffer even on exception or cancellation, BEFORE finalize reads the local file.
- `driver.stop` is `asyncio.shield`-ed AND idempotent per §2.2 — outer cancellation can't abandon a container, and an unstarted driver doesn't crash the finally block.
- All state-change calls go through fenced endpoints from §2.8 (rejected if `worker_id` doesn't match).
- A terminal-fail path is *normal* — the inner except clause doesn't re-raise.

### 3.4 `_run_step` — per-step body

```python
async def _run_step(self, step: StepConfig) -> None:
    sr = StepResult(step_name=step.name)
    self.result.steps.append(sr)
    await self.trajectory.append(StepStartEvent(step.name))

    # Prepare: workdir upload, setup.sh, step healthcheck
    try:
        await self._prepare_step(step)
    except Exception as exc:
        sr.error = StepError(phase='prepare', from_exc=exc)
        await self.trajectory.append(StepEndEvent(step.name, sr))
        return

    # Agent phase — under timeout, under network policy
    plan = self._network_plan_for(step)
    try:
        async with self._phase_network(plan.agent):
            await asyncio.wait_for(
                self.agent.run(
                    instruction=step.instruction,
                    env=self.driver,
                    trajectory=self.trajectory,
                    mcp=step.mcp_servers,
                    skills_dir=step.skills_dir,
                ),
                timeout=step.agent_timeout_sec,
            )
    except TimeoutError:
        sr.error = StepError(phase='agent', reason='timeout')
    except Exception as exc:
        sr.error = StepError(phase='agent', from_exc=exc)
    # NOTE: continue to verifier even on agent failure — partial credit may apply

    # Artifact collection
    artifacts_dir = await self._collect_artifacts(step)
    sr.artifacts_uri = await self._upload_artifacts(artifacts_dir, step)

    # Verifier phase
    if step.verifier and not self.config.skip_verifier:
        verifier_env = await self._acquire_verifier_env(step)   # shared or fresh
        try:
            async with self._phase_network(plan.verifier):
                sr.verifier_result = await asyncio.wait_for(
                    step.verifier.verify(
                        task=self.task,
                        env=verifier_env,
                        artifacts_dir=artifacts_dir_in_env,
                        trajectory=self.trajectory.reader(),
                    ),
                    timeout=step.verifier_timeout_sec,
                )
        except TimeoutError:
            sr.error = StepError(phase='verifier', reason='timeout')
        except Exception as exc:
            sr.error = StepError(phase='verifier', from_exc=exc)

    await self.trajectory.append(StepEndEvent(step.name, sr))
```

`_should_stop_after_step` consults `step.min_reward` if set — failing the threshold short-circuits remaining steps (Harbor-inherited semantics).

**`_phase_network` context manager** (used in `_run_step`):

```python
@asynccontextmanager
async def _phase_network(self, policy: NetworkPolicy):
    """Temporarily switch the driver to `policy` for this phase; restore the
    trial's baseline on exit. No-op if the policy already matches baseline."""
    baseline = self.config.baseline_network_policy
    if policy == baseline:
        yield
        return
    if not self.driver.capabilities.dynamic_network_policy:
        # Validated at trial init in §2.6; reaching here means a bug.
        raise RuntimeError("phase network policy requires dynamic_network_policy")
    await self.driver.set_network_policy(policy)
    try:
        yield
    finally:
        await asyncio.shield(self.driver.set_network_policy(baseline))
```

Validation that the trial's phase policies are achievable by the driver happens at trial init (§2.6 cap matching). The context manager itself just enforces the temporary switch.

### 3.5 Trajectory streaming protocol

`TrajectoryWriter` lifecycle:

```python
class TrajectoryWriter:
    def __init__(self, local_path: Path, sink: TrajectorySink):
        self._local = local_path.open('ab')          # append-only on worker disk
        self._sink = sink                             # MinIO multipart uploader
        self._buf: list[TrajectoryEvent] = []
        self._buf_bytes: int = 0
        self._last_flush_at: float = time.monotonic()

    async def append(self, event: TrajectoryEvent) -> None:
        line = event.to_json_line()
        self._local.write(line); self._local.flush()  # local durability first
        self._buf.append(event); self._buf_bytes += len(line)
        if self._should_flush():
            await self._flush()

    async def __aenter__(self): ...
    async def __aexit__(self, *exc): await self._flush(final=True)

    def _should_flush(self) -> bool:
        return (
            self._buf_bytes >= FLUSH_BYTES                # 1 MB default
            or len(self._buf) >= FLUSH_EVENTS             # 100 events default
            or time.monotonic() - self._last_flush_at >= FLUSH_SEC  # 10 s default
        )
```

Flush strategy (whichever first):
- **1 MB buffered** → upload chunk
- **100 events buffered** → upload chunk
- **10 seconds since last flush** → upload chunk

**Local-first durability:** every event is written to the worker's local file BEFORE being acked to the agent. If the worker crashes between flushes, the local file may have a few seconds of events not yet uploaded — those are lost when the trial is reclaimed. We accept this trade-off in v1; v2 can add per-event WAL.

**MinIO upload** uses S3 multipart with one part per flush chunk (parts can be `>=` 5 MB except the last; we coalesce sub-5MB chunks in memory until the threshold is reached or the buffer ages 10s, then flush as a part). The worker is the sole writer for a given trial's trajectory object, so part-ordering is trivial.

**Index update** (Control Plane is index manager, not data manager):
```
PATCH /trials/{trial_id}/trajectory_index
  body: {bytes_uploaded, events_count, last_step_id, checksum_sha256, upload_id, parts: [...]}
```

The index row is what queries hit. It points at the MinIO object; the object holds the data.

**Retry on flush failure:** exponential backoff (3 attempts default). Final failure escalates to `failure_reason='trajectory_flush_failed'` and the trial is marked `failed`.

### 3.6 Trajectory reading (for verifier and clients)

`TrajectoryReader` materializes events for consumers:

| Consumer | Method | Notes |
|---|---|---|
| `LLMJudgeVerifier` mid-trial | `trajectory.reader().tail(n=50)` | reads from worker's local file (cheap) |
| Client `GET /trials/{id}/events?from=N&limit=M` | streams from MinIO via Control Plane proxy | v1: pagination; v1.5: SSE live tail |
| Training pipeline | `loom traj fetch <trial_id> > events.jsonl` | direct MinIO read via signed URL |
| ATIF projection at finalize | reads worker's local file (post-final-flush) | deterministic transform |

**`LLMJudgeVerifier` excerpt strategy** (resolves the §2.4 open question):
- Default: `tail(50)` — the last 50 events, which covers most reasoning context for a judge
- Configurable per-verifier-config: `excerpt: {kind: "tail", n: int}` or `{kind: "all"}` or `{kind: "tool_use_only"}` or `{kind: "step_summary", aggregate: true}`
- Token-budget guard: every excerpt strategy enforces `max_tokens` (default 32k); over budget → progressively prune older events

### 3.7 ATIF projection at finalize

After `async with self.trajectory:` exits (final flush complete):

```python
async def _finalize_trajectory(self) -> None:
    # Source of truth: the local file (already mirrored to MinIO)
    events = TrajectoryReader(self.trajectory_local_path).iter_all()
    atif = project_to_atif(events, trial=self.task, agent=self.agent)

    atif_uri = await self.minio.put_json(
        bucket="trajectories",
        key=f"{self.team_id}/{self.trial_id}/atif.json",
        body=atif.model_dump(),
    )
    await cp.update_trajectory_index(
        self.trial_id,
        atif_uri=atif_uri,
        atif_schema_version="1.7",
    )
```

`project_to_atif` is a pure function — re-runnable if ATIF v1.8 ships and we want to backfill. It does NOT consume MinIO objects; it consumes the local file (which is byte-identical to MinIO). Order of operations on `_finalize_trajectory`:
1. Final buffer flush completes (MinIO has all events)
2. Local file is closed for writing
3. Projector runs on local file
4. ATIF object uploaded
5. Index updated with `atif_uri`
6. Local file deleted

### 3.8 Verifier env modes

`TrialConfig.verifier_env_mode` ∈ `{shared, separate}`:

**Shared (default):**
- Verifier runs in the same `Driver` instance the agent used.
- Cheap (no rebuild), but agent-installed packages may affect verifier behavior.
- `_acquire_verifier_env(step)` returns `self.driver`.

**Separate:**
- After agent finishes, agent's `Driver.stop()` is called.
- A fresh `Driver` is constructed from `tests/Dockerfile` (or `task.environment` if no separate tests build context).
- Artifact dir on host is uploaded into the fresh env via `verifier_driver.upload`.
- Verifier runs there.
- Trustworthy grading. ~30–60s additional overhead per step.
- `_acquire_verifier_env(step)` constructs and starts a new Driver.

Default for v1: **shared** (matches "internal trust" decision). Tasks that need trustworthy grading (e.g., SkillLearnBench public leaderboard runs) set `verifier_env_mode: separate` in their task config.

### 3.9 Artifact handling

**Artifact spec on `step.artifacts`:** a list of POSIX-style path patterns (glob), evaluated *inside* the sandbox. v1 is Linux-only (Windows artifact globbing is a v2 concern when we add a `WindowsContainerDriver`).

**Pattern syntax:** subset of POSIX `find -path` patterns. Allowed:
- Literal paths: `outputs/results.csv`
- Single-segment glob: `outputs/*.json`
- Recursive glob: `outputs/**/*.csv`
- Multiple patterns per step: `artifacts: ["outputs/*.json", "logs/run.log"]`

Collection algorithm:
1. For each pattern: `driver.exec(f"find {shlex.quote(workspace_root)} -path {shlex.quote(workspace_root + '/' + pattern)} -type f -print0", user=task.user)` — produces a NUL-separated list. Patterns are anchored at the task's workspace root (default `/workspace`) so a literal pattern like `outputs/*.json` doesn't traverse the whole sandbox. `shlex.quote` defends against quote-containing pattern strings. (Stdout cap applies; if exceeded, log warning and proceed with truncated list — concrete tasks should keep artifact lists bounded.)
2. For each matched file: `driver.download(matched_path, host_tmp_dir / relpath)` — copy out of sandbox to worker disk.
3. Stream-upload each file to MinIO at `s3://artifacts/{team_id}/{trial_id}/{step_name}/{relative_path}` via boto3 multipart-streaming upload (no full-file buffering — supports multi-GB artifacts).
4. Record `artifact_uri` (the directory prefix `s3://artifacts/{team_id}/{trial_id}/{step_name}/`) on `step_result.artifacts_uri`.
5. For `verifier_env_mode=separate`: also `verifier_driver.upload(host_tmp_dir / relpath, /artifacts/relpath)` for every matched file.

**Empty matches are non-fatal** — the verifier sees the empty artifact dir and decides whether that's an error. (A `StructuredOutputVerifier` for a required file would fail; a permissive verifier wouldn't.)

**Large-file handling:** MinIO uploads use `boto3.s3.transfer.TransferConfig(multipart_threshold=8MB, multipart_chunksize=8MB, use_threads=True)`. Worker disk is the bottleneck for very large artifacts; tasks producing >50GB of artifacts per step should set `step.artifacts_streaming=true` (v1.5) to upload-as-found rather than collect-then-upload.

### 3.10 State transitions emitted by Trial.run()

Worker → Control Plane state PATCHes (all fenced by `worker_id`):

| When | New state | Notes |
|---|---|---|
| `run()` entry | `running` | from `claimed` |
| `run()` clean exit, success | `succeeded` | also: `finished_at`, `reward`, `trajectory_uri`, `atif_uri` |
| `run()` exception, retry-eligible | `queued` | `attempt_count++`, `next_attempt_at=...` |
| `run()` exception, terminal | `failed` | also: `failure_reason`, `finished_at` |
| Cancellation observed | `cancelled` | also: `cancellation_observed_at` |

Worker heartbeat is independent — every 5s the worker hits `PATCH /workers/{id}/heartbeat`, regardless of trial state.

### 3.11 Result fetch endpoints (client side)

| Endpoint | Auth | Returns |
|---|---|---|
| `GET /trials/{id}` | `read:own` / `read:team` | TrialResult (no trajectory inline) |
| `GET /trials/{id}/trajectory` | same | redirects to signed MinIO URL for the JSONL |
| `GET /trials/{id}/atif` | same | redirects to signed MinIO URL for the ATIF doc |
| `GET /trials/{id}/events?from=N&limit=M` | same | paginated JSONL stream from MinIO |
| `GET /trials/{id}/events:tail` | same | **v1.5**: SSE live tail (reserved endpoint, NotImplemented in v1) |
| `GET /trials/{id}/artifacts` | same | lists artifact URIs from `s3://artifacts/{team_id}/{trial_id}/` |

---

### Decisions in Section 3 that need explicit approval

1. **`requires_caps` is derived by Control Plane, not by submitter.** Submitters cannot under-spec.
2. **Worker claim is HTTP poll at 1s** (not push from Control Plane). Simpler, matches Postgres-as-queue choice.
3. **`Trial.run()` continues to verifier even on agent failure** — partial credit may apply via verifier on collected artifacts.
4. **Local-first trajectory durability**: events hit worker disk before ack to agent; flushes to MinIO are best-effort with retry. Up to ~10s of events lost on hard worker crash. v1 accepts this; v2 adds per-event WAL.
5. **Trajectory flush triggers**: 1 MB buffered OR 100 events OR 10 seconds, whichever first.
6. **`LLMJudgeVerifier` excerpt default = `tail(50)`** with `max_tokens=32k` guard; pruning is oldest-first.
7. **ATIF projection happens at finalize, from the local file** (byte-identical to MinIO). Projector is a pure function; re-runnable later for ATIF version bumps.
8. **`verifier_env_mode=shared` is the v1 default.** Separate is opt-in per task.
9. **Terminal-fail path doesn't re-raise** in `Trial.run()` — once `failure_reason` is recorded, the function returns normally with a failed `TrialResult`.
10. **State PATCHes are fenced by `worker_id`** at every transition (re-stating §2.8 for emphasis — every endpoint in §3.10 uses this).
11. **Webhook notification + SSE live tail are v1.5.** v1 is poll + MinIO direct read.
12. **Finalize + terminal state PATCH always run** — including under cancellation. `Trial.run()`'s outer `try/finally` records the terminal state before re-raising `CancelledError`.
13. **`Driver.start/stop/exec` have explicit idempotency contracts** (added to §2.2): `stop()` is safe before/after/multiple-times; `start()` is once-only with `DriverAlreadyStartedError`; pre-`start()` or post-`stop()` exec/upload/download raise `DriverNotStartedError`.
14. **Verifiers write structured output to sandbox files**, never parse `ExecResult.stdout` for structured data — avoids the 10 MB stdout cap. PytestVerifier uses junit XML + JSON report files; ScriptVerifier reads `$LOOM_VERIFIER_OUTPUT`.
15. **Worker startup performs orphan-trajectory sweep** — deletes local JSONL files for trials whose state is terminal or whose `worker_id` no longer matches.
16. **Heartbeat runs on a dedicated OS thread**, not the asyncio loop — insulates worker liveness from agent code blocking the loop.
17. **Worker token does NOT encode `worker_id`** — token carries scopes; `worker_id` is assigned at `POST /workers/register`. Decouples rotation from identity.
18. **Artifact patterns are POSIX-only globs evaluated inside the sandbox** via `find -path`. Linux-only in v1. Multi-GB artifacts use boto3 multipart-streaming upload.
19. **`_phase_network` context manager** sets temporary policy + restores baseline on exit, shielded.

---

## Section 4 — Data Model ✅ APPROVED 2026-06-05

This section locks in every schema referenced from §1–§3. All models are Pydantic v2 (or dataclass-frozen where immutable). Field names use snake_case. Times are timezone-aware UTC datetimes.

### 4.1 Task schema (on-disk + parsed)

A task is a directory:

```
my-task/
  task.toml                  # top-level config, parsed into TaskConfig
  instruction.md             # default step instruction
  environment/
    Dockerfile               # OR omit if task.toml sets docker_image
  tests/                     # default verifier inputs
    Dockerfile               # OR omit for shared verifier env
  solution/                  # OPTIONAL — reference solution for OracleAgent
    solve.sh                 # canonical solver script; uploaded + executed by OracleAgent
  steps/<step-name>/         # OPTIONAL, multi-step only
    instruction.md
    workdir/                 # uploaded to sandbox workdir at step start
    setup.sh                 # optional, runs in sandbox before agent
    tests/                   # per-step verifier inputs
```

The optional `solution/solve.sh` is the agent-of-last-resort: an `OracleAgent` (see §2.1) uploads and executes it, providing a deterministic upper-bound run that bypasses LLM uncertainty. Used as a sanity-check baseline in CI and as the v1 fixture for testing the trial harness end-to-end. Tasks distributed externally MAY omit `solution/` to keep the answer private.

`task.toml` parses into:

```python
class TaskConfig(BaseModel):
    schema_version: Literal["1"]
    task: TaskMetadata
    environment: EnvironmentConfig
    agent: AgentDefaults                 # task-level defaults; per-step overrideable
    verifier: VerifierDefaults
    steps: list[StepConfig]              # always ≥1; synthesized if absent
    multi_step: MultiStepConfig | None = None

class TaskMetadata(BaseModel):
    id: str                              # globally unique within registry
    name: str
    description: str | None = None
    labels: list[str] = []

class EnvironmentConfig(BaseModel):
    os: OS                                                          # "linux" | "windows"
    gpu_vendor: GPUVendor = "none"
    docker_image: str | None = None
    dockerfile: PurePosixPath | None = None                         # relative to task dir
    healthcheck: HealthcheckSpec | None = None
    workdir: PurePosixPath = PurePosixPath("/workspace")
    user: str | int = "agent"
    network_policies_supported: frozenset[NetworkPolicyKind] = frozenset({"public"})
    baseline_network_policy: NetworkPolicy = Public()
    skills_dir: PurePosixPath | None = None
    mcp_servers: list[MCPConnection] = []
    build_timeout_sec: float = 1200

class AgentDefaults(BaseModel):
    name: str                             # registry key, e.g. "litellm-agent"
    version: str | None = None
    model: ModelSpec | None = None
    timeout_sec: float = 1800
    setup_timeout_sec: float = 360
    user: str | int | None = None
    extra_mcp_servers: list[MCPConnection] = []
    skills: list[SkillRef] = []

class VerifierDefaults(BaseModel):
    name: str                             # "pytest" | "script" | "llm_judge" | "structured" | "composite"
    args: dict[str, Any] = {}
    timeout_sec: float = 300
    env_mode: VerifierEnvMode = "shared"
    user: str | int | None = None

class StepConfig(BaseModel):
    name: str
    instruction_file: PurePosixPath = PurePosixPath("instruction.md")
    agent: AgentOverrides | None = None
    verifier: VerifierOverrides | None = None
    artifacts: list[str] = []                                       # POSIX glob patterns
    min_reward: dict[str, float] | float | None = None              # short-circuit threshold
    network: StepNetworkPlan | None = None
    healthcheck: HealthcheckSpec | None = None                      # per-step recheck after setup.sh

class MultiStepConfig(BaseModel):
    reward_strategy: MultiStepRewardStrategy = "mean"               # "mean" | "min" | "weighted" | "final"
    weights: dict[str, float] | None = None                         # required if "weighted"
```

**Implicit single-step synthesis** (referenced from §2.5 / H1):
```python
def normalize_steps(cfg: TaskConfig) -> TaskConfig:
    if cfg.steps:
        return cfg
    return cfg.model_copy(update={"steps": [StepConfig(name="main")]})
```

**Task identity:** `task_checksum = dirhash(task_dir, "sha256")` — covers all files in the directory. Stored on every `TrialResult` and `trials` row for reproducibility.

### 4.2 Supporting types

```python
type OS = Literal["linux", "windows"]
type GPUVendor = Literal["none", "nvidia"]
type VerifierEnvMode = Literal["shared", "separate"]
type MultiStepRewardStrategy = Literal["mean", "min", "weighted", "final"]
type ResourceMode = Literal["auto", "limit", "guarantee"]

class ModelSpec(BaseModel):
    provider: str                         # "anthropic" | "openai" | "together" | ...
    name: str                             # provider-canonical model id
    tier: str | None = None               # provider-specific tier label
    region: str | None = None
    max_input_tokens: int | None = None   # used for excerpt-pruning guards
    max_output_tokens: int | None = None

class MCPConnection(BaseModel):
    name: str
    transport: Literal["stdio", "sse", "websocket", "http"]
    command: list[str] | None = None      # stdio only; required if transport == "stdio"
    url: str | None = None                # sse/ws/http only
    env: dict[str, str] = {}              # stdio
    headers: dict[str, str] = {}          # http-shaped

    @model_validator(mode="after")
    def _check_transport(self) -> "MCPConnection":
        if self.transport == "stdio":
            if not self.command:
                raise ValueError("stdio transport requires `command`")
            if self.url is not None:
                raise ValueError("stdio transport must not set `url`")
        else:  # sse | websocket | http
            if not self.url:
                raise ValueError(f"{self.transport} transport requires `url`")
            if self.command is not None:
                raise ValueError(f"{self.transport} transport must not set `command`")
        return self

class HealthcheckSpec(BaseModel):
    command: str                          # shell command; rc=0 means healthy
    start_period_sec: float = 0           # grace period before failures count
    interval_sec: float = 5
    timeout_sec: float = 3
    retries: int = 6

class SkillRef(BaseModel):
    name: str
    version: str | None = None
    source: SkillSource                   # tagged union below

class SkillSource(BaseModel):
    """Tagged union: local-path | git | registry."""
    kind: Literal["local", "git", "registry"]
    path: PurePosixPath | None = None     # local
    repo: str | None = None               # git
    ref: str | None = None                # git
    id: str | None = None                 # registry

# NetworkPolicy as a tagged union
class _NetworkPolicy(BaseModel):
    kind: NetworkPolicyKind
class Public(_NetworkPolicy):
    kind: Literal["public"] = "public"
class NoNetwork(_NetworkPolicy):
    kind: Literal["no-network"] = "no-network"
class Allowlist(_NetworkPolicy):
    kind: Literal["allowlist"] = "allowlist"
    domains: tuple[str, ...]
    cidrs: tuple[str, ...] = ()
NetworkPolicy = Public | NoNetwork | Allowlist
NetworkPolicyKind = Literal["public", "no-network", "allowlist"]

class StepNetworkPlan(BaseModel):
    agent_phase: NetworkPolicy | None = None     # None = use baseline
    verifier_phase: NetworkPolicy | None = None
```

### 4.3 TrialConfig — runtime configuration provided at submission

```python
class TrialConfig(BaseModel):
    schema_version: Literal["1"] = "1"

    # Build/runtime
    force_build: bool = False
    delete_env: bool = True
    skip_verifier: bool = False
    verifier_env_mode: VerifierEnvMode | None = None    # overrides task default

    # Timeouts — `override_*` replaces task default; `*_multiplier` scales the resolved value
    override_agent_timeout_sec: float | None = None
    override_verifier_timeout_sec: float | None = None
    override_env_build_timeout_sec: float | None = None
    agent_timeout_multiplier: float = 1.0
    verifier_timeout_multiplier: float = 1.0
    env_build_timeout_multiplier: float = 1.0

    # Retry policy
    retry: RetryPolicy = RetryPolicy()

    # Scheduling
    submit_priority: int = 100                          # within-team only; 0–1000

    # Per-trial overrides on the task's defaults
    extra_mcp_servers: list[MCPConnection] = []
    extra_skills: list[SkillRef] = []
    baseline_network_policy_override: NetworkPolicy | None = None

class RetryPolicy(BaseModel):
    max_attempts: int = 1                               # 1 = no retry
    retry_on: frozenset[RetryReason] = frozenset()
    backoff: BackoffSpec = BackoffSpec()

class BackoffSpec(BaseModel):
    base_sec: float = 30
    max_sec: float = 600
    multiplier: float = 2.0
    jitter: float = 0.2                                 # multiplicative: actual = base × random(1-j, 1+j)

class RetryReason(StrEnum):
    WORKER_CRASH = "worker_crash"
    ENV_START_FAILURE = "env_start_failure"
    AGENT_TIMEOUT = "agent_timeout"
    VERIFIER_TIMEOUT = "verifier_timeout"
    TRAJECTORY_FLUSH_FAILED = "trajectory_flush_failed"
```

Timeout resolution rule (canonical):
```
final = (override OR task_default) × multiplier
```

### 4.4 Trajectory event catalog

The trajectory is JSONL — one event per line. All events share a common envelope:

```python
class TrajectoryEvent(BaseModel):
    """Discriminated union by `kind` (Pydantic tagged-union)."""
    kind: EventKind
    emitted_at: datetime                        # UTC
    trial_id: UUID
    step_id: str                                # step name; "main" for single-step
    seq: int                                    # monotonic per-trial sequence
    # subclass payload follows
```

**Event kinds** (categorized):

| Category | Kind | Payload (key fields) |
|---|---|---|
| Trial | `trial_start` | task_id, agent_info, config_snapshot |
| Trial | `trial_end` | final_state, reward, failure_reason |
| Trial | `trial_error` | error_type, message, traceback |
| Trial | `trial_cancelled` | cancellation_requested_at, observed_at |
| Step | `step_start` | step_name, instruction_excerpt |
| Step | `step_end` | step_result (StepResult instance) |
| Env | `env_start` | image_ref, build_time_sec |
| Env | `env_ready` | healthcheck_attempts |
| Env | `env_stop` | duration_sec, exit_status |
| Env | `env_exec` | cmd, user, cwd, return_code, stdout_bytes, stderr_bytes, truncated, duration_sec |
| File | `file_upload` | src_size_bytes, dst_path, duration_sec |
| File | `file_download` | src_path, dst_size_bytes, duration_sec |
| Agent | `llm_call` | **see 4.4.1 — load-bearing for training data** |
| Agent | `tool_use` | tool_name, args (JSON), result (JSON-or-error), duration_sec |
| Agent | `agent_thought` | content (string), tokens (optional) |
| Verifier | `verifier_start` | verifier_name, env_mode |
| Verifier | `verifier_end` | result (VerifierResult) |
| Verifier | `verifier_check` | check (CheckResult) |
| Net | `network_policy_change` | from_policy, to_policy, phase |
| Sys | `worker_lost_claim` | original_worker_id, detected_at |
| Sys | `worker_drain_interrupted` | drain_timeout_sec |

#### 4.4.1 `llm_call` event — the training-data load-bearing payload

This is the most important event type. Schema:

```python
class LLMCallEvent(TrajectoryEvent):
    kind: Literal["llm_call"]

    # Model identification (frozen per call — historic if rate card changes)
    model: ModelSpec
    rate_card_hash: str                              # sha256 of rate table at emit time

    # Input
    system_prompt: str | None
    messages: list[ChatMessage]                      # OpenAI-compatible
    tools: list[ToolSpec] | None
    tool_choice: str | dict | None

    # Output
    response: ChatMessage                            # assistant message
    finish_reason: str                               # "stop", "length", "tool_use", ...

    # Usage — RAW, NOT derived (per H5 in harbor-design-review.md)
    input_tokens: int
    cached_input_tokens: int                         # cache hit
    cache_write_tokens: int                          # cache miss + write
    output_tokens: int
    thinking_tokens: int                             # for thinking-capable models
    provider_extras: dict[str, int]                  # named provider-specific counters

    # Derived (recomputable if rate card changes)
    cost_usd_snapshot: float                         # = f(usage, rate_card_hash)

    # Timing
    duration_sec: float
    streamed: bool
    time_to_first_token_sec: float | None

    # Attribution
    gateway_request_id: str                          # for cross-referencing LLM Gateway logs
    cache_keys: list[str] = []                       # cache control breakpoint identifiers
```

**Why raw + derived:** the H5 decision (cost snapshot becomes inaccurate as prices change) — we store raw usage forever, and the `loom_cost_replay <trial_id> --rate-card <id>` CLI re-derives cost on demand.

**`provider_extras`** is the named-counter escape hatch (NOT opaque `dict[str, Any]`) — provider-specific counters like Anthropic's `cache_creation_input_tokens` go here as int-valued fields, so they remain queryable.

### 4.5 TrialResult — the persisted, indexed result

```python
class TrialResult(BaseModel):
    schema_version: Literal["1"] = "1"

    # Identity
    id: UUID
    task_id: str
    task_checksum: str                              # sha256 of task dir
    team_id: UUID

    # What was run
    agent: AgentInfo
    config: TrialConfig

    # State
    state: TrialState                               # see §2.8 enum
    failure_reason: FailureReason | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None

    # Reward (aggregated from steps per multi_step.reward_strategy)
    reward: dict[str, float] | None = None

    # Per-step results — NO trial-level agent_result/verifier_result (fixes round-3 wart C)
    steps: list[StepResult] = []

    # Pointers into MinIO — data lives outside this struct
    trajectory_uri: str | None = None               # s3://trajectories/{team}/{trial}/events.jsonl
    atif_uri: str | None = None                     # s3://trajectories/{team}/{trial}/atif.json
    atif_schema_version: str | None = None
    artifacts_prefix: str | None = None             # s3://artifacts/{team}/{trial}/

class StepResult(BaseModel):
    step_name: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    verifier_result: VerifierResult | None = None
    error: StepError | None = None
    artifacts_uri: str | None = None                # s3://artifacts/{team}/{trial}/{step_name}/

class StepError(BaseModel):
    phase: Literal["prepare", "agent", "artifacts", "verifier"]
    reason: Literal["timeout", "exception", "missing_artifacts", "cancelled"]
    message: str
    traceback: str | None = None                    # only for exception
    occurred_at: datetime

class AgentInfo(BaseModel):
    name: str
    version: str
    mode: Literal["out-of-box", "in-box"]
    model: ModelSpec | None = None

class TrialState(StrEnum):
    QUEUED = "queued"
    CLAIMED = "claimed"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

class FailureReason(StrEnum):
    AGENT_ERROR = "agent_error"
    AGENT_TIMEOUT = "agent_timeout"
    ENV_START_FAILURE = "env_start_failure"
    ENV_HEALTHCHECK_FAILED = "env_healthcheck_failed"
    VERIFIER_ERROR = "verifier_error"
    VERIFIER_TIMEOUT = "verifier_timeout"
    TRAJECTORY_FLUSH_FAILED = "trajectory_flush_failed"
    EXHAUSTED_RETRIES = "exhausted_retries"
    WORKER_LOST_CLAIM = "worker_lost_claim"
    INTERNAL_ERROR = "internal_error"
```

### 4.6 VerifierResult — referenced from §2.4

```python
class VerifierResult(BaseModel):
    rewards: dict[str, float]
    checks: list[CheckResult] = []
    confidence: float | None = None                 # 0.0–1.0; LLM judges populate
    structured: dict[str, Any] | None = None        # schemaless extras
    error: VerifierError | None = None              # structured failure (NOT exception)

    @field_validator("confidence")
    @classmethod
    def _confidence_range(cls, v):
        if v is not None and not 0.0 <= v <= 1.0:
            raise ValueError("confidence must be in [0, 1]")
        return v

class CheckResult(BaseModel):
    name: str
    passed: bool
    score: float | None = None
    message: str | None = None
    duration_sec: float | None = None

class VerifierError(BaseModel):
    kind: Literal["missing_tests", "parse_failure", "exec_failure", "timeout", "internal"]
    message: str
    detail: dict[str, Any] = {}
```

### 4.7 Persistent storage layout (Postgres)

Core tables:

```sql
CREATE TABLE teams (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE team_quotas (
    team_id UUID PRIMARY KEY REFERENCES teams(id),
    fair_share_weight REAL NOT NULL DEFAULT 1.0,
    max_attempts INT NOT NULL DEFAULT 3,
    in_flight_count INT NOT NULL DEFAULT 0          -- materialized; trigger-maintained
);

CREATE TABLE tasks (
    id TEXT PRIMARY KEY,                            -- task_metadata.id
    checksum TEXT NOT NULL,                         -- dirhash sha256
    config JSONB NOT NULL,                          -- serialized TaskConfig
    source TEXT,                                    -- git URL / package ref / local path
    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE agents (
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    mode TEXT NOT NULL,                             -- 'out-of-box' | 'in-box'
    spec JSONB NOT NULL,                            -- import path, factory args
    PRIMARY KEY (name, version)
);

CREATE TABLE workers (
    id UUID PRIMARY KEY,
    hostname TEXT NOT NULL,
    version TEXT NOT NULL,
    capabilities JSONB NOT NULL,                    -- list[Capabilities]
    registered_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL                            -- 'active' | 'draining' | 'gone'
);

CREATE TABLE trials (
    id UUID PRIMARY KEY,
    team_id UUID NOT NULL REFERENCES teams(id),
    task_id TEXT NOT NULL REFERENCES tasks(id),
    config JSONB NOT NULL,                          -- serialized TrialConfig
    requires_caps JSONB NOT NULL,                   -- scalar fields matching Capabilities
    state TEXT NOT NULL,                            -- TrialState
    failure_reason TEXT,
    submit_priority INT NOT NULL DEFAULT 100,
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    cancellation_requested_at TIMESTAMPTZ,
    cancellation_observed_at TIMESTAMPTZ,
    worker_id UUID REFERENCES workers(id),
    attempt_count INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    result JSONB,                                   -- serialized TrialResult once terminal
    trajectory_index JSONB                          -- {trajectory_uri, atif_uri, bytes, events, checksum, ...}
);

CREATE INDEX idx_trials_state_queued ON trials(state) WHERE state = 'queued';
CREATE INDEX idx_trials_team_inflight ON trials(team_id) WHERE state IN ('claimed','running');
CREATE INDEX idx_trials_worker ON trials(worker_id) WHERE worker_id IS NOT NULL;

CREATE TABLE tokens (
    token_hash BYTEA PRIMARY KEY,                   -- sha256(raw)
    type TEXT NOT NULL,                             -- 'worker' | 'team'
    scopes TEXT[] NOT NULL,
    team_id UUID REFERENCES teams(id),              -- nullable for worker tokens
    issued_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ
);

CREATE TABLE rate_cards (
    id TEXT PRIMARY KEY,                            -- hash of the table; e.g. "2026-06-01-anthropic-v3"
    captured_at TIMESTAMPTZ NOT NULL,
    table JSONB NOT NULL                            -- {(provider, model, tier, region): {input_per_mtok, output_per_mtok, cache_*}}
);
```

`trial_result.steps` is denormalized into `trials.result` (JSONB). For step-level analytics later we may materialize a separate `step_results` table; v1 keeps it embedded.

### 4.8 ATIF v1.7 projection mapping

The projector at `_finalize_trajectory` consumes the local JSONL and emits an ATIF v1.7 document. Mapping table (Loom event → ATIF field):

| Loom event | ATIF location |
|---|---|
| `trial_start` | `trajectory.session_id`, `trajectory.trajectory_id`, `metadata.task_*`, `metadata.agent_*` |
| `step_start` | new `steps[i]` entry with `step_id` |
| `llm_call` | `steps[i].messages`, `steps[i].metrics.{input_tokens,output_tokens,...}`, `steps[i].cost_usd` |
| `tool_use` | `steps[i].tool_calls[]` |
| `agent_thought` | `steps[i].reasoning_content` |
| `env_exec` | NOT projected to ATIF (Loom-internal observability) |
| `verifier_*` | `metadata.verifier_*` |
| `step_end` | `steps[i].is_copied_context = false`, `steps[i].llm_call_count` (count of llm_call events for this step) |
| `trial_end` | `metadata.final_state`, `metadata.reward` |
| `trial_error` | `metadata.error` |

Aggregation rules:
- `steps[i].llm_call_count` = count of `llm_call` events with matching `step_id`.
- **If `llm_call_count == 1`**: populate `steps[i].messages`, `steps[i].reasoning_content`, `steps[i].metrics.*` directly from that single call.
- **If `llm_call_count > 1`**: ATIF v1.7 explicitly accepts aggregate metrics without per-call attribution. Loom projects as follows:
  - `steps[i].messages` = the **final** call's `messages` array (terminal context that produced the agent's last action this step). The intermediate calls' messages are NOT lost — they remain in the Loom event log forever and can be retrieved via `GET /trials/{id}/events?kind=llm_call&step_id={step}`. The projection only chooses what ATIF sees.
  - `steps[i].metrics.input_tokens` etc. = SUM across all `llm_call` events for that step.
  - `steps[i].cost_usd` = SUM of per-call `cost_usd_snapshot`.
  - `steps[i].reasoning_content` = concatenation of all `agent_thought` events for that step, separated by `\n---\n`.
- **If `llm_call_count == 0`**: deterministic step (no LLM). `steps[i].messages`, `reasoning_content`, and `metrics` MUST be absent per ATIF v1.7.
- `steps[i].is_copied_context` = `false` in v1 (no context-copy semantics yet; v1.5 if needed).

The projector is a pure function — re-runnable when ATIF v1.8 ships.

### 4.9 Forward declaration audit

Cross-checking that every type referenced from §1–§3 is defined in §4:

| Forward decl | Defined in |
|---|---|
| `TrajectoryEvent` and subtypes | §4.4 |
| `TrajectoryWriter` / `TrajectoryReader` | §3.5 (behavior) + interface in §4.10 below |
| `MCPConnection` | §4.2 |
| `HealthcheckSpec` | §4.2 |
| `NetworkPolicy` (+ kinds) | §4.2 |
| `TrialConfig` (+ RetryPolicy, BackoffSpec, RetryReason) | §4.3 |
| `ModelSpec` | §4.2 |
| `VerifierResult` / `CheckResult` / `VerifierError` | §4.6 |
| `Aggregator` / `AggregatorFn` | §2.4 (kept there) |
| `Capabilities` / `RequiredCapabilities` | §2.3 (+ scalar fields canonicalized for SQL) |
| `ExecResult` | §2.2 |
| `MAX_EXEC_STREAM_BYTES` | constant; §2.2 |
| `FINALIZE_TIMEOUT_SEC` / `STATE_PATCH_TIMEOUT_SEC` | constants; §3.3 |

### 4.10 `TrajectoryWriter` / `TrajectoryReader` interface

```python
class TrajectoryWriter:
    async def append(self, event: TrajectoryEvent) -> None: ...
    async def __aenter__(self) -> "TrajectoryWriter": ...
    async def __aexit__(self, *exc) -> None: ...      # final flush guaranteed
    @property
    def local_path(self) -> Path: ...                  # path to on-disk JSONL
    @property
    def remote_uri(self) -> str: ...                   # s3://... once first flush completes

class TrajectoryReader:
    """Iterates events from a JSONL source (local file or remote MinIO URI)."""
    def iter_all(self) -> Iterator[TrajectoryEvent]: ...
    def tail(self, n: int) -> list[TrajectoryEvent]: ...
    def iter_kind(self, kind: EventKind) -> Iterator[TrajectoryEvent]: ...
    def excerpt(self, strategy: ExcerptStrategy, *, max_tokens: int) -> list[TrajectoryEvent]:
        """Reader-side selection for LLMJudgeVerifier. Strategies:
           - tail(n): last n events
           - all: every event (subject to max_tokens trimming, oldest-first)
           - tool_use_only: only tool_use + llm_call events
           - step_summary(aggregate): one summary event per step
           Token-budget pruning is enforced at the strategy level.
        """
```

---

### Section 4 self-review — fixes applied inline before approval

Issues found in the first draft and corrected in the above:
1. **`Public`/`NoNetwork`/`Allowlist` were originally bare classes** — gave them explicit Pydantic discriminators (`kind` Literal field) for correct serialization.
2. **`RateCardHash` was implicit** — added a `rate_cards` table in §4.7 so historical cost replays are deterministic.
3. **Missing `provider_extras` schema** — defined as `dict[str, int]` (named counters), not opaque `dict[str, Any]` (RFC0001 R3 critique applies here).
4. **`ATIF v1.7 projection mapping` was missing** — added §4.8 with the explicit Loom-event → ATIF-field table.
5. **`TrajectoryWriter`/`TrajectoryReader` interfaces** — were referenced from §3 but no formal definition; §4.10 now provides one with `excerpt()` resolving the §2.4/§3.6 open question.
6. **`StepError` was missing** — defined in §4.5 with `phase` × `reason` × structured fields.
7. **`AgentInfo.mode`** — added so persisted results record whether agent was OOB or in-box.
8. **`tokens.type`** discriminator added — distinguishes worker vs team tokens at the row level.

Issues NOT fixed in this section (and why):
- Cross-team priority preemption — explicitly v2 per §2.6.
- Step-level analytics view — denormalized in `trials.result` JSONB; materialized table is v2.
- ATIF `is_copied_context = true` semantics — no v1 use case; v1.5 if SFT pipelines need it.

### Section 4 explicit decisions

1. **`task.toml` is the on-disk format** (TOML, not YAML/JSON). Matches Harbor and `pyproject.toml` convention.
2. **Tasks are content-addressed** by `task_checksum = dirhash(dir, sha256)`. Stored on every trial.
3. **Implicit single-step is synthesized at parse time** to `[StepConfig(name="main")]`.
4. **Raw token usage stored permanently; cost is derived** from `rate_card_hash`-keyed lookup. `loom_cost_replay` CLI recomputes against any rate card.
5. **`llm_call.provider_extras: dict[str, int]`** (named counters, not opaque dict).
6. **`TrialResult.steps` is the source of truth**; no trial-level `agent_result`/`verifier_result` (fixes Harbor wart C).
7. **MinIO is the data plane**; Postgres holds only the index. `trials.trajectory_index` is a small JSONB pointer block; the JSONL lives in `s3://trajectories/`.
8. **All trajectory events share `(trial_id, step_id, seq)`** for deterministic ordering and re-projection.
9. **ATIF v1.7 projection is a pure function** over the local JSONL; re-runnable for schema bumps.
10. **`rate_cards` table is admin-managed**; rate-card hash is captured at LLM call time and frozen on the event.
11. **Excerpt strategies for LLMJudgeVerifier** are first-class: `tail(n)`, `all`, `tool_use_only`, `step_summary` — token-budget-aware, oldest-first pruning.

✅ APPROVED 2026-06-05 (self-review pass; user delegated approval per §"do the self-review; you don't need my approval")

---



## Section 5 — Error Handling ✅ APPROVED 2026-06-05

Mechanisms partially specified in §2.8 and §3.3 are consolidated here with the full taxonomy. Five subsections: timeout taxonomy, exception taxonomy, retry behavior, partial-failure semantics, and observability of errors.

### 5.1 Timeout taxonomy

Every timeout has: a default, an override on `TrialConfig`, a multiplier, and what happens on expiry.

| Timeout | Default (sec) | Override field | Multiplier | On expiry |
|---|---|---|---|---|
| Env build | from `task.environment.build_timeout_sec` (1200) | `override_env_build_timeout_sec` | `env_build_timeout_multiplier` | Trial fails with `env_start_failure`; retry-eligible if `retry_on` includes `env_start_failure` |
| Env healthcheck (single attempt) | `healthcheck.timeout_sec` (3) | — | — | Counts as one failed retry inside healthcheck loop |
| Env healthcheck (all retries) | `healthcheck.retries × healthcheck.interval_sec` + `start_period_sec` | — | — | Trial fails with `env_healthcheck_failed`; retry-eligible if configured |
| Agent setup (in-box only) | from `task.agent.setup_timeout_sec` (360) | — | — | Trial fails with `agent_error` (kind=`setup_timeout`) |
| Agent run | from `task.agent.timeout_sec` (1800) | `override_agent_timeout_sec` | `agent_timeout_multiplier` | Step records `StepError(phase='agent', reason='timeout')`; continues to verifier; aggregate may still produce partial reward |
| Verifier | from `task.verifier.timeout_sec` (300) | `override_verifier_timeout_sec` | `verifier_timeout_multiplier` | Step records `StepError(phase='verifier', reason='timeout')`; step short-circuits; trial may still succeed if other steps did |
| Worker heartbeat (Control Plane sweep) | 15s (worker considered dead) | server config | — | All worker's trials reclaimed to `queued` with `next_attempt_at = NOW() + 30s` |
| Finalize trajectory | `FINALIZE_TIMEOUT_SEC` = 60 | — | — | Trial downgraded to `failed` with `trajectory_flush_failed` (per §3.3) |
| State PATCH | `STATE_PATCH_TIMEOUT_SEC` = 15 | — | — | Best-effort; worker logs and continues; crash detector eventually reclaims |
| `Driver.exec` (single call) | per-call `timeout_sec` arg (None = no limit) | caller-supplied | — | `asyncio.TimeoutError` raised from `exec()`; whoever called it decides what to do |
| Worker drain (SIGTERM) | `--drain-timeout-sec` 600 | CLI flag | — | Remaining trials receive `CancelledError`; their own §3.3 finally runs |

**Resolution rule** (canonical, repeated from §4.3):
```
final_timeout = (override OR task_default) × multiplier
```

### 5.2 Exception taxonomy

All Loom internal exceptions inherit from a single root. Public exceptions are stable; internal are namespaced.

```python
class LoomError(Exception):
    """Root for all Loom-defined exceptions."""

# Driver layer
class DriverError(LoomError): pass
class DriverAlreadyStartedError(DriverError): pass
class DriverNotStartedError(DriverError): pass
class DriverExecError(DriverError):
    return_code: int
    stdout: bytes
    stderr: bytes

# Agent layer
class AgentError(LoomError): pass
class AgentSetupTimeoutError(AgentError): pass

# Verifier layer
class VerifierError(LoomError):
    """NOT raised by VerifierResult.error — that's a struct field. This is raised
    when the verifier framework itself fails (registry lookup, dispatch)."""

# Trajectory layer
class TrajectoryError(LoomError): pass
class TrajectoryFlushFailedError(TrajectoryError): pass

# Control plane / worker comm
class WorkerLostClaimError(LoomError):
    """Raised by fenced state-update endpoints when the worker no longer owns the trial."""

# Configuration
class ConfigError(LoomError): pass
class TaskSchemaError(ConfigError): pass
class CapabilityMismatchError(ConfigError):
    """Raised at trial init when required caps can't be satisfied."""
```

**Classification function** (used in `Trial.run()` to set `failure_reason`):

```python
def classify_failure(exc: BaseException) -> FailureReason:
    match exc:
        case AgentSetupTimeoutError(): return FailureReason.AGENT_ERROR
        case asyncio.TimeoutError():   return FailureReason.AGENT_TIMEOUT     # context-dependent; see note
        case DriverError():            return FailureReason.ENV_START_FAILURE
        case VerifierError():          return FailureReason.VERIFIER_ERROR
        case TrajectoryError():        return FailureReason.TRAJECTORY_FLUSH_FAILED
        case _:                        return FailureReason.INTERNAL_ERROR
```

**Note on `asyncio.TimeoutError` ambiguity:** the classifier above is wrong if the timeout came from the verifier or env. `Trial.run()` catches and classifies timeouts AT each phase boundary instead — `_run_step` records the phase-specific `StepError`, and the trial-level classification only fires for unhandled exceptions bubbling out of the step loop (which is rare; most timeouts are step-local).

### 5.3 Retry behavior

A trial is retry-eligible iff:
1. `attempt_count < config.retry.max_attempts`, AND
2. the failure's `RetryReason` ∈ `config.retry.retry_on`.

Mapping from terminal failure → `RetryReason`:

| Failure | RetryReason |
|---|---|
| Worker crash (heartbeat lapse) | `worker_crash` |
| `env_start_failure` | `env_start_failure` |
| `env_healthcheck_failed` | `env_start_failure` (subsumed) |
| `agent_timeout` | `agent_timeout` |
| `verifier_timeout` | `verifier_timeout` |
| `trajectory_flush_failed` | `trajectory_flush_failed` |
| `agent_error` / `verifier_error` (non-timeout) | — (NOT retry-eligible by default; semantic errors don't benefit from retry) |
| `internal_error` | — (escalate to ops) |
| `worker_lost_claim` | implicit — handled by reclamation, not by retry policy |

**Backoff computation:**

```python
def next_attempt_at(attempt_count: int, backoff: BackoffSpec) -> datetime:
    delay = min(backoff.base_sec * (backoff.multiplier ** (attempt_count - 1)),
                backoff.max_sec)
    jittered = delay * random.uniform(1 - backoff.jitter, 1 + backoff.jitter)
    return now() + timedelta(seconds=jittered)
```

After `max_attempts`, the trial transitions to `failed` with `failure_reason='exhausted_retries'` and the original `failure_reason` recorded on the *last* step result.

**Worker-crash specific path** (reclaim, not worker-driven retry):
- Crash detector PATCHes state back to `queued` with `next_attempt_at = NOW() + 30s`
- It does NOT increment `attempt_count` (the increment happens at next claim — see §2.6 claim query `attempt_count = attempt_count + 1`)
- So worker-crash retries get full backoff budget; non-crash retries (e.g., env failures) consume the same budget but go through the explicit worker-driven path

### 5.4 Partial-failure semantics

Loom's central principle for partial failure: **continue collecting data wherever it's still meaningful**.

**Step-level partial failure** (per §3.4):
- Agent timeout / agent crash → continue to artifact collection + verifier (verifier may compute partial credit from whatever was produced)
- Artifact collection failure (e.g., glob pattern fails inside sandbox) → continue to verifier with empty `artifacts_dir`; verifier may explicitly fail or run a degraded check
- Verifier failure → step result has `verifier_result=None` and `error.phase='verifier'`; subsequent steps in the trial still execute (multi-step only)
- `step.min_reward` threshold failure → subsequent steps are skipped, but already-completed steps' results are kept; trial succeeds with the partial step list

**Trial-level partial failure:**
- One step failing → trial proceeds to next step (unless `min_reward` short-circuits)
- All steps with `verifier_result=None` → trial state may still be `succeeded` if no exception bubbled out; reward aggregation produces `None` (no data) instead of 0 (which would imply "we measured and got zero")
- Final state: `succeeded` if no terminal exception; `failed` only if an unrecoverable exception escaped the step loop

**Trajectory partial-failure:**
- Trajectory write failure (disk full, file handle lost) escalates to `TrajectoryFlushFailedError` from within the writer; bubbles out of `Trial.run()` and into the outer finally; trial finalizes as `failed`
- MinIO upload failure during flush is retried with backoff (3 attempts); final failure escalates as above
- Local file present but MinIO unreachable at finalize → `_finalize_trajectory` times out (§3.3); trial downgraded to `failed`/`trajectory_flush_failed`; the local file remains and may be re-uploaded by a future worker (v1.5 — v1 just logs the orphan)

**Verifier "soft-failure" via `VerifierResult.error`:**
- A verifier that *intentionally* reports a failure mode (missing tests, parse error of judge response) sets `result.error` and returns NORMALLY. This is NOT an exception. The step records the result with `verifier_result.error` populated; trial proceeds to next step / finalization. Aggregation treats `error != None` results as 0-contribution in mean/min strategies.

### 5.5 Error observability

Every error path emits trajectory events so postmortem doesn't require log diving.

| Failure | Trajectory events emitted | Where |
|---|---|---|
| Env start failure | `env_start` then `trial_error` | inner except in `Trial.run()` |
| Agent timeout (step) | `step_end` with `StepResult.error.reason='timeout'` | `_run_step` |
| Agent crash (step) | `step_end` with `StepResult.error.from_exc=...` | `_run_step` |
| Verifier timeout | `step_end` with `StepResult.error.phase='verifier'` | `_run_step` |
| Verifier soft-failure | `verifier_end` event with `result.error` populated; `step_end` follows normally | `_run_step` |
| Trajectory flush failure | `trial_error` (best-effort — may be the failure cause itself) | outer except |
| Cancellation | `trial_cancelled` | inner CancelledError branch |
| Worker lost claim | `worker_lost_claim` (best-effort; may not be flushed if worker disappears) | fenced state PATCH on `WorkerLostClaimError` |
| Worker drain interrupted | `worker_drain_interrupted` | SIGTERM handler |

**System-level errors** (Control Plane crashes, Postgres unavailable, MinIO unreachable) emit OpenTelemetry spans + Prometheus metrics. The Control Plane SHOULD NOT crash workers — workers that can't reach Control Plane fall into a degraded mode (continue executing in-flight trials with local-only trajectory writes; PATCH state on reconnect via fenced UPDATE which will succeed iff our worker_id still owns the row).

### 5.6 Section 5 self-review — fixes applied inline

1. **`asyncio.TimeoutError` classification was wrong in initial draft** — would have classified all timeouts as agent timeouts. Corrected: timeouts are classified at the phase that issued the `wait_for`, NOT in the trial-level classifier. Added explicit note in §5.2.
2. **Worker-crash retry path was conflated with worker-initiated retry** — split into separate text; the crash detector does NOT increment `attempt_count` (the next claim does, per §2.6 SQL).
3. **`VerifierError` collision** — there's both a `VerifierError` *exception* (raised by framework dispatch) AND a `VerifierError` *struct field* (returned in `VerifierResult.error`). Originally I called them both `VerifierError`. Renamed: the exception type stays `VerifierError` (in `loom.errors`); the struct stays `VerifierError` (in `loom.models.verifier`) and is fully qualified in docs. Code uses different imports; not a runtime collision.
4. **MinIO unreachable at finalize was previously orphan-only** — added explicit "future worker may re-upload" path as a v1.5 capability; v1 just preserves the local file (no auto re-upload).
5. **`worker_lost_claim` event emission is best-effort** — flagged because the worker may have already lost its trajectory writer at that point. Document this honestly rather than promising a flush.

### Section 5 explicit decisions

1. **Phase-local timeout classification** — timeouts are caught at the phase that issued them; only the rare unhandled timeout bubbles to the trial-level classifier.
2. **Semantic errors are NOT retry-eligible by default.** Only resource/transient failures (`worker_crash`, `env_start_failure`, `agent_timeout`, `verifier_timeout`, `trajectory_flush_failed`) appear in the default `retry_on` set; users can opt-in others per-trial.
3. **Worker-crash reclaim does not consume an attempt**; reclaim PATCHes back to `queued` and the next claim increments `attempt_count`.
4. **Continue-on-step-failure** is the default — agent/verifier/artifact failures don't short-circuit the trial unless `min_reward` triggers it.
5. **Verifier soft-failure** via `VerifierResult.error` is the standard pattern; exceptions are reserved for framework-level failures.
6. **Aggregation treats `verifier_result.error != None` as 0-contribution** in `mean`/`weighted` strategies; in `min` strategy a soft-failure pulls the trial reward to 0.
7. **System-level errors emit OTEL spans + Prometheus metrics**, not trajectory events (trajectory is for the trial; system observability is separate).
8. **Workers degrade gracefully** when Control Plane is unreachable — keep executing in-flight, write trajectory locally, reconcile state on reconnect via fenced PATCH.

✅ APPROVED 2026-06-05 (self-review pass; user delegated approval)

---



## Section 6 — Testing Strategy ✅ APPROVED 2026-06-05

Four tiers (unit, contract, integration, end-to-end) plus a property-test layer on the state machine. Each tier names the swap points the spec made for it.

### 6.1 Tier overview

| Tier | What | Fakes used | Speed budget | Where it runs |
|---|---|---|---|---|
| Unit | Pure logic: protocol satisfaction checks, schema validators, classification functions, backoff math, fairness query construction, ATIF projection mapping | none | <100ms / test | `tests/unit/` |
| Contract | Verify each Protocol's concrete implementations satisfy the contract via shared parametrized suites | FakeDriver / FakeAgent / FakeVerifier / FakeLLMGateway | <500ms / test | `tests/contract/` |
| Integration | Trial.run() lifecycle with in-process fakes for Driver + LLM Gateway; real Postgres (testcontainers); real MinIO (testcontainers) | FakeDriver only | <5s / test | `tests/integration/` |
| End-to-end | Full stack: real DockerDriver, real Postgres, real MinIO, real LLM Gateway proxying a faked-provider, worker + control plane processes | none | <30s / test (target); <2min hard cap | `tests/e2e/` |
| Property | Hypothesis tests over the trial state machine, fairness scheduling under random submissions, retry behavior | full fakes | <10s / property | `tests/property/` |

### 6.2 Swap points the design provides

Every "real" component has a fake counterpart designed in:

- **`Driver`** → `FakeDriver` (in-memory filesystem, deterministic exec results from a registered command table, instant start/stop). Used by every tier except E2E.
- **`AgentRuntime`** → `ScriptedAgent` (replays a pre-recorded sequence of trajectory events + tool uses) and `OracleAgent` (runs a `solution.sh` from the task). Lets us exercise the trial loop without needing real LLM calls.
- **`Verifier`** → `FakeVerifier(returns: VerifierResult)` for known-result tests; `ScriptVerifier` itself for tests with deterministic check logic.
- **LLM Gateway** → `FakeLLMGateway` accepts OpenAI-compatible requests, returns scripted responses, emits `llm_call` trajectory events identical to the real Gateway. Critical for integration tests that exercise the proxy path without burning tokens.
- **`TrajectoryWriter`** → `InMemoryTrajectoryWriter` captures events to a list; used for assertions in unit + contract + integration.
- **MinIO** → testcontainers MinIO for integration; in-memory for unit (`FakeObjectStore`).
- **Postgres** → testcontainers Postgres for integration + E2E; in-memory `sqlite` is NOT used (different SQL dialect would mask bugs in the DRF query).

### 6.3 Unit tests — what's covered

```
tests/unit/
  test_schema_validation.py        # Pydantic validators on TaskConfig, TrialConfig, NetworkPolicy union, MCPConnection
  test_classify_failure.py         # exception → FailureReason
  test_backoff.py                  # next_attempt_at correctness incl. jitter bounds
  test_aggregate_step_rewards.py   # mean/min/weighted/final strategies, error-as-zero rule
  test_atif_projection.py          # event log → ATIF v1.7 mapping
  test_excerpt_strategies.py       # tail/all/tool_use_only/step_summary correctness + token budget
  test_fair_share_query.py         # generated SQL parses, parameters bind correctly (against test PG)
  test_normalize_steps.py          # implicit single-step synthesis
  test_capability_match.py         # requires_caps × worker_caps row matching
  test_token_hash.py               # hash-only storage, length, prefix tags
```

Coverage target: **≥90% of `loom/` excluding driver implementations and FastAPI routes** (those are integration-tested).

### 6.4 Contract tests — parametrized suites

The pattern: one test file per Protocol, marked with `@pytest.mark.parametrize("impl", ALL_IMPLS)` over every concrete implementation. New implementations register their fixture and inherit the suite.

```
tests/contract/
  test_driver_contract.py          # start/stop/exec/upload/download/healthcheck/network — for DockerDriver + FakeDriver
  test_agent_contract.py           # run() emits events, respects timeout, populates context — for LiteLLMAgent + ClaudeCodeAgent + ScriptedAgent
  test_verifier_contract.py        # verify() returns VerifierResult with rewards/checks/error — for Pytest/Script/LLMJudge/Structured/Composite/FakeVerifier
  test_aggregator_contract.py      # for MEAN/MIN/MAX/WEIGHTED + custom AggregatorFn examples
```

Contract test cases worth calling out:
- Driver: `stop()` is idempotent (call it before start, after start, and twice — all succeed without raising).
- Driver: `exec()` raises `DriverNotStartedError` before start and after stop.
- Driver: `exec()` truncates at `MAX_EXEC_STREAM_BYTES` and sets `truncated=True`.
- Agent: `run()` emits `step_start`, `step_end` events with matching `step_id`.
- Agent: cancellation propagates through `run()` cleanly (CancelledError raised, no orphaned tasks).
- Verifier: `verify()` returns `VerifierResult` even on framework-internal failure (uses `result.error`, doesn't raise — exception types `VerifierError` reserved for dispatch failures).

### 6.5 Integration tests — trial lifecycle slices

Each test boots Postgres + MinIO containers, instantiates Control Plane in-process (`fastapi.TestClient`), and runs one or more `TrialRunner`s with `FakeDriver` + `ScriptedAgent` + `FakeVerifier`. No real Docker.

Critical scenarios:

```
tests/integration/
  test_happy_path.py
    - Single-step trial: submit → claim → run → success; verify trajectory events + TrialResult + MinIO contents
    - Multi-step trial: 3 steps, all succeed; verify per-step results + aggregated reward
  test_multi_step_short_circuit.py
    - Step 2 fails min_reward; verify step 3 skipped, trial succeeds with partial reward
  test_cancellation.py
    - cancel queued → state=cancelled, no worker involved
    - cancel claimed → worker observes state on next check, releases
    - cancel running → worker bails at step boundary, finalize+state PATCH still run
  test_agent_timeout.py
    - ScriptedAgent runs longer than agent_timeout_sec
    - Verify StepError(phase='agent', reason='timeout')
    - Verify verifier still runs and may produce partial credit
  test_verifier_soft_failure.py
    - FakeVerifier returns VerifierResult with error populated (not raised)
    - Verify trial state is succeeded (no exception bubbled), reward aggregation treats as 0-contribution
  test_trajectory_flush_failed.py
    - FakeObjectStore raises on PUT
    - Verify _finalize_trajectory times out; trial downgraded to failed/trajectory_flush_failed
    - Verify terminal state PATCH still fires
  test_worker_lost_claim.py
    - Trial claimed by worker A; force-reclaim via crash detector
    - Worker A's subsequent state PATCH gets 0 rows (fenced); worker logs + aborts; trial reclaimed cleanly
  test_fairness_drf.py
    - 3 teams submit 10 trials each simultaneously
    - 2 workers claim trials
    - Verify each team's in-flight is balanced within ±1 over time
  test_retry_policy.py
    - Trial with retry_on={env_start_failure}, max_attempts=3
    - FakeDriver fails first 2 starts, succeeds 3rd
    - Verify backoff timing + final success
  test_in_box_loom_aware.py
    - InBoxAgent CLI mock writes JSONL to /loom/trajectory.jsonl inside FakeDriver
    - Verify host TrialRunner tails it and emits events identically to OOB path
  test_separate_verifier_env.py
    - verifier_env_mode=separate
    - Verify agent driver stops, fresh driver starts, artifacts upload into it
  test_phase_network_policy.py
    - StepConfig.network.agent_phase = NoNetwork; verifier_phase = Allowlist
    - Verify driver.set_network_policy is called with the right values + restored on exit
```

### 6.6 End-to-end tests — real Docker, real everything

Slowest tier. Runs in CI on `[loom-e2e]` label only (manual + nightly), not on every PR. Goal: verify that real Docker + boto3 + LiteLLM behave as the integration tier's fakes assumed.

```
tests/e2e/
  test_oracle_succeeds.py          # OracleAgent + DockerDriver + real PytestVerifier; canonical task
  test_litellm_agent_smoke.py      # LiteLLMAgent against a stubbed-provider LLM Gateway
  test_real_minio_upload.py        # large artifact upload + download + checksum match
  test_worker_drain_sigterm.py     # send SIGTERM, verify trials finalize before exit
```

E2E uses **testcontainers** for Postgres + MinIO and a **stubbed LLM provider** (a small FastAPI app the Gateway forwards to) so tests don't depend on Anthropic/OpenAI being reachable or burn tokens.

### 6.7 Property-based tests

`hypothesis` strategies over:

- **Trial state machine**: generate random sequences of valid state transitions (with random fail/retry/cancel inputs) and assert: (a) terminal states are absorbing, (b) `attempt_count` never exceeds `max_attempts` for non-crash failures, (c) every active state has a heartbeat-checked owner.
- **Fairness scheduler**: generate random submission patterns (per-team rate, priority, capabilities) and assert: (a) no team is starved if every team has queued trials, (b) DRF property — `in_flight × weight` stays within 1 of the team with max DRF score.
- **Backoff math**: jittered delays stay in `[delay × (1-j), delay × (1+j)]`; multiplier respects `max_sec` cap.
- **ATIF projection**: round-trip property — for any synthesized event sequence, projecting + re-projecting yields the same ATIF doc (idempotency).

### 6.8 Test fixtures and shared scaffolding

`tests/conftest.py` provides:

```python
@pytest.fixture
def postgres():
    with PostgresContainer("postgres:16") as pg:
        run_migrations(pg.url())
        yield pg

@pytest.fixture
def minio():
    with MinioContainer() as m:
        m.create_bucket("trajectories")
        m.create_bucket("artifacts")
        yield m

@pytest.fixture
def control_plane(postgres, minio):
    app = create_control_plane(postgres.url(), minio.endpoint())
    with TestClient(app) as client:
        yield client

@pytest.fixture
def fake_driver():
    yield FakeDriver(filesystem={}, exec_table={})

@pytest.fixture
def scripted_agent_factory():
    def make(events: list[TrajectoryEvent]) -> ScriptedAgent:
        return ScriptedAgent(events=events)
    return make

@pytest.fixture
def trial_runner(control_plane, fake_driver, scripted_agent_factory):
    # Standard TrialRunner wired with fakes
    ...
```

### 6.9 Performance tests (post-v1)

NOT in v1. Planned for v1.1:

- Submit-to-claim latency under load (target: p50 <100ms with 1k queued trials)
- Trajectory write throughput (target: 1k events/sec sustained per trial)
- DRF query plan stability with `EXPLAIN ANALYZE` at 10k queued trials × 50 teams
- Worker scale-up: register 100 workers, verify Control Plane is responsive

### 6.10 Test data fixtures

`tests/fixtures/tasks/` contains canonical task directories:

- `tasks/hello-world/` — minimal single-step task: instruction "print hello", verifier checks stdout
- `tasks/skillflow-mini/` — small SkillFlow-shaped task for development
- `tasks/multi-step-3/` — 3-step task with min_reward gating
- `tasks/in-box-cli/` — task that requires an in-box agent (for testing InBox path)
- `tasks/healthcheck-flaky/` — task whose healthcheck fails N times before succeeding (tests retry within healthcheck loop)
- `tasks/large-artifact/` — task producing a 1GB artifact (E2E only; tests multipart streaming)

Each task ships with `solution/solve.sh` so `OracleAgent` can run it for verification.

### 6.11 Section 6 self-review — fixes applied inline

1. **Original draft used SQLite for unit tests of the DRF query** — bad call because PostgreSQL-specific syntax (`SKIP LOCKED`, `jsonb` operators, `LATERAL`) would either fail or behave differently. Corrected: all SQL-touching tests use testcontainers Postgres, even at the "unit" tier where they're actually narrow integration tests.
2. **Contract tests didn't have an idempotency check for `Driver.stop`** — added explicitly (§6.4) since this was a §3 self-review finding.
3. **Property tests on the state machine were vague** — pinned three concrete invariants (terminal absorption, attempt budget, ownership).
4. **Performance benchmarks were originally mixed in with E2E** — separated into §6.9 since they're not pass/fail gates and run on different cadence.
5. **`tasks/large-artifact/` was missing from fixtures** but the design includes multi-GB artifact support — added.

### Section 6 explicit decisions

1. **Five tiers** (unit, contract, integration, E2E, property). Every Protocol gets a contract test suite.
2. **Real Postgres + MinIO via testcontainers** for integration and E2E. No SQLite. No in-memory PG.
3. **Real Docker only in E2E**. Integration uses `FakeDriver`.
4. **LLM Gateway has a fake** that emits the same trajectory events as the real one. Integration tests never touch real providers.
5. **E2E runs on a separate CI label** (`loom-e2e`), nightly + manual; not on every PR.
6. **Coverage target ≥90% on `loom/`** excluding driver implementations and FastAPI routes.
7. **Property tests pin three state-machine invariants** + DRF fairness + backoff math + ATIF projection idempotency.
8. **Performance tests are v1.1**, not v1.

✅ APPROVED 2026-06-05 (self-review pass; user delegated approval)

---

## Section 7 — Cross-cutting Concerns ✅ APPROVED 2026-06-05

The previous six sections cover the runtime mechanics. This section covers operational fundamentals that span every section: schema migration, logging, metrics, secrets, and configuration loading. These are real v1 requirements without which the system can't be deployed.

### 7.1 Schema migrations — Alembic

The repo already includes Alembic (`alembic>=1.13,<2.0` in `pyproject.toml`). Loom adopts it as the canonical migration tool for Postgres.

- **Location:** `migrations/versions/` (Alembic standard layout). `alembic/env.py` reads `LOOM_DB_URL` from environment.
- **Convention:** every PR that changes §4.7 tables ships an Alembic migration in the same commit. CI fails if a model change has no matching migration.
- **Schema versioning on JSONB columns** (`trials.config`, `tasks.config`, `trial.result`): the inner Pydantic models carry their own `schema_version: Literal["1"]` field (already specified in §4). Migrations *between* JSONB schema versions are application-level: a `loom data migrate --from 1 --to 2` CLI walks rows and updates `schema_version` + payload. The DB schema only changes when we add/remove columns; JSONB-internal evolution is data migration, not schema migration.
- **Downgrade discipline:** every migration ships an `op.execute(...)` downgrade. We don't promise downgrade across major versions, but within a minor version line downgrades must work for emergency rollback.
- **Test fixture:** the testcontainers Postgres in §6.5 runs `alembic upgrade head` before yielding the connection.

### 7.2 Logging — structured JSON to stdout

All Loom processes (Control Plane, Workers, LLM Gateway) emit structured JSON logs to stdout. Stderr is reserved for unhandled crashes only. This matches k8s + sidecar log shippers (Fluent Bit, Vector) without configuration.

**Log record schema:**

```python
class LogRecord(BaseModel):
    ts: datetime                    # UTC, ISO-8601 with µs precision
    level: Literal["debug", "info", "warn", "error", "fatal"]
    msg: str                        # free-form, MUST be a literal string (no f-strings)
    service: Literal["control-plane", "worker", "llm-gateway"]
    component: str                  # e.g. "scheduler", "trial-runner", "driver.docker"
    # Correlation fields — emitted whenever in scope
    trace_id: str | None = None     # OpenTelemetry trace ID
    span_id: str | None = None
    trial_id: UUID | None = None
    step_id: str | None = None
    worker_id: UUID | None = None
    team_id: UUID | None = None
    # Domain extras — anything additional, named keys only (no opaque "extra: dict")
    **kwargs: Any
```

Loom uses `structlog` with a JSON renderer; the correlation fields are added via `contextvars` (`bind_contextvars(trial_id=...)` at the top of `TrialRunner.run()`) so they auto-attach to every log line in scope.

**Log levels:**
- `debug` — disabled in production by default; enable per-team via Control Plane flag
- `info` — normal operations (trial state transitions, worker registration, large milestones)
- `warn` — recoverable abnormalities (retry triggered, heartbeat lapse detected, trajectory flush retried)
- `error` — failures we caught and handled (trial failed, env didn't start)
- `fatal` — about to exit the process

**Forbidden patterns:**
- No `print()` anywhere in the codebase (lint rule).
- No f-string log messages — use `log.info("trial_state_changed", from_state=..., to_state=...)` so messages are aggregable across instances.

### 7.3 Metrics — Prometheus enumeration

Each service exposes `/metrics` (default port 9090) with the following metrics. Cardinality is bounded per label by design — no per-trial label values (use trace_id in spans for that).

**Control Plane:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `loom_trials_state_total` | Counter | `from_state`, `to_state`, `team_id` | Trial state transitions |
| `loom_trials_inflight` | Gauge | `team_id`, `state` | Current trials in `claimed`/`running` per team |
| `loom_queue_depth` | Gauge | `team_id` | Queued trials per team |
| `loom_claim_latency_sec` | Histogram | `result` (`hit`/`miss`) | Time for `POST /trials/claim` to return |
| `loom_state_patch_total` | Counter | `endpoint`, `result` (`ok`/`fenced`/`timeout`) | Trial state PATCH outcomes |
| `loom_workers_active` | Gauge | (none) | Workers with fresh heartbeat |
| `loom_worker_reclaim_total` | Counter | (none) | Trials reclaimed by crash detector |

**Worker:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `loom_trial_duration_sec` | Histogram | `terminal_state`, `failure_reason` | Trial wall time |
| `loom_phase_duration_sec` | Histogram | `phase` (`env_start`/`agent_run`/`verifier`/`finalize`) | Per-phase wall time |
| `loom_driver_exec_total` | Counter | `result` (`ok`/`timeout`/`truncated`) | Sandbox exec calls |
| `loom_trajectory_flush_total` | Counter | `result` (`ok`/`retry`/`failed`) | Trajectory flush attempts |
| `loom_trajectory_bytes_total` | Counter | (none) | Cumulative bytes uploaded |
| `loom_runner_concurrent` | Gauge | (none) | Trials in-flight on this worker |

**LLM Gateway:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `loom_llm_calls_total` | Counter | `provider`, `model`, `team_id` | LLM calls proxied |
| `loom_llm_latency_sec` | Histogram | `provider`, `model`, `streamed` | End-to-end call latency |
| `loom_llm_tokens_total` | Counter | `provider`, `model`, `kind` (`input`/`cached_input`/`cache_write`/`output`/`thinking`) | Tokens by category |
| `loom_llm_errors_total` | Counter | `provider`, `error_kind` | Provider failures |

OTEL spans complement these — every `Trial.run()` is a parent span; LLM calls, exec calls, verifier checks are child spans. Spans carry `trial_id`/`step_id` attributes for query-side joins.

### 7.4 Secrets management

Two classes of secrets:

**1. LLM provider API keys** — owned by the LLM Gateway, never seen by Workers or Control Plane.
- **Storage:** k8s `Secret` mounted as env vars on the Gateway deployment (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, etc.). For non-k8s deploys (local dev, VM): `.env` file with strict mode (`set -a; source .env; set +a`).
- **Rotation:** rolling restart of Gateway pods reloads env vars. No in-process key rotation — too risky for v1.
- **Per-team keys (v1.5):** if needed, the Gateway can be configured with `LITELLM_TEAM_KEYS_JSON=...` mapping `team_id → provider keys`. v1 uses single global keys; team-level cost attribution comes from token usage × rate card, not separate billing identities.

**2. Loom-internal secrets** — worker tokens, team tokens, database password.
- **Worker tokens:** provisioned by admin via `POST /admin/worker-tokens` (returns the raw token once; only hash is stored). Admin embeds in k8s secret on the worker deployment.
- **Team tokens:** issued via the existing platform auth surface (already inherited).
- **Database password:** k8s secret → env var on Control Plane (`LOOM_DB_URL` is the full DSN; password is part of it).
- **MinIO credentials:** k8s secret on Control Plane (`LOOM_MINIO_ACCESS_KEY`, `LOOM_MINIO_SECRET_KEY`). Signed URLs sent to workers don't expose these.
- **Never:** any secret in trajectory events, log lines, or stored in `tasks.config` JSONB.

A lint rule rejects any string in code that matches secret patterns (`sk-`, `Bearer ey...`); the canonical pattern is to read from env at startup and pass via DI.

### 7.5 Configuration loading

All three services use `pydantic_settings.BaseSettings` for typed config from env + optional file.

**Control Plane** (`loom_control_plane/config.py`):

```python
class ControlPlaneSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="LOOM_CP_", env_file=".env", extra="forbid")

    db_url: PostgresDsn                        # LOOM_CP_DB_URL
    minio_endpoint: str                        # LOOM_CP_MINIO_ENDPOINT
    minio_access_key: SecretStr                # LOOM_CP_MINIO_ACCESS_KEY
    minio_secret_key: SecretStr                # LOOM_CP_MINIO_SECRET_KEY
    llm_gateway_url: HttpUrl                   # LOOM_CP_LLM_GATEWAY_URL
    bind_host: str = "0.0.0.0"
    bind_port: int = 8080
    log_level: LogLevel = "info"
    metrics_port: int = 9090
    worker_heartbeat_expiry_sec: int = 15
    worker_reclaim_sweep_interval_sec: int = 30
```

**Worker** (`loom_worker/config.py`):

```python
class WorkerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="LOOM_WORKER_", env_file=".env", extra="forbid")

    control_plane_url: HttpUrl
    worker_token: SecretStr                    # LOOM_WORKER_TOKEN
    max_concurrent: int = 5
    drain_timeout_sec: int = 600
    claim_poll_interval_sec: float = 1.0
    heartbeat_interval_sec: float = 5.0
    trajectory_cache_dir: Path = Path("/var/lib/loom/trajectories")
    log_level: LogLevel = "info"
    metrics_port: int = 9090
    # Driver configs
    docker_socket: Path = Path("/var/run/docker.sock")
```

**LLM Gateway** (`loom_llm_gateway/config.py`): provider keys + LiteLLM config + same logging/metrics fields.

**Config precedence:** env vars > `.env` file > defaults. No support for nested config files; if it's complex enough to need one, it's complex enough to need a Helm chart.

**Validation at startup:** all three services call `settings = Settings()` at top of `main`. Any missing required field → immediate exit with a clear message ("LOOM_CP_DB_URL is required"). No lazy validation.

### 7.6 Cross-cutting Section 7 self-review

Issues found and fixed inline:
1. **Original `Log record schema` had `extra: dict[str, Any]`** — would have given an escape hatch that defeats structured logging. Replaced with `**kwargs` at the dataclass level meaning concrete log calls pass named keyword args (the JSON renderer captures them), but no opaque dict on the record itself.
2. **Initial metrics list had per-trial labels** (e.g., `trial_id` as a label) — cardinality explosion. Moved trial-level identity to trace span attributes, kept labels bounded.
3. **Secrets section initially said "LiteLLM rotates keys hot"** — overpromised. Walked back to "rolling restart of pods reloads"; in-process hot rotation is v1.5+.
4. **Config used `BaseSettings` v1 syntax** — corrected to `SettingsConfigDict` (pydantic-settings 2.x) since the rest of the spec is Pydantic v2.

### Section 7 explicit decisions

1. **Alembic for Postgres schema migrations.** JSONB inner-schema migration via app-level `loom data migrate` CLI.
2. **Structured JSON logs to stdout, no f-strings, correlation IDs via contextvars.** `structlog` with JSON renderer.
3. **Prometheus metrics enumerated** — bounded cardinality. OTEL spans cover high-cardinality trial-level identity.
4. **LLM provider keys live only in the Gateway**, mounted as env vars. Workers and Control Plane never see them.
5. **Pydantic-settings v2** for all config; env vars with prefixes; `.env` file fallback; validation at process startup.
6. **`/metrics` on port 9090** for all services. Bounded labels by design.
7. **Lint rules** forbid `print()`, f-string log messages, and inline secret literals.

✅ APPROVED 2026-06-05 (self-review pass; user delegated approval)

---

## Final spec self-review (per brainstorming skill)

Per the brainstorming skill's checklist, doing a final pass on the whole spec before user review.

### 8.1 Placeholder scan

Searched for `TODO`, `TBD`, `XXX`, `???`, `[FILL IN]`:
- ✅ None found in approved sections (§1–§6)
- Forward-marker phrases used legitimately: "(v2)", "(v1.5)", "deferred" — these are intentional scope markers, not unfilled placeholders.

### 8.2 Internal consistency

Cross-section claims that have to match:

| Claim | §1 | §2 | §3 | §4 | §5 | §7 | Consistent? |
|---|---|---|---|---|---|---|---|
| 6-state trial FSM | – | §2.8 | §3.10 | §4.5 `TrialState` enum | §5.3 retry path | metrics §7.3 | ✅ |
| `worker_id` fencing on PATCH | – | §2.8 | §3.3, §3.10 | – | §5.5 worker_lost_claim | metric `loom_state_patch_total` | ✅ |
| `requires_caps` scalar matching | – | §2.6 SQL | §3.1.1 derivation | §4.7 column | – | – | ✅ |
| `verifier_env_mode` shared default | – | §2.4 | §3.8 default | §4.7 (in config) | §5.4 partial-failure | – | ✅ |
| Hard timeouts on finalize/state-PATCH | – | – | §3.3 60s/15s | §4.9 constants | §5.1 timeout table | – | ✅ |
| `MAX_EXEC_STREAM_BYTES` = 10 MB | – | §2.2 | §3.5 | §4.9 | §5.1 | – | ✅ |
| ATIF projection deterministic/replayable | – | – | §3.7 | §4.8 mapping | – | – | ✅ |
| Raw usage stored + cost derived | – | – | – | §4.4.1 + §4.7 `rate_cards` | – | metric `loom_llm_tokens_total` per kind | ✅ |
| Verifier soft-failure via `result.error` | – | §2.4 | – | §4.6 | §5.4, §5.6 | – | ✅ |
| OutOfBox agent crash doesn't kill worker | – | §2.8 | – | – | §5.4 | metric `loom_runner_concurrent` stays positive | ✅ |
| Postgres is sole writer for state | §1 | §2.6, §2.8 | §3.10 | §4.7 | §5.5 degraded mode | §7.1 Alembic | ✅ |
| Pydantic v2 throughout | – | §2 type sketches | §3 type sketches | §4 BaseModel | §5 BaseModel | §7.5 pydantic-settings v2 | ✅ |
| Structured logs / no f-strings | – | – | – | – | §5.5 OTEL | §7.2 explicit rule | ✅ |

No contradictions found.

### 8.3 Scope check

Spec covers exactly the v1 runtime core: Trial / Agent / Driver / Verifier / Trajectory + the minimum control plane + worker fabric + cross-cutting operational fundamentals (migrations, logging, metrics, secrets, config). Explicitly out-of-scope and deferred:
- Service layer (FastAPI team-facing API, dashboard, RBAC) → separate spec (task #3)
- Benchmark integrations (SkillFlow, SkillLearnBench) → separate spec (task #2)
- Agent integrations (Claude Code shim, OpenHands shim) → separate spec (task #1)
- `loom` CLI surface → small standalone spec or folded into service layer
- Docker registry + image distribution policy → ops doc
- Worker version compatibility matrix → ops doc
- v2 features: subprocess isolation for OOB agents, durable mid-step resume, cross-team priority preemption, elastic worker scaling, SSE live tail, second/third env backends
- v1.5 features: webhook notifications, streaming exec, large-artifact streaming upload, in-box fallback for non-Loom-aware CLIs, automatic re-upload of orphaned local trajectories, per-team LLM API keys

Scope is appropriate for a single implementation plan.

### 8.4 Ambiguity check

Searched for phrases like "may", "might", "possibly", "should consider", "TBD":
- "May produce partial credit" (§3.4, §5.4) — intentional: the verifier *may* but isn't required to. Documented.
- "May raise" / "may fail" — used for documented exception paths. Not ambiguous.
- "v2 if needed" — explicit deferral; not ambiguity.
- "Should cooperate with asyncio" — documented constraint on agent implementations. Not a Loom-side ambiguity.
- "Should ship" / "should include" in §7 — these are normative requirements for v1 implementations, not soft suggestions.

No requirements would be interpreted two ways by a careful implementer.

### 8.5 Final readiness statement

The spec is internally consistent, scope-appropriate, and free of placeholders or load-bearing ambiguities. Ready for implementation planning.

**Total approved sections:** 7 (architecture, contracts, data flow, data model, error handling, testing, cross-cutting).
**Total explicit decisions:** 77 across all sections.
**Total lines:** ~2,200.
