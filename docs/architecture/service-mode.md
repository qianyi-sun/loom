# Service mode

Service mode runs Loom as a distributed cluster: a FastAPI Control
Plane owns trial state, Workers poll for work and execute trials,
an LLM Gateway centralizes provider calls + cost attribution, and a
REST `loom_service` + React SPA give researchers and admins a UI.

Postgres + MinIO are the only stateful services.

## Service-prefix convention

| Service | Routes mounted at | Audience |
|---|---|---|
| `loom_service` | `/api/v1/*` | External (SPA, `curl`, customer scripts) |
| Control Plane | root (`/trials`, `/workers/...`, `/admin/worker-tokens`, etc.) | Workers + `loom_service` (cluster-internal only) |
| LLM Gateway | `/v1/*` (OpenAI dialect), `/v1beta/*` (Gemini dialect), `/admin/*` | Agents from inside sandboxes |

Layering: SPA / external curl → `loom_service` (`/api/v1/*`) → CP
over cluster DNS. `deploy/k8s/ingress.yaml` exposes only
`loom_service` (at `loom.example.com`) and the LLM Gateway (at
`gateway.loom.example.com`); CP is reachable only inside the cluster.
For operator-side admin curls, port-forward CP:
`kubectl port-forward deploy/loom-control-plane 8080:8080`.

## Process model

```
                        ┌──────────────────────────────────────┐
                        │     loom_service (REST + SPA)        │
                        │  /api/v1/{trials, batches,         │
                        │     benchmarks, tasks, tokens,       │
                        │     usage, rate-cards, teams,        │
                        │     trajectory, atif, health}        │
                        └────┬──────────────────┬──────────────┘
                             │                  │
                             ▼                  ▼
        ┌────────────────────────┐    ┌─────────────────────┐
        │   Control Plane        │    │   LLM Gateway       │
        │   POST /trials         │    │   /v1/chat/         │
        │   POST /trials/claim   │    │       completions   │
        │   PATCH /trials/{id}/  │◄───┤   /v1/messages      │
        │         state          │    │   /v1/responses     │
        │   POST /trials/{id}/   │    │   /v1beta/models/   │
        │        cancel          │    │       {model_path}  │
        │   POST /workers/...    │    │   /admin/rate-cards │
        │   ...                  │    │   (LiteLLM-backed)  │
        └────┬───────────────────┘    └─────────┬───────────┘
             │                                  │
             │       ┌──────────────────────────┘
             │       │
             ▼       ▼
        ┌────────────────────┐    ┌──────────────┐
        │   Worker (×N)      │    │   MinIO      │
        │   claim → run →    │───►│  (S3-compat) │
        │   PATCH state →    │    │              │
        │   finalize         │    │  trajectories│
        │                    │    │  + atif      │
        └────┬───────────────┘    └──────────────┘
             │
             ▼
        ┌──────────────┐
        │   Postgres   │  ← all state lives here
        │              │     (trials, llm_calls,
        │              │      cloud_compute_records,
        │              │      teams, tokens, ...)
        └──────────────┘
```

Stateless services (Control Plane, Gateway, Worker, loom_service) can
all scale horizontally. Postgres is the durability boundary.

## Trial lifecycle (one happy path)

```
   Researcher          Service              Control Plane         Worker                    Sandbox
   ----------          -------              -------------         ------                    -------
        |                  |                      |                  |                          |
   POST trials -----------> POST /trials --------> INSERT trial      |                          |
        | (via SPA            (forwarder)          (state=queued)    |                          |
        |  or curl)            |                      |              |                          |
        |                      |                     [trial visible  |                          |
        |                      |                      in DRF queue]  |                          |
        |                      |                      |              |                          |
        |                      |                      | <-- POST /trials/claim --|              |
        |                      |                      | DRF SQL (CTE + UPDATE):                 |
        |                      |                      | SELECT ... FOR UPDATE OF t SKIP LOCKED  |
        |                      |                      | UPDATE trials SET state='claimed',      |
        |                      |                      |   worker_id=:wid, claimed_at=NOW(),     |
        |                      |                      |   attempt_count=attempt_count+1         |
        |                      |                      | ----------- claim row ----> spawn Trial.run()
        |                      |                      |                                         |
        |                      |                      |              | --- start --> [container/cloud-sandbox]
        |                      |                      |              | upload bundle           |
        |                      |                      |              | set baseline policy     |
        |                      |                      |              | (still state=claimed)   |
        |                      |                      |              |                          |
        |                      |                      | <-- PATCH -- | state=running           |
        |                      |                      |   (fenced)   |                          |
        |                      |                      |              | exec_streaming(agent argv)
        |                      |                      |              | -----------------------> |
        |                      |                      |              | <<-- events stream --    |
        |                      |                      |              |     append to local      |
        |                      |                      |              |     trajectory.jsonl     |
        |                      |                      |              |     (1 MB / 100 ev /     |
        |                      |                      |              |      10 s flush to MinIO |
        |                      |                      |              |      multipart, ≥ 5 MiB  |
        |                      |                      |              |      mid-trial parts)    |
        |                      |                      |              |                          |
        |                      |                      |              | LLM calls --HTTP-->   ┌─────────┐
        |                      |                      |              |                       │ Gateway │
        |                      |                      |              | <-- resp + cost ----- └─────────┘
        |                      |                      |              |   (writes llm_calls    (also writes
        |                      |                      |              |    row before resp)    to Postgres)
        |                      |                      |              |                          |
        |                      |                      |              | verifier.run()           |
        |                      |                      | <-- PATCH -- | state=succeeded|failed   |
        |                      |                      |   (fenced)   |                          |
        |                      |                      |              | finalize (always runs):  |
        |                      |                      |              |  1. last trajectory part |
        |                      |                      |              |  2. fetch llm_calls      |
        |                      |                      |              |     (CP HTTP)            |
        |                      |                      |              |  3. project_to_atif      |
        |                      |                      |              |  4. upload atif.json     |
        |                      |                      |              | PATCH /trajectory_index  |
        |                      |                      |              | --- stop --> [delete sandbox]
        |                      |                      |              |                          |
   GET /trials/{id} ----------> GET /api/v1/         | <-- SELECT --|                          |
                                  trials/{id}                                                   |
        | <-- result + atif URL ----- (forwarder) -- |              |                          |
```

State machine: see [overview.md](overview.md#state-machine) — six
states only (`queued → claimed → running → succeeded | failed |
cancelled`). Finalize is a side-effect of reaching a terminal state,
not a state of its own.

## Fencing

Every Worker → CP state PATCH carries `(state, worker_id,
failure_reason?)`. The CP's `PATCH /trials/{id}/state` UPDATE matches
on both `id = :trial_id` AND `worker_id = :worker_id`; if a different
Worker has taken the trial over (heartbeat timeout → crash detector
reassigned ownership), the UPDATE matches zero rows and the CP
returns 409. Two Workers can never both think they own a trial.

`loom_worker.HttpControlPlaneClient` translates `409 Conflict` →
`False` return (the Worker logs + abandons the trial). The trial
stays in whatever state the new owner has put it in.

## DRF scheduling

`team_quotas` table carries `fair_share_weight` and an `in_flight_count`
counter per team. The claim SQL (canonical in
`src/loom_control_plane/scheduler/claim.py`) is a single CTE + UPDATE
that atomically transitions one trial from `queued` to `claimed`:

```sql
WITH next AS (
  SELECT t.id
    FROM trials t
    JOIN team_quotas q ON q.team_id = t.team_id
   WHERE t.state = 'queued'
     AND t.attempt_count < q.max_attempts
     AND (t.next_attempt_at IS NULL OR t.next_attempt_at <= NOW())
     AND t.requires_caps->>'os' = ANY(:worker_os)
     AND t.requires_caps->>'gpu_vendor' = ANY(:worker_gpu_vendors)
     AND (t.requires_caps->'network_policies') <@ (:worker_network_policies)::jsonb
   ORDER BY (q.in_flight_count * 1.0) / NULLIF(q.fair_share_weight, 0) ASC,
            t.submit_priority DESC,
            t.submitted_at ASC
   LIMIT 1
   FOR UPDATE OF t SKIP LOCKED
)
UPDATE trials t
   SET state = 'claimed',
       worker_id = :worker_id,
       claimed_at = NOW(),
       attempt_count = attempt_count + 1
  FROM next
 WHERE t.id = next.id
 RETURNING t.id, t.team_id, t.task_id, t.config, t.requires_caps,
           t.attempt_count;
```

Ordering, most-important to least:
1. Lowest `in_flight_count / fair_share_weight` (Dominant Resource
   Fairness — the team with the smallest current share of the fleet
   wins)
2. Highest `submit_priority`
3. Oldest `submitted_at`

The eligibility predicate also filters by worker capabilities (OS,
GPU vendor, supported network policies) and retry windows
(`attempt_count`, `next_attempt_at`).

`FOR UPDATE OF t SKIP LOCKED` lets multiple Workers claim
concurrently without double-claiming.

Hypothesis property tests in `tests/property/test_drf_fairness_property.py`
verify fairness invariants across many simulated worker/team
configurations.

## LLM Gateway

`src/loom_llm_gateway/`:

- LiteLLM-backed proxy for OpenAI/Anthropic/Google + native httpx
  passthrough for dialect-specific routes
- Bearer auth (team-scoped or step-scoped JWT)
- Rate-card lookup (`rate_cards` table) per (provider, model)
- Cost compute at request time; row inserted into `llm_calls` BEFORE
  the response returns (so finalize can fetch a guaranteed-complete
  set)
- Per-call attribution via `(team_id, trial_id, step_id)` fields on
  the `llm_calls` row

Routes (mounted at the Gateway service root — agents inside
sandboxes hit them directly):

| Method | Path | Notes |
|---|---|---|
| POST | `/v1/messages` | Anthropic dialect |
| POST | `/v1/chat/completions` | OpenAI dialect |
| POST | `/v1/responses` | OpenAI Responses dialect |
| POST | `/v1beta/models/{model_path}` | Gemini dialect (Google's `v1beta` namespace) |
| POST | `/admin/rate-cards` | Upsert rate card (gated on `admin:rate_cards` scope) |
| GET | `/healthz` | Liveness |

## Auth tokens

The current token model is suitable for local and shared development. The
production auth redesign is tracked in
[`auth-threat-model.md`](auth-threat-model.md): singleton admin secret,
admin-approved team registration, audit logging, and rotation must ship before
this model is treated as production-grade.

Four token kinds, all bearer-format:

| Prefix | Issued by | Scope |
|---|---|---|
| `team:*` | Operator (`POST /tokens` with `admin:tokens`) | Long-lived; submit trials, view results |
| `worker:*` | Auto-issued at worker `POST /workers/register` | Long-lived; claim trials, PATCH state |
| `step:*` | Worker mints per-step JWT (`mint_step_token`) | Short-lived (per-step); CLI agent calls Gateway with bounded scope |
| `admin:*` | Operator (DB seed or admin issue) | Manage tokens, rate-cards, teams |

`tokens` table tracks `last_seen_at` per token for rotation hygiene.

## Auto-cancellation: trial source-state awareness

Cancel requests (`POST /trials/{id}/cancel`) check the trial's current
state before propagating:
- `queued` → DB UPDATE state=cancelled only (Worker never sees it)
- `claimed` → DB UPDATE + Worker's per-trial cancel channel fires on
  next heartbeat / when Trial.run() reaches a checkpoint
- `running` → DB UPDATE + cancel channel fires; Driver tears down
  sandbox; finalize still runs (cancelled trials still get an ATIF
  document for audit)
- `succeeded | failed | cancelled` → no-op

## Persistence schema

| Table | Owner | Purpose |
|---|---|---|
| `trials` | CP | State machine + DRF claim |
| `llm_calls` | Gateway | Per-call cost attribution; FK to trial |
| `tasks` + `benchmarks` | Operator CLI | Cluster task catalog |
| `team_quotas` | CP | DRF weights + license allowlists |
| `tokens` | Service / CP | Bearer tokens (all 4 kinds) |
| `batches` | Service | Batch grouping + idempotency keys |
| `cloud_compute_records` | Cloud drivers | Per-sandbox lifetime + cost; `cloud_provider` column |

Migrations: `migrations/versions/0001_initial_schema.py` through
`0008_cloud_compute_records.py`. Run via
`alembic -c migrations/alembic.ini upgrade head`.

## SPA

`web/` — React 18 + Vite + TypeScript + TanStack Query + React Router.
11 pages:

- **TrialsList** — keyset cursor pagination, state filter
- **TrialDetail** — header + paginated trajectory viewer +
  EventTimeline (one row per kind, click-to-expand JSON) +
  ATIF download button
- **BatchesList** + **BatchDetail** — `refetchInterval: 5000`
  while state ∈ {submitted, running}; stops on terminal
- **NewBatch** — textarea JSON parse for `task_filter` +
  `trial_config` with local validation
- **Tasks** — cluster task catalog
- **Benchmarks** — registered adapters
- **RateCardsAdmin** — list + create form (gated on
  `admin:rate_cards`)
- **UsageDashboard** — date-range picker + inline SVG bar chart +
  breakdown table; `daytona_compute_seconds` + `daytona_cost_usd`
  surfaced via a CTE join against `cloud_compute_records` (see
  `src/loom_service/routes/usage.py`)
- **Settings** — own team tokens + admin token create/revoke
- **NotFound**

Auth model: token-paste into the SPA login form, stored in
`localStorage`. The API client surfaces 401s via a callback that
auto-clears the token and bounces back to the login form.

**Deployment:** `deploy/Dockerfile.web` is a multi-stage build —
node-slim builds the Vite bundle, nginx-alpine serves it.
`deploy/k8s/web.yaml` is a 2-replica Deployment + Service on port
80. The `loom-ingress` routes `loom.example.com/api/v1/*` to
`loom-service:8090` and everything else to `loom-web:80`; nginx
inside `loom-web` falls back to `index.html` for client-side
React Router paths.

For local dev: `cd web && npm run dev` runs Vite's dev server on
:5173 with HMR. Vite's `server.proxy` config sends `/api/*` to
`localhost:8090`, which is what `docker-compose.dev.yml` exposes
`loom-service` at — no separate proxy config needed.

## Cross-cutting concerns

- **Logging** — structlog JSON to stdout. Correlation IDs via
  `bind_trial_context`. Does **not** nest — bind at outermost scope.
- **Metrics** — Prometheus on `/metrics` (each service binds its own
  port, default 9090). Bounded cardinality — no per-trial labels.
- **License enforcement** — per-team `license_allowlist` (default
  `[MIT, Apache-2.0, BSD-3-Clause, CC-BY-4.0]`). `POST /trials`
  returns 403 if the task's adapter declares a license outside the
  allowlist.
- **Idempotency** — batches + trials accept an `idempotency_key`;
  partial unique index `WHERE NOT NULL` on `trials.idempotency_key`.
  Cross-team collisions return 409.
- **Worker shutdown ordering** — stop heartbeat thread first
  (prevents fence-bump races); then drain in-flight trials with a
  configurable budget (default 5 min); then exit.

## See also

- [overview.md](overview.md)
- [cli-mode.md](cli-mode.md) — same `Trial.run()`, no server stack
- [trajectory-and-atif.md](trajectory-and-atif.md) — what the
  trajectory writer + ATIF projector produce
- [driver-protocol.md](driver-protocol.md) — sandbox contract the
  Worker calls
- `../operator-runbook.md` — production deployment + ops
- `src/loom_control_plane/` — CP source
- `src/loom_llm_gateway/` — Gateway source
- `src/loom_worker/` — Worker source
- `src/loom_service/` — REST surface
- `web/` — SPA
