# Service mode

Service mode runs Loom as a distributed cluster: a FastAPI Control
Plane owns trial state, Workers poll for work and execute trials,
an LLM Gateway centralizes provider calls + cost attribution, and a
REST `loom_service` + React SPA give researchers and admins a UI.
Team and worker auth still use database-backed bearer tokens, while admin auth
uses the file-backed singleton secret specified in
[auth-registration-spec.md](auth-registration-spec.md).

Postgres + MinIO are the only stateful services.

Local service-mode development uses `docker compose`, so operators need Docker
CLI with the Compose plugin before running `loom service up`, `down`, or
`status`. On macOS, install and start Docker Desktop first and confirm
`docker compose version` succeeds.

## Service-prefix convention

| Service | Routes mounted at | Audience |
|---|---|---|
| `loom_service` | `/api/v1/*` | External (SPA, `curl`, customer scripts) |
| Control Plane | root (`/trials`, `/workers/...`, `/admin/worker-tokens`, etc.) | Workers + `loom_service` (cluster-internal only) |
| LLM Gateway | `/v1/*` (Loom-attributed dialects), `/openai/v1/*` (provider facade for stock SDKs), `/v1beta/*` (Gemini dialect), `/admin/*` | Agents from inside sandboxes |

Layering: SPA / external curl → `loom_service` (`/api/v1/*`) → CP
over cluster DNS. `deploy/k8s/ingress.yaml` exposes only
`loom_service` under `/api/v1` and `loom-web` at `/` on the configured
public host. The Control Plane and LLM Gateway are reachable only inside the
cluster; sandbox traffic reaches the Gateway through the worker's
subprocess gateway URL, singleton/gateway-router path, or another
sandbox-facing internal route, not public Ingress. For operator-side admin curls,
port-forward CP:
`kubectl port-forward deploy/loom-control-plane 8080:8080`.

The SPA's launch model picker is BYO-provider aware. It reads
`GET /api/v1/provider-connections` for the connection list and
`GET /api/v1/models` for the default, classifier-filtered model catalog.
`GET /api/v1/models?view=raw` preserves noisy provider entries with
`hidden_reason` metadata for debugging. Manual model ids are stored with
`POST /api/v1/provider-connections/{id}/models` and are submitted on
Trial/Batch payloads as `provider_connection_id` + `provider_model_id`.
`GET /api/v1/agents` also carries service-mode readiness metadata:
`service_mode_ready`, `readiness_message`, and a `runtime_contract`
covering sandbox executables/modules, provider dialect, env vars, and
capture mode. The SPA disables unavailable agents with a setup-needed
message, and service submit routes enforce the same readiness check so
clients cannot create doomed batches by bypassing the browser.

Model-backed `litellm` submissions may include
`trial_config.request_params` for safe request controls such as
`temperature`, `top_p`, `seed`, max output limits, reasoning effort,
tool-choice mode, and provider decoding extras. The Control Plane
validates the trial config, the worker strips payload/secret-bearing
keys before the call, and the LLM Gateway persists the effective
non-sensitive controls in `llm_calls.request_params` for trial/batch
debug evidence. This field is not a generic subprocess-agent setting
channel; external CLI adapters must expose their own runtime-compatible
settings path.

Use `loom agents audit-runtime --image <trial-sandbox-image>` to verify
that the resolved trial image contains the executables and Python modules
declared by the agent catalog. For tasks with `environment.docker_image`,
that is the configured image. For tasks with `environment.dockerfile`, the
worker builds a deterministic `loom-task:<hash>` image from the materialized
task bundle before the first trial and reuses that local image on later
trials with the same task checksum, Dockerfile path, and optional
`environment.docker_build_context`. Build-only contexts should live under
`.loom-build/`; workspace materialization skips that directory so hidden build
assets are not copied into the agent-visible workdir. Before a cache-miss
build, the worker rejects contexts above `LOOM_TASK_IMAGE_BUILD_MAX_FILES`
(default 2000) or `LOOM_TASK_IMAGE_BUILD_MAX_BYTES` (default 536870912), and
`build_timeout_sec` bounds the Docker build call. Worker-created docker-py
clients also use `LOOM_WORKER_DOCKER_API_TIMEOUT_SEC`, which must be at least
as large as the largest expected pull/build/sidecar budget so the SDK does not
time out before the task's own timeout policy. Tasks may also declare
`environment.sidecars`; Docker-backed workers start those auxiliary containers
on the same per-trial network, wait for declared healthchecks through the final
Docker probe's timeout window, pass `environment.environment` into the primary
sandbox, and clean the sidecars up after the trial. This checks the trial
sandbox boundary, not the `loom-worker` container. A clean runtime audit is
necessary before flipping an external adapter to
`service_mode_ready=true`; it still must be followed by a live end-to-end
smoke that proves the adapter can finish a platform trial. Use
`loom agents smoke-runtime --image <trial-sandbox-image>` for that second
gate. It runs each displayed agent through `LocalTrialRunner`, a real
Docker sandbox, and a deterministic provider stub, then reports whether the
platform trial reached `succeeded`.

The repo ships a candidate agent-capable sandbox image for operator smoke
work:

```bash
docker build -f deploy/Dockerfile.agent-sandbox -t loom-agent-sandbox:dev .
loom agents audit-runtime --image loom-agent-sandbox:dev --json
loom agents smoke-runtime --image loom-agent-sandbox:dev --json
```

`deploy/Dockerfile.agent-sandbox` uses Python 3.12 plus Node 22 because
the displayed catalog spans both Python-module agents and modern Node
CLIs. Python CLI-only agents such as `aider` and `mini-swe-agent` are
installed in isolated virtual environments and linked onto `PATH` so
their pinned dependencies do not conflict with OpenHands. `openhands`
and `openhands-sdk` both use Loom's
`loom_launcher.openhands_sdk_runner` module because upstream OpenHands SDK
exposes a Python library rather than a stable one-shot CLI, and the old
`openhands.server` entry point is not a usable non-interactive runner. A
dynamic OpenHands install on top of an arbitrary benchmark task image uses a
pinned `uv` installer to create `/opt/loom-agents/openhands-sdk` with Python
3.12, installs `loom-launcher` from a pinned repository subdirectory ref, then
invokes that venv's interpreter. This fixes Python-version drift in task images
without baking the selected provider or model into the image; model choice is
still passed at trial runtime through the adapter invocation and gateway
environment. A
successful image build or dependency audit does not by itself make an
agent ready: the catalog should only be flipped after a platform-dev trial
smoke passes. As of the #289 all-agent smoke, the displayed catalog is
ready when the selected trial sandbox image satisfies the declared runtime
dependencies; an audit of a thinner image can still report `blocked`.

## Process model

```
                        ┌──────────────────────────────────────┐
                        │     loom_service (REST + SPA)        │
                        │  /api/v1/{trials, batches,         │
                        │     benchmarks, tasks, tokens,       │
                        │     usage, rate-cards, teams,        │
                        │     team registrations,              │
                        │     admin audit events,              │
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
        |                      |                      |              | finalize (always runs):  |
        |                      |                      |              |  1. last trajectory part |
        |                      |                      |              |  2. maybe fetch llm_calls|
        |                      |                      |              |     (CP HTTP; skipped if |
        |                      |                      |              |      agent already wrote |
        |                      |                      |              |      gateway llm events) |
        |                      |                      |              |  3. project_to_atif      |
        |                      |                      |              |  4. upload atif.json     |
        |                      |                      |              | PATCH /trajectory_index  |
        |                      |                      |              |   (result + output index)|
        |                      |                      | <-- PATCH -- | state=succeeded|failed   |
        |                      |                      |   (fenced)   |                          |
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

After a successful trial finalizes, the Worker sends a fenced
`PATCH /trials/{id}/trajectory_index` before reporting terminal
`state=succeeded`. This ordering keeps Control Plane state atomic from
the user's perspective: a trial should not persist as `succeeded`
without the result projection needed for reward gates and detail APIs.
If that projection is rejected or fails, the Worker reports
`trajectory_flush_failed` instead of success so the run remains
diagnosable and rerunnable.

The projection carries two durable outputs:

- `trials.result`: the serialized `TrialResult` plus an
  `aggregate_reward` scalar for list/batch rollups. Trial and batch
  read APIs aggregate `llm_calls` separately for
  `total_prompt_tokens`, `total_completion_tokens`, and
  `llm_calls_count`. Model-backed terminal trials also project
  `llm_evidence_status`; `no_calls_invalid` means the run cannot be used as
  benchmark evidence because the selected model path did not persist any
  gateway calls. Batch detail includes `no_call_trial_count` and promotes a
  zero-call model-backed batch to diagnosis reason `batch.no_llm_calls`.
- `trials.trajectory_index`: trajectory URI, ATIF URI/schema version,
  and the actual uploaded artifact object keys and sizes.

The service detail API uses that projection to set `trajectory_ready`,
`atif_ready`, and artifact download links. Clients should not infer
output availability by guessing MinIO keys.

The same detail API also exposes user-facing diagnosis and debug evidence:

- `GET /api/v1/trials/{trial_id}` includes `debug_evidence`;
  `GET /api/v1/trials/{trial_id}/debug` returns that object directly.
  Trial debug evidence includes per-call `request_params` summaries
  and `request_params_status_counts` so score-alignment audits can
  distinguish matched generation settings from legacy/unavailable
  request-parameter evidence.
- `GET /api/v1/batches/{batch_id}` includes `debug_evidence`;
  `GET /api/v1/batches/{batch_id}/debug` returns that object directly.
  Batch debug evidence carries the same provider summary fields across
  its child calls.
- Run Library batch detail includes the same batch `debug_evidence` after the
  Run Library read policy allows access.
- `GET /api/v1/trials/{trial_id}` and
  `GET /api/v1/batches/{batch_id}` also include `diagnosis`.
  `GET /api/v1/trials/{trial_id}/diagnosis` and
  `GET /api/v1/batches/{batch_id}/diagnosis` return that deterministic
  `DiagnosisReport` directly.
- Run Library batch detail includes the same batch `diagnosis` after the Run
  Library read policy allows access.

Diagnosis reports are schema-versioned and human-readable. They are derived
from redacted debug evidence and persisted run state, not from LLM
summarization. The stable fields are `entity`, `summary`, `primary_cause`,
`impact`, `evidence`, `next_actions`, and `reason_clusters`. Batch diagnosis
clusters failed child trials by stable reason code, reports affected counts
and ratios, and states whether the aggregate score is reliable for
model-quality comparison.

Debug evidence is schema-versioned and machine-readable. The stable fields are
`entity`, `lifecycle`, `worker`, `agent`, `provider`, `failure`, `task` or
`task_selection`, `reward`, `evidence_refs`, and `next_actions`. The
`failure.reason_code` is stable for API/CLI agents and uses prefixes such as
`trial.verifier_error` or `batch.fanout_submit_failed`. The service redacts
bearer tokens, provider keys, secret refs, internal service URLs, and signed
object-store URLs before returning either the detail response or the direct
debug endpoint.

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
     AND (
       COALESCE(t.requires_caps->>'cpu_arch', 'x86_64') = 'any'
       OR COALESCE(t.requires_caps->>'cpu_arch', 'x86_64') = ANY(:worker_cpu_arches)
     )
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

The `cpu_arch` predicate is backward-compatible and conservative: legacy trial
rows without `requires_caps.cpu_arch` are treated as `x86_64`. ARM64 remote
workers therefore only claim tasks explicitly submitted with
`environment.cpu_arch = "arm64"` or `"any"`.

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
- Provider-connection facade cost lookup via
  `provider_connections.rate_card_provider` for BYO endpoints
- Cost compute at request time; row inserted into `llm_calls` BEFORE
  the response returns, so finalize can fetch a guaranteed-complete
  set for adapters that did not already write gateway-backed
  `llm_call` trajectory events.
- Per-call attribution via `(team_id, trial_id, step_id)` fields on
  the `llm_calls` row
- Per-call redacted request-parameter audit via `request_params`;
  legacy rows where this column is NULL surface as
  `status=unavailable_legacy` rather than silently implying matched
  inference settings.

Routes (mounted at the Gateway service root — agents inside sandboxes
hit them through a sandbox-facing Gateway URL):

| Method | Path | Notes |
|---|---|---|
| POST | `/v1/messages` | Anthropic dialect |
| POST | `/v1/chat/completions` | OpenAI dialect |
| POST | `/openai/v1/chat/completions` | OpenAI-compatible provider facade for stock SDKs |
| POST | `/v1/responses` | OpenAI Responses dialect; also routes provider-connection calls when the step JWT carries `provider_connection_id`; OpenAI-compatible provider connections with chat-only upstreams can fall back to `/chat/completions` after the upstream missing-`messages` `/responses` error |
| POST | `/openai/v1/responses` | OpenAI-compatible Responses provider facade for stock SDKs and Codex CLI, including the same chat-only upstream compatibility fallback |
| POST | `/v1beta/models/{model_path}` | Gemini dialect (Google's `v1beta` namespace) |
| POST | `/admin/rate-cards` | Upsert rate card (gated on `admin:rate_cards` scope) |
| GET | `/healthz` | Liveness |

## Auth tokens

Team, worker, and user-owned API credentials remain database-backed. Browser
users sign in with username/password sessions. Admin authority comes from a
file-backed singleton secret or a platform-admin browser user; DB-backed admin
rows are ignored and revoked by migration.

Four token kinds, all bearer-format:

| Prefix | Issued by | Scope |
|---|---|---|
| `loom_api_...` | Team owner or platform user (`POST /api/v1/tokens`) | Named, scoped API token carrying team and creating-user identity; submit trials, view results, optionally manage providers/tokens |
| `worker:*` | Auto-issued at worker `POST /workers/register` | Long-lived; claim trials, PATCH state |
| `step:*` | Worker mints per-step JWT (`mint_step_token`) | Short-lived (per-step); CLI agent calls Gateway with bounded scope |
| `admin:*` | Operator secret file (`loom service init-admin`) | Manage tokens, rate-cards, teams |

Admin callers to `POST /api/v1/tokens`, `POST /api/v1/tokens/{prefix}/rotate`,
and `DELETE /api/v1/tokens/{prefix}` must send `X-Loom-Admin-Actor`; those
token mutations are recorded in `admin_audit_events` with safe metadata
such as names and token hash prefixes. Team callers minting, rotating, or
revoking their own user-owned API tokens do not need an admin actor, but they must
hold `tokens:manage`.

`tokens` table tracks `last_seen_at` and `last_used_at` per token for rotation
hygiene. DB-backed bearer verification debounces those timestamp writes to at
most once per token per 60 seconds so worker heartbeats, claims, and writebacks
do not serialize on one shared token row under high concurrency. The
`admin_audit_events` table records the first #10 backend audit surface: team
registration approve/reject and service-token admin mint/revoke. Wider admin
mutation audit coverage should be added deliberately as follow-up work.

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
| `team_quotas` | CP | DRF weights + legacy license metadata |
| `pending_team_registrations` | Service | Default-closed team onboarding queue and admin review state |
| `team_invites` | Service | Hashed invite links for user membership onboarding |
| `tokens` | Service / CP | Bearer tokens (all 4 kinds) |
| `batches` | Service | Batch grouping + idempotency keys |
| `cloud_compute_records` | Cloud drivers | Per-sandbox lifetime + cost; `cloud_provider` column |

Migrations live in `migrations/versions/` and are applied with
`alembic -c migrations/alembic.ini upgrade head`. Local `loom service up`
runs this automatically after Postgres is healthy. Direct service startup does
not auto-migrate: `loom-service`, the Control Plane, and the LLM Gateway refuse
to start when the database Alembic revision is behind repository code, so
production deploys must run migrations before rolling DB-facing services. Their
production images carry `migrations/alembic.ini` and `migrations/versions/` so
the startup gate compares the live DB against the same migration tree as the
running image.

## SPA

`web/` — React 18 + Vite + TypeScript + TanStack Query + React Router.
Core pages include:

- **Home** — authenticated `/` overview backed by
  `GET /api/v1/overview`. The service aggregates current-team readiness,
  provider health, benchmark readiness, active workers, recent batch/trial
  activity, and next actions so the SPA does not fan out across multiple
  resources on first load.
- **TrialsList** — keyset cursor pagination, state filter
- **TrialDetail** — header + artifact download links + Debug evidence card +
  paginated trajectory viewer + EventTimeline (one row per event,
  click-to-expand JSON) + ATIF download button
- **BatchesList** + **BatchDetail** — `refetchInterval: 5000`
  while state ∈ {submitted, running}; stops on terminal.
  Multi-benchmark batch detail responses include `benchmark_summary`,
  grouped server-side from trial task ids to task benchmark ids and
  benchmark display names, so the SPA can show per-benchmark score,
  expected/completed trial counts, and platform failures without
  parsing task ids. Batch detail also renders the Debug evidence card from the
  same `debug_evidence` object exposed to API and CLI callers.
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
- **Settings** — signed-out username/password login, account request,
  password-reset request, and CLI onboarding; signed-in current team, role,
  team switcher, joined browser members, role-aware setup links, and API-token
  summaries
- **Admin access** — account setup/reset approvals, fixed-team maintenance,
  owner/team-admin legacy invite create/list/revoke/resend, and one-time
  API-token reveal with CLI setup commands. The page is split into role-aware
  sections, and platform-admin invite creation uses a team selector rather than
  requiring raw team ids.
- **NotFound**

Auth model: browser users sign in through `/api/v1/auth/login` with username
and password. The service sets an HttpOnly session cookie; auth responses return
a CSRF token that the API client keeps in memory, sends with
`credentials: "include"`, and attaches to unsafe methods through the configured
CSRF header. A 401 from `/auth/me` means signed out, and later 401s clear
session state/query cache before returning the user to Settings. User-owned
bearer tokens remain supported for CLI/API automation, but the production SPA
no longer stores normal bearer-token login state in `localStorage`.

**Deployment:** `deploy/Dockerfile.web` is a multi-stage build —
node-slim builds the Vite bundle, nginx-alpine serves it.
`deploy/k8s/web.yaml` is a 2-replica Deployment + Service on port
80. The TLS `loom-ingress` routes `<ingress_host>/api/v1/*` to
`loom-service:8090` and everything else to `loom-web:80`; nginx inside
`loom-web` falls back to `index.html` for client-side React Router paths.

For local dev: `cd web && npm run dev` runs Vite's dev server on
:5173 with HMR. Vite's `server.proxy` config sends `/api/*` to
`localhost:8090`, which is what `docker-compose.dev.yml` exposes
`loom-service` at — no separate proxy config needed. The compose web
service pins `node:20.19.5-slim` and uses `npm ci` because
`web/package-lock.json` is bind-mounted; avoid switching back to
`npm install` or a floating Node tag unless lockfile stability has
been re-verified. Dev compose host ports bind to
`${LOOM_DEV_BIND_ADDR:-127.0.0.1}` by default; set
`LOOM_DEV_BIND_ADDR=0.0.0.0` only for deliberate shared-dev exposure.

## Cross-cutting concerns

- **Logging** — structlog JSON to stdout. Correlation IDs via
  `bind_trial_context`. Does **not** nest — bind at outermost scope.
- **Metrics** — Prometheus on `/metrics` (each service binds its own
  port, default 9090). Bounded cardinality — no per-trial labels.
- **License metadata** — `Task.license`, benchmark license fields, and legacy
  `team_quotas.license_allowlist` values are informational. `POST /trials`,
  benchmark readiness, `POST /api/v1/tasks/count`, and batch creation do not
  reject or hide tasks based on SPDX value. `license_execution_policy` tags are
  retained for catalog provenance but are not execution policy inputs.
- **Idempotency** — batches + trials accept an `idempotency_key`;
  partial unique index `WHERE NOT NULL` on `trials.idempotency_key`.
  Cross-team collisions return 409.
- **Task config validation** — `POST /trials` and
  `POST /api/v1/batches` validate the stored `TaskConfig` before
  queueing work. The batch runner filters legacy invalid task rows out
  of existing batches, lowers `expected_trial_count` to the valid slate,
  marks mixed slates `partial_failed`, and finishes all-invalid slates as
  `all_failed` instead of retrying forever.
- **Batch fan-out failures** — child `POST /trials` failures that are
  deterministic policy/config rejections (`400`, `403`, `404`, `409`,
  `422`) are recorded on `batches.fanout_errors`. The runner lowers
  `expected_trial_count`, marks the batch `partial_failed` or
  `all_failed`, and skips the same idempotency key on later ticks. Network
  errors, `429`, and `5xx` remain retryable.
- **Transient gateway retry + failed-case reruns** — trial config defaults
  to retrying `gateway_error` up to 3 attempts with bounded backoff. The
  worker classifies gateway `5xx`, timeouts, connection resets, and remote
  protocol drops as retryable gateway failures, while provider `4xx` remains
  a non-retryable provider/config error. Batch Detail can create a linked
  rerun batch through `POST /api/v1/batches/{id}/rerun-failed`; the child
  batch stores `rerun_of_batch_id` and exact `rerun_targets` so the runner
  re-submits only those task/sample/combination coordinates. Detail views
  expose both original rollups and effective rollups where successful reruns
  replace the original transient failures.
- **Runnable task counts** — benchmark `task_count` and
  `POST /api/v1/tasks/count` are user-facing runnable counts, not raw
  task-table row counts. Placeholder rows with empty or incomplete
  `TaskConfig` data do not make a benchmark look launchable in New Batch.
  `GET /api/v1/benchmarks` also returns raw/valid/invalid counts, compatibility
  license count fields (`license_allowed_task_count` equals valid tasks,
  `license_blocked_task_count` is 0, `blocked_licenses` is empty), readiness
  state, blocker reason, and a user-facing readiness message so the SPA can
  disable blocked benchmarks without hard-coded benchmark names. The batch
  creation path still performs the final strict validation and rejects invalid
  explicit selections with HTTP 400.
- **Task bundle lookup** — workers fetch the full task body from
  `GET /tasks/{task_id}/bundle`; task ids may include slashes such as
  `humaneval/HumanEval/26`.
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
