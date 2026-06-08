# Loom — Service Layer Design

**Status:** DRAFT — awaiting user review.
**Date:** 2026-06-06
**Owner:** Hongjian + Claude.
**Scope:** REST API + thin React SPA that sits above Loom's runtime core (v0.7). Researchers use it to browse runs, view trajectories + ATIF, kick off campaigns, manage their tokens, and (for admins) manage rate cards. Auth reuses Loom bearer tokens.

---

## 1. Goal

v0.7 is `curl`-driven. To make Loom usable by research scientists who don't want to write scripts, ship a thin product layer:

- A REST API service (`loom_service`) reading the same Postgres + MinIO the runtime uses.
- A React SPA consuming the API, focused on the heavy workflows: list trials, drill into a trial, watch a campaign, mint a token.
- Reuse of the existing `tokens` table for auth — no SSO, no OAuth in v1.

Non-goals (deferred to v1.5+): full SaaS platform, per-org billing, RBAC beyond Loom's existing scope model, write-heavy admin UI.

## 2. Architecture

A new sibling service alongside the four existing packages:

```
src/loom_service/                   (NEW package)
  __init__.py
  __main__.py                       # python -m loom_service
  app.py                            # FastAPI factory
  config.py                         # LOOM_SVC_-prefixed settings
  routes/
    trials.py                       # GET /api/v1/trials, /trials/{id}
    trajectory.py                   # /trials/{id}/trajectory (paginated)
    atif.py                         # /trials/{id}/atif
    campaigns.py                    # /campaigns CRUD
    tasks.py                        # /tasks browse
    benchmarks.py                   # /benchmarks browse
    teams.py                        # /teams (current user's team)
    rate_cards.py                   # /rate-cards (admin)
    tokens.py                       # /tokens CRUD (own)
    usage.py                        # /usage (per-team aggregates)
  campaign_runner.py                # background submitter for campaigns
  pagination.py                     # cursor helpers

web/                                (NEW SPA root)
  package.json
  vite.config.ts
  src/
    main.tsx
    App.tsx
    api/                            # generated client (OpenAPI-typed)
    pages/
      TrialsList.tsx
      TrialDetail.tsx
      TrajectoryViewer.tsx
      AtifViewer.tsx
      CampaignsList.tsx
      CampaignDetail.tsx
      Tasks.tsx
      Benchmarks.tsx
      Settings.tsx                   # tokens, rate cards
      Usage.tsx
    components/
      EventTimeline.tsx              # the trajectory event renderer
      JsonViewer.tsx
      TokenBadge.tsx
      ...
```

`loom_service` is stateless. It reads from Postgres and MinIO, mints presigned URLs for artifacts + trajectories. It does NOT own any new state today — all writes route to existing tables (a campaign is a row in `campaigns` + N rows in `trials`).

Two new ops surfaces:
```
deploy/Dockerfile.service                # python:3.11-slim + loom-service
deploy/Dockerfile.web                    # nginx:alpine serving Vite build
```

A reverse-proxy (nginx in the ingress) routes `/api/v1/*` to `loom-service:8090` and `/*` (everything else) to `loom-web:80`.

## 3. API design principles

- **`/api/v1/` prefix on every route.** Bumping versions is a route prefix, not a header.
- **Cursor pagination, never offset.** Cursor encodes `(submitted_at desc, id desc)`; opaque base64.
- **Presigned URLs for blobs.** The API hands back signed MinIO URLs for trajectory JSONL / ATIF JSON / artifacts. SPA downloads directly from MinIO, not through the API.
- **Tokens carry scope.** Existing scopes (`submit`, `read:own`, `worker:*`, `admin:*`) drive authorization. New scope `service:read` is implicit on every team/admin token (no migration; existing checks already cover this).
- **OpenAPI spec is canonical.** FastAPI generates it; SPA's typed client is generated from it via `openapi-typescript`.

## 4. Authentication

The service reuses `loom.auth.verify_bearer_token` (Plan 4's shared helper). Three principal types accepted:

- **Team tokens** — see only their team's trials, campaigns, usage.
- **Worker tokens** — *not* accepted; the service layer is for humans + admins, not the runtime fabric. The route handler 403s on `type='worker'`.
- **Admin tokens** — see everything; can manage rate cards, mint team tokens, view cross-team usage.

JWT step tokens (from the agent integrations spec) are also rejected — they're for LLM dialect endpoints only.

SPA stores the user's personal token in `localStorage` after first paste. Every request sends `Authorization: Bearer <token>`. On 401 the SPA redirects to the settings page with a prompt to re-paste.

## 5. Routes (concrete)

### 5.1 Trials

```
GET /api/v1/trials
  Query: ?team_id=...&task_id=...&state=running,succeeded
         &campaign_id=...&submitted_after=...&submitted_before=...
         &cursor=...&limit=50
  → 200 {
      items: [{id, task_id, state, team_id, campaign_id,
               submitted_at, started_at, finished_at,
               aggregate_reward, cost_usd, agent_name, model}],
      next_cursor: "..." | null,
    }

GET /api/v1/trials/{trial_id}
  → 200 {
      ...trial fields,
      atif_url: "https://minio/...?presigned",     # 1-hour expiry
      trajectory_url: "https://minio/...?presigned",
      artifacts: [{key, size, download_url}],
      steps: [{step_id, verifier_result, agent_info, error}],
    }

POST /api/v1/trials
  Body: {task_id, config, idempotency_key?}
  → 201 {trial_id}              # same shape as Control Plane's POST /trials
                                # (service forwards to Control Plane)

POST /api/v1/trials/{trial_id}/cancel
  → 200 {state: "cancelled"}    # forwards to Control Plane
```

The service forwards write operations (submit, cancel) to Control Plane's existing endpoints — it doesn't duplicate the logic. Reads come straight from Postgres.

### 5.2 Trajectory + ATIF

```
GET /api/v1/trials/{trial_id}/trajectory
  Query: ?cursor=...&limit=200    # event-level pagination over the JSONL
  → 200 {
      events: [{trial_id, step_id, seq, kind, emitted_at, ...}],
      next_cursor: "..." | null,
    }

GET /api/v1/trials/{trial_id}/trajectory/download
  → 302 → presigned MinIO URL (whole JSONL file)

GET /api/v1/trials/{trial_id}/atif
  → 302 → presigned MinIO URL (ATIF v1.7 doc)
```

The paginated `trajectory` endpoint reads the JSONL from MinIO via the existing `TrajectoryReader` (Plan 2) and streams events as JSON. For large trajectories (>50 MB) the SPA prefers `download` and parses locally.

### 5.3 Campaigns

A new first-class concept. A campaign = a named submission of N trials against a task (or benchmark) with shared trial config.

```sql
CREATE TABLE campaigns (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    team_id      uuid NOT NULL REFERENCES teams(id),
    name         text NOT NULL,
    description  text,
    task_filter  jsonb NOT NULL,              -- {benchmark_id, splits, task_ids, ...}
    trial_config jsonb NOT NULL,              -- shared TrialConfig
    state        text NOT NULL DEFAULT 'submitted',  -- submitted / running / finished / cancelled
    created_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    created_by_token_prefix text NOT NULL     -- 8-char hash prefix of the submitter token
);

ALTER TABLE trials ADD COLUMN campaign_id uuid REFERENCES campaigns(id);
CREATE INDEX trials_campaign_idx ON trials (campaign_id) WHERE campaign_id IS NOT NULL;
```

```
POST /api/v1/campaigns
  Body: {
    name: "swe-bench-verified vs claude-opus-4-7",
    description: "...",
    task_filter: {benchmark_id: "swe-bench-verified", splits: ["test"]},
    trial_config: {retry: {max_attempts: 2}, ...},
  }
  → 201 {campaign_id, expected_trial_count}

GET /api/v1/campaigns
  Query: ?team_id=...&state=running&cursor=...&limit=50
  → 200 {items: [...], next_cursor}

GET /api/v1/campaigns/{id}
  → 200 {
      ...campaign fields,
      trial_summary: {queued: N, claimed: N, running: N, succeeded: N, failed: N, cancelled: N},
      aggregate_reward: float | null,    # mean of finished trials' aggregate
      total_cost_usd: float,
    }

POST /api/v1/campaigns/{id}/cancel
  → 200 {state: "cancelled"}   # cancels all queued/claimed/running trials in the campaign
```

`campaign_runner.py` is a background task in `loom_service`'s lifespan that picks newly-submitted campaigns and submits per-task trials to Control Plane in batches (rate-limited to avoid overwhelming the queue). Submission is idempotent — `campaigns.id` + `task_id` is unique in `trials.idempotency_key`.

### 5.4 Tasks + Benchmarks

```
GET /api/v1/tasks
  Query: ?benchmark_id=...&license=MIT&cursor=...&limit=50
  → 200 {items: [{id, name, benchmark_id, license, checksum, source}], next_cursor}

GET /api/v1/tasks/{task_id}
  → 200 {id, checksum, config, source, license, benchmark_id, ...}

GET /api/v1/benchmarks
  → 200 {items: [{id, display_name, license_spdx, splits, instance_count}], next_cursor}

GET /api/v1/benchmarks/{id}
  → 200 {id, display_name, upstream_*, license_*, splits, instance_count, imported_at, imported_by}
```

The benchmarks routes depend on the benchmark integrations spec landing first; if it doesn't, this section is a no-op (no `benchmarks` table → endpoint returns empty list).

### 5.5 Tokens

```
GET /api/v1/tokens
  → 200 {items: [{token_hash_prefix, type, scopes, team_id, issued_at, expires_at, revoked_at}]}
    # team tokens: see all tokens scoped to the caller's team_id.
    # admin tokens: see all tokens.
    # The presenting bearer's own row is always included.

POST /api/v1/tokens   (admin:tokens scope required in v1)
  Body: {type: "team" | "worker" | "admin", team_id: UUID?, scopes: [...], expires_in_days: 90}
  → 201 {token: "loom_team_...", token_hash_prefix: "...", expires_at: "..."}
    # team_id is required for type="team", forbidden for type="worker"|"admin".
    # v1.5 will add a team-token-managed mint flow (a team token can mint
    # additional same-team tokens with subset scopes) — v1 keeps it admin-only
    # to avoid scope-escalation surface area.

DELETE /api/v1/tokens/{prefix}
  → 204
```

### 5.6 Rate cards (admin only)

```
GET /api/v1/rate-cards
  → 200 {items: [{id, captured_at, hash}]}
    # `hash` is the Plan 4 `compute_rate_card_hash(table)` value already
    # exposed by Gateway responses as `rate_card_hash`. Same field, just
    # surfaced on the listing.

GET /api/v1/rate-cards/{id}
  → 200 {id, captured_at, table: {...}}

POST /api/v1/rate-cards
  Body: {id, table: {entries: [...]}}
  → 201   # forwards to Gateway's existing /admin/rate-cards
```

### 5.7 Teams + Usage

```
GET /api/v1/teams/{team_id}
  → 200 {id, name, fair_share_weight, quota, license_allowlist, members: [{token_prefix, scopes, last_seen_at}]}

GET /api/v1/usage
  Query: ?team_id=...&start=2026-06-01&end=2026-06-30&group_by=day
  → 200 {
      buckets: [{
        start_at, end_at,
        trial_count, succeeded_count, failed_count,
        total_cost_usd, llm_input_tokens, llm_output_tokens,
      }],
    }
```

Usage rolls up from the `llm_calls` table (introduced in the agent integrations spec) joined with `trials`.

## 6. SPA pages

Each page is one or two API calls + a render. React + Vite + TypeScript + TanStack Query.

| Page                  | Route                          | Key API calls                                          |
|-----------------------|--------------------------------|--------------------------------------------------------|
| Trials list           | `/trials`                      | `GET /trials` paginated; filter bar drives query       |
| Trial detail          | `/trials/:id`                  | `GET /trials/:id`; tabs for trajectory / atif / artifacts |
| Trajectory viewer     | inside trial detail            | `GET /trials/:id/trajectory` paginated, rendered as `<EventTimeline>` |
| ATIF viewer           | inside trial detail            | redirect through `GET /trials/:id/atif`, JSON pretty-printed |
| Campaigns list        | `/campaigns`                   | `GET /campaigns`                                       |
| Campaign detail       | `/campaigns/:id`               | `GET /campaigns/:id` + linked trial-list view filtered by campaign_id |
| New campaign          | `/campaigns/new`               | task picker + agent picker + config form + `POST /campaigns` |
| Tasks browser         | `/tasks`                       | `GET /tasks` filterable by benchmark                   |
| Benchmarks            | `/benchmarks`                  | `GET /benchmarks`                                      |
| Settings              | `/settings`                    | `GET /tokens`, `POST /tokens`, `DELETE /tokens/:prefix` |
| Admin rate cards      | `/settings/rate-cards`         | admin-gated; `GET/POST /rate-cards`                    |
| Usage                 | `/usage`                       | `GET /usage` with date-range picker + per-day chart    |

### 6.1 Trajectory viewer details

The highest-value UI surface. Renders the event timeline grouped by step. Per-event renderers:

- `TrialStartEvent / TrialEndEvent` → narrow horizontal divider
- `StepStartEvent / StepEndEvent` → collapsible section header with step_id + reward
- `LLMCallEvent` → assistant icon + model + cost badge; click expands input messages + response + token counts
- `ToolUseEvent` → tool icon + tool name + inputs/output; collapsible
- `AgentThoughtEvent` → italic text block
- `EnvExecEvent` → terminal icon + command + truncated output; click expands full stdout/stderr
- `NetworkPolicyChange` → notice banner
- `WorkerLostClaim` → red warning banner

All event types from Plan 1's catalog render. Unknown event kinds (forward compatibility) render as a generic `<JsonViewer>` block.

## 7. Background work: campaign runner

`loom_service.campaign_runner.run_loop()` is started as an `asyncio.create_task` in the FastAPI lifespan. Its job:

1. `SELECT ... FOR UPDATE SKIP LOCKED` on `campaigns WHERE state IN ('submitted', 'running')` (the SKIP LOCKED is what makes multiple `loom_service` replicas coordinate; the FOR UPDATE was missing in the prose-only earlier description).
2. If state='submitted', resolve `task_filter` to a concrete list of `task_id`s (one DB query). If the resolved list is empty, transition straight to `state='finished'` and `description += " (no matching tasks)"`.
3. Submit trials in batches of 50 to Control Plane's `POST /trials` with `idempotency_key = f"campaign:{campaign_id}:{task_id}"` per trial. Rate-limited at 100/sec by default via `asyncio.Semaphore`.
4. As trials finish, the campaign's `state` rolls forward: any trial in queued/claimed/running ⇒ `running`; all finished ⇒ `finished` (with `finished_at = now()`).
5. Crash recovery: on service restart, in-progress campaigns are picked back up; the idempotency_key guarantees no duplicate submissions even if step 3 was mid-batch when the service crashed.
6. Cancel path: when `POST /campaigns/{id}/cancel` lands, the service writes `campaigns.state = 'cancelled'` then calls Control Plane's `POST /trials/cancel-bulk` with `{campaign_id}` (one HTTP round-trip, atomic SQL on Control Plane's side).

## 8. Authorization rules

| Endpoint                              | Required scope                     | Cross-team allowed? |
|---------------------------------------|-------------------------------------|---------------------|
| `GET /api/v1/trials`                  | `read:own` or `admin:*`             | Only with `admin:*` |
| `GET /api/v1/trials/{id}`             | `read:own` if team_id matches; `admin:*` otherwise | — |
| `POST /api/v1/trials`                 | `submit`                            | No                  |
| `POST /api/v1/trials/{id}/cancel`     | `submit` + same team                | Admin only          |
| `GET /api/v1/campaigns*`              | `read:own`                          | Admin only          |
| `POST /api/v1/campaigns`              | `submit`                            | No                  |
| `GET /api/v1/tasks*`                  | any valid token                     | n/a (tasks aren't team-scoped) |
| `GET /api/v1/benchmarks*`             | any valid token                     | n/a                 |
| `GET /api/v1/tokens`                  | team token sees own-team tokens; admin sees all | Admin only |
| `POST /api/v1/tokens`                 | `admin:tokens` (token minting is admin-only in v1; team-managed tokens are v1.5) | Admin only |
| `DELETE /api/v1/tokens/{prefix}`      | bearer matches `prefix` (revoke own) OR `admin:tokens` | Admin only |
| `GET /api/v1/rate-cards*`             | `admin:rate_cards`                  | n/a                 |
| `POST /api/v1/rate-cards`             | `admin:rate_cards`                  | n/a                 |
| `GET /api/v1/teams/{id}`              | own team or `admin:*`               | Admin only          |
| `GET /api/v1/usage`                   | own team (auto-filtered) or `admin:*` | Admin only        |

New scope `admin:rate_cards` is introduced — currently rate-card management uses `admin:tokens` by accident. Spec straightens this out; v0.7 admins get both scopes implicitly.

## 9. Deployment

### 9.1 docker-compose.test.yml additions

```yaml
  service:
    image: loom-service:dev
    build: { context: .., dockerfile: deploy/Dockerfile.service }
    environment:
      LOOM_SVC_DB_URL: postgresql+psycopg://loom:loom@postgres/loom
      LOOM_SVC_MINIO_ENDPOINT: http://minio:9000
      LOOM_SVC_MINIO_ACCESS_KEY: loomtest
      LOOM_SVC_MINIO_SECRET_KEY: loomtest
      LOOM_SVC_CONTROL_PLANE_URL: http://control-plane:8080
      LOOM_SVC_GATEWAY_URL: http://llm-gateway:9100
    depends_on: { postgres: {condition: service_healthy}, control-plane: {condition: service_started} }
    ports: [ "58090:8090" ]

  web:
    image: loom-web:dev
    build: { context: ../web, dockerfile: ../deploy/Dockerfile.web }
    environment:
      # SPA uses a relative API base so the browser hits the same origin
      # nginx is serving from; nginx proxies /api/v1/* to loom-service.
      # (Hard-coding `http://localhost:58090/...` would have broken any
      # access through the k8s ingress where the browser isn't at
      # localhost.)
      VITE_API_BASE: /api/v1
    depends_on: { service: {condition: service_started} }
    ports: [ "53000:80" ]
```

### 9.2 k8s manifests

`deploy/k8s/service.yaml` and `deploy/k8s/web.yaml` follow Plan 7's pattern. Ingress rules add:

```yaml
- host: loom.example.com
  http:
    paths:
      - path: /api/v1/         → loom-service:8090
      - path: /                → loom-web:80
```

### 9.3 OpenAPI client generation

`web/package.json` adds:
```
"scripts": {
  "gen-api": "openapi-typescript http://localhost:58090/openapi.json -o src/api/schema.d.ts"
}
```

Run during `npm run build`. The SPA's `api/` module wraps `fetch` with typed responses.

## 10. Testing strategy

- **Unit tests** (loom_service): pagination cursor encoding, authorization-rule decisions, campaign-runner state transitions. ~80 LOC of tests per route family.
- **Integration tests** (loom_service): one per route, against the same testcontainers Postgres + MinIO fixture Plan 5 uses. Asserts auth scope enforcement + payload shape against the OpenAPI schema.
- **SPA tests**: React Testing Library for each page (3-4 cases per page: happy path, error 401, empty state). Vitest + happy-dom. No e2e Playwright in v1 — too brittle for the volume of UI surface.
- **System smoke**: extend `tests/system/` with `test_full_stack_service_layer.py` — bring up compose stack including `service` + `web`, exercise the trial-submit → trial-list → trial-detail flow over real HTTP.

## 11. Migration + backwards compatibility

- All new tables (`campaigns`) and new columns (`trials.campaign_id`, `tokens.last_seen_at`) are additive. No data migration of existing rows.
- Control Plane's existing `POST /trials` gets one additive change: accept an optional `idempotency_key: str` field in the body; if present, the INSERT becomes `ON CONFLICT (idempotency_key) DO NOTHING` and the response carries `{trial_id, idempotent_hit: bool}`. campaign_runner uses `idempotency_key = f"campaign:{campaign_id}:{task_id}"` to prevent duplicate submissions on retries. (Spec'd here; Plan 19 amends Plan 5's `/trials` route.)
- Control Plane also gets one new endpoint: `POST /trials/cancel-bulk` taking `{campaign_id}` and atomically transitioning every queued/claimed/running trial in that campaign to `cancelled`. (~one new SQL statement; same pattern as v0.7's single-trial cancel route.) campaign_runner calls this on campaign cancel instead of N per-trial calls.
- Worker tokens cannot use the service layer (by design — see §4); no migration needed.
- The new `admin:rate_cards` scope is added to existing admin tokens via an Alembic migration (`UPDATE tokens SET scopes = array_append(scopes, 'admin:rate_cards') WHERE 'admin:tokens' = ANY(scopes)`).
- `tokens.last_seen_at TIMESTAMP WITH TIME ZONE` is added; the service layer + Control Plane update it on each `verify_bearer_token` success (debounced to once per 60s per token to avoid write amplification).

## 12. Out of scope

- **SSO / OAuth.** Personal bearer tokens only. v1.5 work if non-engineers need to use the UI.
- **RBAC beyond Loom's scope model.** No granular per-resource permissions. v1.5.
- **Write-heavy admin UX.** Rate-card upload is the only admin write in the UI; everything else stays curl-driven.
- **Real-time updates.** SPA polls (TanStack Query with 5s `refetchInterval` for in-progress views). No WebSockets in v1.
- **Multi-tenant SaaS hardening.** Loom assumes internal-trust model; v1.5 adds quotas + rate limiting + abuse detection if it goes external.
- **Billing.** Cost is surfaced (read), not invoiced. Billing integration is its own product.

## 13. Open questions

None at spec write-time.

## 14. Implementation sequencing

Six plans:

1. **Plan 17 — Service skeleton.** `loom_service` package, FastAPI factory, settings, `loom.auth` reuse, health + tokens routes (the smallest read+write workflow). Includes the new `admin:rate_cards` scope migration. ~3 days. No SPA yet.
2. **Plan 18 — Read routes.** Trials list/detail, trajectory paginated read, ATIF presigned, tasks + benchmarks browse. ~4 days. Stand-alone API users (curl, Python client) get value here.
3. **Plan 19 — Campaigns.** New table + Alembic migration, POST/GET/cancel routes, `campaign_runner` background task. ~4 days.
4. **Plan 20 — Rate-card + team + usage endpoints.** `/rate-cards`, `/teams/{id}`, `/usage` (with `llm_calls` rollup if agent integrations have shipped; else degraded). ~3 days.
5. **Plan 21 — SPA scaffold + read pages.** Vite + React + TanStack Query, trials list, trial detail (with trajectory viewer), tasks + benchmarks pages, settings (tokens), token-paste login flow. ~5 days.
6. **Plan 22 — SPA write pages + admin.** New campaign form, rate-card upload, usage dashboard. ~4 days.

Total: ~23 working days. Plans 17–20 ship the API as a usable surface even without the SPA; 21–22 bolt on the UI. Plan 19 (campaigns) is independently valuable for power users.

After all six ship, Loom has a real product surface that researchers can self-serve against.
