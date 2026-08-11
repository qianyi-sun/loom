# pgbouncer transaction-mode connection multiplexing

> Archived implementation design. Current behavior is documented in
> `docs/architecture/pgbouncer.md`.

**Status: design**
**Issue:** #609
**Related:** #547 (closed umbrella), #610 (MinIO distributed follow-up)

## Motivation

Loom's prod topology is elastic — worker count autoscales up to whatever Slurm's fair-share quota allows. Without a connection multiplexer between the app tier and Postgres, worker autoscaling is silently capped by Postgres's `max_connections` (default 100), not by Slurm quota or hardware. 150 workers × 5 connections each = 750 connection demand, and Postgres refuses beyond 100.

pgbouncer in transaction mode sits between Loom services (control-plane, gateway, service) and Postgres. It accepts hundreds of app-side connections and multiplexes them onto a small pool of real Postgres backends. The prerequisite audit for Loom's SQLAlchemy usage is in [#609 comment](https://github.com/qianyi-sun/loom/issues/609#issuecomment-4906937108); transaction mode is safe for the main pool with `prepare_threshold=None`, and LISTEN watchers bypass pgbouncer via direct connection.

## Non-goals

- Postgres HA / SPOF removal (deferred to k3s migration; see #609 Options B/C).
- MinIO distributed mode (#610).
- Client-side SCRAM auth (defer; plaintext client auth is acceptable inside NetworkPolicy-locked cluster network).
- CLI command for on-demand pool inspection (`SHOW POOLS` via kubectl exec is sufficient for now).

## Architecture

Two independent paths to Postgres:

```
┌────────────────┐  ┌────────────────┐  ┌────────────────┐
│ loom-control-  │  │  loom-service  │  │ loom-llm-      │
│    plane       │  │                │  │   gateway      │
│  (SQLAlchemy)  │  │  (SQLAlchemy)  │  │  (SQLAlchemy)  │
└────────┬───────┘  └────────┬───────┘  └────────┬───────┘
         │                   │                   │
         │  loom-pgbouncer:6432 (transaction)    │
         └─────────────────┬─┴───────────────────┘
                           ▼
              ┌──────────────────────────┐
              │      loom-pgbouncer      │
              │  Deployment × 2, PDB     │
              │  transaction pool mode   │
              │  default_pool_size=25    │
              └─────────────┬────────────┘
                            ▼
              ┌──────────────────────────┐
              │       loom-postgres      │
              │  (StatefulSet, unchanged)│
              │  max_connections=150     │
              └──────────────▲───────────┘
                             │
                             │ loom-postgres:5432 (direct)
   ┌─────────────────────────┼───────────────────────────┐
   │                         │                           │
┌──┴──────────────┐  ┌───────┴───────────┐  ┌────────────┴──────┐
│ egress-xds      │  │ loom-service      │  │ Alembic migration │
│ provider watch  │  │ trajectory SSE    │  │  Job              │
│ (psycopg LISTEN)│  │ (psycopg LISTEN)  │  │ (LOOM_DB_URL)     │
└─────────────────┘  └───────────────────┘  └───────────────────┘
```

- **Main SQLAlchemy pool** → pgbouncer → Postgres.
- **LISTEN watchers + Alembic** → Postgres direct (bypass).

pgbouncer runs as a Deployment with 2 replicas behind Service `loom-pgbouncer:6432`. k8s Service round-robins client connections across pods. PodDisruptionBudget (`minAvailable: 1`) prevents correlated pod loss during voluntary disruptions (node drain, cluster upgrade). Rolling update strategy (`maxUnavailable: 0, maxSurge: 1`) ensures a new pod is ready before an old pod terminates.

## Invariant that does most of the work

**`db_url` remains defined as "the direct Postgres DSN"**, unchanged from today's meaning. `db_url_pool` is new — an additive DSN that, when set, points at pgbouncer. This preserves:

- LISTEN watchers (already read `db_url`) → direct connection, no code change.
- Alembic migrations (already read `LOOM_DB_URL` from `cp-db-url` secret) → direct connection, no template change.
- Admin ops that need session semantics → keep using `db_url`.

Only SQLAlchemy engine construction sites gain a small change: prefer `db_url_pool` when set, else `db_url`.

## Config surface

Four schema additions to `config/loom-schema.toml`, reusing existing patterns exactly.

### 1. New `service_config.db_url_pool`

```toml
[service_config.db_url_pool]
used_by     = ["control-plane", "llm-gateway", "loom-service"]
python_type = "PostgresDsn"
required    = false
default     = ""
secret      = { key_per_service = {
                  control-plane = "cp-db-url-pool",
                  llm-gateway   = "gw-db-url-pool",
                  loom-service  = "svc-db-url-pool",
              } }
description = "PgBouncer pool DSN (transaction-mode multiplexer). When non-empty, SQLAlchemy engines use this instead of db_url. Empty = direct-to-Postgres, no pooler. LISTEN/NOTIFY watchers and Alembic migrations always use db_url regardless."
```

### 2. New `render_config.pgbouncer`

```toml
[render_config.pgbouncer]
python_type = "table"
description = "PgBouncer connection multiplexer between Loom services and Postgres. When enabled, renders Deployment + Service + PDB + NetworkPolicy and bootstrap derives *-db-url-pool secrets. Rollback: enabled=false, re-render, redeploy — services fall back to db_url (direct)."
fields = {
    enabled           = true,
    default_pool_size = 25,
    max_client_conn   = 500,
    min_pool_size     = 5,
}
```

`reserve_pool_size`, `reserve_pool_timeout_sec`, `server_idle_timeout_sec`, `server_lifetime_sec` are hardcoded in the template with sensible defaults (5, 3, 600, 3600 respectively). Add schema fields if metrics justify tuning.

### 3. New `render_config.postgres`

```toml
[render_config.postgres]
python_type = "table"
description = "Postgres sizing knobs. max_connections bumped from Postgres default 100 to leave room for pgbouncer backend pool (default_pool_size × pgbouncer.replicas) + LISTEN watchers + Alembic + admin slack."
fields = { max_connections = 150 }
```

Wired into `postgres.yaml.j2` as a container arg: `-c max_connections={{ postgres.max_connections }}`.

### 4. Extended `render_config.replicas`

```toml
[render_config.replicas]
fields = { service = 2, control_plane = 2, gateway = 2, web = 0, worker = 3,
           egress_xds = 0, egress_proxy = 0,
           pgbouncer = 2 }  # NEW
```

### Profile overrides

- `deploy/environments/development.cluster.toml`: `pgbouncer.enabled = false` (kind dev doesn't need it; keeps kind boot simple).
- `deploy/environments/staging.cluster.toml`: `pgbouncer.enabled = true` (schema default).
- `deploy/environments/production.cluster.toml`: `pgbouncer.enabled = true`.

## Bootstrap: derive, don't duplicate

Two DSN secrets per service creates fat-finger risk. Resolution: operators only ever set the direct URL. Bootstrap derives the pool URL by parsing and rewriting host+port.

New helper in `src/loom_config/bootstrap.py`:

```python
def _rewrite_dsn_host_port(dsn: str, *, host: str, port: int) -> str:
    """Rewrite the host+port in a psycopg/SQLAlchemy DSN, preserving
    credentials, database name, and query parameters."""
    parsed = urlsplit(dsn)
    userinfo = f"{parsed.username}:{parsed.password}"
    return urlunsplit((
        parsed.scheme,
        f"{userinfo}@{host}:{port}",
        parsed.path,
        parsed.query,
        parsed.fragment,
    ))
```

Bootstrap output shape changes (when `pgbouncer.enabled=true`) from a single kubectl command to a shell script with variables:

```bash
# Smoke defaults: filled in mechanically.
CP_DB_URL="postgresql+psycopg://loom:loom@loom-postgres:5432/loom"
GW_DB_URL="$CP_DB_URL"   # same credentials, same host
SVC_DB_URL="$CP_DB_URL"

# Real deploys: operator edits the *_DB_URL lines above; pool URLs
# are derived automatically.
CP_DB_URL_POOL="$(loom cluster derive-pool-dsn "$CP_DB_URL")"
GW_DB_URL_POOL="$(loom cluster derive-pool-dsn "$GW_DB_URL")"
SVC_DB_URL_POOL="$(loom cluster derive-pool-dsn "$SVC_DB_URL")"

kubectl create secret generic loom-secrets \
  --namespace=loom \
  --from-literal=cp-db-url="$CP_DB_URL" \
  --from-literal=cp-db-url-pool="$CP_DB_URL_POOL" \
  --from-literal=gw-db-url="$GW_DB_URL" \
  --from-literal=gw-db-url-pool="$GW_DB_URL_POOL" \
  --from-literal=svc-db-url="$SVC_DB_URL" \
  --from-literal=svc-db-url-pool="$SVC_DB_URL_POOL" \
  ...
```

New CLI: `loom cluster derive-pool-dsn <direct-dsn>` prints the pool DSN (host `loom-pgbouncer`, port `6432`). Standalone command so operators can inspect the derivation.

When `pgbouncer.enabled=false`, bootstrap output keeps its current single-kubectl-command shape and omits `*-db-url-pool` entries entirely. Operators who never enable pgbouncer see no change.

## Runtime: `db_engine_url` computed field

Each service's Settings class gains two computed_fields (hand-written, subclassing the codegen-produced generated class). Pattern already used in Loom's config structure.

```python
# src/loom_control_plane/config/__init__.py (hand-written)

from pydantic import computed_field
from loom_control_plane.config._generated import _GeneratedControlPlaneSettings


class ControlPlaneSettings(_GeneratedControlPlaneSettings):
    @computed_field
    @property
    def db_engine_url(self) -> str:
        """DSN for SQLAlchemy engine construction.

        Returns db_url_pool when set (pgbouncer path), else db_url
        (direct). Callers constructing SQLAlchemy engines MUST use this,
        never db_url directly — db_url is reserved for LISTEN watchers
        and Alembic which need direct-to-Postgres semantics.
        """
        if self.db_url_pool:
            return str(self.db_url_pool)
        return str(self.db_url)

    @computed_field
    @property
    def db_engine_connect_args(self) -> dict[str, object]:
        """psycopg3 connect_args paired with db_engine_url.

        prepare_threshold=None when routed through pgbouncer (transaction
        mode is incompatible with server-side prepared statements).
        Empty dict on the direct path.
        """
        if self.db_url_pool:
            return {"prepare_threshold": None}
        return {}
```

Same shape added to `LoomServiceSettings` and `LLMGatewaySettings`.

Engine construction sites (`loom_control_plane/app.py:64`, `loom_service/app.py:101`, gateway equivalent) become:

```python
engine = create_async_engine(
    settings.db_engine_url,
    connect_args=settings.db_engine_connect_args,
    pool_pre_ping=True,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
    pool_timeout=settings.db_pool_timeout_sec,
)
```

LISTEN watchers unchanged — they still call `psycopg.AsyncConnection.connect(str(settings.db_url), ...)`. Migration job unchanged — it still reads `LOOM_DB_URL` from `cp-db-url`.

## k8s template: `pgbouncer.yaml.j2`

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: loom-pgbouncer
spec:
  replicas: {{ replicas.pgbouncer }}
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 0
      maxSurge: 1
  selector:
    matchLabels: { app: loom-pgbouncer }
  template:
    metadata:
      labels: { app: loom-pgbouncer }
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9127"
    spec:
      containers:
        - name: pgbouncer
          image: bitnami/pgbouncer:1.24.0
          ports: [ { containerPort: 6432 } ]
          env:
            - name: POSTGRESQL_USERNAME
              valueFrom: { secretKeyRef: { name: loom-secrets, key: postgres-user } }
            - name: POSTGRESQL_PASSWORD
              valueFrom: { secretKeyRef: { name: loom-secrets, key: postgres-password } }
            - name: POSTGRESQL_HOST
              value: loom-postgres
            - name: PGBOUNCER_PORT
              value: "6432"
            - name: PGBOUNCER_POOL_MODE
              value: transaction
            - name: PGBOUNCER_DEFAULT_POOL_SIZE
              value: "{{ pgbouncer.default_pool_size }}"
            - name: PGBOUNCER_MIN_POOL_SIZE
              value: "{{ pgbouncer.min_pool_size }}"
            - name: PGBOUNCER_MAX_CLIENT_CONN
              value: "{{ pgbouncer.max_client_conn }}"
            - name: PGBOUNCER_AUTH_TYPE
              value: plain
          readinessProbe:
            tcpSocket: { port: 6432 }
            periodSeconds: 5
          resources:
            requests: { cpu: 100m, memory: 128Mi }
            limits: { cpu: 500m, memory: 512Mi }
        - name: exporter
          image: prometheuscommunity/pgbouncer-exporter:0.7.0
          args: ["--pgBouncer.connectionString=postgres://loom@localhost:6432/pgbouncer?sslmode=disable"]
          ports: [ { containerPort: 9127, name: metrics } ]
          resources:
            requests: { cpu: 20m, memory: 32Mi }
            limits: { cpu: 100m, memory: 128Mi }
---
apiVersion: v1
kind: Service
metadata:
  name: loom-pgbouncer
spec:
  selector: { app: loom-pgbouncer }
  ports:
    - name: sql
      port: 6432
      targetPort: 6432
    - name: metrics
      port: 9127
      targetPort: 9127
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: loom-pgbouncer
spec:
  minAvailable: 1
  selector:
    matchLabels: { app: loom-pgbouncer }
```

Wired into `_TEMPLATE_ORDER` between `postgres.yaml.j2` and `control-plane.yaml.j2`.

### NetworkPolicy additions

Append to `network-policies.yaml.j2`:

```yaml
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: loom-pgbouncer
spec:
  podSelector:
    matchLabels: { app: loom-pgbouncer }
  policyTypes: [Ingress, Egress]
  ingress:
    - from:
        - podSelector: { matchLabels: { app: loom-control-plane } }
        - podSelector: { matchLabels: { app: loom-llm-gateway } }
        - podSelector: { matchLabels: { app: loom-service } }
      ports:
        - { port: 6432, protocol: TCP }
    # Prometheus scrape.
    - from:
        - namespaceSelector: {}
      ports:
        - { port: 9127, protocol: TCP }
  egress:
    - to:
        - podSelector: { matchLabels: { app: loom-postgres } }
      ports:
        - { port: 5432, protocol: TCP }
    - to:
        - namespaceSelector: { matchLabels: { kubernetes.io/metadata.name: kube-system } }
          podSelector: { matchLabels: { k8s-app: kube-dns } }
      ports:
        - { port: 53, protocol: UDP }
        - { port: 53, protocol: TCP }
```

`loom-postgres` NetworkPolicy is extended to accept ingress from `loom-pgbouncer` (added to the existing `from:` list alongside control-plane, gateway, service, migration).

## Runtime safety nets

Two lightweight boot-time probes enforce the design invariants at exactly the points where violating them silently would hurt most.

### LISTEN watcher NOTIFY self-test

Both watchers (`src/loom_egress_xds/provider_connections_watcher.py` and `src/loom_service/routes/trajectory.py`) grow a startup probe: after acquiring the LISTEN subscription, issue a `NOTIFY loom_watcher_selftest '<random-uuid>'` and wait up to 1s for it to round-trip.

- **Pass** (notification observed on the LISTEN connection): watcher enters push mode.
- **Fail** (timeout): log `ERROR` with the invariant explanation, fall back to poll-only mode permanently, and expose a Prometheus gauge `loom_listen_watcher_push_mode{watcher=...} = 0`. Operators alert on the gauge; existing poll fallback preserves correctness.

Why the probe exists even though the config invariant + doctor check nominally prevent misconfiguration: the poll fallback masks the failure as a silent performance regression rather than a loud error. Users experience notifications lagging by the poll interval (~5-10s) instead of <100ms, and no one files a bug for "feels slightly laggy." The probe converts a debug-in-production regression into a boot-time alert.

Implementation: ~30 lines per watcher, factored into a shared `loom_listen.self_test.notify_round_trip(conn, timeout_sec)` helper for testability.

### Alembic direct-connection probe

`migrations/env.py` gains a probe at the top of `run_migrations_online`:

```python
def _assert_direct_postgres_connection(conn) -> None:
    """Alembic MUST run direct-to-Postgres. Under pgbouncer transaction
    mode, several patterns silently break: SET LOCAL vanishes after
    commit, session-scoped advisory locks release early, autocommit=on
    with DDL routes to changing backends. Alembic works with the current
    Loom migrations on either path today, but the invariant is worth
    enforcing at every migration run so future migration authors can't
    accidentally depend on session semantics that only work on one path.

    Probe: set a synthetic application_name, commit, read it back. Under
    session-preserving semantics the value persists; under transaction-
    mode pgbouncer the next statement lands on a different backend with
    the default application_name.
    """
    marker = f"alembic-probe-{uuid4()}"
    conn.exec_driver_sql(f"SET application_name = '{marker}'")
    conn.commit()
    actual = conn.exec_driver_sql("SHOW application_name").scalar()
    if actual != marker:
        raise RuntimeError(
            f"Alembic connection is not direct-to-Postgres. "
            f"application_name did not persist across commit "
            f"(saw {actual!r}, expected {marker!r}). This means the "
            f"connection routes through pgbouncer transaction mode, "
            f"which silently breaks session-scoped operations that "
            f"Alembic migrations may depend on. "
            f"Fix: point LOOM_DB_URL at loom-postgres:5432 direct, "
            f"not loom-pgbouncer:6432."
        )
```

Runs once at migration Job start, before any migration DDL. Fails the Job loudly with actionable fix instructions.

Why the probe exists even though `cp-db-url` is invariant-controlled: migration Jobs run rarely and their failures are operationally expensive. A cheap boot-time check that says "you're not in the shape you should be in" catches misconfigurations six months from now that a code-review pass might miss.

## Doctor check

`loom cluster doctor` gains a check:

```python
def _check_pgbouncer_invariants(state) -> list[DoctorFinding]:
    findings = []
    if not state.render_config.pgbouncer.enabled:
        return findings
    for svc, direct_key, pool_key in [
        ("control-plane", "cp-db-url",  "cp-db-url-pool"),
        ("llm-gateway",   "gw-db-url",  "gw-db-url-pool"),
        ("loom-service",  "svc-db-url", "svc-db-url-pool"),
    ]:
        direct = _dsn_host(state.secrets.get(direct_key, ""))
        pool = _dsn_host(state.secrets.get(pool_key, ""))
        if direct and direct != "loom-postgres":
            findings.append(finding(
                f"{direct_key} host is {direct!r}, expected 'loom-postgres'. "
                f"LISTEN watchers and Alembic depend on direct-to-Postgres."))
        if pool and pool != "loom-pgbouncer":
            findings.append(finding(
                f"{pool_key} host is {pool!r}, expected 'loom-pgbouncer'. "
                f"pgbouncer.enabled=true but service would bypass the pool."))
    return findings
```

Runs on every `loom cluster doctor` invocation. Included in cluster-smoke's post-render assertions.

## Failure modes

| # | Failure | Blast radius | Response |
|---|---|---|---|
| F1 | Both pgbouncer pods dead | All SQLAlchemy queries fail | PDB `minAvailable=1` + rolling `maxUnavailable=0` prevent voluntary correlated loss. Involuntary case: services return 5xx until pod restart. LISTEN + Alembic unaffected. |
| F2 | Single pgbouncer pod dies | Half of throughput temporarily | k8s Service excludes dead pod after readiness fails. SQLAlchemy `pool_pre_ping` reconnects to surviving pod. |
| F3 | Postgres dies | All queries fail | Unchanged from today. Fixed post-k3s migration (#609). |
| F4 | Backend pool exhaustion | Clients queue then error | Prometheus alert on `pgbouncer_pool_client_waiting > 0 sustained 60s`. Bump `default_pool_size`. |
| F5 | Alembic routed through pgbouncer | Would work for today's migrations but silently break session-dependent patterns (SET LOCAL, autocommit-DDL, session advisory locks) in future migrations | Alembic direct-connection probe fires at Job boot with actionable fix instructions. Doctor check backstops at deploy time. |
| F6 | LISTEN watcher routed through pgbouncer | Silent NOTIFY drop; watcher falls back to polling; user-visible as unexplained UI lag | Watcher NOTIFY self-test at boot detects and flips watcher into permanent poll-mode with a Prometheus alert. Doctor check backstops at deploy time. |
| F7 | Bad DSN in secret | Service fails at boot | Kubelet restart loop; operator sees failure quickly. Doctor check catches invariant violations. |
| F8 | Password rotation | Both direct + pool DSN secrets need updating | Bootstrap derives pool from direct; both regenerate together. |
| F9 | Prometheus exporter dies | No metrics | Prometheus scrape-failure alert. Sidecar restart handled by kubelet. |

## Testing strategy

### Unit tests (`tests/unit/`)

- `test_dsn_rewrite.py` — `_rewrite_dsn_host_port`: standard, no-port input, query preservation, path preservation, malformed input raises.
- `test_settings_db_engine_url.py` — computed_fields on ControlPlaneSettings, LoomServiceSettings, LLMGatewaySettings: `db_url_pool=""` → direct; `db_url_pool` set → pool URL + `prepare_threshold=None`.
- `test_doctor_pgbouncer_invariants.py` — happy path, direct-URL misdirection, pool-URL misdirection, pgbouncer.enabled=false skips checks.
- `test_listen_watcher_selftest.py` — fake psycopg connection: NOTIFY round-trips within timeout → push mode; no notification within timeout → poll mode + gauge=0.
- `test_alembic_direct_probe.py` — fake engine: application_name persists across commit → passes; simulated pgbouncer-mode reset → raises with the fix message.

### Integration tests (`tests/integration/`)

Uses `testcontainers` for real Postgres + real pgbouncer.

- `test_pgbouncer_end_to_end.py`:
  - Bring up Postgres + pgbouncer configured in transaction mode
  - SQLAlchemy engine with `db_engine_url` (pool) + `prepare_threshold=None` executes 100-iteration workload
  - LISTEN watcher on direct URL receives NOTIFY issued from a SQLAlchemy session (via pgbouncer)
  - Chaos: `docker kill` pgbouncer container, wait for restart, verify next query succeeds via `pool_pre_ping`
- `test_bootstrap_pool_derivation.py`:
  - Run `render_bootstrap_command` with `pgbouncer.enabled=true` in smoke mode
  - Assert both direct and pool DSN keys present
  - Assert pool URL mechanically derived from direct URL (same credentials, host `loom-pgbouncer`, port `6432`)
- `test_alembic_probe_integration.py`:
  - Point Alembic at pgbouncer transaction-mode DSN → probe raises with the fix message; migration Job would fail cleanly
  - Point Alembic at direct DSN → probe passes; migrations complete
- `test_listen_selftest_integration.py`:
  - Watcher connected via direct DSN → self-test round-trips within timeout; enters push mode
  - Watcher connected via pgbouncer transaction-mode DSN → self-test times out; watcher falls back to poll mode; Prometheus gauge=0

### Cluster smoke

New variant in `tests/cluster/`:

- `test_cluster_smoke_pgbouncer.py`:
  - Render with `pgbouncer.enabled=true` (staging-like profile)
  - `kind create cluster` + `kubectl apply`
  - Wait for pgbouncer Deployment ready + control-plane, gateway, service ready (readiness probes hit DB)
  - Run one trial end-to-end (existing smoke shape)
  - Verify a trajectory event streams via SSE (exercises direct-to-Postgres LISTEN)
  - Verify `alembic_version` row present (Alembic ran)
  - Verify `kubectl exec loom-pgbouncer -- psql -c "SHOW POOLS"` returns non-empty rows

Runs behind `ci:cluster-smoke` label per Loom convention.

## Rollout

1. PR merges to `dev` with `pgbouncer.enabled=true` as schema default. Development profile pins to `false`.
2. CI cluster-smoke exercises `pgbouncer.enabled=true` in a staging-like render (behind `ci:cluster-smoke` label on this PR).
3. First deploy to staging: `loom cluster render` picks up schema default; new bootstrap output shape appears; operator inspects and applies.
4. Monitor Prometheus dashboard for `pgbouncer_pool_client_waiting`, `pgbouncer_pool_server_active`, `pgbouncer_pool_server_idle` over first week of real trial load. Tune `default_pool_size` upward if `client_waiting > 0` sustained.

## Rollback

Fast rollback (config-only, no data migration):

1. Set `pgbouncer.enabled = false` in the target profile.
2. `loom cluster render` — omits pgbouncer Deployment/Service/PDB, and bootstrap emits no `*-db-url-pool` secrets.
3. `kubectl apply` — services observe `db_url_pool=""` on next Pod restart; `db_engine_url` falls back to `db_url`; SQLAlchemy pools reconnect direct-to-Postgres.
4. Delete `loom-pgbouncer` Deployment + Service + PDB (optional; leaving them running is harmless if unreferenced).

No secret rotation. No data migration. No schema change to reverse.

## Estimated implementation footprint

- Config schema: 4 additions to `config/loom-schema.toml` (~40 lines).
- Bootstrap: `_rewrite_dsn_host_port` helper + shell-script output shape change (~50 lines + tests).
- Settings: `db_engine_url` + `db_engine_connect_args` on 3 services (or one shared mixin) (~30 lines).
- Engine construction call sites: 3 one-line changes.
- k8s templates: new `pgbouncer.yaml.j2` (~90 lines), NetworkPolicy extension (~30 lines).
- Postgres template: `max_connections` container arg (~2 lines).
- Doctor check: `_check_pgbouncer_invariants` (~30 lines + tests).
- Development profile override: `pgbouncer.enabled = false` (~2 lines).
- CLI: `loom cluster derive-pool-dsn` command (~20 lines + tests).
- LISTEN watcher NOTIFY self-test: shared `loom_listen.self_test` helper + wiring into 2 watcher sites (~60 lines + tests).
- Alembic direct-connection probe: `_assert_direct_postgres_connection` in `migrations/env.py` (~20 lines + tests).
- Tests: unit + integration + cluster smoke as enumerated.

Total: ~600-800 lines including tests. Reviewable in one sitting.

## Deferred (add if metrics justify)

- Additional pgbouncer knobs in schema: `reserve_pool_size`, `reserve_pool_timeout_sec`, `server_idle_timeout_sec`, `server_lifetime_sec`. Hardcoded to defaults for now; promote to schema if we need to tune based on Prometheus data.
- `pgbouncer_image` render_config field. Hardcoded to `bitnami/pgbouncer:1.24.0`; promote when we need to pin a specific version for CVE reasons.
- SCRAM client-side auth. Standard practice inside NetworkPolicy-locked cluster networks is plain; upgrade path is a 5-line pgbouncer config change plus adding SCRAM verifier computation to bootstrap.
