# PgBouncer transaction-mode pooling

Cluster renders enable PgBouncer by default. It multiplexes short application
transactions onto a bounded set of Postgres connections while operations that
require session state keep a direct database connection.

## Rendered resources

When `pgbouncer.enabled=true`, `loom cluster render` includes:

- a `loom-pgbouncer` Deployment (two replicas by default);
- a ClusterIP Service on port `6432`;
- a PodDisruptionBudget;
- a NetworkPolicy; and
- a Prometheus exporter sidecar and PgBouncer alert rules.

The schema defaults are `default_pool_size=25`, `min_pool_size=5`, and
`max_client_conn=500`. Postgres defaults to `max_connections=150`; operators
must preserve room for every PgBouncer replica, direct listeners, migrations,
and administrative connections when tuning either side.

## Database URLs

Control Plane, LLM Gateway, and Service settings expose two URLs:

- `db_url` points directly to `loom-postgres:5432`;
- `db_url_pool` points to `loom-pgbouncer:6432`.

Normal SQLAlchemy work uses `db_url_pool` when present and disables prepared
statement caching with `prepare_threshold=None`. If no pool URL is configured,
the services fall back to `db_url`.

`loom cluster bootstrap-secrets` derives each `*-db-url-pool` value from its
direct sibling when PgBouncer is enabled. Operators edit only the direct DSNs.
`loom cluster derive-pool-dsn` performs the same host/port rewrite explicitly.

## Direct-connection operations

Alembic and `LISTEN` watchers use `db_url`, not the transaction pool. PgBouncer
does not preserve session state between transactions, so routing either through
the pool can produce missed notifications or unsafe migration behavior. Startup
probes and `loom cluster doctor` reject direct URLs that target PgBouncer and
pool URLs that do not target it.

## Diagnostics

Use the exporter metrics and the PgBouncer admin database to inspect saturation:

```bash
kubectl exec deploy/loom-pgbouncer -c pgbouncer -- \
  psql -h localhost -p 6432 -U loom pgbouncer -c 'SHOW POOLS'
```

`LoomPgBouncerClientsWaiting` indicates clients waiting for a backend;
`LoomPgBouncerDown` indicates that the exporter cannot reach the pool. A
`LoomListenNotifySelfTestFailure` usually means a watcher is using the pooled
URL instead of the direct URL.

## Disable and fallback

Set `pgbouncer.enabled=false`, re-render, and apply the manifests. PgBouncer
resources and pool secrets are omitted; services use their direct `db_url`.
Disabling the pool does not require a data migration or credential rotation.
