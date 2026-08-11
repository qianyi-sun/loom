# Storage Retention

Loom can apply server-side lifecycle expiration to S3-compatible object stores
with `loom cluster bootstrap-storage-lifecycle`. The command reads a strict
TOML policy, renders deterministic bucket rules, and applies them through the
configured S3 client.

## Supported live path

The live apply path supports `minio`, `s3`, `r2`, `b2`, and `wasabi`. The
policy backend must match `LOOM_SVC_STORAGE_BACKEND`. Endpoint, region, and
credentials come from the service storage settings or the documented MinIO
root-credential fallback.

The parser also accepts `gcs`, and the Python module contains a pure GCS JSON
renderer. The CLI does not dispatch to that renderer or apply GCS lifecycle
configuration; `bootstrap-storage-lifecycle` is currently an S3-compatible
operation.

## Policy file

Start from `config/storage-lifecycle.example.toml`:

```toml
backend = "minio"

[[retention]]
bucket = "trajectories"
strategy = "expire_after_days"
days = 30

[[retention]]
bucket = "artifacts"
strategy = "expire_after_days"
days = 180

[[retention]]
bucket = "atif"
strategy = "keep_forever"
```

Current strategies are:

| Strategy | S3-compatible effect |
| --- | --- |
| `expire_after_days` | Emits one enabled expiration rule for the bucket |
| `keep_forever` | Emits no rule and leaves existing objects unexpired |
| `cleanup_incomplete_uploads_after_hours` | Parses but is not emitted by the S3-compatible renderer |

The multipart-cleanup strategy is omitted because MinIO drops that action on
round-trip, which would create permanent drift. Do not rely on it to clean
incomplete uploads.

Multiple different strategies may target a bucket, but duplicate instances of
the same strategy are rejected. Expiration actions for one bucket are merged
into a stable `loom-<bucket>` rule. Reapplying the same policy is idempotent.
Buckets whose rendered policy is empty are skipped; Loom does not delete
unrelated lifecycle rules from them.

## Apply and verify

Inspect the S3-compatible render:

```bash
uv run --no-sync loom cluster bootstrap-storage-lifecycle \
  --config /secure/storage-lifecycle.toml \
  --dry-run
```

Apply it:

```bash
uv run --no-sync loom cluster bootstrap-storage-lifecycle \
  --config /secure/storage-lifecycle.toml \
  --endpoint http://127.0.0.1:9000
```

Check live rules against the same policy:

```bash
uv run --no-sync loom cluster doctor \
  --storage-lifecycle-config /secure/storage-lifecycle.toml \
  --storage-lifecycle-endpoint http://127.0.0.1:9000
```

Storage lifecycle expiration is separate from Loom's typed
[staging data lifecycle](staging-data-lifecycle.md), which makes
application-state-aware, exact-object deletion decisions. See the
[operator runbook](../runbooks/operator-runbook.md#storage-retention)
for credential and port-forward handling.
