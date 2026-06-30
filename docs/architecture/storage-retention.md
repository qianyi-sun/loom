# Storage Retention

Status: shipped. Operator-configurable lifecycle rules applied to the
object store via the storage backend's native API. Pluggable across
S3-compatible providers (MinIO, AWS S3, R2, B2, Wasabi). GCS and Azure
renderers will land when those backends ship.

## Goal

Cap unbounded growth of trajectories + artifacts in the object store
without operator intervention beyond a one-time bootstrap step. The
default MinIO PVC is fixed at 500Gi; without retention the disk fills
and trial uploads start failing with "no space left on device."

## Design

Three layers of cleanup, in priority order.

1. **Server-side lifecycle rules.** The 95% case. Every S3-compatible
   backend supports native lifecycle policies; the server applies
   them in the background with zero application load. This is what
   we ship.

2. **Application-side preservation tags.** Escape hatches for "don't
   expire this object, it's referenced from Run Library." Object
   gets tagged at create time; the lifecycle rule excludes that tag.
   *Not yet implemented* — S3 lifecycle can't natively express
   "tag is absent," so a clean implementation needs the application
   to tag expire-able objects (inverse semantics). Deferred until
   Run Library has an explicit "pin" concept.

3. **Application-side sweeper.** Only for predicates that need joins
   against application state. **Not in scope.** Build only when
   concretely needed.

## Architecture

Layering:

```
┌────────────────────────────────────────┐
│  config/storage-lifecycle.toml         │  ← single source of truth
└─────────────────┬──────────────────────┘
                  │ load_retention_config(path)
                  ▼
┌────────────────────────────────────────┐
│  RetentionConfig                       │  ← provider-neutral
│   RetentionRule × N                    │     dataclasses
└─────────────────┬──────────────────────┘
                  │ render_s3_lifecycle(rule)
       ┌──────────┴──────────┐
       ▼                     ▼
┌──────────────┐    ┌──────────────────┐
│ S3-compat    │    │ GCS / Azure      │
│ renderer     │    │ renderer (later) │
│ → dict for   │    │ → native rules   │
│   boto3 API  │    └──────────────────┘
└──────┬───────┘
       │ apply_lifecycle_to_s3(s3, cfg)
       ▼
┌──────────────────────────────────────────┐
│  boto3.client('s3').put_bucket_lifecycle │
│  → MinIO / S3 / R2 / B2 / Wasabi         │
└──────────────────────────────────────────┘
```

- Source code: `src/loom/storage_retention.py` (renderer + apply),
  `src/loom/storage_retention_loader.py` (TOML → dataclass).
- Apply path: `loom cluster bootstrap-storage-lifecycle` in
  `src/loom_cli/cluster_cmd.py`.
- Config: `config/storage-lifecycle.example.toml`. Operators copy and
  edit; the CLI loads the operator's path via `--config`.

The renderer is data-only — no I/O, no SDK calls — so it's easy to
unit-test and re-usable by future renderers that need the same shape
(e.g., a `loom cluster render` step that bakes lifecycle XML into a
deployment artifact).

## Strategies

| Name | Use case | Maps to |
|---|---|---|
| `expire_after_days` | Trajectories, raw artifacts (95% case) | S3 lifecycle `Expiration.Days` |
| `keep_forever` | ATIF, evidence bundles | Sentinel; no rule emitted |
| `cleanup_incomplete_uploads_after_hours` | Stuck multipart uploads | `AbortIncompleteMultipartUpload.DaysAfterInitiation` (hours rounded up to days) |

Multiple strategies per bucket are valid (e.g. expiry + multipart
cleanup). Two of the *same* strategy on one bucket is rejected at
config-load time — the last would silently win, which is rarely the
operator's intent.

### Not shipped (build only when concretely needed)

- `preserve_tag` — see "Application-side preservation tags" above.
- `tier_transition_after_days` — AWS S3 only; MinIO has one tier.
- `keep_latest_n_versions` — Loom doesn't use bucket versioning.
- Per-prefix expiry within one bucket — operators split into separate
  buckets if needed (config + lifecycle XML both stay simpler).

## Defaults

The bundled `storage-lifecycle.example.toml` sets:

- `trajectories`: expire after 30 days + multipart cleanup after
  14 days. Trajectories accumulate fastest and lose most of their
  investigation value past a successful trial finalize.
- `artifacts`: expire after 180 days + multipart cleanup after
  7 days. Larger objects, generous review window.
- `atif`: keep forever + multipart cleanup after 7 days. ATIF
  projections are small, queryable, and represent the permanent
  record of every trial.

These match SOC2-equivalent service-data-retention conventions.
Operators are expected to tune per their team's investigation
cadence and disk size.

## Idempotency

Re-applying the same config produces byte-identical lifecycle XML:

- Each rule has a stable, deterministic `ID` derived from
  `(strategy, bucket, days|hours)`.
- The renderer is a pure function of the input config.
- `put_bucket_lifecycle_configuration` is idempotent on the same
  config.

Operators can re-run `bootstrap-storage-lifecycle` as part of any
deploy or upgrade workflow without fear of churn.

## Drift detection

The realistic failure mode the unit tests cannot catch is "operator
forgot to run `bootstrap-storage-lifecycle`," followed by "operator
edited the config and forgot to re-apply," followed (rarely) by
"someone removed the rules out-of-band via `mc ilm rule remove`."

`loom cluster doctor --storage-lifecycle-config <path>` closes this:
it calls `get_bucket_lifecycle_configuration` per bucket mentioned in
the config, compares the live rule set against what
`render_bucket_lifecycle` would produce, and reports drift by
category (missing, extra, content). Re-running the bootstrap is
always the fix.

Doctor opt-in (not on by default) because it requires `boto3` + MinIO
credentials, which the schema-reconciliation half of doctor doesn't.
Operators with the storage config wired in pass the flag; everyone
else gets unchanged behavior.

The check is operator-driven, not continuous. A Prometheus metric
would require the CP to grow a `boto3` client and poll periodically —
meaningful expansion of CP's responsibilities for a low-frequency
failure mode. The doctor command + the post-`up` reminder cover the
realistic gaps without that complexity.

## Operator nudge after `cluster up`

After `loom cluster up` reaches `all_ready`, the CLI prints a
"Next steps" block naming the canonical post-deploy commands
(`bootstrap-storage-lifecycle` + `doctor --storage-lifecycle-config`).
Visible. Discoverable. Closes the "I didn't know I had to run that"
failure mode without taking on the brittleness of auto-applying
lifecycle from inside the deploy flow (port-forward orchestration is
not worth it for a once-per-deploy operator step).

## What happens when the disk fills

Even with retention, growth can outpace it (large research bursts,
disk smaller than expected). The companion `LoomMinioPVCUsageHigh`
(80% warning) and `LoomMinioPVCUsageCritical` (95% critical) alerts in
`deploy/k8s/prometheus-rules.yaml` surface the condition before
writes fail. The operator-runbook's "MinIO PVC usage" section walks
through the remediation paths in priority order.

## Out of scope

- AWS S3 / GCS renderers. Design supports them; implementation lands
  when a deployment actually uses external storage. The design
  discussion is tracked in #221.
- Application-side sweeper.
- Application-side preservation tagging at create time.
- Per-prefix expiry within one bucket.

## See also

- [`operator-runbook.md#storage-retention-policy`](../operator-runbook.md#storage-retention-policy) — operator workflow.
- [`operator-runbook.md#minio-pvc-usage`](../operator-runbook.md#minio-pvc-usage) — alert response.
- [`cluster-deploy.md`](cluster-deploy.md) — where MinIO sits in the deploy.
