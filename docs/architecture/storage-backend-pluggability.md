# Storage Backend Pluggability

Status: design (no implementation). Informs the downstream renderers
referenced in [`storage-retention.md`](storage-retention.md) and the
external-storage on-ramp tracked in #221.

## Goal

Make the choice of object-store backend a first-class deployment
decision rather than an undocumented configuration knob. Today
`loom cluster` ships with single-replica MinIO as the default and
`--storage external` as a flag that *implies* swap-in support for AWS
S3 / GCS — but the external path has no end-to-end test, no
documented on-ramp, and no per-backend monitoring story. This spec
fixes that.

At v1.0, "Loom on managed object storage" should be a deployment
shape that operators can stand up by following the runbook, with the
same operational guarantees as MinIO on a PVC.

## Reference deployment shapes

Four shapes Loom targets. The first is the current default; the
others are the focus of this spec.

### 1. MinIO single-node (cluster-internal scratch)

What it is today:

- `deploy/k8s/minio.yaml` — single-replica StatefulSet, 500Gi PVC.
- Loom bootstraps the buckets at first start.
- Retention applied by `loom cluster bootstrap-storage-lifecycle`.
- Backup via operator-driven `mc mirror`.
- Monitoring via `LoomMinioPVCUsage{High,Critical}` on the PVC.

Honestly framed: this is **cluster-internal scratch space**, not
durable archival storage. No replication, no off-cluster backup
without operator effort. Fine for research clusters that treat the
object store as ephemeral; not fine for runs you want to keep
forever.

Stays as the default for developer / single-research-cluster
deployments. Not deprecated.

### 2. AWS S3 (managed, durable, first-class target)

Where Loom should be by v1.0:

- Operator pre-creates the buckets with the desired ACL + region.
- Loom applies the lifecycle rules via the same
  `bootstrap-storage-lifecycle` subcommand (the S3 renderer in
  `loom.storage_retention` already supports this).
- Credentials via IRSA on EKS; access-key fallback for vanilla k8s.
- Backup is provider-side: S3 versioning + cross-region replication;
  operator configures both, Loom does not.
- Monitoring via CloudWatch metrics (`BucketSizeBytes`,
  `NumberOfObjects`); the platform doesn't ship a dashboard.

End-to-end smoke test landed by the same v1.0 milestone — a
staging-smoke variant that points `LOOM_SVC_MINIO_ENDPOINT` at an
S3 bucket in a CI account.

### 3. GCS (managed, durable, second-class at v1.0)

Same shape as AWS S3, but the lifecycle renderer doesn't exist
yet. GCS uses Object Lifecycle Management with a different JSON
schema and labels (not S3 tags), so we need a separate renderer in
`loom.storage_retention`. The dataclass surface
(`RetentionRule`, `RetentionConfig`) is already provider-neutral and
can be reused unchanged.

GCS's S3-compatible API (HMAC keys + interoperability) covers
read/write, but does NOT cover lifecycle / IAM. So for an
operator using GCS:

- Read/write traffic continues to use the boto3 S3 client (no Loom
  changes).
- Lifecycle apply requires the GCS native renderer + the
  `google-cloud-storage` SDK.

Land the renderer when the first GCS deployment ships. Until then,
`loom cluster bootstrap-storage-lifecycle` raises with a clear
"GCS lifecycle renderer not yet implemented; apply rules manually
via `gsutil lifecycle set`" message.

### 4. On-prem multi-node MinIO with erasure coding

Stretch shape; not a separate code path. The operator runs
distributed MinIO somewhere (their own k8s, bare metal, ESXi) and
points `LOOM_SVC_MINIO_ENDPOINT` at the load-balanced front door.
Loom doesn't deploy or manage it. From Loom's perspective this is
the same as AWS S3 with a different endpoint URL.

Document the recommended MinIO configuration (erasure set, drive
layout, lifecycle rule timing) but don't bake any of it into
`deploy/k8s`.

### Not covered

- Azure Blob Storage. Architecturally similar; defer until a real
  deployment asks for it. The dataclass surface generalizes.
- S3-compatible providers we don't explicitly test (Cloudflare R2,
  Backblaze B2, Wasabi, Linode Object Storage). Should work
  out-of-the-box for read/write and lifecycle via the S3 renderer;
  not in the test matrix.

## What the operator pre-creates vs. what Loom bootstraps

A clean division along these lines:

| Item | MinIO (in-cluster) | AWS S3 / GCS / managed |
|---|---|---|
| Bucket creation | Loom (`bootstrap-storage-lifecycle`) | Operator (Terraform, console, gcloud) |
| Bucket region / class | n/a | Operator |
| Bucket ACL | Loom (private default) | Operator |
| Lifecycle rules | Loom (`bootstrap-storage-lifecycle`) | Loom (same subcommand) |
| Replication / versioning | n/a | Operator |
| IAM / IRSA / WI policies | n/a | Operator |
| Credentials → `loom-secrets` | Loom (`bootstrap-secrets`) | Operator (or external secret manager) |
| Lifecycle rule SCHEMA | Loom | Loom |
| Endpoint URL | Loom default | Operator-supplied |
| PVC sizing | Loom | n/a |

The rule of thumb: **Loom owns the policy schema and applies it;
the operator owns the bucket existence, durability characteristics,
and access policies on managed providers.**

No `bootstrap-buckets` subcommand for the managed case. Bucket
creation on AWS/GCS is part of the operator's IaC, not part of
Loom's deploy. Loom's bootstrap focuses on the things only Loom
knows how to configure (lifecycle rules tied to Loom's bucket
purposes).

## Credentials model

Three backends, three idiomatic credential paths each:

| Backend | Idiomatic | Fallback |
|---|---|---|
| MinIO (in-cluster) | Shared root key in `loom-secrets` | n/a |
| AWS S3 | IRSA (EKS service-account → IAM role) | Access key in `loom-secrets` |
| GCS | Workload Identity (GKE service-account → IAM principal) | SA JSON in `loom-secrets` |

Schema in `loom-schema.toml`:

```toml
[storage.credentials]
auth_kind = "static_keys"  # | "irsa" | "workload_identity" | "sa_json"

# Only when auth_kind = "static_keys" (covers MinIO + AWS-fallback +
# GCS-HMAC + every S3-compatible provider). The actual values flow
# from `loom-secrets`; this just names the keys to look up.
[storage.credentials.static_keys]
access_key_secret = "minio-access-key"
secret_key_secret = "minio-secret-key"

# auth_kind = "irsa" needs the operator to annotate the
# ServiceAccount; no values in loom-schema.toml beyond the choice
# itself.
```

The `auth_kind` discriminator is what makes this provider-neutral:
application code asks "give me a credentials provider," the
storage adapter picks the right one based on the discriminator, and
the boto3 / google-cloud-storage SDK uses the provider transparently.

### Why this matters

Today, `LOOM_SVC_MINIO_ACCESS_KEY` and `LOOM_SVC_MINIO_SECRET_KEY`
are required env vars on every component that talks to the object
store. If an operator deploys on EKS with IRSA, those env vars
shouldn't be required — boto3 already discovers the role through the
pod's service-account metadata. The `auth_kind = "irsa"` path tells
Loom not to require those env vars and not to pass explicit
credentials to the boto3 client constructor.

## Lifecycle renderer parity

Provider matrix:

| Provider family | Renderer | Status |
|---|---|---|
| S3-compatible (MinIO, AWS S3, R2, B2, Wasabi) | `loom.storage_retention.render_s3_lifecycle` | **Shipped in #226** |
| GCS native | `loom.storage_retention.render_gcs_lifecycle` (to add) | Design only |
| Azure Blob | `loom.storage_retention.render_azure_lifecycle` (to add) | Out of v1.0 scope |

The S3 renderer handles the bulk because the lifecycle XML is
shared across every S3-compatible provider. GCS needs a separate
renderer because:

- GCS uses Object Lifecycle Management with a JSON config, not S3
  lifecycle XML.
- GCS conditions are named differently (`age` vs `Days`,
  `numNewerVersions` instead of versioned-keep semantics).
- GCS has labels, not tags. The `preserve_tag` strategy (deferred
  in #226) needs translation if it lands.

The dataclass surface (`RetentionRule`, `RetentionConfig`) is
unchanged — only the rendering function differs. Configuration
shape, validation, and idempotency contract all carry over.

### When to land the GCS renderer

When the first GCS deployment asks for it. Until then, calling
`bootstrap-storage-lifecycle` with `backend = "gcs"` raises with a
documented error directing the operator to run `gsutil lifecycle
set` manually with the rules from `--dry-run` output.

## Migration paths

Two real cases worth designing for:

### MinIO single-node → AWS S3 (research cluster moving to managed)

Likely path for any deployment that outgrows 500Gi. Steps:

1. **Operator pre-creates the destination buckets** with the desired
   region, ACL, and replication config.
2. **Apply Loom's lifecycle rules to the destination:**
   ```bash
   loom cluster bootstrap-storage-lifecycle \
     --config config/storage-lifecycle.toml \
     --endpoint https://s3.us-east-1.amazonaws.com
   ```
3. **Mirror the data:**
   ```bash
   mc mirror --overwrite loom-minio/ s3://destination-bucket/
   ```
   Wall-clock time is dominated by bandwidth; ~1 GB/s typical for
   in-region transfers, so 500Gi → minutes to ~an hour.
4. **Switch the application** by updating `LOOM_SVC_MINIO_ENDPOINT`
   and credentials in `loom-secrets`, then rolling-restart the
   workers + service.
5. **Decommission the old PVC** once new uploads land cleanly and
   trial finalization succeeds.

Downtime: 1-2 minutes for the rolling restart, if scheduled during
low traffic. Zero-downtime via dual-write is achievable but not
worth the complexity for v1.0.

### AWS S3 → MinIO (rare; not optimized)

Symmetric to the above using `mc mirror s3://... loom-minio/`.
Document but don't optimize. Most deployments won't reverse-migrate.

## Monitoring parity

`LoomMinioPVCUsage{High,Critical}` (from #220) target
`kubelet_volume_stats_*` for the PVC. Neither metric exists for
managed object storage. Loom does NOT ship a unified backend-agnostic
monitoring story. Operators wire the backend's native monitoring:

| Backend | Recommended monitoring |
|---|---|
| MinIO in-cluster | `LoomMinioPVCUsage{High,Critical}` (current; ships in `prometheus-rules.yaml`) |
| AWS S3 | CloudWatch `BucketSizeBytes`; alert via CloudWatch Alarms or import to Prometheus via `cloudwatch-exporter` |
| GCS | Stackdriver / Cloud Monitoring `storage.googleapis.com/storage/total_bytes`; alert via Monitoring |
| MinIO distributed (on-prem) | Distributed MinIO ships per-drive Prometheus metrics; operator scrapes + alerts |

The runbook section for each external backend should list the
relevant metric + a sample alert threshold (80% of provisioned
capacity, or absolute byte threshold for managed providers without
a hard quota).

### Why not a unified dashboard?

Tempting but wrong:

- The metric names differ across providers, so a "unified" panel
  ends up being N per-backend panels with conditional rendering.
- Operators using managed providers already invest in that
  provider's monitoring stack and prefer to keep the alerting
  there.
- Loom can't usefully alert on what it can't see; managed providers
  don't expose per-bucket usage to the application by default.

The runbook will explicitly say "wire your backend's native
monitoring per this section" rather than promising a single
dashboard.

## Backup posture

| Deployment | Backup strategy | Loom's role |
|---|---|---|
| MinIO single-node (default) | `mc mirror` to a second disk or S3 (operator schedules cron) | Document the procedure (`docs/operator-runbook.md`); no automation |
| MinIO distributed (on-prem) | Erasure coding + scheduled `mc mirror` for disaster recovery | Document expected redundancy level |
| AWS S3 | Versioning + cross-region replication, both operator-configured | Document the recommended bucket policy |
| GCS | Versioning + cross-region replication | Same |

Loom doesn't run backups. Loom documents what posture each shape
implies and what the operator is responsible for. The operator
runbook's "Backup" section needs a per-shape table that calls this
out explicitly, replacing the current MinIO-centric instructions.

## Implementation roadmap

In order, after this design lands:

1. **`storage.backend` + `storage.credentials` schema in
   `loom-schema.toml`.** Wires the `auth_kind` discriminator
   through the existing settings classes. Touches the codegen.
2. **boto3 credentials provider abstraction.** Replaces the
   explicit access-key/secret-key passing in
   `loom_service.storage.create_minio_client` with a factory that
   honors the `auth_kind`. Touches every component that holds an
   S3 client.
3. **`bootstrap-storage-lifecycle` reads the new schema.** Falls
   back to env vars for the static-keys case so existing
   deployments keep working.
4. **Staging-smoke variant against real AWS S3 in a CI account.**
   Tests the `irsa` + `static_keys` paths end-to-end.
5. **GCS lifecycle renderer.** Lands when the first GCS deployment
   asks for it; not blocking on the rest.
6. **Operator-runbook per-shape sections.** One subsection per
   reference shape (MinIO / S3 / GCS), each with bootstrap + apply +
   monitoring + backup walkthrough.

Steps 1-3 are the v1.0 critical path. Steps 4-6 round it out.

## Decision log

These are the choices this spec makes:

| Decision | Rationale |
|---|---|
| Operator pre-creates buckets on managed providers | Bucket creation involves region/ACL/replication policy that Loom doesn't know enough to set sensibly. |
| Same `bootstrap-storage-lifecycle` for every backend | The lifecycle policy is Loom's concern; the per-backend rendering is the renderer's. Same UX. |
| `auth_kind` discriminator over per-backend env vars | Lets boto3 / google-cloud-storage SDKs do their own credential discovery for IRSA / Workload Identity, which is the idiomatic path. |
| No unified backend-agnostic monitoring dashboard | Different providers expose different metrics; "unified" devolves to N conditional panels. Operators use the backend's native monitoring. |
| Loom does not run backups | Backup is a property of the deployment shape, not the platform. Loom documents expectations. |
| GCS renderer is lazy | Defer until a real deployment needs it. The dataclass surface is already provider-neutral. |
| MinIO single-node stays as the default | Right for the developer / single-research-cluster case. Not deprecated. |
| Cloudflare R2 / Backblaze B2 / Wasabi work via the S3 path; not first-class | They share the lifecycle schema and pass through unchanged. Not in the test matrix. |

## See also

- [`storage-retention.md`](storage-retention.md) — the policy this design generalizes.
- [`cluster-deploy.md`](cluster-deploy.md) — where MinIO sits in the deploy.
- [`operator-runbook.md#minio-pvc-usage`](../operator-runbook.md#minio-pvc-usage) — the per-backend monitoring story this design needs to generalize.
- #219 (shipped via #226), #220 (shipped via #226) — sibling issues whose downstream work this design unblocks.
