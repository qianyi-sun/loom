"""Storage retention policy: provider-neutral rules + per-backend rendering.

Design
======

Loom's object store (MinIO by default; pluggable via `--storage external`)
accumulates trajectories and artifacts indefinitely without a retention
policy. We need server-side lifecycle rules — set them once at deploy
time and the storage backend expires objects in the background, with no
ongoing application load.

The shape is layered:

  ┌────────────────────────────────────────┐
  │  storage-lifecycle.toml                │  ← single source of truth
  └─────────────────┬──────────────────────┘
                    │
                    ▼
  ┌────────────────────────────────────────┐
  │  RetentionRule (typed dataclass below) │  ← provider-neutral
  └─────────────────┬──────────────────────┘
                    │
         ┌──────────┴──────────┐
         ▼                     ▼
  ┌──────────────┐    ┌──────────────────┐
  │ S3-compat    │    │ GCS / Azure      │
  │ renderer     │    │ renderer (later) │
  │ → lifecycle  │    │ → native rules   │
  │   dict       │    └──────────────────┘
  └──────────────┘

S3-compatible (MinIO / AWS S3 / Cloudflare R2 / Backblaze B2 / Wasabi)
share one renderer because they share the lifecycle schema. GCS and
Azure get separate renderers when those backends are added.

The renderer emits the dict shape that boto3
``put_bucket_lifecycle_configuration`` accepts (NOT raw XML — boto3
handles serialization).

Strategies shipped
==================

``expire_after_days``
    Delete objects N days after creation.

``keep_forever``
    Sentinel — emits no lifecycle rule. Documents intent explicitly so
    a future PR that mass-adds expiry doesn't accidentally apply it to
    long-term buckets like ATIF.

``cleanup_incomplete_uploads_after_hours``
    Aborts stuck multipart uploads. Doesn't touch completed objects.
    Matches the behavior previously documented in
    ``docs/architecture/cluster-deploy.md``.

Not shipped (build only when concretely needed)
-----------------------------------------------

``preserve_tag`` (tag-based escape hatch)
    S3 lifecycle cannot natively express "object does NOT have tag X".
    A correct implementation either inverts semantics (tag everything
    expire-able and use a positive tag filter — burdensome for the
    application) or runs a sweeper that joins against application
    state. We defer until Run Library has an explicit "pin" concept.

``tier_transition_after_days``
    AWS S3 only — MinIO has one storage tier.

``keep_latest_n_versions``
    Requires bucket versioning, which Loom doesn't use.

``per_prefix_expiry``
    If an operator needs different retention per prefix within one
    bucket, they should split the prefixes into separate buckets. The
    config is simpler and the lifecycle XML is simpler.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

Strategy = Literal[
    "expire_after_days",
    "keep_forever",
    "cleanup_incomplete_uploads_after_hours",
]

# Subset of providers that share the S3 lifecycle schema. The renderer
# uses this list defensively — if the operator's `backend` value isn't
# here, we raise at config-validate time rather than rendering an XML
# the backend doesn't honor.
S3_COMPATIBLE_BACKENDS: frozenset[str] = frozenset({
    "minio",
    "s3",
    "r2",
    "b2",
    "wasabi",
})

# GCS speaks its own lifecycle JSON dialect — see render_gcs_lifecycle
# below. Listed separately so the validator surfaces "supported, but
# uses a different renderer" vs "outright unknown backend."
GCS_BACKENDS: frozenset[str] = frozenset({"gcs"})

SUPPORTED_BACKENDS: frozenset[str] = S3_COMPATIBLE_BACKENDS | GCS_BACKENDS


@dataclass(frozen=True)
class RetentionRule:
    """One bucket's retention policy. Provider-neutral.

    The same rule renders into MinIO lifecycle XML, AWS S3 lifecycle XML,
    or (when those renderers ship) GCS / Azure equivalents.
    """

    bucket: str
    strategy: Strategy
    days: int | None = None
    hours: int | None = None
    rule_id: str | None = None

    def __post_init__(self) -> None:
        if self.strategy == "expire_after_days":
            if self.days is None or self.days < 1:
                raise ValueError(
                    f"strategy=expire_after_days requires days >= 1; "
                    f"got days={self.days!r}",
                )
            if self.hours is not None:
                raise ValueError(
                    "strategy=expire_after_days does not accept hours; "
                    "use cleanup_incomplete_uploads_after_hours instead",
                )
        elif self.strategy == "cleanup_incomplete_uploads_after_hours":
            if self.hours is None or self.hours < 1:
                raise ValueError(
                    f"strategy=cleanup_incomplete_uploads_after_hours "
                    f"requires hours >= 1; got hours={self.hours!r}",
                )
            if self.days is not None:
                raise ValueError(
                    "strategy=cleanup_incomplete_uploads_after_hours "
                    "does not accept days",
                )
        elif self.strategy == "keep_forever":
            if self.days is not None or self.hours is not None:
                raise ValueError(
                    "strategy=keep_forever does not accept days or hours",
                )


@dataclass(frozen=True)
class RetentionConfig:
    """The parsed contents of storage-lifecycle.toml."""

    backend: str
    rules: tuple[RetentionRule, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.backend not in SUPPORTED_BACKENDS:
            raise ValueError(
                f"backend={self.backend!r} is not supported by the "
                f"current renderers; supported: "
                f"{sorted(SUPPORTED_BACKENDS)}. (Azure Blob renderer "
                f"is the next family on the roadmap.)",
            )
        # Multiple rules per bucket are valid (e.g. one for object
        # expiry + one for multipart cleanup), but two of the SAME
        # strategy on one bucket isn't — the last would win silently.
        seen: set[tuple[str, str]] = set()
        for r in self.rules:
            key = (r.bucket, r.strategy)
            if key in seen:
                raise ValueError(
                    f"duplicate strategy={r.strategy!r} for "
                    f"bucket={r.bucket!r}; one strategy per bucket",
                )
            seen.add(key)


def render_bucket_lifecycle(
    config: RetentionConfig,
    *,
    bucket: str,
) -> dict[str, Any]:
    """Render the LifecycleConfiguration for ``bucket``.

    Returns the dict to pass as ``LifecycleConfiguration`` in the boto3
    ``put_bucket_lifecycle_configuration`` call. If no rule targets
    this bucket, returns an empty ``{"Rules": []}`` so the caller can
    decide whether to issue the API call or skip.

    All RetentionRules targeting the same bucket get merged into a
    single S3 LifecycleRule. AWS S3 accepts multiple rules per bucket
    and merges actions itself; MinIO is stricter and rejects rules that
    contain only AbortIncompleteMultipartUpload without a paired
    Expiration. Producing a single consolidated rule per bucket is
    accepted by both backends and is also what `mc ilm rule add`
    emits when you stack multiple ILM actions.

    `keep_forever` rules contribute nothing (no S3 action) but do not
    suppress merging — a bucket configured `keep_forever` with a
    sibling multipart-cleanup rule renders just the cleanup action,
    which is the right behavior (preserve forever but don't accumulate
    stuck uploads).
    """
    matching = [r for r in config.rules if r.bucket == bucket]
    if not matching:
        return {"Rules": []}

    merged: dict[str, Any] = {
        "ID": f"loom-{bucket}",
        "Status": "Enabled",
        "Filter": {"Prefix": ""},
    }
    has_expiration = False
    for rule in matching:
        if rule.strategy == "expire_after_days":
            assert rule.days is not None  # narrowed by __post_init__
            merged["Expiration"] = {"Days": rule.days}
            has_expiration = True
        # cleanup_incomplete_uploads_after_hours: kept in the config
        # schema for forward compatibility but NOT rendered for
        # S3-compatible backends. MinIO accepts the apply but silently
        # drops `AbortIncompleteMultipartUpload` from the persisted
        # config, which would make doctor report perpetual drift. An
        # AWS-S3-specific renderer can honor the strategy when it
        # ships (#221's roadmap).
        # keep_forever: contributes nothing; absence of Expiration on
        # the merged rule means objects never auto-expire.

    if not has_expiration:
        # Bucket is keep_forever (or has only non-rendered strategies).
        # Emit no rule; the caller skips the bucket entirely.
        return {"Rules": []}

    return {"Rules": [merged]}


def apply_lifecycle_to_s3(
    s3_client: Any,
    config: RetentionConfig,
    *,
    buckets: tuple[str, ...] | None = None,
) -> dict[str, dict[str, Any]]:
    """Apply every bucket's lifecycle rules via boto3.

    ``buckets`` lets callers restrict to a subset; ``None`` uses every
    bucket mentioned in the config. Returns a dict mapping bucket name
    to the rendered LifecycleConfiguration that was sent — useful for
    audit logs and idempotency assertions.

    Re-applying the same config produces the same XML, so re-runs are
    no-ops at the storage layer. Buckets whose rendered rules are
    empty are skipped (rather than calling delete_bucket_lifecycle,
    which would also remove any unrelated rules the operator added
    out-of-band).
    """
    target_buckets: set[str] = set(buckets) if buckets is not None else {
        r.bucket for r in config.rules
    }
    applied: dict[str, dict[str, Any]] = {}
    for bucket in sorted(target_buckets):
        rendered = render_bucket_lifecycle(config, bucket=bucket)
        if not rendered["Rules"]:
            continue
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration=rendered,
        )
        applied[bucket] = rendered
    return applied


# ──────────────────────────────────────────────────────────────────────
# GCS renderer
# ──────────────────────────────────────────────────────────────────────

# GCS speaks its own lifecycle JSON dialect, not S3-compatible XML. Two
# important differences from the S3 path:
#
#   - GCS supports MULTI-RULE policies natively (one action per rule).
#     We don't need MinIO's single-merged-rule workaround.
#   - GCS condition fields use `age` (days since object creation) rather
#     than `Days`. Other names also differ (storageClass, numNewerVersions,
#     etc.) but we don't use those today.
#
# This renderer produces the dict shape that the GCS REST API's
# `lifecycle` field on a bucket-patch accepts. The SDK integration
# (google-cloud-storage) is intentionally NOT taken on in this PR —
# adding the dep is a separate scope. Operators with a GCS deployment
# today can capture the rendered JSON via `--dry-run` and apply it via
# `gsutil lifecycle set` or `gcloud storage buckets update --lifecycle-file`.


def render_gcs_lifecycle(
    config: RetentionConfig,
    *,
    bucket: str,
) -> dict[str, Any]:
    """Render the bucket's retention rules as GCS lifecycle JSON.

    Returns a ``{"rule": [...]}`` dict matching the GCS bucket
    resource's ``lifecycle`` field. Empty list when no rules apply.

    Unlike the S3 path, GCS accepts multi-rule policies natively — we
    emit one rule per RetentionRule (no merging required).
    """
    if config.backend not in GCS_BACKENDS:
        raise ValueError(
            f"render_gcs_lifecycle called with backend={config.backend!r}; "
            f"GCS renderer only handles {sorted(GCS_BACKENDS)}",
        )

    rules: list[dict[str, Any]] = []
    for rule in config.rules:
        if rule.bucket != bucket:
            continue
        if rule.strategy == "expire_after_days":
            assert rule.days is not None
            rules.append({
                "action": {"type": "Delete"},
                "condition": {"age": rule.days},
            })
        elif rule.strategy == "cleanup_incomplete_uploads_after_hours":
            assert rule.hours is not None
            # GCS expresses multipart cleanup in days. Round up.
            days = max(1, (rule.hours + 23) // 24)
            rules.append({
                "action": {"type": "AbortIncompleteMultipartUpload"},
                "condition": {"age": days},
            })
        # keep_forever: contributes nothing; absence of a Delete action
        # means objects never auto-expire.

    return {"rule": rules}


def apply_lifecycle_to_gcs(
    gcs_client: Any,
    config: RetentionConfig,
    *,
    buckets: tuple[str, ...] | None = None,
) -> dict[str, dict[str, Any]]:
    """Apply rendered GCS lifecycle to each bucket via google-cloud-storage.

    Mirror of ``apply_lifecycle_to_s3``. The SDK integration is left as a
    follow-up; today this function raises ``NotImplementedError`` with a
    clear pointer at the operator workaround (use ``--dry-run`` and
    ``gsutil lifecycle set``).

    The signature is shipped now so that callers (bootstrap-storage-
    lifecycle) can dispatch on backend without conditional imports.
    """
    raise NotImplementedError(
        "GCS lifecycle apply requires google-cloud-storage integration "
        "(deferred until the first GCS deployment lands). For now, run "
        "`loom cluster bootstrap-storage-lifecycle --dry-run` to capture "
        "the rendered JSON, then apply via:\n"
        "  gcloud storage buckets update gs://<bucket> "
        "--lifecycle-file=<file>.json\n"
        "or `gsutil lifecycle set <file>.json gs://<bucket>`.",
    )
