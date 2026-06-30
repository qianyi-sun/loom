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
        if self.backend not in S3_COMPATIBLE_BACKENDS:
            raise ValueError(
                f"backend={self.backend!r} is not supported by the "
                f"current renderer; supported: "
                f"{sorted(S3_COMPATIBLE_BACKENDS)}. (Add GCS / Azure "
                f"renderers when those backends ship.)",
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


def render_s3_lifecycle(rule: RetentionRule) -> dict[str, Any] | None:
    """Render one rule as an S3 ``LifecycleConfiguration.Rule`` dict.

    Returns ``None`` for ``keep_forever`` (the rule is intentionally
    no-op; the caller filters Nones out before sending to the API).

    Returned dicts match the boto3
    ``put_bucket_lifecycle_configuration`` schema, which is the same
    shape MinIO's ``mc ilm`` and AWS S3 accept. Boto3 handles XML
    serialization downstream.
    """
    if rule.strategy == "keep_forever":
        return None

    rule_id = rule.rule_id or _default_rule_id(rule)
    out: dict[str, Any] = {
        "ID": rule_id,
        "Status": "Enabled",
        # An empty prefix matches all objects in the bucket. S3 requires
        # a filter clause; the simplest is Prefix="".
        "Filter": {"Prefix": ""},
    }

    if rule.strategy == "expire_after_days":
        assert rule.days is not None  # narrowed by __post_init__
        out["Expiration"] = {"Days": rule.days}
    elif rule.strategy == "cleanup_incomplete_uploads_after_hours":
        assert rule.hours is not None
        # S3 lifecycle expresses this in days. We accept hours in the
        # config (operators reason about partial-day cleanup windows)
        # and round up to the next whole day. AWS supports a minimum
        # of 1 day.
        days = max(1, (rule.hours + 23) // 24)
        out["AbortIncompleteMultipartUpload"] = {
            "DaysAfterInitiation": days,
        }
    return out


def _default_rule_id(rule: RetentionRule) -> str:
    """Stable rule id so re-applying the same config produces
    byte-identical XML (true idempotency)."""
    base = f"loom-{rule.strategy}-{rule.bucket}"
    if rule.strategy == "expire_after_days":
        return f"{base}-{rule.days}d"
    if rule.strategy == "cleanup_incomplete_uploads_after_hours":
        return f"{base}-{rule.hours}h"
    return base


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
    """
    rendered: list[dict[str, Any]] = []
    for rule in config.rules:
        if rule.bucket != bucket:
            continue
        out = render_s3_lifecycle(rule)
        if out is not None:
            rendered.append(out)
    return {"Rules": rendered}


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
