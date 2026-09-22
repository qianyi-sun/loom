"""Validated retention rules for S3-compatible object stores.

Expiration rules render to boto3 lifecycle configuration. ``keep_forever``
emits no rule; multipart-cleanup settings are retained for config compatibility
but currently emit no action. Non-S3 lifecycle backends are unsupported.
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
    """One bucket's retention policy."""

    bucket: str
    strategy: Strategy
    days: int | None = None
    hours: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.bucket, str) or not self.bucket.strip():
            raise ValueError("retention bucket must be a non-empty string")
        if self.strategy not in (
            "expire_after_days", "keep_forever", "cleanup_incomplete_uploads_after_hours",
        ):
            raise ValueError(f"unsupported retention strategy: {self.strategy!r}")
        for name, duration in (("days", self.days), ("hours", self.hours)):
            if duration is not None and type(duration) is not int:
                raise ValueError(f"{name} must be an integer")
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
                f"current renderers; supported: "
                f"{sorted(S3_COMPATIBLE_BACKENDS)}.",
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
            opposite = "keep_forever" if r.strategy == "expire_after_days" else "expire_after_days"
            if r.strategy in ("keep_forever", "expire_after_days") and (r.bucket, opposite) in seen:
                raise ValueError(f"conflicting keep_forever and expiration rules for bucket={r.bucket!r}")
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

    ``keep_forever`` and multipart-cleanup rules contribute no S3 action.
    Only expiration is currently rendered.
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
