"""Doctor check: compare live MinIO/S3 bucket lifecycle against config.

Used by `loom cluster doctor --storage-lifecycle-config PATH` to
surface the realistic failure mode that the unit tests cannot catch:
the operator forgot to run ``bootstrap-storage-lifecycle``, OR the
rules were applied at one point but later removed out-of-band (rare
but possible — ``mc ilm rule remove``, MinIO restored from an old
backup, manual edit).

Kept separate from ``storage_retention.py`` so the renderer module
stays pure data. This module talks to a live S3 client and is
exercised against a stub in tests; the CLI plumbs the real client in.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from loom.storage_retention import (
    RetentionConfig,
    render_bucket_lifecycle,
)


@dataclass(frozen=True)
class BucketDrift:
    """One bucket's diff between expected and live lifecycle rules."""

    bucket: str
    expected_rule_ids: tuple[str, ...]
    live_rule_ids: tuple[str, ...]
    detail: str

    @property
    def is_drift(self) -> bool:
        return self.detail != ""


def check_lifecycle_drift(
    s3_client: Any,
    config: RetentionConfig,
) -> tuple[BucketDrift, ...]:
    """Compare expected lifecycle rules (from ``config``) against live
    bucket state. Returns one BucketDrift per bucket mentioned in
    ``config``; the ``is_drift`` property surfaces whether action is
    needed.

    Empty live rules + non-empty expected = "operator forgot to apply"
    or "rules were removed since last apply."

    Empty expected + any live rules = informational only; the operator
    may have applied additional rules out-of-band that we don't manage.
    Reported as drift so the operator sees it but with a "live-only"
    detail; the caller decides whether to fail the check on this.

    Different rule IDs OR mismatched content within matching IDs = the
    config changed and the operator forgot to re-apply.
    """
    buckets_in_config = sorted({r.bucket for r in config.rules})
    drifts: list[BucketDrift] = []
    for bucket in buckets_in_config:
        expected = render_bucket_lifecycle(config, bucket=bucket)
        live = _fetch_live_lifecycle(s3_client, bucket)
        drift = _compare(bucket, expected, live)
        drifts.append(drift)
    return tuple(drifts)


def _fetch_live_lifecycle(s3_client: Any, bucket: str) -> dict[str, Any]:
    """Wrap the boto3 get_bucket_lifecycle_configuration call.

    Returns ``{"Rules": []}`` when no lifecycle is configured (boto3
    raises ``NoSuchLifecycleConfiguration`` in that case). Other errors
    propagate so the caller can decide whether to treat them as failure
    or as an unreachable storage backend.
    """
    try:
        resp = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)
    except Exception as exc:
        # boto3 raises ClientError with code NoSuchLifecycleConfiguration
        # when the bucket exists but has no lifecycle policy. We treat
        # that as "empty rules" — operators care about the diff, not the
        # exception type.
        if "NoSuchLifecycleConfiguration" in str(exc):
            return {"Rules": []}
        # Bucket doesn't exist or transport error — propagate.
        raise
    return {"Rules": list(resp.get("Rules", []))}


def _compare(
    bucket: str,
    expected: dict[str, Any],
    live: dict[str, Any],
) -> BucketDrift:
    """Diff one bucket's rules. Returns BucketDrift with a
    human-readable ``detail`` string (empty when no drift)."""
    expected_ids = tuple(sorted(r["ID"] for r in expected["Rules"]))
    live_ids = tuple(sorted(r.get("ID", "") for r in live["Rules"]))

    if expected_ids == live_ids == ():
        # Bucket is intentionally keep_forever (or has no rules); live
        # is also empty. No drift.
        return BucketDrift(bucket, expected_ids, live_ids, "")

    missing = set(expected_ids) - set(live_ids)
    extra = set(live_ids) - set(expected_ids)
    detail_parts: list[str] = []
    if missing:
        detail_parts.append(
            f"missing on storage: {sorted(missing)}",
        )
    if extra:
        detail_parts.append(
            f"present on storage but not in config: {sorted(extra)}",
        )
    # Content drift on matching IDs — same name but different parameters
    # (e.g., days changed in config but not re-applied).
    if not detail_parts:
        live_by_id = {r["ID"]: r for r in live["Rules"]}
        expected_by_id = {r["ID"]: r for r in expected["Rules"]}
        content_drift = sorted(
            rid for rid in expected_ids
            if live_by_id.get(rid) != expected_by_id.get(rid)
        )
        if content_drift:
            detail_parts.append(
                f"content drift on rule(s): {content_drift}; re-run "
                "`loom cluster bootstrap-storage-lifecycle`",
            )

    detail = "; ".join(detail_parts)
    return BucketDrift(bucket, expected_ids, live_ids, detail)


def format_drift_report(drifts: tuple[BucketDrift, ...]) -> str:
    """Operator-readable report. Empty string when nothing drifts."""
    bad = [d for d in drifts if d.is_drift]
    if not bad:
        return ""
    lines = ["Storage lifecycle drift detected:"]
    for d in bad:
        lines.append(f"  - bucket={d.bucket}: {d.detail}")
    lines.append(
        "\nRe-apply with `loom cluster bootstrap-storage-lifecycle "
        "--config <path>`.",
    )
    return "\n".join(lines)
