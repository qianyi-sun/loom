"""Unit tests for the storage retention doctor check (#229).

Covers the diff logic in `loom.storage_retention_doctor.check_lifecycle_drift`
and the report formatter. Uses a stub S3 client so we test the
boto3-shape contract without a live MinIO (that's #230's job).
"""

from __future__ import annotations

import pytest

from loom.storage_retention import RetentionConfig, RetentionRule
from loom.storage_retention_doctor import (
    BucketDrift,
    check_lifecycle_drift,
    format_drift_report,
)


class _StubS3:
    """Replays a pre-loaded map from bucket → lifecycle response."""

    def __init__(self, by_bucket: dict[str, dict] | None = None) -> None:
        self._by_bucket = by_bucket or {}
        self.calls: list[str] = []

    def get_bucket_lifecycle_configuration(
        self, *, Bucket: str,  # noqa: N803
    ) -> dict:
        self.calls.append(Bucket)
        if Bucket not in self._by_bucket:
            # Mimic boto3's behavior when no lifecycle is set.
            raise RuntimeError(
                "NoSuchLifecycleConfiguration: no lifecycle policy",
            )
        return self._by_bucket[Bucket]


def _expire_rule(bucket: str, days: int) -> dict:
    """Convenience: the dict our renderer would emit for an
    expire-after-days rule."""
    return {
        "ID": f"loom-expire_after_days-{bucket}-{days}d",
        "Status": "Enabled",
        "Filter": {"Prefix": ""},
        "Expiration": {"Days": days},
    }


# ──────────────────────────────────────────────────────────────────────
# happy paths
# ──────────────────────────────────────────────────────────────────────


def test_no_drift_when_live_matches_config() -> None:
    """The minimal good case: live MinIO returns exactly what we'd
    render. No drift reported."""
    cfg = RetentionConfig(backend="minio", rules=(
        RetentionRule(
            bucket="trajectories", strategy="expire_after_days", days=30,
        ),
    ))
    s3 = _StubS3({"trajectories": {"Rules": [_expire_rule("trajectories", 30)]}})

    drifts = check_lifecycle_drift(s3, cfg)
    assert len(drifts) == 1
    assert not drifts[0].is_drift
    assert format_drift_report(drifts) == ""


def test_keep_forever_bucket_with_no_live_rules_is_not_drift() -> None:
    """A bucket whose config strategy is keep_forever renders empty
    rules; a live bucket with no lifecycle policy should also report
    empty. They match — no drift."""
    cfg = RetentionConfig(backend="minio", rules=(
        RetentionRule(bucket="atif", strategy="keep_forever"),
    ))
    s3 = _StubS3()  # atif not in stub → NoSuchLifecycleConfiguration

    drifts = check_lifecycle_drift(s3, cfg)
    assert len(drifts) == 1
    assert not drifts[0].is_drift


# ──────────────────────────────────────────────────────────────────────
# the realistic failure modes
# ──────────────────────────────────────────────────────────────────────


def test_missing_live_rules_is_flagged() -> None:
    """Operator never ran bootstrap-storage-lifecycle (or the rules
    were removed). Most common case; report should be very clear."""
    cfg = RetentionConfig(backend="minio", rules=(
        RetentionRule(
            bucket="trajectories", strategy="expire_after_days", days=30,
        ),
    ))
    s3 = _StubS3()  # No lifecycle on the live bucket

    drifts = check_lifecycle_drift(s3, cfg)
    assert drifts[0].is_drift
    assert "missing on storage" in drifts[0].detail

    report = format_drift_report(drifts)
    assert "trajectories" in report
    assert "Re-apply" in report


def test_content_drift_when_days_changed_in_config() -> None:
    """The operator edited storage-lifecycle.toml (e.g., shortened the
    window) but never re-ran the bootstrap. Same rule ID, different
    `Expiration.Days`."""
    cfg = RetentionConfig(backend="minio", rules=(
        RetentionRule(
            bucket="trajectories", strategy="expire_after_days", days=14,
        ),
    ))
    # Live still has the OLD 14-day rule under the SAME ID since the ID
    # incorporates days. So the IDs differ, not the content. The check
    # should catch this as "missing on storage."
    s3 = _StubS3({"trajectories": {"Rules": [_expire_rule("trajectories", 30)]}})

    drifts = check_lifecycle_drift(s3, cfg)
    assert drifts[0].is_drift
    # Days changed → rule ID changed → missing+extra branch
    assert "missing on storage" in drifts[0].detail


def test_extra_live_rule_is_flagged_informationally() -> None:
    """Operator added a rule out-of-band (via `mc ilm rule add`). We
    don't manage that rule, but we should surface its existence."""
    cfg = RetentionConfig(backend="minio", rules=(
        RetentionRule(
            bucket="trajectories", strategy="expire_after_days", days=30,
        ),
    ))
    s3 = _StubS3({"trajectories": {"Rules": [
        _expire_rule("trajectories", 30),
        {
            "ID": "operator-added-rule",
            "Status": "Enabled",
            "Filter": {"Prefix": "evidence/"},
            "Expiration": {"Days": 7},
        },
    ]}})

    drifts = check_lifecycle_drift(s3, cfg)
    assert drifts[0].is_drift
    assert "operator-added-rule" in drifts[0].detail
    assert "present on storage but not in config" in drifts[0].detail


# ──────────────────────────────────────────────────────────────────────
# multi-bucket
# ──────────────────────────────────────────────────────────────────────


def test_per_bucket_drift_independent() -> None:
    """One bucket clean, one bucket missing — report covers both."""
    cfg = RetentionConfig(backend="minio", rules=(
        RetentionRule(
            bucket="trajectories", strategy="expire_after_days", days=30,
        ),
        RetentionRule(
            bucket="artifacts", strategy="expire_after_days", days=180,
        ),
    ))
    s3 = _StubS3({
        "trajectories": {"Rules": [_expire_rule("trajectories", 30)]},
        # 'artifacts' missing — operator only applied to one bucket
    })

    drifts = check_lifecycle_drift(s3, cfg)
    by_bucket = {d.bucket: d for d in drifts}
    assert not by_bucket["trajectories"].is_drift
    assert by_bucket["artifacts"].is_drift

    report = format_drift_report(drifts)
    assert "artifacts" in report
    assert "trajectories" not in report  # only listed when drifting


# ──────────────────────────────────────────────────────────────────────
# transport errors propagate
# ──────────────────────────────────────────────────────────────────────


def test_transport_error_propagates() -> None:
    """Network issue / wrong credentials / bucket gone → don't swallow.
    The caller surfaces the real error to the operator."""

    class _BoomS3:
        def get_bucket_lifecycle_configuration(
            self, *, Bucket: str,  # noqa: N803
        ) -> dict:
            raise RuntimeError("ConnectionRefused")

    cfg = RetentionConfig(backend="minio", rules=(
        RetentionRule(
            bucket="trajectories", strategy="expire_after_days", days=30,
        ),
    ))
    with pytest.raises(RuntimeError, match="ConnectionRefused"):
        check_lifecycle_drift(_BoomS3(), cfg)


# ──────────────────────────────────────────────────────────────────────
# BucketDrift dataclass behavior
# ──────────────────────────────────────────────────────────────────────


def test_bucket_drift_is_drift_property() -> None:
    clean = BucketDrift("x", (), (), "")
    dirty = BucketDrift("x", ("a",), (), "missing")
    assert not clean.is_drift
    assert dirty.is_drift
