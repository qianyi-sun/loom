"""Unit tests for the GCS lifecycle renderer (#254).

GCS uses its own lifecycle JSON dialect — not S3-compatible XML. The
renderer produces a dict matching the GCS bucket resource's
``lifecycle`` field. Multi-rule per bucket is allowed natively (no
MinIO-style merging required).
"""

from __future__ import annotations

import pytest

from loom.storage_retention import (
    GCS_BACKENDS,
    RetentionConfig,
    RetentionRule,
    apply_lifecycle_to_gcs,
    render_gcs_lifecycle,
)

# ──────────────────────────────────────────────────────────────────────
# Backend acceptance
# ──────────────────────────────────────────────────────────────────────


def test_gcs_backend_now_accepted_by_retention_config() -> None:
    """The validator should accept gcs as a valid backend (different
    renderer from S3-compatible, but still a supported renderer)."""
    cfg = RetentionConfig(backend="gcs", rules=(
        RetentionRule(
            bucket="trajectories", strategy="expire_after_days", days=30,
        ),
    ))
    assert cfg.backend == "gcs"


def test_gcs_backends_constant_exposed() -> None:
    """Operators and downstream code need to introspect which backends
    use the GCS renderer."""
    assert "gcs" in GCS_BACKENDS
    assert "minio" not in GCS_BACKENDS  # S3-compatible, not GCS


def test_unknown_backend_still_rejected() -> None:
    with pytest.raises(ValueError, match="not supported"):
        RetentionConfig(backend="azure", rules=())


# ──────────────────────────────────────────────────────────────────────
# render_gcs_lifecycle
# ──────────────────────────────────────────────────────────────────────


def test_render_rejects_non_gcs_backend() -> None:
    """Defensive check — caller must use the right renderer for the
    configured backend."""
    cfg = RetentionConfig(backend="minio", rules=(
        RetentionRule(
            bucket="trajectories", strategy="expire_after_days", days=30,
        ),
    ))
    with pytest.raises(ValueError, match="GCS renderer only handles"):
        render_gcs_lifecycle(cfg, bucket="trajectories")


def test_expire_after_days_renders_delete_action() -> None:
    cfg = RetentionConfig(backend="gcs", rules=(
        RetentionRule(
            bucket="trajectories", strategy="expire_after_days", days=30,
        ),
    ))
    out = render_gcs_lifecycle(cfg, bucket="trajectories")
    assert out == {"rule": [{
        "action": {"type": "Delete"},
        "condition": {"age": 30},
    }]}


def test_keep_forever_renders_no_rule() -> None:
    """keep_forever contributes no action — same semantics as the S3
    renderer."""
    cfg = RetentionConfig(backend="gcs", rules=(
        RetentionRule(bucket="atif", strategy="keep_forever"),
    ))
    out = render_gcs_lifecycle(cfg, bucket="atif")
    assert out == {"rule": []}


def test_cleanup_incomplete_uploads_renders_abort_action() -> None:
    """GCS supports AbortIncompleteMultipartUpload as a lifecycle
    action — unlike MinIO, where the field is silently dropped. We
    can emit it on GCS without doctor reporting drift."""
    cfg = RetentionConfig(backend="gcs", rules=(
        RetentionRule(
            bucket="trajectories",
            strategy="cleanup_incomplete_uploads_after_hours",
            hours=168,
        ),
    ))
    out = render_gcs_lifecycle(cfg, bucket="trajectories")
    assert out == {"rule": [{
        "action": {"type": "AbortIncompleteMultipartUpload"},
        "condition": {"age": 7},
    }]}


def test_cleanup_hours_round_up_to_days() -> None:
    """Same rounding semantics as the S3 renderer — operators reason
    about partial days; the API takes whole days."""
    cfg = RetentionConfig(backend="gcs", rules=(
        RetentionRule(
            bucket="trajectories",
            strategy="cleanup_incomplete_uploads_after_hours",
            hours=25,
        ),
    ))
    out = render_gcs_lifecycle(cfg, bucket="trajectories")
    assert out["rule"][0]["condition"]["age"] == 2


def test_cleanup_sub_day_floors_at_one() -> None:
    cfg = RetentionConfig(backend="gcs", rules=(
        RetentionRule(
            bucket="trajectories",
            strategy="cleanup_incomplete_uploads_after_hours",
            hours=12,
        ),
    ))
    out = render_gcs_lifecycle(cfg, bucket="trajectories")
    assert out["rule"][0]["condition"]["age"] == 1


def test_multiple_rules_per_bucket_emitted_separately() -> None:
    """GCS accepts multi-rule policies natively (no MinIO-style
    merging required). The renderer can emit one GCS rule per
    RetentionRule."""
    cfg = RetentionConfig(backend="gcs", rules=(
        RetentionRule(
            bucket="trajectories", strategy="expire_after_days", days=30,
        ),
        RetentionRule(
            bucket="trajectories",
            strategy="cleanup_incomplete_uploads_after_hours",
            hours=336,
        ),
    ))
    out = render_gcs_lifecycle(cfg, bucket="trajectories")
    assert len(out["rule"]) == 2
    actions = {r["action"]["type"] for r in out["rule"]}
    assert actions == {"Delete", "AbortIncompleteMultipartUpload"}


def test_per_bucket_filtering() -> None:
    """Only rules matching the requested bucket get emitted."""
    cfg = RetentionConfig(backend="gcs", rules=(
        RetentionRule(
            bucket="trajectories", strategy="expire_after_days", days=30,
        ),
        RetentionRule(
            bucket="artifacts", strategy="expire_after_days", days=180,
        ),
    ))
    out_t = render_gcs_lifecycle(cfg, bucket="trajectories")
    out_a = render_gcs_lifecycle(cfg, bucket="artifacts")
    assert out_t["rule"][0]["condition"]["age"] == 30
    assert out_a["rule"][0]["condition"]["age"] == 180


# ──────────────────────────────────────────────────────────────────────
# apply_lifecycle_to_gcs (placeholder)
# ──────────────────────────────────────────────────────────────────────


def test_apply_lifecycle_to_gcs_raises_with_actionable_message() -> None:
    """The SDK integration is deferred. Calling apply should raise a
    clear error pointing at the operator workaround (capture --dry-run
    output, apply via gsutil / gcloud)."""
    cfg = RetentionConfig(backend="gcs", rules=(
        RetentionRule(
            bucket="trajectories", strategy="expire_after_days", days=30,
        ),
    ))
    with pytest.raises(NotImplementedError, match="gsutil lifecycle set"):
        apply_lifecycle_to_gcs(object(), cfg)
