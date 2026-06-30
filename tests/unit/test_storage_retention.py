"""Unit tests for the storage retention renderer + loader (#219).

Tests the data layer: RetentionRule validation, RetentionConfig
validation, S3 lifecycle rendering, and TOML loading. The apply path
is exercised in a separate integration test against a stub S3 client.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from loom.storage_retention import (
    RetentionConfig,
    RetentionRule,
    apply_lifecycle_to_s3,
    render_bucket_lifecycle,
)
from loom.storage_retention_loader import load_retention_config

# ──────────────────────────────────────────────────────────────────────
# RetentionRule validation
# ──────────────────────────────────────────────────────────────────────


def test_expire_after_days_requires_positive_days() -> None:
    with pytest.raises(ValueError, match="days >= 1"):
        RetentionRule(bucket="trajectories", strategy="expire_after_days")
    with pytest.raises(ValueError, match="days >= 1"):
        RetentionRule(
            bucket="trajectories", strategy="expire_after_days", days=0,
        )


def test_expire_after_days_rejects_hours() -> None:
    with pytest.raises(ValueError, match="does not accept hours"):
        RetentionRule(
            bucket="trajectories",
            strategy="expire_after_days",
            days=30,
            hours=24,
        )


def test_cleanup_incomplete_requires_positive_hours() -> None:
    with pytest.raises(ValueError, match="hours >= 1"):
        RetentionRule(
            bucket="trajectories",
            strategy="cleanup_incomplete_uploads_after_hours",
        )


def test_cleanup_incomplete_rejects_days() -> None:
    with pytest.raises(ValueError, match="does not accept days"):
        RetentionRule(
            bucket="trajectories",
            strategy="cleanup_incomplete_uploads_after_hours",
            hours=24,
            days=1,
        )


def test_keep_forever_rejects_any_duration() -> None:
    with pytest.raises(ValueError, match="does not accept days or hours"):
        RetentionRule(
            bucket="atif", strategy="keep_forever", days=30,
        )


# ──────────────────────────────────────────────────────────────────────
# RetentionConfig validation
# ──────────────────────────────────────────────────────────────────────


def test_unsupported_backend_raises() -> None:
    with pytest.raises(ValueError, match="not supported by the current"):
        RetentionConfig(backend="gcs", rules=())


def test_duplicate_strategy_for_one_bucket_raises() -> None:
    """Two `expire_after_days` rules on the same bucket would silently
    let the last one win; surface as an error at config-load time."""
    rule = RetentionRule(
        bucket="trajectories", strategy="expire_after_days", days=30,
    )
    rule_b = RetentionRule(
        bucket="trajectories", strategy="expire_after_days", days=60,
    )
    with pytest.raises(ValueError, match="duplicate strategy"):
        RetentionConfig(backend="minio", rules=(rule, rule_b))


def test_two_different_strategies_for_one_bucket_are_allowed() -> None:
    """Object expiry + multipart cleanup on the same bucket is a
    legitimate combination."""
    expire = RetentionRule(
        bucket="trajectories", strategy="expire_after_days", days=30,
    )
    cleanup = RetentionRule(
        bucket="trajectories",
        strategy="cleanup_incomplete_uploads_after_hours",
        hours=168,
    )
    cfg = RetentionConfig(backend="minio", rules=(expire, cleanup))
    assert len(cfg.rules) == 2


# ──────────────────────────────────────────────────────────────────────
# Bucket lifecycle rendering (consolidated single-rule-per-bucket)
# ──────────────────────────────────────────────────────────────────────


def test_expire_after_days_renders_single_consolidated_rule() -> None:
    """One expire_after_days rule renders to one S3 LifecycleRule
    with stable bucket-scoped ID."""
    cfg = RetentionConfig(backend="minio", rules=(
        RetentionRule(
            bucket="trajectories", strategy="expire_after_days", days=30,
        ),
    ))
    out = render_bucket_lifecycle(cfg, bucket="trajectories")
    assert out == {"Rules": [{
        "ID": "loom-trajectories",
        "Status": "Enabled",
        "Filter": {"Prefix": ""},
        "Expiration": {"Days": 30},
    }]}


def test_keep_forever_only_renders_empty_rules() -> None:
    """A bucket whose ONLY config is keep_forever produces no rule —
    the caller skips the bucket. (Multipart cleanup paired with
    keep_forever is covered by a separate test below.)"""
    cfg = RetentionConfig(backend="minio", rules=(
        RetentionRule(bucket="atif", strategy="keep_forever"),
    ))
    out = render_bucket_lifecycle(cfg, bucket="atif")
    assert out == {"Rules": []}


def test_cleanup_strategy_is_currently_unrendered_on_s3() -> None:
    """`cleanup_incomplete_uploads_after_hours` parses but does NOT
    render for the S3 backend. MinIO accepts the apply but silently
    drops `AbortIncompleteMultipartUpload` from the persisted state,
    which would make doctor report perpetual drift. AWS-S3-specific
    renderer can honor the strategy when it ships."""
    cfg = RetentionConfig(backend="minio", rules=(
        RetentionRule(
            bucket="trajectories",
            strategy="cleanup_incomplete_uploads_after_hours",
            hours=336,
        ),
    ))
    out = render_bucket_lifecycle(cfg, bucket="trajectories")
    # Cleanup-only bucket → no rule emitted at all.
    assert out == {"Rules": []}


def test_cleanup_paired_with_expire_renders_only_expire() -> None:
    """A bucket with both expire_after_days and cleanup_incomplete
    renders just the expire action; cleanup is dropped (see above)."""
    cfg = RetentionConfig(backend="minio", rules=(
        RetentionRule(
            bucket="trajectories", strategy="expire_after_days", days=30,
        ),
        RetentionRule(
            bucket="trajectories",
            strategy="cleanup_incomplete_uploads_after_hours",
            hours=336,
        ),
    ))
    out = render_bucket_lifecycle(cfg, bucket="trajectories")
    assert len(out["Rules"]) == 1
    rule = out["Rules"][0]
    assert rule["ID"] == "loom-trajectories"
    assert rule["Expiration"] == {"Days": 30}
    assert "AbortIncompleteMultipartUpload" not in rule


def test_keep_forever_paired_with_cleanup_renders_no_rule() -> None:
    """A bucket configured keep_forever + multipart cleanup renders
    nothing on the S3 backend (cleanup is unrendered; keep_forever
    contributes no action). This is what we want for atif-class
    workloads."""
    cfg = RetentionConfig(backend="minio", rules=(
        RetentionRule(bucket="atif", strategy="keep_forever"),
        RetentionRule(
            bucket="atif",
            strategy="cleanup_incomplete_uploads_after_hours",
            hours=168,
        ),
    ))
    out = render_bucket_lifecycle(cfg, bucket="atif")
    assert out == {"Rules": []}


def test_render_is_byte_stable() -> None:
    """Re-rendering the same config produces the same dict. True
    idempotency of `put_bucket_lifecycle_configuration` depends on it."""
    cfg = RetentionConfig(backend="minio", rules=(
        RetentionRule(
            bucket="trajectories", strategy="expire_after_days", days=30,
        ),
    ))
    a = render_bucket_lifecycle(cfg, bucket="trajectories")
    b = render_bucket_lifecycle(cfg, bucket="trajectories")
    assert a == b


# ──────────────────────────────────────────────────────────────────────
# apply_lifecycle_to_s3 against a stub client
# ──────────────────────────────────────────────────────────────────────


class _StubS3:
    """Capture put_bucket_lifecycle_configuration calls."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def put_bucket_lifecycle_configuration(
        self,
        *,
        Bucket: str,  # noqa: N803  — boto3 API name; must match
        LifecycleConfiguration: dict[str, object],  # noqa: N803
    ) -> None:
        self.calls.append({
            "bucket": Bucket,
            "lifecycle": LifecycleConfiguration,
        })


def test_apply_skips_keep_forever_buckets() -> None:
    cfg = RetentionConfig(backend="minio", rules=(
        RetentionRule(bucket="atif", strategy="keep_forever"),
        RetentionRule(
            bucket="trajectories", strategy="expire_after_days", days=30,
        ),
    ))
    stub = _StubS3()
    applied = apply_lifecycle_to_s3(stub, cfg)
    # `atif` would render to {Rules: []} — must NOT be applied
    # (an empty PUT would clobber any operator-side rules).
    assert "atif" not in applied
    assert "trajectories" in applied
    assert len(stub.calls) == 1
    assert stub.calls[0]["bucket"] == "trajectories"


def test_apply_is_idempotent() -> None:
    cfg = RetentionConfig(backend="minio", rules=(
        RetentionRule(
            bucket="trajectories", strategy="expire_after_days", days=30,
        ),
    ))
    stub = _StubS3()
    apply_lifecycle_to_s3(stub, cfg)
    apply_lifecycle_to_s3(stub, cfg)
    assert len(stub.calls) == 2
    assert stub.calls[0] == stub.calls[1]


def test_apply_respects_buckets_subset() -> None:
    """Caller can apply to a subset of the configured buckets."""
    cfg = RetentionConfig(backend="minio", rules=(
        RetentionRule(
            bucket="trajectories", strategy="expire_after_days", days=30,
        ),
        RetentionRule(
            bucket="artifacts", strategy="expire_after_days", days=180,
        ),
    ))
    stub = _StubS3()
    applied = apply_lifecycle_to_s3(stub, cfg, buckets=("trajectories",))
    assert set(applied) == {"trajectories"}
    assert {c["bucket"] for c in stub.calls} == {"trajectories"}


# ──────────────────────────────────────────────────────────────────────
# TOML loader
# ──────────────────────────────────────────────────────────────────────


def test_loader_parses_well_formed_config(tmp_path: Path) -> None:
    path = tmp_path / "storage-lifecycle.toml"
    path.write_text(textwrap.dedent("""
        backend = "minio"

        [[retention]]
        bucket = "trajectories"
        strategy = "expire_after_days"
        days = 30

        [[retention]]
        bucket = "atif"
        strategy = "keep_forever"
    """))
    cfg = load_retention_config(path)
    assert cfg.backend == "minio"
    assert len(cfg.rules) == 2


def test_loader_missing_backend_raises(tmp_path: Path) -> None:
    path = tmp_path / "storage-lifecycle.toml"
    path.write_text("[[retention]]\nbucket = 'x'\nstrategy = 'keep_forever'\n")
    with pytest.raises(ValueError, match="missing required fields"):
        load_retention_config(path)


def test_loader_unknown_rule_key_raises(tmp_path: Path) -> None:
    path = tmp_path / "storage-lifecycle.toml"
    path.write_text(textwrap.dedent("""
        backend = "minio"

        [[retention]]
        bucket = "trajectories"
        strategy = "expire_after_days"
        days = 30
        retain_for_audit = true
    """))
    with pytest.raises(ValueError, match="unknown keys"):
        load_retention_config(path)


def test_loader_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_retention_config(tmp_path / "missing.toml")


def test_loader_validates_via_dataclass_post_init(tmp_path: Path) -> None:
    """The dataclass __post_init__ checks should surface at load time."""
    path = tmp_path / "storage-lifecycle.toml"
    path.write_text(textwrap.dedent("""
        backend = "minio"

        [[retention]]
        bucket = "trajectories"
        strategy = "expire_after_days"
        days = 0
    """))
    with pytest.raises(ValueError, match="days >= 1"):
        load_retention_config(path)


def test_example_file_loads_cleanly() -> None:
    """The shipped example file is part of the contract — if it
    breaks, every operator's bootstrap step breaks."""
    repo_root = Path(__file__).resolve().parents[2]
    example = repo_root / "config" / "storage-lifecycle.example.toml"
    cfg = load_retention_config(example)
    assert cfg.backend == "minio"
    # The example covers our three reference buckets.
    buckets = {r.bucket for r in cfg.rules}
    assert {"trajectories", "artifacts", "atif"}.issubset(buckets)
