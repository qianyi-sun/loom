from __future__ import annotations

import pytest

from loom_cli.rollout.operator.staging_epoch_sql import (
    READ_STAGING_EPOCH_SQL,
    STAGING_EPOCH_SQL_CONTRACT_DIGEST,
    render_staging_epoch_advance_sql,
)


def test_epoch_sql_binds_exact_validated_literals_without_psql_variables() -> None:
    statement = render_staging_epoch_advance_sql(
        request_id="req-manifest-ownership-deadbeef",
        evidence_sha256="a" * 64,
        expected_epoch=2,
        allow_bootstrap=False,
    )

    assert "\n" not in statement
    assert ":'" not in statement
    assert "__" not in statement
    assert "request_id = 'req-manifest-ownership-deadbeef'" in statement
    assert f"evidence_sha256 = '{'a' * 64}'" in statement
    assert "AND epoch = 2::bigint" in statement
    assert "WHERE false" in statement
    assert len(STAGING_EPOCH_SQL_CONTRACT_DIGEST) == 64
    assert "\n" not in READ_STAGING_EPOCH_SQL


@pytest.mark.parametrize(
    ("request_id", "evidence_sha256", "expected_epoch", "allow_bootstrap"),
    [
        ("req-alpha';DELETE", "a" * 64, 2, False),
        ("req-alpha", "g" * 64, 2, False),
        ("req-alpha", "a" * 64, -1, False),
        ("req-alpha", "a" * 64, True, False),
        ("req-alpha", "a" * 64, 2, 1),
    ],
)
def test_epoch_sql_rejects_unbounded_or_ambiguous_authority(
    request_id: str,
    evidence_sha256: str,
    expected_epoch: int,
    allow_bootstrap: bool,
) -> None:
    with pytest.raises(ValueError, match="authority"):
        render_staging_epoch_advance_sql(
            request_id=request_id,
            evidence_sha256=evidence_sha256,
            expected_epoch=expected_epoch,
            allow_bootstrap=allow_bootstrap,
        )


def test_epoch_sql_bootstrap_is_an_explicit_boolean_literal() -> None:
    statement = render_staging_epoch_advance_sql(
        request_id="req-alpha",
        evidence_sha256="f" * 64,
        expected_epoch=0,
        allow_bootstrap=True,
    )

    assert "WHERE true" in statement
    assert "AND epoch = 0::bigint" in statement
