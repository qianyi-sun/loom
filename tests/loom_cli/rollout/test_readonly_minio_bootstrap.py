from __future__ import annotations

import json

import pytest

from loom_cli.rollout.readonly_minio_bootstrap import (
    READONLY_MINIO_ACCESS_KEY,
    READONLY_MINIO_BUCKETS,
    ReadonlyMinioCredential,
    readonly_minio_policy,
    readonly_minio_policy_digest,
)


def test_readonly_minio_policy_has_exact_non_mutating_staging_authority() -> None:
    policy = readonly_minio_policy()
    bucket_statement, object_statement, server_info_statement = policy["Statement"]

    assert bucket_statement["Effect"] == "Allow"
    assert bucket_statement["Action"] == [
        "s3:GetBucketLocation",
        "s3:GetBucketVersioning",
        "s3:ListBucket",
        "s3:ListBucketVersions",
    ]
    assert bucket_statement["Resource"] == [
        f"arn:aws:s3:::{bucket}" for bucket in READONLY_MINIO_BUCKETS
    ]
    assert object_statement == {
        "Effect": "Allow",
        "Action": ["s3:GetObjectVersion"],
        "Resource": [f"arn:aws:s3:::{bucket}/*" for bucket in READONLY_MINIO_BUCKETS],
    }
    assert server_info_statement == {
        "Effect": "Allow",
        "Action": ["admin:ServerInfo"],
        "Resource": ["arn:aws:s3:::*"],
    }
    rendered = json.dumps(policy).lower()
    assert "deleteobject" not in rendered
    assert "putobject" not in rendered
    assert "serverupdate" not in rendered
    assert "servicerestart" not in rendered
    assert "servicestop" not in rendered
    assert len(readonly_minio_policy_digest()) == 64


def test_readonly_minio_credential_round_trips_without_secret_in_metadata() -> None:
    credential = ReadonlyMinioCredential(
        access_key=READONLY_MINIO_ACCESS_KEY,
        secret_key="a" * 48,
    )

    assert ReadonlyMinioCredential.from_bytes(credential.to_bytes()) == credential
    assert credential.secret_key not in credential.metadata_digest


@pytest.mark.parametrize(
    "payload",
    (
        b"{}",
        json.dumps(
            {
                "access_key": "wrong",
                "region": "us-east-1",
                "schema_version": 1,
                "secret_key": "a" * 48,
            }
        ).encode(),
        json.dumps(
            {
                "access_key": READONLY_MINIO_ACCESS_KEY,
                "region": "us-east-1",
                "schema_version": 1,
                "secret_key": "short",
            }
        ).encode(),
    ),
)
def test_readonly_minio_credential_rejects_drift(payload: bytes) -> None:
    with pytest.raises(ValueError, match="credential"):
        ReadonlyMinioCredential.from_bytes(payload)
