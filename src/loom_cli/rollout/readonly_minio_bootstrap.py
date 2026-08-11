"""Exact read-only MinIO authority for staging preflight and checkpoints."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass

READONLY_MINIO_ACCESS_KEY = "loom-rollout-readonly"
READONLY_MINIO_POLICY_NAME = "loom-rollout-readonly-capacity-v1"
READONLY_MINIO_BUCKETS = (
    "loom-staging-artifacts",
    "loom-staging-trajectories",
)

_SECRET_RE = re.compile(r"^[A-Za-z0-9_-]{32,128}$")


def readonly_minio_policy() -> dict[str, object]:
    """Return exact list and immutable-version read authority."""
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetBucketLocation",
                    "s3:GetBucketVersioning",
                    "s3:ListBucket",
                    "s3:ListBucketVersions",
                ],
                "Resource": [f"arn:aws:s3:::{bucket}" for bucket in READONLY_MINIO_BUCKETS],
            },
            {
                "Effect": "Allow",
                "Action": ["s3:GetObjectVersion"],
                "Resource": [f"arn:aws:s3:::{bucket}/*" for bucket in READONLY_MINIO_BUCKETS],
            },
            {
                "Effect": "Allow",
                "Action": ["admin:ServerInfo"],
                "Resource": ["arn:aws:s3:::*"],
            },
        ],
    }


def readonly_minio_policy_bytes() -> bytes:
    return (
        json.dumps(readonly_minio_policy(), sort_keys=True, separators=(",", ":")).encode() + b"\n"
    )


def readonly_minio_policy_digest() -> str:
    return hashlib.sha256(readonly_minio_policy_bytes()).hexdigest()


@dataclass(frozen=True, slots=True)
class ReadonlyMinioCredential:
    """Private static key whose server policy cannot mutate object state."""

    access_key: str
    secret_key: str
    region: str = "us-east-1"

    def __post_init__(self) -> None:
        if (
            self.access_key != READONLY_MINIO_ACCESS_KEY
            or _SECRET_RE.fullmatch(self.secret_key) is None
            or self.region != "us-east-1"
        ):
            raise ValueError("readonly MinIO credential authority is invalid")

    @property
    def metadata_digest(self) -> str:
        payload = json.dumps(
            {
                "access_key_sha256": hashlib.sha256(self.access_key.encode()).hexdigest(),
                "policy_sha256": readonly_minio_policy_digest(),
                "region": self.region,
                "schema_version": 1,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(payload).hexdigest()

    def to_bytes(self) -> bytes:
        return (
            json.dumps(
                {
                    "access_key": self.access_key,
                    "region": self.region,
                    "schema_version": 1,
                    "secret_key": self.secret_key,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            + b"\n"
        )

    @classmethod
    def from_bytes(cls, payload: bytes) -> ReadonlyMinioCredential:
        try:
            raw = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("readonly MinIO credential is invalid") from exc
        if (
            not isinstance(raw, dict)
            or set(raw)
            != {
                "access_key",
                "region",
                "schema_version",
                "secret_key",
            }
            or raw.get("schema_version") != 1
        ):
            raise ValueError("readonly MinIO credential is invalid")
        access_key = raw.get("access_key")
        secret_key = raw.get("secret_key")
        region = raw.get("region")
        if (
            not isinstance(access_key, str)
            or not isinstance(secret_key, str)
            or not isinstance(region, str)
        ):
            raise ValueError("readonly MinIO credential is invalid")
        return cls(access_key=access_key, secret_key=secret_key, region=region)


__all__ = [
    "READONLY_MINIO_ACCESS_KEY",
    "READONLY_MINIO_BUCKETS",
    "READONLY_MINIO_POLICY_NAME",
    "ReadonlyMinioCredential",
    "readonly_minio_policy",
    "readonly_minio_policy_bytes",
    "readonly_minio_policy_digest",
]
