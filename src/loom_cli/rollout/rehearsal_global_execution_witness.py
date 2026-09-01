"""Isolated deterministic signing authority for rehearsal-only witnesses."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from loom_capacity_manager.global_execution_witness import (
    build_global_execution_witness_export,
)

_REHEARSAL_NAMESPACE_RE = re.compile(r"loom-rehearsal-[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?\Z")
_SIGNING_DOMAIN = b"loom-rehearsal-global-execution-witness-v1\0"
_PLAN_DIGEST_RE = re.compile(r"[0-9a-f]{64}\Z")
# Four sequential transient units are each bounded at three minutes. Keep the
# isolated fixture fresh across that full validation window plus cleanup.
_WITNESS_LIFETIME = timedelta(minutes=30)


def rehearsal_global_execution_signing_authority(
    namespace: str,
) -> tuple[Ed25519PrivateKey, str]:
    """Return a namespace-bound test signer and its public-key fingerprint."""

    if not isinstance(namespace, str) or _REHEARSAL_NAMESPACE_RE.fullmatch(namespace) is None:
        raise ValueError("rehearsal global execution namespace is invalid")
    private_key = Ed25519PrivateKey.from_private_bytes(
        hashlib.sha256(_SIGNING_DOMAIN + namespace.encode("ascii")).digest()
    )
    public_key = private_key.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    return private_key, hashlib.sha256(public_key).hexdigest()


def rehearsal_global_execution_public_key_sha256(namespace: str) -> str:
    """Return only the isolated signer fingerprint embedded in validation argv."""

    _private_key, fingerprint = rehearsal_global_execution_signing_authority(namespace)
    return fingerprint


def build_rehearsal_global_execution_witness_config_map(
    *,
    namespace: str,
    plan_digest: str,
    now: datetime | None = None,
) -> dict[str, object]:
    """Build two valid, isolated shadow witnesses for supervisor validation."""

    if not isinstance(plan_digest, str) or _PLAN_DIGEST_RE.fullmatch(plan_digest) is None:
        raise ValueError("rehearsal global execution plan digest is invalid")
    observed_at = datetime.now(UTC) if now is None else now
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("rehearsal global execution clock is invalid")
    private_key, _fingerprint = rehearsal_global_execution_signing_authority(namespace)
    expires_at = observed_at.astimezone(UTC) + _WITNESS_LIFETIME
    data = {
        f"{pool_id}.json": build_global_execution_witness_export(
            private_key=private_key,
            signing_key_id="loom-rehearsal-v1",
            pool_id=pool_id,
            execution_epoch=0,
            execution_state="shadow",
            executable_new_capacity_ceiling=0,
            expires_at=expires_at,
        ).decode("ascii")
        for pool_id in ("gb10", "oldlab")
    }
    manifest: dict[str, object] = {
        "apiVersion": "v1",
        "data": data,
        "kind": "ConfigMap",
        "metadata": {
            "annotations": {"loom.openai.dev/plan-sha256": plan_digest},
            "name": "loom-global-execution-witness-v1",
            "namespace": namespace,
        },
    }
    return manifest


def rehearsal_global_execution_witness_config_map_ready(
    observed: object,
    *,
    expected: Mapping[str, object],
) -> bool:
    """Return whether Kubernetes retained the exact isolated witness data."""

    if not isinstance(observed, Mapping):
        return False
    metadata = observed.get("metadata")
    expected_metadata = expected.get("metadata")
    return bool(
        observed.get("apiVersion") == "v1"
        and observed.get("kind") == "ConfigMap"
        and isinstance(metadata, Mapping)
        and isinstance(expected_metadata, Mapping)
        and metadata.get("name") == expected_metadata.get("name")
        and metadata.get("namespace") == expected_metadata.get("namespace")
        and metadata.get("annotations") == expected_metadata.get("annotations")
        and observed.get("data") == expected.get("data")
    )


__all__ = [
    "build_rehearsal_global_execution_witness_config_map",
    "rehearsal_global_execution_public_key_sha256",
    "rehearsal_global_execution_signing_authority",
    "rehearsal_global_execution_witness_config_map_ready",
]
