"""Produce short-lived manager-signed legacy execution witnesses."""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from uuid import UUID

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom_capacity_manager.config import (
    CapacityManagerSettings,
    read_owner_only_bytes,
    read_owner_only_secret,
)
from loom_capacity_manager.models import CapacityAuthorityState

_EXPORT_TIMEOUT_SECONDS = 10.0
_WITNESS_TTL = timedelta(seconds=30)
_SIGNED_FIELDS = frozenset(
    {
        "authority",
        "pool_id",
        "execution_epoch",
        "execution_state",
        "executable_new_capacity_ceiling",
        "expires_at",
        "signing_key_id",
    }
)


def _canonical_witness_bytes(value: Mapping[str, object]) -> bytes:
    payload = {key: value[key] for key in sorted(_SIGNED_FIELDS) if key in value}
    if set(payload) != _SIGNED_FIELDS:
        raise ValueError("global execution witness fields are invalid")
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def load_global_execution_signing_key(path: Path) -> Ed25519PrivateKey:
    """Load one raw Ed25519 private key from a current-UID 0600 file."""

    try:
        payload = read_owner_only_bytes(path, max_bytes=32)
    except (OSError, ValueError) as exc:
        raise ValueError("global execution signing key must be owner-only") from exc
    if len(payload) != 32:
        raise ValueError("global execution signing key is invalid")
    return Ed25519PrivateKey.from_private_bytes(payload)


def build_global_execution_witness_export(
    *,
    private_key: Ed25519PrivateKey,
    signing_key_id: str,
    pool_id: str,
    execution_epoch: int,
    execution_state: str,
    executable_new_capacity_ceiling: int,
    expires_at: datetime,
) -> bytes:
    """Return one canonical signed witness with its independently pinnable key."""

    if not isinstance(private_key, Ed25519PrivateKey):
        raise ValueError("global execution signing key is invalid")
    if expires_at.tzinfo is None or expires_at.utcoffset() is None:
        raise ValueError("global execution witness expiry is invalid")
    witness: dict[str, object] = {
        "authority": "global-capacity-manager",
        "pool_id": pool_id,
        "execution_epoch": execution_epoch,
        "execution_state": execution_state,
        "executable_new_capacity_ceiling": executable_new_capacity_ceiling,
        "expires_at": expires_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "signing_key_id": signing_key_id,
    }
    canonical = _canonical_witness_bytes(witness)
    witness["canonical_digest"] = hashlib.sha256(canonical).hexdigest()
    witness["signature_base64"] = base64.b64encode(private_key.sign(canonical)).decode("ascii")
    public_key = private_key.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    exported: Mapping[str, object] = {
        "manager_public_key_base64": base64.b64encode(public_key).decode("ascii"),
        "manager_public_key_sha256": hashlib.sha256(public_key).hexdigest(),
        "schema_version": 1,
        "witness": witness,
    }
    return (
        json.dumps(
            cast(dict[str, object], exported),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        + b"\n"
    )


async def build_current_global_execution_witness_export(
    session: Any,
    *,
    private_key: Ed25519PrivateKey,
    signing_key_id: str,
    expected_authority_incarnation: UUID,
    pool_id: str,
    ttl: timedelta,
) -> bytes:
    """Read and sign one authority snapshot using the database clock."""

    result = await session.execute(
        select(CapacityAuthorityState, func.clock_timestamp()).where(
            CapacityAuthorityState.singleton_id == 1
        )
    )
    authority, database_now = result.one()
    if authority.authority_incarnation != expected_authority_incarnation:
        raise ValueError("capacity authority incarnation mismatch")
    if not isinstance(database_now, datetime) or (
        database_now.tzinfo is None or database_now.utcoffset() is None
    ):
        raise ValueError("capacity database clock is invalid")
    if not isinstance(ttl, timedelta) or not timedelta(0) < ttl <= timedelta(minutes=2):
        raise ValueError("global execution witness TTL is invalid")
    return build_global_execution_witness_export(
        private_key=private_key,
        signing_key_id=signing_key_id,
        pool_id=pool_id,
        execution_epoch=authority.execution_epoch,
        execution_state=authority.execution_state,
        executable_new_capacity_ceiling=authority.executable_new_capacity_ceiling,
        expires_at=database_now + ttl,
    )


async def export_global_execution_witness(
    settings: CapacityManagerSettings,
    *,
    pool_id: str,
) -> bytes:
    """Load protected inputs and export one current database-backed witness."""

    key_path = settings.global_execution_signing_key_file
    signing_key_id = settings.global_execution_signing_key_id
    if key_path is None or signing_key_id is None:
        raise ValueError("global execution signing key is unavailable")
    private_key = load_global_execution_signing_key(key_path)
    database_url = read_owner_only_secret(settings.db_url_file)
    engine = create_async_engine(database_url, pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with asyncio.timeout(_EXPORT_TIMEOUT_SECONDS):
            async with session_factory() as session:
                try:
                    return await build_current_global_execution_witness_export(
                        session,
                        private_key=private_key,
                        signing_key_id=signing_key_id,
                        expected_authority_incarnation=(
                            settings.expected_authority_incarnation
                        ),
                        pool_id=pool_id,
                        ttl=_WITNESS_TTL,
                    )
                finally:
                    await session.rollback()
    finally:
        await engine.dispose()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export one short-lived manager-signed execution witness."
    )
    parser.add_argument("--pool-id", choices=("gb10", "oldlab"), required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    try:
        arguments = _parser().parse_args(argv)
        encoded = asyncio.run(
            export_global_execution_witness(
                CapacityManagerSettings(),
                pool_id=arguments.pool_id,
            )
        )
        sys.stdout.buffer.write(encoded)
        sys.stdout.buffer.flush()
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception:
        sys.stderr.write("error: global execution witness export failed safely\n")
        raise SystemExit(1) from None


__all__ = [
    "build_current_global_execution_witness_export",
    "build_global_execution_witness_export",
    "export_global_execution_witness",
    "load_global_execution_signing_key",
    "main",
]


if __name__ == "__main__":  # pragma: no cover - module entry point
    main()
