"""Read the durable personal-dev native-builder agent status."""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

import boto3
from botocore.config import Config
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

from loom.db.schema import PersonalDevNativeBuilderAgent, PersonalDevNativeBuildGrant
from loom.personal_dev_native_builder_protocol import NativeBuilderAgentStatus
from loom_benchmark_tool.db_url import normalize_db_url

_SCHEMA = "loom-personal-dev-native-builder-agent-status-v1"
_PUBLIC_STORE_ERROR = "personal-dev native-builder public store is unavailable"
_BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
_REGION = re.compile(r"[a-z0-9][a-z0-9-]{0,62}")


class NativeBuilderProbeError(RuntimeError):
    """The durable native-builder identity cannot be reported unambiguously."""


class NativeBuilderProbeInventoryDriftError(NativeBuilderProbeError):
    """The signed agent inventory differs from durable running grants."""


class NativeBuilderPublicStoreUnavailableError(NativeBuilderProbeError):
    """The public artifact bucket cannot be authenticated and read."""


def _public_store_error() -> NativeBuilderPublicStoreUnavailableError:
    return NativeBuilderPublicStoreUnavailableError(_PUBLIC_STORE_ERROR)


def _probe_public_store_client(client: Any, *, bucket: str) -> None:
    """Require one exact successful authenticated HEAD of the artifact bucket."""

    try:
        response = client.head_bucket(Bucket=bucket)
    except Exception:
        raise _public_store_error() from None
    if not isinstance(response, Mapping):
        raise _public_store_error()
    metadata = response.get("ResponseMetadata")
    if not isinstance(metadata, Mapping):
        raise _public_store_error()
    status_code = metadata.get("HTTPStatusCode")
    if type(status_code) is not int or status_code != 200:
        raise _public_store_error()


def _probe_public_store_from_environment() -> None:
    """Probe the configured public store only for explicitly enabled native mode."""

    if os.environ.get("LOOM_SVC_PERSONAL_DEV_NATIVE_BUILDER_ENABLED") != "true":
        return
    endpoint = os.environ.get("LOOM_SVC_MINIO_PUBLIC_ENDPOINT", "")
    access_key = os.environ.get("LOOM_SVC_MINIO_ACCESS_KEY", "")
    secret_key = os.environ.get("LOOM_SVC_MINIO_SECRET_KEY", "")
    region = os.environ.get("LOOM_SVC_MINIO_REGION", "")
    bucket = os.environ.get("LOOM_SVC_ARTIFACTS_BUCKET", "")
    try:
        parsed = urlsplit(endpoint)
        port = parsed.port
        endpoint.encode("ascii")
    except (UnicodeError, ValueError):
        raise _public_store_error() from None
    if (
        not 1 <= len(endpoint) <= 2048
        or parsed.scheme != "https"
        or not parsed.netloc
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or (port is not None and not 1 <= port <= 65535)
        or any(character in endpoint for character in "\r\n\0")
        or not access_key
        or len(access_key) > 1024
        or not secret_key
        or len(secret_key) > 4096
        or _REGION.fullmatch(region) is None
        or _BUCKET.fullmatch(bucket) is None
        or ".." in bucket
        or ".-" in bucket
        or "-." in bucket
    ):
        raise _public_store_error()

    try:
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            verify=True,
            config=Config(
                signature_version="s3v4",
                connect_timeout=2,
                read_timeout=2,
                retries={"total_max_attempts": 1, "mode": "standard"},
                proxies={},
            ),
        )
        close = getattr(client, "close", None)
        if not callable(close):
            raise _public_store_error()
        try:
            _probe_public_store_client(client, bucket=bucket)
        finally:
            close()
    except NativeBuilderPublicStoreUnavailableError:
        raise
    except Exception:
        raise _public_store_error() from None


def _uuid_inventory(value: object) -> tuple[UUID, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise NativeBuilderProbeError("native builder agent inventory is invalid")
    try:
        parsed = tuple(UUID(item) for item in value)
    except ValueError:
        raise NativeBuilderProbeError(
            "native builder agent inventory is invalid"
        ) from None
    if any(str(item) != raw for item, raw in zip(parsed, value, strict=True)):
        raise NativeBuilderProbeError("native builder agent inventory is invalid")
    return parsed


def _utc_timestamp(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise NativeBuilderProbeError("native builder last-seen time is invalid")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


async def read_native_builder_agent_status(
    session: AsyncSession,
) -> dict[str, object]:
    """Return at most one exact, secret-free durable agent identity."""

    result = await session.execute(
        select(
            PersonalDevNativeBuilderAgent.instance_id,
            PersonalDevNativeBuilderAgent.key_id,
            PersonalDevNativeBuilderAgent.provider,
            PersonalDevNativeBuilderAgent.platform,
            PersonalDevNativeBuilderAgent.protocol_version,
            PersonalDevNativeBuilderAgent.host_name,
            PersonalDevNativeBuilderAgent.host_architecture,
            PersonalDevNativeBuilderAgent.host_boot_id,
            PersonalDevNativeBuilderAgent.agent_image,
            PersonalDevNativeBuilderAgent.builder_image,
            PersonalDevNativeBuilderAgent.runtime_profile_sha256,
            PersonalDevNativeBuilderAgent.max_concurrency,
            PersonalDevNativeBuilderAgent.managed_grant_ids_json,
            PersonalDevNativeBuilderAgent.active_grant_ids_json,
            PersonalDevNativeBuilderAgent.available,
            PersonalDevNativeBuilderAgent.unavailable_reason,
            PersonalDevNativeBuilderAgent.readiness_evidence_sha256,
            PersonalDevNativeBuilderAgent.last_seen_at,
        )
        .order_by(PersonalDevNativeBuilderAgent.instance_id)
        .limit(2)
        .execution_options(autoflush=False)
    )
    rows = result.mappings().all()
    if len(rows) > 1:
        raise NativeBuilderProbeError("native builder agent identity is ambiguous")
    if not rows:
        return {"agent": None, "schema": _SCHEMA}

    row = rows[0]
    try:
        status = NativeBuilderAgentStatus(
            agent_instance_id=row["instance_id"],
            agent_key_id=row["key_id"],
            provider=row["provider"],
            platform=row["platform"],
            protocol_version=row["protocol_version"],
            host_name=row["host_name"],
            host_architecture=row["host_architecture"],
            host_boot_id=row["host_boot_id"],
            agent_image=row["agent_image"],
            builder_image=row["builder_image"],
            runtime_profile_sha256=row["runtime_profile_sha256"],
            max_concurrency=row["max_concurrency"],
            managed_grant_ids=_uuid_inventory(row["managed_grant_ids_json"]),
            active_grant_ids=_uuid_inventory(row["active_grant_ids_json"]),
            available=row["available"],
            unavailable_reason=row["unavailable_reason"],
            readiness_evidence_sha256=row["readiness_evidence_sha256"],
        )
        agent: dict[str, Any] = json.loads(status.canonical_bytes())
        agent["last_seen_at"] = _utc_timestamp(row["last_seen_at"])
    except (TypeError, ValueError) as exc:
        raise NativeBuilderProbeError(
            "native builder agent status is invalid"
        ) from exc

    assigned = (
        await session.execute(
            select(
                PersonalDevNativeBuildGrant.id,
                PersonalDevNativeBuildGrant.required_agent_key_id,
                PersonalDevNativeBuildGrant.provider,
                PersonalDevNativeBuildGrant.platform,
                PersonalDevNativeBuildGrant.agent_image,
                PersonalDevNativeBuildGrant.builder_image,
                PersonalDevNativeBuildGrant.runtime_profile_sha256,
            )
            .where(
                PersonalDevNativeBuildGrant.running_agent_instance_id
                == status.agent_instance_id,
                PersonalDevNativeBuildGrant.state == "running",
            )
            .order_by(PersonalDevNativeBuildGrant.id)
            .limit(65)
            .execution_options(autoflush=False)
        )
    ).mappings().all()
    assigned_ids = tuple(item["id"] for item in assigned)
    if assigned_ids != status.managed_grant_ids or any(
        item["required_agent_key_id"] != status.agent_key_id
        or item["provider"] != status.provider
        or item["platform"] != status.platform
        or item["agent_image"] != status.agent_image
        or item["builder_image"] != status.builder_image
        or item["runtime_profile_sha256"] != status.runtime_profile_sha256
        for item in assigned
    ):
        raise NativeBuilderProbeInventoryDriftError(
            "native builder durable inventory drift was detected"
        )
    return {"agent": agent, "schema": _SCHEMA}


def canonical_native_builder_agent_status(value: dict[str, object]) -> bytes:
    """Serialize one probe document as canonical newline-terminated ASCII JSON."""

    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        + b"\n"
    )


async def _run(database_url: str) -> bytes:
    await asyncio.to_thread(_probe_public_store_from_environment)
    engine = create_async_engine(
        normalize_db_url(database_url),
        isolation_level="REPEATABLE READ",
        poolclass=NullPool,
    )
    try:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            await session.execute(text("SET TRANSACTION READ ONLY"))
            status = await read_native_builder_agent_status(session)
            return canonical_native_builder_agent_status(status)
    finally:
        await engine.dispose()


def main() -> int:
    database_url = os.environ.get("LOOM_SVC_DB_URL", "")
    if not database_url:
        sys.stderr.write("personal-dev native-builder probe failed\n")
        return 2
    try:
        payload = asyncio.run(_run(database_url))
        sys.stdout.buffer.write(payload)
        sys.stdout.buffer.flush()
    except NativeBuilderProbeInventoryDriftError:
        sys.stderr.write("personal-dev native-builder probe failed\n")
        return 3
    except NativeBuilderPublicStoreUnavailableError:
        sys.stderr.write("personal-dev native-builder probe failed\n")
        return 4
    except Exception:
        sys.stderr.write("personal-dev native-builder probe failed\n")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
