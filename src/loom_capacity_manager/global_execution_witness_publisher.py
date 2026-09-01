"""Publish short-lived signed execution witnesses through a stable ConfigMap."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import ssl
import urllib.request
from datetime import timedelta
from ipaddress import ip_address
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom_capacity_manager.config import read_owner_only_secret
from loom_capacity_manager.global_execution_witness import (
    build_current_global_execution_witness_export,
    load_global_execution_signing_key,
)

_LOGGER = logging.getLogger(__name__)
_CONFIG_MAP_NAMESPACE = "loom-dev"
_CONFIG_MAP_NAME = "loom-global-execution-witness-v1"
_POOLS = ("gb10", "oldlab")
_WITNESS_TTL = timedelta(seconds=30)
_MAX_EXPORT_BYTES = 64 * 1024
_MAX_CA_BYTES = 64 * 1024
_MAX_RESPONSE_BYTES = 256 * 1024
_MAX_TOKEN_BYTES = 16 * 1024
_KEY_ID = re.compile(r"[a-z0-9][a-z0-9_.-]{0,127}")


class GlobalExecutionWitnessPublisherSettings(BaseSettings):
    """Protected inputs and bounded Kubernetes publication coordinates."""

    model_config = SettingsConfigDict(
        env_prefix="LOOM_CAPACITY_WITNESS_",
        extra="forbid",
        frozen=True,
    )

    db_url_file: Path
    expected_authority_incarnation: UUID
    signing_key_file: Path
    signing_key_id: str
    kubernetes_api_server: str
    kubernetes_token_file: Path = Path("/var/run/secrets/loom-witness/token")
    kubernetes_ca_file: Path = Path("/var/run/secrets/loom-witness/ca.crt")
    request_timeout_seconds: float = Field(default=5.0, gt=0, le=15)
    interval_seconds: float = Field(default=10.0, ge=5, le=20)

    @field_validator("signing_key_id")
    @classmethod
    def _valid_key_id(cls, value: str) -> str:
        if _KEY_ID.fullmatch(value) is None:
            raise ValueError("global execution signing key id is invalid")
        return value

    @field_validator("kubernetes_api_server")
    @classmethod
    def _safe_api_server(cls, value: str) -> str:
        parsed = urlsplit(value)
        try:
            address = ip_address(parsed.hostname or "")
            port = parsed.port
        except ValueError as exc:
            raise ValueError("Kubernetes API server is invalid") from exc
        if (
            parsed.scheme != "https"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path
            or parsed.query
            or parsed.fragment
            or port is None
            or not address.is_private
            or address.is_loopback
            or address.is_link_local
            or address.is_multicast
            or address.is_unspecified
        ):
            raise ValueError("Kubernetes API server is invalid")
        return f"https://{address.compressed}:{port}"


def _read_bounded_projected_file(
    path: Path,
    *,
    max_bytes: int,
    error: str,
) -> bytes:
    with path.open("rb") as stream:
        payload = stream.read(max_bytes + 1)
    if not 0 < len(payload) <= max_bytes:
        raise ValueError(error)
    return payload


def _read_projected_token(path: Path) -> str:
    payload = _read_bounded_projected_file(
        path,
        max_bytes=_MAX_TOKEN_BYTES,
        error="projected Kubernetes token is invalid",
    )
    try:
        token = payload.decode("ascii").strip()
    except UnicodeDecodeError as exc:
        raise ValueError("projected Kubernetes token is invalid") from exc
    if not token or any(character.isspace() for character in token):
        raise ValueError("projected Kubernetes token is invalid")
    return token


async def _build_current_exports(
    settings: GlobalExecutionWitnessPublisherSettings,
) -> dict[str, bytes]:
    private_key = load_global_execution_signing_key(settings.signing_key_file)
    database_url = read_owner_only_secret(settings.db_url_file)
    engine = create_async_engine(database_url, pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with session_factory() as session:
            try:
                return {
                    pool_id: await build_current_global_execution_witness_export(
                        session,
                        private_key=private_key,
                        signing_key_id=settings.signing_key_id,
                        expected_authority_incarnation=(settings.expected_authority_incarnation),
                        pool_id=pool_id,
                        ttl=_WITNESS_TTL,
                    )
                    for pool_id in _POOLS
                }
            finally:
                await session.rollback()
    finally:
        await engine.dispose()


def _validated_config_map_data(exports: dict[str, bytes]) -> dict[str, str]:
    if set(exports) != set(_POOLS):
        raise ValueError("global execution witness exports are invalid")
    output: dict[str, str] = {}
    for pool_id in _POOLS:
        encoded = exports[pool_id]
        if not isinstance(encoded, bytes) or not 0 < len(encoded) <= _MAX_EXPORT_BYTES:
            raise ValueError("global execution witness export is invalid")
        try:
            value = encoded.decode("ascii")
        except UnicodeDecodeError as exc:
            raise ValueError("global execution witness export is invalid") from exc
        if not value.endswith("\n") or "\x00" in value:
            raise ValueError("global execution witness export is invalid")
        output[f"{pool_id}.json"] = value
    return output


def _patch_config_map(
    settings: GlobalExecutionWitnessPublisherSettings,
    exports: dict[str, bytes],
) -> None:
    data = json.dumps(
        {"data": _validated_config_map_data(exports)},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    token = _read_projected_token(settings.kubernetes_token_file)
    url = (
        f"{settings.kubernetes_api_server}/api/v1/namespaces/"
        f"{_CONFIG_MAP_NAMESPACE}/configmaps/{_CONFIG_MAP_NAME}"
    )
    request = urllib.request.Request(
        url,
        data=data,
        method="PATCH",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/merge-patch+json",
            "Accept": "application/json",
        },
    )
    ca_payload = _read_bounded_projected_file(
        settings.kubernetes_ca_file,
        max_bytes=_MAX_CA_BYTES,
        error="projected Kubernetes CA bundle is invalid",
    )
    try:
        ca_bundle = ca_payload.decode("ascii")
    except UnicodeDecodeError as exc:
        raise ValueError("projected Kubernetes CA bundle is invalid") from exc
    context = ssl.create_default_context(cadata=ca_bundle)
    with urllib.request.urlopen(
        request,
        timeout=settings.request_timeout_seconds,
        context=context,
    ) as response:
        body = response.read(_MAX_RESPONSE_BYTES + 1)
        if response.status != 200 or len(body) > _MAX_RESPONSE_BYTES:
            raise RuntimeError("global execution witness publication was rejected")


async def publish_global_execution_witnesses_once(
    settings: GlobalExecutionWitnessPublisherSettings,
) -> None:
    """Generate both pool exports before atomically publishing either one."""

    exports = await _build_current_exports(settings)
    _validated_config_map_data(exports)
    await asyncio.to_thread(_patch_config_map, settings, exports)


async def run_global_execution_witness_publisher(
    settings: GlobalExecutionWitnessPublisherSettings,
) -> None:
    while True:
        try:
            await publish_global_execution_witnesses_once(settings)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _LOGGER.error(
                "global execution witness publication failed safely (%s)",
                type(exc).__name__,
            )
        await asyncio.sleep(settings.interval_seconds)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    asyncio.run(
        run_global_execution_witness_publisher(
            GlobalExecutionWitnessPublisherSettings(),
        )
    )


__all__ = [
    "GlobalExecutionWitnessPublisherSettings",
    "main",
    "publish_global_execution_witnesses_once",
    "run_global_execution_witness_publisher",
]


if __name__ == "__main__":  # pragma: no cover - module entry point
    main()
